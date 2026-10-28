import functools
import logging
import os
import random
import string
from collections.abc import Callable, Generator, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import orjson
import psycopg
import pyarrow as pa
import pyarrow.fs
import pypgstac.hydration
import shapely.wkb
from psycopg import sql
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb, set_json_dumps, set_json_loads
from pypgstac.hydration import DO_NOT_MERGE_MARKER

from stac_geoparquet._wkb import ewkb_to_iso
from stac_geoparquet.arrow import (
    ACCEPTED_SCHEMA_OPTIONS,
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
    InferredSchema,
    parse_stac_items_to_arrow,
)
from stac_geoparquet.arrow._api import parse_stac_items_to_parquet
from stac_geoparquet.arrow._util import env_flag, resolve_output_path_and_filesystem

logger = logging.getLogger(__name__)


def dumps(data: dict) -> str:
    """
    Custom JSON dumps function for psycopg.
    """
    return orjson.dumps(data).decode()


set_json_dumps(dumps)
set_json_loads(orjson.loads)

logger = logging.getLogger(__name__)

# How many rows are read at a time by default, which psycopg holds in memory. A
# server-side cursor waits a round trip for each fetch, so it fetches more.
STREAM_ITERSIZE = 100
FETCH_ITERSIZE = 1000

PGSTAC_SCHEMA_OPTIONS = ACCEPTED_SCHEMA_OPTIONS | Literal["PreScan"]
"""The `schema` options of the pgstac functions, which add "PreScan"."""

# Stands in for each geometry while pgstac_schema infers a schema
_EMPTY_WKB = shapely.to_wkb(shapely.Point(), flavor="iso")

# The columns of pgstac's items table that PgstacRowFactory reads, in order
_ITEM_COLUMNS = ("id", "geometry", "collection", "content")

GEOMETRY_OPTIONS = Literal["geojson", "wkb"]
"""How items' geometries are given: as GeoJSON, or as ISO WKB bytes."""


class PgstacRowFactory:
    """
    Row factory for psycopg that builds a STAC item dict from the `id`,
    `geometry`, `collection` and `content` columns of pgstac's items table.

    If `wkb_geometry` is True, the geometry is ISO WKB bytes instead of GeoJSON.
    Base items are looked up on a new connection to the cursor's database, as
    the cursor's own is busy reading the rows.
    """

    def __init__(self, cursor: psycopg.Cursor[Any], wkb_geometry: bool = False) -> None:
        self.cursor = cursor
        self.wkb_geometry = wkb_geometry
        self._base_items: dict[tuple[str, int | None], dict[str, Any]] = {}

    def __call__(
        self, values: tuple[str, str | bytes, str, dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Convert the values to a dictionary.
        """
        item_id, wkb, collection, content = values
        # A binary cursor returns EWKB bytes, a text cursor hex
        if isinstance(wkb, str):
            wkb = bytes.fromhex(wkb)
        if self.wkb_geometry:
            geometry = ewkb_to_iso(wkb)
        else:
            geometry = shapely.wkb.loads(wkb).__geo_interface__
        item: dict[str, Any] = {
            "id": item_id,
            "geometry": geometry,
            "collection": collection,
        }
        if "bbox" in content:
            item["bbox"] = content["bbox"]
        else:
            item["bbox"] = list(shapely.wkb.loads(wkb).bounds)

        item["assets"] = content["assets"]
        if "links" in content:
            item["links"] = content["links"]
        if "stac_extensions" in content:
            item["stac_extensions"] = content["stac_extensions"]
        # pgstac only stores stac_version when it differs from the collection's
        if "stac_version" in content:
            item["stac_version"] = content["stac_version"]
        item["properties"] = content["properties"]

        # pgstac 0.10 tags an item stored after its collection changed with the
        # base item it was dehydrated against
        base_item = self.get_baseitem(
            item["collection"], content.get("pgstac:base_item")
        )
        pypgstac.hydration.hydrate(base_item, item)
        logger.debug(item)
        return item

    def get_baseitem(
        self, collection: str, base_item_id: int | None = None
    ) -> dict[str, Any]:
        """
        Get the base item for the collection, or the one with `base_item_id`.
        """
        key = (collection, base_item_id)
        if key in self._base_items:
            return self._base_items[key]
        logger.info(f"Getting Base Item for {collection}")
        conninfo = self.cursor.connection.info
        dsn = conninfo.dsn
        password = conninfo.password
        with psycopg.connect(dsn, password=password) as conn:
            with conn.cursor() as cur:
                if base_item_id is None:
                    cur.execute("select collection_base_item(%s);", (collection,))
                else:
                    cur.execute(
                        "select collection_base_item(%s, %s);",
                        (collection, base_item_id),
                    )
                base_item = cur.fetchone()
                if base_item is None or base_item[0] is None:
                    raise ValueError(f"Base item for {collection} not found")
        self._base_items[key] = base_item[0]
        return base_item[0]


def pgstac_dsn(conninfo: str | None, statement_timeout: int | None) -> str:
    """
    Get the DSN from the connection info.
    """
    if conninfo is None:
        conninfo = ""
    connd = psycopg.conninfo.conninfo_to_dict(conninfo)
    options: str = str(connd.get("options", ""))
    options += " -c search_path=pgstac,public"
    if statement_timeout is not None:
        options += f" -c statement_timeout={statement_timeout}"
    connd["options"] = options
    return psycopg.conninfo.make_conninfo("", **connd)


@contextmanager
def _connect(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    statement_timeout: int | None,
) -> Iterator[psycopg.Connection]:
    """
    Yield a pgstac connection

    This supports:
    - a connection string
    - a callable connection factory
    - an environment variable when set to `None`

    If `conninfo` is a callable, users are responsible for setting `search_path`
    (pgstac,public) and `statement_timeout` on the connection themselves.
    """
    if callable(conninfo):
        with conninfo() as conn:
            yield conn
        return
    with psycopg.connect(pgstac_dsn(conninfo, statement_timeout)) as conn:
        yield conn


def _items_query(
    conn: psycopg.Connection,
    *,
    collection: str | None,
    start_datetime: datetime | None,
    end_datetime: datetime | None,
    search: dict[str, Any] | None,
    columns: tuple[str, ...] = _ITEM_COLUMNS,
) -> tuple[sql.Composed, Any]:
    """
    Get the query and arguments that select `columns` of the requested pgstac
    items.
    """
    if search is not None and (
        collection is not None or start_datetime is not None or end_datetime is not None
    ):
        raise ValueError("Cannot use search and collection/datetime at the same time")
    if start_datetime is not None and end_datetime is None:
        end_datetime = datetime.now(timezone.utc)

    select = sql.SQL("SELECT {} FROM items").format(
        sql.SQL(", ").join(map(sql.Identifier, columns))
    )
    query: sql.Composed
    args: Any

    if search is not None:
        logger.info(f"Using CQL2 Filter {search}")
        unsupported = sorted({"fields", "token"} & search.keys())
        if unsupported:
            raise ValueError(f"search does not support {', '.join(unsupported)}")
        # pgstac's search() returns one page of results as a FeatureCollection, so
        # use the WHERE and ORDER BY clauses it would build to select from items
        # Sort by datetime, collection and id after sortby, so a limit always picks
        # the same items
        sortby = list(search.get("sortby") or [])
        fields = {s["field"].removeprefix("properties.") for s in sortby}
        sortby += [
            {"field": field, "direction": "asc"}
            for field in ("datetime", "collection", "id")
            if field not in fields
        ]
        # Not conn.cursor(): the caller's connection can have another row_factory
        with psycopg.Cursor(conn, row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT stac_search_to_where(%(s)s), sort_sqlorderby(%(s)s);",
                {"s": Jsonb({**search, "sortby": sortby})},
            )
            row = cur.fetchone()
            assert row is not None
        # pgstac builds these clauses from the search, quoting its values
        where, order_by = row
        query = select + sql.SQL(" WHERE ") + sql.SQL(where)
        query += sql.SQL(" ORDER BY ") + sql.SQL(order_by)
        if "limit" in search:
            query += sql.SQL(" LIMIT {}").format(sql.Literal(int(search["limit"])))
        # The WHERE clause can contain %, so don't treat the query as having parameters
        args = None
    elif (
        collection is not None
        and start_datetime is not None
        and end_datetime is not None
    ):
        logger.info(
            f"Using Collection {collection}, Start {start_datetime}, End {end_datetime}"
        )
        query = select + sql.SQL(
            " WHERE collection = %s AND datetime >= %s AND datetime < %s;"
        )
        args = (collection, start_datetime, end_datetime)
    elif collection is not None:
        logger.info(f"Using Collection {collection}")
        query = select + sql.SQL(" WHERE collection = %s;")
        args = (collection,)
    else:
        logger.info("With no filter, fetching all items")
        query = select + sql.SQL(";")
        args = ()
    return query, args


def _rows(
    conn: psycopg.Connection,
    query: sql.Composed,
    args: Any,
    *,
    stream: bool | None,
    cursor_itersize: int | None,
    row_factory: Any,
) -> Generator[Any, None, None]:
    if stream is None:
        # Before libpq 17, rows stream one at a time, which is slower than fetching
        stream = env_flag(
            "STAC_GEOPARQUET_PGSTAC_STREAM", psycopg.capabilities.has_stream_chunked()
        )
    if cursor_itersize is None:
        default = STREAM_ITERSIZE if stream else FETCH_ITERSIZE
        cursor_itersize = int(
            os.environ.get("STAC_GEOPARQUET_PGSTAC_CURSOR_ITERSIZE") or default
        )
    logger.debug(f"Query: {query.as_string(conn)}, Args: {args}")
    if stream:
        size = cursor_itersize if psycopg.capabilities.has_stream_chunked() else 1
        # Not the connection's cursor_factory: a ClientCursor can't return binary rows
        with psycopg.Cursor(conn, row_factory=row_factory) as cur:
            yield from cur.stream(query, args, binary=True, size=size)
    else:
        curname = "".join(random.choices(string.ascii_lowercase, k=32))
        with conn.cursor(curname, row_factory=row_factory) as cur:
            cur.itersize = cursor_itersize
            cur.execute(query, args, binary=True)
            yield from cur


def pgstac_to_iter(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int | None = None,
    row_func: Callable | None = None,
    geometry: GEOMETRY_OPTIONS = "geojson",
    stream: bool | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Iterate over pgstac items as STAC item dicts.

    Their geometries are GeoJSON, or ISO WKB bytes with `geometry="wkb"`, and
    `row_func` gets them the same way. See
    [`pgstac_to_arrow`][stac_geoparquet.pgstac_reader.pgstac_to_arrow] for
    `search`, `statement_timeout`, `cursor_itersize`, `row_func` and `stream`.
    """
    logger.info("Fetching Data from PGStac Into an Iterator of Items")

    with _connect(conninfo, statement_timeout) as conn:
        query, args = _items_query(
            conn,
            collection=collection,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            search=search,
        )
        row_factory = functools.partial(
            PgstacRowFactory, wkb_geometry=geometry == "wkb"
        )
        for rec in _rows(
            conn,
            query,
            args,
            stream=stream,
            cursor_itersize=cursor_itersize,
            row_factory=row_factory,
        ):
            if row_func is not None:
                rec = row_func(rec)
            yield rec


def _structure(value: Any) -> Any:
    """
    A hashable summary of the keys and value types in `value`. Lists of dicts
    keep their length, and pgstac's do-not-merge marker is kept: hydration
    depends on both.
    """
    # type() and list comprehensions: this runs on every item
    value_type = type(value)
    if value_type is dict:
        return tuple([(key, _structure(v)) for key, v in value.items()])
    if value_type is list:
        if value and type(value[0]) is dict:
            return tuple([_structure(v) for v in value])
        return frozenset([_structure(v) for v in value])
    if value_type is str and value == DO_NOT_MERGE_MARKER:
        return DO_NOT_MERGE_MARKER
    return value_type


def pgstac_schema(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int | None = None,
    row_func: Callable | None = None,
    geometry: GEOMETRY_OPTIONS = "wkb",
    stream: bool | None = None,
) -> InferredSchema:
    """
    Infer the schema of pgstac items, after `row_func` if one is given.

    The schema only depends on the keys and types of values in the items, so this
    reads each item's stored content and converts just one item of each
    structure. A `row_func` is called with that one item of each structure, so if
    the fields it adds depend on an item's values rather than its structure, use
    `schema="ChunksToDisk"` or pass a schema instead. With `geometry="wkb"`, the
    geometries aren't read, and the items a `row_func` gets have an empty one; if
    what it adds depends on the geometry, pass `geometry="geojson"`. The result's
    `count` is the number of items converted.

    Pass the result as `schema` to
    [`pgstac_to_parquet`][stac_geoparquet.pgstac_reader.pgstac_to_parquet] or
    [`sync_pgstac_to_parquet`][stac_geoparquet.pgstac_reader.sync_pgstac_to_parquet],
    for example to give several files the same schema. It describes the items as
    they were when it ran: a field that an item gains later is dropped. See
    [`pgstac_to_arrow`][stac_geoparquet.pgstac_reader.pgstac_to_arrow] for the
    other arguments.
    """
    # Only a row_func that gets GeoJSON needs the real geometries
    read_geometry = row_func is not None and geometry == "geojson"
    columns: tuple[str, ...] = ("collection", "id", "content")
    if read_geometry:
        columns += ("geometry",)
    with _connect(conninfo, statement_timeout) as conn:
        query, args = _items_query(
            conn,
            collection=collection,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            search=search,
            columns=columns,
        )
        rows = _rows(
            conn,
            query,
            args,
            stream=stream,
            cursor_itersize=cursor_itersize,
            row_factory=tuple_row,
        )
        one_per_structure: dict[Any, tuple[Any, ...]] = {}
        # Closed on an error too: a paused stream holds the connection's lock
        with closing(rows):
            for row in rows:
                collection_id, _, content = row[:3]
                bbox_size = len(content.get("bbox", ()))
                base_item_id = content.get("pgstac:base_item")
                key = (collection_id, base_item_id, bbox_size, _structure(content))
                one_per_structure.setdefault(key, row)
        with psycopg.Cursor(conn) as cur:
            factory = PgstacRowFactory(cur, wkb_geometry=not read_geometry)
            items = [
                factory(
                    (item_id, wkb[0] if wkb else _EMPTY_WKB, collection_id, content)
                )
                for collection_id, item_id, content, *wkb in one_per_structure.values()
            ]
    if row_func is not None:
        items = [row_func(item) for item in items]
    schema = InferredSchema()
    schema.update_from_items(items)
    if not schema.count:
        raise ValueError("No items found to infer a schema from")
    return schema


def _pgstac_items(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    collection: str | None,
    start_datetime: datetime | None,
    end_datetime: datetime | None,
    search: dict[str, Any] | None,
    *,
    statement_timeout: int | None,
    cursor_itersize: int | None,
    row_func: Callable | None,
    geometry: GEOMETRY_OPTIONS,
    stream: bool | None,
    schema: PGSTAC_SCHEMA_OPTIONS,
) -> tuple[ACCEPTED_SCHEMA_OPTIONS, Iterator[dict[str, Any]]]:
    """
    Get the schema and items for pgstac_to_arrow and pgstac_to_parquet.
    """
    if schema == "PreScan":
        schema = pgstac_schema(
            conninfo,
            collection,
            start_datetime,
            end_datetime,
            search,
            statement_timeout=statement_timeout,
            cursor_itersize=cursor_itersize,
            row_func=row_func,
            geometry=geometry,
            stream=stream,
        )
    items = pgstac_to_iter(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        statement_timeout=statement_timeout,
        cursor_itersize=cursor_itersize,
        row_func=row_func,
        # Without a row_func nothing sees the items before Arrow, so skip GeoJSON
        geometry="wkb" if row_func is None else geometry,
        stream=stream,
    )
    return schema, items


def pgstac_to_arrow(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int | None = None,
    schema: PGSTAC_SCHEMA_OPTIONS = "FirstBatch",
    statement_timeout: int | None = None,
    row_func: Callable | None = None,
    tmpdir: str | Path | None = None,
    drop_invalid_properties: bool = True,
    geometry: GEOMETRY_OPTIONS = "wkb",
    cursor_itersize: int | None = None,
    stream: bool | None = None,
) -> pa.RecordBatchReader:
    """
    Convert pgstac items to an arrow record batch reader.

    `search` selects items with a STAC API search body. Its `limit` is the most
    items to export. They're sorted by its `sortby`, then by datetime, collection
    and id; `fields` and `token` aren't supported.

    If `row_func` is given, it is called with each item dict, and what it returns
    is converted in place of the item. Its items' geometries are ISO WKB bytes,
    or GeoJSON with `geometry="geojson"`, which is slower.

    `schema` can also be "PreScan", which reads the items twice: once to infer
    the schema from all of them with
    [`pgstac_schema`][stac_geoparquet.pgstac_reader.pgstac_schema], which says how
    it calls `row_func`, and once to convert them.

    `cursor_itersize` is the number of rows read from the database at a time. It
    defaults to the `STAC_GEOPARQUET_PGSTAC_CURSOR_ITERSIZE` environment variable,
    or to 100 rows when streaming and 1000 when fetching from a server-side
    cursor, where each fetch waits a round trip to the database.

    If `stream` is True, the rows are streamed from a single query, so
    `statement_timeout` limits the whole export, including converting and writing
    the items. If it's False, they're fetched from a server-side cursor, and it
    limits each fetch. It defaults to the `STAC_GEOPARQUET_PGSTAC_STREAM`
    environment variable, or to streaming with libpq 17 or later, which streams
    rows in batches; before that, streamed rows arrive one at a time.
    `statement_timeout` is in milliseconds.
    """
    schema, items = _pgstac_items(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        statement_timeout=statement_timeout,
        cursor_itersize=cursor_itersize,
        row_func=row_func,
        geometry=geometry,
        stream=stream,
        schema=schema,
    )
    return parse_stac_items_to_arrow(
        items,
        chunk_size=chunk_size,
        schema=schema,
        tmpdir=tmpdir,
        drop_invalid_properties=drop_invalid_properties,
    )


def pgstac_to_parquet(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    output_path: str | Path,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int | None = None,
    schema: PGSTAC_SCHEMA_OPTIONS = "FirstBatch",
    statement_timeout: int | None = None,
    row_func: Callable | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
    geometry: GEOMETRY_OPTIONS = "wkb",
    cursor_itersize: int | None = None,
    stream: bool | None = None,
    **kwargs: Any,
) -> str:
    """
    Convert pgstac items to a parquet file.

    See [`pgstac_to_arrow`][stac_geoparquet.pgstac_reader.pgstac_to_arrow] for
    `search`, `statement_timeout`, `row_func`, `geometry`, `schema="PreScan"`,
    `cursor_itersize` and `stream`. Other keyword arguments are passed on to
    [`parse_stac_items_to_parquet`][stac_geoparquet.arrow.parse_stac_items_to_parquet].
    """
    filesystem, filepath = resolve_output_path_and_filesystem(output_path, filesystem)

    filedir = Path(filepath).parent
    filesystem.create_dir(str(filedir), recursive=True)

    logger.debug(f"Exporting PgSTAC items to {filepath}")

    schema, items = _pgstac_items(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        statement_timeout=statement_timeout,
        cursor_itersize=cursor_itersize,
        row_func=row_func,
        geometry=geometry,
        stream=stream,
        schema=schema,
    )

    return parse_stac_items_to_parquet(
        items,
        chunk_size=chunk_size,
        schema=schema,
        output_path=filepath,
        schema_version=schema_version,
        filesystem=filesystem,
        **kwargs,
    )


@dataclass
class Partition:
    collection: str
    partition: str
    start: datetime | None
    end: datetime | None
    last_updated: datetime


def get_pgstac_partitions(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    updated_after: datetime | None = None,
) -> Iterator[Partition]:
    with _connect(conninfo, None) as conn:
        with conn.cursor(row_factory=psycopg.rows.class_row(Partition)) as cur:
            # note: '.000001 seconds' is the minimum resolution for timestamptz in Postgres, so this
            # is added since the inclusive upper bound in the dtrange can be used with exclusive <
            # in the pgstac_to_parquet function
            q = """
                SELECT
                    collection,
                    CASE WHEN lower(partition_dtrange) = '-infinity' OR upper(partition_dtrange) = 'infinity' THEN
                        'items.parquet'
                    ELSE
                        format(
                            'items_%%s_%%s.parquet',
                            to_char(lower(partition_dtrange),'YYYYMMDD'),
                            to_char(upper(partition_dtrange),'YYYYMMDD')
                        )
                    END AS partition,
                    lower(dtrange) as start,
                    upper(dtrange) + '.000001 seconds' as end,
                    last_updated
                FROM partitions_view
                """
            args: Any = ()
            if updated_after is not None:
                q += " WHERE last_updated >= %s"
                args = (updated_after,)
            q += " ORDER BY last_updated asc"
            cur.execute(q, args)
            for row in cur:
                logger.debug(f"Found PgSTAC Partition: {row}")
                yield row


def sync_pgstac_to_parquet(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    output_path: str | Path,
    updated_after: datetime | None = None,
    chunk_size: int | None = None,
    schema: PGSTAC_SCHEMA_OPTIONS = "FirstBatch",
    statement_timeout: int | None = None,
    row_func: Callable | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
    geometry: GEOMETRY_OPTIONS = "wkb",
    **kwargs: Any,
) -> str:
    """
    Use the last_updated partition metadata in pgstac to sync only changed
    items to parquet.
    """

    filesystem, filepath = resolve_output_path_and_filesystem(output_path, filesystem)

    filedir = Path(filepath)
    filesystem.create_dir(str(filedir), recursive=True)

    logger.info(
        f"Syncing PgSTAC partitions that have been updated since {updated_after} to {output_path} on filesystem {filesystem}."
    )
    for p in get_pgstac_partitions(conninfo, updated_after):
        of = filedir / p.collection / p.partition
        logger.debug(of)

        pgstac_to_parquet(
            conninfo,
            output_path=of,
            collection=p.collection,
            start_datetime=p.start,
            end_datetime=p.end,
            row_func=row_func,
            chunk_size=chunk_size,
            schema=schema,
            statement_timeout=statement_timeout,
            schema_version=schema_version,
            filesystem=filesystem,
            geometry=geometry,
            **kwargs,
        )
    return str(of)
