import logging
import random
import string
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import psycopg
import pyarrow as pa
import pyarrow.fs
import pypgstac.hydration
import shapely.wkb
from psycopg import sql
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb, set_json_dumps, set_json_loads

from stac_geoparquet.arrow import (
    ACCEPTED_SCHEMA_OPTIONS,
    DEFAULT_JSON_CHUNK_SIZE,
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
    parse_stac_items_to_arrow,
)
from stac_geoparquet.arrow._api import parse_stac_items_to_parquet
from stac_geoparquet.arrow._util import resolve_output_path_and_filesystem

logger = logging.getLogger(__name__)


def dumps(data: dict) -> str:
    """
    Custom JSON dumps function for psycopg.
    """
    return orjson.dumps(data).decode()


set_json_dumps(dumps)
set_json_loads(orjson.loads)

logger = logging.getLogger(__name__)


class PgstacRowFactory:
    """
    Custom row factory for psycopg to return a tuple of the columns.
    """

    def __init__(self, cursor: psycopg.Cursor[Any]) -> None:
        self.cursor = cursor
        self._base_items: dict[tuple[str, int | None], dict[str, Any]] = {}

    def __call__(
        self,
        values: tuple[str, str, str, datetime, datetime, dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Convert the values to a dictionary.
        """
        columns = [
            "id",
            "geometry",
            "collection",
            "datetime",
            "end_datetime",
            "content",
        ]
        item: dict[str, Any] = dict(zip(columns, values))
        # datetime is in the content too
        item.pop("datetime")
        item.pop("end_datetime")

        geom = shapely.wkb.loads(item["geometry"], hex=True)

        item["geometry"] = geom.__geo_interface__
        content = item.pop("content")
        assert isinstance(content, dict)
        if "bbox" in content:
            item["bbox"] = content["bbox"]
        else:
            item["bbox"] = list(geom.bounds)

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
) -> tuple[sql.Composed, Any]:
    """
    Get the query and arguments that select the requested pgstac items.
    """
    if search is not None and (
        collection is not None or start_datetime is not None or end_datetime is not None
    ):
        raise ValueError("Cannot use search and collection/datetime at the same time")
    if start_datetime is not None and end_datetime is None:
        end_datetime = datetime.now(timezone.utc)

    select = sql.SQL("SELECT * FROM items")
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
    cursor_itersize: int,
    row_factory: Any,
) -> Iterator[Any]:
    curname = "".join(random.choices(string.ascii_lowercase, k=32))
    with conn.cursor(curname, row_factory=row_factory) as cur:
        cur.itersize = cursor_itersize
        cur.execute(query, args)
        logger.debug(f"Query: {query.as_string(conn)}, Args: {args}")
        yield from cur


def pgstac_to_iter(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
    row_func: Callable | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Iterate over pgstac items as STAC item dicts.

    See [`pgstac_to_arrow`][stac_geoparquet.pgstac_reader.pgstac_to_arrow] for
    `search`.
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
        for rec in _rows(
            conn,
            query,
            args,
            cursor_itersize=cursor_itersize,
            row_factory=PgstacRowFactory,
        ):
            if row_func is not None:
                rec = row_func(rec)
            yield rec


def pgstac_to_arrow(
    conninfo: str | Callable[[], psycopg.Connection] | None,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: ACCEPTED_SCHEMA_OPTIONS = "FirstBatch",
    statement_timeout: int | None = None,
    row_func: Callable | None = None,
    tmpdir: str | Path | None = None,
    drop_invalid_properties: bool = True,
) -> pa.RecordBatchReader:
    """
    Convert pgstac items to an arrow record batch reader.

    `search` selects items with a STAC API search body. Its `limit` is the most
    items to export. They're sorted by its `sortby`, then by datetime, collection
    and id; `fields` and `token` aren't supported.
    """
    items = pgstac_to_iter(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        statement_timeout=statement_timeout,
        cursor_itersize=chunk_size,
        row_func=row_func,
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
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: ACCEPTED_SCHEMA_OPTIONS = "FirstBatch",
    statement_timeout: int | None = None,
    row_func: Callable | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
    **kwargs: Any,
) -> str:
    """
    Convert pgstac items to a parquet file.

    See [`pgstac_to_arrow`][stac_geoparquet.pgstac_reader.pgstac_to_arrow] for
    `search`.
    """
    filesystem, filepath = resolve_output_path_and_filesystem(output_path, filesystem)

    filedir = Path(filepath).parent
    filesystem.create_dir(str(filedir), recursive=True)

    logger.debug(f"Exporting PgSTAC items to {filepath}")

    items = pgstac_to_iter(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        statement_timeout=statement_timeout,
        cursor_itersize=chunk_size,
        row_func=row_func,
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
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: ACCEPTED_SCHEMA_OPTIONS = "FirstBatch",
    statement_timeout: int | None = None,
    row_func: Callable | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
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
            **kwargs,
        )
    return str(of)
