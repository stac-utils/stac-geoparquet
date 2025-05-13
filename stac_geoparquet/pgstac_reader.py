import functools
import logging
from typing import Any, Callable, Iterator, Tuple
from pathlib import Path

import orjson
import psycopg
import pyarrow as pa
import pyarrow.fs
import pypgstac.hydration
import shapely.wkb
from psycopg.types.json import set_json_dumps, set_json_loads
from dataclasses import dataclass
from datetime import datetime, timezone


from stac_geoparquet.arrow import (
    DEFAULT_JSON_CHUNK_SIZE,
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
    parse_stac_items_to_arrow,
    to_parquet,
)
from stac_geoparquet.arrow._schema.models import InferredSchema
import random
import string


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

    def __call__(
        self,
        values: tuple[
            str, str, str, datetime, datetime, dict[str, Any]
        ],
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
        if "stac_extensions" in content:
            item["stac_extensions"] = content["stac_extensions"]
        item["properties"] = content["properties"]

        base_item = self.get_baseitem(item["collection"])
        pypgstac.hydration.hydrate(base_item, item)
        return item

    @functools.lru_cache(maxsize=256)
    def get_baseitem(self, collection: str) -> dict[str, Any]:
        """
        Get the base item for the collection.
        """
        conninfo = self.cursor.connection.info
        dsn = conninfo.dsn
        password = conninfo.password
        with psycopg.connect(dsn, password=password) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select collection_base_item(%s);",
                    (collection,),
                )
                base_item = cur.fetchone()
                if base_item is None:
                    raise ValueError(f"Collection {collection} not found")
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
    conninfo = psycopg.conninfo.make_conninfo("", **connd)
    return conninfo


def pgstac_to_iter(
    conninfo: str | None,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
    row_func: Callable | None = None,
) -> Iterator[dict[str, Any]]:
    conninfo = pgstac_dsn(conninfo, statement_timeout)

    if search is not None and (
        collection is not None or start_datetime is not None or end_datetime is not None
    ):
        raise ValueError("Cannot use search and collection/datetime at the same time")
    if start_datetime is not None and end_datetime is None:
        end_datetime = datetime.now(timezone.utc)

    query: str
    args: Any

    if search is not None:
        query = "SELECT * FROM search(%s);"
        args = (search,)
    elif (
        collection is not None
        and start_datetime is not None
        and end_datetime is not None
    ):
        query = "SELECT * FROM items WHERE collection = %s AND datetime >= %s AND datetime < %s;"
        args = (collection, start_datetime, end_datetime)
    elif collection is not None:
        query = "SELECT * FROM items WHERE collection = %s;"
        args = (collection,)
    else:
        query = "SELECT * FROM items;"
        args = ()
    curname = "".join(random.choices(string.ascii_lowercase, k=32))
    with psycopg.connect(conninfo) as conn:
        with conn.cursor(curname, row_factory=PgstacRowFactory) as cur:
            cur.itersize = cursor_itersize
            cur.execute(query, args)
            for rec in cur:
                if row_func is not None:
                    rec = row_func(rec)
                yield rec


def pgstac_to_arrow(
    conninfo: str,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
    row_func: Callable | None = None,
) -> pa.RecordBatchReader:
    """
    Convert pgstac items to an arrow record batch reader.
    """
    items = pgstac_to_iter(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        statement_timeout=statement_timeout,
        cursor_itersize=cursor_itersize,
        row_func=row_func,
    )
    return parse_stac_items_to_arrow(items, chunk_size=chunk_size, schema=schema)


def pgstac_to_parquet(
    conninfo: str,
    output_path: str | Path,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
    row_func: Callable | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
    **kwargs: Any,
) -> Path:
    """
    Convert pgstac items to a parquet file.
    """
    if isinstance(output_path, str):
        output_path = Path(output_path)

    record_batch_reader = pgstac_to_arrow(
        conninfo,
        collection,
        start_datetime,
        end_datetime,
        search,
        chunk_size,
        schema,
        statement_timeout=statement_timeout,
        cursor_itersize=cursor_itersize,
        row_func=row_func,
    )

    to_parquet(
        record_batch_reader,
        output_path=output_path,
        filesystem=filesystem,
        schema_version=schema_version,
        **kwargs,
    )
    return output_path


@dataclass
class Partition:
    collection: str
    partition: str
    start: datetime
    end: datetime
    last_updated: datetime

def get_pgstac_partitions(conninfo: str, updated_after: datetime | None = None) -> Iterator[Partition]:
    db = pgstac_dsn(conninfo, None)
    with psycopg.connect(db) as conn:
        with conn.cursor(row_factory=psycopg.rows.class_row(Partition)) as cur:
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
                    upper(dtrange) as end,
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
                yield row

def sync_pgstac_to_parquet(
    conninfo: str,
    output_path: str | Path,
    updated_after: datetime | None = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
    row_func: Callable | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
    **kwargs: Any,
) -> Path:
    """
    Use the last_updated partition metadata in pgstac to sync only changed
    items to parquet.
    """
    output_dir = Path(output_path)
    for p in get_pgstac_partitions(conninfo, updated_after):
        od = output_dir / p.collection
        od.mkdir(parents=True, exist_ok=True)
        of = od / p.partition

        pgstac_to_parquet(
            conninfo,
            output_path = of,
            collection = p.collection,
            start_datetime = p.start,
            end_datetime = p.end,
            row_func=row_func,
            chunk_size=chunk_size,
            schema=schema,
            statement_timeout=statement_timeout,
            cursor_itersize=cursor_itersize,
            schema_version=schema_version,
            filesystem=filesystem,
            **kwargs,
        )
    return output_dir
