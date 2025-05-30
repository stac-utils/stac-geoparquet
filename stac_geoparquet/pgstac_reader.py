import functools
import logging
import random
import string
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Union

import orjson
import psycopg
import pyarrow as pa
import pyarrow.fs
import pypgstac.hydration
import shapely.wkb
from psycopg.types.json import set_json_dumps, set_json_loads

from stac_geoparquet.arrow import (
    DEFAULT_JSON_CHUNK_SIZE,
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
    parse_stac_items_to_arrow,
    to_parquet,
)
from stac_geoparquet.arrow._schema.models import InferredSchema

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
        if "stac_extensions" in content:
            item["stac_extensions"] = content["stac_extensions"]
        item["properties"] = content["properties"]

        base_item = self.get_baseitem(item["collection"])
        pypgstac.hydration.hydrate(base_item, item)
        logger.debug(item)
        return item

    @functools.lru_cache(maxsize=256)
    def get_baseitem(self, collection: str) -> dict[str, Any]:
        """
        Get the base item for the collection.
        """
        logger.info(f"Getting Base Item for {collection}")
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


def pgstac_dsn(conninfo: Union[str, None], statement_timeout: Union[int, None]) -> str:
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
    conninfo: Union[str, None],
    collection: Union[str, None] = None,
    start_datetime: Union[datetime, None] = None,
    end_datetime: Union[datetime, None] = None,
    search: Union[dict[str, Any], None] = None,
    statement_timeout: Union[int, None] = None,
    cursor_itersize: int = 10000,
    row_func: Union[Callable, None] = None,
) -> Iterator[dict[str, Any]]:
    logger.info("Fetching Data from PGStac Into an Iterator of Items")
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
        logger.info(f"Using CQL2 Filter {search}")
        query = "SELECT * FROM search(%s);"
        args = (search,)
    elif (
        collection is not None
        and start_datetime is not None
        and end_datetime is not None
    ):
        logger.info(
            f"Using Collection {collection}, Start {start_datetime}, End {end_datetime}"
        )
        query = "SELECT * FROM items WHERE collection = %s AND datetime >= %s AND datetime < %s;"
        args = (collection, start_datetime, end_datetime)
    elif collection is not None:
        logger.info(f"Using Collection {collection}")
        query = "SELECT * FROM items WHERE collection = %s;"
        args = (collection,)
    else:
        logger.info("With no filter, fetching all items")
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
    collection: Union[str, None] = None,
    start_datetime: Union[datetime, None] = None,
    end_datetime: Union[datetime, None] = None,
    search: Union[dict[str, Any], None] = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: Union[pa.Schema, InferredSchema, None] = None,
    statement_timeout: Union[int, None] = None,
    cursor_itersize: int = 10000,
    row_func: Union[Callable, None] = None,
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
    output_path: Union[str, Path],
    collection: Union[str, None] = None,
    start_datetime: Union[datetime, None] = None,
    end_datetime: Union[datetime, None] = None,
    search: Union[dict[str, Any], None] = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: Union[pa.Schema, InferredSchema, None] = None,
    statement_timeout: Union[int, None] = None,
    cursor_itersize: int = 10000,
    row_func: Union[Callable, None] = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: Union[pyarrow.fs.FileSystem, None] = None,
    **kwargs: Any,
) -> str:
    """
    Convert pgstac items to a parquet file.
    """
    if filesystem is None:
        filesystem, filepath = pyarrow.fs.FileSystem.from_uri(output_path)
    else:
        filepath = output_path

    filedir = Path(filepath).parent
    filesystem.create_dir(str(filedir), recursive=True)

    logger.info(f"Exporting PgSTAC to {filesystem} {filepath}")

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
        output_path=filepath,
        filesystem=filesystem,
        schema_version=schema_version,
        **kwargs,
    )
    return str(filepath)


@dataclass
class Partition:
    collection: str
    partition: str
    start: datetime
    end: datetime
    last_updated: datetime


def get_pgstac_partitions(
    conninfo: str, updated_after: Union[datetime, None] = None
) -> Iterator[Partition]:
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
                logger.info(f"Found PgSTAC Partition: {row}")
                yield row


def sync_pgstac_to_parquet(
    conninfo: str,
    output_path: Union[str, Path],
    updated_after: Union[datetime, None] = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: Union[pa.Schema, InferredSchema, None] = None,
    statement_timeout: Union[int, None] = None,
    cursor_itersize: int = 10000,
    row_func: Union[Callable, None] = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: Union[pyarrow.fs.FileSystem, None] = None,
    **kwargs: Any,
) -> str:
    """
    Use the last_updated partition metadata in pgstac to sync only changed
    items to parquet.
    """

    if filesystem is None:
        filesystem, filepath = pyarrow.fs.FileSystem.from_uri(output_path)
    else:
        filepath = output_path

    filedir = Path(filepath)
    filesystem.create_dir(str(filedir), recursive=True)

    logger.info(
        f"Syncing PgSTAC partitions that have been updated since {updated_after} to {output_path} on filesystem {filesystem}."
    )
    for p in get_pgstac_partitions(conninfo, updated_after):
        of = filedir / p.collection / p.partition
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
            cursor_itersize=cursor_itersize,
            schema_version=schema_version,
            filesystem=filesystem,
            **kwargs,
        )
    return str(of)
