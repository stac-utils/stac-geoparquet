import datetime
import functools
import logging
from typing import Any, Iterator

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
            str, str, str, datetime.datetime, datetime.datetime, dict[str, Any]
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

        # Ensure that properties known to often come in mixed string/numeric
        # types are consistent across all items.
        if "naip:year" in item["properties"]:
            item["properties"]["naip:year"] = int(item["properties"]["naip:year"])
        if "proj:epsg" in item["properties"]:
            item["properties"]["proj:epsg"] = int(item["properties"]["proj:epsg"])
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
    start_datetime: datetime.datetime | None = None,
    end_datetime: datetime.datetime | None = None,
    search: dict[str, Any] | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
) -> Iterator[dict[str, Any]]:
    conninfo = pgstac_dsn(conninfo, statement_timeout)

    if search is not None and (
        collection is not None or start_datetime is not None or end_datetime is not None
    ):
        raise ValueError("Cannot use search and collection/datetime at the same time")
    if start_datetime is not None and end_datetime is None:
        end_datetime = datetime.datetime.now(datetime.timezone.utc)

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
                yield rec


def pgstac_to_arrow(
    conninfo: str,
    collection: str | None = None,
    start_datetime: datetime.datetime | None = None,
    end_datetime: datetime.datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
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
    )
    return parse_stac_items_to_arrow(items, chunk_size=chunk_size, schema=schema)


def pgstac_to_parquet(
    conninfo: str,
    output_path: str,
    collection: str | None = None,
    start_datetime: datetime.datetime | None = None,
    end_datetime: datetime.datetime | None = None,
    search: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    statement_timeout: int | None = None,
    cursor_itersize: int = 10000,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pyarrow.fs.FileSystem | None = None,
    **kwargs: Any,
) -> str:
    """
    Convert pgstac items to a parquet file.
    """
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
    )

    to_parquet(
        record_batch_reader,
        output_path=output_path,
        filesystem=filesystem,
        schema_version=schema_version,
        **kwargs,
    )
    return output_path
