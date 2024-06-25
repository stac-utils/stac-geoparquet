from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

import pyarrow as pa
from deltalake import write_deltalake

from stac_geoparquet.arrow._api import parse_stac_ndjson_to_arrow
from stac_geoparquet.arrow._constants import DEFAULT_JSON_CHUNK_SIZE
from stac_geoparquet.arrow._to_parquet import (
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
    create_geoparquet_metadata,
)

if TYPE_CHECKING:
    from deltalake import DeltaTable


def parse_stac_ndjson_to_delta_lake(
    input_path: str | Path | Iterable[str | Path],
    table_or_uri: str | Path | DeltaTable,
    *,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | None = None,
    limit: int | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    **kwargs: Any,
) -> None:
    """Convert one or more newline-delimited JSON STAC files to Delta Lake

    Args:
        input_path: One or more paths to files with STAC items.
        table_or_uri: A path to the output Delta Lake table

    Args:
        chunk_size: The chunk size to use for reading JSON into memory. Defaults to
            65536.
        schema: The schema to represent the input STAC data. Defaults to None, in which
            case the schema will first be inferred via a full pass over the input data.
            In this case, there will be two full passes over the input data: one to
            infer a common schema across all data and another to read the data and
            iteratively convert to GeoParquet.
        limit: The maximum number of JSON records to convert.
        schema_version: GeoParquet specification version; if not provided will default
            to latest supported version.
    """
    record_batch_reader = parse_stac_ndjson_to_arrow(
        input_path, chunk_size=chunk_size, schema=schema, limit=limit
    )
    schema = record_batch_reader.schema.with_metadata(
        create_geoparquet_metadata(
            record_batch_reader.schema, schema_version=schema_version
        )
    )
    write_deltalake(
        table_or_uri, record_batch_reader, schema=schema, engine="rust", **kwargs
    )
