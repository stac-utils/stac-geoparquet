from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from stac_geoparquet.arrow._api import parse_stac_ndjson_to_arrow
from stac_geoparquet.arrow._constants import (
    DEFAULT_JSON_CHUNK_SIZE,
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
)
from stac_geoparquet.arrow._crs import WGS84_CRS_JSON
from stac_geoparquet.arrow._schema.models import InferredSchema
from stac_geoparquet.arrow.types import ArrowStreamExportable


def parse_stac_ndjson_to_parquet(
    input_path: str | Path | Iterable[str | Path],
    output_path: str | Path,
    *,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    limit: int | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    **kwargs: Any,
) -> None:
    """Convert one or more newline-delimited JSON STAC files to GeoParquet

    Args:
        input_path: One or more paths to files with STAC items.
        output_path: A path to the output Parquet file.

    Keyword Args:
        chunk_size: The chunk size. Defaults to 65536.
        schema: The schema to represent the input STAC data. Defaults to None, in which
            case the schema will first be inferred via a full pass over the input data.
            In this case, there will be two full passes over the input data: one to
            infer a common schema across all data and another to read the data and
            iteratively convert to GeoParquet.
        limit: The maximum number of JSON records to convert.
        schema_version: GeoParquet specification version; if not provided will default
            to latest supported version.

    All other keyword args are passed on to
    [`pyarrow.parquet.ParquetWriter`][pyarrow.parquet.ParquetWriter].
    """
    record_batch_reader = parse_stac_ndjson_to_arrow(
        input_path, chunk_size=chunk_size, schema=schema, limit=limit
    )
    to_parquet(
        record_batch_reader,
        output_path=output_path,
        schema_version=schema_version,
        **kwargs,
    )


def to_parquet(
    table: pa.Table | pa.RecordBatchReader | ArrowStreamExportable,
    output_path: str | Path,
    *,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    **kwargs: Any,
) -> None:
    """Write an Arrow table with STAC data to GeoParquet

    This writes metadata compliant with either GeoParquet 1.0 or 1.1.

    Args:
        table: STAC in Arrow form. This can be a pyarrow Table, a pyarrow
            RecordBatchReader, or any other Arrow stream object exposed through the
            [Arrow PyCapsule
            Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            A RecordBatchReader or stream object will not be materialized in memory.
        output_path: The destination for saving.

    Keyword Args:
        schema_version: GeoParquet specification version; if not provided will default
            to latest supported version.

    All other keyword args are passed on to
    [`pyarrow.parquet.ParquetWriter`][pyarrow.parquet.ParquetWriter].
    """
    # Coerce to record batch reader to avoid materializing entire stream
    reader = pa.RecordBatchReader.from_stream(table)

    schema = reader.schema.with_metadata(
        create_geoparquet_metadata(reader.schema, schema_version=schema_version)
    )
    with pq.ParquetWriter(output_path, schema, **kwargs) as writer:
        for batch in reader:
            writer.write_batch(batch)


def create_geoparquet_metadata(
    schema: pa.Schema,
    *,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS,
) -> dict[bytes, bytes]:
    # TODO: include bbox of geometries
    column_meta = {
        "encoding": "WKB",
        # TODO: specify known geometry types
        "geometry_types": [],
        "crs": WGS84_CRS_JSON,
        "edges": "planar",
    }

    if schema_version_has_bbox_mapping(schema_version):
        column_meta["covering"] = {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            }
        }

    geo_meta: dict[str, Any] = {
        "version": schema_version,
        "columns": {"geometry": column_meta},
        "primary_column": "geometry",
    }

    if "proj:geometry" in schema.names:
        # Note we don't include proj:bbox as a covering here for a couple different
        # reasons. For one, it's very common for the projected geometries to have a
        # different CRS in each row, so having statistics for proj:bbox wouldn't be
        # useful. Additionally, because of this we leave proj:bbox as a list instead of
        # a struct.
        geo_meta["columns"]["proj:geometry"] = {
            "encoding": "WKB",
            "geometry_types": [],
            # Note that we have to set CRS to `null` to signify that the CRS is unknown.
            # If the CRS key is missing, it gets inferred as WGS84.
            "crs": None,
        }

    return {b"geo": json.dumps(geo_meta).encode("utf-8")}


def schema_version_has_bbox_mapping(
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS,
) -> bool:
    """
    Return true if this GeoParquet schema version supports bounding box covering
    metadata.
    """
    return int(schema_version.split(".")[1]) >= 1
