from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from stac_geoparquet.arrow._api import parse_stac_ndjson_to_arrow
from stac_geoparquet.arrow._crs import WGS84_CRS_JSON
from stac_geoparquet.arrow._schema.models import InferredSchema


def parse_stac_ndjson_to_parquet(
    input_path: str | Path | Iterable[str | Path],
    output_path: str | Path,
    *,
    chunk_size: int = 65536,
    schema: pa.Schema | InferredSchema | None = None,
    limit: int | None = None,
    schema_version: Literal["1.0.0", "1.1.0"] = "1.0.0",
    **kwargs: Any,
) -> None:
    """Convert one or more newline-delimited JSON STAC files to GeoParquet

    Args:
        input_path: One or more paths to files with STAC items.
        output_path: A path to the output Parquet file.

    Other args:
        chunk_size: The chunk size. Defaults to 65536.
        schema: The schema to represent the input STAC data. Defaults to None, in which
            case the schema will first be inferred via a full pass over the input data.
            In this case, there will be two full passes over the input data: one to
            infer a common schema across all data and another to read the data and
            iteratively convert to GeoParquet.
        limit: The maximum number of JSON records to convert.
        schema_version: GeoParquet specification version; if not provided will default
            to latest supported version.
    """

    batches_iter = parse_stac_ndjson_to_arrow(
        input_path, chunk_size=chunk_size, schema=schema, limit=limit
    )
    first_batch = next(batches_iter)
    schema = first_batch.schema.with_metadata(
        create_geoparquet_metadata(
            pa.Table.from_batches([first_batch]), schema_version=schema_version
        )
    )
    with pq.ParquetWriter(output_path, schema, **kwargs) as writer:
        writer.write_batch(first_batch)
        for batch in batches_iter:
            writer.write_batch(batch)


def to_parquet(
    table: pa.Table,
    where: Any,
    *,
    schema_version: Literal["1.0.0", "1.1.0"] = "1.0.0",
    **kwargs: Any,
) -> None:
    """Write an Arrow table with STAC data to GeoParquet

    This writes metadata compliant with GeoParquet 1.1.

    Args:
        table: The table to write to Parquet
        where: The destination for saving.

    Other args:
        schema_version: GeoParquet specification version; if not provided will default
            to latest supported version.
    """
    metadata = table.schema.metadata or {}
    metadata.update(create_geoparquet_metadata(table, schema_version=schema_version))
    table = table.replace_schema_metadata(metadata)

    pq.write_table(table, where, **kwargs)


def create_geoparquet_metadata(
    table: pa.Table,
    *,
    schema_version: Literal["1.0.0", "1.1.0"],
) -> dict[bytes, bytes]:
    # TODO: include bbox of geometries
    column_meta = {
        "encoding": "WKB",
        # TODO: specify known geometry types
        "geometry_types": [],
        "crs": WGS84_CRS_JSON,
        "edges": "planar",
    }

    if int(schema_version.split(".")[1]) >= 1:
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

    if "proj:geometry" in table.schema.names:
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
