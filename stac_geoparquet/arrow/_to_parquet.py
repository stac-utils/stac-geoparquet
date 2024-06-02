import json
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from stac_geoparquet.arrow._api import parse_stac_ndjson_to_arrow
from stac_geoparquet.arrow._crs import WGS84_CRS_JSON
from stac_geoparquet.arrow._schema.models import InferredSchema


def parse_stac_ndjson_to_parquet(
    input_path: Union[str, Path, Iterable[Union[str, Path]]],
    output_path: Union[str, Path],
    *,
    chunk_size: int = 65536,
    schema: Optional[Union[pa.Schema, InferredSchema]] = None,
    **kwargs: Any,
) -> None:
    """Convert one or more newline-delimited JSON STAC files to GeoParquet

    Args:
        input_path: One or more paths to files with STAC items.
        output_path: A path to the output Parquet file.
        chunk_size: The chunk size. Defaults to 65536.
        schema: The schema to represent the input STAC data. Defaults to None, in which
            case the schema will first be inferred via a full pass over the input data.
            In this case, there will be two full passes over the input data: one to
            infer a common schema across all data and another to read the data and
            iteratively convert to GeoParquet.
    """

    batches_iter = parse_stac_ndjson_to_arrow(
        input_path, chunk_size=chunk_size, schema=schema
    )
    first_batch = next(batches_iter)
    schema = first_batch.schema.with_metadata(_create_geoparquet_metadata())
    with pq.ParquetWriter(output_path, schema, **kwargs) as writer:
        writer.write_batch(first_batch)
        for batch in batches_iter:
            writer.write_batch(batch)


def to_parquet(table: pa.Table, where: Any, **kwargs: Any) -> None:
    """Write an Arrow table with STAC data to GeoParquet

    This writes metadata compliant with GeoParquet 1.1.

    Args:
        table: The table to write to Parquet
        where: The destination for saving.
    """
    metadata = table.schema.metadata or {}
    metadata.update(_create_geoparquet_metadata())
    table = table.replace_schema_metadata(metadata)

    pq.write_table(table, where, **kwargs)


def _create_geoparquet_metadata() -> dict[bytes, bytes]:
    # TODO: include bbox of geometries
    column_meta = {
        "encoding": "WKB",
        # TODO: specify known geometry types
        "geometry_types": [],
        "crs": WGS84_CRS_JSON,
        "edges": "planar",
        "covering": {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            }
        },
    }
    geo_meta = {
        "version": "1.1.0-dev",
        "columns": {"geometry": column_meta},
        "primary_column": "geometry",
    }
    return {b"geo": json.dumps(geo_meta).encode("utf-8")}
