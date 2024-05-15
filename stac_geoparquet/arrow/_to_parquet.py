import json
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from stac_geoparquet.arrow._to_arrow import parse_stac_items_to_batches
from stac_geoparquet.json_reader import read_json
from stac_geoparquet.arrow._util import update_batch_schema
from stac_geoparquet.arrow._crs import WGS84_CRS_JSON


def parse_stac_ndjson_to_parquet(
    input_path: Union[Union[str, Path], Iterable[Union[str, Path]]],
    output_path: Union[str, Path],
    *,
    chunk_size: int = 65536,
    schema: Optional[pa.Schema] = None,
    downcast: bool = True,
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
    batches = parse_stac_items_to_batches(
        read_json(input_path), chunk_size=chunk_size, schema=schema, downcast=downcast
    )
    if schema is None:
        unified_batches = []
        for batch in batches:
            if schema is None:
                schema = batch.schema
            else:
                schema = pa.unify_schemas(
                    [schema, batch.schema], promote_options="permissive"
                )
            unified_batches.append(update_batch_schema(batch, schema))
        batches = unified_batches

    assert schema is not None
    schema = schema.with_metadata(_create_geoparquet_metadata())

    with pq.ParquetWriter(output_path, schema, **kwargs) as writer:
        for batch in batches:
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
