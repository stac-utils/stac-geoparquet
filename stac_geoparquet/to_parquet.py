import json

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

WGS84_CRS_JSON = CRS.from_epsg(4326).to_json_dict()


def to_parquet(table: pa.Table, where, **kwargs) -> None:
    """Write an Arrow table with STAC data to GeoParquet

    This writes metadata compliant with GeoParquet 1.1.

    Args:
        table: The table to write to Parquet
        where: The destination for saving.
    """
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

    metadata = table.schema.metadata or {}
    metadata.update({b"geo": json.dumps(geo_meta).encode("utf-8")})
    table = table.replace_schema_metadata(metadata)

    pq.write_table(table, where, **kwargs)
