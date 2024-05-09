from copy import deepcopy
from typing import Any, Dict, Optional, Sequence

import pyarrow as pa
import shapely
import shapely.geometry


def stac_items_to_arrow(
    items: Sequence[Dict[str, Any]], *, schema: Optional[pa.Schema] = None
) -> pa.RecordBatch:
    """Convert dicts representing STAC Items to Arrow

    This converts GeoJSON geometries to WKB before Arrow conversion to allow multiple
    geometry types.

    All items will be parsed into a single RecordBatch, meaning that each internal array
    is fully contiguous in memory for the length of `items`.

    Args:
        items: STAC Items to convert to Arrow

    Kwargs:
        schema: An optional schema that describes the format of the data. Note that this
            must represent the geometry column as binary type.

    Returns:
        Arrow RecordBatch with items in Arrow
    """
    # Preprocess GeoJSON to WKB in each STAC item
    # Otherwise, pyarrow will try to parse coordinates into a native geometry type and
    # if you have multiple geometry types pyarrow will error with
    # `ArrowInvalid: cannot mix list and non-list, non-null values`
    wkb_items = []
    for item in items:
        wkb_item = deepcopy(item)
        wkb_item["geometry"] = shapely.to_wkb(
            shapely.geometry.shape(wkb_item["geometry"]), flavor="iso"
        )

        # If a proj:geometry key exists in top-level properties, convert that to WKB
        if "proj:geometry" in wkb_item["properties"]:
            wkb_item["proj:geometry"] = shapely.to_wkb(
                shapely.geometry.shape(wkb_item["proj:geometry"]), flavor="iso"
            )

        # If a proj:geometry key exists in any asset properties, convert that to WKB
        for asset_value in wkb_item["assets"].values():
            if "proj:geometry" in asset_value:
                asset_value["proj:geometry"] = shapely.to_wkb(
                    shapely.geometry.shape(asset_value["proj:geometry"]),
                    flavor="iso",
                )

        wkb_items.append(wkb_item)

    if schema is not None:
        array = pa.array(wkb_items, type=pa.struct(schema))
    else:
        array = pa.array(wkb_items)

    return pa.RecordBatch.from_struct_array(array)
