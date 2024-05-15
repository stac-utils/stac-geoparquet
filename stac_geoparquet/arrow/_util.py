from copy import deepcopy
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Sequence,
)

import ciso8601
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
import shapely.geometry
import orjson
from itertools import islice

from stac_geoparquet.arrow._crs import WGS84_CRS_JSON


def update_batch_schema(
    batch: pa.RecordBatch,
    schema: pa.Schema,
) -> pa.RecordBatch:
    """Update a batch with new schema."""
    return pa.record_batch(batch.to_pydict(), schema=schema)


def batched_iter(
    lst: Iterable[Dict[str, Any]], n: int
) -> Iterable[Sequence[Dict[str, Any]]]:
    """Yield successive n-sized chunks from iterable."""
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(lst)
    while batch := tuple(islice(it, n)):
        yield batch


def stac_items_to_arrow(
    items: Iterable[Dict[str, Any]],
    *,
    schema: Optional[pa.Schema] = None,
    downcast: bool = True,
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
            wkb_item["properties"]["proj:geometry"] = shapely.to_wkb(
                shapely.geometry.shape(wkb_item["properties"]["proj:geometry"]),
                flavor="iso",
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
    return _process_arrow_batch(
        pa.RecordBatch.from_struct_array(array), downcast=downcast
    )


def _bring_properties_to_top_level(
    batch: pa.RecordBatch,
) -> pa.RecordBatch:
    """Bring all the fields inside of the nested "properties" struct to the top level"""
    properties_field = batch.schema.field("properties")
    properties_column = batch["properties"]

    for field_idx in range(properties_field.type.num_fields):
        inner_prop_field = properties_field.type.field(field_idx)
        batch = batch.append_column(
            inner_prop_field, pc.struct_field(properties_column, field_idx)
        )

    batch = batch.drop_columns(
        [
            "properties",
        ]
    )
    return batch


def _convert_geometry_to_wkb(
    batch: pa.RecordBatch,
) -> pa.RecordBatch:
    """Convert the geometry column in the table to WKB"""
    geoms = shapely.from_geojson(
        [orjson.dumps(item) for item in batch["geometry"].to_pylist()]
    )
    wkb_geoms = shapely.to_wkb(geoms)
    return batch.drop_columns(
        [
            "geometry",
        ]
    ).append_column("geometry", pa.array(wkb_geoms))


def _convert_timestamp_columns(
    batch: pa.RecordBatch,
) -> pa.RecordBatch:
    """Convert all timestamp columns from a string to an Arrow Timestamp data type"""
    allowed_column_names = {
        "datetime",  # common metadata
        "start_datetime",
        "end_datetime",
        "created",
        "updated",
        "expires",  # timestamps extension
        "published",
        "unpublished",
    }
    for column_name in allowed_column_names:
        try:
            column = batch[column_name]
        except KeyError:
            continue

        field_index = batch.schema.get_field_index(column_name)

        if pa.types.is_timestamp(column.type):
            continue

        # STAC allows datetimes to be null. If all rows are null, the column type may be
        # inferred as null. We cast this to a timestamp column.
        elif pa.types.is_null(column.type):
            batch = batch.set_column(
                field_index, column_name, column.cast(pa.timestamp("us"))
            )

        elif pa.types.is_string(column.type):
            batch = batch.set_column(
                field_index, column_name, _convert_timestamp_column(column)
            )
        else:
            raise ValueError(
                f"Inferred time column '{column_name}' was expected to be a string or"
                f" timestamp data type but got {column.type}"
            )

    return batch


def _convert_timestamp_column(column: pa.Array) -> pa.TimestampArray:
    """Convert an individual timestamp column from string to a Timestamp type"""
    return pa.array(
        [ciso8601.parse_rfc3339(str(t)) for t in column], pa.timestamp("us", tz="UTC")
    )


def _is_bbox_3d(bbox_col: pa.Array) -> bool:
    """Infer whether the bounding box column represents 2d or 3d bounding boxes."""
    offsets_set = set()
    offsets = bbox_col.offsets.to_numpy()
    offsets_set.update(np.unique(offsets[1:] - offsets[:-1]))

    if len(offsets_set) > 1:
        raise ValueError("Mixed 2d-3d bounding boxes not yet supported")

    offset = list(offsets_set)[0]
    if offset == 6:
        return True
    elif offset == 4:
        return False
    else:
        raise ValueError(f"Unexpected bbox offset: {offset=}")


def _convert_bbox_to_struct(
    batch: pa.RecordBatch, downcast: bool = True
) -> pa.RecordBatch:
    """Convert bbox column to a struct representation

    Since the bbox in JSON is stored as an array, pyarrow automatically converts the
    bbox column to a ListArray. But according to GeoParquet 1.1, we should save the bbox
    column as a StructArray, which allows for Parquet statistics to infer any spatial
    partitioning in the dataset.

    Args:
        table: _description_
        downcast: if True, will use float32 coordinates for the bounding boxes instead
            of float64. Float rounding is applied to ensure the float32 bounding box
            strictly contains the original float64 box. This is recommended when
            possible to minimize file size.

    Returns:
        New table
    """
    bbox_col_idx = batch.schema.get_field_index("bbox")
    bbox_col = batch.column(bbox_col_idx)
    bbox_3d = _is_bbox_3d(bbox_col)

    assert (
        pa.types.is_list(bbox_col.type)
        or pa.types.is_large_list(bbox_col.type)
        or pa.types.is_fixed_size_list(bbox_col.type)
    )
    if bbox_3d:
        coords = bbox_col.flatten().to_numpy().reshape(-1, 6)
    else:
        coords = bbox_col.flatten().to_numpy().reshape(-1, 4)

    if downcast:
        coords = coords.astype(np.float32)

    if bbox_3d:
        xmin = coords[:, 0]
        ymin = coords[:, 1]
        zmin = coords[:, 2]
        xmax = coords[:, 3]
        ymax = coords[:, 4]
        zmax = coords[:, 5]

        if downcast:
            # Round min values down to the next float32 value
            # Round max values up to the next float32 value
            xmin = np.nextafter(xmin, -np.Infinity)
            ymin = np.nextafter(ymin, -np.Infinity)
            zmin = np.nextafter(zmin, -np.Infinity)
            xmax = np.nextafter(xmax, np.Infinity)
            ymax = np.nextafter(ymax, np.Infinity)
            zmax = np.nextafter(zmax, np.Infinity)

        struct_arr = pa.StructArray.from_arrays(
            [
                xmin,
                ymin,
                zmin,
                xmax,
                ymax,
                zmax,
            ],
            names=[
                "xmin",
                "ymin",
                "zmin",
                "xmax",
                "ymax",
                "zmax",
            ],
        )

    else:
        xmin = coords[:, 0]
        ymin = coords[:, 1]
        xmax = coords[:, 2]
        ymax = coords[:, 3]

        if downcast:
            # Round min values down to the next float32 value
            # Round max values up to the next float32 value
            xmin = np.nextafter(xmin, -np.Infinity)
            ymin = np.nextafter(ymin, -np.Infinity)
            xmax = np.nextafter(xmax, np.Infinity)
            ymax = np.nextafter(ymax, np.Infinity)

        struct_arr = pa.StructArray.from_arrays(
            [
                xmin,
                ymin,
                xmax,
                ymax,
            ],
            names=[
                "xmin",
                "ymin",
                "xmax",
                "ymax",
            ],
        )

    return batch.set_column(bbox_col_idx, "bbox", struct_arr)


def _assign_geoarrow_metadata(
    batch: pa.RecordBatch,
) -> pa.RecordBatch:
    """Tag the primary geometry column with `geoarrow.wkb` on the field metadata."""
    existing_field_idx = batch.schema.get_field_index("geometry")
    existing_field = batch.schema.field(existing_field_idx)
    ext_metadata = {"crs": WGS84_CRS_JSON}
    field_metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": orjson.dumps(ext_metadata),
    }
    new_field = existing_field.with_metadata(field_metadata)
    return batch.set_column(
        existing_field_idx, new_field, batch.column(existing_field_idx)
    )


def _process_arrow_batch(
    batch: pa.RecordBatch, *, downcast: bool = True
) -> pa.RecordBatch:
    batch = _bring_properties_to_top_level(batch)
    batch = _convert_timestamp_columns(batch)
    batch = _convert_bbox_to_struct(batch, downcast=downcast)
    batch = _assign_geoarrow_metadata(batch)
    return batch