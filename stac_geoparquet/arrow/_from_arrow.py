"""Convert STAC Items in Arrow Table format to JSON Lines or Python dicts."""

import orjson
import operator
import os
from functools import reduce
from typing import Any, Dict, Iterable, List, Sequence, Tuple, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
from numpy.typing import NDArray
import shapely.geometry


def stac_batch_to_items(batch: pa.RecordBatch) -> Iterable[dict]:
    """Convert a stac arrow recordbatch to item dicts."""
    batch = _undo_stac_transformations(batch)
    # Find all paths in the schema that have a WKB geometry
    geometry_paths = [["geometry"]]
    try:
        batch.schema.field("properties").type.field("proj:geometry")
        geometry_paths.append(["properties", "proj:geometry"])
    except KeyError:
        pass

    assets_struct = batch.schema.field("assets").type
    for asset_idx in range(assets_struct.num_fields):
        asset_field = assets_struct.field(asset_idx)
        if "proj:geometry" in pa.schema(asset_field).names:
            geometry_paths.append(["assets", asset_field.name, "proj:geometry"])

    # Convert each geometry column to a Shapely geometry, and then assign the
    # geojson geometry when converting each row to a dictionary.
    geometries: List[NDArray[np.object_]] = []
    for geometry_path in geometry_paths:
        col = batch
        for path_segment in geometry_path:
            if isinstance(col, pa.RecordBatch):
                col = col[path_segment]
            elif pa.types.is_struct(col.type):
                col = pc.struct_field(col, path_segment)
            else:
                raise AssertionError(f"unexpected type {type(col)}")

        geometries.append(shapely.from_wkb(col))

    struct_batch = batch.to_struct_array()
    for row_idx in range(len(struct_batch)):
        row_dict = struct_batch[row_idx].as_py()
        for geometry_path, geometry_column in zip(geometry_paths, geometries):
            geojson_g = geometry_column[row_idx].__geo_interface__
            geojson_g["coordinates"] = convert_tuples_to_lists(geojson_g["coordinates"])
            set_by_path(row_dict, geometry_path, geojson_g)

        yield row_dict


def stac_table_to_ndjson(table: pa.Table, dest: Union[str, os.PathLike[str]]) -> None:
    """Write a STAC Table to a newline-delimited JSON file."""
    with open(dest, "wb") as f:
        for item_dict in stac_table_to_items(table):
            f.write(orjson.dumps(item_dict))
            f.write(b"\n")


def stac_table_to_items(table: pa.Table) -> Iterable[dict]:
    """Convert a STAC Table to a generator of STAC Item `dict`s"""
    for batch in table.to_batches():
        yield from stac_batch_to_items(batch)


def _undo_stac_transformations(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Undo the transformations done to convert STAC Json into an Arrow Table

    Note that this function does _not_ undo the GeoJSON -> WKB geometry transformation,
    as that is easier to do when converting each item in the table to a dict.
    """
    batch = _convert_timestamp_columns_to_string(batch)
    batch = _lower_properties_from_top_level(batch)
    batch = _convert_bbox_to_array(batch)
    return batch


def _convert_timestamp_columns_to_string(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert any datetime columns in the table to a string representation"""
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

        batch = batch.drop_columns((column_name,)).append_column(
            column_name, pc.strftime(column, format="%Y-%m-%dT%H:%M:%SZ")
        )

    return batch


def _lower_properties_from_top_level(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Take properties columns from the top level and wrap them in a struct column"""
    stac_top_level_keys = {
        "stac_version",
        "stac_extensions",
        "type",
        "id",
        "bbox",
        "geometry",
        "collection",
        "links",
        "assets",
    }

    properties_column_names: List[str] = []
    properties_column_fields: List[pa.Field] = []
    for column_idx in range(batch.num_columns):
        column_name = batch.column_names[column_idx]
        if column_name in stac_top_level_keys:
            continue

        properties_column_names.append(column_name)
        properties_column_fields.append(batch.schema.field(column_idx))

    struct_arr = pa.StructArray.from_arrays(
        batch.select(properties_column_names).columns, fields=properties_column_fields
    )

    return batch.drop_columns(properties_column_names).append_column(
        "properties", struct_arr
    )


def _convert_bbox_to_array(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert the struct bbox column back to a list column for writing to JSON"""

    bbox_col_idx = batch.schema.get_field_index("bbox")
    bbox_col = batch.column(bbox_col_idx)

    # new_chunks = []
    # for chunk in bbox_col.chunks:
    assert pa.types.is_struct(bbox_col.type)

    if bbox_col.type.num_fields == 4:
        xmin = bbox_col.field("xmin").to_numpy()
        ymin = bbox_col.field("ymin").to_numpy()
        xmax = bbox_col.field("xmax").to_numpy()
        ymax = bbox_col.field("ymax").to_numpy()
        coords = np.column_stack(
            [
                xmin,
                ymin,
                xmax,
                ymax,
            ]
        )

        list_arr = pa.FixedSizeListArray.from_arrays(coords.flatten("C"), 4)

    elif bbox_col.type.num_fields == 6:
        xmin = bbox_col.field("xmin").to_numpy()
        ymin = bbox_col.field("ymin").to_numpy()
        zmin = bbox_col.field("zmin").to_numpy()
        xmax = bbox_col.field("xmax").to_numpy()
        ymax = bbox_col.field("ymax").to_numpy()
        zmax = bbox_col.field("zmax").to_numpy()
        coords = np.column_stack(
            [
                xmin,
                ymin,
                zmin,
                xmax,
                ymax,
                zmax,
            ]
        )

        list_arr = pa.FixedSizeListArray.from_arrays(coords.flatten("C"), 6)

    else:
        raise ValueError("Expected 4 or 6 fields in bbox struct.")

    return batch.set_column(bbox_col_idx, "bbox", list_arr)


def convert_tuples_to_lists(t: List | Tuple) -> List[Any]:
    """Convert tuples to lists, recursively

    For example, converts:
    ```
    (
        (
            (-112.4820566, 38.1261015),
            (-112.4816283, 38.1331311),
            (-112.4833551, 38.1338897),
            (-112.4832919, 38.1307687),
            (-112.4855415, 38.1291793),
            (-112.4820566, 38.1261015),
        ),
    )
    ```

    to

    ```py
    [
        [
            [-112.4820566, 38.1261015],
            [-112.4816283, 38.1331311],
            [-112.4833551, 38.1338897],
            [-112.4832919, 38.1307687],
            [-112.4855415, 38.1291793],
            [-112.4820566, 38.1261015],
        ]
    ]
    ```

    From https://stackoverflow.com/a/1014669.
    """
    return list(map(convert_tuples_to_lists, t)) if isinstance(t, (list, tuple)) else t


def get_by_path(root: Dict[str, Any], keys: Sequence[str]) -> Any:
    """Access a nested object in root by item sequence.

    From https://stackoverflow.com/a/14692747
    """
    return reduce(operator.getitem, keys, root)


def set_by_path(root: Dict[str, Any], keys: Sequence[str], value: Any) -> None:
    """Set a value in a nested object in root by item sequence.

    From https://stackoverflow.com/a/14692747
    """
    get_by_path(root, keys[:-1])[keys[-1]] = value  # type: ignore
