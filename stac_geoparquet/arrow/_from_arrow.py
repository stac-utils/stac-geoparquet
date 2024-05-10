"""Convert STAC Items in Arrow Table format to JSON Lines or Python dicts."""

import json
import operator
import os
from functools import reduce
from typing import Iterable, List, Sequence, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
from numpy.typing import NDArray
import shapely.geometry


def stac_table_to_ndjson(table: pa.Table, dest: Union[str, os.PathLike[str]]) -> None:
    """Write a STAC Table to a newline-delimited JSON file."""
    with open(dest, "w") as f:
        for item_dict in stac_table_to_items(table):
            json.dump(item_dict, f, separators=(",", ":"))
            f.write("\n")


def stac_table_to_items(table: pa.Table) -> Iterable[dict]:
    """Convert a STAC Table to a generator of STAC Item `dict`s"""
    table = _undo_stac_table_transformations(table)

    # Find all paths in the schema that have a WKB geometry
    geometry_paths = [["geometry"]]
    try:
        table.schema.field("properties").type.field("proj:geometry")
        geometry_paths.append(["properties", "proj:geometry"])
    except KeyError:
        pass

    assets_struct = table.schema.field("assets").type
    for asset_idx in range(assets_struct.num_fields):
        asset_field = assets_struct.field(asset_idx)
        if "proj:geometry" in pa.schema(asset_field).names:
            geometry_paths.append(["assets", asset_field.name, "proj:geometry"])

    for batch in table.to_batches():
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
                geojson_g["coordinates"] = convert_tuples_to_lists(
                    geojson_g["coordinates"]
                )
                set_by_path(row_dict, geometry_path, geojson_g)

            yield row_dict


def _undo_stac_table_transformations(table: pa.Table) -> pa.Table:
    """Undo the transformations done to convert STAC Json into an Arrow Table

    Note that this function does _not_ undo the GeoJSON -> WKB geometry transformation,
    as that is easier to do when converting each item in the table to a dict.
    """
    table = _convert_timestamp_columns_to_string(table)
    table = _lower_properties_from_top_level(table)
    table = _convert_bbox_to_array(table)
    return table


def _convert_timestamp_columns_to_string(table: pa.Table) -> pa.Table:
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
            column = table[column_name]
        except KeyError:
            continue

        table = table.drop(column_name).append_column(
            column_name, pc.strftime(column, format="%Y-%m-%dT%H:%M:%SZ")
        )

    return table


def _lower_properties_from_top_level(table: pa.Table) -> pa.Table:
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
    for column_idx in range(table.num_columns):
        column_name = table.column_names[column_idx]
        if column_name in stac_top_level_keys:
            continue

        properties_column_names.append(column_name)
        properties_column_fields.append(table.schema.field(column_idx))

    properties_array_chunks = []
    for batch in table.select(properties_column_names).to_batches():
        struct_arr = pa.StructArray.from_arrays(
            batch.columns, fields=properties_column_fields
        )
        properties_array_chunks.append(struct_arr)

    return table.drop_columns(properties_column_names).append_column(
        "properties", pa.chunked_array(properties_array_chunks)
    )


def _convert_bbox_to_array(table: pa.Table) -> pa.Table:
    """Convert the struct bbox column back to a list column for writing to JSON"""

    bbox_col_idx = table.schema.get_field_index("bbox")
    bbox_col = table.column(bbox_col_idx)

    new_chunks = []
    for chunk in bbox_col.chunks:
        assert pa.types.is_struct(chunk.type)

        if bbox_col.type.num_fields == 4:
            xmin = chunk.field("xmin").to_numpy()
            ymin = chunk.field("ymin").to_numpy()
            xmax = chunk.field("xmax").to_numpy()
            ymax = chunk.field("ymax").to_numpy()
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
            xmin = chunk.field("xmin").to_numpy()
            ymin = chunk.field("ymin").to_numpy()
            zmin = chunk.field("zmin").to_numpy()
            xmax = chunk.field("xmax").to_numpy()
            ymax = chunk.field("ymax").to_numpy()
            zmax = chunk.field("zmax").to_numpy()
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

        new_chunks.append(list_arr)

    return table.set_column(bbox_col_idx, "bbox", new_chunks)


def convert_tuples_to_lists(t: Sequence):
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


def get_by_path(root, items):
    """Access a nested object in root by item sequence.

    From https://stackoverflow.com/a/14692747
    """
    return reduce(operator.getitem, items, root)


def set_by_path(root, items, value):
    """Set a value in a nested object in root by item sequence.

    From https://stackoverflow.com/a/14692747
    """
    get_by_path(root, items[:-1])[items[-1]] = value  # type: ignore
