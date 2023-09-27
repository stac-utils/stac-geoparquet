"""Convert STAC Items in Arrow Table format to JSON Lines or """

import json
from typing import Iterable, List

import pyarrow as pa
import pyarrow.compute as pc
import shapely


def stac_table_to_ndjson(table: pa.Table, dest: str):
    """Write a STAC Table to a newline-delimited JSON file."""
    with open(dest, "w") as f:
        for item_dict in stac_table_to_items(table):
            json.dump(item_dict, f, separators=(",", ":"))
            f.write("\n")


def stac_table_to_items(table: pa.Table) -> Iterable[dict]:
    """Convert a STAC Table to a generator of STAC Item `dict`s"""
    table = _undo_stac_table_transformations(table)

    # Convert WKB geometry column to GeoJSON, and then assign the geojson geometry when
    # converting each row to a dictionary.
    for batch in table.to_batches():
        geoms = shapely.from_wkb(batch["geometry"])
        geojson_strings = shapely.to_geojson(geoms)

        # RecordBatch is missing a `drop()` method, so we keep all columns other than
        # geometry instead
        keep_column_names = [name for name in batch.column_names if name != "geometry"]
        struct_batch = batch.select(keep_column_names).to_struct_array()

        for row_idx in range(len(struct_batch)):
            row_dict = struct_batch[row_idx].as_py()
            row_dict["geometry"] = json.loads(geojson_strings[row_idx])
            yield row_dict


def _undo_stac_table_transformations(table: pa.Table) -> pa.Table:
    """Undo the transformations done to convert STAC Json into an Arrow Table

    Note that this function does _not_ undo the GeoJSON -> WKB geometry transformation,
    as that is easier to do when converting each item in the table to a dict.
    """
    table = _convert_timestamp_columns_to_string(table)["created"]
    table = _lower_properties_from_top_level(table)
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
