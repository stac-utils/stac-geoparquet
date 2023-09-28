"""Convert STAC data into Arrow tables
"""

import json
from tempfile import NamedTemporaryFile
from typing import IO, Any, Sequence, Union

import ciso8601
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow.json import read_json
import shapely.geometry


def parse_stac_items_to_arrow(items: Sequence[dict[str, Any]]):
    inferred_arrow_table = _stac_items_to_arrow(items)
    return _updates_for_inferred_arrow_table(inferred_arrow_table)


def parse_stac_ndjson_to_arrow(path: Union[str, IO[bytes]]):
    table = _stac_ndjson_to_arrow(path)
    return _updates_for_inferred_arrow_table(table)


def _updates_for_inferred_arrow_table(table: pa.Table) -> pa.Table:
    table = _bring_properties_to_top_level(table)
    table = _convert_geometry_to_wkb(table)
    table = _convert_timestamp_columns(table)
    return table


def _stac_ndjson_to_arrow(path: Union[str, IO[bytes]]) -> pa.Table:
    """Parse a newline-delimited JSON file to Arrow

    Args:
        path: The path or opened file object (in binary mode) with newline-delimited
            JSON data.

    Returns:
        pyarrow table matching on-disk schema
    """
    table = read_json(path)
    return table


def _stac_items_to_arrow(items: Sequence[dict[str, Any]]) -> pa.Table:
    """Convert dicts representing STAC Items to Arrow

    First writes a tempfile with newline-delimited JSON data, then uses the pyarrow JSON
    parser to load into memory.

    Args:
        items: _description_

    Returns:
        _description_
    """
    # TODO:!! Can just call pa.array() on the list of python dicts!!
    with NamedTemporaryFile("w+b", suffix=".json") as f:
        for item in items:
            f.write(json.dumps(item, separators=(",", ":")).encode("utf-8"))
            f.write("\n".encode("utf-8"))

        return _stac_ndjson_to_arrow(f)


def _bring_properties_to_top_level(table: pa.Table) -> pa.Table:
    """Bring all the fields inside of the nested "properties" struct to the top level"""
    properties_field = table.schema.field("properties")
    properties_column = table["properties"]

    for field_idx in range(properties_field.type.num_fields):
        inner_prop_field = properties_field.type.field(field_idx)
        table = table.append_column(
            inner_prop_field, pc.struct_field(properties_column, field_idx)
        )

    table = table.drop("properties")
    return table


def _convert_geometry_to_wkb(table: pa.Table) -> pa.Table:
    """Convert the geometry column in the table to WKB"""
    geoms = shapely.from_geojson(
        [json.dumps(item) for item in table["geometry"].to_pylist()]
    )
    wkb_geoms = shapely.to_wkb(geoms)
    return table.drop("geometry").append_column("geometry", pa.array(wkb_geoms))


def _convert_timestamp_columns(table: pa.Table) -> pa.Table:
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
            column = table[column_name]
            column.type
        except KeyError:
            continue

        if pa.types.is_timestamp(column.type):
            continue
        elif pa.types.is_string(column.type):
            table = table.drop(column_name).append_column(
                column_name, _convert_timestamp_column(column)
            )
        else:
            raise ValueError(
                f"Inferred time column '{column_name}' was expected to be a string or timestamp data type but got {column.type}"
            )

    return table


def _convert_timestamp_column(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """Convert an individual timestamp column from string to a Timestamp type"""
    chunks = []
    for chunk in column.chunks:
        parsed_chunk = []
        for item in chunk:
            if not item.is_valid:
                parsed_chunk.append(None)
            else:
                parsed_chunk.append(ciso8601.parse_rfc3339(item.as_py()))

        pyarrow_chunk = pa.array(parsed_chunk)
        chunks.append(pyarrow_chunk)

    return pa.chunked_array(chunks)
