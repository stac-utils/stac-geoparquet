from typing import IO, Sequence, Any, Union

import pystac
import geopandas
import pyarrow.compute
import pandas as pd
import numpy as np
import pyarrow as pa
from tempfile import NamedTemporaryFile
import json
import pyarrow.json
import shapely.geometry
import ciso8601

dir(ciso8601)


def _stac_ndjson_to_arrow(path: Union[str, IO[bytes]]) -> pa.Table:
    """Parse a newline-delimited JSON file to Arrow

    Args:
        path: The path or opened file object (in binary mode) with newline-delimited
            JSON data.

    Returns:
        pyarrow table matching on-disk schema
    """
    table = pa.json.read_json(path)
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
    with NamedTemporaryFile("w+b", suffix=".json") as f:
        for item in items:
            f.write(json.dumps(item, separators=(",", ":")).encode("utf-8"))
            f.write("\n".encode("utf-8"))

        return _stac_ndjson_to_arrow(f)


def bring_properties_to_top_level(table: pa.Table) -> pa.Table:
    properties_field = table.schema.field("properties")
    properties_column = table["properties"]

    for field_idx in range(properties_field.type.num_fields):
        inner_prop_field = properties_field.type.field(field_idx)
        table = table.append_column(
            inner_prop_field, pa.compute.struct_field(properties_column, field_idx)
        )

    table = table.drop("properties")
    return table


def convert_geometry_to_wkb(table: pa.Table) -> pa.Table:
    """Convert the geometry column in the table to WKB"""
    geoms = shapely.from_geojson(
        [json.dumps(item) for item in table["geometry"].to_pylist()]
    )
    wkb_geoms = shapely.to_wkb(geoms)
    return table.drop("geometry").append_column("geometry", pa.array(wkb_geoms))


def convert_timestamps(table: pa.Table) -> pa.Table:
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
            column_name, _convert_timestamp_column(column)
        )

    return table


def _convert_timestamp_column(column: pa.ChunkedArray) -> pa.Table:
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


# path = "/Users/kyle/tmp/sentinel-stac/combined.jsonl"
# table = _stac_ndjson_to_arrow(path)
# table = bring_properties_to_top_level(table)
# table = convert_geometry_to_wkb(table)
# table = convert_timestamps(table)


# table["created"][0]

# # table2 = bring_properties_to_top_level(table)

# # table2["geometry"]
