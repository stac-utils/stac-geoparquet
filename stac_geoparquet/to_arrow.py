"""Convert STAC data into Arrow tables"""

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import ciso8601
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
import shapely.geometry


def _chunks(lst: Sequence[Dict[str, Any]], n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def parse_stac_items_to_arrow(
    items: Sequence[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
) -> pa.Table:
    """Parse a collection of STAC Items to a :class:`pyarrow.Table`.

    The objects under `properties` are moved up to the top-level of the
    Table, similar to :meth:`geopandas.GeoDataFrame.from_features`.

    Args:
        items: the STAC Items to convert
        chunk_size: The chunk size to use for Arrow record batches. This only takes
            effect if `schema` is not None. When `schema` is None, the input will be
            parsed into a single contiguous record batch. Defaults to 8192.
        schema: The schema of the input data. If provided, can improve memory use;
            otherwise all items need to be parsed into a single array for schema
            inference. Defaults to None.

    Returns:
        a pyarrow Table with the STAC-GeoParquet representation of items.
    """

    if schema is not None:
        # If schema is provided, then for better memory usage we parse input STAC items
        # to Arrow batches in chunks.
        batches = []
        for chunk in _chunks(items, chunk_size):
            batches.append(_stac_items_to_arrow(chunk, schema=schema))

        stac_table = pa.Table.from_batches(batches, schema=schema)
    else:
        # If schema is _not_ provided, then we must convert to Arrow all at once, or
        # else it would be possible for a STAC item late in the collection (after the
        # first chunk) to have a different schema and not match the schema inferred for
        # the first chunk.
        stac_table = pa.Table.from_batches([_stac_items_to_arrow(items)])

    return _process_arrow_table(stac_table)


def parse_stac_ndjson_to_arrow(
    path: Union[str, Path],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
):
    # Define outside of if/else to make mypy happy
    items: List[dict] = []

    # If the schema was not provided, then we need to load all data into memory at once
    # to perform schema resolution.
    if schema is None:
        with open(path) as f:
            for line in f:
                items.append(json.loads(line))

        return parse_stac_items_to_arrow(items, chunk_size=chunk_size, schema=schema)

    # Otherwise, we can stream over the input, converting each batch of `chunk_size`
    # into an Arrow RecordBatch at a time. This is much more memory efficient.
    with open(path) as f:
        batches: List[pa.RecordBatch] = []
        for line in f:
            items.append(json.loads(line))

            if len(items) >= chunk_size:
                batches.append(_stac_items_to_arrow(items, schema=schema))
                items = []

    # Don't forget the last chunk in case the total number of items is not a multiple of
    # chunk_size.
    if len(items) > 0:
        batches.append(_stac_items_to_arrow(items, schema=schema))

    stac_table = pa.Table.from_batches(batches, schema=schema)
    return _process_arrow_table(stac_table)


def _process_arrow_table(table: pa.Table) -> pa.Table:
    table = _bring_properties_to_top_level(table)
    table = _convert_timestamp_columns(table)
    table = _convert_bbox_to_struct(table)
    return table


def _stac_items_to_arrow(
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
        # Note: this mutates the existing items. Should we
        wkb_item["geometry"] = shapely.to_wkb(
            shapely.geometry.shape(wkb_item["geometry"]), flavor="iso"
        )
        wkb_items.append(wkb_item)

    if schema is not None:
        array = pa.array(wkb_items, type=pa.struct(schema))
    else:
        array = pa.array(wkb_items)
    return pa.RecordBatch.from_struct_array(array)


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
                f"Inferred time column '{column_name}' was expected to be a string or"
                f" timestamp data type but got {column.type}"
            )

    return table


def _convert_timestamp_column(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """Convert an individual timestamp column from string to a Timestamp type"""
    chunks = []
    for chunk in column.chunks:
        parsed_chunk: List[Optional[datetime]] = []
        for item in chunk:
            if not item.is_valid:
                parsed_chunk.append(None)
            else:
                parsed_chunk.append(ciso8601.parse_rfc3339(item.as_py()))

        pyarrow_chunk = pa.array(parsed_chunk)
        chunks.append(pyarrow_chunk)

    return pa.chunked_array(chunks)


def _convert_bbox_to_struct(table: pa.Table, *, downcast: bool = True) -> pa.Table:
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
    bbox_col_idx = table.schema.get_field_index("bbox")
    bbox_col = table.column(bbox_col_idx)

    new_chunks = []
    for chunk in bbox_col.chunks:
        assert (
            pa.types.is_list(chunk.type)
            or pa.types.is_large_list(chunk.type)
            or pa.types.is_fixed_size_list(chunk.type)
        )
        coords = chunk.flatten().to_numpy().reshape(-1, 4)
        xmin = coords[:, 0]
        ymin = coords[:, 1]
        xmax = coords[:, 2]
        ymax = coords[:, 3]

        if downcast:
            coords = coords.astype(np.float32)

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
        new_chunks.append(struct_arr)

    return table.set_column(bbox_col_idx, "bbox", new_chunks)
