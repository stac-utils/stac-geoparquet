"""Convert STAC data into Arrow tables"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Sequence, Union

import ciso8601
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
import shapely.geometry

from stac_geoparquet.arrow._schema.models import InferredSchema
from stac_geoparquet.arrow._util import stac_items_to_arrow


def _chunks(
    lst: Sequence[Dict[str, Any]], n: int
) -> Generator[Sequence[Dict[str, Any]], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def parse_stac_items_to_arrow(
    items: Sequence[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[Union[pa.Schema, InferredSchema]] = None,
    downcast: bool = False,
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
        downcast: if True, store bbox as float32 for memory and disk saving.

    Returns:
        a pyarrow Table with the STAC-GeoParquet representation of items.
    """

    if schema is not None:
        if isinstance(schema, InferredSchema):
            schema = schema.inner

        # If schema is provided, then for better memory usage we parse input STAC items
        # to Arrow batches in chunks.
        batches = []
        for chunk in _chunks(items, chunk_size):
            batches.append(stac_items_to_arrow(chunk, schema=schema))

        table = pa.Table.from_batches(batches, schema=schema)
    else:
        # If schema is _not_ provided, then we must convert to Arrow all at once, or
        # else it would be possible for a STAC item late in the collection (after the
        # first chunk) to have a different schema and not match the schema inferred for
        # the first chunk.
        table = pa.Table.from_batches([stac_items_to_arrow(items)])

    return _process_arrow_table(table, downcast=downcast)


def parse_stac_ndjson_to_arrow(
    path: Union[Union[str, Path], Iterable[Union[str, Path]]],
    *,
    chunk_size: int = 8192,
    schema: Optional[Union[pa.Schema, InferredSchema]] = None,
):
    # Define outside of if/else to make mypy happy
    items: List[dict] = []

    # If the schema was not provided, then we need to load all data into memory at once
    # to perform schema resolution.
    if schema is None:
        inferred_schema = InferredSchema()
        inferred_schema.update_from_ndjson(path, chunk_size=chunk_size)
        return parse_stac_ndjson_to_arrow(
            path, chunk_size=chunk_size, schema=inferred_schema
        )

    # Check if path is an iterable
    # If so, recursively call this function on each item in the iterable
    if not isinstance(path, (str, Path)):
        for p in path:
            yield from parse_stac_ndjson_to_arrow(
                p, chunk_size=chunk_size, schema=schema
            )

        return

    if isinstance(schema, InferredSchema):
        schema = schema.inner

    # Otherwise, we can stream over the input, converting each batch of `chunk_size`
    # into an Arrow RecordBatch at a time. This is much more memory efficient.
    with open(path) as f:
        for line in f:
            items.append(json.loads(line))

            if len(items) >= chunk_size:
                batch = stac_items_to_arrow(items, schema=schema)
                yield from _process_arrow_table(
                    pa.Table.from_batches([batch]), downcast=False
                ).to_batches()
                items = []

    # Don't forget the last chunk in case the total number of items is not a multiple of
    # chunk_size.
    if len(items) > 0:
        batch = stac_items_to_arrow(items, schema=schema)
        yield from _process_arrow_table(
            pa.Table.from_batches([batch]), downcast=False
        ).to_batches()


def _process_arrow_table(table: pa.Table, *, downcast: bool = True) -> pa.Table:
    table = _bring_properties_to_top_level(table)
    table = _convert_timestamp_columns(table)
    table = _convert_bbox_to_struct(table, downcast=downcast)
    return table


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

        field_index = table.schema.get_field_index(column_name)

        if pa.types.is_timestamp(column.type):
            continue

        # STAC allows datetimes to be null. If all rows are null, the column type may be
        # inferred as null. We cast this to a timestamp column.
        elif pa.types.is_null(column.type):
            table = table.set_column(
                field_index, column_name, column.cast(pa.timestamp("us"))
            )

        elif pa.types.is_string(column.type):
            table = table.set_column(
                field_index, column_name, _convert_timestamp_column(column)
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


def _is_bbox_3d(bbox_col: pa.ChunkedArray) -> bool:
    """Infer whether the bounding box column represents 2d or 3d bounding boxes."""
    offsets_set = set()
    for chunk in bbox_col.chunks:
        offsets = chunk.offsets.to_numpy()
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


def _convert_bbox_to_struct(table: pa.Table, *, downcast: bool) -> pa.Table:
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
    bbox_3d = _is_bbox_3d(bbox_col)

    new_chunks = []
    for chunk in bbox_col.chunks:
        assert (
            pa.types.is_list(chunk.type)
            or pa.types.is_large_list(chunk.type)
            or pa.types.is_fixed_size_list(chunk.type)
        )
        if bbox_3d:
            coords = chunk.flatten().to_numpy().reshape(-1, 6)
        else:
            coords = chunk.flatten().to_numpy().reshape(-1, 4)

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

        new_chunks.append(struct_arr)

    return table.set_column(bbox_col_idx, "bbox", new_chunks)
