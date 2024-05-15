"""Convert STAC data into Arrow tables"""

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union, Iterable

import ciso8601
from itertools import islice
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
import shapely.geometry
import orjson

from stac_geoparquet.arrow._to_parquet import WGS84_CRS_JSON
from stac_geoparquet.json_reader import read_json


def _chunks(
    lst: Iterable[Dict[str, Any]], n: int
) -> Iterable[Sequence[Dict[str, Any]]]:
    """Yield successive n-sized chunks from iterable."""
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(lst)
    while batch := tuple(islice(it, n)):
        yield batch


def parse_stac_items_to_batches(
    items: Iterable[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
    downcast: bool = True,
) -> Iterable[pa.RecordBatch]:
    """Parse a collection of STAC Items to an iterable of :class:`pyarrow.RecordBatch`.

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
        an iterable of pyarrow RecordBatches with the STAC-GeoParquet representation of items.
    """

    if schema is not None:
        # If schema is provided, then for better memory usage we parse input STAC items
        # to Arrow batches in chunks.
        for chunk in _chunks(items, chunk_size):
            yield _stac_items_to_arrow(chunk, schema=schema, downcast=downcast)
    else:
        yield _stac_items_to_arrow(items, downcast=downcast)


def parse_stac_items_to_arrow(
    items: Iterable[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
    downcast: bool = True,
) -> pa.Table:
    batches = parse_stac_items_to_batches(
        items, chunk_size=chunk_size, schema=schema, downcast=downcast
    )
    return pa.Table.from_batches(batches, schema=schema)


def parse_stac_ndjson_to_batches(
    path: Union[str, Path],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
    downcast: bool = True,
) -> Iterable[pa.RecordBatch]:
    return parse_stac_items_to_batches(
        read_json(path), chunk_size=chunk_size, schema=schema, downcast=downcast
    )


def parse_stac_ndjson_to_arrow(
    path: Union[str, Path],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
    downcast: bool = True,
) -> pa.Table:
    batches = parse_stac_items_to_batches(
        read_json(path), chunk_size=chunk_size, schema=schema, downcast=downcast
    )
    return pa.Table.from_batches(batches, schema=schema)


def _process_arrow_table(
    table: Union[pa.Table, pa.RecordBatch], *, downcast: bool = True
) -> Union[pa.Table, pa.RecordBatch]:
    table = _bring_properties_to_top_level(table)
    table = _convert_timestamp_columns(table)
    table = _convert_bbox_to_struct(table, downcast=downcast)
    table = _assign_geoarrow_metadata(table)
    return table


def _stac_items_to_arrow(
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
        # Note: this mutates the existing items. Should we
        wkb_item["geometry"] = shapely.to_wkb(
            shapely.geometry.shape(wkb_item["geometry"]), flavor="iso"
        )
        wkb_items.append(wkb_item)

    if schema is not None:
        array = pa.array(wkb_items, type=pa.struct(schema))
    else:
        array = pa.array(wkb_items)
    return _process_arrow_table(
        pa.RecordBatch.from_struct_array(array), downcast=downcast
    )


def _bring_properties_to_top_level(
    table: Union[pa.Table, pa.RecordBatch],
) -> Union[pa.Table, pa.RecordBatch]:
    """Bring all the fields inside of the nested "properties" struct to the top level"""
    properties_field = table.schema.field("properties")
    properties_column = table["properties"]

    for field_idx in range(properties_field.type.num_fields):
        inner_prop_field = properties_field.type.field(field_idx)
        table = table.append_column(
            inner_prop_field, pc.struct_field(properties_column, field_idx)
        )

    table = table.drop_columns(
        [
            "properties",
        ]
    )
    return table


def _convert_geometry_to_wkb(
    table: Union[pa.Table, pa.RecordBatch],
) -> Union[pa.Table, pa.RecordBatch]:
    """Convert the geometry column in the table to WKB"""
    geoms = shapely.from_geojson(
        [orjson.dumps(item) for item in table["geometry"].to_pylist()]
    )
    wkb_geoms = shapely.to_wkb(geoms)
    return table.drop_columns(
        [
            "geometry",
        ]
    ).append_column("geometry", pa.array(wkb_geoms))


def _convert_timestamp_columns(
    table: Union[pa.Table, pa.RecordBatch],
) -> Union[pa.Table, pa.RecordBatch]:
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
    table: Union[pa.Table, pa.RecordBatch], *, downcast: bool
) -> Union[pa.Table, pa.RecordBatch]:
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

    return table.set_column(bbox_col_idx, "bbox", struct_arr)


def _assign_geoarrow_metadata(
    table: Union[pa.Table, pa.RecordBatch],
) -> Union[pa.Table, pa.RecordBatch]:
    """Tag the primary geometry column with `geoarrow.wkb` on the field metadata."""
    existing_field_idx = table.schema.get_field_index("geometry")
    existing_field = table.schema.field(existing_field_idx)
    ext_metadata = {"crs": WGS84_CRS_JSON}
    field_metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": orjson.dumps(ext_metadata),
    }
    new_field = existing_field.with_metadata(field_metadata)
    return table.set_column(
        existing_field_idx, new_field, table.column(existing_field_idx)
    )
