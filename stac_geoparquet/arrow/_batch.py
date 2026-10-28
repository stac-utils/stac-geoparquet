from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pystac
import shapely
from numpy.typing import NDArray

from stac_geoparquet._wkb import to_iso_wkb
from stac_geoparquet.arrow._from_arrow import (
    convert_bbox_to_array,
    convert_timestamp_columns_to_string,
    lower_properties_from_top_level,
)
from stac_geoparquet.arrow._to_arrow import (
    assign_geoarrow_metadata,
    bring_properties_to_top_level,
    convert_bbox_to_struct,
    convert_timestamp_columns,
)
from stac_geoparquet.arrow._util import convert_tuples_to_lists, set_by_path

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


logger = logging.getLogger(__name__)


# Above this, the geometries don't fit in a binary array's 32-bit offsets
_MAX_BINARY_BYTES = np.iinfo(np.int32).max


def item_geometries_to_wkb(item: pystac.Item | dict[str, Any]) -> dict[str, Any]:
    """
    Return a copy of a STAC item with `geometry` and any `proj:geometry` in its
    properties or assets converted to ISO WKB. `item` is not modified.
    """
    if isinstance(item, pystac.Item):
        item = item.to_dict(transform_hrefs=False)

    wkb_item = dict(item)
    wkb_item["geometry"] = to_iso_wkb(wkb_item["geometry"])

    # If a proj:geometry key exists in top-level properties, convert that to WKB
    if "proj:geometry" in wkb_item["properties"]:
        properties = dict(wkb_item["properties"])
        properties["proj:geometry"] = to_iso_wkb(properties["proj:geometry"])
        wkb_item["properties"] = properties

    # If a proj:geometry key exists in any asset properties, convert that to WKB
    wkb_item["assets"] = {
        key: {**asset, "proj:geometry": to_iso_wkb(asset["proj:geometry"])}
        if "proj:geometry" in asset
        else asset
        for key, asset in wkb_item["assets"].items()
    }

    return wkb_item


def _pop_geometry_array(wkb_items: Sequence[dict[str, Any]]) -> pa.Array:
    """Move the geometries out of `wkb_items` into one binary array."""
    geometries = [item["geometry"] for item in wkb_items]
    offsets = np.zeros(len(geometries) + 1, dtype=np.int64)
    np.cumsum([len(geometry) for geometry in geometries], out=offsets[1:])
    if offsets[-1] > _MAX_BINARY_BYTES:
        raise ValueError(
            "A chunk's geometries are over 2 GiB: use a smaller chunk_size"
        )

    for item in wkb_items:
        item["geometry"] = None
    return pa.Array.from_buffers(
        pa.binary(),
        len(geometries),
        [
            None,
            pa.py_buffer(offsets.astype(np.int32)),
            pa.py_buffer(b"".join(geometries)),
        ],
    )


class StacJsonBatch:
    """
    An Arrow RecordBatch of STAC Items that has been **minimally converted** to Arrow.
    That is, it aligns as much as possible to the raw STAC JSON representation.

    The **only** transformations that have already been applied here are those that are
    necessary to represent the core STAC items in Arrow.

    - `geometry` has been converted to WKB binary
    - `properties.proj:geometry`, if it exists, has been converted to WKB binary
      ISO encoding
    - The `proj:geometry` in any asset properties, if it exists, has been converted to
      WKB binary.

    No other transformations have yet been applied. I.e. all properties are still in a
    top-level `properties` struct column.
    """

    inner: pa.RecordBatch
    """The underlying pyarrow RecordBatch"""

    def __init__(self, batch: pa.RecordBatch) -> None:
        self.inner = batch

    @classmethod
    def from_dicts(
        cls,
        items: Iterable[pystac.Item | dict[str, Any]],
        *,
        schema: pa.Schema | None = None,
    ) -> Self:
        """Construct a StacJsonBatch from an iterable of dicts representing STAC items.

        All items will be parsed into a single RecordBatch, meaning that each internal
        array is fully contiguous in memory for the length of `items`.

        Args:
            items: STAC Items to convert to Arrow

        Kwargs:
            schema: An optional schema that describes the format of the data. Note that
                this must represent the geometry column and any `proj:geometry` columns
                as binary type.

        Returns:
            a new StacJsonBatch of data.
        """
        # Preprocess GeoJSON to WKB in each STAC item
        # Otherwise, pyarrow will try to parse coordinates into a native geometry type
        # and if you have multiple geometry types pyarrow will error with
        # `ArrowInvalid: cannot mix list and non-list, non-null values`
        wkb_items = [item_geometries_to_wkb(item) for item in items]
        return cls._from_wkb_dicts(wkb_items, schema=schema)

    @classmethod
    def _from_wkb_dicts(
        cls,
        wkb_items: Sequence[dict[str, Any]],
        *,
        schema: pa.Schema | None,
    ) -> Self:
        """
        Like `from_dicts`, for items returned by `item_geometries_to_wkb`. Unless
        `schema` gives the geometry another type, the geometries are moved out of
        the items, leaving None.
        """
        logger.info(f"Items Length: {len(wkb_items)}")

        if len(wkb_items) == 0:
            raise ValueError("No items provided to StacJsonBatch.from_dicts")

        # Build the geometry column from one buffer: pa.array's growing buffers
        # can need twice its size. The None left in each item keeps its field.
        geometry = None
        if schema is None or schema.field("geometry").type == pa.binary():
            geometry = _pop_geometry_array(wkb_items)

        if schema is not None:
            array = pa.array(wkb_items, type=pa.struct(schema))
        else:
            array = pa.array(wkb_items)

        batch = pa.RecordBatch.from_struct_array(array)
        if geometry is not None:
            index = batch.schema.get_field_index("geometry")
            field = batch.schema.field(index).with_type(geometry.type)
            batch = batch.set_column(index, field, geometry)
        return cls(batch)

    def iter_dicts(self) -> Iterable[dict]:
        batch = self.inner

        # Find all paths in the schema that have a WKB geometry
        geometry_paths = [["geometry"]]
        try:
            batch.schema.field("properties").type.field("proj:geometry")
            geometry_paths.append(["properties", "proj:geometry"])
        except KeyError:
            pass

        assets_fields = (
            batch.schema.field("assets").type if "assets" in batch.schema.names else []
        )
        for asset_field in assets_fields:
            if "proj:geometry" in pa.schema(asset_field).names:
                geometry_paths.append(["assets", asset_field.name, "proj:geometry"])

        # Convert each geometry column to a Shapely geometry, and then assign the
        # geojson geometry when converting each row to a dictionary.
        geometries: list[NDArray[np.object_]] = []
        for geometry_path in geometry_paths:
            col = batch
            for path_segment in geometry_path:
                if isinstance(col, pa.RecordBatch):
                    col = col[path_segment]
                elif pa.types.is_struct(col.type):
                    col = pc.struct_field(col, path_segment)  # type: ignore
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

            assets = row_dict.setdefault("assets", {})
            if assets and isinstance(
                assets, dict
            ):  # the isinstance should never fail but better safe than sorry
                row_dict["assets"] = {
                    key: value for key, value in assets.items() if value is not None
                }

            yield row_dict

    def to_arrow_batch(self, drop_invalid_properties: bool = True) -> StacArrowBatch:
        batch = self.inner

        batch = bring_properties_to_top_level(
            batch, drop_invalid_properties=drop_invalid_properties
        )
        batch = convert_timestamp_columns(batch)
        batch = convert_bbox_to_struct(batch)
        batch = assign_geoarrow_metadata(batch)

        return StacArrowBatch(batch)

    def to_ndjson(self, dest: str | Path | os.PathLike[bytes]) -> None:
        with open(dest, "ab") as f:
            for item_dict in self.iter_dicts():
                f.write(orjson.dumps(item_dict))
                f.write(b"\n")


class StacArrowBatch:
    """
    An Arrow RecordBatch of STAC Items that has been processed to match the
    STAC-GeoParquet specification.
    """

    inner: pa.RecordBatch
    """The underlying pyarrow RecordBatch"""

    def __init__(self, batch: pa.RecordBatch) -> None:
        self.inner = batch

    def to_json_batch(self) -> StacJsonBatch:
        batch = self.inner

        batch = convert_timestamp_columns_to_string(batch)
        batch = lower_properties_from_top_level(batch)
        batch = convert_bbox_to_array(batch)

        return StacJsonBatch(batch)
