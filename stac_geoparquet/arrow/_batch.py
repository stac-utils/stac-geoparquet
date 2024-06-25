from __future__ import annotations

import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pystac
import shapely
import shapely.geometry
from numpy.typing import NDArray

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
        wkb_items = []
        for item in items:
            if isinstance(item, pystac.Item):
                item = item.to_dict(transform_hrefs=False)

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

        return cls(pa.RecordBatch.from_struct_array(array))

    def iter_dicts(self) -> Iterable[dict]:
        batch = self.inner

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

            yield row_dict

    def to_arrow_batch(self) -> StacArrowBatch:
        batch = self.inner

        batch = bring_properties_to_top_level(batch)
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
