import json
from typing import List, Optional

import pyarrow as pa
import shapely
import shapely.geometry

from stac_geoparquet.arrow._schema.default_schemas import (
    TimestampResolution,
    item_core,
    item_eo,
    item_proj,
)


class PartialSchema:
    inner: pa.Schema

    def to_dict_input(self) -> pa.Schema:
        """Convert this partial schema to one that works on input STAC data"""
        return self.inner

    def preprocess_item(self, item: dict) -> dict:
        """
        Any pre-processing steps to be applied to the input STAC dict before converting
        with Arrow.

        Note: this pre-processing is allowed to mutate input.
        """
        return item


class Core(PartialSchema):
    def __init__(
        self,
        *,
        timestamp_resolution: TimestampResolution = "us",
        asset_keys: Optional[List[str]] = None,
    ) -> None:
        schema = item_core(
            timestamp_resolution=timestamp_resolution, asset_keys=asset_keys
        )
        self.inner = schema
        super().__init__()

    def to_dict_input(self) -> pa.Schema:
        schema = self.inner
        schema = _timestamp_to_string(schema)
        schema = _lower_properties(schema)
        schema = _bbox_struct_to_list(schema)
        return schema

    def preprocess_item(self, item: dict) -> dict:
        item["geometry"] = shapely.to_wkb(
            shapely.geometry.shape(item["geometry"]), flavor="iso"
        )
        return item


class EO(PartialSchema):
    def __init__(
        self,
        *,
        properties: bool = True,
        asset_keys: Optional[List[str]] = None,
    ) -> None:
        schema = item_eo(properties=properties, asset_keys=asset_keys)
        self.inner = schema
        super().__init__()


class Proj(PartialSchema):
    properties: bool
    asset_keys: Optional[List[str]]

    def __init__(
        self,
        *,
        properties: bool = True,
        asset_keys: Optional[List[str]] = None,
    ) -> None:
        schema = item_proj(properties=properties, asset_keys=asset_keys)
        self.inner = schema
        self.properties = properties
        self.asset_keys = asset_keys
        super().__init__()

    def to_dict_input(self) -> pa.Schema:
        schema = self.inner
        schema = _lower_properties(schema)
        return schema

    def preprocess_item(self, item: dict) -> dict:
        projjson = item["properties"].get("proj:projjson")
        if projjson is not None:
            item["properties"]["proj:projjson"] = json.dumps(
                projjson, separators=(",", ":")
            )

        geometry = item["properties"].get("proj:geometry")
        if geometry is not None:
            item["properties"]["proj:geometry"] = shapely.to_wkb(
                shapely.geometry.shape(geometry), flavor="iso"
            )

        # TODO: handle projjson and geometry inside asset keys

        return super().preprocess_item(item)


STAC_TOP_LEVEL_KEYS = {
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


def _lower_properties(schema: pa.Schema) -> pa.Schema:
    """Take properties fields from the top level and wrap them in a struct column"""

    properties_fields: List[pa.Field] = []
    top_level_fields: List[pa.Field] = []

    for field_idx in range(len(schema)):
        field = schema.field(field_idx)
        if field.name in STAC_TOP_LEVEL_KEYS:
            # Add to top-level fields
            top_level_fields.append(field)
        else:
            # Put inside properties struct
            properties_fields.append(field)

    top_level_fields.append(pa.field("properties", pa.struct(properties_fields)))
    return pa.schema(top_level_fields)


def _bbox_struct_to_list(schema: pa.Schema) -> pa.Schema:
    """Convert the bbox struct field to a variable-sized list"""
    bbox_idx = schema.get_field_index("bbox")
    bbox_field = schema.field(bbox_idx)
    return schema.set(bbox_idx, bbox_field.with_type(pa.list_(pa.float64())))


def _timestamp_to_string(schema: pa.Schema) -> pa.Schema:
    new_fields = []
    for field_idx in range(len(schema)):
        field = schema.field(field_idx)
        if pa.types.is_timestamp(field.type):
            new_fields.append(field.with_type(pa.utf8()))
        # elif pa.types.is_struct(field.type):
        #     field.type
        #     pa.struct([]).field(0)
        else:
            new_fields.append(field)

    return pa.schema(new_fields)
