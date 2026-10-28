from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pystac

from stac_geoparquet.arrow._batch import (
    StacArrowBatch,
    StacJsonBatch,
    item_geometries_to_wkb,
)
from stac_geoparquet.arrow._constants import BBOX_3D_METADATA
from stac_geoparquet.arrow._memory import chunks_within_memory
from stac_geoparquet.arrow._util import resolve_chunk_size
from stac_geoparquet.json_reader import read_json_chunked


def geoparquet_schema(
    json_schema: pa.Schema, drop_invalid_properties: bool
) -> pa.Schema:
    """The GeoParquet schema that items with `json_schema` convert to."""
    batch = StacJsonBatch(pa.RecordBatch.from_pylist([], schema=json_schema))
    return batch.to_arrow_batch(drop_invalid_properties).inner.schema


def _with_3d_bbox(schema: pa.Schema) -> pa.Schema:
    index = schema.get_field_index("bbox")
    return schema.set(index, schema.field(index).with_metadata(BBOX_3D_METADATA))


class InferredSchema:
    """
    A schema representing the original STAC JSON with absolutely minimal modifications.

    The only modifications from the data are converting any geometry fields from
    GeoJSON to WKB, and marking the bbox field when some items have 3d bboxes.
    """

    inner: pa.Schema
    """The underlying Arrow schema."""

    count: int
    """The total number of items scanned."""

    def __init__(self, inner: pa.Schema | None = None) -> None:
        self.inner = pa.schema([]) if inner is None else inner
        self.count = 0

    def update_from_json(
        self,
        path: str | Path | Iterable[str | Path],
        *,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> None:
        """
        Update this inferred schema from one or more newline-delimited JSON STAC files.

        Args:
            path: One or more paths to files with STAC items.
            chunk_size: The chunk size to load into memory at a time. Defaults to
                the `STAC_GEOPARQUET_CHUNK_SIZE` environment variable, or 65536.

        Keyword Args:
            limit: The maximum number of JSON Items to use for schema inference
        """
        chunk_size = resolve_chunk_size(chunk_size)
        for batch in read_json_chunked(path, chunk_size=chunk_size, limit=limit):
            self.update_from_items(batch)

    def update_from_items(
        self,
        items: Iterable[pystac.Item | dict[str, Any]],
    ) -> None:
        """Update this inferred schema from STAC Items."""
        wkb_items = map(item_geometries_to_wkb, items)
        # Small chunks give the same schema in less memory
        for chunk in chunks_within_memory(wkb_items, 1024):
            self.count += len(chunk)
            batch = StacJsonBatch._from_wkb_dicts(chunk, schema=None).inner
            schema = batch.schema
            # Mark the schema if any item has a 3d bbox, so that items with 2d bboxes
            # convert to 3d ones with a null z
            bbox = batch.column("bbox") if "bbox" in schema.names else None
            if bbox is not None and pc.max(pc.list_value_length(bbox)).as_py() == 6:
                schema = _with_3d_bbox(schema)
            self.update_from_schema(schema)
            chunk.clear()

    def update_from_schema(self, schema: pa.Schema) -> None:
        """Merge a STAC JSON schema into this one."""
        new_schema = pa.unify_schemas(
            [self.inner, schema], promote_options="permissive"
        )
        # unify_schemas only keeps the first schema's field metadata
        if "bbox" in schema.names and schema.field("bbox").metadata == BBOX_3D_METADATA:
            new_schema = _with_3d_bbox(new_schema)
        self.inner = new_schema

    def _resolved(self) -> pa.Schema:
        """The schema with `manual_updates` applied, which items are converted with."""
        resolved = InferredSchema(self.inner)
        resolved.manual_updates()
        return resolved.inner

    def to_geoparquet(self, drop_invalid_properties: bool = True) -> pa.Schema:
        """
        The schema of these items as written to GeoParquet, with their properties
        as top-level columns.
        """
        return geoparquet_schema(self._resolved(), drop_invalid_properties)

    @classmethod
    def from_geoparquet(cls, schema: pa.Schema) -> InferredSchema:
        """
        Make the STAC JSON schema of items from the schema of a GeoParquet file
        or table, for example to write more items to it.
        """
        batch = pa.RecordBatch.from_pylist([], schema=schema)
        json_schema = StacArrowBatch(batch).to_json_batch().inner.schema

        bbox_index = json_schema.get_field_index("bbox")
        bbox = json_schema.field(bbox_index).with_type(pa.list_(pa.float64()))
        json_schema = json_schema.set(bbox_index, bbox)
        if schema.field("bbox").type.num_fields == 6:
            json_schema = _with_3d_bbox(json_schema)

        # Keep the properties in the order of their columns
        properties_index = json_schema.get_field_index("properties")
        properties_field = json_schema.field(properties_index)
        properties = sorted(
            properties_field.type, key=lambda f: schema.get_field_index(f.name)
        )
        json_schema = json_schema.set(
            properties_index, properties_field.with_type(pa.struct(properties))
        )
        return cls(json_schema)

    def manual_updates(self) -> None:
        schema = self.inner
        properties_field = schema.field("properties")
        properties_schema = pa.schema(properties_field.type)

        # The datetime column can be inferred as `null` in the case of a Collection with
        # start_datetime and end_datetime. But `null` is incompatible with Delta Lake,
        # so we coerce to a Timestamp type.
        if pa.types.is_null(properties_schema.field("datetime").type):
            field_idx = properties_schema.get_field_index("datetime")
            properties_schema = properties_schema.set(
                field_idx,
                properties_schema.field(field_idx).with_type(
                    pa.timestamp("us", tz="UTC")
                ),
            )

        if "proj:epsg" in properties_schema.names and pa.types.is_null(
            properties_schema.field("proj:epsg").type
        ):
            field_idx = properties_schema.get_field_index("proj:epsg")
            properties_schema = properties_schema.set(
                field_idx,
                properties_schema.field(field_idx).with_type(pa.int64()),
            )

        if "proj:wkt2" in properties_schema.names and pa.types.is_null(
            properties_schema.field("proj:wkt2").type
        ):
            field_idx = properties_schema.get_field_index("proj:wkt2")
            properties_schema = properties_schema.set(
                field_idx,
                properties_schema.field(field_idx).with_type(pa.string()),
            )

        # Note: proj:projjson can also be null, but we don't have a type we can cast
        # that to.

        properties_idx = schema.get_field_index("properties")
        updated_schema = schema.set(
            properties_idx,
            properties_field.with_type(pa.struct(properties_schema)),
        )

        self.inner = updated_schema
