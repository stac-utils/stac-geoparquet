from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from stac_geoparquet.arrow._batch import StacJsonBatch, has_3d_bbox, with_bbox_size
from stac_geoparquet.arrow._util import resolve_chunk_size
from stac_geoparquet.json_reader import read_json_chunked


def geoparquet_schema(
    json_schema: pa.Schema, drop_invalid_properties: bool
) -> pa.Schema:
    """The GeoParquet schema that items with `json_schema` convert to."""
    if "bbox" in json_schema.names and not has_3d_bbox(json_schema):
        # A bbox list of any length is read as 2d
        json_schema = with_bbox_size(json_schema, 4)
    batch = StacJsonBatch(pa.RecordBatch.from_pylist([], schema=json_schema))
    return batch.to_arrow_batch(drop_invalid_properties).inner.schema


class InferredSchema:
    """
    A schema representing the original STAC JSON with absolutely minimal modifications.

    The only modifications from the data are converting any geometry fields from
    GeoJSON to WKB, and typing the bbox as a fixed-size list of 4 or 6 values.
    """

    inner: pa.Schema
    """The underlying Arrow schema."""

    count: int
    """The total number of items scanned."""

    def __init__(self) -> None:
        self.inner = pa.schema([])
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

    def update_from_items(self, items: Sequence[dict[str, Any]]) -> None:
        """Update this inferred schema from a sequence of STAC Items."""
        self.count += len(items)
        batch = StacJsonBatch.from_dicts(items, schema=None).inner
        current_schema = batch.schema
        if "bbox" in current_schema.names:
            # The bbox type says whether bboxes are 2d or 3d: a list of 4 or 6
            longest = pc.max(pc.list_value_length(batch.column("bbox"))).as_py()
            current_schema = with_bbox_size(current_schema, 6 if longest == 6 else 4)
        new_schema = pa.unify_schemas(
            [self.inner, current_schema], promote_options="permissive"
        )
        if "bbox" in new_schema.names:
            # Merging lists of 4 and 6 gives a list of any length
            size = 6 if has_3d_bbox(self.inner) or has_3d_bbox(current_schema) else 4
            new_schema = with_bbox_size(new_schema, size)
        self.inner = new_schema

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
