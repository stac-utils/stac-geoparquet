from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow as pa

from stac_geoparquet.arrow._batch import StacJsonBatch
from stac_geoparquet.arrow._constants import DEFAULT_JSON_CHUNK_SIZE
from stac_geoparquet.json_reader import read_json_chunked


class InferredSchema:
    """
    A schema representing the original STAC JSON with absolutely minimal modifications.

    The only modification from the data is converting any geometry fields from GeoJSON
    to WKB.
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
        chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
        limit: int | None = None,
    ) -> None:
        """
        Update this inferred schema from one or more newline-delimited JSON STAC files.

        Args:
            path: One or more paths to files with STAC items.
            chunk_size: The chunk size to load into memory at a time. Defaults to 65536.

        Keyword Args:
            limit: The maximum number of JSON Items to use for schema inference
        """
        for batch in read_json_chunked(path, chunk_size=chunk_size, limit=limit):
            self.update_from_items(batch)

    def update_from_items(self, items: Sequence[dict[str, Any]]) -> None:
        """Update this inferred schema from a sequence of STAC Items."""
        self.count += len(items)
        current_schema = StacJsonBatch.from_dicts(items, schema=None).inner.schema
        new_schema = pa.unify_schemas(
            [self.inner, current_schema], promote_options="permissive"
        )
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
