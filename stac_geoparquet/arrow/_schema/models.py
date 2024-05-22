from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import pyarrow as pa

from stac_geoparquet.arrow._util import stac_items_to_arrow
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
        path: Union[str, Path, Iterable[Union[str, Path]]],
        *,
        chunk_size: int = 65536,
        limit: Optional[int] = None,
    ) -> None:
        """
        Update this inferred schema from one or more newline-delimited JSON STAC files.

        Args:
            path: One or more paths to files with STAC items.
            chunk_size: The chunk size to load into memory at a time. Defaults to 65536.

        Other args:
            limit: The maximum number of JSON Items to use for schema inference
        """
        for batch in read_json_chunked(path, chunk_size=chunk_size, limit=limit):
            self.update_from_items(batch)

    def update_from_items(self, items: Sequence[Dict[str, Any]]) -> None:
        """Update this inferred schema from a sequence of STAC Items."""
        self.count += len(items)
        current_schema = stac_items_to_arrow(items, schema=None).schema
        new_schema = pa.unify_schemas(
            [self.inner, current_schema], promote_options="permissive"
        )
        self.inner = new_schema
