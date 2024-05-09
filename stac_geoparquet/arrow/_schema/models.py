import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import pyarrow as pa

from stac_geoparquet.arrow._util import stac_items_to_arrow


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

    def update_from_ndjson(
        self,
        path: Union[Union[str, Path], Iterable[Union[str, Path]]],
        *,
        chunk_size: int = 10000,
    ):
        # Handle multi-path input
        if not isinstance(path, (str, Path)):
            for p in path:
                self.update_from_ndjson(p)

            return

        # Handle single-path input
        with open(path) as f:
            items = []
            for line in f:
                item = json.loads(line)
                items.append(item)

                if len(items) >= chunk_size:
                    self.update_from_items(items)
                    items = []

            # Handle remainder
            if len(items) > 0:
                self.update_from_items(items)

    def update_from_items(self, items: List[Dict[str, Any]]):
        self.count += len(items)
        current_schema = stac_items_to_arrow(items, schema=None).schema
        new_schema = pa.unify_schemas(
            [self.inner, current_schema], promote_options="permissive"
        )
        self.inner = new_schema
