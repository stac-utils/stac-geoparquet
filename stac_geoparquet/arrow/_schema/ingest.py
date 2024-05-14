"""Schema-aware ingestion"""

import pyarrow as pa
from copy import deepcopy
from typing import Sequence

import json

from stac_geoparquet.arrow._schema.models import PartialSchema


# TODO: convert `items` to an Iterable to allow generator input. Then this function
# should return a generator of Arrow RecordBatches for output.
def ingest(items: Sequence[dict], schema_fragments: Sequence[PartialSchema]):
    """_summary_"""
    # Preprocess items
    new_items = []
    for item in items:
        new_item = deepcopy(item)
        for schema_fragment in schema_fragments:
            schema_fragment.preprocess_item(new_item)

        new_items.append(new_item)

    # Combine Arrow schemas across fragments
    arrow_schema_fragments = [fragment.to_dict_input() for fragment in schema_fragments]
    unified_arrow_schema = pa.unify_schemas(
        arrow_schema_fragments, promote_options="permissive"
    )

    struct_array = pa.array(new_items, pa.struct(unified_arrow_schema))
    return pa.RecordBatch.from_struct_array(struct_array)


def _example():
    path = "/Users/kyle/github/stac-utils/stac-geoparquet/tests/data/naip-pc.json"
    with open(path) as f:
        items = json.load(f)

    schema_fragments = [Core(), EO(), Proj()]

    batch = ingest(items, schema_fragments)
    # Works!
