from pathlib import Path
import json

import pytest
import geopandas

import stac_geoparquet
from stac_geoparquet.from_arrow import stac_table_to_items
from stac_geoparquet.to_arrow import parse_stac_items_to_arrow


HERE = Path(__file__).parent


@pytest.fixture
def naip_items():
    with open(HERE / "data" / "naip-pc.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items)
    items_rt = list(stac_table_to_items(table))

    with open("orig.json", "w") as f:
        json.dump(items[0], f, indent=2, sort_keys=True)

    with open("new.json", "w") as f:
        json.dump(items_rt[0], f, indent=2, sort_keys=True)

    # items[0] == items_rt[0]
    # item = items[0]
    # item_rt = items_rt[0]
    # differing_items = {k: item[k] for k in item if k in item_rt and item[k] != item_rt[k]}

    # p1 = item['properties']
    # p2 = item_rt['properties']
    # differs(p1, p2)

    # l1 = item["links"]
    # l2 = item_rt["links"]

    # l1[0] == l2[0]

    # a = {'a': 1, 'b': 2}
    # b = {"b": 2, "a": 1}
    # a == b

    # return geopandas.read_parquet()


def differs(x: dict, y: dict) -> dict:
    return {k: x[k] for k in x if k in y and x[k] != y[k]}
