import json
from pathlib import Path

import pytest
from deltalake import DeltaTable

import stac_geoparquet.arrow

from .json_equals import assert_json_value_equal

HERE = Path(__file__).parent

TEST_COLLECTIONS = [
    "3dep-lidar-copc",
    # "3dep-lidar-dsm",
    "cop-dem-glo-30",
    "io-lulc-annual-v02",
    # "io-lulc",
    "landsat-c2-l1",
    "landsat-c2-l2",
    "naip",
    "planet-nicfi-analytic",
    "sentinel-1-rtc",
    "sentinel-2-l2a",
    "us-census",
]


def test_no_deltalake_raises():
    # Dev environment has deltalake, so pretend we don't have it
    stac_geoparquet.arrow._delta_lake._deltalake_installed = False
    with pytest.raises(ImportError, match="The `deltalake` package is required"):
        stac_geoparquet.arrow.parse_stac_ndjson_to_delta_lake("foo", "bar")
    # Reset for subsequent tests
    stac_geoparquet.arrow._delta_lake._deltalake_installed = True


@pytest.mark.parametrize("collection_id", TEST_COLLECTIONS)
def test_round_trip_via_delta_lake(collection_id: str, tmp_path: Path):
    path = HERE / "data" / f"{collection_id}-pc.json"
    out_path = tmp_path / collection_id
    stac_geoparquet.arrow.parse_stac_ndjson_to_delta_lake(path, out_path)

    # Read back into table and convert to json
    dt = DeltaTable(out_path)
    table = dt.to_pyarrow_table()
    items_result = list(stac_geoparquet.arrow.stac_table_to_items(table))

    # Compare with original json
    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)
