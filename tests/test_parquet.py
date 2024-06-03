import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from stac_geoparquet.arrow import parse_stac_ndjson_to_parquet, stac_table_to_items

from .json_equals import assert_json_value_equal

HERE = Path(__file__).parent


def test_to_parquet_deprecated():
    with pytest.warns(FutureWarning):
        import stac_geoparquet.to_parquet

    stac_geoparquet.to_parquet.to_parquet


TEST_COLLECTIONS = [
    "3dep-lidar-copc",
    "3dep-lidar-dsm",
    "cop-dem-glo-30",
    "io-lulc-annual-v02",
    "io-lulc",
    "landsat-c2-l1",
    "landsat-c2-l2",
    "naip",
    "planet-nicfi-analytic",
    "sentinel-1-rtc",
    "sentinel-2-l2a",
    "us-census",
]


@pytest.mark.parametrize("collection_id", TEST_COLLECTIONS)
def test_round_trip_via_parquet(collection_id: str, tmp_path: Path):
    path = HERE / "data" / f"{collection_id}-pc.json"
    out_path = tmp_path / "file.parquet"
    # Convert to Parquet
    parse_stac_ndjson_to_parquet(path, out_path)

    # Read back into table and convert to json
    table = pq.read_table(out_path)
    items_result = list(stac_table_to_items(table))

    # Compare with original json
    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)
