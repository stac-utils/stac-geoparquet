import json
from pathlib import Path

import pyarrow as pa
import pytest

from stac_geoparquet.arrow import (
    parse_stac_items_to_arrow,
    parse_stac_ndjson_to_arrow,
    stac_table_to_items,
    stac_table_to_ndjson,
)

from .json_equals import assert_json_value_equal

HERE = Path(__file__).parent

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
def test_round_trip_read_write(collection_id: str):
    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    table = pa.Table.from_batches(parse_stac_items_to_arrow(items))
    items_result = list(stac_table_to_items(table))

    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)


@pytest.mark.parametrize("collection_id", TEST_COLLECTIONS)
def test_round_trip_write_read_ndjson(collection_id: str, tmp_path: Path):
    # First load into a STAC-GeoParquet table
    path = HERE / "data" / f"{collection_id}-pc.json"
    table = pa.Table.from_batches(parse_stac_ndjson_to_arrow(path))

    # Then write to disk
    stac_table_to_ndjson(table, tmp_path / "tmp.ndjson")

    # Then read back and assert tables match
    table = pa.Table.from_batches(parse_stac_ndjson_to_arrow(tmp_path / "tmp.ndjson"))


def test_table_contains_geoarrow_metadata():
    collection_id = "naip"
    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    table = pa.Table.from_batches(parse_stac_items_to_arrow(items))
    field_meta = table.schema.field("geometry").metadata
    assert field_meta[b"ARROW:extension:name"] == b"geoarrow.wkb"
    assert json.loads(field_meta[b"ARROW:extension:metadata"])["crs"]["id"] == {
        "authority": "EPSG",
        "code": 4326,
    }


@pytest.mark.parametrize("collection_id", TEST_COLLECTIONS)
def test_parse_json_to_arrow(collection_id: str):
    path = HERE / "data" / f"{collection_id}-pc.json"
    table = pa.Table.from_batches(parse_stac_ndjson_to_arrow(path))
    items_result = list(stac_table_to_items(table))

    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)


def test_to_arrow_deprecated():
    with pytest.warns(FutureWarning):
        import stac_geoparquet.to_arrow
    stac_geoparquet.to_arrow.parse_stac_items_to_arrow


def test_from_arrow_deprecated():
    with pytest.warns(FutureWarning):
        import stac_geoparquet.from_arrow

    stac_geoparquet.from_arrow.stac_table_to_items
