import itertools
import json
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stac_geoparquet.arrow import (
    DEFAULT_JSON_CHUNK_SIZE,
    parse_stac_items_to_arrow,
    parse_stac_ndjson_to_arrow,
    stac_table_to_items,
    stac_table_to_ndjson,
    to_parquet,
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

CHUNK_SIZES = [2, DEFAULT_JSON_CHUNK_SIZE]


@pytest.mark.parametrize(
    "collection_id,chunk_size", itertools.product(TEST_COLLECTIONS, CHUNK_SIZES)
)
def test_round_trip_read_write(collection_id: str, chunk_size: int):
    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items, chunk_size=chunk_size).read_all()
    items_result = list(stac_table_to_items(table))

    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)


@pytest.mark.parametrize(
    "collection_id,chunk_size", itertools.product(TEST_COLLECTIONS, CHUNK_SIZES)
)
def test_round_trip_write_read_ndjson(
    collection_id: str, chunk_size: int, tmp_path: Path
):
    # First load into a STAC-GeoParquet table
    path = HERE / "data" / f"{collection_id}-pc.json"
    table = parse_stac_ndjson_to_arrow(path, chunk_size=chunk_size).read_all()

    # Then write to disk
    stac_table_to_ndjson(table, tmp_path / "tmp.ndjson")

    with open(path) as f:
        orig_json = json.load(f)

    rt_json = []
    with open(tmp_path / "tmp.ndjson") as f:
        for line in f:
            rt_json.append(json.loads(line))

    # Then read back and assert JSON data matches
    assert_json_value_equal(orig_json, rt_json, precision=0)


def test_table_contains_geoarrow_metadata():
    collection_id = "naip"
    with open(HERE / "data" / f"{collection_id}-pc.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items).read_all()
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


def test_to_parquet_two_geometry_columns():
    """
    When writing STAC Items that have a proj:geometry field, there should be two
    geometry columns listed in the GeoParquet metadata.
    """
    with open(HERE / "data" / "3dep-lidar-copc-pc.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items).read_all()
    with BytesIO() as bio:
        to_parquet(table, bio)
        bio.seek(0)
        pq_meta = pq.read_metadata(bio)

    geo_meta = json.loads(pq_meta.metadata[b"geo"])
    assert geo_meta["primary_column"] == "geometry"
    assert "geometry" in geo_meta["columns"].keys()
    assert "proj:geometry" in geo_meta["columns"].keys()
