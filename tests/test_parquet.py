import json
from pathlib import Path

import jsonschema
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


def test_metadata(tmp_path: Path):
    collection_id = "3dep-lidar-copc"
    path = HERE / "data" / f"{collection_id}-pc.json"
    collection_metadata = json.loads(
        (HERE / f"data/{collection_id}-pc-collection.json").read_text()
    )
    out_path = tmp_path / "file.parquet"
    # Convert to Parquet
    parse_stac_ndjson_to_parquet(
        path, out_path, collection_metadata=collection_metadata
    )
    table = pq.read_table(out_path)

    metadata = table.schema.metadata
    stac_geoparquet_metadata = json.loads(metadata[b"stac-geoparquet"])
    expected_stac_geoparquet_metadata = {
        "version": "1.0.0",
        "stac:collection": collection_metadata,
    }
    assert stac_geoparquet_metadata == expected_stac_geoparquet_metadata
    geo = json.loads(metadata[b"geo"])
    assert geo["version"] == "1.1.0"
    assert set(geo) == {"version", "columns", "primary_column"}

    schema = json.loads((HERE / "../spec/json-schema/metadata.json").read_text())

    # TODO: determine how to version this.
    # The jsonschema we provide embeds a reference to the jsonschema
    # for STAC collections. But that is versioned, and the version
    # must match. Do we do it dynamically, based on the version on
    # stac:collection?
    stac_geoparquet_metadata.pop("stac:collection")
    jsonschema.validate(stac_geoparquet_metadata, schema)
