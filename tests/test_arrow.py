import itertools
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet as pq
import pystac
import pytest
import shapely
import shapely.geometry

from stac_geoparquet.arrow import (
    DEFAULT_JSON_CHUNK_SIZE,
    _api,
    _batch,
    parse_stac_items_to_arrow,
    parse_stac_items_to_parquet,
    parse_stac_ndjson_to_arrow,
    stac_table_to_items,
    stac_table_to_ndjson,
    to_parquet,
)
from stac_geoparquet.arrow._schema.models import InferredSchema
from stac_geoparquet.arrow._util import resolve_output_path_and_filesystem

from .json_equals import assert_json_value_equal

HERE = Path(__file__).parent

TEST_COLLECTIONS = [
    # Microsoft Planetary Computer
    "3dep-lidar-copc-pc",
    "3dep-lidar-dsm-pc",
    "cop-dem-glo-30-pc",
    "io-lulc-annual-v02-pc",
    "io-lulc-pc",
    "landsat-c2-l1-pc",
    "landsat-c2-l2-pc",
    "naip-pc",
    "planet-nicfi-analytic-pc",
    "sentinel-1-rtc-pc",
    "sentinel-2-l2a-pc",
    "us-census-pc",
    # Other
    "umbra-sar",
]

CHUNK_SIZES = [2, DEFAULT_JSON_CHUNK_SIZE]


@pytest.mark.parametrize(
    "collection_id,chunk_size", itertools.product(TEST_COLLECTIONS, CHUNK_SIZES)
)
def test_round_trip_read_write(collection_id: str, chunk_size: int):
    with open(HERE / "data" / f"{collection_id}.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items, chunk_size=chunk_size).read_all()
    items_result = list(stac_table_to_items(table))

    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)


@pytest.mark.parametrize(
    "collection_id,chunk_size", itertools.product(TEST_COLLECTIONS, CHUNK_SIZES)
)
@pytest.mark.parametrize("drop_type", [True, False])
@pytest.mark.parametrize("input_as_batches", [True, False])
def test_round_trip_write_read_ndjson(
    collection_id: str,
    chunk_size: int,
    tmp_path: Path,
    drop_type: bool,
    input_as_batches: bool,
) -> None:
    # First load into a STAC-GeoParquet table
    path = HERE / "data" / f"{collection_id}.json"
    table: pa.Table | pa.RecordBatchReader = parse_stac_ndjson_to_arrow(
        path, chunk_size=chunk_size
    ).read_all()
    if drop_type:
        table = table.drop(["type"])

    if input_as_batches:
        table = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

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
    collection_id = "naip-pc"
    with open(HERE / "data" / f"{collection_id}.json") as f:
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
    path = HERE / "data" / f"{collection_id}.json"
    table = pa.Table.from_batches(parse_stac_ndjson_to_arrow(path))
    items_result = list(stac_table_to_items(table))

    with open(HERE / "data" / f"{collection_id}.json") as f:
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


def test_to_parquet_two_geometry_columns(tmp_path: Path):
    """
     When writing STAC Items that have a proj:geometry field, there should be two
    geometry columns listed in the GeoParquet metadata.
    """
    collection_id = "3dep-lidar-copc-pc"
    with open(HERE / "data" / f"{collection_id}.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items).read_all()
    path = tmp_path / "output.parquet"
    to_parquet(table, path)
    pq_meta = pq.read_metadata(path)

    geo_meta = json.loads(pq_meta.metadata[b"geo"])
    assert geo_meta["primary_column"] == "geometry"
    assert "geometry" in geo_meta["columns"].keys()
    assert "proj:geometry" in geo_meta["columns"].keys()


def test_to_parquet_with_nonlocal_filesystem(tmp_path: Path):
    """Test to_parquet with filesystem parameter and Path object.

    This test verifies that Path objects are properly converted to strings
    when using remote/non-local filesystems, which is required for compatibility
    with PyArrow's ParquetWriter.
    """
    collection_id = "naip-pc"
    with open(HERE / "data" / f"{collection_id}.json") as f:
        items = json.load(f)

    table = parse_stac_items_to_arrow(items).read_all()

    mock_fs = pa.fs.SubTreeFileSystem(str(tmp_path), pa.fs.LocalFileSystem())
    output_path = Path("test.parquet")

    to_parquet(table, output_path, filesystem=mock_fs)

    actual_file = tmp_path / "test.parquet"
    assert actual_file.exists()
    result_table = pq.read_table(str(actual_file))
    assert result_table.num_rows == table.num_rows


def test_parse_stac_items_to_parquet_with_filesystem(tmp_path: Path):
    """Test parse_stac_items_to_parquet with filesystem parameter."""
    collection_id = "naip-pc"
    with open(HERE / "data" / f"{collection_id}.json") as f:
        items = json.load(f)

    mock_fs = pa.fs.SubTreeFileSystem(str(tmp_path), pa.fs.LocalFileSystem())

    # Test Path object (tests the str conversion fix)
    output_path = Path("test_path.parquet")
    parse_stac_items_to_parquet(items, output_path=output_path, filesystem=mock_fs)
    actual_file = tmp_path / "test_path.parquet"
    assert actual_file.exists()
    result_table = pq.read_table(str(actual_file))
    assert result_table.num_rows > 0

    # Test nested directory with Path
    output_nested = Path("output/data/test.parquet")
    parse_stac_items_to_parquet(items, output_path=output_nested, filesystem=mock_fs)
    actual_nested = tmp_path / "output" / "data" / "test.parquet"
    assert actual_nested.exists()
    assert pq.read_table(str(actual_nested)).num_rows > 0

    # Test String path (backward compatibility)
    output_string = "test_string.parquet"
    parse_stac_items_to_parquet(items, output_path=output_string, filesystem=mock_fs)
    actual_string = tmp_path / "test_string.parquet"
    assert actual_string.exists()
    assert pq.read_table(str(actual_string)).num_rows > 0


def test_parse_stac_items_to_parquet_with_scheme_prefixed_output_path(
    tmp_path: Path,
):
    """
    Regression test for output path mangling in `parse_stac_items_to_parquet`

    When a filesystem and scheme-prefixed `output_path` is explicitly provided, the
    scheme-prefixed output_path (e.g., "file://folder/item.parquet") must be resolved
    to a filesystem-relative path before being run through `Path`, since
    `Path("file://folder/item.parquet")` collapses the double slash after the scheme
    (e.g., "file:/folder/item.parquet").
    """
    collection_id = "naip-pc"
    with open(HERE / "data" / f"{collection_id}.json") as f:
        items = json.load(f)

    filesystem = pa.fs.LocalFileSystem()
    output_path = f"file://{tmp_path}/nested/items.parquet"

    parse_stac_items_to_parquet(items, output_path=output_path, filesystem=filesystem)

    actual_file = tmp_path / "nested" / "items.parquet"
    assert actual_file.exists()
    result_table = pq.read_table(str(actual_file))
    assert result_table.num_rows > 0


@pytest.mark.parametrize(
    ["output_path", "expected_path", "filesystem"],
    [
        ("file:///test", "/test", None),
        ("file:///test", "/test", pa.fs.LocalFileSystem()),
        (Path("/test"), "/test", None),
        (Path("/test"), "/test", pa.fs.LocalFileSystem()),
        ("s3://bucket/key", "bucket/key", None),
        ("s3://bucket/key", "bucket/key", pa.fs.S3FileSystem(anonymous=True)),
    ],
)
def test_resolve_output_path_and_filesystem(
    output_path: str | Path,
    expected_path: str | Path,
    filesystem: pa.fs.FileSystem | None,
):
    """Ensure output_path is parsed correctly with or without filesystem provided"""
    returned_filesystem, returned_path = resolve_output_path_and_filesystem(
        output_path, filesystem
    )
    assert returned_path == expected_path
    if filesystem is not None:
        assert returned_filesystem is filesystem


@pytest.mark.parametrize("colliding_key", ["collection", "geometry"])
def test_properties_key_colliding_with_top_level_field(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    colliding_key: str,
):
    """Regression test to avoid clobbering top level metadata with properties

    Per STAC GeoParquet specification:
    > STAC GeoParquet does not support properties that are named such that they
    > collide with a top-level key.
    """
    item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "item1",
        "properties": {
            "datetime": "2020-01-01T00:00:00Z",
            colliding_key: "bogus-value",
        },
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "links": [],
        "assets": {"data": {"href": "https://example.com/a.tif"}},
        "bbox": [0, 0, 0, 0],
        "collection": "test",
    }

    with pytest.raises(ValueError, match=colliding_key):
        parse_stac_items_to_arrow([item], drop_invalid_properties=False).read_all()

    with caplog.at_level("WARNING"):
        table = parse_stac_items_to_arrow([item]).read_all()
    assert f"properties.{colliding_key}" in caplog.text
    assert table.schema.names.count(colliding_key) == 1


def test_all_null_timestamp_column_is_timezone_aware():
    """Regression test for tz-naive output from an all-null timestamp column

    STAC allows a null `datetime` when `start_datetime`/`end_datetime` are set. When
    every row is null the column was inferred as null type. Comparisons between TZ-aware
    and TZ-naive columns fail (e.g., when coalescing datetime and start_datetime).
    """
    item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test",
        "collection": "test",
        "bbox": [0, 0, 0, 0],
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {
            "datetime": None,
            "start_datetime": "2020-01-01T00:00:00Z",
            "end_datetime": "2020-01-01T00:00:10Z",
        },
        "assets": {},
        "links": [],
    }

    table = parse_stac_items_to_arrow([item]).read_all()

    assert table.schema.field("datetime").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("start_datetime").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("end_datetime").type == pa.timestamp("us", tz="UTC")


def test_to_parquet_empty_assets(tmp_path: Path):
    item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "a",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {},
        "links": [],
    }
    path = tmp_path / "out.parquet"
    to_parquet(parse_stac_items_to_arrow([item]), path)

    items = list(stac_table_to_items(pq.read_table(path)))
    assert items[0]["assets"] == {}


def _read_items(collection_id: str) -> list[dict]:
    with open(HERE / "data" / f"{collection_id}.json") as f:
        return json.load(f)


def _naip_schema() -> InferredSchema:
    schema = InferredSchema()
    schema.update_from_items(_read_items("naip-pc"))
    return schema


def test_parse_stac_items_to_arrow_with_schema_and_no_items():
    schema = _naip_schema()

    reader = parse_stac_items_to_arrow([], schema=schema)

    items_reader = parse_stac_items_to_arrow(_read_items("naip-pc"), schema=schema)
    assert reader.schema.equals(items_reader.schema, check_metadata=True)
    assert reader.read_all().num_rows == 0


@pytest.mark.parametrize("schema", ["FirstBatch"])
def test_no_items(schema, tmp_path):
    with pytest.raises(ValueError, match="No items"):
        parse_stac_items_to_arrow([], schema=schema, tmpdir=tmp_path)


def test_parse_stac_ndjson_to_arrow_with_schema_and_no_items(tmp_path):
    path = tmp_path / "empty.ndjson"
    path.write_text("")

    schema = _naip_schema()
    reader = parse_stac_ndjson_to_arrow(path, schema=schema)

    items_reader = parse_stac_items_to_arrow(_read_items("naip-pc"), schema=schema)
    assert reader.schema.equals(items_reader.schema)
    assert reader.read_all().num_rows == 0


@pytest.mark.parametrize(
    "collection_id,chunk_size", itertools.product(TEST_COLLECTIONS, CHUNK_SIZES)
)
def test_round_trip_read_write_with_inferred_schema(
    collection_id: str, chunk_size: int, tmp_path: Path
):
    """Regression test for a reader declared with the pre-conversion schema

    With an explicit schema, the returned reader was declared with the JSON-shaped
    schema (a nested `properties` struct) while its batches have properties moved to
    the top level, so consuming the reader raised `ArrowInvalid`.
    """
    with open(HERE / "data" / f"{collection_id}.json") as f:
        items = json.load(f)

    schema = InferredSchema()
    schema.update_from_items(items)
    schema.manual_updates()

    reader = parse_stac_items_to_arrow(items, chunk_size=chunk_size, schema=schema)
    assert "properties" not in reader.schema.names

    to_parquet(reader, tmp_path / "items.parquet")
    table = pq.read_table(tmp_path / "items.parquet")
    items_result = list(stac_table_to_items(table))

    assert len(items_result) == len(items)
    for result, expected in zip(items_result, items):
        assert_json_value_equal(result, expected, precision=0)


@pytest.mark.parametrize("include_srid", [False, True], ids=["wkb", "ewkb"])
def test_wkb_geometry_input_matches_geojson(include_srid: bool):
    items = _read_items("naip-pc")
    for item in items:
        geometry = shapely.set_srid(shapely.geometry.shape(item["geometry"]), 4326)
        item["geometry"] = shapely.to_wkb(geometry, include_srid=include_srid)

    table = parse_stac_items_to_arrow(items).read_all()

    expected = parse_stac_items_to_arrow(_read_items("naip-pc")).read_all()
    assert table.equals(expected)


def test_geometry_written_as_iso_wkb():
    point = shapely.Point(0, 0, 1)
    item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "a",
        "geometry": point.__geo_interface__,
        "bbox": [0, 0, 0, 0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {},
        "links": [],
    }
    table = parse_stac_items_to_arrow([item]).read_all()
    assert table["geometry"][0].as_py() == shapely.to_wkb(point, flavor="iso")


def test_parse_pystac_items():
    items = _read_items("naip-pc")
    pystac_items = [pystac.Item.from_dict(item) for item in items]
    table = parse_stac_items_to_arrow(pystac_items).read_all()
    expected = parse_stac_items_to_arrow(items).read_all()
    # pystac migrates the items' extensions, so only their geometries match
    assert table["geometry"].equals(expected["geometry"])


def test_parse_stac_items_to_arrow_does_not_mutate_input():
    items = _read_items("3dep-lidar-copc-pc")
    for item in items:
        asset = next(iter(item["assets"].values()))
        asset["proj:geometry"] = item["properties"]["proj:geometry"]
    original = json.loads(json.dumps(items))

    table = parse_stac_items_to_arrow(items).read_all()

    assert items == original
    asset_key = next(iter(items[0]["assets"]))
    asset_type = table.schema.field("assets").type.field(asset_key).type
    assert asset_type.field("proj:geometry").type == pa.binary()


@pytest.mark.parametrize("collection_id", TEST_COLLECTIONS)
@pytest.mark.parametrize("with_schema", [False, True])
def test_geometry_column_from_one_buffer(collection_id: str, with_schema: bool):
    wkb_items = [
        _batch.item_geometries_to_wkb(item) for item in _read_items(collection_id)
    ]
    expected = pa.RecordBatch.from_struct_array(pa.array(wkb_items))
    schema = expected.schema if with_schema else None

    batch = _batch.StacJsonBatch._from_wkb_dicts(wkb_items, schema=schema).inner

    assert batch.equals(expected)
    assert all(item["geometry"] is None for item in wkb_items)


def test_geometry_column_too_large_for_one_buffer(monkeypatch):
    monkeypatch.setattr(_batch, "_MAX_BINARY_BYTES", 0)
    wkb_items = [_batch.item_geometries_to_wkb(item) for item in _read_items("naip-pc")]

    with pytest.raises(ValueError, match="chunk_size"):
        _batch.StacJsonBatch._from_wkb_dicts(wkb_items, schema=None)


def test_geometry_column_keeps_other_schema_types():
    items = _read_items("naip-pc")
    wkb_items = [_batch.item_geometries_to_wkb(item) for item in items]
    schema = pa.RecordBatch.from_struct_array(pa.array(wkb_items)).schema
    schema = schema.set(
        schema.get_field_index("geometry"), pa.field("geometry", pa.large_binary())
    )

    batch = _batch.StacJsonBatch.from_dicts(items, schema=schema).inner

    assert batch.schema.equals(schema)


@pytest.mark.parametrize(
    "schema", ["FirstBatch", "FullFile", "ChunksToDisk", "InferredSchema"]
)
def test_parse_stac_items_to_arrow_converts_geometries_once(
    monkeypatch, tmp_path, schema
):
    items = _read_items("naip-pc")
    if schema == "InferredSchema":
        schema = _naip_schema()
    calls = []
    convert = _batch.item_geometries_to_wkb

    def counting(item):
        calls.append(item["id"])
        return convert(item)

    monkeypatch.setattr(_api, "item_geometries_to_wkb", counting)
    monkeypatch.setattr(_batch, "item_geometries_to_wkb", counting)

    parse_stac_items_to_arrow(items, schema=schema, tmpdir=tmp_path).read_all()

    assert sorted(calls) == sorted(item["id"] for item in items)
