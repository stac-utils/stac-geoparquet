import gc
import json
import pathlib
import threading
import weakref
from datetime import datetime

import docker
import psycopg
import psycopg_pool
import pyarrow.fs
import pyarrow.parquet
import pypgstac
import pystac
import pytest
import shapely
import shapely.geometry
from packaging.version import Version
from pypgstac.db import PgstacDB
from pypgstac.hydration import DO_NOT_MERGE_MARKER
from pypgstac.load import Loader

from stac_geoparquet import pgstac_reader
from stac_geoparquet.arrow import InferredSchema
from stac_geoparquet.pgstac_reader import (
    Partition,
    PgstacRowFactory,
    pgstac_dsn,
    pgstac_to_arrow,
    pgstac_to_iter,
)

HERE = pathlib.Path(__file__).parent

# pypgstac can reject a database migrated by a different pgstac version
DOCKERIMG = f"ghcr.io/stac-utils/pgstac:v{pypgstac.__version__}"
NAIPDATA = HERE / "data" / "naip-pc.json"


def test_sync_pgstac_to_parquet_with_scheme_prefixed_output_path(tmp_path, monkeypatch):
    """
    Regression test for output path mangling in `sync_pgstac_to_parquet`

    When specifying a schema prefixed `output_path` with a `filesystem` the code had
    previously parsed the `output_path` into a `filepath` using `FileSystem.from_uri`,
    but still used `output_path` through `Path()`. This caused inputs like
    `s3://bucket/key` to be mangled into `s3:/bucket/key` (note the single slash after
    scheme), causing a failure inside of `FileSystem.create_dir`.
    """
    partition = Partition(
        collection="naip",
        partition="items.parquet",
        start=None,
        end=None,
        last_updated=datetime(2024, 1, 1),
    )
    monkeypatch.setattr(
        pgstac_reader, "get_pgstac_partitions", lambda *a, **k: iter([partition])
    )

    captured: dict = {}

    def fake_pgstac_to_parquet(
        conninfo: str, output_path: str | pathlib.Path, **kwargs
    ) -> str:
        captured["output_path"] = output_path
        return str(output_path)

    monkeypatch.setattr(pgstac_reader, "pgstac_to_parquet", fake_pgstac_to_parquet)

    filesystem = pyarrow.fs.LocalFileSystem()
    output_path = f"file://{tmp_path}/root"

    pgstac_reader.sync_pgstac_to_parquet(
        "postgres://unused", output_path, filesystem=filesystem
    )

    assert (tmp_path / "root").exists()
    assert "naip" in str(captured["output_path"])


@pytest.fixture(scope="session")
def pgstac_postgres():
    """
    Start a pgstac postgres instance in Docker for testing.
    Yields the connection info string.
    """
    assert NAIPDATA.exists(), "NAIP data file does not exist."
    client = docker.from_env()
    client.images.pull(DOCKERIMG)
    container = client.containers.run(
        DOCKERIMG,
        name="test-pgstac",
        ports={"5432/tcp": 5433},
        environment={
            "POSTGRES_DB": "pgstac",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "pgstac",
        },
        detach=True,
        auto_remove=False,
        remove=False,
    )
    constr = pgstac_dsn(
        "postgres://postgres:pgstac@localhost:5433/pgstac", statement_timeout=30000
    )

    try:
        with PgstacDB(constr, debug=True) as db:
            db.wait()
            assert Version(db.version) >= Version("0.3.4")
            loader = Loader(db)
            naip_collection = {"id": "naip", "title": "NAIP Imagery Test"}
            loader.load_collections([naip_collection])
            collection_count = db.query_one(
                "SELECT COUNT(*) FROM collections WHERE id = 'naip'"
            )
            assert collection_count == 1

            items = json.loads(NAIPDATA.read_text())
            assert len(items) == 4, "Expected 4 items in NAIP data file."

            loader.load_items(
                items,
                pypgstac.load.Methods.ignore,
            )
            item_count = db.query_one(
                "SELECT COUNT(*) FROM items WHERE collection = 'naip'"
            )
            assert item_count == 4
            yield constr
    finally:
        pass
        try:
            container.kill()
        except Exception:
            pass
        try:
            container.remove(force=True)
        except Exception:
            pass


def test_pgstac_reader_iter(pgstac_postgres):
    """
    Test reading from a pgstac instance.
    """
    items = list(pgstac_to_iter(pgstac_postgres))
    assert len(items) == 4
    expected_versions = {
        item["id"]: item["stac_version"] for item in json.loads(NAIPDATA.read_text())
    }
    for item in items:
        assert item["collection"] == "naip"
        assert item["geometry"]["type"] == "Polygon"
        assert item["stac_version"] == expected_versions[item["id"]]


def test_pgstac_reader_arrow(pgstac_postgres):
    """
    Test reading from a pgstac instance.
    """
    items = pgstac_to_arrow(pgstac_postgres)
    assert items.schema.field("collection").name == "collection"
    assert items.schema.field("collection").type == "string"
    assert items.schema.field("id").name == "id"
    assert items.schema.field("id").type == "string"
    assert items.schema.field("geometry").name == "geometry"


def test_sync_pgstac_to_parquet_with_conn_factory(pgstac_postgres, tmp_path):
    """
    conninfo can be provided as a Callable.

    This test exercises the `_connect` function while also threading through the call
    stack of sync_pgstac_to_parquet -> get_pgstac_partitions -> pgstac_to_parquet.
    """
    filesystem = pyarrow.fs.LocalFileSystem()

    written = pgstac_reader.sync_pgstac_to_parquet(
        lambda: psycopg.connect(pgstac_postgres),
        str(tmp_path / "root"),
        filesystem=filesystem,
    )

    table = pyarrow.parquet.read_table(written, filesystem=filesystem)
    assert table.num_rows == 4
    assert set(table.column("collection").to_pylist()) == {"naip"}


def test_pgstac_reader_keeps_item_stac_version(pgstac_postgres):
    """
    pgstac only stores an item's `stac_version` when it differs from its
    collection's, so the stored value must win over the collection's during
    hydration.
    """
    collection_id = "mixed-stac-version"
    naip_item = json.loads(NAIPDATA.read_text())[0]
    items = [
        {**naip_item, "id": item_id, "collection": collection_id, "stac_version": v}
        for item_id, v in [("same-as-collection", "1.0.0"), ("newer", "1.1.0")]
    ]

    with PgstacDB(pgstac_postgres) as db:
        loader = Loader(db)
        loader.load_collections([{"id": collection_id, "stac_version": "1.0.0"}])
        loader.load_items(items, pypgstac.load.Methods.ignore)
    try:
        result = pgstac_to_iter(pgstac_postgres, collection=collection_id)
        assert {item["id"]: item["stac_version"] for item in result} == {
            "same-as-collection": "1.0.0",
            "newer": "1.1.0",
        }
    finally:
        with psycopg.connect(pgstac_postgres, autocommit=True) as conn:
            conn.execute("SELECT delete_collection(%s)", (collection_id,))


@pytest.mark.parametrize(
    "search",
    [
        {"ids": ["pr_m_1806544_ne_20_030_20221212_20230329"]},
        # psycopg must not read the % in the WHERE clause as a placeholder
        {
            "filter-lang": "cql2-json",
            "filter": {"op": "like", "args": [{"property": "id"}, "%1806544_ne%"]},
        },
    ],
)
def test_pgstac_to_iter_search(pgstac_postgres, search):
    items = list(pgstac_to_iter(pgstac_postgres, search=search))
    assert [item["id"] for item in items] == [
        "pr_m_1806544_ne_20_030_20221212_20230329"
    ]


def test_pgstac_to_iter_search_limit_and_sortby(pgstac_postgres):
    search = {
        "collections": ["naip"],
        "sortby": [{"field": "id", "direction": "asc"}],
        "limit": 2,
    }

    items = list(pgstac_to_iter(pgstac_postgres, search=search))

    ids = sorted(item["id"] for item in json.loads(NAIPDATA.read_text()))
    assert [item["id"] for item in items] == ids[:2]


@pytest.mark.parametrize(
    "sortby",
    [
        None,
        [{"field": "gsd", "direction": "desc"}],
        [{"field": "properties.datetime", "direction": "desc"}],
    ],
    ids=["default", "after a sortby", "after datetime"],
)
def test_pgstac_to_iter_search_breaks_ties(pgstac_postgres, sortby):
    # The NAIP items share their datetime and collection, so only their ids differ
    search = {"collections": ["naip"]}
    if sortby is not None:
        search["sortby"] = sortby

    items = pgstac_to_iter(pgstac_postgres, search=search)

    ids = sorted(item["id"] for item in json.loads(NAIPDATA.read_text()))
    assert [item["id"] for item in items] == ids


@pytest.mark.parametrize("key", ["fields", "token"])
def test_pgstac_to_iter_search_unsupported(pgstac_postgres, key):
    with pytest.raises(ValueError, match=key):
        list(pgstac_to_iter(pgstac_postgres, search={"collections": ["naip"], key: {}}))


def test_row_factory_is_garbage_collected(pgstac_postgres):
    with psycopg.connect(pgstac_postgres) as conn:
        factory = PgstacRowFactory(conn.cursor())
        assert factory.get_baseitem("naip") is factory.get_baseitem("naip")
        ref = weakref.ref(factory)
        del factory
        gc.collect()
        assert ref() is None


# Each item is stored after its collection's item_assets changed, so pgstac 0.10
# tags it with the base item it was dehydrated against
EDITED_ASSETS = [
    ("before-edits", {}),
    ("second", {"type": "image/tiff"}),
    ("third", {"type": "image/tiff", "roles": ["data"]}),
]


@pytest.fixture
def edited_collection(pgstac_postgres):
    collection_id = "edited-item-assets"
    naip_item = json.loads(NAIPDATA.read_text())[0]
    with PgstacDB(pgstac_postgres) as db:
        for item_id, asset in EDITED_ASSETS:
            collection = {"id": collection_id, "item_assets": {"data": asset}}
            Loader(db).load_collections([collection], pypgstac.load.Methods.upsert)
            item = {
                **naip_item,
                "id": item_id,
                "collection": collection_id,
                "assets": {"data": {"href": "x.tif", **asset}},
            }
            Loader(db).load_items([item], pypgstac.load.Methods.ignore)
    yield collection_id
    with psycopg.connect(pgstac_postgres, autocommit=True) as conn:
        conn.execute("SELECT delete_collection(%s)", (collection_id,))


def test_items_hydrate_against_their_base_item(pgstac_postgres, edited_collection):
    items = pgstac_to_iter(pgstac_postgres, collection=edited_collection)
    assert {item["id"]: item["assets"]["data"] for item in items} == {
        item_id: {"href": "x.tif", **asset} for item_id, asset in EDITED_ASSETS
    }


def test_missing_base_item(pgstac_postgres):
    with psycopg.connect(pgstac_postgres) as conn:
        factory = PgstacRowFactory(conn.cursor())
        with pytest.raises(ValueError, match="not found"):
            factory.get_baseitem("missing")


def _assert_naip_geometries(ids, geometries):
    expected = {
        item["id"]: shapely.geometry.shape(item["geometry"])
        for item in json.loads(NAIPDATA.read_text())
    }
    assert len(ids) == len(expected)
    for item_id, wkb in zip(ids, geometries):
        assert shapely.from_wkb(wkb).equals(expected[item_id])


@pytest.fixture(params=["arrow", "parquet"])
def read_table(request, pgstac_postgres, tmp_path):
    """Read the NAIP items with pgstac_to_arrow or pgstac_to_parquet."""

    def read(**kwargs):
        kwargs.setdefault("collection", "naip")
        if request.param == "arrow":
            return pgstac_to_arrow(pgstac_postgres, **kwargs).read_all()
        path = pgstac_reader.pgstac_to_parquet(
            pgstac_postgres, tmp_path / "items.parquet", **kwargs
        )
        return pyarrow.parquet.read_table(path)

    return read


@pytest.mark.parametrize(
    ("kwargs", "geometry_type"),
    [({}, bytes), ({"geometry": "wkb"}, bytes), ({"geometry": "geojson"}, dict)],
)
def test_row_func_geometry_type(read_table, kwargs, geometry_type):
    seen = []

    def row_func(item):
        seen.append(type(item["geometry"]))
        return item

    table = read_table(row_func=row_func, **kwargs)

    assert seen == [geometry_type] * 4
    _assert_naip_geometries(table["id"].to_pylist(), table["geometry"].to_pylist())


@pytest.mark.parametrize("kwargs", [{}, {"row_func": lambda item: item}])
def test_no_geojson_round_trip(read_table, monkeypatch, kwargs):
    """Without a GeoJSON row_func, geometries go straight from WKB to Arrow (#175)."""

    def fail(*args, **kwargs):
        raise AssertionError("geometry was converted to or from GeoJSON")

    monkeypatch.setattr(shapely.geometry.Polygon, "__geo_interface__", property(fail))
    monkeypatch.setattr(shapely.geometry, "shape", fail)

    table = read_table(**kwargs)

    monkeypatch.undo()
    _assert_naip_geometries(table["id"].to_pylist(), table["geometry"].to_pylist())


def _replace_geometry(item):
    item["geometry"] = {"type": "Point", "coordinates": [1.0, 2.0]}
    return item


def _set_coordinates(item):
    item["geometry"]["type"] = "Point"
    item["geometry"]["coordinates"] = (1.0, 2.0)
    return item


@pytest.mark.parametrize("row_func", [_replace_geometry, _set_coordinates])
def test_row_func_changes_geometry(read_table, row_func):
    table = read_table(row_func=row_func, geometry="geojson")
    geoms = shapely.from_wkb(table["geometry"].to_pylist())
    assert all(geom.equals(shapely.Point(1, 2)) for geom in geoms)


# pgstac returns hex EWKB with an SRID to a text cursor and bytes to a binary one
@pytest.mark.parametrize("as_hex", [True, False], ids=["hex", "bytes"])
@pytest.mark.parametrize("wkb_geometry", [False, True])
def test_row_factory_geometry(monkeypatch, as_hex, wkb_geometry):
    monkeypatch.setattr(
        PgstacRowFactory, "get_baseitem", lambda self, c, base_item_id: {}
    )
    polygon = shapely.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
    ewkb = shapely.to_wkb(
        shapely.set_srid(polygon, 4326), hex=as_hex, include_srid=True
    )
    content = {"properties": {"datetime": "2024-01-01T00:00:00Z"}, "assets": {}}
    row = ("a", ewkb, "c", content)

    # the cursor is only used to look up base items
    factory = PgstacRowFactory(None, wkb_geometry=wkb_geometry)
    item = factory(row)

    assert item["bbox"] == [0.0, 0.0, 1.0, 1.0]
    if wkb_geometry:
        assert item["geometry"] == shapely.to_wkb(polygon, flavor="iso")
    else:
        assert item["geometry"] == polygon.__geo_interface__


@pytest.mark.parametrize(
    ("kwargs", "geometry_type"), [({}, dict), ({"geometry": "wkb"}, bytes)]
)
def test_pgstac_to_iter_geometry(pgstac_postgres, kwargs, geometry_type):
    seen = []

    def row_func(item):
        seen.append(type(item["geometry"]))
        return item

    items = pgstac_to_iter(
        pgstac_postgres, collection="naip", row_func=row_func, **kwargs
    )

    assert [type(item["geometry"]) for item in items] == [geometry_type] * 4
    assert seen == [geometry_type] * 4


def test_pgstac_to_iter_ignores_connection_row_factory(pgstac_postgres):
    def connect():
        return psycopg.connect(pgstac_postgres, row_factory=psycopg.rows.dict_row)

    items = list(pgstac_to_iter(connect))
    assert len(items) == 4
    assert all(item["geometry"]["type"] == "Polygon" for item in items)


@pytest.fixture
def reads(monkeypatch):
    """How each query's rows are read: streamed, or fetched from a server cursor."""
    calls = []
    stream = psycopg.Cursor.stream
    execute = psycopg.ServerCursor.execute

    def spy_stream(self, query, params=None, **kwargs):
        calls.append(("stream", kwargs["size"]))
        return stream(self, query, params, **kwargs)

    def spy_execute(self, query, params=None, **kwargs):
        calls.append(("fetch", self.itersize))
        return execute(self, query, params, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "stream", spy_stream)
    monkeypatch.setattr(psycopg.ServerCursor, "execute", spy_execute)
    return calls


needs_libpq_17 = pytest.mark.skipif(
    not psycopg.capabilities.has_stream_chunked(),
    reason="streams by default only with libpq 17",
)


@pytest.mark.parametrize(
    ("kwargs", "env", "expected"),
    [
        pytest.param({}, {}, [("stream", 100)], marks=needs_libpq_17),
        pytest.param({"cursor_itersize": 3}, {}, [("stream", 3)], marks=needs_libpq_17),
        ({"stream": False}, {}, [("fetch", 1000)]),
        pytest.param(
            {},
            {"STAC_GEOPARQUET_PGSTAC_CURSOR_ITERSIZE": "7"},
            [("stream", 7)],
            marks=needs_libpq_17,
        ),
        ({}, {"STAC_GEOPARQUET_PGSTAC_STREAM": "false"}, [("fetch", 1000)]),
    ],
)
def test_cursor_itersize(read_table, reads, monkeypatch, kwargs, env, expected):
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    table = read_table(**kwargs)

    assert table.num_rows == 4
    assert reads == expected


def test_before_libpq_17(read_table, reads, monkeypatch):
    monkeypatch.setattr(psycopg.capabilities, "has_stream_chunked", lambda: False)

    read_table()
    read_table(stream=True)

    assert reads == [("fetch", 1000), ("stream", 1)]


@pytest.mark.parametrize("stream", [True, False])
def test_client_cursor_factory(pgstac_postgres, stream):
    def connect():
        return psycopg.connect(pgstac_postgres, cursor_factory=psycopg.ClientCursor)

    items = list(
        pgstac_to_iter(connect, search={"collections": ["naip"]}, stream=stream)
    )

    assert len(items) == 4


def test_stopping_early_leaves_a_pooled_connection_usable(pgstac_postgres):
    with psycopg_pool.ConnectionPool(pgstac_postgres, min_size=1, max_size=1) as pool:
        items = pgstac_to_iter(pool.connection)
        next(items)
        items.close()

        with pool.connection() as conn:
            assert conn.execute("SELECT 1").fetchone() == (1,)


def test_prescan_with_row_func(read_table):
    def add_extra(item):
        item["properties"]["extra"] = "x"
        return item

    table = read_table(schema="PreScan", chunk_size=1, row_func=add_extra)

    assert table["extra"].to_pylist() == ["x"] * 4
    _assert_naip_geometries(table["id"].to_pylist(), table["geometry"].to_pylist())


def test_prescan_with_pystac_items(read_table):
    table = read_table(schema="PreScan", row_func=pystac.Item.from_dict)

    assert table.num_rows == 4


def test_prescan_types_null_properties(read_table):
    def null_epsg(item):
        item["properties"]["proj:epsg"] = None
        return item

    table = read_table(schema="PreScan", row_func=null_epsg)

    assert table.schema.field("proj:epsg").type == "int64"


def test_prescan_closes_its_connection(pgstac_postgres):
    connections = []

    def connect():
        connections.append(psycopg.connect(pgstac_postgres))
        return connections[-1]

    table = pgstac_to_arrow(connect, schema="PreScan").read_all()

    assert table.num_rows == 4
    # One connection to infer the schema, and one to read the items
    assert [conn.closed for conn in connections] == [True, True]


def test_pgstac_schema(pgstac_postgres, tmp_path):
    def add_extra(item):
        item["properties"]["extra"] = "x"
        return item

    schema = pgstac_reader.pgstac_schema(pgstac_postgres, row_func=add_extra)

    assert "extra" in schema.inner.field("properties").type.names
    path = pgstac_reader.pgstac_to_parquet(
        pgstac_postgres, tmp_path / "items.parquet", schema=schema, row_func=add_extra
    )
    assert pyarrow.parquet.read_schema(path).field("extra").type == "string"


def test_pgstac_schema_with_no_items(pgstac_postgres):
    with pytest.raises(ValueError, match="No items found"):
        pgstac_reader.pgstac_schema(pgstac_postgres, collection="missing")


@pytest.mark.parametrize(
    ("value", "structure"),
    [
        ({"a": 1, "b": [1.0, 2.0]}, (("a", int), ("b", frozenset({float})))),
        # The marker removes a key when pgstac hydrates the item
        ({"a": DO_NOT_MERGE_MARKER}, (("a", DO_NOT_MERGE_MARKER),)),
        # pgstac merges lists of dicts with its base item's only if they're as long
        ({"a": [{"b": 1}, {"b": 2}]}, (("a", ((("b", int),), (("b", int),))),)),
    ],
)
def test_structure(value, structure):
    assert pgstac_reader._structure(value) == structure


def test_prescan_across_base_items(pgstac_postgres, edited_collection):
    # The second and third items are stored alike, but hydrate differently
    schema = pgstac_reader.pgstac_schema(pgstac_postgres, collection=edited_collection)
    asset_type = schema.inner.field("assets").type.field("data").type
    assert "roles" in asset_type.names
    table = pgstac_to_arrow(
        pgstac_postgres, collection=edited_collection, schema="PreScan", chunk_size=1
    ).read_all()
    roles = [asset["data"]["roles"] for asset in table["assets"].to_pylist()]
    assert dict(zip(table["id"].to_pylist(), roles)) == {
        "before-edits": None,
        "second": None,
        "third": ["data"],
    }


def test_pgstac_schema_with_search(pgstac_postgres):
    schema = pgstac_reader.pgstac_schema(
        pgstac_postgres, search={"collections": ["naip"]}
    )

    assert "gsd" in schema.inner.field("properties").type.names


def test_prescan_with_2d_and_3d_bboxes(pgstac_postgres):
    collection_id = "mixed-bbox"
    naip_item = json.loads(NAIPDATA.read_text())[0]
    xmin, ymin, xmax, ymax = naip_item["bbox"]
    bboxes = {"2d": naip_item["bbox"], "3d": [xmin, ymin, 0.0, xmax, ymax, 10.0]}
    items = [
        {**naip_item, "id": item_id, "collection": collection_id, "bbox": bbox}
        for item_id, bbox in bboxes.items()
    ]
    with PgstacDB(pgstac_postgres) as db:
        Loader(db).load_collections([{"id": collection_id}])
        Loader(db).load_items(items, pypgstac.load.Methods.ignore)
    try:
        # Both in one chunk
        table = pgstac_to_arrow(
            pgstac_postgres, collection=collection_id, schema="PreScan"
        ).read_all()

        zmax = [bbox["zmax"] for bbox in table["bbox"].to_pylist()]
        assert dict(zip(table["id"].to_pylist(), zmax)) == {"2d": None, "3d": 10.0}
    finally:
        with psycopg.connect(pgstac_postgres, autocommit=True) as conn:
            conn.execute("SELECT delete_collection(%s)", (collection_id,))


def test_error_during_schema_scan_is_raised(pgstac_postgres, monkeypatch):
    def fail(value):
        raise RuntimeError("scan failed")

    monkeypatch.setattr(pgstac_reader, "_structure", fail)
    errors = []

    def scan():
        try:
            pgstac_reader.pgstac_schema(pgstac_postgres, stream=True)
        except RuntimeError as e:
            errors.append(e)

    # A paused stream holds the connection's lock, so the scan hangs unless it
    # closes the stream
    thread = threading.Thread(target=scan, daemon=True)
    thread.start()
    thread.join(30)

    assert errors


@pytest.fixture
def varied_collection(pgstac_postgres):
    collection_id = "varied-structure"
    templates = json.loads((HERE / "data" / "sentinel-2-l2a-pc.json").read_text())
    items = []
    for i in range(10):
        item = json.loads(json.dumps(templates[i % len(templates)]))
        item["id"] = f"item-{i}"
        item["collection"] = collection_id
        # The first item lacks a property that later ones have
        if i % 3 == 0:
            item["properties"].pop("eo:cloud_cover")
        elif i % 3 == 1:
            item["assets"].pop("AOT")
        items.append(item)
    with PgstacDB(pgstac_postgres) as db:
        Loader(db).load_collections([{"id": collection_id}])
        Loader(db).load_items(items, pypgstac.load.Methods.ignore)
    yield collection_id
    with psycopg.connect(pgstac_postgres, autocommit=True) as conn:
        conn.execute("SELECT delete_collection(%s)", (collection_id,))


@pytest.mark.parametrize(
    "collection_fixture", ["naip", "edited_collection", "varied_collection"]
)
def test_pgstac_schema_matches_every_item(pgstac_postgres, request, collection_fixture):
    collection_id = collection_fixture
    if collection_fixture != "naip":
        collection_id = request.getfixturevalue(collection_fixture)
    expected = InferredSchema()
    expected.update_from_items(
        pgstac_to_iter(pgstac_postgres, collection=collection_id)
    )

    schema = pgstac_reader.pgstac_schema(pgstac_postgres, collection=collection_id)

    assert schema.to_geoparquet().equals(expected.to_geoparquet())


@pytest.mark.parametrize(
    ("geometry", "points"), [("wkb", set()), ("geojson", {"Polygon"})]
)
def test_pgstac_schema_geometry(pgstac_postgres, geometry, points):
    seen = set()

    def row_func(item):
        if isinstance(item["geometry"], dict):
            seen.add(item["geometry"]["type"])
        else:
            assert shapely.from_wkb(item["geometry"]).is_empty
        return item

    pgstac_reader.pgstac_schema(
        pgstac_postgres, collection="naip", row_func=row_func, geometry=geometry
    )

    assert seen == points
