import gc
import json
import pathlib
import weakref
from datetime import datetime

import docker
import psycopg
import pyarrow.fs
import pyarrow.parquet
import pypgstac
import pytest
from packaging.version import Version
from pypgstac.db import PgstacDB
from pypgstac.load import Loader

from stac_geoparquet import pgstac_reader
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
