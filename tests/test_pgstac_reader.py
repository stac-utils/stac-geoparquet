import json
import pathlib
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
