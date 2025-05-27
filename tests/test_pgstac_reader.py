import json
import pathlib

import docker
import pypgstac
import pytest
from pypgstac.db import PgstacDB
from pypgstac.load import Loader

from stac_geoparquet.pgstac_reader import (
    pgstac_dsn,
    pgstac_to_arrow,
    pgstac_to_iter,
)

HERE = pathlib.Path(__file__).parent

DOCKERIMG = "ghcr.io/stac-utils/pgstac:latest"
NAIPDATA = HERE / "data" / "naip-pc.json"


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
            assert db.version >= "0.3.4"
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
    for item in items:
        assert item["collection"] == "naip"
        assert item["geometry"]["type"] == "Polygon"


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
