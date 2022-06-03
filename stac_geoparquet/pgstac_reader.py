from __future__ import annotations
import textwrap

import datetime
import logging
from typing import Any
import urllib.parse
import itertools
import pandas as pd

import fsspec
import requests
import pystac
import dataclasses
import pypgstac.hydration
import shapely.wkb

from stac_geoparquet import to_geodataframe


logger = logging.getLogger(__name__)


def _pairwise(iterable):
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


@dataclasses.dataclass
class CollectionConfig:
    """
    Additional collection-based configuration to inject, matching the dynamic properties from the API.
    """
    collection_id: str
    partition_frequency: str | None = None
    stac_api: str = "https://planetarycomputer.microsoft.com/api/stac/v1"
    should_inject_dynamic_properties: bool = True

    def __post_init__(self):
        self._render_config: str | None = None
        self._collection: pystac.Collection | None = None

    @property
    def collection(self):
        if self._collection is None:
            self._collection = pystac.read_file(
                f"{self.stac_api}/collections/{self.collection_id}"
            )
        return self._collection

    @property
    def render_config(self) -> str:
        if self._render_config is None:
            r = requests.get(
                f"https://planetarycomputer.microsoft.com/api/data/v1/mosaic/info?collection={self.collection_id}"
            )

            r.raise_for_status()
            options = r.json()["renderOptions"][0]["options"].split("&")

            d = {}
            for k, v in [x.split("=") for x in options]:
                d[k] = v
            self._render_config = urllib.parse.urlencode(d)

        return self._render_config

    def inject_links(self, item):
        item["links"] = [
            {
                "rel": "collection",
                "type": "application/json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection_id}",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection_id}",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection_id}/items/{item['id']}",
            },
            {
                "rel": "preview",
                "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection={self.collection_id}&item={item['id']}",
                "title": "Map of item",
                "type": "text/html",
            },
        ]

    def inject_assets(self, item):
        item["assets"]["tilejson"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection={self.collection_id}&item={item['id']}&{self.render_config}",
            "roles": ["tiles"],
            "title": "TileJSON with default rendering",
            "type": "application/json",
        }
        item["assets"]["rendered_preview"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection={self.collection_id}&item={item['id']}&{self.render_config}",
            "rel": "preview",
            "roles": ["overview"],
            "title": "Rendered preview",
            "type": "image/png",
        }

    def generate_endpoints(self) -> list[tuple[datetime.datetime, datetime.datetime]]:
        if self.partition_frequency is None:
            raise ValueError("Set partition_frequency")

        start_datetime, end_datetime = self.collection.extent.temporal.intervals[0]
        if end_datetime is None:
            end_datetime = pd.Timestamp.utcnow()

        idx = pd.date_range(start_datetime, end_datetime, freq=self.partition_frequency)
        pairs = _pairwise(idx)
        return list(pairs)

    def export_partition(
        self,
        conninfo: str,
        query: str,
        output_protocol: str,
        output_path: str,
        to_parquet_options: dict[str, Any] | None = None,
        rewrite=False,
    ) -> str:
        to_parquet_options = to_parquet_options or {}

        fs = fsspec.filesystem(output_protocol, **to_parquet_options.get("storage_options", {}))
        if fs.exists(output_path) and not rewrite:
            logger.info("Path %s already exists.", output_path)
            return output_path

        db = pypgstac.db.PgstacDB(conninfo)
        with db.connect():
            # logger.debug("Reading base item")
            # TODO: proper escaping
            base_item = db.query_one(
                f"select * from collection_base_item('{self.collection_id}');"
            )
            records = list(db.query(query))

        items = self.make_pgstac_items(records, base_item)
        df = to_geodataframe(items)
        df.to_parquet(output_path, **to_parquet_options)

    def export_collection(
        self,
        conninfo: str,
        output_protocol: str,
        output_path: str,
        storage_options: dict[str, Any],
    ) -> list[str]:
        base_query = textwrap.dedent(
            f"""\
        select *
        from pgstac.items
        where collection = '{self.collection_id}'
        """
        )
        if output_protocol:
            output_path = f"{output_protocol}://{output_path}"
            to_parquet_options = dict(storage_options=storage_options)
        else:
            to_parquet_options = {}

        if not self.partition_frequency:
            results = [
                self.export_partition(
                    conninfo,
                    base_query,
                    output_protocol,
                    output_path,
                    to_parquet_options=to_parquet_options,
                )
            ]

        else:
            endpoints = self.generate_endpoints()
            extra_wheres = [
                f"and datetime >= '{a.isoformat()}' and datetime < '{b.isoformat()}'" for a, b in endpoints
            ]
            queries = [base_query + where for where in extra_wheres]
            output_paths = [
                f"{output_path}/part-{i}_{a.isoformat()}_{b.isoformat()}.parquet"
                for i, (a, b) in enumerate(endpoints)
            ]

            N = len(endpoints)

            results = []
            for i, (query, part_path) in enumerate(zip(queries, output_paths), 1):
                logger.info("Processing query %d/%d", i, N)
                results.append(
                    self.export_partition(
                        conninfo,
                        query,
                        output_protocol,
                        part_path,
                        to_parquet_options,
                    )
                )

        return results

    def make_pgstac_items(self, records, base_item):
        """
        Make STAC items out of pgstac records.

        Parameters
        ----------
        records: list[tuple]
            The dehydrated records from pgstac.items table.
        base_item: dict[str, Any]
            The base item from the ``collection_base_item`` pgstac function for this collection.
            Used for rehydration
        cfg: CollectionConfig
            The :class:`CollectionConfig` used for injecting dynamic properties.
        """
        columns = [
            "id",
            "geometry",
            "collection",
            "datetime",
            "end_datetime",
            "content",
        ]

        items = []

        for record in records:
            item = dict(zip(columns, record))
            # datetime is in the content too
            item.pop("datetime")
            item.pop("end_datetime")

            geom = shapely.wkb.loads(item["geometry"], hex=True)

            item["geometry"] = geom.__geo_interface__
            item["bbox"] = list(geom.bounds)
            content = item.pop("content")

            item["assets"] = content["assets"]
            item["stac_extensions"] = content["stac_extensions"]
            item["properties"] = content["properties"]

            pypgstac.hydration.hydrate(base_item, item)

            if self.should_inject_dynamic_properties:
                self.inject_links(item)
                self.inject_assets(item)

            items.append(item)

        return items
