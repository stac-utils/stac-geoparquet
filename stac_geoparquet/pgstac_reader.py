from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import hashlib
import itertools
import logging
import textwrap
from typing import Any

import dateutil.tz
import fsspec
import pandas as pd
import pyarrow.fs
import pypgstac.db
import pypgstac.hydration
import pystac
import shapely.wkb
import tqdm.auto

from stac_geoparquet import to_geodataframe

logger = logging.getLogger(__name__)


def _pairwise(
    iterable: collections.abc.Iterable,
) -> Any:
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


@dataclasses.dataclass
class CollectionConfig:
    """
    Additional collection-based configuration to inject, matching the
    dynamic properties from the API.
    """

    collection_id: str
    partition_frequency: str | None = None
    stac_api: str = "https://planetarycomputer.microsoft.com/api/stac/v1"
    should_inject_dynamic_properties: bool = True
    render_config: str | None = None

    def __post_init__(self) -> None:
        self._collection: pystac.Collection | None = None

    @property
    def collection(self) -> pystac.Collection:
        if self._collection is None:
            self._collection = pystac.read_file(
                f"{self.stac_api}/collections/{self.collection_id}"
            )  # type: ignore
        assert self._collection is not None
        return self._collection

    def inject_links(self, item: dict[str, Any]) -> None:
        item["links"] = [
            {
                "rel": "collection",
                "type": "application/json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection_id}",  # noqa: E501
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection_id}",  # noqa: E501
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection_id}/items/{item['id']}",  # noqa: E501
            },
            {
                "rel": "preview",
                "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection={self.collection_id}&item={item['id']}",  # noqa: E501
                "title": "Map of item",
                "type": "text/html",
            },
        ]

    def inject_assets(self, item: dict[str, Any]) -> None:
        item["assets"]["tilejson"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection={self.collection_id}&item={item['id']}&{self.render_config}",  # noqa: E501
            "roles": ["tiles"],
            "title": "TileJSON with default rendering",
            "type": "application/json",
        }
        item["assets"]["rendered_preview"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection={self.collection_id}&item={item['id']}&{self.render_config}",  # noqa: E501
            "rel": "preview",
            "roles": ["overview"],
            "title": "Rendered preview",
            "type": "image/png",
        }

    def generate_endpoints(
        self, since: datetime.datetime | None = None
    ) -> list[tuple[datetime.datetime, datetime.datetime]]:
        if self.partition_frequency is None:
            raise ValueError("Set partition_frequency")

        start_datetime, end_datetime = self.collection.extent.temporal.intervals[0]

        # https://github.com/dateutil/dateutil/issues/349
        if start_datetime and start_datetime.tzinfo == dateutil.tz.tz.tzlocal():
            start_datetime = start_datetime.astimezone(datetime.timezone.utc)

        if end_datetime and end_datetime.tzinfo == dateutil.tz.tz.tzlocal():
            end_datetime = end_datetime.astimezone(datetime.timezone.utc)

        if end_datetime is None:
            end_datetime = pd.Timestamp.utcnow()

        # we need to ensure that the `end_datetime` is past the end of the last partition
        # to avoid missing out on the last partition of data.
        offset = pd.tseries.frequencies.to_offset(self.partition_frequency)

        if not offset.is_on_offset(start_datetime):
            start_datetime = start_datetime - offset

        if not offset.is_on_offset(end_datetime):
            end_datetime = end_datetime + offset

        idx = pd.date_range(start_datetime, end_datetime, freq=self.partition_frequency)

        if since:
            idx = idx[idx >= since]

        pairs = _pairwise(idx)
        return list(pairs)

    def export_partition(
        self,
        conninfo: str,
        query: str,
        output_protocol: str,
        output_path: str,
        storage_options: dict[str, Any] | None = None,
        rewrite: bool = False,
        skip_empty_partitions: bool = False,
    ) -> str | None:
        storage_options = storage_options or {}
        az_fs = fsspec.filesystem(output_protocol, **storage_options)
        if az_fs.exists(output_path) and not rewrite:
            logger.debug("Path %s already exists.", output_path)
            return output_path

        db = pypgstac.db.PgstacDB(conninfo)
        with db:
            assert db.connection is not None
            db.connection.execute("set statement_timeout = 300000;")
            # logger.debug("Reading base item")
            # TODO: proper escaping
            base_item = db.query_one(
                f"select * from collection_base_item('{self.collection_id}');"
            )
            records = list(db.query(query))

        if skip_empty_partitions and len(records) == 0:
            logger.debug("No records found for query %s.", query)
            return None

        items = self.make_pgstac_items(records, base_item)  # type: ignore[arg-type]
        df = to_geodataframe(items)
        filesystem = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(az_fs))
        df.to_parquet(output_path, index=False, filesystem=filesystem)
        return output_path

    def export_partition_for_endpoints(
        self,
        endpoints: tuple[datetime.datetime, datetime.datetime],
        conninfo: str,
        output_protocol: str,
        output_path: str,
        storage_options: dict[str, Any],
        part_number: int | None = None,
        total: int | None = None,
        rewrite: bool = False,
        skip_empty_partitions: bool = False,
    ) -> str | None:
        """
        Export results for a pair of endpoints.
        """
        a, b = endpoints
        base_query = textwrap.dedent(
            f"""\
        select *
        from pgstac.items
        where collection = '{self.collection_id}'
        """
        )

        query = (
            base_query
            + f"and datetime >= '{a.isoformat()}' and datetime < '{b.isoformat()}'"
        )

        partition_path = _build_output_path(output_path, part_number, total, a, b)
        return self.export_partition(
            conninfo,
            query,
            output_protocol=output_protocol,
            output_path=partition_path,
            storage_options=storage_options,
            rewrite=rewrite,
            skip_empty_partitions=skip_empty_partitions,
        )

    def export_collection(
        self,
        conninfo: str,
        output_protocol: str,
        output_path: str,
        storage_options: dict[str, Any],
        rewrite: bool = False,
        skip_empty_partitions: bool = False,
    ) -> list[str | None]:
        base_query = textwrap.dedent(
            f"""\
        select *
        from pgstac.items
        where collection = '{self.collection_id}'
        """
        )
        if output_protocol:
            output_path = f"{output_protocol}://{output_path}"

        if not self.partition_frequency:
            logger.info("Exporting single-partition collection %s", self.collection_id)
            logger.debug("query=%s", base_query)
            results = [
                self.export_partition(
                    conninfo,
                    base_query,
                    output_protocol,
                    output_path,
                    storage_options=storage_options,
                    rewrite=rewrite,
                )
            ]

        else:
            endpoints = self.generate_endpoints()
            total = len(endpoints)
            logger.info(
                "Exporting %d partitions for collection %s", total, self.collection_id
            )

            results = []
            for i, endpoint in tqdm.auto.tqdm(enumerate(endpoints), total=total):
                results.append(
                    self.export_partition_for_endpoints(
                        endpoints=endpoint,
                        conninfo=conninfo,
                        output_protocol=output_protocol,
                        output_path=output_path,
                        storage_options=storage_options,
                        rewrite=rewrite,
                        skip_empty_partitions=skip_empty_partitions,
                        part_number=i,
                        total=total,
                    )
                )

        return results

    def make_pgstac_items(
        self,
        records: list[
            tuple[str, str, str, datetime.datetime, datetime.datetime, dict[str, Any]]
        ],
        base_item: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Make STAC items out of pgstac records.

        Args:
            records: list[tuple]
                The dehydrated records from pgstac.items table.
            base_item: dict[str, Any]
                The base item from the ``collection_base_item`` pgstac function for this
                collection. Used for rehydration
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
            content = item.pop("content")
            assert isinstance(content, dict)
            if "bbox" in content:
                item["bbox"] = content["bbox"]
            else:
                item["bbox"] = list(geom.bounds)

            item["assets"] = content["assets"]
            if "stac_extensions" in content:
                item["stac_extensions"] = content["stac_extensions"]
            item["properties"] = content["properties"]

            pypgstac.hydration.hydrate(base_item, item)

            if self.should_inject_dynamic_properties:
                self.inject_links(item)
                self.inject_assets(item)

            items.append(item)

        return items


def _build_output_path(
    base_output_path: str,
    part_number: int | None,
    total: int | None,
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
) -> str:
    a, b = start_datetime, end_datetime
    base_output_path = base_output_path.rstrip("/")

    if part_number is not None and total is not None:
        output_path = (
            f"{base_output_path}/part-{part_number:0{len(str(total * 10))}}_"
            f"{a.isoformat()}_{b.isoformat()}.parquet"
        )
    else:
        token = hashlib.md5(
            "".join([a.isoformat(), b.isoformat()]).encode()
        ).hexdigest()
        output_path = (
            f"{base_output_path}/part-{token}_{a.isoformat()}_{b.isoformat()}.parquet"
        )
    return output_path
