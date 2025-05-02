from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import hashlib
import itertools
import logging
import textwrap
from typing import Any, Literal

import dateutil.tz
import fsspec
import orjson
import pandas as pd
import pypgstac.db
import pypgstac.hydration
import pystac
import shapely.wkb
import tqdm.auto
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

from stac_geoparquet.arrow import parse_stac_ndjson_to_parquet

logger = logging.getLogger(__name__)

EXPORT_FORMAT = Literal["geoparquet", "ndjson"]


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
        format: EXPORT_FORMAT = "geoparquet",
    ) -> str | None:
        storage_options = storage_options or {}
        fs = fsspec.filesystem(output_protocol, **storage_options)

        base_item, records = _enumerate_db_items(self.collection_id, conninfo, query)

        if skip_empty_partitions and len(records) == 0:
            logger.debug("No records found for query %s.", query)
            return None

        items = self.make_pgstac_items(records, base_item)  # type: ignore[arg-type]

        logger.debug("Exporting %d items as %s to %s", len(items), format, output_path)
        _write_ndjson(output_path, fs, items)
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
        format: EXPORT_FORMAT = "geoparquet",
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

        partition_path = _build_output_path(
            output_path, part_number, total, a, b, format=format
        )
        return self.export_partition(
            conninfo,
            query,
            output_protocol=output_protocol,
            output_path=partition_path,
            storage_options=storage_options,
            rewrite=rewrite,
            skip_empty_partitions=skip_empty_partitions,
            format=format,
        )

    def export_collection(
        self,
        conninfo: str,
        output_protocol: str,
        output_path: str,
        storage_options: dict[str, Any],
        rewrite: bool = False,
        skip_empty_partitions: bool = False,
        format: EXPORT_FORMAT = "geoparquet",
    ) -> list[str | None]:
        base_query = textwrap.dedent(
            f"""\
        select *
        from pgstac.items
        where collection = '{self.collection_id}'
        """
        )

        intermediate_path = f"/tmp/{self.collection_id}.ndjson"
        results: list[str | None] = []
        if not self.partition_frequency:
            logger.info(
                "Exporting single-partition collection %s to ndjson", self.collection_id
            )
            logger.debug("query=%s", base_query)
            # First write NDJSON to disk
            self.export_partition(
                conninfo,
                base_query,
                "file",
                intermediate_path,
                storage_options={"auto_mkdir": True},
                rewrite=rewrite,
                format="ndjson",
            )
            if output_protocol:
                output_path = f"{output_protocol}://{output_path}.parquet"
            logger.debug("Writing geoparquet to %s", output_path)
            results.append(intermediate_path)
            parse_stac_ndjson_to_parquet(
                results,
                output_path,
                filesystem=fsspec.filesystem(output_protocol, **storage_options),
            )
        else:
            endpoints = self.generate_endpoints()
            total = len(endpoints)
            if output_protocol:
                output_path = f"{output_protocol}://{output_path}.parquet"
            logger.info(
                "Exporting %d partitions for collection %s", total, self.collection_id
            )
            for i, endpoint in tqdm.auto.tqdm(enumerate(endpoints), total=total):
                partition = self.export_partition_for_endpoints(
                    endpoints=endpoint,
                    conninfo=conninfo,
                    output_protocol="file",
                    output_path=intermediate_path,
                    storage_options={"auto_mkdir": True},
                    rewrite=rewrite,
                    skip_empty_partitions=skip_empty_partitions,
                    part_number=i,
                    total=total,
                    format="ndjson",
                )
                if partition:
                    results.append(partition)
                    partition_path = _build_output_path(
                        output_path,
                        i,
                        total,
                        endpoint[0],
                        endpoint[1],
                        format="geoparquet",
                    )
                    logger.debug("Writing geoparquet to %s", partition_path)
                    parse_stac_ndjson_to_parquet(
                        partition,
                        partition_path,
                        filesystem=fsspec.filesystem(
                            output_protocol, **storage_options
                        ),
                    )

        # delete every file in the results list
        for result in results:
            logger.debug("Cleaning up %s", result)
            fsspec.filesystem("file").rm(result, recursive=True)

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
    format: EXPORT_FORMAT = "geoparquet",
) -> str:
    a, b = start_datetime, end_datetime
    base_output_path = base_output_path.rstrip("/")
    file_extensions = {
        "geoparquet": "parquet",
        "ndjson": "ndjson",
    }

    if part_number is not None and total is not None:
        output_path = (
            f"{base_output_path}/part-{part_number:0{len(str(total * 10))}}_"
            f"{a.isoformat()}_{b.isoformat()}.{file_extensions[format]}"
        )
    else:
        token = hashlib.md5(
            "".join([a.isoformat(), b.isoformat()]).encode()
        ).hexdigest()
        output_path = f"{base_output_path}/part-{token}_{a.isoformat()}_{b.isoformat()}.{file_extensions[format]}"
    return output_path


@retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(logger, logging.DEBUG))
def _enumerate_db_items(
    collection_id: str, conninfo: str, query: str
) -> tuple[Any, list[Any]]:
    db = pypgstac.db.PgstacDB(conninfo)
    with db:
        assert db.connection is not None
        db.connection.execute("set statement_timeout = 300000;")
        # logger.debug("Reading base item")
        # TODO: proper escaping
        base_item = db.query_one(
            f"select * from collection_base_item('{collection_id}');"
        )
        records = list(db.query(query))
    return base_item, records


def _write_ndjson(
    output_path: str, fs: fsspec.AbstractFileSystem, items: list[dict]
) -> None:
    with fs.open(output_path, "wb") as f:
        for item in items:
            f.write(orjson.dumps(item))
            f.write(b"\n")
