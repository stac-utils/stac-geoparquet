from __future__ import annotations

from typing import Any

import dataclasses
import pypgstac.hydration
import shapely.wkb


@dataclasses.dataclass
class CollectionConfig:
    collection: str
    render_config: str

    def inject_links(self, item):
        item["links"] = [
            {
                "rel": "collection",
                "type": "application/json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection}",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection}",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{self.collection}/items/{item['id']}",
            },
            {
                "rel": "preview",
                "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection={self.collection}&item={item['id']}",
                "title": "Map of item",
                "type": "text/html",
            },
        ]
 
    def inject_assets(self, item):
        item["assets"]["tilejson"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection={self.collection}&item={item['id']}&{self.render_config}",
            "roles": ["tiles"],
            "title": "TileJSON with default rendering",
            "type": "application/json",
        }
        item["assets"]["rendered_preview"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection={self.collection}&item={item['id']}&{self.render_config}",
            "rel": "preview",
            "roles": ["overview"],
            "title": "Rendered preview",
            "type": "image/png",
        }


def make_pgstac_items(records: list[tuple], base_item: dict[str, Any], cfg: CollectionConfig | None):
    columns = ["id", "geometry", "collection", "datetime", "end_datetime", "content"]

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

        cfg.inject_links(item)
        cfg.inject_assets(item)

        items.append(item)

    return items
