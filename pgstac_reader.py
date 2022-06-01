import pypgstac.hydration
import shapely.wkb


def make_pgstac_items(records, base_item):
    columns = ["id", "geometry", "collection", "datetime", "end_datetime", "content"]

    items = []
    for record in records:
        item = dict(zip(columns, record))
        geom = shapely.wkb.loads(item["geometry"], hex=True)

        item["geometry"] = geom.__geo_interface__
        item["bbox"] = list(geom.bounds)
        content = item.pop("content")

        item["assets"] = content["assets"]
        item["stac_extensions"] = content["stac_extensions"]
        item["properties"] = content["properties"]

        pypgstac.hydration.hydrate(base_item, item)

        item.pop("datetime")  # ?
        item.pop("end_datetime")

        item["links"] = [
            {
                "rel": "collection",
                "type": "application/json",
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/{item['id']}",
            },
            {
                "rel": "preview",
                "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection=naip&item={item['id']}",
                "title": "Map of item",
                "type": "text/html",
            },
        ]
        items.append(item)

        item["assets"]["tilejson"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection=naip&item={item['id']}&assets=image&asset_bidx=image%7C1%2C2%2C3",
            "roles": ["tiles"],
            "title": "TileJSON with default rendering",
            "type": "application/json",
        }

        item["assets"]["rendered_preview"] = {
            "href": f"https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection=naip&item={item['id']}&assets=image&asset_bidx=image%7C1%2C2%2C3",
            "rel": "preview",
            "roles": ["overview"],
            "title": "Rendered preview",
            "type": "image/png",
        }

    return items


# with db.connect():
#     base_item = db.query_one("select * from collection_base_item('naip');")
#     records = list(db.query("select * from pgstac.items where collection = 'naip' limit 100;"))
