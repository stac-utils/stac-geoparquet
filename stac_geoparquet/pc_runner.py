import json

import azure.data.tables
import requests
import urllib.parse
from stac_geoparquet.pgstac_reader import CollectionConfig

PARTITION_FREQUENCIES = {
    "3dep-lidar-classification": "AS",
    "3dep-lidar-copc": "AS",
    "3dep-lidar-dsm": "AS",
    "3dep-lidar-dtm": "AS",
    "3dep-lidar-dtm-native": "AS",
    "3dep-lidar-hag": "AS",
    "3dep-lidar-intensity": "AS",
    "3dep-lidar-pointsourceid": "AS",
    "3dep-lidar-returns": "AS",
    "3dep-seamless": None,
    "alos-dem": None,
    "alos-fnf-mosaic": "AS",
    "alos-palsar-mosaic": "AS",
    "aster-l1t": "AS",
    "chloris-biomass": None,
    "cil-gdpcir-cc-by": None,
    "cil-gdpcir-cc-by-sa": None,
    "cil-gdpcir-cc0": None,
    "cop-dem-glo-30": None,
    "cop-dem-glo-90": None,
    "eclipse": None,
    "ecmwf-forecast": "MS",
    "era5-pds": None,
    "esa-worldcover": None,
    "fia": None,
    "gap": None,
    "gbif": None,
    "gnatsgo-rasters": None,
    "gnatsgo-tables": None,
    "goes-cmi": "W-MON",
    "hrea": None,
    "io-lulc": None,
    "io-lulc-9-class": None,
    "jrc-gsw": None,
    "landsat-c2-l1": "MS",
    "landsat-c2-l2": "MS",
    "mobi": None,
    "modis-09A1-061": "MS",
    "modis-09Q1-061": "MS",
    "modis-10A1-061": "MS",
    "modis-10A2-061": "MS",
    "modis-11A1-061": "MS",
    "modis-11A2-061": "MS",
    "modis-13A1-061": "MS",
    "modis-13Q1-061": "MS",
    "modis-14A1-061": "MS",
    "modis-14A2-061": "MS",
    "modis-15A2H-061": "MS",
    "modis-15A3H-061": "MS",
    "modis-16A3GF-061": "MS",
    "modis-17A2H-061": "MS",
    "modis-17A2HGF-061": "MS",
    "modis-17A3HGF-061": "MS",
    "modis-21A2-061": "MS",
    "modis-43A4-061": "MS",
    "modis-64A1-061": "MS",
    "mtbs": None,
    "naip": "AS",
    "nasa-nex-gddp-cmip6": None,
    "nasadem": None,
    "noaa-c-cap": None,
    "nrcan-landcover": None,
    "planet-nicfi-analytic": "AS",
    "planet-nicfi-visual": "AS",
    "sentinel-1-grd": "MS",
    "sentinel-1-rtc": "MS",
    "sentinel-2-l2a": "MS",
    "us-census": None,
}


def build_render_config(render_params, assets):
    flat = []
    if assets:
        for asset in assets:
            flat.append(("assets", asset))

    for k, v in render_params.items():
        if isinstance(v, list):
            flat.extend([(k, v2) for v2 in v])
        else:
            flat.append((k, v))
    return urllib.parse.urlencode(flat)


def generate_configs_from_storage_table(table_client: azure.data.tables.TableClient):
    configs = {}
    for entity in table_client.list_entities():
        collection_id = entity["RowKey"]
        data = json.loads(entity["Data"])

        render_params = data["render_config"]["render_params"]
        assets = data["render_config"]["assets"]
        render_config = build_render_config(render_params, assets)
        configs[collection_id] = CollectionConfig(
            collection_id, render_config=render_config
        )

    return configs


def generate_configs_from_api(url):
    configs = {}
    r = requests.get(url)
    r.raise_for_status()

    for collection in r.json()["collections"]:
        configs[collection["id"]] = CollectionConfig(collection["id"])

    return configs


def get_configs(table_client):
    table_configs = generate_configs_from_storage_table(table_client)
    api_configs = generate_configs_from_api(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections"
    )

    configs = {**api_configs, **table_configs}

    for k, v in configs.items():
        v.partition_frequency = PARTITION_FREQUENCIES.get(k)

    return configs
