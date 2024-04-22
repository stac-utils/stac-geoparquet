import json

import pystac_client  # type: ignore


def download(collection: str):
    items = list(
        pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        .search(collections=collection, max_items=4)
        .items_as_dicts()
    )

    with open(f"{collection}-pc.json", "w") as f:
        json.dump(items, f, indent=2)


def main():
    download("3dep-lidar-copc")
    download("3dep-lidar-dsm")
    download("cop-dem-glo-30")
    download("io-lulc-annual-v02")
    download("io-lulc")
    download("landsat-c2-l1")
    download("landsat-c2-l2")
    download("naip")
    download("planet-nicfi-analytic")
    download("sentinel-1-rtc")
    download("sentinel-2-l2a")
    download("us-census")


if __name__ == "__main__":
    main()
