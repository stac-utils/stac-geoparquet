import json

import pystac_client


def download(collection: str):
    items = list(
        pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        .search(collections=collection, max_items=4)
        .items_as_dicts()
    )

    with open(f"{collection}-pc.json", "w") as f:
        json.dump(items, f, indent=2)


def main():
    download("naip")
    download("3dep-lidar-dsm")


if __name__ == "__main__":
    main()
