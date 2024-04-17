### `naip-pc.json`

4 items from the NAIP STAC collection in planetary computer.

```py
import pystac_client
import json

items = list(
    pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    .search(collections="naip", max_items=4)
    .items_as_dicts()
)

with open('naip-pc.json', 'w') as f:
    json.dump(items, f, indent=2)
```
