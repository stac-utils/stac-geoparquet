import os

# Before the tests import stac_geoparquet, which reads some of them then
for name in [name for name in os.environ if name.startswith("STAC_GEOPARQUET_")]:
    del os.environ[name]
