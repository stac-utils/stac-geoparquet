FROM mcr.microsoft.com/planetary-computer/python:2022.10.31.0
RUN /srv/conda/envs/notebook/bin/python3 -m pip install git+https://github.com/stac-utils/stac-geoparquet pypgstac psycopg[binary,pool] azure-data-tables
