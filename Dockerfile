FROM mcr.microsoft.com/planetary-computer/python:2022.8.1.0
RUN /srv/conda/envs/notebook/bin/python3 -m pip install stac-geoparquet pypgstac psycopg[binary,pool] azure-data-tables
