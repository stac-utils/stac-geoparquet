from pyproj import CRS

WGS84_CRS_JSON = CRS.from_epsg(4326).to_json_dict()
