# STAC GeoParquet Specification

## Overview

This document specifies how to map a set of [STAC Items](https://github.com/radiantearth/stac-spec/tree/v1.0.0/item-spec) into
[GeoParquet](https://geoparquet.org). It is directly inspired by the [STAC GeoParquet](https://github.com/stac-utils/stac-geoparquet)
library, but aims to provide guidance for anyone putting STAC data into GeoParquet. 

## Guidelines

Generally most all the fields in a STAC Item should be mapped to a row in GeoParquet. We embrace Parquet structures where possible, mapping
from JSON into nested structures. We do pull the properties to the top level, so that it is easier to query and use them. The names of the
most of the fields should be the same in STAC and in GeoParquet.

| Field           | GeoParquet Type    | Required | Details                                            |
| --------------- | ----------------   | ---------|--------------------------------------------------- |
| type            | String             | Optional | This is just needed for GeoJSON, so it is optional and not recommended to include in GeoParquet |
| stac_extensions | List of Strings    | Required | This column is required, but can be blank if no STAC extensions were used |
| id              | String             | Required | Required, should be unique |
| geometry        | Binary (WKB)       | Required | For GeoParquet 1.0 this must be well-known Binary. |
| bbox 		      | List of Decimals   | Required | Can be 4 or 6 decimals, so won't be a fixed size list. |
| properties      | per field          | Required | Each property should use the relevant Parquet type, and be pulled out of the properties object to be a top-level Parquet field |
| links           | List of structs    | Required | Each struct in the array should have Strings of `href`, `rel` and `type` |
| assets          | A struct of assets | Required | Each struct has each full asset key and object as a sub-struct, it's a direct mapping from the JSON to Parquet |
| collection      | String             | Required | The ID of the collection this Item is a part of |


* Must be valid GeoParquet, with proper metadata. Ideally the geometry types are defined and as narrow as possible.
* Strongly recommend to only have one GeoParquet per STAC 'Collection'. Not doing this will lead to an expanded GeoParquet schema (the union of all the schemas of the collection) with lots of empty data
* Any field in 'properties' should be moved up to be a top-level field in the GeoParquet.

## Mapping to other geospatial data formats

The principles here can likely be used to map into other geospatial data formats (GeoPackage, FlatGeobuf, etc), but we embrace Parquet's nested 'structs' for some of the mappings, so other formats will need to do something different. The obvious thing to do is to dump JSON into those fields, but that's outside the scope of this document, and we recommend creating a general document for that.

## Use cases

* Provide a STAC GeoParquet that mirrors a static Collection as a way to query the whole dataset instead of reading every specific GeoJSON file.
* As an output format for STAC API responses that is more efficient than paging through thousands of pages of GeoJSON.