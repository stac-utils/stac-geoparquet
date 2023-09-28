# STAC GeoParquet Specification

## Overview

This document specifies how to map a set of [STAC Items](https://github.com/radiantearth/stac-spec/tree/v1.0.0/item-spec) into
[GeoParquet](https://geoparquet.org). It is directly inspired by the [STAC GeoParquet](https://github.com/stac-utils/stac-geoparquet)
library, but aims to provide guidance for anyone putting STAC data into GeoParquet. 

## Guidelines

Generally most all the fields in a STAC Item should be mapped to a column in GeoParquet. We embrace Parquet structures where possible, mapping
from JSON into nested structures. We do pull the properties to the top level, so that it is easier to query and use them. The names of
most of the fields should be the same in STAC and in GeoParquet.

| Field           | GeoParquet Type    | Required | Details                                            |
| --------------- | ------------------ | ---------|--------------------------------------------------- |
| type            | String             | Optional | This is just needed for GeoJSON, so it is optional and not recommended to include in GeoParquet |
| stac_extensions | List of Strings    | Required | This column is required, but can be empty if no STAC extensions were used |
| id              | String             | Required | Required, should be unique within each collection |
| geometry        | Binary (WKB)       | Required | For GeoParquet 1.0 this must be well-known Binary. |
| bbox 	          | Struct of Floats   | Required | Can be a 4 or 6 value struct, depending on dimension of the data |
| properties      | per field          | Required | Each property should use the relevant Parquet type, and be pulled out of the properties object to be a top-level Parquet field |
| links           | List of Link structs | Required | See [Link Struct](#link-struct) for more info |
| assets          | An Assets struct   | Required | See [Asset Struct](#asset-struct) for more info |
| collection      | String             | Required | The ID of the collection this Item is a part of |


* Must be valid GeoParquet, with proper metadata. Ideally the geometry types are defined and as narrow as possible.
* Strongly recommend to only have one GeoParquet per STAC 'Collection'. Not doing this will lead to an expanded GeoParquet schema (the union of all the schemas of the collection) with lots of empty data
* Any field in 'properties' should be moved up to be a top-level field in the GeoParquet. 
* STAC GeoParquet does not support properties that are named such that they collide with a top-level key.
* datetime columns should be stored as a native timestamp, not as a string
* The Collection JSON should be included in the Parquet metadata (TODO: flesh this out more)

### Link Struct

Each Link Struct has 2 required fields and 2 optional ones:

| Field Name | Type   | Description |
| ---------- | ------ | ----------- |
| href       | string | **REQUIRED.** The actual link in the format of an URL. Relative and absolute links are both allowed. |
| rel        | string | **REQUIRED.** Relationship between the current document and the linked document. See chapter "Relation types" for more information. |
| type       | string | [Media type](../catalog-spec/catalog-spec.md#media-types) of the referenced entity. |
| title      | string | A human readable title to be used in rendered displays of the link. |


### Asset Struct

TODO: Explain this more, and how it works best if it's just one collection.

Each struct has each full asset key and object as a sub-struct, it's a direct mapping from the JSON to Parquet

## Mapping to other geospatial data formats

The principles here can likely be used to map into other geospatial data formats (GeoPackage, FlatGeobuf, etc), but we embrace Parquet's nested 'structs' for some of the mappings, so other formats will need to do something different. The obvious thing to do is to dump JSON into those fields, but that's outside the scope of this document, and we recommend creating a general document for that.

## Use cases

* Provide a STAC GeoParquet that mirrors a static Collection as a way to query the whole dataset instead of reading every specific GeoJSON file.
* As an output format for STAC API responses that is more efficient than paging through thousands of pages of GeoJSON.