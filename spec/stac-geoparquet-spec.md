# STAC GeoParquet Specification

## Overview

This document specifies how to map a set of [STAC Items](https://github.com/radiantearth/stac-spec/tree/v1.0.0/item-spec) into
[GeoParquet](https://geoparquet.org). It is directly inspired by the [STAC GeoParquet](https://github.com/stac-utils/stac-geoparquet)
library, but aims to provide guidance for anyone putting STAC data into GeoParquet.

## Use cases

- Provide a STAC GeoParquet that mirrors a static Collection as a way to query the whole dataset instead of reading every specific GeoJSON file.
- As an output format for STAC API responses that is more efficient than paging through thousands of pages of GeoJSON.
- Provide efficient access to specific fields of a STAC item, thanks to Parquet's columnar format.

## Guidelines

Each row in the Parquet Dataset represents a single STAC item. Most all the fields in a STAC Item should be mapped to a column in GeoParquet. We embrace Parquet structures where possible, mapping
from JSON into nested structures. We do pull the properties to the top level, so that it is easier to query and use them. The names of
most of the fields should be the same in STAC and in GeoParquet.

| Field              | GeoParquet Type      | Required | Details                                                                                                                                                                                                                                                 |
| ------------------ | -------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| type               | String               | Optional | This is just needed for GeoJSON, so it is optional and not recommended to include in GeoParquet                                                                                                                                                         |
| stac_extensions    | List of Strings      | Required | This column is required, but can be empty if no STAC extensions were used                                                                                                                                                                               |
| id                 | String               | Required | Required, should be unique within each collection                                                                                                                                                                                                       |
| geometry           | Binary (WKB)         | Required | For GeoParquet 1.0 this must be well-known Binary                                                                                                                                                                                                       |
| bbox               | Struct of Floats     | Required | Can be a 4 or 6 value struct, depending on dimension of the data. It must conform to the ["Bounding Box Columns"](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md#bounding-box-columns) definition of GeoParquet 1.1. |
| links              | List of Link structs | Required | See [Link Struct](#link-struct) for more info                                                                                                                                                                                                           |
| assets             | An Assets struct     | Required | See [Asset Struct](#asset-struct) for more info                                                                                                                                                                                                         |
| collection         | String               | Optional | The ID of the collection this Item is a part of. See notes below on 'Collection' and 'Collection JSON' in the Parquet metadata                                                                                                                          |
| _property columns_ | _varies_             | -        | Each property should use the relevant Parquet type, and be pulled out of the properties object to be a top-level Parquet field                                                                                                                          |

- Must be valid GeoParquet, with proper metadata. Ideally the geometry types are defined and as narrow as possible.
- Strongly recommend storing items that are mostly homogeneous (i.e. have the same fields). Parquet is a columnar format; storing items with many different fields will lead to an expanded parquet Schema with lots of empty data. In practice, this means storing a single collection or only collections with very similar item properties in a single stac-geoparquet dataset.
- Any field in 'properties' of the STAC item should be moved up to be a top-level field in the GeoParquet.
- STAC GeoParquet does not support properties that are named such that they collide with a top-level key.
- datetime columns should be stored as a [native timestamp][timestamp], not as a string
- The Collection JSON objects should be included in the Parquet metadata. See [Collection JSON](#stac-collection-objects) below.
- Any other properties that would be stored as GeoJSON in a STAC JSON Item (e.g. `proj:geometry`) should be stored as a binary column with WKB encoding. This simplifies the handling of collections with multiple geometry types.

### Link Struct

The GeoParquet dataset can contain zero or more Link Structs. Each Link Struct has 2 required fields and 2 optional ones:

| Field Name | Type   | Description                                                                                                                         |
| ---------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| href       | string | **REQUIRED.** The actual link in the format of an URL. Relative and absolute links are both allowed.                                |
| rel        | string | **REQUIRED.** Relationship between the current document and the linked document. See chapter "Relation types" for more information. |
| type       | string | [Media type][media-type] of the referenced entity.                                                                                  |
| title      | string | A human readable title to be used in rendered displays of the link.                                                                 |

See [Link Object][link] for more.

### Asset Struct

The GeoParquet dataset can contain zero or more Asset Structs. Each Asset Struct can have the following fields:

| Field Name  | Type      | Description                                                                                                                                                                                  |
| ----------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| href        | string    | **REQUIRED.** URI to the asset object. Relative and absolute URI are both allowed.                                                                                                           |
| title       | string    | The displayed title for clients and users.                                                                                                                                                   |
| description | string    | A description of the Asset providing additional details, such as how it was processed or created. [CommonMark 0.29](http://commonmark.org/) syntax MAY be used for rich text representation. |
| type        | string    | [Media type][media-type] of the asset. See the [common media types][common-media-types] in the best practice doc for commonly used asset types.                                              |
| roles       | \[string] | The [semantic roles][asset-roles] of the asset, similar to the use of `rel` in links.                                                                                                        |

Each struct has each full asset key and object as a sub-struct, it's a direct mapping from the JSON to Parquet

To take advantage of Parquet's columnar nature and compression, the assets should be uniform so they can be represented by a simple schema, which in turn means every item should probably come from the same STAC collection.

See [Asset Object][asset] for more.

### Parquet Metadata

stac-geoparquet uses Parquet [File Metadata](https://parquet.apache.org/docs/file-format/metadata/) to store metadata about the dataset.
All stac-geoparquet metadata is stored under the key `stac-geoparquet` in the parquet file metadata.

See [`example-metadata.json`](https://github.com/stac-utils/stac-geoparquet/blob/main/spec/example-metadata.json) for an example.

A [jsonschema schema file][schema] is provided for tools to validate against.
Note that the json-schema for stac-geoparquet does *not* validate the
`collection` object against the STAC json-schema. You'll need to validate that
separately.


| Field Name    | Type                    | Description                                                                                |
| --------------| ------------------------| ------------------------------------------------------------------------------------------ |
| `version`     | string                  | The stac-geoparquet metadata version. The stac-geoparquet version this dataset implements. |
| `collections` | Map<string, Collection> | A mapping from collection ID to STAC collection objects.                                   |
| `collection`  | STAC Collection object  | **deprecated**. Use `collections` instead.                                                 |

Note that this metadata is distinct from the file metadata required by
[geoparquet].

#### Geoparquet Version

The field `version` stores the version of the stac-geoparquet
specification the data complies with. Readers can use this field to understand what
features and fields are available.

Currently, the only allowed values are `"1.1.0"` and `"1.0.0"`.

Note: early versions of this specification didn't include a `version` field. Readers
aiming for maximum compatibility may attempt to read files without this key present,
despite it being required from 1.0.0 onwards.

#### STAC Collection Objects

To make a stac-geoparquet file a fully self-contained representation, you can
include [STAC Collection][Collection] JSON objects in the Parquet metadata
under the `collections` key. This should be a mapping from Collection ID to
Collection object. As usual, the ID used as the key of the mapping must match
the ID in the Collection object.

Because parquet is a columnar format and stores the union of all the fields from
all the items, we recommend only storing STAC collections with the same or
mostly the same fields in the same stac-geoparquet dataset. STAC collections
with very different schemas should likely be distributed in separate
stac-geoparquet datasets.

#### STAC Collection Object

Version 1.0.0 of this specification included a singular `collection` field that
stored a single STAC collection object. In version 1.1.0, this field is
deprecated in favor of `collections`.

## Referencing a STAC Geoparquet Collections in a STAC Collection JSON

A common use case of stac-geoparquet is to create a mirror of a STAC collection. To refer to this mirror in the original collection, use an [Asset Object](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#asset-object) at the collection level of the STAC JSON that includes the `application/vnd.apache.parquet` Media type and `collection-mirror` Role type to describe the function of the Geoparquet STAC Co
For example:

| Field Name  | Type      | Value                            |
| ----------- | --------- | -------------------------------- |
| href        | string    | s3://example/uri/to/file.parquet |
| title       | string    | An example STAC GeoParquet.      |
| description | string    | Example description.             |
| type        | string    | `application/vnd.apache.parquet` |
| roles       | \[string] | [collection-mirror]\*            |

\*Note the IANA has not approved the new Media type `application/vnd.apache.parquet` yet, it's been [submitted for approval](https://issues.apache.org/jira/browse/PARQUET-1889).

The description should ideally include details about the spatial partitioning method.

## Mapping to other geospatial data formats

The principles here can likely be used to map into other geospatial data formats (GeoPackage, FlatGeobuf, etc), but we embrace Parquet's nested 'structs' for some of the mappings, so other formats will need to do something different. The obvious thing to do is to dump JSON into those fields, but that's outside the scope of this document, and we recommend creating a general document for that.

[media-type]: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-media-type
[asset]: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-object
[asset-roles]: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-roles
[link]: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#link-object
[common-media-types]: https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
[timestamp]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
[parquet-metadata]: https://github.com/apache/parquet-format#metadata
[Collection]: https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#
[schema]: https://github.com/stac-utils/stac-geoparquet/blob/main/spec/json-schema/metadata.json
