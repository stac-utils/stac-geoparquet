# Drawbacks

Trying to represent STAC data in GeoParquet has some drawbacks.

## Unable to represent undefined values

Parquet is unable to represent the difference between _undefined_ and _null_, and so is unable to perfectly round-trip STAC data with _undefined_ values.

In JSON a value can have one of three states: defined, undefined, or null. The `"b"` key in the next three examples illustrates this:

Defined:

```json
{
  "a": 1,
  "b": "foo"
}
```

Undefined:

```json
{
  "a": 2
}
```

Null:

```json
{
  "a": 3,
  "b": null
}
```

Because Parquet is a columnar format, it is only able to represent undefined at the _column_ level. So if those three JSON items above were converted to Parquet, the column `"b"` would exist because it exists in the first and third item, and the second item would have `"b"` inferred as `null`:

| a   | b     |
| --- | ----- |
| 1   | "foo" |
| 2   | null  |
| 3   | null  |

Then when the second item is converted back to JSON, it will be returned as

```json
{
  "a": 2
  "b": null
}
```

which is not strictly equal to the input.

## Schema difficulties

JSON is schemaless while Parquet requires a strict schema, and it can be very difficult to unite these two systems. This is such an important consideration that we have a [documentation page](./schema.md) just to discuss this point.
