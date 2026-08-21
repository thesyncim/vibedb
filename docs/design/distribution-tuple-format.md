# Placement tuple format

The placement tuple codec defines cross-shard scalar identity. Tuple version,
mapper version, field order, field type, field spelling, and mapper parameters
are part of placement identity.

The current tuple version is 1. The current native mapper version is 1.

## Scalar set

A placement scalar is exactly one of these types:

- String
- Exact JSON number

Boolean, null, timestamp, object, array, and nested values are not placement
scalars.

A string contributes its raw bytes. The constructor does not validate UTF-8.
There is no collation, normalization, escape transform, or NUL special case.

A number constructor accepts a valid JSON number spelling. Exact equal values
encode identically. For example, `5`, `5.0`, `5e0`, and `50e-1` have one
placement identity. Positive and negative zero also have one identity.

## Framing

Every scalar is self-delimiting. The first byte is a type tag:

| Tag | Type |
| ---: | --- |
| `0x00` | Reserved and never emitted |
| `0x01` | String |
| `0x02` | Number |

A string encoding is:

```text
0x01 | uvarint byte length | raw bytes
```

A nonzero number encoding is:

```text
0x02 | sign form | adjusted-weight field | uvarint digit count | significant digits
```

Zero uses the number tag and one zero-form byte. It has no weight or digit
payload.

The weight field has zero, positive, and negative forms. A nonzero magnitude
uses an explicit uvarint byte length.

Tuple encoding concatenates scalar encodings in field order. Each field is
self-delimiting, so concatenation is unambiguous.

## Error behavior

`AppendScalar` returns the destination unchanged for an unsupported scalar.
`AppendTuple` can return a destination that contains the valid prefix before a
later invalid scalar.

The zero value of `Scalar` is invalid and cannot encode.

## Native mapping

The native mapper accepts 1 through 8 tuple fields. A complete tuple is encoded
and hashed with xxHash64. The mapper uses the high hash bits to select one
virtual bucket and returns the bucket-start point in the 8-byte keyspace.

Virtual bucket width is 8 through 24 bits. The default is 20 bits.

A shorter leading prefix cannot predict the remaining tuple hash. The native
mapper therefore maps it to the complete keyspace. A tenant-only predicate can
scatter when a later placement field is not bound.

xxHash64 is only the distribution hash. Canonical scalar and tuple bytes define
equality.

## Change control

Do not change tuple bytes, number canonicalization, mapper hashing, or bucket
selection without regenerating every dependent placement artifact in the same
change.

## Implementation references

- `distribution/scalar.go`
- `distribution/decimal.go`
- `distribution/tuple.go`
- `distribution/mapper.go`
- `distribution/bucket.go`
