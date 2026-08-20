# Placement scalar and tuple codec

This document specifies the byte format `distribution.CurrentTupleCodec`
produces, precisely enough that an independent implementation can reproduce
byte-for-byte identical output from the same logical input. The Go source under
`distribution/` (`scalar.go`, `tuple.go`, `decimal.go`) is the authoritative
implementation; this document and the committed golden vectors at
`distribution/testdata/tuple_vectors.txt` are the portable specification of
what that implementation must keep producing.

This format is placement identity: two callers that reach the same encoded
bytes for the same shard key must resolve to the same shard across processes
and languages. The repository is unreleased and accepts one current contract.
A change to any rule below must update the implementation, every dependent
placement artifact, these golden vectors, and the current routing tests
together. The repository carries one unreleased grammar and no compatibility
decoder ladder.

## Scope

Two scalar kinds exist, and no others:

| Kind | Meaning |
| --- | --- |
| String | an arbitrary byte string; no UTF-8 validation, no escaping |
| Number | an exact decimal value, free of float64 rounding |

`Bool`, timestamp, `Any`, object, array, and nested/composite values are not
part of the scalar domain and have no encoding here.

A **tuple** is an ordered, fixed-arity sequence of scalars — one per
shard-key column, in column-declaration order.

## From tuple bytes to a virtual bucket

The current native mapper hashes the complete canonical tuple byte string with
XXH64, takes the high `BucketBits` (20 by default) as a `VirtualBucket`, and
represents that bucket by its 8-byte big-endian keyspace start:

```text
hash         = XXH64(tuple_bytes)
bucket       = hash >> (64 - BucketBits)
keyspace_id  = bucket << (64 - BucketBits)
```

Physical manifests own contiguous runs in that keyspace. Explicit catalog
bucket widths require every shard boundary to be bucket-aligned. The mapper
uses the complete tuple deliberately: `(tenant, locality-key)` spreads one
tenant across buckets instead of making tenant identity a physical shard. A
shorter leading prefix cannot predict the missing hash input and maps to the
full keyspace. Applications that need a narrow tenant-only scan use an index;
the router never invents a false tenant-local range.

The mapper path is allocation-free for canonical tuples up to 256 bytes and
has committed bucket vectors in `distribution/bucket_test.go`. `BucketBits`,
mapper identity, tuple version, arity, and ordered placement paths are immutable
placement identity. The repository accepts one current mapping contract, not a
ladder of named protocol generations.

### Implementation map

| File | Responsibility |
| --- | --- |
| `distribution/scalar.go` | `Scalar`, `ScalarKind`, `NewString`, `NewNumber` — the closed constructor surface |
| `distribution/decimal.go` | JSON number spelling validation and canonical decomposition (frozen fork, no shared code with `query/decimal.go`) |
| `distribution/tuple.go` | `TupleCodec`, `CurrentTupleCodec`, the tag/form bytes this document specifies, and the actual `Append*` logic |
| `distribution/bucket.go` | fixed virtual-bucket geometry and keyspace projection |
| `distribution/bucket_manifest.go` | allocation-free bucket-interval ownership and target lookup |
| `distribution/mapper.go` | full-tuple hash mapping and honest prefix scatter |
| `distribution/errors.go` | `InvalidNumberError`, `UnsupportedScalarError` |

## Framing principle

Every scalar's encoding is **self-delimiting**: its first byte is a tag that
identifies String or Number, and every variable-length field that follows
carries its own explicit length before the bytes it covers. A tuple's
encoding is simply the concatenation, in order, of each element's scalar
encoding:

```
tuple_bytes = scalar_bytes(v[0]) || scalar_bytes(v[1]) || ... || scalar_bytes(v[n-1])
```

No terminator, no arity prefix, and no total-length prefix appear anywhere in
a tuple's bytes. This is safe only because every scalar's own length is
recoverable by reading forward from its tag byte — a decoder never needs to
look ahead or backtrack to find where one scalar ends and the next begins.
That, in turn, means the number of bytes the *first* scalar in a byte string
occupies is a deterministic function of that byte string's own content, not
of how many scalars the reader expects: reading two different tuples from
offset zero can never produce the same bytes, regardless of their arities or
where a value's payload happens to split across two adjacent columns (see
the worked example at the end of "Tuple encoding" below). The one real
limitation is that this guarantee is anchored at offset zero — see
"Non-goals and what this format does not guarantee" for what starting a read
at an arbitrary interior offset does *not* give you.

The empty tuple (zero scalars) encodes to the empty byte string.

## Tuple codec identity is out-of-band

`TupleVersion` (a `uint32`, with the only accepted value named
`CurrentTupleVersion`) is never embedded inside the encoded bytes. It travels
beside the bytes as explicit metadata, for example alongside a stored
`TablePlacement`, and must match the current codec before those bytes can be
compared or interpreted. Only the current codec is accepted.

## Uvarint

Every variable-length field's length is encoded as an **unsigned LEB128
varint** (the same algorithm as Go's `encoding/binary.AppendUvarint` /
`ReadUvarint`, and as used by protocol buffers): the value is split into
7-bit groups, least-significant group first; every byte except the last has
its high bit (0x80) set to mean "more bytes follow"; the last byte has its
high bit clear. For example, `5` encodes as one byte `0x05`; `300` encodes as
two bytes `0xAC 0x02`.

## Reserved tag byte

| Byte | Meaning |
| --- | --- |
| `0x00` | reserved; never emitted by this codec |
| `0x01` | String scalar |
| `0x02` | Number scalar |

`0x00` is reserved specifically so that a zero-filled buffer, or a
short/truncated read that starts mid-record, is never misinterpreted as a valid
scalar tag. No decoder ships today—see "Non-goals" below—so this property is
currently defensive rather than exercised by shipped code.

## String encoding

```
tag(1 byte, 0x01) | uvarint(byte length of s) | s, verbatim
```

The bytes of `s` are copied through unmodified: no UTF-8 well-formedness
validation, no escaping of any byte (including NUL, control bytes, or
invalid/partial UTF-8 sequences), and no case folding or normalization of any
kind. Two Strings compare equal for placement purposes exactly when they are
byte-identical; there is no collation.

Because the length is explicit rather than terminator-based, an embedded NUL
byte, an embedded byte equal to any other scalar's tag value, or arbitrary
non-UTF-8 bytes all pass straight through with no special-casing.

### Worked examples

| Input | Encoding (hex) | Breakdown |
| --- | --- | --- |
| `""` | `0100` | tag `01`, length `00`, no bytes |
| `"a"` | `010161` | tag `01`, length `01`, byte `61` (`'a'`) |
| `"a\x00b"` | `0103610062` | tag `01`, length `03`, bytes `61 00 62` |

## Number encoding

A Number scalar is constructed from a **validated JSON number spelling**: an
optional leading `-`, an integer part (`0` or a nonzero digit followed by
digits, never a leading zero), an optional `.` followed by one or more
fraction digits, and an optional exponent (`e`/`E`, optional `+`/`-`, one or
more digits). No leading `+`, no leading zero in the integer part unless the
integer part is exactly `0`, no bare/empty fraction or exponent digit run,
`NaN`/`Infinity` and any other non-numeric text, extra characters after a
well-formed number, and multiple dots or exponent markers are all rejected
before encoding is ever attempted (`InvalidNumberError`).

### Canonical decomposition

Every accepted spelling reduces to an exact decimal value:

```
value = (-1)^sign x 0.d0 d1 ... dk x 10^(weight + 1)
```

where `d0 ... dk` are the value's significant digits with every leading and
trailing zero stripped (so the digit sequence is empty exactly when the
value is zero), and `weight` is the signed, arbitrary-precision decimal
integer exponent of `d0` — the source spelling's exponent literal, adjusted
by the mantissa's decimal-point position. This is the same decomposition
`query/decimal.go` and `query/groupkey.go` use for exact-value comparison and
group-key equality, forked into `distribution/decimal.go` so this frozen
format cannot drift if that package's comparison logic later changes. Two
spellings that reduce to the same `(sign, weight, digit sequence)` triple —
for example `5`, `5.0`, `5e0`, and `50e-1`, all of which are `+0.5 x 10^1`, or
`-0`, `0`, `0.0`, and `0e999...`, all of which have an empty digit sequence —
produce byte-identical encodings. Spelling, digit grouping, and exponent
notation never participate in the encoding directly; only the reduced triple
does.

`weight` has unbounded magnitude in principle (a JSON exponent literal is not
bounded), so it is carried as a canonical, sign-and-magnitude decimal digit
string, not a fixed-width integer. Its magnitude is computed from the source
spelling's exponent digits plus a small integer adjustment (the digit
position of the mantissa) using ordinary decimal carry/borrow arithmetic, so
it stays exact for arbitrarily long exponent literals.

### Layout

```
tag(1 byte, 0x02) | numberForm(1 byte)
```

`numberForm`:

| Byte | Meaning |
| --- | --- |
| `0x00` | zero — every canonical zero spelling (`0`, `-0`, `0.0`, `-0e999...`, ...) folds here; encoding stops immediately (2 bytes total) |
| `0x01` | positive (nonzero) |
| `0x02` | negative (nonzero) |

If `numberForm` is not zero, two more fields follow: the weight field, then
the digit field.

**Weight field:**

```
weightForm(1 byte)
if weightForm != 0x00:
    uvarint(magnitude digit count) | magnitude digits (ASCII '0'-'9', no leading zero)
```

| `weightForm` byte | Meaning |
| --- | --- |
| `0x00` | weight is exactly `0`; no magnitude bytes follow |
| `0x01` | weight is positive; magnitude follows |
| `0x02` | weight is negative; magnitude follows |

The magnitude, when present, is the canonical (no leading zero) decimal
digit string for `abs(weight)`, written as ASCII digit bytes, prefixed by its
own byte-length uvarint (not a fixed width — this is what makes arbitrarily
large exponents representable without a fixed-size or big-integer field).

**Digit field:**

```
uvarint(total significant digit count) | intDigits | fracDigits
```

`intDigits` followed immediately by `fracDigits` is the value's full
significant-digit sequence (`d0 d1 ... dk` from the decomposition above),
written as ASCII digit bytes with no separator between the two runs and no
decimal point marker — the split between "integer part" and "fraction part"
of the original spelling is not recoverable from the encoding and does not
matter for placement identity (`1` and `0.1e1` both reduce to digit sequence
`"1"` at weight `0`). The uvarint gives the combined length of
`intDigits || fracDigits`.

### Worked examples

| Input | Encoding (hex) | Breakdown |
| --- | --- | --- |
| `"0"`, `"-0"`, `"0.0"` | `0200` | tag `02`, numberForm `00` (zero); nothing else follows |
| `"5"`, `"5.0"`, `"5e0"`, `"50e-1"` | `0201000135` | tag `02`, numberForm `01` (positive), weightForm `00` (weight 0), digit-count `01`, digit `35` (`'5'`) |
| `"42"` | `0201010131023432` | tag `02`, numberForm `01`, weightForm `01` (positive weight), weight-magnitude-length `01`, weight digit `31` (`'1'`, i.e. weight = 1), digit-count `02`, digits `3432` (`'4' '2'`) |
| `"-42"`, `"-42.0"`, `"-4.2e1"` | `0202010131023432` | identical to `"42"` except numberForm `02` (negative) |

The `"42"` example shows the weight arithmetic concretely: `42 = 0.42 x
10^2`, so its adjusted weight is `1` (0-indexed exponent of the leading
digit), encoded as weightForm `01` (positive), a one-digit magnitude `01`,
and the magnitude digit `'1'`.

## Tuple encoding

```
tuple_bytes = concat(scalar_bytes(v) for v in values)
```

No arity byte, no total-length prefix, no separator. This is safe because of
the framing principle above: every scalar's own encoding tells a reader
exactly how many bytes it occupies, so scalars can be concatenated and later
split apart unambiguously by whoever holds the current layout rules, even
though no such splitting code ships today.

Two tuples of the same arity, column type sequence, and equal per-column
values (under the equivalence classes above) produce byte-identical
encodings; two tuples that differ in any column's value, in arity, or in any
column's split between adjacent columns produce different encodings. For
example, encoding `["ab", "c"]` and `["a", "bc"]` as two-element String
tuples gives `01 02 6162 01 01 63` and `01 01 61 01 02 6263` respectively —
different bytes, because the length prefix on each element records exactly
where that element's payload ends, and `"ab"`+`"c"` does not carry the same
length prefixes as `"a"`+`"bc"` even though the flattened payload bytes
(`"abc"`) are the same in both cases.

## Non-goals and what this format does not guarantee

- **No decoder ships.** Nothing above should be read as "there
  is a `Decode` function" — there is not. The format is specified as
  self-delimiting because that is what makes tuple concatenation safe, not
  because a decoder exists today.
- **Byte-equality is the only comparison this format defines.** It does not
  define an ordering between two Number or String encodings; a byte-lexical
  comparison of two tuples is not claimed to match numeric or
  collated ordering. (Contrast with `internal/orderedkey`, which is a
  separate, order-preserving format for a different purpose and is
  explicitly not reused here — see "Provenance" below.)
- **Cross-type byte containment is not claimed beyond the tag byte.** A
  String's payload bytes can, by coincidence, contain a byte sequence that
  looks like a well-formed Number encoding (or vice versa) if you start
  reading from the wrong offset; this format does not promise "resynchronization"
  from an arbitrary byte offset, only that reading each scalar from its own
  correct starting offset (position 0 of the tuple, then immediately after
  the previous scalar's own self-delimited length) recovers the original
  sequence unambiguously.

## Provenance

The Number canonicalization algorithm (weight/digit decomposition, the
prefix/middle/fill/tail representation of an arbitrary-precision weight
magnitude) is an independent fork of the equivalent logic in
`query/decimal.go` and `query/groupkey.go`, kept deliberately separate so
that this placement format cannot be perturbed by future changes to that
package's comparison or group-key logic. `distribution/decimal.go` imports
nothing from `query` or `internal/orderedkey`.

## Conformance

`distribution/testdata/tuple_vectors.txt` is the committed golden-vector
fixture for this format; `distribution/golden_vectors_test.go` loads it and
checks every vector byte-for-byte against `distribution.CurrentTupleCodec`. A
mismatch there means either this document/implementation regressed, or the
fixture was miscopied — either way, per the fixture's own header, it is a
stop condition for design review, not something to patch by editing the
expected bytes.
