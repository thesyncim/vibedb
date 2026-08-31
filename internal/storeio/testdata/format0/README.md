# Development format 0 golden images

> [!CAUTION]
> These fixtures describe one unreleased development commit. They are not a
> compatibility promise, a migration input set, or evidence that another
> commit can open the same bytes.

This directory holds sparse hexadecimal golden images for `internal/storeio`.
The tests compare them byte-for-byte with the current encoders and then probe
reserved fields, checksums, complements, bounds, and corruption rejection.

Format 0 deliberately has no compatibility ladder. When a format change is
intentional, update the current fixture and its test oracle together. Keep old,
malformed layouts rejected; do not add a decoder branch merely to preserve an
obsolete development image.

## File grammar

Each `.hex` file is a readable sparse binary image:

```text
# storeio development format 0; unspecified bytes are zero
size 4096
00000000 564254...
```

The first non-comment directive is `size <decimal-bytes>`. Remaining records
are `<hex-offset> <hex-bytes>`. Unspecified bytes are zero. Records must be
in-bounds and non-overlapping; malformed hex, duplicate size declarations, and
overlapping ranges fail the loader.

This representation keeps reserved-zero areas visible without committing large
binary blobs. It is only a test fixture syntax; store files do not use it.

## Fixture inventory

| Fixture | Bytes represented | Contract exercised |
| --- | ---: | --- |
| `empty_inline_superblock.hex` | 4,096 | Generation 7 root, empty mutable state, unsealed capacity |
| `sealed_inline_superblock.hex` | 4,096 | Same root with a 1 GiB physical-capacity seal |
| `mutable_prefix_4k.hex` | 16,384 | Two roots at generations 7 and 8 plus empty journal slots |
| `posting_page.hex` | 4,096 | Common page envelope and one exact-index posting segment |
| `materialization_max_patches.hex` | 4,096 | One 8 KiB target and the seven-patch/data ceiling |
| `materialization_max_targets.hex` | 4,096 | Six targets, six patches, and 512-byte patch sectors |

All fixtures use the fixed test store ID declared in
`format0_golden_test.go`. Most use a 4 KiB page size; the max-patches target is
8 KiB to exercise patches across the larger page.

## What the tests freeze

The goldens deliberately pin details that ordinary round-trip tests can miss:

- format version and page-kind numeric identities;
- mutable-prefix root and journal offsets for supported page geometries;
- fixed header, state-root, reference, record, and trailer sizes;
- generation and checksum complement fields;
- reserved-zero byte ranges;
- state-root capacity, journal identity, and page-limit fields;
- materialization table offsets and maximum counts;
- posting-segment identities and payload placement;
- rejection after corruption, including corruption followed by resealing.

The tests also open the encoded structures through the production decoders.
Matching a fixture without passing decoder admission is not sufficient.

## Regenerate one fixture

The print test writes one sparse fixture to standard output. Run it from the
module root and inspect the diff before replacing the checked-in file:

```sh
STOREIO_FORMAT0_GOLDEN=posting_page \
  go test ./internal/storeio -run TestFormat0PrintGolden -v
```

Valid selector names are:

```text
mutable_prefix_4k
empty_inline_superblock
sealed_inline_superblock
posting_page
materialization_max_patches
materialization_max_targets
```

The Go test harness adds its own `=== RUN`, `--- PASS`, and `PASS` lines under
`-v`; copy only the sparse image printed by the test. A convenient review-safe
workflow is to redirect to a temporary file outside this directory, strip test
harness output if necessary, and compare it with the tracked fixture.

Do not mechanically rewrite all fixtures when only one codec changed. A narrow
diff makes unintentional shifts in offsets or reserved bytes visible.

## Validate the set

Run the complete golden suite:

```sh
go test ./internal/storeio -run '^TestFormat0' -count=1
```

Then run the package because other recovery and graph tests reuse the same
format helpers:

```sh
go test ./internal/storeio -count=1
```

Review a changed fixture together with the encoder/decoder change. At minimum,
answer these questions:

1. Which field, offset, size, or checksum changed?
2. Is the current development format intentionally being replaced?
3. Do reserved bytes remain zero and closed enums remain closed?
4. Do corruption tests still reject resealed invalid state?
5. Were every dependent bounds check and readable format reference updated?

## Do not use these fixtures for migration

The images are deliberately small and synthetic. They are not complete stores,
backup seeds, fuzz corpora, or supported inputs to `durable.Open`.

Never copy one into a database directory or patch a production file from its
bytes. Verification and recovery must use the current `store/durable` tools
against a preserved source copy. Cross-commit migration is not implemented by
this fixture suite.

## Source map

- `internal/storeio/format0_golden_test.go` — fixture builders and assertions
- `internal/storeio/page.go` — common page envelope and checksum
- `internal/storeio/state_root.go` — state-root payload encoding
- `internal/storeio/inline_superblock.go` — inline superblock codec
- `internal/storeio/mutable_file_layout.go` — root, journal, and data offsets
- `internal/storeio/materialization_journal.go` — journal records and limits
- `internal/storeio/posting_page.go` — posting-page encoding
