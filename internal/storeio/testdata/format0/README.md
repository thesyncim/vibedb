# Development format 0 golden images

[Format guide](../../../../docs/format.md)

Sparse hex fixtures for `internal/storeio` that pin exact Format 0 byte
layouts, reserved-zero ranges, checksums, complements, and corruption rejection
for the current unreleased commit (test oracles only—never use for migration or
production stores).

## Sparse `.hex` grammar

First non-comment line is `size <decimal-bytes>`, followed by non-overlapping
`<hex-offset> <hex-bytes>` records; unspecified bytes are zero.

## Inventory

| Fixture | Size | Exercises |
| --- | ---: | --- |
| `empty_inline_superblock.hex` | 4,096 B | Gen 7 root, empty state, unsealed capacity |
| `sealed_inline_superblock.hex` | 4,096 B | Same root with 1 GiB physical-capacity seal |
| `mutable_prefix_4k.hex` | 16,384 B | Roots at gen 7 & 8 + empty journal slots |
| `posting_page.hex` | 4,096 B | Common page envelope + exact-index posting segment |
| `materialization_max_patches.hex` | 4,096 B | 8 KiB target + 7-patch ceiling |
| `materialization_max_targets.hex` | 4,096 B | 6 targets, 6 patches, 512 B patch sectors |

## Regenerate and validate

Regenerate only the changed fixture (selectors: `mutable_prefix_4k`,
`empty_inline_superblock`, `sealed_inline_superblock`, `posting_page`,
`materialization_max_patches`, `materialization_max_targets`):

```sh
STOREIO_FORMAT0_GOLDEN=posting_page go test ./internal/storeio -run TestFormat0PrintGolden -v
go test ./internal/storeio -run '^TestFormat0' -count=1
go test ./internal/storeio -count=1
```

## Source map

- [format0_golden_test.go](../../format0_golden_test.go) · [page.go](../../page.go) · [state_root.go](../../state_root.go) · [inline_superblock.go](../../inline_superblock.go) · [mutable_file_layout.go](../../mutable_file_layout.go) · [materialization_journal.go](../../materialization_journal.go) · [posting_page.go](../../posting_page.go)
