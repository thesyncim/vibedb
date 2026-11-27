# Verify, salvage, and repack

[Documentation](../README.md) / [Operations](README.md) / Verification

Offline inspection, salvage, and compaction tools for `store/durable` files
(`cmd/vibedb-verify`, built from the exact writer revision). These operate on
embedded store files only, not RF3 node logs or Raft state.

## Operations

| Command / API | Input | Purpose |
| --- | --- | --- |
| `vibedb-verify verify <file\|dir>` | Quiescent store file or DB dir | Structural graph check (file) or primary/journal/`txn.vtm` pairing check (dir) |
| `vibedb-verify salvage <src> <dst>` | Damaged store file (read-only) | Rebuilds a fresh file from surviving self-describing primary leaves |
| `vibedb-verify repack <src> <dst>` | Cleanly closed store copy | Reclaims churn space and restores lexical locality (preserves schema, exact indexes, opaque & overflow values) |
| `(*durable.Collection).CompactOnline` | Open collection | Single-flight live compaction into same-file staging while reads continue |

> [!WARNING]
> `repack` opens its source **read/write** and may apply pending recovery
> rollback. Always copy the closed database first and run `repack` against the
> copy. `salvage` and `repack` require an absent `<dst>` path (they also create
> `<dst>.rjournal`).

## Usage

```sh
GOEXPERIMENT=simd go build -o ./bin/vibedb-verify ./cmd/vibedb-verify

./bin/vibedb-verify verify  ./copy/c-7573657273.vjc
./bin/vibedb-verify verify  ./copy/database-dir
./bin/vibedb-verify salvage ./broken.vjc ./salvaged.vjc
./bin/vibedb-verify repack  ./closed.vjc ./repacked.vjc
```

Every CLI command ends with `result ok` (exit 0) or `result fail findings=<n>`
(exit 1). Always close writers before copying inputs (`Verify` does not take
the writer lock).

## Important caveats

- **Directory vs file `verify`:** Directory verification checks transaction
  metadata (`txn.vtm` and journal pairing) only—verify individual `.vjc` files
  as well for full graph checks.
- **Partial `salvage`:** `salvage` skips overflow values (`overflow_skipped`),
  older duplicate leaves, indexes, and schemas. Require `overflow_skipped=0`
  for complete inline recovery.
- **Cutover:** Always verify `<dst>`, test-open with the exact build, and keep
  the original until application checks pass.
- **`CompactOnline`:** Explicit, single-flight Go API; continuous writes can
  cause retryable conflicts or starvation.

## Source map

- [cmd/vibedb-verify/main.go](../../cmd/vibedb-verify/main.go) · [store/durable/store_file_verify.go](../../store/durable/store_file_verify.go)
- [store/durable/store_file_repack.go](../../store/durable/store_file_repack.go) · [store/durable/store_file_online_compact.go](../../store/durable/store_file_online_compact.go)
