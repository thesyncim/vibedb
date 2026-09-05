# Verify, salvage, and repack

[Documentation](../README.md) / [Operations](README.md) · [Development status](../status.md)

This page covers offline inspection and repair of the `store/durable` file
format. It does not turn the development format into a compatibility promise.

## Choose the operation

| Need | Operation | Input | Output |
| --- | --- | --- | --- |
| Check a store graph | `verify` | Quiescent store file | Findings and counters |
| Check transaction pairing | `verify` | Quiescent database directory | Journal/decision findings |
| Recover inline rows after catalog loss | `salvage` | Damaged store file | New store file |
| Reclaim space and restore scan locality | `repack` | Cleanly closed store file | New clustered store file |
| Reclaim space while serving | `(*durable.Collection).CompactOnline` | Open collection | Same-file replacement generation |

`verify` and `salvage` read the source. **`repack` opens its source read/write
and can apply pending recovery rollback.** Run repack against a complete
quiescent copy when the original must remain unchanged:

```text
vibedb-verify verify  <store-file|database-dir>
vibedb-verify salvage <store-file> <output-file>
vibedb-verify repack  <store-file> <output-file>
```

`salvage` and `repack` create the output with exclusive creation. Choose a path
that does not exist. Never point either command at the source path.

## Prepare a stable input

`Verify` does not acquire the writer lock. It also does not apply the in-place
materialization rollback that ordinary open may apply. A concurrent writer can
retire and reuse an extent while the verifier reads it.

Prepare the input in this order:

1. Stop writes and close the collection or database successfully.
2. Copy the complete, closed database directory to a separate location.
3. Run the operation against the copy.

For a database directory, keep collection primaries, every `.rjournal`, and
`txn.vtm` together. A raw copy made while the database is live is not a
supported backup cut.

## Verify a store file

```sh
go run ./cmd/vibedb-verify verify ./data/collection-file
```

The verifier selects the newest structurally valid root, with fallback to the
preceding valid generation, then walks the reachable graph. It checks:

- page checksums, identities, reference bounds, and graph shape;
- global bytewise key order;
- aliasing between reachable extents;
- overlap between the durable free set and reachable pages;
- structural exact-index roots and leaves.

The final lines are machine-oriented:

```text
summary root_slot=1 generation=42 file_end=1048576 documents=12 free_extents=3 findings=0
result ok
```

`result ok` means `Findings` was empty for this offline walk. It does not prove
application invariants, make the file compatible with another commit, or run
every online exact-posting/live-slot admission check performed by `Open`.

The Go API exposes the same result:

```go
f, err := os.Open(path)
if err != nil { return err }
defer f.Close()

report, err := durable.Verify(f)
if err != nil { return err } // I/O prevented the walk
if !report.OK() {
    return fmt.Errorf("store has %d structural findings", len(report.Findings))
}
```

An unreadable root or superblock is normally a finding, not an API error.
`VerifyReport` also records the selected root slot, generation, file end,
document/free-extent counts, and page counts by kind.

## Verify a database directory

```sh
go run ./cmd/vibedb-verify verify ./data/database
```

Directory verification inspects primary/journal identity pairing and scans
`txn.vtm` decisions and conditional journal records. It detects missing,
unreadable, mismatched, in-doubt, and torn-tail state without appending,
recycling, truncating, or deleting anything.

This check is transaction metadata inspection, not a recursive structural walk
of every collection. Verify individual collection files as well when both
properties matter.

## Salvage catalog loss

```sh
go run ./cmd/vibedb-verify salvage ./broken.vdb ./salvaged.vdb
```

Salvage ignores routing state and scans page-aligned extents for valid,
self-describing primary leaves. For each leaf bucket it keeps the highest
generation, extracts live rows in lexical order, and builds a fresh store.

Use salvage only for its narrow recovery model:

| Property | Contract |
| --- | --- |
| Source | Read-only; may have damaged catalog/routing pages |
| Output | New, empty file |
| Inline values | Recovered from surviving valid leaves |
| Overflow values | Counted in `OverflowSkipped`, not recovered |
| Duplicate leaf versions | Older versions counted and skipped |
| Index/schema recovery | Not reconstructed from arbitrary corrupt metadata |

A zero process exit status does not mean an overflow-bearing source was
recovered completely. Read the summary and require `overflow_skipped=0` for an
exact inline-row recovery.

## Repack a healthy store

```sh
go run ./cmd/vibedb-verify repack ./closed.vdb ./repacked.vdb
```

Repack opens a cleanly closed source, snapshots it, scans live keys in bytewise
lexical order, and writes a new store. The output has no reclaimable space from
the source's churn.

Repack preserves configured schema, exact indexes, opaque values, and overflow
values through the normal bounded write path. A faster bulk path is selected
only for inline, schema-free, non-opaque data. It is incorrect to describe the
whole operation as inline-only.

Verify the new file before replacing anything. Keep the original until the new
file has been opened by the exact intended build and application checks pass.

## Online compaction

`CompactOnline` rewrites a live collection into authenticated same-file staging
while reads continue. It rebuilds exact indexes and preserves opaque and
overflow values.

It is explicit, single-flight work. It may return queue pressure, publication
conflict, starvation, or checkpoint-group ownership errors. Retry policy belongs
to the caller. Continuous writes can prevent convergence; there is no general
automatic-compaction guarantee.

The report's device-byte count is instrumentation around the operation, not an
exact promise about physical device write amplification. Benchmark thresholds
in tests qualify their fixtures and hosts; they are not production SLAs.

## Replacement checklist

1. Preserve the source and its journals.
2. Produce a new output path; never overwrite in place.
3. Inspect every finding and every skipped counter.
4. Verify the output offline.
5. Open and test it with the exact same build and options.
6. Replace paths only through an operator-controlled, recoverable cutover.

## Source map

- [cmd/vibedb-verify/main.go](../../cmd/vibedb-verify/main.go) — CLI grammar, exit status, directory checks
- [cmd/vibedb-verify/main_test.go](../../cmd/vibedb-verify/main_test.go) — output and failure behavior
- [store/durable/store_file_verify.go](../../store/durable/store_file_verify.go) — verify and salvage contracts
- [store/durable/store_file_verify_test.go](../../store/durable/store_file_verify_test.go) — corruption and salvage cases
- [store/durable/store_file_repack.go](../../store/durable/store_file_repack.go) — offline repack paths
- [store/durable/store_file_repack_test.go](../../store/durable/store_file_repack_test.go) — capability preservation
- [store/durable/store_file_online_compact.go](../../store/durable/store_file_online_compact.go) — live compaction lifecycle
- [store/durable/store_file_online_compact_test.go](../../store/durable/store_file_online_compact_test.go) — indexes and value modes
