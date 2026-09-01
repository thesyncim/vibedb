# Unsafe-code boundary

VibeDB uses `unsafe` for pointer-free external storage, mapped byte views,
word/SIMD loads, and platform I/O. Unsafe code does not bypass validation: each
use must prove bounds, alignment, ownership, and lifetime.

## Review contract

1. Keep Go pointers out of pointer-free external storage.
2. Never retain a Go pointer as `uintptr` across a safe point.
3. Validate length, offset, alignment, and capacity before a typed view/load.
4. Keep the backing owner alive for the complete borrow.
5. Invalidate borrowed views before releasing the backing storage.
6. Provide a portable differential oracle for optimized code.
7. Run the matching race, checkptr, corruption, and lifecycle tests.

External or mapped memory can make RSS materially larger than Go `HeapAlloc`.
Borrowed bytes expire at the callback, snapshot, session, collection, mapping,
or I/O lifetime documented by the owning API.

Regenerate the inventory after a production `unsafe` import changes:

```bash
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

## Generated root-module inventory

This inventory includes direct `unsafe` imports in non-test Go files in the
root module across build tags. It excludes tests, `testdata`, vendor, nested Go
modules, dependencies, Cgo, and the Git directory. It is an import inventory,
not a proof that all transitive code is memory-safe.

<!-- unsafe-file-list:start -->
The root module contains 73 non-test Go files that import `unsafe`:

```text
autosplit/recorder.go
cmd/vibedb-gateway/serve_request_wire.go
gateway/catalog.go
gateway/index_metadata.go
gateway/replicated_request_ledger_stream_reader.go
gateway/replicated_table.go
internal/distributedtxn/replicated_codec.go
internal/raftstore/namespace_proof_linux.go
internal/raftstore/preallocate_windows.go
internal/raftstore/ready_write_linux.go
internal/rafttransport/frame.go
internal/replicatedstate/apply_batch.go
internal/replicatedstate/codec_overlap.go
internal/replicatedstate/mutation_canonical.go
internal/replication/completion.go
internal/replication/types.go
internal/storeio/common_primary_leaf.go
internal/storeio/common_primary_unified_scalar_capacity.go
internal/storeio/global_tablet_catalog.go
internal/storeio/index_term_leaf.go
internal/storeio/page_cache_inplace.go
internal/storeio/page_checksum_simd_amd64.go
internal/storeio/page_checksum_simd_arm64.go
internal/storeio/primary_graph.go
internal/storeio/read_epochs.go
internal/storeio/recovery_journal.go
internal/storeio/retired_interval_index.go
internal/storeio/ring_linux.go
internal/storeio/unified_canonical_form.go
planner/memo.go
planner/optimizer.go
planner/statistics.go
query/apply_cache.go
query/apply_kernel.go
query/compiler.go
query/correlated_mark_runtime.go
query/correlation_slots.go
query/exec.go
query/file_candidates.go
query/heap_work_budget.go
query/join_file.go
query/join_pair_budget.go
query/lateral_statement.go
query/recursive_fixpoint_storage.go
query/relation_join.go
query/relation_runtime.go
query/relation_spool.go
query/result_budget.go
query/set_kernel.go
query/set_tree.go
query/source_filter.go
query/sql_scalar_statement.go
query/window_kernel.go
sql/driver/tx.go
sql/driver/write.go
store/durable/store_file_batch.go
store/durable/store_file_free_scratch.go
store/durable/store_file_hole_punch_darwin.go
store/durable/store_file_index_bound.go
store/durable/store_file_operations.go
store/durable/store_file_primary_concurrent.go
store/durable/store_file_primary_fold_parallel.go
store/durable/store_file_primary_unified_overlay.go
store/durable/store_file_resources.go
store/segment.go
store/segment_stream.go
store/store_document_template.go
store/store_document_template_read.go
store/store_index_packed.go
store/store_mapped_docs.go
store/store_mapped_keys.go
store/store_mapped_persist.go
store/store_owned_documents.go
```
<!-- unsafe-file-list:end -->

## Source map

- `internal/unsafeaudit`
- `internal/storemem`
- `internal/storeio`
- `store/store_mapped_*` and `store/store_owned_documents.go`
