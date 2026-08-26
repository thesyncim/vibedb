# Unsafe-code boundary

VibeDB uses `unsafe` where a bounded low-level representation or platform API
needs it. Unsafe code is not an alternative path around validation. Each use
must preserve ownership, alignment, bounds, and lifetime contracts.

## Main use families

### External memory

`internal/storemem.Block` uses anonymous `mmap` on supported Unix targets and a
Go byte slice elsewhere. Its bytes become invalid after `Close`.

Mapped key, document, index, and durable arena structures create typed views of
pointer-free byte storage. Owning state must retain the backing block because
the garbage collector cannot find a pointer that exists only inside mapped
memory.

External memory can make RSS much larger than Go `HeapAlloc`. Use VibeDB
external-memory metrics when you assess residency.

### Borrowed views

Borrowed keys, documents, and typed slices remain valid only for the lifetime
of their owner. The owner can be a callback, immutable state, snapshot,
session, collection, or I/O submission.

Do not retain borrowed data after a mutation, snapshot close, session run,
session release, collection close, or callback return when that event ends the
documented lifetime.

### Word loads and SIMD

Checksum and SIMD paths use unaligned or architecture-specific word loads.
Each path must validate the available byte count before the load. Portable and
optimized implementations need differential tests.

### Platform I/O

Linux `io_uring` code maps kernel rings and registered buffers. Darwin hole
punching uses an unsafe ABI call. These paths must keep platform build tags,
alignment checks, fallback behavior, and resource teardown tests.

## Review rules

For each unsafe change:

1. Keep Go pointers out of pointer-free external storage.
2. Do not retain a Go pointer as `uintptr` across a safe point.
3. Prove size, alignment, offset, and capacity before a typed view or load.
4. Keep the backing owner alive for the complete borrow.
5. Invalidate all views before you release external memory.
6. Add portable differential tests for an optimized path.
7. Run race, checkptr, corruption, and lifecycle tests that match the change.
8. Regenerate the production-file inventory.

Run this command after an unsafe import changes:

```bash
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

## Generated production-file inventory

The test parses Go imports. It excludes tests, testdata, vendor content, the
Git directory, and nested Go modules.

<!-- unsafe-file-list:start -->
The root module contains 67 non-test Go files that import `unsafe`:

```text
autosplit/recorder.go
gateway/catalog.go
gateway/index_metadata.go
gateway/replicated_table.go
gateway/replicated_transaction.go
internal/distributedtxn/replicated_codec.go
internal/raftstore/preallocate_windows.go
internal/rafttransport/frame.go
internal/replicatedstate/apply_batch.go
internal/replicatedstate/codec_overlap.go
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
query/sql_scalar_statement.go
query/window_kernel.go
sql/driver/tx.go
sql/driver/write.go
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

## Implementation references

- `internal/unsafeaudit/audit.go` and `audit_test.go`
- `internal/storemem/block.go`
- `store/store_mapped_keys.go` and `store/store_mapped_docs.go`
- `store/store_owned_documents.go` and `store/store_index_packed.go`
- `internal/storeio/iouring.go`
