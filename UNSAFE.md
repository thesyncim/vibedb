# Unsafe-code inventory

The repository has one production implementation; there is no alternate
"safe" compatibility build. Every `unsafe` use belongs to one of the current
families below and must preserve ordinary Go ownership, bounds, alignment, and
garbage-collector visibility.

## Review rules

An unsafe change must be reviewed for:

1. an ordinary checked path that proves every length, offset, and alignment;
2. the exact lifetime of each derived pointer or typed slice;
3. retained typed ownership and `runtime.KeepAlive` where a syscall or native
   load outlives the last ordinary Go reference;
4. zero pointer storage in mmap/external byte arenas;
5. corruption, alias, race, `-d=checkptr=2`, and allocation tests appropriate
   to the changed family.

`unsafe.Sizeof`, `Alignof`, and `Offsetof` used only for exact capacity
accounting still belong in this inventory because changing a Go layout changes
the configured bound.

## Current families

### External and pointer-free arenas

`store/store_mapped_docs.go`, `store/store_mapped_keys.go`,
`store/store_mapped_persist.go`, `store/store_owned_documents.go`,
`store/store_index_packed.go`, `store/durable/store_file_free_scratch.go`,
`store/durable/store_file_resources.go`, and
`internal/storeio/retired_interval_index.go` construct typed views over exact,
aligned partitions of owned byte blocks. `store_file_resources.go` slices one
`storemem` allocation into a pointer-free `storeio.FreeExtent` reuse arena and a
parallel `uint64` index. The owning block outlives every view; the stored
structs contain no Go pointers; all counts, products, padding, and partition
ends are overflow-checked before conversion.

### Borrowed views, alias checks, and typed source tags

`internal/storeio/common_primary_leaf.go`,
`internal/storeio/global_tablet_catalog.go`,
`internal/storeio/page_cache_inplace.go`, `store/segment.go`,
`store/segment_stream.go`, `store/store_document_template_read.go`,
`query/exec.go`, `query/window_kernel.go`, and
`query/correlated_mark_runtime.go` use pointer identity or address ranges to
reject overlap, verify a borrowed entry belongs to its owner, retain one of a
closed set of typed source pointers, skip a copy when a ping-pong merge buffer
already aliases its destination, or rebase a scalar's payload view onto the
owner's regrown backing block. Address arithmetic is bounded by the validated
owning slice, and returned views never outlive that owner. `window_kernel.go`
and `correlated_mark_runtime.go` also perform `unsafe.Sizeof` budget accounting
described under exact capacity accounting below.

`internal/storeio/read_epochs.go` converts the address of a live stack marker
to an opaque non-zero reader token. The integer is never converted back or
dereferenced.

### Direct word and SIMD loads

`internal/storeio/index_term_leaf.go`,
`internal/storeio/page_checksum_simd_amd64.go`, and
`internal/storeio/page_checksum_simd_arm64.go` form aligned typed/vector views
only after proving a complete in-range block. Scalar tails handle the
remainder. The architecture-specific implementations must remain
differential-tested against their scalar references and must not retain
pointers.

### Native page I/O

`internal/storeio/ring_linux.go` maps io_uring queues, checks kernel-provided
dimensions and offsets, constructs UAPI views, and passes bounded buffers to
registration and submission syscalls. One locked writer owns the ring;
registered buffers remain mapped until the kernel releases them; completion
tokens, file indexes, buffer indexes, lengths, and offsets are validated before
use.

`store/durable/store_file_hole_punch_darwin.go` passes a pointer-free
`fpunchhole` request struct to the Darwin `fcntl(F_PUNCHHOLE)` syscall and holds
it live with `runtime.KeepAlive` across the call. The two 32-bit header fields
keep the following `off_t` values naturally aligned; the kernel copies the
request in and returns before the struct is released.

### Exact capacity accounting

`unsafe.Sizeof`, `Alignof`, and `Offsetof` bind option-derived memory limits,
arena capacities, and cache-line padding to the actual Go layouts a component
allocates. None of these sites construct or dereference a pointer through
`unsafe`.

The original budget files remain: `internal/storeio/recovery_journal.go`,
`query/compiler.go`, `query/file_candidates.go`, `query/heap_work_budget.go`,
`query/join_file.go`, `query/join_pair_budget.go`, `query/result_budget.go`,
`store/durable/store_file_index_bound.go`, and
`store/store_document_template.go`.

The query execution kernels extend the same discipline. `query/apply_kernel.go`,
`query/apply_cache.go`, `query/set_kernel.go`, `query/set_tree.go`,
`query/relation_join.go`, `query/relation_runtime.go`,
`query/relation_spool.go`, `query/correlation_slots.go`,
`query/lateral_statement.go`, `query/recursive_fixpoint_storage.go`, and
`query/sql_scalar_statement.go` size per-row, per-slot, and per-value budgets
against their real struct widths before reserving arena capacity.

`sql/driver/tx.go` and `sql/driver/write.go` charge staged transaction
mutations and seed and index-entry bytes against the write budget.
`store/durable/store_file_primary_concurrent.go`,
`store/durable/store_file_primary_fold_parallel.go`, and
`store/durable/store_file_operations.go` size per-stripe cache-line padding and
per-context committer scratch.
`store/durable/store_file_primary_unified_overlay.go` places `unsafe.Sizeof` in
negative-length array declarations that fail the build if a record's Go layout
drifts from its fixed on-disk byte width. `internal/storeio/primary_graph.go`,
`internal/storeio/unified_canonical_form.go`, and
`internal/storeio/common_primary_unified_scalar_capacity.go` report and bound
retained bytes from the same layouts.

## Complete production file list

The list below is the complete, machine-checked set of non-test Go files in the
root module that import `unsafe`. It is generated from the source tree, not
maintained by hand: `internal/unsafeaudit` walks the module with `go/parser`,
skips nested modules and `testdata`, and
`TestUnsafeFileListMatchesSource` byte-compares the rendered block against the
region between the markers below. Regenerate it after adding or removing an
`unsafe` import with:

```sh
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

<!-- unsafe-file-list:start -->
The root module contains 54 non-test Go files that import `unsafe`:

```text
gateway/catalog.go
gateway/index_metadata.go
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

