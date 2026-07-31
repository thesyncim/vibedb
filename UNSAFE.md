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
`store/store_index_packed.go`, `store/durable/store_file.go`,
`store/durable/store_file_free_scratch.go`, and
`internal/storeio/retired_interval_index.go` construct typed views over exact,
aligned partitions of owned byte blocks. The owning block outlives every view;
the stored structs contain no Go pointers; all counts, products, padding, and
partition ends are overflow-checked before conversion.

### Borrowed views, alias checks, and typed source tags

`internal/storeio/common_primary_leaf.go`,
`internal/storeio/global_tablet_catalog.go`,
`internal/storeio/page_cache_inplace.go`, `store/segment.go`,
`store/segment_stream.go`, `store/store_document_template_read.go`, and
`query/exec.go` use pointer identity or address ranges to reject overlap,
verify a borrowed entry belongs to its owner, or retain one of a closed set of
typed source pointers. Address arithmetic is bounded by the validated owning
slice, and returned views never outlive that owner.

`internal/storeio/read_epochs.go` converts the address of a live stack marker
to an opaque non-zero reader token. The integer is never converted back or
dereferenced.

### Direct word and SIMD loads

`internal/storeio/index_term_leaf.go`,
`internal/storeio/page_checksum_simd_amd64.go`,
`internal/storeio/page_checksum_simd_arm64.go`,
`store/store_float64_reduce_simd_amd64.go`, and
`store/store_float64_reduce_simd_arm64.go` form aligned typed/vector views only
after proving a complete in-range block. Scalar tails handle the remainder.
The architecture-specific implementations must remain differential-tested
against their scalar references and must not retain pointers.

### Linux page I/O

`internal/storeio/ring_linux.go` maps io_uring queues, checks kernel-provided
dimensions and offsets, constructs UAPI views, and passes bounded buffers to
registration and submission syscalls. One locked writer owns the ring;
registered buffers remain mapped until the kernel releases them; completion
tokens, file indexes, buffer indexes, lengths, and offsets are validated before
use.

### Exact capacity accounting

`internal/storeio/recovery_journal.go`, `query/compiler.go`,
`query/file_candidates.go`, `query/heap_work_budget.go`, `query/join_file.go`,
`query/join_pair_budget.go`, `query/result_budget.go`,
`store/durable/store_file_index_bound.go`, and
`store/store_document_template.go` use `unsafe.Sizeof` to bind option-derived
memory limits to the actual Go layouts they allocate. They do not construct or
dereference pointers through `unsafe`.

## Complete production file list

The following list is the current set of non-test Go files that import
`unsafe`:

```text
internal/storeio/common_primary_leaf.go
internal/storeio/global_tablet_catalog.go
internal/storeio/index_term_leaf.go
internal/storeio/page_cache_inplace.go
internal/storeio/page_checksum_simd_amd64.go
internal/storeio/page_checksum_simd_arm64.go
internal/storeio/read_epochs.go
internal/storeio/recovery_journal.go
internal/storeio/retired_interval_index.go
internal/storeio/ring_linux.go
query/compiler.go
query/exec.go
query/file_candidates.go
query/heap_work_budget.go
query/join_file.go
query/join_pair_budget.go
query/result_budget.go
store/durable/store_file.go
store/durable/store_file_free_scratch.go
store/durable/store_file_index_bound.go
store/segment.go
store/segment_stream.go
store/store_document_template.go
store/store_document_template_read.go
store/store_float64_reduce_simd_amd64.go
store/store_float64_reduce_simd_arm64.go
store/store_index_packed.go
store/store_mapped_docs.go
store/store_mapped_keys.go
store/store_mapped_persist.go
store/store_owned_documents.go
```

Review this list with:

```sh
rg -l '"unsafe"' --glob '*.go' --glob '!**/*_test.go' . | sort
```
