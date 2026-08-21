# Bounded foreground mutations

The durable engine has an inline resident-overlay path and a routed
copy-on-write path. Admission selects the path from persisted geometry,
mutation shape, index work, and current resource pressure.

## Point operations

A point put or delete reserves every required page descriptor before it writes
or publishes. An eligible mutation updates the resident overlay. Other shapes
construct immutable pages and an alternate root.

A successful point operation publishes rows and exact postings together.

## Batch operations

`Collection.Update` provides one logical failure-atomic publication. It stages
distinct keys, validates JSON and schema, maintains exact postings, and then
publishes the logical cut.

Storage preparation can publish a content-equivalent topology generation. A
later logical validation can still reject the batch. Thus, Generation may
advance while rows, document count, and exact-index answers stay unchanged.

This contract prevents a stale promise that every rejected batch preserves the
numeric generation.

## Resource bounds

The zero-value batch limit is 64 documents and 16,793,600 key-plus-value bytes.
Buffer geometry reserves the worst case before the mutation starts.

Async and chain-fence durability lanes refuse the primitive batch shape with
`ErrPrimaryBatchUnsupportedLane`. Journal-backed synchronous and buffered
lanes accept it.

## Implementation references

- `store/durable/store_file_batch.go`
- `store/durable/store_file_primary_mutation.go`
- `store/durable/store_file_logical_cut.go`
- `internal/conformance/capability_matrix.go`
