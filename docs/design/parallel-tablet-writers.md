# Write concurrency

VibeDB separates admission concurrency from generation publication. It bounds
both.

## Facade concurrency

Each facade collection has its own direct-mutation fence. A write to one
collection does not take the fence of an unrelated collection.

A transaction takes all dirty collection fences in name order. This closes the
window between conflict validation and participant publication without adding
a database-wide lock to every point write.

## Heap concurrency

One heap collection has one writer mutex. Readers use an immutable state pointer
and do not take that mutex. A point write rebuilds at most one bounded chunk.

## Durable concurrency

The durable engine can admit concurrent mutation requests through bounded
queues and buffers. One publisher orders journal, page, and root transitions.
The selected durability lane defines when each caller can return.

Queue admission is not unbounded buffering. An operation reserves the required
descriptors and bytes before an irreversible write.

Long-lived snapshots can reduce write progress because they pin retired
extents. Reclamation pressure returns a typed capacity error instead of
overwriting a referenced extent.

## Multi-collection ordering

Heap and durable database transactions take participant locks or gates in a
stable order. Coherent database snapshots use the related ordered gate protocol.

Do not infer one serial global write order from this design. Unrelated
collections retain independent point-write concurrency.

## Implementation references

- `vibedb.go` and `vibedb_txn.go`
- `store/engine.go`
- `store/durable/store_file_mutation_combiner.go`
- `store/durable/store_file_primary_concurrent.go`
- `store/durable/store_database_snapshot.go`
