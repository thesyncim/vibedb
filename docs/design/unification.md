# Engine unification

**Status:** active design for the remaining public-engine consolidation.

The durable ordered-primary engine already has one canonical leaf grammar,
transactional batches, exact-index maintenance, buffered and synchronous
durability modes, and the SQL catalog used by both `database/sql` and
`pgwire`. The remaining duplication is the separate mutable heap collection.

## Target

One `Collection` owns canonical frames and selects either a real device or an
ephemeral device. Durability is configuration, not a second mutable engine.
The corresponding immutable view is `Snapshot`, and `Database` is the
multi-collection catalog. `Segment` remains query/build substrate rather than
an independently evolving mutable database.

The end state must provide the same point-read, scan, query, schema, index, and
transaction semantics in ephemeral and persistent modes. A mode may differ in
its durability boundary and device costs, not in its document representation
or query behavior.

## Remaining work

1. Add the ephemeral device without adding an alternate page or leaf codec.
2. Move builder and query entry points behind the unified collection surface.
3. Remove the heap collection only after every current caller and benchmark
   uses the unified engine.
4. Add parallel tablet staging while retaining one serialized publication
   point; see [parallel tablet writers](parallel-tablet-writers.md).
5. Keep every stored format field at version 0 and regenerate the format-0
   golden images whenever the unreleased layout changes.

## Gates

- heap and durable semantic differential tests cover reads, writes, batches,
  snapshots, indexes, SQL, cancellation, and resource errors before cutover;
- the ephemeral mode introduces no device-only durability claim;
- current point-read, mutation, scan, memory, and file-space benchmark lanes do
  not regress beyond their documented gates;
- the final tree has one public mutable collection, one leaf grammar, one SQL
  runtime, and no compatibility wrapper for the removed surface.
