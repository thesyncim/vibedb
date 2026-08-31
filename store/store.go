// Package store is VibeDB's in-memory collection engine and the source model
// shared by durable reads and query execution.
//
// [Builder] bulk-loads unique keyed JSON documents into an immutable graph and
// publishes it as a mutable [Collection]. It builds document, key, shape, and
// zone metadata; exact indexes are created through the collection index API,
// not materialized by Builder. [Snapshot] is the lock-free read view used by
// the query engine. [Database] adds a named catalog, coherent multi-collection
// snapshots, and failure-atomic [Database.Update] publication.
//
// The raw store API is not identical to the root vibedb facade or to
// store/durable. See docs/store.md for the ownership and semantic boundaries.
//
// # Measuring a collection's footprint
//
// runtime.MemStats.HeapAlloc understates a loaded collection by roughly an
// order of magnitude, and this is by construction rather than by accident. A
// published collection keeps its documents — source bytes, structural tapes,
// key directory, and index pages — in pointer-free blocks that
// internal/storemem places outside the Go heap on common Unix platforms, so
// they are process RSS that HeapAlloc cannot see. Read [Stats]'s External*Bytes
// fields for what the collection holds off-heap, and getrusage for the process
// total. Measured examples live in docs/performance.md.
//
// The gap runs the other way during a load. Both write paths stage a chunk's
// source and structural tapes on the Go heap and only move them off it when the
// chunk is compacted, so the Go heap high-water mark — MemStats.HeapSys, not
// HeapAlloc — is what a bulk load actually costs, and it is several times the
// collection's steady state. A footprint measurement that reads HeapAlloc after
// the load has finished will see neither number.
package store

// New validates options and returns an initialized in-memory collection. It
// freezes options immediately rather than deferring configuration errors or
// option capture to the first mutation, so no mutation can observe a
// misconfigured collection.
//
// The returned collection is standalone and unnamed; only
// [Database.CreateCollection] gives a collection a catalog name.
func New(options Options) (*Collection, error) {
	normalized, err := options.Normalized()
	if err != nil {
		return nil, err
	}
	collection := &Collection{Options: normalized}
	if _, err := collection.initLocked(); err != nil {
		return nil, err
	}
	return collection, nil
}
