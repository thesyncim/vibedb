// Package store is the in-memory source model that durable JSON collections are
// built from, together with the read-side snapshot machinery the query engine
// reads through. It is not a user-facing mutable database.
//
// A [Builder] bulk-loads a corpus into an immutable keyed graph — source bytes,
// structural tapes, key directory, and exact-index pages — that package
// store/durable freezes onto a page file with CreateFromPrimary. A [Snapshot] is
// the lock-free read view (GetRaw, Range, and the index and candidate mask
// probes a plan is built against) that this package and the durable snapshot
// both expose, so query has one execution core over both backends. Optional
// schema and exact-index definitions shape what a build materializes.
//
// The [Collection] keeps one construction-time mutation beyond reads: Put, which
// the SQL driver uses to stage transient point-lookup rows before a durable
// write. The former user-facing mutable surface — online delete and online
// index declaration — is gone; a durable collection's indexes are frozen at
// construction, and nothing in production mutated a heap collection in place.
//
// # Measuring a collection's footprint
//
// runtime.MemStats.HeapAlloc understates a loaded collection by roughly an
// order of magnitude, and this is by construction rather than by accident. A
// published collection keeps its documents — source bytes, structural tapes,
// key directory, and index pages — in pointer-free blocks that
// internal/storemem places outside the Go heap on common Unix platforms, so
// they are process RSS that HeapAlloc cannot see. A 100,000-document, 24 MiB
// corpus measured 3.9 MiB of HeapAlloc against 165 MiB of peak RSS. Read
// [Stats]'s External*Bytes fields for what the collection holds off-heap, and
// getrusage for the process total.
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
