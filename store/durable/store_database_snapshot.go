package durable

import (
	"slices"
	"strings"
)

// A DatabaseSnapshot is a logically immutable view of every collection in a
// [Database] at one instant. Its zero value is an empty snapshot.
//
// It must be closed. Unlike a heap [store.DatabaseSnapshot], which retains
// nothing but Go pointers, this one holds one generation lease per collection,
// and a lease blocks reuse of every extent its collection's writer retires
// after the snapshot was taken. Holding one open across a sustained write loop
// fills Options.MaxRetiredExtents on every collection it covers, not just one.
// Prefer one database snapshot per query.
//
// # What the cut guarantees
//
// This is the paragraph to read before relying on the word "consistent", and it
// is worth being exact, because the guarantee is strong in one direction and
// deliberately silent in another.
//
// Every collection's lease is acquired while every collection's snapshotGate is
// held on its read side. A commit takes that gate's write side across the
// instant it swaps its collection's state pointer, so for the whole interval
// during which this snapshot loads state pointers and acquires leases, no
// collection in the database can publish. The set of generations captured is
// therefore a set that genuinely coexisted: there is no pair of collections A
// and B for which the snapshot holds a generation of A that had already been
// replaced when the captured generation of B was published. That is the absence
// of snapshot skew, and it is exactly what a join needs — a join resolving
// references against a state its driving side never saw would return rows that
// were never simultaneously true, and this makes that unrepresentable rather
// than merely unlikely.
//
// The leases are what carry the cut forward in time. Once acquired, each pins
// its collection's captured generation against the extent reclaimer, so every
// page the snapshot may read stays readable no matter how many commits land
// afterwards. The cut is thus consistent at capture and stable thereafter, and
// those are two separate mechanisms — the gates give the first, the leases the
// second — which is why both are needed and why neither alone would do.
//
// # What the cut does not guarantee
//
// It does not make separate writes atomic. A Database has no cross-collection
// transaction: an application that puts one document into orders and then
// another into customers has performed two independent commits, and a snapshot
// may legitimately fall between them. The snapshot is consistent with respect
// to the database's own commits, not with respect to an application's intent
// spanning several of them.
//
// It does not survive a crash. A lease is process memory, not a durable
// reservation. After a restart each collection recovers to its own last
// committed generation, and because those commits were independent, the
// recovered set need not be a cut this snapshot would ever have produced.
// Consistency here is a read-time property of a running process.
//
// It is not a timestamp, and this is the limitation that matters for where the
// engine might go next. The cut is established by mutual exclusion: it works
// because every writer to every collection in the database must pass through a
// gate that this process holds. A distributed version has no such gate — locks
// do not cross machines, and a remote writer cannot be made to block on a
// mutex here — so the same guarantee would have to be rebuilt on a
// timestamp-based cut: a version counter each collection's commit stamps its
// state with, and a read that selects, per collection, the newest state at or
// below a chosen cut timestamp. That is a different mechanism with a different
// cost profile (it needs multi-version retention rather than momentary
// exclusion), and it is deliberately not what this is. Nothing here should be
// read as a step toward it.
type DatabaseSnapshot struct {
	// entries is ordered by name so lookup can binary-search it, and so a
	// snapshot's iteration order is the catalog's own stable order.
	entries []databaseSnapshotEntry
}

// A databaseSnapshotEntry binds one collection name to its captured view.
type databaseSnapshotEntry struct {
	name     string
	snapshot *Snapshot
}

// Snapshot captures every cataloged collection at one instant, acquiring one
// generation lease per collection. The caller must Close the result.
//
// It briefly holds every collection's publication gate, so it blocks concurrent
// commits across the database for the duration of the capture — the time to
// load one pointer and acquire one lease per collection — and a commit already
// in progress delays it. Reads are unaffected: existing snapshots are
// untouched and single-collection [Collection.Snapshot] is unchanged.
//
// A collection created after Snapshot returns is absent from the result. One
// dropped afterwards is a caller error, because [Database.DropCollection]
// closes the collection and its open snapshots must be closed first.
//
// If any collection's lease cannot be acquired — the usual reason is
// Options.MaxSnapshotLeases exhausted by snapshots that were never closed — the
// leases already taken are released and the error is returned, so a failed
// capture pins nothing.
func (d *Database) Snapshot() (DatabaseSnapshot, error) {
	var dst DatabaseSnapshot
	err := d.SnapshotInto(&dst)
	return dst, err
}

// SnapshotInto is [Database.Snapshot] writing into caller-owned storage,
// reusing dst's entry slice. It is the form a query loop uses: the per-call
// cost drops to one *Snapshot per collection, which is what a single-collection
// [Collection.Snapshot] already costs, rather than that plus a fresh catalog.
//
// dst is closed first if it holds a previous capture, because silently
// overwriting it would leak that capture's leases — and a leaked lease does not
// merely waste memory, it wedges its collection's writer once
// Options.MaxRetiredExtents fills.
func (d *Database) SnapshotInto(dst *DatabaseSnapshot) error {
	if dst == nil {
		return ErrDatabaseClosed
	}
	if err := dst.Close(); err != nil {
		return err
	}
	dst.entries = dst.entries[:0]
	if d == nil {
		return ErrDatabaseClosed
	}
	// The catalog lock is held throughout: it keeps the collection set stable
	// between choosing the lock order and releasing the last gate, which is what
	// makes the order a total order over a fixed set rather than over a set that
	// can grow mid-acquisition.
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return ErrDatabaseClosed
	}
	if len(d.order) == 0 {
		return nil
	}

	// d.order is already sorted by name, so the acquisition order is fixed by
	// the catalog rather than recomputed here. Unlocking is deferred in reverse
	// so an unexpected panic during the capture cannot leave the database
	// wedged with gates held.
	for _, entry := range d.order {
		entry.collection.snapshotGate.RLock()
	}
	defer func() {
		for i := len(d.order) - 1; i >= 0; i-- {
			d.order[i].collection.snapshotGate.RUnlock()
		}
	}()

	for _, entry := range d.order {
		snapshot, err := entry.collection.snapshotGateHeld()
		if err != nil {
			// A partial capture must pin nothing. The gates are still held, so
			// releasing here cannot race a reclaimer that is waiting on them.
			for i := range dst.entries {
				_ = dst.entries[i].snapshot.Close()
			}
			dst.entries = dst.entries[:0]
			return err
		}
		dst.entries = append(dst.entries, databaseSnapshotEntry{
			name: entry.name, snapshot: snapshot,
		})
	}
	return nil
}

// snapshotGateHeld acquires a generation lease with the caller already holding
// the collection's publication gate on its read side.
//
// It exists so a multi-collection capture can hold every gate across every
// lease acquisition. [Collection.Snapshot] cannot be reused for that: it takes
// and releases the gate itself, and releasing between two collections is
// precisely the window that makes two independent snapshots skew.
func (c *Collection) snapshotGateHeld() (*Snapshot, error) {
	if c == nil {
		return nil, ErrClosed
	}
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		return nil, stateErr
	}
	lease, err := c.leases.Acquire(state.root.Generation)
	if err != nil {
		return nil, err
	}
	return &Snapshot{collection: c, state: state, lease: lease}, nil
}

// Collection returns the captured view of name, reporting whether the
// collection was cataloged when the snapshot was taken. The returned snapshot
// stays owned by the DatabaseSnapshot; closing it individually is not required
// and closing it early would invalidate the cut for every other reader of the
// same capture.
func (s DatabaseSnapshot) Collection(name string) (*Snapshot, bool) {
	i, found := slices.BinarySearchFunc(
		s.entries, name,
		func(entry databaseSnapshotEntry, target string) int {
			return strings.Compare(entry.name, target)
		},
	)
	if !found {
		return nil, false
	}
	return s.entries[i].snapshot, true
}

// Len returns the number of collections the snapshot captured.
func (s DatabaseSnapshot) Len() int { return len(s.entries) }

// AppendNames appends the captured collection names to dst in name order.
func (s DatabaseSnapshot) AppendNames(dst []string) []string {
	for _, entry := range s.entries {
		dst = append(dst, entry.name)
	}
	return dst
}

// All iterates the captured collections in name order.
func (s DatabaseSnapshot) All(fn func(name string, snapshot *Snapshot) bool) {
	for _, entry := range s.entries {
		if !fn(entry.name, entry.snapshot) {
			return
		}
	}
}

// Close releases every captured collection's generation lease. It is
// idempotent, and it reports the first error while still closing the rest —
// a lease left held would wedge its collection's writer, which is worse than
// losing the second error message.
//
// The entry storage is kept so a [Database.SnapshotInto] loop stays warm; only
// the leases and the captured states are given back.
func (s *DatabaseSnapshot) Close() error {
	if s == nil {
		return nil
	}
	var first error
	for i := range s.entries {
		if err := s.entries[i].snapshot.Close(); err != nil && first == nil {
			first = err
		}
		s.entries[i].snapshot = nil
	}
	s.entries = s.entries[:0]
	return first
}
