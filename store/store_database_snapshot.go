package store

import (
	"slices"
	"strings"
)

// Multi-collection reads.
//
// A single collection publishes each new state through one atomic pointer, so
// Collection.Snapshot is a lock-free load. Loading several of those pointers one
// after another does not compose into a view of the database: a write to the
// second collection can land between the two loads, and the result is a pair
// of states that never coexisted. Reading one collection at generation 7 and
// another at generation 12 when generation 12 was published after 7 was
// replaced is snapshot skew, and a join across the two would resolve
// references against a state its own driving side never saw.
//
// Database.Snapshot closes that window by taking every collection's writer
// lock before loading any state. Writers already serialize on that lock, so
// while it is held no collection can be mid-commit, and the loads observe one
// instant. The locks are taken in collection-name order — a total order over a
// set the catalog lock is holding still — and no writer ever holds two
// collections' locks, so the acquisition cannot deadlock.
//
// The cost lands entirely on the reader. Nothing is added to the write path:
// no shared gate to acquire per mutation, no atomic beyond the ones a write
// already performs. A snapshot holds every writer lock for as long as it takes
// to load N pointers, and single-collection Collection.Snapshot is untouched.

// A DatabaseSnapshot is a logically immutable view of every collection in a
// [Database] at one instant. Its zero value is an empty snapshot.
//
// It is safe for concurrent use and remains valid independently of later
// mutations, of collections being dropped from the catalog, and of new
// collections being created. Each per-collection view carries the ordinary
// [Snapshot] contract.
//
// What it guarantees is the absence of skew: every collection is observed at
// the same instant, so a generation seen for one collection coexisted with the
// generation seen for every other. What it does not do is make separate writes
// atomic. A Database has no cross-collection transaction, so an application
// that writes one document to orders and then another to customers has
// performed two independent commits, and a snapshot may fall between them.
// The snapshot is consistent with respect to the database's own commits, not
// with respect to an application's intent spanning several of them.
type DatabaseSnapshot struct {
	// entries is ordered by name so lookup can binary-search it, and so a
	// snapshot's iteration order is the catalog's own stable order.
	entries []databaseSnapshotEntry
}

// A databaseSnapshotEntry binds one collection name to its captured view.
type databaseSnapshotEntry struct {
	name     string
	snapshot Snapshot
}

// Snapshot captures every cataloged collection at one instant.
//
// It briefly acquires every collection's writer lock, so it blocks concurrent
// mutation across the database for the duration of the capture — the time to
// load one pointer per collection — and a mutation already in progress
// delays it. Reads are unaffected: [Snapshot.GetRaw] and [Snapshot.Range] take
// no lock, and existing snapshots are untouched.
//
// A collection created after Snapshot returns is absent from the result, and
// one dropped afterwards remains present and valid.
func (d *Database) Snapshot() DatabaseSnapshot {
	if d == nil {
		return DatabaseSnapshot{}
	}
	// The catalog lock is held throughout: it keeps the collection set stable
	// between choosing the lock order and releasing the last lock, which is
	// what makes the order a total order over a fixed set rather than over a
	// set that can grow mid-acquisition.
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.collections) == 0 {
		return DatabaseSnapshot{}
	}

	collections := make([]*Collection, 0, len(d.collections))
	for _, collection := range d.collections {
		collections = append(collections, collection)
	}
	slices.SortFunc(collections, func(a, b *Collection) int {
		return strings.Compare(a.name, b.name)
	})

	// Every writer lock is held simultaneously, so no collection is between
	// deciding its next state and publishing it while any state is read.
	// Unlocking is deferred in reverse so an unexpected panic in the capture
	// cannot leave the database wedged.
	for _, collection := range collections {
		collection.mu.Lock()
	}
	defer func() {
		for i := len(collections) - 1; i >= 0; i-- {
			collections[i].mu.Unlock()
		}
	}()

	entries := make([]databaseSnapshotEntry, 0, len(collections))
	for _, collection := range collections {
		entries = append(entries, databaseSnapshotEntry{
			name:     collection.name,
			snapshot: Snapshot{state: collection.state.Load()},
		})
	}
	return DatabaseSnapshot{entries: entries}
}

// Collection returns the captured view of name, reporting whether the
// collection was cataloged when the snapshot was taken.
func (s DatabaseSnapshot) Collection(name string) (Snapshot, bool) {
	i, found := slices.BinarySearchFunc(
		s.entries, name,
		func(entry databaseSnapshotEntry, target string) int {
			return strings.Compare(entry.name, target)
		},
	)
	if !found {
		return Snapshot{}, false
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

// All iterates the captured collections in name order. It is the form a
// multi-collection reader uses when it needs every view rather than a named
// one.
func (s DatabaseSnapshot) All(fn func(name string, snapshot Snapshot) bool) {
	for _, entry := range s.entries {
		if !fn(entry.name, entry.snapshot) {
			return
		}
	}
}
