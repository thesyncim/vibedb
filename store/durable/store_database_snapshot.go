package durable

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// MaxSnapshotCollections bounds the caller-owned catalog accepted by
	// SnapshotCollectionsInto. A million members is already far beyond a
	// practical process-local durable catalog, while fixing the maximum retained
	// metadata and the amount of work one capture can request.
	MaxSnapshotCollections = 1 << 20
)

var (
	// ErrSnapshotCollectionsLimit reports an external catalog larger than the
	// reusable capture API's documented finite bound.
	ErrSnapshotCollectionsLimit = errors.New(
		"vibedb: durable snapshot collection limit exceeded",
	)
)

var nextCollectionSnapshotOrder atomic.Uint64

func (c *Collection) snapshotOrderID() uint64 {
	if id := c.snapshotOrder.Load(); id != 0 {
		return id
	}
	id := nextCollectionSnapshotOrder.Add(1)
	if c.snapshotOrder.CompareAndSwap(0, id) {
		return id
	}
	return c.snapshotOrder.Load()
}

func sortCollectionSnapshotOrder(collections []*Collection) {
	for _, collection := range collections {
		collection.snapshotOrderID()
	}
	slices.SortFunc(collections, func(a, b *Collection) int {
		left, right := a.snapshotOrder.Load(), b.snapshotOrder.Load()
		switch {
		case left < right:
			return -1
		case left > right:
			return 1
		default:
			return 0
		}
	})
}

// snapshotCutHold is the writer-and-gate hold seized across a no-skew database
// cut. It records how much of the cut [seizeSnapshotCut] acquired so [release]
// can undo exactly that much in LIFO order, whether the cut completed or a
// materialize aborted it midway.
type snapshotCutHold struct {
	order   []*Collection
	gates   int
	writers int
}

// seizeSnapshotCut holds the cut over collections given in a total lock order.
// It is the shared acquisition both database-cut entry points use:
// [Database.SnapshotInto] over its process-global snapshotOrder, and
// [SnapshotCollections] over the same handle order derived from caller-owned
// collections. Using one order everywhere is what lets overlapping catalogs that
// name the same handles differently avoid choosing opposite lock orders.
//
// It locks every writer first, then walks the collections a second time,
// materializing each deferred primary lane under its writer and immediately
// taking that collection's read publication gate. The materialize and the read
// gate interleave per collection, and that interleave is the whole correctness
// of the cut: materializePrimaryParentsLocked publishes under the collection's
// snapshotGate WRITE side, so if every materialize ran before any read gate, a
// capture that blocked materializing some member — contending an in-flight
// committer-callback promotion that takes the gate WITHOUT the writer lock —
// would wait there holding no gates, leaving every other member free to publish
// behind its back and skew the cut. Taking member X's read gate before
// materializing member X+1 means the capture already holds every earlier
// member's gate the instant it can block. That is exactly what
// TestDurableDatabaseSnapshotHoldsEveryGateAtOnce and
// TestSnapshotCollectionsHoldsEveryPublicationGate assert.
//
// By return every writer and every read gate is held; the caller pins each
// member with the whole cut seized, then calls release. Deadlock freedom: a
// blocked acquisition here waits only on an in-flight gate writer (a
// committer-callback promotion) that runs to completion without the writer lock
// or any gate this hold owns, so ordered acquisition over the fixed member set
// cannot cycle. On a materialize error the partial hold is released and the
// error returned.
func seizeSnapshotCut(order []*Collection) (snapshotCutHold, error) {
	hold := snapshotCutHold{order: order}
	for _, collection := range order {
		collection.writer.Lock()
		hold.writers++
	}
	for _, collection := range order {
		if collection.closed {
			hold.release()
			return snapshotCutHold{}, ErrClosed
		}
		if collection.deferredCanonicalLane() &&
			collection.primaryRouter.Load() != nil &&
			(len(collection.primaryPendingParents) != 0 ||
				collection.primaryUnifiedOverlay.hasPending()) {
			if err := collection.materializePrimaryParentsLocked(primaryMaterializationSnapshot); err != nil {
				hold.release()
				return snapshotCutHold{}, err
			}
		}
		collection.snapshotGate.RLock()
		hold.gates++
	}
	return hold, nil
}

// seizeSnapshotCutContext is the cancellable external-catalog variant. The
// ordinary Database and SnapshotCollections paths stay on seizeSnapshotCut, so
// opting out of cancellation adds no context branch to their acquisition loops.
func seizeSnapshotCutContext(
	ctx context.Context, order []*Collection,
) (snapshotCutHold, error) {
	hold := snapshotCutHold{order: order}
	for _, collection := range order {
		if err := ctx.Err(); err != nil {
			hold.release()
			return snapshotCutHold{}, err
		}
		collection.writer.Lock()
		hold.writers++
		if err := ctx.Err(); err != nil {
			hold.release()
			return snapshotCutHold{}, err
		}
	}
	for _, collection := range order {
		if err := ctx.Err(); err != nil {
			hold.release()
			return snapshotCutHold{}, err
		}
		if collection.closed {
			hold.release()
			return snapshotCutHold{}, ErrClosed
		}
		if collection.deferredCanonicalLane() &&
			collection.primaryRouter.Load() != nil &&
			(len(collection.primaryPendingParents) != 0 ||
				collection.primaryUnifiedOverlay.hasPending()) {
			if err := collection.materializePrimaryParentsLocked(primaryMaterializationSnapshot); err != nil {
				hold.release()
				return snapshotCutHold{}, err
			}
		}
		if err := ctx.Err(); err != nil {
			hold.release()
			return snapshotCutHold{}, err
		}
		collection.snapshotGate.RLock()
		hold.gates++
		if err := ctx.Err(); err != nil {
			hold.release()
			return snapshotCutHold{}, err
		}
	}
	return hold, nil
}

func snapshotCaptureContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// release undoes the hold in LIFO order — read gates first, then writers — over
// exactly the prefix that was acquired. Releasing the gates before the writers
// keeps a reclaimer waiting on a gate from proceeding until no writer this cut
// held could still be mid-publish.
func (h snapshotCutHold) release() {
	for i := h.gates - 1; i >= 0; i-- {
		h.order[i].snapshotGate.RUnlock()
	}
	for i := h.writers - 1; i >= 0; i-- {
		h.order[i].writer.Unlock()
	}
}

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
// It does not invent atomicity for independent point writes. An application
// that puts one document into orders through Collection.Update and then
// another into customers through a second Update has performed two commits, and
// a snapshot may legitimately fall between them. Database.Update (and
// UpdateCollections) is the cross-collection transaction: its decision-log
// sync is the sole commit point, every participant publishes under gates held
// at once, and this cut therefore never observes a torn multi-collection
// commit. The snapshot is consistent with respect to the database's own
// commits — including those transactions — not with respect to an
// application's intent that spans several independent ones.
//
// It does not survive a crash as a lease. A lease is process memory, not a
// durable reservation. After a restart OpenDatabase reconciles txn.vtm against
// each collection's journal so every decided multi-collection transaction is
// all-committed or all-aborted across participants; independent
// single-collection commits recover per file as before. Consistency of this
// snapshot value is still a read-time property of a running process.
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

	// storage is the destination-owned high-water entry bank. entries is either
	// empty or a published prefix of storage. Keeping the bank separate lets a
	// capture stage entries without exposing a partial result and preserves each
	// entry's reusable Snapshot object after Close.
	storage []databaseSnapshotEntry

	// ordered and gateOrder are external-catalog capture scratch. They are
	// cleared before every return so a reusable destination never retains caller
	// strings or collection handles after the capture.
	ordered   []NamedCollection
	gateOrder []*Collection
}

// A databaseSnapshotEntry binds one collection name to its captured view.
type databaseSnapshotEntry struct {
	name string
	// snapshot is non-nil only while this entry is published. storage remains
	// owned by the destination across Close and is rebound to the next captured
	// generation without allocating.
	snapshot *Snapshot
	storage  *Snapshot
}

// A NamedCollection binds an application-owned collection handle to the name
// a [DatabaseSnapshot] uses to resolve it.
//
// It is the adapter for catalogs that own durable collections themselves
// instead of using [Database]. Collection may be nil to represent a cataloged
// collection that is still logically empty and has no backing file yet.
type NamedCollection struct {
	Name       string
	Collection *Collection
	// BatchDocumentsHint bounds only the initial dedup-map reservation used by
	// UpdateCollections. Zero preserves the collection-wide maximum. A precise
	// hot-path hint avoids reserving a cold maintenance batch (for example a
	// retry-ring release) on every small transaction; the batch still grows up
	// to the collection's hard MaxBatchDocuments limit.
	BatchDocumentsHint int
}

// SnapshotCollections captures application-owned collections at one instant.
//
// It provides the same no-skew cut as [Database.Snapshot], but leaves catalog
// ownership with the caller. The input slice is copied and sorted; callers may
// reuse it as soon as this function returns. Names must be non-empty and
// unique, and one collection handle may not be published under two names.
// Gates are acquired by a process-global collection identity, not by these
// caller-selected names, so overlapping captures cannot choose opposite lock
// orders when two catalogs name the same handles differently.
//
// A nil Collection is retained as a cataloged empty collection. This supports
// catalogs that defer allocating a durable file until the first write without
// making an empty relation indistinguishable from an absent one.
//
// The returned snapshot owns one generation lease per non-empty collection
// and must be closed.
func SnapshotCollections(collections []NamedCollection) (DatabaseSnapshot, error) {
	if len(collections) == 0 {
		return DatabaseSnapshot{}, nil
	}
	ordered := slices.Clone(collections)
	slices.SortFunc(ordered, func(a, b NamedCollection) int {
		return strings.Compare(a.Name, b.Name)
	})
	seen := make(map[*Collection]string, len(ordered))
	for i := range ordered {
		entry := &ordered[i]
		if entry.Name == "" {
			return DatabaseSnapshot{}, ErrCollectionName
		}
		if i != 0 && ordered[i-1].Name == entry.Name {
			return DatabaseSnapshot{}, ErrCollectionExists
		}
		if entry.Collection == nil {
			continue
		}
		if previous, exists := seen[entry.Collection]; exists {
			return DatabaseSnapshot{}, fmt.Errorf(
				"vibedb: one durable collection cannot be cataloged as both %q and %q",
				previous, entry.Name)
		}
		seen[entry.Collection] = entry.Name
	}

	gateOrder := make([]*Collection, 0, len(seen))
	for collection := range seen {
		gateOrder = append(gateOrder, collection)
	}
	sortCollectionSnapshotOrder(gateOrder)

	// Seize the whole cut over the process-global handle order, then pin the
	// members with every writer and publication gate held at once. Rejecting one
	// handle under two names above is what keeps this read-locking each gate at
	// most once, never recursively across a waiting writer.
	hold, err := seizeSnapshotCut(gateOrder)
	if errors.Is(err, ErrCheckpointGroupPressure) {
		if group := commonCheckpointGroup(gateOrder); group != nil {
			if checkpointErr := group.Checkpoint(); checkpointErr != nil {
				return DatabaseSnapshot{}, errors.Join(err, checkpointErr)
			}
			hold, err = seizeSnapshotCut(gateOrder)
		}
	}
	if err != nil {
		return DatabaseSnapshot{}, err
	}
	defer hold.release()

	result := DatabaseSnapshot{
		entries: make([]databaseSnapshotEntry, 0, len(ordered)),
	}
	for i := range ordered {
		entry := &ordered[i]
		var snapshot *Snapshot
		if entry.Collection != nil {
			var err error
			snapshot, err = entry.Collection.snapshotGateHeld()
			if err != nil {
				_ = result.Close()
				return DatabaseSnapshot{}, err
			}
		}
		result.entries = append(result.entries, databaseSnapshotEntry{
			name: strings.Clone(entry.Name), snapshot: snapshot,
		})
	}
	return result, nil
}

// SnapshotCollectionsInto captures application-owned collections into reusable
// caller-owned storage. It has the same name, ordering, nil-collection, cut,
// and lease semantics as [SnapshotCollections], with a finite catalog bound of
// [MaxSnapshotCollections].
//
// dst is closed before validation or capture starts. On every return with an
// error its previous capture and every partially acquired new lease have been
// released, Len reports zero, and no collection is published through dst.
// Successful calls copy names into destination-owned immutable strings, so the
// input slice and its strings may be reused immediately. After one successful
// call for a stable catalog, the take/use/close loop allocates nothing: dst
// retains its ordering buffers, names, entry bank, and one Snapshot object per
// materialized collection.
//
// dst is single-consumer and must not be copied or accessed concurrently with
// this call. A nil Collection remains a cataloged empty collection, exactly as
// in SnapshotCollections.
func SnapshotCollectionsInto(
	dst *DatabaseSnapshot, collections []NamedCollection,
) error {
	return snapshotCollectionsInto(nil, dst, collections)
}

// SnapshotCollectionsIntoContext is [SnapshotCollectionsInto] with cooperative
// cancellation before and after ordering, while acquiring each writer/gate,
// and while pinning each collection. Cancellation never publishes a partial
// destination. A cancellation that arrives while waiting for a mutex is
// observed immediately after that mutex is acquired; Go mutexes do not expose
// a cancellable blocking acquisition.
func SnapshotCollectionsIntoContext(
	ctx context.Context,
	dst *DatabaseSnapshot,
	collections []NamedCollection,
) error {
	if ctx == nil {
		return snapshotCollectionsInto(nil, dst, collections)
	}
	return snapshotCollectionsInto(ctx, dst, collections)
}

func snapshotCollectionsInto(
	ctx context.Context,
	dst *DatabaseSnapshot,
	collections []NamedCollection,
) error {
	if dst == nil {
		return fmt.Errorf(
			"vibedb: SnapshotCollectionsInto requires non-nil destination",
		)
	}
	if err := dst.Close(); err != nil {
		return err
	}
	if err := snapshotCaptureContextErr(ctx); err != nil {
		return err
	}
	if len(collections) > MaxSnapshotCollections {
		return fmt.Errorf(
			"%w: %d > %d",
			ErrSnapshotCollectionsLimit, len(collections),
			MaxSnapshotCollections,
		)
	}
	if len(collections) == 0 {
		return nil
	}

	dst.ordered = slices.Grow(dst.ordered[:0], len(collections))
	dst.ordered = append(dst.ordered, collections...)
	defer dst.clearSnapshotCollectionScratch()

	slices.SortFunc(dst.ordered, func(a, b NamedCollection) int {
		return strings.Compare(a.Name, b.Name)
	})
	if err := snapshotCaptureContextErr(ctx); err != nil {
		return err
	}

	dst.gateOrder = slices.Grow(dst.gateOrder[:0], len(collections))
	for i := range dst.ordered {
		entry := &dst.ordered[i]
		if entry.Name == "" {
			return ErrCollectionName
		}
		if i != 0 && dst.ordered[i-1].Name == entry.Name {
			return ErrCollectionExists
		}
		if entry.Collection != nil {
			dst.gateOrder = append(dst.gateOrder, entry.Collection)
		}
		if err := snapshotCaptureContextErr(ctx); err != nil {
			return err
		}
	}

	sortCollectionSnapshotOrder(dst.gateOrder)
	if err := snapshotCaptureContextErr(ctx); err != nil {
		return err
	}
	for i := 1; i < len(dst.gateOrder); i++ {
		if dst.gateOrder[i-1] != dst.gateOrder[i] {
			continue
		}
		first, second := duplicateCollectionNames(
			dst.ordered, dst.gateOrder[i],
		)
		return fmt.Errorf(
			"vibedb: one durable collection cannot be cataloged as both %q and %q",
			first, second,
		)
	}

	staged := dst.prepareSnapshotEntries(len(dst.ordered))
	for i := range dst.ordered {
		input := &dst.ordered[i]
		entry := &staged[i]
		if entry.name != input.Name {
			entry.name = strings.Clone(input.Name)
		}
		entry.snapshot = nil
		if input.Collection != nil && entry.storage == nil {
			entry.storage = new(Snapshot)
		}
	}
	if err := snapshotCaptureContextErr(ctx); err != nil {
		return err
	}

	var (
		hold snapshotCutHold
		err  error
	)
	if ctx == nil {
		hold, err = seizeSnapshotCut(dst.gateOrder)
	} else {
		hold, err = seizeSnapshotCutContext(ctx, dst.gateOrder)
	}
	if errors.Is(err, ErrCheckpointGroupPressure) {
		if group := commonCheckpointGroup(dst.gateOrder); group != nil {
			if checkpointErr := group.Checkpoint(); checkpointErr != nil {
				return errors.Join(err, checkpointErr)
			}
			if ctx == nil {
				hold, err = seizeSnapshotCut(dst.gateOrder)
			} else {
				hold, err = seizeSnapshotCutContext(ctx, dst.gateOrder)
			}
		}
	}
	if err != nil {
		return err
	}
	defer hold.release()

	for i := range dst.ordered {
		if err := snapshotCaptureContextErr(ctx); err != nil {
			closeDatabaseSnapshotEntries(staged)
			return err
		}
		input := &dst.ordered[i]
		if input.Collection == nil {
			continue
		}
		entry := &staged[i]
		if err := input.Collection.snapshotGateHeldInto(entry.storage); err != nil {
			closeDatabaseSnapshotEntries(staged)
			return err
		}
		entry.snapshot = entry.storage
	}
	if err := snapshotCaptureContextErr(ctx); err != nil {
		closeDatabaseSnapshotEntries(staged)
		return err
	}

	// Publication is one slice-header assignment after every member is pinned.
	dst.entries = staged
	return nil
}

func commonCheckpointGroup(collections []*Collection) *CheckpointGroup {
	var group *CheckpointGroup
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		current := collection.checkpointGroup.Load()
		if current == nil {
			return nil
		}
		if group == nil {
			group = current
			continue
		}
		if group != current {
			return nil
		}
	}
	return group
}

func duplicateCollectionNames(
	ordered []NamedCollection, collection *Collection,
) (first, second string) {
	for i := range ordered {
		if ordered[i].Collection != collection {
			continue
		}
		if first == "" {
			first = ordered[i].Name
			continue
		}
		return first, ordered[i].Name
	}
	return first, second
}

func (s *DatabaseSnapshot) prepareSnapshotEntries(
	count int,
) []databaseSnapshotEntry {
	if len(s.storage) < count {
		s.storage = slices.Grow(s.storage, count-len(s.storage))
		s.storage = s.storage[:count]
	}
	return s.storage[:count]
}

func (s *DatabaseSnapshot) clearSnapshotCollectionScratch() {
	clear(s.ordered)
	s.ordered = s.ordered[:0]
	clear(s.gateOrder)
	s.gateOrder = s.gateOrder[:0]
}

func closeDatabaseSnapshotEntries(entries []databaseSnapshotEntry) error {
	var first error
	for i := range entries {
		if entries[i].snapshot != nil {
			if err := entries[i].snapshot.Close(); err != nil && first == nil {
				first = err
			}
		}
		entries[i].snapshot = nil
	}
	return first
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
// reusing dst's entry bank and one Snapshot object per collection. It is the
// form a query loop uses: after the destination reaches the catalog's high-water
// size, taking and closing a stable catalog allocates nothing.
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

	// Seize the cut over the process-global handle order (snapshotOrder, kept in
	// lock step with the name-ordered d.order by reorder so this stays
	// allocation-free). Every writer and publication gate is held on return, the
	// same interleaved materialize-then-gate discipline SnapshotCollections uses;
	// see seizeSnapshotCut for why the two must interleave to keep the cut from
	// skewing. Handle order rather than name order is what lets a capture that
	// overlaps a caller-owned SnapshotCollections avoid the opposite lock order.
	staged := dst.prepareSnapshotEntries(len(d.order))
	for i, source := range d.order {
		entry := &staged[i]
		entry.name = source.name
		entry.snapshot = nil
		if entry.storage == nil {
			entry.storage = new(Snapshot)
		}
	}

	hold, err := seizeSnapshotCut(d.snapshotOrder)
	if err != nil {
		return err
	}
	defer hold.release()

	// Final acquisition — pin every member's state and acquire its lease with every gate
	// held at once, so the captured generations are a set that genuinely
	// coexisted: no member could publish between the first pin and the last.
	for i, source := range d.order {
		entry := &staged[i]
		err := source.collection.snapshotGateHeldInto(entry.storage)
		if err != nil {
			// A partial capture must pin nothing. The gates are still held, so
			// releasing here cannot race a reclaimer that is waiting on them.
			_ = closeDatabaseSnapshotEntries(staged)
			return err
		}
		entry.snapshot = entry.storage
	}
	dst.entries = staged
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
	snapshot := new(Snapshot)
	if err := c.snapshotGateHeldFreshInto(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// snapshotGateHeldInto is snapshotGateHeld using reusable caller-owned
// Snapshot storage. The caller holds the collection's publication gate and
// writer lock as part of a multi-collection cut.
func (c *Collection) snapshotGateHeldInto(snapshot *Snapshot) error {
	if c == nil {
		return ErrClosed
	}
	if snapshot == nil {
		return fmt.Errorf("vibedb: snapshot capture requires non-nil storage")
	}
	if err := snapshot.Close(); err != nil {
		return err
	}
	snapshot.once = sync.Once{}
	return c.snapshotGateHeldFreshInto(snapshot)
}

// snapshotGateHeldFreshInto binds unowned storage. The caller has already
// validated/reset the destination and holds the multi-collection cut.
func (c *Collection) snapshotGateHeldFreshInto(snapshot *Snapshot) error {
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		return stateErr
	}
	// The catalog and epoch pointer must come from one publication generation,
	// exactly as in pinSnapshot. Online CREATE INDEX replaces both under
	// snapshotGate, so a database-wide capture cannot read either later.
	indexes := c.options.indexes
	indexNameIDs := c.options.indexNameIDs
	indexDefinitions := c.options.Indexes
	epoch := c.primaryEpoch
	primaryRouter := c.primaryRouter.Load()
	lease, err := c.leases.Acquire(state.root.Generation)
	if err != nil {
		return publicReaderLeaseError(err)
	}
	snapshot.collection = c
	snapshot.state = state
	snapshot.primaryRouter = primaryRouter
	snapshot.indexes = indexes
	snapshot.indexNameIDs = indexNameIDs
	snapshot.indexDefinitions = indexDefinitions
	snapshot.epoch = epoch
	snapshot.lease = lease
	return nil
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
// the leases and the captured states are given back. Names, ordering scratch,
// reusable Snapshot objects, and their scan/index scratch remain at their
// observed high-water capacities. Call [DatabaseSnapshot.Release] when the
// destination is leaving a pool or a rare broad capture should not pin that
// memory.
func (s *DatabaseSnapshot) Close() error {
	if s == nil {
		return nil
	}
	first := closeDatabaseSnapshotEntries(s.entries)
	s.entries = s.entries[:0]
	return first
}

// Release closes the capture and drops every destination-owned reusable
// buffer, name, and Snapshot object. It is idempotent. The zeroed destination
// remains reusable, but its next capture pays cold allocation costs again.
//
// Close is normally preferable in an execution loop because it retains the
// catalog and each child Snapshot's scan reconstruction buffers. Release is the
// deterministic memory-lifetime boundary for pool eviction and one-off high
// cardinality captures.
func (s *DatabaseSnapshot) Release() error {
	if s == nil {
		return nil
	}
	first := s.Close()
	for i := range s.storage {
		entry := &s.storage[i]
		if entry.storage != nil {
			if err := entry.storage.Close(); err != nil && first == nil {
				first = err
			}
			*entry.storage = Snapshot{}
		}
		*entry = databaseSnapshotEntry{}
	}
	clear(s.ordered)
	clear(s.gateOrder)
	s.entries = nil
	s.storage = nil
	s.ordered = nil
	s.gateOrder = nil
	return first
}
