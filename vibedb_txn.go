package vibedb

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/internal/txnclock"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrTxConflict reports that another committed write changed a key or coarse
	// collection dependency in this serializable transaction after Begin.
	// Nothing was published; the caller owns the retry loop.
	ErrTxConflict = errors.New("vibedb: transaction conflict")
	// ErrTxTooLarge reports that a transaction exceeded a bounded limit
	// (collections, documents, or bytes). Nothing was staged or published.
	ErrTxTooLarge = errors.New("vibedb: transaction exceeds a bounded limit")
	// ErrTxDone reports use of a finished or escaped transaction handle.
	ErrTxDone = errors.New("vibedb: transaction is finished")
	// ErrTxReadOnly reports a mutation attempted in a read-only transaction.
	ErrTxReadOnly = errors.New("vibedb: mutation in a read-only transaction")
	// ErrTxUnsupportedLane reports a multi-collection commit on a durability
	// lane the database transaction protocol refuses. It is the durable
	// sentinel so errors.Is matches across the facade and engine.
	ErrTxUnsupportedLane = durable.ErrDatabaseTransactionUnsupportedLane
	// ErrCommitOutcomeUnknown reports the sole unknown-outcome window of a
	// multi-collection COMMIT: the decision record's sync. The unknown outcome
	// is atomic — reopen reveals either every participating collection's
	// writes or none of them. Every collection handle under the catalog
	// refuses further writes with the sticky persistence failure until the
	// database is closed and reopened.
	ErrCommitOutcomeUnknown = durable.ErrCommitOutcomeUnknown
	// ErrTxNested reports Database.Update or Database.View re-entered on the
	// same goroutine. Native transactions have no savepoints; compose by
	// control flow instead.
	ErrTxNested = errors.New("vibedb: nested database transaction")
)

const (
	defaultFacadeMaxBatchDocuments = store.MaxChunkDocuments
	defaultFacadeBatchValueBytes   = 16 << 20
	defaultFacadeMaxBatchBytes     = defaultFacadeMaxBatchDocuments*defaultMaxKeyBytes +
		defaultFacadeBatchValueBytes
	maxSerializableReadKeys           = 4096
	maxSerializableReadBytes          = 1 << 20
	maxSerializableReadCollections    = 128
	maxSerializableHistoryCollections = maxSerializableReadCollections
	maxTxnActiveCount                 = ^uint64(0)
	maxTxnRevision                    = ^uint64(0)
)

// Update runs fn inside a read-write transaction. On a nil return it commits;
// on an error it rolls back; on panic it rolls back and re-panics. An escaped
// or finished *Tx is inert (ErrTxDone).
func (d *Database) Update(fn func(*Tx) error) error {
	return d.runClosure(false, fn)
}

// View runs fn inside a read-only coherent cut. Commit publishes nothing;
// mutations refuse with ErrTxReadOnly.
func (d *Database) View(fn func(*Tx) error) error {
	return d.runClosure(true, fn)
}

func (d *Database) runClosure(readOnly bool, fn func(*Tx) error) (err error) {
	if d == nil {
		return ErrClosed
	}
	if fn == nil {
		return fmt.Errorf("%w: nil transaction function", ErrInvalidOptions)
	}
	if err := d.enterUpdateClosure(); err != nil {
		return err
	}
	defer d.leaveUpdateClosure()

	var tx *Tx
	if readOnly {
		tx, err = d.BeginReadOnly()
	} else {
		tx, err = d.Begin()
	}
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Begin starts a serializable read-write transaction: it samples and arms one
// database-global logical clock before lazily capturing participating
// collections. Commit validates those reads even if no writes were staged.
func (d *Database) Begin() (*Tx, error) {
	return d.begin(false)
}

// BeginReadOnly starts a read-only transaction over one coherent cut. The
// logical clock is not armed and commit needs no validation.
func (d *Database) BeginReadOnly() (*Tx, error) {
	return d.begin(true)
}

func (d *Database) begin(readOnly bool) (*Tx, error) {
	if d == nil || d.closed.Load() {
		return nil, ErrClosed
	}
	// colls stays nil until a second distinct collection needs it: a lone
	// collection state lives in solo, so single-collection transactions
	// (the common 1-key commit) pay no map make, insert, or bucket.
	tx := &Tx{
		db:       d,
		readOnly: readOnly,
	}
	if !readOnly {
		d.armClockForBegin(tx)
	}
	if err := tx.captureCut(); err != nil {
		if !readOnly {
			d.finishClock(tx, nil)
		}
		return nil, err
	}
	return tx, nil
}

// Tx holds collection snapshots plus bounded per-collection overlays.
// A Tx must not be copied after first use.
type Tx struct {
	db       *Database
	readOnly bool
	done     bool

	// Read-only transactions retain one coherent database cut. Their states
	// borrow its durable snapshots; read-write states own independent snapshots.
	diskCut *durable.DatabaseSnapshot

	// colls holds collection states once a transaction touches a second
	// distinct collection. The first lives in solo: lookups compare one
	// name and every per-state loop below visits solo plus the map, so a
	// lone collection never buys map storage.
	colls map[string]*txCollectionState
	solo  *txCollectionState

	beginRev   uint64
	clockArmed bool
	// clockShard selects this transaction's stripe of the live-transaction
	// directory; clockInserted reports whether arm registered an entry
	// there (a saturated coordinator arms without registering).
	clockShard    uint64
	clockInserted bool

	// Exact read tracking is bounded across the transaction. A collection
	// adaptively escalates to its coarse marker before either bound is crossed.
	readKeys        int
	readBytes       int
	readCollections int
	dynamicStates   int

	// stagedTotals track cross-collection admission against TxnLimits.
	stagedDocs  int
	stagedBytes int64
	dirtyCount  int
}

type txCollectionState struct {
	name string
	// coll is the facade handle this state was ensured through, cached so
	// commit-time paths (fence acquisition, validation, publication) reuse
	// it instead of resolving the catalog handle per phase under handlesMu.
	coll *Collection

	diskSnap *durable.Snapshot
	heapSnap store.Snapshot
	hasHeap  bool
	absent   bool // not present in the captured collection view

	pending map[string]*txMutation
	order   []string

	stagedDocs  int
	stagedBytes int

	maxDocs  int
	maxBytes int
	maxKey   int
	maxDoc   int

	overlaySource query.FileOverlaySource

	// readSet maps each exactly-read key to its owned copy. Staging reuses
	// the owned copy instead of owning the key a second time, so a key
	// staged after being read costs one clone rather than two.
	readSet     map[string]string
	readOrder   []string
	coarseRead  bool
	readTracked bool

	keyChunk  []byte
	keyChunks [][]byte
	canonical []byte
}

type txMutation struct {
	document []byte
	remove   bool
	existed  bool
}

// txnActiveShardCount stripes the live-transaction directory so concurrent
// Begins land on different shard locks. It is a power of two so assignment is
// a mask over a round-robin tick; shards carry no affinity, only contention
// relief, so any transaction may use any stripe.
const txnActiveShardCount = 64

// txnActiveShard is one stripe of the live-transaction directory: a revision
// bucket map owned by its own mutex. Stripes are independent; cross-stripe
// scans retain a conservative oldest bound because registration samples its
// revision while holding the stripe lock. The database clock gate excludes
// quiescent cleanup, and the global holder count uses saturating CAS updates.
type txnActiveShard struct {
	mu   sync.Mutex
	revs map[uint64]uint64
}

// Conflict histories are stored as *txnclock.ExternalHistory values in the
// database's txnHistories sync.Map. No per-history lock is needed: every
// history mutation runs while holding that collection's txnFence, as does
// every validation that reads it, so same-collection access is already
// exclusive. The rare paths that drop histories they do not hold a fence for
// (capacity overflow, quiescence) only delete map keys and never mutate a
// history object in place, so a validator holding a loaded pointer always
// sees a stable object.

// Collection returns the transaction view of name. It never errors; invalid
// names and finished transactions fail on the first operation instead.
func (t *Tx) Collection(name string) *TxCollection {
	if t == nil {
		return &TxCollection{initialErr: ErrTxDone}
	}
	if t.done {
		return &TxCollection{name: name, tx: t, initialErr: ErrTxDone}
	}
	if !validCollectionName(name) {
		return &TxCollection{name: name, tx: t, initialErr: ErrInvalidCollectionName}
	}
	state, err := t.ensureCollection(name)
	return &TxCollection{name: name, tx: t, state: state, initialErr: err}
}

// Commit validates conflicts, publishes the dirty set, records into conflict
// clocks, and finishes the transaction. A second Commit returns ErrTxDone.
func (t *Tx) Commit() error {
	if t == nil || t.done {
		return ErrTxDone
	}
	if t.readOnly {
		t.finish(nil)
		return nil
	}
	db := t.db
	if db == nil || db.closed.Load() {
		t.finish(nil)
		return ErrClosed
	}

	dirty := t.dirtyStates()
	targets := t.targetStates()
	if len(targets) == 0 {
		t.finish(nil)
		return nil
	}

	// Release read leases before taking the commit mutex so writers are not
	// blocked by generation leases during validation/publication.
	t.releaseCuts()

	// Choose the commit lock mode before locking. A transaction confined to a
	// single collection (its only participant is its only dirty collection)
	// cannot create a cross-collection read-write cycle with another confined
	// transaction on a different collection, so shared mode is sufficient;
	// same-collection peers still serialize on that collection's txnFence.
	// Anything wider (cross-collection reads, multi-collection writes) takes
	// the exclusive mode to preserve serializable validation.
	sharedCommit := len(dirty) == 1 && len(targets) == 1 &&
		targets[0].name == dirty[0].name
	if sharedCommit {
		db.commitMu.RLock()
		defer db.commitMu.RUnlock()
	} else {
		db.commitMu.Lock()
		defer db.commitMu.Unlock()
	}
	// dirty is name-sorted, so every transaction takes collection fences in one
	// stable order. Keep them through conflict validation, materialization,
	// publication, and conflict-clock recording. A direct Put/Delete on a dirty
	// collection therefore linearizes wholly before validation or after COMMIT;
	// writes to unrelated collections remain independent.
	lockedCollections := make([]*Collection, 0, len(targets))
	for _, state := range targets {
		// The handle is cached at ensure time: every participant state
		// carries the catalog handle it resolved through.
		collection := state.coll
		collection.txnFence.Lock()
		lockedCollections = append(lockedCollections, collection)
	}
	defer func() {
		for i := len(lockedCollections) - 1; i >= 0; i-- {
			lockedCollections[i].txnFence.Unlock()
		}
	}()
	if db.closed.Load() {
		t.finish(nil)
		return ErrClosed
	}

	if db.profile == Buffered && len(dirty) >= 2 {
		t.finish(nil)
		return fmt.Errorf(
			"%w: Buffered profile refuses multi-collection transactions",
			ErrTxUnsupportedLane,
		)
	}

	if err := t.validateDependencies(targets); err != nil {
		t.finish(nil)
		return err
	}
	if len(dirty) == 0 {
		// Lazy collection snapshots need validation even without publication:
		// successive reads may otherwise straddle one atomic database commit.
		t.finish(nil)
		return nil
	}
	for _, state := range dirty {
		if err := t.validateState(state); err != nil {
			t.finish(nil)
			return err
		}
	}
	if hook := db.testAfterTxValidation; hook != nil {
		hook()
	}
	for _, state := range dirty {
		if !state.absent {
			continue
		}
		if _, _, err := state.coll.backend(true); err != nil {
			t.finish(nil)
			return err
		}
		state.absent = false
	}

	var commitErr error
	published := t.publicationKeys(dirty)
	switch {
	case len(dirty) == 1 && db.profile != Memory:
		commitErr = t.commitSingleDurable(dirty[0])
	case db.profile == Memory:
		commitErr = t.commitMemory(dirty)
	default:
		commitErr = t.commitMultiDurable(dirty)
	}
	if commitErr != nil {
		// Once publication has been attempted, conservatively advance logical
		// history even when the storage outcome is unknown. Ordinary failures may
		// create a false conflict for an already-open transaction; omitting this
		// edge could allow an uncertain durable publication to go undetected.
		t.finish(published)
		return facadeTxnError(commitErr)
	}

	t.finish(published)
	return nil
}

// txPublishedKeys is one collection's contribution to a commit's conflict
// history: the keys whose publication concurrent validators must observe.
// A slice replaces the old name-keyed map because publication recording
// assigns one global revision to the whole commit and never looks anything
// up by name; the map was pure per-commit overhead.
type txPublishedKeys struct {
	name string
	keys []string
}

func (t *Tx) publicationKeys(dirty []*txCollectionState) []txPublishedKeys {
	var published []txPublishedKeys
	for _, state := range dirty {
		var keys []string
		for _, key := range state.order {
			entry := state.pending[key]
			if !entry.existed && entry.remove {
				continue
			}
			keys = append(keys, key)
		}
		if len(keys) > 0 {
			published = append(published, txPublishedKeys{name: state.name, keys: keys})
		}
	}
	return published
}

// Rollback discards staged writes and finishes the transaction. After Commit it
// is a nil no-op.
func (t *Tx) Rollback() error {
	if t == nil || t.done {
		return nil
	}
	t.finish(nil)
	return nil
}

func (t *Tx) finish(published []txPublishedKeys) {
	if t.done {
		return
	}
	t.done = true
	t.releaseCuts()
	if t.db != nil && !t.readOnly {
		t.db.finishClock(t, published)
	}
	t.scrubStates()
	t.db = nil
	t.colls = nil
	t.solo = nil
}

func (s *txCollectionState) scrub() {
	if s.diskSnap != nil {
		_ = s.diskSnap.Close()
		s.diskSnap = nil
	}
	s.heapSnap = store.Snapshot{}
	s.hasHeap = false
	s.coll = nil
	s.pending = nil
	s.order = nil
	s.overlaySource = query.FileOverlaySource{}
	s.readSet = nil
	s.readOrder = nil
	s.keyChunk = nil
	s.keyChunks = nil
	s.canonical = nil
}

func (t *Tx) scrubStates() {
	if t.solo != nil {
		t.solo.scrub()
	}
	for _, state := range t.colls {
		state.scrub()
	}
}

func (t *Tx) releaseCuts() {
	// Read-only states borrow from the database cut; read-write states own
	// their lazily captured snapshots. Close each durable lease exactly once.
	ownedByCut := t.diskCut != nil
	if ownedByCut {
		_ = t.diskCut.Close()
		t.diskCut = nil
	}
	if t.solo != nil {
		t.solo.releaseCut(ownedByCut)
	}
	for _, state := range t.colls {
		state.releaseCut(ownedByCut)
	}
}

// captureCut preserves a coherent database snapshot for read-only transactions,
// which neither track dependencies nor validate at Commit. The backend holds
// every collection's publication gate while acquiring this cut, including for
// disjoint commits that share the facade commit lock.
//
// Read-write transactions capture collection snapshots lazily on first touch.
// Their Begin stays O(1); beginRev-anchored validation at Commit rejects any
// fractured reads, including transactions with no publishable writes.
// releaseCut drops one state's snapshot leases. States borrowed from the
// database cut keep the cut's lease; read-write states own and close theirs.
func (s *txCollectionState) releaseCut(ownedByCut bool) {
	if s.diskSnap != nil {
		if !ownedByCut {
			_ = s.diskSnap.Close()
		}
		s.diskSnap = nil
	}
	s.heapSnap = store.Snapshot{}
	s.hasHeap = false
}

// pinState records a newly created collection state. The first state of a
// read-write transaction lives in solo without map storage; the second
// distinct name migrates it into a lazily made map. Read-only cuts populate
// every collection up front, so they go straight to the map.
func (t *Tx) pinState(state *txCollectionState) {
	if !t.readOnly && t.solo == nil && t.colls == nil {
		t.solo = state
		return
	}
	if t.colls == nil {
		t.colls = make(map[string]*txCollectionState)
	}
	if t.solo != nil {
		t.colls[t.solo.name] = t.solo
		t.solo = nil
	}
	t.colls[state.name] = state
}

func (t *Tx) captureCut() error {
	db := t.db
	if db.profile != Memory && db.disk == nil {
		return ErrClosed
	}
	if !t.readOnly {
		return nil
	}
	if db.profile == Memory {
		cut := db.heap.Snapshot()
		cut.All(func(name string, snap store.Snapshot) bool {
			state := t.newCollectionState(name)
			state.heapSnap = snap
			state.hasHeap = true
			t.pinState(state)
			return true
		})
		return nil
	}
	cut, err := db.disk.Snapshot()
	if err != nil {
		return facadeError(err)
	}
	t.diskCut = &cut
	cut.All(func(name string, snap *durable.Snapshot) bool {
		state := t.newCollectionState(name)
		state.diskSnap = snap
		state.absent = snap == nil
		t.pinState(state)
		return true
	})
	return nil
}

func (t *Tx) newCollectionState(name string) *txCollectionState {
	maxDocs, maxBytes := t.db.batchBounds(name)
	maxKey, maxDoc := t.db.documentBounds(name)
	return newTxCollectionState(name, maxDocs, maxBytes, maxKey, maxDoc)
}

// newCollectionStateResolved builds the state for a read-write touch from an
// already-resolved backend instead of repeating catalog lookups. Document
// limits reuse Collection.bounds so transactions admit exactly what direct
// writes admit; batch limits match batchBounds (constants for memory,
// persisted limits for a resolved durable collection, engine fallbacks when
// absent). A collection dropped concurrently between resolution and use may
// observe either source; both directions fail safe because the engines
// re-enforce their own limits at publish.
func (t *Tx) newCollectionStateResolved(name string, coll *Collection, disk *durable.Collection) *txCollectionState {
	maxDocs, maxBytes := defaultFacadeMaxBatchDocuments, defaultFacadeMaxBatchBytes
	maxKey, maxDoc := defaultMaxKeyBytes, defaultMaxDocumentBytes
	if db := t.db; db != nil {
		maxKey, maxDoc = coll.bounds(disk)
		if db.profile != Memory && disk != nil {
			maxDocs, maxBytes = disk.MaxBatchDocuments(), disk.MaxBatchBytes()
		} else if db.profile != Memory {
			if db.engine.MaxBatchDocuments > 0 {
				maxDocs = db.engine.MaxBatchDocuments
			}
			if db.engine.MaxBatchBytes > 0 {
				maxBytes = db.engine.MaxBatchBytes
			}
		}
	}
	return newTxCollectionState(name, maxDocs, maxBytes, maxKey, maxDoc)
}

func newTxCollectionState(name string, maxDocs, maxBytes, maxKey, maxDoc int) *txCollectionState {
	state := &txCollectionState{
		name:     name,
		pending:  make(map[string]*txMutation),
		maxDocs:  maxDocs,
		maxBytes: maxBytes,
		maxKey:   maxKey,
		maxDoc:   maxDoc,
	}
	state.overlaySource = query.NewFileOverlaySource(state)
	return state
}

func (t *Tx) ensureCollection(name string) (*txCollectionState, error) {
	if t.solo != nil && t.solo.name == name {
		return t.solo, nil
	}
	if state := t.colls[name]; state != nil {
		return state, nil
	}
	if t.dynamicStates >= maxSerializableReadCollections {
		// Check the catalog before asking for a stable facade handle: rejected
		// names must not grow Database.handles for the database's lifetime.
		var exists bool
		if !t.readOnly {
			db := t.db
			// Close resets the memory catalog under handlesMu's write
			// leg, so the shared leg still excludes teardown here.
			db.handlesMu.RLock()
			if db.closed.Load() {
				db.handlesMu.RUnlock()
				return nil, ErrClosed
			}
			if db.profile == Memory {
				_, exists = db.heap.Collection(name)
			} else {
				_, exists = db.disk.Collection(name)
			}
			db.handlesMu.RUnlock()
		}
		if !exists {
			return nil, fmt.Errorf("%w: dynamic collections", ErrTxTooLarge)
		}
	}
	if t.readOnly {
		// Every collection present at BeginReadOnly already has a state. A
		// name outside that cut remains absent even if it was created later.
		// Limits resolve through the catalog exactly as at Begin.
		state := t.newCollectionState(name)
		state.absent = true
		if t.dynamicStates >= maxSerializableReadCollections {
			return nil, fmt.Errorf("%w: dynamic collections", ErrTxTooLarge)
		}
		t.dynamicStates++
		t.pinState(state)
		return state, nil
	}
	// Resolve the live backend without creating it, then capture only this
	// collection's snapshot. Collections that exist do not consume the
	// dynamic-state budget; only genuinely absent names do, so callers may
	// keep the lazy TxCollection API without growing a live transaction
	// through repeated rejected names.
	coll := t.db.Collection(name)
	memory, disk, err := coll.backend(false)
	if err != nil {
		return nil, err
	}
	state := t.newCollectionStateResolved(name, coll, disk)
	state.absent = true
	state.coll = coll
	if memory == nil && disk == nil {
		if t.dynamicStates >= maxSerializableReadCollections {
			return nil, fmt.Errorf("%w: dynamic collections", ErrTxTooLarge)
		}
		t.dynamicStates++
	} else if err := state.captureSnap(memory, disk); err != nil {
		return nil, err
	}
	t.pinState(state)
	return state, nil
}

// captureSnap pins this collection's current snapshot for the transaction.
// Exactly one of memory/disk is non-nil for a database-owned handle.
func (s *txCollectionState) captureSnap(memory *store.Collection, disk *durable.Collection) error {
	switch {
	case memory != nil:
		snap, err := memory.Snapshot()
		if err != nil {
			return facadeError(err)
		}
		s.heapSnap = snap
		s.hasHeap = true
		s.absent = false
	case disk != nil:
		snap, err := disk.Snapshot()
		if err != nil {
			return facadeError(err)
		}
		s.diskSnap = snap
		s.absent = false
	}
	return nil
}

func (t *Tx) dirtyStates() []*txCollectionState {
	dirty := make([]*txCollectionState, 0, t.dirtyCount)
	if t.solo != nil && t.solo.hasPublishableWrites() {
		dirty = append(dirty, t.solo)
	}
	for _, state := range t.colls {
		if state.hasPublishableWrites() {
			dirty = append(dirty, state)
		}
	}
	// Name order keeps multi-collection fence acquisition deadlock-free. A
	// lone participant is already ordered; skip the sort on the confined
	// fast path, where it is pure per-commit overhead.
	if len(dirty) > 1 {
		sort.Slice(dirty, func(i, j int) bool {
			return dirty[i].name < dirty[j].name
		})
	}
	return dirty
}

func (t *Tx) targetStates() []*txCollectionState {
	states := make([]*txCollectionState, 0, len(t.colls))
	if s := t.solo; s != nil && (s.hasPublishableWrites() || s.coarseRead || len(s.readOrder) != 0) {
		states = append(states, s)
	}
	for _, state := range t.colls {
		if state.hasPublishableWrites() || state.coarseRead || len(state.readOrder) != 0 {
			states = append(states, state)
		}
	}
	if len(states) > 1 {
		sort.Slice(states, func(i, j int) bool { return states[i].name < states[j].name })
	}
	return states
}

func (s *txCollectionState) hasPublishableWrites() bool {
	for _, key := range s.order {
		entry := s.pending[key]
		if !entry.existed && entry.remove {
			continue
		}
		return true
	}
	return false
}

func (t *Tx) validateState(state *txCollectionState) error {
	coll := state.coll
	memory, disk, err := coll.backend(false)
	if err != nil {
		return err
	}
	if memory == nil && disk == nil {
		for _, key := range state.order {
			if state.pending[key].existed {
				return fmt.Errorf("%w: collection %q key %q", ErrTxConflict, state.name, key)
			}
		}
		return nil
	}
	if memory != nil {
		snap, err := memory.Snapshot()
		if err != nil {
			return facadeError(err)
		}
		for _, key := range state.order {
			_, found := snap.GetRaw(key)
			if found != state.pending[key].existed {
				return fmt.Errorf("%w: collection %q key %q", ErrTxConflict, state.name, key)
			}
		}
		return nil
	}
	snap, err := disk.Snapshot()
	if err != nil {
		return facadeError(err)
	}
	defer snap.Close()
	var scratch []byte
	for _, key := range state.order {
		cur, found, err := snap.AppendRaw(scratch[:0], byteview.Bytes(key))
		if err != nil {
			return facadeError(err)
		}
		scratch = cur
		if found != state.pending[key].existed {
			return fmt.Errorf("%w: collection %q key %q", ErrTxConflict, state.name, key)
		}
	}
	return nil
}

func (t *Tx) validateDependencies(states []*txCollectionState) error {
	db := t.db
	// Participant fences exclude changes to the loaded histories. An unrelated
	// collection can still overflow and remove them, so check the conservative
	// floor both before and after all lookups. A reset after the final check
	// cannot invalidate already-checked dependencies while their fences remain
	// held; a reset during the pass must remain visible to this active revision.
	if db.txnClockConflict(t.beginRev) {
		return fmt.Errorf("%w: bounded database history", ErrTxConflict)
	}
	if hook := db.testAfterTxHistoryGuards; hook != nil {
		hook()
	}
	for _, state := range states {
		var history *txnclock.ExternalHistory
		if v, ok := db.txnHistories.Load(state.name); ok {
			history = v.(*txnclock.ExternalHistory)
		}
		if state.coarseRead {
			if history.ConflictCollection(t.beginRev) {
				return fmt.Errorf("%w: collection %q", ErrTxConflict, state.name)
			}
			continue
		}
		for _, key := range state.readOrder {
			if history.ConflictPoint(t.beginRev, key) {
				return fmt.Errorf("%w: collection %q key %q", ErrTxConflict, state.name, key)
			}
		}
		for _, key := range state.order {
			if _, alreadyRead := state.readSet[key]; alreadyRead {
				continue
			}
			if history.ConflictPoint(t.beginRev, key) {
				return fmt.Errorf("%w: collection %q key %q", ErrTxConflict, state.name, key)
			}
		}
	}
	if db.txnClockConflict(t.beginRev) {
		return fmt.Errorf("%w: bounded database history", ErrTxConflict)
	}
	return nil
}

func (d *Database) txnClockConflict(begin uint64) bool {
	floor := d.txnHistoryFloor.Load()
	return d.txnRevisionStopped.Load() || d.txnClockSaturated.Load() ||
		(floor != 0 && begin < floor)
}

func (t *Tx) commitSingleDurable(state *txCollectionState) error {
	_, disk, err := state.coll.backend(false)
	if err != nil {
		return err
	}
	if disk == nil {
		return fmt.Errorf("%w: collection %q missing at commit", ErrTxConflict, state.name)
	}
	return disk.Update(func(batch *durable.WriteBatch) error {
		return fillDurableBatch(batch, state)
	})
}

func (t *Tx) commitMultiDurable(dirty []*txCollectionState) error {
	return t.db.disk.Update(func(batch *durable.DatabaseBatch) error {
		for _, state := range dirty {
			wb, err := batch.Collection(state.name)
			if err != nil {
				return err
			}
			if err := fillDurableBatch(wb, state); err != nil {
				return err
			}
		}
		return nil
	})
}

func fillDurableBatch(batch *durable.WriteBatch, state *txCollectionState) error {
	for _, key := range state.order {
		entry := state.pending[key]
		if !entry.existed && entry.remove {
			continue
		}
		if entry.remove {
			if err := batch.Delete(byteview.Bytes(key)); err != nil {
				return err
			}
			continue
		}
		if err := batch.Put(byteview.Bytes(key), entry.document); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tx) commitMemory(dirty []*txCollectionState) error {
	targets := make([]*store.Collection, 0, len(dirty))
	for _, state := range dirty {
		memory, _, err := state.coll.backend(false)
		if err != nil {
			return err
		}
		if memory == nil {
			return fmt.Errorf("%w: collection %q missing at commit", ErrTxConflict, state.name)
		}
		targets = append(targets, memory)
	}
	// Iterate the dirty slice directly: each write batch is independent, so
	// the name-keyed re-index map was pure per-commit overhead.
	return store.UpdateCollections(targets, func(batch *store.DatabaseBatch) error {
		for _, state := range dirty {
			wb, err := batch.Collection(state.name)
			if err != nil {
				return err
			}
			for _, key := range state.order {
				entry := state.pending[key]
				if !entry.existed && entry.remove {
					continue
				}
				if entry.remove {
					if err := wb.Delete(key); err != nil {
						return err
					}
					continue
				}
				if err := wb.Put(key, entry.document); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// TxCollection is the facade Collection vocabulary minus lifecycle and DDL.
type TxCollection struct {
	name       string
	tx         *Tx
	state      *txCollectionState
	initialErr error
}

// Get returns an owned copy of key's transaction-visible document.
func (c *TxCollection) Get(key string) ([]byte, bool, error) {
	return c.Append(nil, key)
}

// Append appends key's transaction-visible document to dst.
func (c *TxCollection) Append(dst []byte, key string) ([]byte, bool, error) {
	if err := c.ready(); err != nil {
		return dst, false, err
	}
	if err := c.keyError(key); err != nil {
		return dst, false, err
	}
	if err := c.tx.trackPointRead(c.state, key); err != nil {
		return dst, false, err
	}
	if entry, ok := c.state.pending[key]; ok {
		if entry.remove {
			return dst, false, nil
		}
		return append(dst, entry.document...), true, nil
	}
	if c.state.hasHeap {
		raw, ok := c.state.heapSnap.GetRaw(key)
		if !ok {
			return dst, false, nil
		}
		return append(dst, raw.Bytes()...), true, nil
	}
	if c.state.diskSnap != nil {
		out, ok, err := c.state.diskSnap.AppendRaw(dst, byteview.Bytes(key))
		return out, ok, facadeError(err)
	}
	return dst, false, nil
}

// Put stages a canonicalized document for key.
func (c *TxCollection) Put(key string, document []byte) (created bool, err error) {
	if err := c.ready(); err != nil {
		return false, err
	}
	if c.tx.readOnly {
		return false, ErrTxReadOnly
	}
	if err := c.keyError(key); err != nil {
		return false, err
	}
	if len(document) == 0 || len(document) > c.state.maxDoc {
		return false, ErrDocumentTooLarge
	}
	if err := validateDocument(c.tx.db.engine.Collection, document); err != nil {
		return false, err
	}
	// Admit the dependency before canonicalization can allocate reusable scratch.
	// A rejected dynamic/read collection therefore retains no document-sized
	// buffer in the transaction.
	if err := c.tx.trackPointRead(c.state, key); err != nil {
		return false, err
	}
	canonical, err := vibejson.AppendCanonicalize(c.state.canonical[:0], document)
	if err != nil {
		c.state.canonical = nil
		return false, err
	}
	// Transfer scratch ownership to the staged mutation instead of copying:
	// the canonical buffer is already an owned exact-size rendering, so the
	// extra alloc+memcpy per Put is pure overhead. The next Put allocates a
	// fresh scratch, matching the previous per-Put allocation count.
	c.state.canonical = nil
	owned := canonical
	baseExisted, err := c.baseExisted(key)
	if err != nil {
		c.state.canonical = nil
		return false, err
	}
	visible, err := c.visible(key)
	if err != nil {
		c.state.canonical = nil
		return false, err
	}
	if err := c.tx.stage(c.state, key, owned, false, baseExisted); err != nil {
		c.state.canonical = nil
		return false, err
	}
	return !visible, nil
}

// Delete stages removal of key.
func (c *TxCollection) Delete(key string) (deleted bool, err error) {
	if err := c.ready(); err != nil {
		return false, err
	}
	if c.tx.readOnly {
		return false, ErrTxReadOnly
	}
	if err := c.keyError(key); err != nil {
		return false, err
	}
	if err := c.tx.trackPointRead(c.state, key); err != nil {
		return false, err
	}
	baseExisted, err := c.baseExisted(key)
	if err != nil {
		return false, err
	}
	visible, err := c.visible(key)
	if err != nil {
		return false, err
	}
	if err := c.tx.stage(c.state, key, nil, true, baseExisted); err != nil {
		return false, err
	}
	return visible, nil
}

// Range visits the transaction-visible generation of this collection.
func (c *TxCollection) Range(fn func(key string, document []byte) error) error {
	if err := c.ready(); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	if err := c.tx.trackCollectionRead(c.state); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(c.state.pending))
	visit := func(key string, document []byte) error {
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		if entry, ok := c.state.pending[key]; ok {
			if entry.remove {
				return nil
			}
			return fn(key, entry.document)
		}
		return fn(key, document)
	}
	if c.state.hasHeap {
		var visitErr error
		c.state.heapSnap.Range(func(key string, value vibejson.RawValue) bool {
			visitErr = visit(key, value.Bytes())
			return visitErr == nil
		})
		if visitErr != nil {
			return visitErr
		}
	} else if c.state.diskSnap != nil {
		if err := c.state.diskSnap.RangeRaw(func(key, document []byte) error {
			return visit(byteview.String(key), document)
		}); err != nil {
			return facadeError(err)
		}
	}
	for _, key := range c.state.order {
		entry := c.state.pending[key]
		if entry.remove || entry.existed {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		if err := fn(key, entry.document); err != nil {
			return err
		}
	}
	return nil
}

// Run executes compiled against this collection's snapshot ⊕ overlay.
func (c *TxCollection) Run(compiled *query.Query) (query.Result, error) {
	if err := c.ready(); err != nil {
		return query.Result{}, err
	}
	if compiled == nil {
		return query.Result{}, ErrInvalidQuery
	}
	if err := c.tx.trackCollectionRead(c.state); err != nil {
		return query.Result{}, err
	}
	if len(c.state.pending) == 0 {
		if c.state.hasHeap {
			result, err := compiled.Run(query.FromSnapshot(c.state.heapSnap))
			return result, facadeError(err)
		}
		if c.state.diskSnap != nil {
			result, err := compiled.Run(query.FromFile(c.state.diskSnap))
			return result, facadeError(err)
		}
		result, err := compiled.Run(query.FromSnapshot(store.Snapshot{}))
		return result, facadeError(err)
	}
	if c.state.hasHeap {
		result, err := compiled.Run(query.FromSnapshotOverlay(
			c.state.heapSnap, &c.state.overlaySource,
		))
		return result, facadeError(err)
	}
	if c.state.diskSnap != nil {
		result, err := compiled.Run(query.FromFileOverlay(
			c.state.diskSnap, &c.state.overlaySource,
		))
		return result, facadeError(err)
	}
	// Absent at begin: overlay-only view via an empty heap snapshot merge.
	result, err := compiled.Run(query.FromSnapshotOverlay(
		store.Snapshot{}, &c.state.overlaySource,
	))
	return result, facadeError(err)
}

func (c *TxCollection) ready() error {
	if c == nil {
		return ErrTxDone
	}
	if c.initialErr != nil {
		return c.initialErr
	}
	if c.tx == nil || c.tx.done || c.state == nil {
		return ErrTxDone
	}
	if c.tx.db == nil || c.tx.db.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (c *TxCollection) keyError(key string) error {
	if len(key) == 0 || len(key) > c.state.maxKey {
		return ErrKeyTooLarge
	}
	return nil
}

func (c *TxCollection) baseExisted(key string) (bool, error) {
	if entry, ok := c.state.pending[key]; ok {
		return entry.existed, nil
	}
	return c.snapshotHas(key)
}

func (c *TxCollection) visible(key string) (bool, error) {
	if entry, ok := c.state.pending[key]; ok {
		return !entry.remove, nil
	}
	return c.snapshotHas(key)
}

func (c *TxCollection) snapshotHas(key string) (bool, error) {
	if c.state.hasHeap {
		_, ok := c.state.heapSnap.GetRaw(key)
		return ok, nil
	}
	if c.state.diskSnap != nil {
		ok, err := c.state.diskSnap.ContainsKey(byteview.Bytes(key))
		return ok, facadeError(err)
	}
	return false, nil
}

func (t *Tx) trackPointRead(state *txCollectionState, key string) error {
	if t == nil || t.readOnly || state == nil || state.coarseRead {
		return nil
	}
	if state.readSet != nil {
		if _, exists := state.readSet[key]; exists {
			return nil
		}
	}
	if !state.readTracked {
		if t.readCollections >= maxSerializableReadCollections {
			return fmt.Errorf("%w: read collections", ErrTxTooLarge)
		}
		t.readCollections++
		state.readTracked = true
	}
	bytes := len(state.name) + len(key) + 2*10 + 1
	if t.readKeys >= maxSerializableReadKeys ||
		t.readBytes+bytes > maxSerializableReadBytes {
		return t.trackCollectionRead(state)
	}
	if state.readSet == nil {
		state.readSet = make(map[string]string)
	}
	owned := state.ownKey(key)
	state.readSet[owned] = owned
	state.readOrder = append(state.readOrder, owned)
	t.readKeys++
	t.readBytes += bytes
	return nil
}

func (t *Tx) trackCollectionRead(state *txCollectionState) error {
	if t == nil || t.readOnly || state == nil || state.coarseRead {
		return nil
	}
	if !state.readTracked {
		if t.readCollections >= maxSerializableReadCollections {
			return fmt.Errorf("%w: read collections", ErrTxTooLarge)
		}
		t.readCollections++
		state.readTracked = true
	}
	// readKeys/readBytes are monotonic retained-memory high-water counters.
	// ownKey's arena remains live until transaction finish even after exact
	// dependencies are replaced by the coarse marker, so subtracting here would
	// permit sequential escalations to retain an unbounded number of arenas.
	state.readSet = nil
	state.readOrder = nil
	state.coarseRead = true
	return nil
}

func (t *Tx) stage(
	state *txCollectionState,
	key string,
	document []byte,
	remove, existed bool,
) error {
	entry, present := state.pending[key]
	oldKeyLen := len(key)
	oldDocLen := 0
	if present {
		oldDocLen = len(entry.document)
	}
	newDocLen := 0
	if !remove {
		newDocLen = len(document)
	}

	nextCollDocs := state.stagedDocs
	nextCollBytes := state.stagedBytes
	if !present {
		nextCollDocs++
	} else {
		nextCollBytes -= oldKeyLen + oldDocLen
	}
	nextCollBytes += oldKeyLen + newDocLen
	if nextCollDocs > state.maxDocs {
		return fmt.Errorf("%w: documents", ErrTxTooLarge)
	}
	if nextCollBytes > state.maxBytes {
		return fmt.Errorf("%w: bytes", ErrTxTooLarge)
	}

	nextDocs := t.stagedDocs
	nextBytes := t.stagedBytes
	if present {
		nextDocs--
		nextBytes -= int64(oldKeyLen + oldDocLen)
	}
	nextDocs++
	nextBytes += int64(oldKeyLen + newDocLen)
	limits := t.db.txnLimits
	if nextDocs > limits.MaxDocuments {
		return fmt.Errorf("%w: documents", ErrTxTooLarge)
	}
	if nextBytes > limits.MaxBytes {
		return fmt.Errorf("%w: bytes", ErrTxTooLarge)
	}

	wasDirty := state.hasPublishableWrites()
	willPublish := existed || !remove
	if present {
		willPublish = state.entryWouldPublish(existed, remove)
	}
	if !wasDirty && willPublish && t.dirtyCount+1 > limits.MaxCollections {
		return fmt.Errorf("%w: collections", ErrTxTooLarge)
	}

	if !present {
		// Prefer the read tracker's owned copy: Put and Delete admit the
		// read dependency before staging, so the key is usually already
		// owned and a second clone is pure waste. A coarse (escalated)
		// collection has no read set and owns here instead.
		if owned, ok := state.readSet[key]; ok {
			key = owned
		} else {
			key = state.ownKey(key)
		}
		entry = &txMutation{existed: existed}
		state.pending[key] = entry
		state.order = append(state.order, key)
	} else {
		state.stagedBytes -= oldKeyLen + oldDocLen
		state.stagedDocs--
		t.stagedBytes -= int64(oldKeyLen + oldDocLen)
		t.stagedDocs--
	}
	entry.remove = remove
	if remove {
		entry.document = nil
	} else {
		entry.document = document
	}
	state.stagedDocs++
	state.stagedBytes += len(key) + len(entry.document)
	t.stagedDocs++
	t.stagedBytes += int64(len(key) + len(entry.document))

	nowDirty := state.hasPublishableWrites()
	switch {
	case !wasDirty && nowDirty:
		t.dirtyCount++
	case wasDirty && !nowDirty:
		t.dirtyCount--
	}
	return nil
}

func (s *txCollectionState) entryWouldPublish(existed, remove bool) bool {
	// After replacing the pending entry for this key, would the collection
	// still have any publishable write? Conservative: treat a publishable
	// replacement as dirty; exact recount happens after mutation.
	if existed || !remove {
		return true
	}
	for _, key := range s.order {
		entry := s.pending[key]
		if entry == nil {
			continue
		}
		if !entry.existed && entry.remove {
			continue
		}
		// Another key already keeps the collection dirty.
		return true
	}
	return false
}

// ownKeyInlineBytes bounds the exact-size clone fast path in ownKey. Keys at
// or below this size are cloned exactly: one small allocation and no arena
// retention. The chunk arena's 4KB floor is pure waste for few-key
// transactions, which previously paid two minimum-size arenas (read tracking
// plus staging) per key — the top allocator in transaction profiles. Larger
// keys amortize through the arena as before.
const ownKeyInlineBytes = 128

func (s *txCollectionState) ownKey(key string) string {
	if len(key) == 0 {
		return ""
	}
	if len(key) <= ownKeyInlineBytes {
		return strings.Clone(key)
	}
	if cap(s.keyChunk)-len(s.keyChunk) < len(key) {
		if cap(s.keyChunk) != 0 {
			s.keyChunks = append(s.keyChunks, s.keyChunk)
		}
		next := cap(s.keyChunk) * 2
		if next < 4096 {
			next = 4096
		}
		if next < len(key) {
			next = len(key)
		}
		s.keyChunk = make([]byte, 0, next)
	}
	start := len(s.keyChunk)
	s.keyChunk = append(s.keyChunk, key...)
	return byteview.String(s.keyChunk[start:len(s.keyChunk):len(s.keyChunk)])
}

// Lookup, RangeInserts, RangePresent, and LenDelta implement query.FileOverlay.
func (s *txCollectionState) Lookup(key []byte) (value []byte, present, shadowed bool) {
	mutation, ok := s.pending[string(key)]
	if !ok {
		return nil, false, false
	}
	if mutation.remove {
		return nil, false, true
	}
	return mutation.document, true, true
}

func (s *txCollectionState) RangeInserts(visit func(value []byte) error) error {
	for _, key := range s.order {
		mutation := s.pending[key]
		if mutation.existed || mutation.remove {
			continue
		}
		if err := visit(mutation.document); err != nil {
			return err
		}
	}
	return nil
}

func (s *txCollectionState) RangePresent(visit func(value []byte) error) error {
	for _, key := range s.order {
		mutation := s.pending[key]
		if mutation.remove {
			continue
		}
		if err := visit(mutation.document); err != nil {
			return err
		}
	}
	return nil
}

func (s *txCollectionState) LenDelta() int64 {
	var delta int64
	for _, key := range s.order {
		mutation := s.pending[key]
		switch {
		case mutation.existed && mutation.remove:
			delta--
		case !mutation.existed && !mutation.remove:
			delta++
		}
	}
	return delta
}

func (d *Database) documentBounds(name string) (maxKey, maxDoc int) {
	if d == nil {
		return defaultMaxKeyBytes, defaultMaxDocumentBytes
	}
	if d.disk != nil {
		if collection, ok := d.disk.Collection(name); ok && collection != nil {
			return collection.MaxKeyBytes(), collection.MaxDocumentBytes()
		}
	}
	return d.maxKeyBytes, d.maxDocumentBytes
}

func (d *Database) batchBounds(name string) (maxDocs, maxBytes int) {
	maxDocs = defaultFacadeMaxBatchDocuments
	maxBytes = defaultFacadeMaxBatchBytes
	if d == nil {
		return maxDocs, maxBytes
	}
	if d.profile == Memory {
		if coll, ok := d.heap.Collection(name); ok && coll != nil {
			return defaultFacadeMaxBatchDocuments, defaultFacadeMaxBatchBytes
		}
		return maxDocs, maxBytes
	}
	if d.disk != nil {
		if coll, ok := d.disk.Collection(name); ok && coll != nil {
			return coll.MaxBatchDocuments(), coll.MaxBatchBytes()
		}
	}
	if d.engine.MaxBatchDocuments > 0 {
		maxDocs = d.engine.MaxBatchDocuments
	}
	if d.engine.MaxBatchBytes > 0 {
		maxBytes = d.engine.MaxBatchBytes
	}
	return maxDocs, maxBytes
}

func (d *Database) armClockForBegin(tx *Tx) {
	tx.clockArmed = true
	d.clockMu.RLock()
	defer d.clockMu.RUnlock()
	if d.txnClockSaturated.Load() || d.txnActiveCount.Load() == maxTxnActiveCount {
		d.txnClockSaturated.Store(true)
		tx.beginRev = d.txnRevision.Load()
		return
	}
	shard := d.txnArmTick.Add(1) & (txnActiveShardCount - 1)
	s := &d.txnActive[shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	// Sampling inside the stripe prevents an oldest scan from passing this
	// registration and publishing a newer pruning bound before it is visible.
	tx.beginRev = d.txnRevision.Load()
	if !d.addTxnHolder() {
		return
	}
	if s.revs == nil {
		s.revs = make(map[uint64]uint64)
	}
	s.revs[tx.beginRev]++
	tx.clockShard = shard
	tx.clockInserted = true
}

func (d *Database) addTxnHolder() bool {
	for {
		active := d.txnActiveCount.Load()
		if active == maxTxnActiveCount {
			d.txnClockSaturated.Store(true)
			return false
		}
		if d.txnActiveCount.CompareAndSwap(active, active+1) {
			if active+1 == maxTxnActiveCount {
				d.txnClockSaturated.Store(true)
				return false
			}
			return true
		}
	}
}

func (d *Database) removeTxnHolder() {
	for {
		active := d.txnActiveCount.Load()
		if active == 0 || active == maxTxnActiveCount {
			return
		}
		if d.txnActiveCount.CompareAndSwap(active, active-1) {
			return
		}
	}
}

func (d *Database) finishClock(tx *Tx, published []txPublishedKeys) {
	// This transaction is one active holder. If it is the only holder, a future
	// Begin necessarily captures its already-published storage state and no
	// logical history needs to be materialized.
	//
	// Recording precedes unregistration: a concurrent validator can only rely
	// on this publication's history if the overlap gate observed this
	// transaction as active, so the record must land while it still counts.
	if published != nil && d.txnActiveCount.Load() > 1 {
		d.recordPublished(published)
	}
	if !tx.clockArmed {
		return
	}
	tx.clockArmed = false
	if tx.clockInserted {
		tx.clockInserted = false
		d.clockMu.RLock()
		s := &d.txnActive[tx.clockShard]
		s.mu.Lock()
		if n := s.revs[tx.beginRev]; n <= 1 {
			delete(s.revs, tx.beginRev)
		} else {
			s.revs[tx.beginRev] = n - 1
		}
		d.removeTxnHolder()
		s.mu.Unlock()
		// Only the oldest holder can advance the oldest bound, so only it
		// pays for a rescan. A stale-low hint merely retains more history
		// (bounded by HistoryKeys/Floor), never less.
		if !d.txnClockSaturated.Load() && tx.beginRev <= d.txnOldestHint.Load() {
			d.refreshOldestHint()
		}
		d.clockMu.RUnlock()
	}
	if d.txnActiveCount.Load() == 0 {
		d.quiesceClock()
		return
	}
	if d.txnClockSaturated.Load() {
		return
	}
	if floor := d.txnHistoryFloor.Load(); floor != 0 && d.oldestTxnHistoryRevision() >= floor {
		d.clockMu.Lock()
		if f := d.txnHistoryFloor.Load(); f != 0 && d.oldestTxnHistoryRevision() >= f {
			d.txnHistoryFloor.Store(0)
		}
		d.clockMu.Unlock()
	}
}

// refreshOldestHint recomputes the oldest live begin revision by scanning the
// directory stripes while the caller holds clockMu shared. Registrations
// sample their revision inside the stripe lock, so anything a scan misses
// cannot precede its initial revision sample. A concurrent finish can only
// leave a stale-low bound, which retains extra history. Stripes never nest.
func (d *Database) refreshOldestHint() {
	min := d.txnRevision.Load()
	for i := range d.txnActive {
		s := &d.txnActive[i]
		s.mu.Lock()
		for rev := range s.revs {
			if rev < min {
				min = rev
			}
		}
		s.mu.Unlock()
	}
	d.txnOldestHint.Store(min)
}

// quiesceClock resets clock state when the last active transaction finishes.
// The exclusive gate excludes registration and history recording; the count
// must be checked again after acquiring it because a new Begin can race the
// last finisher's original zero observation.
func (d *Database) quiesceClock() {
	if hook := d.testBeforeClockQuiesce; hook != nil {
		hook()
	}
	d.clockMu.Lock()
	defer d.clockMu.Unlock()
	if d.txnActiveCount.Load() != 0 {
		return
	}
	d.clearHistories()
	d.txnHistoryFloor.Store(0)
	d.txnOldestHint.Store(d.txnRevision.Load())
}

func (d *Database) clearHistories() {
	d.txnHistories.Range(func(key, _ any) bool {
		d.txnHistories.Delete(key)
		return true
	})
	d.txnHistoriesCount.Store(0)
}

func (d *Database) oldestTxnHistoryRevision() uint64 {
	// Saturated holders are intentionally absent from the exact directory.
	// Freeze the effective pruning bound at zero even if a scan that began
	// before saturation finishes afterward. Validation also fails closed.
	if d.txnClockSaturated.Load() {
		return 0
	}
	return d.txnOldestHint.Load()
}

func (d *Database) recordClockKey(name, key string) {
	if d.txnActiveCount.Load() == 0 {
		return
	}
	revision, ok := d.nextTxnRevision()
	if !ok {
		return
	}
	if hook := d.testAfterTxnRevisionAssigned; hook != nil {
		hook(revision)
	}
	// A shared hold keeps an existing history attached until its record lands.
	// Only a missing history needs exclusive creation/overflow admission.
	d.clockMu.RLock()
	if d.txnActiveCount.Load() == 0 {
		d.clockMu.RUnlock()
		return
	}
	if value, exists := d.txnHistories.Load(name); exists {
		value.(*txnclock.ExternalHistory).RecordAt(revision, d.oldestTxnHistoryRevision(), []string{key})
		d.clockMu.RUnlock()
		return
	}
	d.clockMu.RUnlock()
	d.clockMu.Lock()
	defer d.clockMu.Unlock()
	if d.txnActiveCount.Load() == 0 {
		return
	}
	history := d.historyForRecordLocked(name)
	if history == nil {
		return
	}
	history.RecordAt(revision, d.oldestTxnHistoryRevision(), []string{key})
}

func (d *Database) recordPublished(published []txPublishedKeys) {
	revision, ok := d.nextTxnRevision()
	if !ok {
		return
	}
	d.clockMu.RLock()
	missing := false
	for i := range published {
		if len(published[i].keys) != 0 {
			if _, exists := d.txnHistories.Load(published[i].name); !exists {
				missing = true
				break
			}
		}
	}
	if missing {
		d.clockMu.RUnlock()
		d.clockMu.Lock()
		defer d.clockMu.Unlock()
	} else {
		defer d.clockMu.RUnlock()
	}
	// Resolve every history before recording any: on capacity overflow the
	// whole publication is dropped (floor set, histories cleared), so a
	// partial record can never strand a validator with half a publication's
	// dependencies.
	type pendingRecord struct {
		history *txnclock.ExternalHistory
		keys    []string
	}
	pending := make([]pendingRecord, 0, len(published))
	for i := range published {
		if len(published[i].keys) == 0 {
			continue
		}
		history := d.historyForRecordLocked(published[i].name)
		if history == nil {
			return
		}
		pending = append(pending, pendingRecord{history: history, keys: published[i].keys})
	}
	// The caller holds every dirty collection's fence, so each record lands
	// while concurrent validators of that collection are excluded.
	oldest := d.oldestTxnHistoryRevision()
	for _, p := range pending {
		p.history.RecordUniqueAt(revision, oldest, p.keys)
	}
}

// historyForRecordLocked returns the named collection's conflict history, creating
// it subject to the relation cap. A nil return means the publication must be
// dropped: capacity overflow set the history floor and cleared every history,
// so validators conservatively conflict instead of trusting a partial record.
//
// The caller holds clockMu exclusively when any history is missing, or shared
// when every history already exists. Creation and all directory resets are
// exclusive, so a loaded history cannot be detached before recording ends.
func (d *Database) historyForRecordLocked(name string) *txnclock.ExternalHistory {
	if v, ok := d.txnHistories.Load(name); ok {
		return v.(*txnclock.ExternalHistory)
	}
	if floor := d.txnHistoryFloor.Load(); floor != 0 && d.oldestTxnHistoryRevision() >= floor {
		d.txnHistoryFloor.Store(0)
	}
	if d.txnHistoriesCount.Load()+1 > maxSerializableHistoryCollections {
		// Reservations can arrive out of order. The floor must cover every
		// revision being discarded, not just this recorder's older revision.
		floor := d.txnRevision.Load()
		if previous := d.txnHistoryFloor.Load(); previous > floor {
			floor = previous
		}
		d.txnHistoryFloor.Store(floor)
		d.clearHistories()
		return nil
	}
	history := &txnclock.ExternalHistory{}
	d.txnHistories.Store(name, history)
	d.txnHistoriesCount.Add(1)
	return history
}

// nextTxnRevision assigns the revision a publication records under: one
// successful CAS per published commit, so a multi-collection publication
// carries a single revision across every collection (see
// TestNativeSerializableMultiCollectionPublicationUsesOneRevision). The
// stopped and at-maximum checks preserve the fail-closed exhaustion
// semantics: once stopped, every validation conflicts and nothing records.
func (d *Database) nextTxnRevision() (uint64, bool) {
	for {
		if d.txnRevisionStopped.Load() {
			return d.txnRevision.Load(), false
		}
		revision := d.txnRevision.Load()
		if revision == maxTxnRevision {
			// Never publish a wrapped value, even temporarily: another
			// allocator or Begin could otherwise reuse it before the latch.
			d.txnRevisionStopped.Store(true)
			return maxTxnRevision, false
		}
		if d.txnRevision.CompareAndSwap(revision, revision+1) {
			return revision + 1, true
		}
	}
}

// txnActiveEntriesForTest totals live directory buckets across stripes.
// Production never needs a global total; tests use it to assert directory
// turnover and cleanup without depending on stripe placement.
func (d *Database) txnActiveEntriesForTest() int {
	total := 0
	for i := range d.txnActive {
		s := &d.txnActive[i]
		s.mu.Lock()
		total += len(s.revs)
		s.mu.Unlock()
	}
	return total
}

// txnOldestScannedForTest recomputes the exact oldest live begin revision.
// Validation and pruning use the atomic hint instead; tests use the exact
// scan to assert turnover without depending on hint refresh timing.
func (d *Database) txnOldestScannedForTest() uint64 {
	min := d.txnRevision.Load()
	for i := range d.txnActive {
		s := &d.txnActive[i]
		s.mu.Lock()
		for rev := range s.revs {
			if rev < min {
				min = rev
			}
		}
		s.mu.Unlock()
	}
	return min
}

func (d *Database) enterUpdateClosure() error {
	id := goroutineID()
	depthAny, _ := d.updateDepth.Load(id)
	depth, _ := depthAny.(int)
	if depth > 0 {
		return ErrTxNested
	}
	d.updateDepth.Store(id, depth+1)
	return nil
}

func (d *Database) leaveUpdateClosure() {
	id := goroutineID()
	depthAny, _ := d.updateDepth.Load(id)
	depth, _ := depthAny.(int)
	if depth <= 1 {
		d.updateDepth.Delete(id)
		return
	}
	d.updateDepth.Store(id, depth-1)
}

func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// "goroutine 123 ["
	s := byteview.String(buf[:n])
	const prefix = "goroutine "
	if !strings.HasPrefix(s, prefix) {
		return 0
	}
	s = s[len(prefix):]
	var id uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
	}
	return id
}

func facadeTxnError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrTxConflict) ||
		errors.Is(err, ErrTxTooLarge) ||
		errors.Is(err, ErrTxUnsupportedLane) ||
		errors.Is(err, ErrCommitOutcomeUnknown) {
		return err
	}
	if errors.Is(err, durable.ErrDatabaseTransactionUnsupportedLane) {
		return fmt.Errorf("%w: %w", ErrTxUnsupportedLane, err)
	}
	return facadeError(err)
}
