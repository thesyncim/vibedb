package vibedb

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"

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
// database-global logical clock before capturing a coherent multi-collection
// cut.
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
	tx := &Tx{
		db:       d,
		readOnly: readOnly,
		colls:    make(map[string]*txCollectionState),
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

// Tx is one leased multi-collection cut plus bounded per-collection overlays.
// A Tx must not be copied after first use.
type Tx struct {
	db       *Database
	readOnly bool
	done     bool

	colls map[string]*txCollectionState

	beginRev   uint64
	clockArmed bool

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

	diskSnap *durable.Snapshot
	heapSnap store.Snapshot
	hasHeap  bool
	absent   bool // not present in the begin cut

	pending map[string]*txMutation
	order   []string

	stagedDocs  int
	stagedBytes int

	maxDocs  int
	maxBytes int
	maxKey   int
	maxDoc   int

	overlaySource query.FileOverlaySource

	readSet     map[string]struct{}
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

// txnActiveRevision is one live logical-revision bucket in an ordered,
// intrusive directory. Begin revisions are monotonic, so new buckets append at
// the tail; Finish unlinks any bucket in constant time and the head is always
// the oldest active revision. The map and list retain only live buckets.
type txnActiveRevision struct {
	count                uint64
	previous, next       uint64
	hasPrevious, hasNext bool
}

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
	if len(dirty) == 0 {
		// With no publication this cut can serialize at Begin, so even a
		// read-write handle that only read needs no commit-time validation.
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
	preParticipants := t.participantStates()
	sharedCommit := len(dirty) == 1 && len(preParticipants) == 1 &&
		preParticipants[0].name == dirty[0].name
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
	participants := t.participantStates()
	lockedCollections := make([]*Collection, 0, len(participants))
	for _, state := range participants {
		collection := db.Collection(state.name)
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

	if err := t.validateDependencies(participants); err != nil {
		t.finish(nil)
		return err
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
		coll := db.Collection(state.name)
		if _, _, err := coll.backend(true); err != nil {
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

func (t *Tx) publicationKeys(dirty []*txCollectionState) map[string][]string {
	published := make(map[string][]string, len(dirty))
	for _, state := range dirty {
		keys := make([]string, 0, len(state.order))
		for _, key := range state.order {
			entry := state.pending[key]
			if !entry.existed && entry.remove {
				continue
			}
			keys = append(keys, key)
		}
		if len(keys) > 0 {
			published[state.name] = keys
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

func (t *Tx) finish(published map[string][]string) {
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
}

func (t *Tx) scrubStates() {
	for _, state := range t.colls {
		if state.diskSnap != nil {
			_ = state.diskSnap.Close()
			state.diskSnap = nil
		}
		state.heapSnap = store.Snapshot{}
		state.hasHeap = false
		state.pending = nil
		state.order = nil
		state.overlaySource = query.FileOverlaySource{}
		state.readSet = nil
		state.readOrder = nil
		state.keyChunk = nil
		state.keyChunks = nil
		state.canonical = nil
	}
}

func (t *Tx) releaseCuts() {
	// Each state owns its lazily captured collection snapshot. Memory
	// snapshots are plain values; durable snapshots hold read resources and
	// are closed exactly once here. scrubStates repeats the same nil-guarded
	// close only as a backstop for paths that never released cuts.
	for _, state := range t.colls {
		if state.diskSnap != nil {
			_ = state.diskSnap.Close()
			state.diskSnap = nil
		}
		state.heapSnap = store.Snapshot{}
		state.hasHeap = false
	}
}

// captureCut is intentionally trivial: per-collection snapshots are captured
// lazily by ensureCollection on first touch. A database-wide snapshot at Begin
// locked every collection plus the catalog (O(shards) mutexes per Begin) and
// serialized every Begin against every commit's publication, which capped
// disjoint-collection throughput at one core's worth of snapshots.
//
// Serializability is preserved by commit-time validation, which never
// depended on cut timing: write-write conflicts are rechecked against fresh
// live state (validateState) and read-write dependencies against
// beginRev-anchored conflict histories (validateDependencies). A transaction
// that observed fractured state therefore always aborts instead of
// committing it; under low contention nothing aborts and Begin is O(1).
func (t *Tx) captureCut() error {
	if t.db.profile != Memory && t.db.disk == nil {
		return ErrClosed
	}
	return nil
}

func (t *Tx) newCollectionState(name string) *txCollectionState {
	maxDocs, maxBytes := t.db.batchBounds(name)
	maxKey, maxDoc := t.db.documentBounds(name)
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
	if state := t.colls[name]; state != nil {
		return state, nil
	}
	state := t.newCollectionState(name)
	state.absent = true
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
	if memory == nil && disk == nil {
		if t.dynamicStates >= maxSerializableReadCollections {
			return nil, fmt.Errorf("%w: dynamic collections", ErrTxTooLarge)
		}
		t.dynamicStates++
	} else if err := state.captureSnap(memory, disk); err != nil {
		return nil, err
	}
	t.colls[name] = state
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
	for _, state := range t.colls {
		if state.hasPublishableWrites() {
			dirty = append(dirty, state)
		}
	}
	sort.Slice(dirty, func(i, j int) bool {
		return dirty[i].name < dirty[j].name
	})
	return dirty
}

func (t *Tx) participantStates() []*txCollectionState {
	states := make([]*txCollectionState, 0, len(t.colls))
	for _, state := range t.colls {
		if state.hasPublishableWrites() || state.coarseRead || len(state.readOrder) != 0 {
			states = append(states, state)
		}
	}
	sort.Slice(states, func(i, j int) bool { return states[i].name < states[j].name })
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
	db := t.db
	coll := db.Collection(state.name)
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
	// Read-only pass over beginRev-anchored histories: shared mode lets
	// disjoint collections validate concurrently. All history mutations take
	// the exclusive mode, so shared readers never observe a torn map or entry.
	db.clockMu.RLock()
	defer db.clockMu.RUnlock()
	if db.txnRevisionStopped ||
		(db.txnHistoryFloor != 0 && t.beginRev < db.txnHistoryFloor) {
		return fmt.Errorf("%w: bounded database history", ErrTxConflict)
	}
	for _, state := range states {
		history := db.txnHistories[state.name]
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
	return nil
}

func (t *Tx) commitSingleDurable(state *txCollectionState) error {
	coll := t.db.Collection(state.name)
	_, disk, err := coll.backend(false)
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
	participants := make([]*store.Collection, 0, len(dirty))
	byName := make(map[string]*txCollectionState, len(dirty))
	for _, state := range dirty {
		coll := t.db.Collection(state.name)
		memory, _, err := coll.backend(false)
		if err != nil {
			return err
		}
		if memory == nil {
			return fmt.Errorf("%w: collection %q missing at commit", ErrTxConflict, state.name)
		}
		participants = append(participants, memory)
		byName[state.name] = state
	}
	return store.UpdateCollections(participants, func(batch *store.DatabaseBatch) error {
		for name, state := range byName {
			wb, err := batch.Collection(name)
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
		state.readSet = make(map[string]struct{})
	}
	owned := state.ownKey(key)
	state.readSet[owned] = struct{}{}
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
		key = state.ownKey(key)
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

func (s *txCollectionState) ownKey(key string) string {
	if len(key) == 0 {
		return ""
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
	d.clockMu.Lock()
	tx.beginRev = d.txnRevision
	active := d.txnActiveCount.Load()
	if active != maxTxnActiveCount {
		count := d.addActiveRevisionLocked(tx.beginRev)
		nextActive := incrementTxnHolderCount(active)
		if count == maxTxnActiveCount || nextActive == maxTxnActiveCount {
			d.saturateActiveRevisionsLocked()
		} else {
			d.txnActiveCount.Store(nextActive)
		}
	}
	tx.clockArmed = true
	d.clockMu.Unlock()
}

func (d *Database) finishClock(tx *Tx, published map[string][]string) {
	d.clockMu.Lock()
	defer d.clockMu.Unlock()
	// This transaction is one active holder. If it is the only holder, a future
	// Begin necessarily captures its already-published storage state and no
	// logical history needs to be materialized.
	if published != nil && d.txnActiveCount.Load() > 1 {
		d.recordPublishedLocked(published)
	}
	if !tx.clockArmed {
		return
	}
	active := d.txnActiveCount.Load()
	if active != maxTxnActiveCount && active != 0 {
		d.removeActiveRevisionLocked(tx.beginRev)
		d.txnActiveCount.Store(active - 1)
	}
	tx.clockArmed = false
	if d.txnActiveCount.Load() == 0 {
		d.clearActiveRevisionsLocked()
		d.txnHistories = nil
		d.txnHistoryFloor = 0
		return
	}
	oldest := d.oldestActiveLocked()
	if d.txnHistoryFloor != 0 && oldest >= d.txnHistoryFloor {
		d.txnHistoryFloor = 0
	}
}

func (d *Database) recordClockKey(name, key string) {
	if d.txnActiveCount.Load() == 0 {
		return
	}
	d.clockMu.Lock()
	defer d.clockMu.Unlock()
	if d.txnActiveCount.Load() == 0 {
		return
	}
	revision, ok := d.nextTxnRevisionLocked()
	if !ok {
		return
	}
	oldest := d.oldestActiveLocked()
	if !d.ensureHistoryCapacityLocked(revision, oldest, 1, name) {
		return
	}
	d.txnHistories[name].RecordAt(revision, oldest, []string{key})
}

func (d *Database) recordPublishedLocked(published map[string][]string) {
	revision, ok := d.nextTxnRevisionLocked()
	if !ok {
		return
	}
	oldest := d.oldestActiveLocked()
	missing := 0
	for name, keys := range published {
		if len(keys) != 0 && d.txnHistories[name] == nil {
			missing++
		}
	}
	if len(d.txnHistories)+missing > maxSerializableHistoryCollections {
		d.txnHistoryFloor = revision
		d.txnHistories = nil
		return
	}
	if d.txnHistoryFloor != 0 && oldest >= d.txnHistoryFloor {
		d.txnHistoryFloor = 0
	}
	if d.txnHistories == nil {
		d.txnHistories = make(map[string]*txnclock.ExternalHistory, missing)
	}
	for name, keys := range published {
		if len(keys) == 0 {
			continue
		}
		history := d.txnHistories[name]
		if history == nil {
			history = &txnclock.ExternalHistory{}
			d.txnHistories[name] = history
		}
		history.RecordUniqueAt(revision, oldest, keys)
	}
}

func (d *Database) ensureHistoryCapacityLocked(
	revision, oldest uint64,
	missing int,
	name string,
) bool {
	if d.txnHistoryFloor != 0 && oldest >= d.txnHistoryFloor {
		d.txnHistoryFloor = 0
	}
	if d.txnHistories[name] != nil {
		return true
	}
	if len(d.txnHistories)+missing > maxSerializableHistoryCollections {
		d.txnHistoryFloor = revision
		d.txnHistories = nil
		return false
	}
	if d.txnHistories == nil {
		d.txnHistories = make(map[string]*txnclock.ExternalHistory)
	}
	d.txnHistories[name] = &txnclock.ExternalHistory{}
	return true
}

func (d *Database) nextTxnRevisionLocked() (uint64, bool) {
	if d.txnRevisionStopped {
		return d.txnRevision, false
	}
	if d.txnRevision == maxTxnRevision {
		atMax, activeAtMax := d.txnActive[maxTxnRevision]
		if d.txnActiveCount.Load() == maxTxnActiveCount ||
			(activeAtMax && atMax.count != 0) {
			d.txnRevisionStopped = true
			d.txnHistories = nil
			return d.txnRevision, false
		}
		return d.txnRevision, true
	}
	d.txnRevision++
	return d.txnRevision, true
}

func incrementTxnHolderCount(count uint64) uint64 {
	if count >= maxTxnActiveCount-1 {
		return maxTxnActiveCount
	}
	return count + 1
}

func (d *Database) addActiveRevisionLocked(revision uint64) uint64 {
	if d.txnActive == nil {
		d.txnActive = make(map[uint64]txnActiveRevision)
	}
	entry, exists := d.txnActive[revision]
	if exists {
		entry.count = incrementTxnHolderCount(entry.count)
		d.txnActive[revision] = entry
		return entry.count
	}
	entry.count = 1
	if d.txnActiveLinked {
		entry.previous = d.txnActiveNewest
		entry.hasPrevious = true
		newest := d.txnActive[d.txnActiveNewest]
		newest.next = revision
		newest.hasNext = true
		d.txnActive[d.txnActiveNewest] = newest
	} else {
		d.txnActiveOldest = revision
		d.txnActiveLinked = true
	}
	d.txnActiveNewest = revision
	d.txnActive[revision] = entry
	return 1
}

func (d *Database) removeActiveRevisionLocked(revision uint64) {
	entry, exists := d.txnActive[revision]
	if !exists || entry.count == 0 {
		return
	}
	if entry.count > 1 {
		entry.count--
		d.txnActive[revision] = entry
		return
	}
	if entry.hasPrevious {
		previous := d.txnActive[entry.previous]
		previous.next = entry.next
		previous.hasNext = entry.hasNext
		d.txnActive[entry.previous] = previous
	} else if entry.hasNext {
		d.txnActiveOldest = entry.next
	}
	if entry.hasNext {
		next := d.txnActive[entry.next]
		next.previous = entry.previous
		next.hasPrevious = entry.hasPrevious
		d.txnActive[entry.next] = next
	} else if entry.hasPrevious {
		d.txnActiveNewest = entry.previous
	}
	delete(d.txnActive, revision)
	if len(d.txnActive) == 0 {
		d.clearActiveRevisionsLocked()
	}
}

func (d *Database) saturateActiveRevisionsLocked() {
	// Once an exact holder count saturates, retain one permanent conservative
	// oldest-revision sentinel. Future Begin/Finish calls need no directory
	// entries, so fail-closed saturation cannot grow memory without bound.
	oldest := d.oldestActiveLocked()
	d.txnActive = map[uint64]txnActiveRevision{
		oldest: {count: maxTxnActiveCount},
	}
	d.txnActiveOldest = oldest
	d.txnActiveNewest = oldest
	d.txnActiveLinked = true
	d.txnActiveCount.Store(maxTxnActiveCount)
}

func (d *Database) clearActiveRevisionsLocked() {
	d.txnActive = nil
	d.txnActiveOldest = 0
	d.txnActiveNewest = 0
	d.txnActiveLinked = false
}

func (d *Database) oldestActiveLocked() uint64 {
	if !d.txnActiveLinked {
		return maxTxnRevision
	}
	return d.txnActiveOldest
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
