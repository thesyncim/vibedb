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
	// ErrTxConflict reports that another committed write changed a key in this
	// transaction's write set after Begin. Nothing was published; the caller
	// owns the retry loop.
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

// Begin starts a read-write transaction: arms per-collection conflict clocks,
// then captures one coherent multi-collection cut.
func (d *Database) Begin() (*Tx, error) {
	return d.begin(false)
}

// BeginReadOnly starts a read-only transaction over one coherent cut. Clocks
// are not armed; mutations refuse with ErrTxReadOnly.
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
		d.armClocksForBegin(tx)
	}
	if err := tx.captureCut(); err != nil {
		if !readOnly {
			d.finishClocks(tx, nil)
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

	diskCut  durable.DatabaseSnapshot
	hasDisk  bool
	heapCut  store.DatabaseSnapshot
	hasHeap  bool

	colls map[string]*txCollectionState

	// beginRevs records Conflict begin revisions for every collection whose
	// clock was Begun for this transaction (cataloged at Begin or first write).
	beginRevs map[string]uint64

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

	overlaySource query.FileOverlaySource
	beginRev      uint64
	hasBegin      bool

	keyChunk  []byte
	keyChunks [][]byte
	canonical []byte
}

type txMutation struct {
	document []byte
	remove   bool
	existed  bool
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
	state := t.ensureCollection(name)
	return &TxCollection{name: name, tx: t, state: state}
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
		t.finish(nil)
		return nil
	}

	// Release read leases before taking the commit mutex so writers are not
	// blocked by generation leases during validation/publication.
	t.releaseCuts()

	db.commitMu.Lock()
	defer db.commitMu.Unlock()
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

	for _, state := range dirty {
		if err := t.validateState(state); err != nil {
			t.finish(nil)
			return err
		}
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
	switch {
	case len(dirty) == 1 && db.profile != Memory:
		commitErr = t.commitSingleDurable(dirty[0])
	case db.profile == Memory:
		commitErr = t.commitMemory(dirty)
	default:
		commitErr = t.commitMultiDurable(dirty)
	}
	if commitErr != nil {
		t.finish(nil)
		return facadeTxnError(commitErr)
	}

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
	t.finish(published)
	return nil
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
		t.db.finishClocks(t, published)
	}
	t.db = nil
	t.colls = nil
	t.beginRevs = nil
}

func (t *Tx) releaseCuts() {
	if t.hasDisk {
		_ = t.diskCut.Close()
		t.diskCut = durable.DatabaseSnapshot{}
		t.hasDisk = false
	}
	t.heapCut = store.DatabaseSnapshot{}
	t.hasHeap = false
	for _, state := range t.colls {
		state.diskSnap = nil
		state.heapSnap = store.Snapshot{}
		state.hasHeap = false
	}
}

func (t *Tx) captureCut() error {
	db := t.db
	switch db.profile {
	case Memory:
		t.heapCut = db.heap.Snapshot()
		t.hasHeap = true
		for _, info := range db.heap.AppendCollections(nil) {
			snap, ok := t.heapCut.Collection(info.Name)
			state := t.newCollectionState(info.Name)
			state.hasHeap = ok
			state.heapSnap = snap
			state.absent = !ok
			t.colls[info.Name] = state
			if !t.readOnly {
				t.beginClock(state)
			}
		}
	default:
		if db.disk == nil {
			return ErrClosed
		}
		cut, err := db.disk.Snapshot()
		if err != nil {
			return facadeError(err)
		}
		t.diskCut = cut
		t.hasDisk = true
		cut.All(func(name string, snap *durable.Snapshot) bool {
			state := t.newCollectionState(name)
			state.diskSnap = snap
			state.absent = snap == nil
			t.colls[name] = state
			if !t.readOnly {
				t.beginClock(state)
			}
			return true
		})
	}
	return nil
}

func (t *Tx) newCollectionState(name string) *txCollectionState {
	maxDocs, maxBytes := t.db.batchBounds(name)
	state := &txCollectionState{
		name:     name,
		pending:  make(map[string]*txMutation),
		maxDocs:  maxDocs,
		maxBytes: maxBytes,
	}
	state.overlaySource = query.NewFileOverlaySource(state)
	return state
}

func (t *Tx) ensureCollection(name string) *txCollectionState {
	if state := t.colls[name]; state != nil {
		return state
	}
	state := t.newCollectionState(name)
	state.absent = true
	if t.hasHeap {
		if snap, ok := t.heapCut.Collection(name); ok {
			state.heapSnap = snap
			state.hasHeap = true
			state.absent = false
		}
	}
	if t.hasDisk {
		if snap, ok := t.diskCut.Collection(name); ok {
			state.diskSnap = snap
			state.absent = snap == nil
		}
	}
	t.colls[name] = state
	if !t.readOnly && !t.done {
		t.beginClock(state)
	}
	return state
}

func (t *Tx) beginClock(state *txCollectionState) {
	if state.hasBegin || t.db == nil {
		return
	}
	t.db.clockMu.Lock()
	clock := t.db.clockLocked(state.name)
	clock.Arm()
	state.beginRev = clock.Begin()
	t.db.clockMu.Unlock()
	state.hasBegin = true
	if t.beginRevs == nil {
		t.beginRevs = make(map[string]uint64)
	}
	t.beginRevs[state.name] = state.beginRev
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
	keys := state.order
	db.clockMu.Lock()
	clock := db.clockLocked(state.name)
	_, _, conflict := clock.Conflict(state.beginRev, keys)
	db.clockMu.Unlock()
	if conflict {
		return fmt.Errorf("%w: collection %q", ErrTxConflict, state.name)
	}
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
	// Catalog-owned create currently mints a legacy journal header; the first
	// multi-collection prepare needs the conditional format word. Flushing each
	// dirty participant runs the bounded foreground checkpoint/recycle that
	// remints at the conditional format while no staged batch is held — the
	// same upgrade ensureConditionalJournalFormatLocked performs for an empty
	// live window. Without this, a collection that already holds kind-3 records
	// fails prepare with ErrConditionalPrepareUnsupportedJournal.
	for _, state := range dirty {
		coll := t.db.Collection(state.name)
		if err := coll.Flush(); err != nil {
			return err
		}
	}
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
	maxKey, maxDoc := c.tx.db.documentBounds()
	if len(document) == 0 || len(document) > maxDoc {
		return false, ErrDocumentTooLarge
	}
	if len(key) > maxKey {
		return false, ErrKeyTooLarge
	}
	if err := validateDocument(c.tx.db.engine.Collection, document); err != nil {
		return false, err
	}
	canonical, err := vibejson.AppendCanonicalize(c.state.canonical[:0], document)
	if err != nil {
		return false, err
	}
	c.state.canonical = canonical
	owned := append([]byte(nil), canonical...)
	baseExisted, err := c.baseExisted(key)
	if err != nil {
		return false, err
	}
	visible, err := c.visible(key)
	if err != nil {
		return false, err
	}
	if err := c.tx.stage(c.state, key, owned, false, baseExisted); err != nil {
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
	maxKey, _ := c.tx.db.documentBounds()
	if len(key) == 0 || len(key) > maxKey {
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

func (d *Database) documentBounds() (maxKey, maxDoc int) {
	if d == nil {
		return defaultMaxKeyBytes, defaultMaxDocumentBytes
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

// clockLocked returns the per-collection conflict clock, creating it if needed.
// Caller must hold clockMu.
func (d *Database) clockLocked(name string) *txnclock.Clock {
	if d.clocks == nil {
		d.clocks = make(map[string]*txnclock.Clock)
	}
	clock := d.clocks[name]
	if clock == nil {
		clock = &txnclock.Clock{}
		d.clocks[name] = clock
	}
	return clock
}

func (d *Database) armClocksForBegin(tx *Tx) {
	d.clockMu.Lock()
	d.rwTxCount++
	d.clockMu.Unlock()
	_ = tx
}

func (d *Database) finishClocks(tx *Tx, published map[string][]string) {
	d.clockMu.Lock()
	defer d.clockMu.Unlock()
	if published != nil {
		for name, keys := range published {
			d.clockLocked(name).RecordKeys(keys)
		}
	}
	for name, rev := range tx.beginRevs {
		if clock := d.clocks[name]; clock != nil {
			clock.Finish(rev)
			clock.Disarm()
		}
	}
	if d.rwTxCount > 0 {
		d.rwTxCount--
	}
}

func (d *Database) recordClockKey(name, key string) {
	d.clockMu.Lock()
	defer d.clockMu.Unlock()
	clock := d.clocks[name]
	if clock == nil {
		return
	}
	clock.RecordKeys([]string{key})
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
