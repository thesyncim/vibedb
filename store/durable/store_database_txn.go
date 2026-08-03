package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Multi-collection durable write.
//
// UpdateCollections is the caller-owned write dual of SnapshotCollections:
// stage every dirty participant, prepare a kind-5 conditional record in each
// journal, append and sync one decision record (the sole atomic commit point),
// then publish under every snapshotGate held at once. Database.Update wraps the
// same protocol over the catalog.
//
// A write set that dirties exactly one collection takes today's
// Collection.Update path unchanged — no conditional record, no decision, one
// sync, byte-identical journal output.

const txnMarkerFilename = "txn.vtm"

var (
	// ErrDatabaseTransactionUnsupportedLane reports that a participant's
	// durability lane cannot join a multi-collection commit. Supported lanes
	// are sync-journal and buffered-journal; buffered-volatile, async-COW, and
	// chain-fence are refused with nothing staged.
	ErrDatabaseTransactionUnsupportedLane = errors.New(
		"vibedb: a database transaction requires a journal-backed lane (sync-journal or buffered-journal)",
	)
	// ErrTxnTooLarge reports that a multi-collection commit exceeds a
	// cross-participant bound (collections, documents, bytes, or decision
	// record capacity). Nothing was staged.
	ErrTxnTooLarge = errors.New(
		"vibedb: database transaction exceeds a bounded limit",
	)
	// ErrTxnLimitsRequired reports that UpdateCollections received a zero
	// TxnLimits dimension for a K ≥ 2 commit. The primitive never substitutes
	// defaults; Database.Update owns that normalization.
	ErrTxnLimitsRequired = errors.New(
		"vibedb: database transaction requires explicit non-zero TxnLimits",
	)
	// ErrTxnParticipant reports a nil participant, an unnamed collection, a
	// duplicate name, or a DatabaseBatch.Collection name outside the member set.
	ErrTxnParticipant = errors.New(
		"vibedb: invalid database transaction participant",
	)
	// ErrTxnLogPoisoned reports that a prior decision-sync failure poisoned the
	// catalog-scoped transaction log. Close and reopen the database to resolve
	// the unknown outcome before retrying.
	ErrTxnLogPoisoned = errors.New(
		"vibedb: database transaction log is poisoned after an unknown commit outcome",
	)
)

// TxnLogOptions configures OpenTxnLog. A zero Capacity selects the decision-log
// package default at mint time.
type TxnLogOptions struct {
	Capacity uint64
}

// TxnLog owns one database directory's txn.vtm decision log: lazy mint under
// the L2 creation fence, decision append/sync, epoch recycle under pressure,
// undischarged-decision accounting, and catalog-scope poison after a decision
// sync failure.
type TxnLog struct {
	dir  string
	path string
	opts TxnLogOptions

	commitMu sync.Mutex
	marker   *storeio.TxnMarker
	// nextTxnID is monotonic within the current epoch, seeded from the open
	// scan's MaxTxnID (or 1 for a fresh mint).
	nextTxnID uint64
	// undischarged counts decisions whose participant journals may still hold
	// matching kind-5 records. Recycle is legal only when this is zero and no
	// registered journal holds a current-epoch conditional.
	undischarged int
	poison       error

	regMu       sync.Mutex
	registered  map[*Collection]struct{}
	collections []*Collection
}

// OpenTxnLog opens dir's decision log, lazily leaving txn.vtm unminted when
// absent. The mint fence runs at the head of the first K ≥ 2 commit.
func OpenTxnLog(dir string, options TxnLogOptions) (*TxnLog, error) {
	if dir == "" {
		return nil, fmt.Errorf("vibedb: transaction log requires a directory")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir = filepath.Clean(absolute)
	path := filepath.Join(dir, txnMarkerFilename)
	log := &TxnLog{
		dir:         dir,
		path:        path,
		opts:        options,
		nextTxnID:   1,
		registered:  make(map[*Collection]struct{}),
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return log, nil
		}
		return nil, err
	}
	if err := log.openExisting(); err != nil {
		return nil, err
	}
	return log, nil
}

func (l *TxnLog) openExisting() error {
	marker, decisions, err := storeio.OpenTxnMarker(
		l.path, storeio.TxnMarkerOptions{Capacity: l.opts.Capacity},
	)
	if err != nil {
		return err
	}
	l.marker = marker
	l.nextTxnID = decisions.MaxTxnID() + 1
	if l.nextTxnID == 0 {
		l.nextTxnID = 1
	}
	// Open cannot prove discharge without per-collection roots; treat every
	// scanned decision as undischarged until a pressure fold or T5's
	// reconciliation clears them. An empty scan leaves undischarged at zero.
	if decisions != nil {
		// MaxDCSN counts every record (decision + retirement). Seed a lower
		// bound from MaxTxnID presence: each decided txn is one undischarged
		// unit until folded. Exact membership is rebuilt on first pressure fold.
		if decisions.MaxTxnID() != 0 {
			l.undischarged = 1
		}
	}
	return nil
}

// Close closes the underlying decision-log file when present.
func (l *TxnLog) Close() error {
	if l == nil {
		return nil
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	err := l.marker.Close()
	l.marker = nil
	return err
}

// registerCollection records c as catalog-scoped under this log so a later
// decision-sync failure poisons it alongside commit participants.
func (l *TxnLog) registerCollection(c *Collection) {
	if l == nil || c == nil {
		return
	}
	l.regMu.Lock()
	defer l.regMu.Unlock()
	if _, ok := l.registered[c]; ok {
		return
	}
	l.registered[c] = struct{}{}
	l.collections = append(l.collections, c)
}

func (l *TxnLog) registeredCollections() []*Collection {
	l.regMu.Lock()
	defer l.regMu.Unlock()
	out := make([]*Collection, len(l.collections))
	copy(out, l.collections)
	return out
}

// DatabaseBatch is the per-commit staging handle passed to UpdateCollections /
// Database.Update. Collection returns the WriteBatch for a member name.
type DatabaseBatch struct {
	byName map[string]*WriteBatch
}

// Collection returns the member WriteBatch for name.
func (b *DatabaseBatch) Collection(name string) (*WriteBatch, error) {
	if b == nil {
		return nil, ErrTxnParticipant
	}
	batch, ok := b.byName[name]
	if !ok || batch == nil {
		return nil, fmt.Errorf("%w: %q", ErrTxnParticipant, name)
	}
	return batch, nil
}

// UpdateCollections stages per-collection mutations via fn, then commits the
// dirty member set. A single dirty member routes through Collection.Update.
// K ≥ 2 runs the conditional-prepare / decision-sync / multi-gate publish
// protocol under log's commit mutex.
//
// limits is fail-closed at its zero value for K ≥ 2: UpdateCollections never
// substitutes package defaults.
func UpdateCollections(
	log *TxnLog,
	members []NamedCollection,
	limits TxnLimits,
	fn func(*DatabaseBatch) error,
) error {
	if fn == nil {
		return errors.New("vibedb: UpdateCollections requires a function")
	}
	if log == nil {
		return fmt.Errorf("%w: nil transaction log", ErrTxnParticipant)
	}
	ordered, err := validateTxnMembers(members)
	if err != nil {
		return err
	}

	batch := &DatabaseBatch{byName: make(map[string]*WriteBatch, len(ordered))}
	batches := make([]*WriteBatch, len(ordered))
	for i, member := range ordered {
		wb := &WriteBatch{
			collection: member.Collection,
			position:   make(map[string]int, member.Collection.options.MaxBatchDocuments),
			active:     true,
		}
		batches[i] = wb
		batch.byName[member.Name] = wb
	}
	defer closeDurableWriteBatches(batches)

	if err := fn(batch); err != nil {
		return err
	}

	dirty := make([]NamedCollection, 0, len(ordered))
	var totalDocs int
	var totalBytes int64
	for _, member := range ordered {
		wb := batch.byName[member.Name]
		if wb.Len() == 0 {
			continue
		}
		dirty = append(dirty, member)
		totalDocs += wb.Len()
		totalBytes += int64(len(wb.keys) + len(wb.values))
	}
	if len(dirty) == 0 {
		return nil
	}
	if len(dirty) == 1 {
		return applyWriteBatchViaUpdate(
			dirty[0].Collection, batch.byName[dirty[0].Name],
		)
	}

	if err := checkTxnLimits(limits, len(dirty), totalDocs, totalBytes); err != nil {
		return err
	}
	for _, member := range ordered {
		log.registerCollection(member.Collection)
	}
	return log.commitMulti(dirty, batch.byName)
}

func checkTxnLimits(limits TxnLimits, collections, documents int, bytes int64) error {
	if limits.MaxCollections == 0 || limits.MaxDocuments == 0 || limits.MaxBytes == 0 {
		return ErrTxnLimitsRequired
	}
	maxCollections := limits.MaxCollections
	if maxCollections > storeio.TxnMarkerMaxParticipants {
		maxCollections = storeio.TxnMarkerMaxParticipants
	}
	if collections > maxCollections {
		return fmt.Errorf(
			"%w: MaxCollections %d > %d",
			ErrTxnTooLarge, collections, maxCollections,
		)
	}
	if documents > limits.MaxDocuments {
		return fmt.Errorf(
			"%w: MaxDocuments %d > %d",
			ErrTxnTooLarge, documents, limits.MaxDocuments,
		)
	}
	if bytes > limits.MaxBytes {
		return fmt.Errorf(
			"%w: MaxBytes %d > %d",
			ErrTxnTooLarge, bytes, limits.MaxBytes,
		)
	}
	if collections > storeio.TxnMarkerMaxParticipants {
		return fmt.Errorf(
			"%w: decision participant capacity %d",
			ErrTxnTooLarge, storeio.TxnMarkerMaxParticipants,
		)
	}
	return nil
}

func validateTxnMembers(members []NamedCollection) ([]NamedCollection, error) {
	if len(members) == 0 {
		return nil, nil
	}
	ordered := append([]NamedCollection(nil), members...)
	// Writer seizure uses the process-global snapshot order over handles;
	// member slice order only names the DatabaseBatch map.
	seenName := make(map[string]struct{}, len(ordered))
	seenHandle := make(map[*Collection]string, len(ordered))
	for i := range ordered {
		member := ordered[i]
		if member.Name == "" {
			return nil, ErrCollectionName
		}
		if _, dup := seenName[member.Name]; dup {
			return nil, fmt.Errorf(
				"%w: duplicate name %q", ErrTxnParticipant, member.Name,
			)
		}
		seenName[member.Name] = struct{}{}
		if member.Collection == nil {
			return nil, fmt.Errorf(
				"%w: nil collection %q", ErrTxnParticipant, member.Name,
			)
		}
		if previous, exists := seenHandle[member.Collection]; exists {
			return nil, fmt.Errorf(
				"vibedb: one durable collection cannot be cataloged as both %q and %q",
				previous, member.Name,
			)
		}
		seenHandle[member.Collection] = member.Name
	}
	return ordered, nil
}

func closeDurableWriteBatches(batches []*WriteBatch) {
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		batch.active = false
		batch.reset()
	}
}

func applyWriteBatchViaUpdate(c *Collection, src *WriteBatch) error {
	entries := append([]writeBatchEntry(nil), src.entries...)
	keys := append([]byte(nil), src.keys...)
	values := append([]byte(nil), src.values...)
	return c.Update(func(dst *WriteBatch) error {
		for _, entry := range entries {
			key := keys[entry.keyOffset : entry.keyOffset+entry.keyLength]
			if entry.remove {
				if err := dst.Delete(key); err != nil {
					return err
				}
				continue
			}
			value := values[entry.valueOffset : entry.valueOffset+entry.valueLength]
			if err := dst.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
}

func databaseTxnLaneSupported(c *Collection) bool {
	return c.syncJournalLane() || c.bufferedJournalAckLane()
}

// Test hooks. Production leaves them nil.
var (
	databaseTxnAfterMintHook  func(l *TxnLog)
	databaseTxnAfterStageHook func(index int, name string) error
)

func (l *TxnLog) commitMulti(
	dirty []NamedCollection,
	byName map[string]*WriteBatch,
) (err error) {
	l.commitMu.Lock()
	defer l.commitMu.Unlock()

	if l.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, l.poison)
	}
	// L2 mint fence: complete before any writer is seized for staging, and
	// before any prepare may reference the minted MarkerID.
	if err := l.ensureMintedLocked(); err != nil {
		return err
	}
	if err := l.ensureDecisionRoomLocked(len(dirty)); err != nil {
		return err
	}

	order := make([]*Collection, len(dirty))
	for i := range dirty {
		order[i] = dirty[i].Collection
	}
	sortCollectionSnapshotOrder(order)
	nameOf := make(map[*Collection]string, len(dirty))
	for i := range dirty {
		nameOf[dirty[i].Collection] = dirty[i].Name
	}

	for _, c := range order {
		c.writer.Lock()
	}
	locked := len(order)
	defer func() {
		for i := locked - 1; i >= 0; i-- {
			order[i].writer.Unlock()
		}
	}()

	for _, c := range order {
		if failure := c.PersistenceError(); failure != nil {
			return failure
		}
		if c.closed {
			return ErrClosed
		}
		if !databaseTxnLaneSupported(c) {
			return fmt.Errorf(
				"%w: %s", ErrDatabaseTransactionUnsupportedLane, nameOf[c],
			)
		}
		// Catalog-owned journals mint/recycle at the conditional format word.
		// T5 wires this at open; until then the coordinator sets it under the
		// writer for every multi-collection participant.
		c.journalCatalogOwned = true
	}

	staged := make([]stagedPrimaryBatch, len(order))
	stagedLive := 0
	defer func() {
		if err == nil {
			return
		}
		for i := stagedLive - 1; i >= 0; i-- {
			order[i].unwindStagedPrimaryBatch(&staged[i])
		}
	}()

	for i, c := range order {
		wb := byName[nameOf[c]]
		st, stageErr := c.stagePrimaryBatchLocked(wb)
		if stageErr != nil {
			return stageErr
		}
		if !st.live {
			// Absent-only deletes: treat as not participating. Should not
			// reach commitMulti with a dirty Len()>0 that stages empty, but
			// refuse closed rather than preparing a hollow record.
			return fmt.Errorf(
				"%w: staged empty batch for %q", ErrTxnParticipant, nameOf[c],
			)
		}
		staged[i] = st
		stagedLive = i + 1
		if databaseTxnAfterStageHook != nil {
			if hookErr := databaseTxnAfterStageHook(i, nameOf[c]); hookErr != nil {
				return hookErr
			}
		}
	}

	header := l.marker.Header()
	txnID := l.nextTxnID
	l.nextTxnID++
	participants := make([]storeio.TxnParticipant, len(order))
	for i, c := range order {
		if prepErr := c.preparePrimaryBatchConditionalLocked(
			&staged[i], header.MarkerID, header.Epoch, txnID, true,
		); prepErr != nil {
			return prepErr
		}
		participants[i] = storeio.TxnParticipant{
			StoreID:            c.storeID,
			JournalID:          c.journalID,
			PreparedGeneration: staged[i].generation,
		}
	}

	if _, appendErr := l.marker.AppendDecision(txnID, participants); appendErr != nil {
		if errors.Is(appendErr, storeio.ErrTxnMarkerFull) {
			return fmt.Errorf("%w: decision log full", ErrTxnTooLarge)
		}
		return appendErr
	}
	if syncErr := l.marker.Sync(); syncErr != nil {
		poisoned := journalCommitOutcomeUnknown(syncErr)
		l.poison = poisoned
		for _, c := range l.registeredCollections() {
			_ = joinCatalogCommitOutcomeUnknown(c, syncErr)
		}
		return poisoned
	}
	l.undischarged++

	// Publish is infallible by construction: every fallible prepare completed
	// and the decision is durable. Acquire every gate in snapshot order, flip,
	// then release gates and writers LIFO.
	for _, c := range order {
		c.snapshotGate.Lock()
	}
	for i, c := range order {
		c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
		c.publishPrimaryBatchGateHeld(staged[i])
		staged[i].live = false
	}
	for i := len(order) - 1; i >= 0; i-- {
		order[i].snapshotGate.Unlock()
	}
	stagedLive = 0
	return nil
}

func (l *TxnLog) ensureMintedLocked() error {
	if l.marker != nil {
		return nil
	}
	marker, err := storeio.CreateTxnMarker(
		l.path, storeio.TxnMarkerOptions{Capacity: l.opts.Capacity},
	)
	if err != nil {
		if os.IsExist(err) {
			return l.openExisting()
		}
		return err
	}
	l.marker = marker
	l.nextTxnID = 1
	l.undischarged = 0
	if databaseTxnAfterMintHook != nil {
		databaseTxnAfterMintHook(l)
	}
	return nil
}

func (l *TxnLog) ensureDecisionRoomLocked(participantCount int) error {
	padded, ok := txnDecisionRecordBytes(participantCount)
	if !ok {
		return fmt.Errorf(
			"%w: decision participant capacity", ErrTxnTooLarge,
		)
	}
	if l.marker == nil {
		return fmt.Errorf("vibedb: decision log mint missing before capacity check")
	}
	if l.marker.Cursor()+uint64(padded) <= l.marker.Header().Capacity {
		return nil
	}
	if err := l.foldLaggardsAndRecycleLocked(); err != nil {
		return err
	}
	if l.marker.Cursor()+uint64(padded) <= l.marker.Header().Capacity {
		return nil
	}
	return fmt.Errorf(
		"%w: decision record does not fit an empty log", ErrTxnTooLarge,
	)
}

func txnDecisionRecordBytes(participantCount int) (int, bool) {
	if participantCount <= 0 ||
		participantCount > storeio.TxnMarkerMaxParticipants {
		return 0, false
	}
	raw := storeio.TxnMarkerRecordPrefixSize +
		storeio.TxnMarkerRecordTrailerSize +
		participantCount*storeio.TxnParticipantSize
	sector := storeio.TxnMarkerMinSectorSize
	padded := (raw + sector - 1) / sector * sector
	return padded, true
}

func (l *TxnLog) foldLaggardsAndRecycleLocked() error {
	header := l.marker.Header()
	for _, c := range l.registeredCollections() {
		c.writer.Lock()
		holds := c.journalHoldsConditional(header.MarkerID, header.Epoch)
		var foldErr error
		if holds {
			foldErr = c.checkpointPastConditionalsLocked()
		}
		c.writer.Unlock()
		if foldErr != nil {
			return foldErr
		}
	}
	for _, c := range l.registeredCollections() {
		c.writer.Lock()
		holds := c.journalHoldsConditional(header.MarkerID, header.Epoch)
		c.writer.Unlock()
		if holds {
			return fmt.Errorf(
				"vibedb: decision log recycle blocked by conditional records",
			)
		}
	}
	l.undischarged = 0
	return l.marker.Recycle(header.Epoch + 1)
}

// Update is the catalog-owned convenience form of UpdateCollections. It holds
// the catalog read lock for the member set, lazily mints and attaches a TxnLog,
// normalizes zero TxnLimits dimensions to the pinned package defaults, and
// registers every cataloged collection for catalog-scope poison.
func (d *Database) Update(fn func(*DatabaseBatch) error) error {
	if d == nil {
		return ErrDatabaseClosed
	}
	if fn == nil {
		return errors.New("vibedb: Database.Update requires a function")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return ErrDatabaseClosed
	}
	log, err := ensureDatabaseTxnLog(d)
	if err != nil {
		return err
	}
	members := make([]NamedCollection, 0, len(d.order))
	for _, entry := range d.order {
		log.registerCollection(entry.collection)
		members = append(members, NamedCollection{
			Name: entry.name, Collection: entry.collection,
		})
	}
	return UpdateCollections(log, members, defaultTxnLimits(), fn)
}

// Interim Database↔TxnLog registry. T5, which owns store_database.go, wires
// attach into OpenDatabase and detach-plus-Close into Database.Close, and may
// migrate this map to a Database struct field (these seams then become thin
// wrappers). Until then Database.Update lazily mints and attaches.
var (
	databaseTxnRegistryMu sync.Mutex
	databaseTxnRegistry   = map[*Database]*TxnLog{}
)

func attachDatabaseTxnLog(d *Database, l *TxnLog) {
	if d == nil || l == nil {
		return
	}
	databaseTxnRegistryMu.Lock()
	databaseTxnRegistry[d] = l
	databaseTxnRegistryMu.Unlock()
}

func lookupDatabaseTxnLog(d *Database) *TxnLog {
	if d == nil {
		return nil
	}
	databaseTxnRegistryMu.Lock()
	defer databaseTxnRegistryMu.Unlock()
	return databaseTxnRegistry[d]
}

func detachDatabaseTxnLog(d *Database) *TxnLog {
	if d == nil {
		return nil
	}
	databaseTxnRegistryMu.Lock()
	defer databaseTxnRegistryMu.Unlock()
	l := databaseTxnRegistry[d]
	delete(databaseTxnRegistry, d)
	return l
}

func ensureDatabaseTxnLog(d *Database) (*TxnLog, error) {
	databaseTxnRegistryMu.Lock()
	defer databaseTxnRegistryMu.Unlock()
	if l := databaseTxnRegistry[d]; l != nil {
		return l, nil
	}
	l, err := OpenTxnLog(d.dir, TxnLogOptions{})
	if err != nil {
		return nil, err
	}
	databaseTxnRegistry[d] = l
	return l, nil
}
