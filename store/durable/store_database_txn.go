package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Multi-collection durable write.
//
// UpdateCollections is the caller-owned write dual of SnapshotCollections:
// stage every dirty participant, prepare a kind-4 conditional record in each
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

// TxnLogOptions configures transaction-log construction and recovery. A zero Capacity selects the decision-log
// package default at mint time. SealedCapacity requires a non-zero exact
// Capacity and makes txn.vtm a Linux-only immutable physical allocation.
type TxnLogOptions struct {
	Capacity       uint64
	SealedCapacity bool
}

// TxnLog owns one database directory's txn.vtm decision log: lazy mint under
// the L2 creation fence, decision append/sync, epoch recycle under pressure,
// undischarged-decision accounting, and catalog-scope poison after a decision
// sync failure.
type TxnLog struct {
	dir  string
	path string
	root *os.Root
	// rootInfo is the physical directory identity captured from root. It lets
	// lazy mint reject mismatched participants before creating txn.vtm.
	rootInfo os.FileInfo
	opts     TxnLogOptions

	commitMu sync.Mutex
	marker   *storeio.TxnMarker
	// nextTxnID is monotonic within the current epoch, seeded from the open
	// scan's MaxTxnID (or 1 for a fresh mint).
	nextTxnID uint64
	// undischarged counts decisions whose participant journals may still hold
	// matching kind-4 records. Recycle is legal only when this is zero and no
	// registered journal holds a current-epoch conditional.
	undischarged int
	poison       error

	regMu       sync.Mutex
	registered  map[*Collection]struct{}
	collections []*Collection
}

// NewTxnLog constructs an unminted decision-log owner for a fresh catalog.
// Any pre-existing txn.vtm is refused: only OpenCollectionsWithTransactions
// may reopen a marker because decision-log recycle requires the complete live
// participant catalog. The mint fence runs at the head of the first K ≥ 2
// commit.
func NewTxnLog(dir string, options TxnLogOptions) (*TxnLog, error) {
	log, err := newTxnLogDirectory(dir, options)
	if err != nil {
		return nil, err
	}
	if _, err := log.root.Lstat(txnMarkerFilename); err == nil {
		_ = log.Close()
		return nil, ErrTransactionLogRecoveryRequired
	} else if !os.IsNotExist(err) {
		_ = log.Close()
		return nil, err
	}
	holds, err := directoryHoldsAnyConditional(log.root)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	if holds {
		_ = log.Close()
		return nil, fmt.Errorf(
			"%w: fresh transaction log directory retains conditional records",
			ErrTransactionLogMissing,
		)
	}
	return log, nil
}

func newTxnLogDirectory(
	dir string, options TxnLogOptions,
) (*TxnLog, error) {
	if dir == "" {
		return nil, fmt.Errorf("vibedb: transaction log requires a directory")
	}
	if options.SealedCapacity &&
		(options.Capacity == 0 ||
			options.Capacity > storeio.TxnMarkerMaxCapacityBytes ||
			options.Capacity%storeio.TxnMarkerMinSectorSize != 0) {
		return nil, fmt.Errorf(
			"%w: sealed transaction log requires an exact sector-aligned capacity",
			ErrSealedJournalCapacity,
		)
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(filepath.Clean(absolute))
	if err != nil {
		return nil, err
	}
	dir = filepath.Clean(dir)
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	rootInfo, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	path := filepath.Join(dir, txnMarkerFilename)
	log := &TxnLog{
		dir:        dir,
		path:       path,
		root:       root,
		rootInfo:   rootInfo,
		opts:       options,
		nextTxnID:  1,
		registered: make(map[*Collection]struct{}),
	}
	return log, nil
}

func syncTxnLogDirectory(root *os.Root) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if root == nil {
		return fmt.Errorf("vibedb: transaction log directory is not open")
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

// Close closes the underlying decision-log file when present.
func (l *TxnLog) Close() error {
	if l == nil {
		return nil
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	var markerErr, rootErr error
	if l.marker != nil {
		markerErr = l.marker.Close()
	}
	l.marker = nil
	if l.root != nil {
		rootErr = l.root.Close()
		l.root = nil
	}
	return errors.Join(markerErr, rootErr)
}

// ValidateCollections proves that every named collection is an exact live
// regular-file entry in the physical directory owned by this transaction log.
// It performs no registration, marker minting, staging, or publication.
//
// Integration layers use this before they admit work whose apply path may
// require a multi-collection decision. The commit path repeats the same proof:
// this construction-time check prevents a static wiring error from being
// discovered only after a committed command reaches apply; it is not a lease
// over future namespace changes.
func (l *TxnLog) ValidateCollections(members []NamedCollection) error {
	ordered, err := validateTxnMembers(members)
	if err != nil {
		return err
	}
	if len(ordered) == 0 {
		return nil
	}
	if l == nil {
		return fmt.Errorf("%w: nil transaction log", ErrTxnParticipant)
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	return l.validateCollectionsLocked(ordered)
}

// validateCollectionsLocked performs ValidateCollections' live-handle and
// directory proof while commitMu is held. Commit admission uses it before
// adding caller-supplied handles to the persistent catalog registry, so a
// definite invalid-member refusal cannot brick all later commits.
func (l *TxnLog) validateCollectionsLocked(ordered []NamedCollection) error {
	if l.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, l.poison)
	}
	if l.root == nil || l.rootInfo == nil {
		return fmt.Errorf("vibedb: transaction log directory is not open")
	}
	if l.marker != nil {
		current, currentErr := l.marker.EntryCurrent()
		if currentErr != nil {
			return fmt.Errorf("vibedb: prove transaction log entry: %w", currentErr)
		}
		if !current {
			return fmt.Errorf(
				"%w: transaction log entry changed while open",
				storeio.ErrTxnMarkerCorrupt,
			)
		}
	}
	collections := make([]*Collection, len(ordered))
	nameOf := make(map[*Collection]string, len(ordered))
	for i := range ordered {
		collections[i] = ordered[i].Collection
		nameOf[ordered[i].Collection] = ordered[i].Name
	}
	sortCollectionSnapshotOrder(collections)
	for _, collection := range collections {
		collection.writer.Lock()
	}
	defer func() {
		for i := len(collections) - 1; i >= 0; i-- {
			collections[i].writer.Unlock()
		}
	}()
	for _, collection := range collections {
		name := nameOf[collection]
		if collection.closed {
			return fmt.Errorf("%w: closed collection %q", ErrTxnParticipant, name)
		}
		if failure := collection.PersistenceError(); failure != nil {
			return fmt.Errorf("vibedb: collection %q persistence: %w", name, failure)
		}
		if !databaseTxnLaneSupported(collection) {
			return fmt.Errorf("%w: %s", ErrDatabaseTransactionUnsupportedLane, name)
		}
		if collection.file == nil {
			return fmt.Errorf("%w: closed collection %q", ErrTxnParticipant, name)
		}
		var matches bool
		var matchErr error
		if l.marker != nil {
			matches, matchErr = l.marker.MatchesFileDirectory(collection.file)
		} else {
			matches, matchErr = storeio.FileMatchesDirectory(l.rootInfo, collection.file)
		}
		if matchErr != nil {
			return fmt.Errorf(
				"vibedb: prove collection %q transaction directory: %w",
				name, matchErr,
			)
		}
		if !matches {
			return fmt.Errorf("%w: %s", ErrTransactionLogDirectoryMismatch, name)
		}
	}
	return nil
}

// registerCollection records c as catalog-scoped under this log so a later
// decision-sync failure poisons it alongside commit participants.
func (l *TxnLog) registerCollection(c *Collection) {
	if l == nil || c == nil {
		return
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	l.registerCollectionLocked(c)
}

// AdoptCollection attaches a freshly published collection to an already-open
// catalog transaction log. It is intentionally not a recovery API: the
// collection must have an empty paired journal. The proof and registration run
// under the log commit fence, so the handle cannot be admitted concurrently
// with a decision append.
func (l *TxnLog) AdoptCollection(c *Collection) error {
	if l == nil || c == nil {
		return fmt.Errorf("%w: nil collection", ErrTxnParticipant)
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	if l.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, l.poison)
	}
	c.writer.Lock()
	defer c.writer.Unlock()
	if c.closed || c.file == nil || !databaseTxnLaneSupported(c) {
		return fmt.Errorf("%w: collection is not transaction-capable", ErrTxnParticipant)
	}
	if !c.journalEnabled() || c.journal.Cursor() != 0 {
		return fmt.Errorf(
			"%w: adopted collection journal is not empty", ErrTxnParticipant,
		)
	}
	matches, err := storeio.FileMatchesDirectory(l.rootInfo, c.file)
	if err != nil {
		return fmt.Errorf(
			"vibedb: prove adopted collection transaction directory: %w", err,
		)
	}
	if !matches {
		return ErrTransactionLogDirectoryMismatch
	}
	l.registerCollectionLocked(c)
	return nil
}

func (l *TxnLog) registerCollectionLocked(c *Collection) {
	if c == nil {
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

// DetachCollection removes one live collection from this log's catalog scope.
// If the current marker contains decisions, detach first folds every registered
// participant past its current-epoch conditionals and recycles the marker to an
// empty epoch. It then checkpoints the target's remaining ordinary journal
// window so the exact handle can be passed back to [TxnLog.AdoptCollection] if
// the caller's catalog mutation definitely did not publish.
//
// The complete discharge and unregister run under commitMu. On error the
// collection remains registered; callers must not close or unlink it. The
// caller must keep the collection out of concurrent UpdateCollections member
// sets until the catalog mutation publishes or AdoptCollection rolls it back,
// because a commit automatically registers every supplied member.
func (l *TxnLog) DetachCollection(c *Collection) error {
	if l == nil || c == nil {
		return fmt.Errorf("%w: nil collection", ErrTxnParticipant)
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()

	l.regMu.Lock()
	_, registered := l.registered[c]
	l.regMu.Unlock()
	if !registered {
		return nil
	}
	if l.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, l.poison)
	}
	if l.root == nil || l.rootInfo == nil {
		return fmt.Errorf("vibedb: transaction log directory is not open")
	}
	// A failed decision append can leave durable conditional prepares while the
	// marker cursor remains zero. Cursor position therefore cannot prove there
	// is nothing to resolve; rescan and discharge every registered journal
	// whenever a marker exists before detaching catalog ownership.
	if l.marker != nil {
		if err := l.foldLaggardsAndRecycleLocked(); err != nil {
			return err
		}
	}

	// A marker recycle consumes every conditional window, but the target may
	// still hold a later ordinary atomic/delta window. Empty that window too so
	// rollback can use AdoptCollection's deliberately strict empty-journal seam.
	c.writer.Lock()
	var checkpointErr error
	if c.closed || c.file == nil || !databaseTxnLaneSupported(c) {
		checkpointErr = fmt.Errorf(
			"%w: collection is not transaction-capable", ErrTxnParticipant,
		)
	} else if c.journalEnabled() && c.journal.Cursor() != 0 {
		checkpointErr = c.checkpointPastConditionalsLocked(nil, 0)
	}
	c.writer.Unlock()
	if checkpointErr != nil {
		return checkpointErr
	}

	l.unregisterCollectionLocked(c)
	return nil
}

func (l *TxnLog) unregisterCollection(c *Collection) {
	if l == nil || c == nil {
		return
	}
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	l.unregisterCollectionLocked(c)
}

func (l *TxnLog) unregisterCollectionLocked(c *Collection) {
	l.regMu.Lock()
	defer l.regMu.Unlock()
	if _, ok := l.registered[c]; !ok {
		return
	}
	delete(l.registered, c)
	for i := range l.collections {
		if l.collections[i] != c {
			continue
		}
		copy(l.collections[i:], l.collections[i+1:])
		l.collections[len(l.collections)-1] = nil
		l.collections = l.collections[:len(l.collections)-1]
		break
	}
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
	return log.commitMulti(dirty, ordered, batch.byName)
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
	databaseTxnAfterMintHook              func(l *TxnLog)
	databaseTxnAfterStageHook             func(index int, name string) error
	databaseTxnBeforeMarkerRecycleHook    func(l *TxnLog)
	databaseTxnBeforeRetirementAppendHook func(l *TxnLog)
)

func (l *TxnLog) commitMulti(
	dirty []NamedCollection,
	members []NamedCollection,
	byName map[string]*WriteBatch,
) (err error) {
	l.commitMu.Lock()
	defer l.commitMu.Unlock()
	if err := l.validateCollectionsLocked(members); err != nil {
		return err
	}
	for _, member := range members {
		l.registerCollectionLocked(member.Collection)
	}

	if l.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, l.poison)
	}
	// L2 mint fence: complete before any writer is seized for staging, and
	// before any prepare may reference the minted MarkerID.
	if l.marker == nil {
		if err := l.verifyRootDirectoryLocked(); err != nil {
			return err
		}
		if err := l.ensureMintedLocked(); err != nil {
			return err
		}
	}
	if err := l.verifyMarkerDirectoryLocked(); err != nil {
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
	}
	if l.nextTxnID == ^uint64(0) {
		return fmt.Errorf("%w: transaction id space exhausted", ErrTxnTooLarge)
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
		st, stageErr := c.stagePrimaryBatchConditionalLocked(wb)
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
		// A positional write can report a short/error result after the raw
		// checksummed decision body reached the page cache. Padding is not part of
		// the authenticated record, so reopen may still observe a committed
		// decision. Treat every unexpected append failure as catalog-wide unknown
		// outcome; only the fully preflighted capacity refusal is definite.
		poisoned := journalCommitOutcomeUnknown(appendErr)
		l.poison = poisoned
		for _, c := range l.registeredCollections() {
			_ = joinCatalogCommitOutcomeUnknown(c, appendErr)
		}
		return poisoned
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

func (l *TxnLog) verifyRootDirectoryLocked() error {
	if l == nil || l.root == nil || l.rootInfo == nil {
		return fmt.Errorf("vibedb: transaction log directory is not open")
	}
	l.regMu.Lock()
	defer l.regMu.Unlock()
	for _, collection := range l.collections {
		if collection == nil || collection.file == nil {
			return fmt.Errorf("%w: nil registered collection", ErrTxnParticipant)
		}
		matches, err := storeio.FileMatchesDirectory(l.rootInfo, collection.file)
		if err != nil {
			return fmt.Errorf(
				"vibedb: prove collection transaction directory: %w", err,
			)
		}
		if !matches {
			return ErrTransactionLogDirectoryMismatch
		}
	}
	return nil
}

func (l *TxnLog) verifyMarkerDirectoryLocked() error {
	if l == nil || l.marker == nil {
		return fmt.Errorf("vibedb: transaction log marker is not open")
	}
	current, err := l.marker.EntryCurrent()
	if err != nil {
		return fmt.Errorf("vibedb: prove transaction log entry: %w", err)
	}
	if !current {
		return fmt.Errorf(
			"%w: transaction log entry changed while open",
			storeio.ErrTxnMarkerCorrupt,
		)
	}
	l.regMu.Lock()
	defer l.regMu.Unlock()
	for _, collection := range l.collections {
		if collection == nil || collection.file == nil {
			return fmt.Errorf("%w: nil registered collection", ErrTxnParticipant)
		}
		matches, err := l.marker.MatchesFileDirectory(collection.file)
		if err != nil {
			return fmt.Errorf(
				"vibedb: prove collection transaction directory: %w", err,
			)
		}
		if !matches {
			return ErrTransactionLogDirectoryMismatch
		}
	}
	return nil
}

func (l *TxnLog) ensureMintedLocked() error {
	if l.marker != nil {
		return nil
	}
	marker, err := storeio.CreateTxnMarkerAt(
		l.root, txnMarkerFilename,
		storeio.TxnMarkerOptions{
			Capacity: l.opts.Capacity, SealedCapacity: l.opts.SealedCapacity,
		},
	)
	if err != nil {
		if os.IsExist(err) {
			// Absence at NewTxnLog is not an ownership lease. A racing creator or
			// an earlier unknown mint outcome may own this entry and may later name
			// participants unknown to this handle. Never adopt it here.
			refusal := fmt.Errorf(
				"%w: transaction marker appeared before mint",
				ErrTransactionLogRecoveryRequired,
			)
			l.poison = refusal
			return refusal
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
	if l.marker.NextSequence() == 0 {
		return fmt.Errorf(
			"%w: decision-log sequence space exhausted", ErrTxnTooLarge,
		)
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
	decisions, err := rescanTxnLogMarker(l)
	if err != nil {
		return err
	}
	for _, c := range l.registeredCollections() {
		c.writer.Lock()
		holds, holdsErr := c.journalHoldsConditional(header.MarkerID, header.Epoch)
		var foldErr error
		if holdsErr == nil && holds {
			foldErr = c.checkpointPastConditionalsLocked(
				participantBindingResolver(decisions, c.storeID, c.journalID),
				decisions.Epoch(),
			)
		}
		c.writer.Unlock()
		if holdsErr != nil || foldErr != nil {
			return errors.Join(holdsErr, foldErr)
		}
	}
	for _, c := range l.registeredCollections() {
		c.writer.Lock()
		holds, holdsErr := c.journalHoldsConditional(header.MarkerID, header.Epoch)
		c.writer.Unlock()
		if holdsErr != nil {
			return holdsErr
		}
		if holds {
			return fmt.Errorf(
				"vibedb: decision log recycle blocked by conditional records",
			)
		}
	}
	// The registered catalog is complete for transaction recovery, but retired
	// or crash-orphaned sidecars can intentionally remain until their namespace
	// cleanup fence succeeds. Never recycle away the only decisions that can
	// resolve such a conditional. Cleanup may remove the sidecar and a later
	// reopen/recycle will re-evaluate this same directory-wide predicate.
	directoryHolds, err := directoryHoldsAnyConditional(l.root)
	if err != nil {
		return err
	}
	if directoryHolds {
		return fmt.Errorf(
			"vibedb: decision log recycle blocked by an unregistered conditional journal",
		)
	}
	if databaseTxnBeforeMarkerRecycleHook != nil {
		databaseTxnBeforeMarkerRecycleHook(l)
	}
	if recycleErr := l.marker.Recycle(header.Epoch + 1); recycleErr != nil {
		// A recycle header write or sync may already have published the next
		// epoch even though the in-memory marker deliberately retains the old
		// header. No later decision may be appended through that stale view.
		// Treat every recycle failure as an unknown catalog-wide outcome and
		// require a complete reopen/reconciliation before further work.
		poisoned := journalCommitOutcomeUnknown(recycleErr)
		l.poison = poisoned
		for _, c := range l.registeredCollections() {
			_ = joinCatalogCommitOutcomeUnknown(c, recycleErr)
		}
		return poisoned
	}
	l.undischarged = 0
	return nil
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

// Database↔TxnLog registry. Database open installs the complete recovered log,
// Update lazily constructs a fresh log only for a new catalog, and Close
// removes and closes the owned handle.
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
	l, err := NewTxnLog(d.dir, TxnLogOptions{})
	if err != nil {
		return nil, err
	}
	databaseTxnRegistry[d] = l
	return l, nil
}
