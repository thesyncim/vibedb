package durable

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

var (
	// ErrTransactionCollectionMissing reports that a durable decision names a
	// target whose collection file or journal is missing or mismatched,
	// and no target-retired record covers that StoreID.
	ErrTransactionCollectionMissing = errors.New(
		"vibedb: a committed database transaction names a missing collection",
	)
	// ErrTransactionLogMissing reports that collection journals hold uncovered
	// conditional transaction records but the database's decision log is
	// absent or unusable. By the mint fence this state is unreachable by
	// crash; presumed abort would silently roll back acknowledged commits.
	ErrTransactionLogMissing = errors.New(
		"vibedb: collection journals hold conditional transaction records but the database's decision log is missing",
	)
	// ErrTransactionLogRecoveryRequired reports that txn.vtm already exists and
	// may name targets outside the caller's current set. Existing decision
	// logs may be opened only by a complete-catalog recovery operation.
	ErrTransactionLogRecoveryRequired = errors.New(
		"vibedb: existing transaction decision log requires complete catalog recovery",
	)
	// ErrTransactionLogDirectoryMismatch reports a decision log paired with a
	// collection file from another directory. Transaction identities are scoped
	// to one catalog directory and must never be reused across databases.
	ErrTransactionLogDirectoryMismatch = errors.New(
		"vibedb: transaction decision log and collection belong to different directories",
	)
)

// databaseTxnRecovery is the loaded decision-log state OpenDatabase and the
// complete-catalog caller-owned recovery path share before replay begins.
type databaseTxnRecovery struct {
	dir       string
	log       *TxnLog
	decisions *storeio.TxnDecisions
	// absent is true when txn.vtm is not present (or was removed as mint
	// residue). Collections open with the L1/L3 absent-log resolver.
	absent bool
}

// TransactionCollectionOpen is one caller-owned collection descriptor and its
// exact durable open profile. OpenCollectionsWithTransactions never closes
// File, on success or error.
type TransactionCollectionOpen struct {
	File    *os.File
	Options Options
}

// checkpointGroupGenericCatalogAfterPrecheckHook is a package-test seam for
// the directory-certificate check to marker-open interval. Production leaves
// it nil.
var checkpointGroupGenericCatalogAfterPrecheckHook func()

// OpenCollectionsWithTransactions is the sole current recovery entry point for
// a caller-owned catalog. It proves every descriptor belongs to dir, opens and
// scan-validates the complete set without replay, validates every decision and
// exact prepare binding across that set, then replays and reconciles as one
// private phase. It returns collections in request order only after all phases
// succeed. On error it resource-closes every constructed engine and the
// transaction log without flushing or recycling an unreplayed journal; the
// caller retains every File descriptor.
func OpenCollectionsWithTransactions(
	dir string,
	txnOptions TxnLogOptions,
	requests []TransactionCollectionOpen,
) ([]*Collection, *TxnLog, error) {
	if len(requests) > MaxSnapshotCollections {
		return nil, nil, fmt.Errorf(
			"%w: transaction catalog has %d collections, maximum %d",
			ErrTxnTooLarge, len(requests), MaxSnapshotCollections,
		)
	}
	log, err := newTxnLogDirectory(dir, txnOptions)
	if err != nil {
		return nil, nil, err
	}
	if err := validateTransactionCollectionRequests(log.rootInfo, requests); err != nil {
		return nil, nil, errors.Join(err, log.Close())
	}
	// Format 0 replaces txn.vtm as the commit authority. Reject before loading
	// marker recovery: that load may otherwise reconcile or recycle records
	// whose certified suffix must instead be presumed aborted.
	if err := rejectCheckpointGroupCertificate(log.root); err != nil {
		return nil, nil, errors.Join(err, log.Close())
	}
	if checkpointGroupGenericCatalogAfterPrecheckHook != nil {
		checkpointGroupGenericCatalogAfterPrecheckHook()
	}
	// Close the precheck-to-recovery interval with one non-recursive read lease.
	// It remains held through marker reconciliation and all participant opens,
	// so generic recovery cannot mutate txn.vtm or a member journal after a
	// certificate has been published. Member opens are told the outer lease is
	// held but retain their post-LockWriter identity/certificate recheck.
	releaseNamespace, err := acquireCheckpointGroupGenericCatalogNamespace(log.root)
	if err != nil {
		return nil, nil, errors.Join(err, log.Close())
	}
	defer releaseNamespace()
	// Request identity is now frozen against the pinned directory. Only after
	// that non-mutating proof may marker recovery remove/fence a mint residue.
	recovery, err := loadDatabaseTxnRecoveryFromLog(log, requests)
	if err != nil {
		return nil, nil, err
	}
	abort := func(collections []*Collection, cause error) (
		[]*Collection, *TxnLog, error,
	) {
		cleanupErr := error(nil)
		for i := len(collections) - 1; i >= 0; i-- {
			if collections[i] != nil {
				cleanupErr = errors.Join(
					cleanupErr, collections[i].closeResources(),
				)
			}
		}
		if recovery.log != nil {
			cleanupErr = errors.Join(cleanupErr, recovery.log.Close())
			recovery.log = nil
		}
		return nil, nil, errors.Join(cause, cleanupErr)
	}

	collections := make([]*Collection, len(requests))
	seenStores := make(map[[16]byte]int, len(requests))
	for i := range requests {
		cfg := collectionOpenConfig{
			deferJournalReplay:           true,
			checkpointGroupNamespaceHeld: true,
		}
		if recovery.absent {
			cfg.absentLog = true
		} else {
			cfg.decisions = recovery.decisions
		}
		collection, openErr := openCollection(
			requests[i].File, requests[i].Options, cfg,
		)
		if openErr != nil {
			return abort(collections[:i], openErr)
		}
		collections[i] = collection
		if previous, duplicate := seenStores[collection.storeID]; duplicate {
			return abort(collections[:i+1], fmt.Errorf(
				"%w: requests %d and %d have duplicate StoreID %x",
				ErrTxnCollection, previous, i, collection.storeID,
			))
		}
		seenStores[collection.storeID] = i
	}
	if err := preflightAndReplayDatabaseTxnRecovery(
		recovery, collections,
	); err != nil {
		return abort(collections, err)
	}
	if err := reconcileDatabaseTxnAfterOpens(
		recovery, collections,
	); err != nil {
		return abort(collections, err)
	}
	log = recovery.log
	recovery.log = nil
	return collections, log, nil
}

func validateTransactionCollectionRequests(
	directory os.FileInfo, requests []TransactionCollectionOpen,
) error {
	infos := make([]os.FileInfo, len(requests))
	seenPointers := make(map[*os.File]struct{}, len(requests))
	for i := range requests {
		file := requests[i].File
		if file == nil {
			return fmt.Errorf(
				"%w: nil collection file at request %d", ErrTxnCollection, i,
			)
		}
		if _, duplicate := seenPointers[file]; duplicate {
			return fmt.Errorf(
				"%w: duplicate collection file at request %d", ErrTxnCollection, i,
			)
		}
		seenPointers[file] = struct{}{}
		info, statErr := file.Stat()
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf(
				"%w: request %d is not a regular file", ErrTxnCollection, i,
			)
		}
		for previous := 0; previous < i; previous++ {
			if os.SameFile(info, infos[previous]) {
				return fmt.Errorf(
					"%w: requests %d and %d name the same physical file",
					ErrTxnCollection, previous, i,
				)
			}
		}
		infos[i] = info
		matches, matchErr := storeio.FileMatchesDirectory(
			directory, file,
		)
		if matchErr != nil {
			return fmt.Errorf(
				"vibedb: prove collection transaction directory: %w", matchErr,
			)
		}
		if !matches {
			return ErrTransactionLogDirectoryMismatch
		}
	}
	return nil
}

// preflightAndReplayDatabaseTxnRecovery separates catalog recovery into a
// read-only validation phase and a mutating replay phase. No member journal is
// applied or recycled until every collection and every marker target has
// been validated against the complete opened catalog.
func preflightAndReplayDatabaseTxnRecovery(
	recovery *databaseTxnRecovery,
	collections []*Collection,
) error {
	if recovery == nil {
		return nil
	}
	if recovery.decisions != nil {
		if err := validateTxnDecisionCollections(
			recovery.decisions, collections,
		); err != nil {
			return err
		}
	}
	for _, c := range collections {
		if c == nil || !c.journalEnabled() {
			continue
		}
		var resolve recoveryJournalDecisionResolver
		var epoch uint64
		if recovery.absent {
			resolve = absentLogResolver()
			if _, _, observedEpoch, err := journalConditionalIdentity(c.journal); err != nil {
				return err
			} else {
				epoch = observedEpoch
			}
		} else {
			resolve = targetBindingResolver(
				recovery.decisions, c.storeID, c.journalID,
			)
			epoch = recovery.decisions.Epoch()
		}
		if err := c.preflightRecoveryJournalResolved(resolve, epoch); err != nil {
			return err
		}
	}
	for _, c := range collections {
		if c == nil || !c.journalEnabled() {
			continue
		}
		rootGeneration := c.committer.DurableGeneration()
		var resolve recoveryJournalDecisionResolver
		var epoch uint64
		if recovery.absent {
			resolve = absentLogResolver()
			if _, _, observedEpoch, err := journalConditionalIdentity(c.journal); err != nil {
				return err
			} else {
				epoch = observedEpoch
			}
		} else {
			resolve = targetBindingResolver(
				recovery.decisions, c.storeID, c.journalID,
			)
			epoch = recovery.decisions.Epoch()
		}
		if err := c.replayRecoveryJournalResolvedLocked(
			rootGeneration, resolve, epoch,
		); err != nil {
			return err
		}
	}
	return nil
}

func journalConditionalIdentity(
	journal *storeio.RecoveryJournal,
) (markerID [16]byte, txnID, epoch uint64, err error) {
	if journal == nil || journal.Cursor() == 0 {
		return markerID, 0, 0, nil
	}
	err = journal.Replay(journal.BaseGeneration(), func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindConditionalBatch {
			markerID = rec.Conditional.MarkerID
			txnID = rec.Conditional.TxnID
			epoch = rec.Conditional.MarkerEpoch
		}
		return nil
	})
	return markerID, txnID, epoch, err
}

func loadDatabaseTxnRecovery(
	dir string, options TxnLogOptions,
) (*databaseTxnRecovery, error) {
	log, err := newTxnLogDirectory(dir, options)
	if err != nil {
		return nil, err
	}
	return loadDatabaseTxnRecoveryFromLog(log, nil)
}

func loadDatabaseTxnRecoveryFromLog(
	log *TxnLog, requests []TransactionCollectionOpen,
) (*databaseTxnRecovery, error) {
	recovery := &databaseTxnRecovery{dir: log.dir, log: log}

	info, statErr := log.root.Lstat(txnMarkerFilename)
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			_ = log.Close()
			return nil, statErr
		}
		holds, holdErr := directoryHoldsAnyConditional(log.root)
		if holdErr == nil && !holds {
			holds, holdErr = requestsHoldAnyConditional(requests)
		}
		if holdErr != nil {
			_ = log.Close()
			return nil, holdErr
		}
		if holds {
			_ = log.Close()
			return nil, ErrTransactionLogMissing
		}
		recovery.absent = true
		return recovery, nil
	}
	if !info.Mode().IsRegular() {
		_ = log.Close()
		return nil, fmt.Errorf(
			"%w: %q is not a regular non-symlink file",
			ErrUnsupportedDatabaseLayout, txnMarkerFilename,
		)
	}

	marker, decisions, err := storeio.OpenTxnMarkerAt(
		log.root, txnMarkerFilename,
		storeio.TxnMarkerOptions{
			Capacity: log.opts.Capacity, SealedCapacity: log.opts.SealedCapacity,
		},
	)
	if err != nil {
		if errors.Is(err, storeio.ErrTxnMarkerNoValidHeader) {
			holds, holdErr := directoryHoldsAnyConditional(log.root)
			if holdErr == nil && !holds {
				holds, holdErr = requestsHoldAnyConditional(requests)
			}
			if holdErr != nil {
				_ = log.Close()
				return nil, holdErr
			}
			if holds {
				_ = log.Close()
				return nil, fmt.Errorf(
					"%w: %w",
					ErrTransactionLogMissing, storeio.ErrTxnMarkerNoValidHeader,
				)
			}
			// L2 creation residue: no journal references the file; remove and
			// reopen as absent so the next commit can create a fresh marker.
			current, identityErr := log.root.Lstat(txnMarkerFilename)
			if identityErr != nil || !current.Mode().IsRegular() ||
				!os.SameFile(info, current) {
				_ = log.Close()
				if identityErr != nil {
					return nil, identityErr
				}
				return nil, fmt.Errorf(
					"%w: transaction marker changed during mint residue recovery",
					storeio.ErrTxnMarkerCorrupt,
				)
			}
			if rmErr := log.root.Remove(txnMarkerFilename); rmErr != nil &&
				!errors.Is(rmErr, os.ErrNotExist) {
				_ = log.Close()
				return nil, rmErr
			}
			if syncErr := syncTxnLogDirectory(log.root); syncErr != nil {
				_ = log.Close()
				return nil, syncErr
			}
			recovery.absent = true
			return recovery, nil
		}
		_ = log.Close()
		return nil, err
	}

	log.marker = marker
	if decisions.MaxTxnID() == ^uint64(0) {
		_ = marker.Close()
		log.marker = nil
		_ = log.Close()
		return nil, fmt.Errorf("%w: transaction id space exhausted", ErrTxnTooLarge)
	}
	log.nextTxnID = decisions.MaxTxnID() + 1
	if decisions.MaxTxnID() != 0 {
		log.undischarged = 1
	}
	recovery.decisions = decisions
	return recovery, nil
}

// reconcileDatabaseTxnAfterOpens validates target presence, clears
// discharge accounting, and applies L4 log-removal when legal. collections is
// every successfully opened catalog member.
func reconcileDatabaseTxnAfterOpens(
	recovery *databaseTxnRecovery, collections []*Collection,
) error {
	if recovery == nil {
		return nil
	}
	for _, c := range collections {
		if recovery.log != nil {
			recovery.log.registerCollection(c)
		}
	}
	if recovery.absent || recovery.decisions == nil {
		return nil
	}
	if err := validateTxnDecisionCollections(
		recovery.decisions, collections,
	); err != nil {
		return err
	}
	discharged := txnDecisionsDischarged(recovery.decisions, collections)
	holds, err := collectionsHoldConditional(
		collections,
		recovery.decisions.MarkerID(),
		recovery.decisions.Epoch(),
	)
	if err != nil {
		return err
	}
	directoryHolds := false
	if discharged && !holds && recovery.log != nil {
		directoryHolds, err = directoryHoldsAnyConditional(recovery.log.root)
		if err != nil {
			return err
		}
	}
	if discharged && !holds && !directoryHolds {
		if recovery.log != nil && recovery.log.opts.SealedCapacity {
			// A sealed marker is part of the caller-qualified physical profile.
			// Keep the already-proved file instead of unlinking and immediately
			// reminting it on every exact open. Its discharged records are harmless;
			// ordinary capacity pressure recycles them under the complete catalog.
			recovery.log.commitMu.Lock()
			recovery.log.undischarged = 0
			recovery.log.commitMu.Unlock()
			return nil
		}
		// L4: remove residue. Re-evaluate from the live opens; a crash around
		// the unlink re-enters open and observes the same predicate.
		if recovery.log != nil {
			recovery.log.commitMu.Lock()
			if recovery.log.marker != nil {
				removeErr := recovery.log.marker.Remove()
				recovery.log.marker = nil
				if removeErr != nil {
					recovery.log.commitMu.Unlock()
					return fmt.Errorf(
						"vibedb: remove discharged transaction log: %w",
						removeErr,
					)
				}
			}
			recovery.log.undischarged = 0
			recovery.log.commitMu.Unlock()
		}
		recovery.absent = true
		recovery.decisions = nil
		return nil
	}
	if discharged && recovery.log != nil {
		recovery.log.commitMu.Lock()
		recovery.log.undischarged = 0
		recovery.log.commitMu.Unlock()
	}
	return nil
}

func validateTxnDecisionCollections(
	decisions *storeio.TxnDecisions, collections []*Collection,
) error {
	if decisions == nil {
		return nil
	}
	byStore := make(map[[16]byte]*Collection, len(collections))
	preparesByStore := make(
		map[[16]byte]map[conditionalPrepareIdentity]struct{}, len(collections),
	)
	for _, c := range collections {
		if c == nil {
			continue
		}
		byStore[c.storeID] = c
		prepares, err := collectionConditionalPrepares(c)
		if err != nil {
			return err
		}
		preparesByStore[c.storeID] = prepares
	}
	markerID := decisions.MarkerID()
	epoch := decisions.Epoch()
	var validationErr error
	decisions.RangeDecisions(func(
		txnID uint64, targets []storeio.TxnCollectionRef) bool {
		for _, p := range targets {
			if decisions.Retired(p.StoreID) {
				continue
			}
			c, exists := byStore[p.StoreID]
			if !exists {
				validationErr = fmt.Errorf(
					"%w: store %x",
					ErrTransactionCollectionMissing, p.StoreID,
				)
				return false
			}
			if c.journalID != p.JournalID {
				validationErr = fmt.Errorf(
					"%w: journal identity mismatch for store %x",
					ErrTransactionCollectionMissing, p.StoreID,
				)
				return false
			}
			// A selected root at PreparedGeneration is not consumption proof: a
			// narrower replay may have checkpointed only a prefix while retaining
			// the conditional record. Only an exact live prepare or a paired journal
			// base advanced past it by a successful recycle proves recoverability.
			if c.journal != nil &&
				c.journal.BaseGeneration() >= p.PreparedGeneration {
				continue
			}
			prepare := conditionalPrepareIdentity{
				markerID: markerID, epoch: epoch, txnID: txnID,
				generation: p.PreparedGeneration,
			}
			if _, exists := preparesByStore[p.StoreID][prepare]; !exists {
				validationErr = fmt.Errorf(
					"%w: store %x journal %x has neither a recycled base covering generation %d nor exact transaction %d prepare at that generation",
					ErrTransactionCollectionMissing, p.StoreID, p.JournalID,
					p.PreparedGeneration, txnID,
				)
				return false
			}
		}
		return true
	})
	return validationErr
}

type conditionalPrepareIdentity struct {
	markerID   [16]byte
	epoch      uint64
	txnID      uint64
	generation uint64
}

func collectionConditionalPrepares(
	c *Collection,
) (map[conditionalPrepareIdentity]struct{}, error) {
	prepares := make(map[conditionalPrepareIdentity]struct{})
	if c == nil || c.journal == nil || c.journal.Cursor() == 0 {
		return prepares, nil
	}
	err := c.journal.Replay(
		c.journal.BaseGeneration(), func(rec storeio.RecoveryRecord) error {
			if rec.Kind != storeio.RecoveryRecordKindConditionalBatch {
				return nil
			}
			prepares[conditionalPrepareIdentity{
				markerID:   rec.Conditional.MarkerID,
				epoch:      rec.Conditional.MarkerEpoch,
				txnID:      rec.Conditional.TxnID,
				generation: rec.Generation,
			}] = struct{}{}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return prepares, nil
}

func txnDecisionsDischarged(
	decisions *storeio.TxnDecisions, collections []*Collection,
) bool {
	if decisions == nil {
		return true
	}
	byStore := make(map[[16]byte]*Collection, len(collections))
	for _, c := range collections {
		byStore[c.storeID] = c
	}
	discharged := true
	decisions.RangeDecisions(func(
		_ uint64, targets []storeio.TxnCollectionRef) bool {
		for _, p := range targets {
			if decisions.Retired(p.StoreID) {
				continue
			}
			c, exists := byStore[p.StoreID]
			if !exists {
				discharged = false
				return false
			}
			if c.journal == nil ||
				c.journal.BaseGeneration() < p.PreparedGeneration {
				discharged = false
				return false
			}
		}
		return true
	})
	return discharged
}

func collectionsHoldConditional(
	collections []*Collection, markerID [16]byte, epoch uint64,
) (bool, error) {
	for _, c := range collections {
		c.writer.Lock()
		holds, err := c.journalHoldsConditional(markerID, epoch)
		c.writer.Unlock()
		if err != nil {
			return false, err
		}
		if holds {
			return true, nil
		}
	}
	return false, nil
}

func txnDecisionsNameStore(
	decisions *storeio.TxnDecisions, storeID [16]byte,
) bool {
	if decisions == nil || decisions.Retired(storeID) {
		return false
	}
	found := false
	decisions.RangeDecisions(func(
		_ uint64, targets []storeio.TxnCollectionRef) bool {
		for _, p := range targets {
			if p.StoreID == storeID {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// retireCollectionBeforeDrop folds past live conditionals and appends a
// target-retired record when the drop barrier requires it. The caller
// holds the catalog write lock; the collection is still open.
func (d *Database) retireCollectionBeforeDrop(entry *databaseEntry) error {
	log := lookupDatabaseTxnLog(d)
	if log == nil {
		return nil
	}
	log.commitMu.Lock()
	defer log.commitMu.Unlock()
	if log.checkpointGroup != nil || log.checkpointGroupRetired {
		return ErrCheckpointGroupOwned
	}
	if log.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, log.poison)
	}
	if log.marker == nil {
		return nil
	}
	header := log.marker.Header()
	decisions, err := rescanTxnLogMarker(log)
	if err != nil {
		return err
	}
	c := entry.collection
	c.writer.Lock()
	holds, holdsErr := c.journalHoldsConditional(header.MarkerID, header.Epoch)
	var foldErr error
	if holdsErr == nil && holds {
		foldErr = c.checkpointPastConditionalsLocked(
			targetBindingResolver(decisions, c.storeID, c.journalID),
			decisions.Epoch(),
		)
	}
	storeID := c.storeID
	c.writer.Unlock()
	if holdsErr != nil || foldErr != nil {
		return errors.Join(holdsErr, foldErr)
	}
	named := txnDecisionsNameStore(decisions, storeID)
	if !holds && !named && log.undischarged == 0 {
		return nil
	}
	if !log.marker.FitsRetirement() {
		// Capacity/sequence exhaustion is known before any append bytes are
		// written. Discharge the complete registered catalog and recycle under
		// the ordinary pressure protocol, then re-evaluate from the new epoch:
		// the retirement is no longer needed once no old decision remains.
		if err := log.foldLaggardsAndRecycleLocked(); err != nil {
			return err
		}
		return nil
	}
	if databaseTxnBeforeRetirementAppendHook != nil {
		databaseTxnBeforeRetirementAppendHook(log)
	}
	if _, err := log.marker.AppendRetirement(storeID); err != nil {
		if errors.Is(err, storeio.ErrTxnMarkerFull) {
			return err
		}
		poisoned := journalCommitOutcomeUnknown(err)
		log.poison = poisoned
		for _, collection := range log.registeredCollections() {
			_ = joinCatalogCommitOutcomeUnknown(collection, err)
		}
		return poisoned
	}
	if err := log.marker.Sync(); err != nil {
		poisoned := journalCommitOutcomeUnknown(err)
		log.poison = poisoned
		for _, collection := range log.registeredCollections() {
			_ = joinCatalogCommitOutcomeUnknown(collection, err)
		}
		return poisoned
	}
	return nil
}

// rescanTxnLogMarker closes and reopens the decision log under commitMu so
// DropCollection observes decisions appended since open. The caller holds
// commitMu; catalog exclusion prevents concurrent commit.
func rescanTxnLogMarker(l *TxnLog) (*storeio.TxnDecisions, error) {
	if l == nil || l.marker == nil {
		return nil, nil
	}
	marker, decisions, err := storeio.OpenTxnMarkerAt(
		l.root, txnMarkerFilename,
		storeio.TxnMarkerOptions{
			Capacity: l.opts.Capacity, SealedCapacity: l.opts.SealedCapacity,
		},
	)
	if err != nil {
		return nil, err
	}
	old := l.marker
	sameFile, sameErr := old.SameFile(marker)
	if sameErr != nil || !sameFile || old.Header() != marker.Header() {
		_ = marker.Close()
		if sameErr != nil {
			return nil, sameErr
		}
		return nil, fmt.Errorf(
			"%w: transaction log identity changed during rescan",
			storeio.ErrTxnMarkerCorrupt,
		)
	}
	l.marker = marker
	if err := l.verifyMarkerDirectoryLocked(); err != nil {
		l.marker = old
		_ = marker.Close()
		return nil, err
	}
	if err := old.Close(); err != nil {
		l.marker = nil
		_ = marker.Close()
		return nil, err
	}
	return decisions, nil
}

func targetBindingResolver(
	decisions *storeio.TxnDecisions, storeID, journalID [16]byte,
) recoveryJournalDecisionResolver {
	return func(
		markerID [16]byte, epoch, txnID, preparedGeneration uint64,
	) (bool, error) {
		if decisions == nil || markerID != decisions.MarkerID() ||
			epoch != decisions.Epoch() {
			return false, ErrTransactionCollectionBinding
		}
		targets, ok := decisions.Lookup(markerID, epoch, txnID)
		if !ok {
			return false, nil
		}
		for _, p := range targets {
			if p.StoreID == storeID && p.JournalID == journalID &&
				p.PreparedGeneration == preparedGeneration {
				return true, nil
			}
		}
		return false, ErrTransactionCollectionBinding
	}
}

func absentLogResolver() recoveryJournalDecisionResolver {
	return func([16]byte, uint64, uint64, uint64) (bool, error) {
		return false, ErrTransactionLogMissing
	}
}

// directoryHoldsAnyConditional reports whether any stable recovery-journal
// sidecar under root still holds a conditional record. Caller-owned catalogs
// (notably SQL) use current opaque storage identities rather than
// collectionname's encoded names. Arbitrary files that merely share the
// journal suffix are outside the database namespace and must remain untouched.
// Used for absent-marker and mint-residue policy before collections are opened;
// root keeps the scan in the same physical directory whose marker was checked.
func directoryHoldsAnyConditional(root *os.Root) (bool, error) {
	if root == nil {
		return false, fmt.Errorf("vibedb: transaction recovery directory is not open")
	}
	directory, err := root.Open(".")
	if err != nil {
		return false, err
	}
	items, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return false, errors.Join(readErr, closeErr)
	}
	for _, item := range items {
		base := item.Name()
		if !databaseTxnRecoveryJournalName(base) {
			continue
		}
		before, err := root.Lstat(base)
		if err != nil {
			return false, err
		}
		if !before.Mode().IsRegular() {
			return false, fmt.Errorf(
				"%w: recovery journal %q is not a regular non-symlink file",
				ErrUnsupportedDatabaseLayout, base,
			)
		}
		file, err := root.OpenFile(base, os.O_RDONLY, 0)
		if err != nil {
			return false, err
		}
		fileInfo, statErr := file.Stat()
		after, pathErr := root.Lstat(base)
		if statErr != nil || pathErr != nil || !fileInfo.Mode().IsRegular() ||
			!after.Mode().IsRegular() || !os.SameFile(before, after) ||
			!os.SameFile(fileInfo, after) {
			_ = file.Close()
			if statErr != nil {
				return false, statErr
			}
			if pathErr != nil {
				return false, pathErr
			}
			return false, fmt.Errorf(
				"%w: recovery journal %q changed while opening",
				ErrUnsupportedDatabaseLayout, base,
			)
		}
		holds, _, _, bindingErr := journalConditionalBinding(file)
		if bindingErr != nil {
			return false, bindingErr
		}
		if holds {
			return true, nil
		}
	}
	return false, nil
}

// databaseTxnRecoveryJournalName admits the two current database-owned journal
// namespaces: reversible durable collection names and SQL's 32-byte opaque
// storage identities encoded as 64 lowercase hexadecimal characters. It does
// not claim arbitrary application sidecars ending in .rjournal.
func databaseTxnRecoveryJournalName(base string) bool {
	if _, canonical := collectionname.DecodeJournal(base); canonical {
		return true
	}
	const sqlStorageIdentityHexLength = 64
	const sqlJournalSuffix = collectionname.PrimarySuffix + collectionname.JournalSuffix
	if !strings.HasSuffix(base, sqlJournalSuffix) {
		return false
	}
	identity := strings.TrimSuffix(base, sqlJournalSuffix)
	if len(identity) != sqlStorageIdentityHexLength {
		return false
	}
	for i := range identity {
		c := identity[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func requestsHoldAnyConditional(
	requests []TransactionCollectionOpen,
) (bool, error) {
	for i := range requests {
		if requests[i].File == nil {
			continue
		}
		holds, _, _, err := journalFileConditionalBinding(
			RecoveryJournalPath(requests[i].File.Name()),
		)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		if holds {
			return true, nil
		}
	}
	return false, nil
}

// journalFileConditionalBinding opens a journal file and reports whether its
// live window holds any kind-4 record, returning one observed binding for the
// absent-log epoch plumbing.
func journalFileConditionalBinding(
	path string,
) (holds bool, markerID [16]byte, epoch uint64, err error) {
	file, err := os.Open(path)
	if err != nil {
		return false, [16]byte{}, 0, err
	}
	return journalConditionalBinding(file)
}

// journalConditionalBinding consumes file on every return path.
func journalConditionalBinding(
	file *os.File,
) (holds bool, markerID [16]byte, epoch uint64, err error) {
	journal, err := storeio.InspectRecoveryJournal(file)
	if err != nil {
		return false, [16]byte{}, 0, errors.Join(err, file.Close())
	}
	defer func() {
		// RecoveryJournalInspection owns and closes file after a successful open.
		err = errors.Join(err, journal.Close())
	}()
	if journal.Cursor() == 0 {
		return false, [16]byte{}, 0, nil
	}
	err = journal.Replay(journal.BaseGeneration(), func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindConditionalBatch {
			holds = true
			markerID = rec.Conditional.MarkerID
			epoch = rec.Conditional.MarkerEpoch
		}
		return nil
	})
	return holds, markerID, epoch, err
}

// peekCollectionConditionalEpoch returns a kind-4 MarkerEpoch from the
// collection's paired journal, if any. Open uses it so the absent-log resolver
// is reached for uncovered records (epoch checks run before the resolver).
func peekCollectionConditionalEpoch(primaryPath string) (uint64, bool, error) {
	path := RecoveryJournalPath(primaryPath)
	holds, _, epoch, err := journalFileConditionalBinding(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return epoch, holds, nil
}

func isTxnMarkerFilename(base string) bool {
	return base == txnMarkerFilename
}

func validateTxnMarkerLayout(root *os.Root, base string) error {
	info, err := root.Lstat(base)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf(
			"%w: reserved database entry %q is not a regular non-symlink file",
			ErrUnsupportedDatabaseLayout, base,
		)
	}
	return nil
}
