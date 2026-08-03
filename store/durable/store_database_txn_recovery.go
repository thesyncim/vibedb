package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

var (
	// ErrTransactionParticipantMissing reports that a durable decision names a
	// participant whose collection file or journal is missing or mismatched,
	// and no participant-retired record covers that StoreID.
	ErrTransactionParticipantMissing = errors.New(
		"vibedb: a committed database transaction names a missing collection",
	)
	// ErrTransactionLogMissing reports that collection journals hold uncovered
	// conditional transaction records but the database's decision log is
	// absent or unusable. By the mint fence this state is unreachable by
	// crash; presumed abort would silently roll back acknowledged commits.
	ErrTransactionLogMissing = errors.New(
		"vibedb: collection journals hold conditional transaction records but the database's decision log is missing",
	)
)

// databaseTxnRecovery is the loaded decision-log state OpenDatabase and the
// driver-facing recovery entry points share before per-collection opens.
type databaseTxnRecovery struct {
	dir       string
	log       *TxnLog
	decisions *storeio.TxnDecisions
	// absent is true when txn.vtm is not present (or was removed as mint
	// residue). Collections open with the L1/L3 absent-log resolver.
	absent bool
}

// RecoverDatabaseTransactions loads and validates dir's txn.vtm for a
// caller-owned catalog (the SQL driver). The returned TxnLog is the handle the
// caller later commits through; OpenWithTransactions consumes the decisions
// for per-collection replay. When txn.vtm is absent the decisions pointer is
// nil and the log is an unminted handle — the same lazy-mint seam
// Database.Update uses.
func RecoverDatabaseTransactions(
	dir string, options TxnLogOptions,
) (*storeio.TxnDecisions, *TxnLog, error) {
	recovery, err := loadDatabaseTxnRecovery(dir, options)
	if err != nil {
		return nil, nil, err
	}
	return recovery.decisions, recovery.log, nil
}

// OpenWithTransactions opens a collection file with catalog-owned journal
// wiring and the decision resolver OpenDatabase uses. A nil txns pointer
// selects the absent-log resolver (ErrTransactionLogMissing on every
// uncovered kind-5 lookup). Standalone [Open] passes a nil resolver instead
// and fails uncovered kind-5 with ErrCollectionInDoubt.
func OpenWithTransactions(
	file *os.File, options Options, txns *storeio.TxnDecisions,
) (*Collection, error) {
	cfg := collectionOpenConfig{catalogOwned: true}
	if txns == nil {
		cfg.absentLog = true
	} else {
		cfg.decisions = txns
	}
	return openCollection(file, options, cfg)
}

func loadDatabaseTxnRecovery(
	dir string, options TxnLogOptions,
) (*databaseTxnRecovery, error) {
	if dir == "" {
		return nil, fmt.Errorf("vibedb: transaction recovery requires a directory")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir = filepath.Clean(absolute)
	path := filepath.Join(dir, txnMarkerFilename)
	recovery := &databaseTxnRecovery{dir: dir}

	info, statErr := os.Stat(path)
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			return nil, statErr
		}
		log, err := OpenTxnLog(dir, options)
		if err != nil {
			return nil, err
		}
		recovery.log = log
		recovery.absent = true
		return recovery, nil
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%w: %q is not a regular non-symlink file",
			ErrUnsupportedDatabaseLayout, txnMarkerFilename,
		)
	}

	marker, decisions, err := storeio.OpenTxnMarker(
		path, storeio.TxnMarkerOptions{Capacity: options.Capacity},
	)
	if err != nil {
		if errors.Is(err, storeio.ErrTxnMarkerNoValidHeader) {
			holds, holdErr := directoryHoldsAnyConditional(dir)
			if holdErr != nil {
				return nil, holdErr
			}
			if holds {
				return nil, fmt.Errorf(
					"%w: %w",
					ErrTransactionLogMissing, storeio.ErrTxnMarkerNoValidHeader,
				)
			}
			// L2 mint residue: no journal references the file; remove and
			// reopen as absent so the next commit can re-mint.
			if rmErr := os.Remove(path); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
				return nil, rmErr
			}
			if syncErr := syncRecoveryJournalParent(path); syncErr != nil {
				return nil, syncErr
			}
			log, openErr := OpenTxnLog(dir, options)
			if openErr != nil {
				return nil, openErr
			}
			recovery.log = log
			recovery.absent = true
			return recovery, nil
		}
		return nil, err
	}

	log := &TxnLog{
		dir:        dir,
		path:       path,
		opts:       options,
		nextTxnID:  1,
		registered: make(map[*Collection]struct{}),
		marker:     marker,
	}
	log.nextTxnID = decisions.MaxTxnID() + 1
	if log.nextTxnID == 0 {
		log.nextTxnID = 1
	}
	if decisions.MaxTxnID() != 0 {
		log.undischarged = 1
	}
	recovery.log = log
	recovery.decisions = decisions
	return recovery, nil
}

// reconcileDatabaseTxnAfterOpens validates participant presence, clears
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
	if err := validateTxnDecisionParticipants(
		recovery.decisions, collections,
	); err != nil {
		return err
	}
	discharged := txnDecisionsDischarged(recovery.decisions, collections)
	holds := collectionsHoldConditional(
		collections,
		recovery.decisions.MarkerID(),
		recovery.decisions.Epoch(),
	)
	if discharged && !holds {
		// L4: remove residue. Re-evaluate from the live opens; a crash around
		// the unlink re-enters open and observes the same predicate.
		if recovery.log != nil {
			recovery.log.commitMu.Lock()
			if recovery.log.marker != nil {
				_ = recovery.log.marker.Close()
				recovery.log.marker = nil
			}
			recovery.log.undischarged = 0
			recovery.log.commitMu.Unlock()
		}
		path := filepath.Join(recovery.dir, txnMarkerFilename)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("vibedb: remove discharged transaction log: %w", err)
		}
		if err := syncRecoveryJournalParent(path); err != nil {
			return fmt.Errorf("vibedb: persist transaction log removal: %w", err)
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

func validateTxnDecisionParticipants(
	decisions *storeio.TxnDecisions, collections []*Collection,
) error {
	if decisions == nil {
		return nil
	}
	byStore := make(map[[16]byte]*Collection, len(collections))
	for _, c := range collections {
		byStore[c.storeID] = c
	}
	markerID := decisions.MarkerID()
	epoch := decisions.Epoch()
	maxTxn := decisions.MaxTxnID()
	for txnID := uint64(1); txnID <= maxTxn; txnID++ {
		participants, ok := decisions.Lookup(markerID, epoch, txnID)
		if !ok {
			continue
		}
		for _, p := range participants {
			if decisions.Retired(p.StoreID) {
				continue
			}
			c, exists := byStore[p.StoreID]
			if !exists {
				return fmt.Errorf(
					"%w: store %x",
					ErrTransactionParticipantMissing, p.StoreID,
				)
			}
			if c.journalID != p.JournalID {
				return fmt.Errorf(
					"%w: journal identity mismatch for store %x",
					ErrTransactionParticipantMissing, p.StoreID,
				)
			}
		}
	}
	return nil
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
	markerID := decisions.MarkerID()
	epoch := decisions.Epoch()
	maxTxn := decisions.MaxTxnID()
	for txnID := uint64(1); txnID <= maxTxn; txnID++ {
		participants, ok := decisions.Lookup(markerID, epoch, txnID)
		if !ok {
			continue
		}
		for _, p := range participants {
			if decisions.Retired(p.StoreID) {
				continue
			}
			c, exists := byStore[p.StoreID]
			if !exists {
				return false
			}
			if c.Generation() < p.PreparedGeneration {
				return false
			}
		}
	}
	return true
}

func collectionsHoldConditional(
	collections []*Collection, markerID [16]byte, epoch uint64,
) bool {
	for _, c := range collections {
		c.writer.Lock()
		holds := c.journalHoldsConditional(markerID, epoch)
		c.writer.Unlock()
		if holds {
			return true
		}
	}
	return false
}

func txnDecisionsNameStore(
	decisions *storeio.TxnDecisions, storeID [16]byte,
) bool {
	if decisions == nil || decisions.Retired(storeID) {
		return false
	}
	markerID := decisions.MarkerID()
	epoch := decisions.Epoch()
	maxTxn := decisions.MaxTxnID()
	for txnID := uint64(1); txnID <= maxTxn; txnID++ {
		participants, ok := decisions.Lookup(markerID, epoch, txnID)
		if !ok {
			continue
		}
		for _, p := range participants {
			if p.StoreID == storeID {
				return true
			}
		}
	}
	return false
}

// retireCollectionBeforeDrop folds past live conditionals and appends a
// participant-retired record when the drop barrier requires it. The caller
// holds the catalog write lock; the collection is still open.
func (d *Database) retireCollectionBeforeDrop(entry *databaseEntry) error {
	log := lookupDatabaseTxnLog(d)
	if log == nil {
		return nil
	}
	log.commitMu.Lock()
	defer log.commitMu.Unlock()
	if log.marker == nil {
		return nil
	}
	header := log.marker.Header()
	c := entry.collection
	c.writer.Lock()
	holds := c.journalHoldsConditional(header.MarkerID, header.Epoch)
	var foldErr error
	if holds {
		foldErr = c.checkpointPastConditionalsLocked()
	}
	storeID := c.storeID
	c.writer.Unlock()
	if foldErr != nil {
		return foldErr
	}

	decisions, err := rescanTxnLogMarker(log)
	if err != nil {
		return err
	}
	named := txnDecisionsNameStore(decisions, storeID)
	if !holds && !named && log.undischarged == 0 {
		return nil
	}
	if _, err := log.marker.AppendRetirement(storeID); err != nil {
		return err
	}
	if err := log.marker.Sync(); err != nil {
		return err
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
	path := l.path
	if err := l.marker.Close(); err != nil {
		l.marker = nil
		return nil, err
	}
	l.marker = nil
	marker, decisions, err := storeio.OpenTxnMarker(
		path, storeio.TxnMarkerOptions{Capacity: l.opts.Capacity},
	)
	if err != nil {
		return nil, err
	}
	l.marker = marker
	return decisions, nil
}

func participantBindingResolver(
	decisions *storeio.TxnDecisions, storeID, journalID [16]byte,
) recoveryJournalDecisionResolver {
	return func(markerID [16]byte, epoch, txnID uint64) (bool, error) {
		participants, ok := decisions.Lookup(markerID, epoch, txnID)
		if !ok {
			return false, nil
		}
		for _, p := range participants {
			if p.StoreID == storeID && p.JournalID == journalID {
				return true, nil
			}
		}
		return false, nil
	}
}

func absentLogResolver() recoveryJournalDecisionResolver {
	return func([16]byte, uint64, uint64) (bool, error) {
		return false, ErrTransactionLogMissing
	}
}

// directoryHoldsAnyConditional reports whether any canonical collection
// journal in dir still holds a kind-5 record. Used for L2 mint-residue policy
// before collections are opened.
func directoryHoldsAnyConditional(dir string) (bool, error) {
	items, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		base := item.Name()
		if _, canonical := collectionname.DecodeJournal(base); !canonical {
			continue
		}
		holds, _, _, err := journalFileConditionalBinding(
			filepath.Join(dir, base),
		)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		}
		if holds {
			return true, nil
		}
	}
	return false, nil
}

// journalFileConditionalBinding opens a journal file and reports whether its
// live window holds any kind-5 record, returning one observed binding for the
// absent-log epoch plumbing.
func journalFileConditionalBinding(
	path string,
) (holds bool, markerID [16]byte, epoch uint64, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return false, [16]byte{}, 0, err
	}
	defer file.Close()
	journal, err := storeio.OpenRecoveryJournal(file)
	if err != nil {
		return false, [16]byte{}, 0, err
	}
	defer journal.Close()
	if journal.Header().FormatVersion != storeio.RecoveryJournalFormatConditional ||
		journal.Cursor() == 0 {
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

// peekCollectionConditionalEpoch returns a kind-5 MarkerEpoch from the
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
