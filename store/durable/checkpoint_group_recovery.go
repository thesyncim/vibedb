package durable

import (
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// OpenCollectionsWithCheckpointGroup is the format-0 recovery entry point for
// a fixed replicated catalog. names is parallel to requests and is the exact
// fixed group membership. Recovery accepts only the certificate's transaction
// high-water and applied cut: conditionals at or below the high-water commit;
// every later conditional is presumed aborted and folded away. The returned
// collections are attached exclusively before they become reachable.
//
// A missing certificate returns ErrCheckpointGroupMissing without opening or
// replaying a collection. First activation can then use
// OpenCollectionsWithTransactions followed by NewCheckpointGroup, which only
// creates format 0 beside an empty member set and empty marker.
func OpenCollectionsWithCheckpointGroup(
	dir string,
	txnOptions TxnLogOptions,
	requests []TransactionCollectionOpen,
	names []string,
	options CheckpointGroupOptions,
) ([]*Collection, *TxnLog, *CheckpointGroup, error) {
	return openCollectionsWithCheckpointGroup(
		dir, txnOptions, requests, names, "", options,
	)
}

// OpenCollectionsWithSeededCheckpointGroup is the explicit crash-recovery
// entry point for child-image activation. Only when the certificate is absent
// may seedMember be empty while another exact fixed member is non-empty; the
// marker and every journal must still be clean. No collection is replayed in
// that interval, and the returned ErrCheckpointGroupMissing remains the
// caller's non-serving seed-pending signal.
func OpenCollectionsWithSeededCheckpointGroup(
	dir string,
	txnOptions TxnLogOptions,
	requests []TransactionCollectionOpen,
	names []string,
	seedMember string,
	options CheckpointGroupOptions,
) ([]*Collection, *TxnLog, *CheckpointGroup, error) {
	if seedMember == "" {
		return nil, nil, nil, fmt.Errorf("%w: empty seed member", ErrTxnParticipant)
	}
	return openCollectionsWithCheckpointGroup(
		dir, txnOptions, requests, names, seedMember, options,
	)
}

func openCollectionsWithCheckpointGroup(
	dir string,
	txnOptions TxnLogOptions,
	requests []TransactionCollectionOpen,
	names []string,
	seedPendingMember string,
	options CheckpointGroupOptions,
) ([]*Collection, *TxnLog, *CheckpointGroup, error) {
	if len(requests) != len(names) || len(requests) == 0 ||
		len(requests) > checkpointGroupMaxMembers {
		return nil, nil, nil, fmt.Errorf(
			"%w: checkpoint request/name membership", ErrTxnParticipant,
		)
	}
	options, err := options.normalized()
	if err != nil {
		return nil, nil, nil, err
	}
	log, err := newTxnLogDirectory(dir, txnOptions)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := validateTransactionCollectionRequests(log.rootInfo, requests); err != nil {
		return nil, nil, nil, errors.Join(err, log.Close())
	}
	// Serialize the missing-certificate inspection (which may remove a marker
	// mint residue) with first activation. With a certificate present this also
	// keeps generic namespace entry points from interleaving while the fixed
	// membership is authenticated and attached.
	releaseNamespace, err := acquireCheckpointGroupRecoveryNamespace()
	if err != nil {
		return nil, nil, nil, errors.Join(err, log.Close())
	}
	defer releaseNamespace()
	certificateFile, certificate, err := openCheckpointGroupCertificate(log)
	if errors.Is(err, os.ErrNotExist) {
		if cleanErr := validateMissingCheckpointGroupActivation(
			log, requests, names, seedPendingMember,
		); cleanErr != nil {
			return nil, nil, nil, errors.Join(cleanErr, log.Close())
		}
		return nil, nil, nil, ErrCheckpointGroupMissing
	}
	if err != nil {
		return nil, nil, nil, errors.Join(err, log.Close())
	}
	recovery, err := loadDatabaseTxnRecoveryFromLog(log, requests)
	if err != nil {
		if !checkpointGroupMarkerRepairable(err) {
			return nil, nil, nil, errors.Join(err, certificateFile.Close())
		}
		// Generic recovery deliberately fails closed when conditionals outlive a
		// missing/torn marker. Format 0 has stronger authority: its certificate
		// binds the resolver, so reopen may replace the implementation log after
		// the complete fixed membership has been opened and folded.
		log, err = newTxnLogDirectory(dir, txnOptions)
		if err != nil {
			return nil, nil, nil, errors.Join(err, certificateFile.Close())
		}
		recovery = &databaseTxnRecovery{dir: log.dir, log: log, absent: true}
	}
	markerRepair := recovery.absent || recovery.log.marker == nil || recovery.decisions == nil
	abort := func(collections []*Collection, cause error) (
		[]*Collection, *TxnLog, *CheckpointGroup, error,
	) {
		cleanup := certificateFile.Close()
		for i := len(collections) - 1; i >= 0; i-- {
			if collections[i] != nil {
				cleanup = errors.Join(cleanup, collections[i].closeResources())
			}
		}
		if recovery.log != nil {
			cleanup = errors.Join(cleanup, recovery.log.Close())
			recovery.log = nil
		}
		return nil, nil, nil, errors.Join(cause, cleanup)
	}
	if recovery.log == nil {
		return abort(nil, fmt.Errorf(
			"%w: checkpoint recovery has no transaction-log owner", ErrCheckpointGroupCorrupt,
		))
	}

	collections := make([]*Collection, len(requests))
	seenStores := make(map[[16]byte]int, len(requests))
	for i := range requests {
		collection, openErr := openCollection(
			requests[i].File, requests[i].Options,
			collectionOpenConfig{
				deferJournalReplay:      true,
				decisions:               recovery.decisions,
				checkpointGroupRecovery: true,
			},
		)
		if openErr != nil {
			return abort(collections[:i], openErr)
		}
		collections[i] = collection
		if previous, duplicate := seenStores[collection.storeID]; duplicate {
			return abort(collections[:i+1], fmt.Errorf(
				"%w: requests %d and %d have duplicate StoreID %x",
				ErrTxnParticipant, previous, i, collection.storeID,
			))
		}
		seenStores[collection.storeID] = i
	}
	named := make([]NamedCollection, len(collections))
	for i := range collections {
		named[i] = NamedCollection{Name: names[i], Collection: collections[i]}
	}
	members, err := checkpointGroupMembers(named)
	if err != nil {
		return abort(collections, err)
	}
	if err := validateCheckpointGroupCertificateMembers(certificate, members); err != nil {
		return abort(collections, err)
	}
	if err := validateCheckpointGroupDirectoryMembership(recovery.log, members); err != nil {
		return abort(collections, err)
	}
	var header storeio.TxnMarkerHeader
	transitionalRollover := false
	if !markerRepair {
		header = recovery.log.marker.Header()
		sameMarker := certificate.markerID == header.MarkerID
		sameEpoch := certificate.markerEpoch == header.Epoch
		transitionalRollover = sameMarker && certificate.markerEpoch != ^uint64(0) &&
			certificate.markerEpoch+1 == header.Epoch
		markerRepair = !sameMarker || (!sameEpoch && !transitionalRollover)
	}
	if transitionalRollover {
		if recovery.log.marker.Cursor() != 0 {
			return abort(collections, fmt.Errorf(
				"%w: non-empty transitional marker", ErrCheckpointGroupCorrupt,
			))
		}
		for _, collection := range collections {
			if collection.journal == nil || collection.journal.Cursor() != 0 {
				return abort(collections, fmt.Errorf(
					"%w: transitional marker with live journal", ErrCheckpointGroupCorrupt,
				))
			}
		}
	} else {
		if !markerRepair {
			if err := validateCheckpointMarkerRecords(
				recovery.decisions, certificate.txnBase, members,
			); err != nil {
				return abort(collections, err)
			}
			if err := validateCheckpointCommittedParticipants(
				recovery.decisions, collections, certificate.txnHighWater,
			); err != nil {
				return abort(collections, err)
			}
		}
		if err := replayCheckpointGroupMembers(
			recovery, collections, certificate,
		); err != nil {
			return abort(collections, err)
		}
	}
	for _, collection := range collections {
		if collection.journal == nil || collection.journal.Cursor() != 0 {
			return abort(collections, fmt.Errorf(
				"%w: recovery left a live member journal", ErrCheckpointGroupCorrupt,
			))
		}
		recovery.log.registerCollection(collection)
	}

	markerRepaired := false
	if markerRepair {
		if certificate.sequence == math.MaxUint64 ||
			certificate.markerEpoch == math.MaxUint64 {
			return abort(collections, fmt.Errorf(
				"%w: marker repair exhausted certificate sequence or epoch",
				ErrCheckpointGroupSequence,
			))
		}
		if err := replaceCheckpointGroupMarker(
			recovery.log,
			storeio.TxnMarkerRecoveryAnchor{
				MarkerID:     certificate.markerID,
				Epoch:        certificate.markerEpoch + 1,
				BaseSequence: certificate.txnHighWater,
			},
		); err != nil {
			return abort(collections, err)
		}
		header = recovery.log.marker.Header()
		certificate.sequence++
		certificate.markerID = header.MarkerID
		certificate.markerEpoch = header.Epoch
		certificate.txnBase = certificate.txnHighWater
		markerRepaired = true
	}
	group, err := checkpointGroupFromCertificateLocked(
		recovery.log, certificateFile, certificate, members, options,
	)
	if err == nil {
		err = group.validateRecoveredMembersLocked(true)
	}
	if err != nil {
		return abort(collections, err)
	}
	if markerRepaired {
		recovery.log.commitMu.Lock()
		err = group.writeCertificateLocked(group.certificateLocked())
		recovery.log.commitMu.Unlock()
		if err != nil {
			return abort(collections, err)
		}
	}
	// Recovery has now folded every certified prepare and discarded every later
	// suffix, so reopen always starts from a clean implementation-log epoch.
	// This removes any unsynced marker prefix and prevents replay from colliding
	// with a surviving suffix ordinal. A crash after Recycle's marker fence but
	// before the same-cut certificate rewrite re-enters transitionalRollover.
	if markerRepaired {
		// replaceCheckpointGroupMarker already installed a clean epoch and the
		// same-cut certificate rewrite above made it authoritative.
	} else if transitionalRollover {
		recovery.log.commitMu.Lock()
		group.sequence++
		err = group.writeCertificateLocked(group.certificateLocked())
		recovery.log.commitMu.Unlock()
		if err != nil {
			return abort(collections, err)
		}
	} else if err = group.recycleMarkerLocked(); err != nil {
		return abort(collections, err)
	}
	recovery.log.commitMu.Lock()
	if err = group.attachLocked(); err != nil {
		recovery.log.commitMu.Unlock()
		return abort(collections, err)
	}
	recovery.log.commitMu.Unlock()

	log = recovery.log
	recovery.log = nil
	return collections, log, group, nil
}

// validateMissingCheckpointGroupActivation distinguishes the one legitimate
// missing-certificate state from loss of an already-active certificate. SQL
// publishes the empty fixed participant files before creating format 0, so a
// crash in that narrow seam has an empty marker, empty durable roots, and empty
// journals. Anything else must fail before generic recovery can fold records
// using txn.vtm as authority.
func validateMissingCheckpointGroupActivation(
	log *TxnLog,
	requests []TransactionCollectionOpen,
	names []string,
	seedPendingMember string,
) error {
	if len(requests) != len(names) {
		return fmt.Errorf("%w: missing-certificate membership", ErrCheckpointGroupCorrupt)
	}
	seedIndex := -1
	seenNames := make(map[string]struct{}, len(names))
	for i, name := range names {
		if name == "" {
			return fmt.Errorf("%w: empty missing-certificate member name", ErrCheckpointGroupCorrupt)
		}
		if _, duplicate := seenNames[name]; duplicate {
			return fmt.Errorf("%w: duplicate missing-certificate member name", ErrCheckpointGroupCorrupt)
		}
		seenNames[name] = struct{}{}
		if name == seedPendingMember {
			seedIndex = i
		}
	}
	if seedPendingMember != "" && seedIndex < 0 {
		return fmt.Errorf("%w: seed member is outside missing-certificate membership", ErrCheckpointGroupCorrupt)
	}
	recovery, err := loadDatabaseTxnRecoveryFromLog(log, requests)
	if err != nil {
		return fmt.Errorf("%w: missing certificate has recovery state: %v", ErrCheckpointGroupCorrupt, err)
	}
	cleanup := func(collections []*Collection) error {
		var result error
		for i := len(collections) - 1; i >= 0; i-- {
			if collections[i] != nil {
				result = errors.Join(result, collections[i].closeResources())
			}
		}
		if recovery.log != nil {
			result = errors.Join(result, recovery.log.Close())
			recovery.log = nil
		}
		return result
	}
	if recovery.decisions != nil &&
		(recovery.decisions.MaxTxnID() != 0 || recovery.decisions.RetirementCount() != 0) {
		return errors.Join(
			fmt.Errorf("%w: missing certificate beside a non-empty marker", ErrCheckpointGroupCorrupt),
			cleanup(nil),
		)
	}
	if recovery.log != nil && recovery.log.marker != nil && recovery.log.marker.Cursor() != 0 {
		return errors.Join(
			fmt.Errorf("%w: missing certificate beside marker records", ErrCheckpointGroupCorrupt),
			cleanup(nil),
		)
	}

	collections := make([]*Collection, 0, len(requests))
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
			return errors.Join(
				fmt.Errorf("%w: inspect missing-certificate member: %v", ErrCheckpointGroupCorrupt, openErr),
				cleanup(collections),
			)
		}
		collections = append(collections, collection)
		if (seedPendingMember == "" || i == seedIndex) && collection.Len() != 0 ||
			collection.journal == nil || collection.journal.Cursor() != 0 {
			return errors.Join(
				fmt.Errorf("%w: missing certificate beside non-empty member %d", ErrCheckpointGroupCorrupt, i),
				cleanup(collections),
			)
		}
	}
	if cleanupErr := cleanup(collections); cleanupErr != nil {
		return cleanupErr
	}
	return nil
}

func checkpointGroupMarkerRepairable(err error) bool {
	return errors.Is(err, ErrTransactionLogMissing) ||
		errors.Is(err, storeio.ErrTxnMarkerNoValidHeader) ||
		errors.Is(err, storeio.ErrTxnMarkerCorrupt) ||
		errors.Is(err, storeio.ErrTxnMarkerRecord)
}

func replaceCheckpointGroupMarker(
	log *TxnLog,
	anchor storeio.TxnMarkerRecoveryAnchor,
) error {
	if log == nil || log.root == nil {
		return ErrCheckpointGroupCorrupt
	}
	log.commitMu.Lock()
	defer log.commitMu.Unlock()
	holds, err := directoryHoldsAnyConditional(log.root)
	if err != nil {
		return err
	}
	if holds {
		return fmt.Errorf(
			"%w: marker replacement blocked by an unowned conditional journal",
			ErrCheckpointGroupCorrupt,
		)
	}
	if log.marker != nil {
		if err := log.marker.Close(); err != nil {
			log.marker = nil
			return err
		}
		log.marker = nil
	}
	info, err := log.root.Lstat(txnMarkerFilename)
	if err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf(
				"%w: transaction marker is not a regular file",
				ErrCheckpointGroupCorrupt,
			)
		}
		if err := log.root.Remove(txnMarkerFilename); err != nil {
			return err
		}
		if err := syncTxnLogDirectory(log.root); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	marker, err := storeio.CreateTxnMarkerAtRecoveryAnchor(
		log.root, txnMarkerFilename,
		storeio.TxnMarkerOptions{
			Capacity: log.opts.Capacity, SealedCapacity: log.opts.SealedCapacity,
		},
		anchor,
	)
	if err != nil {
		return err
	}
	log.marker = marker
	if anchor.BaseSequence == math.MaxUint64 {
		log.nextTxnID = math.MaxUint64
	} else {
		log.nextTxnID = anchor.BaseSequence + 1
	}
	log.undischarged = 0
	if databaseTxnAfterMintHook != nil {
		databaseTxnAfterMintHook(log)
	}
	return nil
}

func replayCheckpointGroupMembers(
	recovery *databaseTxnRecovery,
	collections []*Collection,
	certificate checkpointGroupCertificate,
) error {
	if recovery == nil {
		return ErrCheckpointGroupCorrupt
	}
	epoch := certificate.markerEpoch
	for _, collection := range collections {
		if err := validateCheckpointGroupJournalRecords(collection, certificate); err != nil {
			return err
		}
		resolver := checkpointCertificateResolver(
			certificate.markerID, certificate.markerEpoch,
			certificate.txnBase, certificate.txnHighWater,
		)
		if err := collection.preflightRecoveryJournalCertifiedPrefix(resolver, epoch); err != nil {
			return err
		}
	}
	for _, collection := range collections {
		resolver := checkpointCertificateResolver(
			certificate.markerID, certificate.markerEpoch,
			certificate.txnBase, certificate.txnHighWater,
		)
		if err := collection.replayRecoveryJournalCertifiedPrefixLocked(
			collection.committer.DurableGeneration(), resolver, epoch,
		); err != nil {
			return err
		}
	}
	return nil
}

// validateCheckpointGroupJournalRecords rejects every ordinary journal kind.
// Once format 0 owns an initially empty fixed member, all later mutations use
// kind-4 conditional batches; accepting an unconditional record would allow
// data outside the authenticated certificate to enter replay.
func validateCheckpointGroupJournalRecords(
	collection *Collection,
	certificate checkpointGroupCertificate,
) error {
	if collection == nil || collection.journal == nil || collection.journal.Cursor() == 0 {
		return nil
	}
	lastTxn := certificate.txnBase
	return collection.journal.Replay(
		collection.journal.BaseGeneration(),
		func(record storeio.RecoveryRecord) error {
			if record.Kind != storeio.RecoveryRecordKindConditionalBatch {
				return fmt.Errorf(
					"%w: fixed member contains journal kind %d",
					ErrCheckpointGroupCorrupt, record.Kind,
				)
			}
			conditional := record.Conditional
			if conditional.MarkerID != certificate.markerID ||
				conditional.MarkerEpoch != certificate.markerEpoch ||
				conditional.TxnID <= lastTxn {
				return fmt.Errorf(
					"%w: conditional transaction identity or order after %d",
					ErrCheckpointGroupCorrupt, lastTxn,
				)
			}
			lastTxn = conditional.TxnID
			return nil
		},
	)
}

func validateCheckpointCommittedParticipants(
	decisions *storeio.TxnDecisions,
	collections []*Collection,
	txnHighWater uint64,
) error {
	byStore := make(map[[16]byte]*Collection, len(collections))
	preparesByStore := make(
		map[[16]byte]map[conditionalPrepareIdentity]struct{}, len(collections),
	)
	for _, collection := range collections {
		byStore[collection.storeID] = collection
		prepares, err := collectionConditionalPrepares(collection)
		if err != nil {
			return err
		}
		preparesByStore[collection.storeID] = prepares
	}
	markerID, epoch := decisions.MarkerID(), decisions.Epoch()
	var result error
	decisions.RangeDecisions(func(txnID uint64, participants []storeio.TxnParticipant) bool {
		if txnID > txnHighWater {
			return true
		}
		for _, participant := range participants {
			collection := byStore[participant.StoreID]
			if collection == nil || collection.journalID != participant.JournalID {
				result = fmt.Errorf(
					"%w: committed transaction %d participant",
					ErrCheckpointGroupCorrupt, txnID,
				)
				return false
			}
			if collection.journal.BaseGeneration() >= participant.PreparedGeneration {
				continue
			}
			identity := conditionalPrepareIdentity{
				markerID: markerID, epoch: epoch, txnID: txnID,
				generation: participant.PreparedGeneration,
			}
			if _, ok := preparesByStore[participant.StoreID][identity]; !ok {
				result = fmt.Errorf(
					"%w: committed transaction %d prepare", ErrCheckpointGroupCorrupt, txnID,
				)
				return false
			}
		}
		return true
	})
	return result
}
