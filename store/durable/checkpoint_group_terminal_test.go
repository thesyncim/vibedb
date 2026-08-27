package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestCheckpointGroupMarkerBaseSequenceMismatchFailsBeforeMemberReplay(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*storeio.TxnMarkerHeader)
	}{
		{
			name: "aligned-terminal",
			mutate: func(header *storeio.TxnMarkerHeader) {
				header.BaseSequence = math.MaxUint64
			},
		},
		{
			name: "transitional-terminal",
			mutate: func(header *storeio.TxnMarkerHeader) {
				header.Epoch++
				header.RecycleCount++
				header.BaseSequence = math.MaxUint64
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, _, _, _ := newCheckpointGroupTestStore(t, 8)
			crashImage := copyCheckpointGroupDirectory(t, dir)
			checkpointGroupTestRewriteMarkerHeader(t, crashImage, test.mutate)
			beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)
			requests, files := checkpointGroupTestOpenRequests(t, crashImage)
			collections, log, group, err := OpenCollectionsWithCheckpointGroup(
				crashImage,
				TxnLogOptions{},
				requests,
				[]string{"system", "user"},
				CheckpointGroupOptions{CheckpointEvery: 8},
			)
			for _, file := range files {
				_ = file.Close()
			}
			if collections != nil || log != nil || group != nil ||
				!errors.Is(err, ErrCheckpointGroupCorrupt) {
				t.Fatalf(
					"marker base mismatch = collections %v log %v group %v err %v",
					collections, log, group, err,
				)
			}
			requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
		})
	}
}

func TestCheckpointGroupLiveMarkerBaseMismatchRefusesBeforeCallback(t *testing.T) {
	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	group.mu.Lock()
	originalBase := group.txnBase
	group.txnBase++
	group.mu.Unlock()
	t.Cleanup(func() {
		group.mu.Lock()
		group.txnBase = originalBase
		group.mu.Unlock()
	})
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeHeader := log.marker.Header()
	beforeCursor := log.marker.Cursor()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	group.mu.Unlock()
	called := false
	err := group.Update(1, members[:1], defaultTxnLimits(), func(*DatabaseBatch) error {
		called = true
		return nil
	})
	if called || !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("live marker base mismatch = called %v, err %v", called, err)
	}
	checkpointGroupTestRequireUnchangedOwner(
		t, dir, group, beforeDirectory, beforeStats,
		beforeHeader, beforeCursor, beforeOwner,
	)
}

func TestCheckpointGroupLiveMarkerSequenceMismatchRefusesBeforeCallback(t *testing.T) {
	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	log.commitMu.Lock()
	retired := [16]byte{0xa5}
	if _, err := log.marker.AppendRetirement(retired); err != nil {
		log.commitMu.Unlock()
		t.Fatal(err)
	}
	if err := log.marker.Sync(); err != nil {
		log.commitMu.Unlock()
		t.Fatal(err)
	}
	log.commitMu.Unlock()
	if log.marker.Header().BaseSequence != group.txnBase ||
		log.marker.NextSequence() != group.txn+2 {
		t.Fatalf(
			"marker sequence mismatch fixture = base %d/%d next %d txn %d",
			log.marker.Header().BaseSequence, group.txnBase,
			log.marker.NextSequence(), group.txn,
		)
	}
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeHeader := log.marker.Header()
	beforeCursor := log.marker.Cursor()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	group.mu.Unlock()
	called := false
	err := group.Update(1, members[:1], defaultTxnLimits(), func(*DatabaseBatch) error {
		called = true
		return nil
	})
	if called || !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("live marker sequence mismatch = called %v, err %v", called, err)
	}
	checkpointGroupTestRequireUnchangedOwner(
		t, dir, group, beforeDirectory, beforeStats,
		beforeHeader, beforeCursor, beforeOwner,
	)
}

func TestCheckpointGroupLastLiveTransactionReopensReadOnly(t *testing.T) {
	dir, _, _, _ := newCheckpointGroupTestStore(t, 8)
	crashImage := copyCheckpointGroupDirectory(t, dir)
	checkpointGroupTestRewriteSingleDiskCertificate(
		t,
		crashImage,
		func(certificate *checkpointGroupCertificate) {
			certificate.txnBase = math.MaxUint64 - 2
			certificate.txnHighWater = math.MaxUint64 - 2
		},
	)
	checkpointGroupTestRewriteMarkerHeader(
		t,
		crashImage,
		func(header *storeio.TxnMarkerHeader) {
			header.BaseSequence = math.MaxUint64 - 2
		},
	)
	collections, log, group := openCheckpointGroupTestCopy(t, crashImage)
	members := []NamedCollection{
		{Name: "system", Collection: collections[0]},
		{Name: "user", Collection: collections[1]},
	}
	checkpointGroupPut(t, group, 1, members[:1], "last-live-transaction")
	if got := log.marker.NextSequence(); got != math.MaxUint64 {
		t.Fatalf("last live transaction next marker DCSN = %d", got)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("certify last live transaction: %v", err)
	}
	if got := group.CheckpointAppliedIndex(); got != 1 {
		t.Fatalf("last live certified applied = %d", got)
	}
	group.mu.Lock()
	certified := group.certificateLocked()
	group.mu.Unlock()
	if certified.txnHighWater != math.MaxUint64-1 {
		t.Fatalf("last live certified transaction = %d", certified.txnHighWater)
	}
	closeCheckpointGroupTestHandles(t, collections, log, group)
	checkpointGroupTestRequireTerminalReopenSucceeds(t, crashImage)
}

func TestCheckpointGroupAlignedTerminalRecoveryCleansUncertifiedSuffixOnce(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, members[:1], "uncertified")
	checkpointGroupTestRewriteCertificateSequences(
		t, group, math.MaxUint64-1, math.MaxUint64,
	)
	crashImage := copyCheckpointGroupDirectory(t, dir)
	markerPath := filepath.Join(crashImage, txnMarkerFilename)
	certificatePath := filepath.Join(crashImage, checkpointGroupFilename)
	markerBefore, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	certificateBefore, err := os.ReadFile(certificatePath)
	if err != nil {
		t.Fatal(err)
	}
	directoryBefore := checkpointGroupDirectoryBytes(t, crashImage)

	collections, log, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf(
			"terminal suffix cleanup cut = %d/%d",
			reopened.AppliedIndex(), reopened.CheckpointAppliedIndex(),
		)
	}
	for _, collection := range collections {
		if collection.journal == nil || collection.journal.Cursor() != 0 {
			t.Fatal("terminal suffix cleanup retained a participant journal")
		}
		if _, found := collectionDoc(t, collection, "certified"); !found {
			t.Fatal("terminal suffix cleanup lost the certified prefix")
		}
	}
	if _, found := collectionDoc(t, collections[0], "uncertified"); found {
		t.Fatal("terminal suffix cleanup retained the uncertified row")
	}
	markerAfter, markerErr := os.ReadFile(markerPath)
	certificateAfter, certificateErr := os.ReadFile(certificatePath)
	if markerErr != nil || certificateErr != nil ||
		!bytes.Equal(markerAfter, markerBefore) ||
		!bytes.Equal(certificateAfter, certificateBefore) {
		t.Fatalf(
			"terminal suffix cleanup rewrote marker/certificate: marker %v certificate %v",
			markerErr, certificateErr,
		)
	}
	directoryAfter := checkpointGroupDirectoryBytes(t, crashImage)
	if bytes.Equal(
		directoryAfter[filepath.Base(RecoveryJournalPath(collections[0].file.Name()))],
		directoryBefore[filepath.Base(RecoveryJournalPath(collections[0].file.Name()))],
	) {
		t.Fatal("terminal suffix cleanup did not fold the dirty participant journal")
	}
	closeCheckpointGroupTestHandles(t, collections, log, reopened)
	checkpointGroupTestRequireTerminalReopenSucceeds(t, crashImage)
}

func TestCheckpointGroupTerminalMarkerCounterRecoveryCleansSuffixReadOnly(t *testing.T) {
	for _, test := range []struct {
		name               string
		stageTerminalEpoch bool
		mutateCertificate  func(*checkpointGroupCertificate)
		mutateHeader       func(*storeio.TxnMarkerHeader)
	}{
		{
			name:               "epoch",
			stageTerminalEpoch: true,
		},
		{
			name: "recycle-count",
			mutateHeader: func(header *storeio.TxnMarkerHeader) {
				header.RecycleCount = math.MaxUint64
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, log, group := newCheckpointGroupTestStore(t, 8)
			if test.stageTerminalEpoch {
				checkpointGroupTestStageUncertifiedAtMarkerEpoch(
					t, dir, members, log, group, math.MaxUint64,
					"uncertified-terminal-marker",
				)
			} else {
				checkpointGroupPut(
					t, group, 1, members[:1], "uncertified-terminal-marker",
				)
			}
			crashImage := copyCheckpointGroupDirectory(t, dir)
			if !test.stageTerminalEpoch && test.mutateCertificate != nil {
				checkpointGroupTestRewriteSingleDiskCertificate(
					t, crashImage, test.mutateCertificate,
				)
			}
			if !test.stageTerminalEpoch {
				checkpointGroupTestRewriteMarkerHeader(t, crashImage, test.mutateHeader)
			}
			markerPath := filepath.Join(crashImage, txnMarkerFilename)
			certificatePath := filepath.Join(crashImage, checkpointGroupFilename)
			markerBefore, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal(err)
			}
			certificateBefore, err := os.ReadFile(certificatePath)
			if err != nil {
				t.Fatal(err)
			}

			collections, log, reopened := openCheckpointGroupTestCopy(t, crashImage)
			if reopened.AppliedIndex() != 0 || reopened.CheckpointAppliedIndex() != 0 {
				t.Fatalf(
					"terminal marker suffix cut = %d/%d",
					reopened.AppliedIndex(), reopened.CheckpointAppliedIndex(),
				)
			}
			for _, collection := range collections {
				if collection.journal == nil || collection.journal.Cursor() != 0 {
					t.Fatal("terminal marker suffix retained a participant journal")
				}
			}
			if _, found := collectionDoc(
				t, collections[0], "uncertified-terminal-marker",
			); found {
				t.Fatal("terminal marker suffix retained an uncertified row")
			}
			markerAfter, markerErr := os.ReadFile(markerPath)
			certificateAfter, certificateErr := os.ReadFile(certificatePath)
			if markerErr != nil || certificateErr != nil ||
				!bytes.Equal(markerAfter, markerBefore) ||
				!bytes.Equal(certificateAfter, certificateBefore) {
				t.Fatalf(
					"terminal marker suffix rewrote marker/certificate: marker %v certificate %v",
					markerErr, certificateErr,
				)
			}
			closeCheckpointGroupTestHandles(t, collections, log, reopened)
			checkpointGroupTestRequireTerminalReopenSucceeds(t, crashImage)
		})
	}
}

func checkpointGroupTestStageUncertifiedAtMarkerEpoch(
	t testing.TB,
	dir string,
	members []NamedCollection,
	log *TxnLog,
	group *CheckpointGroup,
	epoch uint64,
	key string,
) {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	checkpointGroupTestRewriteSingleDiskCertificate(
		t,
		dir,
		func(certificate *checkpointGroupCertificate) {
			certificate.markerEpoch = epoch
		},
	)
	log.commitMu.Lock()
	if err := log.marker.Close(); err != nil {
		log.commitMu.Unlock()
		t.Fatal(err)
	}
	checkpointGroupTestRewriteMarkerHeader(
		t,
		dir,
		func(header *storeio.TxnMarkerHeader) {
			header.Epoch = epoch
		},
	)
	marker, _, err := storeio.OpenTxnMarker(
		filepath.Join(dir, txnMarkerFilename),
		storeio.TxnMarkerOptions{
			Capacity: log.opts.Capacity, SealedCapacity: log.opts.SealedCapacity,
		},
	)
	if err != nil {
		log.commitMu.Unlock()
		t.Fatal(err)
	}
	log.marker = marker
	group.markerEpoch = epoch
	log.commitMu.Unlock()

	write := &WriteBatch{
		collection: members[0].Collection,
		position:   make(map[string]int, 1),
		active:     true,
	}
	defer closeDurableWriteBatches([]*WriteBatch{write})
	if err := write.Put([]byte(key), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := group.commitTransitionLocked(
		1,
		1,
		members[:1],
		map[string]*WriteBatch{members[0].Name: write},
		TxnLimits{MaxCollections: 1, MaxDocuments: 1, MaxBytes: 1 << 20},
	); err != nil {
		t.Fatal(err)
	}
	group.txn = 1
	group.applied = 1
	group.updates.Add(1)
	log.commitMu.Lock()
	err = log.marker.Sync()
	log.commitMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupAlignedTerminalRecoveryFoldsCertifiedSuffixOnce(t *testing.T) {
	dir, _, _, _ := newCheckpointGroupTestStore(t, 8)
	crashImage := copyCheckpointGroupDirectory(t, dir)
	checkpointGroupTestRewriteSingleDiskCertificate(
		t,
		crashImage,
		func(certificate *checkpointGroupCertificate) {
			certificate.txnBase = math.MaxUint64 - 2
			certificate.txnHighWater = math.MaxUint64 - 2
		},
	)
	checkpointGroupTestRewriteMarkerHeader(
		t,
		crashImage,
		func(header *storeio.TxnMarkerHeader) {
			header.BaseSequence = math.MaxUint64 - 2
		},
	)
	collections, log, group := openCheckpointGroupTestCopy(t, crashImage)
	members := []NamedCollection{
		{Name: "system", Collection: collections[0]},
		{Name: "user", Collection: collections[1]},
	}
	checkpointGroupPut(t, group, 1, members[:1], "certified-terminal")
	fault := errors.New("stop after terminal certificate sync")
	previousHook := checkpointGroupFaultHook
	t.Cleanup(func() { checkpointGroupFaultHook = previousHook })
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterCertificateSync {
			return fault
		}
		return nil
	}
	err := group.Checkpoint()
	checkpointGroupFaultHook = previousHook
	if !errors.Is(err, fault) {
		t.Fatalf("terminal certified-prefix fault = %v", err)
	}
	damaged := copyCheckpointGroupDirectory(t, crashImage)
	clearCheckpointGroupTestPoison(group)
	closeCheckpointGroupTestHandles(t, collections, log, group)

	markerPath := filepath.Join(damaged, txnMarkerFilename)
	certificatePath := filepath.Join(damaged, checkpointGroupFilename)
	markerBefore, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	certificateBefore, err := os.ReadFile(certificatePath)
	if err != nil {
		t.Fatal(err)
	}
	recoveredCollections, recoveredLog, recovered := openCheckpointGroupTestCopy(t, damaged)
	if recovered.txn != math.MaxUint64-1 || recovered.CheckpointAppliedIndex() != 1 {
		t.Fatalf(
			"terminal certified-prefix cut = txn %d applied %d",
			recovered.txn, recovered.CheckpointAppliedIndex(),
		)
	}
	if _, found := collectionDoc(t, recoveredCollections[0], "certified-terminal"); !found {
		t.Fatal("terminal recovery lost the certified-but-unfolded row")
	}
	for _, collection := range recoveredCollections {
		if collection.journal == nil || collection.journal.Cursor() != 0 {
			t.Fatal("terminal recovery retained a certified participant journal")
		}
	}
	markerAfter, markerErr := os.ReadFile(markerPath)
	certificateAfter, certificateErr := os.ReadFile(certificatePath)
	if markerErr != nil || certificateErr != nil ||
		!bytes.Equal(markerAfter, markerBefore) ||
		!bytes.Equal(certificateAfter, certificateBefore) {
		t.Fatalf(
			"terminal certified-prefix recovery rewrote marker/certificate: marker %v certificate %v",
			markerErr, certificateErr,
		)
	}
	closeCheckpointGroupTestHandles(t, recoveredCollections, recoveredLog, recovered)
	checkpointGroupTestRequireTerminalReopenSucceeds(t, damaged)
}

func TestCheckpointGroupParticipantTerminalAdmissionIsSideEffectFree(t *testing.T) {
	t.Run("declared-dirty-recycle-count", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members[1:], "dirty-terminal-member")
		checkpointGroupTestRewriteLiveJournalHeader(
			t,
			group,
			members[1].Collection,
			func(header *storeio.RecoveryJournalHeader) {
				header.RecycleCount = math.MaxUint64
			},
		)
		checkpointGroupTestRequireRejectedParticipantUpdate(
			t, dir, group, log, members[1:], 2,
		)
	})

	t.Run("declared-decision-sequence", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupTestRewriteLiveJournalHeader(
			t,
			group,
			members[0].Collection,
			func(header *storeio.RecoveryJournalHeader) {
				header.BaseSequence = math.MaxUint64
			},
		)
		checkpointGroupTestRequireRejectedParticipantUpdate(
			t, dir, group, log, members[:1], 1,
		)
	})

	t.Run("declared-logical-generation", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		collection := members[0].Collection
		checkpointGroupTestSetVisibleGeneration(
			t, collection, fileLogicalCutGenerationMask,
		)
		if got := collection.Generation(); got != fileLogicalCutGenerationMask {
			t.Fatalf("terminal fixture generation = %d", got)
		}
		checkpointGroupTestRequireRejectedParticipantUpdate(
			t, dir, group, log, members[:1], 1,
		)
	})

	t.Run("generation-budget-exact-boundary", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		budget, ok := checkpointGroupMemberGenerationBudget(
			members[0].Collection.options.MaxBatchDocuments,
		)
		if !ok || budget >= fileLogicalCutGenerationMask {
			t.Fatalf("generation budget = %d,%t", budget, ok)
		}
		checkpointGroupTestSetVisibleGeneration(
			t, members[0].Collection, fileLogicalCutGenerationMask-budget,
		)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		stop := errors.New("stop after exact-boundary admission")
		called := false
		err := group.Update(1, members[:1], defaultTxnLimits(), func(*DatabaseBatch) error {
			called = true
			return stop
		})
		if !called || !errors.Is(err, stop) {
			t.Fatalf("exact generation budget = called %v, err %v", called, err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats,
			beforeHeader, beforeCursor, beforeOwner,
		)
	})

	t.Run("generation-budget-one-short", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		budget, ok := checkpointGroupMemberGenerationBudget(
			members[0].Collection.options.MaxBatchDocuments,
		)
		if !ok || budget >= fileLogicalCutGenerationMask {
			t.Fatalf("generation budget = %d,%t", budget, ok)
		}
		checkpointGroupTestSetVisibleGeneration(
			t, members[0].Collection, fileLogicalCutGenerationMask-budget+1,
		)
		checkpointGroupTestRequireRejectedParticipantUpdate(
			t, dir, group, log, members[:1], 1,
		)
	})

	t.Run("recycle-budget-exact-boundary", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		budget, ok := checkpointGroupMemberRecycleBudget(
			members[0].Collection.options.MaxBatchDocuments,
		)
		if !ok {
			t.Fatal("invalid recycle budget")
		}
		checkpointGroupTestRewriteLiveJournalHeader(
			t,
			group,
			members[0].Collection,
			func(header *storeio.RecoveryJournalHeader) {
				header.RecycleCount = math.MaxUint64 - budget
			},
		)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		stop := errors.New("stop after exact recycle-boundary admission")
		called := false
		err := group.Update(1, members[:1], defaultTxnLimits(), func(*DatabaseBatch) error {
			called = true
			return stop
		})
		if !called || !errors.Is(err, stop) {
			t.Fatalf("exact recycle budget = called %v, err %v", called, err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats,
			beforeHeader, beforeCursor, beforeOwner,
		)
	})

	t.Run("recycle-budget-one-short", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		budget, ok := checkpointGroupMemberRecycleBudget(
			members[0].Collection.options.MaxBatchDocuments,
		)
		if !ok {
			t.Fatal("invalid recycle budget")
		}
		checkpointGroupTestRewriteLiveJournalHeader(
			t,
			group,
			members[0].Collection,
			func(header *storeio.RecoveryJournalHeader) {
				header.RecycleCount = math.MaxUint64 - budget + 1
			},
		)
		checkpointGroupTestRequireRejectedParticipantUpdate(
			t, dir, group, log, members[:1], 1,
		)
	})

	t.Run("checkpoint-due-max-minus-one", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 1)
		checkpointGroupPut(t, group, 1, members[:1], "dirty")
		checkpointGroupTestRewriteLiveJournalHeader(
			t,
			group,
			members[0].Collection,
			func(header *storeio.RecoveryJournalHeader) {
				header.RecycleCount = math.MaxUint64 - 1
			},
		)
		checkpointGroupTestRequireRejectedParticipantUpdate(
			t, dir, group, log, members[:1], 2,
		)
	})
}

func TestCheckpointGroupParticipantTopologyGenerationTerminalAdmission(t *testing.T) {
	// Prove this exact fresh-store workload takes the content-equivalent topology
	// path under the same fixed collection profile.
	_, controlMembers, _, control := newCheckpointGroupTestStore(t, 8)
	if err := control.Update(
		1, controlMembers[:1], defaultTxnLimits(), checkpointGroupTerminalTopologyBatch,
	); err != nil {
		t.Fatalf("topology control update: %v", err)
	}
	if splits := controlMembers[0].Collection.Stats().PrimaryLeafSplits; splits == 0 {
		t.Fatal("terminal workload control did not require a topology publication")
	}

	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	collection := members[0].Collection
	checkpointGroupTestSetVisibleGeneration(
		t, collection, fileLogicalCutGenerationMask-1,
	)
	beforeState := collection.state.Load()
	beforeVisible := collection.visibleState.Load()
	beforeDurable := collection.durableState.Load()
	beforeCut := collection.logicalCut.Load()
	beforeCollectionStats := collection.Stats()
	beforeCommitterStats := collection.committer.Stats()
	beforePublished := collection.committer.PublishedGeneration()
	beforeDurableGeneration := collection.committer.DurableGeneration()
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeHeader := log.marker.Header()
	beforeCursor := log.marker.Cursor()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	group.mu.Unlock()
	called := false
	err := group.Update(1, members[:1], defaultTxnLimits(), func(batch *DatabaseBatch) error {
		called = true
		return checkpointGroupTerminalTopologyBatch(batch)
	})
	if called || !errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf("terminal topology update = called %v, err %v", called, err)
	}
	checkpointGroupTestRequireUnchangedOwner(
		t, dir, group, beforeDirectory, beforeStats,
		beforeHeader, beforeCursor, beforeOwner,
	)
	if collection.state.Load() != beforeState ||
		collection.visibleState.Load() != beforeVisible ||
		collection.durableState.Load() != beforeDurable ||
		collection.logicalCut.Load() != beforeCut ||
		collection.Stats() != beforeCollectionStats ||
		collection.committer.Stats() != beforeCommitterStats ||
		collection.committer.PublishedGeneration() != beforePublished ||
		collection.committer.DurableGeneration() != beforeDurableGeneration {
		t.Fatal("terminal topology admission changed root, cut, collection, or committer state")
	}
}

func TestCheckpointGroupSeedParticipantTerminalAdmissionIsSideEffectFree(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(testing.TB, *CheckpointGroup, *Collection)
	}{
		{
			name: "generation",
			mutate: func(t testing.TB, _ *CheckpointGroup, collection *Collection) {
				checkpointGroupTestSetVisibleGeneration(
					t, collection, fileLogicalCutGenerationMask-1,
				)
			},
		},
		{
			name: "recycle-count",
			mutate: func(t testing.TB, group *CheckpointGroup, collection *Collection) {
				budget, ok := checkpointGroupMemberRecycleBudget(1)
				if !ok {
					t.Fatal("invalid one-entry recycle budget")
				}
				checkpointGroupTestRewriteLiveJournalHeader(
					t,
					group,
					collection,
					func(header *storeio.RecoveryJournalHeader) {
						header.RecycleCount = math.MaxUint64 - budget + 1
					},
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
			if _, err := members[1].Collection.Put(
				[]byte("row"), []byte(`{"value":"staged"}`),
			); err != nil {
				t.Fatal(err)
			}
			seed := CheckpointGroupSeed{
				Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
			}
			seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
			group, err := NewSeededCheckpointGroup(
				log, members, seed, CheckpointGroupOptions{CheckpointEvery: 8},
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = group.Close() })
			test.mutate(t, group, members[0].Collection)
			beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
			beforeStats := group.Stats()
			beforeHeader := log.marker.Header()
			beforeCursor := log.marker.Cursor()
			group.mu.Lock()
			beforeOwner := group.certificateLocked()
			group.mu.Unlock()
			err = group.Seed(
				seed, members[0], defaultTxnLimits(), []byte("state"),
			)
			if !errors.Is(err, ErrCheckpointGroupSequence) {
				t.Fatalf("terminal Seed = %v", err)
			}
			checkpointGroupTestRequireUnchangedOwner(
				t, dir, group, beforeDirectory, beforeStats,
				beforeHeader, beforeCursor, beforeOwner,
			)
			if _, found, readErr := members[0].Collection.AppendRaw(
				nil, []byte("state"),
			); readErr != nil || found {
				t.Fatalf("terminal Seed row = found %v, err %v", found, readErr)
			}
		})
	}
}

func TestCheckpointGroupParticipantFinalDCSNCanBeCertifiedAndFolded(t *testing.T) {
	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupTestRewriteLiveJournalHeader(
		t,
		group,
		members[0].Collection,
		func(header *storeio.RecoveryJournalHeader) {
			header.BaseSequence = math.MaxUint64 - 1
		},
	)

	checkpointGroupPut(t, group, 1, members[:1], "last-dcsn")
	terminal := members[0].Collection.journal
	if terminal.NextSequence() != 0 || terminal.Cursor() == 0 {
		t.Fatalf(
			"final-DCSN suffix = next %d cursor %d",
			terminal.NextSequence(), terminal.Cursor(),
		)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint final DCSN: %v", err)
	}
	if terminal.NextSequence() != 0 || terminal.Cursor() != 0 ||
		terminal.Header().BaseSequence != math.MaxUint64 {
		t.Fatalf(
			"folded final DCSN = base %d next %d cursor %d",
			terminal.Header().BaseSequence, terminal.NextSequence(), terminal.Cursor(),
		)
	}
	checkpointGroupTestRequireRejectedParticipantUpdate(
		t, dir, group, log, members[:1], 2,
	)

	// Terminal DCSN is participant-local. Another fixed member may still append,
	// and the all-member physical fold can preserve the exhausted member's
	// BaseSequence-Max anchor without inventing a zero record sequence.
	checkpointGroupPut(t, group, 2, members[1:], "other-member")
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint beside terminal DCSN: %v", err)
	}
	if terminal.NextSequence() != 0 || terminal.Cursor() != 0 ||
		terminal.Header().BaseSequence != math.MaxUint64 {
		t.Fatalf(
			"later fold changed terminal DCSN = base %d next %d cursor %d",
			terminal.Header().BaseSequence, terminal.NextSequence(), terminal.Cursor(),
		)
	}
}

func TestCheckpointGroupParticipantRecycleTerminalCheckpointRefusesBeforeMutation(t *testing.T) {
	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members[:1], "dirty")
	checkpointGroupTestRewriteLiveJournalHeader(
		t,
		group,
		members[0].Collection,
		func(header *storeio.RecoveryJournalHeader) {
			header.RecycleCount = math.MaxUint64
		},
	)
	faults := []*storeio.FaultJournal{
		storeio.NewFaultJournal(members[0].Collection.journal),
		storeio.NewFaultJournal(members[1].Collection.journal),
	}
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeHeader := log.marker.Header()
	beforeCursor := log.marker.Cursor()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	group.mu.Unlock()

	err := group.Checkpoint()
	if !errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf("terminal participant checkpoint = %v", err)
	}
	for i, fault := range faults {
		if fault.Syncs() != 0 {
			t.Fatalf(
				"terminal participant checkpoint synced journal %d: syncs=%d",
				i, fault.Syncs(),
			)
		}
	}
	checkpointGroupTestRequireUnchangedOwner(
		t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
	)
}

func TestCheckpointGroupParticipantRecycleTerminalRecoveryRefusesBeforeMutation(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members[:1], "dirty")
	crashImage := copyCheckpointGroupDirectory(t, dir)
	checkpointGroupTestRewriteJournalHeaderFile(
		t,
		RecoveryJournalPath(filepath.Join(crashImage, "system.vjc")),
		func(header *storeio.RecoveryJournalHeader) {
			header.RecycleCount = math.MaxUint64
		},
	)
	beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)
	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
		crashImage,
		TxnLogOptions{},
		requests,
		[]string{"system", "user"},
		CheckpointGroupOptions{CheckpointEvery: 8},
	)
	for _, file := range files {
		_ = file.Close()
	}
	if collections != nil || log != nil || recovered != nil ||
		!errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf(
			"terminal participant recovery = collections %v log %v group %v err %v",
			collections, log, recovered, err,
		)
	}
	requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
}

func TestCheckpointGroupRecoveryGenerationBudgetRefusesBeforeMemberReplay(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified-dirty")
	fault := errors.New("stop after certified dirty prefix")
	previousFaultHook := checkpointGroupFaultHook
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterCertificateSync {
			return fault
		}
		return nil
	}
	err := group.Checkpoint()
	checkpointGroupFaultHook = previousFaultHook
	if !errors.Is(err, fault) {
		t.Fatalf("certified dirty-prefix fixture = %v", err)
	}
	crashImage := copyCheckpointGroupDirectory(t, dir)
	beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)

	previousReplayHook := checkpointGroupBeforeMemberReplayHook
	checkpointGroupBeforeMemberReplayHook = func(collections []*Collection) {
		collection := collections[1]
		visible := collection.visibleState.Load()
		exhausted := *visible
		exhausted.root.Generation = fileLogicalCutGenerationMask - 1
		collection.visibleState.Store(&exhausted)
		cut, ok := packFileLogicalCut(fileLogicalCutGenerationMask-1, 0)
		if !ok {
			panic("terminal recovery generation did not fit logical cut")
		}
		collection.logicalCut.Store(cut)
	}
	t.Cleanup(func() {
		checkpointGroupBeforeMemberReplayHook = previousReplayHook
	})
	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
		crashImage,
		TxnLogOptions{},
		requests,
		[]string{"system", "user"},
		CheckpointGroupOptions{CheckpointEvery: 8},
	)
	checkpointGroupBeforeMemberReplayHook = previousReplayHook
	for _, file := range files {
		_ = file.Close()
	}
	if collections != nil || log != nil || recovered != nil ||
		!errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf(
			"terminal generation recovery = collections %v log %v group %v err %v",
			collections, log, recovered, err,
		)
	}
	requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
}

func TestCheckpointGroupCleanTerminalParticipantRecovery(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    func(*storeio.RecoveryJournalHeader)
		cleanNoop bool
	}{
		{
			name: "recycle-count",
			mutate: func(header *storeio.RecoveryJournalHeader) {
				header.RecycleCount = math.MaxUint64
			},
			cleanNoop: true,
		},
		{
			name: "decision-sequence",
			mutate: func(header *storeio.RecoveryJournalHeader) {
				header.BaseSequence = math.MaxUint64
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, _, _, _ := newCheckpointGroupTestStore(t, 8)
			crashImage := copyCheckpointGroupDirectory(t, dir)
			checkpointGroupTestRewriteJournalHeaderFile(
				t,
				RecoveryJournalPath(filepath.Join(crashImage, "system.vjc")),
				test.mutate,
			)
			collections, log, group := openCheckpointGroupTestCopy(t, crashImage)
			reopenedDir := log.dir
			members := []NamedCollection{
				{Name: "system", Collection: collections[0]},
				{Name: "user", Collection: collections[1]},
			}
			checkpointGroupTestRequireRejectedParticipantUpdate(
				t, reopenedDir, group, log, members[:1], 1,
			)
			if test.cleanNoop {
				journalPath := RecoveryJournalPath(collections[0].file.Name())
				beforeJournal, readErr := os.ReadFile(journalPath)
				if readErr != nil {
					t.Fatal(readErr)
				}
				checkpointGroupPut(t, group, 1, members[1:], "other-member")
				if err := group.Checkpoint(); err != nil {
					t.Fatalf("checkpoint beside clean terminal recycle count: %v", err)
				}
				afterJournal, readErr := os.ReadFile(journalPath)
				if readErr != nil || !bytes.Equal(afterJournal, beforeJournal) {
					t.Fatalf("clean terminal journal changed: %v", readErr)
				}
				if got := collections[0].journal.Header().RecycleCount; got != math.MaxUint64 {
					t.Fatalf("clean terminal recycle count = %d", got)
				}
				closeCheckpointGroupTestHandles(t, collections, log, group)
				reopenedCollections, reopenedLog, reopenedGroup := openCheckpointGroupTestCopy(
					t, reopenedDir,
				)
				if _, found := collectionDoc(
					t, reopenedCollections[1], "other-member",
				); !found {
					t.Fatal("other-member update was not durable after terminal sibling reopen")
				}
				afterReopenJournal, readErr := os.ReadFile(journalPath)
				if readErr != nil || !bytes.Equal(afterReopenJournal, beforeJournal) {
					t.Fatalf("clean terminal journal changed across reopen: %v", readErr)
				}
				closeCheckpointGroupTestHandles(
					t, reopenedCollections, reopenedLog, reopenedGroup,
				)
				return
			}
			checkpointGroupPut(t, group, 1, members[1:], "other-member")
			if err := group.Checkpoint(); err != nil {
				t.Fatalf("checkpoint beside recovered terminal DCSN: %v", err)
			}
		})
	}
}

func checkpointGroupTestRequireRejectedParticipantUpdate(
	t testing.TB,
	dir string,
	group *CheckpointGroup,
	log *TxnLog,
	members []NamedCollection,
	applied uint64,
) {
	t.Helper()
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeHeader := log.marker.Header()
	beforeCursor := log.marker.Cursor()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	group.mu.Unlock()
	called := false
	err := group.Update(applied, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		called = true
		write, collectionErr := batch.Collection(members[0].Name)
		if collectionErr != nil {
			return collectionErr
		}
		return write.Put([]byte("must-not-publish"), []byte(`{"n":1}`))
	})
	if called || !errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf("terminal participant update = called %v, err %v", called, err)
	}
	checkpointGroupTestRequireUnchangedOwner(
		t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
	)
}

func checkpointGroupTerminalTopologyBatch(batch *DatabaseBatch) error {
	write, err := batch.Collection("system")
	if err != nil {
		return err
	}
	for i := 0; i < write.collection.options.MaxBatchDocuments; i++ {
		key := fmt.Appendf(nil, "topology-%04d", i)
		value := fmt.Appendf(nil, `{"n":%d,"pad":"`, i)
		value = appendWideJSONSafePattern(value, 1_850, i*37)
		value = append(value, '"', '}')
		if err := write.Put(key, value); err != nil {
			return err
		}
	}
	return nil
}

func checkpointGroupTestSetVisibleGeneration(
	t testing.TB,
	collection *Collection,
	generation uint64,
) {
	t.Helper()
	collection.writer.Lock()
	originalVisible := collection.visibleState.Load()
	originalCut := collection.logicalCut.Load()
	if originalVisible == nil {
		collection.writer.Unlock()
		t.Fatal("generation fixture has no visible state")
	}
	exhausted := *originalVisible
	exhausted.root.Generation = generation
	terminalCut, ok := packFileLogicalCut(generation, 0)
	if !ok {
		collection.writer.Unlock()
		t.Fatalf("generation %d did not fit its canonical cut", generation)
	}
	collection.visibleState.Store(&exhausted)
	collection.logicalCut.Store(terminalCut)
	collection.writer.Unlock()
	t.Cleanup(func() {
		collection.writer.Lock()
		collection.visibleState.Store(originalVisible)
		collection.logicalCut.Store(originalCut)
		collection.writer.Unlock()
	})
}

func checkpointGroupTestRewriteLiveJournalHeader(
	t testing.TB,
	group *CheckpointGroup,
	collection *Collection,
	mutate func(*storeio.RecoveryJournalHeader),
) {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	collection.writer.Lock()
	defer collection.writer.Unlock()
	if collection.journal == nil {
		t.Fatal("terminal participant fixture has no journal")
	}
	path := RecoveryJournalPath(collection.file.Name())
	if err := collection.journal.Close(); err != nil {
		t.Fatal(err)
	}
	checkpointGroupTestRewriteJournalHeaderFile(t, path, mutate)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	collection.journal = journal
}

func checkpointGroupTestRewriteJournalHeaderFile(
	t testing.TB,
	path string,
	mutate func(*storeio.RecoveryJournalHeader),
) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	header := journal.Header()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	mutate(&header)
	encoded := make([]byte, storeio.RecoveryJournalHeaderSize)
	if _, err := storeio.EncodeRecoveryJournalHeader(encoded, header); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	for slot := 0; slot < 2; slot++ {
		offset := int64(slot * storeio.RecoveryJournalHeaderSize)
		if n, writeErr := file.WriteAt(encoded, offset); writeErr != nil || n != len(encoded) {
			_ = file.Close()
			t.Fatalf("rewrite journal slot %d = %d,%v", slot, n, writeErr)
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
