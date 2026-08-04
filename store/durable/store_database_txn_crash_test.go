package durable

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Multi-collection crash matrix (design doc "Crash matrix" / walkthrough
// windows W1–W9, A1, R1, E1, and the publish-vs-cut race). Reopen oracles go
// through OpenDatabase; one path also exercises the driver-facing
// RecoverDatabaseTransactions + OpenWithTransactions composition.
//
// Named tests that already live in the T5 recovery suite
// (RecoverySecondCrash, StandaloneOpenInDoubt, DecisionLogMissing) are not
// redefined here — same package, same contractual names.

func upgradeConditionalJournals(t *testing.T, colls ...*Collection) {
	t.Helper()
	for _, coll := range colls {
		coll.writer.Lock()
		coll.journalCatalogOwned = true
		if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
			coll.writer.Unlock()
			t.Fatalf("ensure conditional: %v", err)
		}
		coll.writer.Unlock()
	}
}

func mintEmptyTxnMarker(t *testing.T, dir string) storeio.TxnMarkerHeader {
	t.Helper()
	path := filepath.Join(dir, txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	return header
}

func appendSyncedDecision(
	t *testing.T, dir string, txnID uint64, participants []storeio.TxnParticipant,
) storeio.TxnMarkerHeader {
	t.Helper()
	path := filepath.Join(dir, txnMarkerFilename)
	marker, decisions, err := storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		marker, err = storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = decisions
	}
	if _, err := marker.AppendDecision(txnID, participants); err != nil {
		t.Fatal(err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	return header
}

func assertAbortedNames(t *testing.T, db *Database, names ...string) {
	t.Helper()
	for _, name := range names {
		coll, ok := db.Collection(name)
		if !ok {
			t.Fatalf("missing %s", name)
		}
		if _, found := collectionDoc(t, coll, "k"); found {
			t.Fatalf("%s: presumed-abort applied prepared key", name)
		}
	}
}

func assertNoConditional(
	t *testing.T, coll *Collection, markerID [16]byte, epoch uint64,
) {
	t.Helper()
	coll.writer.Lock()
	holds := coll.journalHoldsConditional(markerID, epoch)
	coll.writer.Unlock()
	if holds {
		t.Fatal("conditional survived reopen fold")
	}
}

// TestDatabaseTxnCrashMatrix covers W1 / W3 / W4 whole-database reopen shapes.
func TestDatabaseTxnCrashMatrix(t *testing.T) {
	t.Run("prepare-subset", func(t *testing.T) {
		// W1: A's prepare durable, B's absent — no decision → abort both.
		db, a, b := openTxnDBWithAB(t)
		header := mintEmptyTxnMarker(t, db.Dir())
		_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 1, "k", `{"n":1}`)
		_ = b // B holds no prepare
		ctrl := newTxnFaultController(t, "a", "b")
		img := ctrl.Capture("w1-prepare-subset", db.Dir())
		_ = db.Close()

		reopened := reopenTxnDatabase(t, img.Dir)
		assertAbortedNames(t, reopened, "a", "b")
		coll, _ := reopened.Collection("a")
		assertNoConditional(t, coll, header.MarkerID, header.Epoch)

		// Live path: B's prepare is dropped (absent on disk); no decision.
		// DropAppend returns success to the writer but leaves no record bytes,
		// so capture immediately after the definite prepare-path reject from a
		// subsequent ENOSPC on the same member is the W1b cover — here use
		// ENOSPC so the coordinator aborts before the decision.
		db2, a2, b2 := openTxnDBWithAB(t)
		upgradeConditionalJournals(t, a2, b2)
		ctrl2 := newTxnFaultController(t, "a", "b")
		ctrl2.AttachOpenJournals(map[string]*Collection{"a": a2, "b": b2})
		ctrl2.ProgramJournal("b", storeio.JournalFaultPlan{
			Phase: storeio.JournalFaultENOSPCAppend, AppendIndex: 0,
		})
		err := db2.Update(func(batch *DatabaseBatch) error {
			ab, _ := batch.Collection("a")
			bb, _ := batch.Collection("b")
			mustTxnPut(t, ab, "k", `{"n":2}`)
			mustTxnPut(t, bb, "k", `{"n":2}`)
			return nil
		})
		if err == nil {
			t.Fatal("expected prepare-subset fault")
		}
		if errors.Is(err, ErrCommitOutcomeUnknown) {
			t.Fatalf("prepare-subset classified unknown: %v", err)
		}
		img2 := ctrl2.Capture("w1-b-prepare-absent", db2.Dir())
		_ = db2.Close()
		assertReopenOutcome(t, img2.Dir, []string{"a", "b"}, false, "")
	})

	t.Run("post-decision", func(t *testing.T) {
		// W3: decision durable, crash before any/all publishes → roll forward.
		for _, tc := range []struct {
			name     string
			publishA bool
			publishB bool
		}{
			{"zero-published", false, false},
			{"one-published", true, false},
			{"all-published", true, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if tc.publishA && tc.publishB {
					db, _, _ := openTxnDBWithAB(t)
					mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
					img := cloneDatabaseDir(t, db.Dir())
					_ = db.Close()
					assertReopenOutcome(t, img, []string{"a", "b"}, true, `{"n":1}`)
					// Driver-facing composition once. After a clean close the
					// discharged log may already have been removed (L4).
					_, log, err := RecoverDatabaseTransactions(img, TxnLogOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if log != nil {
						_ = log.Close()
					}
					return
				}
				db, a, b := openTxnDBWithAB(t)
				header := mintEmptyTxnMarker(t, db.Dir())
				const txnID = uint64(1)
				genA := prepareMaybePublish(t, a, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`, tc.publishA)
				genB := prepareMaybePublish(t, b, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`, tc.publishB)
				_ = appendSyncedDecision(t, db.Dir(), txnID, []storeio.TxnParticipant{
					{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
					{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
				})
				ctrl := newTxnFaultController(t, "a", "b")
				img := ctrl.Capture("w3-"+tc.name, db.Dir())
				_ = db.Close()
				assertReopenOutcome(t, img.Dir, []string{"a", "b"}, true, `{"n":1}`)
			})
		}
	})

	t.Run("partial-checkpoint", func(t *testing.T) {
		// W4: decision durable, A checkpointed past it, B not → complete B.
		db, a, b := openTxnDBWithAB(t)
		header := mintEmptyTxnMarker(t, db.Dir())
		const txnID = uint64(1)
		genA := prepareMaybePublish(t, a, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`, true)
		genB := prepareMaybePublish(t, b, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`, false)
		_ = appendSyncedDecision(t, db.Dir(), txnID, []storeio.TxnParticipant{
			{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
			{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
		})
		ctrl := newTxnFaultController(t, "a", "b")
		img := ctrl.Capture("w4-partial-checkpoint", db.Dir())
		_ = db.Close()
		assertReopenOutcome(t, img.Dir, []string{"a", "b"}, true, `{"n":1}`)
	})
}

// TestDatabaseTxnPrepareFailureAborts is W1b: prepare append and sync seams
// poison with the plain persistence error and return a definite reject.
func TestDatabaseTxnPrepareFailureAborts(t *testing.T) {
	cases := []struct {
		name  string
		phase storeio.JournalFaultPhase
	}{
		{"append", storeio.JournalFaultENOSPCAppend},
		{"sync", storeio.JournalFaultSyncError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, a, b := openTxnDBWithAB(t)
			upgradeConditionalJournals(t, a, b)
			ctrl := newTxnFaultController(t, "a", "b")
			ctrl.AttachOpenJournals(map[string]*Collection{"a": a, "b": b})
			if tc.phase == storeio.JournalFaultSyncError {
				ctrl.ProgramJournal("b", storeio.JournalFaultPlan{
					Phase: tc.phase, SyncIndex: 0,
				})
			} else {
				ctrl.ProgramJournal("b", storeio.JournalFaultPlan{
					Phase: tc.phase, AppendIndex: 0,
				})
			}
			err := db.Update(func(batch *DatabaseBatch) error {
				ab, _ := batch.Collection("a")
				bb, _ := batch.Collection("b")
				mustTxnPut(t, ab, "k", `{"n":1}`)
				mustTxnPut(t, bb, "k", `{"n":1}`)
				return nil
			})
			if err == nil {
				t.Fatal("expected prepare failure")
			}
			if errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("prepare classified unknown: %v", err)
			}
			persistence := b.PersistenceError()
			if persistence == nil {
				t.Fatal("expected sticky persistence poison on failing member")
			}
			if errors.Is(persistence, ErrCommitOutcomeUnknown) {
				t.Fatalf("sticky poison classified unknown: %v", persistence)
			}
			img := ctrl.Capture("w1b-"+tc.name, db.Dir())
			_ = db.Close()
			// No durable decision → reopen all-aborted.
			assertReopenOutcome(t, img.Dir, []string{"a", "b"}, false, "")
		})
	}
}

// TestDatabaseTxnDecisionTornTail is W2: exhaustive byte-prefix sweep over the
// decision record; every incomplete prefix reopens all-aborted.
func TestDatabaseTxnDecisionTornTail(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	// Small capacity so the on-disk image is bounded for the prefix sweep.
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{
		Capacity: 64 * storeio.TxnMarkerMinSectorSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	const txnID = uint64(1)
	genA := prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`)
	genB := prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`)
	participants := []storeio.TxnParticipant{
		{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
		{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
	}
	marker, _, err = storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := marker.AppendDecision(txnID, participants); err != nil {
		t.Fatal(err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	full := mustReadTxnMarkerBytes(t, db.Dir())
	baseImg := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	regionStart := 2 * storeio.TxnMarkerHeaderSize
	raw := storeio.TxnMarkerRecordPrefixSize +
		2*storeio.TxnParticipantSize + storeio.TxnMarkerRecordTrailerSize
	padded := (raw + storeio.TxnMarkerMinSectorSize - 1) /
		storeio.TxnMarkerMinSectorSize * storeio.TxnMarkerMinSectorSize
	decisionEnd := regionStart + padded
	if decisionEnd > len(full) {
		t.Fatalf("decision end %d > file %d", decisionEnd, len(full))
	}
	for size := 0; size <= decisionEnd; size++ {
		img := cloneDatabaseDir(t, baseImg)
		writeTxnMarkerPrefix(t, img, full, size)
		// Classify from the marker alone: a prefix that decodes the decision
		// must reopen committed; every incomplete prefix must abort.
		committedPrefix := decisionPrefixCommitted(t, img, header.MarkerID, header.Epoch, txnID)
		db2, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
		if err != nil {
			if size < regionStart || !committedPrefix {
				continue
			}
			t.Fatalf("prefix %d: OpenDatabase: %v", size, err)
		}
		if committedPrefix {
			assertCommittedAB(t, db2)
		} else {
			assertAbortedNames(t, db2, "a", "b")
		}
		_ = db2.Close()
	}

	// Live torn-append seam: the seam persists a byte prefix and returns
	// success, so the coordinator may publish in memory; the crash image on
	// disk still has a torn decision and must reopen all-aborted.
	db3, a3, b3 := openTxnDBWithAB(t)
	upgradeConditionalJournals(t, a3, b3)
	ctrl := newTxnFaultController(t, "a", "b")
	ctrl.AttachOpenJournals(map[string]*Collection{"a": a3, "b": b3})
	prevMint := databaseTxnAfterMintHook
	databaseTxnAfterMintHook = func(l *TxnLog) {
		if prevMint != nil {
			prevMint(l)
		}
		ctrl.ProgramMarker(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultTornAppend, AppendIndex: 0,
		})
	}
	t.Cleanup(func() { databaseTxnAfterMintHook = prevMint })
	_ = db3.Update(func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	})
	if fm := ctrl.Marker(); fm == nil || !fm.Faulted() {
		t.Fatal("decision torn-append fault did not fire")
	}
	img := ctrl.Capture("w2-torn-append", db3.Dir())
	_ = db3.Close()
	assertReopenOutcome(t, img.Dir, []string{"a", "b"}, false, "")
}

// decisionPrefixCommitted reports whether dir's txn.vtm decodes a committed
// decision for (markerID, epoch, txnID).
func decisionPrefixCommitted(
	t *testing.T, dir string, markerID [16]byte, epoch, txnID uint64,
) bool {
	t.Helper()
	marker, decisions, err := storeio.OpenTxnMarker(
		filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
	)
	if err != nil {
		return false
	}
	defer marker.Close()
	_, ok := decisions.Lookup(markerID, epoch, txnID)
	return ok
}

// TestDatabaseTxnParticipantJournalMissing is W5.
func TestDatabaseTxnParticipantJournalMissing(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	journalB := RecoveryJournalPath(filepath.Join(img, collectionFilename(t, "b")))
	if err := os.Remove(journalB); err != nil {
		t.Fatal(err)
	}
	_, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if err == nil {
		t.Fatal("missing journal opened clean")
	}
	if !errors.Is(err, storeio.ErrRecoveryJournalMissing) &&
		!errors.Is(err, ErrTransactionParticipantMissing) {
		t.Fatalf("missing journal err=%v want journal-missing or participant-missing", err)
	}

	// Participant primary absent under a live decision → typed participant miss.
	db2, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db2, "k", `{"n":2}`, "k", `{"n":2}`)
	img2 := cloneDatabaseDir(t, db2.Dir())
	_ = db2.Close()
	primaryB := filepath.Join(img2, collectionFilename(t, "b"))
	if err := os.Remove(primaryB); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(RecoveryJournalPath(primaryB)); err != nil {
		t.Fatal(err)
	}
	assertReopenFailClosed(t, img2, ErrTransactionParticipantMissing)
}

// TestDatabaseTxnDecisionSyncFailurePoisonsCatalog is W6: live unknown-outcome
// poison, reopen resolves all-or-nothing (append landed → committed).
func TestDatabaseTxnDecisionSyncFailurePoisonsCatalog(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	upgradeConditionalJournals(t, a, b)
	ctrl := newTxnFaultController(t, "a", "b")
	ctrl.AttachOpenJournals(map[string]*Collection{"a": a, "b": b})
	prevMint := databaseTxnAfterMintHook
	databaseTxnAfterMintHook = func(l *TxnLog) {
		if prevMint != nil {
			prevMint(l)
		}
		ctrl.ProgramMarker(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 0,
		})
	}
	t.Cleanup(func() { databaseTxnAfterMintHook = prevMint })

	err := db.Update(func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	})
	if !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("err=%v want ErrCommitOutcomeUnknown", err)
	}
	if fm := ctrl.Marker(); fm == nil || !fm.Faulted() {
		t.Fatal("decision sync fault did not fire")
	}
	for _, coll := range []*Collection{a, b} {
		if !errors.Is(coll.PersistenceError(), ErrCommitOutcomeUnknown) {
			t.Fatalf("poison=%v", coll.PersistenceError())
		}
	}
	img := ctrl.Capture("w6-decision-sync", db.Dir())
	_ = db.Close()
	// Append landed before sync failed; reopen rolls forward.
	assertReopenOutcome(t, img.Dir, []string{"a", "b"}, true, `{"n":1}`)
}

// TestDatabaseTxnDecisionDirectoryFence is W7 / L2: mint parent-dir fence
// failure leaves no usable marker and zero conditionals; reopen is clean.
func TestDatabaseTxnDecisionDirectoryFence(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	beforeA := journalBytes(t, a)
	beforeB := journalBytes(t, b)
	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultCreateParentDirSync,
	})
	t.Cleanup(func() {
		storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	})
	err := db.Update(func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	})
	if err == nil {
		t.Fatal("expected mint fence failure")
	}
	if errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("mint fence classified unknown: %v", err)
	}
	if !storeio.TxnMarkerCreateFaulted() {
		t.Fatal("parent-dir fence fault did not fire")
	}
	if !bytes.Equal(beforeA, journalBytes(t, a)) || !bytes.Equal(beforeB, journalBytes(t, b)) {
		t.Fatal("mint fence mutated journals")
	}
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	// Reopen clean: no conditionals, no torn commit. Marker may be absent or
	// no-valid-header residue (both re-mintable under L2).
	reopened := reopenTxnDatabase(t, img)
	assertAbortedNames(t, reopened, "a", "b")
	if err := reopened.Update(func(batch *DatabaseBatch) error {
		ab, _ := batch.Collection("a")
		bb, _ := batch.Collection("b")
		mustTxnPut(t, ab, "k", `{"n":1}`)
		mustTxnPut(t, bb, "k", `{"n":1}`)
		return nil
	}); err != nil {
		t.Fatalf("remint commit after fence residue: %v", err)
	}
}

// TestDatabaseTxnDropParticipantRetiresFirst is W8.
func TestDatabaseTxnDropParticipantRetiresFirst(t *testing.T) {
	// Clean drop then reopen.
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	if err := db.DropCollection("b"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	reopened := reopenTxnDatabase(t, img)
	if _, ok := reopened.Collection("b"); ok {
		t.Fatal("b still cataloged")
	}
	collA, ok := reopened.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	doc, found := collectionDoc(t, collA, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("a doc=%q found=%v", doc, found)
	}
	_ = reopened.Close()

	// Crash image: retirement durable, primary still present.
	db2, _, b2 := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db2, "k", `{"n":2}`, "k", `{"n":2}`)
	storeB := b2.storeID
	log := lookupDatabaseTxnLog(db2)
	if log == nil || log.marker == nil {
		t.Fatal("expected attached txn log")
	}
	b2.writer.Lock()
	if err := b2.checkpointPastConditionalsLocked(); err != nil {
		b2.writer.Unlock()
		t.Fatal(err)
	}
	b2.writer.Unlock()
	log.commitMu.Lock()
	if _, err := log.marker.AppendRetirement(storeB); err != nil {
		log.commitMu.Unlock()
		t.Fatal(err)
	}
	if err := log.marker.Sync(); err != nil {
		log.commitMu.Unlock()
		t.Fatal(err)
	}
	log.commitMu.Unlock()
	img2 := cloneDatabaseDir(t, db2.Dir())
	_ = db2.Close()
	db3 := reopenTxnDatabase(t, img2)
	if err := db3.DropCollection("b"); err != nil {
		t.Fatalf("finish drop: %v", err)
	}
	if _, ok := db3.Collection("b"); ok {
		t.Fatal("b remained after finished drop")
	}

	// Crash image: checkpoint past conditionals done, retirement not yet.
	db4, _, b4 := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db4, "k", `{"n":3}`, "k", `{"n":3}`)
	b4.writer.Lock()
	if err := b4.checkpointPastConditionalsLocked(); err != nil {
		b4.writer.Unlock()
		t.Fatal(err)
	}
	b4.writer.Unlock()
	img4 := cloneDatabaseDir(t, db4.Dir())
	_ = db4.Close()
	db5 := reopenTxnDatabase(t, img4)
	if err := db5.DropCollection("b"); err != nil {
		t.Fatalf("drop after checkpoint-only image: %v", err)
	}
}

// TestDatabaseTxnMaterializationRollbackOrder is W9: torn in-flight
// materialization capsule on a participant does not prevent decision replay;
// open recovers materialization first, then applies the durable decision.
func TestDatabaseTxnMaterializationRollbackOrder(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	header := mintEmptyTxnMarker(t, db.Dir())
	const txnID = uint64(1)
	genA := prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`)
	genB := prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`)
	_ = appendSyncedDecision(t, db.Dir(), txnID, []storeio.TxnParticipant{
		{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
		{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
	})
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	primaryA := filepath.Join(img, collectionFilename(t, "a"))
	tearMaterializationSlot(t, primaryA)
	assertReopenOutcome(t, img, []string{"a", "b"}, true, `{"n":1}`)

	// Driver-facing composition on the same torn-capsule image.
	img2 := cloneDatabaseDir(t, img)
	tearMaterializationSlot(t, filepath.Join(img2, collectionFilename(t, "a")))
	decisions, log, err := RecoverDatabaseTransactions(img2, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	file, err := os.OpenFile(filepath.Join(img2, collectionFilename(t, "a")), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	coll, err := OpenWithTransactions(file, txnTestOptions(), decisions)
	if err != nil {
		t.Fatalf("OpenWithTransactions: %v", err)
	}
	defer coll.Close()
	doc, found := collectionDoc(t, coll, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("doc=%q found=%v", doc, found)
	}
}

// TestDatabaseTxnAbortedGenerationAliasing is A1 at database scope.
func TestDatabaseTxnAbortedGenerationAliasing(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	header := mintEmptyTxnMarker(t, db.Dir())
	_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 1, "abort", `{"n":0}`)
	// kind-3 at the same generation on A after the aborted kind-5.
	if err := a.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("k"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("k"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	reopened := reopenTxnDatabase(t, img)
	collA, _ := reopened.Collection("a")
	doc, found := collectionDoc(t, collA, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("a kind-3 missing: %q,%v", doc, found)
	}
	if _, found := collectionDoc(t, collA, "abort"); found {
		t.Fatal("aborted kind-5 key applied")
	}
	assertNoConditional(t, collA, header.MarkerID, header.Epoch)
	// Second reopen idempotent.
	dir := reopened.Dir()
	_ = reopened.Close()
	reopened2 := reopenTxnDatabase(t, dir)
	collA2, _ := reopened2.Collection("a")
	doc, found = collectionDoc(t, collA2, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("second open: %q,%v", doc, found)
	}
}

// TestDatabaseTxnStrayConditionalTxnIDReuse is R1.
func TestDatabaseTxnStrayConditionalTxnIDReuse(t *testing.T) {
	t.Run("stray-consumed", func(t *testing.T) {
		// Decision append failure leaves stray undecided kind-5 for txn N.
		db, a, b := openTxnDBWithAB(t)
		upgradeConditionalJournals(t, a, b)
		ctrl := newTxnFaultController(t, "a", "b")
		ctrl.AttachOpenJournals(map[string]*Collection{"a": a, "b": b})
		prevMint := databaseTxnAfterMintHook
		databaseTxnAfterMintHook = func(l *TxnLog) {
			if prevMint != nil {
				prevMint(l)
			}
			ctrl.ProgramMarker(storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultAppendError, AppendIndex: 0,
			})
		}
		t.Cleanup(func() { databaseTxnAfterMintHook = prevMint })
		err := db.Update(func(batch *DatabaseBatch) error {
			ab, _ := batch.Collection("a")
			bb, _ := batch.Collection("b")
			mustTxnPut(t, ab, "stray", `{"n":1}`)
			mustTxnPut(t, bb, "stray", `{"n":1}`)
			return nil
		})
		if err == nil {
			t.Fatal("expected decision append failure")
		}
		if ctrl.log == nil || ctrl.log.marker == nil {
			t.Fatal("expected minted marker after prepare")
		}
		header := ctrl.log.marker.Header()
		img := ctrl.Capture("r1-stray", db.Dir())
		_ = db.Close()

		first := reopenTxnDatabase(t, img.Dir)
		coll, _ := first.Collection("a")
		assertNoConditional(t, coll, header.MarkerID, header.Epoch)
		if _, found := collectionDoc(t, coll, "stray"); found {
			t.Fatal("stray applied")
		}
		dir := first.Dir()
		_ = first.Close()
		second := reopenTxnDatabase(t, dir)
		coll2, _ := second.Collection("a")
		coll2.writer.Lock()
		cursor := coll2.journal.Cursor()
		coll2.writer.Unlock()
		if cursor != 0 {
			t.Fatalf("second reopen cursor=%d, want 0", cursor)
		}
	})

	t.Run("adversarial-txnid-reuse", func(t *testing.T) {
		// Stray undecided kind-5 for txn N on A, plus a durable decision that
		// reuses TxnID N naming only B — participant binding must skip A's stray.
		db, a, b := openTxnDBWithAB(t)
		header := mintEmptyTxnMarker(t, db.Dir())
		const txnID = uint64(9)
		_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, txnID, "stray", `{"bad":1}`)
		genB := prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`)
		_ = appendSyncedDecision(t, db.Dir(), txnID, []storeio.TxnParticipant{
			{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
		})
		img := cloneDatabaseDir(t, db.Dir())
		_ = db.Close()

		reopened := reopenTxnDatabase(t, img)
		collA, _ := reopened.Collection("a")
		collB, _ := reopened.Collection("b")
		if _, found := collectionDoc(t, collA, "stray"); found {
			t.Fatal("stray applied under reused TxnID")
		}
		doc, found := collectionDoc(t, collB, "k")
		if !found || doc != `{"n":1}` {
			t.Fatalf("reused decision outcome on b: %q,%v", doc, found)
		}
		assertNoConditional(t, collA, header.MarkerID, header.Epoch)
	})
}

// TestDatabaseTxnDecisionEpochMismatch is E1: restored older log beside newer
// journals (and the reverse) fails closed.
func TestDatabaseTxnDecisionEpochMismatch(t *testing.T) {
	t.Run("log-ahead-of-journals", func(t *testing.T) {
		db, a, b := openTxnDBWithAB(t)
		header := mintEmptyTxnMarker(t, db.Dir())
		_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 1, "k", `{"n":1}`)
		_ = prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch, 1, "k", `{"n":1}`)
		path := filepath.Join(db.Dir(), txnMarkerFilename)
		marker, _, err := storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := marker.Recycle(header.Epoch + 1); err != nil {
			t.Fatal(err)
		}
		if err := marker.Close(); err != nil {
			t.Fatal(err)
		}
		img := cloneDatabaseDir(t, db.Dir())
		_ = db.Close()
		assertReopenFailClosed(t, img, ErrTransactionMarkerEpochMismatch)
	})

	t.Run("journals-ahead-of-log", func(t *testing.T) {
		db, a, b := openTxnDBWithAB(t)
		header := mintEmptyTxnMarker(t, db.Dir())
		_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch+5, 1, "k", `{"n":1}`)
		_ = prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch+5, 1, "k", `{"n":1}`)
		img := cloneDatabaseDir(t, db.Dir())
		_ = db.Close()
		assertReopenFailClosed(t, img, ErrTransactionMarkerEpochMismatch)
	})
}

// TestDatabaseTxnPublishExcludesSnapshotCut hammers multi-collection publish
// against Snapshot / SnapshotCollections under -race: no cut observes a torn
// participant set.
func TestDatabaseTxnPublishExcludesSnapshotCut(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b", "c")
	var stop atomic.Bool
	var wg sync.WaitGroup
	var torn atomic.Uint64

	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for !stop.Load() {
			n++
			doc := `{"n":1}`
			if n%2 == 0 {
				doc = `{"n":2}`
			}
			if err := db.Update(func(batch *DatabaseBatch) error {
				a, _ := batch.Collection("a")
				b, _ := batch.Collection("b")
				_ = a.Put([]byte("k"), []byte(doc))
				_ = b.Put([]byte("k"), []byte(doc))
				return nil
			}); err != nil {
				t.Errorf("Update: %v", err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			snap, err := db.Snapshot()
			if err != nil {
				t.Errorf("Snapshot: %v", err)
				return
			}
			a, _ := snap.Collection("a")
			b, _ := snap.Collection("b")
			av, aok, _ := a.AppendRaw(nil, []byte("k"))
			bv, bok, _ := b.AppendRaw(nil, []byte("k"))
			_ = snap.Close()
			if aok != bok || (aok && string(av) != string(bv)) {
				torn.Add(1)
				t.Errorf("torn Snapshot cut: a=%q,%v b=%q,%v", av, aok, bv, bok)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		a, _ := db.Collection("a")
		b, _ := db.Collection("b")
		for !stop.Load() {
			snap, err := SnapshotCollections([]NamedCollection{
				{Name: "a", Collection: a},
				{Name: "b", Collection: b},
			})
			if err != nil {
				continue
			}
			ac, _ := snap.Collection("a")
			bc, _ := snap.Collection("b")
			av, aok, _ := ac.AppendRaw(nil, []byte("k"))
			bv, bok, _ := bc.AppendRaw(nil, []byte("k"))
			_ = snap.Close()
			if aok != bok || (aok && string(av) != string(bv)) {
				torn.Add(1)
				t.Errorf("torn SnapshotCollections cut: a=%q,%v b=%q,%v", av, aok, bv, bok)
				return
			}
		}
	}()

	time.Sleep(1500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if torn.Load() != 0 {
		t.Fatalf("observed %d torn cuts", torn.Load())
	}
}
