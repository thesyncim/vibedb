package durable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

func mustTxnUpdate2(t testing.TB, db *Database, aKey, aDoc, bKey, bDoc string) {
	t.Helper()
	if err := db.Update(func(batch *DatabaseBatch) error {
		a, err := batch.Collection("a")
		if err != nil {
			return err
		}
		b, err := batch.Collection("b")
		if err != nil {
			return err
		}
		if err := a.Put([]byte(aKey), []byte(aDoc)); err != nil {
			return err
		}
		return b.Put([]byte(bKey), []byte(bDoc))
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func reopenTxnDatabase(t testing.TB, dir string) *Database {
	t.Helper()
	db, err := OpenDatabase(dir, DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func collectionFilename(t testing.TB, name string) string {
	t.Helper()
	filename, ok := collectionname.Encode(name)
	if !ok {
		t.Fatalf("encode %q", name)
	}
	return filename
}

func testTxnDecisionsInDir(
	t testing.TB, dir string,
) *storeio.TxnDecisions {
	t.Helper()
	path := filepath.Join(dir, txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	opened, decisions, err := storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	return decisions
}

func testTxnDecisions(t testing.TB) *storeio.TxnDecisions {
	t.Helper()
	return testTxnDecisionsInDir(t, t.TempDir())
}

func TestOpenWithTransactionsDirectoryIdentityFailsClosed(t *testing.T) {
	decisions := testTxnDecisions(t)

	t.Run("different directory", func(t *testing.T) {
		file, err := os.Create(filepath.Join(t.TempDir(), "collection.vdb"))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, err := OpenWithTransactions(file, Options{}, decisions); !errors.Is(
			err, ErrTransactionLogDirectoryMismatch,
		) {
			t.Fatalf("different directory error = %v, want directory mismatch", err)
		}
	})

	t.Run("unresolved symlink", func(t *testing.T) {
		target := t.TempDir()
		realPath := filepath.Join(target, "collection.vdb")
		if err := os.WriteFile(realPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(t.TempDir(), "collection-dir")
		if err := os.Symlink(target, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		file, err := os.Open(filepath.Join(link, "collection.vdb"))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		_, err = OpenWithTransactions(file, Options{}, decisions)
		if err == nil || errors.Is(err, ErrTransactionLogDirectoryMismatch) {
			t.Fatalf("unresolved directory error = %v, want resolution failure", err)
		}
	})

	t.Run("retargeted symlink", func(t *testing.T) {
		dirA := t.TempDir()
		dirB := t.TempDir()
		decisions := testTxnDecisionsInDir(t, dirB)
		const name = "collection.vdb"
		if err := os.WriteFile(filepath.Join(dirA, name), []byte("a"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dirB, name), []byte("b"), 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(t.TempDir(), "collection-dir")
		if err := os.Symlink(dirA, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		file, err := os.Open(filepath.Join(link, name))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(dirB, link); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenWithTransactions(file, Options{}, decisions); !errors.Is(
			err, ErrTransactionLogDirectoryMismatch,
		) {
			t.Fatalf("retargeted directory error = %v, want directory mismatch", err)
		}
	})
}

// cloneDatabaseDir copies every regular file in src to a fresh temp directory.
// Crash-image tests use this before Database.Close, which checkpoints and
// recycles journals and would otherwise discard unpublished kind-5 records.
func cloneDatabaseDir(t testing.TB, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, entry.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func prepareUnpublishedOn(
	t *testing.T, coll *Collection, markerID [16]byte, epoch, txnID uint64, key, doc string,
) uint64 {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	coll.journalCatalogOwned = true
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		t.Fatalf("ensure conditional: %v", err)
	}
	batch := coll.fileWriteBatch()
	defer coll.releaseFileWriteBatch(batch)
	if err := batch.Put([]byte(key), []byte(doc)); err != nil {
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchLocked(batch)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if !staged.live {
		t.Fatal("expected live staged batch")
	}
	gen := staged.generation
	if err := coll.preparePrimaryBatchConditionalLocked(
		&staged, markerID, epoch, txnID, true,
	); err != nil {
		coll.unwindStagedPrimaryBatch(&staged)
		t.Fatalf("prepare: %v", err)
	}
	coll.unwindStagedPrimaryBatch(&staged)
	return gen
}

func assertCommittedAB(t *testing.T, db *Database) {
	t.Helper()
	for _, name := range []string{"a", "b"} {
		coll, ok := db.Collection(name)
		if !ok {
			t.Fatalf("missing %s", name)
		}
		doc, found := collectionDoc(t, coll, "k")
		if !found || doc != `{"n":1}` {
			t.Fatalf("%s doc=%q found=%v want committed", name, doc, found)
		}
	}
}

func openTxnDBWithAB(t *testing.T) (*Database, *Collection, *Collection) {
	t.Helper()
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a, err := db.CreateCollection("a", txnTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.CreateCollection("b", txnTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	return db, a, b
}

// TestDatabaseTxnReconcileCommittedRollForward covers W3/W4 shapes at the
// integration level: decision durable with zero/one/all participants already
// published; reopen recovers all-committed content.
func TestDatabaseTxnReconcileCommittedRollForward(t *testing.T) {
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
				dir := db.Dir()
				_ = db.Close()
				assertCommittedAB(t, reopenTxnDatabase(t, dir))
				return
			}

			db, a, b := openTxnDBWithAB(t)
			path := filepath.Join(db.Dir(), txnMarkerFilename)
			marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			header := marker.Header()
			const txnID = uint64(1)
			genA := prepareMaybePublish(t, a, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`, tc.publishA)
			genB := prepareMaybePublish(t, b, header.MarkerID, header.Epoch, txnID, "k", `{"n":1}`, tc.publishB)
			participants := []storeio.TxnParticipant{
				{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
				{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
			}
			if _, err := marker.AppendDecision(txnID, participants); err != nil {
				t.Fatal(err)
			}
			if err := marker.Sync(); err != nil {
				t.Fatal(err)
			}
			_ = marker.Close()

			img := cloneDatabaseDir(t, db.Dir())
			_ = db.Close()
			assertCommittedAB(t, reopenTxnDatabase(t, img))
		})
	}
}

// prepareMaybePublish prepares a kind-5 record. When publish is true it also
// publishes and checkpoints past the conditional (W4: this participant's root
// already covers the decision); otherwise it unwinds memory and leaves the
// durable prepare in the journal (W3).
func prepareMaybePublish(
	t *testing.T, coll *Collection, markerID [16]byte, epoch, txnID uint64,
	key, doc string, publish bool,
) uint64 {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	coll.journalCatalogOwned = true
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		t.Fatalf("ensure conditional: %v", err)
	}
	batch := coll.fileWriteBatch()
	defer coll.releaseFileWriteBatch(batch)
	if err := batch.Put([]byte(key), []byte(doc)); err != nil {
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchLocked(batch)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if !staged.live {
		t.Fatal("expected live staged batch")
	}
	gen := staged.generation
	if err := coll.preparePrimaryBatchConditionalLocked(
		&staged, markerID, epoch, txnID, true,
	); err != nil {
		coll.unwindStagedPrimaryBatch(&staged)
		t.Fatalf("prepare: %v", err)
	}
	if !publish {
		coll.unwindStagedPrimaryBatch(&staged)
		return gen
	}
	coll.snapshotGate.Lock()
	coll.publishPrimaryBatchGateHeld(staged)
	coll.snapshotGate.Unlock()
	staged.live = false
	if err := coll.checkpointPastConditionalsLocked(); err != nil {
		t.Fatalf("checkpoint past conditionals: %v", err)
	}
	return coll.state.Load().root.Generation
}

// TestDatabaseTxnReconcilePresumedAbort proves prepared-only images reopen
// all-aborted (no decision → presumed abort, window consumed).
func TestDatabaseTxnReconcilePresumedAbort(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	_ = marker.Close()
	_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 7, "k", `{"n":1}`)
	_ = prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch, 7, "k", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	reopened := reopenTxnDatabase(t, img)
	for _, name := range []string{"a", "b"} {
		coll, ok := reopened.Collection(name)
		if !ok {
			t.Fatalf("missing %s", name)
		}
		if _, found := collectionDoc(t, coll, "k"); found {
			t.Fatalf("%s: presumed-abort applied prepared key", name)
		}
		coll.writer.Lock()
		holds := coll.journalHoldsConditional(header.MarkerID, header.Epoch)
		coll.writer.Unlock()
		if holds {
			t.Fatalf("%s: stray conditional survived reopen", name)
		}
	}
}

// TestDatabaseTxnParticipantMissingFailClosed deletes one participant under a
// live decision and proves OpenDatabase fails; a retirement record opens clean.
func TestDatabaseTxnParticipantMissingFailClosed(t *testing.T) {
	db, _, b := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	storeB := b.storeID
	dir := db.Dir()
	img := cloneDatabaseDir(t, dir)
	_ = db.Close()

	primaryB := filepath.Join(img, collectionFilename(t, "b"))
	journalB := RecoveryJournalPath(primaryB)
	if err := os.Remove(primaryB); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(journalB); err != nil {
		t.Fatal(err)
	}
	_, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if !errors.Is(err, ErrTransactionParticipantMissing) {
		t.Fatalf("OpenDatabase err=%v want ErrTransactionParticipantMissing", err)
	}

	marker, decisions, err := storeio.OpenTxnMarker(
		filepath.Join(img, txnMarkerFilename), storeio.TxnMarkerOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if decisions.Retired(storeB) {
		t.Fatal("B unexpectedly already retired")
	}
	if _, err := marker.AppendRetirement(storeB); err != nil {
		t.Fatal(err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = marker.Close()

	reopened := reopenTxnDatabase(t, img)
	if _, ok := reopened.Collection("b"); ok {
		t.Fatal("removed collection still cataloged")
	}
	coll, ok := reopened.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	doc, found := collectionDoc(t, coll, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("a doc=%q found=%v", doc, found)
	}
}

// TestDatabaseTxnDropBarrierRetiresFirst exercises DropCollection's
// checkpoint → retirement → deletion ordering via crash images.
func TestDatabaseTxnDropBarrierRetiresFirst(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	if err := db.DropCollection("b"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}
	primaryB := filepath.Join(db.Dir(), collectionFilename(t, "b"))
	if _, err := os.Stat(primaryB); !os.IsNotExist(err) {
		t.Fatalf("b primary survived drop: %v", err)
	}
	dir := db.Dir()
	_ = db.Close()
	reopened := reopenTxnDatabase(t, dir)
	if _, ok := reopened.Collection("b"); ok {
		t.Fatal("b still cataloged after drop+reopen")
	}
	a, ok := reopened.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	doc, found := collectionDoc(t, a, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("a doc=%q found=%v", doc, found)
	}

	// Retirement present, primary still on disk (crash between retirement sync
	// and primary removal) — reopen + DropCollection finishes.
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
	img := cloneDatabaseDir(t, db2.Dir())
	_ = db2.Close()

	db3 := reopenTxnDatabase(t, img)
	if err := db3.DropCollection("b"); err != nil {
		t.Fatalf("finish drop: %v", err)
	}
	if _, ok := db3.Collection("b"); ok {
		t.Fatal("b remained after finished drop")
	}
}

// TestDatabaseTxnStandaloneOpenInDoubt proves durable.Open refuses an in-doubt
// file and succeeds after the database reopens and checkpoints past it.
func TestDatabaseTxnStandaloneOpenInDoubt(t *testing.T) {
	db, a, b := openTxnDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	_ = marker.Close()
	_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 3, "k", `{"n":1}`)
	_ = prepareUnpublishedOn(t, b, header.MarkerID, header.Epoch, 3, "k", `{"n":1}`)

	primaryA := filepath.Join(db.Dir(), collectionFilename(t, "a"))
	storeBytes, err := os.ReadFile(primaryA)
	if err != nil {
		t.Fatal(err)
	}
	journalBytes, err := os.ReadFile(RecoveryJournalPath(primaryA))
	if err != nil {
		t.Fatal(err)
	}
	imgDir := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	file, err := os.OpenFile(img, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	_, err = Open(file, txnTestOptions())
	if !errors.Is(err, ErrCollectionInDoubt) {
		t.Fatalf("standalone Open err=%v want ErrCollectionInDoubt", err)
	}

	reopened := reopenTxnDatabase(t, imgDir)
	coll, ok := reopened.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	coll.writer.Lock()
	holds := coll.journalHoldsConditional(header.MarkerID, header.Epoch)
	cursor := coll.journal.Cursor()
	coll.writer.Unlock()
	if holds || cursor != 0 {
		t.Fatalf("after reopen holds=%v cursor=%d", holds, cursor)
	}
	primaryAfter := filepath.Join(imgDir, collectionFilename(t, "a"))
	storeBytes, err = os.ReadFile(primaryAfter)
	if err != nil {
		t.Fatal(err)
	}
	journalBytes, err = os.ReadFile(RecoveryJournalPath(primaryAfter))
	if err != nil {
		t.Fatal(err)
	}
	_ = reopened.Close()

	img2 := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	file2, err := os.OpenFile(img2, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file2.Close()
	standalone, err := Open(file2, txnTestOptions())
	if err != nil {
		t.Fatalf("standalone Open after checkpoint: %v", err)
	}
	_ = standalone.Close()
}

// TestDatabaseTxnAbsentLogCleanOpen is L1: no txn.vtm and no conditionals —
// open matches baseline and engages no transaction machinery.
func TestDatabaseTxnAbsentLogCleanOpen(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	coll, err := db.CreateCollection("orders", txnTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coll.Put([]byte("k"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if lookupDatabaseTxnLog(db) != nil {
		t.Fatal("TxnLog attached without txn.vtm")
	}
	dir := db.Dir()
	_ = db.Close()
	if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
		t.Fatalf("txn.vtm present after single-collection history: %v", err)
	}

	reopened := reopenTxnDatabase(t, dir)
	if lookupDatabaseTxnLog(reopened) != nil {
		t.Fatal("TxnLog attached on clean absent reopen")
	}
	got, ok := reopened.Collection("orders")
	if !ok {
		t.Fatal("missing orders")
	}
	doc, found := collectionDoc(t, got, "k")
	if !found || doc != `{"n":1}` {
		t.Fatalf("doc=%q found=%v", doc, found)
	}
}

// TestDatabaseTxnDecisionLogMissing is L3 at the reconciliation level.
func TestDatabaseTxnDecisionLogMissing(t *testing.T) {
	db, a, _ := openTxnDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	_ = marker.Close()
	_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 4, "k", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	if err := os.Remove(filepath.Join(img, txnMarkerFilename)); err != nil {
		t.Fatal(err)
	}
	_, err = OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if !errors.Is(err, ErrTransactionLogMissing) {
		t.Fatalf("OpenDatabase err=%v want ErrTransactionLogMissing", err)
	}

	// Covered-only variant: advance root past the kind-5, delete log, open clean.
	db2, a2, _ := openTxnDBWithAB(t)
	path2 := filepath.Join(db2.Dir(), txnMarkerFilename)
	marker2, err := storeio.CreateTxnMarker(path2, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header2 := marker2.Header()
	_ = marker2.Close()
	_ = prepareUnpublishedOn(t, a2, header2.MarkerID, header2.Epoch, 5, "k", `{"n":1}`)
	if err := a2.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("cover"), []byte(`{"c":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	advanceDurableRootWithoutRecycle(t, a2)
	a2.writer.Lock()
	holds := a2.journalHoldsConditional(header2.MarkerID, header2.Epoch)
	a2.writer.Unlock()
	if !holds {
		t.Fatal("expected covered kind-5 retained")
	}
	img2 := cloneDatabaseDir(t, db2.Dir())
	_ = db2.Close()
	if err := os.Remove(filepath.Join(img2, txnMarkerFilename)); err != nil {
		t.Fatal(err)
	}
	reopened := reopenTxnDatabase(t, img2)
	coll, ok := reopened.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	coll.writer.Lock()
	holds = coll.journalHoldsConditional(header2.MarkerID, header2.Epoch)
	cursor := coll.journal.Cursor()
	coll.writer.Unlock()
	if holds || cursor != 0 {
		t.Fatalf("covered-only absent log left window holds=%v cursor=%d", holds, cursor)
	}
}

// TestDatabaseTxnLogRemovalIdempotent is L4.
func TestDatabaseTxnLogRemovalIdempotent(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	dir := db.Dir()
	_ = db.Close()

	// First reopen: replay discharges + folds; L4 removes txn.vtm.
	reopened := reopenTxnDatabase(t, dir)
	_ = reopened.Close()
	if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
		t.Fatalf("txn.vtm survived discharged reopen: %v", err)
	}
	again := reopenTxnDatabase(t, dir)
	assertCommittedAB(t, again)
	_ = again.Close()

	// Image violating the removal predicate: a live decision names a missing
	// participant. Open fails closed and must not remove txn.vtm.
	db2, _, b2 := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db2, "k", `{"n":9}`, "k", `{"n":9}`)
	img := cloneDatabaseDir(t, db2.Dir())
	_ = db2.Close()
	_ = b2
	primaryB := filepath.Join(img, collectionFilename(t, "b"))
	if err := os.Remove(primaryB); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(RecoveryJournalPath(primaryB)); err != nil {
		t.Fatal(err)
	}
	_, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if !errors.Is(err, ErrTransactionParticipantMissing) {
		t.Fatalf("OpenDatabase err=%v want ErrTransactionParticipantMissing", err)
	}
	if _, err := os.Stat(filepath.Join(img, txnMarkerFilename)); err != nil {
		t.Fatalf("txn.vtm removed despite undischarged decision: %v", err)
	}
}

// TestDatabaseTxnMintResidueOpen is L2 residue policy.
func TestDatabaseTxnMintResidueOpen(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("a", txnTestOptions()); err != nil {
		t.Fatal(err)
	}
	dir := db.Dir()
	_ = db.Close()

	path := filepath.Join(dir, txnMarkerFilename)
	if err := os.WriteFile(path, make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened := reopenTxnDatabase(t, dir)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("mint residue not removed: %v", err)
	}
	_ = reopened.Close()

	// Same headerless file beside a journal holding a kind-5 → fail closed.
	db2, a2, _ := openTxnDBWithAB(t)
	_ = prepareUnpublishedOn(t, a2, conditionalMarkerID(1), 1, 1, "k", `{"n":1}`)
	img := cloneDatabaseDir(t, db2.Dir())
	_ = db2.Close()
	path2 := filepath.Join(img, txnMarkerFilename)
	if err := os.WriteFile(path2, make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if !errors.Is(err, ErrTransactionLogMissing) &&
		!errors.Is(err, storeio.ErrTxnMarkerNoValidHeader) {
		t.Fatalf("err=%v want tamper fail-closed", err)
	}
}

// TestDatabaseTxnRecoverySecondCrash proves crash mid-replay then reopen is
// deterministic.
func TestDatabaseTxnRecoverySecondCrash(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	var interrupted bool
	prev := recoveryJournalReplayBatchEntryHook
	recoveryJournalReplayBatchEntryHook = func(
		_ *Collection, rec storeio.RecoveryRecord, _ int,
	) error {
		if !interrupted && rec.Kind == storeio.RecoveryRecordKindConditionalBatch {
			interrupted = true
			return errors.New("injected mid-replay crash")
		}
		return nil
	}
	_, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	recoveryJournalReplayBatchEntryHook = prev
	if err == nil {
		t.Fatal("expected mid-replay failure")
	}
	if !interrupted {
		t.Fatal("mid-replay hook did not fire on kind-5")
	}

	recoveryJournalReplayBatchEntryHook = nil
	reopened := reopenTxnDatabase(t, img)
	assertCommittedAB(t, reopened)
}

// TestDatabaseTxnLogLifecycleOpenCloseReopen pins attach/detach around Close.
func TestDatabaseTxnLogLifecycleOpenCloseReopen(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	if lookupDatabaseTxnLog(db) == nil {
		t.Fatal("expected attached TxnLog after multi-collection commit")
	}
	dir := db.Dir()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if lookupDatabaseTxnLog(db) != nil {
		t.Fatal("TxnLog seam survived Close")
	}
	if err := db.Update(func(*DatabaseBatch) error { return nil }); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Update after Close=%v want ErrDatabaseClosed", err)
	}

	reopened, err := OpenDatabase(dir, DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Update(func(batch *DatabaseBatch) error {
		a, _ := batch.Collection("a")
		b, _ := batch.Collection("b")
		_ = a.Put([]byte("k"), []byte(`{"n":2}`))
		_ = b.Put([]byte("k"), []byte(`{"n":2}`))
		return nil
	}); err != nil {
		t.Fatalf("reopened Update: %v", err)
	}
	if lookupDatabaseTxnLog(reopened) == nil {
		t.Fatal("expected TxnLog after remint commit")
	}
}

// TestDatabaseTxnStrayConsumptionAtOpen is R1's database-level half.
func TestDatabaseTxnStrayConsumptionAtOpen(t *testing.T) {
	db, a, _ := openTxnDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	_ = marker.Close()
	_ = prepareUnpublishedOn(t, a, header.MarkerID, header.Epoch, 11, "stray", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	reopened := reopenTxnDatabase(t, img)
	coll, _ := reopened.Collection("a")
	coll.writer.Lock()
	holds := coll.journalHoldsConditional(header.MarkerID, header.Epoch)
	coll.writer.Unlock()
	if holds {
		t.Fatal("stray conditional survived open fold")
	}
	if err := reopened.Update(func(batch *DatabaseBatch) error {
		a, _ := batch.Collection("a")
		b, _ := batch.Collection("b")
		_ = a.Put([]byte("k"), []byte(`{"n":2}`))
		_ = b.Put([]byte("k"), []byte(`{"n":2}`))
		return nil
	}); err != nil {
		t.Fatalf("commit after stray consumption: %v", err)
	}
}

// TestRecoverDatabaseTransactionsAndOpenWithTransactions exercises the
// driver-facing composition.
func TestRecoverDatabaseTransactionsAndOpenWithTransactions(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	decisions, log, err := RecoverDatabaseTransactions(img, TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if decisions == nil {
		t.Fatal("expected decisions after committed multi-collection close")
	}

	primaryA := filepath.Join(img, collectionFilename(t, "a"))
	file, err := os.OpenFile(primaryA, os.O_RDWR, 0)
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
	if !coll.journalCatalogOwned {
		t.Fatal("OpenWithTransactions left journalCatalogOwned false")
	}
}
