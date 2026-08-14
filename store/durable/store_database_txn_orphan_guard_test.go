package durable

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func copyTxnConditionalJournal(
	t *testing.T, sourceCollection, destinationJournal string,
) {
	t.Helper()
	source := RecoveryJournalPath(sourceCollection)
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read conditional journal %q: %v", source, err)
	}
	if err := os.WriteFile(destinationJournal, contents, 0o600); err != nil {
		t.Fatalf("copy conditional journal to %q: %v", destinationJournal, err)
	}
}

func TestTxnLogOnlineRecycleBlockedByUnregisteredConditional(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)
	log := lookupDatabaseTxnLog(db)
	if log == nil || log.marker == nil {
		t.Fatal("committed transaction did not attach a marker")
	}
	beforeHeader := log.marker.Header()
	beforeCursor := log.marker.Cursor()

	unregistered := filepath.Join(
		db.Dir(), strings.Repeat("0123456789abcdef", 4)+".vjc.rjournal",
	)
	copyTxnConditionalJournal(
		t,
		filepath.Join(db.Dir(), collectionFilename(t, "b")),
		unregistered,
	)

	log.commitMu.Lock()
	recycleErr := log.foldLaggardsAndRecycleLocked()
	log.commitMu.Unlock()
	if recycleErr == nil || !strings.Contains(
		recycleErr.Error(), "unregistered conditional journal",
	) {
		t.Fatalf("online recycle = %v, want unregistered-conditional refusal", recycleErr)
	}
	if log.marker.Header() != beforeHeader || log.marker.Cursor() != beforeCursor {
		t.Fatalf(
			"blocked recycle changed marker header/cursor = %+v/%d, want %+v/%d",
			log.marker.Header(), log.marker.Cursor(), beforeHeader, beforeCursor,
		)
	}
	if log.undischarged != 1 {
		t.Fatalf("blocked recycle cleared undischarged = %d, want 1", log.undischarged)
	}
	if log.poison != nil {
		t.Fatalf("safe recycle refusal poisoned transaction log: %v", log.poison)
	}
	for _, name := range []string{"a", "b"} {
		collection, _ := db.Collection(name)
		if persistence := collection.PersistenceError(); persistence != nil {
			t.Fatalf("safe recycle refusal poisoned %s: %v", name, persistence)
		}
	}

	if err := os.Remove(unregistered); err != nil {
		t.Fatalf("remove unregistered conditional: %v", err)
	}
	log.commitMu.Lock()
	retryErr := log.foldLaggardsAndRecycleLocked()
	log.commitMu.Unlock()
	if retryErr != nil {
		t.Fatalf("recycle after orphan cleanup: %v", retryErr)
	}
	if log.marker.Header().Epoch != beforeHeader.Epoch+1 ||
		log.marker.Cursor() != 0 || log.undischarged != 0 {
		t.Fatalf(
			"successful retry marker = epoch %d cursor %d undischarged %d, want %d/0/0",
			log.marker.Header().Epoch, log.marker.Cursor(), log.undischarged,
			beforeHeader.Epoch+1,
		)
	}
}

func TestRetiredOrphanConditionalRetainsMarkerUntilCleanup(t *testing.T) {
	db, _, _ := openTxnDBWithAB(t)
	mustTxnUpdate2(t, db, "k", `{"n":1}`, "k", `{"n":1}`)

	orphanPrimary := filepath.Join(
		db.Dir(), collectionFilename(t, "retired-orphan"),
	)
	orphanJournal := RecoveryJournalPath(orphanPrimary)
	copyTxnConditionalJournal(
		t,
		filepath.Join(db.Dir(), collectionFilename(t, "b")),
		orphanJournal,
	)
	if err := db.DropCollection("b"); err != nil {
		t.Fatalf("DropCollection(b): %v", err)
	}
	if _, err := os.Stat(orphanJournal); err != nil {
		t.Fatalf("drop removed copied orphan: %v", err)
	}
	image := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()

	// Recovery needs the retained decision to validate the missing participant's
	// retirement before orphan cleanup. The directory-wide guard therefore
	// keeps txn.vtm through this open; cleanup then removes the retired orphan.
	first := reopenTxnDatabase(t, image)
	a, ok := first.Collection("a")
	if !ok {
		t.Fatal("first reopen lost collection a")
	}
	if doc, found := collectionDoc(t, a, "k"); !found || doc != `{"n":1}` {
		t.Fatalf("first reopen a/k = %q,%v, want committed", doc, found)
	}
	if _, err := os.Stat(filepath.Join(image, txnMarkerFilename)); err != nil {
		t.Fatalf("marker removed before retired orphan cleanup: %v", err)
	}
	if _, err := os.Stat(orphanJournalForImage(image, orphanJournal)); !errors.Is(
		err, os.ErrNotExist,
	) {
		t.Fatalf("retired orphan survived cleanup: %v", err)
	}

	// Model a crash immediately after that safe ordering point. With no orphan
	// left, the next recovery may remove the discharged marker; an absent-marker
	// reopen is then clean and idempotent rather than bricked by a conditional.
	postCleanupCrash := cloneDatabaseDir(t, image)
	_ = first.Close()
	second := reopenTxnDatabase(t, postCleanupCrash)
	a, ok = second.Collection("a")
	if !ok {
		t.Fatal("second reopen lost collection a")
	}
	if doc, found := collectionDoc(t, a, "k"); !found || doc != `{"n":1}` {
		t.Fatalf("second reopen a/k = %q,%v, want committed", doc, found)
	}
	if _, err := os.Stat(
		filepath.Join(postCleanupCrash, txnMarkerFilename),
	); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discharged marker survived orphan-free reopen: %v", err)
	}
	_ = second.Close()
	third := reopenTxnDatabase(t, postCleanupCrash)
	if _, ok := third.Collection("a"); !ok {
		t.Fatal("absent-marker reopen lost collection a")
	}
}

func orphanJournalForImage(image, original string) string {
	return filepath.Join(image, filepath.Base(original))
}
