package durable

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestDropCollectionRetryAfterRetirementSyncFailureIsPoisoned(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b")
	if err := db.Update(func(batch *DatabaseBatch) error {
		return putTxnPair(t, batch, "a", "b")
	}); err != nil {
		t.Fatalf("seed committed decision: %v", err)
	}
	log := lookupDatabaseTxnLog(db)
	if log == nil || log.marker == nil {
		t.Fatal("seed commit did not mint transaction marker")
	}
	db.mu.RLock()
	entry := db.collections["b"]
	primary := filepath.Join(db.dir, entry.filename)
	db.mu.RUnlock()

	var fault *storeio.FaultTxnMarker
	previousHook := databaseTxnBeforeRetirementAppendHook
	databaseTxnBeforeRetirementAppendHook = func(current *TxnLog) {
		if current != log || fault != nil {
			return
		}
		fault = storeio.NewFaultTxnMarker(current.marker)
		fault.Program(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 0,
		})
	}
	t.Cleanup(func() { databaseTxnBeforeRetirementAppendHook = previousHook })

	firstErr := db.DropCollection("b")
	if !errors.Is(firstErr, ErrCommitOutcomeUnknown) ||
		!errors.Is(firstErr, syscall.EIO) {
		t.Fatalf("first DropCollection = %v, want outcome-unknown EIO", firstErr)
	}
	if fault == nil || !fault.Faulted() || fault.Syncs() != 1 {
		t.Fatalf("retirement sync fault = %+v, want one fired sync", fault)
	}
	if _, ok := db.Collection("b"); !ok {
		t.Fatal("failed retirement removed collection from live catalog")
	}
	if _, err := os.Stat(primary); err != nil {
		t.Fatalf("failed retirement removed primary: %v", err)
	}
	cursor := log.marker.Cursor()
	secondErr := db.DropCollection("b")
	if !errors.Is(secondErr, ErrTxnLogPoisoned) ||
		!errors.Is(secondErr, ErrCommitOutcomeUnknown) {
		t.Fatalf("retry DropCollection = %v, want poisoned outcome-unknown", secondErr)
	}
	if log.marker.Cursor() != cursor || fault.Syncs() != 1 {
		t.Fatalf(
			"poisoned retry changed marker cursor/syncs = %d/%d, want %d/1",
			log.marker.Cursor(), fault.Syncs(), cursor,
		)
	}
	if _, ok := db.Collection("b"); !ok {
		t.Fatal("poisoned retry removed collection from live catalog")
	}
}

func TestDropCollectionFoldsBeforeFullMarkerRetirement(t *testing.T) {
	db, err := OpenDatabase(
		t.TempDir(), DatabaseOptions{Options: txnTestOptions()},
	)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})
	for _, name := range []string{"a", "b", "c"} {
		if _, err := db.CreateCollection(name, txnTestOptions()); err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
	}

	// One two-participant decision consumes the entire one-sector marker. A
	// subsequent retirement therefore has a definite no-room result before any
	// retirement append can make its outcome ambiguous.
	log, err := NewTxnLog(db.Dir(), TxnLogOptions{
		Capacity: storeio.TxnMarkerMinSectorSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	attachDatabaseTxnLog(db, log)
	if err := db.Update(func(batch *DatabaseBatch) error {
		a, err := batch.Collection("a")
		if err != nil {
			return err
		}
		b, err := batch.Collection("b")
		if err != nil {
			return err
		}
		if err := a.Put([]byte("before"), []byte(`{"n":1}`)); err != nil {
			return err
		}
		return b.Put([]byte("before"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("fill marker: %v", err)
	}
	if log.marker.Cursor() != storeio.TxnMarkerMinSectorSize {
		t.Fatalf(
			"filled marker cursor = %d, want %d",
			log.marker.Cursor(), storeio.TxnMarkerMinSectorSize,
		)
	}
	if log.marker.FitsRetirement() {
		t.Fatal("retirement unexpectedly fits full one-sector marker")
	}
	beforeEpoch := log.marker.Header().Epoch

	if err := db.DropCollection("b"); err != nil {
		if errors.Is(err, ErrCommitOutcomeUnknown) {
			t.Fatalf("definite retirement pressure became outcome-unknown: %v", err)
		}
		t.Fatalf("DropCollection(b): %v", err)
	}
	if log.poison != nil {
		t.Fatalf("successful pressure recycle poisoned transaction log: %v", log.poison)
	}
	if log.marker.Header().Epoch != beforeEpoch+1 ||
		log.marker.Cursor() != 0 || log.undischarged != 0 {
		t.Fatalf(
			"pressure fold marker = epoch %d cursor %d undischarged %d, want %d/0/0",
			log.marker.Header().Epoch, log.marker.Cursor(), log.undischarged,
			beforeEpoch+1,
		)
	}
	for _, name := range []string{"a", "c"} {
		collection, ok := db.Collection(name)
		if !ok {
			t.Fatalf("pressure drop lost collection %s", name)
		}
		if persistence := collection.PersistenceError(); persistence != nil {
			t.Fatalf("pressure drop poisoned %s: %v", name, persistence)
		}
	}

	dir := db.Dir()
	if err := db.Close(); err != nil {
		t.Fatalf("close after pressure recycle: %v", err)
	}
	closed = true
	reopened, err := OpenDatabase(
		dir, DatabaseOptions{Options: txnTestOptions()},
	)
	if err != nil {
		t.Fatalf("reopen after pressure recycle: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Update(func(batch *DatabaseBatch) error {
		a, err := batch.Collection("a")
		if err != nil {
			return err
		}
		c, err := batch.Collection("c")
		if err != nil {
			return err
		}
		if err := a.Put([]byte("after"), []byte(`{"n":2}`)); err != nil {
			return err
		}
		return c.Put([]byte("after"), []byte(`{"n":2}`))
	}); err != nil {
		t.Fatalf("commit after reopen: %v", err)
	}
}
