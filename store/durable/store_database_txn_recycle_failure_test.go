package durable

import (
	"errors"
	"syscall"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestTxnLogRecycleSyncFailurePoisonsCatalog(t *testing.T) {
	db := newTxnTestDatabase(t, "a", "b", "catalog-only")
	if err := db.Update(func(batch *DatabaseBatch) error {
		return putTxnPair(t, batch, "a", "b")
	}); err != nil {
		t.Fatalf("seed committed decision: %v", err)
	}

	log := lookupDatabaseTxnLog(db)
	if log == nil || log.marker == nil {
		t.Fatal("seed commit did not mint a transaction marker")
	}
	if log.undischarged != 1 {
		t.Fatalf("seed undischarged = %d, want 1", log.undischarged)
	}
	beforeHeader := log.marker.Header()

	var fault *storeio.FaultTxnMarker
	hookCalls := 0
	previousHook := databaseTxnBeforeMarkerRecycleHook
	databaseTxnBeforeMarkerRecycleHook = func(current *TxnLog) {
		hookCalls++
		if current != log {
			return
		}
		fault = storeio.NewFaultTxnMarker(current.marker)
		fault.Program(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultSyncError,
		})
	}
	t.Cleanup(func() { databaseTxnBeforeMarkerRecycleHook = previousHook })

	log.commitMu.Lock()
	recycleErr := func() error {
		defer log.commitMu.Unlock()
		return log.foldLaggardsAndRecycleLocked()
	}()
	databaseTxnBeforeMarkerRecycleHook = previousHook

	if !errors.Is(recycleErr, ErrCommitOutcomeUnknown) ||
		!errors.Is(recycleErr, syscall.EIO) {
		t.Fatalf("recycle failure = %v, want outcome-unknown EIO", recycleErr)
	}
	faulted, syncs := false, 0
	if fault != nil {
		faulted = fault.Faulted()
		syncs = fault.Syncs()
	}
	if hookCalls != 1 || !faulted || syncs != 1 {
		t.Fatalf(
			"recycle fault hook calls/faulted/syncs = %d/%v/%d, want 1/true/1",
			hookCalls, faulted, syncs,
		)
	}
	if log.marker.Header() != beforeHeader {
		t.Fatalf(
			"failed recycle advanced live marker header = %+v, want %+v",
			log.marker.Header(), beforeHeader,
		)
	}
	if log.undischarged != 1 {
		t.Fatalf("failed recycle cleared undischarged = %d, want 1", log.undischarged)
	}
	if !errors.Is(log.poison, ErrCommitOutcomeUnknown) {
		t.Fatalf("transaction log poison = %v, want outcome unknown", log.poison)
	}

	for _, name := range []string{"a", "b", "catalog-only"} {
		collection, ok := db.Collection(name)
		if !ok {
			t.Fatalf("missing registered collection %q", name)
		}
		if persistence := collection.PersistenceError(); !errors.Is(
			persistence, ErrCommitOutcomeUnknown,
		) || !errors.Is(persistence, syscall.EIO) {
			t.Fatalf(
				"%s persistence poison = %v, want outcome-unknown EIO",
				name, persistence,
			)
		}
	}

	// Capture before live-handle teardown. The recycle header write completed
	// before the injected fence error, so recovery may legally select either
	// the old header or the newly written header after a real crash.
	crashImage := cloneDatabaseDir(t, db.Dir())

	if err := db.Update(func(batch *DatabaseBatch) error {
		return putTxnPair(t, batch, "a", "b")
	}); !errors.Is(err, ErrTxnLogPoisoned) {
		t.Fatalf("later transaction = %v, want ErrTxnLogPoisoned", err)
	}
	target, _ := db.Collection("catalog-only")
	if err := log.DetachCollection(target); !errors.Is(err, ErrTxnLogPoisoned) {
		t.Fatalf("later DetachCollection = %v, want ErrTxnLogPoisoned", err)
	}
	if log.undischarged != 1 {
		t.Fatalf("later rejects changed undischarged = %d, want 1", log.undischarged)
	}

	assertReopenOutcome(
		t, crashImage, []string{"a", "b"}, true, `{"n":1}`,
	)
}
