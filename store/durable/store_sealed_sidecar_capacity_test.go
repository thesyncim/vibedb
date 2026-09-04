//go:build linux

package durable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func requireSealedSidecarEnvironment(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		t.Skipf("filesystem cannot prove strict sidecar allocation: %v", err)
	}
	t.Fatal(err)
}

func TestSealedRecoveryJournalCreateAndExactOpen(t *testing.T) {
	options := sealedSyncJournalOptions(t)
	path := filepath.Join(t.TempDir(), "sealed.vjc")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Create(file, options)
	if err != nil {
		requireSealedSidecarEnvironment(t, err)
	}
	if got := collection.SealedRecoveryJournalBytes(); got != options.SealedRecoveryJournalBytes {
		t.Fatalf("sealed journal bytes = %d, want %d", got, options.SealedRecoveryJournalBytes)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file, syncPrimaryJournalTestOptions()); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("unqualified sealed collection open = %v, want mismatch", err)
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
}

func TestSealedTxnLogCreateRecoveryAndExactMismatch(t *testing.T) {
	const capacity = uint64(64 * storeio.TxnMarkerMinSectorSize)
	dir := t.TempDir()
	options := TxnLogOptions{Capacity: capacity, SealedCapacity: true}
	log, err := NewTxnLog(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	log.commitMu.Lock()
	err = log.ensureMintedLocked()
	log.commitMu.Unlock()
	if err != nil {
		requireSealedSidecarEnvironment(t, err)
	}
	if !log.marker.Header().SealedCapacity || log.marker.Header().Capacity != capacity {
		t.Fatalf("sealed marker = %+v", log.marker.Header())
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := OpenCollectionsWithTransactions(
		dir, TxnLogOptions{}, nil,
	); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("unqualified sealed marker recovery = %v, want mismatch", err)
	}
	collections, reopened, err := OpenCollectionsWithTransactions(
		dir, options, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(collections) != 0 || reopened == nil {
		t.Fatalf("sealed empty-catalog recovery = %d collections, log %p",
			len(collections), reopened)
	}
	// A sealed marker is part of the exact physical profile. Empty-catalog
	// reconciliation retains the already-qualified file instead of unlinking it
	// and forcing an identical remint on every open.
	if reopened.marker == nil || !reopened.marker.Header().SealedCapacity ||
		reopened.marker.Header().Capacity != capacity {
		t.Fatalf("recovered sealed marker = %v", reopened.marker)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenCollectionsWithTransactions(dir, TxnLogOptions{
		Capacity:       capacity + storeio.TxnMarkerMinSectorSize,
		SealedCapacity: true,
	}, nil); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("wrong sealed marker recovery = %v, want mismatch", err)
	}
}

func TestSealedTxnLogRetainsDischargedWindowUntilPressureRecycle(t *testing.T) {
	const capacity = uint64(64 * storeio.TxnMarkerMinSectorSize)
	dir := t.TempDir()
	options := TxnLogOptions{Capacity: capacity, SealedCapacity: true}
	log, err := NewTxnLog(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.EnsureMinted(); err != nil {
		requireSealedSidecarEnvironment(t, err)
	}
	var storeID, journalID [16]byte
	storeID[0], journalID[0] = 1, 2
	if _, err := log.marker.AppendDecision(1, []storeio.TxnCollectionRef{{
		StoreID: storeID, JournalID: journalID, PreparedGeneration: 2,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.marker.AppendRetirement(storeID); err != nil {
		t.Fatal(err)
	}
	if err := log.marker.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	collections, reopened, err := OpenCollectionsWithTransactions(dir, options, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(collections) != 0 || reopened.marker == nil ||
		reopened.marker.Cursor() == 0 || reopened.undischarged != 0 {
		t.Fatalf(
			"discharged sealed recovery = collections %d marker %p cursor %d undischarged %d",
			len(collections), reopened.marker, reopened.marker.Cursor(), reopened.undischarged,
		)
	}
	beforeEpoch := reopened.marker.Header().Epoch
	reopened.commitMu.Lock()
	err = reopened.foldLaggardsAndRecycleLocked()
	reopened.commitMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if reopened.marker.Cursor() != 0 ||
		reopened.marker.Header().Epoch != beforeEpoch+1 {
		t.Fatalf(
			"pressure recycle = epoch %d cursor %d, want %d/0",
			reopened.marker.Header().Epoch, reopened.marker.Cursor(), beforeEpoch+1,
		)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
