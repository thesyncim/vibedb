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
	log, err := OpenTxnLog(dir, options)
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

	if _, _, err := RecoverDatabaseTransactions(dir, TxnLogOptions{}); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("unqualified sealed marker recovery = %v, want mismatch", err)
	}
	decisions, reopened, err := RecoverDatabaseTransactions(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if decisions == nil {
		t.Fatal("sealed marker recovery returned nil decisions")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := RecoverDatabaseTransactions(dir, TxnLogOptions{
		Capacity:       capacity + storeio.TxnMarkerMinSectorSize,
		SealedCapacity: true,
	}); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("wrong sealed marker recovery = %v, want mismatch", err)
	}
}
