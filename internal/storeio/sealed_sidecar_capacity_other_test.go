//go:build !linux

package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStrictAllocationAndSealedSidecarsFailClosedOffLinux(t *testing.T) {
	t.Run("allocator", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "strict-allocation-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := StrictlyAllocateFile(file, 4096); !errors.Is(err, ErrStrictAllocationUnsupported) {
			t.Fatalf("strict allocation = %v, want unsupported", err)
		}
		if info, err := file.Stat(); err != nil || info.Size() != 0 {
			t.Fatalf("unsupported strict allocation changed EOF: info=%v err=%v", info, err)
		}
	})

	t.Run("recovery-journal", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "sealed-recovery-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		header := testJournalHeader(t, 8*RecoveryJournalMinSectorSize)
		header.SealedCapacity = true
		if _, err := CreateRecoveryJournal(file, header); !errors.Is(err, ErrStrictAllocationUnsupported) {
			t.Fatalf("sealed recovery create = %v, want unsupported", err)
		}
		if info, err := file.Stat(); err != nil || info.Size() != 0 {
			t.Fatalf("unsupported sealed recovery create published bytes: info=%v err=%v", info, err)
		}
	})

	t.Run("transaction-marker", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "txn.vtm")
		if _, err := CreateTxnMarker(path, TxnMarkerOptions{
			Capacity: 8 * TxnMarkerMinSectorSize, SealedCapacity: true,
		}); !errors.Is(err, ErrStrictAllocationUnsupported) {
			t.Fatalf("sealed marker create = %v, want unsupported", err)
		}
		info, err := os.Stat(path)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err == nil && info.Size() != 0 {
			t.Fatalf("unsupported sealed marker create published %d bytes", info.Size())
		}
	})
}
