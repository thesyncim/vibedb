//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestVerifierSealedSidecarInspectionIsReadOnly(t *testing.T) {
	dir := t.TempDir()
	primaryName, ok := collectionname.Encode("sealed")
	if !ok {
		t.Fatal("encode fixture name")
	}
	journalPath := filepath.Join(dir, primaryName+collectionname.JournalSuffix)
	journalFile, err := os.OpenFile(journalPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	var storeID, journalID [16]byte
	storeID[0], journalID[0] = 1, 2
	journal, err := storeio.CreateRecoveryJournal(journalFile, storeio.RecoveryJournalHeader{
		FormatVersion: storeio.RecoveryJournalFormatConditional,
		StoreID:       storeID, JournalID: journalID,
		PageSize: 4096, SectorSize: storeio.RecoveryJournalMinSectorSize,
		BaseGeneration: 1,
		Capacity:       8 * storeio.RecoveryJournalMinSectorSize,
		SealedCapacity: true,
	})
	if err != nil {
		_ = journalFile.Close()
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			t.Skipf("filesystem cannot prove strict sidecar allocation: %v", err)
		}
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	marker, err := storeio.CreateTxnMarker(filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{
		Capacity: 8 * storeio.TxnMarkerMinSectorSize, SealedCapacity: true,
	})
	if err != nil {
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			t.Skipf("filesystem cannot prove strict sidecar allocation: %v", err)
		}
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}

	before := directoryDigest(t, dir)
	if _, _, err := scanJournalPairing(journalPath); err != nil {
		t.Fatalf("scan sealed recovery journal: %v", err)
	}
	if _, err := verifyDatabaseTxn(dir); err != nil {
		t.Fatalf("verify sealed decision log: %v", err)
	}
	if after := directoryDigest(t, dir); after != before {
		t.Fatalf("sealed verifier inspection mutated sidecars: before=%s after=%s", before, after)
	}
}
