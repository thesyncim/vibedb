package durable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func sealedSyncJournalOptions(t testing.TB) Options {
	t.Helper()
	options := syncPrimaryJournalTestOptions()
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	required := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		storeio.RecoveryJournalMinSectorSize,
		normalized.MaxBatchDocuments,
		normalized.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
	)
	options.SealedRecoveryJournalBytes = uint64(required)
	return options
}

func TestSealedRecoveryJournalOptionRequiresCompleteConditionalRecord(t *testing.T) {
	valid := sealedSyncJournalOptions(t)
	if _, err := valid.normalized(); err != nil {
		t.Fatal(err)
	}
	short := valid
	short.SealedRecoveryJournalBytes -= storeio.RecoveryJournalMinSectorSize
	if _, err := short.normalized(); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("one-sector-short sealed journal = %v, want %v", err, ErrSealedJournalCapacity)
	}
	wrongLane := valid
	wrongLane.Durability = DurabilityBufferedVisible
	if _, err := wrongLane.normalized(); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("buffered sealed journal = %v, want %v", err, ErrSealedJournalCapacity)
	}
}

func TestSealedRecoveryJournalAdmitsReplicatedSQLCeiling(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.MaxBatchDocuments = 64
	options.MaxBatchBytes = (16 << 20) + options.MaxBatchDocuments*256
	options.SealedRecoveryJournalBytes = storeio.RecoveryJournalMaxCapacityBytes
	if _, err := options.normalized(); err != nil {
		t.Fatalf("replicated SQL ceiling profile: %v", err)
	}

	short := options
	short.SealedRecoveryJournalBytes -= storeio.RecoveryJournalMinSectorSize
	if _, err := short.normalized(); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("one-sector-short ceiling profile = %v, want %v", err, ErrSealedJournalCapacity)
	}
}

func TestSealedRecoveryJournalOpenRejectsRootWithoutJournalIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "async.vjc")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	async := syncPrimaryJournalTestOptions()
	async.Durability = DurabilityAsyncVisible
	collection, err := Create(file, async)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file, sealedSyncJournalOptions(t)); !errors.Is(err, ErrSealedJournalCapacity) {
		t.Fatalf("sealed open of unrooted async collection = %v, want mismatch", err)
	}
	if _, err := os.Stat(RecoveryJournalPath(path)); !os.IsNotExist(err) {
		t.Fatalf("failed exact open created a recovery journal: %v", err)
	}
}

func TestSealedTxnLogOptionsRejectAbsentMarkerEagerly(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts TxnLogOptions
	}{
		{name: "zero", opts: TxnLogOptions{SealedCapacity: true}},
		{name: "unaligned", opts: TxnLogOptions{Capacity: storeio.TxnMarkerMinSectorSize + 1, SealedCapacity: true}},
		{name: "too-large", opts: TxnLogOptions{Capacity: storeio.TxnMarkerMaxCapacityBytes + storeio.TxnMarkerMinSectorSize, SealedCapacity: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if log, err := NewTxnLog(dir, tc.opts); !errors.Is(err, ErrSealedJournalCapacity) {
				if log != nil {
					_ = log.Close()
				}
				t.Fatalf("NewTxnLog invalid absent profile = %v, want mismatch", err)
			}
			if _, log, err := OpenCollectionsWithTransactions(
				dir, tc.opts, nil,
			); !errors.Is(err, ErrSealedJournalCapacity) {
				if log != nil {
					_ = log.Close()
				}
				t.Fatalf("OpenCollectionsWithTransactions invalid absent profile = %v, want mismatch", err)
			}
			if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
				t.Fatalf("invalid strict options minted txn.vtm: %v", err)
			}
		})
	}
}
