//go:build linux

package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func installSealedSidecarAllocationFixture(t *testing.T) *int {
	t.Helper()
	production := linuxStrictAllocationOps
	t.Cleanup(func() { linuxStrictAllocationOps = production })
	calls := new(int)
	linuxStrictAllocationOps.fallocate = func(
		fd int, mode uint32, _ int64, length int64,
	) error {
		(*calls)++
		if mode == 0 {
			return unix.Ftruncate(fd, length)
		}
		if mode != unix.FALLOC_FL_UNSHARE_RANGE {
			t.Fatalf("unexpected fallocate mode %#x", mode)
		}
		return nil
	}
	return calls
}

func TestSealedRecoveryJournalExactOpenAndImmutableGrowth(t *testing.T) {
	calls := installSealedSidecarAllocationFixture(t)
	const capacity = uint64(8 * RecoveryJournalMinSectorSize)
	path := filepath.Join(t.TempDir(), "sealed.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	header := testJournalHeader(t, capacity)
	header.SealedCapacity = true
	journal, err := CreateRecoveryJournal(file, header)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Header().Capacity != capacity || !journal.Header().SealedCapacity {
		t.Fatalf("sealed recovery header = %+v", journal.Header())
	}
	if err := journal.GrowCapacity(capacity, true); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("sealed no-op GrowCapacity = %v, want mismatch", err)
	}
	if err := journal.Recycle(header.BaseGeneration+1, true); err != nil {
		t.Fatalf("sealed recovery recycle: %v", err)
	}
	if !journal.Header().SealedCapacity || journal.Header().Capacity != capacity {
		t.Fatalf("sealed recovery recycle changed profile: %+v", journal.Header())
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	generic, err := OpenRecoveryJournal(file)
	if err != nil {
		t.Fatalf("self-described sealed open: %v", err)
	}
	if !generic.Header().SealedCapacity || generic.Header().Capacity != capacity {
		t.Fatalf("reopened recycled recovery profile: %+v", generic.Header())
	}
	if err := generic.GrowCapacity(capacity+RecoveryJournalMinSectorSize, true); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("generic sealed handle GrowCapacity = %v, want mismatch", err)
	}
	if err := generic.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := InspectRecoveryJournal(file)
	if err != nil {
		t.Fatalf("read-only sealed inspection: %v", err)
	}
	if !inspection.Header().SealedCapacity || inspection.Header().Capacity != capacity {
		t.Fatalf("inspection header = %+v", inspection.Header())
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	before := *calls
	if _, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("explicit unsealed open = %v, want mismatch", err)
	}
	if *calls != before {
		t.Fatal("capacity mismatch reached allocation proof")
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
		SealedCapacityBytes: capacity + RecoveryJournalMinSectorSize,
	}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("wrong exact recovery capacity = %v, want mismatch", err)
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
		SealedCapacityBytes: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(recoveryJournalRegionStart + capacity)
	if err := file.Truncate(wantSize - 1); err != nil {
		t.Fatal(err)
	}
	before = *calls
	if _, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
		SealedCapacityBytes: capacity,
	}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("short sealed recovery journal = %v, want mismatch", err)
	}
	if *calls != before {
		t.Fatal("short EOF reached allocation repair")
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(wantSize + 1); err != nil {
		t.Fatal(err)
	}
	before = *calls
	if _, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
		SealedCapacityBytes: capacity,
	}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("long sealed recovery journal = %v, want mismatch", err)
	}
	if *calls != before {
		t.Fatal("long recovery EOF reached allocation repair")
	}
	_ = file.Close()
}

func TestSealedRecoveryJournalPairingPrecedesProofAndScan(t *testing.T) {
	calls := installSealedSidecarAllocationFixture(t)
	const capacity = uint64(8 * RecoveryJournalMinSectorSize)
	path := filepath.Join(t.TempDir(), "paired.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	header := testJournalHeader(t, capacity)
	header.SealedCapacity = true
	journal, err := CreateRecoveryJournal(file, header)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	productionScan := recoveryJournalScanTail
	t.Cleanup(func() { recoveryJournalScanTail = productionScan })
	scans := 0
	recoveryJournalScanTail = func(journal *RecoveryJournal) error {
		scans++
		return productionScan(journal)
	}
	for _, tc := range []struct {
		name string
		edit func(*RecoveryJournalPairing)
		want error
	}{
		{name: "store", edit: func(p *RecoveryJournalPairing) { p.StoreID[0] ^= 0xff }, want: ErrRecoveryJournalIdentity},
		{name: "journal", edit: func(p *RecoveryJournalPairing) { p.JournalID[0] ^= 0xff }, want: ErrRecoveryJournalIdentity},
		{name: "page", edit: func(p *RecoveryJournalPairing) { p.PageSize *= 2 }, want: ErrRecoveryJournalGeometry},
		{name: "epoch", edit: func(p *RecoveryJournalPairing) { p.RootGeneration = 0 }, want: ErrRecoveryJournalEpoch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			pairing := RecoveryJournalPairing{
				StoreID: header.StoreID, JournalID: header.JournalID,
				PageSize: header.PageSize, RootGeneration: header.BaseGeneration,
			}
			tc.edit(&pairing)
			beforeCalls, beforeScans := *calls, scans
			_, openErr := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
				SealedCapacityBytes: capacity, Pairing: &pairing,
			})
			_ = file.Close()
			if !errors.Is(openErr, tc.want) {
				t.Fatalf("mismatched paired open = %v, want %v", openErr, tc.want)
			}
			if *calls != beforeCalls || scans != beforeScans {
				t.Fatalf("pair mismatch reached proof/scan: calls %d->%d scans %d->%d", beforeCalls, *calls, beforeScans, scans)
			}
		})
	}
}

func TestSealedRecoveryJournalOpenSyncFailureRetriesProof(t *testing.T) {
	calls := installSealedSidecarAllocationFixture(t)
	const capacity = uint64(8 * RecoveryJournalMinSectorSize)
	path := filepath.Join(t.TempDir(), "retry.rjournal")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	header := testJournalHeader(t, capacity)
	header.SealedCapacity = true
	journal, err := CreateRecoveryJournal(file, header)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	productionSync := strictAllocationDataSync
	t.Cleanup(func() { strictAllocationDataSync = productionSync })
	syncFailure := errors.New("strict allocation fence failed")
	strictAllocationDataSync = func(*os.File) error { return syncFailure }
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	before := *calls
	if opened, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
		SealedCapacityBytes: capacity,
	}); !errors.Is(err, syncFailure) || opened != nil {
		t.Fatalf("failed strict fence open = (%v,%v), want nil/failure", opened, err)
	}
	_ = file.Close()
	firstProofCalls := *calls - before
	if firstProofCalls != 2 {
		t.Fatalf("first proof calls = %d, want mode-zero+unshare", firstProofCalls)
	}

	strictAllocationDataSync = productionSync
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{
		SealedCapacityBytes: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retryProofCalls := *calls - before - firstProofCalls; retryProofCalls != 2 {
		t.Fatalf("retry proof calls = %d, want absolute mode-zero+unshare", retryProofCalls)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSealedSidecarsRejectPostSyncEOFChange(t *testing.T) {
	installSealedSidecarAllocationFixture(t)
	productionSync := strictAllocationDataSync
	t.Cleanup(func() { strictAllocationDataSync = productionSync })

	t.Run("recovery-create", func(t *testing.T) {
		const capacity = uint64(8 * RecoveryJournalMinSectorSize)
		path := filepath.Join(t.TempDir(), "post-sync.rjournal")
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		total := int64(recoveryJournalRegionStart + capacity)
		strictAllocationDataSync = func(file *os.File) error {
			if err := productionSync(file); err != nil {
				return err
			}
			return file.Truncate(total - 1)
		}
		header := testJournalHeader(t, capacity)
		header.SealedCapacity = true
		if _, err := CreateRecoveryJournal(file, header); !errors.Is(err, ErrSealedCapacityMismatch) {
			t.Fatalf("post-sync recovery EOF change = %v, want mismatch", err)
		}
		strictAllocationDataSync = productionSync
	})

	t.Run("marker-open", func(t *testing.T) {
		const capacity = uint64(8 * TxnMarkerMinSectorSize)
		path := filepath.Join(t.TempDir(), "txn.vtm")
		marker, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: capacity, SealedCapacity: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := marker.Close(); err != nil {
			t.Fatal(err)
		}
		total := int64(txnMarkerRegionStart + capacity)
		strictAllocationDataSync = func(file *os.File) error {
			if err := productionSync(file); err != nil {
				return err
			}
			return file.Truncate(total + 1)
		}
		if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{Capacity: capacity, SealedCapacity: true}); !errors.Is(err, ErrSealedCapacityMismatch) {
			t.Fatalf("post-sync marker EOF change = %v, want mismatch", err)
		}
		strictAllocationDataSync = productionSync
	})
}

func TestSealedTxnMarkerRequiresExactOpenProfile(t *testing.T) {
	calls := installSealedSidecarAllocationFixture(t)
	const capacity = uint64(8 * TxnMarkerMinSectorSize)
	path := filepath.Join(t.TempDir(), "txn.vtm")
	marker, err := CreateTxnMarker(path, TxnMarkerOptions{
		Capacity: capacity, SealedCapacity: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !marker.Header().SealedCapacity || marker.Header().Capacity != capacity {
		t.Fatalf("sealed marker header = %+v", marker.Header())
	}
	if err := marker.Recycle(marker.Header().Epoch + 1); err != nil {
		t.Fatalf("sealed marker recycle: %v", err)
	}
	if !marker.Header().SealedCapacity || marker.Header().Capacity != capacity {
		t.Fatalf("sealed marker recycle changed profile: %+v", marker.Header())
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, decisions, err := InspectTxnMarker(path)
	if err != nil {
		t.Fatalf("read-only sealed marker inspection: %v", err)
	}
	if !inspection.Header().SealedCapacity || decisions == nil {
		t.Fatalf("sealed marker inspection = (%+v,%v)", inspection.Header(), decisions)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}

	before := *calls
	if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("zero-option sealed marker open = %v, want mismatch", err)
	}
	if *calls != before {
		t.Fatal("marker option mismatch reached allocation proof")
	}
	if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{
		Capacity: capacity + TxnMarkerMinSectorSize, SealedCapacity: true,
	}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("wrong sealed marker capacity = %v, want mismatch", err)
	}
	opened, _, err := OpenTxnMarker(path, TxnMarkerOptions{
		Capacity: capacity, SealedCapacity: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !opened.Header().SealedCapacity || opened.Header().Capacity != capacity {
		t.Fatalf("reopened recycled marker profile: %+v", opened.Header())
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(txnMarkerRegionStart + capacity)
	if err := file.Truncate(wantSize + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before = *calls
	if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{
		Capacity: capacity, SealedCapacity: true,
	}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("long sealed marker = %v, want mismatch", err)
	}
	if *calls != before {
		t.Fatal("long marker EOF reached allocation repair")
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(wantSize - 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before = *calls
	if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{
		Capacity: capacity, SealedCapacity: true,
	}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("short sealed marker = %v, want mismatch", err)
	}
	if *calls != before {
		t.Fatal("short marker EOF reached allocation repair")
	}
}
