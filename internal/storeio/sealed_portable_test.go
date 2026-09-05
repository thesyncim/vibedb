package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPortableSealedHeadersRequireExplicitMode(t *testing.T) {
	journal := testJournalHeader(t, 4096)
	journal.SealedCapacity, journal.PortableCapacity, journal.RecycleCount = true, true, 1
	buf := make([]byte, RecoveryJournalHeaderSize)
	if _, err := EncodeRecoveryJournalHeader(buf, journal); err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeRecoveryJournalHeader(buf); err != nil || got != journal {
		t.Fatalf("portable journal header = %+v, %v", got, err)
	}
	if flags := binary.LittleEndian.Uint32(buf[88:92]); flags != 3 {
		t.Fatalf("flags = %d", flags)
	}
	// A checksummed portable bit without a sealed bound must be rejected.
	binary.LittleEndian.PutUint32(buf[88:92], recoveryJournalFlagPortableCapacity)
	resealSidecarHeader(buf)
	if _, err := DecodeRecoveryJournalHeader(buf); !errors.Is(err, ErrRecoveryJournalCorrupt) {
		t.Fatalf("unsealed portable journal = %v", err)
	}
	marker := TxnMarkerHeader{Format: TxnMarkerFormat, MarkerID: [16]byte{1}, Epoch: 1,
		Capacity: 4096, SealedCapacity: true, PortableCapacity: true, RecycleCount: 1}
	if _, err := EncodeTxnMarkerHeader(buf, marker); err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeTxnMarkerHeader(buf); err != nil || got != marker {
		t.Fatalf("portable marker header = %+v, %v", got, err)
	}
	binary.LittleEndian.PutUint32(buf[64:68], txnMarkerFlagPortableCapacity)
	resealSidecarHeader(buf)
	if _, err := DecodeTxnMarkerHeader(buf); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("unsealed portable marker = %v", err)
	}
}

func TestPortableAllocationPreservesLiveBytesAndOffset(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "allocation")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	want := bytes.Repeat([]byte{0x9d, 0x41, 0x00}, 1000)
	if _, err := file.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	const target = 32 << 10
	for _, size := range []int64{target, target, 4096} {
		if err := allocateSealedFile(file, size, true); err != nil {
			t.Fatal(err)
		}
	}
	if pos, err := file.Seek(0, io.SeekCurrent); err != nil || pos != int64(len(want)) {
		t.Fatalf("offset = %d, %v", pos, err)
	}
	got, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != target || !bytes.Equal(got[:len(want)], want) || !allZero(got[len(want):]) {
		t.Fatal("allocation changed live bytes, file size, or zero extension")
	}
}

func TestPortableSealedRecoveryJournalReopenAndFailures(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "portable-journal")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	header := testJournalHeader(t, 4096)
	header.SealedCapacity, header.PortableCapacity = true, true
	journal, err := CreateRecoveryJournal(file, header)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	appendPut(t, journal, 2, "key", "value")
	if err := journal.Sync(true); err != nil {
		t.Fatal(err)
	}
	before := journal.Cursor()
	write := journal.writeAt
	journal.writeAt = func([]byte, int64) (int, error) { return 0, syscall.ENOSPC }
	if seq, err := journal.Append(RecoveryRecordKindPut, 3, []byte("later"), []byte("value")); seq != 0 || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("disk-full append = %d, %v", seq, err)
	}
	if journal.Cursor() != before {
		t.Fatal("failed append advanced journal")
	}
	journal.writeAt = write
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{SealedCapacityBytes: 4096}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("strict reader accepted portable journal: %v", err)
	}
	_ = file.Close()
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	journal, err = OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{SealedCapacityBytes: 4096, AllowPortableCapacity: true})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	records := replayAll(t, journal, 1)
	if len(records) != 1 || string(records[0].Key) != "key" || string(records[0].Value) != "value" {
		t.Fatalf("replay = %+v", records)
	}
	if err := journal.GrowCapacity(8192, true); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("portable journal grew: %v", err)
	}
	if err := journal.Recycle(2, true); err != nil {
		t.Fatal(err)
	}
	if !journal.Header().PortableCapacity {
		t.Fatal("recycle lost portable mode")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(int64(recoveryJournalRegionStart + 4096 - 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{SealedCapacityBytes: 4096, AllowPortableCapacity: true}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("short portable file repaired: %v", err)
	}
}

func TestPortableSealedTxnMarkerReopenAndFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "portable.vtm")
	options := TxnMarkerOptions{Capacity: 4096, SealedCapacity: true, PortableCapacity: true}
	marker, err := CreateTxnMarker(path, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := marker.AppendDecision(1, testTxnCollectionRefs(1)); err != nil {
		t.Fatal(err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatal(err)
	}
	write := marker.writeAt
	marker.writeAt = func([]byte, int64) (int, error) { return 0, syscall.ENOSPC }
	if seq, err := marker.AppendDecision(2, testTxnCollectionRefs(1)); seq != 0 || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("disk-full decision = %d, %v", seq, err)
	}
	marker.writeAt = write
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{Capacity: 4096, SealedCapacity: true}); !errors.Is(err, ErrSealedCapacityMismatch) {
		t.Fatalf("strict reader accepted portable marker: %v", err)
	}
	marker, decisions, err := OpenTxnMarker(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer marker.Close()
	if decisions.MaxTxnID() != 1 || !marker.Header().PortableCapacity {
		t.Fatalf("recovered decision=%d, header=%+v", decisions.MaxTxnID(), marker.Header())
	}
	if err := marker.Recycle(marker.Header().Epoch + 1); err != nil {
		t.Fatal(err)
	}
	if !marker.Header().PortableCapacity {
		t.Fatal("recycle lost portable mode")
	}
}

func TestPortableReopenNeedsNoAllocationSyncButWritesStillSync(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "journal")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	header := testJournalHeader(t, 4096)
	header.SealedCapacity, header.PortableCapacity = true, true
	journal, err := CreateRecoveryJournal(file, header)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	markerPath := filepath.Join(t.TempDir(), "txn.vtm")
	options := TxnMarkerOptions{Capacity: 4096, SealedCapacity: true, PortableCapacity: true}
	marker, err := CreateTxnMarker(markerPath, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	productionSync := strictAllocationDataSync
	t.Cleanup(func() { strictAllocationDataSync = productionSync })
	strictAllocationDataSync = func(*os.File) error {
		t.Error("read-only portable reopen performed allocation sync")
		return syscall.EIO
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	journal, err = OpenRecoveryJournalWithOptions(file, RecoveryJournalOpenOptions{SealedCapacityBytes: 4096, AllowPortableCapacity: true})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer journal.Close()
	marker, _, err = OpenTxnMarker(markerPath, options)
	if err != nil {
		t.Fatal(err)
	}
	defer marker.Close()
	journal.journalDataSync = func(*os.File) error { return syscall.EIO }
	marker.markerSync = func(*os.File) error { return syscall.EIO }
	if err := journal.Sync(true); !errors.Is(err, syscall.EIO) {
		t.Fatalf("portable journal swallowed failed write fence: %v", err)
	}
	if err := marker.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("portable decision log swallowed failed commit fence: %v", err)
	}
}
