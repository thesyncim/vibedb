package storeio

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func testTxnParticipants(n int) []TxnParticipant {
	out := make([]TxnParticipant, n)
	for i := range out {
		for j := range out[i].StoreID {
			out[i].StoreID[j] = byte(i + 1)
			out[i].JournalID[j] = byte(0x40 + i)
		}
		out[i].PreparedGeneration = uint64(i + 2)
	}
	return out
}

func createTestTxnMarker(t testing.TB, capacity uint64) (*TxnMarker, string) {
	t.Helper()
	ProgramTxnMarkerCreateFault(TxnMarkerFaultPlan{})
	path := filepath.Join(t.TempDir(), "txn.vtm")
	m, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: capacity})
	if err != nil {
		t.Fatalf("CreateTxnMarker: %v", err)
	}
	return m, path
}

func reopenTestTxnMarker(t testing.TB, path string) (*TxnMarker, *TxnDecisions) {
	t.Helper()
	m, decisions, err := OpenTxnMarker(path, TxnMarkerOptions{})
	if err != nil {
		t.Fatalf("OpenTxnMarker: %v", err)
	}
	return m, decisions
}

func TestTxnMarkerHeaderRoundTrip(t *testing.T) {
	var markerID [16]byte
	for i := range markerID {
		markerID[i] = byte(i + 1)
	}
	h := TxnMarkerHeader{
		FormatVersion: TxnMarkerFormatVersion,
		MarkerID:      markerID,
		Epoch:         3,
		BaseSequence:  7,
		Capacity:      8 * TxnMarkerMinSectorSize,
		RecycleCount:  2,
	}
	buf := make([]byte, TxnMarkerHeaderSize)
	if _, err := EncodeTxnMarkerHeader(buf, h); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeTxnMarkerHeader(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != h {
		t.Fatalf("round trip mismatch: %+v != %+v", got, h)
	}
	for _, mut := range []struct {
		name  string
		apply func([]byte)
	}{
		{"magic", func(b []byte) { b[0] ^= 0xff }},
		{"version", func(b []byte) { b[8] ^= 0xff }},
		{"checksum", func(b []byte) { b[TxnMarkerHeaderSize-8] ^= 0x01 }},
		{"identity", func(b []byte) { b[16] ^= 0xff }},
		{"recyclecount", func(b []byte) {
			for i := 56; i < 64; i++ {
				b[i] = 0
			}
			sum := PageChecksum(b[:TxnMarkerHeaderSize-8])
			binary.LittleEndian.PutUint32(b[TxnMarkerHeaderSize-8:TxnMarkerHeaderSize-4], sum)
			binary.LittleEndian.PutUint32(b[TxnMarkerHeaderSize-4:], ^sum)
		}},
	} {
		corrupt := append([]byte(nil), buf...)
		mut.apply(corrupt)
		if _, err := DecodeTxnMarkerHeader(corrupt); err == nil {
			t.Fatalf("%s corruption accepted", mut.name)
		}
	}
}

func TestTxnMarkerCreateOpenEmpty(t *testing.T) {
	m, path := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
	header := m.Header()
	if header.FormatVersion != TxnMarkerFormatVersion ||
		header.Epoch != 1 || header.RecycleCount != 1 ||
		header.MarkerID == ([16]byte{}) {
		t.Fatalf("created header = %+v", header)
	}
	if m.NextSequence() != 1 || m.Cursor() != 0 {
		t.Fatalf("cursor/sequence = %d/%d, want 0/1", m.Cursor(), m.NextSequence())
	}
	raw := make([]byte, TxnMarkerHeaderSize)
	for slot := 0; slot < txnMarkerHeaderSlots; slot++ {
		if _, err := m.file.ReadAt(raw, int64(slot)*TxnMarkerHeaderSize); err != nil {
			t.Fatalf("read slot %d: %v", slot, err)
		}
		decoded, err := DecodeTxnMarkerHeader(raw)
		if err != nil {
			t.Fatalf("slot %d decode: %v", slot, err)
		}
		if decoded != header {
			t.Fatalf("slot %d header = %+v, want %+v", slot, decoded, header)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, decisions := reopenTestTxnMarker(t, path)
	defer m.Close()
	if m.Header() != header {
		t.Fatalf("reopened header = %+v, want %+v", m.Header(), header)
	}
	if decisions.MaxTxnID() != 0 || decisions.MaxDCSN() != 0 {
		t.Fatalf("empty decisions max txn/dcsn = %d/%d", decisions.MaxTxnID(), decisions.MaxDCSN())
	}
	if _, ok := decisions.Lookup(header.MarkerID, header.Epoch, 1); ok {
		t.Fatal("empty log reported a committed txn")
	}
}

func TestTxnMarkerDecisionRetirementRoundTrip(t *testing.T) {
	m, path := createTestTxnMarker(t, 256*TxnMarkerMinSectorSize)
	participants := testTxnParticipants(3)
	dcsn, err := m.AppendDecision(11, participants)
	if err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	if dcsn != 1 {
		t.Fatalf("dcsn = %d, want 1", dcsn)
	}
	var retired [16]byte
	retired[0] = 0xab
	retDCSN, err := m.AppendRetirement(retired)
	if err != nil {
		t.Fatalf("AppendRetirement: %v", err)
	}
	if retDCSN != 2 {
		t.Fatalf("retirement dcsn = %d, want 2", retDCSN)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	header := m.Header()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, decisions := reopenTestTxnMarker(t, path)
	defer m.Close()
	got, ok := decisions.Lookup(header.MarkerID, header.Epoch, 11)
	if !ok {
		t.Fatal("decision missing after reopen")
	}
	if len(got) != len(participants) {
		t.Fatalf("participants = %d, want %d", len(got), len(participants))
	}
	for i := range participants {
		if got[i] != participants[i] {
			t.Fatalf("participant %d = %+v, want %+v", i, got[i], participants[i])
		}
	}
	if !decisions.Retired(retired) {
		t.Fatal("retirement missing after reopen")
	}
	if decisions.MaxTxnID() != 11 || decisions.MaxDCSN() != 2 {
		t.Fatalf("max txn/dcsn = %d/%d, want 11/2", decisions.MaxTxnID(), decisions.MaxDCSN())
	}
	if m.NextSequence() != 3 {
		t.Fatalf("next sequence = %d, want 3", m.NextSequence())
	}
	dcsn2, err := m.AppendDecision(12, testTxnParticipants(1))
	if err != nil {
		t.Fatalf("second AppendDecision: %v", err)
	}
	if dcsn2 != 3 {
		t.Fatalf("second dcsn = %d, want 3", dcsn2)
	}
}

func TestTxnMarkerSequenceRejection(t *testing.T) {
	participants := testTxnParticipants(1)
	buf := make([]byte, 2*TxnMarkerMinSectorSize)
	n, err := encodeTxnDecisionRecord(buf, 5, 1, participants)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, _, err := decodeTxnMarkerRecord(buf[:n], 5); err != nil {
		t.Fatalf("exact sequence: %v", err)
	}
	if _, _, err := decodeTxnMarkerRecord(buf[:n], 4); !errors.Is(err, errTxnMarkerTruncatableTail) {
		t.Fatalf("decreasing sequence = %v, want truncatable", err)
	}
	if _, _, err := decodeTxnMarkerRecord(buf[:n], 6); !errors.Is(err, errTxnMarkerTruncatableTail) {
		t.Fatalf("duplicate/skip sequence = %v, want truncatable", err)
	}
}

func TestTxnMarkerRecycleSelectsGreaterRecycleCount(t *testing.T) {
	m, path := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
	if _, err := m.AppendDecision(1, testTxnParticipants(1)); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	beforeID := m.Header().MarkerID
	if err := m.Recycle(2); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if m.Header().Epoch != 2 || m.Header().RecycleCount != 2 ||
		m.Header().MarkerID != beforeID || m.Cursor() != 0 {
		t.Fatalf("recycled header = %+v cursor=%d", m.Header(), m.Cursor())
	}
	if _, err := m.AppendDecision(9, testTxnParticipants(1)); err != nil {
		t.Fatalf("post-recycle AppendDecision: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, decisions := reopenTestTxnMarker(t, path)
	defer m.Close()
	if m.Header().Epoch != 2 || m.Header().RecycleCount != 2 {
		t.Fatalf("reopened header = %+v", m.Header())
	}
	if _, ok := decisions.Lookup(beforeID, 1, 1); ok {
		t.Fatal("pre-recycle decision still visible")
	}
	if _, ok := decisions.Lookup(beforeID, 2, 9); !ok {
		t.Fatal("post-recycle decision missing")
	}
}

func TestTxnMarkerTornRecycleFallsBack(t *testing.T) {
	m, path := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
	fm := NewFaultTxnMarker(m)
	if _, err := m.AppendDecision(1, testTxnParticipants(1)); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	fm.Program(TxnMarkerFaultPlan{Phase: TxnMarkerFaultTornRecycle})
	if err := m.Recycle(2); err == nil {
		t.Fatal("torn recycle returned nil")
	}
	if !fm.Faulted() {
		t.Fatal("torn recycle fault did not fire")
	}
	if m.Header().Epoch != 1 || m.Header().RecycleCount != 1 {
		t.Fatalf("failed recycle half-applied: %+v", m.Header())
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, decisions := reopenTestTxnMarker(t, path)
	defer m.Close()
	if m.Header().Epoch != 1 || m.Header().RecycleCount != 1 {
		t.Fatalf("fallback header = %+v", m.Header())
	}
	if _, ok := decisions.Lookup(m.Header().MarkerID, 1, 1); !ok {
		t.Fatal("fallback lost the pre-recycle decision")
	}
}

func TestTxnMarkerTornTailBytePrefixSweep(t *testing.T) {
	m, path := createTestTxnMarker(t, 256*TxnMarkerMinSectorSize)
	if _, err := m.AppendDecision(1, testTxnParticipants(2)); err != nil {
		t.Fatalf("first AppendDecision: %v", err)
	}
	if _, err := m.AppendDecision(2, testTxnParticipants(3)); err != nil {
		t.Fatalf("second AppendDecision: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for size := txnMarkerRegionStart; size <= int(info.Size()); size++ {
		prefixPath := filepath.Join(t.TempDir(), "prefix.vtm")
		if err := os.WriteFile(prefixPath, full[:size], 0o600); err != nil {
			t.Fatalf("write prefix %d: %v", size, err)
		}
		// Re-extend with zeros: preallocated padding is already zero, so a
		// prefix that includes the CRC-covered body is a complete record.
		if err := os.Truncate(prefixPath, info.Size()); err != nil {
			t.Fatalf("truncate prefix %d: %v", size, err)
		}
		opened, decisions, err := OpenTxnMarker(prefixPath, TxnMarkerOptions{})
		if err != nil {
			if errors.Is(err, ErrTxnMarkerNoValidHeader) ||
				errors.Is(err, ErrTxnMarkerCorrupt) ||
				errors.Is(err, ErrTxnMarkerRecord) {
				continue
			}
			t.Fatalf("prefix %d: unexpected open error %v", size, err)
		}
		if decisions.MaxDCSN() > 2 {
			opened.Close()
			t.Fatalf("prefix %d decoded past durable end: max dcsn=%d", size, decisions.MaxDCSN())
		}
		if _, ok := decisions.Lookup(opened.Header().MarkerID, opened.Header().Epoch, 2); ok {
			firstPadded, ok := checkedTxnDecisionPaddedSize(2)
			if !ok {
				opened.Close()
				t.Fatal("first decision size")
			}
			secondLogical := TxnMarkerRecordPrefixSize +
				3*TxnParticipantSize + TxnMarkerRecordTrailerSize
			need := txnMarkerRegionStart + firstPadded + secondLogical
			if size < need {
				opened.Close()
				t.Fatalf("prefix %d decoded second decision before its bytes landed", size)
			}
		}
		opened.Close()
	}
}

func TestTxnMarkerHostileCapacityClamp(t *testing.T) {
	m, path := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	header := m.Header()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	hostile := header
	hostile.Capacity = TxnMarkerMaxCapacityBytes + uint64(TxnMarkerMinSectorSize)
	buf := make([]byte, TxnMarkerHeaderSize)
	clear(buf)
	copy(buf[0:8], txnMarkerMagic)
	binary.LittleEndian.PutUint32(buf[8:12], hostile.FormatVersion)
	binary.LittleEndian.PutUint32(buf[12:16], TxnMarkerHeaderSize)
	copy(buf[16:32], hostile.MarkerID[:])
	binary.LittleEndian.PutUint64(buf[32:40], hostile.Epoch)
	binary.LittleEndian.PutUint64(buf[40:48], hostile.BaseSequence)
	binary.LittleEndian.PutUint64(buf[48:56], hostile.Capacity)
	binary.LittleEndian.PutUint64(buf[56:64], hostile.RecycleCount)
	sum := PageChecksum(buf[:TxnMarkerHeaderSize-8])
	binary.LittleEndian.PutUint32(buf[TxnMarkerHeaderSize-8:TxnMarkerHeaderSize-4], sum)
	binary.LittleEndian.PutUint32(buf[TxnMarkerHeaderSize-4:], ^sum)
	for slot := 0; slot < txnMarkerHeaderSlots; slot++ {
		if _, err := file.WriteAt(buf, int64(slot)*TxnMarkerHeaderSize); err != nil {
			t.Fatalf("write hostile header: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := DecodeTxnMarkerHeader(buf); !errors.Is(err, ErrTxnMarkerCorrupt) {
		t.Fatalf("decode hostile capacity = %v, want corrupt", err)
	}
	if _, _, err := OpenTxnMarker(path, TxnMarkerOptions{}); !errors.Is(err, ErrTxnMarkerNoValidHeader) {
		t.Fatalf("Open hostile capacity = %v, want no-valid-header", err)
	}
}

func TestTxnMarkerCapacityClampIsIndependent(t *testing.T) {
	if got, want := TxnMarkerMaxCapacityBytes, uint64(16)<<20; got != want {
		t.Fatalf("transaction marker capacity clamp = %d, want %d", got, want)
	}
	if TxnMarkerMaxCapacityBytes >= RecoveryJournalMaxCapacityBytes {
		t.Fatalf(
			"transaction marker clamp %d inherited recovery-journal clamp %d",
			TxnMarkerMaxCapacityBytes, RecoveryJournalMaxCapacityBytes,
		)
	}
}

func TestTxnMarkerFaultSeamPhases(t *testing.T) {
	t.Run("torn-append", func(t *testing.T) {
		m, path := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
		fm := NewFaultTxnMarker(m)
		if _, err := m.AppendDecision(1, testTxnParticipants(1)); err != nil {
			t.Fatalf("clean append: %v", err)
		}
		if err := m.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		fm.Program(TxnMarkerFaultPlan{Phase: TxnMarkerFaultTornAppend, AppendIndex: 1})
		if _, err := m.AppendDecision(2, testTxnParticipants(1)); err != nil {
			t.Fatalf("torn append returned error: %v", err)
		}
		if !fm.Faulted() {
			t.Fatal("torn append fault did not fire")
		}
		if err := m.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		m, decisions := reopenTestTxnMarker(t, path)
		defer m.Close()
		if decisions.MaxDCSN() != 1 {
			t.Fatalf("after torn append max dcsn = %d, want 1", decisions.MaxDCSN())
		}
		if m.NextSequence() != 2 {
			t.Fatalf("next sequence = %d, want 2", m.NextSequence())
		}
	})

	t.Run("append-error", func(t *testing.T) {
		m, _ := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
		defer m.Close()
		fm := NewFaultTxnMarker(m)
		fm.Program(TxnMarkerFaultPlan{Phase: TxnMarkerFaultAppendError, AppendIndex: 0})
		before := m.Cursor()
		_, err := m.AppendDecision(1, testTxnParticipants(1))
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("append error = %v, want EIO", err)
		}
		if !fm.Faulted() || m.Cursor() != before {
			t.Fatalf("append-error seam state faulted=%v cursor=%d", fm.Faulted(), m.Cursor())
		}
	})

	t.Run("enospc", func(t *testing.T) {
		m, _ := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
		defer m.Close()
		fm := NewFaultTxnMarker(m)
		fm.Program(TxnMarkerFaultPlan{Phase: TxnMarkerFaultENOSPCAppend, AppendIndex: 0})
		_, err := m.AppendDecision(1, testTxnParticipants(1))
		if !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("append error = %v, want ENOSPC", err)
		}
		if !fm.Faulted() {
			t.Fatal("ENOSPC fault did not fire")
		}
	})

	t.Run("sync-error", func(t *testing.T) {
		m, _ := createTestTxnMarker(t, 64*TxnMarkerMinSectorSize)
		defer m.Close()
		fm := NewFaultTxnMarker(m)
		if _, err := m.AppendDecision(1, testTxnParticipants(1)); err != nil {
			t.Fatalf("AppendDecision: %v", err)
		}
		fm.Program(TxnMarkerFaultPlan{Phase: TxnMarkerFaultSyncError, SyncIndex: 0})
		if err := m.Sync(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("Sync = %v, want EIO", err)
		}
		if !fm.Faulted() {
			t.Fatal("sync fault did not fire")
		}
	})
}

func TestTxnMarkerCreationFence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase TxnMarkerFaultPhase
	}{
		{"header-write", TxnMarkerFaultCreateHeaderWrite},
		{"file-sync", TxnMarkerFaultCreateFileSync},
		{"parent-dir-sync", TxnMarkerFaultCreateParentDirSync},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ProgramTxnMarkerCreateFault(TxnMarkerFaultPlan{Phase: tc.phase})
			t.Cleanup(func() { ProgramTxnMarkerCreateFault(TxnMarkerFaultPlan{}) })

			dir := t.TempDir()
			path := filepath.Join(dir, "txn.vtm")
			m, err := CreateTxnMarker(path, TxnMarkerOptions{
				Capacity: 8 * TxnMarkerMinSectorSize,
			})
			if err == nil {
				m.Close()
				t.Fatal("CreateTxnMarker succeeded under creation fault")
			}
			if !TxnMarkerCreateFaulted() {
				t.Fatal("creation fault did not fire")
			}

			_, err = os.Stat(path)
			switch {
			case errors.Is(err, os.ErrNotExist):
			case err != nil:
				t.Fatalf("stat mint residue: %v", err)
			default:
				opened, decisions, openErr := OpenTxnMarker(path, TxnMarkerOptions{})
				switch {
				case errors.Is(openErr, ErrTxnMarkerNoValidHeader):
				case openErr == nil:
					if decisions.MaxDCSN() != 0 {
						opened.Close()
						t.Fatalf("creation fault left a decodable partial log: max dcsn=%d", decisions.MaxDCSN())
					}
					if len(decisions.decisions) != 0 || len(decisions.retired) != 0 {
						opened.Close()
						t.Fatal("creation fault left non-empty decisions")
					}
					opened.Close()
				default:
					t.Fatalf("reopen mint residue = %v", openErr)
				}
			}
		})
	}
}

func TestTxnMarkerFull(t *testing.T) {
	m, _ := createTestTxnMarker(t, 2*TxnMarkerMinSectorSize)
	defer m.Close()
	appended := 0
	for {
		_, err := m.AppendDecision(uint64(appended+1), testTxnParticipants(1))
		if errors.Is(err, ErrTxnMarkerFull) {
			break
		}
		if err != nil {
			t.Fatalf("AppendDecision: %v", err)
		}
		appended++
	}
	if appended == 0 {
		t.Fatal("capacity too small to append any decision")
	}
	if _, err := m.AppendDecision(uint64(appended+1), testTxnParticipants(1)); !errors.Is(err, ErrTxnMarkerFull) {
		t.Fatalf("full append = %v, want ErrTxnMarkerFull", err)
	}
}

func TestTxnMarkerEmptyScanDoesNotAllocate(t *testing.T) {
	m, path := createTestTxnMarker(t, 2<<20)
	defer m.Close()
	var decisions TxnDecisions
	var scanErr error
	if allocs := testing.AllocsPerRun(10, func() {
		scanErr = m.scanDecisions(&decisions)
	}); allocs != 0 {
		t.Fatalf("empty decision scan allocations = %.1f, want 0", allocs)
	}
	if scanErr != nil {
		t.Fatal(scanErr)
	}
	wantDir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	wantDir = filepath.Clean(wantDir)
	if resolved, err := filepath.EvalSymlinks(wantDir); err == nil {
		wantDir = filepath.Clean(resolved)
	}
	if decisions.SourceDir() != wantDir {
		t.Fatalf("source directory = %q, want %q", decisions.SourceDir(), wantDir)
	}
}

func TestTxnMarkerCanonicalDirectoryFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "txn.vtm")
	if _, err := canonicalTxnMarkerDir(path); err == nil {
		t.Fatal("canonical transaction directory accepted an unresolved path")
	}
}

func TestTxnMarkerRejectsLeafSymlink(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.vtm")
	marker, err := CreateTxnMarker(realPath, TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, "txn.vtm")
	if err := os.Symlink(filepath.Base(realPath), linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, _, err := OpenTxnMarker(linkPath, TxnMarkerOptions{}); err == nil {
		t.Fatal("transaction marker accepted a leaf symlink")
	}
}

func TestTxnMarkerRemoveUsesPinnedDirectory(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	link := filepath.Join(t.TempDir(), "database")
	if err := os.Symlink(dirA, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	pathA := filepath.Join(link, "txn.vtm")
	markerA, err := CreateTxnMarker(pathA, TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pathB := filepath.Join(dirB, "txn.vtm")
	markerB, err := CreateTxnMarker(pathB, TxnMarkerOptions{})
	if err != nil {
		_ = markerA.Close()
		t.Fatal(err)
	}
	if err := markerB.Close(); err != nil {
		_ = markerA.Close()
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		_ = markerA.Close()
		t.Fatal(err)
	}
	if err := os.Symlink(dirB, link); err != nil {
		_ = markerA.Close()
		t.Fatal(err)
	}
	if err := markerA.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dirA, "txn.vtm")); !os.IsNotExist(err) {
		t.Fatalf("original marker after remove: %v", err)
	}
	if _, err := os.Stat(pathB); err != nil {
		t.Fatalf("retarget marker was removed: %v", err)
	}
}
