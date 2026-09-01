package seglog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func appendEntry(t *testing.T, l *Log, group, index, term uint64, payload string) Location {
	t.Helper()
	if cap(l.events) == 0 {
		if err := l.ReserveEvents(4096); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := l.index[group]; !ok {
		if err := l.ReserveGroup(group, 1024); err != nil {
			t.Fatal(err)
		}
	}
	loc, err := l.Append(Record{GroupID: group, Index: index, Term: term, Kind: RecordEntry, Payload: []byte(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func TestRoundTripRotationAndIndexRebuild(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendEntry(t, l, 2, 1, 1, "two-one")
	appendEntry(t, l, 1, 1, 3, "one-one")
	if err = l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	appendEntry(t, l, 1, 2, 3, "one-two")
	appendEntry(t, l, 2, 2, 2, "two-two")
	if err = l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.DeepVerify(); err != nil {
		t.Fatal(err)
	}
	if got := l.Manifest(); got.Generation != 4 || got.ActiveID != 2 || len(got.Segments) != 1 || len(got.Groups) != 2 || got.Groups[0].DurableLastIndex != 2 {
		t.Fatalf("manifest = %+v", got)
	}
	for _, group := range []uint64{1, 2} {
		g, ok := l.Group(group)
		if !ok || len(g.Entries) != 2 || g.Entries[0].SegmentID != 1 || g.Entries[1].SegmentID != 2 {
			t.Fatalf("group %d index = %+v, %v", group, g, ok)
		}
	}
}

func TestCompactFormatOverheadAndCanonicalIndex(t *testing.T) {
	if recordHeaderBytes != 40 || recordSize(0) != 40 {
		t.Fatalf("record overhead = %d/%d", recordHeaderBytes, recordSize(0))
	}
	sequential := make([]segmentEvent, 100)
	off := uint64(segmentHeaderBytes)
	for i := range sequential {
		sequential[i] = segmentEvent{Kind: RecordEntry, GroupID: 1, Index: uint64(i + 1), Term: 1, Offset: off, Bytes: 48}
		off += 48
	}
	encoded, err := marshalSegmentIndex(sequential, off)
	if err != nil {
		t.Fatal(err)
	}
	bodyBytes := len(encoded) - segmentIndexHeaderBytes - 4
	if bodyBytes > 6*len(sequential) {
		t.Fatalf("sequential compact index = %.2f bytes/event", float64(bodyBytes)/100)
	}
	decoded, err := unmarshalSegmentIndex(encoded, off, 100)
	if err != nil || !slices.Equal(decoded, sequential) {
		t.Fatalf("decode = %v, equal=%v", err, slices.Equal(decoded, sequential))
	}
	interleaved := make([]segmentEvent, 0, 400)
	off = segmentHeaderBytes
	for i := uint64(1); i <= 100; i++ {
		for group := uint64(1); group <= 4; group++ {
			interleaved = append(interleaved, segmentEvent{Kind: RecordEntry, GroupID: group, Index: i, Term: 1, Offset: off, Bytes: 48})
			off += 48
		}
	}
	encoded, err = marshalSegmentIndex(interleaved, off)
	if err != nil {
		t.Fatal(err)
	}
	bodyBytes = len(encoded) - segmentIndexHeaderBytes - 4
	if bodyBytes > 7*len(interleaved) {
		t.Fatalf("interleaved compact index = %.2f bytes/event", float64(bodyBytes)/400)
	}
}

func TestCompactIndexRejectsMalformedCanonicalVarints(t *testing.T) {
	events := []segmentEvent{{Kind: RecordEntry, GroupID: 1, Index: 1, Term: 1, Offset: segmentHeaderBytes, Bytes: recordHeaderBytes}}
	encoded, _ := marshalSegmentIndex(events, segmentHeaderBytes+recordHeaderBytes)
	noncanonical := append(slices.Clone(encoded[:41]), append([]byte{0}, encoded[41:]...)...)
	noncanonical[40] = 0x81
	binary.LittleEndian.PutUint32(noncanonical[12:16], uint32(len(noncanonical)))
	putCRC(noncanonical)
	overflow := append(slices.Clone(encoded[:segmentIndexHeaderBytes]), bytes.Repeat([]byte{0xff}, 10)...)
	overflow = append(overflow, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(overflow[12:16], uint32(len(overflow)))
	putCRC(overflow)
	trailing := append(slices.Clone(encoded[:len(encoded)-4]), 0)
	trailing = append(trailing, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(trailing[12:16], uint32(len(trailing)))
	putCRC(trailing)
	for name, malformed := range map[string][]byte{"noncanonical": noncanonical, "overflow": overflow, "trailing": trailing} {
		if _, err := unmarshalSegmentIndex(malformed, segmentHeaderBytes+recordHeaderBytes, 1); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s = %v", name, err)
		}
	}
}

func TestRotateUsesIncrementalHashWithoutReadingActive(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "one")
	appendEntry(t, l, 1, 2, 1, "two")
	if err := l.active.Close(); err != nil {
		t.Fatal(err)
	}
	writeOnly, err := os.OpenFile(filepath.Join(dir, activeName(1)), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	l.active = writeOnly
	if err = l.Rotate(nil); err != nil {
		t.Fatalf("rotation attempted an active read or failed publication: %v", err)
	}
	_ = l.Close()
	l, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if len(l.Manifest().Segments) != 1 {
		t.Fatal("segment was not sealed")
	}
}

func TestAppendSteadyStateZeroAlloc(t *testing.T) {
	l, err := Create(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.ReserveGroup(1, 2048); err != nil {
		t.Fatal(err)
	}
	if err = l.ReserveEvents(2048); err != nil {
		t.Fatal(err)
	}
	r := Record{GroupID: 1, Index: 1, Term: 1, Kind: RecordEntry, Payload: []byte("small raft entry")}
	if _, err = l.Append(r); err != nil {
		t.Fatal(err)
	}
	var runErr error
	allocs := testing.AllocsPerRun(1000, func() { r.Index++; _, runErr = l.Append(r) })
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("steady-state Append allocations = %v, want 0", allocs)
	}
}

func TestLogicalTruncationPersistsAndRebuilds(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		appendEntry(t, l, 7, i, 2, "x")
	}
	if err = l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = l.SetTruncate(7, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g, ok := l.Group(7)
	if !ok || g.TruncateIndex != 2 || g.TruncateTerm != 2 || len(g.Entries) != 2 || g.Entries[0].Index != 3 {
		t.Fatalf("rebuilt group = %+v, %v", g, ok)
	}
	if _, err = l.Append(Record{GroupID: 7, Index: 2, Term: 2, Kind: RecordEntry}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("append below truncation = %v", err)
	}
}

func TestLogicalSuffixTruncationAllowsTermReplacement(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	for i := uint64(1); i <= 4; i++ {
		appendEntry(t, l, 3, i, 1, "old")
	}
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateSuffix(3, 3); err != nil {
		t.Fatal(err)
	}
	appendEntry(t, l, 3, 3, 2, "new")
	appendEntry(t, l, 3, 4, 2, "new")
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g, _ := l.Group(3)
	if len(g.Entries) != 4 || g.Entries[2].Index != 3 || g.Entries[2].Term != 2 || g.Entries[3].Term != 2 {
		t.Fatalf("replacement index = %+v", g)
	}
}

func TestUncommittedSuffixTruncationIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	for i := uint64(1); i <= 3; i++ {
		appendEntry(t, l, 4, i, 1, "old")
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateSuffix(4, 2); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g, _ := l.Group(4)
	if len(g.Entries) != 3 || g.Entries[2].Index != 3 {
		t.Fatalf("uncommitted truncate survived: %+v", g)
	}
}

func TestSuffixTruncationCanDurablyEmptyGroup(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 5, 1, 1, "only")
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateSuffix(5, 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g, ok := l.Group(5)
	if !ok || len(g.Entries) != 0 {
		t.Fatalf("group = %+v, %v", g, ok)
	}
}

func TestActiveUncommittedTailIsDiscardedAtEveryBoundary(t *testing.T) {
	probe := Record{GroupID: 1, Index: 2, Term: 1, Kind: RecordEntry, Payload: []byte("payload")}
	encoded, _ := marshalRecord(probe, nil)
	for cut := 0; cut <= len(encoded); cut++ {
		t.Run(testName(cut), func(t *testing.T) {
			dir := t.TempDir()
			l, err := Create(dir)
			if err != nil {
				t.Fatal(err)
			}
			appendEntry(t, l, 1, 1, 1, "durable")
			if err = l.Sync(); err != nil {
				t.Fatal(err)
			}
			durable := l.Manifest().DurableOffset
			loc := appendEntry(t, l, 1, 2, 1, "payload")
			if loc.Offset != durable {
				t.Fatalf("offset %d != durable %d", loc.Offset, durable)
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			if err = os.Truncate(filepath.Join(dir, activeName(1)), int64(durable)+int64(cut)); err != nil {
				t.Fatal(err)
			}
			l, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			g, _ := l.Group(1)
			if len(g.Entries) != 1 || g.Entries[0].Index != 1 {
				t.Fatalf("invented tail at cut %d: %+v", cut, g)
			}
			st, _ := l.active.Stat()
			if uint64(st.Size()) != durable {
				t.Fatalf("size = %d, want %d", st.Size(), durable)
			}
		})
	}
}

func TestCommittedTornTailRejectedAtEveryBoundary(t *testing.T) {
	encoded, _ := marshalRecord(Record{GroupID: 1, Index: 2, Term: 1, Kind: RecordEntry, Payload: []byte("payload")}, nil)
	for cut := 0; cut < len(encoded); cut++ {
		t.Run(testName(cut), func(t *testing.T) {
			dir := t.TempDir()
			l, _ := Create(dir)
			appendEntry(t, l, 1, 1, 1, "durable")
			first := l.Manifest().DurableOffset
			appendEntry(t, l, 1, 2, 1, "payload")
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
			_ = l.Close()
			if err := os.Truncate(filepath.Join(dir, activeName(1)), int64(first)+int64(cut)); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("cut %d open = %v", cut, err)
			}
		})
	}
}

func TestCorruptionChecks(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, dir string, m Manifest)
	}{
		{"manifest", func(t *testing.T, dir string, _ Manifest) { flipByte(t, filepath.Join(dir, ManifestName), 20) }},
		{"sealed-header", func(t *testing.T, dir string, _ Manifest) { flipByte(t, filepath.Join(dir, sealedName(1)), 30) }},
		{"sealed-index", func(t *testing.T, dir string, m Manifest) {
			flipByte(t, filepath.Join(dir, sealedName(1)), int64(m.Segments[0].IndexOffset)+segmentIndexHeaderBytes)
		}},
		{"sealed-footer", func(t *testing.T, dir string, m Manifest) {
			flipByte(t, filepath.Join(dir, sealedName(1)), int64(m.Segments[0].Bytes)-10)
		}},
		{"active-header", func(t *testing.T, dir string, m Manifest) {
			flipByte(t, filepath.Join(dir, activeName(m.ActiveID)), 50)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			l, _ := Create(dir)
			appendEntry(t, l, 1, 1, 1, "value")
			if err := l.Rotate(nil); err != nil {
				t.Fatal(err)
			}
			m := l.Manifest()
			_ = l.Close()
			tc.mutate(t, dir, m)
			if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("open = %v", err)
			}
		})
	}
}

func TestOpenSkipsSealedPayloadAndDeepVerifyDetectsDamage(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	loc := appendEntry(t, l, 1, 1, 1, "sealed payload")
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	flipByte(t, filepath.Join(dir, sealedName(1)), int64(loc.Offset+recordHeaderBytes))
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("metadata-only Open read or validated sealed payload: %v", err)
	}
	defer l.Close()
	g, _ := l.Group(1)
	if len(g.Entries) != 1 || g.Entries[0] != loc {
		t.Fatalf("metadata index = %+v", g)
	}
	if err = l.DeepVerify(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DeepVerify = %v", err)
	}
}

func TestSealedIndexSemanticMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "payload")
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	m := l.Manifest()
	_ = l.Close()
	meta := m.Segments[0]
	path := filepath.Join(dir, sealedName(1))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	index := make([]byte, meta.IndexBytes)
	if _, err = f.ReadAt(index, int64(meta.IndexOffset)); err != nil {
		t.Fatal(err)
	}
	// group delta, count, kind, and index are one byte each here; alter the
	// canonical term while retaining a valid index CRC.
	index[segmentIndexHeaderBytes+4] = 2
	putCRC(index)
	if _, err = f.WriteAt(index, int64(meta.IndexOffset)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("open = %v", err)
	}
}

func TestSegmentChainMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "a")
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	appendEntry(t, l, 1, 2, 1, "b")
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	path := filepath.Join(dir, sealedName(2))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, segmentHeaderBytes)
	_, _ = f.ReadAt(b, 0)
	b[40] ^= 0x40
	putCRC(b)
	_, _ = f.WriteAt(b, 0)
	_ = f.Sync()
	_ = f.Close()
	if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("open = %v", err)
	}
}

func TestCommittedActiveRecordChecksumRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	loc := appendEntry(t, l, 1, 1, 1, "committed")
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	flipByte(t, filepath.Join(dir, activeName(1)), int64(loc.Offset+recordHeaderBytes))
	if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("open = %v", err)
	}
}

func TestPerGroupDurableMetadataMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 9, 1, 4, "committed")
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	path := filepath.Join(dir, ManifestName)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	off := manifestHeaderBytes + int(binaryUint32(b[56:60]))*manifestSegmentBytes
	// Change DurableLastTerm while retaining a structurally valid manifest CRC.
	b[off+32] ^= 1
	putCRC(b)
	if err = os.WriteFile(path, b, 0o640); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("open = %v", err)
	}
}

func TestLogIdentityRejectsSegmentGraft(t *testing.T) {
	left, right := t.TempDir(), t.TempDir()
	a, _ := Create(left)
	b, _ := Create(right)
	_ = a.Close()
	_ = b.Close()
	data, err := os.ReadFile(filepath.Join(right, activeName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(left, activeName(1)), data, 0o640); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(left); !errors.Is(err, ErrCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("open = %v", err)
	}
}

func TestManifestRejectsNonMonotonicMetadata(t *testing.T) {
	m := Manifest{Generation: 1, ActiveID: 3, ActiveGeneration: 3, DurableSegmentID: 3, DurableOffset: segmentHeaderBytes, Segments: []SegmentMeta{{ID: 2}, {ID: 1}}}
	if _, err := marshalManifest(m); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("marshal segments = %v", err)
	}
	m.Segments = nil
	m.Groups = []GroupMeta{{GroupID: 2}, {GroupID: 1}}
	if _, err := marshalManifest(m); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("marshal groups = %v", err)
	}
}

func TestRetainedChainAnchorAllowsPrefixReclamation(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "reclaim")
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.SetTruncate(1, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	appendEntry(t, l, 1, 2, 2, "retain")
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	m := l.Manifest()
	_ = l.Close()
	anchor := m.Segments[0]
	m.Generation++
	m.AnchorID, m.AnchorGeneration, m.AnchorHash = anchor.ID, anchor.Generation, anchor.Hash
	m.Segments = slices.Clone(m.Segments[1:])
	if err := publishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, sealedName(anchor.ID))); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g, _ := l.Group(1)
	if len(g.Entries) != 1 || g.Entries[0].Index != 2 || l.Manifest().AnchorID != 1 {
		t.Fatalf("anchored rebuild = %+v manifest=%+v", g, l.Manifest())
	}
}

func TestRetainedChainAnchorAllowsNoSealedSegments(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "reclaim")
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.SetTruncate(1, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	m := l.Manifest()
	_ = l.Close()
	anchor := m.Segments[0]
	m.Generation++
	m.AnchorID, m.AnchorGeneration, m.AnchorHash = anchor.ID, anchor.Generation, anchor.Hash
	m.Segments = nil
	if err := publishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, sealedName(anchor.ID))); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g, ok := l.Group(1)
	if !ok || len(g.Entries) != 0 || g.TruncateIndex != 1 {
		t.Fatalf("empty retained rebuild = %+v, %v", g, ok)
	}
}

func TestRetainedChainAnchorMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "one")
	if err := l.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	m := l.Manifest()
	_ = l.Close()
	anchor := m.Segments[0]
	m.Generation++
	m.AnchorID, m.AnchorGeneration, m.AnchorHash = anchor.ID, anchor.Generation, anchor.Hash
	m.AnchorHash[0] ^= 1
	m.Segments = nil
	if err := publishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, sealedName(anchor.ID))); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("open = %v", err)
	}
}

func TestRotationCrashPublicationPhases(t *testing.T) {
	for _, phase := range []RotationPhase{RotationSealedSynced, RotationSealedRenamed, RotationNextPublished, RotationManifestPublished} {
		t.Run(testName(int(phase)), func(t *testing.T) {
			dir := t.TempDir()
			l, _ := Create(dir)
			appendEntry(t, l, 1, 1, 1, "entry")
			injected := errors.New("injected crash")
			err := l.Rotate(func(got RotationPhase) error {
				if got == phase {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("rotate = %v", err)
			}
			assertPoisoned(t, l)
			_ = l.Close()
			l, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			g, ok := l.Group(1)
			if !ok || len(g.Entries) != 1 {
				t.Fatalf("recovered = %+v, %v", g, ok)
			}
			if phase == RotationSealedSynced {
				if l.Manifest().ActiveID != 1 {
					t.Fatalf("active = %d", l.Manifest().ActiveID)
				}
			} else if l.Manifest().ActiveID != 2 || len(l.Manifest().Segments) != 1 {
				t.Fatalf("manifest = %+v", l.Manifest())
			}
		})
	}
}

func TestManifestPublicationErrorsPoisonHandle(t *testing.T) {
	for _, operation := range []string{"sync", "truncate"} {
		t.Run(operation, func(t *testing.T) {
			l, _ := Create(t.TempDir())
			appendEntry(t, l, 1, 1, 1, "entry")
			if operation == "truncate" {
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			injected := errors.New("publish failed after ambiguous mutation")
			l.publishHook = func(Manifest) error { return injected }
			var err error
			if operation == "sync" {
				err = l.Sync()
			} else {
				err = l.SetTruncate(1, 1, 1)
			}
			if !errors.Is(err, injected) || !errors.Is(err, ErrPoisoned) {
				t.Fatalf("operation = %v", err)
			}
			assertPoisoned(t, l)
			_ = l.Close()
		})
	}
}

func assertPoisoned(t *testing.T, l *Log) {
	t.Helper()
	if _, err := l.Append(Record{GroupID: 1, Index: 2, Term: 1, Kind: RecordEntry}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("Append after fault = %v", err)
	}
	for name, call := range map[string]func() error{"Sync": l.Sync, "SetTruncate": func() error { return l.SetTruncate(1, 1, 1) }, "Rotate": func() error { return l.Rotate(nil) }, "TruncateSuffix": func() error { return l.TruncateSuffix(1, 1) }} {
		if err := call(); !errors.Is(err, ErrPoisoned) {
			t.Fatalf("%s after fault = %v", name, err)
		}
	}
}

func TestRotationRecoveryIgnoresIncompleteNextStage(t *testing.T) {
	dir := t.TempDir()
	l, _ := Create(dir)
	appendEntry(t, l, 1, 1, 1, "entry")
	injected := errors.New("injected crash")
	if err := l.Rotate(func(got RotationPhase) error {
		if got == RotationSealedRenamed {
			return injected
		}
		return nil
	}); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	_ = l.Close()
	stage := filepath.Join(dir, ".00000000000000000002.tmp")
	if err := os.WriteFile(stage, []byte("torn header"), 0o640); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.Manifest().ActiveID != 2 {
		t.Fatalf("active = %d", l.Manifest().ActiveID)
	}
	if _, err = os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage remains: %v", err)
	}
}

func flipByte(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte{0}
	if _, err = f.ReadAt(b, offset); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x80
	if _, err = f.WriteAt(b, offset); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}
func testName(n int) string {
	const hex = "0123456789abcdef"
	if n < 16 {
		return string([]byte{'0', hex[n]})
	}
	return string([]byte{hex[(n>>4)&15], hex[n&15]})
}
