package seglog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func appendEntry(t *testing.T, l *Log, group, index, term uint64, payload string) Location {
	t.Helper()
	loc, err := l.Append(Record{GroupID: group, Index: index, Term: term, Kind: 1, Payload: []byte(payload)})
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
	if _, err = l.Append(Record{GroupID: 7, Index: 2, Term: 2}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("append below truncation = %v", err)
	}
}

func TestActiveUncommittedTailIsDiscardedAtEveryBoundary(t *testing.T) {
	probe := Record{GroupID: 1, Index: 2, Term: 1, Kind: 1, Payload: []byte("payload")}
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
	encoded, _ := marshalRecord(Record{GroupID: 1, Index: 2, Term: 1, Payload: []byte("payload")}, nil)
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
		{"sealed-record", func(t *testing.T, dir string, _ Manifest) {
			flipByte(t, filepath.Join(dir, sealedName(1)), segmentHeaderBytes+64)
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
	b, err := marshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = unmarshalManifest(b); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unmarshal = %v", err)
	}
	m.Segments = nil
	m.Groups = []GroupMeta{{GroupID: 2}, {GroupID: 1}}
	b, _ = marshalManifest(m)
	if _, err = unmarshalManifest(b); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("groups = %v", err)
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
