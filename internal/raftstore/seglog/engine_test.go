package seglog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func waveID(n uint64) WaveID {
	var id WaveID
	binary.LittleEndian.PutUint64(id[:8], n)
	id[15] = 0xa5
	return id
}

func newReservedEngine(t *testing.T, groups ...uint64) *Engine {
	t.Helper()
	e, err := CreateEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err = e.Reserve(1<<20, 4096, 4096); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		if err = e.ReserveGroup(group, 4096); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func TestPersistWaveOneWriteOneSyncAndNoManifestPublication(t *testing.T) {
	e := newReservedEngine(t, 1, 2)
	manifestPath := filepath.Join(e.log.dir, ManifestName)
	before, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	writes, syncs, publications := 0, 0, 0
	originalWrite := e.writeAt
	e.writeAt = func(f *os.File, p []byte, off int64) (int, error) {
		writes++
		return originalWrite(f, p, off)
	}
	e.syncData = func(*os.File) error { syncs++; return nil }
	e.log.publishHook = func(Manifest) error { publications++; return nil }
	h1, h2 := HardState{Term: 1, Vote: 7, Commit: 1}, HardState{Term: 2, Vote: 9, Commit: 1}
	w := Wave{ID: waveID(1), Batches: []ReadyBatch{
		{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("a")}}, Hard: &h1},
		{GroupID: 2, Entries: []Entry{{Index: 1, Term: 2, Data: []byte("b")}}, Hard: &h2},
	}}
	if err = e.PersistWave(w); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if writes != 1 || syncs != 1 || publications != 0 || !bytes.Equal(before, after) {
		t.Fatalf("writes=%d syncs=%d publications=%d manifestChanged=%v", writes, syncs, publications, !bytes.Equal(before, after))
	}
	for group, wantTerm := range map[uint64]uint64{1: 1, 2: 2} {
		state, ok := e.Group(group)
		if !ok || len(state.Entries) != 1 || state.Entries[0].Term != wantTerm || state.Hard.Commit != 1 {
			t.Fatalf("group %d = %+v, %v", group, state, ok)
		}
		payload := make([]byte, state.Entries[0].Bytes)
		if _, err = e.log.active.ReadAt(payload, int64(state.Entries[0].Offset)); err != nil {
			t.Fatal(err)
		}
		if string(payload) != map[uint64]string{1: "a", 2: "b"}[group] {
			t.Fatalf("group %d location read %q", group, payload)
		}
	}
}

func TestPersistWaveRequiresCanonicalGroupOrderBeforeIO(t *testing.T) {
	e := newReservedEngine(t, 1, 2)
	writes, syncs := 0, 0
	e.writeAt = func(*os.File, []byte, int64) (int, error) { writes++; return 0, nil }
	e.syncData = func(*os.File) error { syncs++; return nil }
	w := Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 2, Entries: []Entry{{Index: 1, Term: 1}}}, {GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}
	if err := e.PersistWave(w); !errors.Is(err, ErrRaftState) {
		t.Fatalf("PersistWave=%v", err)
	}
	if writes != 0 || syncs != 0 {
		t.Fatalf("invalid order reached I/O: writes=%d syncs=%d", writes, syncs)
	}
}

func TestWaveFrameOverheadAndOptionalHardState(t *testing.T) {
	e := newReservedEngine(t, 1)
	entry := Entry{Index: 1, Term: 1, Data: []byte("0123456789")}
	frame, _, _, err := e.prepareWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{entry}}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	// 72 fixed bytes + 1 wave batch-count byte + 3 common batch bytes +
	// 3 common entry bytes.
	if got, want := len(frame)-len(entry.Data), 79; got != want {
		t.Fatalf("single-entry overhead=%d want=%d", got, want)
	}
	hard := HardState{Term: 1, Vote: 1, Commit: 1}
	withHard, _, _, err := e.prepareWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{entry}, Hard: &hard}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(withHard) - len(frame); got != 3 {
		t.Fatalf("common optional HardState bytes=%d want=3", got)
	}
}

func TestHardStateAndReplacementLegality(t *testing.T) {
	e := newReservedEngine(t, 1)
	h := HardState{Term: 2, Vote: 7, Commit: 2}
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("1")}, {Index: 2, Term: 2, Data: []byte("2")}, {Index: 3, Term: 2, Data: []byte("3")}}, Hard: &h}}}); err != nil {
		t.Fatal(err)
	}
	tests := []ReadyBatch{
		{GroupID: 1, Hard: &HardState{Term: 1, Vote: 7, Commit: 2}},
		{GroupID: 1, Hard: &HardState{Term: 2, Vote: 8, Commit: 2}},
		{GroupID: 1, Hard: &HardState{Term: 2, Vote: 7, Commit: 1}},
		{GroupID: 1, Hard: &HardState{Term: 2, Vote: 7, Commit: 4}},
		{GroupID: 1, ReplaceFrom: 2, Entries: []Entry{{Index: 2, Term: 3, Data: []byte("bad")}}},
	}
	for i, batch := range tests {
		if err := e.PersistWave(Wave{ID: waveID(uint64(i + 2)), Batches: []ReadyBatch{batch}}); !errors.Is(err, ErrRaftState) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	next := HardState{Term: 3, Vote: 8, Commit: 3}
	if err := e.PersistWave(Wave{ID: waveID(20), Batches: []ReadyBatch{{GroupID: 1, ReplaceFrom: 3, Entries: []Entry{{Index: 3, Term: 3, Data: []byte("new")}}, Hard: &next}}}); err != nil {
		t.Fatal(err)
	}
	state, _ := e.Group(1)
	if len(state.Entries) != 3 || state.Entries[2].Term != 3 || state.Hard != next {
		t.Fatalf("replacement state=%+v", state)
	}
}

func TestCheckpointPrefixAndRestart(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 32, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(4, 16); err != nil {
		t.Fatal(err)
	}
	h := HardState{Term: 1, Vote: 1, Commit: 3}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 4, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("1")}, {Index: 2, Term: 1, Data: []byte("2")}, {Index: 3, Term: 1, Data: []byte("3")}}, Hard: &h}}}); err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{ID: [16]byte{9}, Index: 2, Term: 1}
	if err = e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 4, TruncateIndex: 2, TruncateTerm: 1, Checkpoint: &cp}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	state, _ := e.Group(4)
	if state.TruncateIndex != 2 || state.Checkpoint != cp || len(state.Entries) != 1 || state.Entries[0].Index != 3 || state.Hard != h {
		t.Fatalf("restarted state=%+v", state)
	}
}

func TestActiveWaveTornAtEveryByteBoundary(t *testing.T) {
	source := t.TempDir()
	e, err := CreateEngine(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 16, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	h := HardState{Term: 1, Vote: 1, Commit: 1}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("payload")}}, Hard: &h}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, _ := os.ReadFile(filepath.Join(source, ManifestName))
	active, _ := os.ReadFile(filepath.Join(source, activeName(1)))
	frame := active[segmentHeaderBytes:]
	for cut := 0; cut < len(frame); cut++ {
		dir := filepath.Join(t.TempDir(), "cut")
		if err = os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, ManifestName), manifest, 0o644); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, activeName(1)), active[:segmentHeaderBytes+cut], 0o644); err != nil {
			t.Fatal(err)
		}
		recovered, openErr := openEngine(dir, func(*os.File) error { return nil })
		if openErr != nil {
			t.Fatalf("cut %d/%d: %v", cut, len(frame), openErr)
		}
		if _, ok := recovered.Group(1); ok || recovered.Sequence() != 0 {
			t.Fatalf("cut %d invented state", cut)
		}
		_ = recovered.Close()
	}
}

func TestOutcomeUnknownRecoveryAndWaveIDConflict(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 32, 32); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 32); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("prior")}}}}}); err != nil {
		t.Fatal(err)
	}
	w := Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 2, Term: 1, Data: []byte("survives")}}}}}
	injected := errors.New("sync outcome unknown")
	e.syncData = func(*os.File) error { return injected }
	if err = e.PersistWave(w); !errors.Is(err, injected) {
		t.Fatalf("persist=%v", err)
	}
	if _, ok := e.Group(1); ok || e.Sequence() != 0 {
		t.Fatal("poisoned handle exposed state after ambiguous sync")
	}
	if err = e.PersistWave(w); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("poison=%v", err)
	}
	_ = e.Close()
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.Reserve(4096, 32, 32); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 32); err != nil {
		t.Fatal(err)
	}
	writes, syncs := 0, 0
	e.writeAt = func(*os.File, []byte, int64) (int, error) { writes++; return 0, errors.New("must not write") }
	e.syncData = func(*os.File) error { syncs++; return nil }
	if err = e.PersistWave(w); err != nil {
		t.Fatalf("retry=%v", err)
	}
	if writes != 0 || syncs != 0 {
		t.Fatalf("retry writes=%d syncs=%d", writes, syncs)
	}
	conflict := Wave{ID: w.ID, Batches: []ReadyBatch{{GroupID: 1, ReplaceFrom: 2, Entries: []Entry{{Index: 2, Term: 2, Data: []byte("different")}}}}}
	if err = e.PersistWave(conflict); !errors.Is(err, ErrWaveConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if err = e.PersistWave(w); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("conflict poison=%v", err)
	}
}

func TestOpenSyncsRecoveredActivePrefixBeforeExposure(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 16, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("durable")}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	recovered, err := openEngine(dir, func(*os.File) error { syncs++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if syncs != 1 {
		t.Fatalf("startup data syncs=%d want=1", syncs)
	}
	if state, ok := recovered.Group(1); !ok || len(state.Entries) != 1 {
		t.Fatalf("recovered state=%+v, %v", state, ok)
	}
	_ = recovered.Close()
	injected := errors.New("startup sync failed")
	recovered, err = openEngine(dir, func(*os.File) error { return injected })
	if recovered != nil || !errors.Is(err, injected) {
		t.Fatalf("failed startup sync exposed engine=%v err=%v", recovered != nil, err)
	}
}

func TestEngineOpenUsesSealedMetadataAndDeepVerifyReadsPayload(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 32, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("sealed-value")}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	sealed := filepath.Join(dir, sealedName(1))
	f, err := os.OpenFile(sealed, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	byteAtPayload := []byte{0}
	if _, err = f.ReadAt(byteAtPayload, segmentHeaderBytes+72); err != nil {
		t.Fatal(err)
	}
	byteAtPayload[0] ^= 0xff
	if _, err = f.WriteAt(byteAtPayload, segmentHeaderBytes+72); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatalf("metadata-only Open read sealed payload: %v", err)
	}
	defer e.Close()
	state, ok := e.Group(1)
	if !ok || len(state.Entries) != 1 || state.Entries[0].Bytes != uint64(len("sealed-value")) {
		t.Fatalf("sealed index state=%+v, %v", state, ok)
	}
	if err = e.DeepVerify(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DeepVerify=%v", err)
	}
}

func TestCompleteActiveWaveCorruptionIsRejected(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 16, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("payload")}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, activeName(1))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte{0}
	if _, err = f.ReadAt(b, segmentHeaderBytes+72); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 1
	if _, err = f.WriteAt(b, segmentHeaderBytes+72); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err = OpenEngine(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenEngine=%v", err)
	}
}

func TestPersistWaveSteadyStateZeroAlloc(t *testing.T) {
	e := newReservedEngine(t, 1)
	e.syncData = func(*os.File) error { return nil }
	data := []byte("small raft entry")
	entries := []Entry{{Index: 1, Term: 1, Data: data}}
	batches := []ReadyBatch{{GroupID: 1, Entries: entries}}
	w := Wave{ID: waveID(1), Batches: batches}
	if err := e.PersistWave(w); err != nil {
		t.Fatal(err)
	}
	var runErr error
	n := uint64(1)
	allocs := testing.AllocsPerRun(1000, func() {
		n++
		binary.LittleEndian.PutUint64(w.ID[:8], n)
		entries[0].Index = n
		runErr = e.PersistWave(w)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("steady PersistWave allocations=%v want=0", allocs)
	}
}

func TestEngineRecoversEveryRotationPhase(t *testing.T) {
	injected := errors.New("rotation crash")
	for _, phase := range []RotationPhase{RotationSealedSynced, RotationSealedRenamed, RotationNextPublished, RotationManifestPublished} {
		t.Run(phase.String(), func(t *testing.T) {
			dir := t.TempDir()
			e, err := CreateEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.Reserve(4096, 64, 16); err != nil {
				t.Fatal(err)
			}
			if err = e.ReserveGroup(1, 16); err != nil {
				t.Fatal(err)
			}
			if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("one")}}}}}); err != nil {
				t.Fatal(err)
			}
			err = e.Rotate(func(got RotationPhase) error {
				if got == phase {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("Rotate=%v", err)
			}
			_ = e.Close()
			e, err = OpenEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if e.log.manifest.DurableOffset != segmentHeaderBytes {
				t.Fatalf("pending marker=%d", e.log.manifest.DurableOffset)
			}
			state, ok := e.Group(1)
			if !ok || len(state.Entries) != 1 || state.Entries[0].Index != 1 {
				t.Fatalf("state=%+v, %v", state, ok)
			}
			if err = e.Reserve(4096, 64, 16); err != nil {
				t.Fatal(err)
			}
			if err = e.ReserveGroup(1, 16); err != nil {
				t.Fatal(err)
			}
			if err = e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 2, Term: 1, Data: []byte("two")}}}}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func (phase RotationPhase) String() string {
	switch phase {
	case RotationSealedSynced:
		return "sealed-synced"
	case RotationSealedRenamed:
		return "sealed-renamed"
	case RotationNextPublished:
		return "next-published"
	case RotationManifestPublished:
		return "manifest-published"
	default:
		return "unknown"
	}
}

func TestEnginePendingSealSuffixCuts(t *testing.T) {
	source := t.TempDir()
	e, err := CreateEngine(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 64, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("payload")}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.log.Sync(); err != nil {
		t.Fatal(err)
	}
	dataBytes := e.log.manifest.DurableOffset
	indexBytes, err := marshalSegmentIndex(e.log.events, dataBytes)
	if err != nil {
		t.Fatal(err)
	}
	var sum [32]byte
	copy(sum[:], e.log.activeHash.Sum(nil))
	footer := marshalSegmentFooter(segmentFooter{ID: 1, Generation: 1, Records: e.log.records, DataBytes: dataBytes, Hash: sum, IndexOffset: dataBytes, IndexBytes: uint64(len(indexBytes)), Events: uint64(len(e.log.events))})
	suffix := append(indexBytes, footer...)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(source, ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	active, err := os.ReadFile(filepath.Join(source, activeName(1)))
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut <= len(suffix); cut++ {
		dir := filepath.Join(t.TempDir(), "cut")
		if err = os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, ManifestName), manifest, 0o640); err != nil {
			t.Fatal(err)
		}
		candidate := append(bytes.Clone(active), suffix[:cut]...)
		if err = os.WriteFile(filepath.Join(dir, activeName(1)), candidate, 0o640); err != nil {
			t.Fatal(err)
		}
		recovered, openErr := openEngine(dir, func(*os.File) error { return nil })
		if openErr != nil {
			t.Fatalf("cut %d/%d: %v", cut, len(suffix), openErr)
		}
		state, ok := recovered.Group(1)
		if !ok || len(state.Entries) != 1 || recovered.log.activeOffset != dataBytes || recovered.log.manifest.DurableOffset != segmentHeaderBytes {
			t.Fatalf("cut %d state=%+v ok=%v off=%d marker=%d", cut, state, ok, recovered.log.activeOffset, recovered.log.manifest.DurableOffset)
		}
		_ = recovered.Close()
	}
}

func TestEntryTypesRoundTripAndCompactCost(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 64, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	normal := Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("n")}}}}}
	normalFrame, _, _, err := e.prepareWave(normal, true)
	if err != nil {
		t.Fatal(err)
	}
	conf := Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Type: pb.EntryConfChange, Data: []byte("n")}}}}}
	confFrame, _, _, err := e.prepareWave(conf, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(confFrame) != len(normalFrame)+1 {
		t.Fatalf("config entry overhead=%d want=1", len(confFrame)-len(normalFrame))
	}
	entries := []Entry{{Index: 1, Term: 1, Type: pb.EntryNormal, Data: []byte("n")}, {Index: 2, Term: 1, Type: pb.EntryConfChange, Data: []byte("c1")}, {Index: 3, Term: 1, Type: pb.EntryConfChangeV2, Data: []byte("c2")}}
	if err = e.PersistWave(Wave{ID: waveID(3), Batches: []ReadyBatch{{GroupID: 1, Entries: entries}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	state, _ := e.Group(1)
	for i, want := range []pb.EntryType{pb.EntryNormal, pb.EntryConfChange, pb.EntryConfChangeV2} {
		if state.Entries[i].Type != want {
			t.Fatalf("entry %d type=%v want=%v", i, state.Entries[i].Type, want)
		}
	}
}

func TestEntryTypeMalformedRejected(t *testing.T) {
	e := newReservedEngine(t, 1)
	writes := 0
	e.writeAt = func(*os.File, []byte, int64) (int, error) { writes++; return 0, nil }
	err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Type: pb.EntryType(99)}}}}})
	if !errors.Is(err, ErrRaftState) || writes != 0 {
		t.Fatalf("PersistWave=%v writes=%d", err, writes)
	}
	valid := Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Type: pb.EntryConfChange, Data: []byte("x")}}}}}
	frame, _, _, err := e.prepareWave(valid, false)
	if err != nil {
		t.Fatal(err)
	}
	malformed := bytes.Clone(frame)
	malformed[78] = 3 // batch-count, group, flags, count, index, term, then type.
	digest := sha256.Sum256(malformed[72:])
	copy(malformed[40:72], digest[:])
	sealWaveHeader(malformed, 1, valid.ID)
	if _, _, _, err = decodeWaveFrame(malformed, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode malformed type=%v", err)
	}
}

func TestEngineDeepVerifyMatchesCanonicalSealedIndex(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 64, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("payload")}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := unmarshalManifest(manifestBytes)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, sealedName(1))
	_, footer, events, err := readSealedMetadata(path, manifest.LogID, 0, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	for i := range events {
		if _, ok := typeForEventKind(events[i].Kind); ok {
			events[i].Offset--
			changed = true
			break
		}
	}
	if !changed {
		t.Fatal("missing entry event")
	}
	encoded, err := marshalSegmentIndex(events, footer.DataBytes)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(encoded)) != footer.IndexBytes {
		t.Fatalf("index size changed: %d != %d", len(encoded), footer.IndexBytes)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(encoded, int64(footer.IndexOffset)); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatalf("metadata Open=%v", err)
	}
	defer e.Close()
	if err = e.DeepVerify(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DeepVerify=%v", err)
	}
}

func TestEngineDeepVerifyChecksFooterFrameCount(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 64, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, ManifestName)
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := unmarshalManifest(manifestBytes)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, sealedName(1))
	_, footer, _, err := readSealedMetadata(path, manifest.LogID, 0, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	footer.Records++
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(marshalSegmentFooter(footer), stat.Size()-segmentFooterBytes); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	manifest.Segments[0].Records++
	manifestBytes, err = marshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifestPath, manifestBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatalf("metadata Open=%v", err)
	}
	defer e.Close()
	if err = e.DeepVerify(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("DeepVerify=%v", err)
	}
}

func TestWaveVerifierEnforcesCrossFrameSequence(t *testing.T) {
	e := newReservedEngine(t, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 2, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	firstHeader := make([]byte, recordHeaderBytes)
	if _, err := e.log.active.ReadAt(firstHeader, segmentHeaderBytes); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := inspectWaveHeader(firstHeader)
	if err != nil {
		t.Fatal(err)
	}
	secondOffset := int64(segmentHeaderBytes + firstBytes)
	secondHeader := make([]byte, recordHeaderBytes)
	if _, err = e.log.active.ReadAt(secondHeader, secondOffset); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(secondHeader[8:16], 3)
	binary.LittleEndian.PutUint32(secondHeader[36:40], crc32.Checksum(secondHeader[:36], crcTable))
	if _, err = e.log.active.WriteAt(secondHeader, secondOffset); err != nil {
		t.Fatal(err)
	}
	verifier := &Engine{groups: make(map[uint64]*engineGroup), waves: make(map[WaveID][32]byte)}
	if _, _, err = verifyWaveFrames(e.log.active, e.log.activeOffset, 1, verifier); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify=%v", err)
	}
}

func TestReadyIdentityAndSharedBatchBlobRoundTrip(t *testing.T) {
	dir := t.TempDir()
	e, err := CreateEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 64, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(7, 16); err != nil {
		t.Fatal(err)
	}
	blob := []byte("ciphertext-and-tag")
	digest := [16]byte{4}
	w := Wave{ID: waveID(9), Batches: []ReadyBatch{{GroupID: 7, NodeIncarnation: 3, ReadyID: 1, ReadyDigest: digest, Blob: blob, Entries: []Entry{{Index: 1, Term: 1, DataOffset: 0, DataBytes: 3}, {Index: 2, Term: 1, Type: pb.EntryConfChangeV2, DataOffset: 3, DataBytes: 2}}}}}
	if err = e.PersistWave(w); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	state, ok := e.Group(7)
	if !ok || state.NodeIncarnation != 3 || state.ReadyID != 1 || state.ReadyDigest != digest || state.ReadyWaveID != w.ID || len(state.Entries) != 2 {
		t.Fatalf("state=%+v ok=%v", state, ok)
	}
	if state.Entries[0].Offset != state.Entries[1].Offset || state.Entries[0].Bytes != uint64(len(blob)) || state.Entries[1].DataOffset != 3 || state.Entries[1].DataBytes != 2 {
		t.Fatalf("locations=%+v", state.Entries)
	}
	if err = e.ReserveReaders(1); err != nil {
		t.Fatal(err)
	}
	if err = e.PrepareSegment(state.Entries[0].SegmentID); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(blob))
	read, err := e.ReadLocation(state.Entries[1], buf)
	if err != nil || !bytes.Equal(read, blob) {
		t.Fatalf("read=%q err=%v", read, err)
	}
}

func TestNodeBatchSpaceAccounting(t *testing.T) {
	measure := func(groups, entriesPerGroup int) (frameBytes, indexBytes int) {
		t.Helper()
		e := &Engine{
			groups:       make(map[uint64]*engineGroup, groups),
			frameBuf:     make([]byte, 0, 4096),
			eventScratch: make([]segmentEvent, 0, groups*(entriesPerGroup+2)+1),
		}
		wave := Wave{ID: WaveID{1}, Batches: make([]ReadyBatch, groups)}
		for group := 1; group <= groups; group++ {
			e.groups[uint64(group)] = &engineGroup{GroupState: GroupState{Checkpoint: Checkpoint{Index: 1, Term: 1}, Hard: HardState{Term: 1, Commit: 1}, Entries: make([]EntryLocation, 0, 64)}}
			entries := make([]Entry, entriesPerGroup)
			for i := range entries {
				entries[i] = Entry{Index: uint64(i + 2), Term: 2, DataOffset: uint64(i), DataBytes: 1}
			}
			wave.Batches[group-1] = ReadyBatch{GroupID: uint64(group), NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}, Hard: &HardState{Term: 2, Vote: 1, Commit: uint64(entriesPerGroup + 1)}, Entries: entries, Blob: make([]byte, entriesPerGroup+16)}
		}
		frame, _, events, err := e.prepareWave(wave, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := range events {
			if isBlobEvent(events[i].Kind) {
				events[i].Offset += segmentHeaderBytes
			}
		}
		index, err := marshalSegmentIndex(events, uint64(segmentHeaderBytes+len(frame)))
		if err != nil {
			t.Fatal(err)
		}
		return len(frame), len(index)
	}
	if frame, index := measure(1, 32); frame != 274 || index != 340 {
		t.Fatalf("one group frame/index = %d/%d, want 274/340", frame, index)
	}
	if frame, index := measure(4, 8); frame != 397 || index != 469 {
		t.Fatalf("four groups frame/index = %d/%d, want 397/469", frame, index)
	}
}

func TestPreparedLocationReadZeroAlloc(t *testing.T) {
	e := newReservedEngine(t, 1)
	data := []byte("prepared-read")
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: data}}}}}); err != nil {
		t.Fatal(err)
	}
	state, _ := e.Group(1)
	buf := make([]byte, len(data))
	var runErr error
	allocs := testing.AllocsPerRun(1000, func() { _, runErr = e.ReadLocation(state.Entries[0], buf) })
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("prepared ReadLocation allocations=%v want=0", allocs)
	}
}
