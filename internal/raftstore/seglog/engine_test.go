package seglog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
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
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{1, 1, []byte("1"), 0}, {2, 2, []byte("2"), 0}, {3, 2, []byte("3"), 0}}, Hard: &h}}}); err != nil {
		t.Fatal(err)
	}
	tests := []ReadyBatch{
		{GroupID: 1, Hard: &HardState{Term: 1, Vote: 7, Commit: 2}},
		{GroupID: 1, Hard: &HardState{Term: 2, Vote: 8, Commit: 2}},
		{GroupID: 1, Hard: &HardState{Term: 2, Vote: 7, Commit: 1}},
		{GroupID: 1, Hard: &HardState{Term: 2, Vote: 7, Commit: 4}},
		{GroupID: 1, ReplaceFrom: 2, Entries: []Entry{{2, 3, []byte("bad"), 0}}},
	}
	for i, batch := range tests {
		if err := e.PersistWave(Wave{ID: waveID(uint64(i + 2)), Batches: []ReadyBatch{batch}}); !errors.Is(err, ErrRaftState) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	next := HardState{Term: 3, Vote: 8, Commit: 3}
	if err := e.PersistWave(Wave{ID: waveID(20), Batches: []ReadyBatch{{GroupID: 1, ReplaceFrom: 3, Entries: []Entry{{3, 3, []byte("new"), 0}}, Hard: &next}}}); err != nil {
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
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 4, Entries: []Entry{{1, 1, []byte("1"), 0}, {2, 1, []byte("2"), 0}, {3, 1, []byte("3"), 0}}, Hard: &h}}}); err != nil {
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
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{1, 1, []byte("payload"), 0}}, Hard: &h}}}); err != nil {
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
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{1, 1, []byte("prior"), 0}}}}}); err != nil {
		t.Fatal(err)
	}
	w := Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{2, 1, []byte("survives"), 0}}}}}
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
	conflict := Wave{ID: w.ID, Batches: []ReadyBatch{{GroupID: 1, ReplaceFrom: 2, Entries: []Entry{{2, 2, []byte("different"), 0}}}}}
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
