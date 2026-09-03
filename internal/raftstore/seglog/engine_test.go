package seglog

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	testLogID   = [16]byte{0x71, 0x19}
	testAuthKey = sha256.Sum256([]byte("seglog engine test authentication key"))
)

func createTestEngine(dir string) (*Engine, error) {
	return CreateEngineAuthenticated(dir, testLogID, testAuthKey, 32<<20)
}

func openTestEngine(dir string) (*Engine, error) {
	return OpenEngineAuthenticated(dir, testLogID, testAuthKey)
}

func openTestEngineWithSync(dir string, sync func(*os.File) error) (*Engine, error) {
	return openEngine(dir, sync, testLogID, testAuthKey)
}

func cloneSeglogDir(t *testing.T, source, destination string, capacity uint64) {
	t.Helper()
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(source, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		path := filepath.Join(destination, entry.Name())
		file, openErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if strings.HasPrefix(entry.Name(), "segment-") {
			if reserveErr := reservePhysicalFile(file, capacity); reserveErr != nil {
				t.Fatal(reserveErr)
			}
		}
		if len(data) != 0 {
			if writeErr := writeFullAt(file, data, 0); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}

func readTestMetadata(t *testing.T, dir string, logID [16]byte, key [32]byte) metadataState {
	t.Helper()
	metadata, segments, err := openMetadataStore(dir, logID, key)
	if err != nil {
		t.Fatal(err)
	}
	state := stateFromMetadata(metadata.slot, segments)
	if err = metadata.Close(); err != nil {
		t.Fatal(err)
	}
	return state
}

func waveID(n uint64) WaveID {
	var id WaveID
	binary.LittleEndian.PutUint64(id[:8], n)
	id[15] = 0xa5
	return id
}

func newReservedEngine(t *testing.T, groups ...uint64) *Engine {
	t.Helper()
	e, err := createTestEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err = e.Reserve(1<<20, 4096, 4096); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveReaders(2); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		if err = e.ReserveGroup(group, 4096); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func newEngineAt(t *testing.T, dir string, groups ...uint64) *Engine {
	t.Helper()
	e, err := createTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(1<<20, 4096, 4096); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveReaders(2); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		if err = e.ReserveGroup(group, 4096); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func TestPersistWaveOneWriteOneSyncAndNoMetadataPublication(t *testing.T) {
	e := newReservedEngine(t, 1, 2)
	statePath := filepath.Join(e.log.dir, metadataName)
	before, err := os.ReadFile(statePath)
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
	e.log.publishHook = func(metadataState) error { publications++; return nil }
	h1, h2 := HardState{Term: 1, Vote: 7, Commit: 1}, HardState{Term: 2, Vote: 9, Commit: 1}
	w := Wave{ID: waveID(1), Batches: []ReadyBatch{
		{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("a")}}, Hard: &h1},
		{GroupID: 2, Entries: []Entry{{Index: 1, Term: 2, Data: []byte("b")}}, Hard: &h2},
	}}
	if err = e.PersistWave(w); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if writes != 1 || syncs != 1 || publications != 0 || !bytes.Equal(before, after) {
		t.Fatalf("writes=%d syncs=%d publications=%d stateChanged=%v", writes, syncs, publications, !bytes.Equal(before, after))
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
	// 3 common entry bytes + 1 canonical empty shared-blob length byte.
	if got, want := len(frame)-len(entry.Data), 80; got != want {
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
	e, err := createTestEngine(dir)
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
	e, err = openTestEngine(dir)
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
	e, err := createTestEngine(source)
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
	activeID, capacity := e.log.state.ActiveFileID, e.log.state.SegmentCapacity
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	active, _ := os.ReadFile(segmentPath(source, activeID))
	frame := active[segmentHeaderBytes:]
	cutParent := t.TempDir()
	for cut := 0; cut < len(frame); cut++ {
		dir := filepath.Join(cutParent, "cut")
		cloneSeglogDir(t, source, dir, capacity)
		if err = os.Truncate(segmentPath(dir, activeID), int64(segmentHeaderBytes+cut)); err != nil {
			t.Fatal(err)
		}
		recovered, openErr := openTestEngineWithSync(dir, func(*os.File) error { return nil })
		if openErr != nil {
			t.Fatalf("cut %d/%d: %v", cut, len(frame), openErr)
		}
		if _, ok := recovered.Group(1); ok || recovered.Sequence() != 0 {
			t.Fatalf("cut %d invented state", cut)
		}
		_ = recovered.Close()
		if err = os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOutcomeUnknownRecoveryAndWaveIDConflict(t *testing.T) {
	dir := t.TempDir()
	e, err := createTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 32, 32); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 32); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("prior")}}}}}); err != nil {
		t.Fatal(err)
	}
	w := Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 2, ReadyDigest: [16]byte{2}, Entries: []Entry{{Index: 2, Term: 1, Data: []byte("survives")}}}}}
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
	e, err = openTestEngine(dir)
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
	conflict := Wave{ID: w.ID, Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 2, ReadyDigest: [16]byte{2}, ReplaceFrom: 2, Entries: []Entry{{Index: 2, Term: 2, Data: []byte("different")}}}}}
	if err = e.PersistWave(conflict); !errors.Is(err, ErrWaveConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if err = e.PersistWave(w); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("conflict poison=%v", err)
	}
}

func TestWaveRetryStateStaysBoundedAcrossWritesAndRotations(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1)
	e.waveLimit = 2
	var latest Wave
	for index := uint64(1); index <= 50; index++ {
		latest = Wave{ID: waveID(index), Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: index, ReadyDigest: [16]byte{byte(index)}, Entries: []Entry{{Index: index, Term: 1}}}}}
		if err := e.PersistWave(latest); err != nil {
			t.Fatalf("index=%d err=%v", index, err)
		}
		if len(e.waves) != 1 {
			t.Fatalf("index=%d retry states=%d", index, len(e.waves))
		}
		if index%10 == 0 {
			if err := e.Rotate(nil); err != nil {
				t.Fatalf("rotate index=%d: %v", index, err)
			}
			if err := e.WaitSeal(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	e, err = openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.Reserve(1<<20, 4096, 2); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 4096); err != nil {
		t.Fatal(err)
	}
	if err := e.PersistWave(latest); err != nil {
		t.Fatalf("latest retry=%v", err)
	}
	conflict := latest
	conflict.Batches = []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 50, ReadyDigest: [16]byte{50}, ReplaceFrom: 50, Entries: []Entry{{Index: 50, Term: 2}}}}
	if err := e.PersistWave(conflict); !errors.Is(err, ErrWaveConflict) {
		t.Fatalf("conflicting reuse=%v", err)
	}
}

func TestCheckpointBaseSummaryMatchesFullSealedReplay(t *testing.T) {
	dir := t.TempDir()
	engine := newEngineAt(t, dir, 1)
	hard1 := HardState{Term: 1, Vote: 1, Commit: 1}
	if err := engine.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}, Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, Hard: &hard1}}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	hard2 := HardState{Term: 2, Vote: 1, Commit: 3}
	if err := engine.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 2, ReadyDigest: [16]byte{2}, ReplaceFrom: 2, Entries: []Entry{{Index: 2, Term: 2}, {Index: 3, Term: 2}}, Hard: &hard2}}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	want, ok := engine.Group(1)
	if !ok {
		t.Fatal("group missing before checkpoint")
	}
	id := fileID{0xee, 1}
	digest, err := writeCatalogCheckpoint(dir, catalogCheckpoint{ID: id, LogID: testLogID, Generation: engine.log.metadata.slot.Generation, Tail: engine.log.metadata.slot.CatalogTail, CatalogHash: engine.log.metadata.slot.CatalogHash, Segments: engine.log.state.Segments, BaseSequence: engine.sealedSequence, GroupIDs: engine.sealedSummaryOrder, GroupSummaries: engine.sealedSummaries}, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	next := engine.log.metadata.slot
	next.Generation++
	next.CheckpointID, next.CheckpointTail, next.CheckpointHash = [16]byte(id), next.CatalogTail, digest
	if err = engine.log.metadata.publish(next, nil); err != nil {
		t.Fatal(err)
	}
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	got, ok := engine.Group(1)
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("base replay mismatch\n got=%+v\nwant=%+v", got, want)
	}
}

func TestSharedWaveRefSurvivesOneGroupAdvanceAfterReopen(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1, 2)
	shared := Wave{ID: waveID(1), Batches: []ReadyBatch{
		{GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}, Entries: []Entry{{Index: 1, Term: 1}}},
		{GroupID: 2, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{2}, Entries: []Entry{{Index: 1, Term: 1}}},
	}}
	if err := e.PersistWave(shared); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.Reserve(1<<20, 64, 4); err != nil {
		t.Fatal(err)
	}
	for _, group := range []uint64{1, 2} {
		if err = e.ReserveGroup(group, 16); err != nil {
			t.Fatal(err)
		}
	}
	advance := Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 2, ReadyDigest: [16]byte{3}, Entries: []Entry{{Index: 2, Term: 1}}}}}
	if err = e.PersistWave(advance); err != nil {
		t.Fatal(err)
	}
	if state, ok := e.waves[shared.ID]; !ok || state.refs != 1 {
		t.Fatalf("shared retry state=%+v ok=%v", state, ok)
	}
	if err = e.PersistWave(shared); err != nil {
		t.Fatalf("exact shared retry=%v", err)
	}
	conflict := shared
	conflict.Batches = append([]ReadyBatch(nil), shared.Batches...)
	conflict.Batches[1].ReadyDigest[0] ^= 1
	if err = e.PersistWave(conflict); !errors.Is(err, ErrWaveConflict) {
		t.Fatalf("shared conflict=%v", err)
	}
}

func TestCatalogHardSuffixBackpressuresBeforeRotationMutation(t *testing.T) {
	oldSoft, oldHard, oldWriter := catalogCheckpointSoftRecords, catalogCheckpointHardRecords, catalogCheckpointWriter
	catalogCheckpointSoftRecords, catalogCheckpointHardRecords = 1, 1
	catalogCheckpointWriter = func(string, catalogCheckpoint, [32]byte) ([32]byte, error) { return [32]byte{}, ErrBounds }
	t.Cleanup(func() {
		catalogCheckpointSoftRecords, catalogCheckpointHardRecords, catalogCheckpointWriter = oldSoft, oldHard, oldWriter
	})
	e := newReservedEngine(t, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	before := e.log.state
	beforeOffset := e.log.activeOffset
	if err := e.Rotate(nil); !errors.Is(err, ErrBounds) {
		t.Fatalf("rotation at hard catalog suffix = %v", err)
	}
	if e.log.state.Generation != before.Generation || e.log.state.ActiveID != before.ActiveID || e.log.activeOffset != beforeOffset || e.sealPending {
		t.Fatalf("hard-bound refusal mutated state: before=%+v/%d after=%+v/%d", before, beforeOffset, e.log.state, e.log.activeOffset)
	}
}

func TestMetadataCheckpointWriterFailureRetriesWithoutPoison(t *testing.T) {
	oldSoft, oldWriter := catalogCheckpointSoftRecords, catalogCheckpointWriter
	catalogCheckpointSoftRecords = 1
	t.Cleanup(func() { catalogCheckpointSoftRecords, catalogCheckpointWriter = oldSoft, oldWriter })
	e := newReservedEngine(t, 1)
	defer e.Close()
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	injected := syscall.EIO
	catalogCheckpointWriter = func(string, catalogCheckpoint, [32]byte) ([32]byte, error) { return [32]byte{}, injected }
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	e.runMetadataMaintenance()
	if err := e.log.usable(); err != nil {
		t.Fatalf("ordinary checkpoint I/O poisoned engine: %v", err)
	}
	if catalogSuffixRecords(e.log.metadata.slot) == 0 {
		t.Fatal("failed checkpoint unexpectedly published")
	}
	catalogCheckpointWriter = oldWriter
	e.runMetadataMaintenance()
	if err := e.log.usable(); err != nil || catalogSuffixRecords(e.log.metadata.slot) != 0 {
		t.Fatalf("checkpoint maintenance did not recover: suffix=%d err=%v", catalogSuffixRecords(e.log.metadata.slot), err)
	}
}

func TestMetadataCheckpointSnapshotConcurrentWithReclaimFenceUpdate(t *testing.T) {
	oldWriter := catalogCheckpointWriter
	t.Cleanup(func() { catalogCheckpointWriter = oldWriter })
	e, _, _ := newReclaimableEngine(t, t.TempDir())
	defer e.Close()
	if err := e.PersistWave(Wave{ID: waveID(20), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 3, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	e.writeMu.Lock()
	e.log.metadata.needsHealing = true
	e.writeMu.Unlock()
	entered := make(chan struct{})
	start := make(chan struct{})
	catalogCheckpointWriter = func(dir string, checkpoint catalogCheckpoint, key [32]byte) ([32]byte, error) {
		close(entered)
		<-start
		return oldWriter(dir, checkpoint, key)
	}
	maintenanceDone := make(chan struct{})
	go func() {
		e.runMetadataMaintenance()
		close(maintenanceDone)
	}()
	<-entered
	persistDone := make(chan error, 1)
	checkpoint := Checkpoint{ID: [16]byte{0x44}, Index: 3, Term: 1}
	hard := HardState{Term: 1, Vote: 1, Commit: 3}
	go func() {
		<-start
		persistDone <- e.PersistWave(Wave{ID: waveID(21), Batches: []ReadyBatch{{GroupID: 1, Checkpoint: &checkpoint, Hard: &hard}}})
	}()
	close(start)
	if err := <-persistDone; err != nil {
		t.Fatal(err)
	}
	<-maintenanceDone
	if err := e.log.usable(); err != nil {
		t.Fatal(err)
	}
}

func TestReserveCannotShrinkActiveSealBudget(t *testing.T) {
	e := newReservedEngine(t, 1)
	for i := uint64(1); i <= 2; i++ {
		if err := e.PersistWave(Wave{ID: waveID(i), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: i, Term: 1}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.log.events) <= len(e.eventScratch) {
		t.Fatalf("fixture log events=%d scratch=%d", len(e.log.events), len(e.eventScratch))
	}
	before := e.sealHeadroom
	if err := e.Reserve(cap(e.frameBuf), len(e.eventScratch), e.waveLimit); !errors.Is(err, ErrBounds) {
		t.Fatalf("shrink active event budget=%v", err)
	}
	if e.sealHeadroom != before {
		t.Fatalf("seal headroom changed %d -> %d", before, e.sealHeadroom)
	}
}

func TestAutomaticRotationNeverExceedsReservedCapacity(t *testing.T) {
	dir := t.TempDir()
	const capacity = 256 << 10
	e, err := CreateEngineAuthenticated(dir, testLogID, testAuthKey, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.Reserve(16<<10, 128, 128); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 128); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{7}, 8<<10)
	for i := uint64(1); i <= 128; i++ {
		wave := Wave{ID: waveID(i), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: i, Term: 1, Data: payload}}}}}
		for {
			err = e.PersistWave(wave)
			if !errors.Is(err, ErrBackpressure) {
				break
			}
			if err = e.WaitSeal(); err != nil {
				t.Fatal(err)
			}
		}
		if err != nil {
			t.Fatalf("wave %d: %v", i, err)
		}
	}
	if err = e.Rotate(nil); errors.Is(err, ErrBackpressure) {
		if err = e.WaitSeal(); err == nil {
			err = e.Rotate(nil)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if len(e.log.state.Segments) < 2 {
		t.Fatalf("automatic rotations=%d, want at least 2", len(e.log.state.Segments))
	}
	for _, segment := range e.log.state.Segments {
		stat, statErr := os.Stat(segmentPath(dir, segment.FileID))
		if statErr != nil {
			t.Fatal(statErr)
		}
		if stat.Size() > capacity {
			t.Fatalf("segment %d EOF=%d capacity=%d", segment.ID, stat.Size(), capacity)
		}
	}
}

func TestOpenSyncsRecoveredActivePrefixBeforeExposure(t *testing.T) {
	dir := t.TempDir()
	e, err := createTestEngine(dir)
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
	activeBytes := e.log.activeOffset
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	var recoveryIO recoveryIOCounters
	recovered, err := openEngineAuthenticatedObserved(dir, func(*os.File) error { syncs++; return nil }, testLogID, testAuthKey, &recoveryIO)
	if err != nil {
		t.Fatal(err)
	}
	if syncs != 1 {
		t.Fatalf("startup data syncs=%d want=1", syncs)
	}
	if recoveryIO.activeScanBytes != activeBytes-segmentHeaderBytes {
		t.Fatalf("active scan bytes=%d want=%d", recoveryIO.activeScanBytes, activeBytes-segmentHeaderBytes)
	}
	if state, ok := recovered.Group(1); !ok || len(state.Entries) != 1 {
		t.Fatalf("recovered state=%+v, %v", state, ok)
	}
	_ = recovered.Close()
	injected := errors.New("startup sync failed")
	recovered, err = openTestEngineWithSync(dir, func(*os.File) error { return injected })
	if recovered != nil || !errors.Is(err, injected) {
		t.Fatalf("failed startup sync exposed engine=%v err=%v", recovered != nil, err)
	}
}

func TestEngineOpenUsesSealedMetadataAndDeepVerifyReadsPayload(t *testing.T) {
	dir := t.TempDir()
	e, err := createTestEngine(dir)
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
	if err = e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	sealedID := e.log.state.Segments[0].FileID
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	sealed := segmentPath(dir, sealedID)
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
	e, err = openTestEngine(dir)
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
	e, err := createTestEngine(dir)
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
	activeFileID := e.log.state.ActiveFileID
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	path := segmentPath(dir, activeFileID)
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
	if _, err = openTestEngine(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("OpenEngine=%v", err)
	}
}

func TestOpenRejectsSegmentFilesBeyondAuthenticatedCapacity(t *testing.T) {
	t.Run("active", func(t *testing.T) {
		dir := t.TempDir()
		e, err := createTestEngine(dir)
		if err != nil {
			t.Fatal(err)
		}
		state := e.log.state
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		if err = os.Truncate(segmentPath(dir, state.ActiveFileID), int64(state.SegmentCapacity+1)); err != nil {
			t.Fatal(err)
		}
		if _, err = openTestEngine(dir); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("oversized active Open=%v", err)
		}
	})
	t.Run("sealed", func(t *testing.T) {
		dir := t.TempDir()
		e := newEngineAt(t, dir, 1)
		if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
			t.Fatal(err)
		}
		if err := e.Rotate(nil); err != nil {
			t.Fatal(err)
		}
		if err := e.WaitSeal(); err != nil {
			t.Fatal(err)
		}
		state := e.log.state
		sealedFile := state.Segments[0].FileID
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(segmentPath(dir, sealedFile), int64(state.SegmentCapacity+1)); err != nil {
			t.Fatal(err)
		}
		if _, err := openTestEngine(dir); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("oversized sealed Open=%v", err)
		}
	})
}

func TestOpenDegradesMissingOrBadReserveToCapacityBackpressure(t *testing.T) {
	for _, damage := range []struct {
		name string
		fn   func(string) error
	}{
		{name: "missing", fn: os.Remove},
		{name: "bad", fn: func(path string) error {
			if err := os.Truncate(path, 1); err != nil {
				return err
			}
			return os.WriteFile(path, []byte{0xff}, 0o640)
		}},
	} {
		t.Run(damage.name, func(t *testing.T) {
			dir := t.TempDir()
			e, err := createTestEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			reserve := e.log.state.Reserves[0]
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			if err = damage.fn(segmentPath(dir, reserve.FileID)); err != nil {
				t.Fatal(err)
			}
			e, err = openTestEngine(dir)
			if err != nil {
				t.Fatalf("Open=%v", err)
			}
			defer e.Close()
			if e.log.state.Reserves[0].Ready || !e.log.metadata.needsHealing {
				t.Fatalf("reserve=%+v healing=%v", e.log.state.Reserves[0], e.log.metadata.needsHealing)
			}
			if err = e.Rotate(nil); !errors.Is(err, ErrBounds) {
				t.Fatalf("Rotate without two reserves=%v", err)
			}
		})
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
	for _, phase := range []RotationPhase{RotationSealedSynced, RotationSealFileClosed, RotationSealedMetadataPublished, RotationNextPublished, RotationPendingMetadataPublished} {
		t.Run(phase.String(), func(t *testing.T) {
			dir := t.TempDir()
			e, err := createTestEngine(dir)
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
			if phase == RotationSealedSynced || phase == RotationSealFileClosed || phase == RotationSealedMetadataPublished {
				if err != nil {
					t.Fatalf("Rotate=%v", err)
				}
				err = e.WaitSeal()
			}
			if !errors.Is(err, injected) {
				t.Fatalf("Rotate=%v", err)
			}
			_ = e.Close()
			var recoveryIO recoveryIOCounters
			e, err = openEngineAuthenticatedObserved(dir, func(*os.File) error { return nil }, testLogID, testAuthKey, &recoveryIO)
			if err != nil {
				t.Fatal(err)
			}
			if phase == RotationSealedSynced || phase == RotationSealFileClosed {
				if recoveryIO.pendingPromotions != 1 || recoveryIO.pendingPayloadBytes != 0 || recoveryIO.pendingSealBytes != 0 {
					t.Fatalf("fast pending promotion I/O=%+v", recoveryIO)
				}
			}
			defer e.Close()
			if e.log.state.DurableOffset != segmentHeaderBytes {
				t.Fatalf("pending marker=%d", e.log.state.DurableOffset)
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
	case RotationSealFileClosed:
		return "seal-file-closed"
	case RotationSealedMetadataPublished:
		return "sealed-metadata-published"
	case RotationNextPublished:
		return "next-published"
	case RotationPendingMetadataPublished:
		return "pending-metadata-published"
	default:
		return "unknown"
	}
}

func TestEnginePendingSealSuffixCuts(t *testing.T) {
	source := t.TempDir()
	e, err := createTestEngine(source)
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
	dataBytes := e.log.activeOffset
	pendingFileID, capacity := e.log.state.ActiveFileID, e.log.state.SegmentCapacity
	indexBytes, _, err := e.marshalEngineSealedIndex(dataBytes)
	if err != nil {
		t.Fatal(err)
	}
	var sum [32]byte
	copy(sum[:], e.log.activeHash.Sum(nil))
	footer := marshalSegmentFooter(segmentFooter{ID: 1, Generation: 1, Records: e.log.records, DataBytes: dataBytes, Hash: sum, IndexOffset: dataBytes, IndexBytes: uint64(len(indexBytes)), Events: uint64(len(e.log.events))})
	suffix := append(indexBytes, footer...)
	injected := errors.New("freeze after metadata publication")
	if err = e.Rotate(func(phase RotationPhase) error {
		if phase == RotationPendingMetadataPublished {
			return injected
		}
		return nil
	}); !errors.Is(err, injected) {
		t.Fatalf("freeze=%v", err)
	}
	if err = e.Close(); err != nil {
		// The injected post-publication cut poisons the old handle by design.
	}
	cutParent := t.TempDir()
	for cut := 0; cut <= len(suffix); cut++ {
		dir := filepath.Join(cutParent, "cut")
		cloneSeglogDir(t, source, dir, capacity)
		file, openErr := os.OpenFile(segmentPath(dir, pendingFileID), os.O_RDWR, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if err = file.Truncate(int64(dataBytes)); err == nil && cut != 0 {
			_, err = file.WriteAt(suffix[:cut], int64(dataBytes))
		}
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
		var recoveryIO recoveryIOCounters
		recovered, openErr := openEngineAuthenticatedObserved(dir, func(*os.File) error { return nil }, testLogID, testAuthKey, &recoveryIO)
		if openErr != nil {
			t.Fatalf("cut %d/%d: %v", cut, len(suffix), openErr)
		}
		state, ok := recovered.Group(1)
		if !ok || len(state.Entries) != 1 || recovered.log.activeOffset != segmentHeaderBytes || recovered.log.state.DurableOffset != segmentHeaderBytes {
			t.Fatalf("cut %d state=%+v ok=%v off=%d marker=%d", cut, state, ok, recovered.log.activeOffset, recovered.log.state.DurableOffset)
		}
		if recoveryIO.maintenanceReserveAttempts != 0 || recoveryIO.maintenanceCheckpointAttempts != 0 {
			t.Fatalf("cut %d performed Open maintenance: %+v", cut, recoveryIO)
		}
		_ = recovered.Close()
		if err = os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnginePendingRotationCrashCutsRecover(t *testing.T) {
	for _, phase := range []RotationPhase{RotationPendingMetadataPublished, RotationSealedSynced, RotationSealFileClosed, RotationSealedMetadataPublished} {
		t.Run(phase.String(), func(t *testing.T) {
			dir := t.TempDir()
			e := newEngineAt(t, dir, 1)
			if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("one")}}}}}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("crash cut")
			err := e.Rotate(func(got RotationPhase) error {
				if got == phase {
					return injected
				}
				return nil
			})
			if phase == RotationPendingMetadataPublished {
				if !errors.Is(err, injected) {
					t.Fatalf("rotate error=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if closeErr := e.Close(); !errors.Is(closeErr, injected) {
					t.Fatalf("close error=%v", closeErr)
				}
			}
			_ = e.Close()
			e, err = openTestEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			location, term, compacted, ok, err := e.LookupExact(1, 1)
			if err != nil || compacted || !ok || term != 1 || location.Bytes == 0 {
				t.Fatalf("lookup=%+v term=%d compacted=%v ok=%v err=%v", location, term, compacted, ok, err)
			}
		})
	}
}

func TestEngineEmptyRotationIsCanonical(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir)
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if e.log.state.Segments[0].State != SegmentSealed {
		t.Fatalf("sealed=%+v", e.log.state.Segments[0])
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
}

func TestFrozenSegmentSummaryIsStableDuringNewActiveWrites(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("one")}}}}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	e.sealBuildHookTest = func() { close(entered); <-release }
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 2, Term: 2, Data: []byte("two")}}}}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	e.sealBuildHookTest = nil
	first, _, _, ok, err := e.LookupExact(1, 1)
	if err != nil || !ok || first.SegmentID != 1 {
		t.Fatalf("first=%+v ok=%v err=%v", first, ok, err)
	}
	second, _, _, ok, err := e.LookupExact(1, 2)
	if err != nil || !ok || second.SegmentID != 2 {
		t.Fatalf("second=%+v ok=%v err=%v", second, ok, err)
	}
}

func TestSealerDoesNotOpenHistoricalSegments(t *testing.T) {
	e := newReservedEngine(t, 1)
	for index := uint64(1); index <= 4; index++ {
		if err := e.PersistWave(Wave{ID: waveID(index), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: index, Term: 1, Data: []byte{byte(index)}}}}}}); err != nil {
			t.Fatal(err)
		}
		if index < 4 {
			if err := e.Rotate(nil); err != nil {
				t.Fatal(err)
			}
			if err := e.WaitSeal(); err != nil {
				t.Fatal(err)
			}
		}
	}
	var opened []string
	e.sealOpenHookTest = func(path string) { opened = append(opened, filepath.Base(path)) }
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if len(opened) != 1 || opened[0] != segmentFileName(e.log.state.Segments[len(e.log.state.Segments)-1].FileID) {
		t.Fatalf("sealer opens=%v", opened)
	}
}

func TestSegmentSummaryCapacityRejectsBeforeMutation(t *testing.T) {
	e := newReservedEngine(t, 1, 2)
	e.activeBuild = &segmentBuildArena{groups: make([]segmentGroupBuild, 0, 1)}
	beforeOffset, beforeSequence, beforeEvents := e.log.activeOffset, e.sequence, len(e.log.events)
	for retry := 0; retry < 2; retry++ {
		err := e.PersistWave(Wave{ID: waveID(uint64(1 + retry)), Batches: []ReadyBatch{
			{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("one")}}},
			{GroupID: 2, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("two")}}},
		}})
		if !errors.Is(err, ErrBounds) {
			t.Fatalf("retry %d error=%v", retry, err)
		}
		if e.log.activeOffset != beforeOffset || e.sequence != beforeSequence || len(e.log.events) != beforeEvents || e.groups[2].buildSegmentID != 0 {
			t.Fatalf("retry mutated offset=%d sequence=%d events=%d tag=%d", e.log.activeOffset, e.sequence, len(e.log.events), e.groups[2].buildSegmentID)
		}
	}
}

func TestReserveMovesActiveSegmentSummarySlots(t *testing.T) {
	e := newReservedEngine(t, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 7}}}}}); err != nil {
		t.Fatal(err)
	}
	old := e.activeBuild
	if err := e.Reserve(1<<20, cap(old.groups)+1, 4096); err != nil {
		t.Fatal(err)
	}
	if e.activeBuild == old || len(e.activeBuild.groups) != 1 || e.activeBuild.groups[0].GroupID != 1 {
		t.Fatalf("rebuilt arena=%+v old=%p new=%p", e.activeBuild.groups, old, e.activeBuild)
	}
	if group := e.groups[1]; group.buildSegmentID != e.log.state.ActiveID || group.buildSlot != 0 {
		t.Fatalf("group slot tag=%d slot=%d active=%d", group.buildSegmentID, group.buildSlot, e.log.state.ActiveID)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestControlOnlySummaryAndCumulativeSequenceAcrossRotations(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("one")}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	hard := HardState{Term: 1, Vote: 1, Commit: 1}
	if err := e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 1, Hard: &hard}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	meta, ok := e.Metadata(1)
	if !ok || meta.LastIndex != 1 || meta.Hard != hard || e.sequence != 2 {
		t.Fatalf("meta=%+v ok=%v sequence=%d", meta, ok, e.sequence)
	}
	for _, segment := range e.log.state.Segments {
		f, openErr := os.Open(segmentPath(dir, segment.FileID))
		if openErr != nil {
			t.Fatal(openErr)
		}
		_, header, _, _, readErr := readSealedSealedMetadata(f, segment, e.log.state.SegmentCapacity, e.log.state.LogID, segment.ID-1, segment.PreviousHash, e.authKey)
		_ = f.Close()
		if readErr != nil || header.LastSequence != min(segment.ID, 2) {
			t.Fatalf("segment=%d sequence=%d err=%v", segment.ID, header.LastSequence, readErr)
		}
	}
}

func TestEntryTypesRoundTripAndCompactCost(t *testing.T) {
	dir := t.TempDir()
	e, err := createTestEngine(dir)
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
	e, err = openTestEngine(dir)
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
	e, err := createTestEngine(dir)
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
	state := readTestMetadata(t, dir, testLogID, testAuthKey)
	path := segmentPath(dir, state.Segments[0].FileID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	footer, header, runs, _, err := readSealedSealedMetadata(f, state.Segments[0], state.SegmentCapacity, state.LogID, 0, [32]byte{}, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Inline.ExtentOffset == 0 {
		t.Fatalf("runs=%#v", runs)
	}
	runs[0].Inline.Term++
	directory, retryBytes, retryCount, err := appendSealedDirectory(nil, runs)
	if err != nil {
		t.Fatal(err)
	}
	if retryBytes != header.RetryBytes || retryCount != header.RetryCount {
		t.Fatalf("retry table geometry changed: bytes=%d/%d count=%d/%d", retryBytes, header.RetryBytes, retryCount, header.RetryCount)
	}
	if len(directory) != int(header.DirectoryBytes) {
		t.Fatalf("directory size changed: %d != %d", len(directory), header.DirectoryBytes)
	}
	headerBytes, err := marshalSealedIndexHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(directory, int64(footer.IndexOffset+sealedIndexHeaderBytes)); err != nil {
		t.Fatal(err)
	}
	var segmentHeaderData [segmentHeaderBytes]byte
	if _, err = f.ReadAt(segmentHeaderData[:], 0); err != nil {
		t.Fatal(err)
	}
	segmentHeader, err := unmarshalSegmentHeader(segmentHeaderData[:])
	if err != nil {
		t.Fatal(err)
	}
	top := append(headerBytes[:], directory...)
	footer.Auth = segmentSealedMetadataMAC(testAuthKey, segmentHeader, top, footer)
	stat, _ := f.Stat()
	if _, err = f.WriteAt(marshalSegmentFooter(footer), stat.Size()-segmentFooterBytes); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = openTestEngine(dir)
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
	e, err := createTestEngine(dir)
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
	state := readTestMetadata(t, dir, testLogID, testAuthKey)
	path := segmentPath(dir, state.Segments[0].FileID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	footerBytes := make([]byte, segmentFooterBytes)
	if _, err = f.ReadAt(footerBytes, stat.Size()-segmentFooterBytes); err != nil {
		t.Fatal(err)
	}
	footer, err := unmarshalSegmentFooter(footerBytes)
	if err != nil {
		t.Fatal(err)
	}
	footer.Records++
	topBytes := make([]byte, sealedIndexHeaderBytes)
	if _, err = f.ReadAt(topBytes, int64(footer.IndexOffset)); err != nil {
		t.Fatal(err)
	}
	indexHeader, err := unmarshalSealedIndexHeader(topBytes)
	if err != nil {
		t.Fatal(err)
	}
	topBytes = make([]byte, sealedIndexHeaderBytes+int(indexHeader.DirectoryBytes))
	if _, err = f.ReadAt(topBytes, int64(footer.IndexOffset)); err != nil {
		t.Fatal(err)
	}
	segmentBytes := make([]byte, segmentHeaderBytes)
	if _, err = f.ReadAt(segmentBytes, 0); err != nil {
		t.Fatal(err)
	}
	segment, err := unmarshalSegmentHeader(segmentBytes)
	if err != nil {
		t.Fatal(err)
	}
	footer.Auth = segmentSealedMetadataMAC(testAuthKey, segment, topBytes, footer)
	if _, err = f.WriteAt(marshalSegmentFooter(footer), stat.Size()-segmentFooterBytes); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = openTestEngine(dir)
	if !errors.Is(err, ErrCorrupt) {
		if err == nil {
			_ = e.Close()
		}
		t.Fatalf("metadata Open=%v", err)
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
	verifier := &Engine{log: e.log, groups: make(map[uint64]*engineGroup), waves: make(map[WaveID]waveState), authKey: testAuthKey, authMAC: hmac.New(sha256.New, testAuthKey[:])}
	if _, _, err = verifyWaveFrames(e.log.active, e.log.activeOffset, 1, verifier); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify=%v", err)
	}
}

func TestReadyIdentityAndSharedBatchBlobRoundTrip(t *testing.T) {
	dir := t.TempDir()
	e, err := createTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 64, 16); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(7, 16); err != nil {
		t.Fatal(err)
	}
	blob := []byte("ciphertext-and-tag!!!")
	digest := [16]byte{4}
	w := Wave{ID: waveID(9), Blob: blob, Batches: []ReadyBatch{{GroupID: 7, NodeIncarnation: 3, ReadyID: 1, ReadyDigest: digest, Entries: []Entry{{Index: 1, Term: 1, ExtentID: 1, ExtentBytes: uint64(len(blob)), DataOffset: 0, DataBytes: 3}, {Index: 2, Term: 1, Type: pb.EntryConfChangeV2, ExtentID: 1, ExtentBytes: uint64(len(blob)), DataOffset: 3, DataBytes: 2}}}}}
	if err = validateWaveExtents(w.Batches, uint64(len(w.Blob))); err != nil {
		t.Fatalf("extents: %v", err)
	}
	if _, err = validateBatch(e.groups[7], &w.Batches[0]); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err = e.PersistWave(w); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = openTestEngine(dir)
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

func TestWaveExtentCanonicalGeometry(t *testing.T) {
	valid := []ReadyBatch{{GroupID: 1, Entries: []Entry{
		{Index: 1, Term: 1, ExtentID: 1, ExtentOffset: 0, ExtentBytes: 20, DataOffset: 0, DataBytes: 4},
		{Index: 2, Term: 1, ExtentID: 2, ExtentOffset: 20, ExtentBytes: 18, DataOffset: 0, DataBytes: 2},
	}}}
	if err := validateWaveExtents(valid, 38); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*[]ReadyBatch, *uint64){
		"first offset":   func(b *[]ReadyBatch, _ *uint64) { (*b)[0].Entries[0].ExtentOffset = 1 },
		"id skip":        func(b *[]ReadyBatch, _ *uint64) { (*b)[0].Entries[1].ExtentID = 3 },
		"extent gap":     func(b *[]ReadyBatch, n *uint64) { (*b)[0].Entries[1].ExtentOffset++; *n++ },
		"data gap":       func(b *[]ReadyBatch, _ *uint64) { (*b)[0].Entries[1].DataOffset = 1 },
		"trailing bytes": func(_ *[]ReadyBatch, n *uint64) { *n++ },
	} {
		t.Run(name, func(t *testing.T) {
			batches := []ReadyBatch{{GroupID: 1, Entries: append([]Entry(nil), valid[0].Entries...)}}
			bytes := uint64(38)
			mutate(&batches, &bytes)
			if err := validateWaveExtents(batches, bytes); !errors.Is(err, ErrRaftState) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestNodeBatchFrameSpaceAccounting(t *testing.T) {
	measure := func(groups, entriesPerGroup int) int {
		t.Helper()
		e := &Engine{
			groups:       make(map[uint64]*engineGroup, groups),
			frameBuf:     make([]byte, 0, 4096),
			eventScratch: make([]segmentEvent, 0, groups*(entriesPerGroup+3)+1),
		}
		wave := Wave{ID: WaveID{1}, Batches: make([]ReadyBatch, groups), Blob: make([]byte, groups*entriesPerGroup+16)}
		for group := 1; group <= groups; group++ {
			e.groups[uint64(group)] = &engineGroup{GroupState: GroupState{Checkpoint: Checkpoint{Index: 1, Term: 1}, Hard: HardState{Term: 1, Commit: 1}, Entries: make([]EntryLocation, 0, 64)}}
			entries := make([]Entry, entriesPerGroup)
			for i := range entries {
				entries[i] = Entry{Index: uint64(i + 2), Term: 2, ExtentID: 1, ExtentBytes: uint64(len(wave.Blob)), DataOffset: uint64((group-1)*entriesPerGroup + i), DataBytes: 1}
			}
			wave.Batches[group-1] = ReadyBatch{GroupID: uint64(group), NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}, Hard: &HardState{Term: 2, Vote: 1, Commit: uint64(entriesPerGroup + 1)}, Entries: entries}
		}
		frame, _, _, err := e.prepareWave(wave, false)
		if err != nil {
			t.Fatal(err)
		}
		return len(frame)
	}
	if frame := measure(1, 32); frame != 370 {
		t.Fatalf("one group frame = %d, want 370", frame)
	}
	if frame := measure(4, 8); frame != 442 {
		t.Fatalf("four groups frame = %d, want 442", frame)
	}
}

func TestPrepareWaveEncodeOnlyNeedsNoEngineState(t *testing.T) {
	e := &Engine{frameBuf: make([]byte, 0, 256), eventScratch: make([]segmentEvent, 0, 4)}
	wave := Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}
	if frame, _, _, err := e.prepareWave(wave, false); err != nil || len(frame) == 0 {
		t.Fatalf("encode-only frame=%d err=%v", len(frame), err)
	}
	if _, _, _, err := e.prepareWave(wave, true); !errors.Is(err, ErrBounds) {
		t.Fatalf("stateful incomplete Engine error=%v", err)
	}
}

func TestAuthenticatedEngineRejectsRecomputedSealedIndexCRC(t *testing.T) {
	dir := t.TempDir()
	key := [32]byte{1, 2, 3}
	e, err := CreateEngineAuthenticated(dir, testLogID, key, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reserve(4096, 16, 8); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 8); err != nil {
		t.Fatal(err)
	}
	if err = e.PersistWave(Wave{ID: WaveID{1}, Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	state := readTestMetadata(t, dir, testLogID, key)
	if len(state.Segments) != 1 {
		t.Fatalf("state: %#v", state)
	}
	path := segmentPath(dir, state.Segments[0].FileID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	index := make([]byte, state.Segments[0].IndexBytes)
	if _, err = f.ReadAt(index, int64(state.Segments[0].IndexOffset)); err != nil {
		t.Fatal(err)
	}
	index[45] ^= 1 // Ready digest remains structurally valid.
	putCRC(index)
	if _, err = f.WriteAt(index, int64(state.Segments[0].IndexOffset)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if opened, openErr := OpenEngineAuthenticated(dir, testLogID, key); opened != nil || !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("authenticated Open = %#v, %v", opened, openErr)
	}
}

func TestAuthenticatedEngineRejectsRecomputedActiveSHAAndCRC(t *testing.T) {
	dir := t.TempDir()
	key := [32]byte{9, 8, 7}
	e, err := CreateEngineAuthenticated(dir, testLogID, key, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Reserve(4096, 16, 8)
	_ = e.ReserveGroup(1, 8)
	if err = e.PersistWave(Wave{ID: WaveID{1}, Batches: []ReadyBatch{{GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1}}}}); err != nil {
		t.Fatal(err)
	}
	activeFileID := e.log.state.ActiveFileID
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	path := segmentPath(dir, activeFileID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	frame := data[segmentHeaderBytes:]
	frame[len(frame)-1] ^= 1
	forged := sha256.Sum256(frame[72:])
	copy(frame[40:72], forged[:])
	binary.LittleEndian.PutUint32(frame[32:36], crc32.Checksum(frame[40:], crcTable))
	binary.LittleEndian.PutUint32(frame[36:40], crc32.Checksum(frame[:36], crcTable))
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if opened, openErr := OpenEngineAuthenticated(dir, testLogID, key); opened != nil || !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("authenticated active Open = %#v, %v", opened, openErr)
	}
}

func TestAuthenticatedIncarnationRotateRecovery(t *testing.T) {
	dir := t.TempDir()
	key := [32]byte{4, 5, 6}
	e, err := CreateEngineAuthenticated(dir, testLogID, key, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Reserve(4096, 16, 8)
	_ = e.ReserveGroup(7, 8)
	if err = e.PersistWave(Wave{ID: WaveID{1}, Batches: []ReadyBatch{{GroupID: 7, BeginIncarnation: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenEngineAuthenticated(dir, testLogID, key)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	state, ok := e.Group(7)
	if !ok || state.NodeIncarnation != 1 || state.ReadyID != 0 {
		t.Fatalf("recovered incarnation = %#v, %v", state, ok)
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
