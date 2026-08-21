package raftstore

import (
	"bytes"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testIdentity() Identity {
	identity := Identity{
		Distribution: "orders", Shard: "0000-7fff", AllocationGeneration: 7, MemberID: 1,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = byte(index + 1)
	}
	for index := range identity.ClusterIncarnation {
		identity.ClusterIncarnation[index] = byte(index + 17)
	}
	for index := range identity.ShardIncarnation {
		identity.ShardIncarnation[index] = byte(index + 33)
	}
	for index := range identity.GroupID {
		identity.GroupID[index] = byte(index + 49)
	}
	for index := range identity.StoreID {
		identity.StoreID[index] = byte(index + 65)
	}
	return identity
}

func testKey() Key {
	key := Key{ID: "test-key-1", Wrapped: []byte("opaque-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	return key
}

func testBootstrap() Bootstrap {
	index, term := uint64(1), uint64(1)
	return Bootstrap{TopologyRecoveryEpoch: 11, Snapshot: &pb.Snapshot{
		Data:     []byte("bootstrap-state"),
		Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}}},
	}}
}

func testOptions() Options {
	return Options{
		MaxFileBytes: 160 << 20, MaxRecordBytes: DefaultMaxRecordBytes, MaxRecords: 1024,
		MaxEntries: 8192, MaxLiveBytes: MinimumReadyLiveBytes,
		random: bytes.NewReader(testRandomBytes()),
		ops: fileOps{
			preallocate:     func(file *os.File, size int64) error { return file.Truncate(size) },
			ensureAllocated: func(*os.File, int64) error { return nil },
		},
	}
}

func testRandomBytes() []byte {
	result := make([]byte, 64)
	for index := range result {
		result[index] = byte(index + 1)
	}
	return result
}

func createTestStore(t testing.TB) (string, *Store, Options) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft.wal")
	options := testOptions()
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return path, store, options
}

func entry(index, term uint64, data string) *pb.Entry {
	typeValue := pb.EntryNormal
	return &pb.Entry{Index: uint64Pointer(index), Term: uint64Pointer(term), Type: &typeValue, Data: []byte(data)}
}

func hard(term, commit uint64) *pb.HardState {
	return &pb.HardState{Term: uint64Pointer(term), Vote: uint64Pointer(1), Commit: uint64Pointer(commit)}
}

func TestCreatePersistOpenAndDetachedStorage(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil || incarnation != 1 {
		t.Fatalf("BeginIncarnation = %d, %v", incarnation, err)
	}
	first := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "a")}, MustSync: true}
	if err := store.Persist(first); err != nil {
		t.Fatalf("Persist first: %v", err)
	}
	syncs := store.SyncCount()
	if err := store.Persist(first); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if store.SyncCount() != syncs {
		t.Fatalf("exact retry synced: %d -> %d", syncs, store.SyncCount())
	}
	changed := first
	changed.Entries = []*pb.Entry{entry(2, 2, "changed")}
	if err := store.Persist(changed); !errors.Is(err, ErrRetryConflict) || !errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("changed retry = %v", err)
	}
	second := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(3, 2), Entries: []*pb.Entry{entry(3, 3, "uncommitted")}}
	if err := store.Persist(second); err != nil {
		t.Fatalf("Persist second: %v", err)
	}
	third := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(4, 3), Entries: []*pb.Entry{entry(3, 4, "replacement")}}
	if err := store.Persist(third); err != nil {
		t.Fatalf("replace suffix: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()
	if reopened.CurrentIncarnation() != incarnation {
		t.Fatalf("incarnation = %d", reopened.CurrentIncarnation())
	}
	state, conf, err := reopened.InitialState()
	if err != nil || state.GetTerm() != 4 || state.GetCommit() != 3 || conf.Equivalent(&pb.ConfState{Voters: []uint64{1}}) != nil {
		t.Fatalf("InitialState = %v %v %v", state, conf, err)
	}
	entries, err := reopened.Entries(2, 4, math.MaxUint64)
	if err != nil || len(entries) != 2 || string(entries[0].GetData()) != "a" || string(entries[1].GetData()) != "replacement" {
		t.Fatalf("Entries = %v, %v", entries, err)
	}
	entries[0].Data[0] = 'X'
	entries[0].Index = uint64Pointer(99)
	state.Term = uint64Pointer(99)
	conf.Voters[0] = 99
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Data[0] = 'X'
	snapshot.Metadata.ConfState.Voters[0] = 99
	again, _ := reopened.Entries(2, 3, math.MaxUint64)
	againState, againConf, _ := reopened.InitialState()
	againSnapshot, _ := reopened.Snapshot()
	if again[0].GetIndex() != 2 || string(again[0].GetData()) != "a" || againState.GetTerm() != 4 || againConf.GetVoters()[0] != 1 || string(againSnapshot.GetData()) != "bootstrap-state" {
		t.Fatal("caller mutation reached store-owned values")
	}
}

func TestEmptyPersistIsZeroWriteAndAllowsReadyIDGap(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	writes, syncs := 0, 0
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		return file.WriteAt(data, offset)
	}
	store.options.ops.sync = func(file *os.File) error { syncs++; return file.Sync() }
	before := store.SyncCount()
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	if writes != 0 || syncs != 0 || store.SyncCount() != before {
		t.Fatalf("empty Persist did writes=%d syncs=%d", writes, syncs)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}}); err != nil {
		t.Fatalf("durable Ready after empty ID gap: %v", err)
	}
}

func TestBeginIncarnationIsDurableAndNeverReused(t *testing.T) {
	path, store, options := createTestStore(t)
	first, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.CurrentIncarnation() != first {
		t.Fatalf("CurrentIncarnation = %d", reopened.CurrentIncarnation())
	}
	second, err := reopened.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if second != first+1 {
		t.Fatalf("next incarnation = %d after %d", second, first)
	}
}

func TestCapacityProfileIsSealedAndSurvivesRestart(t *testing.T) {
	path, store, options := createTestStore(t)
	want := CapacityProfile{
		Format:       CapacityFormatStatic,
		LogBaseIndex: 1,
		MaxEntries:   uint64(options.MaxEntries),
	}
	beforeIncarnation := store.CurrentIncarnation()
	beforeRemaining := store.RemainingBytes()
	beforeSyncs := store.SyncCount()
	if got, err := store.CapacityProfile(); err != nil || got != want {
		t.Fatalf("CapacityProfile = %+v, %v, want %+v", got, err, want)
	}
	if store.CurrentIncarnation() != beforeIncarnation ||
		store.RemainingBytes() != beforeRemaining || store.SyncCount() != beforeSyncs {
		t.Fatal("CapacityProfile mutated WAL state")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := store.CapacityProfile(); got != (CapacityProfile{}) || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed CapacityProfile = %+v, %v, want zero, ErrClosed", got, err)
	}

	reopened, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.CapacityProfile(); err != nil || got != want {
		t.Fatalf("reopened CapacityProfile = %+v, %v, want %+v", got, err, want)
	}
}

func TestCreateFromNewerImmutableSnapshotBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "learner.wal")
	options := testOptions()
	index, term := uint64(101), uint64(7)
	base := Bootstrap{TopologyRecoveryEpoch: 11, Snapshot: &pb.Snapshot{
		Data: []byte("certified-snapshot-base"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}},
		},
	}}
	store, err := Create(path, testIdentity(), testKey(), base, options)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := store.CapacityProfile()
	if err != nil || profile.Format != CapacityFormatImmutableBase || profile.LogBaseIndex != index {
		t.Fatalf("CapacityProfile = %+v, %v", profile, err)
	}
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	baseTerm, termErr := store.Term(index)
	hardState, confState, stateErr := store.InitialState()
	if first != index+1 || last != index || baseTerm != term || termErr != nil || stateErr != nil ||
		hardState.GetCommit() != index || hardState.GetTerm() != term ||
		confState.Equivalent(base.Snapshot.GetMetadata().GetConfState()) != nil {
		t.Fatalf("base recovery first=%d last=%d term=%d/%v hard=%v conf=%v/%v",
			first, last, baseTerm, termErr, hardState, confState, stateErr)
	}
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(8, index+1), Entries: []*pb.Entry{entry(index+1, 8, "tail")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), base.TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gotBase, _ := reopened.Snapshot()
	gotEntries, entriesErr := reopened.Entries(index+1, index+2, math.MaxUint64)
	if !bytes.Equal(gotBase.GetData(), base.Snapshot.GetData()) ||
		gotBase.GetMetadata().GetIndex() != index || entriesErr != nil || len(gotEntries) != 1 ||
		!bytes.Equal(gotEntries[0].GetData(), []byte("tail")) {
		t.Fatalf("recovered base=%v entries=%v/%v", gotBase, gotEntries, entriesErr)
	}
}

func TestSingleWriterWrongIdentityAndWrongKeyFailClosed(t *testing.T) {
	path, store, options := createTestStore(t)
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	wrongIdentity := testIdentity()
	wrongIdentity.AllocationGeneration++
	if _, err := Open(path, wrongIdentity, testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("wrong identity = %v", err)
	}
	wrongKey := testKey()
	wrongKey.Material[0] ^= 0xff
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, wrongKey, options); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("wrong key = %v", err)
	}
}

func TestWriterLockCoversHardLinkAliasAcrossProcess(t *testing.T) {
	if helperPath := os.Getenv("VIBEDB_RAFTSTORE_LOCK_HELPER"); helperPath != "" {
		store, err := Open(helperPath, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), testOptions())
		if err == nil {
			_ = store.Close()
			t.Fatal("subprocess acquired already locked WAL inode")
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("subprocess lock error = %v", err)
		}
		return
	}
	path, store, _ := createTestStore(t)
	alias := filepath.Join(filepath.Dir(path), "hardlink-alias.wal")
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestWriterLockCoversHardLinkAliasAcrossProcess$")
	command.Env = append(os.Environ(), "VIBEDB_RAFTSTORE_LOCK_HELPER="+alias)
	if output, err := command.CombinedOutput(); err != nil {
		_ = store.Close()
		t.Fatalf("lock helper: %v\n%s", err, output)
	}
}

func TestStorageErrorsMatchRaftContract(t *testing.T) {
	_, store, _ := createTestStore(t)
	if _, err := store.Entries(0, 1, 1); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted Entries = %v", err)
	}
	if _, err := store.Term(0); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted Term = %v", err)
	}
	if _, err := store.Term(2); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("unavailable Term = %v", err)
	}
}

func TestNilDefaultEntryTypeExactRetryAndOverlap(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	nilType := &pb.Entry{Index: uint64Pointer(2), Term: uint64Pointer(2), Data: []byte("x")}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{nilType}}
	if err := store.Persist(batch); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(batch); err != nil {
		t.Fatalf("exact nil-Type retry: %v", err)
	}
	overlap := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2), Entries: []*pb.Entry{nilType}}
	if err := store.Persist(overlap); err != nil {
		t.Fatalf("semantic nil/default overlap: %v", err)
	}
}

func TestCommitImageDeltaClearsTruncatedTailPointers(t *testing.T) {
	image := logImage{entries: []*pb.Entry{entry(2, 2, "a"), entry(3, 2, "b"), entry(4, 2, "c")}, first: 2, last: 4, hard: hard(2, 2), baseTerm: 1}
	old := image.entries
	delta := imageDelta{replace: true, prefixLength: 1, entries: []*pb.Entry{entry(3, 3, "B")}, last: 3, hard: hard(3, 2)}
	commitImageDelta(&image, delta)
	if old[2] != nil {
		t.Fatal("truncated tail pointer retained in backing array")
	}
}

func TestEntryTermMustNotDecreaseAcrossRetainedBoundary(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(3, 1), Entries: []*pb.Entry{entry(2, 3, "a")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(3, 1), Entries: []*pb.Entry{entry(3, 2, "b")}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("decreasing boundary term = %v", err)
	}
}

func TestPersistUsesActualRecordSizeAfterWorstCaseAdmissionCloses(t *testing.T) {
	options := testOptions()
	options.MaxRecordBytes = 64 << 10
	options.MaxLiveBytes = 64 << 10
	options.MaxFileBytes = HeaderBytes + MaxBootstrapRecordBytes + options.MaxLiveBytes + int64(options.MaxRecordBytes)
	options.allowSmallBounds = true
	options.MaxRecords = 1024
	path := filepath.Join(t.TempDir(), "tail.wal")
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	store.options.ops.sync = func(*os.File) error { return nil }
	readyID := uint64(1)
	term := uint64(2)
	for store.ReserveReady() == nil {
		if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: readyID, HardState: hard(term, 1)}); err != nil {
			t.Fatalf("fill Ready %d: %v", readyID, err)
		}
		readyID++
		term++
	}
	if store.RemainingBytes() >= int64(options.MaxRecordBytes) || store.RemainingBytes() < recordDamageGranule {
		t.Fatalf("unexpected tail %d", store.RemainingBytes())
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: readyID, HardState: hard(term, 1)}); err != nil {
		t.Fatalf("tiny durable Ready in reserved tail: %v", err)
	}
}

func TestConcurrentCreatePublishesExactlyOneLockedWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.wal")
	const creators = 8
	start := make(chan struct{})
	results := make(chan *Store, creators)
	errorsSeen := make(chan error, creators)
	var wait sync.WaitGroup
	for creator := 0; creator < creators; creator++ {
		wait.Add(1)
		go func(creator int) {
			defer wait.Done()
			options := testOptions()
			random := testRandomBytes()
			random[0] += byte(creator)
			options.random = bytes.NewReader(random)
			<-start
			store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
			if err != nil {
				errorsSeen <- err
				return
			}
			results <- store
		}(creator)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsSeen)
	var winner *Store
	for store := range results {
		if winner != nil {
			_ = store.Close()
			t.Fatal("more than one concurrent Create succeeded")
		}
		winner = store
	}
	if winner == nil {
		t.Fatalf("no Create succeeded; errors=%d", len(errorsSeen))
	}
	if len(errorsSeen) != creators-1 {
		_ = winner.Close()
		t.Fatalf("loser errors=%d, want %d", len(errorsSeen), creators-1)
	}
	options := testOptions()
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrLocked) {
		_ = winner.Close()
		t.Fatalf("Open while winner live = %v", err)
	}
	if err := winner.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("Open winner after handoff: %v", err)
	}
	defer reopened.Close()
	if reopened.Identity() != testIdentity() {
		t.Fatal("published WAL identity changed")
	}
}

func TestUnknownProtobufFieldsAreRejectedInsteadOfSilentlyDropped(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	unknown := []byte{0x98, 0x06, 0x01}
	unknownEntry := entry(2, 2, "x")
	unknownEntry.ProtoReflect().SetUnknown(unknown)
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, Entries: []*pb.Entry{unknownEntry}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown Entry fields = %v", err)
	}
	unknownSnapshot := &pb.Snapshot{}
	unknownSnapshot.ProtoReflect().SetUnknown(unknown)
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, Snapshot: unknownSnapshot}); !errors.Is(err, ErrUnsupportedSnapshot) {
		t.Fatalf("unknown Snapshot fields = %v", err)
	}
	unknownHard := &pb.HardState{}
	unknownHard.ProtoReflect().SetUnknown(unknown)
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: unknownHard}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("semantically empty HardState with unknown fields = %v", err)
	}
}

func TestPublicOptionsCoverEveryRaftModelReady(t *testing.T) {
	if MaxReadyEntries != raftmodel.MaxMessageEntries || MinimumReadyRecordBytes%recordDamageGranule != 0 ||
		MinimumReadyRecordBytes > DefaultMaxRecordBytes || MinimumReadyLiveBytes > DefaultMaxLiveBytes {
		t.Fatalf("Ready geometry drift: entries=%d record=%d live=%d", MaxReadyEntries, MinimumReadyRecordBytes, MinimumReadyLiveBytes)
	}
	valid := testOptions()
	if _, err := normalizeOptions(valid); err != nil {
		t.Fatalf("valid production bounds: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "record", mutate: func(o *Options) { o.MaxRecordBytes = MinimumReadyRecordBytes - recordDamageGranule }},
		{name: "entries", mutate: func(o *Options) { o.MaxEntries = MaxReadyEntries - 1 }},
		{name: "live-bytes", mutate: func(o *Options) { o.MaxLiveBytes = MinimumReadyLiveBytes - 1 }},
		{name: "records", mutate: func(o *Options) { o.MaxRecords = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if _, err := normalizeOptions(options); !errors.Is(err, ErrBounds) {
				t.Fatalf("undersized Options = %v", err)
			}
		})
	}
}

func TestTopologyRecoveryEpochIsRequiredAndComparedBeforeUse(t *testing.T) {
	path, store, options := createTestStore(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, testIdentity(), 0, testKey(), options); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero expected epoch = %v", err)
	}
	for _, epoch := range []uint64{testBootstrap().TopologyRecoveryEpoch - 1, testBootstrap().TopologyRecoveryEpoch + 1} {
		if _, err := Open(path, testIdentity(), epoch, testKey(), options); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("mismatched expected epoch %d = %v", epoch, err)
		}
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.BeginIncarnation(); err != nil {
		t.Fatalf("Begin after exact epoch: %v", err)
	}
}

func TestRecordExhaustionStillAcceptsEmptyReadyWithoutWritesOrSyncs(t *testing.T) {
	options := testOptions()
	options.MaxRecords = 2
	path := filepath.Join(t.TempDir(), "records-full.wal")
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	first := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}}
	if err := store.Persist(first); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveReady(); !errors.Is(err, ErrFull) {
		t.Fatalf("ReserveReady at record limit = %v", err)
	}
	writes, syncs := 0, 0
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		return file.WriteAt(data, offset)
	}
	store.options.ops.sync = func(file *os.File) error {
		syncs++
		return file.Sync()
	}
	empty := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, MustSync: true}
	if err := store.Persist(empty); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(empty); err != nil {
		t.Fatalf("exact empty retry: %v", err)
	}
	nonempty := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(3, 3), Entries: []*pb.Entry{entry(3, 3, "y")}}
	if err := store.Persist(nonempty); !errors.Is(err, ErrFull) {
		t.Fatalf("nonempty Ready after record exhaustion = %v", err)
	}
	if writes != 0 || syncs != 0 {
		t.Fatalf("post-exhaustion Ready path wrote=%d synced=%d", writes, syncs)
	}
}

func TestReserveReadyIncludesExactLogicalLiveByteHeadroom(t *testing.T) {
	options := testOptions()
	oneEntryBytes := int64(32 + len("x"))
	options.MaxLiveBytes = MinimumReadyLiveBytes + oneEntryBytes
	path := filepath.Join(t.TempDir(), "live-reserve.wal")
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveReady(); err != nil {
		t.Fatalf("exact live-byte reserve = %v", err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(3, 3), Entries: []*pb.Entry{entry(3, 3, "y")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveReady(); !errors.Is(err, ErrFull) {
		t.Fatalf("one byte-footprint below live reserve = %v", err)
	}
}

func TestReserveReadyIncludesExactLogicalEntryHeadroom(t *testing.T) {
	options := testOptions()
	options.MaxEntries = MaxReadyEntries + 1
	options.MaxLiveBytes = DefaultMaxLiveBytes
	options.MaxFileBytes = DefaultMaxFileBytes
	path := filepath.Join(t.TempDir(), "entry-reserve.wal")
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveReady(); err != nil {
		t.Fatalf("exact entry-count reserve = %v", err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(3, 3), Entries: []*pb.Entry{entry(3, 3, "y")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveReady(); !errors.Is(err, ErrFull) {
		t.Fatalf("one slot below entry reserve = %v", err)
	}
}
