package raftstore

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftsim"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func driveNodeReady(t *testing.T, node *raftmodel.Node) {
	t.Helper()
	var workspace raftmodel.NormalApplyBatchWorkspace
	for {
		has, err := node.HasReady()
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			return
		}
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("CaptureReady = %v, %v", captured, err)
		}
		if err := node.PersistReady(); err != nil {
			t.Fatalf("PersistReady: %v", err)
		}
		if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatalf("DrainMessages: %v", err)
		}
		if err := node.InstallSnapshot(); err != nil {
			t.Fatalf("InstallSnapshot: %v", err)
		}
		if err := node.ApplyCommitted(
			&workspace, func(raftmodel.AppliedNormalBatch) error { return nil },
		); err != nil {
			t.Fatalf("ApplyCommitted: %v", err)
		}
		if _, err := node.RecordReadStates(); err != nil {
			t.Fatalf("RecordReadStates: %v", err)
		}
		if err := node.AdvanceReady(); err != nil {
			t.Fatalf("AdvanceReady: %v", err)
		}
	}
}

func TestActualRaftModelNodeRestartOnDiskStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	options := testOptions()
	memoryBootstrap, err := raftsim.NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := memoryBootstrap.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := Bootstrap{TopologyRecoveryEpoch: 11, Snapshot: snapshot}
	store, err := Create(path, testIdentity(), testKey(), bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	machine, err := raftsim.NewMemoryMachine([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	node, err := raftmodel.NewNode(1, incarnation, store, machine)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Campaign(); err != nil {
		t.Fatal(err)
	}
	driveNodeReady(t, node)
	if err := node.Propose([]byte("durable-command")); err != nil {
		t.Fatal(err)
	}
	driveNodeReady(t, node)
	if machine.Applied() <= 1 {
		t.Fatalf("proposal was not applied: %d", machine.Applied())
	}
	appliedBefore := machine.Applied()
	lastBefore, err := store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), bootstrap.TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("Open restart: %v", err)
	}
	defer reopened.Close()
	newIncarnation, err := reopened.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if newIncarnation <= incarnation {
		t.Fatalf("incarnation reused: %d after %d", newIncarnation, incarnation)
	}
	restarted, err := raftmodel.NewNode(1, newIncarnation, reopened, machine)
	if err != nil {
		t.Fatalf("NewNode restart: %v", err)
	}
	if restarted.Published().Applied != appliedBefore {
		t.Fatalf("published applied = %d, want %d", restarted.Published().Applied, appliedBefore)
	}
	lastAfter, err := reopened.LastIndex()
	if err != nil || lastAfter != lastBefore {
		t.Fatalf("last after restart = %d, %v; want %d", lastAfter, err, lastBefore)
	}
	driveNodeReady(t, restarted)
}

func TestDiskStoreDifferentialAgainstMemoryStorage(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	oracle := raft.NewMemoryStorage()
	bootstrap := testBootstrap().Snapshot
	if err := oracle.ApplySnapshot(cloneSnapshot(bootstrap)); err != nil {
		t.Fatal(err)
	}
	if err := oracle.SetHardState(hard(1, 1)); err != nil {
		t.Fatal(err)
	}
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "a"), entry(3, 2, "b"), entry(4, 2, "c")}},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(3, 3), Entries: []*pb.Entry{entry(3, 3, "B"), entry(4, 3, "C")}},
		{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(4, 5), Entries: []*pb.Entry{entry(5, 4, "d")}},
	}
	for _, batch := range batches {
		if err := store.Persist(batch); err != nil {
			t.Fatal(err)
		}
		if err := oracle.Append(cloneEntries(batch.Entries)); err != nil {
			t.Fatal(err)
		}
		if err := oracle.SetHardState(cloneHardState(batch.HardState)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	storeHard, storeConf, err := reopened.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	oracleHard, oracleConf, err := oracle.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(storeHard, oracleHard) || storeConf.Equivalent(oracleConf) != nil {
		t.Fatalf("state differs: disk=%v/%v memory=%v/%v", storeHard, storeConf, oracleHard, oracleConf)
	}
	first, _ := reopened.FirstIndex()
	last, _ := reopened.LastIndex()
	diskEntries, err := reopened.Entries(first, last+1, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	memoryEntries, err := oracle.Entries(first, last+1, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if len(diskEntries) != len(memoryEntries) {
		t.Fatalf("entry lengths %d != %d", len(diskEntries), len(memoryEntries))
	}
	for index := range diskEntries {
		if !proto.Equal(diskEntries[index], memoryEntries[index]) {
			t.Fatalf("entry %d differs", index)
		}
	}
}

func TestInteriorRecordCorruptionFailsClosed(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, recordPrefixBytes)
	if _, err := file.ReadAt(prefix, HeaderBytes); err != nil {
		t.Fatal(err)
	}
	envelope, err := inspectRecordPrefix(prefix, store.header, store.options)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(HeaderBytes + envelope.total + recordPrefixBytes + len(store.header.keyID) + 1)
	byteValue := []byte{0}
	if _, err := file.ReadAt(byteValue, offset); err != nil {
		t.Fatal(err)
	}
	byteValue[0] ^= 0x80
	if _, err := file.WriteAt(byteValue, offset); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open corrupt interior = %v", err)
	}
}

func TestBoundsPreflightBeforeEncodingAndTermMonotonicity(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	tooMany := make([]*pb.Entry, MaxReadyEntries+1)
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, Entries: tooMany}); !errors.Is(err, ErrBounds) {
		t.Fatalf("too many entries = %v", err)
	}
	large := make([]byte, raftmodel.MaxProposalBytes)
	oversizedTotal := []*pb.Entry{entry(2, 2, ""), entry(3, 2, ""), entry(4, 2, ""), entry(5, 2, ""), entry(6, 2, "")}
	for _, value := range oversizedTotal {
		value.Data = large
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, Entries: oversizedTotal}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized total = %v", err)
	}
	decreasing := []*pb.Entry{entry(2, 3, "a"), entry(3, 2, "b")}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, Entries: decreasing}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("decreasing terms = %v", err)
	}
}

func TestBootstrapSnapshotPresenceRoundTripsExactly(t *testing.T) {
	for _, explicitFalse := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil", true: "explicit-false"}[explicitFalse], func(t *testing.T) {
			bootstrap := testBootstrap()
			if explicitFalse {
				bootstrap.Snapshot.Metadata.ConfState.AutoLeave = boolPointer(false)
			}
			path := filepath.Join(t.TempDir(), "presence.wal")
			options := testOptions()
			store, err := Create(path, testIdentity(), testKey(), bootstrap, options)
			if err != nil {
				t.Fatal(err)
			}
			created, err := store.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(created, bootstrap.Snapshot) {
				t.Fatalf("Create snapshot presence changed: %v vs %v", created, bootstrap.Snapshot)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, testIdentity(), bootstrap.TopologyRecoveryEpoch, testKey(), options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			recovered, err := reopened.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(recovered, bootstrap.Snapshot) {
				t.Fatalf("Open snapshot presence changed: %v vs %v", recovered, bootstrap.Snapshot)
			}
		})
	}
}

func TestOpenCrashCutsSelectOnlyAuthenticatedCurrentImage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash-cuts.wal")
	options := testOptions()
	options.ops.sync = func(*os.File) error { return nil }
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "selected")}, MustSync: true}
	empty, payload, semanticDigest, delta, err := store.prepareBatchLocked(batch)
	if err != nil || empty {
		t.Fatalf("prepare batch empty=%v err=%v", empty, err)
	}
	record, recordDigest, _, err := marshalRecord(recordKindReady, 1, store.current.recordSequence+1, incarnation, batch.ReadyID, store.current.chainDigest, payload, store.header, store.options)
	if err != nil {
		t.Fatal(err)
	}
	recordOffset := store.current.walEnd
	if _, err := store.file.WriteAt(record, recordOffset); err != nil {
		t.Fatal(err)
	}
	if err := store.file.Sync(); err != nil {
		t.Fatal(err)
	}
	next := store.current
	next.activeSlot = 1 - store.current.activeSlot
	next.generation++
	next.walEnd += int64(len(record))
	next.recordSequence++
	next.chainDigest = recordDigest
	next.hard = cloneHardState(delta.hard)
	next.last = delta.last
	next.retryPresent = true
	next.retry = retryKey{incarnation: incarnation, readyID: batch.ReadyID}
	next.retryDigest = semanticDigest
	nextBytes, _, err := marshalCurrentSlot(next, next.activeSlot, store.header)
	if err != nil {
		t.Fatal(err)
	}
	targetOffset := int64(StaticHeaderBytes + next.activeSlot*CurrentSlotBytes)
	oldTarget := make([]byte, CurrentSlotBytes)
	if _, err := store.file.ReadAt(oldTarget, targetOffset); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	assertOldImage := func(prefix int) {
		t.Helper()
		file, openErr := os.OpenFile(path, os.O_RDWR, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if _, openErr = file.WriteAt(oldTarget, targetOffset); openErr == nil && prefix != 0 {
			_, openErr = file.WriteAt(nextBytes[:prefix], targetOffset)
		}
		if closeErr := file.Close(); openErr == nil {
			openErr = closeErr
		}
		if openErr != nil {
			t.Fatalf("install cut %d: %v", prefix, openErr)
		}
		reopened, openErr := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
		if openErr != nil {
			t.Fatalf("Open cut %d: %v", prefix, openErr)
		}
		last, lastErr := reopened.LastIndex()
		state, _, stateErr := reopened.InitialState()
		currentIncarnation := reopened.CurrentIncarnation()
		if closeErr := reopened.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if lastErr != nil || stateErr != nil || last != 1 || state.GetTerm() != 1 || state.GetCommit() != 1 ||
			currentIncarnation != incarnation {
			t.Fatalf("cut %d exposed new image: last=%d state=%v lastErr=%v stateErr=%v", prefix, last, state, lastErr, stateErr)
		}
	}
	assertOldImage(0) // A fully synced orphan record is not selected.
	for prefix := 1; prefix < CurrentSlotBytes; prefix++ {
		assertOldImage(prefix)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(nextBytes, targetOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	last, err := reopened.LastIndex()
	state, _, stateErr := reopened.InitialState()
	if err != nil || stateErr != nil || last != 2 || state.GetTerm() != 2 || state.GetCommit() != 2 || reopened.CurrentIncarnation() != incarnation {
		_ = reopened.Close()
		t.Fatalf("full current did not select new image: last=%d state=%v incarnation=%d err=%v/%v", last, state, reopened.CurrentIncarnation(), err, stateErr)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	// The record-ordering barrier is what makes a fully authenticated selector
	// safe. If a selector can reach storage ahead of any incomplete form of its
	// record, recovery must fail closed rather than silently roll back.
	recordCuts := []int{0, 1, recordPrefixBytes - 1, recordPrefixBytes, len(record) - 1}
	for _, prefix := range recordCuts {
		file, err = os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		blank := make([]byte, len(record))
		if _, err = file.WriteAt(blank, recordOffset); err == nil && prefix != 0 {
			_, err = file.WriteAt(record[:prefix], recordOffset)
		}
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatalf("install record cut %d: %v", prefix, err)
		}
		if _, err = Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("selected record cut %d rolled back instead of failing closed: %v", prefix, err)
		}
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt(record, recordOffset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := []byte{0}
	corruptOffset := recordOffset + recordPrefixBytes + int64(len(store.header.keyID))
	if _, err := file.ReadAt(corrupt, corruptOffset); err == nil {
		corrupt[0] ^= 0x80
		_, err = file.WriteAt(corrupt, corruptOffset)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("selected record corruption rolled back instead of failing closed: %v", err)
	}
}
