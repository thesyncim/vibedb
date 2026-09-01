package raftstore

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestPersistGroupUsesOneBarrierAndPublishesOrderedImage(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	before := store.SyncCount()
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
			Entries: []*pb.Entry{entry(2, 2, "a")}, MustSync: true},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2)},
		{NodeIncarnation: incarnation, ReadyID: 3, Entries: []*pb.Entry{entry(3, 2, "b")}, MustSync: true},
	}
	if err := store.PersistGroup(batches); err != nil {
		t.Fatal(err)
	}
	if syncs := store.SyncCount() - before; syncs != 1 {
		t.Fatalf("group barriers = %d, want 1", syncs)
	}
	if last, err := store.LastIndex(); err != nil || last != 3 {
		t.Fatalf("published last = %d, %v", last, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Entries(2, 4, ^uint64(0))
	if err != nil || len(entries) != 2 || string(entries[0].GetData()) != "a" || string(entries[1].GetData()) != "b" {
		t.Fatalf("recovered entries = %v, %v", entries, err)
	}
}

func TestPersistGroupCoalescesRecordsIntoOneWrite(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalWrite := store.options.ops.writeAt
	writes := 0
	store.options.ops.durableWriteAt = nil
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		return originalWrite(file, data, offset)
	}
	if err := store.PersistGroup([]raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
			Entries: []*pb.Entry{entry(2, 2, "a")}, MustSync: true},
		{NodeIncarnation: incarnation, ReadyID: 2},
		{NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(2, 2),
			Entries: []*pb.Entry{entry(3, 2, "b")}, MustSync: true},
	}); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("Ready group writes = %d, want 1", writes)
	}
}

func TestPersistGroupNonSyncTailDefersBarrierUntilRequired(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	before := store.SyncCount()
	if err := store.PersistGroup([]raftmodel.PersistBatch{{
		NodeIncarnation: incarnation, ReadyID: 1, HardState: &pb.HardState{
			Term: uint64Pointer(1), Vote: uint64Pointer(0), Commit: uint64Pointer(1),
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if syncs := store.SyncCount() - before; syncs != 0 {
		t.Fatalf("non-sync tail barriers = %d, want 0", syncs)
	}
	if err := store.PersistGroup([]raftmodel.PersistBatch{{
		NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2),
		Entries: []*pb.Entry{entry(2, 2, "durable")}, MustSync: true,
	}}); err != nil {
		t.Fatal(err)
	}
	if syncs := store.SyncCount() - before; syncs != 1 {
		t.Fatalf("required group barriers = %d, want 1", syncs)
	}
}

func TestCommitOnlyReadyIsVolatileAndFoldedByRecoveryCertificate(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
		Entries: []*pb.Entry{entry(2, 2, "durable")}, MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	remaining := store.RemainingBytes()
	syncs := store.SyncCount()
	proofs := 0
	store.options.ops.observeNamespaceProof = func() { proofs++ }
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2),
	}); err != nil {
		t.Fatal(err)
	}
	hardState, _, err := store.InitialState()
	if err != nil || hardState.GetCommit() != 2 {
		t.Fatalf("live commit = %d, %v", hardState.GetCommit(), err)
	}
	if store.RemainingBytes() != remaining || store.SyncCount() != syncs || proofs != 0 {
		t.Fatalf("commit-only Ready mutated WAL: remaining %d/%d syncs %d/%d proofs %d",
			store.RemainingBytes(), remaining, store.SyncCount(), syncs, proofs)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	hardState, _, err = reopened.InitialState()
	if err != nil || hardState.GetCommit() != 1 {
		t.Fatalf("WAL-only recovered commit = %d, want durable record 1: %v", hardState.GetCommit(), err)
	}
	entries, err := reopened.Entries(2, 3, ^uint64(0))
	if err != nil || len(entries) != 1 || string(entries[0].GetData()) != "durable" {
		t.Fatalf("recovered durable entry = %v, %v", entries, err)
	}
}

func TestLinuxDurableGroupWriteRemainsFusedAcrossCommitOnlyReady(t *testing.T) {
	_, store, _ := createTestStore(t)
	if store.options.ops.durableWriteAt == nil {
		t.Skip("fused durable Ready writes are Linux-specific")
	}
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	original := store.options.ops.durableWriteAt
	durableWrites := 0
	store.options.ops.durableWriteAt = func(file *os.File, data []byte, offset int64) (int, error) {
		durableWrites++
		return original(file, data, offset)
	}
	if err := store.PersistGroup([]raftmodel.PersistBatch{{
		NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
		Entries: []*pb.Entry{entry(2, 2, "first")}, MustSync: true,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistGroup([]raftmodel.PersistBatch{{
		NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistGroup([]raftmodel.PersistBatch{{
		NodeIncarnation: incarnation, ReadyID: 3, HardState: hard(2, 2),
		Entries: []*pb.Entry{entry(3, 2, "second")}, MustSync: true,
	}}); err != nil {
		t.Fatal(err)
	}
	if durableWrites != 2 || store.unsynced {
		t.Fatalf("durable writes = %d, unsynced = %t; want 2, false", durableWrites, store.unsynced)
	}
}

func TestPersistGroupUnknownBarrierPinsExactOrderedRetry(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalBarrier := testRecordBarrier(store)
	store.options.ops.recordBarrier = func(file *os.File) error {
		if err := originalBarrier(file); err != nil {
			return err
		}
		return errors.New("injected group barrier outcome")
	}
	batches := []raftmodel.PersistBatch{
		{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 1),
			Entries: []*pb.Entry{entry(2, 2, "a")}, MustSync: true},
		{NodeIncarnation: incarnation, ReadyID: 2, HardState: hard(2, 2),
			Entries: []*pb.Entry{entry(3, 2, "b")}, MustSync: true},
	}
	if err := store.PersistGroup(batches); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("group barrier failure = %v", err)
	}
	changed := append([]raftmodel.PersistBatch(nil), batches...)
	changed[1].Entries = []*pb.Entry{entry(3, 2, "changed")}
	if err := store.PersistGroup(changed); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed group retry = %v", err)
	}
	store.options.ops.recordBarrier = originalBarrier
	if err := store.PersistGroup(batches); err != nil {
		t.Fatalf("exact group retry = %v", err)
	}
	if last, err := store.LastIndex(); err != nil || last != 3 {
		t.Fatalf("settled last = %d, %v", last, err)
	}
}
