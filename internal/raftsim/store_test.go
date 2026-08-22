package raftsim

import (
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestMemoryStorePersistIsAtomicRetrySafeAndOwned(t *testing.T) {
	store, err := NewMemoryStore([]uint64{3, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	term, index := uint64(2), uint64(2)
	data := []byte("owned")
	batch := raftmodel.PersistBatch{
		NodeIncarnation: 7, ReadyID: 1, MustSync: true,
		Entries:   []*pb.Entry{{Term: &term, Index: &index, Data: data}},
		HardState: &pb.HardState{Term: &term, Commit: &index},
	}
	store.SetNextPersistFault(PersistFailBefore)
	if err := store.Persist(batch); !errors.Is(err, ErrPersistInjected) {
		t.Fatalf("definite failure = %v", err)
	}
	if last, _ := store.LastIndex(); last != 1 {
		t.Fatalf("definite failure persisted index %d", last)
	}

	store.SetNextPersistFault(PersistThenError)
	if err := store.Persist(batch); !errors.Is(err, ErrPersistInjected) {
		t.Fatalf("ambiguous failure = %v", err)
	}
	exactRetry := batch
	exactRetry.Entries = cloneEntries(batch.Entries)
	exactRetry.HardState = cloneHardState(batch.HardState)
	data[0] = 'X'
	if last, _ := store.LastIndex(); last != 2 {
		t.Fatalf("ambiguous failure last index = %d", last)
	}
	entries, err := store.Entries(2, 3, ^uint64(0))
	if err != nil || string(entries[0].GetData()) != "owned" {
		t.Fatalf("owned entry = %q, %v", entries[0].GetData(), err)
	}
	if err := store.Persist(exactRetry); err != nil {
		t.Fatalf("exact retry = %v", err)
	}
	changed := exactRetry
	changed.MustSync = false
	if err := store.Persist(changed); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("changed retry = %v", err)
	}
	if store.PersistCount() != 1 || store.SyncCount() != 1 {
		t.Fatalf("persist/sync counts = %d/%d", store.PersistCount(), store.SyncCount())
	}
}

func TestModelConstructorsRejectOversizedVoterSet(t *testing.T) {
	voters := make([]uint64, MaxMembers+1)
	for i := range voters {
		voters[i] = uint64(i + 1)
	}
	if _, err := NewMemoryStore(voters); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("NewMemoryStore oversized voters error = %v", err)
	}
	if _, err := NewMemoryMachine(voters); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("NewMemoryMachine oversized voters error = %v", err)
	}
	if _, err := NewMemoryStore([]uint64{1, raft.LocalAppendThread}); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("NewMemoryStore reserved voter error = %v", err)
	}
	if _, err := NewScenario([]uint64{1, raft.LocalApplyThread}, nil, nil); !errors.Is(err, ErrInvalidScenario) {
		t.Fatalf("NewScenario reserved voter error = %v", err)
	}
}

func TestMemoryStoreRejectsSkippedReadyAndSeparatesIncarnations(t *testing.T) {
	store, err := NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 2}); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("skipped first Ready = %v", err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: 2, ReadyID: 1}); err != nil {
		t.Fatalf("new incarnation = %v", err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: 1}); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("stale incarnation = %v", err)
	}
}

func TestMemoryStoreRejectsMalformedAndCommittedPrefixChanges(t *testing.T) {
	store, err := NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	term2, index2 := uint64(2), uint64(2)
	index3, term3 := uint64(3), uint64(3)
	original := &pb.Entry{Term: &term2, Index: &index2, Data: []byte("committed")}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 1, Entries: []*pb.Entry{
			original, {Term: &term3, Index: &index3, Data: []byte("also-committed")},
		},
		HardState: &pb.HardState{Term: &term3, Commit: &index3},
	}); err != nil {
		t.Fatal(err)
	}
	changed := protoCloneEntry(original)
	changed.Data[0] = 'X'
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 2, Entries: []*pb.Entry{changed},
	}); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("committed overwrite = %v", err)
	}

	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 2,
		Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: &index2, Term: &term2, ConfState: &pb.ConfState{Voters: []uint64{1}},
		}},
		Entries:   []*pb.Entry{{Index: &index3, Term: &term3}},
		HardState: &pb.HardState{Term: &term3, Commit: &index3},
	}); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("snapshot erasing committed prefix = %v", err)
	}
}

func TestMemoryStoreRejectsMalformedEntriesSnapshotsAndVotes(t *testing.T) {
	term, one, two, three, max := uint64(2), uint64(1), uint64(2), uint64(3), uint64(math.MaxUint64)
	tests := map[string]raftmodel.PersistBatch{
		"nil entry":  {Entries: []*pb.Entry{nil}},
		"zero term":  {Entries: []*pb.Entry{{Index: &two}}},
		"zero index": {Entries: []*pb.Entry{{Term: &term}}},
		"gapped entries": {Entries: []*pb.Entry{
			{Term: &term, Index: &two}, {Term: &term, Index: &max},
		}},
		"terminal index": {Entries: []*pb.Entry{{Term: &term, Index: &max}}},
		"append gap":     {Entries: []*pb.Entry{{Term: &term, Index: &three}}},
		"malformed snapshot": {Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: &two, Term: &term,
		}}},
	}
	for name, batch := range tests {
		t.Run(name, func(t *testing.T) {
			store, err := NewMemoryStore([]uint64{1})
			if err != nil {
				t.Fatal(err)
			}
			batch.NodeIncarnation, batch.ReadyID = 1, 1
			if err := store.Persist(batch); !errors.Is(err, ErrStoreInvariant) {
				t.Fatalf("Persist() error = %v", err)
			}
		})
	}

	store, err := NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 1,
		HardState: &pb.HardState{Term: &one, Vote: &one, Commit: &one},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 2,
		HardState: &pb.HardState{Term: &one, Vote: &two, Commit: &one},
	}); !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("same-term vote rewrite = %v", err)
	}
}

func TestMemoryStoreRejectsSnapshotWithStaleHardState(t *testing.T) {
	store, err := NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	index, term := uint64(2), uint64(2)
	err = store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 1,
		Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}},
		}},
	})
	if !errors.Is(err, ErrStoreInvariant) {
		t.Fatalf("stale HardState after snapshot = %v", err)
	}
	if last, _ := store.LastIndex(); last != 1 {
		t.Fatalf("failed snapshot changed durable last index to %d", last)
	}
}

func TestMemoryStoreRecoveryViewsAreOwned(t *testing.T) {
	store, err := NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	term, index := uint64(2), uint64(2)
	if err := store.Persist(raftmodel.PersistBatch{
		NodeIncarnation: 1, ReadyID: 1,
		Entries:   []*pb.Entry{{Term: &term, Index: &index, Data: []byte("owned")}},
		HardState: &pb.HardState{Term: &term, Commit: &index},
	}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Entries(2, 3, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	entries[0].Data[0] = 'X'
	again, err := store.Entries(2, 3, math.MaxUint64)
	if err != nil || string(again[0].GetData()) != "owned" {
		t.Fatalf("durable entry after caller mutation = %q, %v", again[0].GetData(), err)
	}
}

func TestMemoryMachineAppliesExactOrderedPrefix(t *testing.T) {
	machine, err := NewMemoryMachine([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	meta := raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryNormal}
	publication, err := machine.ApplyNormal(meta, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if publication.Applied != 2 || publication.DataChainDigest == ([32]byte{}) {
		t.Fatalf("publication = %+v", publication)
	}
	record, ok := machine.Entry(2)
	if !ok || record.Index != 2 || record.Term != 2 || record.Digest == ([32]byte{}) {
		t.Fatalf("entry = %+v, %v", record, ok)
	}
	if _, err := machine.ApplyNormal(meta, nil); !errors.Is(err, ErrMachineInvariant) {
		t.Fatalf("duplicate apply = %v", err)
	}
}

func TestMemoryMachineReconcilesOnlyExactStaticSnapshot(t *testing.T) {
	store, err := NewMemoryStore([]uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	machine, err := NewMemoryMachine([]uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	publication, err := machine.InstallSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if publication.Applied != 1 || publication.ReplicaSetVersion != 1 {
		t.Fatalf("publication = %+v", publication)
	}

	different := cloneSnapshot(snapshot)
	different.Data = []byte("different same-cut state")
	if _, err := machine.InstallSnapshot(different); !errors.Is(err, ErrMachineInvariant) {
		t.Fatalf("different same-cut snapshot error = %v", err)
	}
	if got := machine.Published(); got.Applied != 1 || got.ReplicaSetVersion != 1 {
		t.Fatalf("publication changed after refusal = %+v", got)
	}
}

func TestMemoryMachineFailsClosedAtAppliedExhaustion(t *testing.T) {
	machine, err := NewMemoryMachine([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	machine.publication.Applied = math.MaxUint64
	if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{Index: 0, Term: 2, Type: pb.EntryNormal}, nil); !errors.Is(err, ErrMachineInvariant) {
		t.Fatalf("exhausted apply error = %v", err)
	}
}

func protoCloneEntry(entry *pb.Entry) *pb.Entry {
	term, index := entry.GetTerm(), entry.GetIndex()
	return &pb.Entry{Term: &term, Index: &index, Type: entry.Type, Data: append([]byte(nil), entry.Data...)}
}
