package replicatedstate

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestSnapshotAuthorizationFenceTracksDurableMembershipWithoutSnapshot(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.SnapshotAuthorizationFence(); !errors.Is(err, ErrReadBehind) {
		t.Fatalf("uninitialized fence: %v", err)
	}
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	before, err := f.machine.SnapshotAuthorizationFence()
	if err != nil || before.Applied != 1 {
		t.Fatalf("initial fence: %+v %v", before, err)
	}
	publication, err := f.machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryConfChange},
		&pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}})
	if err != nil {
		t.Fatal(err)
	}
	after, err := f.machine.SnapshotAuthorizationFence()
	if err != nil || after.ReplicaSetVersion != publication.ReplicaSetVersion ||
		after.ReplicaSetVersion == before.ReplicaSetVersion || after.Applied != 2 || after.Binding != before.Binding {
		t.Fatalf("membership fence: before=%+v after=%+v err=%v", before, after, err)
	}
	cut, err := f.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if after != cut.Fence() {
		t.Fatal("metadata differs from actual durable snapshot fence")
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		fence, err := f.machine.SnapshotAuthorizationFence()
		if err != nil || fence != after {
			panic("unstable fence")
		}
	}); allocs != 0 {
		t.Fatalf("metadata read allocated: %v", allocs)
	}
	f.machine.schemaTransitioned = true
	if got, err := f.machine.SnapshotAuthorizationFence(); got != (SnapshotFence{}) || !errors.Is(err, ErrSchemaTransitionPending) {
		t.Fatalf("pending schema: %+v %v", got, err)
	}
	f.machine.schemaTransitioned = false
	f.machine.fail(errors.New("failed durable apply"))
	if got, err := f.machine.SnapshotAuthorizationFence(); got != (SnapshotFence{}) || !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("poisoned source: %+v %v", got, err)
	}
}
