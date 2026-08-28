package driver

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReplicatedSnapshotAuthorizationFenceFollowsApplyAndFailsClosed(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "snapshot-authorization")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = claim.Close() })
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	before, err := claim.SnapshotAuthorizationFence()
	if err != nil || before.ReplicaSetVersion != 1 {
		t.Fatalf("bootstrap fence: %+v %v", before, err)
	}
	if _, err := claim.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryConfChange},
		&pb.ConfState{Voters: []uint64{1}, Learners: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	after, err := claim.SnapshotAuthorizationFence()
	if err != nil || after.Applied != 2 || after.ReplicaSetVersion != 2 || after.Binding != before.Binding {
		t.Fatalf("learner fence: %+v %v", after, err)
	}
	for _, blocked := range []struct {
		name string
		set  func(bool)
		want error
	}{
		{"pending base", func(set bool) {
			claim.activationBasePending = [32]byte{}
			if set {
				claim.activationBasePending[0] = 1
			}
		}, ErrReplicatedApplyBasePending},
		{"active WAL selection", func(set bool) { claim.walBaseSelectActive = set }, ErrReplicatedApplyBusy},
		{"pending WAL selection", func(set bool) { claim.walBaseSelectPending = set }, ErrReplicatedApplyBusy},
	} {
		t.Run(blocked.name, func(t *testing.T) {
			blocked.set(true)
			got, err := claim.SnapshotAuthorizationFence()
			blocked.set(false)
			if got != (replicatedstate.SnapshotFence{}) || !errors.Is(err, blocked.want) {
				t.Fatalf("blocked fence: %+v %v", got, err)
			}
		})
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := claim.SnapshotAuthorizationFence(); got != (replicatedstate.SnapshotFence{}) || !errors.Is(err, ErrReplicatedApplyClosed) {
		t.Fatalf("closed fence: %+v %v", got, err)
	}
}
