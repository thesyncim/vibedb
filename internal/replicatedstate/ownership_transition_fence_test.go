package replicatedstate

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestOwnershipGenerationJumpRequiresDurableCaptureAuthority(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryConfChange}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	v := testOwnershipTransition(f.binding, 2)
	v.ToRoutingVersion += 2
	v.ToRouteGeneration += 3
	raw, err := AppendOwnershipTransition(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(normalMeta(3), raw); !errors.Is(err, ErrOwnershipTransition) {
		t.Fatalf("unwitnessed jump: %v", err)
	}
	if f.machine.state.Applied != 2 || f.machine.state.Binding != f.binding {
		t.Fatal("unwitnessed jump published")
	}
}
