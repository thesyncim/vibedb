package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testOwnershipTransition(binding Binding, replicaSetVersion uint64) OwnershipTransition {
	return OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: replicaSetVersion,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  binding.OwnershipEpoch + 1,
		ToRoutingVersion:  binding.RoutingVersion + 1,
		ToRouteGeneration: binding.RouteGeneration + 1,
	}
}

func TestOwnershipTransitionCodecRoundTripAndStrictness(t *testing.T) {
	if MaxOwnershipTransitionBytes > replication.MaxCommandBytes {
		t.Fatalf("ownership record bound %d exceeds command bound %d",
			MaxOwnershipTransitionBytes, replication.MaxCommandBytes)
	}
	transition := testOwnershipTransition(testBinding(), 7)
	encoded, err := AppendOwnershipTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenOwnershipTransition(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(view.Distribution, []byte(transition.From.Distribution)) ||
		!bytes.Equal(view.Shard, []byte(transition.From.Shard)) ||
		view.ExpectedReplicaSetVersion != 7 || view.SourceMember != 1 || view.TargetMember != 2 ||
		view.ToOwnershipEpoch != transition.ToOwnershipEpoch ||
		!bytes.Equal(view.Bytes(), encoded) {
		t.Fatalf("decoded ownership transition = %+v", view)
	}
	corrupt := bytes.Clone(encoded)
	corrupt[120] ^= 1
	if _, err := OpenOwnershipTransition(corrupt); !errors.Is(err, ErrOwnershipTransition) {
		t.Fatalf("corrupt transition err=%v, want ErrOwnershipTransition", err)
	}
	stale := transition
	stale.ToRoutingVersion = stale.From.RoutingVersion
	if got, err := AppendOwnershipTransition([]byte("prefix"), stale); !errors.Is(err, ErrOwnershipTransition) ||
		!bytes.Equal(got, []byte("prefix")) {
		t.Fatalf("invalid transition append = %q, %v", got, err)
	}
}

func FuzzOpenOwnershipTransition(f *testing.F) {
	encoded, err := AppendOwnershipTransition(nil, testOwnershipTransition(testBinding(), 7))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte("not-an-ownership-transition"))
	f.Fuzz(func(t *testing.T, data []byte) {
		view, err := OpenOwnershipTransition(data)
		if err != nil {
			return
		}
		binding := Binding{
			ClusterID: view.ClusterID, ClusterIncarnation: view.ClusterIncarnation,
			TopologyRecoveryEpoch: view.TopologyRecoveryEpoch,
			Distribution:          string(view.Distribution), Shard: string(view.Shard),
			AllocationGeneration: view.AllocationGeneration,
			ShardIncarnation:     view.ShardIncarnation, GroupID: view.GroupID,
			ActivePolicyGeneration: view.ActivePolicyGeneration,
			ProtectionEpoch:        view.ProtectionEpoch, OwnershipEpoch: view.OwnershipEpoch,
			SchemaGeneration: view.SchemaGeneration, RoutingVersion: view.RoutingVersion,
			RouteGeneration: view.RouteGeneration,
		}
		rebuilt, rebuildErr := AppendOwnershipTransition(nil, OwnershipTransition{
			From: binding, ExpectedReplicaSetVersion: view.ExpectedReplicaSetVersion,
			SourceMember: view.SourceMember, TargetMember: view.TargetMember,
			ToOwnershipEpoch: view.ToOwnershipEpoch, ToRoutingVersion: view.ToRoutingVersion,
			ToRouteGeneration: view.ToRouteGeneration,
		})
		if rebuildErr != nil || !bytes.Equal(rebuilt, data) {
			t.Fatalf("accepted ownership record did not round-trip: %v", rebuildErr)
		}
	})
}

func TestMachineOwnershipTransitionOrdersServingFenceAndSurvivesReopen(t *testing.T) {
	fixture := newMachineFixture(t)
	machine := fixture.machine
	if _, err := machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	conf := &pb.ConfState{Voters: []uint64{1, 2}}
	if _, err := machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, conf); err != nil {
		t.Fatalf("promote target: %v", err)
	}

	oldCommand := commandValue(fixture.binding, 1)
	oldCommand.ReplicaSetVersion = 2
	oldEncoded := encodeCommand(t, oldCommand)
	if _, err := machine.ApplyNormal(normalMeta(3), oldEncoded); err != nil {
		t.Fatalf("old-fence command: %v", err)
	}
	oldCompletion, err := machine.LookupCompletion(oldEncoded)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := AppendOwnershipTransition(nil, testOwnershipTransition(fixture.binding, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("AdmitCommand ownership transition: %v", err)
	}
	publication, err := machine.ApplyNormal(normalMeta(4), encoded)
	if err != nil {
		t.Fatalf("ApplyNormal ownership transition: %v", err)
	}
	state := machine.state
	if publication.Applied != 4 || state.LastKind != RecordOwnership ||
		state.Binding.OwnershipEpoch != fixture.binding.OwnershipEpoch+1 ||
		state.Binding.RoutingVersion != fixture.binding.RoutingVersion+1 ||
		state.Binding.RouteGeneration != fixture.binding.RouteGeneration+1 ||
		state.LogicalDigest != publication.LogicalDigest {
		t.Fatalf("ownership publication = %+v state=%+v", publication, state)
	}
	if retry, err := machine.LookupCompletion(oldEncoded); err != nil ||
		!bytes.Equal(retry.Bytes, oldCompletion.Bytes) {
		t.Fatalf("old completion after transition = %+v, %v", retry, err)
	}

	nextBinding := fixture.binding
	nextBinding.OwnershipEpoch++
	nextBinding.RoutingVersion++
	nextBinding.RouteGeneration++
	newCommand := commandValue(nextBinding, 2)
	newCommand.ReplicaSetVersion = 2
	newEncoded, err := replication.AppendCommand(nil, newCommand)
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.AdmitCommand(newEncoded); err != nil {
		t.Fatalf("new-fence admission: %v", err)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, machine.options,
	)
	if err != nil {
		t.Fatalf("reopen at transitioned binding: %v", err)
	}
	if reopened.Applied() != 4 || reopened.state.Binding != nextBinding {
		t.Fatalf("reopened ownership state = %+v", reopened.state)
	}
	if retry, err := reopened.LookupCompletion(oldEncoded); err != nil ||
		!bytes.Equal(retry.Bytes, oldCompletion.Bytes) {
		t.Fatalf("reopened old completion = %+v, %v", retry, err)
	}
}

func TestMachineOwnershipTransitionRequiresExactMembershipAndFence(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	transition := testOwnershipTransition(fixture.binding, 1)
	encoded, err := AppendOwnershipTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.AdmitCommand(encoded); !errors.Is(err, ErrOwnershipTransition) {
		t.Fatalf("transition before target promotion err=%v, want ErrOwnershipTransition", err)
	}
}
