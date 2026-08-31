package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3/raftpb"
)

type retirementHost struct {
	ownerHost
	state              replicatedstate.State
	status             raftmember.RuntimeStatus
	removed            bool
	snapshotCalls      int
	authorizationCalls int
	manifestDigest     [32]byte
}

func (host *retirementHost) Publication(raftmember.GroupKey) (raftmodel.Publication, error) {
	return raftmodel.Publication{ReplicaSetVersion: host.state.ReplicaSetVersion}, nil
}

func (host *retirementHost) SnapshotState(raftmember.GroupKey) (replicatedstate.State, error) {
	host.snapshotCalls++
	return host.state, nil
}

func (host *retirementHost) SnapshotAuthorizationFence(raftmember.GroupKey) (replicatedstate.SnapshotFence, error) {
	host.authorizationCalls++
	return replicatedstate.SnapshotFence{
		Binding:                host.state.Binding,
		RelationManifestDigest: host.manifestDigest,
		ReplicaSetVersion:      host.state.ReplicaSetVersion,
		Applied:                host.state.Applied,
		LastTerm:               host.state.LastTerm,
		LastEntryDigest:        host.state.LastEntryDigest,
		DataChainDigest:        host.state.DataChainDigest,
		SnapshotBaseDigest:     host.state.SnapshotBaseDigest,
	}, nil
}

func (host *retirementHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *retirementHost) Remove(raftmember.GroupKey) error {
	host.removed = true
	return nil
}

func TestReplicaActionFencesOwnershipAndRetirementExactly(t *testing.T) {
	group := peerServerTestGroup()
	fence := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 12}
	binding := replicatedstate.Binding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, Distribution: "d", Shard: "s",
		OwnedRange:           distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		AllocationGeneration: fence.AllocationGeneration, ShardIncarnation: group.ShardIncarnation,
		GroupID: group.GroupID, ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch: fence.Command.ProtectionEpoch, OwnershipEpoch: fence.Command.OwnershipEpoch,
		SchemaGeneration: fence.Command.SchemaGeneration, RoutingVersion: fence.Command.RoutingVersion,
		RouteGeneration: fence.Command.RouteGeneration}
	command, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: fence.Command.ReplicaSetVersion,
		SourceMember: 1, TargetMember: 2, ToOwnershipEpoch: 9,
		ToRoutingVersion: 11, ToRouteGeneration: 12, ToOwnedRange: binding.OwnedRange})
	if err != nil {
		t.Fatal(err)
	}
	transition, err := replicatedstate.OpenOwnershipTransition(command)
	if err != nil || !ownershipTransitionMatchesFence(transition, fence) {
		t.Fatalf("transition fence=%v", err)
	}
	stale := fence
	stale.Command.OwnershipEpoch--
	if ownershipTransitionMatchesFence(transition, stale) {
		t.Fatal("stale ownership fence accepted")
	}

	retired := binding
	retired.OwnershipEpoch = 9
	retired.RoutingVersion = 11
	retired.RouteGeneration = 12
	retireFence := fence
	retireFence.Command.OwnershipEpoch = 9
	retireFence.Command.RoutingVersion = 11
	retireFence.Command.RouteGeneration = 12
	retireFence.Command.ReplicaSetVersion = 8
	state := replicatedstate.State{Binding: retired, ReplicaSetVersion: 8,
		ConfState: &raftpb.ConfState{Voters: []uint64{2, 3, 4}}}
	if !retirementStateMatches(state, retireFence, 1, 2) {
		t.Fatal("exact retired state rejected")
	}
	state.ConfState.Voters = []uint64{1, 2, 3, 4}
	if retirementStateMatches(state, retireFence, 1, 2) {
		t.Fatal("still-voting source accepted")
	}

	member := ownerMember{identity: raftmember.RuntimeIdentity{Group: group,
		AllocationGeneration: fence.AllocationGeneration, MemberID: fence.MemberID,
		StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation,
		RelationManifestDigest: fence.Command.RelationManifestDigest}, command: fence.Command, retiring: true}
	if servingFenceMatchesIdentity(fence, member) {
		t.Fatal("retiring member served")
	}
	member.retiring = false
	owner := &Owner{members: map[raftmember.GroupKey]ownerMember{group: member}}
	if err := owner.syncCommandFenceFromState(group, ReplicaObservation{
		Publication: raftmodel.Publication{ReplicaSetVersion: 8}, State: state,
	}); err != nil {
		t.Fatal(err)
	}
	updated := owner.members[group].command
	if updated.ReplicaSetVersion != 8 || updated.OwnershipEpoch != 9 ||
		updated.RoutingVersion != 11 || updated.RouteGeneration != 12 {
		t.Fatalf("updated command=%+v", updated)
	}

	// A removed source need not have received an observation RPC since the
	// ownership and membership changes applied. Retirement must validate the
	// durable cut, not a command-fence cache refreshed only by such an RPC.
	state.ConfState.Voters = []uint64{2, 3, 4}
	retireFence.MemberID = 1
	probeHost := &retirementHost{state: state, manifestDigest: member.identity.RelationManifestDigest}
	probeOwner := &Owner{host: probeHost, members: map[raftmember.GroupKey]ownerMember{group: member}}
	reply := make(chan ownerReply, 1)
	if err := probeOwner.handle(ownerRequest{kind: requestStatus, group: group, reply: reply}); err != nil {
		t.Fatal(err)
	}
	if observed := <-reply; observed.err != nil || observed.state.Command != retireFence.Command {
		t.Fatalf("probe returned pre-transition command fence: %+v", observed)
	}
	if probeHost.authorizationCalls != 1 || probeHost.snapshotCalls != 0 {
		t.Fatalf("probe cuts: authorization=%d snapshot=%d",
			probeHost.authorizationCalls, probeHost.snapshotCalls)
	}
	probeHost.manifestDigest = [32]byte{99}
	reply = make(chan ownerReply, 1)
	if err := probeOwner.handle(ownerRequest{kind: requestStatus, group: group, reply: reply}); !errors.Is(err, ErrServingFence) {
		t.Fatalf("manifest-mismatched handle = %v", err)
	}
	if observed := <-reply; !errors.Is(observed.err, ErrServingFence) ||
		observed.state.Command != retireFence.Command {
		t.Fatalf("manifest-mismatched probe = %+v", observed)
	}
	if probeHost.authorizationCalls != 2 || probeHost.snapshotCalls != 0 {
		t.Fatalf("manifest-mismatched cuts: authorization=%d snapshot=%d",
			probeHost.authorizationCalls, probeHost.snapshotCalls)
	}
	for _, staleRequest := range []bool{false, true} {
		member.identity.MemberID = 1
		member.command = fence.Command
		host := &retirementHost{state: state, status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 2, Term: retireFence.Term}}
		owner := &Owner{host: host, groups: []raftmember.GroupKey{group},
			members: map[raftmember.GroupKey]ownerMember{group: member}}
		requestFence := retireFence
		if staleRequest {
			requestFence.Command = fence.Command
		}
		err := owner.retireReplica(ownerRequest{group: group, fence: requestFence,
			operation: [32]byte{1}, step: [32]byte{2}, sourceMember: 1, targetMember: 2})
		if staleRequest {
			if !errors.Is(err, ErrServingFence) || host.removed {
				t.Fatalf("stale retirement: removed=%t err=%v", host.removed, err)
			}
		} else if err != nil || !host.removed || len(owner.members) != 0 {
			t.Fatalf("current retirement without observation: removed=%t err=%v", host.removed, err)
		}
	}
}

func TestProbeBeforeReplicaSetPublicationPreservesBootstrapState(t *testing.T) {
	group := peerServerTestGroup()
	command := CommandFence{
		ReplicaSetVersion: 4, ActivePolicyGeneration: 5, ProtectionEpoch: 6,
		OwnershipEpoch: 7, SchemaGeneration: 8, RelationManifestDigest: [32]byte{9},
		RoutingVersion: 10, RouteGeneration: 11,
	}
	member := ownerMember{identity: raftmember.RuntimeIdentity{
		Group: group, AllocationGeneration: 3, MemberID: 2,
		StoreID: [16]byte{4}, NodeIncarnation: 5,
		RelationManifestDigest: command.RelationManifestDigest,
	}, command: command}
	host := &retirementHost{status: raftmember.RuntimeStatus{
		MemberID: member.identity.MemberID, LeaderID: member.identity.MemberID,
		Term: 12, Commit: 1, Applied: 1,
	}}
	owner := &Owner{host: host, members: map[raftmember.GroupKey]ownerMember{group: member}}
	reply := make(chan ownerReply, 1)
	if err := owner.handle(ownerRequest{kind: requestStatus, group: group, reply: reply}); err != nil {
		t.Fatal(err)
	}
	observed := <-reply
	if observed.err != nil || observed.state.Command != command || observed.state.Status != host.status {
		t.Fatalf("pre-publication probe = %+v", observed)
	}
	if host.authorizationCalls != 0 || host.snapshotCalls != 0 {
		t.Fatalf("pre-publication cuts: authorization=%d snapshot=%d",
			host.authorizationCalls, host.snapshotCalls)
	}
}
