package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func retirementProofFixture() (ReplicaRetirementRequest, membershipgrant.Grant, ReplicaObservation) {
	group := peerServerTestGroup()
	fence := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 8, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 1, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 12}
	binding := replicatedstate.Binding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, Distribution: "d", Shard: "s",
		OwnedRange:           distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		AllocationGeneration: fence.AllocationGeneration, ShardIncarnation: group.ShardIncarnation,
		GroupID: group.GroupID, ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch: fence.Command.ProtectionEpoch, OwnershipEpoch: fence.Command.OwnershipEpoch,
		SchemaGeneration: fence.Command.SchemaGeneration, RoutingVersion: fence.Command.RoutingVersion,
		RouteGeneration: fence.Command.RouteGeneration}
	grant := membershipgrant.Grant{Group: group, TransitionID: [16]byte{1}, MetadataEpoch: 7,
		CatalogGeneration: 11, InitialReplicaSetVersion: 5,
		InitialVoters: [3]uint64{1, 2, 3}, InitialRosterDigest: [32]byte{1},
		InitialDescriptorDigest: [32]byte{2}, SourceMember: 1, TargetMember: 4, TargetNode: [16]byte{4}}
	return ReplicaRetirementRequest{Operation: [32]byte{1}, Step: [32]byte{2}, Fence: fence, SourceMember: 1, TargetMember: 4},
		grant, ReplicaObservation{
			Identity: raftmember.RuntimeIdentity{Group: group, Distribution: "d", Shard: "s", AllocationGeneration: 3,
				MemberID: 2, StoreID: [16]byte{2}, NodeIncarnation: 1, RelationManifestDigest: fence.Command.RelationManifestDigest},
			State: replicatedstate.State{Binding: binding, Applied: 10, LastTerm: 12,
				ReplicaSetVersion: 8, ConfState: &pb.ConfState{Voters: []uint64{2, 3, 4}}},
			Publication: raftmodel.Publication{Applied: 10, ReplicaSetVersion: 8,
				ConfState: &pb.ConfState{Voters: []uint64{2, 3, 4}}},
			Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 12, Applied: 10, Commit: 10,
				RaftState: raft.StateLeader},
		}
}

func TestReplicaRetirementProofRequiresExactCommittedRemoval(t *testing.T) {
	request, grant, observation := retirementProofFixture()
	if _, err := NewReplicaRetirementProof(request, grant, observation); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*ReplicaRetirementRequest, *membershipgrant.Grant, *ReplicaObservation)
	}{
		{"source observer", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.Status.MemberID = 1
		}},
		{"foreign observer", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.Status.MemberID = 99
		}},
		{"old observer term", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) { o.Status.Term-- }},
		{"uncommitted publication", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.Status.Commit = 7
		}},
		{"unapplied publication", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.Status.Applied = 7
		}},
		{"state applied mismatch", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) { o.State.Applied-- }},
		{"publication before removal", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.Publication.Applied, o.State.Applied, o.Status.Applied, o.Status.Commit = 7, 7, 7, 7
		}},
		{"state membership mismatch", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.ReplicaSetVersion--
		}},
		{"configuration disagreement", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.Publication.ConfState.Voters[0] = 1
		}},
		{"source remains voter", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.ConfState.Voters = []uint64{1, 2, 3}
			o.Publication.ConfState.Voters = []uint64{1, 2, 3}
		}},
		{"foreign final voter", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.ConfState.Voters = []uint64{2, 4, 5}
			o.Publication.ConfState.Voters = []uint64{2, 4, 5}
		}},
		{"retained learner", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.ConfState.Learners = []uint64{5}
			o.Publication.ConfState.Learners = []uint64{5}
		}},
		{"joint removal", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.ConfState.VotersOutgoing = []uint64{1, 2, 3}
			o.Publication.ConfState.VotersOutgoing = []uint64{1, 2, 3}
		}},
		{"foreign binding", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.Binding.GroupID[0]++
		}},
		{"different allocation", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.Binding.AllocationGeneration++
		}},
		{"different schema", func(_ *ReplicaRetirementRequest, _ *membershipgrant.Grant, o *ReplicaObservation) {
			o.State.Binding.SchemaGeneration++
		}},
		{"different final fence", func(r *ReplicaRetirementRequest, _ *membershipgrant.Grant, _ *ReplicaObservation) {
			r.Fence.Command.ReplicaSetVersion++
		}},
		{"foreign grant", func(_ *ReplicaRetirementRequest, g *membershipgrant.Grant, _ *ReplicaObservation) {
			g.Group.GroupID[0]++
		}},
		{"different source", func(_ *ReplicaRetirementRequest, g *membershipgrant.Grant, _ *ReplicaObservation) { g.SourceMember = 2 }},
		{"different target", func(_ *ReplicaRetirementRequest, g *membershipgrant.Grant, _ *ReplicaObservation) { g.TargetMember = 5 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, g, o := retirementProofFixture()
			test.change(&r, &g, &o)
			if _, err := NewReplicaRetirementProof(r, g, o); !errors.Is(err, ErrServingFence) {
				t.Fatalf("invalid removal proof=%v", err)
			}
		})
	}
}

func provenRetirementOwner(t *testing.T) (*Owner, *retirementHost, ownerRequest, *membershipTestAuthority) {
	t.Helper()
	request, grant, observation := retirementProofFixture()
	proof, err := NewReplicaRetirementProof(request, grant, observation)
	if err != nil {
		t.Fatal(err)
	}
	state := observation.State
	state.ReplicaSetVersion, state.Applied = 7, 7
	state.Binding.OwnershipEpoch--
	state.Binding.RoutingVersion--
	state.Binding.RouteGeneration--
	state.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	host := &retirementHost{state: state, status: raftmember.RuntimeStatus{
		MemberID: 1, Term: 15, Applied: 7, Commit: 7, RaftState: raft.StateCandidate}}
	fence := request.Fence
	member := ownerMember{identity: raftmember.RuntimeIdentity{Group: fence.Group, Distribution: "d", Shard: "s",
		AllocationGeneration: fence.AllocationGeneration, MemberID: fence.MemberID,
		StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation,
		RelationManifestDigest: fence.Command.RelationManifestDigest}, command: fence.Command}
	member.command.ReplicaSetVersion = 7
	member.command.OwnershipEpoch--
	member.command.RoutingVersion--
	member.command.RouteGeneration--
	authority := &membershipTestAuthority{grant: grant, found: true}
	owner := &Owner{host: host, authority: authority, groups: []raftmember.GroupKey{fence.Group},
		members: map[raftmember.GroupKey]ownerMember{fence.Group: member}}
	return owner, host, ownerRequest{kind: requestReplicaRetirement, group: fence.Group, fence: fence,
		operation: request.Operation, step: request.Step, sourceMember: request.SourceMember, targetMember: request.TargetMember,
		retirementProof: &proof}, authority
}

func TestOwnerProvenRetirementClosesSourceWithoutRemovalApply(t *testing.T) {
	for _, staleSchema := range []bool{false, true} {
		owner, host, request, _ := provenRetirementOwner(t)
		if staleSchema {
			host.state.Binding.SchemaGeneration--
			member := owner.members[request.group]
			member.command.SchemaGeneration--
			member.command.RelationManifestDigest = [32]byte{98}
			member.identity.RelationManifestDigest = member.command.RelationManifestDigest
			owner.members[request.group] = member
		}
		if err := owner.validateReplicaRetirement(request); err != nil || host.removed || owner.members[request.group].retiring {
			t.Fatalf("proof preflight staleSchema=%t removed=%t err=%v", staleSchema, host.removed, err)
		}
		// Service has now durably recorded RetirementAuthorized. Source Raft
		// remains on its original voting configuration and isolated higher term.
		request.retirementProof, request.retirementAuthorized = nil, true
		if err := owner.retireReplica(request); err != nil || !host.removed || len(owner.members) != 0 || len(owner.groups) != 0 {
			t.Fatalf("retirement staleSchema=%t removed=%t remaining=%d err=%v", staleSchema, host.removed, len(owner.members), err)
		}
	}
}

func TestOwnerProvenRetirementRejectsForeignOrNewerLocalState(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Owner, *retirementHost, *ownerRequest, *membershipTestAuthority)
	}{
		{"newer membership", func(_ *Owner, h *retirementHost, r *ownerRequest, _ *membershipTestAuthority) {
			h.state.ReplicaSetVersion = r.fence.Command.ReplicaSetVersion + 1
		}},
		{"newer ownership", func(_ *Owner, h *retirementHost, r *ownerRequest, _ *membershipTestAuthority) {
			h.state.Binding.OwnershipEpoch = r.fence.Command.OwnershipEpoch + 1
		}},
		{"newer schema", func(_ *Owner, h *retirementHost, r *ownerRequest, _ *membershipTestAuthority) {
			h.state.Binding.SchemaGeneration = r.fence.Command.SchemaGeneration + 1
		}},
		{"newer protection", func(_ *Owner, h *retirementHost, r *ownerRequest, _ *membershipTestAuthority) {
			h.state.Binding.ProtectionEpoch = r.fence.Command.ProtectionEpoch + 1
		}},
		{"foreign distribution", func(_ *Owner, h *retirementHost, _ *ownerRequest, _ *membershipTestAuthority) {
			h.state.Binding.Distribution = "other"
		}},
		{"foreign shard", func(_ *Owner, h *retirementHost, _ *ownerRequest, _ *membershipTestAuthority) {
			h.state.Binding.Shard = "other"
		}},
		{"different owned range", func(_ *Owner, h *retirementHost, _ *ownerRequest, _ *membershipTestAuthority) {
			h.state.Binding.OwnedRange.Start[0] = 1
		}},
		{"foreign store", func(_ *Owner, _ *retirementHost, r *ownerRequest, _ *membershipTestAuthority) { r.fence.StoreID[0]++ }},
		{"foreign runtime", func(_ *Owner, _ *retirementHost, r *ownerRequest, _ *membershipTestAuthority) {
			r.fence.NodeIncarnation++
		}},
		{"different current manifest", func(o *Owner, _ *retirementHost, r *ownerRequest, _ *membershipTestAuthority) {
			member := o.members[r.group]
			member.command.RelationManifestDigest[0]++
			o.members[r.group] = member
		}},
		{"different current grant", func(_ *Owner, _ *retirementHost, _ *ownerRequest, a *membershipTestAuthority) {
			a.grant.MetadataEpoch++
		}},
		{"retired grant", func(_ *Owner, _ *retirementHost, _ *ownerRequest, a *membershipTestAuthority) { a.found = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner, host, request, authority := provenRetirementOwner(t)
			test.change(owner, host, &request, authority)
			if err := owner.retireReplica(request); err == nil || host.removed || len(owner.members) != 1 || owner.members[request.group].retiring {
				t.Fatalf("unsafe retirement removed=%t remaining=%d err=%v", host.removed, len(owner.members), err)
			}
		})
	}
}
