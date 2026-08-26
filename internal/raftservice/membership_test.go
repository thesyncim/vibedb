package raftservice

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

type membershipTestAuthority struct {
	grant membershipgrant.Grant
	found bool
}

func (authority *membershipTestAuthority) CurrentTransitionGrant(
	raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	return authority.grant, authority.found, nil
}

func (*membershipTestAuthority) PublishCommittedAuthority(
	raftmember.GroupKey, uint64, *pb.ConfState,
) error {
	return nil
}

func (*membershipTestAuthority) PublishDurablePromotion(
	raftmember.GroupKey, raftmember.DurablePromotionProof,
) error {
	return nil
}

func (*membershipTestAuthority) ClearDurablePromotion(raftmember.GroupKey) error { return nil }

func TestMembershipTransitionOrderingAuthorizationAndStaleReplay(t *testing.T) {
	authority := membershipgrant.Grant{Group: membershipTestGroup(), TransitionID: [16]byte{1}, MetadataEpoch: 7,
		CatalogGeneration: 11, InitialReplicaSetVersion: 5,
		InitialVoters: [3]uint64{1, 2, 4}, InitialRosterDigest: [32]byte{1},
		InitialDescriptorDigest: [32]byte{2},
		SourceMember:            1, TargetMember: 3, TargetNode: [16]byte{3}}
	request := MembershipRequest{Fence: ServingFence{Group: authority.Group}, Kind: MembershipAddLearner,
		TransitionID: authority.TransitionID, MetadataEpoch: authority.MetadataEpoch,
		CatalogGeneration: authority.CatalogGeneration, ExpectedReplicaSetVersion: 5,
		SourceMember: 1, TargetMember: 3}
	status := raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2, Commit: 5, Applied: 5}
	publication := raftmodel.Publication{Applied: 5, ReplicaSetVersion: 5,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2}}}
	if err := validateMembershipTransition(request, authority, publication, status,
		raftmodel.MemberProgress{}, false); err != nil {
		t.Fatalf("add learner: %v", err)
	}
	// The identical applied command is a stale replay, not an idempotent second
	// membership mutation.
	publication = raftmodel.Publication{Applied: 6, ReplicaSetVersion: 6,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}}
	if err := validateMembershipTransition(request, authority, publication, status,
		raftmodel.MemberProgress{}, false); !errors.Is(err, ErrMembershipStale) {
		t.Fatalf("stale add replay = %v", err)
	}
	request.ExpectedReplicaSetVersion = 6
	request.Kind = MembershipPromoteVoter
	behind := raftmodel.MemberProgress{Learner: true, RecentActive: true, Match: 5, Next: 6}
	status.Commit = 6
	if err := validateMembershipTransition(request, authority, publication, status,
		behind, true); !errors.Is(err, ErrMembershipNotCaughtUp) {
		t.Fatalf("early promotion = %v", err)
	}
	caught := behind
	caught.Match, caught.Next = 6, 7
	if err := validateMembershipTransition(request, authority, publication, status,
		caught, true); err != nil {
		t.Fatalf("caught-up promotion: %v", err)
	}
	publication = raftmodel.Publication{Applied: 7, ReplicaSetVersion: 7,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}
	request.ExpectedReplicaSetVersion = 7
	request.Kind = MembershipTransferLeader
	voterProgress := raftmodel.MemberProgress{RecentActive: true, Match: 7, Next: 8}
	status.Commit = 7
	if err := validateMembershipTransition(request, authority, publication, status,
		voterProgress, true); err != nil {
		t.Fatalf("safe transfer: %v", err)
	}
	request.MetadataEpoch++
	if err := validateMembershipTransition(request, authority, publication, status,
		voterProgress, true); !errors.Is(err, ErrMembershipUnauthorized) {
		t.Fatalf("wrong metadata epoch = %v", err)
	}
}

func TestMembershipRemovalUsesAnyNonSourceLeaderWithCaughtUpTarget(t *testing.T) {
	authority := membershipgrant.Grant{Group: membershipTestGroup(), TransitionID: [16]byte{2}, MetadataEpoch: 8,
		CatalogGeneration: 12, InitialReplicaSetVersion: 7,
		InitialVoters: [3]uint64{1, 2, 4}, InitialRosterDigest: [32]byte{1},
		InitialDescriptorDigest: [32]byte{2},
		SourceMember:            1, TargetMember: 3, TargetNode: [16]byte{3}}
	request := MembershipRequest{Fence: ServingFence{Group: authority.Group}, Kind: MembershipRemoveVoter,
		TransitionID: authority.TransitionID, MetadataEpoch: authority.MetadataEpoch,
		CatalogGeneration: authority.CatalogGeneration, ExpectedReplicaSetVersion: 9,
		SourceMember: 1, TargetMember: 3, TransferTerm: 4}
	publication := raftmodel.Publication{Applied: 9, ReplicaSetVersion: 9,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}}
	status := raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 4, Commit: 9, Applied: 9}
	caught := raftmodel.MemberProgress{RecentActive: true, Match: 9, Next: 10}
	if err := validateMembershipTransition(request, authority, publication, status,
		caught, true); !errors.Is(err, ErrMembershipStale) {
		t.Fatalf("leader self-removal = %v", err)
	}
	status.MemberID, status.LeaderID = 2, 2
	wrongTerm := request
	wrongTerm.TransferTerm++
	if err := validateMembershipTransition(wrongTerm, authority, publication, status,
		caught, true); !errors.Is(err, ErrMembershipStale) {
		t.Fatalf("wrong transfer witness = %v", err)
	}
	if err := validateMembershipTransition(request, authority, publication, status,
		caught, true); err != nil {
		t.Fatalf("other-leader removal: %v", err)
	}
	status.MemberID, status.LeaderID = 3, 3
	if err := validateMembershipTransition(request, authority, publication, status,
		caught, true); err != nil {
		t.Fatalf("target-leader removal: %v", err)
	}
	status.MemberID, status.LeaderID = 2, 2
	behind := caught
	behind.Match = 8
	if err := validateMembershipTransition(request, authority, publication, status,
		behind, true); !errors.Is(err, ErrMembershipStale) {
		t.Fatalf("uncaught target removal = %v", err)
	}
	publication.ConfState.Voters = []uint64{1, 2, 3, 4, 5}
	if err := validateMembershipTransition(request, authority, publication, status,
		caught, true); !errors.Is(err, ErrMembershipStale) {
		t.Fatalf("non-RF4 removal = %v", err)
	}
}

func TestQueuedMembershipReadsLiveGrantAfterExactRevocation(t *testing.T) {
	group := membershipTestGroup()
	grant := membershipgrant.Grant{Group: group, TransitionID: [16]byte{3},
		MetadataEpoch: 4, CatalogGeneration: 5, InitialReplicaSetVersion: 1,
		InitialVoters: [3]uint64{1, 2, 4}, InitialRosterDigest: [32]byte{1},
		InitialDescriptorDigest: [32]byte{2},
		SourceMember:            1, TargetMember: 3, TargetNode: [16]byte{3}}
	authority := &membershipTestAuthority{grant: grant, found: true}
	owner := &Owner{
		started: true, ingress: make(chan ownerRequest, 1),
		limits: Limits{MaxIngressItems: 1, MaxIngressBytes: 1}, authority: authority,
		members: map[raftmember.GroupKey]ownerMember{group: {}},
	}
	request := MembershipRequest{
		Fence: ServingFence{Group: group}, Kind: MembershipAddLearner,
		TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
		CatalogGeneration: grant.CatalogGeneration, ExpectedReplicaSetVersion: 1,
		SourceMember: grant.SourceMember, TargetMember: grant.TargetMember,
	}
	done := make(chan error, 1)
	go func() { done <- owner.ApplyMembership(context.Background(), request) }()
	queued := <-owner.ingress
	// Catalog-proved revocation wins before the serialized Owner begins
	// admission. No cold copy in ownerMember can authorize the queued request.
	authority.grant, authority.found = membershipgrant.Grant{}, false
	if err := owner.handle(queued); !errors.Is(err, ErrMembershipUnauthorized) {
		t.Fatalf("serialized handle after revoke=%v", err)
	}
	owner.release(queued.bytes)
	if err := <-done; !errors.Is(err, ErrMembershipUnauthorized) {
		t.Fatalf("queued request after revoke=%v", err)
	}
}

func membershipTestGroup() (group raftmember.GroupKey) {
	group.ClusterID[0] = 1
	group.ClusterIncarnation[0] = 2
	group.TopologyRecoveryEpoch = 3
	group.ShardIncarnation[0] = 4
	group.GroupID[0] = 5
	return group
}
