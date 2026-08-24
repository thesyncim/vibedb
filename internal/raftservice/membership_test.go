package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestMembershipTransitionOrderingAuthorizationAndStaleReplay(t *testing.T) {
	authority := MembershipAuthorization{TransitionID: [16]byte{1}, MetadataEpoch: 7,
		CatalogGeneration: 11, SourceMember: 1, TargetMember: 3}
	request := MembershipRequest{Kind: MembershipAddLearner,
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

func TestMembershipRemovalRequiresTargetLeader(t *testing.T) {
	authority := MembershipAuthorization{TransitionID: [16]byte{2}, MetadataEpoch: 8,
		CatalogGeneration: 12, SourceMember: 1, TargetMember: 3}
	request := MembershipRequest{Kind: MembershipRemoveVoter,
		TransitionID: authority.TransitionID, MetadataEpoch: authority.MetadataEpoch,
		CatalogGeneration: authority.CatalogGeneration, ExpectedReplicaSetVersion: 9,
		SourceMember: 1, TargetMember: 3}
	publication := raftmodel.Publication{Applied: 9, ReplicaSetVersion: 9,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}
	status := raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Commit: 9, Applied: 9}
	if err := validateMembershipTransition(request, authority, publication, status,
		raftmodel.MemberProgress{}, false); !errors.Is(err, ErrMembershipStale) {
		t.Fatalf("leader self-removal = %v", err)
	}
	status.MemberID, status.LeaderID = 3, 3
	if err := validateMembershipTransition(request, authority, publication, status,
		raftmodel.MemberProgress{}, false); err != nil {
		t.Fatalf("target-leader removal: %v", err)
	}
}
