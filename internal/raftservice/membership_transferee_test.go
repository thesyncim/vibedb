package raftservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

type membershipTransfereeHost struct {
	splitLeadershipHost
	progress map[uint64]raftmodel.MemberProgress
}

func (host *membershipTransfereeHost) Progress(
	_ raftmember.GroupKey, member uint64,
) (raftmodel.MemberProgress, bool, error) {
	progress, found := host.progress[member]
	return progress, found, nil
}

func (host *membershipTransfereeHost) PrepareLeaderTransfer(
	_ raftmember.GroupKey, target uint64,
) (raftmember.LeaderTransferGuard, error) {
	host.target = target
	return raftmember.LeaderTransferGuard{}, nil
}

// A move's source route lacks the enrolled target and its destination route
// lacks the source; only continuing voters are routable throughout. The
// retiring leader must hand off to one of them whenever one is caught up.
func TestMembershipLeaderTransferPrefersRoutableContinuingVoter(t *testing.T) {
	const commit = 40
	caught := func(match uint64) raftmodel.MemberProgress {
		return raftmodel.MemberProgress{RecentActive: true, Match: match, Next: match + 1}
	}
	lagging := raftmodel.MemberProgress{RecentActive: true, Match: commit - 5, Next: commit - 4}
	for _, test := range []struct {
		name     string
		progress map[uint64]raftmodel.MemberProgress
		want     uint64
	}{
		{name: "most caught-up continuing voter",
			progress: map[uint64]raftmodel.MemberProgress{2: caught(commit), 4: caught(commit + 1), 3: caught(commit)},
			want:     4},
		{name: "tie picks lowest member",
			progress: map[uint64]raftmodel.MemberProgress{2: caught(commit), 4: caught(commit), 3: caught(commit)},
			want:     2},
		{name: "skips lagging continuing voter",
			progress: map[uint64]raftmodel.MemberProgress{2: lagging, 4: caught(commit), 3: caught(commit)},
			want:     4},
		{name: "falls back to caught-up target",
			progress: map[uint64]raftmodel.MemberProgress{2: lagging, 3: caught(commit)},
			want:     3},
	} {
		t.Run(test.name, func(t *testing.T) {
			group := membershipTestGroup()
			grant := membershipgrant.Grant{Group: group, TransitionID: [16]byte{1}, MetadataEpoch: 7,
				CatalogGeneration: 11, InitialReplicaSetVersion: 5,
				InitialVoters: [3]uint64{1, 2, 4}, InitialRosterDigest: [32]byte{1},
				InitialDescriptorDigest: [32]byte{2},
				SourceMember:            1, TargetMember: 3, TargetNode: [16]byte{3}}
			fence := ServingFence{Group: group, AllocationGeneration: 1,
				Command: CommandFence{ReplicaSetVersion: 9}, MemberID: 1, StoreID: [16]byte{1},
				NodeIncarnation: 1, Term: 2}
			host := &membershipTransfereeHost{progress: test.progress}
			host.publication = raftmodel.Publication{Applied: 9, ReplicaSetVersion: 9,
				ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}}
			host.status = raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2, Commit: commit, Applied: commit}
			owner := &Owner{host: host, authority: &membershipTestAuthority{grant: grant, found: true},
				members: map[raftmember.GroupKey]ownerMember{group: {identity: raftmember.RuntimeIdentity{
					Group: group, AllocationGeneration: 1, MemberID: 1, StoreID: fence.StoreID, NodeIncarnation: 1,
				}, command: fence.Command}}}
			request := MembershipRequest{Fence: fence, Kind: MembershipTransferLeader,
				TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
				CatalogGeneration: grant.CatalogGeneration, ExpectedReplicaSetVersion: 9,
				SourceMember: grant.SourceMember, TargetMember: grant.TargetMember}
			if _, err := owner.prepareMembershipLeaderTransfer(request); err != nil {
				t.Fatalf("prepare transfer: %v", err)
			}
			if host.target != test.want {
				t.Fatalf("transferee=%d, want %d", host.target, test.want)
			}
		})
	}
}
