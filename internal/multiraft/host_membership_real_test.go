package multiraft

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// This uses an RF3 voter set plus one enrolled replacement. The replacement
// catches up as a learner before promotion; the old voter is removed only
// after transfer, then the remaining RF3 survives loss of the new leader.
func TestThreeRealHostsOrderLearnerCatchUpBeforePromotion(t *testing.T) {
	identities := realTransferIdentities()
	voters := []uint64{identities[0].MemberID, identities[1].MemberID, identities[2].MemberID}
	hosts := make([]*Host, 4)
	reopen := make([]func() *raftmember.Runtime, 4)
	registries := make([]*rafttransport.StaticRegistry, 0, 4)
	nodes := [4]rafttransport.NodeID{{1}, {2}, {3}, {4}}
	members := make([]rafttransport.Member, 4)
	for index := range hosts {
		runtime, _, reopenRuntime := newRealTransferRuntime(t, identities[index], voters)
		reopen[index] = reopenRuntime
		host, err := NewHost(testHostLimits())
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Add(runtime); err != nil {
			t.Fatal(err)
		}
		hosts[index] = host
		t.Cleanup(func() { _ = host.Close() })
		role := rafttransport.MemberVoter
		if index == 3 {
			role = rafttransport.MemberEnrolled
		}
		members[index] = rafttransport.Member{Group: runtime.Identity().Group,
			ReplicaSetVersion: 1, MemberID: identities[index].MemberID,
			Node: nodes[index], Role: role}
	}
	for index := range hosts {
		registry, err := rafttransport.NewStaticRegistry(nodes[index], members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AuthorizeTransition(rafttransport.TransitionGrant{
			Group: members[0].Group, TransitionID: [16]byte{1}, MetadataEpoch: 2,
			CatalogGeneration: 3, SourceMember: 1, TargetMember: 4,
		}); err != nil {
			t.Fatal(err)
		}
		registries = append(registries, registry)
	}
	cluster := realTransferCluster{t: t, hosts: hosts, registries: registries,
		memberIndex: map[uint64]int{1: 0, 2: 1, 3: 2, 4: 3}, inactive: make(map[int]bool),
		group: members[0].Group, syncAuthority: true}
	group := members[0].Group
	if err := hosts[0].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		for index := 0; index < 3; index++ {
			status, err := hosts[index].Status(group)
			if err != nil || status.LeaderID != 1 || status.Applied < 2 {
				return false
			}
		}
		return true
	})
	target := uint64(4)
	if err := hosts[0].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &target,
	}); err != nil {
		t.Fatal(err)
	}
	learnerConf := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	cluster.driveUntil(func() bool {
		for _, host := range hosts {
			publication, err := host.Publication(group)
			if err != nil || publication.Applied < 3 ||
				!proto.Equal(publication.ConfState, learnerConf) {
				return false
			}
		}
		progress, found, err := hosts[0].Progress(group, target)
		return err == nil && found && progress.Learner && progress.RecentActive &&
			progress.Match >= 3 && progress.PendingSnapshot == 0
	})
	if err := hosts[0].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: &target,
	}); err != nil {
		t.Fatal(err)
	}
	voterConf := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	cluster.driveUntil(func() bool {
		for _, host := range hosts {
			publication, err := host.Publication(group)
			if err != nil || publication.Applied < 4 ||
				!proto.Equal(publication.ConfState, voterConf) {
				return false
			}
		}
		return true
	})
	for index, host := range hosts {
		status, err := host.Status(group)
		if err != nil || status.Applied < 4 {
			t.Fatalf("member %d status = %+v, %v", index+1, status, err)
		}
	}
	if err := hosts[0].TransferLeader(group, target); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		for _, host := range hosts {
			status, err := host.Status(group)
			if err != nil || status.LeaderID != target {
				return false
			}
		}
		return true
	})
	source := uint64(1)
	if err := hosts[3].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &source,
	}); err != nil {
		t.Fatal(err)
	}
	removedConf := &pb.ConfState{Voters: []uint64{2, 3, 4}}
	cluster.driveUntil(func() bool {
		for index := 1; index < len(hosts); index++ {
			publication, err := hosts[index].Publication(group)
			if err != nil || publication.Applied < 5 || !proto.Equal(publication.ConfState, removedConf) {
				return false
			}
		}
		return true
	})
	cluster.inactive[0], cluster.inactive[3] = true, true
	if err := hosts[1].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		left, leftErr := hosts[1].Status(group)
		right, rightErr := hosts[2].Status(group)
		return leftErr == nil && rightErr == nil && left.LeaderID != 0 &&
			left.LeaderID == right.LeaderID && left.LeaderID != target
	})
	if err := hosts[3].Close(); err != nil {
		t.Fatal(err)
	}
	restarted := reopen[3]()
	restartedHost, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err = restartedHost.Add(restarted); err != nil {
		_ = restartedHost.Close()
		t.Fatal(err)
	}
	hosts[3] = restartedHost
	cluster.hosts[3] = restartedHost
	t.Cleanup(func() { _ = restartedHost.Close() })
	cluster.inactive[3] = false
	cluster.driveUntil(func() bool {
		left, leftErr := hosts[1].Status(group)
		rejoined, rejoinedErr := hosts[3].Status(group)
		publication, publicationErr := hosts[3].Publication(group)
		authorityVersion, authorityFound := registries[3].ReplicaSetVersion(group)
		return leftErr == nil && rejoinedErr == nil && publicationErr == nil &&
			left.LeaderID != 0 && rejoined.LeaderID == left.LeaderID &&
			proto.Equal(publication.ConfState, removedConf) &&
			publication.ReplicaSetVersion == authorityVersion && authorityFound
	})
}
