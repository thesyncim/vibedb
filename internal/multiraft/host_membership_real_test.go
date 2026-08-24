package multiraft

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// This uses three durable Runtime instances. The third starts outside the
// voter set, receives the learner transition, catches up, and is only then
// promoted. Snapshot file transfer is deliberately not part of this test.
func TestThreeRealHostsOrderLearnerCatchUpBeforePromotion(t *testing.T) {
	identities := realTransferIdentities()
	voters := []uint64{identities[0].MemberID, identities[1].MemberID}
	hosts := make([]*Host, 3)
	registries := make([]*rafttransport.StaticRegistry, 0, 3)
	nodes := [3]rafttransport.NodeID{{1}, {2}, {3}}
	members := make([]rafttransport.Member, 3)
	for index := range hosts {
		runtime, _ := newRealTransferRuntime(t, identities[index], voters)
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
		if index == 2 {
			role = rafttransport.MemberLearner
		}
		members[index] = rafttransport.Member{Group: runtime.Identity().Group,
			ReplicaSetVersion: 1, MemberID: identities[index].MemberID,
			Node: nodes[index], Role: role}
	}
	for index := range hosts {
		registry, err := rafttransport.NewStaticRegistry(nodes[index], members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: 3})
		if err != nil {
			t.Fatal(err)
		}
		registries = append(registries, registry)
	}
	cluster := realTransferCluster{t: t, hosts: hosts, registries: registries,
		memberIndex: map[uint64]int{1: 0, 2: 1, 3: 2}}
	group := members[0].Group
	if err := hosts[0].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		for index := 0; index < 2; index++ {
			status, err := hosts[index].Status(group)
			if err != nil || status.LeaderID != 1 || status.Applied < 2 {
				return false
			}
		}
		return true
	})
	target := uint64(3)
	if err := hosts[0].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &target,
	}); err != nil {
		t.Fatal(err)
	}
	learnerConf := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
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
	voterConf := &pb.ConfState{Voters: []uint64{1, 2, 3}}
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
}
