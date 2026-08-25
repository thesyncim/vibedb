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
		runtime, _, reopenRuntime := newRealTransferRuntimeWithLearners(
			t, identities[index], voters, []uint64{identities[3].MemberID})
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
			role = rafttransport.MemberLearner
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
		group: members[0].Group, syncAuthority: true, promotionTarget: 4}
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
	authorizationDigest := raftmember.MembershipTransitionDigest(group,
		[16]byte{1}, 2, 3, 1, 4)
	learnerConf := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	cluster.driveUntil(func() bool {
		for _, host := range hosts {
			publication, err := host.Publication(group)
			if err != nil || publication.Applied < 2 ||
				!proto.Equal(publication.ConfState, learnerConf) {
				return false
			}
		}
		progress, found, err := hosts[0].Progress(group, target)
		return err == nil && found && progress.Learner && progress.RecentActive &&
			progress.Match >= 2 && progress.PendingSnapshot == 0
	})
	if err := hosts[0].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: &target,
		Context: append([]byte(nil), authorizationDigest[:]...),
	}); err != nil {
		t.Fatal(err)
	}
	cluster.pausePromotion = true
	voterConf := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	cluster.driveUntil(func() bool {
		for index := 0; index < 3; index++ {
			publication, err := hosts[index].Publication(group)
			if err != nil || publication.Applied < 3 ||
				!proto.Equal(publication.ConfState, voterConf) {
				return false
			}
		}
		return true
	})
	// Committing voters need not send the new commit index to a learner in the
	// same append exchange. Drive one real heartbeat turn so member 4 durably
	// learns the quorum commit before the test pauses its apply. This is a
	// protocol gate, not a timing allowance: production clocks provide the same
	// heartbeat, and DurablePromotion remains false until that HardState is
	// persisted locally.
	if !cluster.promotionPaused {
		if err := hosts[0].RequestTick(group); err != nil {
			t.Fatal(err)
		}
		cluster.driveUntil(func() bool { return cluster.promotionPaused })
	}
	beforeRestart, err := hosts[3].Publication(group)
	if err != nil || !proto.Equal(beforeRestart.ConfState, learnerConf) ||
		beforeRestart.ReplicaSetVersion >= 3 {
		t.Fatalf("target published promotion before crash: %+v, %v", beforeRestart, err)
	}
	if err = hosts[3].Close(); err != nil {
		t.Fatal(err)
	}
	restartedPromotion := reopen[3]()
	restartedPromotionHost, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err = restartedPromotionHost.Add(restartedPromotion); err != nil {
		_ = restartedPromotionHost.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restartedPromotionHost.Close() })
	hosts[3], cluster.hosts[3] = restartedPromotionHost, restartedPromotionHost
	restartMembers := append([]rafttransport.Member(nil), members...)
	for index := range restartMembers {
		restartMembers[index].ReplicaSetVersion = beforeRestart.ReplicaSetVersion
	}
	restartMembers[3].Role = rafttransport.MemberLearner
	restartRegistry, err := rafttransport.NewStaticRegistry(nodes[3], restartMembers,
		rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err = restartRegistry.AuthorizeTransition(rafttransport.TransitionGrant{
		Group: group, TransitionID: [16]byte{1}, MetadataEpoch: 2,
		CatalogGeneration: 3, SourceMember: 1, TargetMember: 4,
	}); err != nil {
		t.Fatal(err)
	}
	proof, found, err := restartedPromotionHost.DurablePromotion(group, target)
	if err != nil || !found || proof.Version != 3 {
		t.Fatalf("reconstructed promotion proof=%+v found=%t err=%v", proof, found, err)
	}
	if err = restartRegistry.PublishDurablePromotion(group, proof); err != nil {
		t.Fatal(err)
	}
	registries[3], cluster.registries[3] = restartRegistry, restartRegistry
	cluster.inactive[0], cluster.inactive[3] = true, false
	cluster.holdTargetUntilVote = true
	if err = hosts[1].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		for index := 1; index < len(hosts); index++ {
			status, statusErr := hosts[index].Status(group)
			publication, publicationErr := hosts[index].Publication(group)
			if statusErr != nil || publicationErr != nil || status.LeaderID != 2 ||
				!proto.Equal(publication.ConfState, voterConf) {
				return false
			}
		}
		return true
	})
	if !cluster.promotionVoteSeen {
		t.Fatal("reopened target did not admit promotion-generation election traffic")
	}
	if err = hosts[1].TransferLeader(group, target); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		for index := 1; index < len(hosts); index++ {
			status, err := hosts[index].Status(group)
			if err != nil || status.LeaderID != target {
				return false
			}
		}
		return true
	})
	source := uint64(1)
	if err := hosts[3].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &source,
		Context: append([]byte(nil), authorizationDigest[:]...),
	}); err != nil {
		t.Fatal(err)
	}
	removedConf := &pb.ConfState{Voters: []uint64{2, 3, 4}}
	cluster.driveUntil(func() bool {
		for index := 1; index < len(hosts); index++ {
			publication, err := hosts[index].Publication(group)
			if err != nil || publication.Applied < 4 || !proto.Equal(publication.ConfState, removedConf) {
				return false
			}
		}
		return true
	})
	cluster.inactive[3] = true
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
