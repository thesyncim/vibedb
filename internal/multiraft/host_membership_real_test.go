package multiraft

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// This uses an RF3 voter set plus one enrolled replacement. The replacement
// catches up as a learner before promotion; the old voter is removed only
// after transfer, then the remaining RF3 survives loss of the new leader.
func TestThreeRealHostsOrderLearnerCatchUpBeforePromotion(t *testing.T) {
	preflightRealTransferLearnerGrant(t)
	identities := realTransferIdentities()
	voters := []uint64{identities[0].MemberID, identities[1].MemberID, identities[2].MemberID}
	hosts := make([]*Host, 4)
	reopen := make([]func() *raftmember.Runtime, 4)
	registries := make([]*rafttransport.StaticRegistry, 0, 4)
	nodes := [4]rafttransport.NodeID{{1}, {2}, {3}, {4}}
	members := make([]rafttransport.Member, 4)
	for index := range hosts {
		// This fixture starts after learner enrollment; a persisted and applied
		// configuration entry establishes version 2 before serving begins.
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
			ReplicaSetVersion: 2, MemberID: identities[index].MemberID,
			Node: nodes[index], Role: role}
	}
	for index := range hosts {
		registry, err := rafttransport.NewStaticRegistry(nodes[index], members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.InstallTransitionGrant(realTransferMembershipGrant(members[0].Group)); err != nil {
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
			if err != nil || status.LeaderID != 1 || status.Applied < 3 {
				return false
			}
		}
		return true
	})
	target := uint64(4)
	authorizationDigest := realTransferMembershipGrant(group).Digest()
	learnerConf := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	cluster.driveUntilConvergedIdle(func() bool {
		for _, host := range hosts {
			publication, err := host.Publication(group)
			if err != nil || publication.Applied < 3 ||
				publication.ConfState.Equivalent(learnerConf) != nil {
				return false
			}
		}
		progress, found, err := hosts[0].Progress(group, target)
		return err == nil && found && progress.Learner && progress.RecentActive &&
			progress.Match >= 3 && progress.PendingSnapshot == 0
	})
	if err := hosts[0].ProposeConfChange(group, &pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: &target,
		Context: append([]byte(nil), authorizationDigest[:]...),
	}); err != nil {
		t.Fatal(err)
	}
	cluster.pausePromotion = true
	voterConf := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	cluster.driveUntilWithLeaderTicks(func() bool {
		for index := 0; index < 3; index++ {
			publication, err := hosts[index].Publication(group)
			if err != nil || publication.Applied < 4 ||
				publication.ConfState.Equivalent(voterConf) != nil {
				return false
			}
		}
		return true
	})
	// Committing voters need not carry the new commit index to a learner in the
	// same append exchange. Cross every protocol-idle boundary with a real tick
	// on the currently observed leader until member 4 has persisted the commit
	// and DurablePromotion reconstructs the unapplied entry. The cluster pauses
	// only from that exact WAL/HardState/publication evidence.
	cluster.driveUntilWithLeaderTicks(func() bool {
		if !cluster.promotionPaused {
			return false
		}
		proof, found, proofErr := hosts[3].DurablePromotion(group, target)
		status, statusErr := hosts[3].Status(group)
		publication, publicationErr := hosts[3].Publication(group)
		if proofErr != nil || statusErr != nil || publicationErr != nil {
			t.Fatal(errors.Join(proofErr, statusErr, publicationErr))
		}
		if !found || proof.TargetMember != target || status.Commit < proof.Version ||
			publication.ReplicaSetVersion >= proof.Version {
			t.Fatalf("promotion pause lacks durable unapplied witness: proof=%+v found=%t status=%+v publication=%+v",
				proof, found, status, publication)
		}
		return true
	})
	beforeRestart, err := hosts[3].Publication(group)
	if err != nil || beforeRestart.ConfState.Equivalent(learnerConf) != nil ||
		beforeRestart.ReplicaSetVersion != 2 {
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
	if err = restartRegistry.InstallTransitionGrant(realTransferMembershipGrant(group)); err != nil {
		t.Fatal(err)
	}
	proof, found, err := restartedPromotionHost.DurablePromotion(group, target)
	if err != nil || !found || proof.Version != 4 {
		t.Fatalf("reconstructed promotion proof=%+v found=%t err=%v", proof, found, err)
	}
	if err = restartRegistry.PublishDurablePromotion(group, proof); err != nil {
		t.Fatal(err)
	}
	registries[3], cluster.registries[3] = restartRegistry, restartRegistry
	cluster.inactive[0], cluster.inactive[3] = true, false
	cluster.holdTargetUntilVote = true
	cluster.suppressTargetTicks = true
	if err = hosts[1].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	replacementLeader := uint64(0)
	cluster.driveUntilWithStaggeredVoterClocks(func() bool {
		leader := uint64(0)
		for index := 1; index < len(hosts); index++ {
			status, statusErr := hosts[index].Status(group)
			publication, publicationErr := hosts[index].Publication(group)
			if statusErr != nil || publicationErr != nil || status.LeaderID == 0 ||
				status.LeaderID == voters[0] || status.LeaderID == target ||
				publication.ConfState.Equivalent(voterConf) != nil {
				return false
			}
			if leader == 0 {
				leader = status.LeaderID
			} else if leader != status.LeaderID {
				return false
			}
		}
		replacementLeader = leader
		return true
	}, voters[1], voters[2], 2*raftmodel.ElectionTick)
	if !cluster.promotionVoteSeen {
		t.Fatal("reopened target did not admit promotion-generation election traffic")
	}
	cluster.suppressTargetTicks = false
	replacementLeaderIndex, ok := cluster.memberIndex[replacementLeader]
	if !ok {
		t.Fatalf("elected replacement leader %d has no host", replacementLeader)
	}
	if err = hosts[replacementLeaderIndex].TransferLeader(group, target); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntilConvergedIdle(func() bool {
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
	cluster.driveUntilConvergedIdle(func() bool {
		for index := 1; index < len(hosts); index++ {
			publication, err := hosts[index].Publication(group)
			if err != nil || publication.Applied < 5 || publication.ConfState.Equivalent(removedConf) != nil {
				return false
			}
		}
		return true
	})
	cluster.inactive[3] = true
	if err := hosts[1].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntilWithStaggeredVoterClocks(func() bool {
		left, leftErr := hosts[1].Status(group)
		right, rightErr := hosts[2].Status(group)
		return leftErr == nil && rightErr == nil && left.LeaderID != 0 &&
			left.LeaderID == right.LeaderID && left.LeaderID != target
	}, voters[1], voters[2], 2*raftmodel.ElectionTick)
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
	// Reopen restores the exact committed membership authority, but LeaderID is
	// volatile Raft state. At a protocol-idle cut the restarted voter therefore
	// needs one real heartbeat from the current leader before it can prove the
	// same live leader as the other voters. Supply only leader ticks: no
	// publication, authority, or applied-position witness is synthesized.
	cluster.driveUntilWithLeaderTicks(func() bool {
		left, leftErr := hosts[1].Status(group)
		rejoined, rejoinedErr := hosts[3].Status(group)
		publication, publicationErr := hosts[3].Publication(group)
		authorityVersion, authorityFound := registries[3].ReplicaSetVersion(group)
		return leftErr == nil && rejoinedErr == nil && publicationErr == nil &&
			left.LeaderID != 0 && rejoined.LeaderID == left.LeaderID &&
			publication.ConfState.Equivalent(removedConf) == nil &&
			publication.ReplicaSetVersion == authorityVersion && authorityFound
	})
}

func preflightRealTransferLearnerGrant(t *testing.T) {
	t.Helper()
	identity := realTransferIdentities()[0]
	bootstrap := realTransferStaticBootstrap(t, []uint64{1, 2, 3}, []uint64{4})
	if bootstrap.GetMetadata().GetIndex() != 1 || bootstrap.GetMetadata().GetTerm() != 1 {
		t.Fatal("static bootstrap must remain index one, term one")
	}
	advanced := proto.Clone(bootstrap).(*pb.Snapshot)
	index := uint64(2)
	advanced.Metadata.Index = &index
	if _, err := replicatedstate.StaticBootstrapForSnapshot(advanced); !errors.Is(err, replicatedstate.ErrStaticSnapshotOnly) {
		t.Fatalf("raw advanced snapshot accepted as static bootstrap: %v", err)
	}
	entry := realTransferLearnerCheckpoint(t, identity, []uint64{4})
	var change pb.ConfChange
	if err := proto.Unmarshal(entry.Data, &change); err != nil || entry.GetIndex() != 2 ||
		entry.GetTerm() != 1 || entry.GetType() != pb.EntryConfChange ||
		change.GetType() != pb.ConfChangeAddLearnerNode || change.GetNodeId() != 4 {
		t.Fatalf("invalid progressed learner checkpoint: entry=%v change=%v err=%v", entry, &change, err)
	}
	group := raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
		TopologyRecoveryEpoch: 29, ShardIncarnation: identity.ShardIncarnation, GroupID: identity.GroupID}
	grant := realTransferMembershipGrant(group)
	grantDigest := grant.Digest()
	if !bytes.Equal(change.Context, grantDigest[:]) {
		t.Fatal("learner checkpoint lost its exact transition grant")
	}
	for _, version := range []uint64{1, 2} {
		members := make([]rafttransport.Member, 4)
		for i := range members {
			role := rafttransport.MemberVoter
			if i == 3 {
				role = rafttransport.MemberLearner
			}
			members[i] = rafttransport.Member{Group: group, ReplicaSetVersion: version,
				MemberID: uint64(i + 1), Node: rafttransport.NodeID{byte(i + 1)}, Role: role}
		}
		registry, err := rafttransport.NewStaticRegistry(members[0].Node, members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		err = registry.InstallTransitionGrant(grant)
		if version == grant.InitialReplicaSetVersion {
			if !errors.Is(err, rafttransport.ErrReplicaSet) {
				t.Fatalf("learner at original voter-only version accepted: %v", err)
			}
		} else if err != nil {
			t.Fatalf("certified progressed learner cut rejected: %v", err)
		}
	}
}

func TestRealTransferGrantLearnerRequiresProgressedCut(t *testing.T) {
	preflightRealTransferLearnerGrant(t)
}

func realTransferMembershipGrant(group raftmember.GroupKey) membershipgrant.Grant {
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{1}, MetadataEpoch: 2, CatalogGeneration: 3,
		InitialReplicaSetVersion: 1, InitialVoters: [3]uint64{1, 2, 3},
		InitialDescriptorDigest: [32]byte{2}, SourceMember: 1, TargetMember: 4,
		TargetNode: [16]byte{4},
	}
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 1,
		[3]membershipgrant.RosterMember{
			{Member: 1, Node: [16]byte{1}},
			{Member: 2, Node: [16]byte{2}},
			{Member: 3, Node: [16]byte{3}},
		})
	return grant
}
