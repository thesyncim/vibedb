package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRF3RetirementObservationRequiresExactRemovedSourceAndPinnedKey(t *testing.T) {
	group := serveRF3TestGroup()
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	var members []rafttransport.Member
	var peers []rafttransport.PhysicalPeer
	var voters [3]membershipgrant.RosterMember
	for index := 0; index < 4; index++ {
		node := rafttransport.NodeID{byte(index + 1)}
		role := rafttransport.MemberVoter
		if index == 3 {
			role = rafttransport.MemberEnrolled
		} else {
			voters[index] = membershipgrant.RosterMember{Member: uint64(index + 1), Node: node}
		}
		members = append(members, rafttransport.Member{Group: group, ReplicaSetVersion: 5, MemberID: uint64(index + 1), Node: node, Role: role})
		peers = append(peers, rafttransport.PhysicalPeer{NodeID: node, TrustDomain: domain, Incarnation: 1, Revision: 1,
			ServiceKeyDigest: [32]byte{byte(index + 11)}, Endpoint: "127.0.0.1:1234", State: rafttransport.PeerEnrolled})
	}
	registry, err := rafttransport.NewStaticRegistryWithPhysicalPeers(peers[1].NodeID, members, peers, rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	grant := membershipgrant.Grant{Group: group, TransitionID: [16]byte{1}, MetadataEpoch: 1, CatalogGeneration: 5,
		InitialReplicaSetVersion: 5, InitialVoters: [3]uint64{1, 2, 3},
		InitialRosterDigest: membershipgrant.CertifiedRosterDigest(group, 5, voters), InitialDescriptorDigest: [32]byte{1},
		SourceMember: 1, TargetMember: 4, TargetNode: peers[3].NodeID}
	if err := registry.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: rafttransport.NodeID{9}, Capabilities: serviceauthz.CapabilityMembership}})
	if err != nil {
		t.Fatal(err)
	}
	authorize := rf3AuthenticatedReplicaObservationAuthorizer(registry, policy)
	peer := rafttransport.PeerBinding{Identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: peers[0].NodeID}, ServiceKeyDigest: peers[0].ServiceKeyDigest}
	request := replicacontrol.Request{Operation: [32]byte{1}, Step: [32]byte{2}, Group: group, TargetMember: 4, ExpectedReplicaSetVersion: 8}
	if authorize(peer, request) {
		t.Fatal("unremoved source received retirement read authority")
	}
	for index, conf := range []*pb.ConfState{{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
		{Voters: []uint64{1, 2, 3, 4}}, {Voters: []uint64{2, 3, 4}}} {
		if err := registry.PublishCommittedAuthority(group, uint64(index+6), conf); err != nil {
			t.Fatal(err)
		}
	}
	if !authorize(peer, request) {
		t.Fatal("certified removed source cannot read its final membership")
	}
	for _, name := range []string{"wrong-key", "other-node", "other-group", "other-cluster", "other-target", "old-version", "discovery", "health", "missing-operation"} {
		t.Run(name, func(t *testing.T) {
			badPeer, badRequest := peer, request
			switch name {
			case "wrong-key":
				badPeer.ServiceKeyDigest[0]++
			case "other-node":
				badPeer.Identity.Node = peers[1].NodeID
			case "other-group":
				badRequest.Group.GroupID[0]++
			case "other-cluster":
				badPeer.Identity.TrustDomain.ClusterID[0]++
			case "other-target":
				badRequest.TargetMember = 3
			case "old-version":
				badRequest.ExpectedReplicaSetVersion = 7
			case "discovery":
				badRequest.ExpectedReplicaSetVersion = 0
			case "health":
				badRequest.HealthOnly = true
			case "missing-operation":
				badRequest.Operation = [32]byte{}
			}
			if authorize(badPeer, badRequest) {
				t.Fatal("request exceeded exact retirement observation authority")
			}
		})
	}
	if err := registry.RevokeTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	if authorize(peer, request) {
		t.Fatal("retired grant retained observation authority")
	}
}
