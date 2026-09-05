package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

func TestRF3ReplicaObservationFollowsCommittedGroupAdoption(t *testing.T) {
	manifest := serveRF3TestManifest()
	parent := serveRF3TestGroup()
	child := parent
	child.GroupID[1]++
	child.ShardIncarnation[1]++
	roster := make([]rafttransport.Member, len(manifest.Members))
	for index, member := range manifest.Members {
		roster[index] = rafttransport.Member{Group: parent, ReplicaSetVersion: 1,
			MemberID: member.MemberID, Node: member.NodeID, Role: rafttransport.MemberVoter}
	}
	registry, err := rafttransport.NewStaticRegistry(manifest.Members[0].NodeID, roster,
		rafttransport.Limits{MaxGroups: 2, MaxMembers: 2 * len(roster)})
	if err != nil {
		t.Fatal(err)
	}
	controller := rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: rafttransport.NodeID{10}}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: controller.Node, Capabilities: serviceauthz.CapabilityMembership}})
	if err != nil {
		t.Fatal(err)
	}
	authorize := rf3ReplicaObservationAuthorizer(registry, policy)
	request := replicacontrol.Request{Group: child, TargetMember: 1}
	if authorize(controller, request) {
		t.Fatal("unadopted child was authorized")
	}
	for index := range roster {
		roster[index].Group = child
	}
	if err = registry.InstallGroup(roster, func(publish func()) error {
		if authorize(controller, request) {
			t.Fatal("child became visible before owner publication")
		}
		publish()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !authorize(controller, request) {
		t.Fatal("committed child was omitted from observation authority")
	}
	wrong := controller
	wrong.Node[0]++
	if authorize(wrong, request) {
		t.Fatal("child observation bypassed membership capability")
	}
	wrong = controller
	wrong.TrustDomain.ClusterIncarnation[0]++
	if authorize(wrong, request) {
		t.Fatal("child observation bypassed trust domain")
	}
}

func TestRF3PlanObservationAuthorityLimitsSourceVoters(t *testing.T) {
	group := serveRF3TestGroup()
	nodes := [...]rafttransport.NodeID{{1}, {2}, {3}, {4}, {5}, {6}}
	roster := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: nodes[0], Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: nodes[1], Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: nodes[2], Role: rafttransport.MemberLearner},
		{Group: group, ReplicaSetVersion: 1, MemberID: 4, Node: nodes[3], Role: rafttransport.MemberEnrolled},
	}
	registry, err := rafttransport.NewStaticRegistry(nodes[0], roster,
		rafttransport.Limits{MaxGroups: 1, MaxMembers: len(roster)})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{
		{Node: nodes[0], Capabilities: serviceauthz.CapabilityDataWrite},
		{Node: nodes[1], Capabilities: serviceauthz.CapabilityDataRead},
		{Node: nodes[2], Capabilities: serviceauthz.CapabilityDataRead},
		{Node: nodes[3], Capabilities: serviceauthz.CapabilityDataRead},
		{Node: nodes[4], Capabilities: serviceauthz.CapabilityDataRead},
		{Node: nodes[5], Capabilities: serviceauthz.CapabilityMembership},
	})
	if err != nil {
		t.Fatal(err)
	}
	authorize := rf3PlanObservationAuthorizer(registry, policy)
	request := splitcontroller.PlanObservationRequest{Group: group}
	peer := func(node rafttransport.NodeID) rafttransport.PeerIdentity {
		return rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: node}
	}
	if !authorize(peer(nodes[5]), request, 1, false) ||
		!authorize(peer(nodes[5]), request, 1, true) {
		t.Fatal("membership controller lost plan-observation authority")
	}
	if !authorize(peer(nodes[1]), request, 1, true) {
		t.Fatal("exact source voter was denied source observation")
	}
	for _, candidate := range []struct {
		name    string
		node    rafttransport.NodeID
		request splitcontroller.PlanObservationRequest
		source  bool
	}{
		{name: "source voter child observation", node: nodes[1], request: request},
		{name: "source learner", node: nodes[2], request: request, source: true},
		{name: "enrolled-only source member", node: nodes[3], request: request, source: true},
		{name: "ordinary data reader", node: nodes[4], request: request, source: true},
		{name: "voter without data read", node: nodes[0], request: request, source: true},
		{name: "voter from another group",
			node: nodes[1], request: func() splitcontroller.PlanObservationRequest {
				wrong := request
				wrong.Group.GroupID[0]++
				return wrong
			}(), source: true,
		},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			if authorize(peer(candidate.node), candidate.request, 1, candidate.source) {
				t.Fatal("plan observation exceeded its exact source-voter authority")
			}
		})
	}
}
