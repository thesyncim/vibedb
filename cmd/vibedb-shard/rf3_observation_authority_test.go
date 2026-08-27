package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
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
