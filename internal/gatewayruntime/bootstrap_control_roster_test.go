package gatewayruntime

import (
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestBootstrapControlRosterRetainsCurrentPhysicalStorage(t *testing.T) {
	gatewayNode := rafttransport.NodeID{1}
	nodes := []rafttransport.NodeID{gatewayNode}
	cut := serviceauthz.ServiceDirectoryCut{}
	for index, lifecycle := range []serviceauthz.ServiceLifecycle{serviceauthz.ServiceJoining, serviceauthz.ServiceActive, serviceauthz.ServiceDraining} {
		node := rafttransport.NodeID{byte(index + 2)}
		cut.Bindings = append(cut.Bindings, serviceauthz.ServiceBinding{Principal: node, PhysicalNode: node, Roles: serviceauthz.ServiceRoleStorage, Lifecycle: lifecycle})
	}
	retired := rafttransport.NodeID{8}
	cut.Bindings = append(cut.Bindings, serviceauthz.ServiceBinding{Principal: retired, PhysicalNode: retired, Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceDecommissioned})
	// A service alias cannot acquire the physical-node bootstrap capability.
	cut.Bindings = append(cut.Bindings, serviceauthz.ServiceBinding{Principal: rafttransport.NodeID{9}, PhysicalNode: gatewayNode, Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive})
	got := appendBootstrapControlNodes(nodes, cut)
	want := []rafttransport.NodeID{{1}, {2}, {3}, {4}}
	if !slices.Equal(got, want) {
		t.Fatalf("roster=%v want=%v", got, want)
	}
	if again := appendBootstrapControlNodes(got, cut); !slices.Equal(again, want) {
		t.Fatalf("refresh duplicated identities: %v", again)
	}
	cut.Bindings = nil
	if removed := appendBootstrapControlNodes(nodes, cut); !slices.Equal(removed, []rafttransport.NodeID{gatewayNode}) {
		t.Fatalf("removed identities retained: %v", removed)
	}
}
