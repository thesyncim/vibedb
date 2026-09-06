package gatewayruntime

import (
	"github.com/thesyncim/vibedb/gateway"
	"testing"
)

func TestJoinRetryMatchesIdentityAfterReadinessRefresh(t *testing.T) {
	original := gateway.NodeRecord{Incarnation: 1, DataAddress: "peer", NativeAddress: "native", ControlAddress: "control", Revision: 1, Lifecycle: gateway.NodeJoining}
	live := original
	live.Capacity[0] = 100
	live.Used[0] = 5
	live.MigrationCapacity = 100
	live.MigrationUsed = 5
	live.MaxReceives = 2
	live.ActiveReceives = 1
	live.Revision = 2
	live.Lifecycle = gateway.NodeActive
	live.CatalogGeneration = 4
	if !samePublicNodeRecord(original, live) {
		t.Fatal("verified capacity refresh broke join retry")
	}
	for _, change := range []func(*gateway.NodeRecord){func(n *gateway.NodeRecord) { n.ServiceKeyDigest[0]++ }, func(n *gateway.NodeRecord) { n.Incarnation++ }, func(n *gateway.NodeRecord) { n.DataAddress = "other" }, func(n *gateway.NodeRecord) { n.FailureDomain = "other" }} {
		changed := live
		change(&changed)
		if samePublicNodeRecord(original, changed) {
			t.Fatal("identity substitution accepted")
		}
	}
}

func TestClusterControlBlockerErrorsRemainEncodable(t *testing.T) {
	blockers := clusterBlockers([]gateway.ScalingBlocker{{Code: "controller_error", Detail: "capacity failed\nsource stale\rretry"}})
	if len(blockers) != 1 || blockers[0].Detail != "capacity failed source stale retry" {
		t.Fatalf("noncanonical blocker: %+v", blockers)
	}
}
