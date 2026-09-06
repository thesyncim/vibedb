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

func TestScalingPlacementUsesCurrentCutWithoutRewritingDirectory(t *testing.T) {
	node := gateway.NodeRecord{NodeID: [16]byte{1}, Incarnation: 1, ServiceKeyDigest: [32]byte{2}, DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", Roles: gateway.NodeRoleStorage, FailureDomain: "worker", Lifecycle: gateway.NodeActive, Revision: 3, CatalogGeneration: 1}
	cut := gateway.NodeDirectoryCut{Revision: 5, Digest: [32]byte{9}, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{node}}
	nodes, err := scalingPlacementNodes(cut, 4)
	if err != nil || len(nodes) != 1 || nodes[0].CatalogGeneration != 4 || nodes[0].Revision != 3 || cut.Nodes[0] != node {
		t.Fatalf("directory projection lost immutable cut: %+v %v", nodes, err)
	}
	if _, err := scalingPlacementNodes(cut, 0); err == nil {
		t.Fatal("zero catalog cut accepted")
	}
	cut.CatalogGeneration = 5
	if _, err := scalingPlacementNodes(cut, 4); err == nil {
		t.Fatal("future directory cut accepted")
	}
}
