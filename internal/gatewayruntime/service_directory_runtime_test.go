package gatewayruntime

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type runtimeCatalogFenceReader struct {
	gateway.DirectoryReader
	fences []serviceauthz.ServiceFence
}

func (reader runtimeCatalogFenceReader) CatalogServiceFences(context.Context) ([]serviceauthz.ServiceFence, uint64, error) {
	return reader.fences, 1, nil
}

func TestRuntimeServiceDirectoryKeepsColocatedGatewayCatalogGrants(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{1}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	key := replication.Digest{2}
	node := gateway.NodeRecord{
		NodeID: profile.LocalIdentity().Node, Incarnation: 1, ServiceKeyDigest: key,
		DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", GatewayEndpoint: "gateway",
		DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", GatewayAddress: "localhost:4",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway | gateway.NodeRoleControl,
		Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: profile.LocalIdentity().Node, Incarnation: 3, ServiceKeyDigest: key,
			ServiceID: [16]byte{4}, SessionID: [16]byte{5}, SessionRevision: 6, ParticipantDigest: replication.Digest{7},
		},
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	reader := runtimeCatalogFenceReader{fences: []serviceauthz.ServiceFence{
		{Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead,
			Group: group, Relation: [16]byte{1}, IntentID: [32]byte{2}, FenceDigest: [32]byte{3}},
		{Action: serviceauthz.ServiceActionGatewayCatalogWrite, Operation: serviceauthz.ServiceOperationCatalogWrite,
			Group: group, Relation: [16]byte{1}, IntentID: [32]byte{4}, FenceDigest: [32]byte{5}},
	}}
	snapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: 1, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{node}}
	cut, err := runtimeServiceDirectoryCut(t.Context(), reader, snapshot, profile, policy.Generation())
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Bindings) != 1 || cut.Bindings[0].Roles != serviceauthz.ServiceRoleStorage|serviceauthz.ServiceRoleGateway|serviceauthz.ServiceRoleController {
		t.Fatalf("physical and gateway identities did not merge: %+v", cut.Bindings)
	}
	peer := serviceauthz.AuthenticatedPeer{Identity: profile.LocalIdentity(), KeyDigest: [32]byte(key)}
	authority := serviceauthz.Authority{Node: node.NodeID, Generation: policy.Generation()}
	for _, fence := range reader.fences {
		if fence.SessionID != ([16]byte{}) || fence.SessionRevision != 0 {
			t.Fatal("runtime changed shared catalog grant input")
		}
		request := serviceauthz.ServiceRequest{
			Action: fence.Action, Capability: serviceauthz.CapabilityTopology, Operation: fence.Operation,
			Group: fence.Group, Relation: fence.Relation, SessionID: node.Gateway.SessionID,
			SessionRevision: node.Gateway.SessionRevision, IntentID: fence.IntentID, FenceDigest: fence.FenceDigest,
		}
		if got := gate.CheckInternal(peer, authority, request); got != serviceauthz.DecisionAllow {
			t.Fatalf("co-located gateway lost exact catalog action %d: %v", fence.Action, got)
		}
		request.FenceDigest[0]++
		if got := gate.CheckInternal(peer, authority, request); got == serviceauthz.DecisionAllow {
			t.Fatal("merged gateway admitted an ungranted catalog fence")
		}
	}
}
