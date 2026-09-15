package gatewayruntime

import (
	"context"
	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"testing"
)

type readinessReaderFunc func(context.Context, rafttransport.NodeID, nodecontrol.NodeInfoRequest) (nodecontrol.NodeInfoObservation, error)

func (f readinessReaderFunc) Observe(c context.Context, n rafttransport.NodeID, r nodecontrol.NodeInfoRequest) (nodecontrol.NodeInfoObservation, error) {
	return f(c, n, r)
}
func TestScalingReadinessBindsIdentityAndCopiesMeasuredCapacity(t *testing.T) {
	node := gateway.NodeRecord{NodeID: rafttransport.NodeID{1}, Incarnation: 1, ServiceKeyDigest: replication.Digest{2}, DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", Roles: gateway.NodeRoleStorage, FailureDomain: "worker", Lifecycle: gateway.NodeJoining, Revision: 1, CatalogGeneration: 1}
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{3}, ClusterIncarnation: [16]byte{4}}
	for _, test := range []struct {
		name   string
		change func(*nodecontrol.NodeInfoStoreFacts, *nodecontrol.NodeInfoRequest)
	}{
		{"valid", nil},
		{"foreign store", func(f *nodecontrol.NodeInfoStoreFacts, _ *nodecontrol.NodeInfoRequest) { f.Identity.ClusterID[0]++ }},
		{"wrong pin", func(f *nodecontrol.NodeInfoStoreFacts, _ *nodecontrol.NodeInfoRequest) { f.SPKIPinDigest[0]++ }},
		{"wrong endpoint", func(f *nodecontrol.NodeInfoStoreFacts, _ *nodecontrol.NodeInfoRequest) {
			f.Endpoints.Control = "localhost:9"
		}},
		{"unready", func(f *nodecontrol.NodeInfoStoreFacts, _ *nodecontrol.NodeInfoRequest) {
			f.Readiness.NodeJournalReady = false
		}},
		{"replayed nonce", func(_ *nodecontrol.NodeInfoStoreFacts, r *nodecontrol.NodeInfoRequest) { r.Nonce[0] ^= 1 }},
		{"wrong incarnation", func(_ *nodecontrol.NodeInfoStoreFacts, r *nodecontrol.NodeInfoRequest) { r.Incarnation++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := scalingNodeReadiness{domain: domain, client: readinessReaderFunc(func(_ context.Context, _ rafttransport.NodeID, r nodecontrol.NodeInfoRequest) (nodecontrol.NodeInfoObservation, error) {
				f := nodecontrol.NodeInfoStoreFacts{Identity: nodecontrol.NodeInfoStoreIdentity{ClusterID: domain.ClusterID, ClusterIncarnation: domain.ClusterIncarnation, NodeID: node.NodeID}, SPKIPinDigest: node.ServiceKeyDigest, Endpoints: nodecontrol.NodeInfoEndpoints{Peer: node.DataAddress, Native: node.NativeAddress, Control: node.ControlAddress, Snapshot: "localhost:4"}, Readiness: nodecontrol.NodeInfoReadiness{NodeJournalReady: true, PhysicalStoreReady: true, BoundListenersReady: true}, InventoryRevision: 1, ActualCapacity: autosplit.CapacityVector{autosplit.ResourceLiveBytes: 100}, DeclaredCapacity: autosplit.CapacityVector{autosplit.ResourceLiveBytes: 100}, ActualMigrationCapacity: 100, DeclaredMigrationCapacity: 100, DeclaredMaxReceives: 2}
				if test.change != nil {
					test.change(&f, &r)
				}
				return f.Observation(r)
			})}
			got, err := reader.VerifyNode(t.Context(), node)
			if test.change != nil {
				if err == nil {
					t.Fatal("untrusted readiness accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := node
			expected.Capacity[autosplit.ResourceLiveBytes] = 100
			expected.MigrationCapacity = 100
			expected.MaxReceives = 2
			if got != expected {
				t.Fatalf("readiness altered identity or lost capacity: %+v", got)
			}
		})
	}
}
