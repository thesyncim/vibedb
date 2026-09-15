package gatewayruntime

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type terminalStatusDirectory struct {
	gateway.DirectoryReader
	intent   gateway.ScalingIntent
	node     gateway.NodeRecord
	evidence gateway.NodeReferenceEvidence
}

func (directory *terminalStatusDirectory) ListNodes(context.Context) ([]gateway.NodeRecord, error) {
	return []gateway.NodeRecord{directory.node}, nil
}

func (directory *terminalStatusDirectory) ReadScalingIntent(context.Context, [32]byte) (gateway.ScalingIntent, error) {
	return directory.intent, nil
}

func (directory *terminalStatusDirectory) ReadNode(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error) {
	return directory.node, nil
}

func (directory *terminalStatusDirectory) ListEnrollmentIntents(context.Context, raftmember.GroupKey) ([]gateway.GroupEnrollmentIntent, error) {
	return nil, nil
}

func (directory *terminalStatusDirectory) ScanNodeReferences(context.Context, rafttransport.NodeID, uint64) (gateway.NodeReferenceEvidence, error) {
	return directory.evidence, nil
}

type terminalStatusCatalog struct{}

func (terminalStatusCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return &gateway.Snapshot{}, nil
}

func TestClusterControlStatusUsesFreshTerminalProof(t *testing.T) {
	nodeID := rafttransport.NodeID{1}
	request := gateway.ScalingIntentRequest{Kind: gateway.ScalingDecommission, RequestID: [32]byte{2},
		Drain: gateway.NodeReference{NodeID: nodeID, Incarnation: 1}, MaxMoves: 1}
	intent := gateway.ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: 7,
		Revision: 2, DirectoryRevision: 2, State: gateway.ScalingComplete,
		Blockers: []gateway.ScalingBlocker{{Code: "controller_error", Detail: "stale transient blocker"}}}
	evidence := gateway.NodeReferenceEvidence{NodeID: nodeID, Incarnation: 1,
		CatalogGeneration: 7, DirectoryRevision: 3, DirectoryCutRevision: 3,
		DirectoryCutDigest: [32]byte{1}, CatalogHeadDigest: [32]byte{2},
		ScalingDirectoryDigest: [32]byte{3}, EnrollmentDirectoryDigest: [32]byte{4},
		OperationDirectoryDigest: [32]byte{5}, Digest: [32]byte{6}}
	directory := &terminalStatusDirectory{intent: intent,
		node: gateway.NodeRecord{NodeID: nodeID, Incarnation: 1, Lifecycle: gateway.NodeDecommissioned,
			Revision: 3, CatalogGeneration: 7}, evidence: evidence}
	backend := &ScalingOperatorBackend{directory: directory, catalog: terminalStatusCatalog{}}

	response := backend.observeOnce(context.Background(), clustercontrol.Response{}, intent.ID)
	if !response.SafeToStop || response.State != "decommissioned" || response.RetiringReferences != 0 || len(response.Blockers) != 0 {
		t.Fatalf("fresh terminal proof was obscured by stale blockers: response=%+v", response)
	}

	directory.evidence.ServingReplicas = 1
	response = backend.observeOnce(context.Background(), clustercontrol.Response{}, intent.ID)
	if response.SafeToStop || response.RetiringReferences != 1 || len(response.Blockers) == 0 {
		t.Fatalf("live reference was not retained as a blocker: response=%+v", response)
	}
	foundServing := false
	for _, blocker := range response.Blockers {
		if blocker.Code == "serving_replicas" {
			foundServing = true
		}
	}
	if !foundServing {
		t.Fatalf("fresh serving reference blocker missing: response=%+v", response)
	}
}
