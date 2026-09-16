package gatewayruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type lifecycleBarrierDrainTest struct {
	prepareCalls int
	ackNodes     []gateway.NodeLifecycle
	failTerminal bool
}

func (drain *lifecycleBarrierDrainTest) PrepareFrontendDrain(context.Context, gateway.NodeRecord) error {
	drain.prepareCalls++
	return nil
}

func (drain *lifecycleBarrierDrainTest) AcknowledgeFrontendDrainLifecycle(
	_ context.Context, node gateway.NodeRecord, _ [32]byte,
) error {
	drain.ackNodes = append(drain.ackNodes, node.Lifecycle)
	if drain.failTerminal && node.Lifecycle == gateway.NodeDecommissioned {
		return gateway.ErrScalingRevision
	}
	return nil
}

type lifecycleBarrierDirectoryTest struct {
	gateway.DirectoryReader
	gateway.DirectoryWriter
	node      gateway.NodeRecord
	evidence  gateway.NodeReferenceEvidence
	intent    gateway.ScalingIntent
	retireErr error
}

func (directory *lifecycleBarrierDirectoryTest) ReadNode(
	_ context.Context, nodeID rafttransport.NodeID, incarnation uint64,
) (gateway.NodeRecord, error) {
	if directory.node.NodeID != nodeID || directory.node.Incarnation != incarnation {
		return gateway.NodeRecord{}, gateway.ErrScalingNodeMissing
	}
	return directory.node, nil
}

func (directory *lifecycleBarrierDirectoryTest) ScanNodeReferences(
	_ context.Context, nodeID rafttransport.NodeID, incarnation uint64,
) (gateway.NodeReferenceEvidence, error) {
	if directory.node.NodeID != nodeID || directory.node.Incarnation != incarnation {
		return gateway.NodeReferenceEvidence{}, gateway.ErrScalingNodeMissing
	}
	evidence := directory.evidence
	evidence.NodeID, evidence.Incarnation = nodeID, incarnation
	evidence.DirectoryRevision = directory.node.Revision
	evidence.DirectoryCutRevision = directory.node.Revision
	return evidence, nil
}

func (directory *lifecycleBarrierDirectoryTest) ScanDecommissionedNodeReferences(
	ctx context.Context, nodeID rafttransport.NodeID, incarnation uint64,
) (gateway.NodeReferenceEvidence, error) {
	return directory.ScanNodeReferences(ctx, nodeID, incarnation)
}

func (directory *lifecycleBarrierDirectoryTest) PutScalingIntent(
	_ context.Context, next gateway.ScalingIntent, expected uint64,
) error {
	if directory.intent.Revision != expected {
		return gateway.ErrScalingRevision
	}
	directory.intent = next
	return nil
}

func (directory *lifecycleBarrierDirectoryTest) EnforceFrontendDrain(
	_ context.Context, drainID [32]byte, nodeID rafttransport.NodeID, incarnation, expected uint64,
) error {
	if drainID == ([32]byte{}) || directory.node.NodeID != nodeID || directory.node.Incarnation != incarnation ||
		directory.node.Lifecycle != gateway.NodeActive || directory.node.Revision != expected {
		return gateway.ErrScalingState
	}
	directory.node.Lifecycle = gateway.NodeDraining
	directory.node.Revision++
	return nil
}

func (directory *lifecycleBarrierDirectoryTest) RetireNode(
	_ context.Context, nodeID rafttransport.NodeID, incarnation, expected uint64,
	evidence gateway.NodeReferenceEvidence,
) error {
	if directory.retireErr != nil {
		return directory.retireErr
	}
	if directory.node.NodeID != nodeID || directory.node.Incarnation != incarnation ||
		directory.node.Lifecycle != gateway.NodeDraining || directory.node.Revision != expected {
		return gateway.ErrScalingState
	}
	directory.node.Lifecycle = gateway.NodeDecommissioned
	directory.node.Revision++
	directory.node.RetirementScanDigest = evidence.Digest
	directory.node.RetirementScanDirectoryRevision = expected
	directory.node.RetirementScanCutRevision = evidence.DirectoryCutRevision
	return nil
}

func lifecycleBarrierRetirementFixture(t *testing.T) (
	*lifecycleBarrierDirectoryTest, *lifecycleBarrierDrainTest, gateway.ScalingIntent,
) {
	t.Helper()
	node := preparedAckRosterNode(1, 21, gateway.NodeActive, gateway.NodeRoleStorage|gateway.NodeRoleGateway)
	node.Revision = 1
	evidence := gateway.NodeReferenceEvidence{
		CatalogGeneration: 1, DirectoryRevision: 1, DirectoryCutRevision: 1,
		DirectoryCutDigest: replication.Digest{2}, CatalogHeadDigest: replication.Digest{3},
		ScalingDirectoryDigest: replication.Digest{4}, EnrollmentDirectoryDigest: replication.Digest{5},
		OperationDirectoryDigest: replication.Digest{6}, GatewayDirectoryRevision: 1,
		GatewayDirectoryDigest: replication.Digest{7}, Digest: replication.Digest{8},
	}
	request := gateway.ScalingIntentRequest{Kind: gateway.ScalingDecommission, RequestID: [32]byte{9},
		Drain: gateway.NodeReference{NodeID: node.NodeID, Incarnation: node.Incarnation}, MaxMoves: 1,
		MaxMigrationBytes: 1}
	intent := gateway.ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: 1,
		Revision: 1, DirectoryRevision: 1, State: gateway.ScalingRunning}
	if !intent.Valid() {
		t.Fatal("invalid lifecycle barrier intent fixture")
	}
	directory := &lifecycleBarrierDirectoryTest{node: node, evidence: evidence, intent: intent}
	return directory, new(lifecycleBarrierDrainTest), intent
}

func TestScalingRetirementRequiresTerminalFrontendAckBeforeSafeToStop(t *testing.T) {
	directory, drain, intent := lifecycleBarrierRetirementFixture(t)
	drain.failTerminal = true
	controller := &ScalingController{directory: directory, writer: directory, drain: drain}

	done, err := controller.reconcileRetirement(t.Context(), intent)
	if done || !errors.Is(err, gateway.ErrScalingRevision) {
		t.Fatalf("terminal ACK failure done=%t err=%v", done, err)
	}
	if directory.node.Lifecycle != gateway.NodeDecommissioned || directory.intent.State == gateway.ScalingComplete {
		t.Fatalf("retirement crossed SafeToStop before terminal ACK: node=%+v intent=%+v", directory.node, directory.intent)
	}
	if len(drain.ackNodes) != 3 || drain.ackNodes[0] != gateway.NodeDraining ||
		drain.ackNodes[1] != gateway.NodeDraining || drain.ackNodes[2] != gateway.NodeDecommissioned {
		t.Fatalf("lifecycle ACK sequence=%v, want enforcing twice then terminal", drain.ackNodes)
	}

	drain.failTerminal = false
	done, err = controller.reconcileRetirement(t.Context(), intent)
	if err != nil || !done || directory.intent.State != gateway.ScalingComplete {
		t.Fatalf("terminal ACK retry done=%t err=%v intent=%+v", done, err, directory.intent)
	}
	if len(drain.ackNodes) != 4 || drain.ackNodes[3] != gateway.NodeDecommissioned {
		t.Fatalf("retry did not repeat terminal ACK: %v", drain.ackNodes)
	}
}

func TestScalingRetirementBlocksSoleDesignatedControllerBeforeAdmissionCloses(t *testing.T) {
	directory, drain, intent := lifecycleBarrierRetirementFixture(t)
	controller := &ScalingController{
		directory: directory, writer: directory, drain: drain, controllerNode: directory.node.NodeID,
	}

	done, err := controller.reconcileRetirement(t.Context(), intent)
	if done || !errors.Is(err, ErrScalingControllerBlocked) {
		t.Fatalf("sole designated controller retirement done=%t err=%v", done, err)
	}
	if directory.node.Lifecycle != gateway.NodeActive {
		t.Fatalf("controller retirement changed lifecycle before admission closes: %s", lifecycleName(directory.node.Lifecycle))
	}
	if len(directory.intent.Blockers) != 1 || directory.intent.Blockers[0].Code != "sole_designated_controller" ||
		directory.intent.Blockers[0].Node != directory.node.NodeID {
		t.Fatalf("controller blocker=%+v, want precise sole controller blocker", directory.intent.Blockers)
	}
}

func TestScalingFrontendDrainBarrierSkipsStorageOnlyNodes(t *testing.T) {
	directory, drain, intent := lifecycleBarrierRetirementFixture(t)
	directory.node.Roles = gateway.NodeRoleStorage
	controller := &ScalingController{directory: directory, writer: directory, drain: drain}
	if err := controller.acknowledgeFrontendDrainLifecycle(t.Context(), intent, directory.node); err != nil {
		t.Fatalf("storage-only node unexpectedly blocked: %v", err)
	}
	if len(drain.ackNodes) != 0 {
		t.Fatalf("storage-only node entered frontend ACK roster: %v", drain.ackNodes)
	}
}

func TestFrontendDrainRetiredAckRefusesRemoteSurvivingGatewayPublisher(t *testing.T) {
	profile, subject, _, cut, _ := frontendDrainSourceTestFixture(t)
	if len(cut.ServiceDirectory.ContinuationGrants) != 1 {
		t.Fatalf("fixture continuation grant count=%d, want one", len(cut.ServiceDirectory.ContinuationGrants))
	}
	// The subject is terminal in this cut, so it is retained only as a
	// tombstone. A survivor with a distinct gateway identity is discovery data,
	// not permission for this process to impersonate it as the publisher.
	for index := range cut.ServiceDirectory.Bindings {
		binding := &cut.ServiceDirectory.Bindings[index]
		if binding.Principal == subject.Gateway.NodeID && binding.Roles&serviceauthz.ServiceRoleGateway != 0 {
			binding.Lifecycle = serviceauthz.ServiceDecommissioned
		}
	}
	grant := cut.ServiceDirectory.ContinuationGrants[0]
	grant.State = serviceauthz.ContinuationGrantRetired
	cut.ServiceDirectory.ContinuationGrants[0] = grant
	survivor := preparedAckRosterNode(9, 29, gateway.NodeActive, gateway.NodeRoleStorage|gateway.NodeRoleGateway)
	cut.ServiceDirectory.Bindings = append(cut.ServiceDirectory.Bindings, serviceauthz.ServiceBinding{
		Principal: survivor.Gateway.NodeID, PhysicalNode: survivor.NodeID,
		PhysicalIncarnation: survivor.Incarnation, KeyDigest: [32]byte(survivor.Gateway.ServiceKeyDigest),
		Roles: serviceauthz.ServiceRoleGateway, Lifecycle: serviceauthz.ServiceActive,
		GatewayIncarnation: survivor.Gateway.Incarnation, SessionID: survivor.Gateway.SessionID,
		SessionRevision: survivor.Gateway.SessionRevision, ParticipantDigest: [32]byte(survivor.Gateway.ParticipantDigest),
	})
	if !cut.Valid() {
		t.Fatal("retired survivor publisher cut is invalid")
	}
	runtime := &Runtime{config: Config{TLSProfile: profile}}
	publisher, key, ok := runtime.frontendDrainPreparedAckPublisher(cut)
	if ok || publisher != (rafttransport.NodeID{}) || key != ([32]byte{}) {
		t.Fatalf("publisher=%s key=%x ok=%t, remote survivor was accepted", publisher, key, ok)
	}
}

func TestFrontendDrainAckPublisherRequiresLocalServingIdentity(t *testing.T) {
	profile, _, _, cut, _ := frontendDrainSourceTestFixture(t)
	runtime := &Runtime{config: Config{TLSProfile: profile}}
	publisher, key, ok := runtime.frontendDrainPreparedAckPublisher(cut)
	if !ok || publisher != profile.LocalIdentity().Node || key != profile.LocalServiceKeyDigest() {
		t.Fatalf("publisher=%s key=%x ok=%t, want local publisher=%s key=%x", publisher, key, ok,
			profile.LocalIdentity().Node, profile.LocalServiceKeyDigest())
	}
}
