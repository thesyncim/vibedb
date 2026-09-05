package gateway

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestReplicatedScalingNodeLifecycleDirectoryCutAndRestart(t *testing.T) {
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
	restarted := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x91)

	terminal := scalingTestNodeRecord([16]byte{0x11}, 1, NodeDecommissioned, 1)
	terminal.RetirementScanDigest = replication.Digest{0x01}
	terminal.RetirementScanDirectoryRevision = 1
	terminal.RetirementScanCutRevision = 1
	if err := authority.PutNode(ctx, terminal, 0); !errors.Is(err, ErrScalingRevision) {
		t.Fatalf("terminal node creation=%v", err)
	}

	joining := scalingTestNodeRecord([16]byte{0x11}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}

	// A byte-identical retry is safe after a caller loses its local response.
	if err := authority.PutNode(ctx, active, active.Revision); err != nil {
		t.Fatalf("idempotent node retry=%v", err)
	}
	nodes, revision, err := authority.ReadNodeDirectory(ctx)
	if err != nil || revision != 2 || len(nodes) != 1 || nodes[0] != active {
		t.Fatalf("first directory nodes=%+v revision=%d err=%v", nodes, revision, err)
	}

	// A generic PutNode cannot skip the drain state or manufacture a terminal
	// retirement witness.
	decommissioned := active
	decommissioned.Lifecycle = NodeDecommissioned
	decommissioned.Revision = 3
	decommissioned.RetirementScanDigest = replication.Digest{0x02}
	decommissioned.RetirementScanDirectoryRevision = 2
	decommissioned.RetirementScanCutRevision = 2
	if err := authority.PutNode(ctx, decommissioned, 2); !errors.Is(err, ErrScalingState) {
		t.Fatalf("generic retirement bypass=%v", err)
	}

	second := scalingTestNodeRecord([16]byte{0x12}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, second, 0); err != nil {
		t.Fatal(err)
	}
	cut, err := restarted.ReadNodeDirectoryCut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cut.Revision != 3 || len(cut.Nodes) != 2 || cut.Nodes[0].Revision != 2 || cut.Nodes[1].Revision != 1 {
		t.Fatalf("directory cut=%+v, want global revision 3 with a lower-revision new node", cut)
	}
	if cut.Nodes[0].NodeID[0] != 0x11 || cut.Nodes[1].NodeID[0] != 0x12 {
		t.Fatalf("directory cut is not canonical by physical node ID: %+v", cut.Nodes)
	}

	readBack, err := restarted.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || readBack != active {
		t.Fatalf("metadata did not survive authority reopen: %+v err=%v", readBack, err)
	}
	client.mu.Lock()
	applied := client.applied
	client.mu.Unlock()
	if applied == 0 {
		t.Fatal("fixture did not apply any replicated metadata mutation")
	}
	// A canonical child with a different digest is still invalid under the
	// directory cut; accepting it would let a reader assemble a mixed epoch.
	tampered := active
	tampered.Used[0] = 1
	client.mu.Lock()
	client.rows[string(scalingNodeKey(active.NodeID, active.Incarnation))] = mustAppendNode(tampered)
	client.mu.Unlock()
	if _, err := restarted.ReadNodeDirectoryCut(ctx); err == nil ||
		(!errors.Is(err, ErrReplicatedCatalogConflict) && !errors.Is(err, ErrInvalidScalingMetadata)) {
		t.Fatalf("directory accepted a canonical child with stale digest: %v", err)
	}
}

func TestReplicatedScalingReadRetriesNeverReturnMixedDirectoryCut(t *testing.T) {
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x92)

	joining := scalingTestNodeRecord([16]byte{0x21}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2

	var transitionErr error
	key := scalingNodeKey(joining.NodeID, joining.Incarnation)
	client.mu.Lock()
	client.onRead = func(readKey []byte) {
		if !bytes.Equal(readKey, key) {
			return
		}
		// Clear the hook before the injected write. The peer rereads the same
		// child while applying its CAS; retaining a sync.Once around that write
		// would recursively enter the hook and deadlock.
		client.mu.Lock()
		if client.onRead == nil {
			client.mu.Unlock()
			return
		}
		client.onRead = nil
		client.mu.Unlock()
		transitionErr = peer.PutNode(ctx, active, 1)
	}
	client.mu.Unlock()

	cut, err := authority.ReadNodeDirectoryCut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if transitionErr != nil {
		t.Fatalf("writer injected during cut read=%v", transitionErr)
	}
	if cut.Revision != 2 || len(cut.Nodes) != 1 || cut.Nodes[0].Lifecycle != NodeActive || cut.Nodes[0].Revision != 2 {
		t.Fatalf("mixed stale cut returned: revision=%d nodes=%+v", cut.Revision, cut.Nodes)
	}
}

func TestReplicatedScalingConcurrentEnrollmentAndTargetDrainCAS(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x93)
	joining := scalingTestNodeRecord([16]byte{0x31}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	intent := scalingTestEnrollmentIntent(1, joining.NodeID[0], active.Revision)
	if !intent.Valid() {
		t.Fatal("enrollment fixture is invalid")
	}
	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3

	start := make(chan struct{})
	type raceResult struct {
		kind string
		err  error
	}
	results := make(chan raceResult, 2)
	go func() {
		<-start
		results <- raceResult{kind: "enrollment", err: authority.SubmitEnrollmentIntent(ctx, intent)}
	}()
	go func() {
		<-start
		results <- raceResult{kind: "drain", err: peer.PutNode(ctx, draining, active.Revision)}
	}()
	close(start)
	first, second := <-results, <-results
	var enrollmentErr, drainErr error
	for _, result := range []raceResult{first, second} {
		switch result.kind {
		case "enrollment":
			enrollmentErr = result.err
		case "drain":
			drainErr = result.err
		default:
			t.Fatalf("unknown race participant %q", result.kind)
		}
	}
	// Both writes use the target digest as a CAS fence. A successful reserve
	// leaves the target row unchanged, so the drain may safely follow it; a
	// drain that wins makes the new reserve fail closed.
	if enrollmentErr != nil && !errors.Is(enrollmentErr, ErrScalingIdentity) && !errors.Is(enrollmentErr, ErrReplicatedCatalogConflict) && !errors.Is(enrollmentErr, ErrScalingState) {
		t.Fatalf("unexpected enrollment race error=%v", enrollmentErr)
	}
	if drainErr != nil && !errors.Is(drainErr, ErrScalingIdentity) && !errors.Is(drainErr, ErrReplicatedCatalogConflict) && !errors.Is(drainErr, ErrScalingState) {
		t.Fatalf("unexpected drain race error=%v", drainErr)
	}
	storedNode, err := authority.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	storedEnrollment, enrollmentReadErr := authority.ReadEnrollmentIntent(ctx, intent.IntentID)
	if enrollmentReadErr != nil && !errors.Is(enrollmentReadErr, ErrEnrollmentIntentMissing) {
		t.Fatal(enrollmentReadErr)
	}
	if enrollmentErr == nil && enrollmentReadErr != nil {
		t.Fatalf("successful enrollment was not durable: %v", enrollmentReadErr)
	}
	if enrollmentErr == nil && storedNode.Lifecycle != NodeDraining {
		t.Fatalf("reserve won but target could not drain: %+v", storedNode)
	}
	if enrollmentErr != nil && storedNode.Lifecycle != NodeDraining {
		t.Fatalf("drain did not win when enrollment was rejected: %+v (enrollment=%v drain=%v)", storedNode, enrollmentErr, drainErr)
	}
	if enrollmentErr == nil && storedEnrollment.State != EnrollmentReserved {
		t.Fatalf("unexpected enrollment state=%+v", storedEnrollment)
	}
}

func TestReplicatedScalingConcurrentGroupEnrollmentCreationIsUnique(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x94)
	joining := scalingTestNodeRecord([16]byte{0x41}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	first := scalingTestEnrollmentIntent(3, joining.NodeID[0], active.Revision)
	second := first
	second.IntentID = [32]byte{0x03, 0x02}
	if !first.Valid() || !second.Valid() || first.Group != second.Group {
		t.Fatalf("same-group enrollment fixtures invalid: first=%+v second=%+v", first, second)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() { <-start; results <- authority.SubmitEnrollmentIntent(ctx, first) }()
	go func() { <-start; results <- peer.SubmitEnrollmentIntent(ctx, second) }()
	close(start)
	errA, errB := <-results, <-results
	if (errA == nil) == (errB == nil) {
		t.Fatalf("same-group race produced %v and %v; exactly one reservation must win", errA, errB)
	}
	intents, err := peer.ListEnrollmentIntents(ctx, first.Group)
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].Group != first.Group {
		t.Fatalf("same-group directory contains %+v, want one active intent", intents)
	}
}

func TestReplicatedScalingStaleReservationAndAppliedResponseLoss(t *testing.T) {
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x95)
	joining := scalingTestNodeRecord([16]byte{0x51}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	stale := scalingTestEnrollmentIntent(4, joining.NodeID[0], 1)
	if err := authority.SubmitEnrollmentIntent(ctx, stale); !errors.Is(err, ErrScalingIdentity) {
		t.Fatalf("stale target revision accepted as reservation: %v", err)
	}
	if _, err := authority.ReadEnrollmentIntent(ctx, stale.IntentID); !errors.Is(err, ErrEnrollmentIntentMissing) {
		t.Fatalf("stale reservation left a durable row: %v", err)
	}
	reserved := scalingTestEnrollmentIntent(7, joining.NodeID[0], active.Revision)
	if err := authority.SubmitEnrollmentIntent(ctx, reserved); err != nil {
		t.Fatal(err)
	}
	reserved, err := authority.ReadEnrollmentIntent(ctx, reserved.IntentID)
	if err != nil {
		t.Fatal(err)
	}
	reservedDigest := scalingDigest(mustAppendEnrollmentIntent(reserved))
	if _, err := peer.ReadEnrollmentIntentAt(ctx, reserved.IntentID, reserved.Revision, reservedDigest); err != nil {
		t.Fatalf("exact enrollment restart read=%v", err)
	}
	claimed, err := peer.ClaimEnrollmentPreparation(ctx, reserved.IntentID, reserved.Revision)
	if err != nil {
		t.Fatal(err)
	}
	prepared := claimed
	prepared.State = EnrollmentPrepared
	prepared.Revision++
	prepared.PreparationClaim = [32]byte{}
	proof := scalingTestPreparedProof(prepared, active.Revision)
	prepared.Proof = &proof
	if err := peer.PutEnrollmentIntent(ctx, prepared, claimed.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.ReadEnrollmentIntentAt(ctx, reserved.IntentID, reserved.Revision, reservedDigest); !errors.Is(err, ErrScalingRevision) {
		t.Fatalf("stale enrollment revision/digest read=%v", err)
	}

	request := ScalingIntentRequest{
		Kind:              ScalingScaleOut,
		RequestID:         [32]byte{0xa1},
		Targets:           []NodeReference{{NodeID: [16]byte{0x61}, Incarnation: 1}},
		DesiredNodeCount:  2,
		MaxMoves:          2,
		MaxMigrationBytes: 1 << 20,
	}
	intent := ScalingIntent{
		ID:                request.ID(),
		Request:           request,
		CatalogGeneration: 5,
		Revision:          1,
		DirectoryRevision: 1,
		State:             ScalingReserved,
	}
	if !intent.Valid() {
		t.Fatal("scaling intent fixture is invalid")
	}
	client.mu.Lock()
	client.unknownNext = true
	client.mu.Unlock()
	if err := authority.SubmitScalingIntent(ctx, intent); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("response loss error=%v, want pending outcome", err)
	}
	client.mu.Lock()
	client.holdUnknown = false
	client.mu.Unlock()
	if err := authority.RetryPending(ctx); err != nil {
		t.Fatal(err)
	}
	readBack, err := peer.ReadScalingIntent(ctx, intent.ID)
	if err != nil || !readBack.Valid() || readBack.ID != intent.ID {
		t.Fatalf("applied response-loss retry read=%+v err=%v", readBack, err)
	}
	priorDigest := scalingDigest(mustAppendScalingIntent(intent))
	running := intent
	running.State = ScalingRunning
	running.Revision = 2
	running.DirectoryRevision = 2
	if err := authority.PutScalingIntent(ctx, running, intent.Revision); err != nil {
		t.Fatalf("scaling intent advance=%v", err)
	}
	if _, err := peer.ReadScalingIntentAt(ctx, intent.ID, intent.Revision, priorDigest); !errors.Is(err, ErrScalingRevision) {
		t.Fatalf("stale scaling intent read=%v", err)
	}
	if err := authority.PutScalingIntent(ctx, running, running.Revision); err != nil {
		t.Fatalf("exact post-retry metadata replay=%v", err)
	}
}

func TestReplicatedScalingCancelledReservationReleasesDrainAcrossAuthorities(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x99)
	joining := scalingTestNodeRecord([16]byte{0x61}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}

	request := ScalingIntentRequest{
		Kind:              ScalingScaleIn,
		RequestID:         [32]byte{0x62},
		Drain:             NodeReference{NodeID: active.NodeID, Incarnation: active.Incarnation},
		MaxMoves:          1,
		MaxMigrationBytes: 1 << 20,
		HysteresisPPM:     50_000,
	}
	intent := ScalingIntent{
		ID: request.ID(), Request: request, CatalogGeneration: 5,
		Revision: 1, DirectoryRevision: 1, State: ScalingReserved,
	}
	if !intent.Valid() {
		t.Fatal("scaling cancellation fixture is invalid")
	}
	if err := authority.SubmitScalingIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := peer.PutNode(ctx, draining, active.Revision); err != nil {
		t.Fatal(err)
	}

	cancelled := intent
	cancelled.State = ScalingCancelled
	cancelled.Revision = 2
	cancelled.DirectoryRevision = 2
	if err := peer.PutScalingIntent(ctx, cancelled, intent.Revision); err != nil {
		t.Fatal(err)
	}
	readBack, err := authority.ReadScalingIntent(ctx, intent.ID)
	if err != nil || readBack.State != ScalingCancelled || readBack.Revision != cancelled.Revision {
		t.Fatalf("cancelled reservation did not survive cross-authority resume: %+v err=%v", readBack, err)
	}

	evidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.OutstandingMoves != 0 || !evidence.ZeroAllReferences() {
		t.Fatalf("cancelled reservation still blocked safe-to-stop: %+v", evidence)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, evidence); err != nil {
		t.Fatal(err)
	}
	retired, err := peer.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || retired.Lifecycle != NodeDecommissioned {
		t.Fatalf("cancelled reservation did not release terminal retirement: %+v err=%v", retired, err)
	}
}

func TestReplicatedScalingCancelledEnrollmentReleasesTargetAcrossAuthorities(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x9a)
	target := scalingTestNodeRecord([16]byte{0xa4}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, target, 0); err != nil {
		t.Fatal(err)
	}
	active := target
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}

	intent := scalingTestEnrollmentIntent(0xa5, active.NodeID[0], active.Revision)
	if !intent.Valid() {
		t.Fatal("enrollment cancellation fixture is invalid")
	}
	if err := authority.SubmitEnrollmentIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.CancelEnrollmentIntent(ctx, intent.IntentID, intent.Revision+1); !errors.Is(err, ErrScalingRevision) {
		t.Fatalf("stale enrollment cancellation=%v", err)
	}
	stillReserved, err := authority.ReadEnrollmentIntent(ctx, intent.IntentID)
	if err != nil || stillReserved.State != EnrollmentReserved || stillReserved.Revision != intent.Revision {
		t.Fatalf("stale cancellation changed reservation: %+v err=%v", stillReserved, err)
	}
	cancelled, err := peer.CancelEnrollmentIntent(ctx, intent.IntentID, intent.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != EnrollmentCancelled || cancelled.Revision != intent.Revision+1 {
		t.Fatalf("cancelled enrollment=%+v", cancelled)
	}
	readBack, err := authority.ReadEnrollmentIntent(ctx, intent.IntentID)
	if err != nil || readBack.State != EnrollmentCancelled || readBack.Revision != cancelled.Revision {
		t.Fatalf("cancelled enrollment did not survive cross-authority resume: %+v err=%v", readBack, err)
	}
	if intents, err := peer.ListEnrollmentIntents(ctx, intent.Group); err != nil {
		t.Fatal(err)
	} else if len(intents) != 0 {
		t.Fatalf("cancelled reservation remained in active group directory: %+v", intents)
	}

	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := authority.PutNode(ctx, draining, active.Revision); err != nil {
		t.Fatal(err)
	}
	evidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.LearnerReplicas != 0 || !evidence.ZeroAllReferences() {
		t.Fatalf("cancelled enrollment still blocked safe-to-stop: %+v", evidence)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, evidence); err != nil {
		t.Fatal(err)
	}
	retired, err := peer.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || retired.Lifecycle != NodeDecommissioned {
		t.Fatalf("cancelled enrollment did not release terminal retirement: %+v err=%v", retired, err)
	}
}

func TestReplicatedScalingReferenceScanBlocksRetirement(t *testing.T) {
	ctx := context.Background()
	authority, _, _ := newCatalogAuthorityFixture(t)
	joining := scalingTestNodeRecord([16]byte{0x71}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	reserved := scalingTestEnrollmentIntent(5, joining.NodeID[0], active.Revision)
	if err := authority.SubmitEnrollmentIntent(ctx, reserved); err != nil {
		t.Fatal(err)
	}
	reserved, err := authority.ReadEnrollmentIntent(ctx, reserved.IntentID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := authority.ClaimEnrollmentPreparation(ctx, reserved.IntentID, reserved.Revision)
	if err != nil {
		t.Fatal(err)
	}
	prepared := claimed
	prepared.State = EnrollmentPrepared
	prepared.Revision++
	prepared.PreparationClaim = [32]byte{}
	proof := scalingTestPreparedProof(prepared, active.Revision)
	prepared.Proof = &proof
	if err := authority.PutEnrollmentIntent(ctx, prepared, claimed.Revision); err != nil {
		t.Fatal(err)
	}

	operation := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0x71, 0x01}, Kind: ReplicatedOperationMove,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
		Proof: [32]byte{0x72},
	})
	if err := authority.SubmitOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := authority.PutNode(ctx, draining, 2); err != nil {
		t.Fatal(err)
	}
	evidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.LearnerReplicas != 1 || evidence.OutstandingMoves == 0 || evidence.Digest == (replication.Digest{}) {
		t.Fatalf("reference scan omitted durable blockers: %+v", evidence)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, evidence); !errors.Is(err, ErrScalingState) {
		t.Fatalf("retirement ignored enrollment/proof/move references: %v", err)
	}
}

func TestReplicatedScalingPublishEnrollmentReceiptPersistsCatalogCut(t *testing.T) {
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
	target := scalingTestNodeRecord([16]byte{0x72}, 1, NodeJoining, 1)
	target.DataEndpoint = "ep-b"
	target.NativeEndpoint = "ep-b-native"
	target.ControlEndpoint = "ep-b-control"
	if err := authority.PutNode(ctx, target, 0); err != nil {
		t.Fatal(err)
	}
	active := target
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	descriptor := current.ReplicatedShardDescriptors()[0]
	sourceDescriptor := descriptor.Replicas[0]
	client.mu.Lock()
	head := bytes.Clone(client.rows[string(replicatedCatalogHeadKey)])
	client.mu.Unlock()
	intent := GroupEnrollmentIntent{
		IntentID:                  [32]byte{0xc1, 1},
		Group:                     descriptor.Group,
		Distribution:              descriptor.Distribution,
		Shard:                     descriptor.Shard,
		AllocationGeneration:      descriptor.AllocationGeneration,
		CatalogGeneration:         current.Generation(),
		ExpectedCatalogHeadDigest: scalingDigest(head),
		ReplicaOrdinal:            0,
		Source: ReplicaIdentity{
			Member: sourceDescriptor.Member, Node: sourceDescriptor.Node, StoreID: sourceDescriptor.StoreID,
			NodeIncarnation: sourceDescriptor.NodeIncarnation, Endpoint: sourceDescriptor.Endpoint,
			NativeEndpoint: sourceDescriptor.NativeEndpoint, ControlEndpoint: sourceDescriptor.ControlEndpoint,
		},
		SnapshotSourceMember:     sourceDescriptor.Member,
		Target:                   ReplicaIdentity{Member: 4, Node: target.NodeID, StoreID: [16]byte{14}, NodeIncarnation: 1, Endpoint: "ep-b", NativeEndpoint: "ep-b-native", ControlEndpoint: "ep-b-control"},
		ExpectedRosterDigest:     replication.Digest(replicatedCatalogInitialRosterDigest(current, 0)),
		ExpectedDescriptorDigest: replication.Digest(replicatedCatalogInitialDescriptorDigest(current, 0)),
		ExpectedManifestDigest:   replication.Digest{0xc3},
		ExpectedCommand:          descriptor.Command,
		TargetNodeRevision:       active.Revision,
		State:                    EnrollmentReserved,
		Revision:                 1,
	}
	if !intent.Valid() {
		t.Fatal("catalog-backed enrollment fixture is invalid")
	}
	if err := authority.SubmitEnrollmentIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	reserved, err := authority.ReadEnrollmentIntent(ctx, intent.IntentID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := authority.ClaimEnrollmentPreparation(ctx, reserved.IntentID, reserved.Revision)
	if err != nil {
		t.Fatal(err)
	}
	prepared := claimed
	prepared.State = EnrollmentPrepared
	prepared.Revision++
	prepared.PreparationClaim = [32]byte{}
	proof := scalingTestPreparedProof(prepared, active.Revision)
	prepared.Proof = &proof
	if err := authority.PutEnrollmentIntent(ctx, prepared, claimed.Revision); err != nil {
		t.Fatal(err)
	}
	enrolled, err := authority.PublishEnrollmentReceipt(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.State != EnrollmentEnrolled || enrolled.Receipt == nil || !enrolled.Receipt.Valid() {
		t.Fatalf("published enrollment=%+v", enrolled)
	}
	if enrolled.Receipt.BaseCatalogHeadDigest != intent.ExpectedCatalogHeadDigest ||
		enrolled.Receipt.PublicationPredecessorGeneration != current.Generation() ||
		enrolled.Receipt.EnrolledCatalogGeneration != current.Generation()+1 {
		t.Fatalf("publication witnesses=%+v", *enrolled.Receipt)
	}
	published, err := authority.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published.Generation() != current.Generation()+1 {
		t.Fatalf("published catalog generation=%d", published.Generation())
	}
	publishedDescriptor := published.ReplicatedShardDescriptors()[0]
	if publishedDescriptor.EnrolledTarget == nil ||
		publishedDescriptor.EnrolledTarget.Node != target.NodeID ||
		publishedDescriptor.EnrolledTarget.Member != enrolled.Target.Member {
		t.Fatalf("catalog enrolled target=%+v", publishedDescriptor.EnrolledTarget)
	}
	restarted := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(published), 0xc2)
	readBack, err := restarted.ReadEnrollmentIntent(ctx, intent.IntentID)
	if err != nil || readBack != enrolled {
		t.Fatalf("enrollment receipt did not survive authority reopen: %+v err=%v", readBack, err)
	}
	retry, err := authority.PublishEnrollmentReceipt(ctx, prepared)
	if err != nil || retry != enrolled {
		t.Fatalf("applied publication retry=%+v err=%v", retry, err)
	}
	evidence, err := restarted.ScanNodeReferences(ctx, target.NodeID, target.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.EnrolledTargets != 1 || evidence.LearnerReplicas != 1 {
		t.Fatalf("published target references=%+v", evidence)
	}
}

func TestReplicatedScalingRetirementPersistsEvidenceAndRevalidatesCut(t *testing.T) {
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x96)
	joining := scalingTestNodeRecord([16]byte{0x81}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := authority.PutNode(ctx, draining, 2); err != nil {
		t.Fatal(err)
	}

	evidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.ZeroAllReferences() {
		t.Fatalf("clean node has unexpected references: %+v", evidence)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, evidence); err != nil {
		t.Fatal(err)
	}
	retired, err := peer.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || retired.Lifecycle != NodeDecommissioned || retired.RetirementScanDigest != evidence.Digest {
		t.Fatalf("retired witness did not persist across reopen: %+v err=%v", retired, err)
	}

	// Re-run the path with a cut change injected after the fresh reference scan
	// and before the terminal CAS. A successful terminal write from this stale
	// witness would allow a controller to retire against an incomplete node
	// directory.
	joining = scalingTestNodeRecord([16]byte{0x82}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active = joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	draining = active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := authority.PutNode(ctx, draining, 2); err != nil {
		t.Fatal(err)
	}
	newNode := scalingTestNodeRecord([16]byte{0x83}, 1, NodeJoining, 1)
	key := scalingNodeKey(active.NodeID, active.Incarnation)
	var reads int
	var injectedErr error
	client.mu.Lock()
	client.onRead = func(readKey []byte) {
		if !bytes.Equal(readKey, key) {
			return
		}
		reads++
		if reads == 3 {
			injectedErr = peer.PutNode(ctx, newNode, 0)
		}
	}
	client.mu.Unlock()
	stale, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, stale); err == nil {
		t.Fatal("retirement CAS accepted a directory cut changed after the reference scan")
	}
	if injectedErr != nil {
		t.Fatalf("directory mutation injection=%v", injectedErr)
	}
	currentEvidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, currentEvidence); err != nil {
		t.Fatalf("fresh retirement after cut change=%v", err)
	}
}

type scalingTestGatewayScanner struct {
	mu        sync.Mutex
	evidence  GatewayParticipantEvidence
	callCount int
}

func (scanner *scalingTestGatewayScanner) ScanGatewayParticipant(_ context.Context, _ NodeRecord) (GatewayParticipantEvidence, error) {
	scanner.mu.Lock()
	defer scanner.mu.Unlock()
	scanner.callCount++
	return scanner.evidence, nil
}

func (scanner *scalingTestGatewayScanner) setActive(active bool) {
	scanner.mu.Lock()
	scanner.evidence.Active = active
	scanner.mu.Unlock()
}

func TestReplicatedScalingFrontendProofBlocksAndThenAllowsRetirement(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	scanner := &scalingTestGatewayScanner{}
	authority.gatewayParticipants = scanner

	joining := scalingTestNodeRecord([16]byte{0x91}, 1, NodeJoining, 1)
	joining.Roles = NodeRoleStorage | NodeRoleGateway
	joining.GatewayEndpoint = distribution.EndpointID("gateway-91")
	joining.GatewayAddress = "127.0.0.1:8301"
	joining.Gateway = GatewayIdentity{
		NodeID: joining.NodeID, Incarnation: joining.Incarnation,
		ServiceKeyDigest: joining.ServiceKeyDigest, ServiceID: [16]byte{0x92},
		SessionID: [16]byte{0x93}, SessionRevision: 1,
		ParticipantDigest: replication.Digest{0x94},
	}
	scanner.evidence = GatewayParticipantEvidence{
		NodeID: joining.NodeID, Incarnation: joining.Incarnation,
		ServiceKeyDigest: joining.ServiceKeyDigest, ServiceID: joining.Gateway.ServiceID,
		SessionID: joining.Gateway.SessionID, SessionRevision: joining.Gateway.SessionRevision,
		ParticipantDigest: joining.Gateway.ParticipantDigest,
		DirectoryRevision: 1, Active: true, Digest: replication.Digest{0x95},
	}
	if !joining.Valid() {
		t.Fatal("gateway retirement fixture is invalid")
	}
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := authority.PutNode(ctx, draining, 2); err != nil {
		t.Fatal(err)
	}
	if !scanner.evidence.ValidFor(draining) {
		t.Fatalf("frontend proof fixture does not bind to the persisted node: evidence=%+v node=%+v", scanner.evidence, draining)
	}

	blocked, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.GatewayParticipantRefs != 1 || blocked.GatewayDirectoryRevision != 1 || blocked.GatewayDirectoryDigest == (replication.Digest{}) {
		t.Fatalf("active frontend proof was not counted: %+v", blocked)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, blocked); !errors.Is(err, ErrScalingState) {
		t.Fatalf("retirement ignored active frontend participant: %v", err)
	}

	scanner.setActive(false)
	evidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.GatewayParticipantRefs != 0 || !evidence.ZeroAllReferences() {
		t.Fatalf("inactive frontend proof did not clear references: %+v", evidence)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, evidence); err != nil {
		t.Fatal(err)
	}
	retired, err := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x98).ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || retired.Lifecycle != NodeDecommissioned {
		t.Fatalf("frontend-safe retirement did not persist: %+v err=%v", retired, err)
	}
	scanner.mu.Lock()
	calls := scanner.callCount
	scanner.mu.Unlock()
	if calls < 3 {
		t.Fatalf("retirement did not revalidate the frontend proof during CAS: calls=%d", calls)
	}
}

func TestReplicatedScalingOperationDirectoryBound(t *testing.T) {
	ctx := context.Background()
	authority, _, _ := newCatalogAuthorityFixture(t)
	records := make([]ReplicatedOperationRecord, maxReplicatedOperations+1)
	for index := range records {
		records[index] = testReplicatedOperation(ReplicatedOperationRecord{
			ID: [32]byte{byte(index + 1)}, Kind: ReplicatedOperationMove,
			State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
			Proof: [32]byte{byte(index + 2)},
		})
	}
	if err := authority.SubmitOperations(ctx, records); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("operation history bound error=%v", err)
	}
	ids, err := authority.ReadOperationIDs(ctx)
	if err != nil || len(ids) != 0 {
		t.Fatalf("over-bound operation submission left rows ids=%v err=%v", ids, err)
	}
}

func TestReplicatedScalingCompletedMoveRetentionAndSafeRetire(t *testing.T) {
	ctx := context.Background()
	authority, peerClient, current := newCatalogAuthorityFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x97)
	joining := scalingTestNodeRecord([16]byte{0xa1}, 1, NodeJoining, 1)
	if err := authority.PutNode(ctx, joining, 0); err != nil {
		t.Fatal(err)
	}
	active := joining
	active.Lifecycle = NodeActive
	active.Revision = 2
	if err := authority.PutNode(ctx, active, 1); err != nil {
		t.Fatal(err)
	}
	draining := active
	draining.Lifecycle = NodeDraining
	draining.Revision = 3
	if err := authority.PutNode(ctx, draining, 2); err != nil {
		t.Fatal(err)
	}

	operation := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0xa1, 0x01}, Kind: ReplicatedOperationMove,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
		Proof: [32]byte{0xa2},
	})
	if err := authority.SubmitOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	blocked, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.OutstandingMoves != 1 {
		t.Fatalf("planned move was not retained as a retirement reference: %+v", blocked)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, blocked); !errors.Is(err, ErrScalingState) {
		t.Fatalf("retirement ignored a live move operation: %v", err)
	}

	complete := operation
	complete.State = ReplicatedOperationComplete
	complete.Revision = 2
	if err := authority.PublishOperation(ctx, operation.Revision, complete); err != nil {
		t.Fatal(err)
	}
	retained, err := peer.ReadOperation(ctx, operation.ID)
	if err != nil || !retained.Equal(complete) {
		t.Fatalf("completed operation was not retained for restart-safe observation: %+v err=%v", retained, err)
	}
	settled, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if settled.OutstandingMoves != 0 {
		t.Fatalf("completed move still blocked safe-to-stop: %+v", settled)
	}
	if err := authority.DeleteOperation(ctx, operation.ID, operation.Revision); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale completed-operation GC was accepted: %v", err)
	}
	peerClient.mu.Lock()
	peerClient.unknownNext = true
	peerClient.mu.Unlock()
	if err := authority.DeleteOperation(ctx, operation.ID, complete.Revision); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("lost completed-operation GC response=%v", err)
	}
	peerClient.mu.Lock()
	peerClient.holdUnknown = false
	peerClient.mu.Unlock()
	if err := authority.RetryPending(ctx); err != nil {
		t.Fatalf("settling lost completed-operation GC response=%v", err)
	}
	if _, err := peer.ReadOperation(ctx, operation.ID); !errors.Is(err, ErrReplicatedOperationMissing) {
		t.Fatalf("completed operation survived settled GC=%v", err)
	}
	if err := authority.DeleteOperation(ctx, operation.ID, complete.Revision); err != nil {
		t.Fatalf("exact delete retry after applied response loss=%v", err)
	}

	// A second completed move can use the bounded directory after the first
	// terminal history was explicitly reaped. This exercises the repeated
	// scale request path without relying on an in-memory operation queue.
	second := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0xa1, 0x02}, Kind: ReplicatedOperationMove,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
		Proof: [32]byte{0xa3},
	})
	if err := authority.SubmitOperation(ctx, second); err != nil {
		t.Fatal(err)
	}
	secondComplete := second
	secondComplete.State = ReplicatedOperationComplete
	secondComplete.Revision = 2
	if err := authority.PublishOperation(ctx, second.Revision, secondComplete); err != nil {
		t.Fatal(err)
	}
	if err := authority.DeleteOperation(ctx, second.ID, secondComplete.Revision); err != nil {
		t.Fatal(err)
	}
	ids, err := peer.ReadOperationIDs(ctx)
	if err != nil || len(ids) != 0 {
		t.Fatalf("completed operation history was not reaped: ids=%x err=%v", ids, err)
	}

	evidence, err := authority.ScanNodeReferences(ctx, active.NodeID, active.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.ZeroAllReferences() {
		t.Fatalf("reaped completed moves left a safe-to-stop reference: %+v", evidence)
	}
	if err := authority.RetireNode(ctx, active.NodeID, active.Incarnation, draining.Revision, evidence); err != nil {
		t.Fatal(err)
	}
	retired, err := peer.ReadNode(ctx, active.NodeID, active.Incarnation)
	if err != nil || retired.Lifecycle != NodeDecommissioned {
		t.Fatalf("retirement after completed-history GC did not persist: %+v err=%v", retired, err)
	}
	peerClient.mu.Lock()
	peerApplied := peerClient.applied
	peerClient.mu.Unlock()
	if peerApplied == 0 {
		t.Fatal("shared replicated fixture did not apply completed-history mutations")
	}
}
