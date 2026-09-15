package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

// TestPublishEnrollmentReceiptUsesAnExactPreparedRowAndAllowsUnrelatedHead
// advances exercises the durable boundary that turns a pre-membership target
// into an authenticated catalog participant.  In particular, a receipt may
// be retried after a later unrelated catalog generation, but a fabricated
// Prepared -> Enrolled row cannot bypass the publisher.
func TestPublishEnrollmentReceiptUsesAnExactPreparedRowAndAllowsUnrelatedHead(t *testing.T) {
	ctx := context.Background()
	authority, _, current := newCatalogAuthorityFixture(t)
	descriptors := current.ReplicatedShardDescriptors()
	if len(descriptors) != 1 || len(descriptors[0].Replicas) != ServingReplicaCount {
		t.Fatalf("catalog fixture descriptors=%+v", descriptors)
	}
	descriptor := descriptors[0]
	target := ReplicaIdentity{
		Member: 4, Node: [16]byte{4}, StoreID: [16]byte{14}, NodeIncarnation: 24,
		Endpoint: "ep-b", NativeEndpoint: "ep-b-native", ControlEndpoint: "ep-b-control",
	}
	targetNode := scalingTestNodeRecord(target.Node, target.NodeIncarnation, NodeJoining, 1)
	targetNode.DataEndpoint, targetNode.NativeEndpoint, targetNode.ControlEndpoint = target.Endpoint, target.NativeEndpoint, target.ControlEndpoint
	targetNode.DataAddress, targetNode.NativeAddress, targetNode.ControlAddress = current.endpoints[target.Endpoint], current.endpoints[target.NativeEndpoint], current.endpoints[target.ControlEndpoint]
	if err := authority.PutNode(ctx, targetNode, 0); err != nil {
		t.Fatal(err)
	}
	targetNode.Lifecycle = NodeActive
	targetNode.Revision = 2
	if err := authority.PutNode(ctx, targetNode, 1); err != nil {
		t.Fatal(err)
	}
	parent := scalingTestRunningParent(t, authority, targetNode)

	headResult, err := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if err != nil || !headResult.Found {
		t.Fatalf("read catalog head: %v", err)
	}
	intent := GroupEnrollmentIntent{
		IntentID: [32]byte{0xc1, 0x01}, Group: descriptor.Group,
		ParentScalingIntentID:  parent.ID,
		ReservedMigrationBytes: 7,
		Distribution:           descriptor.Distribution, Shard: descriptor.Shard,
		AllocationGeneration: descriptor.AllocationGeneration, CatalogGeneration: current.Generation(),
		ExpectedCatalogHeadDigest: scalingDigest(headResult.Value), ReplicaOrdinal: 0,
		Source: ReplicaIdentity{Member: descriptor.Replicas[0].Member, Node: descriptor.Replicas[0].Node,
			NodeIncarnation: descriptor.Replicas[0].NodeIncarnation, StoreID: descriptor.Replicas[0].StoreID,
			Endpoint: descriptor.Replicas[0].Endpoint, NativeEndpoint: descriptor.Replicas[0].NativeEndpoint,
			ControlEndpoint: descriptor.Replicas[0].ControlEndpoint},
		SnapshotSourceMember: descriptor.Replicas[0].Member, Target: target,
		ExpectedRosterDigest:     replication.Digest(replicatedCatalogInitialRosterDigest(current, 0)),
		ExpectedDescriptorDigest: replication.Digest(replicatedCatalogInitialDescriptorDigest(current, 0)),
		ExpectedManifestDigest:   replication.Digest(descriptor.Command.RelationManifestDigest),
		ExpectedCommand:          descriptor.Command, TargetNodeRevision: targetNode.Revision,
		State: EnrollmentReserved, Revision: 1,
	}
	if !intent.Valid() {
		t.Fatal("publisher fixture intent is invalid")
	}
	if err := authority.SubmitEnrollmentIntent(ctx, intent); err != nil {
		t.Fatalf("reserve enrollment: %v", err)
	}
	parent, err = authority.ReadScalingIntent(ctx, parent.ID)
	if err != nil || parent.PlannedReplicas != 1 || parent.CompletedReplicas != 0 {
		t.Fatalf("parent after admission=%+v err=%v", parent, err)
	}
	reserved, err := authority.ClaimEnrollmentPreparation(ctx, intent.IntentID, intent.Revision)
	if err != nil {
		t.Fatalf("claim preparation: %v", err)
	}
	proof := scalingTestPreparedProof(reserved, targetNode.Revision)
	prepared := reserved
	prepared.State = EnrollmentPrepared
	prepared.Revision++
	prepared.PreparationClaim = [32]byte{}
	prepared.Proof = &proof
	if err := authority.PutEnrollmentIntent(ctx, prepared, reserved.Revision); err != nil {
		t.Fatalf("persist prepared proof: %v", err)
	}

	// A generic metadata write cannot manufacture the receipt edge, even when
	// supplied with a shape-valid row.
	fabricated := prepared
	fabricated.State = EnrollmentEnrolled
	fabricated.Revision++
	fabricated.Receipt = &CertifiedEnrollmentReceipt{
		IntentID: prepared.IntentID, IntentDigest: prepared.Digest(),
		BaseCatalogGeneration:            prepared.CatalogGeneration,
		BaseCatalogHeadDigest:            prepared.ExpectedCatalogHeadDigest,
		BaseDescriptorDigest:             prepared.ExpectedDescriptorDigest,
		PublicationPredecessorGeneration: current.Generation(),
		PublicationPredecessorHeadDigest: prepared.ExpectedCatalogHeadDigest,
		EnrolledCatalogGeneration:        current.Generation() + 1,
		EnrolledCatalogHeadDigest:        replication.Digest{0xe1},
		EnrolledDescriptorDigest:         replication.Digest{0xe2}, Target: prepared.Target,
		InitialReplicaSetVersion: prepared.ExpectedCommand.ReplicaSetVersion,
		GrantDigest:              replication.Digest{0xe3}, TransitionID: EnrollmentTransitionDigest(prepared),
	}
	if err := authority.PutEnrollmentIntent(ctx, fabricated, prepared.Revision); !errors.Is(err, ErrScalingState) {
		t.Fatalf("generic Prepared -> Enrolled transition=%v", err)
	}

	enrolled, err := authority.PublishEnrollmentReceipt(ctx, prepared)
	if err != nil {
		t.Fatalf("publish enrollment receipt: %v", err)
	}
	if enrolled.State != EnrollmentEnrolled || enrolled.Receipt == nil || !enrolled.Valid() {
		t.Fatalf("publisher returned invalid enrolled row: %+v", enrolled)
	}
	if enrolled.Receipt.PublicationPredecessorGeneration != current.Generation() ||
		enrolled.Receipt.EnrolledCatalogGeneration != current.Generation()+1 {
		t.Fatalf("receipt generations=%d -> %d, want %d -> %d",
			enrolled.Receipt.PublicationPredecessorGeneration, enrolled.Receipt.EnrolledCatalogGeneration,
			current.Generation(), current.Generation()+1)
	}

	// The same prepared request is a safe outcome-unknown retry after the row
	// and catalog command have already committed.
	retry, err := authority.PublishEnrollmentReceipt(ctx, prepared)
	if err != nil || retry.State != EnrollmentEnrolled || retry.Receipt == nil {
		t.Fatalf("receipt retry=%+v err=%v", retry, err)
	}

	// Advance an unrelated catalog generation without changing the enrolled
	// group. The durable receipt must remain retryable against that later head.
	latest := authority.holder.Current()
	if !EnrollmentReceiptMatchesSnapshot(enrolled, latest) || EnrollmentReceiptMatchesSnapshot(enrolled, current) {
		t.Fatal("receipt must match its published group and reject the pre-enrollment cut")
	}
	persisted := toPersisted(latest)
	persisted.Generation++
	if persisted.RequestLedger != nil {
		persisted.RequestLedger.Generation = persisted.Generation
	}
	raw, err := vibejson.Marshal(&persisted)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := decodeSnapshotBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.Publish(ctx, latest.Generation(), unrelated); err != nil {
		t.Fatalf("publish unrelated catalog generation: %v", err)
	}
	if _, err := authority.PublishEnrollmentReceipt(ctx, prepared); err != nil {
		t.Fatalf("retry receipt after unrelated catalog head=%v", err)
	}
	if !EnrollmentReceiptMatchesSnapshot(enrolled, unrelated) {
		t.Fatal("unrelated catalog generation invalidated an unchanged enrolled group")
	}
	substituted := enrolled
	substituted.Target.ControlEndpoint = "substituted-control"
	if EnrollmentReceiptMatchesSnapshot(substituted, unrelated) {
		t.Fatal("receipt accepted a substituted target identity")
	}
	changedGroup, err := decodeSnapshotBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	changedGroup.replicatedShards[0].command.ReplicaSetVersion++
	if EnrollmentReceiptMatchesSnapshot(enrolled, changedGroup) {
		t.Fatal("receipt accepted a changed group descriptor")
	}

	// The preparation claim is consumed by Reserved -> Prepared. Later
	// updates rely on the durable proof and receipt and must remain writable
	// after a restart, without resurrecting that Reserved-only claim.
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(authority.holder.Current()), 0xce)
	moving := enrolled
	moving.State = EnrollmentMoving
	moving.MoveOperationID = [32]byte{0xc2}
	moving.Revision++
	if err := peer.PutEnrollmentIntent(ctx, moving, enrolled.Revision); err != nil {
		t.Fatalf("persist moving enrollment after consumed preparation claim: %v", err)
	}
	parent, err = authority.ReadScalingIntent(ctx, parent.ID)
	if err != nil || len(parent.OutstandingMoves) != 1 || parent.OutstandingMoves[0] != moving.MoveOperationID {
		t.Fatalf("parent after move journal=%+v err=%v", parent, err)
	}
	complete := moving
	complete.State = EnrollmentComplete
	complete.Revision++
	if err := authority.PutEnrollmentIntent(ctx, complete, moving.Revision); err != nil {
		t.Fatalf("complete enrollment after consumed preparation claim: %v", err)
	}
	stored, err := peer.ReadEnrollmentIntent(ctx, complete.IntentID)
	if err != nil || stored.State != EnrollmentComplete || stored.PreparationClaim != ([32]byte{}) {
		t.Fatalf("completed enrollment=%+v err=%v", stored, err)
	}
	parent, err = peer.ReadScalingIntent(ctx, parent.ID)
	if err != nil || parent.PlannedReplicas != 1 || parent.CompletedReplicas != 1 || parent.AdmittedMigrationBytes != 7 || len(parent.OutstandingMoves) != 0 {
		t.Fatalf("parent after completion=%+v err=%v", parent, err)
	}
	if err := peer.PutEnrollmentIntent(ctx, complete, moving.Revision); err != nil {
		t.Fatalf("completion retry: %v", err)
	}
	retriedParent, err := authority.ReadScalingIntent(ctx, parent.ID)
	if err != nil || retriedParent.CompletedReplicas != 1 || retriedParent.Revision != parent.Revision {
		t.Fatalf("completion retry double-counted parent: %+v err=%v", retriedParent, err)
	}
}

func TestEnrollmentMoveMatchesSnapshotRequiresPostRemoveIdentity(t *testing.T) {
	authority, _, enrolled := enrollmentMoveFixture(t)
	moving := enrolled
	moving.State = EnrollmentMoving
	moving.Revision++
	moving.MoveOperationID = [32]byte{0xd3}
	if EnrollmentMoveMatchesSnapshot(moving, authority.holder.Current()) {
		t.Fatal("a catalog cut without a retained transition receipt must not prove a completed move")
	}

	transition, publication, final := publishEnrollmentMoveTransition(t, authority, moving)
	if !EnrollmentMoveMatchesSnapshotWithReceipt(moving, final, transition, publication) {
		t.Fatalf("exact post-remove target cut must prove a collected move: transition=%+v publication=%+v", transition.Key, publication)
	}
	later := enrollmentMoveNextHead(t, final)
	if !EnrollmentMoveMatchesSnapshotWithReceipt(moving, later, transition, publication) {
		t.Fatal("an unrelated later catalog head must preserve the exact transition proof")
	}

	descriptors := later.ReplicatedShardDescriptors()
	descriptors[0].Command.ReplicaSetVersion--
	regressed, err := enrollmentMoveSnapshotWithDescriptors(later, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	if EnrollmentMoveMatchesSnapshotWithReceipt(moving, regressed, transition, publication) {
		t.Fatal("pre-remove replica-set fence must not prove completion")
	}
	descriptors[0].Command.ReplicaSetVersion = later.ReplicatedShardDescriptors()[0].Command.ReplicaSetVersion + 1
	ahead, err := enrollmentMoveSnapshotWithDescriptors(later, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	if EnrollmentMoveMatchesSnapshotWithReceipt(moving, ahead, transition, publication) {
		t.Fatal("unrelated later replica-set fence must not prove completion")
	}
	wrong := publication
	wrong.Key.OperationID[0]++
	if EnrollmentMoveMatchesSnapshotWithReceipt(moving, later, transition, wrong) {
		t.Fatal("a receipt for a different move must not prove completion")
	}
}

func publishEnrollmentMoveTransition(
	t *testing.T, authority *ReplicatedCatalogAuthority, moving GroupEnrollmentIntent,
) (GroupTransitionIntent, GroupPublicationReceipt, *Snapshot) {
	t.Helper()
	current := authority.holder.Current()
	descriptors := current.ReplicatedShardDescriptors()
	if len(descriptors) != 1 {
		t.Fatalf("descriptors=%d, want one", len(descriptors))
	}
	source := descriptors[0]
	// The enrollment fixture intentionally uses a metadata-light descriptor.
	// Fill the transition-only provenance fields in the detached source copy;
	// the final catalog cut remains built from the authority's actual snapshot.
	source.LogicalSchemaDigest = [32]byte{0x31}
	source.RangeIdentity = [32]byte{0x32}
	source.LineageDigest = [32]byte{0x33}
	source.ForwardingRuleDigest = [32]byte{0x34}
	target := enrollmentTargetDescriptor(moving.Target)
	transition := testGroupTransitionIntent(t, current, source, target, moving.Source.Member)
	transition.Key.OperationID = moving.MoveOperationID
	if !transition.Valid() {
		t.Fatalf("move transition fixture is invalid: key=%+v source=%+v replacement=%+v", transition.Key, transition.SourceDescriptor, transition.Replacement)
	}
	final := enrollmentMoveFinalSnapshot(t, current, moving)
	descriptor := final.ReplicatedShardDescriptors()[0]
	headDigest, err := CatalogSnapshotDigest(final)
	if err != nil {
		t.Fatal(err)
	}
	manifest, ok := final.Manifest(moving.Distribution)
	if !ok {
		t.Fatal("final manifest missing")
	}
	publication := GroupPublicationReceipt{
		Key: transition.Key, Phase: TransitionPhasePostRemove,
		PredecessorReceiptDigest:     [32]byte{0x35},
		PredecessorHeadGeneration:    current.Generation() + 1,
		PredecessorHeadDigest:        [32]byte{0x36},
		PredecessorGroupGeneration:   current.Generation() + 1,
		PredecessorGroupHeadDigest:   [32]byte{0x37},
		PredecessorGroupDigest:       [32]byte{0x38},
		PredecessorRosterDigest:      [32]byte{0x39},
		PredecessorRouteDigest:       [32]byte{0x3a},
		CommittedHeadGeneration:      final.Generation(),
		CommittedHeadDigest:          headDigest,
		CommittedGroupGeneration:     final.Generation(),
		CommittedGroupDigest:         DigestReplicatedShardDescriptor(descriptor),
		CommittedRosterDigest:        DigestReplicaRoster(descriptor.Replicas),
		CommittedRouteDigest:         DigestRouteFor(final, moving.Distribution, moving.Shard),
		CommittedCommandFenceDigest:  DigestCommandFence(descriptor.Command),
		CommittedDistributionVersion: manifest.Version(),
		SourceRouteDigest:            transition.SourceRouteDigest,
		SourceRosterDigest:           transition.SourceRosterDigest,
	}
	if !publication.Valid() {
		t.Fatal("move publication fixture is invalid")
	}
	return transition, publication, final
}

func enrollmentMoveFinalSnapshot(t *testing.T, current *Snapshot, intent GroupEnrollmentIntent) *Snapshot {
	t.Helper()
	descriptors := current.ReplicatedShardDescriptors()
	if len(descriptors) != 1 {
		t.Fatalf("descriptors=%d, want one", len(descriptors))
	}
	descriptor := &descriptors[0]
	target := ReplicatedReplicaDescriptor{Member: intent.Target.Member, Node: intent.Target.Node,
		StoreID: intent.Target.StoreID, NodeIncarnation: intent.Target.NodeIncarnation,
		Endpoint: intent.Target.Endpoint, NativeEndpoint: intent.Target.NativeEndpoint,
		ControlEndpoint: intent.Target.ControlEndpoint}
	found := false
	for index := range descriptor.Replicas {
		if descriptor.Replicas[index].Member == intent.Source.Member {
			descriptor.Replicas[index] = target
			found = true
			break
		}
	}
	if !found {
		t.Fatal("source replica missing from enrolled cut")
	}
	descriptor.EnrolledTarget = nil
	descriptor.Command.ReplicaSetVersion = intent.ExpectedCommand.ReplicaSetVersion + 3
	descriptor.Command.OwnershipEpoch = intent.ExpectedCommand.OwnershipEpoch + 1
	descriptor.Command.RoutingVersion = intent.ExpectedCommand.RoutingVersion + 1
	descriptor.Command.RouteGeneration = intent.ExpectedCommand.RouteGeneration + 1
	manifest, ok := current.Manifest(intent.Distribution)
	if !ok {
		t.Fatal("distribution manifest missing")
	}
	ordinal, _ := manifestShardOrdinal(manifest, intent.Shard)
	if ordinal < 0 {
		t.Fatal("shard manifest missing")
	}
	replaced, err := manifest.ReplaceShardLeader(ordinal, distribution.RoutingVersion(intent.ExpectedCommand.RoutingVersion+1), 0,
		intent.Target.Endpoint, distribution.OwnershipEpoch(intent.ExpectedCommand.OwnershipEpoch+1))
	if err != nil {
		t.Fatal(err)
	}
	config := cloneConfig(current.config)
	for index := range config.Manifests {
		if config.Manifests[index].Distribution() == intent.Distribution {
			config.Manifests[index] = replaced
		}
	}
	final, err := NewSnapshotWithReplicatedTableMetadata(config, current.endpoints,
		current.Generation()+2, current.indexDescriptors(), current.statistics.Descriptors(), descriptors,
		current.replicatedTableProfiles(), current.ReplicatedTableDeclarations())
	if err != nil {
		t.Fatal(err)
	}
	return final
}

func enrollmentMoveSnapshotWithDescriptors(current *Snapshot, descriptors []ReplicatedShardDescriptor) (*Snapshot, error) {
	return NewSnapshotWithReplicatedTableMetadata(cloneConfig(current.config), current.endpoints, current.Generation(),
		current.indexDescriptors(), current.statistics.Descriptors(), descriptors, current.replicatedTableProfiles(),
		current.ReplicatedTableDeclarations())
}

func enrollmentMoveNextHead(t *testing.T, current *Snapshot) *Snapshot {
	t.Helper()
	final, err := NewSnapshotWithReplicatedTableMetadata(cloneConfig(current.config), current.endpoints,
		current.Generation()+1, current.indexDescriptors(), current.statistics.Descriptors(),
		current.ReplicatedShardDescriptors(), current.replicatedTableProfiles(), current.ReplicatedTableDeclarations())
	if err != nil {
		t.Fatal(err)
	}
	return final
}
