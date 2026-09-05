package gateway

import (
	"context"
	"errors"
	"testing"

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
	if err := authority.PutNode(ctx, targetNode, 0); err != nil {
		t.Fatal(err)
	}
	targetNode.Lifecycle = NodeActive
	targetNode.Revision = 2
	if err := authority.PutNode(ctx, targetNode, 1); err != nil {
		t.Fatal(err)
	}

	headResult, err := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if err != nil || !headResult.Found {
		t.Fatalf("read catalog head: %v", err)
	}
	intent := GroupEnrollmentIntent{
		IntentID: [32]byte{0xc1, 0x01}, Group: descriptor.Group,
		Distribution: descriptor.Distribution, Shard: descriptor.Shard,
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
}
