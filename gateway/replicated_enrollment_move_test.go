package gateway

import (
	"bytes"
	"context"
	"errors"
	"github.com/thesyncim/vibedb/internal/replication"
	"testing"
)

func enrollmentMoveFixture(t *testing.T) (*ReplicatedCatalogAuthority, *catalogAuthorityClient, GroupEnrollmentIntent) {
	t.Helper()
	ctx := context.Background()
	authority, client, current := newCatalogAuthorityFixture(t)
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

	enrolled, err := authority.PublishEnrollmentReceipt(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return authority, client, enrolled
}

func enrollmentMoveRecord(row GroupEnrollmentIntent) ReplicatedOperationRecord {
	return testReplicatedOperation(ReplicatedOperationRecord{ID: [32]byte{0xd1}, Kind: ReplicatedOperationMove,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: row.Receipt.EnrolledCatalogGeneration,
		Cursor: [8]uint64{1}, Proof: [32]byte{0xd2}})
}

func TestEnrollmentMoveAdmissionSurvivesLostReplyAndExecutorAdvance(t *testing.T) {
	ctx := t.Context()
	authority, client, enrolled := enrollmentMoveFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(authority.holder.Current()), 0xdd)
	move := enrollmentMoveRecord(enrolled)
	next := enrolled
	next.State = EnrollmentMoving
	next.Revision++
	next.MoveOperationID = move.ID
	client.unknownNext = true
	if err := authority.AdmitEnrollmentMove(ctx, next, enrolled.Revision, move, nil); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("lost reply: %v", err)
	}
	pending := authority.session.PendingCommand()
	stored, err := peer.ReadEnrollmentIntent(ctx, enrolled.IntentID)
	if err != nil || stored.State != EnrollmentMoving || stored.MoveOperationID != move.ID {
		t.Fatalf("child discovered without moving parent: %+v %v", stored, err)
	}
	actual, err := peer.ReadOperation(ctx, move.ID)
	if err != nil || !actual.Equal(move) {
		t.Fatalf("moving parent without child: %+v %v", actual, err)
	}
	actual.Revision++
	actual.State = ReplicatedOperationRunning
	if err := peer.PublishOperation(ctx, move.Revision, actual); err != nil {
		t.Fatal(err)
	}
	client.holdUnknown = false
	if err := authority.AdmitEnrollmentMove(ctx, next, enrolled.Revision, move, nil); err != nil {
		t.Fatalf("exact retry after executor advance: %v", err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("lost admission retry changed command bytes")
	}
	parent, err := peer.ReadScalingIntent(ctx, enrolled.ParentScalingIntentID)
	if err != nil || parent.PlannedReplicas != 1 || parent.CompletedReplicas != 0 || parent.AdmittedMigrationBytes != 7 || len(parent.OutstandingMoves) != 1 || parent.OutstandingMoves[0] != move.ID {
		t.Fatalf("parent progress: %+v %v", parent, err)
	}
	changed := move
	changed.Intent = append(bytes.Clone(move.Intent), ' ')
	if err := authority.AdmitEnrollmentMove(ctx, next, enrolled.Revision, changed, nil); err == nil {
		t.Fatal("changed move intent accepted on exact replay")
	}
}

func TestEnrollmentMoveAdmissionDirectoryRacePublishesNeitherChildNorHandoff(t *testing.T) {
	ctx := t.Context()
	authority, client, enrolled := enrollmentMoveFixture(t)
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(authority.holder.Current()), 0xde)
	move := enrollmentMoveRecord(enrolled)
	next := enrolled
	next.State = EnrollmentMoving
	next.Revision++
	next.MoveOperationID = move.ID
	concurrent := move
	concurrent.ID = [32]byte{0xee}
	var concurrentErr error
	client.onRead = func(key []byte) {
		if !bytes.Equal(key, enrollmentDirectoryKey) {
			return
		}
		client.mu.Lock()
		client.onRead = nil
		client.mu.Unlock()
		concurrentErr = peer.SubmitOperation(ctx, concurrent)
	}
	if err := authority.AdmitEnrollmentMove(ctx, next, enrolled.Revision, move, nil); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("overlap race: %v", err)
	}
	if concurrentErr != nil {
		t.Fatal(concurrentErr)
	}
	if _, err := peer.ReadOperation(ctx, move.ID); !errors.Is(err, ErrReplicatedOperationMissing) {
		t.Fatalf("partial child admission: %v", err)
	}
	stored, err := peer.ReadEnrollmentIntent(ctx, enrolled.IntentID)
	if err != nil || stored.State != EnrollmentEnrolled || stored.MoveOperationID != ([32]byte{}) {
		t.Fatalf("partial parent handoff: %+v %v", stored, err)
	}
	parent, err := peer.ReadScalingIntent(ctx, enrolled.ParentScalingIntentID)
	if err != nil || len(parent.OutstandingMoves) != 0 || parent.PlannedReplicas != 1 || parent.AdmittedMigrationBytes != 7 {
		t.Fatalf("partial budget progress: %+v %v", parent, err)
	}
}
