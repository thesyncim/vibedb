package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestReplicatedRestoreStageDiscardsAuthorityAndResumesBundle(t *testing.T) {
	source, sourceIdentity := openRestoreBundleRoot(t, "restore-source", 9, 1)
	defer source.Close()
	sourceBootstrap := snapshotStageBootstrap()
	apply, _, err := source.OpenReplicatedApply(sourceIdentity, sourceBootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer apply.Close()
	if _, err = apply.InstallSnapshot(sourceBootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, apply, sourceIdentity, 2)
	capture, err := apply.BeginRangeSplitCapture(replicatedApplyCapturePartitioner(t, sourceIdentity))
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"email":"restore@example","id":"restore-doc"}`)
	baseKey := testReplicatedApplyKey(t, source, document)
	globalKey, locator := testReplicatedGlobalIndexKey(t, sourceIdentity.Relations[1], "restore@example"), []byte(`["restore-doc"]`)
	commandValue := testReplicatedApplyCommandValue(sourceIdentity, epoch, 2, nil)
	commandValue.Fingerprint = sha256.Sum256([]byte("restore-bundle-source"))
	commandValue.Batches = []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: baseKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: locator,
		}}},
	}
	command, err := replication.AppendCommand(nil, commandValue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil || capture.Head() != 3 {
		t.Fatalf("source apply/capture head=%d err=%v", capture.Head(), err)
	}
	if got := completionResultCode(t, apply, command); got != replicatedstate.ResultApplied {
		t.Fatalf("restore source bundle result = %d", got)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	sourceManifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		&artifact, cut, replicatedstate.SnapshotArtifactOptions{},
	)
	closeErr := cut.Close()
	if writeErr != nil || closeErr != nil || sourceManifest.SystemRows <= 1 ||
		sourceManifest.CaptureRows == 0 || !sourceManifest.Bundle {
		t.Fatalf("source manifest=%+v write=%v close=%v", sourceManifest, writeErr, closeErr)
	}

	target, targetIdentity := openRestoreBundleRoot(t, "restore-target", 77, 2)
	defer target.Close()
	logical, err := ReplicatedRelationManifestDigest(targetIdentity)
	sourceMachine, sourceErr := target.ReplicatedRelationManifestForBinding(targetIdentity, testReplicatedApplyOptions().Placement, sourceIdentity.Binding)
	if err != nil || sourceErr != nil || logical == sourceManifest.RelationManifestDigest || sourceMachine != sourceManifest.RelationManifestDigest {
		t.Fatalf("source machine/schema domain mismatch logical=%x machine=%x want=%x err=%v/%v", logical, sourceMachine, sourceManifest.RelationManifestDigest, err, sourceErr)
	}
	freshBinding := targetIdentity.Binding
	freshBinding.Authority.RoutingVersion++
	freshBinding.Authority.RouteGeneration++
	freshMachine, freshErr := target.ReplicatedRelationManifestForBinding(targetIdentity, testReplicatedApplyOptions().Placement, freshBinding)
	if freshErr != nil || freshMachine == sourceMachine {
		t.Fatalf("fresh routing domain unchanged err=%v", freshErr)
	}
	wrongSource := sourceManifest.Clone()
	wrongSource.RelationManifestDigest[0] ^= 1
	if rejected, _, rejectErr := target.OpenReplicatedRestoreStage(targetIdentity, wrongSource, nil, testReplicatedApplyOptions()); !errors.Is(rejectErr, ErrReplicatedRestoreStageProof) || rejected != nil {
		t.Fatalf("accepted wrong authenticated source schema: stage=%v err=%v", rejected, rejectErr)
	}
	stage, _, err := target.OpenReplicatedRestoreStage(
		targetIdentity, sourceManifest, nil, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	prefix := artifact.Bytes()[:len(artifact.Bytes())-replicatedstate.SnapshotArtifactFooterBytes]
	if _, err = stage.Receive(bytes.NewReader(prefix), func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}); !errors.Is(err, replicatedstate.ErrSnapshotArtifact) || len(persisted) == 0 {
		t.Fatalf("truncated receive cursor=%d err=%v", len(persisted), err)
	}
	resumeOffset := stage.Offset()
	if resumeOffset != uint64(len(prefix)) {
		t.Fatalf("resume offset=%d want=%d", resumeOffset, len(prefix))
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	stage, _, err = target.OpenReplicatedRestoreStage(
		targetIdentity, sourceManifest, persisted, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	received, err := stage.Receive(
		bytes.NewReader(artifact.Bytes()[resumeOffset:]), func(raw []byte) error {
			persisted = append(persisted[:0], raw...)
			return nil
		},
	)
	if err != nil || received.Digest != sourceManifest.Digest {
		t.Fatalf("resumed receive digest=%x err=%v", received.Digest, err)
	}

	core := target.connector.db
	core.mu.RLock()
	if core.replicatedApplyCollection.Len() != 0 || core.replicatedCaptureCollection.Len() != 0 {
		core.mu.RUnlock()
		t.Fatal("restore copied source system or capture authority")
	}
	baseCollection := core.tables["docs"].collection
	globalCollection := core.tables["email_claims"].collection
	core.mu.RUnlock()
	if value, found, readErr := baseCollection.AppendRaw(nil, baseKey); readErr != nil ||
		!found || !bytes.Equal(value, document) {
		t.Fatalf("base image value=%q found=%v err=%v", value, found, readErr)
	}
	if value, found, readErr := globalCollection.AppendRaw(nil, globalKey); readErr != nil ||
		!found || !bytes.Equal(value, locator) {
		t.Fatalf("global image value=%q found=%v err=%v", value, found, readErr)
	}

	targetBootstrap := restoreBootstrap(77)
	entryDigest := sha256.Sum256([]byte("fresh-restore-cut"))
	activation, err := stage.Activate(
		targetBootstrap,
		replicatedstate.StagedSnapshotCut{Applied: 2, Term: 1, EntryDigest: entryDigest},
		replicatedstate.SnapshotArtifactOptions{},
	)
	if err != nil || activation.Apply == nil || activation.SnapshotBase == nil {
		t.Fatalf("activation=%+v err=%v", activation, err)
	}
	manifest := activation.ArtifactManifest
	wantBinding := replicatedStateBindingAt(targetIdentity, testReplicatedApplyOptions().Placement.Range)
	if manifest.State.Binding != wantBinding || manifest.State.Binding == sourceManifest.State.Binding ||
		manifest.State.Applied != 2 || manifest.State.LastEntryDigest != entryDigest ||
		manifest.State.SessionCount != 0 || manifest.State.SessionSlotCount != 0 ||
		manifest.CaptureRows != 0 || manifest.CaptureImageDigest == sourceManifest.CaptureImageDigest ||
		!manifest.Bundle || len(manifest.Relations) != 2 || manifest.Relations[0].Rows != 1 ||
		manifest.Relations[1].Rows != 1 || !equalConfState(manifest.State.ConfState, targetBootstrap.Metadata.ConfState) {
		t.Fatalf("fresh manifest=%+v source=%+v", manifest, sourceManifest)
	}
	if _, err := target.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("restore activated serving before runtime adoption: %v", err)
	}
	if err := activation.Apply.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedRestoreStageRejectsCorruptArtifact(t *testing.T) {
	stage, target, _, artifact, _ := newReplicatedSnapshotStageFixture(t)
	source := stage.expected.Clone()
	_ = stage.Close()
	_ = target.Close()

	target, targetIdentity := openRestoreSingletonRoot(t, "restore-corrupt", 81)
	defer target.Close()
	restore, _, err := target.OpenReplicatedRestoreStage(
		targetIdentity, source, nil, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer restore.Close()
	corrupt := bytes.Clone(artifact)
	corrupt[len(corrupt)/2] ^= 1
	if _, err := restore.Receive(bytes.NewReader(corrupt), func([]byte) error { return nil }); !errors.Is(err, replicatedstate.ErrSnapshotArtifact) {
		t.Fatalf("corrupt artifact error=%v", err)
	}
}

func openRestoreBundleRoot(
	t *testing.T, name string, member uint64, groupByte byte,
) (*Database, ReplicatedShardStoreIdentity) {
	t.Helper()
	_, database, binding, _ := prepareReplicatedTestRoot(t, name, false)
	binding.MemberID = member
	binding.StoreID[0] = byte(member)
	binding.GroupID[0] = groupByte
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = testRuntimeExec(session, `CREATE TABLE email_claims (PRIMARY KEY (key))`, nil); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := database.BindReplicatedShardStoreBundle(
		binding, "docs", []ReplicatedGlobalIndexRelation{{
			Relation: 2, Table: "email_claims", IndexID: 41,
			Incarnation: 7, LocatorCount: 1, Unique: true,
			KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits,
		}},
	)
	rejectReplicatedStrictAllocationUnsupported(t, database, identity, err)
	if err != nil {
		t.Fatal(err)
	}
	return database, identity
}

func openRestoreSingletonRoot(t *testing.T, name string, member uint64) (*Database, ReplicatedShardStoreIdentity) {
	t.Helper()
	_, database, binding, _ := prepareReplicatedTestRoot(t, name, false)
	binding.MemberID = member
	binding.StoreID[0] = byte(member)
	binding.GroupID[0]++
	return database, requireReplicatedShardStoreBind(t, database, binding, "docs")
}

func restoreBootstrap(member uint64) *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{Data: []byte("restore-bootstrap"), Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{member}},
	}}
}

func equalConfState(left, right *pb.ConfState) bool {
	if left == nil || right == nil {
		return left == right
	}
	return proto.Equal(left, right)
}
