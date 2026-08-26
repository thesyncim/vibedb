package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func snapshotStageBootstrap() *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{Data: []byte("snapshot-stage-bootstrap"), Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term,
		ConfState: &pb.ConfState{Voters: []uint64{9}, Learners: []uint64{10}},
	}}
}

func TestReplicatedSnapshotStageInstallsExactBaseAndRetriesActivation(t *testing.T) {
	stage, target, bootstrap, _, _ := newReplicatedSnapshotStageFixture(t)
	defer target.Close()
	defer stage.Close()
	activation, err := stage.Activate(bootstrap)
	if err != nil || activation.Apply == nil || activation.SnapshotBase == nil {
		t.Fatalf("activation=%+v err=%v", activation, err)
	}
	core := target.connector.db
	core.mu.RLock()
	members := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: core.replicatedApplyCollection},
		{Name: "docs", Collection: core.tables["docs"].collection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: core.replicatedCaptureCollection},
	}
	group := core.checkpointGroup
	core.mu.RUnlock()
	if group == nil || !group.Owns(members) {
		t.Fatal("snapshot activation omitted fixed capture membership")
	}
	if activation.ArtifactManifest.CaptureRows != 2 ||
		activation.ArtifactManifest.CaptureImageDigest == ([32]byte{}) {
		t.Fatalf("capture artifact witness = rows %d digest %x",
			activation.ArtifactManifest.CaptureRows,
			activation.ArtifactManifest.CaptureImageDigest)
	}
	recoveredCapture, err := activation.Apply.BeginRangeSplitCapture(
		replicatedApplyCapturePartitioner(t, stage.base),
	)
	if err != nil || recoveredCapture.Head() != 3 {
		t.Fatalf("installed capture head=%d err=%v", recoveredCapture.Head(), err)
	}
	retried, err := stage.Activate(bootstrap)
	if err != nil || retried.Apply != activation.Apply ||
		retried.ArtifactManifest.Digest != activation.ArtifactManifest.Digest {
		t.Fatalf("activation retry=%+v err=%v", retried, err)
	}
	if _, err := target.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("serving before runtime adoption err=%v", err)
	}
}

func TestReplicatedSnapshotStageInstallsCompleteRelationBundle(t *testing.T) {
	openBundleRoot := func(name string, member uint64) (*Database, ReplicatedShardStoreIdentity) {
		_, database, binding, _ := prepareReplicatedTestRoot(t, name, false)
		binding.MemberID = member
		binding.StoreID[0] = byte(member)
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
			}},
		)
		skipReplicatedStrictAllocationUnsupported(t, database, identity, err)
		if err != nil {
			t.Fatal(err)
		}
		return database, identity
	}

	source, sourceIdentity := openBundleRoot("snapshot-bundle-source", 9)
	defer source.Close()
	bootstrap := snapshotStageBootstrap()
	apply, _, err := source.OpenReplicatedApply(
		sourceIdentity, bootstrap, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer apply.Close()
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, apply, sourceIdentity, 2)
	document := []byte(`{"email":"a","id":"bundle-doc"}`)
	baseKey := testReplicatedApplyKey(t, source, document)
	globalKey, locator := []byte{0x91, 0x01, 'a'}, []byte(`["bundle-doc"]`)
	commandValue := testReplicatedApplyCommandValue(sourceIdentity, epoch, 2, nil)
	commandValue.Fingerprint = sha256.Sum256([]byte("snapshot-stage-bundle"))
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
	if _, err = apply.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		&artifact, cut, replicatedstate.SnapshotArtifactOptions{},
	)
	closeErr := cut.Close()
	if writeErr != nil || closeErr != nil || !manifest.Bundle ||
		len(manifest.Relations) != 2 || manifest.Relations[0].Rows != 1 ||
		manifest.Relations[1].Rows != 1 {
		t.Fatalf("source bundle manifest=%+v write=%v close=%v", manifest, writeErr, closeErr)
	}

	target, targetIdentity := openBundleRoot("snapshot-bundle-target", 10)
	defer target.Close()
	stage, _, err := target.OpenReplicatedSnapshotStage(
		targetIdentity, manifest, nil, testReplicatedApplyOptions(),
		replicatedstate.SnapshotArtifactStageOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	var cursor []byte
	received, err := stage.Receive(bytes.NewReader(artifact.Bytes()), func(raw []byte) error {
		cursor = append(cursor[:0], raw...)
		return nil
	})
	if err != nil || received.Digest != manifest.Digest || len(cursor) == 0 {
		t.Fatalf("receive digest=%x cursor=%d err=%v", received.Digest, len(cursor), err)
	}
	activation, err := stage.Activate(bootstrap)
	if err != nil || activation.Apply == nil || !activation.ArtifactManifest.Bundle ||
		len(activation.ArtifactManifest.Relations) != 2 {
		t.Fatalf("activation=%+v err=%v", activation, err)
	}
	activation.ArtifactManifest.Relations[1].Collection[0] ^= 0x20
	retried, err := stage.Activate(bootstrap)
	if err != nil || retried.Apply != activation.Apply ||
		!bytes.Equal(retried.ArtifactManifest.Relations[1].Collection, []byte("email_claims")) {
		t.Fatalf("detached activation retry=%+v err=%v", retried, err)
	}
	activation = retried
	core := target.connector.db
	core.mu.RLock()
	baseCollection := core.tables["docs"].collection
	globalCollection := core.tables["email_claims"].collection
	members, membersErr := replicatedApplyCheckpointMembers(targetIdentity, core)
	group := core.checkpointGroup
	core.mu.RUnlock()
	if membersErr != nil || group == nil || !group.Owns(members) {
		t.Fatalf("bundle checkpoint ownership members=%v group=%v", membersErr, group)
	}
	if value, found, readErr := baseCollection.AppendRaw(nil, baseKey); readErr != nil ||
		!found || !bytes.Equal(value, document) {
		t.Fatalf("activated base value=%q found=%v err=%v", value, found, readErr)
	}
	if value, found, readErr := globalCollection.AppendRaw(nil, globalKey); readErr != nil ||
		!found || !bytes.Equal(value, locator) {
		t.Fatalf("activated global value=%q found=%v err=%v", value, found, readErr)
	}
	if _, err = activation.Apply.InstallSnapshot(activation.SnapshotBase); err != nil {
		t.Fatal(err)
	}
	activatedCut, err := activation.Apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var activatedArtifact bytes.Buffer
	activatedManifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		&activatedArtifact, activatedCut, replicatedstate.SnapshotArtifactOptions{},
	)
	closeErr = activatedCut.Close()
	certificate, certificateErr := replicatedstate.OpenSnapshotBase(activation.SnapshotBase)
	activatedState := activatedManifest.State
	activatedState.SnapshotBaseDigest = manifest.State.SnapshotBaseDigest
	activatedEnvelope, activatedEnvelopeErr := replicatedstate.AppendState(nil, activatedState)
	sourceEnvelope, sourceEnvelopeErr := replicatedstate.AppendState(nil, manifest.State)
	if writeErr != nil || closeErr != nil || certificateErr != nil ||
		activatedManifest.State.SnapshotBaseDigest != certificate.Digest ||
		activatedEnvelopeErr != nil || sourceEnvelopeErr != nil ||
		!bytes.Equal(activatedEnvelope, sourceEnvelope) ||
		!equalReplicatedSnapshotImage(activatedManifest, manifest) ||
		!activatedManifest.Bundle || len(activatedManifest.Relations) != 2 {
		t.Fatalf("activated bundle re-export=%+v write=%v close=%v",
			activatedManifest, writeErr, closeErr)
	}
	if err = activation.Apply.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotArtifactCaptureCommitmentPrecedesBorrowedCallback(t *testing.T) {
	stage, target, _, artifact, _ := newReplicatedSnapshotStageFixture(t)
	defer target.Close()
	defer stage.Close()
	mutated := false
	manifest, err := replicatedstate.VerifySnapshotArtifact(bytes.NewReader(artifact),
		replicatedstate.SnapshotArtifactCallbacks{Row: func(
			collection replicatedstate.SnapshotArtifactCollection, _, value []byte,
		) error {
			if collection == replicatedstate.SnapshotArtifactCapture && len(value) != 0 {
				value[0] ^= 1
				mutated = true
			}
			return nil
		}})
	if err != nil || !mutated || manifest.Digest != stage.expected.Digest {
		t.Fatalf("mutating callback verified=%v digest=%x err=%v", mutated, manifest.Digest, err)
	}
}

func TestSnapshotArtifactResumeRejectsForgedCaptureFooter(t *testing.T) {
	stage, target, _, artifact, _ := newReplicatedSnapshotStageFixture(t)
	defer target.Close()
	defer stage.Close()
	var rawCursor []byte
	_, err := replicatedstate.VerifySnapshotArtifact(bytes.NewReader(artifact),
		replicatedstate.SnapshotArtifactCallbacks{Chunk: func(
			_ replicatedstate.SnapshotArtifactCheckpoint,
			next *replicatedstate.SnapshotArtifactCursor,
		) error {
			var appendErr error
			rawCursor, appendErr = replicatedstate.AppendSnapshotArtifactCursor(rawCursor[:0], next)
			return appendErr
		}})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := replicatedstate.OpenSnapshotArtifactCursor(rawCursor)
	if err != nil || cursor.Offset()+replicatedstate.SnapshotArtifactFooterBytes != uint64(len(artifact)) {
		t.Fatalf("final cursor offset=%d artifact=%d err=%v", cursor.Offset(), len(artifact), err)
	}
	forged := bytes.Clone(artifact[cursor.Offset():])
	forged[168] ^= 1
	// The footer's outer digest cannot make a forged semantic capture
	// commitment agree with the resumable row hash chain in cursor.
	digest := sha256.Sum256(append(
		[]byte("vibedb/replicated-state/snapshot-artifact-footer\x00"), forged[:208]...,
	))
	copy(forged[208:240], digest[:])
	if _, _, err = replicatedstate.ContinueSnapshotArtifact(
		bytes.NewReader(forged), cursor, replicatedstate.SnapshotArtifactCallbacks{},
	); !errors.Is(err, replicatedstate.ErrSnapshotArtifact) {
		t.Fatalf("resumed forged capture footer error = %v", err)
	}
}

func TestSnapshotArtifactLiveCaptureRejectsCorruptionAndNoncanonicalFrames(t *testing.T) {
	stage, target, _, artifact, _ := newReplicatedSnapshotStageFixture(t)
	defer target.Close()
	defer stage.Close()
	captureHeader := -1
	for offset := 0; offset+96 <= len(artifact); offset++ {
		if bytes.Equal(artifact[offset:offset+8], []byte{'V', 'D', 'B', 'S', 'C', 'H', 'K', 0}) &&
			artifact[offset+24] == byte(replicatedstate.SnapshotArtifactCapture) {
			captureHeader = offset
			break
		}
	}
	if captureHeader < 0 {
		t.Fatal("live capture chunk absent")
	}
	corrupt := bytes.Clone(artifact)
	corrupt[captureHeader+96] ^= 1
	reordered := bytes.Clone(artifact)
	reordered[captureHeader+24] = byte(replicatedstate.SnapshotArtifactUser)
	duplicateFooter := append(bytes.Clone(artifact), artifact[len(artifact)-replicatedstate.SnapshotArtifactFooterBytes:]...)
	for name, raw := range map[string][]byte{
		"corrupt-capture":   corrupt,
		"truncated":         artifact[:len(artifact)-1],
		"trailing":          append(bytes.Clone(artifact), 0),
		"reordered-capture": reordered,
		"duplicate-footer":  duplicateFooter,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := replicatedstate.VerifySnapshotArtifact(
				bytes.NewReader(raw), replicatedstate.SnapshotArtifactCallbacks{},
			); !errors.Is(err, replicatedstate.ErrSnapshotArtifact) {
				t.Fatalf("noncanonical live capture error = %v", err)
			}
		})
	}
}

func newReplicatedSnapshotStageFixture(t *testing.T) (
	*ReplicatedSnapshotStage, *Database, *pb.Snapshot, []byte, []byte,
) {
	t.Helper()
	_, source, sourceBinding, _ := prepareReplicatedTestRoot(t, "snapshot-source", false)
	t.Cleanup(func() { _ = source.Close() })
	sourceIdentity := requireReplicatedShardStoreBind(t, source, sourceBinding, "docs")
	bootstrap := snapshotStageBootstrap()
	apply, _, err := source.OpenReplicatedApply(sourceIdentity, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = apply.Close() })
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, apply, sourceIdentity, 2)
	capture, err := apply.BeginRangeSplitCapture(replicatedApplyCapturePartitioner(t, sourceIdentity))
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"snapshot-capture","value":1}`)
	key := testReplicatedApplyKey(t, source, document)
	command := testReplicatedApplyCommand(sourceIdentity, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err = apply.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil || capture.Head() != 3 {
		t.Fatalf("captured snapshot source head=%d err=%v", capture.Head(), err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut,
		replicatedstate.SnapshotArtifactOptions{})
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}

	_, target, targetBinding, _ := prepareReplicatedTestRoot(t, "snapshot-target", false)
	targetBinding.MemberID = 10
	targetBinding.StoreID[0]++
	targetIdentity := requireReplicatedShardStoreBind(t, target, targetBinding, "docs")
	stage, _, err := target.OpenReplicatedSnapshotStage(targetIdentity, manifest, nil,
		testReplicatedApplyOptions(), replicatedstate.SnapshotArtifactStageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var cursor []byte
	if got, err := stage.Receive(bytes.NewReader(artifact.Bytes()), func(raw []byte) error {
		cursor = append(cursor[:0], raw...)
		return nil
	}); err != nil || got.Digest != manifest.Digest || len(cursor) == 0 {
		t.Fatalf("receive digest=%x cursor=%d err=%v", got.Digest, len(cursor), err)
	}
	return stage, target, bootstrap, bytes.Clone(artifact.Bytes()), bytes.Clone(cursor)
}

func TestReplicatedSnapshotStageSameHandleFaultSettlement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		point replicatedSnapshotStageFaultPoint
	}{
		{"group-create", replicatedSnapshotStageAfterGroupCreate},
		{"seed-checkpoint", replicatedSnapshotStageAfterSeed},
		{"machine-open", replicatedSnapshotStageAfterMachineOpen},
		{"snapshot-install", replicatedSnapshotStageAfterSnapshotInstall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stage, target, bootstrap, _, _ := newReplicatedSnapshotStageFixture(t)
			defer target.Close()
			defer stage.Close()
			fault := errors.New("snapshot activation boundary")
			fired := false
			previous := replicatedSnapshotStageFaultHook
			replicatedSnapshotStageFaultHook = func(point replicatedSnapshotStageFaultPoint) error {
				if !fired && point == tc.point {
					fired = true
					return fault
				}
				return nil
			}
			_, err := stage.Activate(bootstrap)
			replicatedSnapshotStageFaultHook = previous
			if !fired || !errors.Is(err, fault) {
				t.Fatalf("fault fired=%v err=%v", fired, err)
			}
			if _, err := target.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
				t.Fatalf("faulted activation served early: %v", err)
			}
			activation, err := stage.Activate(bootstrap)
			if err != nil || activation.Apply == nil || activation.SnapshotBase == nil {
				t.Fatalf("same-handle settlement activation=%+v err=%v", activation, err)
			}
		})
	}
}

func TestReplicatedSnapshotStageProcessReopenResumesEveryActivationBoundary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		point replicatedSnapshotStageFaultPoint
	}{
		{"group-create", replicatedSnapshotStageAfterGroupCreate},
		{"seed-checkpoint", replicatedSnapshotStageAfterSeed},
		{"machine-open", replicatedSnapshotStageAfterMachineOpen},
		{"snapshot-install", replicatedSnapshotStageAfterSnapshotInstall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stage, target, bootstrap, artifact, cursor := newReplicatedSnapshotStageFixture(t)
			path := target.connector.db.path
			base, identity, manifest := stage.base, stage.identity, stage.expected
			fault := errors.New("snapshot activation process boundary")
			fired := false
			previous := replicatedSnapshotStageFaultHook
			replicatedSnapshotStageFaultHook = func(point replicatedSnapshotStageFaultPoint) error {
				if !fired && point == tc.point {
					fired = true
					return fault
				}
				return nil
			}
			_, err := stage.Activate(bootstrap)
			replicatedSnapshotStageFaultHook = previous
			if !fired || !errors.Is(err, fault) {
				t.Fatalf("fault fired=%v err=%v", fired, err)
			}
			if err = stage.Close(); err != nil {
				t.Fatal(err)
			}
			if err = target.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			activation, resumed, err := reopened.ResumeReplicatedSnapshotActivation(
				base, manifest, bootstrap, testReplicatedApplyOptions(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if !resumed {
				receiver, _, openErr := reopened.OpenReplicatedSnapshotStage(
					base, manifest, cursor, testReplicatedApplyOptions(),
					replicatedstate.SnapshotArtifactStageOptions{},
				)
				if openErr != nil {
					t.Fatal(openErr)
				}
				offset := receiver.Offset()
				if offset > uint64(len(artifact)) {
					t.Fatalf("resume offset %d > artifact %d", offset, len(artifact))
				}
				if _, receiveErr := receiver.Receive(bytes.NewReader(artifact[offset:]), func([]byte) error { return nil }); receiveErr != nil {
					t.Fatal(receiveErr)
				}
				activation, err = receiver.Activate(bootstrap)
				if err != nil {
					t.Fatal(err)
				}
			}
			if activation.Apply == nil || activation.ArtifactManifest.CaptureRows != 2 {
				t.Fatalf("resumed=%v activation=%+v", resumed, activation)
			}
			capture, err := activation.Apply.BeginRangeSplitCapture(
				replicatedApplyCapturePartitioner(t, base),
			)
			if err != nil || capture.Head() != 3 {
				t.Fatalf("resumed capture head=%d err=%v", capture.Head(), err)
			}
		})
	}
}

func TestReplicatedSnapshotStageRejectsWrongBindingAndCorruption(t *testing.T) {
	_, source, binding, _ := prepareReplicatedTestRoot(t, "snapshot-reject-source", false)
	defer source.Close()
	identity := requireReplicatedShardStoreBind(t, source, binding, "docs")
	bootstrap := snapshotStageBootstrap()
	apply, _, err := source.OpenReplicatedApply(identity, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer apply.Close()
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	applyReplicatedApplySessionOpen(t, apply, identity, 2)
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut,
		replicatedstate.SnapshotArtifactOptions{})
	_ = cut.Close()
	if err != nil {
		t.Fatal(err)
	}
	wrong := manifest
	wrong.State.Binding.SchemaGeneration++
	if stage, _, err := source.OpenReplicatedSnapshotStage(identity, wrong, nil,
		testReplicatedApplyOptions(), replicatedstate.SnapshotArtifactStageOptions{}); stage != nil ||
		!errors.Is(err, ErrReplicatedSnapshotStageProof) {
		t.Fatalf("wrong binding stage=%v err=%v", stage, err)
	}
}
