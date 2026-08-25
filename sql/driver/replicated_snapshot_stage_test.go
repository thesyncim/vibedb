package driver

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
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
	stage, target, bootstrap := newReplicatedSnapshotStageFixture(t)
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
	retried, err := stage.Activate(bootstrap)
	if err != nil || retried.Apply != activation.Apply ||
		retried.ArtifactManifest.Digest != activation.ArtifactManifest.Digest {
		t.Fatalf("activation retry=%+v err=%v", retried, err)
	}
	if _, err := target.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("serving before runtime adoption err=%v", err)
	}
}

func newReplicatedSnapshotStageFixture(t *testing.T) (*ReplicatedSnapshotStage, *Database, *pb.Snapshot) {
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
	applyReplicatedApplySessionOpen(t, apply, sourceIdentity, 2)
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
	return stage, target, bootstrap
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
			stage, target, bootstrap := newReplicatedSnapshotStageFixture(t)
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
