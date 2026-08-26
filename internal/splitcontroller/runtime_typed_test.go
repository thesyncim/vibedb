package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestDurableRuntimeStoreRecoversTypedSplitAuthority(t *testing.T) {
	plan, _, _, split := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	state := flowSourceState(t, source.machine)
	capture, err := rangesplit.NewSourceCapture(
		plan.partitioner, "typed-runtime-capture", source.capture,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}

	cut, err := source.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	options := rangesplit.ChildArtifactOptions{TargetChunkBytes: rangesplit.MinChildArtifactChunkBytes}
	options.Writers[1] = &artifact
	options.PayloadBuffers[1] = make([]byte, 0, rangesplit.MaxChildArtifactChunkBytes)
	var workspace rangesplit.ChildArtifactWorkspace
	artifacts, buildErr := plan.partitioner.WriteChildArtifacts(cut, options, &workspace)
	closeErr := cut.Close()
	if buildErr != nil || closeErr != nil {
		t.Fatalf("artifacts=%v close=%v", buildErr, closeErr)
	}
	tail, err := plan.partitioner.InitialTailCursor(artifacts)
	if err != nil {
		t.Fatal(err)
	}

	manifest := sha256.Sum256([]byte("exact retained serving manifest"))
	root := t.TempDir()
	store, err := OpenDurableRuntimeStore(root, plan.OperationID(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PersistSourceCapture(1, capture); err != nil {
		t.Fatal(err)
	}
	if err = store.PersistChildArtifacts(1, artifacts); err != nil {
		t.Fatal(err)
	}
	if err = store.PersistTailCursor(1, tail); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	wrongManifest := manifest
	wrongManifest[0] ^= 0xff
	if _, err = OpenDurableRuntimeStore(root, plan.OperationID(), wrongManifest); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("wrong manifest recovered typed authority: %v", err)
	}

	store, err = OpenDurableRuntimeStore(root, plan.OperationID(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recovered, revision, ok, err := store.RecoverSourceCapture(
		plan.partitioner, "typed-runtime-capture", source.capture, state,
	)
	if err != nil || !ok || revision != 1 || recovered.Head() != state.Applied {
		t.Fatalf("capture=%v revision=%d ok=%v err=%v", recovered, revision, ok, err)
	}
	openedArtifacts, revision, ok, err := store.LoadChildArtifacts(plan.partitioner)
	if err != nil || !ok || revision != 1 {
		t.Fatalf("artifacts revision=%d ok=%v err=%v", revision, ok, err)
	}
	wantArtifacts, _ := rangesplit.AppendChildArtifactSet(nil, artifacts)
	gotArtifacts, _ := rangesplit.AppendChildArtifactSet(nil, openedArtifacts)
	if !bytes.Equal(gotArtifacts, wantArtifacts) {
		t.Fatal("recovered artifact authority changed canonical identity")
	}
	openedTail, revision, ok, err := store.LoadTailCursor(plan.partitioner)
	if err != nil || !ok || revision != 1 {
		t.Fatalf("tail revision=%d ok=%v err=%v", revision, ok, err)
	}
	wantTail, _ := rangesplit.AppendTailCursor(nil, tail)
	gotTail, _ := rangesplit.AppendTailCursor(nil, openedTail)
	if !bytes.Equal(gotTail, wantTail) {
		t.Fatal("recovered tail authority changed canonical identity")
	}
	if _, err = source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = store.RecoverSourceCapture(
		plan.partitioner, "typed-runtime-capture", source.capture,
		flowSourceState(t, source.machine),
	); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("stale capture descriptor recovered newer collection: %v", err)
	}

	// A structurally valid but different plan cannot adopt any typed record.
	wrongPartitioner, err := rangesplit.NewPartitioner(
		split, "other-table", []string{"/other"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = store.LoadSourceCaptureDescriptor(wrongPartitioner); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("wrong partition capture err=%v", err)
	}
	if _, _, _, err = store.LoadChildArtifacts(wrongPartitioner); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("wrong partition artifacts err=%v", err)
	}
}

func TestDurableRuntimeStoreTypedRecoveryRequiresExistingCapture(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), sha256.Sum256([]byte("manifest")),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if capture, revision, ok, recoverErr := store.RecoverSourceCapture(
		plan.partitioner, "typed-runtime-capture", source.capture,
		flowSourceState(t, source.machine),
	); recoverErr != nil || ok || revision != 0 || capture != nil {
		t.Fatalf("capture=%v revision=%d ok=%v err=%v", capture, revision, ok, recoverErr)
	}
}
