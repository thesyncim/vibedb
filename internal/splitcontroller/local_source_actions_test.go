package splitcontroller

import (
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestLocalSourceActionsRecoverCaptureAndPublishImmutableArtifacts(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), sha256.Sum256([]byte("source manifest")),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	actions, err := NewLocalSourceActions(store, source.machine, source.capture)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := actions.ExecuteStartCapture(plan)
	if err != nil {
		t.Fatal(err)
	}
	initial, revision, ok, err := store.LoadSourceCaptureDescriptor(plan.partitioner)
	if err != nil || !ok || revision != 1 {
		t.Fatalf("initial descriptor revision=%d ok=%v err=%v", revision, ok, err)
	}
	if _, err = source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err = actions.ExecuteStartCapture(plan); err != nil {
		t.Fatal(err)
	}
	advanced, revision, ok, err := store.LoadSourceCaptureDescriptor(plan.partitioner)
	if err != nil || !ok || revision != 2 || advanced.Head.Applied != 2 {
		t.Fatalf("advanced descriptor revision=%d applied=%d ok=%v err=%v", revision, advanced.Head.Applied, ok, err)
	}
	var chainWorkspace rangesplit.SourceCaptureWorkspace
	if err = capture.ValidateDescriptorAncestor(initial, &chainWorkspace); err != nil {
		t.Fatalf("initial descriptor no longer proves an ancestor: %v", err)
	}
	forged := initial
	forged.Head.EntryDigest[0] ^= 0xff
	if err = capture.ValidateDescriptorAncestor(forged, &chainWorkspace); !errors.Is(err, rangesplit.ErrSourceCapture) {
		t.Fatalf("forged ancestor accepted: %v", err)
	}

	artifacts, err := actions.ExecuteBuildArtifacts(
		plan, capture, rangesplit.MinChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !artifacts.Children[1].Present || artifacts.Children[1].EncodedBytes == 0 {
		t.Fatalf("missing child artifact: %+v", artifacts.Children[1])
	}
	opened, err := actions.OpenChildArtifact(plan, artifacts, 1)
	if err != nil {
		t.Fatal(err)
	}
	if raw, readErr := io.ReadAll(opened); readErr != nil || uint64(len(raw)) != artifacts.Children[1].EncodedBytes {
		t.Fatalf("read artifact bytes=%d err=%v", len(raw), readErr)
	}
	if err = opened.Close(); err != nil {
		t.Fatal(err)
	}
	retried, err := actions.ExecuteBuildArtifacts(
		plan, capture, rangesplit.MinChildArtifactChunkBytes,
	)
	if err != nil || retried != artifacts {
		t.Fatalf("idempotent retry changed artifact set: err=%v", err)
	}
}

func TestLocalSourceActionsRefuseCopiedPlanAuthority(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), OperationID{9}, sha256.Sum256([]byte("source manifest")),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	actions, err := NewLocalSourceActions(store, source.machine, source.capture)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = actions.ExecuteStartCapture(plan); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("copied plan started capture: %v", err)
	}
}
