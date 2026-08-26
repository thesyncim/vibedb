package splitcontroller

import (
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestLocalChildActionsStageRecoverAndRetryTailBeforeGlobalCursor(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	sourceStore, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("source-child-runtime"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceStore.Close()
	sourceActions, err := NewLocalSourceActions(sourceStore, source.machine, source.capture)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := sourceActions.ExecuteStartCapture(plan)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := sourceActions.ExecuteBuildArtifacts(
		plan, capture, rangesplit.MinChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}

	childDatabase, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer childDatabase.Close()
	childCollection, err := childDatabase.CreateCollection(
		"child", durable.Options{MaxBatchDocuments: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	childRoot := t.TempDir()
	childManifest := testManifestDigest("exact-child-runtime")
	childStore, err := OpenDurableRuntimeStore(childRoot, plan.OperationID(), childManifest)
	if err != nil {
		t.Fatal(err)
	}
	childActions, err := NewLocalChildActions(
		childStore, childCollection, rangesplit.MaxChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceActions.OpenChildArtifact(plan, artifacts, 1)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := childActions.ExecuteStageChild(plan, artifacts, 1, artifact)
	closeArtifactErr := artifact.Close()
	if err != nil || closeArtifactErr != nil || staged.Phase() != rangesplit.ChildStageTail {
		t.Fatalf("stage=%v close=%v phase=%v", err, closeArtifactErr, staged.Phase())
	}
	// An exact retry consumes no artifact bytes after durable completion.
	if retried, retryErr := childActions.ExecuteStageChild(
		plan, artifacts, 1, &failOnRead{},
	); retryErr != nil || retried.SourceCut() != staged.SourceCut() {
		t.Fatalf("stage retry cursor=%+v err=%v", retried, retryErr)
	}
	if err = childStore.Close(); err != nil {
		t.Fatal(err)
	}
	childStore, err = OpenDurableRuntimeStore(childRoot, plan.OperationID(), childManifest)
	if err != nil {
		t.Fatal(err)
	}
	defer childStore.Close()
	childActions, err = NewLocalChildActions(
		childStore, childCollection, rangesplit.MaxChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if recovered, ok, observeErr := childActions.Observe(
		plan, artifacts, 1,
	); observeErr != nil || !ok || recovered.SourceCut() != staged.SourceCut() {
		t.Fatalf("recovered=%+v ok=%v err=%v", recovered, ok, observeErr)
	}

	if _, err = source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	first := true
	sinks := []rangesplit.TailSink{
		func(rangesplit.TailBatch) error { return nil },
		func(batch rangesplit.TailBatch) error {
			if applyErr := childActions.ApplyTailBatch(plan, artifacts, 1, batch); applyErr != nil {
				return applyErr
			}
			if first {
				first = false
				return errInjectedTailAck
			}
			return nil
		},
	}
	initial, advanced, err := sourceActions.ExecuteCatchUpTail(
		plan, capture, artifacts, sinks,
	)
	if !errors.Is(err, errInjectedTailAck) || advanced || initial.SourceCut().Applied != 1 {
		t.Fatalf("first catch-up applied=%d advanced=%v err=%v", initial.SourceCut().Applied, advanced, err)
	}
	childAhead, ok, err := childActions.Observe(plan, artifacts, 1)
	if err != nil || !ok || childAhead.SourceCut().Applied != 2 {
		t.Fatalf("child before global retry applied=%d ok=%v err=%v", childAhead.SourceCut().Applied, ok, err)
	}
	settled, advanced, err := sourceActions.ExecuteCatchUpTail(
		plan, capture, artifacts, sinks,
	)
	if err != nil || !advanced || settled.SourceCut().Applied != 2 {
		t.Fatalf("retry catch-up applied=%d advanced=%v err=%v", settled.SourceCut().Applied, advanced, err)
	}
	if childCollection.Len() != artifacts.Children[1].Rows {
		t.Fatalf("idempotent empty tail changed rows=%d want=%d", childCollection.Len(), artifacts.Children[1].Rows)
	}
}

func TestLocalChildActionsRejectWrongChildAndArtifactAuthority(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	childDatabase, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer childDatabase.Close()
	collection, err := childDatabase.CreateCollection("child", durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("child-rejection"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	actions, err := NewLocalChildActions(store, collection, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = actions.ExecuteStageChild(
		plan, rangesplit.ChildArtifactSet{}, 0, &failOnRead{},
	); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("retained child accepted: %v", err)
	}
}

var errInjectedTailAck = errors.New("injected child acknowledgement loss")

type failOnRead struct{}

func (*failOnRead) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func testManifestDigest(marker string) [32]byte {
	var digest [32]byte
	copy(digest[:], marker)
	return digest
}
