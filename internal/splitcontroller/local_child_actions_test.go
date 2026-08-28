package splitcontroller

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/splitartifact"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

type splitArtifactPeerConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
}

func (connection *splitArtifactPeerConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (*splitArtifactPeerConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficSnapshot
}

type splitArtifactTestOpener struct {
	service        *splitartifact.Service
	source, target rafttransport.PeerIdentity
}

func (opener splitArtifactTestOpener) OpenSnapshot(
	ctx context.Context,
	node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	if node != opener.source.Node {
		return nil, rafttransport.ErrNodeNotFound
	}
	client, server := net.Pipe()
	go func() {
		_ = opener.service.Serve(ctx, &splitArtifactPeerConnection{
			Conn: server, identity: opener.target,
		})
	}()
	return &splitArtifactPeerConnection{Conn: client, identity: opener.source}, nil
}

func TestLocalChildActionsStageAuthenticatedRemoteArtifact(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	sourceStore, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("remote-source-runtime"),
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
	childCollection, err := childDatabase.CreateCollection("child", durable.Options{MaxBatchDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	childStore, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("remote-child-runtime"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer childStore.Close()
	childActions, err := NewLocalChildActions(
		childStore, childCollection, rangesplit.MaxChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}

	identity, err := splitartifact.NewIdentity([32]byte(plan.OperationID()), artifacts.Children[1])
	if err != nil {
		t.Fatal(err)
	}
	sourcePeer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{31}}
	targetPeer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{41}}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := splitartifact.NewService(splitartifact.ServiceOptions{
		Source: SplitArtifactSource{Actions: sourceActions, Plan: plan, Set: artifacts},
		Authorize: func(peer rafttransport.PeerIdentity, got splitartifact.Identity) bool {
			return peer == targetPeer && got == identity
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
		MaxChunkBytes:    rangesplit.MinChildArtifactChunkBytes,
		MaxInflightBytes: rangesplit.MinChildArtifactChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := childActions.ExecuteStageChildRemote(
		context.Background(), plan, artifacts, 1,
		splitArtifactTestOpener{service: service, source: sourcePeer, target: targetPeer},
		sourcePeer.Node, deadline, deadline, rangesplit.MinChildArtifactChunkBytes, 1,
		make([]byte, rangesplit.MinChildArtifactChunkBytes),
	)
	if err != nil || cursor.Phase() != rangesplit.ChildStageTail {
		t.Fatalf("remote stage cursor=%+v err=%v", cursor, err)
	}
}

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
