package splitcontroller

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

type tailStreamResolveFunc func(context.Context, OperationID, uint8) (TailStreamResolvedTarget, error)

func (resolve tailStreamResolveFunc) ResolveSplitTail(
	ctx context.Context,
	operation OperationID,
	child uint8,
) (TailStreamResolvedTarget, error) {
	return resolve(ctx, operation, child)
}

type tailStreamOpenFunc func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (open tailStreamOpenFunc) OpenShardControl(
	ctx context.Context,
	node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return open(ctx, node)
}

type tailStreamPeerConn struct {
	net.Conn
	identity  rafttransport.PeerIdentity
	class     rafttransport.TrafficClass
	mu        sync.Mutex
	written   []byte
	failWrite bool
}

func (connection *tailStreamPeerConn) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (*tailStreamPeerConn) PeerKeyDigest() [32]byte { return [32]byte{} }

func (connection *tailStreamPeerConn) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

func (connection *tailStreamPeerConn) Write(raw []byte) (int, error) {
	if connection.failWrite {
		return 0, io.ErrClosedPipe
	}
	written, err := connection.Conn.Write(raw)
	connection.mu.Lock()
	connection.written = append(connection.written, raw[:written]...)
	connection.mu.Unlock()
	return written, err
}

func (connection *tailStreamPeerConn) bytesWritten() []byte {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return bytes.Clone(connection.written)
}

func TestTailStreamTransportRetriesByteIdenticallyAfterLostResponse(t *testing.T) {
	plan, artifacts, childActions, before, batch := testTailStreamTransportFixture(t)
	resolved, err := ResolveLocalTailStreamTarget(plan, artifacts, 1, childActions)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := rangesplit.NewTailStreamBinding(
		[32]byte(plan.OperationID()), artifacts.Children[1],
	)
	if err != nil {
		t.Fatal(err)
	}
	controller := rafttransport.PeerIdentity{
		Node: rafttransport.NodeID{9}, TrustDomain: resolved.TrustDomain,
	}
	destination := rafttransport.NodeID{8}
	destinationPeer := rafttransport.PeerIdentity{Node: destination, TrustDomain: resolved.TrustDomain}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	resolver := tailStreamResolveFunc(func(
		_ context.Context, operation OperationID, child uint8,
	) (TailStreamResolvedTarget, error) {
		if operation != plan.OperationID() || child != 1 {
			return TailStreamResolvedTarget{}, ErrTailStreamConflict
		}
		return resolved, nil
	})
	service, err := NewTailStreamService(TailStreamServiceOptions{
		Resolver: resolver,
		Authorize: func(peer rafttransport.PeerIdentity, got rangesplit.TailStreamBinding) bool {
			return peer == controller && got == binding
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: rangesplit.MaxTailStreamRequestBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	var attemptsMu sync.Mutex
	var attempts [][]byte
	serveResults := make(chan error, 2)
	opens := 0
	opener := tailStreamOpenFunc(func(
		_ context.Context, node rafttransport.NodeID,
	) (rafttransport.PeerConnection, error) {
		if node != destination {
			return nil, ErrTailStreamConflict
		}
		opens++
		clientSide, serverSide := net.Pipe()
		clientConnection := &tailStreamPeerConn{
			Conn: clientSide, identity: destinationPeer, class: rafttransport.TrafficShardControl,
		}
		serverConnection := &tailStreamPeerConn{
			Conn: serverSide, identity: controller, class: rafttransport.TrafficShardControl,
			failWrite: opens == 1,
		}
		go func() {
			serveResults <- service.Serve(context.Background(), serverConnection)
			attemptsMu.Lock()
			attempts = append(attempts, clientConnection.bytesWritten())
			attemptsMu.Unlock()
		}()
		return clientConnection, nil
	})
	client, err := NewTailStreamClient(TailStreamClientOptions{
		Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline,
		MaxConcurrent:    1,
		MaxInflightBytes: uint64(rangesplit.MaxTailStreamRequestBytes + rangesplit.TailStreamResponseBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	wrongClientSide, wrongServerSide := net.Pipe()
	wrongClient, err := NewTailStreamClient(TailStreamClientOptions{
		Opener: tailStreamOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			return &tailStreamPeerConn{
				Conn: wrongClientSide,
				identity: rafttransport.PeerIdentity{
					Node: rafttransport.NodeID{7}, TrustDomain: resolved.TrustDomain,
				},
				class: rafttransport.TrafficShardControl,
			}, nil
		}),
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: uint64(rangesplit.MaxTailStreamRequestBytes + rangesplit.TailStreamResponseBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, wrongErr := wrongClient.Apply(context.Background(), destination, resolved.TrustDomain, rangesplit.TailStreamRequest{
		Binding: binding, Before: before, Batch: batch,
	})
	_ = wrongServerSide.Close()
	if !errors.Is(wrongErr, ErrTailStreamUnauthorized) {
		t.Fatalf("wrong peer error=%v", wrongErr)
	}
	sink, err := NewRemoteTailSink(
		context.Background(), client, destination, resolved.TrustDomain, binding, before,
	)
	if err != nil {
		t.Fatal(err)
	}
	if applyErr := sink.Apply(batch); !errors.Is(applyErr, ErrTailStreamOutcomeUnknown) {
		t.Fatalf("first apply error=%v", applyErr)
	}
	if sink.Cursor() != before {
		t.Fatal("outcome-unknown advanced client cursor")
	}
	firstServeErr := <-serveResults
	if firstServeErr == nil {
		t.Fatal("lost response did not fail server write")
	}
	if applyErr := sink.Apply(batch); applyErr != nil {
		t.Fatalf("retry apply: %v", applyErr)
	}
	if secondServeErr := <-serveResults; secondServeErr != nil {
		t.Fatalf("retry serve: %v", secondServeErr)
	}
	after := sink.Cursor()
	if after.SourceCut().Applied != batch.Applied || after.LastBatchDigest() != batch.Digest {
		t.Fatalf("after=%+v", after)
	}
	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	if len(attempts) != 2 || !bytes.Equal(attempts[0], attempts[1]) {
		t.Fatalf("attempts=%d equal=%v", len(attempts), len(attempts) == 2 && bytes.Equal(attempts[0], attempts[1]))
	}
}

func TestTailStreamServiceRejectsBeforeVariableAllocation(t *testing.T) {
	resolver := tailStreamResolveFunc(func(
		context.Context, OperationID, uint8,
	) (TailStreamResolvedTarget, error) {
		return TailStreamResolvedTarget{}, errors.New("must not resolve")
	})
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewTailStreamService(TailStreamServiceOptions{
		Resolver: resolver, Authorize: func(rafttransport.PeerIdentity, rangesplit.TailStreamBinding) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1, MaxInflightBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	clientSide, serverSide := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(context.Background(), &tailStreamPeerConn{
			Conn: serverSide, identity: rafttransport.PeerIdentity{Node: rafttransport.NodeID{1}},
			class: rafttransport.TrafficShardControl,
		})
	}()
	header := make([]byte, 32)
	discriminator := TailStreamRequestDiscriminator()
	copy(header[:8], discriminator[:])
	// A canonical-looking maximum-sized header exceeds the configured budget.
	header[10] = 32
	total := uint32(rangesplit.MaxTailStreamRequestBytes)
	header[12], header[13], header[14], header[15] = byte(total), byte(total>>8), byte(total>>16), byte(total>>24)
	header[16], header[17] = 0, 1 // 256
	binary.LittleEndian.PutUint32(header[20:24], rangesplit.ChildStageCursorEncodedBytes)
	batch := uint32(rangesplit.MaxTailBatchWireBytes)
	header[24], header[25], header[26], header[27] = byte(batch), byte(batch>>8), byte(batch>>16), byte(batch>>24)
	if _, err = clientSide.Write(header); err != nil {
		t.Fatal(err)
	}
	_ = clientSide.Close()
	if serveErr := <-done; !errors.Is(serveErr, ErrTailStreamBound) {
		t.Fatalf("serve error=%v", serveErr)
	}
}

func TestTailStreamServiceAppliesConnectionBackpressureBeforeRead(t *testing.T) {
	resolver := tailStreamResolveFunc(func(
		context.Context, OperationID, uint8,
	) (TailStreamResolvedTarget, error) {
		return TailStreamResolvedTarget{}, errors.New("must not resolve")
	})
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewTailStreamService(TailStreamServiceOptions{
		Resolver:     resolver,
		Authorize:    func(rafttransport.PeerIdentity, rangesplit.TailStreamBinding) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: rangesplit.MaxTailStreamRequestBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstClient, firstServer := net.Pipe()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.Serve(context.Background(), &tailStreamPeerConn{
			Conn: firstServer, identity: rafttransport.PeerIdentity{Node: rafttransport.NodeID{1}},
			class: rafttransport.TrafficShardControl,
		})
	}()
	deadlineAt := time.Now().Add(time.Second)
	for len(service.slots) != 1 && time.Now().Before(deadlineAt) {
		time.Sleep(time.Millisecond)
	}
	if len(service.slots) != 1 {
		t.Fatal("first stream did not acquire connection slot")
	}
	secondClient, secondServer := net.Pipe()
	secondErr := service.Serve(context.Background(), &tailStreamPeerConn{
		Conn: secondServer, identity: rafttransport.PeerIdentity{Node: rafttransport.NodeID{2}},
		class: rafttransport.TrafficShardControl,
	})
	_ = secondClient.Close()
	if !errors.Is(secondErr, ErrTailStreamBound) {
		t.Fatalf("second error=%v", secondErr)
	}
	_ = firstClient.Close()
	if firstErr := <-firstDone; firstErr == nil {
		t.Fatal("truncated first stream accepted")
	}
}

func testTailStreamTransportFixture(t testing.TB) (
	*Plan,
	rangesplit.ChildArtifactSet,
	*LocalChildActions,
	rangesplit.ChildStageCursor,
	rangesplit.TailBatch,
) {
	t.Helper()
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	sourceStore, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("tail-stream-source"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceStore.Close() })
	sourceActions, err := NewLocalSourceActions(sourceStore, source.machine, source.capture)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := sourceActions.ExecuteStartCapture(plan)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := sourceActions.ExecuteBuildArtifacts(plan, capture, rangesplit.MinChildArtifactChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	database, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	collection, err := database.CreateCollection("child", durable.Options{MaxBatchDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	childStore, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("tail-stream-child"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = childStore.Close() })
	childActions, err := NewLocalChildActions(
		childStore, collection, rangesplit.MaxChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceActions.OpenChildArtifact(plan, artifacts, 1)
	if err != nil {
		t.Fatal(err)
	}
	before, err := childActions.ExecuteStageChild(plan, artifacts, 1, artifact)
	closeErr := artifact.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("stage=%v close=%v", err, closeErr)
	}
	if _, err = source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	captured := errors.New("captured tail batch")
	var batch rangesplit.TailBatch
	_, _, err = sourceActions.ExecuteCatchUpTail(plan, capture, artifacts, []rangesplit.TailSink{
		func(rangesplit.TailBatch) error { return nil },
		func(value rangesplit.TailBatch) error { batch = value; return captured },
	})
	if !errors.Is(err, captured) || batch.Digest == ([32]byte{}) {
		t.Fatalf("capture error=%v batch=%+v", err, batch)
	}
	return plan, artifacts, childActions, before, batch
}
