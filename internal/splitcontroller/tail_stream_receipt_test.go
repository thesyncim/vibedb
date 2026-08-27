package splitcontroller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

func TestTailStreamResumesDurablePendingReceiptThroughRealChildActions(t *testing.T) {
	plan, set, actions, before, configuration := testTailStreamTransportFixture(t)
	source, err := plan.partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	var batch rangesplit.TailBatch
	for tenant := 0; tenant < 1000 && batch.Operations == 0; tenant++ {
		entry := rangesplit.TailEntry{
			Applied: configuration.Applied, Term: configuration.Term,
			BeforeOwnershipEpoch: configuration.BeforeOwnershipEpoch, AfterOwnershipEpoch: configuration.AfterOwnershipEpoch,
			BeforeRoutingVersion: configuration.BeforeRoutingVersion, AfterRoutingVersion: configuration.AfterRoutingVersion,
			BeforeRouteGeneration: configuration.BeforeRouteGeneration, AfterRouteGeneration: configuration.AfterRouteGeneration,
			PreviousEntryDigest: configuration.PreviousEntryDigest, EntryDigest: [32]byte{181},
			BeforeDataChainDigest: configuration.BeforeDataChainDigest, AfterDataChainDigest: [32]byte{182},
			Transitions: []rangesplit.TailTransition{{Key: []byte("remote-receipt"), After: []byte(fmt.Sprintf(`{"tenant":%d}`, tenant))}},
		}
		_, _, err = plan.partitioner.TranslateTailEntry(source, entry, []rangesplit.TailSink{
			func(rangesplit.TailBatch) error { return nil }, func(value rangesplit.TailBatch) error { batch = value; return nil },
		}, &rangesplit.TailWorkspace{})
		if err != nil {
			t.Fatal(err)
		}
	}
	if batch.Operations != 1 {
		t.Fatal("no child mutation")
	}
	stage, revision, err := actions.openStage(plan, set, 1)
	if err != nil {
		t.Fatal(err)
	}
	persist := actions.stagePersistence(plan, set.Children[1], 1, &revision)
	runtimePath := actions.store.runtimeRoot.Name()
	manifest := actions.store.manifest
	fault := errors.New("operation directory sync failed")
	actions.store.syncOperation = func(*os.Root) error { return fault }
	err = stage.ApplyTailBatch(batch, persist)
	if !errors.Is(err, rangesplit.ErrChildStageOutcomeUnknown) {
		t.Fatal(err)
	}
	if _, _, err := actions.Observe(plan, set, 1); !errors.Is(err, ErrRuntimeStoreOutcomeUnknown) {
		t.Fatal("unfenced receipt exposed")
	}
	if err := stage.ApplyTailBatch(batch, persist); !errors.Is(err, rangesplit.ErrChildStageOutcomeUnknown) {
		t.Fatal("uncertain stage allowed writes")
	}
	if _, found, err := actions.collection.AppendRaw(nil, []byte("remote-receipt")); err != nil || found {
		t.Fatal("rows written before durable receipt")
	}
	if err := actions.store.Close(); err != nil {
		t.Fatal(err)
	}
	failedRoot, err := os.OpenRoot(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	fences := 0
	failedStore, err := openDurableRuntimeStoreAtRootWithSync(failedRoot, plan.OperationID(), manifest, true, func(*os.Root) error { fences++; return fault })
	if !errors.Is(err, ErrRuntimeStoreOutcomeUnknown) || failedStore != nil || fences != 1 {
		t.Fatalf("unfenced reopen store=%v error=%v fences=%d", failedStore, err, fences)
	}
	_ = failedRoot.Close()
	repairedRoot, err := os.OpenRoot(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := openDurableRuntimeStoreAtRootWithSync(repairedRoot, plan.OperationID(), manifest, true, func(root *os.Root) error { fences++; return syncRuntimeRoot(root) })
	if err != nil || fences != 2 {
		t.Fatalf("repair error=%v fences=%d", err, fences)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	actions.store = recovered
	pending, ok, err := actions.Observe(plan, set, 1)
	if err != nil || !ok || !pending.ResumesTailBatch(before, batch) {
		t.Fatalf("pending receipt: present=%v error=%v", ok, err)
	}
	resolved, err := ResolveLocalTailStreamTarget(plan, set, 1, actions)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := rangesplit.NewTailStreamBinding([32]byte(plan.OperationID()), set.Children[1])
	if err != nil {
		t.Fatal(err)
	}
	controller := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}, TrustDomain: resolved.TrustDomain}
	destination := rafttransport.PeerIdentity{Node: rafttransport.NodeID{8}, TrustDomain: resolved.TrustDomain}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := NewTailStreamService(TailStreamServiceOptions{
		Resolver: tailStreamResolveFunc(func(context.Context, OperationID, uint8) (TailStreamResolvedTarget, error) { return resolved, nil }),
		Authorize: func(peer rafttransport.PeerIdentity, got rangesplit.TailStreamBinding) bool {
			return peer == controller && got == binding
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1, MaxInflightBytes: rangesplit.MaxTailStreamRequestBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	client, err := NewTailStreamClient(TailStreamClientOptions{
		Opener: tailStreamOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			local, remote := net.Pipe()
			go func() {
				serverDone <- service.Serve(context.Background(), &tailStreamPeerConn{Conn: remote, identity: controller, class: rafttransport.TrafficShardControl})
			}()
			return &tailStreamPeerConn{Conn: local, identity: destination, class: rafttransport.TrafficShardControl}, nil
		}), ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: uint64(rangesplit.MaxTailStreamRequestBytes + rangesplit.TailStreamResponseBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Apply(context.Background(), destination.Node, resolved.TrustDomain, rangesplit.TailStreamRequest{Binding: binding, Before: before, Batch: batch})
	if err != nil {
		t.Fatal(err)
	}
	if err = <-serverDone; err != nil {
		t.Fatal(err)
	}
	if response.LastBatchDigest() != batch.Digest || response.SourceCut().Applied != batch.Applied {
		t.Fatal("remote receipt did not settle exact entry")
	}
	if _, found, err := actions.collection.AppendRaw(nil, []byte("remote-receipt")); err != nil || !found {
		t.Fatalf("real child row: found=%v error=%v", found, err)
	}
}
