package splitcontroller

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type childPrepareTestPreparer struct {
	mu      sync.Mutex
	raw     []byte
	receipt ChildPrepareReceipt
	calls   int
}

func (preparer *childPrepareTestPreparer) PrepareChild(
	_ context.Context, preparation ChildPreparation,
) (ChildPrepareReceipt, error) {
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	raw, err := AppendChildPreparation(nil, preparation)
	if err != nil {
		return ChildPrepareReceipt{}, err
	}
	if len(preparer.raw) != 0 && string(raw) != string(preparer.raw) {
		return ChildPrepareReceipt{}, ErrChildPreparation
	}
	preparer.raw = raw
	preparer.calls++
	if preparer.receipt.ReceiptDigest == ([32]byte{}) {
		preparer.receipt, err = NewChildPrepareReceipt(preparation, preparation.ReplicaTarget())
	}
	return preparer.receipt, err
}

type childPrepareTestOpener struct {
	service *ChildPrepareService
	peer    rafttransport.PeerIdentity
	drop    bool
	done    <-chan struct{}
}

func (opener *childPrepareTestOpener) OpenShardControl(
	ctx context.Context, _ rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	// The lost reply and the single-slot admission limit are separate faults.
	// Wait for the previous server to release its slot before replaying.
	if opener.done != nil {
		select {
		case <-opener.done:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	client, server := net.Pipe()
	done := make(chan struct{})
	opener.done = done
	go func() {
		defer close(done)
		_ = opener.service.Serve(ctx, &planObservationTestConnection{
			Conn: server, peer: opener.peer, class: rafttransport.TrafficShardControl,
		})
	}()
	connection := &planObservationTestConnection{
		Conn: client, peer: opener.peer, class: rafttransport.TrafficShardControl,
	}
	if opener.drop {
		opener.drop = false
		return &planAdmissionDropReadConnection{planObservationTestConnection: connection}, nil
	}
	return connection, nil
}

func TestChildPrepareTransportSettlesExactReplayAfterLostReceipt(t *testing.T) {
	plan, _, target, split := testPlanWithChildLeaders(
		t, []distribution.EndpointID{"node-b", "node-c", "node-d"},
	)
	descriptor, _ := split.Child(int(target.Child))
	preparation, err := NewChildPreparation(
		plan.OperationID(), testManifestDigest("child-allocation"), descriptor, "docs", target, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	controller := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}}
	preparer := new(childPrepareTestPreparer)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewChildPrepareService(ChildPrepareServiceOptions{
		Preparer: preparer,
		Authorize: func(peer rafttransport.PeerIdentity, got ChildPreparation) bool {
			return peer.Node == controller.Node && got.OperationID() == preparation.OperationID() &&
				got.ReplicaTarget().Node == target.Replicas[2].Node
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: MaxChildPrepareWireBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	opener := &childPrepareTestOpener{service: service, peer: controller, drop: true}
	client, err := NewChildPrepareClient(ChildPrepareClientOptions{
		Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline,
		MaxConcurrent: 1, MaxInflightBytes: MaxChildPrepareWireBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Prepare(t.Context(), target.Replicas[2].Node, preparation); !errors.Is(err, ErrRuntimeStoreOutcomeUnknown) {
		t.Fatalf("lost receipt err=%v", err)
	}
	receipt, err := client.Prepare(t.Context(), target.Replicas[2].Node, preparation)
	if err != nil || receipt.Target.Node != target.Replicas[2].Node || preparer.calls != 2 {
		t.Fatalf("receipt=%+v calls=%d err=%v", receipt, preparer.calls, err)
	}
}
