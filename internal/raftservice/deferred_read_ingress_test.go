package raftservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

type independentReadHost struct {
	*deferredReadOwnerHost
	healthy        raftmember.GroupKey
	healthyContext []byte
}

func (h *independentReadHost) ReadIndex(group raftmember.GroupKey, context []byte) error {
	if group != h.healthy {
		return h.deferredReadOwnerHost.ReadIndex(group, context)
	}
	h.healthyContext = append([]byte(nil), context...)
	return nil
}
func (h *independentReadHost) RunOne() (multiraft.Progress, bool, error) {
	p, done, err := h.deferredReadOwnerHost.RunOne()
	if done || err != nil || len(h.healthyContext) == 0 {
		return p, done, err
	}
	context := h.healthyContext
	h.healthyContext = nil
	return multiraft.Progress{Group: h.healthy, Kind: multiraft.ProgressReady,
		ReadyKind: raftmember.DriveReadStatesFinished,
		ReadOutcomes: []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
			Context: context, Index: h.status.Applied, Term: h.status.Term, Incarnation: 1,
		}}}}, true, nil
}
func (h *independentReadHost) AdoptMessage(raftmember.GroupKey, *pb.Message) error {
	// A peer packet is allowed to make the blocked group runnable even though
	// no append-worker notification or logical tick occurs in this fixture.
	h.mu.Lock()
	h.woken = true
	h.mu.Unlock()
	return nil
}

func TestOwnerDeferredReadAllowsOtherGroupAndPeerProgress(t *testing.T) {
	group, command, base, owner, source := newDeferredReadOwnerFixture()
	healthy := group
	healthy.GroupID[0]++
	member := owner.members[group]
	member.identity.Group = healthy
	member.generation = &ownerGeneration{}
	owner.members[healthy] = member
	owner.groups = append(owner.groups, healthy)
	owner.limits.MaxIngressItems = 4
	owner.limits.MaxPendingReadItems = 4
	owner.limits.MaxPendingReadBytes = 4
	host := &independentReadHost{deferredReadOwnerHost: base, healthy: healthy}
	owner.host = host
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	<-host.firstRunEntered
	host.mu.Lock()
	host.readyPending = true
	host.mu.Unlock()
	blockedResult := make(chan error, 1)
	var blockedCut, healthyCut LinearizablePointReadCut
	defer blockedCut.Close()
	defer healthyCut.Close()
	go func() {
		blockedResult <- owner.ReadLinearizablePointInto(ctx, deferredLinearizablePointRequest(group, command), &blockedCut)
	}()
	close(host.releaseFirstRun)
	<-host.secondReadAttempt

	if err := owner.ReadLinearizablePointInto(ctx, deferredLinearizablePointRequest(healthy, command), &healthyCut); err != nil {
		t.Fatalf("healthy group blocked behind unrelated Ready: %v", err)
	}
	if healthyCut.Source() != source || healthyCut.minimumApplied != 7 {
		t.Fatal("healthy read lost its authenticated cut")
	}
	select {
	case err := <-blockedResult:
		t.Fatalf("blocked read settled without its own barrier: %v", err)
	default:
	}
	reply := make(chan ownerReply, 1)
	if err := owner.publish(ownerRequest{kind: requestInbound, group: group, reply: reply,
		inbound: rafttransport.Inbound{Group: group, Message: &pb.Message{}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-reply:
		if got.err != nil {
			t.Fatal(got.err)
		}
	case <-ctx.Done():
		t.Fatal("peer traffic blocked behind deferred read")
	}
	select {
	case err := <-blockedResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("original read never resumed")
	}
	if blockedCut.Source() != source || blockedCut.minimumApplied != 7 {
		t.Fatal("resumed read lost its authenticated cut")
	}
	blockedCut.Close()
	healthyCut.Close()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.ingressItems != 0 || owner.pendingReadItems != 0 {
		t.Fatalf("retained charges: ingress=%d reads=%d", owner.ingressItems, owner.pendingReadItems)
	}
}

func TestDeferredReadRetryKeepsEachGroupIndependentAndPreservesTail(t *testing.T) {
	a := peerServerTestGroup()
	b := a
	b.GroupID[0]++
	q := newDeferredReadIngress(4)
	for i, group := range []raftmember.GroupKey{a, a, b, b} {
		q.retain(ownerRequest{group: group, bytes: int64(i)})
	}
	var calls []int64
	progressed, err := q.retry(func(r ownerRequest) error {
		calls = append(calls, r.bytes)
		if r.group == a {
			return errOwnerReadDeferred
		}
		return nil
	})
	if !progressed || err != nil || len(calls) != 3 || calls[0] != 0 || calls[1] != 2 || calls[2] != 3 || len(q.requests) != 2 || q.requests[1].bytes != 1 {
		t.Fatalf("retry calls=%v retained=%v progressed=%t err=%v", calls, q.requests, progressed, err)
	}
	failure := errors.New("terminal owner failure")
	if _, err := q.retry(func(ownerRequest) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(q.requests) != 1 || q.requests[0].bytes != 1 {
		t.Fatal("lost unsettled tail after fatal retry")
	}
	if _, err := q.retry(func(ownerRequest) error { return nil }); err != nil || len(q.requests) != 0 {
		t.Fatalf("queue not drained: %v", err)
	}
}
