package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

type countingReadIndexHost struct {
	ownerHost
	onReadIndex func()
	calls       int
	contexts    [][16]byte
	status      raftmember.RuntimeStatus
}

func (host *countingReadIndexHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *countingReadIndexHost) ReadIndex(_ raftmember.GroupKey, context []byte) error {
	host.calls++
	if host.onReadIndex != nil {
		host.onReadIndex()
	}
	var retained [16]byte
	copy(retained[:], context)
	host.contexts = append(host.contexts, retained)
	return nil
}

func (host *countingReadIndexHost) Close() error { return nil }

type sharedBarrierFixture struct {
	group      raftmember.GroupKey
	fence      ServingFence
	host       *countingReadIndexHost
	owner      *Owner
	generation *ownerGeneration
}

func newSharedBarrierFixture(t *testing.T) *sharedBarrierFixture {
	t.Helper()
	group := peerServerTestGroup()
	command := CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
		ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	fence := ServingFence{Group: group, AllocationGeneration: 1, Command: command,
		MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1, Term: 2}
	generation := &ownerGeneration{}
	host := &countingReadIndexHost{status: raftmember.RuntimeStatus{
		MemberID: 1, LeaderID: 1, Term: 2, Commit: 7, Applied: 7,
	}}
	owner := &Owner{
		host:    host,
		started: true, ingress: make(chan ownerRequest, 16), done: make(chan struct{}),
		members: map[raftmember.GroupKey]ownerMember{group: {
			identity:   raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1, MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1},
			command:    command,
			generation: generation,
			read:       &ownerTestReadSource{},
		}},
		limits:       Limits{MaxIngressItems: 16, MaxIngressBytes: 1 << 20, MaxPendingReadItems: 16, MaxPendingReadBytes: 1 << 20},
		pendingReads: make(map[[16]byte]*readDelivery),
		metrics:      &ProgressMetrics{},
	}
	return &sharedBarrierFixture{group: group, fence: fence, host: host, owner: owner, generation: generation}
}

func (fixture *sharedBarrierFixture) queue(t *testing.T, force bool) ownerRequest {
	t.Helper()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	request := ownerRequest{
		kind:  requestReadLinear,
		group: fixture.group,
		reply: make(chan ownerReply, 1),
		read: readRequest{
			fence: fixture.fence, minimumApplied: 1,
			delivery: delivery, forceReadIndex: force,
		},
	}
	if err := fixture.owner.publish(request); err != nil {
		t.Fatal(err)
	}
	return <-fixture.owner.ingress
}

func (fixture *sharedBarrierFixture) admit(t *testing.T, request ownerRequest) *readDelivery {
	t.Helper()
	if err := fixture.owner.handle(request); err != nil {
		t.Fatal(err)
	}
	fixture.owner.release(request.bytes)
	return request.read.delivery
}

func (fixture *sharedBarrierFixture) read(t *testing.T, force bool) *readDelivery {
	t.Helper()
	return fixture.admit(t, fixture.queue(t, force))
}

func (fixture *sharedBarrierFixture) settleAll(t *testing.T, err error) {
	t.Helper()
	if len(fixture.host.contexts) == 0 {
		t.Fatal("no barrier to settle")
	}
	for _, context := range fixture.host.contexts {
		outcome := raftmodel.ReadOutcome{Err: err}
		outcome.Barrier.Context = append([]byte(nil), context[:]...)
		outcome.Barrier.Index = 9
		fixture.owner.finishReadOutcomes([]raftmodel.ReadOutcome{outcome})
	}
}

func settleDeliveryReply(t *testing.T, delivery *readDelivery, wantMinimum uint64) {
	t.Helper()
	select {
	case reply := <-delivery.reply:
		if reply.read.generation != nil {
			defer reply.read.generation.release()
		}
		if reply.err != nil {
			t.Fatalf("waiter settled with error: %v", reply.err)
		}
		if reply.read.minimumApplied != wantMinimum {
			t.Fatalf("waiter minimumApplied=%d want %d", reply.read.minimumApplied, wantMinimum)
		}
	default:
		t.Fatal("waiter never settled")
	}
}

func TestSharedReadBarrierCoalescesConcurrentReads(t *testing.T) {
	fixture := newSharedBarrierFixture(t)
	requests := []ownerRequest{fixture.queue(t, false), fixture.queue(t, false), fixture.queue(t, false)}
	primary := fixture.admit(t, requests[0])
	waiterOne := fixture.admit(t, requests[1])
	waiterTwo := fixture.admit(t, requests[2])
	if fixture.host.calls != 1 {
		t.Fatalf("ReadIndex calls=%d want 1 shared round", fixture.host.calls)
	}
	if len(fixture.owner.pendingReads) != 1 || len(fixture.owner.sharedBarrierWaiters) != 1 {
		t.Fatalf("pending=%d waiterGroups=%d want 1 barrier total",
			len(fixture.owner.pendingReads), len(fixture.owner.sharedBarrierWaiters))
	}
	fixture.settleAll(t, nil)
	settleDeliveryReply(t, primary, 9)
	settleDeliveryReply(t, waiterOne, 9)
	settleDeliveryReply(t, waiterTwo, 9)
	if len(fixture.owner.pendingReads) != 0 || len(fixture.owner.sharedBarrierWaiters) != 0 {
		t.Fatal("completed barrier retained state")
	}
	if got := fixture.owner.metrics.readIndexShared.Load(); got != 2 {
		t.Fatalf("shared counter=%d want 2", got)
	}
	// A lone read after completion issues a fresh round: sharing never delays
	// single-client reads waiting for company.
	lone := fixture.read(t, false)
	if fixture.host.calls != 2 {
		t.Fatalf("ReadIndex calls=%d want fresh round after completion", fixture.host.calls)
	}
	fixture.settleAll(t, nil)
	settleDeliveryReply(t, lone, 9)
	if got := fixture.owner.metrics.readIndexShared.Load(); got != 2 {
		t.Fatalf("shared counter=%d want still 2", got)
	}
}

func TestSharedReadBarrierRejectsNewGeneration(t *testing.T) {
	fixture := newSharedBarrierFixture(t)
	a, b := fixture.queue(t, false), fixture.queue(t, false)
	primary := fixture.admit(t, a)
	member := fixture.owner.members[fixture.group]
	member.generation = &ownerGeneration{}
	fixture.owner.members[fixture.group] = member
	other := fixture.admit(t, b)
	if fixture.host.calls != 2 {
		t.Fatalf("ReadIndex calls=%d want separate round per generation", fixture.host.calls)
	}
	fixture.settleAll(t, nil)
	settleDeliveryReply(t, primary, 9)
	settleDeliveryReply(t, other, 9)
}

func TestSharedReadBarrierForceReadIndexStaysSolo(t *testing.T) {
	fixture := newSharedBarrierFixture(t)
	a, b := fixture.queue(t, false), fixture.queue(t, true)
	primary := fixture.admit(t, a)
	forced := fixture.admit(t, b)
	if fixture.host.calls != 2 {
		t.Fatalf("ReadIndex calls=%d want forced retry solo", fixture.host.calls)
	}
	fixture.settleAll(t, nil)
	settleDeliveryReply(t, primary, 9)
	settleDeliveryReply(t, forced, 9)
}

func TestSharedReadBarrierErrorFansOut(t *testing.T) {
	fixture := newSharedBarrierFixture(t)
	a, b := fixture.queue(t, false), fixture.queue(t, false)
	primary := fixture.admit(t, a)
	waiter := fixture.admit(t, b)
	if fixture.host.calls != 1 {
		t.Fatalf("ReadIndex calls=%d want 1", fixture.host.calls)
	}
	outcome := raftmodel.ReadOutcome{Err: errTestSharedBarrier}
	outcome.Barrier.Context = append([]byte(nil), fixture.host.contexts[0][:]...)
	fixture.owner.finishReadOutcomes([]raftmodel.ReadOutcome{outcome})
	for index, delivery := range []*readDelivery{primary, waiter} {
		select {
		case reply := <-delivery.reply:
			if reply.err == nil {
				t.Fatalf("waiter %d missing barrier error", index)
			}
		default:
			t.Fatalf("waiter %d never settled", index)
		}
	}
}

var errTestSharedBarrier = errors.New("test barrier failure")

func TestSharedReadBarrierCutoffPrecedesProtocolCall(t *testing.T) {
	f := newSharedBarrierFixture(t)
	var late ownerRequest
	f.host.onReadIndex = func() { f.host.onReadIndex = nil; late = f.queue(t, false) }
	first := f.read(t, false)
	second := f.admit(t, late)
	if f.host.calls != 2 {
		t.Fatal("read published during ReadIndex joined the earlier proof")
	}
	f.settleAll(t, nil)
	settleDeliveryReply(t, first, 9)
	settleDeliveryReply(t, second, 9)
}

func TestSharedReadBarrierKeepsIndividualFloors(t *testing.T) {
	f := newSharedBarrierFixture(t)
	a, b := f.queue(t, false), f.queue(t, false)
	b.read.minimumApplied = 15
	first, second := f.admit(t, a), f.admit(t, b)
	if f.host.calls != 1 {
		t.Fatal("pre-issue reads did not share")
	}
	f.settleAll(t, nil)
	settleDeliveryReply(t, first, 9)
	settleDeliveryReply(t, second, 15)
	if f.generation.pins.Load() != 0 {
		t.Fatal("generation pins leaked")
	}
}

func TestSharedReadBarrierRetainsCanceledWaiterBudget(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion", true: "close"}[stop], func(t *testing.T) {
			f := newSharedBarrierFixture(t)
			f.owner.limits.MaxPendingReadItems = 2
			a, b, c := f.queue(t, false), f.queue(t, false), f.queue(t, false)
			first, abandoned := f.admit(t, a), f.admit(t, b)
			if !abandoned.state.CompareAndSwap(readDeliveryPending, readDeliveryAbandoned) {
				t.Fatal("cancel failed")
			}
			rejected := c.read.delivery
			if err := f.owner.handle(c); !errors.Is(err, ErrIngressFull) {
				t.Fatal(err)
			}
			f.owner.release(c.bytes)
			reply := <-rejected.reply
			if !errors.Is(reply.err, ErrIngressFull) {
				t.Fatalf("retained waiter escaped bound: %v", reply.err)
			}
			if f.host.calls != 1 || f.owner.sharedReadWaiterCount != 1 {
				t.Fatal("unexpected retained state")
			}
			if stop {
				f.owner.stop(errTestSharedBarrier)
				if reply := <-first.reply; !errors.Is(reply.err, ErrOwnerClosed) {
					t.Fatal(reply.err)
				}
			} else {
				f.settleAll(t, nil)
				settleDeliveryReply(t, first, 9)
				// Settlement releases retained capacity, so a subsequent read proceeds.
				next := f.read(t, false)
				f.settleAll(t, nil)
				settleDeliveryReply(t, next, 9)
			}
			select {
			case <-abandoned.reply:
				t.Fatal("abandoned delivery was replied to")
			default:
			}
			if len(f.owner.pendingReads) != 0 || len(f.owner.sharedBarrierWaiters) != 0 || len(f.owner.inflightBarrier) != 0 || f.owner.sharedReadWaiterCount != 0 || f.generation.pins.Load() != 0 {
				t.Fatal("barrier retained state after settlement")
			}
		})
	}
}

func TestSharedReadBarrierAdmissionSequenceDoesNotWrap(t *testing.T) {
	f := newSharedBarrierFixture(t)
	f.owner.readAdmissionSequence = ^uint64(0)
	d := &readDelivery{reply: make(chan ownerReply, 1)}
	if err := f.owner.publish(ownerRequest{reply: d.reply, read: readRequest{delivery: d}}); !errors.Is(err, ErrIngressFull) {
		t.Fatal(err)
	}
	if len(f.owner.ingress) != 0 || f.owner.ingressItems != 0 || d.admission != 0 {
		t.Fatal("overflow admitted a read")
	}
}
