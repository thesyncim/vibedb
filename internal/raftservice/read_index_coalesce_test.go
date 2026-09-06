package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

type countingReadIndexHost struct {
	ownerHost
	calls    int
	contexts [][16]byte
	status   raftmember.RuntimeStatus
}

func (host *countingReadIndexHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *countingReadIndexHost) ReadIndex(_ raftmember.GroupKey, context []byte) error {
	host.calls++
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
		host: host,
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

func (fixture *sharedBarrierFixture) read(t *testing.T, force bool) *readDelivery {
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
	if err := fixture.owner.handle(request); err != nil {
		t.Fatalf("handle linear read: %v", err)
	}
	return delivery
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
	primary := fixture.read(t, false)
	waiterOne := fixture.read(t, false)
	waiterTwo := fixture.read(t, false)
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
	primary := fixture.read(t, false)
	member := fixture.owner.members[fixture.group]
	member.generation = &ownerGeneration{}
	fixture.owner.members[fixture.group] = member
	other := fixture.read(t, false)
	if fixture.host.calls != 2 {
		t.Fatalf("ReadIndex calls=%d want separate round per generation", fixture.host.calls)
	}
	fixture.settleAll(t, nil)
	settleDeliveryReply(t, primary, 9)
	settleDeliveryReply(t, other, 9)
}

func TestSharedReadBarrierForceReadIndexStaysSolo(t *testing.T) {
	fixture := newSharedBarrierFixture(t)
	primary := fixture.read(t, false)
	forced := fixture.read(t, true)
	if fixture.host.calls != 2 {
		t.Fatalf("ReadIndex calls=%d want forced retry solo", fixture.host.calls)
	}
	fixture.settleAll(t, nil)
	settleDeliveryReply(t, primary, 9)
	settleDeliveryReply(t, forced, 9)
}

func TestSharedReadBarrierErrorFansOut(t *testing.T) {
	fixture := newSharedBarrierFixture(t)
	primary := fixture.read(t, false)
	waiter := fixture.read(t, false)
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
