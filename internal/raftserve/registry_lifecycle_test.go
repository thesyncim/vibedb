package raftserve

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type delayedProposalHost struct {
	registry *Registry
	calls    int
	token    multiraft.ProposalToken
}

func (host *delayedProposalHost) EnqueueTrackedProposal(
	_ raftmember.GroupKey,
	_ []byte,
	token multiraft.ProposalToken,
) error {
	host.calls++
	host.token = token
	return nil
}

func TestRegistryCancellationRetainsUnresolvedAndAdmittedAttempt(t *testing.T) {
	registry := testRegistry(t, 1, 1, 3)
	host := &delayedProposalHost{registry: registry}
	group := testGroup(31)
	data := encodeTestCommand(t, testCommand(group, 3, 2))
	first, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if host.calls != 1 || !first.Cancel() {
		t.Fatalf("first = calls %d cancel %v", host.calls, first.Cancel())
	}
	if got := registry.Stats(); got.OutstandingAttempts != 1 || got.Waiters != 0 {
		t.Fatalf("unresolved cancellation = %+v", got)
	}
	second, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if host.calls != 1 {
		t.Fatalf("unresolved retry amplified Host queue: %d", host.calls)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: host.token, Admitted: true,
	})
	if !second.Cancel() {
		t.Fatal("second cancel failed")
	}
	if got := registry.Stats(); got.OutstandingAttempts != 1 || got.Waiters != 0 {
		t.Fatalf("admitted cancellation = %+v", got)
	}
	third, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if host.calls != 1 || !third.Cancel() {
		t.Fatalf("admitted retry = calls %d cancel %v", host.calls, third.Cancel())
	}

	batch := newTestAppliedBatch(group, 80, 12, data)
	batch.lookups[0].lookup = testCompletion(t, group, data, 80)
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.OutstandingAttempts != 0 ||
		got.OutstandingIdentities != 0 || got.Waiters != 0 {
		t.Fatalf("no-waiter settlement retained state = %+v", got)
	}
}

func TestRegistryCancelBeforeHostLifecycleReturnKeepsOneAttempt(t *testing.T) {
	registry := testRegistry(t, 1, 1, 2)
	group := testGroup(32)
	data := encodeTestCommand(t, testCommand(group, 4, 2))
	identity, err := openCommandIdentity(group, data)
	if err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	first, token, enqueue, err := registry.registerLocked(identity)
	registry.mu.Unlock()
	if err != nil || !enqueue {
		t.Fatalf("register = %+v, %v, %v", first, enqueue, err)
	}
	if !first.Cancel() {
		t.Fatal("cancel before Host return failed")
	}
	registry.mu.Lock()
	second, _, secondEnqueue, err := registry.registerLocked(identity)
	registry.mu.Unlock()
	if err != nil || secondEnqueue {
		t.Fatalf("retry registration = %+v, enqueue %v, %v", second, secondEnqueue, err)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: token.proposalToken(), Admitted: true,
	})
	if got := registry.Stats(); got.OutstandingAttempts != 1 || got.Waiters != 1 {
		t.Fatalf("post-admission state = %+v", got)
	}
	second.Cancel()
}

func TestRegistryProposalRefusalAndMalformedLifecycleNeverHang(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	host := &delayedProposalHost{registry: registry}
	group := testGroup(33)
	data := encodeTestCommand(t, testCommand(group, 5, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: host.token, Cause: multiraft.ErrQueueFull,
	})
	outcome, ready, err := waiter.Poll()
	if err != nil || !ready || outcome.Code != OutcomeProposalRefused ||
		!errors.Is(outcome.Err(), ErrProposalRefused) {
		t.Fatalf("refusal = %+v, %v, %v", outcome, ready, err)
	}
	waiter.Cancel()

	other := encodeTestCommand(t, testCommand(group, 6, 2))
	waiter, err = registry.Enqueue(host, group, other)
	if err != nil {
		t.Fatal(err)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: multiraft.ProposalToken{999, 1, 999, 1}, Admitted: true,
	})
	if _, _, err := waiter.Poll(); !errors.Is(err, ErrRegistryCorrupt) {
		t.Fatalf("poisoned waiter = %v", err)
	}
	if _, err := registry.Enqueue(host, group, other); !errors.Is(err, ErrRegistryCorrupt) {
		t.Fatalf("poisoned enqueue = %v", err)
	}
	batch := newTestAppliedBatch(group, 90, 13, other)
	if err := settleAppliedBatch(registry, batch); !errors.Is(err, ErrRegistryCorrupt) {
		t.Fatalf("poisoned settlement = %v", err)
	}
}

func TestRegistryProposalAdmissionReturnsClosedDeterministicRefusal(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	host := &delayedProposalHost{registry: registry}
	group := testGroup(42)
	data := encodeTestCommand(t, testCommand(group, 15, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: host.token, Cause: replicatedstate.ErrRetryRetired,
	})
	outcome, ready, err := waiter.Poll()
	if err != nil || !ready || outcome.Code != OutcomeRetryRetired ||
		!errors.Is(outcome.Err(), replicatedstate.ErrRetryRetired) ||
		outcome.AppliedIndex != 0 || outcome.CompletionBytes != 0 {
		t.Fatalf("deterministic pre-proposal refusal = %+v, %v, %v",
			outcome, ready, err)
	}
}

func TestRegistryDuplicateAndMalformedLifecyclePoisonWaiters(t *testing.T) {
	for _, test := range []struct {
		name string
		fire func(*Registry, raftmember.GroupKey, multiraft.ProposalToken)
	}{
		{
			name: "duplicate",
			fire: func(registry *Registry, group raftmember.GroupKey, token multiraft.ProposalToken) {
				admission := multiraft.ProposalAdmission{Group: group, Token: token, Admitted: true}
				registry.settleProposalAdmission(admission)
				registry.settleProposalAdmission(admission)
			},
		},
		{
			name: "admitted-with-cause",
			fire: func(registry *Registry, group raftmember.GroupKey, token multiraft.ProposalToken) {
				registry.settleProposalAdmission(multiraft.ProposalAdmission{
					Group: group, Token: token, Admitted: true, Cause: errors.New("impossible"),
				})
			},
		},
		{
			name: "refused-without-cause",
			fire: func(registry *Registry, group raftmember.GroupKey, token multiraft.ProposalToken) {
				registry.settleProposalAdmission(multiraft.ProposalAdmission{
					Group: group, Token: token,
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := testRegistry(t, 1, 1, 1)
			host := &delayedProposalHost{registry: registry}
			group := testGroup(37)
			data := encodeTestCommand(t, testCommand(group, 9, 2))
			waiter, err := registry.Enqueue(host, group, data)
			if err != nil {
				t.Fatal(err)
			}
			test.fire(registry, group, host.token)
			if _, _, err := waiter.Poll(); !errors.Is(err, ErrRegistryCorrupt) {
				t.Fatalf("waiter = %v", err)
			}
		})
	}
}

func TestWaitContextSingleClaimCancellationAndCompletionRace(t *testing.T) {
	registry := testRegistry(t, 1, 1, 2)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(34)
	data := encodeTestCommand(t, testCommand(group, 7, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := waiter.Wait(nil); !errors.Is(err, ErrWaitContext) {
		t.Fatalf("nil context = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, waitErr := waiter.Wait(ctx)
		done <- waitErr
	}()
	for {
		registry.mu.Lock()
		blocking := registry.waiters[waiter.index].blocking
		registry.mu.Unlock()
		if blocking {
			break
		}
	}
	if _, err := waiter.Wait(context.Background()); !errors.Is(err, ErrWaiterBusy) {
		t.Fatalf("second blocking claimant = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait = %v", err)
	}
	if waiter.Cancel() {
		t.Fatal("context cancellation did not release waiter")
	}

	completed, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(group, 100, 14, data)
	batch.lookups[0].lookup = testCompletion(t, group, data, 100)
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	readyContext, readyCancel := context.WithCancel(context.Background())
	readyCancel()
	outcome, err := completed.Wait(readyContext)
	if err != nil || outcome.Code != OutcomeCompletion || outcome.AppliedIndex != 100 {
		t.Fatalf("completion-before-cancel = %+v, %v", outcome, err)
	}
	completed.Cancel()
}

func TestRegistryCloseWakesWaitersClearsBoundsAndFailsClosed(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(35)
	data := encodeTestCommand(t, testCommand(group, 8, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	done := make(chan error, 1)
	go func() {
		defer waitGroup.Done()
		_, waitErr := waiter.Wait(ctx)
		done <- waitErr
	}()
	for {
		registry.mu.Lock()
		blocking := registry.waiters[waiter.index].blocking
		registry.mu.Unlock()
		if blocking {
			break
		}
	}
	for index := range registry.tenantArena {
		registry.tenantArena[index] ^= byte(index + 1)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	waitGroup.Wait()
	if err := <-done; !errors.Is(err, ErrWaiterClosed) {
		t.Fatalf("closed wait = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.OutstandingIdentities != 0 ||
		got.OutstandingGroups != 0 || got.OutstandingAttempts != 0 ||
		got.Waiters != 0 || got.RetainedCompletionBytes != 0 ||
		got.PendingGroups != 0 || got.PendingAdmittedAttempts != 0 {
		t.Fatalf("closed stats = %+v", got)
	}
	for _, value := range registry.tenantArena {
		if value != 0 {
			t.Fatal("Close retained tenant bytes")
		}
	}
	if _, err := registry.Enqueue(host, group, data); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed enqueue = %v", err)
	}
	if _, err := registry.NewHost(multiraft.Limits{}); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed NewHost = %v", err)
	}
	batch := newTestAppliedBatch(group, 110, 15, data)
	if err := settleAppliedBatch(registry, batch); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed settlement = %v", err)
	}
}

func TestRegistryBackwardShiftKeepsProbeCostBoundedByLiveCollisions(t *testing.T) {
	registry := testRegistry(t, 8, 8, 8)
	registry.hashMask = 0
	group := testGroup(36)
	type live struct {
		identity commandIdentity
		token    registrationToken
	}
	liveEntries := make([]live, 0, 4)
	for index := 0; index < 4; index++ {
		identity, err := openCommandIdentity(
			group, encodeTestCommand(t, testCommand(group, byte(index+1), 2)),
		)
		if err != nil {
			t.Fatal(err)
		}
		registry.mu.Lock()
		_, token, _, registerErr := registry.registerLocked(identity)
		registry.mu.Unlock()
		if registerErr != nil {
			t.Fatal(registerErr)
		}
		liveEntries = append(liveEntries, live{identity: identity, token: token})
	}
	for iteration := 0; iteration < 2000; iteration++ {
		remove := liveEntries[iteration%len(liveEntries)]
		registry.mu.Lock()
		registry.rollbackRegistrationLocked(remove.token)
		registry.mu.Unlock()
		identity, err := openCommandIdentity(
			group, encodeTestCommand(t, testCommand(group, byte(iteration%240+10), uint64(iteration/240+3))),
		)
		if err != nil {
			t.Fatal(err)
		}
		registry.mu.Lock()
		_, token, _, registerErr := registry.registerLocked(identity)
		registry.mu.Unlock()
		if registerErr != nil {
			t.Fatal(registerErr)
		}
		liveEntries[iteration%len(liveEntries)] = live{identity: identity, token: token}
		if registry.Stats().LastLookupProbes > len(liveEntries)+1 {
			t.Fatalf("historical churn inflated probes: %+v", registry.Stats())
		}
	}
	occupied := 0
	for _, slot := range registry.table {
		if slot.state == tableOccupied {
			occupied++
		}
	}
	if occupied != len(liveEntries) {
		t.Fatalf("occupied slots = %d, want %d", occupied, len(liveEntries))
	}
}

func TestRegistryGroupTableBackwardShiftPreservesExactCollidingGroups(t *testing.T) {
	registry := testRegistry(t, 3, 3, 3)
	registry.hashMask = 0
	var tokens [3]registrationToken
	var groups [3]raftmember.GroupKey
	for index := range groups {
		groups[index] = testGroup(byte(50 + index))
		identity, err := openCommandIdentity(
			groups[index],
			encodeTestCommand(t, testCommand(groups[index], byte(index+20), 2)),
		)
		if err != nil {
			t.Fatal(err)
		}
		registry.mu.Lock()
		_, token, enqueue, registerErr := registry.registerLocked(identity)
		registry.mu.Unlock()
		if registerErr != nil || !enqueue {
			t.Fatalf("register colliding group %d = %v, enqueue %v",
				index, registerErr, enqueue)
		}
		tokens[index] = token
	}
	registry.mu.Lock()
	registry.rollbackRegistrationLocked(tokens[1])
	_, found, findErr := registry.findGroupLocked(groups[2])
	registry.mu.Unlock()
	if findErr != nil || !found {
		t.Fatalf("group after backward shift = found %v, %v", found, findErr)
	}
	if got := registry.Stats(); got.OutstandingGroups != 2 {
		t.Fatalf("colliding group deletion = %+v", got)
	}
	registry.mu.Lock()
	registry.rollbackRegistrationLocked(tokens[0])
	registry.rollbackRegistrationLocked(tokens[2])
	registry.mu.Unlock()
	if got := registry.Stats(); got.OutstandingGroups != 0 {
		t.Fatalf("colliding group cleanup = %+v", got)
	}
}

func TestRegistryGroupEntryLinksSurviveCollisionBackshiftAndUnlink(t *testing.T) {
	registry := testRegistry(t, 5, 5, 5)
	registry.hashMask = 0
	groups := [...]raftmember.GroupKey{
		testGroup(71), testGroup(72), testGroup(73),
	}
	register := func(group raftmember.GroupKey, client byte) registrationToken {
		t.Helper()
		identity, err := openCommandIdentity(
			group, encodeTestCommand(t, testCommand(group, client, 2)),
		)
		if err != nil {
			t.Fatal(err)
		}
		registry.mu.Lock()
		_, token, enqueue, registerErr := registry.registerLocked(identity)
		registry.mu.Unlock()
		if registerErr != nil || !enqueue {
			t.Fatalf("register group %x client %d = %v, enqueue %v",
				group.GroupID, client, registerErr, enqueue)
		}
		return token
	}
	rollback := func(token registrationToken) {
		t.Helper()
		registry.mu.Lock()
		registry.rollbackRegistrationLocked(token)
		registry.mu.Unlock()
	}
	assertGroupLinks := func(
		group raftmember.GroupKey,
		want ...registrationToken,
	) {
		t.Helper()
		registry.mu.Lock()
		defer registry.mu.Unlock()
		groupIndex, found, err := registry.findGroupLocked(group)
		if err != nil || !found {
			t.Fatalf("find group = %v, found %v", err, found)
		}
		slot := &registry.groupTable[groupIndex]
		if slot.identityCount != uint32(len(want)) {
			t.Fatalf("group identities = %d, want %d", slot.identityCount, len(want))
		}
		entryIndex := slot.entryHead
		previous := noIndex
		for offset, token := range want {
			if entryIndex != token.entry || int(entryIndex) >= len(registry.entries) {
				t.Fatalf("group link %d = %d, want %d", offset, entryIndex, token.entry)
			}
			entry := &registry.entries[entryIndex]
			if !entry.active || entry.position.group != group ||
				entry.groupPrevious != previous {
				t.Fatalf("group link %d = %+v", offset, *entry)
			}
			previous = entryIndex
			entryIndex = entry.freeOrGroupNext
		}
		if entryIndex != noIndex {
			t.Fatalf("group chain has unexpected tail %d", entryIndex)
		}
	}

	// Put the target group after another colliding group so deleting the first
	// slot backward-shifts the target slot and its intrusive head.
	before := register(groups[0], 1)
	tail := register(groups[1], 2)
	middle := register(groups[1], 3)
	head := register(groups[1], 4)
	after := register(groups[2], 5)
	assertGroupLinks(groups[1], head, middle, tail)

	rollback(before)
	assertGroupLinks(groups[1], head, middle, tail)
	registry.mu.Lock()
	groupIndex, found, err := registry.findGroupLocked(groups[2])
	registry.mu.Unlock()
	if err != nil || !found || groupIndex != 1 {
		t.Fatalf("second shifted colliding group = slot %d, found %v, %v",
			groupIndex, found, err)
	}

	rollback(middle)
	assertGroupLinks(groups[1], head, tail)
	for _, token := range [...]registrationToken{head, tail} {
		registry.settleProposalAdmission(multiraft.ProposalAdmission{
			Group: groups[1], Token: token.proposalToken(), Admitted: true,
		})
	}
	headWaiter := Waiter{
		registry: registry, index: head.waiter, generation: head.waiterGeneration,
	}
	if !headWaiter.Cancel() {
		t.Fatal("cancel admitted head waiter")
	}
	if err := registry.TerminateGroup(
		groups[1], multiraft.ProposalGroupLeadershipLost,
	); err != nil {
		t.Fatal(err)
	}
	assertGroupLinks(groups[1], tail)
	tailWaiter := Waiter{
		registry: registry, index: tail.waiter, generation: tail.waiterGeneration,
	}
	if _, outcome, takeErr := tailWaiter.TakeCompletionInto(nil); takeErr != nil ||
		outcome.Code != OutcomeNotLeader {
		t.Fatalf("terminated linked waiter = %+v, %v", outcome, takeErr)
	}
	registry.mu.Lock()
	_, found, err = registry.findGroupLocked(groups[1])
	registry.mu.Unlock()
	if err != nil || found {
		t.Fatalf("empty group remained = found %v, %v", found, err)
	}
	afterWaiter := Waiter{
		registry: registry, index: after.waiter, generation: after.waiterGeneration,
	}
	if _, ready, pollErr := afterWaiter.Poll(); pollErr != nil || ready {
		t.Fatalf("unrelated shifted group changed = ready %v, %v", ready, pollErr)
	}
	rollback(after)
	if got := registry.Stats(); got.OutstandingGroups != 0 ||
		got.OutstandingIdentities != 0 || got.OutstandingAttempts != 0 ||
		got.Waiters != 0 {
		t.Fatalf("collision unlink cleanup = %+v", got)
	}
}

func TestRegistryGenerationExhaustionFailsClosedWithoutSkippingFreeSlots(t *testing.T) {
	for _, test := range []struct {
		name    string
		exhaust func(*Registry)
	}{
		{
			name: "identity",
			exhaust: func(registry *Registry) {
				registry.entries[registry.freeEntry].generation = math.MaxUint64
			},
		},
		{
			name: "attempt",
			exhaust: func(registry *Registry) {
				registry.attempts[registry.freeAttempt].generation = math.MaxUint64
			},
		},
		{
			name: "waiter",
			exhaust: func(registry *Registry) {
				registry.waiters[registry.freeWaiter].generation = math.MaxUint64
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := testRegistry(t, 2, 2, 2)
			host := &delayedProposalHost{registry: registry}
			group := testGroup(38)
			data := encodeTestCommand(t, testCommand(group, 10, 2))
			test.exhaust(registry)

			if waiter, err := registry.Enqueue(host, group, data); waiter != (Waiter{}) ||
				!errors.Is(err, ErrGenerationExhausted) ||
				!errors.Is(err, ErrRegistryCorrupt) {
				t.Fatalf("generation exhaustion = %+v, %v", waiter, err)
			}
			if host.calls != 0 {
				t.Fatalf("generation exhaustion reached Host: %d", host.calls)
			}
			if got := registry.Stats(); got.OutstandingIdentities != 0 ||
				got.OutstandingGroups != 0 || got.OutstandingAttempts != 0 ||
				got.Waiters != 0 || got.PendingAdmittedAttempts != 0 {
				t.Fatalf("generation exhaustion retained live state = %+v", got)
			}
			if _, err := registry.Enqueue(host, group, data); !errors.Is(err, ErrGenerationExhausted) ||
				!errors.Is(err, ErrRegistryCorrupt) {
				t.Fatalf("later enqueue did not fail closed: %v", err)
			}
		})
	}
}

func TestRegistryGroupTerminationReleasesAdmittedAttemptsAndIgnoresLateApply(t *testing.T) {
	registry := testRegistry(t, 1, 1, 2)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(39)
	data := encodeTestCommand(t, testCommand(group, 11, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.TerminateGroup(group, multiraft.ProposalGroupLeadershipLost); err != nil {
		t.Fatal(err)
	}
	outcome, ready, err := waiter.Poll()
	if err != nil || !ready || outcome.Code != OutcomeNotLeader ||
		!errors.Is(outcome.Err(), raftmodel.ErrNotLeader) {
		t.Fatalf("leadership loss = %+v, %v, %v", outcome, ready, err)
	}
	late := newTestAppliedBatch(group, 160, 20, data)
	late.lookups[0].lookup = testCompletion(t, group, data, 160)
	if err := settleAppliedBatch(registry, late); err != nil {
		t.Fatalf("late apply poisoned terminal attempt: %v", err)
	}
	if outcome, ready, err := waiter.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeNotLeader {
		t.Fatalf("late apply replaced infrastructure outcome = %+v, %v, %v", outcome, ready, err)
	}
	if result, outcome, err := waiter.TakeCompletionInto(nil); err != nil ||
		len(result) != 0 || outcome.Code != OutcomeNotLeader {
		t.Fatalf("take leadership outcome = %dB, %+v, %v", len(result), outcome, err)
	}

	retry, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(host.tokens) != 2 {
		t.Fatalf("consumed terminal retry did not enqueue again: %d", len(host.tokens))
	}
	if err := registry.TerminateGroup(group, multiraft.ProposalGroupRemoved); err != nil {
		t.Fatal(err)
	}
	if outcome, ready, err := retry.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeProposalAbandoned ||
		!errors.Is(outcome.Err(), ErrProposalAbandoned) {
		t.Fatalf("removed group outcome = %+v, %v, %v", outcome, ready, err)
	}
	abandonedLate := newTestAppliedBatch(group, 161, 21, data)
	abandonedLate.lookups[0].lookup = testCompletion(t, group, data, 161)
	if err := settleAppliedBatch(registry, abandonedLate); err != nil {
		t.Fatalf("late apply poisoned abandoned attempt: %v", err)
	}
	if outcome, ready, err := retry.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeProposalAbandoned {
		t.Fatalf("late apply replaced abandoned outcome = %+v, %v, %v",
			outcome, ready, err)
	}
	if _, _, err := retry.TakeCompletionInto(nil); err != nil {
		t.Fatal(err)
	}
	zeroWaiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if !zeroWaiter.Cancel() {
		t.Fatal("zero-waiter cancellation failed")
	}
	if got := registry.Stats(); got.OutstandingAttempts != 1 || got.Waiters != 0 {
		t.Fatalf("cancelled admitted retry = %+v", got)
	}
	if err := registry.TerminateGroup(group, multiraft.ProposalHostClosed); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.OutstandingIdentities != 0 ||
		got.OutstandingAttempts != 0 || got.Waiters != 0 {
		t.Fatalf("zero-waiter group termination retained state = %+v", got)
	}
}

func TestRegistryGroupCapacityAndPendingSignalHaveExactLiveCounts(t *testing.T) {
	registry, err := NewRegistry(Limits{
		MaxGroups:                  1,
		MaxOutstandingIdentities:   2,
		MaxOutstandingAttempts:     2,
		MaxWaiters:                 2,
		MaxAttemptsPerIdentity:     2,
		MaxRetainedCompletionBytes: 2 * completionSlotBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	host := &testProposalHost{registry: registry, admit: true}
	firstGroup := testGroup(40)
	firstData := encodeTestCommand(t, testCommand(firstGroup, 13, 2))
	waiter, err := registry.Enqueue(host, firstGroup, firstData)
	if err != nil {
		t.Fatal(err)
	}
	if !registry.hasPendingGroup(firstGroup) {
		t.Fatal("admitted attempt was absent from the pending-group signal")
	}
	if got := registry.Stats(); got.OutstandingGroups != 1 ||
		got.PendingGroups != 1 || got.PendingAdmittedAttempts != 1 {
		t.Fatalf("admitted group stats = %+v", got)
	}

	secondGroup := testGroup(41)
	secondData := encodeTestCommand(t, testCommand(secondGroup, 14, 2))
	if secondWaiter, err := registry.Enqueue(host, secondGroup, secondData); secondWaiter != (Waiter{}) || !errors.Is(err, ErrGroupCapacity) {
		t.Fatalf("second group beyond bound = %+v, %v", secondWaiter, err)
	}
	if len(host.tokens) != 1 {
		t.Fatalf("group-capacity rejection reached Host: %d", len(host.tokens))
	}

	batch := newTestAppliedBatch(firstGroup, 170, 22, firstData)
	batch.lookups[0].lookup = testCompletion(t, firstGroup, firstData, 170)
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	if registry.hasPendingGroup(firstGroup) {
		t.Fatal("settled attempt retained the pending-group signal")
	}
	if got := registry.Stats(); got.OutstandingGroups != 1 ||
		got.PendingGroups != 0 || got.PendingAdmittedAttempts != 0 {
		t.Fatalf("settled retained-waiter group stats = %+v", got)
	}
	if _, _, err := waiter.TakeCompletionInto(make([]byte, 0, completionSlotBytes)); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.OutstandingGroups != 0 ||
		got.OutstandingIdentities != 0 {
		t.Fatalf("consumed group retained capacity = %+v", got)
	}
	if secondWaiter, err := registry.Enqueue(host, secondGroup, secondData); err != nil {
		t.Fatal(err)
	} else if !secondWaiter.Cancel() {
		t.Fatal("reused group capacity waiter cancellation failed")
	}
	if err := registry.TerminateGroup(
		secondGroup, multiraft.ProposalGroupRemoved,
	); err != nil {
		t.Fatal(err)
	}
}
