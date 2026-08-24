package raftserve

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestRegistryPendingListOwnsFirstAttemptIndexZero(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(219)
	waiter, err := registry.Enqueue(
		host, group, encodeTestCommand(t, testCommand(group, 30, 2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(host.tokens) != 1 || host.tokens[0][2] != 0 {
		t.Fatalf("first attempt token = %+v", host.tokens)
	}
	registry.mu.Lock()
	groupIndex, found, findErr := registry.findGroupLocked(group)
	if findErr != nil || !found {
		registry.mu.Unlock()
		t.Fatalf("find index-zero pending group = %v, %v", found, findErr)
	}
	if slot := registry.groupTable[groupIndex]; slot.pendingHead != 0 ||
		slot.pendingAttempts != 1 {
		registry.mu.Unlock()
		t.Fatalf("index-zero pending head = %+v", slot)
	}
	registry.mu.Unlock()
	if err := registry.TerminateGroup(group, multiraft.ProposalGroupRemoved); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	groupIndex, found, findErr = registry.findGroupLocked(group)
	if findErr != nil || !found {
		registry.mu.Unlock()
		t.Fatalf("find terminated index-zero group = %v, %v", found, findErr)
	}
	if slot := registry.groupTable[groupIndex]; slot.pendingHead != noIndex ||
		slot.pendingAttempts != 0 {
		registry.mu.Unlock()
		t.Fatalf("terminated index-zero head = %+v", slot)
	}
	registry.mu.Unlock()
	if outcome, ready, err := waiter.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeProposalAbandoned {
		t.Fatalf("index-zero outcome = %+v, %v, %v", outcome, ready, err)
	}
}

func TestRegistryPendingListUnlinksMiddleSettlementInConstantTime(t *testing.T) {
	registry := testRegistry(t, 3, 3, 3)
	host := &testProposalHost{registry: registry}
	group := testGroup(220)
	commands := [][]byte{
		encodeTestCommand(t, testCommand(group, 31, 2)),
		encodeTestCommand(t, testCommand(group, 32, 2)),
		encodeTestCommand(t, testCommand(group, 33, 2)),
	}
	waiters := make([]Waiter, len(commands))
	for index, data := range commands {
		waiter, err := registry.Enqueue(host, group, data)
		if err != nil {
			t.Fatal(err)
		}
		waiters[index] = waiter
	}
	if len(host.tokens) != len(commands) {
		t.Fatalf("proposal tokens = %d", len(host.tokens))
	}
	for _, token := range host.tokens {
		registry.settleProposalAdmission(multiraft.ProposalAdmission{
			Group: group, Token: token, Admitted: true,
		})
	}

	first := uint32(host.tokens[0][2])
	middle := uint32(host.tokens[1][2])
	head := uint32(host.tokens[2][2])
	registry.mu.Lock()
	groupIndex, found, err := registry.findGroupLocked(group)
	if err != nil || !found {
		registry.mu.Unlock()
		t.Fatalf("find group = %v, %v", found, err)
	}
	slot := &registry.groupTable[groupIndex]
	if slot.pendingAttempts != 3 || slot.pendingHead != head ||
		registry.attempts[head].freeOrPendingNext != middle ||
		registry.attempts[middle].pendingPrevious != head ||
		registry.attempts[middle].freeOrPendingNext != first ||
		registry.attempts[first].pendingPrevious != middle {
		registry.mu.Unlock()
		t.Fatal("admission did not construct the exact intrusive pending chain")
	}
	registry.mu.Unlock()

	batch := newTestAppliedBatch(group, 300, 41, commands[1])
	batch.lookups[0].lookup = testCompletion(t, group, commands[1], 300)
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	groupIndex, found, err = registry.findGroupLocked(group)
	if err != nil || !found {
		registry.mu.Unlock()
		t.Fatalf("find after settlement = %v, %v", found, err)
	}
	slot = &registry.groupTable[groupIndex]
	if slot.pendingAttempts != 2 || slot.pendingHead != head ||
		registry.attempts[head].freeOrPendingNext != first ||
		registry.attempts[first].pendingPrevious != head ||
		registry.attempts[middle].hasFlag(attemptAdmitted) ||
		registry.attempts[middle].freeOrPendingNext != noIndex ||
		registry.attempts[middle].pendingPrevious != noIndex {
		registry.mu.Unlock()
		t.Fatal("middle settlement did not unlink exactly one pending attempt")
	}
	registry.mu.Unlock()

	if err := registry.TerminateGroup(group, multiraft.ProposalGroupLeadershipLost); err != nil {
		t.Fatal(err)
	}
	if stats := registry.Stats(); stats.PendingGroups != 0 || stats.PendingAdmittedAttempts != 0 {
		t.Fatalf("termination stats = %+v", stats)
	}
	for index, waiter := range waiters {
		outcome, ready, err := waiter.Poll()
		if err != nil || !ready {
			t.Fatalf("waiter %d = %+v, %v, %v", index, outcome, ready, err)
		}
		if index == 1 {
			if outcome.Code != OutcomeCompletion {
				t.Fatalf("middle outcome = %+v", outcome)
			}
		} else if outcome.Code != OutcomeNotLeader {
			t.Fatalf("terminated outcome %d = %+v", index, outcome)
		}
		if !waiter.Cancel() {
			t.Fatalf("cancel waiter %d", index)
		}
	}
}

func TestRegistryPendingHeadSurvivesGroupTableBackwardShift(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	registry.hashMask = 0
	groups := [...]raftmember.GroupKey{testGroup(223), testGroup(224)}
	var tokens [2]registrationToken
	var pending Waiter
	for index, group := range groups {
		identity, err := openCommandIdentity(
			group, encodeTestCommand(t, testCommand(group, byte(60+index), 2)),
		)
		if err != nil {
			t.Fatal(err)
		}
		registry.mu.Lock()
		waiter, token, enqueue, registerErr := registry.registerLocked(identity)
		registry.mu.Unlock()
		if registerErr != nil || !enqueue {
			t.Fatalf("register %d = %v, %v", index, registerErr, enqueue)
		}
		tokens[index] = token
		if index == 1 {
			pending = waiter
		}
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: groups[1], Token: tokens[1].proposalToken(), Admitted: true,
	})
	registry.mu.Lock()
	registry.rollbackRegistrationLocked(tokens[0])
	groupIndex, found, findErr := registry.findGroupLocked(groups[1])
	if findErr != nil || !found {
		registry.mu.Unlock()
		t.Fatalf("find shifted group = %v, %v", found, findErr)
	}
	if slot := registry.groupTable[groupIndex]; slot.pendingAttempts != 1 ||
		slot.pendingHead != tokens[1].attempt {
		registry.mu.Unlock()
		t.Fatalf("shifted pending head = %+v", slot)
	}
	registry.mu.Unlock()
	if err := registry.TerminateGroup(
		groups[1], multiraft.ProposalGroupLeadershipLost,
	); err != nil {
		t.Fatal(err)
	}
	if outcome, ready, err := pending.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeNotLeader {
		t.Fatalf("shifted pending outcome = %+v, %v, %v", outcome, ready, err)
	}
}

func TestRegistryPendingTerminationValidatesWholeChainBeforeMutation(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(221)
	for index := byte(0); index < 2; index++ {
		if _, err := registry.Enqueue(
			host, group, encodeTestCommand(t, testCommand(group, 40+index, 2)),
		); err != nil {
			t.Fatal(err)
		}
	}
	head := uint32(host.tokens[1][2])
	tail := uint32(host.tokens[0][2])
	registry.mu.Lock()
	registry.attempts[tail].pendingPrevious = noIndex
	registry.mu.Unlock()
	if err := registry.TerminateGroup(
		group, multiraft.ProposalGroupLeadershipLost,
	); !errors.Is(err, ErrRegistryCorrupt) {
		t.Fatalf("corrupt termination = %v", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	groupIndex, found, findErr := registry.findGroupLocked(group)
	if findErr != nil || !found {
		t.Fatalf("find group = %v, %v", found, findErr)
	}
	slot := registry.groupTable[groupIndex]
	if slot.pendingAttempts != 2 || slot.pendingHead != head {
		t.Fatalf("pending list mutated before validation: %+v", slot)
	}
	for _, attemptIndex := range []uint32{head, tail} {
		attempt := registry.attempts[attemptIndex]
		if !attempt.hasFlag(attemptAdmitted) || attempt.state != attemptPending ||
			attempt.outcome != OutcomePending {
			t.Fatalf("attempt %d partially terminated: %+v", attemptIndex, attempt)
		}
	}
}

func TestRegistryPendingTerminationRemovesWaiterlessEntriesWithoutLosingGroupIndex(t *testing.T) {
	registry := testRegistry(t, 3, 3, 3)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(225)
	for index := byte(0); index < 3; index++ {
		waiter, err := registry.Enqueue(
			host, group, encodeTestCommand(t, testCommand(group, 70+index, 2)),
		)
		if err != nil {
			t.Fatal(err)
		}
		if !waiter.Cancel() {
			t.Fatalf("cancel waiter %d", index)
		}
	}
	if stats := registry.Stats(); stats.PendingAdmittedAttempts != 3 || stats.Waiters != 0 {
		t.Fatalf("waiterless pending setup = %+v", stats)
	}
	if err := registry.TerminateGroup(
		group, multiraft.ProposalGroupRemoved,
	); err != nil {
		t.Fatal(err)
	}
	if stats := registry.Stats(); stats.OutstandingGroups != 0 ||
		stats.OutstandingIdentities != 0 || stats.OutstandingAttempts != 0 ||
		stats.PendingGroups != 0 || stats.PendingAdmittedAttempts != 0 {
		t.Fatalf("waiterless termination retained state = %+v", stats)
	}
}

func TestRegistryPendingTerminationUnlinksOlderWaiterlessAttemptInConstantTime(t *testing.T) {
	registry := testRegistry(t, 1, 2, 2)
	group := testGroup(227)
	identity, err := openCommandIdentity(
		group, encodeTestCommand(t, testCommand(group, 81, 2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	register := func(candidate commandIdentity) (Waiter, registrationToken) {
		registry.mu.Lock()
		waiter, token, enqueue, registerErr := registry.registerLocked(candidate)
		registry.mu.Unlock()
		if registerErr != nil || !enqueue {
			t.Fatalf("register = %v, %v", registerErr, enqueue)
		}
		return waiter, token
	}
	pendingWaiter, pendingToken := register(identity)
	if !pendingWaiter.Cancel() {
		t.Fatal("cancel pending waiter")
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: pendingToken.proposalToken(), Admitted: true,
	})
	newerIdentity := identity
	newerIdentity.attempt[0] ^= 0xff
	newerWaiter, newerToken := register(newerIdentity)
	if !newerWaiter.Cancel() {
		t.Fatal("cancel newer nonpending waiter")
	}
	if err := registry.TerminateGroup(
		group, multiraft.ProposalGroupLeadershipLost,
	); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	newer := &registry.attempts[newerToken.attempt]
	entry := &registry.entries[newerToken.entry]
	if registry.attempts[pendingToken.attempt].hasFlag(attemptActive) ||
		!newer.hasFlag(attemptActive) || newer.entryPrevious != noIndex ||
		newer.next != noIndex || entry.attemptHead != newerToken.attempt ||
		entry.attemptCount != 1 {
		registry.mu.Unlock()
		t.Fatalf("older waiterless unlink = pending %+v newer %+v entry %+v",
			registry.attempts[pendingToken.attempt], *newer, *entry)
	}
	registry.rollbackRegistrationLocked(newerToken)
	registry.mu.Unlock()
	if stats := registry.Stats(); stats.OutstandingGroups != 0 ||
		stats.OutstandingIdentities != 0 || stats.OutstandingAttempts != 0 ||
		stats.Waiters != 0 || stats.PendingGroups != 0 ||
		stats.PendingAdmittedAttempts != 0 {
		t.Fatalf("constant-time waiterless cleanup = %+v", stats)
	}
}

func TestRegistryPendingTerminationValidatesWaiterlessEntryLinksBeforeMutation(t *testing.T) {
	registry := testRegistry(t, 1, 2, 2)
	group := testGroup(226)
	identity, err := openCommandIdentity(
		group, encodeTestCommand(t, testCommand(group, 80, 2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	register := func(candidate commandIdentity) (Waiter, registrationToken) {
		registry.mu.Lock()
		waiter, token, enqueue, registerErr := registry.registerLocked(candidate)
		registry.mu.Unlock()
		if registerErr != nil || !enqueue {
			t.Fatalf("register = %v, %v", registerErr, enqueue)
		}
		return waiter, token
	}

	pendingWaiter, pendingToken := register(identity)
	if !pendingWaiter.Cancel() {
		t.Fatal("cancel pending waiter")
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: pendingToken.proposalToken(), Admitted: true,
	})
	newerIdentity := identity
	newerIdentity.attempt[0] ^= 0xff
	newerWaiter, newerToken := register(newerIdentity)
	if !newerWaiter.Cancel() {
		t.Fatal("cancel newer nonpending waiter")
	}

	registry.mu.Lock()
	pending := &registry.attempts[pendingToken.attempt]
	newer := &registry.attempts[newerToken.attempt]
	if pending.entryPrevious != newerToken.attempt || newer.next != pendingToken.attempt {
		registry.mu.Unlock()
		t.Fatalf("entry attempt links = pending %+v newer %+v", *pending, *newer)
	}
	newer.next = noIndex
	registry.mu.Unlock()

	if err := registry.TerminateGroup(
		group, multiraft.ProposalGroupLeadershipLost,
	); !errors.Is(err, ErrRegistryCorrupt) {
		t.Fatalf("corrupt waiterless termination = %v", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	groupIndex, found, findErr := registry.findGroupLocked(group)
	if findErr != nil || !found {
		t.Fatalf("find group = %v, %v", found, findErr)
	}
	slot := registry.groupTable[groupIndex]
	pending = &registry.attempts[pendingToken.attempt]
	if slot.pendingAttempts != 1 || slot.pendingHead != pendingToken.attempt ||
		!pending.hasFlag(attemptAdmitted) || pending.state != attemptPending ||
		pending.outcome != OutcomePending {
		t.Fatalf("waiterless pending attempt mutated before entry validation: slot %+v attempt %+v", slot, *pending)
	}
}

func TestRegistryPendingTerminationDoesNotInspectSameGroupNonpendingAttempts(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	group := testGroup(222)
	delayed := &delayedProposalHost{registry: registry}
	nonpending, err := registry.Enqueue(
		delayed, group, encodeTestCommand(t, testCommand(group, 51, 2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !nonpending.Cancel() {
		t.Fatal("cancel nonpending waiter")
	}
	nonpendingAttempt := uint32(delayed.token[2])

	hotHost := &testProposalHost{registry: registry, admit: true}
	hot, err := registry.Enqueue(
		hotHost, group, encodeTestCommand(t, testCommand(group, 52, 2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	registry.attempts[nonpendingAttempt].next = nonpendingAttempt
	registry.mu.Unlock()
	if err := registry.TerminateGroup(group, multiraft.ProposalGroupLeadershipLost); err != nil {
		t.Fatalf("termination inspected nonpending entry chain: %v", err)
	}
	outcome, ready, err := hot.Poll()
	if err != nil || !ready || outcome.Code != OutcomeNotLeader {
		t.Fatalf("hot outcome = %+v, %v, %v", outcome, ready, err)
	}
	if registry.failure != nil {
		t.Fatalf("nonpending corruption was traversed: %v", registry.failure)
	}
}
