package raftserve

import (
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

func testAppliedSourceOwner(group raftmember.GroupKey) raftmember.AppliedSourceOwner {
	return raftmember.AppliedSourceOwner{
		Group: group, AllocationGeneration: 7, MemberID: 9,
		StoreID: [16]byte{1}, NodeIncarnation: 11,
	}
}

func TestRegistryAttemptIdentityIncludesAppliedSourceEpoch(t *testing.T) {
	registry := testRegistry(t, 2, 4, 4)
	group := testGroup(211)
	firstOwner := testAppliedSourceOwner(group)
	firstToken, err := registry.claimAppliedSource(firstOwner)
	if err != nil {
		t.Fatal(err)
	}
	host := &delayedProposalHost{registry: registry}
	data := encodeTestCommand(t, testCommand(group, 17, 2))
	first, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	firstProposal := host.token
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, SourceOwner: firstOwner, SourceToken: firstToken,
		Token: firstProposal, Cause: ErrProposalRefused,
	})
	if err := registry.releaseAppliedSource(firstOwner, firstToken); err != nil {
		t.Fatal(err)
	}

	secondOwner := firstOwner
	secondOwner.NodeIncarnation++
	secondToken, err := registry.claimAppliedSource(secondOwner)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if host.calls != 2 || firstProposal == host.token {
		t.Fatalf("replacement registration reused old attempt: calls %d tokens %+v %+v",
			host.calls, firstProposal, host.token)
	}
	if stats := registry.Stats(); stats.OutstandingAttempts != 2 || stats.Waiters != 2 {
		t.Fatalf("source-scoped attempts = %+v", stats)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, SourceOwner: secondOwner, SourceToken: secondToken,
		Token: host.token, Cause: ErrProposalRefused,
	})
	if !first.Cancel() || !second.Cancel() {
		t.Fatal("source-scoped waiter cleanup failed")
	}
	if err := registry.releaseAppliedSource(secondOwner, secondToken); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryAppliedSourceClaimRejectsUnresolvedProposalLifecycle(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	group := testGroup(212)
	identity, err := openCommandIdentity(
		group, encodeTestCommand(t, testCommand(group, 18, 2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	_, registration, enqueue, registerErr := registry.registerLocked(identity)
	registry.mu.Unlock()
	if registerErr != nil || !enqueue {
		t.Fatalf("register unresolved lifecycle = %v, %v", registerErr, enqueue)
	}
	owner := testAppliedSourceOwner(group)
	if _, err := registry.claimAppliedSource(owner); !errors.Is(err, ErrSourceOwnersLive) {
		t.Fatalf("claim with unresolved lifecycle = %v", err)
	}
	registry.mu.Lock()
	registry.rollbackRegistrationLocked(registration)
	registry.mu.Unlock()
	token, err := registry.claimAppliedSource(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.releaseAppliedSource(owner, token); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryOwnedEmptyGroupTerminationPreservesSourceOwner(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	group := testGroup(213)
	owner := testAppliedSourceOwner(group)
	token, err := registry.claimAppliedSource(owner)
	if err != nil {
		t.Fatal(err)
	}

	registry.settleProposalGroupTermination(multiraft.ProposalGroupTermination{
		Group: group, SourceOwner: owner, SourceToken: token,
		Reason: multiraft.ProposalHostClosed,
	})
	if stats := registry.Stats(); stats.LiveSourceOwners != 1 ||
		stats.OutstandingGroups != 1 || stats.OutstandingIdentities != 0 ||
		stats.PendingGroups != 0 || stats.PendingAdmittedAttempts != 0 {
		t.Fatalf("owned empty termination stats = %+v", stats)
	}
	if err := registry.releaseAppliedSource(owner, token); err != nil {
		t.Fatalf("release after owned empty termination = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryOwnedTerminationRejectsZeroIdentityPendingAttempt(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	group := testGroup(214)
	owner := testAppliedSourceOwner(group)
	token, err := registry.claimAppliedSource(owner)
	if err != nil {
		t.Fatal(err)
	}
	host := &delayedProposalHost{registry: registry}
	data := encodeTestCommand(t, testCommand(group, 19, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, SourceOwner: owner, SourceToken: token,
		Token: host.token, Admitted: true,
	})
	if stats := registry.Stats(); stats.OutstandingIdentities != 1 ||
		stats.PendingGroups != 1 || stats.PendingAdmittedAttempts != 1 {
		t.Fatalf("admitted owner stats = %+v", stats)
	}

	registry.mu.Lock()
	groupIndex, found, findErr := registry.findGroupLocked(group)
	if findErr != nil || !found {
		registry.mu.Unlock()
		t.Fatalf("find owned group = %v, %v", found, findErr)
	}
	registry.groupTable[groupIndex].identityCount = 0
	registry.mu.Unlock()
	registry.settleProposalGroupTermination(multiraft.ProposalGroupTermination{
		Group: group, SourceOwner: owner, SourceToken: token,
		Reason: multiraft.ProposalHostClosed,
	})
	if _, _, err := waiter.Poll(); !errors.Is(err, ErrRegistryCorrupt) {
		t.Fatalf("zero-identity pending termination = %v", err)
	}
}

func TestRegistryAppliedSourceClaimFencesIdentityRegistryAndABA(t *testing.T) {
	registry := testRegistry(t, 4, 8, 8)
	group := testGroup(210)
	owner := testAppliedSourceOwner(group)
	token, err := registry.claimAppliedSource(owner)
	if err != nil {
		t.Fatal(err)
	}
	if token.RegistryID == 0 || token.OwnerEpoch == 0 {
		t.Fatalf("claim token = %+v", token)
	}
	if stats := registry.Stats(); stats.LiveSourceOwners != 1 || stats.OutstandingGroups != 1 {
		t.Fatalf("claimed stats = %+v", stats)
	}
	if _, err := registry.claimAppliedSource(owner); !errors.Is(err, ErrSourceOwnerClaimed) {
		t.Fatalf("duplicate claim = %v", err)
	}
	other := owner
	other.NodeIncarnation++
	if _, err := registry.claimAppliedSource(other); !errors.Is(err, ErrSourceOwnerClaimed) {
		t.Fatalf("replacement before release = %v", err)
	}

	batch := newTestAppliedBatch(group, 10, 3, nil)
	if err := settleAppliedBatch(registry, batch); !errors.Is(err, ErrSourceOwnerMismatch) {
		t.Fatalf("ownerless settlement with live owner = %v", err)
	}
	wrongToken := token
	wrongToken.OwnerEpoch++
	if err := settleAppliedBatchOwned(registry, batch, wrongToken); !errors.Is(err, ErrSourceOwnerMismatch) {
		t.Fatalf("wrong-epoch settlement = %v", err)
	}
	wrongRegistry := token
	wrongRegistry.RegistryID++
	if err := settleAppliedBatchOwned(registry, batch, wrongRegistry); !errors.Is(err, ErrSourceOwnerMismatch) {
		t.Fatalf("wrong-registry settlement = %v", err)
	}
	wrongSource := batch
	wrongSource.source.NodeIncarnation++
	if err := settleAppliedBatchOwned(registry, wrongSource, token); !errors.Is(err, ErrSourceOwnerMismatch) {
		t.Fatalf("wrong-source settlement = %v", err)
	}
	if err := settleAppliedBatchOwned(registry, batch, token); err != nil {
		t.Fatalf("owned settlement = %v", err)
	}
	if err := registry.Close(); !errors.Is(err, ErrSourceOwnersLive) {
		t.Fatalf("close with owner = %v", err)
	}
	if err := registry.releaseAppliedSource(owner, wrongToken); !errors.Is(err, ErrSourceOwnerMismatch) {
		t.Fatalf("wrong release = %v", err)
	}
	if err := registry.releaseAppliedSource(owner, token); err != nil {
		t.Fatal(err)
	}

	replacement, err := registry.claimAppliedSource(other)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.RegistryID != token.RegistryID || replacement.OwnerEpoch == token.OwnerEpoch {
		t.Fatalf("replacement token = %+v, old %+v", replacement, token)
	}
	if err := registry.releaseAppliedSource(owner, token); !errors.Is(err, ErrSourceOwnerMismatch) {
		t.Fatalf("stale release = %v", err)
	}
	if err := registry.releaseAppliedSource(other, replacement); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryIDsAreDistinctAndExhaustionNeverWraps(t *testing.T) {
	first := testRegistry(t, 1, 1, 1)
	second := testRegistry(t, 1, 1, 1)
	if first.registryID == 0 || second.registryID == 0 || first.registryID == second.registryID {
		t.Fatalf("registry IDs = %d, %d", first.registryID, second.registryID)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	previous := nextRegistryID.Load()
	nextRegistryID.Store(math.MaxUint64)
	t.Cleanup(func() { nextRegistryID.Store(previous) })
	for attempt := 0; attempt < 2; attempt++ {
		if id, err := allocateRegistryID(); id != 0 || !errors.Is(err, ErrGenerationExhausted) {
			t.Fatalf("exhausted allocation %d = %d, %v", attempt, id, err)
		}
		if current := nextRegistryID.Load(); current != math.MaxUint64 {
			t.Fatalf("exhausted ID wrapped to %d", current)
		}
	}
}
