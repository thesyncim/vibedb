package raftserve

import (
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

type immediateProposalHost struct {
	registry *Registry
}

func (host *immediateProposalHost) EnqueueTrackedProposal(
	group raftmember.GroupKey,
	_ []byte,
	token multiraft.ProposalToken,
) error {
	host.registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: token, Admitted: true,
	})
	return nil
}

type warmSettlementCycle struct {
	registry *Registry
	host     immediateProposalHost
	group    raftmember.GroupKey
	data     []byte
	batch    testAppliedBatch
	result   []byte
}

func newWarmSettlementCycle(tb testing.TB) *warmSettlementCycle {
	tb.Helper()
	registry := testRegistry(tb, 1, 1, 1)
	group := testGroup(41)
	data := encodeTestCommand(tb, testCommand(group, 12, 2))
	batch := newTestAppliedBatch(group, 120, 18, data)
	batch.lookups[0].lookup = testCompletion(tb, group, data, 120)
	cycle := &warmSettlementCycle{
		registry: registry, group: group, data: data, batch: batch,
		result: make([]byte, 0, completionSlotBytes),
	}
	cycle.host.registry = registry
	return cycle
}

func (cycle *warmSettlementCycle) run() error {
	waiter, err := cycle.registry.Enqueue(&cycle.host, cycle.group, cycle.data)
	if err != nil {
		return err
	}
	if err := settleAppliedBatch(cycle.registry, cycle.batch); err != nil {
		return err
	}
	result, outcome, err := waiter.TakeCompletionInto(cycle.result[:0])
	if err != nil {
		return err
	}
	if outcome.Code != OutcomeCompletion || outcome.AppliedIndex != cycle.batch.FirstIndex() ||
		len(result) == 0 {
		return fmt.Errorf("unexpected settled result: %+v, %d bytes", outcome, len(result))
	}
	cycle.result = result[:0]
	return nil
}

func TestRegistryWarmSettlementAllocationsAndFixedStorage(t *testing.T) {
	cycle := newWarmSettlementCycle(t)
	if err := cycle.run(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1_000, func() {
		if err := cycle.run(); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm register, settle, and take allocations = %v, want 0", allocs)
	}
	stats := cycle.registry.Stats()
	if stats.IdentityCapacity != 1 || stats.AttemptCapacity != 1 ||
		stats.WaiterCapacity != 1 || stats.TableCapacity != 2 ||
		stats.GroupCapacity != 1 || stats.GroupTableCapacity != 2 ||
		stats.TenantArenaBytes != tenantSlotBytes ||
		stats.CompletionArenaBytes != completionSlotBytes ||
		stats.OutstandingGroups != 0 || stats.OutstandingIdentities != 0 ||
		stats.OutstandingAttempts != 0 || stats.Waiters != 0 ||
		stats.PendingGroups != 0 || stats.PendingAdmittedAttempts != 0 {
		t.Fatalf("fixed storage geometry = %+v", stats)
	}
	if size := unsafe.Sizeof(attemptRecord{}); size > 96 {
		t.Fatalf("attempt record retained unexpected source-sized state: %dB", size)
	}
	if size := unsafe.Sizeof(entryRecord{}); size > 248 {
		t.Fatalf("entry record exceeds fixed geometry: %dB", size)
	}
	if size := unsafe.Sizeof(pendingGroupSlot{}); size > 96 {
		t.Fatalf("pending group slot exceeds fixed geometry: %dB", size)
	}
	t.Logf(
		"Registry=%dB entry=%dB attempt=%dB waiter=%dB group-slot=%dB source=%dB tenant-slot=%dB completion-slot=%dB",
		unsafe.Sizeof(Registry{}), unsafe.Sizeof(entryRecord{}),
		unsafe.Sizeof(attemptRecord{}), unsafe.Sizeof(waiterRecord{}),
		unsafe.Sizeof(pendingGroupSlot{}),
		unsafe.Sizeof(raftmember.AppliedBatchSource{}), tenantSlotBytes, completionSlotBytes,
	)
}

func BenchmarkRegistryWarmRegisterSettleTake(b *testing.B) {
	cycle := newWarmSettlementCycle(b)
	if err := cycle.run(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(cycle.data)))
	b.ReportMetric(float64(unsafe.Sizeof(Registry{})), "registry-B")
	b.ReportMetric(float64(unsafe.Sizeof(entryRecord{})), "entry-record-B")
	b.ReportMetric(float64(unsafe.Sizeof(attemptRecord{})), "attempt-record-B")
	b.ReportMetric(float64(unsafe.Sizeof(waiterRecord{})), "waiter-record-B")
	b.ReportMetric(float64(unsafe.Sizeof(pendingGroupSlot{})), "group-slot-B")
	b.ResetTimer()
	for range b.N {
		if err := cycle.run(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryTerminateGroupWithUnrelatedPopulation(b *testing.B) {
	for _, unrelated := range [...]int{0, AbsoluteMaxOutstandingIdentities - 1} {
		b.Run(fmt.Sprintf("unrelated-%d", unrelated), func(b *testing.B) {
			capacity := unrelated + 1
			unrelatedGroups := min(unrelated, multiraft.AbsoluteMaxGroups-1)
			registry, err := NewRegistry(Limits{
				MaxGroups:                  unrelatedGroups + 1,
				MaxOutstandingIdentities:   capacity,
				MaxOutstandingAttempts:     capacity,
				MaxWaiters:                 capacity,
				MaxAttemptsPerIdentity:     1,
				MaxRetainedCompletionBytes: int64(capacity * completionSlotBytes),
			})
			if err != nil {
				b.Fatal(err)
			}
			register := func(identity commandIdentity) (Waiter, registrationToken) {
				registry.mu.Lock()
				waiter, token, enqueue, registerErr := registry.registerLocked(identity)
				registry.mu.Unlock()
				if registerErr != nil || !enqueue {
					b.Fatalf("register = %v, enqueue %v", registerErr, enqueue)
				}
				return waiter, token
			}
			for index := 0; index < unrelated; index++ {
				group := benchmarkGroup(uint64(index%unrelatedGroups + 1))
				register(benchmarkIdentity(group, uint64(index+1)))
			}
			hotGroup := benchmarkGroup(uint64(unrelatedGroups + 1))
			hotIdentity := benchmarkIdentity(hotGroup, uint64(capacity))
			_, token := register(hotIdentity)
			admit := func(token registrationToken) {
				registry.settleProposalAdmission(multiraft.ProposalAdmission{
					Group: hotGroup, Token: token.proposalToken(), Admitted: true,
				})
			}
			admit(token)
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(float64(unrelated), "unrelated-identities")
			b.ReportMetric(float64(unrelatedGroups), "unrelated-groups")
			for range b.N {
				if err := registry.TerminateGroup(
					hotGroup, multiraft.ProposalGroupLeadershipLost,
				); err != nil {
					b.Fatal(err)
				}
				// Restore the one hot attempt in place. This fixed work does not
				// traverse either the identity table or an unrelated group list.
				if !rearmTerminatedBenchmarkAttempt(registry, token, hotGroup) {
					b.Fatal("rearm terminated benchmark attempt")
				}
			}
		})
	}
}

func rearmTerminatedBenchmarkAttempt(
	registry *Registry,
	token registrationToken,
	group raftmember.GroupKey,
) bool {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	attempt, entry, ok := registry.validAttemptLocked(
		token.attempt, token.attemptGeneration, attemptComplete,
	)
	if !ok || entry.position.group != group ||
		attempt.outcome != OutcomeNotLeader || attempt.admitted ||
		attempt.lifecyclePending || attempt.waiterCount != 1 ||
		int(token.waiter) >= len(registry.waiters) {
		return false
	}
	waiter := &registry.waiters[token.waiter]
	if !waiter.active || waiter.generation != token.waiterGeneration {
		return false
	}
	if !registry.addGroupPendingLocked(group) {
		return false
	}
	drainSignal(waiter.wake)
	attempt.outcome = 0
	attempt.state = attemptPending
	attempt.admitted = true
	return true
}

func benchmarkGroup(identity uint64) raftmember.GroupKey {
	group := testGroup(91)
	binary.LittleEndian.PutUint64(group.GroupID[:8], identity)
	binary.LittleEndian.PutUint64(group.GroupID[8:], ^identity)
	return group
}

var benchmarkTenant = [...]byte{0x74}

func benchmarkIdentity(
	group raftmember.GroupKey,
	identity uint64,
) commandIdentity {
	result := commandIdentity{
		position: requestPosition{
			group: group, epoch: 1, sequence: identity,
			namespace: requestNamespaceSequenced,
		},
		tenant: benchmarkTenant[:],
	}
	binary.LittleEndian.PutUint64(result.position.sessionDigest[:8], identity)
	binary.LittleEndian.PutUint64(result.position.clientID[:8], identity)
	binary.LittleEndian.PutUint64(result.fingerprint[:8], identity)
	binary.LittleEndian.PutUint64(result.logical[:8], identity)
	binary.LittleEndian.PutUint64(result.attempt[:8], identity)
	return result
}
