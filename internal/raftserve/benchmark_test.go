package raftserve

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
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
	if size := unsafe.Sizeof(pendingGroupSlot{}); size > 152 {
		t.Fatalf("pending group slot exceeds fixed geometry: %dB", size)
	}
	if size := unsafe.Sizeof(waiterRecord{}); size > 48 {
		t.Fatalf("waiter record exceeds fixed geometry: %dB", size)
	}
	maxGroupTableSlots := 1
	for maxGroupTableSlots < 2*multiraft.AbsoluteMaxGroups {
		maxGroupTableSlots <<= 1
	}
	attemptDelta := uintptr(0)
	if size := unsafe.Sizeof(attemptRecord{}); size > 88 {
		attemptDelta = size - 88
	}
	groupDelta := uintptr(0)
	if size := unsafe.Sizeof(pendingGroupSlot{}); size > 96 {
		groupDelta = size - 96
	}
	maxGeometryDelta := attemptDelta*AbsoluteMaxOutstandingAttempts +
		groupDelta*uintptr(maxGroupTableSlots)
	if maxGeometryDelta >= 1<<20 {
		t.Fatalf("maximum ownership and pending-list geometry delta = %dB", maxGeometryDelta)
	}
	t.Logf(
		"Registry=%dB entry=%dB attempt=%dB waiter=%dB group-slot=%dB max-delta=%dB source=%dB tenant-slot=%dB completion-slot=%dB",
		unsafe.Sizeof(Registry{}), unsafe.Sizeof(entryRecord{}),
		unsafe.Sizeof(attemptRecord{}), unsafe.Sizeof(waiterRecord{}),
		unsafe.Sizeof(pendingGroupSlot{}), maxGeometryDelta,
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

type realAppliedBatchSettlementCycle struct {
	runtime   *raftmember.Runtime
	workspace raftmember.ReadyWorkspace
	registry  *Registry
	host      immediateProposalHost
	group     raftmember.GroupKey
	commands  [][]byte
	waiters   []Waiter
	batch     raftmember.AppliedBatch
}

func newRealAppliedBatchSettlementCycle(
	tb testing.TB,
	matched int,
) *realAppliedBatchSettlementCycle {
	return newRealAppliedBatchSettlementCycleWithIdentity(
		tb, matched, "orders", "0000-7fff", 0,
	)
}

func newRealAppliedBatchSettlementCycleWithIdentity(
	tb testing.TB,
	matched int,
	distributionName, shardID string,
	tenantBytes int,
) *realAppliedBatchSettlementCycle {
	tb.Helper()
	if matched <= 0 || matched > raftmodel.MaxNormalApplyBatchEntries {
		tb.Fatalf("invalid real settlement match count %d", matched)
	}
	if tenantBytes < 0 || tenantBytes > replication.MaxIdentityBytes {
		tb.Fatalf("invalid real settlement tenant length %d", tenantBytes)
	}
	seed := byte(100 + matched%100)
	memberID := uint64(seed) + 1
	leaderID := memberID + 1
	runtime, base := newServingRuntimeWithVotersAndIdentity(
		tb, seed, replicatedstate.MaxSessionRetryWindow,
		uint64(max(16, matched)),
		[]uint64{memberID, leaderID},
		distributionName, shardID,
	)
	cycle := &realAppliedBatchSettlementCycle{runtime: runtime, group: runtime.Identity().Group}
	driveRealSettlementRuntimeIdle(tb, cycle, func(raftmember.AppliedBatch) error { return nil })

	opens := make([][]byte, matched)
	for index := range opens {
		command := servingCommand(base, 0, 1)
		tenantLength := 1 + (index*131)%replication.MaxIdentityBytes
		if tenantBytes != 0 {
			tenantLength = tenantBytes
		}
		command.Tenant = bytes.Repeat(
			[]byte{'t'}, tenantLength,
		)
		command.Kind = replication.CommandSessionOpen
		command.AckThrough = 0
		command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
		command.Batches = nil
		binary.BigEndian.PutUint16(command.ClientID[14:], uint16(index+1))
		opens[index] = encodeTestCommand(tb, command)
	}
	stepRealSettlementAppend(tb, cycle, leaderID, 1, 1, opens)
	epochs := make([]uint64, 0, matched)
	driveRealSettlementRuntimeIdle(tb, cycle, func(batch raftmember.AppliedBatch) error {
		for index := 0; index < batch.Len(); index++ {
			lookup, hasCommand, lookupErr := batch.LookupCompletion(index)
			if lookupErr != nil || !hasCommand {
				return errors.Join(errors.New("open completion lookup failed"), lookupErr)
			}
			completion, completionErr := replication.OpenCompletion(lookup.Bytes)
			if completionErr != nil {
				return completionErr
			}
			if completion.ResultCode != replicatedstate.ResultSessionOpened ||
				completion.ClientEpoch == 0 {
				return fmt.Errorf("open completion %d = %+v", index, completion)
			}
			epochs = append(epochs, completion.ClientEpoch)
		}
		return nil
	})
	if len(epochs) != matched {
		tb.Fatalf("real settlement opened %d sessions, want %d", len(epochs), matched)
	}

	cycle.commands = make([][]byte, matched)
	for index := range cycle.commands {
		// Prime one retained completion in each distinct session. The first
		// execution may split at the system-collection physical batch limit; the
		// replay below has no per-session writes and forms one 128-entry batch.
		command := servingCommand(base, epochs[index], 2)
		tenantLength := 1 + (index*131)%replication.MaxIdentityBytes
		if tenantBytes != 0 {
			tenantLength = tenantBytes
		}
		command.Tenant = bytes.Repeat(
			[]byte{'t'}, tenantLength,
		)
		binary.BigEndian.PutUint16(command.ClientID[14:], uint16(index+1))
		cycle.commands[index] = encodeTestCommand(tb, command)
	}
	stepRealSettlementAppend(
		tb, cycle, leaderID, 1+uint64(matched), 2, cycle.commands,
	)
	driveRealSettlementRuntimeIdle(
		tb, cycle, func(raftmember.AppliedBatch) error { return nil },
	)
	stepRealSettlementAppend(
		tb, cycle, leaderID, 1+2*uint64(matched), 2, cycle.commands,
	)
	wantRetry := errors.New("retain real applied batch for settlement benchmark")
	for step := 0; step < 20000; step++ {
		result, err := runtime.DriveReady(
			&cycle.workspace,
			func(raftmember.OutboundMessage) error { return nil },
			func(batch raftmember.AppliedBatch) error {
				cycle.batch = batch
				return raftmember.RetryResultSettlement(wantRetry)
			},
		)
		if errors.Is(err, raftmember.ErrResultSettlementRejected) && errors.Is(err, wantRetry) {
			break
		}
		if err != nil {
			tb.Fatalf("capture real applied batch step %d: %v", step, err)
		}
		if !result.Progressed() {
			tb.Fatal("real Runtime became idle before result settlement")
		}
	}
	if cycle.batch.Len() != matched {
		tb.Fatalf("real applied batch length = %d, want %d", cycle.batch.Len(), matched)
	}
	for index := range cycle.commands {
		entry, ok := cycle.batch.Entry(index)
		if !ok || !bytes.Equal(entry.Data, cycle.commands[index]) {
			tb.Fatalf("real applied entry %d mismatch", index)
		}
	}
	registry, err := NewRegistry(Limits{
		MaxGroups:                  1,
		MaxOutstandingIdentities:   matched,
		MaxOutstandingAttempts:     matched,
		MaxWaiters:                 matched,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: int64(matched * completionSlotBytes),
	})
	if err != nil {
		tb.Fatal(err)
	}
	cycle.registry = registry
	cycle.host.registry = cycle.registry
	cycle.waiters = make([]Waiter, matched)
	tb.Cleanup(func() {
		_ = cycle.registry.Close()
		_, _ = cycle.runtime.DriveReady(
			&cycle.workspace,
			func(raftmember.OutboundMessage) error { return nil },
			func(raftmember.AppliedBatch) error { return nil },
		)
		_ = cycle.runtime.Close()
	})
	return cycle
}

func stepRealSettlementAppend(
	tb testing.TB,
	cycle *realAppliedBatchSettlementCycle,
	leaderID, previousIndex, previousTerm uint64,
	data [][]byte,
) {
	tb.Helper()
	const term = uint64(2)
	entries := make([]*pb.Entry, len(data))
	for index := range data {
		entryIndex := previousIndex + uint64(index) + 1
		entryTerm := term
		entryType := pb.EntryNormal
		entries[index] = &pb.Entry{
			Term: &entryTerm, Index: &entryIndex, Type: &entryType, Data: data[index],
		}
	}
	messageType := pb.MsgApp
	memberID := cycle.runtime.Identity().MemberID
	commit := previousIndex + uint64(len(entries))
	messageTerm := term
	message := &pb.Message{
		Type: &messageType, To: &memberID, From: &leaderID, Term: &messageTerm,
		LogTerm: &previousTerm, Index: &previousIndex, Entries: entries, Commit: &commit,
	}
	if err := cycle.runtime.StepMessage(message); err != nil {
		tb.Fatalf("step real settlement append %d..%d: %v",
			previousIndex+1, commit, err)
	}
}

func driveRealSettlementRuntimeIdle(
	tb testing.TB,
	cycle *realAppliedBatchSettlementCycle,
	settle raftmember.ResultSettlementSink,
) {
	tb.Helper()
	for step := 0; step < 20000; step++ {
		result, err := cycle.runtime.DriveReady(
			&cycle.workspace,
			func(raftmember.OutboundMessage) error { return nil },
			settle,
		)
		if err != nil {
			tb.Fatalf("drive real settlement Runtime step %d: %v", step, err)
		}
		if !result.Progressed() {
			return
		}
	}
	tb.Fatal("real settlement Runtime did not become idle")
}

func (cycle *realAppliedBatchSettlementCycle) run() error {
	for index := range cycle.commands {
		waiter, err := cycle.registry.Enqueue(
			&cycle.host, cycle.group, cycle.commands[index],
		)
		if err != nil {
			return err
		}
		cycle.waiters[index] = waiter
	}
	if err := cycle.registry.SettleAppliedBatch(cycle.batch); err != nil {
		return err
	}
	for index := range cycle.waiters {
		outcome, ready, err := cycle.waiters[index].Poll()
		if err != nil {
			return err
		}
		if !ready || outcome.Code != OutcomeCompletion {
			return fmt.Errorf("real settlement outcome %d = %+v, ready %v", index, outcome, ready)
		}
		if !cycle.waiters[index].Cancel() {
			return fmt.Errorf("real settlement waiter %d did not cancel", index)
		}
	}
	return nil
}

func TestRegistryRealAppliedBatchSettlementWarmAllocations(t *testing.T) {
	for _, matched := range [...]int{1, raftmodel.MaxNormalApplyBatchEntries} {
		t.Run(fmt.Sprintf("matched-%d", matched), func(t *testing.T) {
			cycle := newRealAppliedBatchSettlementCycle(t, matched)
			if err := cycle.run(); err != nil {
				t.Fatal(err)
			}
			runs := 1_000
			if matched > 1 {
				runs = 100
			}
			allocs := testing.AllocsPerRun(runs, func() {
				if err := cycle.run(); err != nil {
					panic(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("real warm settlement allocations = %v, want 0", allocs)
			}
		})
	}
}

func TestRegistryRealAppliedBatchSettlementMaximumIdentity(t *testing.T) {
	cycle := newRealAppliedBatchSettlementCycleWithIdentity(
		t, 1,
		strings.Repeat("d", replication.MaxIdentityBytes),
		strings.Repeat("s", replication.MaxIdentityBytes),
		replication.MaxIdentityBytes,
	)
	lookup, hasCommand, err := cycle.batch.LookupCompletion(0)
	if err != nil || !hasCommand {
		t.Fatalf("real maximum-identity completion lookup = %+v, %v, %v", lookup, hasCommand, err)
	}
	if len(lookup.Bytes) != replicatedstate.MaxMutationCompletionEnvelopeBytes {
		t.Fatalf(
			"real maximum-identity completion bytes = %d, want %d",
			len(lookup.Bytes), replicatedstate.MaxMutationCompletionEnvelopeBytes,
		)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(completion.Tenant) != replication.MaxIdentityBytes ||
		len(completion.Distribution) != replication.MaxIdentityBytes ||
		len(completion.Shard) != replication.MaxIdentityBytes ||
		completion.ResultCode != replicatedstate.ResultApplied ||
		completion.ResultFormat != replicatedstate.ResultFormatMutation ||
		completion.ResultLength != replicatedstate.MutationCompletionResultBytes ||
		len(completion.InlineResult) != replicatedstate.MutationCompletionResultBytes {
		t.Fatalf("real maximum-identity completion = %+v", completion)
	}
	affectedRows, err := replicatedstate.OpenMutationCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil || affectedRows != 1 {
		t.Fatalf("real maximum-identity affected rows = %d, %v", affectedRows, err)
	}
	if err := cycle.run(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1_000, func() {
		if err := cycle.run(); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("real maximum-identity warm settlement allocations = %v, want 0", allocs)
	}
}

func BenchmarkRegistryRuntimeReplicatedApplySettlement(b *testing.B) {
	for _, matched := range [...]int{1, raftmodel.MaxNormalApplyBatchEntries} {
		b.Run(fmt.Sprintf("matched-%d", matched), func(b *testing.B) {
			cycle := newRealAppliedBatchSettlementCycle(b, matched)
			if err := cycle.run(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ReportMetric(float64(matched), "matched-entries")
			b.ResetTimer()
			for range b.N {
				if err := cycle.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRegistryTerminateGroupWithSameGroupNonpendingPopulation(b *testing.B) {
	for _, nonpending := range [...]int{0, AbsoluteMaxOutstandingAttempts - 1} {
		b.Run(fmt.Sprintf("same-group-nonpending-%d", nonpending), func(b *testing.B) {
			staticNonpending := nonpending
			if staticNonpending != 0 {
				// Reserve one nonpending attempt as a newer same-identity
				// predecessor of the hot waiterless attempt on every iteration.
				staticNonpending--
			}
			identityCount := (staticNonpending+AbsoluteMaxAttemptsPerIdentity-1)/
				AbsoluteMaxAttemptsPerIdentity + 1
			attemptCapacity := nonpending + 1
			registry, err := NewRegistry(Limits{
				MaxGroups:                  1,
				MaxOutstandingIdentities:   identityCount,
				MaxOutstandingAttempts:     attemptCapacity,
				MaxWaiters:                 identityCount,
				MaxAttemptsPerIdentity:     min(AbsoluteMaxAttemptsPerIdentity, attemptCapacity),
				MaxRetainedCompletionBytes: int64(identityCount * completionSlotBytes),
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
			hotGroup := benchmarkGroup(1)
			remaining := staticNonpending
			attemptIdentity := uint64(1)
			for identityIndex := 0; remaining != 0; identityIndex++ {
				identity := benchmarkIdentity(hotGroup, uint64(identityIndex+1))
				count := min(remaining, AbsoluteMaxAttemptsPerIdentity)
				for range count {
					candidate := identity
					binary.LittleEndian.PutUint64(candidate.attempt[:8], attemptIdentity)
					attemptIdentity++
					waiter, _ := register(candidate)
					if !waiter.Cancel() {
						b.Fatal("cancel nonpending setup waiter")
					}
				}
				remaining -= count
			}
			hotIdentity := benchmarkIdentity(hotGroup, uint64(identityCount))
			binary.LittleEndian.PutUint64(hotIdentity.attempt[:8], attemptIdentity)
			admit := func(token registrationToken) {
				registry.settleProposalAdmission(multiraft.ProposalAdmission{
					Group: hotGroup, Token: token.proposalToken(), Admitted: true,
				})
			}
			prepareWaiterlessPending := func() registrationToken {
				waiter, token := register(hotIdentity)
				if !waiter.Cancel() {
					b.Fatal("cancel hot setup waiter")
				}
				admit(token)
				if nonpending == 0 {
					return registrationToken{}
				}
				newer := hotIdentity
				newer.attempt[8] ^= 0xff
				newerWaiter, newerToken := register(newer)
				if !newerWaiter.Cancel() {
					b.Fatal("cancel newer same-identity setup waiter")
				}
				return newerToken
			}
			cleanupNewer := func(token registrationToken) {
				if token == (registrationToken{}) {
					return
				}
				registry.mu.Lock()
				registry.rollbackRegistrationLocked(token)
				registry.mu.Unlock()
			}
			newerToken := prepareWaiterlessPending()
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(float64(nonpending), "same-group-nonpending-attempts")
			for iteration := 0; iteration < b.N; iteration++ {
				if err := registry.TerminateGroup(
					hotGroup, multiraft.ProposalGroupLeadershipLost,
				); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				cleanupNewer(newerToken)
				if iteration+1 < b.N {
					newerToken = prepareWaiterlessPending()
					b.StartTimer()
				}
			}
		})
	}
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
