package raftserve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	pb "go.etcd.io/raft/v3/raftpb"
)

type testBatchLookup struct {
	lookup replicatedstate.CompletionLookup
	err    error
}

type testAppliedBatch struct {
	group   raftmember.GroupKey
	source  raftmember.AppliedBatchSource
	entries []raftmodel.NormalApply
	pub     raftmodel.Publication
	lookups []testBatchLookup
}

type gatedTestAppliedBatch struct {
	testAppliedBatch
	gate *testBatchLookupGate
}

type testBatchLookupGate struct {
	entered   chan struct{}
	proceed   chan struct{}
	beginErr  error
	endErr    error
	enterOnce sync.Once
	begins    atomic.Int32
	lookups   atomic.Int32
	ends      atomic.Int32
}

func newTestBatchLookupGate() *testBatchLookupGate {
	return &testBatchLookupGate{entered: make(chan struct{}), proceed: make(chan struct{})}
}

func (batch gatedTestAppliedBatch) BeginCompletionLookup(
	*raftmember.AppliedBatchCompletionWorkspace,
) error {
	batch.gate.begins.Add(1)
	batch.gate.enterOnce.Do(func() { close(batch.gate.entered) })
	<-batch.gate.proceed
	return batch.gate.beginErr
}

func (batch gatedTestAppliedBatch) LookupCompletionIntoWorkspace(
	_ *raftmember.AppliedBatchCompletionWorkspace,
	index int,
	dst []byte,
) (replicatedstate.CompletionLookup, bool, error) {
	batch.gate.lookups.Add(1)
	return batch.testAppliedBatch.LookupCompletionInto(index, dst)
}

func (batch gatedTestAppliedBatch) EndCompletionLookup(
	*raftmember.AppliedBatchCompletionWorkspace,
) error {
	batch.gate.ends.Add(1)
	return batch.gate.endErr
}

type refusingProposalHost struct {
	registry *Registry
}

func (host *refusingProposalHost) EnqueueTrackedProposal(
	group raftmember.GroupKey,
	_ []byte,
	token multiraft.ProposalToken,
) error {
	host.registry.settleProposalAdmission(multiraft.ProposalAdmission{
		Group: group, Token: token, Cause: multiraft.ErrQueueFull,
	})
	return nil
}

func (batch testAppliedBatch) Len() int                                { return len(batch.entries) }
func (batch testAppliedBatch) Group() raftmember.GroupKey              { return batch.group }
func (batch testAppliedBatch) Source() raftmember.AppliedBatchSource   { return batch.source }
func (batch testAppliedBatch) ReadyID() uint64                         { return batch.source.ReadyID }
func (batch testAppliedBatch) FirstIndex() uint64                      { return batch.source.FirstIndex }
func (batch testAppliedBatch) LastIndex() uint64                       { return batch.source.LastIndex }
func (batch testAppliedBatch) FinalPublication() raftmodel.Publication { return batch.pub }

func (batch testAppliedBatch) Entry(index int) (raftmodel.NormalApply, bool) {
	if index < 0 || index >= len(batch.entries) {
		return raftmodel.NormalApply{}, false
	}
	return batch.entries[index], true
}

func (batch testAppliedBatch) LookupCompletionInto(
	index int,
	dst []byte,
) (replicatedstate.CompletionLookup, bool, error) {
	if index < 0 || index >= len(batch.entries) || len(batch.entries[index].Data) == 0 {
		return replicatedstate.CompletionLookup{}, false, nil
	}
	lookup := batch.lookups[index]
	result := lookup.lookup
	result.Bytes = append(dst[:0], result.Bytes...)
	return result, true, lookup.err
}

func (batch testAppliedBatch) BeginCompletionLookup(
	*raftmember.AppliedBatchCompletionWorkspace,
) error {
	return nil
}

func (batch testAppliedBatch) LookupCompletionIntoWorkspace(
	_ *raftmember.AppliedBatchCompletionWorkspace,
	index int,
	dst []byte,
) (replicatedstate.CompletionLookup, bool, error) {
	return batch.LookupCompletionInto(index, dst)
}

func (batch testAppliedBatch) EndCompletionLookup(
	*raftmember.AppliedBatchCompletionWorkspace,
) error {
	return nil
}

func newTestAppliedBatch(
	group raftmember.GroupKey,
	first, ready uint64,
	data ...[]byte,
) testAppliedBatch {
	digest := sha256.Sum256([]byte{byte(first), byte(ready), byte(len(data))})
	source := raftmember.AppliedBatchSource{
		Group: group, AllocationGeneration: 7,
		MemberID: 9, StoreID: [16]byte{1}, NodeIncarnation: 11,
		ReadyID: ready, FirstIndex: first, LastIndex: first + uint64(len(data)) - 1,
		FinalDataChainDigest: digest,
	}
	batch := testAppliedBatch{
		group: group, source: source,
		entries: make([]raftmodel.NormalApply, len(data)),
		lookups: make([]testBatchLookup, len(data)),
		pub:     raftmodel.Publication{Applied: source.LastIndex, DataChainDigest: digest},
	}
	for index := range data {
		batch.entries[index] = raftmodel.NormalApply{
			Meta: raftmodel.ApplyMeta{
				Index: first + uint64(index), Term: 3, Type: pb.EntryNormal,
			},
			Data: data[index],
		}
	}
	return batch
}

func testCompletion(
	t testing.TB,
	group raftmember.GroupKey,
	commandBytes []byte,
	applied uint64,
) replicatedstate.CompletionLookup {
	t.Helper()
	const resultCode = replicatedstate.ResultApplied
	var result [replicatedstate.MutationCompletionResultBytes]byte
	resultBytes, err := replicatedstate.AppendMutationCompletionResult(
		result[:0], resultCode, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	return testCompletionResultBytes(
		t, group, commandBytes, applied, resultCode, resultBytes,
	)
}

func testCompletionResultBytes(
	t testing.TB,
	group raftmember.GroupKey,
	commandBytes []byte,
	applied uint64,
	resultCode uint32,
	resultBytes []byte,
) replicatedstate.CompletionLookup {
	return testCompletionResultBytesFormat(
		t, group, commandBytes, applied, resultCode,
		replicatedstate.ResultFormatMutation, resultBytes,
	)
}

func testCompletionResultBytesFormat(
	t testing.TB,
	group raftmember.GroupKey,
	commandBytes []byte,
	applied uint64,
	resultCode uint32,
	resultFormat uint16,
	resultBytes []byte,
) replicatedstate.CompletionLookup {
	t.Helper()
	command, err := replication.OpenCommand(commandBytes)
	if err != nil {
		t.Fatal(err)
	}
	epoch := command.ClientEpoch
	if command.Kind() == replication.CommandSessionOpen {
		epoch = 17
	}
	encoded, err := replication.AppendCompletion(nil, replication.Completion{
		ClusterID:             replication.ID128(group.ClusterID),
		ClusterIncarnation:    replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		Distribution:          string(command.Distribution), Shard: string(command.Shard),
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     replication.ID128(group.ShardIncarnation),
		GroupID:              replication.ID128(group.GroupID), ReplicaSetVersion: command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: epoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: applied,
		ResultCode: resultCode, ResultFormat: resultFormat,
		Storage: replication.CompletionInline, ResultLength: uint64(len(resultBytes)),
		ResultDigest: replication.CompletionResultDigest(resultCode, resultFormat, resultBytes),
		InlineResult: resultBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return replicatedstate.CompletionLookup{
		Key:   replicatedstate.SessionKey(command.AuthorityClass, command.Tenant, command.ClientID),
		Bytes: encoded, AppliedSequence: applied,
	}
}

func TestRegistrySettlementValidatesRouteGateResultWithoutAllocations(t *testing.T) {
	group := testGroup(21)
	gate, ok := routegate.NewMachine(1, routegate.MaxRetainedRecords)
	if !ok {
		t.Fatal("construct route gate")
	}
	gateCommand := routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: routegate.Identity{1}, Binding: routegate.Binding{2},
	}
	gateBytes, err := routegate.AppendCommand(nil, gateCommand)
	if err != nil {
		t.Fatal(err)
	}
	commandValue := testCommand(group, 1, 2)
	commandValue.Kind = replication.CommandRouteGate
	commandValue.Batches = nil
	commandValue.RouteGate = gateBytes
	commandValue.Fingerprint = sha256.Sum256(gateBytes)
	command := encodeTestCommand(t, commandValue)
	var result [routegate.OutcomeBytes]byte
	resultBytes, err := routegate.AppendOutcome(result[:0], gate.Apply(gateCommand))
	if err != nil {
		t.Fatal(err)
	}
	const applied = uint64(30)
	lookup := testCompletionResultBytesFormat(
		t, group, command, applied, replicatedstate.ResultRouteGate,
		replicatedstate.ResultFormatRouteGate, resultBytes,
	)
	identity, err := openCommandIdentity(group, command)
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := validateCompletionLookup(identity, lookup); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("route-gate completion validation allocations = %v, want 0", allocations)
	}
	wrongFormat := testCompletionResultBytesFormat(
		t, group, command, applied, replicatedstate.ResultRouteGate,
		replicatedstate.ResultFormatMutation, resultBytes,
	)
	if err := validateCompletionLookup(identity, wrongFormat); !errors.Is(err, ErrSettlementResult) {
		t.Fatalf("wrong route-gate result format error = %v", err)
	}
}

func testTransactionCommand(
	t testing.TB,
	group raftmember.GroupKey,
) []byte {
	t.Helper()
	id := distributedtxn.ID{0xc1, 0x55, 0x81}
	control, err := distributedtxn.AppendReplicatedCommand(nil, distributedtxn.ReplicatedCommand{
		Role:               distributedtxn.ReplicatedRoleParticipant,
		Operation:          distributedtxn.ReplicatedPrepareParticipant,
		ID:                 id,
		ExpectedRevision:   1,
		PayloadKind:        distributedtxn.ReplicatedPayloadNone,
		ControllerEpoch:    7,
		ExecutionPinDigest: distributedtxn.Digest(sha256.Sum256([]byte("raftserve/test-execution-pin"))),
	})
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(control)
	if err != nil {
		t.Fatal(err)
	}
	command := testCommand(group, id[0], sequence)
	command.Kind = replication.CommandTransaction
	command.Transaction = control
	command.Batches = nil
	command.ClientID = replication.ID128(id)
	command.ClientEpoch = uint64(distributedtxn.ReplicatedRoleParticipant)
	command.ClientSequence = sequence
	command.AckThrough = 0
	command.Fingerprint = sha256.Sum256(control)
	return encodeTestCommand(t, command)
}

func testTransactionCompletion(
	t testing.TB,
	group raftmember.GroupKey,
	commandBytes []byte,
	applied uint64,
	role distributedtxn.ReplicatedRole,
	operation distributedtxn.ReplicatedOperation,
) replicatedstate.CompletionLookup {
	t.Helper()
	command, err := replication.OpenCommand(commandBytes)
	if err != nil {
		t.Fatal(err)
	}
	var result [24]byte
	result[0] = byte(role)
	result[1] = byte(operation)
	result[2] = 2 // The control-revision-valid bit.
	binary.LittleEndian.PutUint64(result[8:16], 2)
	if _, err = replicatedstate.OpenTransactionCompletionResult(
		replicatedstate.ResultApplied, result[:],
	); err != nil {
		t.Fatal(err)
	}
	encoded, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution:          command.Distribution, Shard: command.Shard,
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: command.ClientEpoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: applied,
		ResultCode:   replicatedstate.ResultApplied,
		ResultFormat: replicatedstate.ResultFormatTransaction,
		Storage:      replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: replication.CompletionResultDigest(
			replicatedstate.ResultApplied, replicatedstate.ResultFormatTransaction, result[:],
		),
		InlineResult: result[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	return replicatedstate.CompletionLookup{
		Key:   replicatedstate.SessionKey(command.AuthorityClass, command.Tenant, command.ClientID),
		Bytes: encoded, AppliedSequence: applied,
	}
}

func TestRegistrySettlementValidatesMutationResultWithoutAllocations(t *testing.T) {
	group := testGroup(19)
	command := encodeTestCommand(t, testCommand(group, 1, 2))
	const applied = uint64(28)
	lookup := testCompletion(t, group, command, applied)
	identity, err := openCommandIdentity(group, command)
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := validateCompletionLookup(identity, lookup); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("mutation completion validation allocations = %v, want 0", allocations)
	}

	legacyEmpty := testCompletionResultBytes(
		t, group, command, applied, replicatedstate.ResultApplied, nil,
	)
	if err := validateCompletionLookup(identity, legacyEmpty); !errors.Is(err, ErrSettlementResult) {
		t.Fatalf("empty applied mutation result error = %v", err)
	}
}

func TestRegistrySettlementValidatesTransactionResultIdentityWithoutAllocations(t *testing.T) {
	group := testGroup(20)
	command := testTransactionCommand(t, group)
	outer, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	control, err := outer.OpenTransactionInto(nil)
	if err != nil || control.ControllerEpoch != 7 ||
		control.ExecutionPinDigest == (distributedtxn.Digest{}) {
		t.Fatalf("transaction controller fence = epoch:%d pin:%x err:%v",
			control.ControllerEpoch, control.ExecutionPinDigest, err)
	}
	const applied = uint64(29)
	lookup := testTransactionCompletion(
		t, group, command, applied,
		distributedtxn.ReplicatedRoleParticipant,
		distributedtxn.ReplicatedPrepareParticipant,
	)
	identity, err := openCommandIdentity(group, command)
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := validateCompletionLookup(identity, lookup); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("transaction completion validation allocations = %v, want 0", allocations)
	}

	registry := testRegistry(t, 1, 1, 1)
	host := &testProposalHost{registry: registry, admit: true}
	waiter, err := registry.Enqueue(host, group, command)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(group, applied, 3, command)
	batch.lookups[0].lookup = lookup
	if err = settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	completion := make([]byte, 0, replicatedstate.MaxCompletionEnvelopeBytes)
	completion, outcome, err := waiter.TakeCompletionInto(completion)
	if err != nil || outcome.Code != OutcomeCompletion || outcome.AppliedIndex != applied ||
		!bytes.Equal(completion, lookup.Bytes) {
		t.Fatalf("transaction completion = %dB, %+v, %v", len(completion), outcome, err)
	}

	for _, mismatch := range []struct {
		role      distributedtxn.ReplicatedRole
		operation distributedtxn.ReplicatedOperation
	}{
		{distributedtxn.ReplicatedRoleParticipant, distributedtxn.ReplicatedAbortParticipant},
		{distributedtxn.ReplicatedRoleCoordinator, distributedtxn.ReplicatedCommitCoordinator},
	} {
		candidate := testTransactionCompletion(
			t, group, command, applied, mismatch.role, mismatch.operation,
		)
		if err := validateCompletionLookup(identity, candidate); !errors.Is(err, ErrSettlementResult) {
			t.Fatalf("mismatched transaction result (%d,%d) error = %v", mismatch.role, mismatch.operation, err)
		}
	}
}

func TestRegistrySettlesBatchPrefixAtomicallyAndReplaysIdempotently(t *testing.T) {
	registry := testRegistry(t, 4, 4, 4)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(21)
	firstCommand := encodeTestCommand(t, testCommand(group, 1, 2))
	secondCommand := encodeTestCommand(t, testCommand(group, 2, 2))
	first, err := registry.Enqueue(host, group, firstCommand)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Enqueue(host, group, secondCommand)
	if err != nil {
		t.Fatal(err)
	}

	firstBatch := newTestAppliedBatch(group, 30, 4, firstCommand)
	firstBatch.lookups[0].lookup = testCompletion(t, group, firstCommand, 30)
	if err := settleAppliedBatch(registry, firstBatch); err != nil {
		t.Fatal(err)
	}
	if outcome, ready, err := first.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeCompletion || outcome.AppliedIndex != 30 {
		t.Fatalf("first = %+v, %v, %v", outcome, ready, err)
	}
	if outcome, ready, err := second.Poll(); err != nil || ready || outcome != (Outcome{}) {
		t.Fatalf("second before prefix = %+v, %v, %v", outcome, ready, err)
	}
	firstRecord := &registry.waiters[first.index]
	firstAttempt := &registry.attempts[firstRecord.attempt]
	wantAttempt := *firstAttempt
	wakeCount := len(firstRecord.wake)
	if err := settleAppliedBatch(registry, firstBatch); err != nil {
		t.Fatal(err)
	}
	if len(firstRecord.wake) != wakeCount || *firstAttempt != wantAttempt {
		t.Fatal("identical source replay changed settled attempt")
	}

	for _, mutate := range []func(*raftmember.AppliedBatchSource){
		func(source *raftmember.AppliedBatchSource) { source.AllocationGeneration++ },
		func(source *raftmember.AppliedBatchSource) { source.MemberID++ },
		func(source *raftmember.AppliedBatchSource) { source.StoreID[1]++ },
	} {
		replay := firstBatch
		mutate(&replay.source)
		if err := settleAppliedBatch(registry, replay); err != nil {
			t.Fatal(err)
		}
		if *firstAttempt != wantAttempt {
			t.Fatal("different physical source aliased the original source")
		}
	}

	secondBatch := newTestAppliedBatch(group, 31, 5, secondCommand)
	secondBatch.lookups[0].lookup = testCompletion(t, group, secondCommand, 31)
	if err := settleAppliedBatch(registry, secondBatch); err != nil {
		t.Fatal(err)
	}
	if outcome, ready, err := second.Poll(); err != nil || !ready ||
		outcome.Code != OutcomeCompletion || outcome.AppliedIndex != 31 {
		t.Fatalf("second = %+v, %v, %v", outcome, ready, err)
	}
	dst := make([]byte, 3, 3+completionSlotBytes)
	dst, outcome, err := first.TakeCompletionInto(dst)
	if err != nil || outcome.CompletionBytes == 0 ||
		!bytes.Equal(dst[3:], firstBatch.lookups[0].lookup.Bytes) {
		t.Fatalf("completion = %dB, %+v, %v", len(dst), outcome, err)
	}
}

func TestRegistrySettlementAcceptsNoWaiterReplayAndEmptyEntries(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(23)
	command := encodeTestCommand(t, testCommand(group, 5, 2))
	waiter, err := registry.Enqueue(host, group, command)
	if err != nil {
		t.Fatal(err)
	}
	if !waiter.Cancel() {
		t.Fatal("cancel failed")
	}
	batch := newTestAppliedBatch(group, 40, 8, nil, command, nil)
	batch.lookups[1].lookup = testCompletion(t, group, command, 41)
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.OutstandingIdentities != 0 || got.Waiters != 0 {
		t.Fatalf("replay retained local state = %+v", got)
	}
}

func TestRegistrySettlementFailsClosedOnMalformedApplicationData(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	group := testGroup(25)
	tests := []struct {
		name string
		data []byte
	}{
		{"unknown", []byte("not-a-replication-command")},
		{"claimed-ownership", []byte{'V', 'D', 'B', 'O', 'W', 'N', 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := newTestAppliedBatch(group, 50, 9, test.data)
			if err := settleAppliedBatch(registry, batch); !errors.Is(err, ErrSettlementResult) {
				t.Fatalf("settlement = %v", err)
			}
		})
	}
}

func TestRegistrySettlementRejectsIncompleteSourceIdentity(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	group := testGroup(27)
	command := encodeTestCommand(t, testCommand(group, 3, 2))
	base := newTestAppliedBatch(group, 60, 10, command)
	tests := []struct {
		name   string
		mutate func(*testAppliedBatch)
	}{
		{"group", func(batch *testAppliedBatch) { batch.source.Group.GroupID[0]++ }},
		{"allocation", func(batch *testAppliedBatch) { batch.source.AllocationGeneration = 0 }},
		{"member", func(batch *testAppliedBatch) { batch.source.MemberID = 0 }},
		{"store", func(batch *testAppliedBatch) { batch.source.StoreID = [16]byte{} }},
		{"node", func(batch *testAppliedBatch) { batch.source.NodeIncarnation = 0 }},
		{"ready", func(batch *testAppliedBatch) { batch.source.ReadyID = 0 }},
		{"interval", func(batch *testAppliedBatch) { batch.source.LastIndex++ }},
		{"digest", func(batch *testAppliedBatch) { batch.source.FinalDataChainDigest[0]++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.mutate(&candidate)
			if err := settleAppliedBatch(registry, candidate); !errors.Is(err, ErrSettlementRange) {
				t.Fatalf("settlement = %v", err)
			}
		})
	}
}

func TestRegistrySettlementDeduplicatesExactAttemptAndLogicalCompletion(t *testing.T) {
	registry := testRegistry(t, 1, 2, 3)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(28)
	commandValue := testCommand(group, 4, 3)
	commandValue.AckThrough = 0
	firstData := encodeTestCommand(t, commandValue)
	first, err := registry.Enqueue(host, group, firstData)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := registry.Enqueue(host, group, firstData)
	if err != nil {
		t.Fatal(err)
	}
	commandValue.AckThrough = 1
	secondData := encodeTestCommand(t, commandValue)
	second, err := registry.Enqueue(host, group, secondData)
	if err != nil {
		t.Fatal(err)
	}
	if len(host.tokens) != 2 {
		t.Fatalf("physical attempts = %d", len(host.tokens))
	}
	if got := registry.Stats(); got.PendingGroups != 1 ||
		got.PendingAdmittedAttempts != 2 {
		t.Fatalf("physical pending attempts = %+v", got)
	}
	batch := newTestAppliedBatch(group, 120, 16, firstData, firstData, secondData)
	completion := testCompletion(t, group, firstData, 120)
	for index := range batch.lookups {
		batch.lookups[index].lookup = completion
	}
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	for index, waiter := range []Waiter{first, exact, second} {
		outcome, ready, pollErr := waiter.Poll()
		if pollErr != nil || !ready || outcome.Code != OutcomeCompletion {
			t.Fatalf("waiter %d = %+v, %v, %v", index, outcome, ready, pollErr)
		}
	}
	if firstOutcome, _, _ := first.Poll(); firstOutcome.AppliedIndex != 120 {
		t.Fatalf("exact duplicate applied index = %d", firstOutcome.AppliedIndex)
	}
	if secondOutcome, _, _ := second.Poll(); secondOutcome.AppliedIndex != 122 {
		t.Fatalf("changed Ack applied index = %d", secondOutcome.AppliedIndex)
	}
	if got := registry.Stats(); got.RetainedCompletionBytes != len(completion.Bytes) ||
		got.PendingGroups != 0 || got.PendingAdmittedAttempts != 0 {
		t.Fatalf("completion accounting = %+v, want %d", got, len(completion.Bytes))
	}
}

func TestRegistrySettlementRollsBackInconsistentSecondOccurrence(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(29)
	data := encodeTestCommand(t, testCommand(group, 5, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(group, 130, 17, data, data)
	batch.lookups[0].lookup = testCompletion(t, group, data, 130)
	batch.lookups[1].lookup = batch.lookups[0].lookup
	batch.lookups[1].lookup.AppliedSequence++
	if err := settleAppliedBatch(registry, batch); !errors.Is(err, ErrSettlementResult) {
		t.Fatalf("inconsistent occurrence = %v", err)
	}
	registry.mu.Lock()
	record := registry.waiters[waiter.index]
	attempt := registry.attempts[record.attempt]
	entry := registry.entries[attempt.entry]
	registry.mu.Unlock()
	if attempt.state != attemptPending || entry.completionLen != 0 ||
		registry.Stats().RetainedCompletionBytes != 0 ||
		registry.Stats().PendingAdmittedAttempts != 1 {
		t.Fatalf("rollback = attempt %+v entry %+v stats %+v", attempt, entry, registry.Stats())
	}
	if _, ready, err := waiter.Poll(); err != nil || ready {
		t.Fatalf("rollback waiter = ready %v, %v", ready, err)
	}
	batch.lookups[1].lookup = batch.lookups[0].lookup
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrySettlementMixedCompletionAndRefusalIsAtomic(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(30)
	completedData := encodeTestCommand(t, testCommand(group, 6, 2))
	refusedData := encodeTestCommand(t, testCommand(group, 7, 2))
	completed, err := registry.Enqueue(host, group, completedData)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := registry.Enqueue(host, group, refusedData)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(group, 140, 18, completedData, refusedData)
	completion := testCompletion(t, group, completedData, 140)
	batch.lookups[0].lookup = completion
	batch.lookups[1].err = replicatedstate.ErrCompletionNotFound
	if err := settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	completedOutcome, completedReady, completedErr := completed.Poll()
	refusedOutcome, refusedReady, refusedErr := refused.Poll()
	if completedErr != nil || refusedErr != nil || !completedReady || !refusedReady ||
		completedOutcome.Code != OutcomeCompletion ||
		refusedOutcome.Code != OutcomeCompletionNotFound ||
		!errors.Is(refusedOutcome.Err(), replicatedstate.ErrCompletionNotFound) {
		t.Fatalf("mixed outcomes = %+v/%v/%v, %+v/%v/%v",
			completedOutcome, completedReady, completedErr,
			refusedOutcome, refusedReady, refusedErr)
	}
	if got := registry.Stats(); got.RetainedCompletionBytes != len(completion.Bytes) ||
		got.PendingGroups != 0 || got.PendingAdmittedAttempts != 0 {
		t.Fatalf("mixed completion accounting = %+v", got)
	}
}

func TestRegistrySettlementLookupDoesNotBlockUnrelatedGroupEnqueueAndWait(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	mainHost := &testProposalHost{registry: registry, admit: true}
	mainGroup := testGroup(51)
	mainData := encodeTestCommand(t, testCommand(mainGroup, 31, 2))
	mainWaiter, err := registry.Enqueue(mainHost, mainGroup, mainData)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(mainGroup, 180, 21, mainData)
	batch.lookups[0].lookup = testCompletion(t, mainGroup, mainData, 180)
	gate := newTestBatchLookupGate()
	gated := gatedTestAppliedBatch{testAppliedBatch: batch, gate: gate}
	settled := make(chan error, 1)
	go func() { settled <- settleAppliedBatch(registry, gated) }()
	<-gate.entered

	registry.mu.Lock()
	mainRecord := registry.waiters[mainWaiter.index]
	mainAttempt := registry.attempts[mainRecord.attempt]
	registry.mu.Unlock()
	if mainAttempt.state != attemptSettling ||
		!mainAttempt.hasFlag(attemptSettlementPinned) {
		t.Fatalf("gated attempt = %+v", mainAttempt)
	}

	type waitResult struct {
		outcome Outcome
		err     error
	}
	unrelatedDone := make(chan waitResult, 1)
	go func() {
		group := testGroup(52)
		data := encodeTestCommand(t, testCommand(group, 32, 2))
		waiter, enqueueErr := registry.Enqueue(
			&refusingProposalHost{registry: registry}, group, data,
		)
		if enqueueErr != nil {
			unrelatedDone <- waitResult{err: enqueueErr}
			return
		}
		outcome, waitErr := waiter.Wait(context.Background())
		unrelatedDone <- waitResult{outcome: outcome, err: waitErr}
	}()
	select {
	case result := <-unrelatedDone:
		if result.err != nil || result.outcome.Code != OutcomeProposalRefused {
			t.Fatalf("unrelated enqueue/wait = %+v, %v", result.outcome, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated group Enqueue/Wait blocked behind completion lookup")
	}

	close(gate.proceed)
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	if gate.begins.Load() != 1 || gate.lookups.Load() != 1 || gate.ends.Load() != 1 {
		t.Fatalf("workspace calls = begin %d lookup %d end %d",
			gate.begins.Load(), gate.lookups.Load(), gate.ends.Load())
	}
	if outcome, ready, pollErr := mainWaiter.Poll(); pollErr != nil || !ready ||
		outcome.Code != OutcomeCompletion {
		t.Fatalf("main settlement = %+v, %v, %v", outcome, ready, pollErr)
	}
}

func TestRegistrySettlementLifecycleCallbacksWhileLookupIsStaged(t *testing.T) {
	for _, test := range []struct {
		name       string
		admitted   bool
		endFailure bool
	}{
		{name: "admitted-success", admitted: true},
		{name: "refused-success"},
		{name: "admitted-end-failure", admitted: true, endFailure: true},
		{name: "refused-end-failure", endFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := testRegistry(t, 1, 1, 1)
			host := &delayedProposalHost{registry: registry}
			group := testGroup(53)
			data := encodeTestCommand(t, testCommand(group, 33, 2))
			waiter, err := registry.Enqueue(host, group, data)
			if err != nil {
				t.Fatal(err)
			}
			batch := newTestAppliedBatch(group, 190, 22, data)
			batch.lookups[0].lookup = testCompletion(t, group, data, 190)
			gate := newTestBatchLookupGate()
			if test.endFailure {
				gate.endErr = errors.New("injected completion-cut close failure")
			}
			settled := make(chan error, 1)
			go func() {
				settled <- settleAppliedBatch(
					registry,
					gatedTestAppliedBatch{testAppliedBatch: batch, gate: gate},
				)
			}()
			<-gate.entered

			admission := multiraft.ProposalAdmission{
				Group: group, Token: host.token, Admitted: test.admitted,
			}
			if !test.admitted {
				admission.Cause = multiraft.ErrQueueFull
			}
			registry.settleProposalAdmission(admission)
			close(gate.proceed)
			settleErr := <-settled
			if test.endFailure {
				if !errors.Is(settleErr, ErrSettlementResult) {
					t.Fatalf("settlement error = %v", settleErr)
				}
				outcome, ready, pollErr := waiter.Poll()
				if test.admitted {
					if pollErr != nil || ready || outcome != (Outcome{}) ||
						registry.Stats().PendingAdmittedAttempts != 1 {
						t.Fatalf("admitted rollback = %+v, %v, %v, stats %+v",
							outcome, ready, pollErr, registry.Stats())
					}
				} else if pollErr != nil || !ready || outcome.Code != OutcomeProposalRefused ||
					registry.Stats().PendingAdmittedAttempts != 0 {
					t.Fatalf("refused rollback = %+v, %v, %v, stats %+v",
						outcome, ready, pollErr, registry.Stats())
				}
				return
			}
			if settleErr != nil {
				t.Fatal(settleErr)
			}
			outcome, ready, pollErr := waiter.Poll()
			if pollErr != nil || !ready || outcome.Code != OutcomeCompletion ||
				outcome.AppliedIndex != 190 || registry.Stats().PendingAdmittedAttempts != 0 {
				t.Fatalf("successful callback race = %+v, %v, %v, stats %+v",
					outcome, ready, pollErr, registry.Stats())
			}
		})
	}
}

func TestRegistryTerminateGroupWaitsForStagedSettlement(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(54)
	data := encodeTestCommand(t, testCommand(group, 34, 2))
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(group, 200, 23, data)
	batch.lookups[0].lookup = testCompletion(t, group, data, 200)
	gate := newTestBatchLookupGate()
	settled := make(chan error, 1)
	go func() {
		settled <- settleAppliedBatch(
			registry, gatedTestAppliedBatch{testAppliedBatch: batch, gate: gate},
		)
	}()
	<-gate.entered
	terminated := make(chan error, 1)
	go func() {
		terminated <- registry.TerminateGroup(
			group, multiraft.ProposalGroupLeadershipLost,
		)
	}()
	select {
	case err := <-terminated:
		t.Fatalf("termination crossed staged settlement: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(gate.proceed)
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	if err := <-terminated; err != nil {
		t.Fatal(err)
	}
	if outcome, ready, pollErr := waiter.Poll(); pollErr != nil || !ready ||
		outcome.Code != OutcomeCompletion {
		t.Fatalf("settlement lost to termination = %+v, %v, %v", outcome, ready, pollErr)
	}
}
