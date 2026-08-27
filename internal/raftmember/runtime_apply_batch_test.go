package raftmember

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftsim"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type runtimeMemoryBatchMachine struct {
	publication        raftmodel.Publication
	batchCalls         int
	originalCompletion []byte
}

func newRuntimeMemoryBatchMachine(t *testing.T, stable *raftsim.MemoryStore) *runtimeMemoryBatchMachine {
	t.Helper()
	_, state, err := stable.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	return &runtimeMemoryBatchMachine{publication: raftmodel.Publication{
		Applied: 1, DataChainDigest: sha256.Sum256([]byte("runtime-memory-batch-base")),
		ConfState: proto.Clone(state).(*pb.ConfState), ReplicaSetVersion: 1,
	}}
}

func (machine *runtimeMemoryBatchMachine) Applied() uint64 {
	return machine.publication.Applied
}

func (machine *runtimeMemoryBatchMachine) Published() raftmodel.Publication {
	publication := machine.publication
	publication.ConfState = proto.Clone(publication.ConfState).(*pb.ConfState)
	return publication
}

func (machine *runtimeMemoryBatchMachine) ApplyNormal(
	meta raftmodel.ApplyMeta,
	data []byte,
) (raftmodel.Publication, error) {
	if meta.Index != machine.publication.Applied+1 || meta.Term == 0 ||
		meta.Type != pb.EntryType_EntryNormal {
		return raftmodel.Publication{}, errors.New("invalid memory apply sequence")
	}
	machine.publication.Applied = meta.Index
	if len(data) != 0 {
		hasher := sha256.New()
		_, _ = hasher.Write(machine.publication.DataChainDigest[:])
		_, _ = hasher.Write(data)
		_ = hasher.Sum(machine.publication.DataChainDigest[:0])
	}
	return machine.Published(), nil
}

func (machine *runtimeMemoryBatchMachine) ApplyNormalBatch(
	entries []raftmodel.NormalApply,
	witnesses [][32]byte,
) (int, raftmodel.Publication, error) {
	machine.batchCalls++
	clear(witnesses)
	if machine.originalCompletion != nil {
		return 0, raftmodel.Publication{}, nil
	}
	for index := range entries {
		publication, err := machine.ApplyNormal(entries[index].Meta, entries[index].Data)
		if err != nil {
			return 0, raftmodel.Publication{}, err
		}
		witnesses[index] = publication.DataChainDigest
	}
	return len(entries), machine.Published(), nil
}

func (machine *runtimeMemoryBatchMachine) ApplyConfiguration(
	meta raftmodel.ApplyMeta,
	state *pb.ConfState,
) (raftmodel.Publication, error) {
	if meta.Index != machine.publication.Applied+1 || meta.Term == 0 || state == nil {
		return raftmodel.Publication{}, errors.New("invalid memory configuration sequence")
	}
	machine.publication.Applied = meta.Index
	machine.publication.ConfState = proto.Clone(state).(*pb.ConfState)
	machine.publication.ReplicaSetVersion = meta.Index
	return machine.Published(), nil
}

func (machine *runtimeMemoryBatchMachine) InstallSnapshot(
	snapshot *pb.Snapshot,
) (raftmodel.Publication, error) {
	if snapshot.GetMetadata().GetIndex() != machine.publication.Applied ||
		snapshot.GetMetadata().GetConfState() == nil ||
		machine.publication.ConfState.Equivalent(snapshot.GetMetadata().GetConfState()) != nil {
		return raftmodel.Publication{}, errors.New("invalid memory snapshot")
	}
	return machine.Published(), nil
}

func drainRuntimeMemoryNode(t *testing.T, node *raftmodel.Node) {
	t.Helper()
	var workspace raftmodel.NormalApplyBatchWorkspace
	for step := 0; step < 1000; step++ {
		hasReady, err := node.HasReady()
		if err != nil {
			t.Fatal(err)
		}
		if !hasReady {
			return
		}
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("CaptureReady = %v, %v", captured, err)
		}
		if err := node.PersistReady(); err != nil {
			t.Fatal(err)
		}
		if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := node.InstallSnapshot(); err != nil {
			t.Fatal(err)
		}
		if err := node.ApplyCommitted(
			&workspace, func(raftmodel.AppliedNormalBatch) error { return nil },
		); err != nil {
			t.Fatal(err)
		}
		if _, err := node.RecordReadStates(); err != nil {
			t.Fatal(err)
		}
		if err := node.AdvanceReady(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("memory Node did not drain")
}

func memoryRuntimeAtApplyBoundary(
	t *testing.T,
	commands [][]byte,
) (*Runtime, *runtimeMemoryBatchMachine) {
	t.Helper()
	stable, err := raftsim.NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	machine := newRuntimeMemoryBatchMachine(t, stable)
	node, err := raftmodel.NewNode(1, 1, stable, machine)
	if err != nil {
		t.Fatal(err)
	}
	drainRuntimeMemoryNode(t, node)
	if err := node.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntimeMemoryNode(t, node)
	for index := range commands {
		if err := node.Propose(commands[index]); err != nil {
			t.Fatalf("Propose %d = %v", index, err)
		}
	}
	for step := 0; step < 1000; step++ {
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("CaptureReady committed batch = %v, %v", captured, err)
		}
		progress, ok := node.CurrentReady()
		if !ok {
			t.Fatal("captured Ready has no progress")
		}
		if err := node.PersistReady(); err != nil {
			t.Fatal(err)
		}
		if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := node.InstallSnapshot(); err != nil {
			t.Fatal(err)
		}
		if progress.CommittedCount != 0 {
			if progress.CommittedCount != len(commands) {
				t.Fatalf("committed batch count = %d, want %d", progress.CommittedCount, len(commands))
			}
			return &Runtime{
				wal: &raftstore.Store{}, database: &sqldriver.Database{},
				apply: &sqldriver.ReplicatedApply{}, node: node,
				identity: RuntimeIdentity{MemberID: 1},
			}, machine
		}
		if err := node.ApplyCommitted(
			new(raftmodel.NormalApplyBatchWorkspace),
			func(raftmodel.AppliedNormalBatch) error { return nil },
		); err != nil {
			t.Fatal(err)
		}
		if _, err := node.RecordReadStates(); err != nil {
			t.Fatal(err)
		}
		if err := node.AdvanceReady(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("committed memory batch did not reach apply boundary")
	return nil, nil
}

func driveRuntimeToNormalApplyBoundary(
	t *testing.T,
	runtime *Runtime,
	workspace *ReadyWorkspace,
) {
	t.Helper()
	for step := 0; step < 1000; step++ {
		if runtime.node.Phase() == raftmodel.PhaseSnapshotInstalled {
			requiresSettlement, err := runtime.node.NextApplyRequiresResultSettlement()
			if err != nil {
				t.Fatalf("normal apply preflight step %d = %v", step, err)
			}
			if requiresSettlement {
				return
			}
		}
		result, err := runtime.DriveReady(
			workspace, func(OutboundMessage) error { return nil }, settleTestApplied,
		)
		if err != nil {
			t.Fatalf("DriveReady to apply boundary step %d = %+v, %v", step, result, err)
		}
		if !result.Progressed() {
			t.Fatal("Runtime became idle before the apply boundary")
		}
	}
	t.Fatal("Runtime did not reach a normal apply boundary")
}

func TestRuntimeDoesNotEmbedReadyBatchWorkspace(t *testing.T) {
	runtimeBytes := unsafe.Sizeof(Runtime{})
	workspaceBytes := unsafe.Sizeof(ReadyWorkspace{})
	if workspaceBytes == 0 || runtimeBytes >= workspaceBytes {
		t.Fatalf("Runtime=%dB ReadyWorkspace=%dB", runtimeBytes, workspaceBytes)
	}
	t.Logf("Runtime=%dB caller-owned ReadyWorkspace=%dB AppliedBatch=%dB",
		runtimeBytes, workspaceBytes, unsafe.Sizeof(AppliedBatch{}))
}

func TestRuntimeInMemoryBatchDriverAndSettlementGate(t *testing.T) {
	commands := make([][]byte, 8)
	for index := range commands {
		commands[index] = []byte{0x80, byte(index), 0, byte(index + 1)}
	}
	runtime, machine := memoryRuntimeAtApplyBoundary(t, commands)
	beforeApplied := machine.Applied()
	beforeBatchCalls := machine.batchCalls
	var workspace ReadyWorkspace
	if result, err := runtime.DriveReady(&workspace, nil, nil); result.Progressed() ||
		!errors.Is(err, ErrResultSettlementRequired) || machine.Applied() != beforeApplied {
		t.Fatalf("nil settlement preflight = %+v applied %d, %v",
			result, machine.Applied(), err)
	}

	wantSinkErr := errors.New("memory sink outcome unknown")
	sinkCalls := 0
	var wantReady, wantFirst, wantLast uint64
	if result, err := runtime.DriveReady(&workspace, nil, func(batch AppliedBatch) error {
		sinkCalls++
		if batch.Len() != len(commands) {
			t.Fatalf("memory applied batch len = %d", batch.Len())
		}
		wantReady, wantFirst, wantLast = batch.ReadyID(), batch.FirstIndex(), batch.LastIndex()
		for index := range commands {
			entry, ok := batch.Entry(index)
			if !ok || !bytes.Equal(entry.Data, commands[index]) {
				t.Fatalf("memory entry %d = %+v, %v", index, entry, ok)
			}
		}
		return RetryResultSettlement(wantSinkErr)
	}); result.Progressed() || !errors.Is(err, ErrResultSettlementRejected) ||
		!errors.Is(err, wantSinkErr) {
		t.Fatalf("memory failed settlement = %+v, %v", result, err)
	}
	if sinkCalls != 1 || machine.batchCalls != beforeBatchCalls+1 ||
		machine.Applied() != beforeApplied+uint64(len(commands)) {
		t.Fatalf("memory pending apply = sink %d batches %d applied %d",
			sinkCalls, machine.batchCalls, machine.Applied())
	}
	progress, ok := runtime.node.CurrentReady()
	if !ok || !progress.SettlementPending || progress.CommittedApplied != len(commands) {
		t.Fatalf("memory pending progress = %+v, %v", progress, ok)
	}
	if err := runtime.Propose(commands[0]); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("memory proposal during settlement = %v", err)
	}
	if err := runtime.ReadIndex([]byte("blocked")); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("memory read during settlement = %v", err)
	}
	if _, err := runtime.SnapshotState(); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("memory snapshot during settlement = %v", err)
	}
	if err := runtime.Close(); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("memory close during settlement = %v", err)
	}

	retryCalls := 0
	result, err := runtime.DriveReady(&workspace, nil, func(batch AppliedBatch) error {
		retryCalls++
		if batch.ReadyID() != wantReady || batch.FirstIndex() != wantFirst ||
			batch.LastIndex() != wantLast || batch.Len() != len(commands) {
			t.Fatalf("memory retry = ready %d range %d..%d len %d",
				batch.ReadyID(), batch.FirstIndex(), batch.LastIndex(), batch.Len())
		}
		return nil
	})
	if err != nil || result.Kind != DriveNormalBatch ||
		result.Applied.Len() != len(commands) || retryCalls != 1 ||
		machine.batchCalls != beforeBatchCalls+1 {
		t.Fatalf("memory settlement retry = %+v calls %d batches %d, %v",
			result, retryCalls, machine.batchCalls, err)
	}
	if runtime.node.Phase() != raftmodel.PhaseEntriesApplied {
		t.Fatalf("memory settled phase = %s", runtime.node.Phase())
	}
	rawApplied := runtime.node.Status().Applied
	publishedApplied := runtime.node.PublishedApplied()
	status, statusErr := runtime.Status()
	if statusErr != nil || rawApplied >= publishedApplied ||
		publishedApplied != wantLast || status.Applied != publishedApplied ||
		status.Commit < status.Applied {
		t.Fatalf("pre-Advance status=%+v err=%v raw=%d published=%d want=%d",
			status, statusErr, rawApplied, publishedApplied, wantLast)
	}
}

func TestRuntimeUnclassifiedSettlementFailureIsTerminal(t *testing.T) {
	runtime, _ := memoryRuntimeAtApplyBoundary(t, [][]byte{{0x80, 1, 2, 3}})
	var workspace ReadyWorkspace
	want := errors.New("unclassified settlement failure")
	result, err := runtime.DriveReady(&workspace, nil, func(AppliedBatch) error {
		return want
	})
	if result.Progressed() || !errors.Is(err, ErrRuntimeFailed) ||
		!errors.Is(err, want) || errors.Is(err, ErrResultSettlementRejected) {
		t.Fatalf("terminal settlement = %+v, %v", result, err)
	}
	if failure := runtime.Failure(); !errors.Is(failure, want) ||
		!errors.Is(failure, ErrRuntimeFailed) {
		t.Fatalf("latched failure = %v", failure)
	}
	if runtime.HasPendingResultSettlement() {
		t.Fatal("terminal settlement failure remained a retryable close gate")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("terminal settlement close = %v", err)
	}
	if !runtime.closed || runtime.node != nil || runtime.apply != nil ||
		runtime.database != nil || runtime.wal != nil {
		t.Fatalf("terminal settlement retained resources: %+v", runtime)
	}
}

func TestRuntimeAppliedBatchSettlementFailureIsRetryableHardGate(t *testing.T) {
	fixture := newRuntimeFixture(t, 181, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	command := testApplySessionOpen(fixture.base)
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatal(err)
	}
	var workspace ReadyWorkspace
	driveRuntimeToNormalApplyBoundary(t, fixture.runtime, &workspace)
	beforeApplied := fixture.apply.Applied()
	if result, err := fixture.runtime.DriveReady(&workspace, nil, nil); result.Progressed() ||
		!errors.Is(err, ErrResultSettlementRequired) {
		t.Fatalf("nil settlement preflight = %+v, %v", result, err)
	}
	beforeProgress, beforeOK := fixture.runtime.node.CurrentReady()
	if fixture.apply.Applied() != beforeApplied || !beforeOK || beforeProgress.CommittedApplied != 0 {
		t.Fatalf("nil sink advanced apply to %d at progress %+v",
			fixture.apply.Applied(), beforeProgress)
	}

	wantSinkErr := errors.New("injected settlement outcome unknown")
	var firstReadyID, firstIndex, lastIndex uint64
	var firstCompletion []byte
	failedSink := func(batch AppliedBatch) error {
		if batch.Len() != 1 || batch.FirstIndex() == 0 ||
			batch.LastIndex() != batch.FirstIndex() ||
			batch.FinalPublication().Applied != batch.LastIndex() {
			t.Fatalf("pending batch = len %d range %d..%d publication %d",
				batch.Len(), batch.FirstIndex(), batch.LastIndex(),
				batch.FinalPublication().Applied)
		}
		entry, ok := batch.Entry(0)
		if !ok || !bytes.Equal(entry.Data, command) {
			t.Fatalf("pending entry = %+v, %v", entry, ok)
		}
		lookup, hasCommand, err := batch.LookupCompletion(0)
		if err != nil || !hasCommand {
			t.Fatalf("pending completion = %+v, %v, %v", lookup, hasCommand, err)
		}
		firstReadyID = batch.ReadyID()
		firstIndex = batch.FirstIndex()
		lastIndex = batch.LastIndex()
		firstCompletion = bytes.Clone(lookup.Bytes)
		return RetryResultSettlement(wantSinkErr)
	}
	if result, err := fixture.runtime.DriveReady(&workspace, nil, failedSink); result.Progressed() ||
		!errors.Is(err, ErrResultSettlementRejected) || !errors.Is(err, wantSinkErr) {
		t.Fatalf("failed settlement = %+v, %v", result, err)
	}
	if !fixture.runtime.HasPendingResultSettlement() {
		t.Fatal("retryable settlement did not retain the close gate")
	}
	afterProgress, afterOK := fixture.runtime.node.CurrentReady()
	pendingBatch, pending := fixture.runtime.pendingAppliedResults()
	if fixture.apply.Applied() != beforeApplied+1 || !afterOK ||
		afterProgress.CommittedApplied != 1 || !pending || pendingBatch.Len() != 1 {
		t.Fatalf("pending published cut = apply %d progress %+v pending %d",
			fixture.apply.Applied(), afterProgress, pendingBatch.Len())
	}
	progress, ok := fixture.runtime.node.CurrentReady()
	if !ok || !progress.SettlementPending || fixture.runtime.node.Phase() != raftmodel.PhaseSnapshotInstalled {
		t.Fatalf("Node settlement gate = %+v, %v phase %s",
			progress, ok, fixture.runtime.node.Phase())
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("proposal during settlement = %v", err)
	}
	if err := fixture.runtime.ReadIndex([]byte("blocked")); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("ReadIndex during settlement = %v", err)
	}
	if _, err := fixture.runtime.SnapshotState(); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("snapshot cut during settlement = %v", err)
	}
	if _, err := fixture.runtime.WALRetentionInput(); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("retention cut during settlement = %v", err)
	}
	if err := fixture.runtime.Close(); !errors.Is(err, ErrResultSettlementPending) {
		t.Fatalf("close during settlement = %v", err)
	}
	if result, err := fixture.runtime.DriveReady(&workspace, nil, nil); result.Progressed() ||
		!errors.Is(err, ErrResultSettlementRequired) {
		t.Fatalf("nil pending retry = %+v, %v", result, err)
	}

	retryCalls := 0
	result, err := fixture.runtime.DriveReady(&workspace, nil, func(batch AppliedBatch) error {
		retryCalls++
		if batch.ReadyID() != firstReadyID || batch.FirstIndex() != firstIndex ||
			batch.LastIndex() != lastIndex || batch.Len() != 1 {
			t.Fatalf("retried range = ready %d range %d..%d len %d",
				batch.ReadyID(), batch.FirstIndex(), batch.LastIndex(), batch.Len())
		}
		lookup, hasCommand, lookupErr := batch.LookupCompletion(0)
		if lookupErr != nil || !hasCommand || !bytes.Equal(lookup.Bytes, firstCompletion) {
			t.Fatalf("retried completion = %+v, %v, %v", lookup, hasCommand, lookupErr)
		}
		return nil
	})
	if err != nil || result.Kind != DriveNormalBatch || result.Applied.Len() != 1 ||
		result.Applied.FirstIndex() != firstIndex || retryCalls != 1 {
		t.Fatalf("settlement retry = %+v calls %d, %v", result, retryCalls, err)
	}
	if _, pending := fixture.runtime.pendingAppliedResults(); pending ||
		fixture.runtime.node.Phase() != raftmodel.PhaseEntriesApplied {
		t.Fatalf("settled state = pending %v phase %s",
			pending, fixture.runtime.node.Phase())
	}
}

func TestRuntimeBatchesEightCommittedCommandsIntoCapturedResultRanges(t *testing.T) {
	fixture := newRuntimeFixture(t, 182, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	const commandCount = 8
	var epochs [commandCount]uint64
	for index := range commandCount {
		open := runtimeReplaySessionOpen(fixture.base, byte(index+10))
		if err := fixture.runtime.Propose(open); err != nil {
			t.Fatalf("propose session %d = %v", index, err)
		}
		drainRuntime(t, fixture.runtime, nil)
		lookup, err := fixture.apply.LookupCompletion(open)
		if err != nil {
			t.Fatalf("lookup session %d = %v", index, err)
		}
		completion, err := replication.OpenCompletion(lookup.Bytes)
		if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened {
			t.Fatalf("session %d completion = %+v, %v", index, completion, err)
		}
		epochs[index] = completion.ClientEpoch
	}

	commands := make([][]byte, commandCount)
	for index := range commandCount {
		jsonKey := []byte(fmt.Sprintf("\"batch-%d\"", index%5))
		key, ok := orderedkey.AppendJSONString(nil, jsonKey, orderedkey.Ascending)
		if !ok {
			t.Fatalf("encode key %d", index)
		}
		document := []byte(fmt.Sprintf(`{"id":"batch-%d","value":%d}`, index%5, index))
		command := testApplyCommandValue(fixture.base, epochs[index], 2, key, document)
		command.ClientID = replication.ID128{byte(index + 10)}
		command.Fingerprint = sha256.Sum256([]byte{0xb8, byte(index)})
		encoded, err := replication.AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		commands[index] = encoded
	}
	// Every command updates one same-sized session header and adds one fixed
	// retry slot. Derive the exact legal prefix from that durable geometry and
	// the authenticated system profile, whose control-row bounds can evolve.
	expectedPrefixEntries := runtimeBatchExpectedPrefix(t, fixture, commands)
	if expectedPrefixEntries < 2 || expectedPrefixEntries >= commandCount {
		t.Fatalf("fixture must exercise multiple non-singleton prefixes, got maximum %d", expectedPrefixEntries)
	}
	beforeApplied := fixture.apply.Applied()
	beforeStats, err := fixture.apply.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	for index := range commands {
		if err := fixture.runtime.Propose(commands[index]); err != nil {
			t.Fatalf("propose command %d = %v", index, err)
		}
	}

	var workspace ReadyWorkspace
	settlementRanges := 0
	settledCommands := 0
	nextApplied := beforeApplied + 1
	for step := 0; step < 1000; step++ {
		stepStats, err := fixture.apply.DurabilityStats()
		if err != nil {
			t.Fatal(err)
		}
		result, err := fixture.runtime.DriveReady(
			&workspace,
			func(OutboundMessage) error { return nil },
			func(batch AppliedBatch) error {
				settlementRanges++
				if batch.Len() != min(expectedPrefixEntries, commandCount-settledCommands) ||
					batch.LastIndex()-batch.FirstIndex()+1 != uint64(batch.Len()) ||
					batch.FirstIndex() != nextApplied ||
					batch.FinalPublication().Applied != batch.LastIndex() ||
					settledCommands+batch.Len() > commandCount {
					t.Fatalf("settlement batch = len %d range %d..%d",
						batch.Len(), batch.FirstIndex(), batch.LastIndex())
				}
				for index := 0; index < batch.Len(); index++ {
					entry, ok := batch.Entry(index)
					commandIndex := settledCommands + index
					if !ok || !bytes.Equal(entry.Data, commands[commandIndex]) {
						t.Fatalf("settlement entry %d = %+v, %v", index, entry, ok)
					}
					lookup, hasCommand, lookupErr := batch.LookupCompletion(index)
					if lookupErr != nil || !hasCommand {
						t.Fatalf("completion %d = %+v, %v, %v",
							index, lookup, hasCommand, lookupErr)
					}
					completion, openErr := replication.OpenCompletion(lookup.Bytes)
					command, commandErr := replication.OpenCommand(commands[commandIndex])
					if openErr != nil || commandErr != nil ||
						completion.ResultCode != replicatedstate.ResultApplied ||
						completion.ClientID != command.ClientID ||
						completion.ClientEpoch != command.ClientEpoch ||
						completion.ClientSequence != command.ClientSequence ||
						completion.Fingerprint != command.Fingerprint ||
						completion.AppliedSequence != batch.FirstIndex()+uint64(index) {
						t.Fatalf("completion %d envelope = %+v, completion %v command %v",
							index, completion, openErr, commandErr)
					}
				}
				settledCommands += batch.Len()
				nextApplied = batch.LastIndex() + 1
				return nil
			},
		)
		if err != nil {
			t.Fatalf("DriveReady batch step %d = %+v, %v", step, result, err)
		}
		if result.Kind == DriveNormalBatch {
			afterStepStats, statsErr := fixture.apply.DurabilityStats()
			if statsErr != nil {
				t.Fatal(statsErr)
			}
			if result.Applied.Len() != min(expectedPrefixEntries, int(beforeApplied+commandCount-result.Applied.FirstIndex()+1)) ||
				afterStepStats.Updates != stepStats.Updates+1 ||
				afterStepStats.TransactionHighWater != stepStats.TransactionHighWater+1 ||
				afterStepStats.LargestUpdateSpan < uint64(result.Applied.Len()) {
				t.Fatalf("captured prefix transaction = range %d..%d before %+v after %+v",
					result.Applied.FirstIndex(), result.Applied.LastIndex(), stepStats, afterStepStats)
			}
			if settledCommands == commandCount {
				break
			}
		}
		if result.Kind == DriveIdle {
			t.Fatal("Runtime became idle before applying the command batch")
		}
	}
	if settledCommands != commandCount ||
		settlementRanges != (commandCount+expectedPrefixEntries-1)/expectedPrefixEntries ||
		settlementRanges >= commandCount || nextApplied != beforeApplied+commandCount+1 {
		t.Fatalf("settled commands = %d ranges %d next applied %d",
			settledCommands, settlementRanges, nextApplied)
	}
	if fixture.apply.Applied() != beforeApplied+commandCount {
		t.Fatalf("journal-durable apply = %d, want %d", fixture.apply.Applied(), beforeApplied+commandCount)
	}
	afterStats, err := fixture.apply.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	if afterStats.Updates != beforeStats.Updates+uint64(settlementRanges) ||
		afterStats.TransactionHighWater != beforeStats.TransactionHighWater+uint64(settlementRanges) ||
		afterStats.Updates-beforeStats.Updates >= commandCount {
		t.Fatalf("one transaction per captured prefix = ranges %d before %+v after %+v",
			settlementRanges, beforeStats, afterStats)
	}
	// Settlement follows each journal-durable group commit, not a checkpoint
	// fold. Only after checking those exact commits, seal their final cut and
	// require the resulting certificate to cover every settled command.
	drainRuntime(t, fixture.runtime, nil)
	preparation, err := fixture.apply.CaptureWALBase(sqldriver.WALBaseCaptureOptions{
		Workspace: make([]byte, 0, replicatedstate.MaxSnapshotArtifactChunkBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := preparation.GenerationInput()
	if err != nil || input.Snapshot.GetMetadata().GetIndex() != beforeApplied+commandCount ||
		fixture.apply.Applied() != beforeApplied+commandCount ||
		fixture.apply.CheckpointAppliedIndex() != beforeApplied+commandCount {
		t.Fatalf("sealed apply/certificate = %d/%d, want %d: %v",
			fixture.apply.Applied(), fixture.apply.CheckpointAppliedIndex(), beforeApplied+commandCount, err)
	}
	t.Logf("settled %d commands as %d exact committed prefixes of at most %d entries",
		settledCommands, settlementRanges, expectedPrefixEntries)
}

func runtimeBatchExpectedPrefix(t testing.TB, fixture runtimeFixture, commands [][]byte) int {
	t.Helper()
	cut, err := fixture.apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	type rowBytes struct{ session, slot int }
	rows := make(map[[32]byte]rowBytes, len(commands))
	stateBytes := 0
	err = cut.RangeSystem(func(key, value []byte) error {
		if _, err := replicatedstate.OpenState(value); err == nil {
			stateBytes = len(key) + len(value)
		} else if record, err := replicatedstate.OpenSessionRecord(value); err == nil {
			row := rows[record.Digest]
			row.session = len(key) + len(value)
			rows[record.Digest] = row
		} else if slot, err := replicatedstate.OpenSessionSlot(value); err == nil {
			row := rows[slot.SessionDigest]
			if row.slot != 0 {
				return errors.New("batch fixture has more than its opening retry slot")
			}
			row.slot = len(key) + len(value)
			rows[slot.SessionDigest] = row
		}
		return nil
	})
	if err != nil || stateBytes == 0 {
		t.Fatalf("read exact system geometry: state=%d err=%v", stateBytes, err)
	}
	commandBytes := 0
	userBytes := 0
	for _, raw := range commands {
		command, err := replication.OpenCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		row := rows[replicatedstate.SessionKey(command.AuthorityClass, command.Tenant, command.ClientID)]
		if row.session == 0 || row.slot == 0 || commandBytes != 0 && commandBytes != row.session+row.slot {
			t.Fatalf("fixture command system geometry differs: %+v want %d", row, commandBytes)
		}
		commandBytes = row.session + row.slot
		// Canonical command bytes include every user key/value, making their sum
		// a conservative upper bound for proving other profiles non-limiting.
		userBytes += len(raw)
	}
	count := len(commands)
	if fixture.base.UserLimits.MaxBatchDocuments < count || fixture.base.UserLimits.MaxBatchBytes < userBytes ||
		fixture.applyID.TxnLimits.MaxCollections < 2 || fixture.applyID.TxnLimits.MaxDocuments < 1+3*count ||
		fixture.applyID.TxnLimits.MaxBytes < int64(stateBytes+commandBytes*count+userBytes) {
		t.Fatal("fixture has another tighter bound than the system profile")
	}
	return runtimeBatchSystemPrefix(fixture.applyID.SystemLimits, stateBytes, commandBytes, count)
}

func runtimeBatchSystemPrefix(limits sqldriver.ReplicatedShardStoreLimits, stateBytes, commandBytes, count int) int {
	if stateBytes <= 0 || commandBytes <= 0 || limits.MaxBatchDocuments < 1 || limits.MaxBatchBytes < stateBytes {
		return 0
	}
	return min(count, (limits.MaxBatchDocuments-1)/2, (limits.MaxBatchBytes-stateBytes)/commandBytes)
}

func TestRuntimeBatchSystemPrefixRespectsExactProfile(t *testing.T) {
	for _, test := range []struct{ documents, bytes, want int }{
		{10, 1000, 4}, {18, 350, 6}, {18, 450, 8}, {18, 349, 5}, {18, 49, 0}, {1, 1000, 0},
	} {
		limits := sqldriver.ReplicatedShardStoreLimits{MaxBatchDocuments: test.documents, MaxBatchBytes: test.bytes}
		if got := runtimeBatchSystemPrefix(limits, 50, 50, 8); got != test.want {
			t.Fatalf("limits=%+v prefix=%d want%d", limits, got, test.want)
		}
	}
}
