package raftmodel

import (
	"crypto/sha256"
	"errors"
	"slices"
	"testing"
	"unsafe"

	pb "go.etcd.io/raft/v3/raftpb"
)

type nodeBatchStateMachine struct {
	*fakeStateMachine
	candidates           []int
	maxApplied           int
	cleanBoundary        bool
	cleanBoundaryWitness bool
	corruptTailWitness   bool
	corruptTrailingNoop  bool
}

func (machine *nodeBatchStateMachine) ApplyNormalBatch(
	entries []NormalApply,
	witnesses [][32]byte,
) (int, Publication, error) {
	machine.candidates = append(machine.candidates, len(entries))
	clear(witnesses)
	if machine.cleanBoundary {
		if machine.cleanBoundaryWitness && len(witnesses) != 0 {
			witnesses[0] = sha256.Sum256([]byte("invalid-clean-boundary-witness"))
		}
		return 0, Publication{}, nil
	}
	applied := len(entries)
	if machine.maxApplied > 0 && applied > machine.maxApplied {
		applied = machine.maxApplied
	}
	for index := 0; index < applied; index++ {
		publication, err := machine.ApplyNormal(entries[index].Meta, entries[index].Data)
		if err != nil {
			return 0, Publication{}, err
		}
		witnesses[index] = publication.DataChainDigest
	}
	if machine.corruptTrailingNoop && applied > 1 && len(entries[applied-1].Data) == 0 {
		invalid := sha256.Sum256([]byte("invalid-trailing-noop-witness"))
		witnesses[applied-1] = invalid
		machine.pub.DataChainDigest = invalid
	}
	if machine.corruptTailWitness && applied < len(entries) {
		witnesses[applied] = sha256.Sum256([]byte("invalid-unselected-witness"))
	}
	return applied, machine.Published(), nil
}

func newDirectNodeBatch(
	t *testing.T,
	data ...[]byte,
) (*Node, *nodeBatchStateMachine) {
	t.Helper()
	node, _, base := newTestNode(t, 1, []uint64{1})
	machine := &nodeBatchStateMachine{fakeStateMachine: base}
	node.machine = machine
	node.phase = PhaseSnapshotInstalled
	node.readyID = 7
	node.ready.CommittedEntries = make([]*pb.Entry, len(data))
	term := uint64(2)
	for index := range data {
		entryIndex := base.pub.Applied + uint64(index) + 1
		node.ready.CommittedEntries[index] = &pb.Entry{
			Type: pb.EntryType_EntryNormal.Enum(), Term: &term, Index: &entryIndex,
			Data: slices.Clone(data[index]),
		}
	}
	return node, machine
}

func settleNodeBatch(t *testing.T, node *Node, batch AppliedNormalBatch) {
	t.Helper()
	if err := node.SettleAppliedNormalBatch(batch); err != nil {
		t.Fatalf("SettleAppliedNormalBatch = %v", err)
	}
}

func TestNodeBatchWorkspaceIsCallerOwnedDensity(t *testing.T) {
	wantWorkspace := uintptr(MaxNormalApplyBatchEntries) *
		(unsafe.Sizeof(NormalApply{}) + unsafe.Sizeof([32]byte{}))
	workspaceBytes := unsafe.Sizeof(NormalApplyBatchWorkspace{})
	nodeBytes := unsafe.Sizeof(Node{})
	if workspaceBytes != wantWorkspace || nodeBytes >= workspaceBytes {
		t.Fatalf("batch density = Node %dB workspace %dB want workspace %dB",
			nodeBytes, workspaceBytes, wantWorkspace)
	}
	t.Logf("Node=%dB caller-owned NormalApplyBatchWorkspace=%dB", nodeBytes, workspaceBytes)
}

func TestNodeApplyNextBatchSplits128PlusOneAndGatesSettlement(t *testing.T) {
	data := make([][]byte, MaxNormalApplyBatchEntries+1)
	for index := range data {
		data[index] = []byte{byte(index + 1)}
	}
	node, machine := newDirectNodeBatch(t, data...)
	var workspace NormalApplyBatchWorkspace

	first, err := node.ApplyNextBatch(&workspace)
	if err != nil || first.Applied != MaxNormalApplyBatchEntries ||
		first.Normal.Len() != MaxNormalApplyBatchEntries || first.Normal.FirstIndex() != 2 ||
		first.Normal.LastIndex() != 1+MaxNormalApplyBatchEntries {
		t.Fatalf("first ApplyNextBatch = %+v, %v", first, err)
	}
	progress, ok := node.CurrentReady()
	if !ok || !progress.SettlementPending || progress.CommittedApplied != MaxNormalApplyBatchEntries {
		t.Fatalf("pending progress = %+v, %v", progress, ok)
	}
	if _, err := node.ApplyNextBatch(&workspace); !errors.Is(err, ErrAppliedSettlementPending) {
		t.Fatalf("second apply before settlement = %v", err)
	}
	if _, err := node.RecordNextReadState(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("read-state before settlement = %v", err)
	}
	if err := node.AdvanceReady(); !errors.Is(err, ErrWrongPhase) {
		t.Fatalf("AdvanceReady before settlement = %v", err)
	}
	settleNodeBatch(t, node, first.Normal)
	if node.Phase() != PhaseSnapshotInstalled {
		t.Fatalf("phase with one entry remaining = %s", node.Phase())
	}

	second, err := node.ApplyNextBatch(&workspace)
	if err != nil || second.Applied != 1 || second.Normal.Len() != 1 ||
		second.Normal.FirstIndex() != uint64(MaxNormalApplyBatchEntries+2) {
		t.Fatalf("second ApplyNextBatch = %+v, %v", second, err)
	}
	settleNodeBatch(t, node, second.Normal)
	if node.Phase() != PhaseEntriesApplied ||
		!slices.Equal(machine.candidates, []int{MaxNormalApplyBatchEntries, 1}) {
		t.Fatalf("final phase/candidates = %s/%v", node.Phase(), machine.candidates)
	}
}

func TestNodeApplyNextBatchStopsAtBytesWithoutDroppingLegalFirst(t *testing.T) {
	firstData := make([]byte, MaxNormalApplyBatchBytes)
	node, machine := newDirectNodeBatch(t, firstData, []byte{1})
	var workspace NormalApplyBatchWorkspace
	first, err := node.ApplyNextBatch(&workspace)
	if err != nil || first.Applied != 1 || first.Normal.Len() != 1 ||
		!slices.Equal(machine.candidates, []int{1}) {
		t.Fatalf("maximum first entry = %+v candidates %v, %v", first, machine.candidates, err)
	}
	settleNodeBatch(t, node, first.Normal)
}

func TestNodeApplyNextBatchStopsBeforeConfigurationAndSupportsPartialPrefix(t *testing.T) {
	node, machine := newDirectNodeBatch(t, []byte("one"), []byte("two"), []byte("three"))
	node.ready.CommittedEntries[2].Type = pb.EntryType_EntryConfChange.Enum()
	machine.maxApplied = 1
	var workspace NormalApplyBatchWorkspace

	first, err := node.ApplyNextBatch(&workspace)
	if err != nil || first.Applied != 1 || first.Normal.Len() != 1 ||
		!slices.Equal(machine.candidates, []int{2}) {
		t.Fatalf("partial prefix = %+v candidates %v, %v", first, machine.candidates, err)
	}
	settleNodeBatch(t, node, first.Normal)
	second, err := node.ApplyNextBatch(&workspace)
	if err != nil || second.Applied != 1 || second.Normal.Len() != 1 ||
		!slices.Equal(machine.candidates, []int{2, 1}) {
		t.Fatalf("second prefix = %+v candidates %v, %v", second, machine.candidates, err)
	}
	settleNodeBatch(t, node, second.Normal)
	requires, err := node.NextApplyRequiresResultSettlement()
	if err != nil || requires {
		t.Fatalf("configuration boundary preflight = %v, %v", requires, err)
	}
}

func TestNodeApplyNextBatchCleanBoundaryFallsBackToSettledSingleton(t *testing.T) {
	node, machine := newDirectNodeBatch(t, []byte("ownership"), []byte("later"))
	machine.cleanBoundary = true
	var workspace NormalApplyBatchWorkspace
	result, err := node.ApplyNextBatch(&workspace)
	if err != nil || result.Applied != 1 || result.Normal.Len() != 1 ||
		!slices.Equal(machine.candidates, []int{2}) || len(machine.calls) != 1 {
		t.Fatalf("clean-boundary fallback = %+v candidates %v calls %d, %v",
			result, machine.candidates, len(machine.calls), err)
	}
	settleNodeBatch(t, node, result.Normal)
}

func TestNodeApplyNextBatchRejectsUnselectedAndCleanBoundaryWitnesses(t *testing.T) {
	t.Run("unselected", func(t *testing.T) {
		node, machine := newDirectNodeBatch(t, []byte("one"), []byte("two"))
		machine.maxApplied = 1
		machine.corruptTailWitness = true
		var workspace NormalApplyBatchWorkspace
		if result, err := node.ApplyNextBatch(&workspace); err == nil ||
			result.Applied != 0 || result.Normal.Len() != 0 {
			t.Fatalf("faulty unselected witness = %+v, %v", result, err)
		}
		if node.Phase() != PhaseFailed || node.entryPos != 0 || machine.pub.Applied != 2 {
			t.Fatalf("fault cut = phase %s entryPos %d machine %d",
				node.Phase(), node.entryPos, machine.pub.Applied)
		}
	})

	t.Run("clean-boundary", func(t *testing.T) {
		node, machine := newDirectNodeBatch(t, []byte("ownership"))
		machine.cleanBoundary = true
		machine.cleanBoundaryWitness = true
		var workspace NormalApplyBatchWorkspace
		if result, err := node.ApplyNextBatch(&workspace); err == nil ||
			result.Applied != 0 || result.Normal.Len() != 0 {
			t.Fatalf("faulty clean witness = %+v, %v", result, err)
		}
		if node.Phase() != PhaseFailed || node.entryPos != 0 || len(machine.calls) != 0 {
			t.Fatalf("clean boundary fault = phase %s entryPos %d calls %d",
				node.Phase(), node.entryPos, len(machine.calls))
		}
	})
}

func TestNodeApplyNextBatchRejectsMixedCommandTrailingNoopWitness(t *testing.T) {
	node, machine := newDirectNodeBatch(t, []byte("command"), nil)
	machine.corruptTrailingNoop = true
	var workspace NormalApplyBatchWorkspace
	if result, err := node.ApplyNextBatch(&workspace); err == nil ||
		result.Applied != 0 || result.Normal.Len() != 0 {
		t.Fatalf("faulty trailing no-op witness = %+v, %v", result, err)
	}
	if node.Phase() != PhaseFailed || node.entryPos != 0 || machine.pub.Applied != 3 {
		t.Fatalf("trailing no-op fault = phase %s entryPos %d machine %d",
			node.Phase(), node.entryPos, machine.pub.Applied)
	}
}
