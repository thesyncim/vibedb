package raftmodel

import (
	"bytes"
	"errors"
	"testing"
)

type nodeCompletionMachine struct {
	*nodeBatchStateMachine
	result []byte
	err    error
}

func (m *nodeCompletionMachine) ApplyNormalWithCompletion(meta ApplyMeta, data []byte) (Publication, []byte, error) {
	pub, err := m.ApplyNormal(meta, data)
	return pub, m.result, errors.Join(err, m.err)
}

func TestNodeOriginalCompletionLivesOnlyUntilSettlement(t *testing.T) {
	node, base := newDirectNodeBatch(t, []byte("first"), []byte("second"))
	base.cleanBoundary = true
	machine := &nodeCompletionMachine{nodeBatchStateMachine: base, result: []byte("original")}
	node.machine = machine
	var workspace NormalApplyBatchWorkspace
	result, err := node.ApplyNextBatch(&workspace)
	if err != nil || result.Applied != 1 {
		t.Fatalf("apply: %+v, %v", result, err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		pending, ok := node.PendingAppliedNormalBatch()
		completion, found := pending.Completion(0)
		if !ok || !found || !bytes.Equal(completion, machine.result) {
			t.Fatalf("pending attempt %d: %q, %v/%v", attempt, completion, ok, found)
		}
		if _, found := pending.Completion(1); found {
			t.Fatal("completion escaped its singleton entry")
		}
		if _, err := node.ApplyNextBatch(&workspace); !errors.Is(err, ErrAppliedSettlementPending) {
			t.Fatalf("apply crossed settlement boundary: %v", err)
		}
	}
	settleNodeBatch(t, node, result.Normal)
	if node.settlementCompletion != nil {
		t.Fatal("settlement retained original result storage")
	}
	machine.result = nil
	result, err = node.ApplyNextBatch(&workspace)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := result.Normal.Completion(0); found {
		t.Fatal("next apply inherited prior completion")
	}
	settleNodeBatch(t, node, result.Normal)
}

func TestNodeRejectsInvalidOriginalCompletionBeforeSettlement(t *testing.T) {
	for _, test := range []struct {
		name         string
		data, result []byte
		err          error
	}{
		{"oversized", []byte("entry"), make([]byte, MaxNormalApplyCompletionBytes+1), nil},
		{"oversized_backing", []byte("entry"), make([]byte, 1, MaxNormalApplyCompletionBytes+1), nil},
		{"no_op", nil, []byte("not a result"), nil},
		{"failed_apply", []byte("entry"), []byte("not durable"), errors.New("apply failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			node, base := newDirectNodeBatch(t, test.data)
			node.machine = &nodeCompletionMachine{nodeBatchStateMachine: base, result: test.result, err: test.err}
			if result, err := node.ApplyNextBatch(nil); err == nil || result.Applied != 0 {
				t.Fatalf("invalid result exposed: %+v, %v", result, err)
			}
			if _, found := node.PendingAppliedNormalBatch(); found || node.settlementCompletion != nil {
				t.Fatal("failed apply exposed original completion")
			}
		})
	}
}
