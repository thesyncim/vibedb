package raftmember

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func (machine *runtimeMemoryBatchMachine) ApplyNormalWithCompletion(
	meta raftmodel.ApplyMeta, data []byte,
) (raftmodel.Publication, []byte, error) {
	publication, err := machine.ApplyNormal(meta, data)
	return publication, bytes.Clone(machine.originalCompletion), err
}

func TestRuntimeOriginalCompletionSurvivesSettlementRetry(t *testing.T) {
	binding, err := BindingForNewWAL(testWALIdentity(9), testTopologyRecoveryEpoch, testAuthorityProfile())
	if err != nil {
		t.Fatal(err)
	}
	command := testApplySessionOpen(sqldriver.ReplicatedShardStoreIdentity{Binding: binding})
	view, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	runtime, machine := memoryRuntimeAtApplyBoundary(t, [][]byte{command})
	// This memory machine's opaque result deliberately differs from what a
	// post-apply lookup would produce. The dummy SQL handle cannot serve a
	// lookup: success therefore proves all APIs consumed the apply result.
	machine.originalCompletion = []byte("original state-machine result")
	wantKey := replicatedstate.SessionKey(view.AuthorityClass, view.Tenant, view.ClientID)
	var ready ReadyWorkspace
	wantRetry := errors.New("sink temporarily unavailable")
	check := func(batch AppliedBatch) {
		t.Helper()
		lookup, hasCommand, err := batch.LookupCompletion(0)
		if err != nil || !hasCommand || lookup.Key != wantKey || lookup.AppliedSequence != batch.LastIndex() ||
			!bytes.Equal(lookup.Bytes, machine.originalCompletion) {
			t.Fatalf("owned lookup = %+v, %v, %v", lookup, hasCommand, err)
		}
		lookup.Bytes[0] ^= 0xff
		dst := make([]byte, 0, replicatedstate.MaxCompletionEnvelopeBytes)
		lookup, hasCommand, err = batch.LookupCompletionInto(0, dst)
		if err != nil || !hasCommand || !bytes.Equal(lookup.Bytes, machine.originalCompletion) ||
			&lookup.Bytes[0] != &dst[:cap(dst)][0] {
			t.Fatalf("into lookup = %+v, %v, %v", lookup, hasCommand, err)
		}
		lookup.Bytes[0] ^= 0xff
		if _, _, err := batch.LookupCompletionInto(0, nil); !errors.Is(err, replicatedstate.ErrCompletionBufferSmall) {
			t.Fatalf("short buffer = %v", err)
		}
		// Bind a test workspace token without a database snapshot. Original
		// completion must use its exact apply cut, not re-read storage.
		workspace := AppliedBatchCompletionWorkspace{owner: batch.apply, source: batch.source}
		lookup, hasCommand, err = batch.LookupCompletionIntoWorkspace(&workspace, 0, dst)
		if err != nil || !hasCommand || !bytes.Equal(lookup.Bytes, machine.originalCompletion) || lookup.Key != wantKey {
			t.Fatalf("workspace lookup = %+v, %v, %v", lookup, hasCommand, err)
		}
		workspace.source.ReadyID++
		if _, _, err := batch.LookupCompletionIntoWorkspace(&workspace, 0, dst); !errors.Is(err, replicatedstate.ErrCompletionWorkspaceBusy) {
			t.Fatalf("foreign workspace = %v", err)
		}
	}
	if _, err := runtime.DriveReady(&ready, nil, func(batch AppliedBatch) error {
		check(batch)
		return RetryResultSettlement(wantRetry)
	}); !errors.Is(err, wantRetry) || !runtime.HasPendingResultSettlement() {
		t.Fatalf("initial sink retry = %v, pending=%v", err, runtime.HasPendingResultSettlement())
	}
	if _, err := runtime.DriveReady(&ready, nil, func(batch AppliedBatch) error {
		check(batch)
		return nil
	}); err != nil || runtime.HasPendingResultSettlement() {
		t.Fatalf("settled retry = %v, pending=%v", err, runtime.HasPendingResultSettlement())
	}
}
