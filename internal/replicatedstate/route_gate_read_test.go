package replicatedstate

import (
	"crypto/sha256"
	"errors"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"testing"
)

func TestRouteGateReadCoherentAppliedStatusAndReopen(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	_, _, epoch := applySessionOpen(t, f.machine, 2, commandValue(f.binding, 1))
	for i, op := range []routegate.Operation{routegate.OperationAcquireShared, routegate.OperationReleaseShared} {
		nested, err := routegate.AppendCommand(nil, routegate.Command{Operation: op, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}})
		if err != nil {
			t.Fatal(err)
		}
		command := commandValue(f.binding, uint64(i+1))
		command.Kind = replication.CommandRouteGate
		command.ClientEpoch = epoch
		command.Batches = nil
		command.RouteGate = nested
		command.Fingerprint = sha256.Sum256(nested)
		applied := uint64(i + 3)
		if _, err := f.machine.ApplyNormal(normalMeta(applied), encodeCommand(t, command)); err != nil {
			t.Fatal(err)
		}
		result, err := f.machine.RouteGateRead(applied)
		if err != nil || result.Fence.Applied != applied || result.Status.Epoch != 1 || result.Status.ActivePins != uint64(1-i) || result.Status.ReleasedPins != uint64(i) {
			t.Fatalf("read %+v %v", result, err)
		}
		if got, err := f.machine.RouteGateRead(applied + 1); !errors.Is(err, ErrReadBehind) || got != (RouteGateReadResult{}) {
			t.Fatalf("future read %+v %v", got, err)
		}
	}
	reopened, err := Open(f.binding, f.bootstrap, f.system, UserCollection{Name: "docs", Target: f.user}, f.log, f.machine.options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reopened.RouteGateRead(4)
	if err != nil || result.Status.ReleasedPins != 1 || result.Status.ActivePins != 0 || result.Fence.Applied != 4 {
		t.Fatalf("reopen %+v %v", result, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := reopened.RouteGateRead(4); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("read allocs=%g", allocations)
	}
	reopened.schemaTransitioned = true
	if _, err := reopened.RouteGateRead(4); !errors.Is(err, ErrSchemaTransitionPending) {
		t.Fatalf("transition fence %v", err)
	}
}
