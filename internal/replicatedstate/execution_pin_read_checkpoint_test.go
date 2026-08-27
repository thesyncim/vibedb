package replicatedstate

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/store/durable"
	"google.golang.org/protobuf/proto"
)

func TestExecutionPinReadsCertifyCheckpointPressure(t *testing.T) {
	for _, operation := range []string{"read", "lookup", "scan"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newNormalBatchFixture(t, 32, 8)
			machine := fixture.machine
			if _, err := machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			// Prove the regression's precondition without poisoning the machine:
			// an individual collection cannot certify this materialization.
			standalone, err := fixture.system.Collection.Snapshot()
			if standalone != nil {
				_ = standalone.Close()
			}
			if !errors.Is(err, durable.ErrCheckpointGroupPressure) {
				t.Fatalf("expected standalone checkpoint pressure, got %v", err)
			}
			before := machine.Published()
			pin := executionpin.PinID{1}
			switch operation {
			case "read":
				got, err := machine.ExecutionPinRead(pin, before.Applied)
				if err != nil || got.Found || got.Fence.Applied != before.Applied {
					t.Fatalf("read under pressure: found=%t applied=%d err=%v", got.Found, got.Fence.Applied, err)
				}
			case "lookup":
				_, found, err := machine.LookupExecutionPin(pin)
				if err != nil || found {
					t.Fatalf("lookup under pressure: found=%t err=%v", found, err)
				}
			case "scan":
				rows, err := machine.ScanActiveExecutionPins(executionpin.ID{1}, executionpin.PinID{}, 1)
				if err != nil || len(rows) != 0 {
					t.Fatalf("scan under pressure: rows=%d err=%v", len(rows), err)
				}
			}
			after := machine.Published()
			if machine.poison != nil || machine.applyCut.Len() != 0 ||
				after.Applied != before.Applied || after.DataChainDigest != before.DataChainDigest ||
				after.ReplicaSetVersion != before.ReplicaSetVersion || !proto.Equal(after.ConfState, before.ConfState) {
				t.Fatalf("read changed logical publication, retained leases, or poisoned machine: before=%+v after=%+v cut=%d poison=%v", before, after, machine.applyCut.Len(), machine.poison)
			}
			// Subsequent replicated work must still settle after read pressure.
			applySessionOpen(t, machine, 2, executionPinSessionPrototype(fixture.binding, id128(0xd0)))
			_, record := acquireActivePinForReopen(t, machine, fixture.binding, 2, executionPinTestBinding())
			got, err := machine.ExecutionPinRead(record.PinID, machine.Published().Applied)
			if err != nil || !got.Found || got.Record != record {
				t.Fatalf("pin write/read after checkpoint pressure: found=%t err=%v", got.Found, err)
			}
		})
	}
}

func TestExecutionPinReadCutOnlyLeasesSystemRelation(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	machine := fixture.machine
	machine.mu.Lock()
	defer machine.mu.Unlock()
	check := func() {
		snapshot, err := machine.executionPinReadCutLocked()
		if err != nil || snapshot == nil || machine.applyCut.Len() != 1 {
			panic("invalid system-only read cut")
		}
		for _, relation := range machine.relations {
			if _, found := machine.applyCut.CollectionHandle(relation.target.Collection); found {
				panic("pin read leased an unrelated relation")
			}
		}
		if err := machine.applyCut.Close(); err != nil {
			panic(err)
		}
	}
	check() // Warm the fixed reusable snapshot storage, including checkpoint pressure.
	if allocations := testing.AllocsPerRun(100, check); allocations != 0 {
		t.Fatalf("system-only pin read cut allocated %g times", allocations)
	}
}
