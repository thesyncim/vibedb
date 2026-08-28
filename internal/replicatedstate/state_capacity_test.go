package replicatedstate

import (
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestTransitionCapacityUsesExactValidatedStateSize(t *testing.T) {
	for _, voters := range []int{1, 3} {
		for extensions := uint8(0); extensions < 32; extensions++ {
			t.Run(fmt.Sprintf("RF%d/extensions_%02x", voters, extensions), func(t *testing.T) {
				state := stateSizeFixture(extensions)
				state.ConfState.Voters = make([]uint64, voters)
				for i := range voters {
					state.ConfState.Voters[i] = uint64(i + 1)
				}
				envelope, err := AppendState(nil, state)
				if err != nil {
					t.Fatal(err)
				}
				var machine Machine
				machine.system.Limits.MaxDocumentBytes = len(envelope)
				machine.system.Limits.MaxDistinctMutations = 1
				machine.system.Limits.MaxBatchBytes = len(stateKey) + len(envelope)
				machine.options.TxnLimits.MaxCollections = 1
				machine.options.TxnLimits.MaxDocuments = 1
				machine.options.TxnLimits.MaxBytes = int64(len(stateKey) + len(envelope))
				check := func() error {
					return machine.checkTransitionCapacityWithCaptureRows(state, nil, commandPlan{}, 0, nil)
				}
				if err := check(); err != nil {
					t.Fatalf("exact capacity rejected: %v", err)
				}
				machine.system.Limits.MaxDocumentBytes--
				if err := check(); !errors.Is(err, ErrAdmissionBound) {
					t.Fatalf("document bound: %v", err)
				}
				machine.system.Limits.MaxDocumentBytes++
				machine.system.Limits.MaxBatchBytes--
				if err := check(); !errors.Is(err, ErrAdmissionBound) {
					t.Fatalf("batch bound: %v", err)
				}
				machine.system.Limits.MaxBatchBytes++
				machine.options.TxnLimits.MaxBytes--
				if err := check(); !errors.Is(err, ErrAdmissionBound) {
					t.Fatalf("transaction bound: %v", err)
				}
				machine.options.TxnLimits.MaxBytes++
				state.ConfState.Voters[0] = 0
				if err := check(); !errors.Is(err, ErrStateCorrupt) {
					t.Fatalf("invalid membership bypassed validation: %v", err)
				}
			})
		}
	}
}

func TestTransitionCapacityReusesPersistedEnvelopeWithoutAllocations(t *testing.T) {
	envelope, err := AppendState(nil, codecState())
	if err != nil {
		t.Fatal(err)
	}
	var machine Machine
	machine.system.Limits.MaxDocumentBytes = MaxStateEnvelopeBytes
	machine.system.Limits.MaxDistinctMutations = 1
	machine.system.Limits.MaxBatchBytes = MaxStateEnvelopeBytes + len(stateKey)
	machine.options.TxnLimits.MaxCollections = 1
	machine.options.TxnLimits.MaxDocuments = 1
	machine.options.TxnLimits.MaxBytes = int64(machine.system.Limits.MaxBatchBytes)
	if allocations := testing.AllocsPerRun(100, func() {
		if err := machine.checkTransitionCapacityWithStateBytes(len(envelope), nil, commandPlan{}, 0, nil); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("already encoded envelope capacity allocations = %g", allocations)
	}
	for _, size := range []int{-1, 0, math.MaxInt, MaxStateEnvelopeBytes + 1} {
		if err := machine.checkTransitionCapacityWithStateBytes(size, nil, commandPlan{}, 0, nil); !errors.Is(err, ErrAdmissionBound) {
			t.Fatalf("invalid encoded size %d accepted: %v", size, err)
		}
	}
}
