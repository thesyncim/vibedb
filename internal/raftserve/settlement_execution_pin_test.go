package raftserve

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func settlementPinAcquire(t testing.TB) executionpin.Command {
	t.Helper()
	binding := executionpin.Binding{RequestKeyDigest: executionpin.Digest{1}, RequestDigest: executionpin.Digest{2},
		CatalogGeneration: 3, SchemaManifestDigest: executionpin.Digest{4}, TransactionManifestDigest: executionpin.Digest{5},
		TargetAuthorityRoot: executionpin.Digest{6}, TargetCount: 2, ExecutionContractDigest: executionpin.Digest{7}, LedgerHomeGroup: executionpin.ID{8}}
	pin, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	return executionpin.Command{Operation: executionpin.OperationAcquire, Binding: binding, PinID: pin,
		AuthorityNode: executionpin.ID{9}, AuthorityGeneration: 1, NextController: executionpin.ID{10}, NextControllerEpoch: 1, NextLeaseSpan: 50}
}

func settlementPinCommand(t testing.TB, outer replication.Command, pin executionpin.Command) []byte {
	t.Helper()
	var err error
	outer.Kind, outer.AuthorityClass, outer.Batches = replication.CommandExecutionPin, replication.CommandAuthorityExecutionPin, nil
	outer.ExecutionPin, err = executionpin.AppendCommand(nil, pin)
	if err != nil {
		t.Fatal(err)
	}
	outer.Fingerprint = sha256.Sum256(outer.ExecutionPin)
	return encodeTestCommand(t, outer)
}

func settlementPinProof(t testing.TB, data []byte, current executionpin.Record, found bool, applied uint64) (executionpin.Record, executionpin.Completion) {
	t.Helper()
	outer, err := replication.OpenCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	command, err := outer.OpenExecutionPin()
	if err != nil {
		t.Fatal(err)
	}
	authority, ok := replication.ExecutionPinAuthorityDigest(outer)
	if !ok {
		t.Fatal("invalid authority")
	}
	transition := executionpin.Apply(current, found, command, applied, executionpin.Digest(authority), executionpin.Digest(replicatedstate.LogicalCommandDigest(outer)))
	if transition.Reason != executionpin.ReasonApplied {
		t.Fatalf("pin transition: %+v", transition)
	}
	proof, err := executionpin.CompletionFromApplied(command, transition.Record, executionpin.Digest(authority), applied)
	if err != nil {
		t.Fatal(err)
	}
	return transition.Record, proof
}

func settlementPinLookup(t testing.TB, group raftmember.GroupKey, data []byte, applied uint64, code uint32, proof executionpin.Completion) replicatedstate.CompletionLookup {
	t.Helper()
	raw, err := executionpin.AppendCompletion(nil, proof)
	if err != nil {
		t.Fatal(err)
	}
	return testCompletionResultBytesFormat(t, group, data, applied, code, replicatedstate.ResultFormatExecutionPin, raw)
}

func TestExecutionPinSettlementAllOperationsAndRefusals(t *testing.T) {
	group := testGroup(61)
	acquire := settlementPinAcquire(t)
	acquireData := settlementPinCommand(t, testCommand(group, 9, 2), acquire)
	record, acquireProof := settlementPinProof(t, acquireData, executionpin.Record{}, false, 100)
	for _, operation := range []executionpin.Operation{executionpin.OperationAcquire, executionpin.OperationRenew, executionpin.OperationRecover, executionpin.OperationRelease, executionpin.OperationExpire} {
		t.Run(string(rune('0'+operation)), func(t *testing.T) {
			command := acquire
			command.Operation = operation
			applied := uint64(120)
			if operation != executionpin.OperationAcquire {
				command.ExpectedController, command.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
				command.ExpectedLeaseAppliedThrough, command.ExpectedLeaseRevision = record.LeaseAppliedThrough, record.LeaseRevision
				command.AcquireCertificateDigest, _ = executionpin.AcquireCertificateDigest(acquireProof.Acquire)
			}
			switch operation {
			case executionpin.OperationAcquire:
				applied = 100
			case executionpin.OperationRecover:
				command.NextController, command.NextControllerEpoch, applied = executionpin.ID{11}, 2, 200
			case executionpin.OperationRelease:
				command.NextController, command.NextControllerEpoch, command.NextLeaseSpan = executionpin.ID{}, 0, 0
				command.PrepareTerminalDigest = executionpin.Digest{12}
			case executionpin.OperationExpire:
				command.NextController, command.NextControllerEpoch, command.NextLeaseSpan = executionpin.ID{}, 0, 0
				command.AcquireCertificateDigest, applied = executionpin.Digest{}, 200
			}
			data := settlementPinCommand(t, testCommand(group, 9, 2), command)
			proof := acquireProof
			if operation != executionpin.OperationAcquire {
				_, proof = settlementPinProof(t, data, record, true, applied)
			}
			identity, err := openCommandIdentity(group, data)
			if err != nil {
				t.Fatal(err)
			}
			lookup := settlementPinLookup(t, group, data, applied, replicatedstate.ResultApplied, proof)
			if err = validateCompletionLookup(identity, lookup); err != nil {
				t.Fatal(err)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				opened, err := openCommandIdentity(group, data)
				if err != nil {
					panic(err)
				}
				if err := validateCompletionLookup(opened, lookup); err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("settlement allocations: %g", allocations)
			}
			for name, mutate := range map[string]func(*executionpin.Completion){
				"authority": func(p *executionpin.Completion) {
					if p.Status == executionpin.StatusActive {
						p.Lease.AuthorityDigest[0]++
					} else {
						p.Terminal.AuthorityDigest[0]++
					}
				},
				"controller": func(p *executionpin.Completion) {
					if p.Status == executionpin.StatusActive {
						p.Lease.Controller[0]++
					} else {
						p.Terminal.Controller[0]++
					}
				},
				"applied": func(p *executionpin.Completion) {
					if p.Status == executionpin.StatusActive {
						p.Lease.Applied++
					} else {
						p.Terminal.Applied++
					}
				},
			} {
				t.Run(name, func(t *testing.T) {
					foreign := proof
					mutate(&foreign)
					if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, applied, replicatedstate.ResultApplied, foreign)); !errors.Is(err, ErrSettlementResult) {
						t.Fatal("canonical foreign proof accepted", err)
					}
				})
			}
			for _, code := range []uint32{replicatedstate.ResultIndexConflict, replicatedstate.ResultIntentBusy, replicatedstate.ResultTargetBound, replicatedstate.ResultStaleFence} {
				refusal, _ := executionpin.RefusalCompletion(operation)
				if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, applied, code, refusal)); err != nil {
					t.Fatal(err)
				}
			}
			if operation == executionpin.OperationExpire {
				_, tombstone := settlementPinProof(t, data, executionpin.Record{}, false, applied)
				if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, applied, replicatedstate.ResultApplied, tombstone)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExecutionPinSettlementRejectsCanonicalForeignProofs(t *testing.T) {
	group := testGroup(62)
	command := settlementPinAcquire(t)
	data := settlementPinCommand(t, testCommand(group, 9, 2), command)
	_, proof := settlementPinProof(t, data, executionpin.Record{}, false, 100)
	identity, err := openCommandIdentity(group, data)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*executionpin.Completion){
		"operation":       func(p *executionpin.Completion) { p.Operation = executionpin.OperationRenew },
		"lease authority": func(p *executionpin.Completion) { p.Lease.AuthorityDigest[0]++ },
		"controller":      func(p *executionpin.Completion) { p.Lease.Controller[0]++ },
		"epoch":           func(p *executionpin.Completion) { p.Lease.ControllerEpoch++ },
		"lease span":      func(p *executionpin.Completion) { p.Lease.LeaseAppliedThrough++ },
		"revision":        func(p *executionpin.Completion) { p.Lease.Revision++ },
		"applied":         func(p *executionpin.Completion) { p.Lease.Applied++ },
	} {
		t.Run(name, func(t *testing.T) {
			altered := proof
			mutate(&altered)
			if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, 100, replicatedstate.ResultApplied, altered)); !errors.Is(err, ErrSettlementResult) {
				t.Fatal(err)
			}
		})
	}
	foreign := command
	foreign.Binding.SchemaManifestDigest[0]++
	foreign.PinID, _ = executionpin.DerivePinID(foreign.Binding)
	foreignData := settlementPinCommand(t, testCommand(group, 9, 2), foreign)
	_, foreignProof := settlementPinProof(t, foreignData, executionpin.Record{}, false, 100)
	if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, 100, replicatedstate.ResultApplied, foreignProof)); !errors.Is(err, ErrSettlementResult) {
		t.Fatal("foreign binding accepted", err)
	}
	refusal, _ := executionpin.RefusalCompletion(command.Operation)
	if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, 100, replicatedstate.ResultApplied, refusal)); !errors.Is(err, ErrSettlementResult) {
		t.Fatal("absent success", err)
	}
	if err := validateCompletionLookup(identity, settlementPinLookup(t, group, data, 100, replicatedstate.ResultIndexConflict, proof)); !errors.Is(err, ErrSettlementResult) {
		t.Fatal("found refusal", err)
	}
}

func TestRegistrySettlesExecutionPinAndExactRetry(t *testing.T) {
	group := testGroup(63)
	command := settlementPinAcquire(t)
	data := settlementPinCommand(t, testCommand(group, 9, 2), command)
	_, proof := settlementPinProof(t, data, executionpin.Record{}, false, 100)
	lookup := settlementPinLookup(t, group, data, 100, replicatedstate.ResultApplied, proof)
	registry := testRegistry(t, 2, 2, 2)
	host := &testProposalHost{registry: registry, admit: true}
	waiter, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	batch := newTestAppliedBatch(group, 100, 2, data)
	batch.lookups[0].lookup = lookup
	if err = settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	got, _, err := waiter.TakeCompletionInto(make([]byte, 0, completionSlotBytes))
	if err != nil || !bytes.Equal(got, lookup.Bytes) {
		t.Fatal("settled proof changed", err)
	}
	retry, err := registry.Enqueue(host, group, data)
	if err != nil {
		t.Fatal(err)
	}
	batch = newTestAppliedBatch(group, 101, 2, data)
	batch.lookups[0].lookup = lookup
	if err = settleAppliedBatch(registry, batch); err != nil {
		t.Fatal(err)
	}
	outcome, ready, err := retry.Poll()
	if err != nil || !ready || outcome.Code != OutcomeCompletion {
		t.Fatal("exact retry did not settle", outcome, ready, err)
	}
	got, _, err = retry.TakeCompletionInto(got[:0])
	if err != nil || !bytes.Equal(got, lookup.Bytes) {
		t.Fatal("retried proof changed", err)
	}
}

func TestServingRuntimeSettlesExecutionPinWithoutPoisoning(t *testing.T) {
	runtime, base := newServingRuntime(t, 91)
	registry := testRegistry(t, 4, 8, 8)
	host, err := registry.NewHost(testServingHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close(); _ = registry.Close() })
	if err = host.Add(runtime); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)
	if err = host.RequestCampaign(runtime.Identity().Group); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)
	open := servingCommand(base, 0, 1)
	open.Kind, open.AuthorityClass, open.Batches = replication.CommandSessionOpen, replication.CommandAuthorityExecutionPin, nil
	open.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	waiter, err := registry.Enqueue(host, runtime.Identity().Group, encodeTestCommand(t, open))
	if err != nil {
		t.Fatal(err)
	}
	if outcome := driveServingWaiter(t, host, waiter); outcome.Code != OutcomeCompletion {
		t.Fatal(outcome)
	}
	raw, _, err := waiter.TakeCompletionInto(make([]byte, 0, completionSlotBytes))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := replication.OpenCompletion(raw)
	if err != nil {
		t.Fatal(err)
	}
	pin := settlementPinAcquire(t)
	data := settlementPinCommand(t, servingCommand(base, opened.ClientEpoch, 2), pin)
	var original []byte
	for attempt := 0; attempt < 2; attempt++ {
		waiter, err = registry.Enqueue(host, runtime.Identity().Group, data)
		if err != nil {
			t.Fatal(err)
		}
		if outcome := driveServingWaiter(t, host, waiter); outcome.Code != OutcomeCompletion {
			t.Fatal(outcome)
		}
		raw, _, err = waiter.TakeCompletionInto(raw[:0])
		if err != nil {
			t.Fatal(err)
		}
		identity, err := openCommandIdentity(runtime.Identity().Group, data)
		if err != nil {
			t.Fatal(err)
		}
		completion, err := replication.OpenCompletion(raw)
		if err != nil || validateExecutionPinSettlement(identity, completion) != nil {
			t.Fatal("invalid durable pin completion", err)
		}
		if attempt == 0 {
			original = bytes.Clone(raw)
		} else if !bytes.Equal(original, raw) {
			t.Fatal("durable retry changed acquisition proof")
		}
		if err := runtime.Failure(); err != nil {
			t.Fatal("settlement poisoned runtime", err)
		}
	}
}
