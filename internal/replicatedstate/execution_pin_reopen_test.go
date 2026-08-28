package replicatedstate

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/store/durable"
)

func acquireActivePinForReopen(t *testing.T, machine *Machine, binding Binding, sequence uint64, pinBinding executionpin.Binding) ([]byte, executionpin.Record) {
	t.Helper()
	pin, err := executionpin.DerivePinID(pinBinding)
	if err != nil {
		t.Fatal(err)
	}
	client := id128(0xd0)
	command := executionPinCommand(binding, client, 2, sequence, executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: pinBinding, PinID: pin,
		AuthorityNode: executionpin.ID(client), AuthorityGeneration: 7,
		NextController: executionpin.ID(id128(0xe0)), NextControllerEpoch: 1, NextLeaseSpan: 97,
	})
	if _, err = machine.ApplyNormal(normalMeta(sequence+1), command); err != nil {
		t.Fatal(err)
	}
	outer, proof := openExecutionPinProof(t, machine, command)
	if outer.ResultCode != ResultApplied || proof.Status != executionpin.StatusActive {
		t.Fatalf("active acquisition: outer=%+v proof=%+v", outer, proof)
	}
	record, found, err := machine.LookupExecutionPin(pin)
	if err != nil || !found {
		t.Fatalf("active record: found=%t err=%v", found, err)
	}
	return command, record
}

func TestExecutionPinActiveIndexSurvivesDurableReopen(t *testing.T) {
	store := newPersistentSessionLifecycleStore(t, 8)
	defer store.close(t)
	if _, err := store.machine.InstallSnapshot(store.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, store.machine, 2, executionPinSessionPrototype(store.binding, id128(0xd0)))
	var commands [2][]byte
	var records [2]executionpin.Record
	for index := range records {
		binding := executionPinTestBinding()
		binding.LedgerHomeGroup[0] += byte(index)
		commands[index], records[index] = acquireActivePinForReopen(t, store.machine, store.binding, uint64(index+2), binding)
	}
	wantState := store.machine.state
	for restart := 0; restart < 2; restart++ {
		store.reopen(t) // Close/reopen the actual collection files and transaction log.
		if store.machine.state.ExecutionPinRecordCount != wantState.ExecutionPinRecordCount ||
			store.machine.state.ExecutionPinResidentBytes != wantState.ExecutionPinResidentBytes ||
			store.machine.state.ActiveExecutionPinCount != 2 || store.machine.state.Applied != wantState.Applied {
			t.Fatalf("active pin accounting changed across durable reopen: got=%+v want=%+v", store.machine.state, wantState)
		}
		for index, record := range records {
			got, found, err := store.machine.LookupExecutionPin(record.PinID)
			if err != nil || !found || got != record {
				t.Fatalf("reopened pin %d: found=%t err=%v", index, found, err)
			}
			active, err := store.machine.ScanActiveExecutionPins(record.Binding.LedgerHomeGroup, executionpin.PinID{}, 2)
			if err != nil || len(active) != 1 || active[0] != record {
				t.Fatalf("reopened active scope %d: rows=%d err=%v", index, len(active), err)
			}
			outer, proof := openExecutionPinProof(t, store.machine, commands[index])
			if outer.ResultCode != ResultApplied || proof.Acquire.PinID != record.PinID {
				t.Fatal("retry proof lost its full pin identity across reopen")
			}
		}
	}
}

func TestExecutionPinReopenRejectsAlteredActiveIndex(t *testing.T) {
	for _, field := range []string{"scope", "pin", "digest"} {
		t.Run(field, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			applySessionOpen(t, fixture.machine, 2, executionPinSessionPrototype(fixture.binding, id128(0xd0)))
			_, record := acquireActivePinForReopen(t, fixture.machine, fixture.binding, 2, executionPinTestBinding())
			key := executionPinActiveStorageKey(record)
			value, found, err := fixture.system.Collection.AppendRaw(nil, key[:])
			if err != nil || !found {
				t.Fatalf("active index: found=%t err=%v", found, err)
			}
			changed := key
			switch field {
			case "scope":
				changed[1] ^= 0xff
			case "pin":
				changed[len(changed)-1] ^= 0xff
			case "digest":
				value[0] ^= 0xff
			}
			if err = fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
				if err := batch.Delete(key[:]); err != nil {
					return err
				}
				return batch.Put(changed[:], value)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options); !errors.Is(err, ErrExecutionPinStateCorrupt) {
				t.Fatalf("altered active %s accepted: %v", field, err)
			}
		})
	}
}
