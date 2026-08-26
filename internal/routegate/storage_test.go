package routegate

import (
	"bytes"
	"testing"
)

func TestDurableHeadPinRestoreRoundTripAndCorruption(t *testing.T) {
	machine, ok := NewMachine(7, 16)
	if !ok {
		t.Fatal("NewMachine")
	}
	identity := testIdentity(31)
	binding := testBinding(67)
	outcome := machine.Apply(Command{
		Operation: OperationAcquireShared, Epoch: 7,
		Identity: identity, Binding: binding,
	})
	if outcome.Reason != ReasonAcquired {
		t.Fatalf("acquire = %+v", outcome)
	}
	head, err := AppendHead(make([]byte, 0, HeadBytes), machine.Status())
	if err != nil || len(head) != HeadBytes {
		t.Fatalf("AppendHead = %d, %v", len(head), err)
	}
	status, err := OpenHead(head)
	if err != nil || status != machine.Status() {
		t.Fatalf("OpenHead = %+v, %v", status, err)
	}
	pin, found := machine.Pin(identity)
	if !found {
		t.Fatal("missing pin")
	}
	stored, err := AppendStoredPin(make([]byte, 0, StoredPinBytes), pin)
	if err != nil || len(stored) != StoredPinBytes {
		t.Fatalf("AppendStoredPin = %d, %v", len(stored), err)
	}
	opened, err := OpenStoredPin(identity, stored)
	if err != nil || opened != pin {
		t.Fatalf("OpenStoredPin = %+v, %v", opened, err)
	}
	restored, err := RestoreMachine(status, 16, []PinRecord{opened})
	if err != nil || restored.Status() != machine.Status() {
		t.Fatalf("RestoreMachine = %+v, %v", restored, err)
	}
	if restoredPin, exists := restored.Pin(identity); !exists || restoredPin != pin {
		t.Fatalf("restored pin = %+v, %v", restoredPin, exists)
	}

	for name, raw := range map[string][]byte{
		"head-checksum": append(bytes.Clone(head[:HeadBytes-1]), head[HeadBytes-1]^1),
		"pin-checksum":  append(bytes.Clone(stored[:StoredPinBytes-1]), stored[StoredPinBytes-1]^1),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "head-checksum" {
				if _, err := OpenHead(raw); err == nil {
					t.Fatal("accepted corrupt head")
				}
				return
			}
			if _, err := OpenStoredPin(identity, raw); err == nil {
				t.Fatal("accepted corrupt pin")
			}
		})
	}
	if _, err := RestoreMachine(status, 16, []PinRecord{opened, opened}); err == nil {
		t.Fatal("accepted duplicate restored pin")
	}
}

func TestDurableCodecsAreAllocationFreeWithCallerStorage(t *testing.T) {
	machine, _ := NewMachine(1, 8)
	identity, binding := testIdentity(9), testBinding(17)
	machine.Apply(Command{
		Operation: OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: binding,
	})
	pin, _ := machine.Pin(identity)
	var head [HeadBytes]byte
	var stored [StoredPinBytes]byte
	if allocs := testing.AllocsPerRun(1000, func() {
		encoded, err := AppendHead(head[:0], machine.Status())
		if err != nil {
			panic(err)
		}
		if _, err = OpenHead(encoded); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("head allocs = %v", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		encoded, err := AppendStoredPin(stored[:0], pin)
		if err != nil {
			panic(err)
		}
		if _, err = OpenStoredPin(identity, encoded); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("pin allocs = %v", allocs)
	}
}

func FuzzDurableCodecsRejectNoncanonicalBytes(f *testing.F) {
	machine, _ := NewMachine(1, 8)
	identity, binding := testIdentity(21), testBinding(33)
	machine.Apply(Command{
		Operation: OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: binding,
	})
	head, _ := AppendHead(nil, machine.Status())
	pin, _ := machine.Pin(identity)
	stored, _ := AppendStoredPin(nil, pin)
	f.Add(head)
	f.Add(stored)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if status, err := OpenHead(raw); err == nil {
			reencoded, encodeErr := AppendHead(nil, status)
			if encodeErr != nil || !bytes.Equal(reencoded, raw) {
				t.Fatalf("noncanonical head accepted")
			}
		}
		if record, err := OpenStoredPin(identity, raw); err == nil {
			reencoded, encodeErr := AppendStoredPin(nil, record)
			if encodeErr != nil || !bytes.Equal(reencoded, raw) {
				t.Fatalf("noncanonical pin accepted")
			}
		}
	})
}
