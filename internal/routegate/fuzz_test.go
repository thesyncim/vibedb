package routegate

import (
	"encoding/binary"
	"testing"
)

func FuzzCommandCanonical(f *testing.F) {
	for _, command := range []Command{
		testCommand(OperationAcquireShared, 1, 1),
		testCommand(OperationReleaseShared, 2, 2),
		testCommand(OperationBeginExclusive, 3, 3),
		testCommand(OperationReleaseExclusive, 4, 4),
		{Operation: OperationCompactReleased, Epoch: 5},
	} {
		raw, err := AppendCommand(nil, command)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		command, err := OpenCommand(raw)
		if err != nil {
			return
		}
		reencoded, err := AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		if string(reencoded) != string(raw) {
			t.Fatal("accepted command did not have a unique encoding")
		}
	})
}

func FuzzSnapshotRestore(f *testing.F) {
	machine, _ := NewMachine(1, 4)
	machine.Apply(testCommand(OperationAcquireShared, 1, 1))
	machine.Apply(testCommand(OperationReleaseShared, 1, 2))
	raw, err := AppendSnapshot(nil, machine, make([]PinRecord, 2))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Fuzz(func(t *testing.T, encoded []byte) {
		restored, openErr := OpenSnapshot(encoded, 64)
		if openErr != nil {
			return
		}
		scratch := make([]PinRecord, restored.Status().RetainedRecords)
		reencoded, appendErr := AppendSnapshot(nil, restored, scratch)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		if string(reencoded) != string(encoded) {
			t.Fatal("accepted snapshot did not have a unique encoding")
		}
	})
}

func FuzzMachineTransitions(f *testing.F) {
	f.Add([]byte{1, 0, 1, 1, 2, 0, 1, 1, 3, 0, 2, 2, 4, 0, 2, 2})
	f.Add([]byte{2, 0, 1, 1, 1, 0, 1, 1, 5, 0, 0, 0})
	f.Fuzz(func(t *testing.T, program []byte) {
		machine, ok := NewMachine(1, 64)
		if !ok {
			t.Fatal("machine")
		}
		for cursor := 0; cursor+4 <= len(program); cursor += 4 {
			operation := Operation(program[cursor]%5 + 1)
			epoch := machine.epoch
			switch program[cursor+1] % 4 {
			case 1:
				if epoch > 1 {
					epoch--
				}
			case 2:
				if epoch != ^uint64(0) {
					epoch++
				}
			case 3:
				epoch = uint64(program[cursor+1]) + 1
			}
			command := Command{Operation: operation, Epoch: epoch}
			if operation != OperationCompactReleased {
				command.Identity = testIdentity(uint64(program[cursor+2]) + 1)
				command.Binding = testBinding(uint64(program[cursor+3]) + 1)
			}
			machine.Apply(command)
			assertMachineInvariant(t, machine)
		}
		scratch := make([]PinRecord, machine.Status().RetainedRecords)
		raw, err := AppendSnapshot(nil, machine, scratch)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := OpenSnapshot(raw, 64)
		if err != nil {
			t.Fatal(err)
		}
		if restored.Status() != machine.Status() {
			t.Fatalf("restored status = %+v, want %+v", restored.Status(), machine.Status())
		}
	})
}

func assertMachineInvariant(t testing.TB, machine *Machine) {
	t.Helper()
	if machine == nil || machine.epoch == 0 || uint64(len(machine.pins)) > machine.maxRecords ||
		machine.activePins > uint64(len(machine.pins)) ||
		machine.releasedPins != uint64(len(machine.pins))-machine.activePins ||
		!validDrainSnapshot(machine.drain, machine.epoch, machine.activePins) {
		t.Fatalf("invalid machine header: %+v", machine.Status())
	}
	var active, released uint64
	for identity, record := range machine.pins {
		if identity == (Identity{}) || record.Binding == (Binding{}) ||
			record.Epoch == 0 || record.Epoch > machine.epoch {
			t.Fatalf("invalid record: %+v", record)
		}
		switch record.State {
		case PinHeld:
			active++
		case PinReleased:
			released++
		default:
			t.Fatalf("invalid pin state: %+v", record)
		}
	}
	if active != machine.activePins || released != machine.releasedPins {
		t.Fatalf("counter mismatch: active=%d released=%d status=%+v", active, released, machine.Status())
	}
}

func TestReplicatedReplayIsByteDeterministic(t *testing.T) {
	left := mustMachine(t, 10, 32)
	right := mustMachine(t, 10, 32)
	commands := []Command{
		testCommand(OperationAcquireShared, 10, 3),
		testCommand(OperationAcquireShared, 10, 1),
		testCommand(OperationReleaseShared, 10, 2),
		testCommand(OperationBeginExclusive, 10, 50),
		testCommand(OperationReleaseShared, 10, 3),
	}
	for _, command := range commands {
		raw, err := AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := OpenCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		if leftOutcome, rightOutcome := left.Apply(opened), right.Apply(opened); leftOutcome != rightOutcome {
			t.Fatalf("outcomes differ: %+v %+v", leftOutcome, rightOutcome)
		}
	}
	leftRaw, err := AppendSnapshot(nil, left, make([]PinRecord, len(left.pins)))
	if err != nil {
		t.Fatal(err)
	}
	rightRaw, err := AppendSnapshot(nil, right, make([]PinRecord, len(right.pins)))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftRaw) != len(rightRaw) ||
		binary.LittleEndian.Uint32(leftRaw[len(leftRaw)-4:]) !=
			binary.LittleEndian.Uint32(rightRaw[len(rightRaw)-4:]) || string(leftRaw) != string(rightRaw) {
		t.Fatal("identical replay produced different canonical state")
	}
}
