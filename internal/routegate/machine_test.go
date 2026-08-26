package routegate

import (
	"math"
	"testing"
	"unsafe"
)

func mustMachine(t testing.TB, epoch, max uint64) *Machine {
	t.Helper()
	machine, ok := NewMachine(epoch, max)
	if !ok {
		t.Fatalf("NewMachine(%d, %d) rejected", epoch, max)
	}
	return machine
}

func requireReason(t testing.TB, outcome Outcome, reason Reason, mutated bool) {
	t.Helper()
	if outcome.Reason != reason || outcome.Mutated != mutated {
		t.Fatalf("outcome = %+v, want reason %d mutated %t", outcome, reason, mutated)
	}
}

func TestReleaseTombstonePreventsDelayedAcquireResurrection(t *testing.T) {
	machine := mustMachine(t, 1, 8)
	acquire := testCommand(OperationAcquireShared, 1, 10)
	release := testCommand(OperationReleaseShared, 1, 10)
	requireReason(t, machine.Apply(release), ReasonReleased, true)
	requireReason(t, machine.Apply(acquire), ReasonAlreadyReleased, false)
	requireReason(t, machine.Apply(release), ReasonAlreadyReleased, false)
	status := machine.Status()
	if status.ActivePins != 0 || status.ReleasedPins != 1 ||
		status.RetainedRecords != 1 || status.Revision != 1 {
		t.Fatalf("status = %+v", status)
	}

	conflict := acquire
	conflict.Binding = testBinding(11)
	requireReason(t, machine.Apply(conflict), ReasonIdentityConflict, false)
}

func TestExclusiveDrainWaitsBlocksAndAdvancesEpoch(t *testing.T) {
	machine := mustMachine(t, 5, 16)
	first := testCommand(OperationAcquireShared, 5, 1)
	second := testCommand(OperationAcquireShared, 5, 2)
	requireReason(t, machine.Apply(first), ReasonAcquired, true)
	requireReason(t, machine.Apply(second), ReasonAcquired, true)
	drain := testCommand(OperationBeginExclusive, 5, 99)
	requireReason(t, machine.Apply(drain), ReasonDrainPending, true)
	requireReason(t, machine.Apply(drain), ReasonIdempotent, false)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 5, 3)), ReasonBlockedByDrain, false)

	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 5, 1)), ReasonReleased, true)
	if machine.Status().Drain.State != DrainPending {
		t.Fatalf("drain advanced before final pin: %+v", machine.Status())
	}
	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 5, 2)), ReasonReleased, true)
	if machine.Status().Drain.State != DrainActive {
		t.Fatalf("drain did not activate: %+v", machine.Status())
	}
	requireReason(t, machine.Apply(testCommand(OperationBeginExclusive, 5, 100)), ReasonIdentityConflict, false)

	finish := testCommand(OperationReleaseExclusive, 5, 99)
	requireReason(t, machine.Apply(finish), ReasonDrainReleased, true)
	requireReason(t, machine.Apply(finish), ReasonIdempotent, false)
	status := machine.Status()
	if status.Epoch != 6 || status.ActivePins != 0 || status.ReleasedPins != 0 ||
		status.RetainedRecords != 0 || status.Drain.State != DrainReleased {
		t.Fatalf("released drain status = %+v", status)
	}
	// The drain's epoch bump makes removal of every old tombstone safe.
	requireReason(t, machine.Apply(first), ReasonStaleEpoch, false)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 6, 3)), ReasonAcquired, true)
}

func TestEpochCompactionReclaimsOnlyReleasedPins(t *testing.T) {
	machine := mustMachine(t, 1, 4)
	held := testCommand(OperationAcquireShared, 1, 1)
	released := testCommand(OperationAcquireShared, 1, 2)
	requireReason(t, machine.Apply(held), ReasonAcquired, true)
	requireReason(t, machine.Apply(released), ReasonAcquired, true)
	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 1, 2)), ReasonReleased, true)
	requireReason(t, machine.Apply(Command{Operation: OperationCompactReleased, Epoch: 1}), ReasonCompacted, true)
	status := machine.Status()
	if status.Epoch != 2 || status.ActivePins != 1 || status.ReleasedPins != 0 || status.RetainedRecords != 1 {
		t.Fatalf("compacted status = %+v", status)
	}
	if _, found := machine.Pin(released.Identity); found {
		t.Fatal("released tombstone survived compaction")
	}
	if record, found := machine.Pin(held.Identity); !found || record.Epoch != 1 || record.State != PinHeld {
		t.Fatalf("active old-epoch pin = %+v, %t", record, found)
	}
	// A delayed old acquire cannot resurrect the reclaimed record.
	requireReason(t, machine.Apply(released), ReasonStaleEpoch, false)
	// An existing old-epoch pin can still release after admission advanced.
	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 1, 1)), ReasonReleased, true)
	requireReason(t, machine.Apply(Command{Operation: OperationCompactReleased, Epoch: 2}), ReasonCompacted, true)
	if machine.Status().RetainedRecords != 0 || machine.Status().Epoch != 3 {
		t.Fatalf("second compact = %+v", machine.Status())
	}
}

func TestNewEpochDrainWaitsForActiveOldEpochPin(t *testing.T) {
	machine := mustMachine(t, 1, 8)
	oldHeld := testCommand(OperationAcquireShared, 1, 1)
	oldReleased := testCommand(OperationReleaseShared, 1, 2)
	requireReason(t, machine.Apply(oldHeld), ReasonAcquired, true)
	requireReason(t, machine.Apply(oldReleased), ReasonReleased, true)
	requireReason(t, machine.Apply(Command{Operation: OperationCompactReleased, Epoch: 1}), ReasonCompacted, true)
	if machine.Status().Epoch != 2 || machine.Status().ActivePins != 1 {
		t.Fatalf("post-compact status = %+v", machine.Status())
	}
	// Compaction removed the old tombstone, but its epoch fence permanently
	// rejects a delayed old acquire.
	delayedAcquire := testCommand(OperationAcquireShared, 1, 2)
	requireReason(t, machine.Apply(delayedAcquire), ReasonStaleEpoch, false)

	// A drain in the new admission epoch still observes the older live pin.
	drain := testCommand(OperationBeginExclusive, 2, 99)
	requireReason(t, machine.Apply(drain), ReasonDrainPending, true)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 2, 3)), ReasonBlockedByDrain, false)
	if machine.Status().Drain.State != DrainPending {
		t.Fatalf("new-epoch drain skipped old pin: %+v", machine.Status())
	}
	// The old pin releases with the epoch durably stored in its own record.
	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 1, 1)), ReasonReleased, true)
	if machine.Status().Drain.State != DrainActive {
		t.Fatalf("drain did not activate after old pin release: %+v", machine.Status())
	}
	requireReason(t, machine.Apply(testCommand(OperationReleaseExclusive, 2, 99)), ReasonDrainReleased, true)
}

func TestCapacityIsShardStateBoundNotRequestParticipantField(t *testing.T) {
	machine := mustMachine(t, 1, 2)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 1, 1)), ReasonAcquired, true)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 1, 2)), ReasonAcquired, true)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 1, 3)), ReasonCapacity, false)
	// Control transitions for admitted records remain possible at capacity.
	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 1, 1)), ReasonReleased, true)
	requireReason(t, machine.Apply(Command{Operation: OperationCompactReleased, Epoch: 1}), ReasonCompacted, true)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 2, 3)), ReasonAcquired, true)
}

func TestCommandsStreamBeyondSmallBatchWidths(t *testing.T) {
	const participants = 4096
	machine := mustMachine(t, 1, participants)
	for ordinal := uint64(1); ordinal <= participants; ordinal++ {
		requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 1, ordinal)), ReasonAcquired, true)
	}
	if machine.Status().ActivePins != participants {
		t.Fatalf("active pins = %d, want %d", machine.Status().ActivePins, participants)
	}
}

func TestMachineRejectsInvalidGeometryAndExhaustion(t *testing.T) {
	for _, limits := range [][2]uint64{{0, 1}, {1, 0}, {1, MaxRetainedRecords + 1}} {
		if machine, ok := NewMachine(limits[0], limits[1]); ok || machine != nil {
			t.Fatalf("accepted limits %+v", limits)
		}
	}
	machine := mustMachine(t, 1, 2)
	machine.revision = math.MaxUint64
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 1, 1)), ReasonExhausted, false)
	machine.revision = 0
	machine.epoch = math.MaxUint64
	requireReason(t, machine.Apply(Command{Operation: OperationCompactReleased, Epoch: math.MaxUint64}), ReasonExhausted, false)
}

func TestRetainedPinSpaceGeometry(t *testing.T) {
	if got := unsafe.Sizeof(storedPin{}); got != 48 {
		t.Fatalf("stored pin bytes = %d, want 48", got)
	}
	if got := unsafe.Sizeof(PinRecord{}); got != SnapshotRecordBytes {
		t.Fatalf("detached pin bytes = %d, want %d", got, SnapshotRecordBytes)
	}
	if got := unsafe.Sizeof(DrainRecord{}); got != 80 {
		t.Fatalf("drain bytes = %d, want 80", got)
	}
}

func BenchmarkMachineApplyIdempotentSharedPin(b *testing.B) {
	machine, ok := NewMachine(1, 1)
	if !ok {
		b.Fatal("machine")
	}
	command := testCommand(OperationAcquireShared, 1, 1)
	if outcome := machine.Apply(command); !outcome.Mutated {
		b.Fatal(outcome)
	}
	b.ReportAllocs()
	for range b.N {
		outcome := machine.Apply(command)
		if outcome.Reason != ReasonIdempotent {
			b.Fatal(outcome)
		}
	}
}

const benchmarkPinBatch = 4096

func benchmarkPinCommands() [benchmarkPinBatch]Command {
	var commands [benchmarkPinBatch]Command
	for index := range commands {
		commands[index] = testCommand(OperationAcquireShared, 1, uint64(index+1))
	}
	return commands
}

func BenchmarkMachineApplyAcquireShared(b *testing.B) {
	commands := benchmarkPinCommands()
	machine, ok := NewMachine(1, benchmarkPinBatch)
	if !ok {
		b.Fatal("machine")
	}
	// Grow the map once outside measurement; the replicated machine retains
	// these buckets across epoch-fenced tombstone reclamation.
	for _, command := range commands {
		machine.Apply(command)
	}
	clear(machine.pins)
	machine.activePins = 0
	b.ReportAllocs()
	b.ResetTimer()
	for completed := 0; completed < b.N; {
		count := min(benchmarkPinBatch, b.N-completed)
		for index := range count {
			outcome := machine.Apply(commands[index])
			if outcome.Reason != ReasonAcquired {
				b.Fatal(outcome)
			}
		}
		completed += count
		b.StopTimer()
		clear(machine.pins)
		machine.activePins = 0
		b.StartTimer()
	}
}

func BenchmarkMachineApplyReleaseShared(b *testing.B) {
	commands := benchmarkPinCommands()
	machine, ok := NewMachine(1, benchmarkPinBatch)
	if !ok {
		b.Fatal("machine")
	}
	for _, command := range commands {
		machine.Apply(command)
	}
	for index := range commands {
		commands[index].Operation = OperationReleaseShared
	}
	b.ReportAllocs()
	b.ResetTimer()
	for completed := 0; completed < b.N; {
		count := min(benchmarkPinBatch, b.N-completed)
		for index := range count {
			outcome := machine.Apply(commands[index])
			if outcome.Reason != ReasonReleased {
				b.Fatal(outcome)
			}
		}
		completed += count
		b.StopTimer()
		for index := range count {
			identity := commands[index].Identity
			record := machine.pins[identity]
			record.State = PinHeld
			machine.pins[identity] = record
		}
		machine.activePins = uint64(benchmarkPinBatch)
		machine.releasedPins = 0
		b.StartTimer()
	}
}
