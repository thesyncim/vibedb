package routegate

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestSnapshotRoundTripCanonicalAndAllocationFreeAppend(t *testing.T) {
	machine := mustMachine(t, 7, 16)
	for _, value := range []uint64{9, 2, 7, 1} {
		requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 7, value)), ReasonAcquired, true)
	}
	requireReason(t, machine.Apply(testCommand(OperationReleaseShared, 7, 2)), ReasonReleased, true)
	requireReason(t, machine.Apply(testCommand(OperationBeginExclusive, 7, 99)), ReasonDrainPending, true)
	scratch := make([]PinRecord, machine.Status().RetainedRecords)
	raw, err := AppendSnapshot(nil, machine, scratch)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, ok := SnapshotBytes(machine.Status().RetainedRecords)
	if !ok || uint64(len(raw)) != wantBytes {
		t.Fatalf("snapshot bytes = %d, want %d", len(raw), wantBytes)
	}
	restored, err := OpenSnapshot(raw, 16)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status() != machine.Status() {
		t.Fatalf("restored = %+v, want %+v", restored.Status(), machine.Status())
	}
	reencoded, err := AppendSnapshot(nil, restored, scratch)
	if err != nil || !bytes.Equal(reencoded, raw) {
		t.Fatalf("snapshot not unique: %v", err)
	}
	dst := make([]byte, 0, len(raw))
	if got := testing.AllocsPerRun(1000, func() {
		var appendErr error
		dst, appendErr = AppendSnapshot(dst[:0], machine, scratch)
		if appendErr != nil {
			panic(appendErr)
		}
	}); !raceDetectorEnabled && got != 0 {
		t.Fatalf("snapshot append allocations = %v", got)
	}
}

func TestSnapshotOrderingIndependentOfMapInsertion(t *testing.T) {
	left := mustMachine(t, 3, 8)
	right := mustMachine(t, 3, 8)
	for _, value := range []uint64{1, 2, 3, 4} {
		left.Apply(testCommand(OperationAcquireShared, 3, value))
	}
	for _, value := range []uint64{4, 3, 2, 1} {
		right.Apply(testCommand(OperationAcquireShared, 3, value))
	}
	leftRaw, err := AppendSnapshot(nil, left, make([]PinRecord, 4))
	if err != nil {
		t.Fatal(err)
	}
	rightRaw, err := AppendSnapshot(nil, right, make([]PinRecord, 4))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftRaw, rightRaw) {
		t.Fatal("equivalent state produced different canonical snapshots")
	}
}

func TestSnapshotRejectsBoundsCorruptionAndNoncanonicalRecords(t *testing.T) {
	machine := mustMachine(t, 1, 2)
	machine.Apply(testCommand(OperationAcquireShared, 1, 1))
	raw, err := AppendSnapshot(nil, machine, make([]PinRecord, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSnapshot(raw, 0); err == nil {
		t.Fatal("accepted zero local admission bound")
	}
	if _, err = OpenSnapshot(raw, MaxRetainedRecords+1); err == nil {
		t.Fatal("accepted oversized local admission bound")
	}
	if _, err = OpenSnapshot(raw, 1); err != nil {
		t.Fatalf("rejected exact local admission bound: %v", err)
	}

	rechecksum := func(encoded []byte) {
		body := encoded[:len(encoded)-SnapshotChecksumBytes]
		binary.LittleEndian.PutUint32(encoded[len(body):], crc32.Checksum(body, castagnoli))
	}
	for _, mutate := range []func([]byte){
		func(encoded []byte) { encoded[4] = 1 },
		func(encoded []byte) { encoded[49] = 1 },
		func(encoded []byte) { encoded[73+SnapshotHeaderBytes] = 1 },
		func(encoded []byte) { binary.LittleEndian.PutUint64(encoded[128:136], 1) },
		func(encoded []byte) { clear(encoded[16:24]) },
	} {
		bad := bytes.Clone(raw)
		mutate(bad)
		rechecksum(bad)
		if _, openErr := OpenSnapshot(bad, 2); openErr == nil {
			t.Fatal("accepted checksummed structural corruption")
		}
	}
	badChecksum := bytes.Clone(raw)
	badChecksum[20] ^= 1
	if _, err = OpenSnapshot(badChecksum, 2); err == nil {
		t.Fatal("accepted checksum corruption")
	}
	if _, err = AppendSnapshot(nil, machine, nil); err != ErrScratch {
		t.Fatalf("scratch error = %v", err)
	}
}

func TestSnapshotExactGeometryBounds(t *testing.T) {
	if bytes, ok := SnapshotBytes(MaxRetainedRecords); !ok || bytes > MaxSnapshotBytes {
		t.Fatalf("max geometry = %d, %t", bytes, ok)
	}
	if _, ok := SnapshotBytes(MaxRetainedRecords + 1); ok {
		t.Fatal("accepted record count above exact geometry")
	}
}

func TestSnapshotAcceptsNewEpochPinsAfterReleasedDrain(t *testing.T) {
	machine := mustMachine(t, 1, 4)
	drain := testCommand(OperationBeginExclusive, 1, 10)
	requireReason(t, machine.Apply(drain), ReasonDrainAcquired, true)
	requireReason(t, machine.Apply(testCommand(OperationReleaseExclusive, 1, 10)), ReasonDrainReleased, true)
	requireReason(t, machine.Apply(testCommand(OperationAcquireShared, 2, 11)), ReasonAcquired, true)
	raw, err := AppendSnapshot(nil, machine, make([]PinRecord, 1))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := OpenSnapshot(raw, 4)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status() != machine.Status() {
		t.Fatalf("restored = %+v, want %+v", restored.Status(), machine.Status())
	}
}

func BenchmarkSnapshotAppend4096Pins(b *testing.B) {
	const records = 4096
	machine, ok := NewMachine(1, records)
	if !ok {
		b.Fatal("machine")
	}
	for value := uint64(1); value <= records; value++ {
		if outcome := machine.Apply(testCommand(OperationAcquireShared, 1, value)); outcome.Reason != ReasonAcquired {
			b.Fatal(outcome)
		}
	}
	scratch := make([]PinRecord, records)
	encodedBytes, ok := SnapshotBytes(records)
	if !ok {
		b.Fatal("snapshot geometry")
	}
	dst := make([]byte, 0, encodedBytes)
	b.ReportAllocs()
	b.SetBytes(int64(encodedBytes))
	b.ResetTimer()
	for range b.N {
		var err error
		dst, err = AppendSnapshot(dst[:0], machine, scratch)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(encodedBytes)/records, "bytes/record")
}
