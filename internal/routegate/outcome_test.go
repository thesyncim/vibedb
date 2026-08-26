package routegate

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestOutcomeRoundTripCanonicalFixedAndAllocationFree(t *testing.T) {
	machine := mustMachine(t, 1, 4)
	outcomes := []Outcome{
		machine.Apply(testCommand(OperationAcquireShared, 1, 1)),
		machine.Apply(testCommand(OperationAcquireShared, 1, 1)),
		machine.Apply(testCommand(OperationReleaseShared, 1, 1)),
		machine.Apply(testCommand(OperationAcquireShared, 1, 1)),
		machine.Apply(Command{Operation: OperationCompactReleased, Epoch: 1}),
		machine.Apply(testCommand(OperationBeginExclusive, 2, 2)),
		machine.Apply(testCommand(OperationReleaseExclusive, 2, 2)),
	}
	for _, outcome := range outcomes {
		raw, err := AppendOutcome(nil, outcome)
		if err != nil {
			t.Fatalf("append %+v: %v", outcome, err)
		}
		if len(raw) != OutcomeBytes {
			t.Fatalf("outcome bytes = %d, want %d", len(raw), OutcomeBytes)
		}
		opened, err := OpenOutcome(raw)
		if err != nil || opened != outcome {
			t.Fatalf("open = %+v, %v, want %+v", opened, err, outcome)
		}
		reencoded, err := AppendOutcome(nil, opened)
		if err != nil || !bytes.Equal(reencoded, raw) {
			t.Fatalf("noncanonical outcome re-encode: %v", err)
		}
		dst := make([]byte, 0, OutcomeBytes)
		if got := testing.AllocsPerRun(1000, func() {
			var appendErr error
			dst, appendErr = AppendOutcome(dst[:0], outcome)
			if appendErr != nil {
				panic(appendErr)
			}
			if _, openErr := OpenOutcome(dst); openErr != nil {
				panic(openErr)
			}
		}); !raceDetectorEnabled && got != 0 {
			t.Fatalf("append/open allocations = %v", got)
		}
	}
}

func TestOutcomeRejectsNoncanonicalState(t *testing.T) {
	machine := mustMachine(t, 1, 2)
	outcome := machine.Apply(testCommand(OperationAcquireShared, 1, 1))
	raw, err := AppendOutcome(nil, outcome)
	if err != nil {
		t.Fatal(err)
	}
	rechecksum := func(encoded []byte) {
		binary.LittleEndian.PutUint32(encoded[outcomeBodyBytes:], crc32.Checksum(encoded[:outcomeBodyBytes], castagnoli))
	}
	for _, mutate := range []func([]byte){
		func(encoded []byte) { encoded[0] = 'X' },
		func(encoded []byte) { encoded[4] = 0 },
		func(encoded []byte) { encoded[5] = 0 },
		func(encoded []byte) { encoded[5] = 2 },
		func(encoded []byte) { encoded[7] = 1 },
		func(encoded []byte) { binary.LittleEndian.PutUint64(encoded[16:24], 0) },
		func(encoded []byte) { binary.LittleEndian.PutUint64(encoded[40:48], 0) },
	} {
		bad := bytes.Clone(raw)
		mutate(bad)
		rechecksum(bad)
		if _, openErr := OpenOutcome(bad); openErr == nil {
			t.Fatal("accepted checksummed noncanonical outcome")
		}
	}
}

func BenchmarkOutcomeAppendOpen(b *testing.B) {
	machine, _ := NewMachine(1, 1)
	outcome := machine.Apply(testCommand(OperationAcquireShared, 1, 1))
	raw := make([]byte, 0, OutcomeBytes)
	b.ReportAllocs()
	b.SetBytes(OutcomeBytes)
	for range b.N {
		var err error
		raw, err = AppendOutcome(raw[:0], outcome)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = OpenOutcome(raw); err != nil {
			b.Fatal(err)
		}
	}
}
