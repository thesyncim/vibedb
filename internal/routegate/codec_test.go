package routegate

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func testIdentity(value uint64) Identity {
	var identity Identity
	binary.LittleEndian.PutUint64(identity[:8], value)
	identity[31] = 0xa5
	return identity
}

func testBinding(value uint64) Binding {
	var binding Binding
	binary.LittleEndian.PutUint64(binding[:8], value)
	binding[31] = 0x5a
	return binding
}

func testCommand(operation Operation, epoch, value uint64) Command {
	return Command{
		Operation: operation, Epoch: epoch,
		Identity: testIdentity(value), Binding: testBinding(value),
	}
}

func TestCommandRoundTripCanonicalFixedAndAllocationFree(t *testing.T) {
	commands := []Command{
		testCommand(OperationAcquireShared, 1, 1),
		testCommand(OperationReleaseShared, 2, 2),
		testCommand(OperationBeginExclusive, 3, 3),
		testCommand(OperationReleaseExclusive, 4, 4),
		{Operation: OperationCompactReleased, Epoch: 5},
	}
	for _, command := range commands {
		dst := make([]byte, 3, 3+CommandBytes)
		encoded, err := AppendCommand(dst, command)
		if err != nil {
			t.Fatalf("append %d: %v", command.Operation, err)
		}
		encoded = encoded[3:]
		if len(encoded) != CommandBytes {
			t.Fatalf("encoded bytes = %d, want %d", len(encoded), CommandBytes)
		}
		opened, err := OpenCommand(encoded)
		if err != nil || opened != command {
			t.Fatalf("open %d = %+v, %v", command.Operation, opened, err)
		}
		reencoded, err := AppendCommand(nil, opened)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("noncanonical re-encode for %d: %v", command.Operation, err)
		}
		preallocated := make([]byte, 0, CommandBytes)
		if got := testing.AllocsPerRun(1000, func() {
			var appendErr error
			preallocated, appendErr = AppendCommand(preallocated[:0], command)
			if appendErr != nil {
				panic(appendErr)
			}
			if _, openErr := OpenCommand(preallocated); openErr != nil {
				panic(openErr)
			}
		}); !raceDetectorEnabled && got != 0 {
			t.Fatalf("append/open allocations = %v", got)
		}
	}
}

func TestCommandRejectsEveryNoncanonicalShape(t *testing.T) {
	valid, err := AppendCommand(nil, testCommand(OperationAcquireShared, 7, 9))
	if err != nil {
		t.Fatal(err)
	}
	checksum := func(raw []byte) {
		binary.LittleEndian.PutUint32(raw[commandBodyBytes:], crc32.Checksum(raw[:commandBodyBytes], castagnoli))
	}
	mutateCanonical := func(offset int, value byte) []byte {
		result := bytes.Clone(valid)
		result[offset] = value
		checksum(result)
		return result
	}
	cases := [][]byte{
		nil,
		valid[:CommandBytes-1],
		append(bytes.Clone(valid), 0),
		mutateCanonical(0, 'X'),
		mutateCanonical(4, byte(OperationInvalid)),
		mutateCanonical(4, 0xff),
		mutateCanonical(5, 1),
		mutateCanonical(6, 1),
		mutateCanonical(7, 1),
	}
	zeroEpoch := bytes.Clone(valid)
	clear(zeroEpoch[8:16])
	checksum(zeroEpoch)
	cases = append(cases, zeroEpoch)
	zeroIdentity := bytes.Clone(valid)
	clear(zeroIdentity[16:48])
	checksum(zeroIdentity)
	cases = append(cases, zeroIdentity)
	zeroBinding := bytes.Clone(valid)
	clear(zeroBinding[48:80])
	checksum(zeroBinding)
	cases = append(cases, zeroBinding)
	badCompact := bytes.Clone(valid)
	badCompact[4] = byte(OperationCompactReleased)
	checksum(badCompact)
	cases = append(cases, badCompact)
	badChecksum := bytes.Clone(valid)
	badChecksum[20] ^= 1
	cases = append(cases, badChecksum)

	for index, raw := range cases {
		if _, openErr := OpenCommand(raw); openErr == nil {
			t.Fatalf("case %d accepted", index)
		}
		if validateErr := ValidateCommand(raw); validateErr == nil {
			t.Fatalf("validator case %d accepted", index)
		}
	}
}

func BenchmarkCommandAppendOpen(b *testing.B) {
	command := testCommand(OperationAcquireShared, 12, 34)
	raw := make([]byte, 0, CommandBytes)
	b.ReportAllocs()
	b.SetBytes(CommandBytes)
	for range b.N {
		var err error
		raw, err = AppendCommand(raw[:0], command)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = OpenCommand(raw); err != nil {
			b.Fatal(err)
		}
	}
}
