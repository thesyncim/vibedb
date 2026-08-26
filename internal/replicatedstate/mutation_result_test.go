package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestMutationCompletionResultCanonicalRoundTrip(t *testing.T) {
	for _, rows := range []int64{0, 1, MaxDistinctMutations, MaxMutationAffectedRows} {
		encoded, err := AppendMutationCompletionResult(nil, ResultApplied, rows)
		if err != nil || len(encoded) != MutationCompletionResultBytes {
			t.Fatalf("append rows %d = %x,%v", rows, encoded, err)
		}
		opened, err := OpenMutationCompletionResult(ResultApplied, encoded)
		if err != nil || opened != rows {
			t.Fatalf("open rows %d = %d,%v", rows, opened, err)
		}
	}
	for _, code := range []uint32{ResultIndexConflict, ResultIntentBusy, ResultTargetBound} {
		encoded, err := AppendMutationCompletionResult(nil, code, 0)
		if err != nil || len(encoded) != 0 {
			t.Fatalf("refusal %d = %x,%v", code, encoded, err)
		}
		if rows, err := OpenMutationCompletionResult(code, encoded); err != nil || rows != 0 {
			t.Fatalf("open refusal %d = %d,%v", code, rows, err)
		}
	}
}

func TestMutationCompletionResultRejectsNoncanonicalWithoutMutatingDestination(t *testing.T) {
	prefix := []byte("unchanged")
	for _, test := range []struct {
		code uint32
		rows int64
	}{
		{0, 0},
		{ResultApplied, -1},
		{ResultApplied, MaxMutationAffectedRows + 1},
		{ResultIndexConflict, 1},
	} {
		got, err := AppendMutationCompletionResult(prefix, test.code, test.rows)
		if !errors.Is(err, ErrCompletionCorrupt) || !bytes.Equal(got, prefix) {
			t.Fatalf("append (%d,%d) = %q,%v", test.code, test.rows, got, err)
		}
	}
	negative := make([]byte, MutationCompletionResultBytes)
	binary.LittleEndian.PutUint64(negative, uint64(math.MaxInt64)+1)
	for _, test := range []struct {
		code uint32
		raw  []byte
	}{
		{ResultApplied, nil},
		{ResultApplied, make([]byte, MutationCompletionResultBytes-1)},
		{ResultApplied, negative},
		{ResultApplied, func() []byte {
			raw := make([]byte, MutationCompletionResultBytes)
			binary.LittleEndian.PutUint64(raw, uint64(MaxMutationAffectedRows+1))
			return raw
		}()},
		{ResultIndexConflict, make([]byte, MutationCompletionResultBytes)},
	} {
		if _, err := OpenMutationCompletionResult(test.code, test.raw); !errors.Is(err, ErrCompletionCorrupt) {
			t.Fatalf("open (%d,%x) = %v", test.code, test.raw, err)
		}
	}
}

func TestMutationCompletionResultAllocationFree(t *testing.T) {
	scratch := make([]byte, 0, MutationCompletionResultBytes)
	encoded, err := AppendMutationCompletionResult(scratch, ResultApplied, 7)
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		encoded = encoded[:0]
		encoded, err = AppendMutationCompletionResult(encoded, ResultApplied, 7)
	}); allocations != 0 || err != nil {
		t.Fatalf("append allocations=%v err=%v", allocations, err)
	}
	var rows int64
	if allocations := testing.AllocsPerRun(1000, func() {
		rows, err = OpenMutationCompletionResult(ResultApplied, encoded)
	}); allocations != 0 || err != nil || rows != 7 {
		t.Fatalf("open allocations=%v rows=%d err=%v", allocations, rows, err)
	}
}
