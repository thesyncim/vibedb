package query

import (
	"errors"
	"math"
	"testing"
)

func TestIntermediateBudgetIsStatementWideAndFailClosed(t *testing.T) {
	frame, err := newStatementFrame(ExecOptions{IntermediateBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := frame.intermediate.reserve("first relation", 60); err != nil {
		t.Fatal(err)
	}
	err = frame.intermediate.reserve("nested relation", 41)
	var budgetErr *IntermediateBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("reserve error = %#v, want typed intermediate budget", err)
	}
	if budgetErr.Bytes != 101 || budgetErr.Limit != 100 ||
		frame.intermediate.used != 60 {
		t.Fatalf("failed reservation = %+v, used=%d", budgetErr, frame.intermediate.used)
	}
	frame.intermediate.release(20)
	if got := frame.intermediate.remaining(); got != 60 {
		t.Fatalf("remaining after release = %d, want 60", got)
	}
}

func TestIntermediateBudgetNormalization(t *testing.T) {
	tests := []struct {
		value int64
		want  int64
		ok    bool
	}{
		{value: 0, want: DefaultIntermediateBytes, ok: true},
		{value: -1, want: -1, ok: true},
		{value: 1, want: 1, ok: true},
		{value: -2, ok: false},
	}
	for _, test := range tests {
		got, err := normalizeIntermediateBytes(ExecOptions{
			IntermediateBytes: test.value,
		})
		if (err == nil) != test.ok || test.ok && got != test.want {
			t.Fatalf("normalize(%d) = (%d, %v), want (%d, ok=%t)",
				test.value, got, err, test.want, test.ok)
		}
	}
}

func TestRelationRetainedBytesSaturates(t *testing.T) {
	if got := relationRetainedBytes(0); got != 512 {
		t.Fatalf("empty row charge = %d, want 512", got)
	}
	if got := relationRetainedBytes(math.MaxInt); got != math.MaxInt64 {
		t.Fatalf("overflow charge = %d, want MaxInt64", got)
	}
}
