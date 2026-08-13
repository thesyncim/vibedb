package distribution

// AppendScalar and AppendTuple sit on the hot path of every write and every
// point lookup that touches a sharded table, so both must run without
// allocating once the caller's buffer has spare capacity — the "zero-
// allocation append APIs where correctness permits" deliverable PR 1a
// promises. Each benchmark reuses one buffer, which is how a caller is
// expected to drive this package, and TestZeroAllocation* pins the
// allocation count as a regression gate rather than only reporting it.

import (
	"strings"
	"testing"
)

var (
	benchNumberSmall = mustNumber("42")
	benchNumberWide  = mustNumber("-1.23456789e-300")
	benchNumberHuge  = mustNumber("1e" + strings.Repeat("9", 200))
	benchStringShort = NewString("tenant-0000123")
	benchStringLong  = NewString(strings.Repeat("x", 256))
)

func mustNumber(spelling string) Scalar {
	s, err := NewNumber(spelling)
	if err != nil {
		panic(err)
	}
	return s
}

// --- AppendScalar: the single-value hot path ---

func BenchmarkAppendScalarNumber(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchNumberSmall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendScalarNumberWideExponent(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchNumberWide)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendScalarNumberHugeExponent(b *testing.B) {
	dst := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchNumberHuge)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendScalarStringShort(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchStringShort)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendScalarStringLong(b *testing.B) {
	dst := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchStringLong)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- AppendTuple: single-column (the common routing case) and multi-column ---

func BenchmarkAppendTupleSingleColumn(b *testing.B) {
	values := []Scalar{benchNumberSmall}
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendTuple(dst[:0], values)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendTupleThreeColumns(b *testing.B) {
	values := []Scalar{benchStringShort, benchNumberSmall, benchStringLong}
	dst := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = CurrentTupleCodec.AppendTuple(dst[:0], values)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Allocation gates: pin the zero-allocation append contract ---

func TestZeroAllocationAppendScalarNumber(t *testing.T) {
	dst := make([]byte, 0, 64)
	allocations := testing.AllocsPerRun(1000, func() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchNumberSmall)
		if err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("AppendScalar(Number) allocations = %v, want 0", allocations)
	}
}

func TestZeroAllocationAppendScalarString(t *testing.T) {
	dst := make([]byte, 0, 64)
	allocations := testing.AllocsPerRun(1000, func() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchStringShort)
		if err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("AppendScalar(String) allocations = %v, want 0", allocations)
	}
}

func TestZeroAllocationAppendTuple(t *testing.T) {
	values := []Scalar{benchStringShort, benchNumberSmall, benchStringLong}
	dst := make([]byte, 0, 512)
	allocations := testing.AllocsPerRun(1000, func() {
		var err error
		dst, err = CurrentTupleCodec.AppendTuple(dst[:0], values)
		if err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("AppendTuple allocations = %v, want 0", allocations)
	}
}

// TestZeroAllocationAppendScalarWideExponent covers the exponent-heavy
// weight-magnitude path (prefix/middle/fill/tail), which is the one part of
// the encoder that walks a caller-supplied byte run instead of a fixed-size
// field, and so is the most plausible place for an accidental allocation to
// creep in.
func TestZeroAllocationAppendScalarWideExponent(t *testing.T) {
	dst := make([]byte, 0, 256)
	allocations := testing.AllocsPerRun(1000, func() {
		var err error
		dst, err = CurrentTupleCodec.AppendScalar(dst[:0], benchNumberHuge)
		if err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("AppendScalar(huge exponent Number) allocations = %v, want 0", allocations)
	}
}
