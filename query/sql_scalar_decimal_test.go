package query

import (
	"errors"
	"math"
	"testing"
)

func TestSQLScalarDecimalArithmeticExact(t *testing.T) {
	tests := []struct {
		name        string
		op          sqlScalarArithmeticOp
		left, right string
		want        string
	}{
		{"add beyond float mantissa", sqlScalarAdd, "9007199254740993", "1", "9007199254740994"},
		{"add scales", sqlScalarAdd, "0.00000000000000000001", "2.5", "2.50000000000000000001"},
		{"subtract", sqlScalarSubtract, "1e40", "1", "9999999999999999999999999999999999999999"},
		{"multiply", sqlScalarMultiply, "123456789.123456789", "0.000000001", "0.123456789123456789"},
		{"finite divide", sqlScalarDivide, "1", "8", "0.125"},
		{"zero divide", sqlScalarDivide, "0e999", "7", "0"},
		{"rounded divide", sqlScalarDivide, "1", "3", "0.3333333333333333333333333333333333"},
		{"ties even divide", sqlScalarDivide, "5", "2", "2.5"},
		{"negative divide", sqlScalarDivide, "-10", "4", "-2.5"},
		{"decimal modulo", sqlScalarModulo, "5.75", "0.5", "0.25"},
		{"negative modulo", sqlScalarModulo, "-5.75", "0.5", "-0.25"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var decimal sqlScalarDecimal
			var budget aggregateBudget
			budget.begin(1 << 20)
			out, start, err := decimal.binary(
				test.op, []byte(test.left), []byte(test.right), 17,
				nil, &budget,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := out[start:]; compareNumberBytes(got, []byte(test.want)) != 0 {
				t.Fatalf("%s %s %s = %s, want exact %s",
					test.left, test.op.String(), test.right, got, test.want)
			}
		})
	}
}

func TestSQLScalarDecimalDivisionByZeroAndBudget(t *testing.T) {
	var decimal sqlScalarDecimal
	var budget aggregateBudget
	budget.begin(1 << 20)
	_, _, err := decimal.binary(
		sqlScalarDivide, []byte("1"), []byte("0.0"), 9, nil, &budget,
	)
	var zero *ScalarDivisionByZeroError
	if !errors.As(err, &zero) || zero.Position() != 9 ||
		!errors.Is(err, ErrScalarDivisionByZero) {
		t.Fatalf("division error = %T %v", err, err)
	}

	budget.begin(aggregateAccBaseBytes)
	_, _, err = decimal.binary(
		sqlScalarMultiply,
		[]byte("123456789012345678901234567890"),
		[]byte("123456789012345678901234567890"),
		13, nil, &budget,
	)
	var bounded *ScalarAggregateBudgetError
	var aggregate *AggregateBudgetError
	if !errors.As(err, &bounded) || bounded.Position() != 13 ||
		!errors.As(err, &aggregate) || !errors.Is(err, ErrAggregateBudget) ||
		errors.Is(err, ErrScalarNumericRange) {
		t.Fatalf("budget error = %T %v", err, err)
	}
}

func TestSQLScalarDecimalSizeOverflowRemainsNumericRange(t *testing.T) {
	var decimal sqlScalarDecimal
	var budget aggregateBudget
	budget.begin(math.MaxInt64)
	err := decimal.reserve(-1, 21, "multiplication", &budget)
	var bounded *ScalarNumericRangeError
	if !errors.As(err, &bounded) || bounded.Position() != 21 ||
		!errors.Is(err, ErrScalarNumericRange) ||
		errors.Is(err, ErrAggregateBudget) {
		t.Fatalf("size overflow = %T %v", err, err)
	}
}

func TestSQLScalarDecimalWarmZeroAlloc(t *testing.T) {
	var decimal sqlScalarDecimal
	var budget aggregateBudget
	arena := make([]byte, 0, 128)
	left := []byte("123456789.123456789")
	right := []byte("7")
	run := func() {
		budget.begin(1 << 20)
		var err error
		arena, _, err = decimal.binary(
			sqlScalarDivide,
			left, right,
			4, arena[:0], &budget,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed exact arithmetic allocated %.2f/run", allocs)
	}
}

func BenchmarkSQLScalarDecimalWarm(b *testing.B) {
	var decimal sqlScalarDecimal
	var budget aggregateBudget
	arena := make([]byte, 0, 128)
	left := []byte("123456789.123456789")
	right := []byte("0.000000001")
	for range 2 {
		budget.begin(1 << 20)
		arena, _, _ = decimal.binary(
			sqlScalarMultiply,
			left, right,
			4, arena[:0], &budget,
		)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		budget.begin(1 << 20)
		var err error
		arena, _, err = decimal.binary(
			sqlScalarMultiply,
			left, right,
			4, arena[:0], &budget,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}
