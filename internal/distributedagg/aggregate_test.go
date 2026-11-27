package distributedagg

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/exchange"
)

func testCell(value string) exchange.Cell { return exchange.Cell{Bytes: []byte(value)} }

func TestMergerExactIdentityArithmeticAndInputOwnership(t *testing.T) {
	merger, err := NewMerger(
		[]Kind{None, Count, Sum, Min, Max}, []uint16{0}, 1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte(`1`)
	minimum := []byte(`2`)
	if err := merger.Add([]exchange.Cell{
		{Bytes: key}, testCell(`18446744073709551615`),
		testCell(`9223372036854775807`), {Bytes: minimum}, testCell(`3`),
	}); err != nil {
		t.Fatal(err)
	}
	copy(key, []byte(`9`))
	minimum[0] = '8'
	if err := merger.Add([]exchange.Cell{
		testCell(`1e0`), testCell(`1`), testCell(`1`), testCell(`1`), testCell(`10`),
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := merger.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("groups = %d, want 1", len(rows))
	}
	want := []string{`1`, `18446744073709551616`, `9223372036854775808`, `1`, `10`}
	for column := range want {
		if rows[0][column].Null || string(rows[0][column].Bytes) != want[column] {
			t.Fatalf("column %d = %q/null=%v, want %q", column,
				rows[0][column].Bytes, rows[0][column].Null, want[column])
		}
	}
}

func TestMergeCellsFiniteDecimalAndNullIdentity(t *testing.T) {
	sum, err := MergeCells(Sum, []exchange.Cell{
		testCell(`1.25`), testCell(`2.5`), {Null: true}, testCell(`-0.75`),
	}, 1<<20)
	if err != nil || sum.Null || string(sum.Bytes) != "3" {
		t.Fatalf("SUM = %q/null=%v, %v; want 3", sum.Bytes, sum.Null, err)
	}
	empty, err := MergeCells(Sum, []exchange.Cell{{Null: true}}, 1<<20)
	if err != nil || !empty.Null {
		t.Fatalf("empty SUM = %+v, %v; want NULL", empty, err)
	}
}

func TestMergerRejectsUnboundedOrOverBudgetPrograms(t *testing.T) {
	if _, err := NewMerger([]Kind{None, Count}, []uint16{0}, 0); !errors.Is(err, ErrAggregate) {
		t.Fatalf("zero budget error = %v", err)
	}
	merger, err := NewMerger([]Kind{None, Count}, []uint16{0}, 256)
	if err != nil {
		t.Fatal(err)
	}
	if err := merger.Add([]exchange.Cell{testCell(`"a"`), testCell(`1`)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("over-budget group error = %v", err)
	}
}

func TestMergerOverflowPromotionReserveAndEdgeCases(t *testing.T) {
	// Reserve on a tiny budget must clamp cleanly without bypassing ErrLimit.
	tiny, err := NewMerger([]Kind{None, Count}, []uint16{0}, 256)
	if err != nil {
		t.Fatal(err)
	}
	tiny.Reserve(1_000_000)
	if err := tiny.Add([]exchange.Cell{testCell(`"a"`), testCell(`1`)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("over-budget group after Reserve = %v, want ErrLimit", err)
	}

	// Test int64 negative overflow, big.Int -> big.Rat promotion, -0, and MIN/MAX
	// mixing canonical int64 with decimals and wide numbers.
	merger, err := NewMerger([]Kind{None, Count, Sum, Min, Max}, []uint16{0}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	merger.Reserve(128)
	rowsCap := cap(merger.rows)
	countsCap := cap(merger.counts[1])
	sumsCap := cap(merger.sums[2])
	merger.Reserve(4)
	if cap(merger.rows) < rowsCap || cap(merger.counts[1]) < countsCap || cap(merger.sums[2]) < sumsCap {
		t.Fatalf("smaller Reserve shrank capacities: rows %d<%d counts %d<%d sums %d<%d",
			cap(merger.rows), rowsCap, cap(merger.counts[1]), countsCap, cap(merger.sums[2]), sumsCap)
	}
	rowsIn := [][]exchange.Cell{
		// Group 1: negative int64 overflow then fractional promotion
		{testCell(`"neg"`), testCell(`0`), testCell(`-9223372036854775808`), testCell(`-9223372036854775808`), testCell(`-0`)},
		{testCell(`"neg"`), testCell(`1`), testCell(`-1`), testCell(`-9223372036854775809`), testCell(`0.5`)},
		{testCell(`"neg"`), testCell(`2`), testCell(`0.5`), testCell(`10`), testCell(`9223372036854775807`)},
		// Group 2: all-null SUM/MIN/MAX
		{testCell(`"nulls"`), testCell(`0`), {Null: true}, {Null: true}, {Null: true}},
		// Group 3: whitespace-padded integers
		{testCell(` 42 `), testCell(` 3 `), testCell(` -0 `), testCell(` -10 `), testCell(` 20 `)},
		{testCell(`42.0`), testCell(`4`), testCell(` 100 `), testCell(` -20 `), testCell(` 15 `)},
	}
	for _, r := range rowsIn {
		if err := merger.Add(r); err != nil {
			t.Fatalf("Add(%v) failed: %v", r, err)
		}
	}
	got, err := merger.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("groups = %d, want 3", len(got))
	}
	if merger.sums[2][0].wide == nil || !merger.sums[2][0].wide.fraction || len(merger.sums[2][0].wide.integer.Bits()) != 0 {
		t.Fatalf("promoted fractional sum retained stale integer words: %+v", merger.sums[2][0].wide)
	}
	wantNeg := []string{`"neg"`, `3`, `-9223372036854775808.5`, `-9223372036854775809`, `9223372036854775807`}
	for col, want := range wantNeg {
		if got[0][col].Null || string(got[0][col].Bytes) != want {
			t.Fatalf("neg col %d = %q (null=%v), want %q", col, got[0][col].Bytes, got[0][col].Null, want)
		}
	}
	if !got[1][2].Null || !got[1][3].Null || !got[1][4].Null {
		t.Fatalf("nulls group should produce NULL for SUM/MIN/MAX: %+v", got[1])
	}
	want42 := []string{` 42 `, `7`, `100`, ` -20 `, ` 20 `}
	for col, want := range want42 {
		if got[2][col].Null || string(got[2][col].Bytes) != want {
			t.Fatalf("group 42 col %d = %q (null=%v), want %q", col, got[2][col].Bytes, got[2][col].Null, want)
		}
	}

	// Reject malformed COUNT/SUM/MIN/MAX tokens that must not pass integer fast paths.
	for _, bad := range []string{"01", "-01", "-", "", "1x", `"1"`, "NaN"} {
		if _, err := MergeCells(Count, []exchange.Cell{testCell(bad)}, 1<<20); err == nil {
			t.Fatalf("MergeCells(Count, %q) succeeded, want error", bad)
		}
		if _, err := MergeCells(Min, []exchange.Cell{testCell(bad)}, 1<<20); err == nil {
			t.Fatalf("MergeCells(Min, %q) succeeded, want error", bad)
		}
	}
	if _, err := MergeCells(Count, []exchange.Cell{testCell("-1")}, 1<<20); err == nil {
		t.Fatalf("MergeCells(Count, -1) succeeded, want error")
	}
}

func BenchmarkMergerAddExistingCountGroup(b *testing.B) {
	merger, err := NewMerger([]Kind{None, Count}, []uint16{0}, 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	row := []exchange.Cell{testCell(`"hot"`), testCell(`1`)}
	if err := merger.Add(row); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(row[0].Bytes) + len(row[1].Bytes)))
	b.ResetTimer()
	for b.Loop() {
		if err := merger.Add(row); err != nil {
			b.Fatal(err)
		}
	}
}
