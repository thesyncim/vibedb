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
