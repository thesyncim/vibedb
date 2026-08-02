package sql

import (
	"testing"
	"unsafe"
)

func TestSelectStmtColdSidecarFootprint(t *testing.T) {
	// SelectStmt is exactly its historical ordinary metadata plus one pointer
	// each for cold set and correlation state. Both stay nil on an ordinary
	// query; pinning words catches accidental inline metadata growth while
	// remaining meaningful on amd64 and 386.
	const words = 25
	if got, want := unsafe.Sizeof(SelectStmt{}), uintptr(words)*unsafe.Sizeof(uintptr(0)); got != want {
		t.Fatalf("SelectStmt footprint = %d bytes (%d pointer words), want %d bytes (%d words)",
			got, got/unsafe.Sizeof(uintptr(0)), want, words)
	}
}

func BenchmarkOrdinarySelectSetAbsenceBaseline(b *testing.B) {
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, benchSimple); err != nil {
		b.Fatal(err)
	}
	if parser.set != nil || statement.Set != nil {
		b.Fatal("ordinary SELECT activated set state")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := parser.Parse(&statement, benchSimple); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(unsafe.Sizeof(SelectStmt{})), "bytes/SelectStmt")
}

func BenchmarkParseSetExpression(b *testing.B) {
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, benchSetExpression); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(benchSetExpression)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := parser.Parse(&statement, benchSetExpression); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseValuesTableSetExpression(b *testing.B) {
	const source = `(VALUES (?, 'one'), (2, NULL) ORDER BY column1 LIMIT ?) ` +
		`UNION ALL TABLE live INTERSECT DISTINCT VALUES (?, 'three') ` +
		`ORDER BY column2 OFFSET ?`
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, source); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(source)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := parser.Parse(&statement, source); err != nil {
			b.Fatal(err)
		}
	}
}
