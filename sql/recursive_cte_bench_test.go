package sql

import "testing"

func BenchmarkRecursiveCTEParserPreparedReuse(b *testing.B) {
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, recursiveCTEParseSQL); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(recursiveCTEParseSQL)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := parser.Parse(&statement, recursiveCTEParseSQL); err != nil {
			b.Fatal(err)
		}
	}
}
