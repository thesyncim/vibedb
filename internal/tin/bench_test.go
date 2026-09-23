package tin

import (
	"strings"
	"testing"
)

var benchCorpus = strings.Repeat(
	"The quick brown fox jumps over the lazy dog near the misty harbor. "+
		"Jalapeño café Über alles: pack my box with five dozen liquor jugs. ", 40)

func BenchmarkFoldASCII(b *testing.B) {
	buf := make([]byte, len(benchCorpus))
	b.SetBytes(int64(len(benchCorpus)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		foldASCII(buf, benchCorpus)
	}
}

func BenchmarkScan(b *testing.B) {
	b.SetBytes(int64(len(benchCorpus)))
	b.ReportAllocs()
	var n uint64
	for i := 0; i < b.N; i++ {
		scanString(benchCorpus, func(h uint64, _ uint32) { n += h })
	}
	_ = n
}

func BenchmarkAdd(b *testing.B) {
	b.SetBytes(int64(len(benchCorpus)))
	b.ReportAllocs()
	var id DocID
	ix := NewIndex()
	for i := 0; i < b.N; i++ {
		id++
		ix.Add(id, benchCorpus)
	}
}

func BenchmarkMatchTerm(b *testing.B) {
	ix := NewIndex()
	for i := DocID(1); i <= 200; i++ {
		ix.Add(i, benchCorpus)
	}
	term, _ := FoldTerm("harbor")
	out := make([]DocID, 0, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Match(Query{Op: OpTerm, Term: term}, out[:0])
	}
}

func BenchmarkScoreTopK(b *testing.B) {
	ix := NewIndex()
	for i := DocID(1); i <= 200; i++ {
		ix.Add(i, benchCorpus)
	}
	term, _ := FoldTerm("harbor")
	out := make([]Scored, 0, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ix.Score(Query{Op: OpTerm, Term: term}, 10, out[:0])
	}
}
