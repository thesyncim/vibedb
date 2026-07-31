package storeio

import (
	"testing"

	"github.com/thesyncim/vibejson"
)

// These benchmarks measure canonical rendering of the 250 B competitive
// corpus shape, the already-canonical check, and the integer predicate. The
// render target is ≤ 250 ns with 0 allocations.

// BenchmarkCanonicalRenderCompetitiveShape renders the non-canonical source
// spelling (unsorted keys, as the corpus generates them), so the member sort
// and every token append are on the measured path. The tape is prebuilt:
// the gate prices the render, and admission builds the tape anyway.
func BenchmarkCanonicalRenderCompetitiveShape(b *testing.B) {
	src := []byte(competitiveShapeJSON)
	index := buildTestIndex(b, src)
	ws := &CanonicalWorkspace{}
	dst := make([]byte, 0, 2*len(src))
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = AppendCanonicalIndexed(dst[:0], index, ws)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCanonicalRenderAlreadyCanonical renders canonical input: the
// sortedness scan short-circuits the sort and every string takes the
// verbatim fast path used by already-canonical input.
func BenchmarkCanonicalRenderAlreadyCanonical(b *testing.B) {
	ws := &CanonicalWorkspace{}
	canonical, err := vibejson.AppendCanonicalize(nil, []byte(competitiveShapeJSON))
	if err != nil {
		b.Fatal(err)
	}
	index := buildTestIndex(b, canonical)
	dst := make([]byte, 0, 2*len(canonical))
	b.SetBytes(int64(len(canonical)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = AppendCanonicalIndexed(dst[:0], index, ws)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCanonicalCheckCanonical prices the already-canonical fast
// check on input that passes it — the cost every steady-state admission
// pays.
func BenchmarkCanonicalCheckCanonical(b *testing.B) {
	ws := &CanonicalWorkspace{}
	canonical, err := vibejson.AppendCanonicalize(nil, []byte(competitiveShapeJSON))
	if err != nil {
		b.Fatal(err)
	}
	index := buildTestIndex(b, canonical)
	b.SetBytes(int64(len(canonical)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !IndexIsCanonical(index, ws) {
			b.Fatal("canonical input rejected")
		}
	}
}

// BenchmarkCanonicalCheckNonCanonical prices the check on the raw corpus
// spelling, which fails at the first out-of-order member.
func BenchmarkCanonicalCheckNonCanonical(b *testing.B) {
	ws := &CanonicalWorkspace{}
	src := []byte(competitiveShapeJSON)
	index := buildTestIndex(b, src)
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if IndexIsCanonical(index, ws) {
			b.Fatal("non-canonical input accepted")
		}
	}
}

// BenchmarkTokenCanonicalIntPredicate prices the canonical-integer admission predicate
// plus payload encode over the corpus-typical spellings (ids, scores) and
// the rejecting spellings the corpus carries (floats stay literals).
func BenchmarkTokenCanonicalIntPredicate(b *testing.B) {
	spellings := [][]byte{
		[]byte("7"), []byte("481"), []byte("99999"), []byte("-12034"),
		[]byte("999999999999999999"), []byte("0"),
		[]byte("0.5"), []byte("1e9"), []byte("1000000000000000000"),
	}
	var payload [12]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := spellings[i%len(spellings)]
		if v, ok := CanonicalIntValue(s); ok {
			buf := AppendZigzagVarint(payload[:0], v)
			if d, n := DecodeZigzagVarint(buf); n == 0 || d != v {
				b.Fatal("round trip failed")
			}
		}
	}
}
