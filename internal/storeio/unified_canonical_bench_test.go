package storeio

import (
	"fmt"
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

// BenchmarkStableExtentCanonicalCertificate measures the exact non-canonical
// concurrent-write qualification sequence. legacy_reparse models the old
// path: build the source tape, render canonical bytes, then build a second tape
// inside PatchStableReplacementKeepsExtent. spanned_render records the scalar
// spans during that render and passes the opaque certificate directly to the
// patch proof.
func BenchmarkStableExtentCanonicalCertificate(b *testing.B) {
	src := []byte(competitiveShapeJSON)
	records := make([]CommonPrimaryLeafRecord, 128)
	for i := range records {
		records[i] = CommonPrimaryLeafRecord{
			Key: fmt.Appendf(nil, "doc:%08d", i),
			Value: CommonPrimaryLeafValue{
				Inline: src,
			},
		}
	}
	page, count := encodeUnifiedTestLeaf(b, records)
	if count == 0 {
		b.Fatal("benchmark leaf encoded no rows")
	}
	view := openUnifiedTestLeaf(b, page)
	slots, ok := view.PostingSlots()
	if !ok {
		b.Fatal("benchmark leaf posting slots")
	}
	key := records[0].Key

	const entries = 512
	b.Run("legacy_reparse", func(b *testing.B) {
		sourceStorage := make([]vibejson.IndexEntry, entries)
		patchStorage := make([]vibejson.IndexEntry, entries)
		spanStorage := make([]UnifiedTokenSpan, 0, 2*entries)
		workspace := NewCanonicalWorkspace(entries, 2*len(src))
		canonical := make([]byte, 0, 2*len(src))
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			index, err := vibejson.BuildIndex(src, sourceStorage)
			if err != nil {
				b.Fatal(err)
			}
			if IndexIsCanonical(index, &workspace) {
				b.Fatal("benchmark source unexpectedly canonical")
			}
			canonical, err = AppendCanonicalIndexed(
				canonical[:0], index, &workspace,
			)
			if err != nil {
				b.Fatal(err)
			}
			stable, err := view.PatchStableReplacementKeepsExtent(
				CommonPrimaryUnifiedReplacement{
					Key: key, Value: canonical, Slot: slots[0],
				},
				patchStorage, &workspace, spanStorage[:0],
			)
			if err != nil || !stable {
				b.Fatalf("legacy certificate = %v,%v", stable, err)
			}
		}
	})

	b.Run("spanned_render", func(b *testing.B) {
		sourceStorage := make([]vibejson.IndexEntry, entries)
		patchStorage := make([]vibejson.IndexEntry, entries)
		spanStorage := make([]UnifiedTokenSpan, 0, 2*entries)
		workspace := NewCanonicalWorkspace(entries, 2*len(src))
		canonical := make([]byte, 0, 2*len(src))
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			index, err := vibejson.BuildIndex(src, sourceStorage)
			if err != nil {
				b.Fatal(err)
			}
			if _, canonicalSource := CanonicalSpanIndexOf(
				index, &workspace, spanStorage[:0],
			); canonicalSource {
				b.Fatal("benchmark source unexpectedly canonical")
			}
			var certificate CanonicalSpanIndex
			canonical, certificate, err = AppendCanonicalIndexedSpans(
				canonical[:0], index, &workspace, spanStorage[:0],
			)
			if err != nil {
				b.Fatal(err)
			}
			stable, err := view.PatchStableCanonicalReplacementKeepsExtent(
				key, slots[0], certificate, patchStorage, &workspace,
			)
			if err != nil || !stable {
				b.Fatalf("spanned certificate = %v,%v", stable, err)
			}
		}
	})
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
