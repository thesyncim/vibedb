package storeio

import (
	"strconv"
	"testing"
)

func TestUnifiedAdmittedRowBodyLenMatchesRender(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		t.Fatal("admit unified leaf")
	}
	trivial, templated := 0, 0
	scratch := make([]byte, 0, 4096)
	for rank := 0; rank < count; rank++ {
		_, body, overflow, rowOK := view.RowRawAt(rank)
		if !rowOK || overflow {
			t.Fatalf("rank %d body: ok=%v overflow=%v", rank, rowOK, overflow)
		}
		if body[0] == unifiedRowTrivial {
			trivial++
		} else {
			templated++
		}
		rendered := view.AppendAdmittedRowBody(scratch[:0], body)
		if got := view.AdmittedRowBodyLen(body); got != len(rendered) {
			t.Fatalf("rank %d admitted length = %d, rendered = %d",
				rank, got, len(rendered))
		}
	}
	if trivial == 0 || templated == 0 {
		t.Fatalf("coverage trivial=%d templated=%d, want both", trivial, templated)
	}
}

func TestCanonicalIntRenderedLenMatchesFormatter(t *testing.T) {
	values := []int64{
		-1 << 63, -1_000_000_000_000_000_000, -1_000_000, -10, -1,
		0, 1, 9, 10, 999_999, 1_000_000,
		999_999_999_999_999_999, 1<<63 - 1,
	}
	for _, value := range values {
		want := len(strconv.FormatInt(value, 10))
		if got := canonicalIntRenderedLen(value); got != want {
			t.Fatalf("canonicalIntRenderedLen(%d) = %d, want %d",
				value, got, want)
		}
	}
}

var admittedRowBodyLenSink int

func BenchmarkUnifiedAdmittedRowBodyLen(b *testing.B) {
	records := unifiedCompetitiveShapeRecords(200)
	page, count := encodeUnifiedTestBench(b, records)
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		b.Fatal("admit")
	}
	var body []byte
	for rank := 0; rank < count; rank++ {
		_, candidate, overflow, rowOK := view.RowRawAt(rank)
		if rowOK && !overflow && candidate[0] != unifiedRowTrivial {
			body = candidate
			break
		}
	}
	if len(body) == 0 {
		b.Fatal("templated row")
	}
	b.Run("render", func(b *testing.B) {
		scratch := make([]byte, 0, 4096)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			scratch = view.AppendAdmittedRowBody(scratch[:0], body)
		}
		admittedRowBodyLenSink = len(scratch)
	})
	b.Run("length-only", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			admittedRowBodyLenSink = view.AdmittedRowBodyLen(body)
		}
	})
}
