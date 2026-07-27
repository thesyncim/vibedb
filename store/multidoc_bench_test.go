package store

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Benchmarks for the multi-document primitives. Each one measures a primitive
// against the naive composition of existing APIs it must beat to earn its
// place: AppendKeyIDs against per-key rehashing, Segment.Append against a
// copy-plus-BuildIndex loop, and AppendPointer against a per-document
// PointerCompiled loop.

var multidocBenchSink int

// multidocBenchDocs returns count distinct small documents with a shared
// schema, the shape a multi-document engine batches: a handful of scalar
// fields, a small array, and one nested object.
func multidocBenchDocs(count int) [][]byte {
	docs := make([][]byte, count)
	for i := range docs {
		docs[i] = fmt.Appendf(nil,
			`{"id":%d,"name":"user-%04d","active":%t,"score":%d.%02d,"tags":["a","b","c-%d"],"meta":{"region":"eu-west-%d","tier":%d}}`,
			i, i, i%2 == 0, i%100, i%97, i%7, i%3, i%5)
	}
	return docs
}

// BenchmarkKeyInternerAppendKeyIDs measures the steady-state tape walk — every
// key already interned, the engine's per-document regime — on one wide object.
// The enriched variant reuses the stored per-key hashes; the unenriched one
// rehashes every key, which is also the cost floor of any composition through
// the public iterators.
func BenchmarkKeyInternerAppendKeyIDs(b *testing.B) {
	for _, tc := range []struct {
		name     string
		hashKeys bool
	}{
		{"Unenriched", false},
		{"Enriched", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			src := []byte(keyHashWideDoc(1000, ""))
			tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, len(src)+2), document.IndexOptions{HashKeys: tc.hashKeys})
			if err != nil {
				b.Fatal(err)
			}
			var in KeyInterner
			ids := in.AppendKeyIDs(nil, tape)
			keys := len(ids)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ids = in.AppendKeyIDs(ids[:0], tape)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(keys), "ns/key")
			multidocBenchSink = len(ids)
		})
	}
}

// BenchmarkSegmentAppend measures batch indexing of 1024 small documents into
// one Segment against the two naive per-document compositions: exact storage
// via a RequiredIndexEntries precount (two passes per document, tight memory)
// and heuristic worst-case storage (one pass, one oversized allocation per
// document). All three retain every index, as an engine would.
func BenchmarkSegmentAppend(b *testing.B) {
	docs := multidocBenchDocs(1024)
	var total int64
	for _, doc := range docs {
		total += int64(len(doc))
	}
	b.Run("Segment", func(b *testing.B) {
		benchmarkSegmentAppend(b, docs, total, document.IndexOptions{})
	})
	b.Run("SegmentHashKeys", func(b *testing.B) {
		benchmarkSegmentAppend(b, docs, total, document.IndexOptions{HashKeys: true})
	})
	b.Run("BaselinePrecount", func(b *testing.B) {
		b.SetBytes(total)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			indexes := make([]vibejson.Index, 0, len(docs))
			for _, doc := range docs {
				owned := append([]byte(nil), doc...)
				need, err := vibejson.RequiredIndexEntries(owned)
				if err != nil {
					b.Fatal(err)
				}
				tape, err := vibejson.BuildIndex(owned, make([]vibejson.IndexEntry, need))
				if err != nil {
					b.Fatal(err)
				}
				indexes = append(indexes, tape)
			}
			multidocBenchSink = len(indexes)
		}
	})
	b.Run("BaselineHeuristic", func(b *testing.B) {
		b.SetBytes(total)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			indexes := make([]vibejson.Index, 0, len(docs))
			for _, doc := range docs {
				owned := append([]byte(nil), doc...)
				tape, err := vibejson.BuildIndex(owned, make([]vibejson.IndexEntry, len(owned)+2))
				if err != nil {
					b.Fatal(err)
				}
				indexes = append(indexes, tape)
			}
			multidocBenchSink = len(indexes)
		}
	})
}

func benchmarkSegmentAppend(b *testing.B, docs [][]byte, total int64, opts document.IndexOptions) {
	b.SetBytes(total)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set := Segment{Options: opts}
		for _, doc := range docs {
			if _, err := set.Append(doc); err != nil {
				b.Fatal(err)
			}
		}
		multidocBenchSink = set.Len()
	}
}

// BenchmarkSegmentExtractPointer measures one compiled pointer applied to
// every document of a 1024-document set: the batch primitive, which hashes
// the pointer's tokens once, against the manual per-document PointerCompiled
// loop, which rehashes them per document on the enriched set.
func BenchmarkSegmentExtractPointer(b *testing.B) {
	docs := multidocBenchDocs(1024)
	pointer := vibejson.MustCompilePointer("/meta/region")
	for _, tc := range []struct {
		name     string
		hashKeys bool
	}{
		{"Unenriched", false},
		{"Enriched", true},
	} {
		var set Segment
		set.Options = document.IndexOptions{HashKeys: tc.hashKeys}
		for _, doc := range docs {
			if _, err := set.Append(doc); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(tc.name+"/AppendPointer", func(b *testing.B) {
			dst := make([]vibejson.RawValue, 0, set.Len())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				dst, err = set.AppendPointer(dst[:0], pointer)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(set.Len()), "ns/doc")
			multidocBenchSink = len(dst)
		})
		b.Run(tc.name+"/ManualLoop", func(b *testing.B) {
			dst := make([]vibejson.RawValue, 0, set.Len())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = dst[:0]
				for d := 0; d < set.Len(); d++ {
					value, ok, err := set.Doc(d).PointerCompiled(pointer)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						dst = append(dst, vibejson.RawValue{})
						continue
					}
					dst = append(dst, value.Raw())
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(set.Len()), "ns/doc")
			multidocBenchSink = len(dst)
		})
	}
}
