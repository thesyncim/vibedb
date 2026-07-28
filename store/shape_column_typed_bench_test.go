package store

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Benchmarks for typed corpus extraction. The fused drivers earn their place
// against the compositions an engine writes today: AppendField into a
// []RawValue column followed by a parse loop (which re-validates every
// spelling because the raw span lost its tape classification), and a
// per-document GetCompiled plus accessor loop. NodesParse isolates the
// honest-exit question — it routes through the same fieldScan but emits a
// []Node intermediate and parses in a second pass, so its delta to Fused
// prices the fusion proper (no intermediate stores, one pass) separately
// from the win of keeping tape classification, which both share and the
// RawValue composition lost. Adversarial corpora — mixed validity,
// all-distinct layouts, all-absent fields — bound the typed drivers' cost
// where the fast path cannot serve.

var (
	typedBenchSinkInt   int64
	typedBenchSinkFloat float64
	typedBenchSinkBool  bool
)

// appendFieldNodes is the trivial-helper alternative: fieldScan's routing,
// emitting the resolved value Node per document (the zero Node for absent)
// instead of a parsed cell.
func appendFieldNodes(c *ShapeCache, dst []vibejson.Node, s *Segment, name string) []vibejson.Node {
	fs := newFieldScan(name)
	for i := range s.docs {
		root := s.docs[i].Root()
		if root.Entry == nil {
			dst = append(dst, vibejson.Node{})
			continue
		}
		if e := fs.next(c, root); e != nil {
			dst = append(dst, vibejson.Node{Src: root.Src, Entry: e})
			continue
		}
		if v, ok := root.GetCompiled(fs.key); ok {
			dst = append(dst, v)
		} else {
			dst = append(dst, vibejson.Node{})
		}
	}
	return dst
}

// typedColumnFloatDocs returns count homogeneous documents whose "m" member
// cycles three float families: short decimals the exact-multiply envelope
// consumes, seventeen-significant-digit spellings with exponents that
// exercise Eisel-Lemire, and long fixed-point spellings in the
// fixed-decimal window.
func typedColumnFloatDocs(count int, b testing.TB) *Segment {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: true}
	for i := range count {
		var m string
		switch i % 3 {
		case 0:
			m = fmt.Sprintf("%d.%02d", i%100, i%97)
		case 1:
			m = fmt.Sprintf("%d.%016de-%d", 1+i%9, i*7, i%40+5)
		default:
			m = fmt.Sprintf("%02d.%014d", i%90+10, i*13)
		}
		doc := fmt.Appendf(nil,
			`{"id":%d,"m":%s,"name":"u-%04d","ts":%d,"a":1,"b":2,"c":3,"d":4}`,
			i, m, i, 1700000000+i)
		if _, err := set.Append(doc); err != nil {
			b.Fatal(err)
		}
	}
	return &set
}

// benchPerDoc reports the per-document rate beside the per-pass time.
func benchPerDoc(b *testing.B, docs int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
}

// BenchmarkTypedColumnInt64 is the headline workload: a dense int64 column
// with validity from each of 1024 same-shape 16-field documents.
func BenchmarkTypedColumnInt64(b *testing.B) {
	set := shapeBenchDocs(1024, true, b)
	docs := set.Len()
	b.Run("Fused", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cache.AppendFieldInt64(cells[:0], valid[:0], set, "ts")
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
		typedBenchSinkBool = valid[0]
	})
	b.Run("RawParse", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		raw := make([]vibejson.RawValue, 0, docs)
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			raw = cache.AppendField(raw[:0], set, "ts")
			cells, valid = cells[:0], valid[:0]
			for _, r := range raw {
				n, ok := r.Int64()
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("NodesParse", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		nodes := make([]vibejson.Node, 0, docs)
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nodes = appendFieldNodes(&cache, nodes[:0], set, "ts")
			cells, valid = cells[:0], valid[:0]
			for _, v := range nodes {
				n, ok := v.Int64()
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("GetParse", func(b *testing.B) {
		key := vibejson.CompileKey("ts")
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cells[:0], valid[:0]
			for d := range docs {
				var n int64
				var ok bool
				if v, present := set.Doc(d).Root().GetCompiled(key); present {
					n, ok = v.Int64()
				}
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
}

// BenchmarkTypedColumnFloat64 extracts float columns from the short-decimal
// field of the standard corpus and from the mixed-hardness float corpus,
// where a third of the spellings ride Eisel-Lemire.
func BenchmarkTypedColumnFloat64(b *testing.B) {
	corpora := []struct {
		name  string
		set   *Segment
		field string
	}{
		{"Short", shapeBenchDocs(1024, true, b), "score"},
		{"Hard", typedColumnFloatDocs(1024, b), "m"},
	}
	for _, corpus := range corpora {
		set, field := corpus.set, corpus.field
		docs := set.Len()
		b.Run(corpus.name+"/Fused", func(b *testing.B) {
			var cache ShapeCache
			warmShapeColumn(&cache, set, b)
			cells := make([]float64, 0, docs)
			valid := make([]bool, 0, docs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cells, valid = cache.AppendFieldFloat64(cells[:0], valid[:0], set, field)
			}
			benchPerDoc(b, docs)
			typedBenchSinkFloat = cells[0]
			typedBenchSinkBool = valid[0]
		})
		b.Run(corpus.name+"/RawParse", func(b *testing.B) {
			var cache ShapeCache
			warmShapeColumn(&cache, set, b)
			raw := make([]vibejson.RawValue, 0, docs)
			cells := make([]float64, 0, docs)
			valid := make([]bool, 0, docs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				raw = cache.AppendField(raw[:0], set, field)
				cells, valid = cells[:0], valid[:0]
				for _, r := range raw {
					f, ok := r.Float64()
					if !ok {
						f = 0
					}
					cells = append(cells, f)
					valid = append(valid, ok)
				}
			}
			benchPerDoc(b, docs)
			typedBenchSinkFloat = cells[0]
		})
		b.Run(corpus.name+"/GetParse", func(b *testing.B) {
			key := vibejson.CompileKey(field)
			cells := make([]float64, 0, docs)
			valid := make([]bool, 0, docs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cells, valid = cells[:0], valid[:0]
				for d := range docs {
					var f float64
					var ok bool
					if v, present := set.Doc(d).Root().GetCompiled(key); present {
						f, ok = v.Float64()
					}
					if !ok {
						f = 0
					}
					cells = append(cells, f)
					valid = append(valid, ok)
				}
			}
			benchPerDoc(b, docs)
			typedBenchSinkFloat = cells[0]
		})
	}
}

// BenchmarkTypedColumnBool extracts the boolean field of the standard
// corpus, the nearly-free third driver.
func BenchmarkTypedColumnBool(b *testing.B) {
	set := shapeBenchDocs(1024, true, b)
	docs := set.Len()
	b.Run("Fused", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		cells := make([]bool, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cache.AppendFieldBool(cells[:0], valid[:0], set, "active")
		}
		benchPerDoc(b, docs)
		typedBenchSinkBool = cells[0]
	})
	b.Run("GetParse", func(b *testing.B) {
		key := vibejson.CompileKey("active")
		cells := make([]bool, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cells[:0], valid[:0]
			for d := range docs {
				var bl bool
				var ok bool
				if v, present := set.Doc(d).Root().GetCompiled(key); present {
					bl, ok = v.Bool()
				}
				cells = append(cells, bl)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkBool = cells[0]
	})
}

// BenchmarkTypedColumnMixedValidity prices the validity handling: 65% valid
// integers, 20% absent under an equal-width foreign layout, 10% null, 5%
// string-typed.
func BenchmarkTypedColumnMixedValidity(b *testing.B) {
	set := typedColumnMixedValiditySet(b, 1024)
	docs := set.Len()
	b.Run("Fused", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cache.AppendFieldInt64(cells[:0], valid[:0], set, "v")
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("RawParse", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		raw := make([]vibejson.RawValue, 0, docs)
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			raw = cache.AppendField(raw[:0], set, "v")
			cells, valid = cells[:0], valid[:0]
			for _, r := range raw {
				n, ok := r.Int64()
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("GetParse", func(b *testing.B) {
		key := vibejson.CompileKey("v")
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cells[:0], valid[:0]
			for d := range docs {
				var n int64
				var ok bool
				if v, present := set.Doc(d).Root().GetCompiled(key); present {
					n, ok = v.Int64()
				}
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
}

// BenchmarkTypedColumnAdversarial covers the corpora the fast path cannot
// serve: 1024 all-distinct layouts against a fresh sighting-gated cache per
// pass, and an all-absent field over the warm homogeneous corpus.
func BenchmarkTypedColumnAdversarial(b *testing.B) {
	b.Run("AllDistinct/Fused", func(b *testing.B) {
		set := shapeBenchHeteroDocs(1024, b)
		docs := set.Len()
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var cache ShapeCache
			cells, valid = cache.AppendFieldInt64(cells[:0], valid[:0], set, "doc0000_field07")
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("AllDistinct/GetParse", func(b *testing.B) {
		set := shapeBenchHeteroDocs(1024, b)
		docs := set.Len()
		key := vibejson.CompileKey("doc0000_field07")
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cells[:0], valid[:0]
			for d := range docs {
				var n int64
				var ok bool
				if v, present := set.Doc(d).Root().GetCompiled(key); present {
					n, ok = v.Int64()
				}
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("AllAbsent/Fused", func(b *testing.B) {
		set := shapeBenchDocs(1024, true, b)
		docs := set.Len()
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cache.AppendFieldInt64(cells[:0], valid[:0], set, "absent_field")
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
	b.Run("AllAbsent/GetParse", func(b *testing.B) {
		set := shapeBenchDocs(1024, true, b)
		docs := set.Len()
		key := vibejson.CompileKey("absent_field")
		cells := make([]int64, 0, docs)
		valid := make([]bool, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cells, valid = cells[:0], valid[:0]
			for d := range docs {
				var n int64
				var ok bool
				if v, present := set.Doc(d).Root().GetCompiled(key); present {
					n, ok = v.Int64()
				}
				if !ok {
					n = 0
				}
				cells = append(cells, n)
				valid = append(valid, ok)
			}
		}
		benchPerDoc(b, docs)
		typedBenchSinkInt = cells[0]
	})
}
