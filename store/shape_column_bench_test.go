package store

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Benchmarks for fused corpus extraction. AppendField earns its place against
// the compositions an engine writes today — per-document Resolve plus a
// cached In, Segment.AppendPointer, and a per-document GetCompiled loop — on
// the homogeneous batch it serves, and must degrade gracefully on the batches
// it does not: shape-clustered runs, alternating shapes, all-distinct
// layouts, non-flat roots, and absent fields. The inline-cache win is
// isolated by appendFieldResolveEach, the identical loop with the inline
// hint cache disabled.

// appendFieldResolveEach is AppendField with the inline cache off: every
// document resolves through the cache table (fold plus probe), the FieldRef
// is still memoized per shape change, and all fallbacks match. The delta to
// AppendField is therefore exactly the inline cache: the table probe skipped
// on a run-extending document plus the hoisted call boundaries.
func appendFieldResolveEach(c *ShapeCache, dst []vibejson.RawValue, s *Segment, name string) []vibejson.RawValue {
	key := vibejson.CompileKey(name)
	var (
		rec *ShapeRecord
		ref FieldRef
		has bool
	)
	for i := range s.docs {
		root := s.docs[i].Root()
		if root.Entry == nil {
			dst = append(dst, vibejson.RawValue{})
			continue
		}
		shape, ok := c.Resolve(root)
		if !ok {
			dst = appendFieldGet(dst, root, key)
			continue
		}
		if shape.rec != rec {
			rec = shape.rec
			ref, has = shape.Field(name)
		}
		if !has {
			dst = appendFieldGet(dst, root, key)
			continue
		}
		if v, hit := ref.In(root); hit {
			dst = append(dst, v.Raw())
			continue
		}
		dst = appendFieldGet(dst, root, key)
	}
	return dst
}

// shapeColumnClusteredDocs returns a 4-shape corpus in runs of runLen: four
// distinct 16-field flat layouts sharing the queried fields, so transitions
// are rare and every shape answers the battery. All four layouts have equal
// width, the harder routing case: the header gate passes and only the
// fingerprint separates them.
func shapeColumnClusteredDocs(runLen, runs int, b testing.TB) *Segment {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: true}
	for r := 0; r < runs; r++ {
		layout := r % 4
		for i := 0; i < runLen; i++ {
			doc := fmt.Appendf(nil,
				`{"id":%d,"ts":%d,"name":"u-%04d","flags":%d,"v%d_a":1,"v%d_b":2,"v%d_c":3,"v%d_d":4,"v%d_e":5,"v%d_f":6,"v%d_g":7,"v%d_h":8,"v%d_i":9,"v%d_j":10,"v%d_k":11,"v%d_l":12}`,
				i, 1700000000+i, i, i%8,
				layout, layout, layout, layout, layout, layout,
				layout, layout, layout, layout, layout, layout)
			if _, err := set.Append(doc); err != nil {
				b.Fatal(err)
			}
		}
	}
	return &set
}

// shapeColumnNestedDocs returns count documents whose roots are objects with
// container-valued members: never flat, so every document takes the exact
// fallback and the shape machinery must stay out of the way.
func shapeColumnNestedDocs(count int, b testing.TB) *Segment {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: true}
	for i := 0; i < count; i++ {
		doc := fmt.Appendf(nil,
			`{"id":%d,"user":{"name":"u-%04d","tier":%d},"tags":[%d,%d],"ts":%d,"score":%d.%02d,"meta":{"region":"eu-west-%d"}}`,
			i, i, i%5, i, i+1, 1700000000+i, i%100, i%97, i%3)
		if _, err := set.Append(doc); err != nil {
			b.Fatal(err)
		}
	}
	return &set
}

// warmShapeColumn resolves every document once so the sighting gate has
// compiled each recurring layout before measurement.
func warmShapeColumn(cache *ShapeCache, set *Segment, b *testing.B) {
	for d := 0; d < set.Len(); d++ {
		cache.Resolve(set.Doc(d).Root())
	}
	for d := 0; d < set.Len(); d++ {
		if _, ok := cache.Resolve(set.Doc(d).Root()); !ok {
			b.Fatal("warm Resolve declined a recurring layout")
		}
	}
}

// BenchmarkShapeColumnSteadyState is the headline workload: one field
// extracted from each of 1024 same-shape 16-field documents, AppendField
// against the per-document engine loop, the inline-cache-off loop,
// AppendPointer, and a per-document GetCompiled loop.
func BenchmarkShapeColumnSteadyState(b *testing.B) {
	set := shapeBenchDocs(1024, true, b)
	docs := set.Len()
	perDoc := func(b *testing.B, got int) {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = got
	}
	b.Run("AppendField", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = cache.AppendField(dst[:0], set, "ts")
		}
		perDoc(b, len(dst))
	})
	b.Run("ResolveEach", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = appendFieldResolveEach(&cache, dst[:0], set, "ts")
		}
		perDoc(b, len(dst))
	})
	b.Run("ResolveIn", func(b *testing.B) {
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		shape, ok := cache.Resolve(set.Doc(0).Root())
		if !ok {
			b.Fatal("Resolve declined the batch shape")
		}
		ref, ok := shape.Field("ts")
		if !ok {
			b.Fatal("Field(ts) missing")
		}
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			for d := 0; d < docs; d++ {
				root := set.Doc(d).Root()
				if _, ok := cache.Resolve(root); !ok {
					b.Fatal("shape miss")
				}
				if v, ok := ref.In(root); ok {
					dst = append(dst, v.Raw())
				} else {
					dst = append(dst, vibejson.RawValue{})
				}
			}
		}
		perDoc(b, len(dst))
	})
	b.Run("AppendPointer", func(b *testing.B) {
		pointer, err := vibejson.CompilePointer("/ts")
		if err != nil {
			b.Fatal(err)
		}
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst, err = set.AppendPointer(dst[:0], pointer)
			if err != nil {
				b.Fatal(err)
			}
		}
		perDoc(b, len(dst))
	})
	b.Run("GetCompiled", func(b *testing.B) {
		key := vibejson.CompileKey("ts")
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			for d := 0; d < docs; d++ {
				dst = appendFieldGet(dst, set.Doc(d).Root(), key)
			}
		}
		perDoc(b, len(dst))
	})
}

// BenchmarkShapeColumnClustered measures the shape-clustered corpus — four
// equal-width layouts in runs of 256 — where the inline cache should hold
// within runs and re-probe only at boundaries, against the same loop with
// the cache off.
func BenchmarkShapeColumnClustered(b *testing.B) {
	set := shapeColumnClusteredDocs(256, 4, b)
	docs := set.Len()
	for _, tc := range []struct {
		name string
		run  func(cache *ShapeCache, dst []vibejson.RawValue) []vibejson.RawValue
	}{
		{"InlineOn", func(cache *ShapeCache, dst []vibejson.RawValue) []vibejson.RawValue {
			return cache.AppendField(dst, set, "ts")
		}},
		{"InlineOff", func(cache *ShapeCache, dst []vibejson.RawValue) []vibejson.RawValue {
			return appendFieldResolveEach(cache, dst, set, "ts")
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var cache ShapeCache
			warmShapeColumn(&cache, set, b)
			dst := make([]vibejson.RawValue, 0, docs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = tc.run(&cache, dst[:0])
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = len(dst)
		})
	}
}

// shapeColumnShiftedDocs returns 1024 documents of two equal-width layouts
// in strict alternation, with the queried field at a different member
// position in each: layout A leads with ts, layout B buries it behind eight
// pad fields. Every document therefore rejects the previous document's
// positional hint and must re-route.
func shapeColumnShiftedDocs(b testing.TB) *Segment {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: true}
	for i := 0; i < 1024; i++ {
		var doc []byte
		if i%2 == 0 {
			doc = fmt.Appendf(nil,
				`{"ts":%d,"a0":0,"a1":1,"a2":2,"a3":3,"a4":4,"a5":5,"a6":6,"a7":7,"a8":8,"a9":9,"a10":10,"a11":11,"a12":12,"a13":13,"a14":14}`,
				1700000000+i)
		} else {
			doc = fmt.Appendf(nil,
				`{"b0":0,"b1":1,"b2":2,"b3":3,"b4":4,"b5":5,"b6":6,"b7":7,"ts":%d,"b8":8,"b9":9,"b10":10,"b11":11,"b12":12,"b13":13,"b14":14}`,
				1700000000+i)
		}
		if _, err := set.Append(doc); err != nil {
			b.Fatal(err)
		}
	}
	return &set
}

// BenchmarkShapeColumnAlternating strictly alternates two equal-width
// layouts, the inline hint's worst recurring cadence. SamePosition keeps the
// queried field at one member position in both layouts — the schema-evolution
// case — where the positional hint survives the layout change and alternation
// costs nothing. Shifted moves the field between positions, so every document
// rejects the hint and pays a full re-route; the fused loop must stay near
// the inline-off loop there, cliff-free.
func BenchmarkShapeColumnAlternating(b *testing.B) {
	for _, corpus := range []struct {
		name string
		set  *Segment
	}{
		{"SamePosition", shapeColumnClusteredDocs(1, 1024, b)},
		{"Shifted", shapeColumnShiftedDocs(b)},
	} {
		set := corpus.set
		docs := set.Len()
		for _, tc := range []struct {
			name string
			run  func(cache *ShapeCache, dst []vibejson.RawValue) []vibejson.RawValue
		}{
			{"InlineOn", func(cache *ShapeCache, dst []vibejson.RawValue) []vibejson.RawValue {
				return cache.AppendField(dst, set, "ts")
			}},
			{"InlineOff", func(cache *ShapeCache, dst []vibejson.RawValue) []vibejson.RawValue {
				return appendFieldResolveEach(cache, dst, set, "ts")
			}},
		} {
			b.Run(corpus.name+"/"+tc.name, func(b *testing.B) {
				var cache ShapeCache
				warmShapeColumn(&cache, set, b)
				dst := make([]vibejson.RawValue, 0, docs)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst = tc.run(&cache, dst[:0])
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
				shapeBenchSink = len(dst)
			})
		}
	}
}

// BenchmarkShapeColumnAdversarial covers the corpora the fast path cannot
// serve. AllDistinct runs 1024 unique equal-width layouts against a fresh
// cache per pass — every document folds, is sighting-gated, and falls back —
// bounded by the fold plus the exact lookup. NonFlat roots reject before the
// fold and must ride the exact fallback at near-GetCompiled cost. Absent
// extracts a field no document has: after one compile the run skips shape
// resolution entirely, so the whole batch is the exact lookup plus the
// header gate. Each names its GetCompiled floor.
func BenchmarkShapeColumnAdversarial(b *testing.B) {
	b.Run("AllDistinct/AppendField", func(b *testing.B) {
		set := shapeBenchHeteroDocs(1024, b)
		docs := set.Len()
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var cache ShapeCache
			dst = cache.AppendField(dst[:0], set, "doc0000_field07")
			if len(dst) != docs {
				b.Fatal("short batch")
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = len(dst)
	})
	b.Run("AllDistinct/GetCompiled", func(b *testing.B) {
		set := shapeBenchHeteroDocs(1024, b)
		docs := set.Len()
		key := vibejson.CompileKey("doc0000_field07")
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			for d := 0; d < docs; d++ {
				dst = appendFieldGet(dst, set.Doc(d).Root(), key)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = len(dst)
	})
	nested := func(b *testing.B) *Segment { return shapeColumnNestedDocs(1024, b) }
	b.Run("NonFlat/AppendField", func(b *testing.B) {
		set := nested(b)
		docs := set.Len()
		var cache ShapeCache
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = cache.AppendField(dst[:0], set, "ts")
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = len(dst)
	})
	b.Run("NonFlat/GetCompiled", func(b *testing.B) {
		set := nested(b)
		docs := set.Len()
		key := vibejson.CompileKey("ts")
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			for d := 0; d < docs; d++ {
				dst = appendFieldGet(dst, set.Doc(d).Root(), key)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = len(dst)
	})
	b.Run("Absent/AppendField", func(b *testing.B) {
		set := shapeBenchDocs(1024, true, b)
		docs := set.Len()
		var cache ShapeCache
		warmShapeColumn(&cache, set, b)
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = cache.AppendField(dst[:0], set, "absent_field")
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = len(dst)
	})
	b.Run("Absent/GetCompiled", func(b *testing.B) {
		set := shapeBenchDocs(1024, true, b)
		docs := set.Len()
		key := vibejson.CompileKey("absent_field")
		dst := make([]vibejson.RawValue, 0, docs)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			for d := 0; d < docs; d++ {
				dst = appendFieldGet(dst, set.Doc(d).Root(), key)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = len(dst)
	})
}

// BenchmarkShapeColumnMultiField is the projection loop: 4 and 16 fields per
// document over the homogeneous corpus, AppendFields against as many
// AppendField passes and a per-document GetCompiled loop. One fold per
// document should amortize across all names.
func BenchmarkShapeColumnMultiField(b *testing.B) {
	set := shapeBenchDocs(1024, true, b)
	docs := set.Len()
	fieldSets := map[string][]string{
		"Fields4":  {"id", "name", "ts", "flags"},
		"Fields16": {"id", "user_id", "name", "email", "active", "score", "tier", "region", "ts", "visits", "balance", "lang", "theme", "tz", "ref", "flags"},
	}
	for _, group := range []string{"Fields4", "Fields16"} {
		names := fieldSets[group]
		keys := make([]vibejson.CompiledKey, len(names))
		for j, name := range names {
			keys[j] = vibejson.CompileKey(name)
		}
		newColumns := func() [][]vibejson.RawValue {
			cols := make([][]vibejson.RawValue, len(names))
			for j := range cols {
				cols[j] = make([]vibejson.RawValue, 0, docs)
			}
			return cols
		}
		resetColumns := func(cols [][]vibejson.RawValue) [][]vibejson.RawValue {
			for j := range cols {
				cols[j] = cols[j][:0]
			}
			return cols
		}
		b.Run(group+"/AppendFields", func(b *testing.B) {
			var cache ShapeCache
			warmShapeColumn(&cache, set, b)
			cols := newColumns()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cols = cache.AppendFields(resetColumns(cols), set, names...)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = len(cols[0])
		})
		b.Run(group+"/ResolveInExtract", func(b *testing.B) {
			// The hand-written engine composition doing the same extraction:
			// one Resolve per document, hoisted FieldRefs, one verified read
			// and append per name, exact fallback on any miss.
			var cache ShapeCache
			warmShapeColumn(&cache, set, b)
			shape, ok := cache.Resolve(set.Doc(0).Root())
			if !ok {
				b.Fatal("Resolve declined the batch shape")
			}
			refs := make([]FieldRef, len(names))
			for j, name := range names {
				if refs[j], ok = shape.Field(name); !ok {
					b.Fatalf("Field(%q) missing", name)
				}
			}
			cols := newColumns()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cols = resetColumns(cols)
				for d := 0; d < docs; d++ {
					root := set.Doc(d).Root()
					if _, ok := cache.Resolve(root); !ok {
						b.Fatal("shape miss")
					}
					for j := range refs {
						if v, ok := refs[j].In(root); ok {
							cols[j] = append(cols[j], v.Raw())
						} else {
							cols[j] = appendFieldGet(cols[j], root, keys[j])
						}
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = len(cols[0])
		})
		b.Run(group+"/AppendFieldN", func(b *testing.B) {
			var cache ShapeCache
			warmShapeColumn(&cache, set, b)
			cols := newColumns()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cols = resetColumns(cols)
				for j, name := range names {
					cols[j] = cache.AppendField(cols[j], set, name)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = len(cols[0])
		})
		b.Run(group+"/GetCompiled", func(b *testing.B) {
			cols := newColumns()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cols = resetColumns(cols)
				for d := 0; d < docs; d++ {
					root := set.Doc(d).Root()
					for j := range keys {
						cols[j] = appendFieldGet(cols[j], root, keys[j])
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = len(cols[0])
		})
	}
}
