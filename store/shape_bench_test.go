package store

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Benchmarks for shape-compiled field access. The shape path earns its place
// against the existing lookup ladder on the workload it serves — a batch of
// same-layout documents — and must degrade gracefully on the workload it does
// not: a heterogeneous batch where every document compiles a new shape, whose
// bound is one ObjectProbe build per document.

var shapeBenchSink int

// shapeBenchDocs returns count documents of one 16-field flat layout with
// varying value spellings, indexed into one Segment.
func shapeBenchDocs(count int, hashKeys bool, b *testing.B) *Segment {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: hashKeys}
	for i := 0; i < count; i++ {
		doc := fmt.Appendf(nil,
			`{"id":%d,"user_id":%d,"name":"user-%04d","email":"u%d@example.com","active":%t,"score":%d.%02d,"tier":%d,"region":"eu-west-%d","ts":%d,"visits":%d,"balance":%d,"lang":"en","theme":"dark","tz":"UTC","ref":"organic","flags":%d}`,
			i, i*7, i, i, i%2 == 0, i%100, i%97, i%5, i%3, 1700000000+i, i%1000, i*13, i%8)
		if _, err := set.Append(doc); err != nil {
			b.Fatal(err)
		}
	}
	return &set
}

// shapeBenchHeteroDocs returns count documents that all differ in shape:
// 16 fields each, key spellings unique per document.
func shapeBenchHeteroDocs(count int, b *testing.B) *Segment {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: true}
	for i := 0; i < count; i++ {
		doc := []byte("{")
		for f := 0; f < 16; f++ {
			if f > 0 {
				doc = append(doc, ',')
			}
			doc = fmt.Appendf(doc, `"doc%04d_field%02d":%d`, i, f, i+f)
		}
		doc = append(doc, '}')
		if _, err := set.Append(doc); err != nil {
			b.Fatal(err)
		}
	}
	return &set
}

// BenchmarkShapeSteadyState is the headline workload: one field extracted
// from each of 1024 same-shape documents, comparing the shape path — the
// full engine loop with a per-document Resolve, and the bare In a caller
// with a known-homogeneous batch can use since In self-verifies — against
// Get, GetCompiled, and a per-document probe build. Flat Get scans every
// member regardless of the queried field's position (the last duplicate must
// win), so the choice of field does not favor the shape path.
func BenchmarkShapeSteadyState(b *testing.B) {
	set := shapeBenchDocs(1024, true, b)
	docs := set.Len()
	var cache ShapeCache
	shape, ok := resolveCompiled(&cache, set.Doc(0).Root())
	if !ok {
		b.Fatal("Resolve declined the batch shape")
	}
	ref, ok := shape.Field("ts")
	if !ok {
		b.Fatal("Field(ts) missing")
	}
	key := vibejson.CompileKey("ts")
	perDoc := func(b *testing.B, hits int) {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = hits
	}
	b.Run("ShapeResolveIn", func(b *testing.B) {
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				root := set.Doc(d).Root()
				s, ok := cache.Resolve(root)
				if !ok || s != shape {
					b.Fatal("shape miss")
				}
				if _, ok := ref.In(root); ok {
					hits++
				}
			}
		}
		perDoc(b, hits)
	})
	b.Run("ShapeInOnly", func(b *testing.B) {
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				if _, ok := ref.In(set.Doc(d).Root()); ok {
					hits++
				}
			}
		}
		perDoc(b, hits)
	})
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				if _, ok := set.Doc(d).Root().Get("ts"); ok {
					hits++
				}
			}
		}
		perDoc(b, hits)
	})
	b.Run("GetCompiled", func(b *testing.B) {
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				if _, ok := set.Doc(d).Root().GetCompiled(key); ok {
					hits++
				}
			}
		}
		perDoc(b, hits)
	})
	b.Run("ProbeBuildGet", func(b *testing.B) {
		storage := make([]vibejson.ProbeSlot, vibejson.RequiredProbeSlots(set.Doc(0).Root()))
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				probe, ok := vibejson.BuildObjectProbe(set.Doc(d).Root(), storage)
				if !ok {
					b.Fatal("probe declined")
				}
				if _, ok := probe.Get("ts"); ok {
					hits++
				}
			}
		}
		perDoc(b, hits)
	})
}

// BenchmarkShapeFingerprintFold isolates the per-document tax of the steady
// state: one Resolve of an already compiled shape, per width and enrichment.
// The enriched fold reads two tape words per member; the unenriched fold
// hashes each key's content inline.
func BenchmarkShapeFingerprintFold(b *testing.B) {
	for _, width := range []int{4, 16, 64, 256} {
		for _, tc := range []struct {
			name     string
			hashKeys bool
		}{
			{"Enriched", true},
			{"Unenriched", false},
		} {
			b.Run(fmt.Sprintf("%s/width%d", tc.name, width), func(b *testing.B) {
				src := []byte(shapeFlatDoc(width, ""))
				tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, len(src)+2), document.IndexOptions{HashKeys: tc.hashKeys})
				if err != nil {
					b.Fatal(err)
				}
				var cache ShapeCache
				root := tape.Root()
				if _, ok := resolveCompiled(&cache, root); !ok {
					b.Fatal("Resolve declined")
				}
				b.ReportAllocs()
				b.ResetTimer()
				hits := 0
				for i := 0; i < b.N; i++ {
					if _, ok := cache.Resolve(root); ok {
						hits++
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(width), "ns/member")
				shapeBenchSink = hits
			})
		}
	}
}

// BenchmarkShapeMiss is the worst case: 1024 documents of 1024 distinct
// shapes. FirstSight measures a fresh cache per pass — every document folds
// its fingerprint and is declined, the all-distinct adversarial regime the
// sighting gate exists for. SightTwice runs the corpus twice per fresh
// cache, so every layout pays its decline and then its full compile plus a
// field extraction; its per-document number spans both sightings and is
// bounded by two probe builds. SteadyHetero measures the same corpus once
// all shapes are compiled, the routing cost of a mixed recurring workload.
// The probe build that bounds all three is ProbeBuildGet.
func BenchmarkShapeMiss(b *testing.B) {
	set := shapeBenchHeteroDocs(1024, b)
	docs := set.Len()
	query := "doc0000_field07"
	queries := make([]string, docs)
	for d := range queries {
		queries[d] = fmt.Sprintf("doc%04d_field07", d)
	}
	b.Run("FirstSight", func(b *testing.B) {
		b.ReportAllocs()
		declines := 0
		for i := 0; i < b.N; i++ {
			var cache ShapeCache
			for d := 0; d < docs; d++ {
				if _, ok := cache.Resolve(set.Doc(d).Root()); !ok {
					declines++
				}
			}
		}
		if declines != b.N*docs {
			b.Fatal("a first sighting compiled")
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = declines
	})
	b.Run("SightTwice", func(b *testing.B) {
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			var cache ShapeCache
			for d := 0; d < docs; d++ {
				cache.Resolve(set.Doc(d).Root())
			}
			for d := 0; d < docs; d++ {
				root := set.Doc(d).Root()
				shape, ok := cache.Resolve(root)
				if !ok {
					b.Fatal("second sighting declined")
				}
				if ref, ok := shape.Field(queries[d]); ok {
					if _, ok := ref.In(root); ok {
						hits++
					}
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = hits
	})
	b.Run("ProbeBuildGet", func(b *testing.B) {
		storage := make([]vibejson.ProbeSlot, vibejson.RequiredProbeSlots(set.Doc(0).Root()))
		b.ReportAllocs()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				probe, ok := vibejson.BuildObjectProbe(set.Doc(d).Root(), storage)
				if !ok {
					b.Fatal("probe declined")
				}
				if _, ok := probe.Get(query); ok {
					hits++
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = hits
	})
	b.Run("SteadyHetero", func(b *testing.B) {
		var cache ShapeCache
		for d := 0; d < docs; d++ {
			if _, ok := resolveCompiled(&cache, set.Doc(d).Root()); !ok {
				b.Fatal("Resolve declined")
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		hits := 0
		for i := 0; i < b.N; i++ {
			for d := 0; d < docs; d++ {
				if _, ok := cache.Resolve(set.Doc(d).Root()); ok {
					hits++
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
		shapeBenchSink = hits
	})
}

// BenchmarkShapeMultiField extracts several fields per document — the
// document-projection workload — via cached FieldRefs after one Resolve,
// against the same count of GetCompiled calls and a per-document probe
// build. Sixteen fields is a full-row projection of the 16-field shape.
func BenchmarkShapeMultiField(b *testing.B) {
	set := shapeBenchDocs(1024, true, b)
	docs := set.Len()
	fieldSets := map[string][]string{
		"Fields4":  {"id", "name", "ts", "flags"},
		"Fields16": {"id", "user_id", "name", "email", "active", "score", "tier", "region", "ts", "visits", "balance", "lang", "theme", "tz", "ref", "flags"},
	}
	for _, group := range []string{"Fields4", "Fields16"} {
		names := fieldSets[group]
		var cache ShapeCache
		shape, ok := resolveCompiled(&cache, set.Doc(0).Root())
		if !ok {
			b.Fatal("Resolve declined")
		}
		refs := make([]FieldRef, len(names))
		keys := make([]vibejson.CompiledKey, len(names))
		for i, name := range names {
			if refs[i], ok = shape.Field(name); !ok {
				b.Fatalf("Field(%q) missing", name)
			}
			keys[i] = vibejson.CompileKey(name)
		}
		b.Run(group+"/ShapeResolveIn", func(b *testing.B) {
			b.ReportAllocs()
			hits := 0
			for i := 0; i < b.N; i++ {
				for d := 0; d < docs; d++ {
					root := set.Doc(d).Root()
					s, ok := cache.Resolve(root)
					if !ok || s != shape {
						b.Fatal("shape miss")
					}
					for r := range refs {
						if _, ok := refs[r].In(root); ok {
							hits++
						}
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = hits
		})
		b.Run(group+"/GetCompiled", func(b *testing.B) {
			b.ReportAllocs()
			hits := 0
			for i := 0; i < b.N; i++ {
				for d := 0; d < docs; d++ {
					root := set.Doc(d).Root()
					for k := range keys {
						if _, ok := root.GetCompiled(keys[k]); ok {
							hits++
						}
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = hits
		})
		b.Run(group+"/ProbeBuildGet", func(b *testing.B) {
			storage := make([]vibejson.ProbeSlot, vibejson.RequiredProbeSlots(set.Doc(0).Root()))
			b.ReportAllocs()
			hits := 0
			for i := 0; i < b.N; i++ {
				for d := 0; d < docs; d++ {
					probe, ok := vibejson.BuildObjectProbe(set.Doc(d).Root(), storage)
					if !ok {
						b.Fatal("probe declined")
					}
					for _, name := range names {
						if _, ok := probe.Get(name); ok {
							hits++
						}
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(docs), "ns/doc")
			shapeBenchSink = hits
		})
	}
}
