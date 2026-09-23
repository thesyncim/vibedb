package durable

import (
	"encoding/json"
	"testing"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
)

// BenchmarkTinSearchSealedVsHeap compares the sealed generation build
// against an open heap index over the same snapshot documents: TinSearch
// runs the packed readers end to end, heap Score the open ones. Both sides
// parse once and reuse outputs; key mapping is outside the timer.
func BenchmarkTinSearchSealedVsHeap(b *testing.B) {
	db, err := OpenDatabase(b.TempDir(), DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	options := testDatabaseOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	}
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		b.Fatal(err)
	}
	const n = 2000
	for i := 0; i < n; i++ {
		body := "common filler words here"
		if i%7 == 0 {
			body += " selective selective selective"
		}
		if i%170 == 0 {
			body += " needle"
		}
		value, err := json.Marshal(map[string]string{"body": body})
		if err != nil {
			b.Fatal(err)
		}
		mustPut(b, docs, string(rune('a'+i/676))+string(rune('a'+i/26%26))+string(rune('a'+i%26)), string(value))
	}
	snap, err := docs.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snap.Close()
	var texts []string
	if err := snap.RangeRaw(func(_, value []byte) error {
		var doc struct {
			Body string `json:"body"`
		}
		if err := json.Unmarshal(value, &doc); err != nil {
			return err
		}
		texts = append(texts, doc.Body)
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	heap := tin.NewIndex()
	for i, text := range texts {
		heap.Add(tin.DocID(i), text)
	}
	queries := []string{"common", "needle AND common", `"filler words"`}
	build, err := snap.TinBuildForPath("/body")
	if err != nil {
		b.Fatal(err)
	}
	parsed := make([]tin.Query, len(queries))
	for i, input := range queries {
		q, err := build.Index().ParseTINQL(input)
		if err != nil {
			b.Fatal(err)
		}
		parsed[i] = q
	}
	// sealed-score vs heap-score is the layout A/B: pre-parsed queries,
	// reused outputs, no key mapping — only the readers differ.
	b.Run("sealed-score", func(b *testing.B) {
		var out []tin.Scored
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, q := range parsed {
				out = build.Index().Score(q, 0, out[:0])
			}
		}
	})
	b.Run("heap-score", func(b *testing.B) {
		var out []tin.Scored
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, q := range parsed {
				out = heap.Score(q, 0, out[:0])
			}
		}
	})
	// sealed-tinsearch is the API cost on top: parse plus key mapping.
	b.Run("sealed-tinsearch", func(b *testing.B) {
		var out []TinHit
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, input := range queries {
				var err error
				out, err = tinSearchInto(docs, snap, "/body", input, out[:0])
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// tinSearchInto is the TinSearch body appending into out for benchmarks.
// The huge topK disables truncation so both sides retrieve every hit.
func tinSearchInto(c *Collection, snap *Snapshot, path, tinql string, out []TinHit) ([]TinHit, error) {
	hits, err := c.TinSearch(snap, path, tinql, 1000000)
	if err != nil {
		return out, err
	}
	return append(out, hits...), nil
}
