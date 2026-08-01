package durable

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

// BenchmarkBufferedCurrentRead keeps one acknowledged inline replacement
// ahead of the physical root, then measures the two live-reader shapes that
// pay the packed-cut protocol. The post-index arm pins the one-way return to
// the original physical reader after online index cutover.
func BenchmarkBufferedCurrentRead(b *testing.B) {
	const documents = 16_384
	keys, docs := unifiedPrimaryCorpus(documents, true)
	options := Options{
		ResidentBytes: 64 << 20, Backend: BackendPortable,
		Durability: DurabilityBufferedVisible,
	}
	collection := unifiedBenchStoreWith(b, keys, docs, options, options)
	keyBytes := make([][]byte, len(keys))
	for index := range keys {
		keyBytes[index] = []byte(keys[index])
	}
	replacement, err := vibejson.AppendCanonicalize(nil, docs[0])
	if err != nil {
		b.Fatal(err)
	}
	replacement = bytes.Clone(replacement)
	const countryPrefix = `"country":"`
	country := bytes.Index(replacement, []byte(countryPrefix))
	if country < 0 {
		b.Fatal("benchmark document has no country")
	}
	country += len(countryPrefix)
	if replacement[country] == 'Z' {
		replacement[country] = 'Y'
	} else {
		replacement[country] = 'Z'
	}
	if created, err := collection.Put(keyBytes[0], replacement); err != nil || created {
		b.Fatalf("pending replacement = created %v, err %v", created, err)
	}

	b.Run("packed-point", func(b *testing.B) {
		benchmarkBufferedCurrentPoint(b, collection, keyBytes)
	})
	b.Run("packed-current-scan", func(b *testing.B) {
		scratch := make([]byte, 0, 512)
		visited := 0
		visit := func([]byte, []byte) error {
			visited++
			return nil
		}
		var warmErr error
		scratch, warmErr = collection.RangeRawCurrentBuffer(scratch[:0], visit)
		if warmErr != nil || visited != documents {
			b.Fatalf("warm current scan = %d rows, err %v", visited, warmErr)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			visited = 0
			var scanErr error
			scratch, scanErr = collection.RangeRawCurrentBuffer(
				scratch[:0], visit,
			)
			if scanErr != nil || visited != documents {
				b.Fatalf("current scan = %d rows, err %v", visited, scanErr)
			}
		}
		b.StopTimer()
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*documents),
			"ns/document",
		)
	})

	if _, err := collection.CreateIndex(store.IndexDefinition{
		Name: "by_country", Paths: []string{"/country"},
	}); err != nil {
		b.Fatal(err)
	}
	b.Run("post-index-point", func(b *testing.B) {
		benchmarkBufferedCurrentPoint(b, collection, keyBytes)
	})
}

func benchmarkBufferedCurrentPoint(
	b *testing.B, collection *Collection, keys [][]byte,
) {
	b.Helper()
	dst := make([]byte, 0, 512)
	for _, key := range keys {
		out, found, err := collection.AppendRaw(dst[:0], key)
		if err != nil || !found {
			b.Fatalf("warm AppendRaw = found %v, err %v", found, err)
		}
		dst = out[:0]
	}
	at := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, found, err := collection.AppendRaw(dst[:0], keys[at])
		if err != nil || !found {
			b.Fatalf("AppendRaw = found %v, err %v", found, err)
		}
		dst = out[:0]
		at++
		if at == len(keys) {
			at = 0
		}
	}
}
