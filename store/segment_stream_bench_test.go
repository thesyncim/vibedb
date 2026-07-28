package store

// Benchmarks for bulk stream ingestion. ReadFrom must beat the two honest
// compositions of existing APIs over the same NDJSON corpus: a bufio.Scanner
// feeding an Append loop (the idiomatic engine loader, one scanner-buffer
// copy plus one arena copy per document) and the streaming Reader feeding an
// Append loop (full validation in the Reader, then a second validating build
// in Append). All three retain every index, as an engine would. Throughput
// is bytes of corpus per second; ns/doc isolates per-document overhead.

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// streamBenchCorpus is one generated NDJSON corpus and its document count.
type streamBenchCorpus struct {
	name string
	data []byte
	docs int
}

// streamBenchDoc appends one realistic document of roughly the given size:
// scalar identity fields, a small tag array, a nested object, and a text
// field padded to reach the target.
func streamBenchDoc(dst []byte, r *rand.Rand, i, size int) []byte {
	pad := max(size-150, 0)
	text := make([]byte, pad)
	for j := range text {
		text[j] = byte('a' + (i+j)%26)
	}
	return fmt.Appendf(dst,
		`{"id":%d,"user":"account-%06d","active":%t,"score":%d.%02d,"tags":["go","json","batch-%d"],"meta":{"region":"zone-%d","tier":%d,"ratio":0.%04d},"text":"%s"}`,
		i, r.Intn(1_000_000), i%3 != 0, r.Intn(100), r.Intn(100), i%16, i%8, i%5, r.Intn(10_000), text)
}

// makeStreamBenchCorpus builds an NDJSON corpus of roughly target bytes with
// the given per-document size policy.
func makeStreamBenchCorpus(name string, target int, size func(r *rand.Rand, i int) int) streamBenchCorpus {
	r := rand.New(rand.NewSource(0xD0C5E7))
	data := make([]byte, 0, target+target/8)
	docs := 0
	for i := 0; len(data) < target; i++ {
		data = streamBenchDoc(data, r, i, size(r, i))
		data = append(data, '\n')
		docs++
	}
	return streamBenchCorpus{name: name, data: data, docs: docs}
}

// streamBenchCorpora returns the three ingestion profiles: mixed document
// sizes (100 B to 10 KiB, skewed small like event streams), small-document
// dominated (1 KiB), and large-document dominated (1 MiB). The ~300 MiB of
// corpora are built once, on first benchmark use, so plain test runs pay
// nothing.
var streamBenchCorpora = sync.OnceValue(func() []streamBenchCorpus {
	return []streamBenchCorpus{
		makeStreamBenchCorpus("Mixed", 96<<20, func(r *rand.Rand, i int) int {
			switch d := r.Intn(10); {
			case d < 6:
				return 100 + r.Intn(300)
			case d < 9:
				return 1<<10 + r.Intn(3<<10)
			default:
				return 10 << 10
			}
		}),
		makeStreamBenchCorpus("Small1KB", 96<<20, func(r *rand.Rand, i int) int { return 1 << 10 }),
		makeStreamBenchCorpus("Large1MB", 100<<20, func(r *rand.Rand, i int) int { return 1 << 20 }),
	}
})

// BenchmarkSegmentStreamIngest measures ReadFrom against both per-document
// compositions on each corpus profile, with key-hash enrichment off and on.
func BenchmarkSegmentStreamIngest(b *testing.B) {
	for _, corpus := range streamBenchCorpora() {
		for _, variant := range []struct {
			name string
			opts document.IndexOptions
		}{
			{"Default", document.IndexOptions{}},
			{"HashKeys", document.IndexOptions{HashKeys: true}},
		} {
			run := func(name string, ingest func(b *testing.B, s *Segment)) {
				b.Run(corpus.name+"/"+variant.name+"/"+name, func(b *testing.B) {
					b.SetBytes(int64(len(corpus.data)))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s := &Segment{Options: variant.opts}
						ingest(b, s)
						if s.Len() != corpus.docs {
							b.Fatalf("ingested %d docs, want %d", s.Len(), corpus.docs)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(corpus.docs), "ns/doc")
				})
			}
			run("ReadFrom", func(b *testing.B, s *Segment) {
				if _, err := s.ReadFrom(bytes.NewReader(corpus.data)); err != nil {
					b.Fatal(err)
				}
			})
			run("ScannerAppend", func(b *testing.B, s *Segment) {
				sc := bufio.NewScanner(bytes.NewReader(corpus.data))
				sc.Buffer(make([]byte, 64<<10), 4<<20)
				for sc.Scan() {
					if len(sc.Bytes()) == 0 {
						continue
					}
					if _, err := s.Append(sc.Bytes()); err != nil {
						b.Fatal(err)
					}
				}
				if err := sc.Err(); err != nil {
					b.Fatal(err)
				}
			})
			// PresplitAppend loops Append over documents already split and
			// resident in memory: no reading, no framing, no scanner copy.
			// It is the upper bound for any batch-append API (AppendMany),
			// which could save at most the per-call overhead this lane
			// still pays; the gap between it and ReadFrom is the honest
			// measure of whether such a surface is worth adding.
			b.Run(corpus.name+"/"+variant.name+"/PresplitAppend", func(b *testing.B) {
				split := bytes.Split(corpus.data, []byte("\n"))
				docs := split[:0]
				for _, doc := range split {
					if len(doc) > 0 {
						docs = append(docs, doc)
					}
				}
				b.SetBytes(int64(len(corpus.data)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s := &Segment{Options: variant.opts}
					for _, doc := range docs {
						if _, err := s.Append(doc); err != nil {
							b.Fatal(err)
						}
					}
					if s.Len() != corpus.docs {
						b.Fatalf("ingested %d docs, want %d", s.Len(), corpus.docs)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(corpus.docs), "ns/doc")
			})
			run("ReaderAppend", func(b *testing.B, s *Segment) {
				rd := vibejson.NewReader(bytes.NewReader(corpus.data))
				for rd.Next() {
					if _, err := s.Append(rd.Bytes()); err != nil {
						b.Fatal(err)
					}
				}
				if err := rd.Err(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
