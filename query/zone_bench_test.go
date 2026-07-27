package query

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// Block-pruning benchmarks.
//
// Every measurement is reported for both data distributions, because a zone
// map's whole reputation rests on which one you are looking at:
//
//   - "clustered" gives each document a value correlated with its insertion
//     order, so a chunk covers a narrow contiguous range and min/max is almost
//     a perfect index. This is the case zone maps are famous for and the case
//     a benchmark that wanted a good number would report alone.
//   - "shuffled" gives the identical multiset of values in random order, so
//     every chunk spans nearly the whole range and min/max proves nothing.
//     This is the honest boundary, and it is reported at the same prominence.
//
// ns/doc and chunks-skipped are custom metrics on every case so the two axes
// can be read together: a case that skips no chunk should also cost what the
// unpruned scan costs, and a case that skips most chunks should cost
// proportionally less.

// zoneBenchDocument builds one benchmark document. The shape averages 257
// bytes, matching the 100k-document / 24.5 MiB corpus the space budget for
// this feature is stated against, so bytes-per-document ratios computed here
// carry over directly.
func zoneBenchDocument(id, value int) []byte {
	return fmt.Appendf(nil,
		`{"id":%d,"value":%d,"cat":"c%02d","name":"user-%08d","email":"u%08d@example.com",`+
			`"note":"%s","active":%t,"score":%d}`,
		id, value, value%100, id, id, zoneBenchNote, id%2 == 0, id%1000)
}

const zoneBenchNote = "the quick brown fox jumps over the lazy dog while the store " +
	"folds one more chunk summary into place and the planner decides"

// zoneBenchCorpus builds a collection of n documents. When shuffled is set the
// same value multiset is permuted across insertion order, which is the only
// difference between the two distributions: identical documents, identical
// bytes, identical chunk count.
func zoneBenchCorpus(tb testing.TB, n int, shuffled bool) (store.Snapshot, int) {
	tb.Helper()
	values := make([]int, n)
	for i := range values {
		values[i] = i
	}
	if shuffled {
		rng := rand.New(rand.NewPCG(0x21ce, 0xb0a7))
		rng.Shuffle(n, func(i, j int) { values[i], values[j] = values[j], values[i] })
	}
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64})
	if err != nil {
		tb.Fatal(err)
	}
	bytes := 0
	for i := 0; i < n; i++ {
		doc := zoneBenchDocument(i, values[i])
		bytes += len(doc)
		if err := builder.Append(fmt.Sprintf("key:%08d", i), doc); err != nil {
			tb.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatal(err)
	}
	return snapshot, bytes
}

// zoneSelectivities are the four fractions of the corpus the range predicates
// below admit.
var zoneSelectivities = []struct {
	name     string
	fraction float64
}{
	{"0.1%", 0.001},
	{"1%", 0.01},
	{"10%", 0.10},
	{"50%", 0.50},
}

const zoneBenchRows = 100_000

func BenchmarkZoneFilteredScanRange(b *testing.B) {
	for _, distribution := range []struct {
		name     string
		shuffled bool
	}{{"clustered", false}, {"shuffled", true}} {
		snapshot, _ := zoneBenchCorpus(b, zoneBenchRows, distribution.shuffled)
		totalChunks, _, _ := snapshot.ZoneStats()
		for _, selectivity := range zoneSelectivities {
			threshold := int(float64(zoneBenchRows) * (1 - selectivity.fraction))
			q := Select(Path("id")).Where(Cmp("value", Ge, threshold))
			for _, pruning := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/pruning=%v", distribution.name, selectivity.name, pruning)
				b.Run(name, func(b *testing.B) {
					zoneBenchRun(b, q, snapshot, totalChunks, pruning)
				})
			}
		}
	}
}

// BenchmarkZoneFilteredScanEquality is the case min/max is worst at. On
// clustered data an equality still lands inside exactly one chunk's range; on
// shuffled data every chunk's range contains the literal, so the zone tier
// prunes nothing at all and the scan pays the metadata walk for no benefit.
// That negative result is the reason this benchmark exists.
func BenchmarkZoneFilteredScanEquality(b *testing.B) {
	for _, distribution := range []struct {
		name     string
		shuffled bool
	}{{"clustered", false}, {"shuffled", true}} {
		snapshot, _ := zoneBenchCorpus(b, zoneBenchRows, distribution.shuffled)
		totalChunks, _, _ := snapshot.ZoneStats()
		q := Select(Path("id")).Where(Cmp("value", Eq, zoneBenchRows/3))
		for _, pruning := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/pruning=%v", distribution.name, pruning), func(b *testing.B) {
				zoneBenchRun(b, q, snapshot, totalChunks, pruning)
			})
		}
	}
}

// BenchmarkZoneFilteredScanAbsentPath measures the case the presence bits
// answer and nothing else in the planner can: a predicate on a path no
// document carries. It is distribution-independent by construction.
func BenchmarkZoneFilteredScanAbsentPath(b *testing.B) {
	snapshot, _ := zoneBenchCorpus(b, zoneBenchRows, false)
	totalChunks, _, _ := snapshot.ZoneStats()
	q := Select(Path("id")).Where(Exists("absent"))
	for _, pruning := range []bool{false, true} {
		b.Run(fmt.Sprintf("pruning=%v", pruning), func(b *testing.B) {
			zoneBenchRun(b, q, snapshot, totalChunks, pruning)
		})
	}
}

func zoneBenchRun(b *testing.B, q *Query, snapshot store.Snapshot, totalChunks int, pruning bool) {
	b.Helper()
	previous := store.SetZonePruning(pruning)
	defer store.SetZonePruning(previous)

	src := FromSnapshot(snapshot)
	var e Exec
	for i := 0; i < 5; i++ {
		if err := q.RunInto(&e, src); err != nil {
			b.Fatal(err)
		}
	}
	skipped := e.Workspace.zonePruned
	rows := e.Result.RowCount

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := q.RunInto(&e, src); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	perDocument := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / float64(snapshot.Len())
	b.ReportMetric(perDocument, "ns/doc")
	b.ReportMetric(float64(skipped), "chunks-skipped")
	b.ReportMetric(100*float64(skipped)/float64(totalChunks), "%chunks-skipped")
	b.ReportMetric(float64(rows), "rows")
}

// BenchmarkZoneMaintenanceWrite measures what the summary costs the write
// path: one linear pass over the document's source per Put, on top of a chunk
// rebuild that already copies up to 64 slots. Compare it against the same
// benchmark at the parent commit to price the feature; there is no runtime
// switch for folding, because a switch would make what is stored depend on a
// global and the differential control needs the identical stored bytes.
func BenchmarkZoneMaintenanceWrite(b *testing.B) {
	for _, mode := range []string{"insert", "replace"} {
		b.Run(mode, func(b *testing.B) {
			collection, err := store.New(store.Options{ChunkDocuments: 64})
			if err != nil {
				b.Fatal(err)
			}
			if mode == "replace" {
				for i := 0; i < 64; i++ {
					if _, err := collection.Put(fmt.Sprintf("key:%08d", i), zoneBenchDocument(i, i)); err != nil {
						b.Fatal(err)
					}
				}
			}
			doc := zoneBenchDocument(0, 0)
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key := fmt.Sprintf("key:%08d", i%64)
				if mode == "insert" {
					key = fmt.Sprintf("key:%08d", i)
				}
				if _, err := collection.Put(key, doc); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// BenchmarkZoneProbe isolates the planner-side cost: one walk of the chunk
// headers with one 8-entry hash scan each, with no document touched at all. It
// is what a query pays when the summaries decline to prune anything.
func BenchmarkZoneProbe(b *testing.B) {
	snapshot, _ := zoneBenchCorpus(b, zoneBenchRows, true)
	code, ok := store.ZoneCodeNumber([]byte("50000"))
	if !ok {
		b.Fatal("ZoneCodeNumber declined")
	}
	probe := store.ZoneProbe{Path: store.ZonePathHash("value"), Lo: code, Hi: code, Op: store.ZoneOpGe}
	masks := make([]store.Mask, 0, 2048)
	chunks, _, _ := snapshot.ZoneStats()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		masks, _, _ = snapshot.AppendZoneMasks(masks[:0], probe)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(chunks), "ns/chunk")
}

// TestZoneCorpusFootprint reports the corpus shape the space claims are stated
// against, so the ratio in store/store_zone.go's documentation is a measured
// number rather than an assumption that drifts as the benchmark document
// changes.
func TestZoneCorpusFootprint(t *testing.T) {
	snapshot, bytes := zoneBenchCorpus(t, zoneBenchRows, false)
	chunks, sound, paths := snapshot.ZoneStats()
	perDocument := float64(bytes) / float64(zoneBenchRows)
	summaryBytes := chunks * store.ZoneChunkBytes()
	t.Logf("corpus: %d documents, %.1f MiB, %.1f B/document", zoneBenchRows, float64(bytes)/(1<<20), perDocument)
	t.Logf("summaries: %d chunks (%d sound, %d tracked paths), %d B total, %.2f B/document, %.3f%% of data",
		chunks, sound, paths, summaryBytes,
		float64(summaryBytes)/float64(zoneBenchRows),
		100*float64(summaryBytes)/float64(bytes))
	if float64(summaryBytes)/float64(bytes) > 0.01 {
		t.Fatalf("summaries are %.3f%% of the corpus, above the 1%% budget",
			100*float64(summaryBytes)/float64(bytes))
	}
}
