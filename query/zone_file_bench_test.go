package query

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Measurement for durable chunk-summary pruning.
//
// The numbers this file produces are meant to be read as a pair. A zone map is
// an ordering bet: it pays when the values a predicate selects sit together in
// a chunk and it costs when they do not. Reporting only the clustered case
// would be advertising, so every benchmark here runs the identical corpus in
// two layouts — ascending and shuffled — and every summary line prints both.
//
// Cold means the store's page cache is empty: the collection is reopened, so
// every directory and document page the query needs is a real read. That is
// the case durable pruning exists for, because a pruned chunk there is a page
// never faulted rather than a scan never run.

const zoneFileBenchDocuments = 100_000

// zoneFileBenchCorpus writes zoneFileBenchDocuments documents whose `v` member covers
// [0, zoneFileBenchDocuments), either in ascending order or shuffled. It returns
// the file path and the file's size so footprint can be compared across
// pruning settings.
func zoneFileBenchCorpus(tb testing.TB, dir string, shuffled bool, summaries bool) (string, int64) {
	tb.Helper()
	name := "clustered"
	if shuffled {
		name = "shuffled"
	}
	if !summaries {
		name += "-nozones"
	}
	path := fmt.Sprintf("%s/%s.vj", dir, name)
	file, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer file.Close()

	values := make([]int, zoneFileBenchDocuments)
	for i := range values {
		values[i] = i
	}
	if shuffled {
		rng := rand.New(rand.NewPCG(11, 13))
		rng.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
	}
	source := &store.Collection{}
	for i, v := range values {
		doc := fmt.Appendf(nil,
			`{"v":%d,"bucket":%d,"label":"row-%06d","note":"lorem ipsum dolor sit amet consectetur adipiscing elit %d"}`,
			v, v%97, v, v)
		if _, err := source.Put(fmt.Sprintf("key-%07d", i), doc); err != nil {
			tb.Fatal(err)
		}
	}
	previous := store.SetZonePruning(summaries)
	_, err = durable.CreateFrom(source, file, zoneFileBenchOptions())
	store.SetZonePruning(previous)
	if err != nil {
		tb.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		tb.Fatal(err)
	}
	return path, info.Size()
}

func zoneFileBenchOptions() durable.Options {
	return durable.Options{Collection: store.Options{ChunkDocuments: 64}}
}

func zoneFileBenchOpen(tb testing.TB, path string) (*durable.Collection, *durable.Snapshot) {
	tb.Helper()
	file, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	collection, err := durable.Open(file, zoneFileBenchOptions())
	if err != nil {
		tb.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatal(err)
	}
	return collection, snapshot
}

// zoneFileBenchSelectivities are the four fractions of the corpus each predicate
// selects. They are expressed as a `v < limit` range so the clustered layout
// has a genuinely prunable answer and the shuffled one genuinely does not.
var zoneFileBenchSelectivities = []struct {
	name  string
	limit int
}{
	{"0.1%", zoneFileBenchDocuments / 1000},
	{"1%", zoneFileBenchDocuments / 100},
	{"10%", zoneFileBenchDocuments / 10},
	{"50%", zoneFileBenchDocuments / 2},
}

// BenchmarkFileZonePruningWarm measures a filtered count over a warm store,
// with and without pruning, at four selectivities in both layouts.
func BenchmarkFileZonePruningWarm(b *testing.B) {
	dir := b.TempDir()
	for _, shuffled := range []bool{false, true} {
		layout := "clustered"
		if shuffled {
			layout = "shuffled"
		}
		path, _ := zoneFileBenchCorpus(b, dir, shuffled, true)
		collection, snapshot := zoneFileBenchOpen(b, path)
		for _, selectivity := range zoneFileBenchSelectivities {
			q := Select(Count()).Where(Cmp("v", Lt, selectivity.limit))
			for _, pruning := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s/pruning=%v", layout, selectivity.name, pruning)
				b.Run(name, func(b *testing.B) {
					previous := store.SetZonePruning(pruning)
					defer store.SetZonePruning(previous)
					e := Exec{Options: ExecOptions{Workers: 4}}
					// Both arms are warmed to the same page-cache state before
					// the timer starts. Without this the arm that runs first
					// pays the file's first residency ramp and the comparison
					// measures warm-up rather than pruning.
					for range 5 {
						if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
							b.Fatal(err)
						}
					}
					skipped := e.Workspace.zonePruned
					b.ReportMetric(float64(skipped), "chunks-skipped")
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
		_ = snapshot.Close()
		_ = collection.Close()
	}
}

// BenchmarkFileZonePruningCold reopens the collection before every measured
// execution, so the store's page cache starts empty and pruning is measured as
// avoided I/O rather than avoided CPU. The reopen is inside the timer because
// excluding it would make the "cold" claim untrue for the very first read,
// which is the read pruning removes; it is charged identically to both arms.
func BenchmarkFileZonePruningCold(b *testing.B) {
	dir := b.TempDir()
	for _, shuffled := range []bool{false, true} {
		layout := "clustered"
		if shuffled {
			layout = "shuffled"
		}
		path, _ := zoneFileBenchCorpus(b, dir, shuffled, true)
		for _, selectivity := range zoneFileBenchSelectivities {
			q := Select(Count()).Where(Cmp("v", Lt, selectivity.limit))
			for _, pruning := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s/pruning=%v", layout, selectivity.name, pruning)
				b.Run(name, func(b *testing.B) {
					previous := store.SetZonePruning(pruning)
					defer store.SetZonePruning(previous)
					b.ResetTimer()
					for b.Loop() {
						collection, snapshot := zoneFileBenchOpen(b, path)
						e := Exec{Options: ExecOptions{Workers: 4}}
						if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
							b.Fatal(err)
						}
						_ = snapshot.Close()
						_ = collection.Close()
					}
				})
			}
		}
	}
}

// TestFileZonePruningReport is the measurement the benchmarks above cannot
// print in one place: chunks skipped and file size, for both layouts at every
// selectivity, plus the footprint the summaries cost. It is a test rather than
// a benchmark because it reports facts about the stored bytes, not timings.
func TestFileZonePruningReport(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two 100k-document stores")
	}
	dir := t.TempDir()
	for _, shuffled := range []bool{false, true} {
		layout := "clustered"
		if shuffled {
			layout = "shuffled"
		}
		withPath, withSize := zoneFileBenchCorpus(t, dir, shuffled, true)
		_, withoutSize := zoneFileBenchCorpus(t, dir, shuffled, false)
		collection, snapshot := zoneFileBenchOpen(t, withPath)
		chunks, sound, paths, err := snapshot.ZoneStats()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d chunks, %d with usable summaries, %d tracked path entries",
			layout, chunks, sound, paths)
		t.Logf("%s: file %d bytes with summaries, %d without: +%d bytes (%.3f%%), %.1f bytes/chunk",
			layout, withSize, withoutSize, withSize-withoutSize,
			100*float64(withSize-withoutSize)/float64(withoutSize),
			float64(withSize-withoutSize)/float64(max(chunks, 1)))
		for _, selectivity := range zoneFileBenchSelectivities {
			q := Select(Count()).Where(Cmp("v", Lt, selectivity.limit))
			previous := store.SetZonePruning(true)
			var e Exec
			if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			store.SetZonePruning(previous)
			t.Logf("%s %s: %d of %d chunks skipped (%.1f%%), %d rows",
				layout, selectivity.name, e.Workspace.zonePruned, chunks,
				100*float64(e.Workspace.zonePruned)/float64(max(chunks, 1)), e.Result.RowCount)
		}
		_ = snapshot.Close()
		_ = collection.Close()
	}
}

// BenchmarkFileZoneWritePath is the other half of the trade. Summaries cost no
// pages and no fsyncs — they ride in slack the chunk-directory leaf already
// had — so what remains is the fold: one linear pass over each document
// written.
//
// Read this benchmark for the absence of a difference, not for a ratio. A
// durable commit is dominated by page encoding, checksums, and fsync, and the
// fold is a few hundred nanoseconds against a millisecond or more; the two arms
// land inside each other's run-to-run spread in both directions. The fold's
// actual cost is measured directly by store's BenchmarkZoneSummaryFold, which
// is the honest way to state a number this small.
func BenchmarkFileZoneWritePath(b *testing.B) {
	documents := [][]byte{}
	for i := range 4096 {
		documents = append(documents, fmt.Appendf(nil,
			`{"v":%d,"bucket":%d,"label":"row-%06d","note":"lorem ipsum dolor sit amet consectetur adipiscing elit %d"}`,
			i, i%97, i, i))
	}
	for _, summaries := range []bool{true, false} {
		b.Run(fmt.Sprintf("put/summaries=%v", summaries), func(b *testing.B) {
			previous := store.SetZonePruning(summaries)
			defer store.SetZonePruning(previous)
			file, err := os.CreateTemp(b.TempDir(), "zone-write-*")
			if err != nil {
				b.Fatal(err)
			}
			defer file.Close()
			collection, err := durable.Create(file, zoneFileBenchOptions())
			if err != nil {
				b.Fatal(err)
			}
			defer collection.Close()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				if _, err := collection.Put(fmt.Sprintf("key-%09d", i), documents[i%len(documents)]); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
		b.Run(fmt.Sprintf("update64/summaries=%v", summaries), func(b *testing.B) {
			previous := store.SetZonePruning(summaries)
			defer store.SetZonePruning(previous)
			file, err := os.CreateTemp(b.TempDir(), "zone-write-batch-*")
			if err != nil {
				b.Fatal(err)
			}
			defer file.Close()
			collection, err := durable.Create(file, zoneFileBenchOptions())
			if err != nil {
				b.Fatal(err)
			}
			defer collection.Close()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				if err := collection.Update(func(batch *durable.WriteBatch) error {
					for range 64 {
						if err := batch.Put(fmt.Sprintf("key-%09d", i), documents[i%len(documents)]); err != nil {
							return err
						}
						i++
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*64), "ns/document")
		})
	}
}
