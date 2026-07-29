package durable

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// benchBatchOptions is the shipped default geometry with only the batch bound
// moved, so a batch arm reports what a caller gets by setting one option. The
// resident budget tracks the reservation because a wider batch may hold more
// dirty pages before its single publication.
func benchBatchOptions(documents int) Options {
	return Options{
		Collection:        store.Options{ChunkDocuments: 64},
		ResidentBytes:     512 << 20,
		Backend:           BackendPortable,
		Durability:        DurabilityAsyncVisible,
		MaxRetiredExtents: 1 << 17,
		MaxBatchDocuments: documents,
	}
}

func benchBatchDocument(i int) []byte {
	return fmt.Appendf(nil, `{"id":%d,"name":"row-%d","score":%d.5,"tag":"benchmark"}`, i, i, i%97)
}

type mutationBenchCorpus struct {
	keys      [][]byte
	documents [2][][]byte
}

func newMutationBenchCorpus(rows int) mutationBenchCorpus {
	corpus := mutationBenchCorpus{
		keys: make([][]byte, rows),
		documents: [2][][]byte{
			make([][]byte, rows), make([][]byte, rows),
		},
	}
	for i := range rows {
		corpus.keys[i] = []byte(fmt.Sprintf("key-%09d", i))
		for version := range 2 {
			corpus.documents[version][i] = fmt.Appendf(
				nil,
				`{"id":%d,"name":"row-%d","score":%d.5,"status":"state-%d"}`,
				i, i, i%97, version,
			)
		}
	}
	return corpus
}

func openMutationBenchCollection(
	b *testing.B, options Options, corpus mutationBenchCorpus,
) (*Collection, func()) {
	b.Helper()
	source, err := store.New(options.Collection)
	if err != nil {
		b.Fatal(err)
	}
	for i, key := range corpus.keys {
		// source is the heap store (string-keyed); convert at this untimed
		// setup boundary.
		if _, err := source.Put(string(key), corpus.documents[0][i]); err != nil {
			b.Fatal(err)
		}
	}
	path := filepath.Join(b.TempDir(), "mutations.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	return collection, func() {
		if err := collection.Close(); err != nil {
			b.Fatal(err)
		}
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// reportBatchWrite converts one arm's counters into the two numbers the batched
// write path is judged by: wall time and device bytes per document, both
// amortised over the whole run rather than per call, so arms with different
// batch sizes are directly comparable.
func reportBatchWrite(b *testing.B, collection *Collection, base Stats, documents int) {
	b.Helper()
	if documents == 0 {
		return
	}
	// Update publishes asynchronously by default. The timer has already
	// stopped at every call site, so fence the measured generations before
	// sampling counters; otherwise a fast arm can report zero device bytes
	// merely because the committer has not consumed its queue yet.
	if err := collection.Flush(); err != nil {
		b.Fatal(err)
	}
	stats := collection.Stats()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(documents), "ns/doc")
	b.ReportMetric(float64(stats.DeviceBytes-base.DeviceBytes)/float64(documents), "devB/doc")
	b.ReportMetric(float64(documents)/float64(max(stats.DeviceCommits-base.DeviceCommits, 1)), "docs/fsync")
}

// BenchmarkFileStoreBatchWrite measures the cost of ingesting the same
// documents one Put at a time and in batches of several sizes. The batch=1 arm
// is the control: it proves how much of the gap is the batched machinery itself
// rather than the grouping.
func BenchmarkFileStoreBatchWrite(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("update/batch=%d", size), func(b *testing.B) {
			collection, done := openBenchCollection(b, benchBatchOptions(size))
			defer done()
			base := collection.Stats()
			written := 0
			// Reused key scratch: unique keys, no per-op key allocation in the
			// timed loop (batch.Put copies the key into its arena).
			var keyBuf []byte
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				start := i * size
				if err := collection.Update(func(batch *WriteBatch) error {
					for j := range size {
						keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", start+j)
						if err := batch.Put(
							keyBuf, benchBatchDocument(start+j),
						); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				written += size
			}
			b.StopTimer()
			reportBatchWrite(b, collection, base, written)
		})
	}
	b.Run("put", func(b *testing.B) {
		collection, done := openBenchCollection(b, benchBatchOptions(1))
		defer done()
		base := collection.Stats()
		written := 0
		var keyBuf []byte
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", i)
			if _, err := collection.Put(keyBuf, benchBatchDocument(i)); err != nil {
				b.Fatal(err)
			}
			written++
		}
		b.StopTimer()
		reportBatchWrite(b, collection, base, written)
	})
}

// BenchmarkFileStoreBatchWriteIndexed repeats the comparison with one exact
// index configured. Indexed batches are the case a naive grouping cannot serve:
// tuple hashes are uniformly distributed, so every document touches a different
// posting leaf.
func BenchmarkFileStoreBatchWriteIndexed(b *testing.B) {
	indexes := []store.IndexDefinition{{Name: "tag", Paths: []string{"/tag"}}}
	document := func(i int) []byte {
		return fmt.Appendf(nil, `{"id":%d,"name":"row-%d","tag":"t%04d"}`, i, i, i%1000)
	}
	for _, size := range []int{1, 100} {
		b.Run(fmt.Sprintf("update/batch=%d", size), func(b *testing.B) {
			options := benchBatchOptions(size)
			options.Indexes = indexes
			collection, done := openBenchCollection(b, options)
			defer done()
			base := collection.Stats()
			written := 0
			var keyBuf []byte
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				start := i * size
				if err := collection.Update(func(batch *WriteBatch) error {
					for j := range size {
						keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", start+j)
						if err := batch.Put(keyBuf, document(start+j)); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				written += size
			}
			b.StopTimer()
			reportBatchWrite(b, collection, base, written)
		})
	}
	b.Run("put", func(b *testing.B) {
		options := benchBatchOptions(1)
		options.Indexes = indexes
		collection, done := openBenchCollection(b, options)
		defer done()
		base := collection.Stats()
		written := 0
		var keyBuf []byte
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", i)
			if _, err := collection.Put(keyBuf, document(i)); err != nil {
				b.Fatal(err)
			}
			written++
		}
		b.StopTimer()
		reportBatchWrite(b, collection, base, written)
	})
}

// BenchmarkFileStoreBatchReplace measures uniform replacements of existing
// keys. The stride is coprime with the 4096-row corpus, so a batch spans
// unrelated chunks and key leaves instead of accidentally measuring one hot
// document page. Indexed arms alternate the routed value on every replacement.
func BenchmarkFileStoreBatchReplace(b *testing.B) {
	const rows = 4096
	corpus := newMutationBenchCorpus(rows)
	for _, metadata := range []struct {
		name    string
		indexes []store.IndexDefinition
	}{
		{name: "plain"},
		{
			name: "indexed",
			indexes: []store.IndexDefinition{
				{Name: "status", Paths: []string{"/status"}},
			},
		},
	} {
		for _, size := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/batch=%d", metadata.name, size), func(b *testing.B) {
				options := benchBatchOptions(size)
				options.Indexes = metadata.indexes
				collection, done := openMutationBenchCollection(
					b, options, corpus,
				)
				defer done()
				versions := make([]uint8, rows)
				indices := make([]int, size)
				base := collection.Stats()
				mutated := 0
				b.ResetTimer()
				for iteration := 0; b.Loop(); iteration++ {
					start := iteration * 811 & (rows - 1)
					for j := range size {
						indices[j] = (start + j*1597) & (rows - 1)
					}
					if err := collection.Update(func(batch *WriteBatch) error {
						for _, index := range indices {
							version := versions[index] ^ 1
							if err := batch.Put(
								corpus.keys[index], corpus.documents[version][index],
							); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						b.Fatalf(
							"%v (retirement scratch %d/%d, stats %+v)",
							err, len(collection.retireScratch), cap(collection.retireScratch),
							collection.Stats(),
						)
					}
					for _, index := range indices {
						versions[index] ^= 1
					}
					mutated += size
				}
				b.StopTimer()
				reportBatchWrite(b, collection, base, mutated)
			})
		}
	}
}

// BenchmarkFileStoreBatchDelete deletes a random permutation of a completed
// store. When one corpus is exhausted, setup rebuilds it outside the timer and
// device counters are accumulated across files.
func BenchmarkFileStoreBatchDelete(b *testing.B) {
	const rows = 4096
	corpus := newMutationBenchCorpus(rows)
	for _, metadata := range []struct {
		name    string
		indexes []store.IndexDefinition
	}{
		{name: "plain"},
		{
			name: "indexed",
			indexes: []store.IndexDefinition{
				{Name: "status", Paths: []string{"/status"}},
			},
		},
	} {
		for _, size := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/batch=%d", metadata.name, size), func(b *testing.B) {
				options := benchBatchOptions(size)
				options.Indexes = metadata.indexes
				order := rand.New(rand.NewSource(20260725)).Perm(rows)
				collection, done := openMutationBenchCollection(
					b, options, corpus,
				)
				base := collection.Stats()
				next := 0
				mutated := 0
				var deviceBytes, deviceCommits uint64
				b.ResetTimer()
				for b.Loop() {
					if next == rows {
						b.StopTimer()
						if err := collection.Flush(); err != nil {
							b.Fatal(err)
						}
						stats := collection.Stats()
						deviceBytes += stats.DeviceBytes - base.DeviceBytes
						deviceCommits += stats.DeviceCommits - base.DeviceCommits
						done()
						collection, done = openMutationBenchCollection(
							b, options, corpus,
						)
						base = collection.Stats()
						next = 0
						b.StartTimer()
					}
					first := next
					next += size
					if err := collection.Update(func(batch *WriteBatch) error {
						for _, index := range order[first:next] {
							if err := batch.Delete(corpus.keys[index]); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						b.Fatalf(
							"%v (retirement scratch %d/%d, stats %+v)",
							err, len(collection.retireScratch), cap(collection.retireScratch),
							collection.Stats(),
						)
					}
					mutated += size
				}
				b.StopTimer()
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				stats := collection.Stats()
				deviceBytes += stats.DeviceBytes - base.DeviceBytes
				deviceCommits += stats.DeviceCommits - base.DeviceCommits
				done()
				b.ReportMetric(
					float64(b.Elapsed().Nanoseconds())/float64(mutated), "ns/doc",
				)
				b.ReportMetric(float64(deviceBytes)/float64(mutated), "devB/doc")
				b.ReportMetric(
					float64(mutated)/float64(max(deviceCommits, 1)), "docs/fsync",
				)
			})
		}
	}
}

// BenchmarkFileStoreCreateFromFloor is the lower bound any online write path is
// measured against: a bulk build writes every page once, sequentially, with no
// copy-on-write directory rewrites at all.
func BenchmarkFileStoreCreateFromFloor(b *testing.B) {
	const documents = 20_000
	source, err := store.New(store.Options{ChunkDocuments: 64})
	if err != nil {
		b.Fatal(err)
	}
	for i := range documents {
		if _, err := source.Put(fmt.Sprintf("key-%09d", i), benchBatchDocument(i)); err != nil {
			b.Fatal(err)
		}
	}
	options := benchBatchOptions(64)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		path := filepath.Join(b.TempDir(), fmt.Sprintf("bulk-%d.vibe", i))
		file, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if openErr != nil {
			b.Fatal(openErr)
		}
		size, buildErr := CreateFromPrimary(source, file, options)
		if buildErr != nil {
			b.Fatal(buildErr)
		}
		b.SetBytes(size)
		if closeErr := file.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*documents), "ns/doc")
}
