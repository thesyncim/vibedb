package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// benchWriteOptions is the shipped default geometry with only the knobs a
// benchmark must pin: the explicit asynchronous mode these throughput tests
// exercise, a portable backend so results do not depend on io_uring
// availability, and a resident budget large enough that eviction noise does
// not hide the write path being measured. Every group-commit knob stays at its
// package default.
func benchWriteOptions() Options {
	return Options{
		Collection:    store.Options{ChunkDocuments: 16},
		ResidentBytes: 64 << 20,
		Backend:       BackendPortable,
		Durability:    DurabilityAsyncVisible,
	}
}

func benchDocument(i int) []byte {
	return fmt.Appendf(nil, `{"id":%d,"name":"row-%d","score":%d.5,"tag":"benchmark"}`, i, i, i%97)
}

func openBenchCollection(b *testing.B, options Options) (*Collection, func()) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	collection, err := Create(file, options)
	if err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	return collection, func() {
		if closeErr := collection.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
		_ = file.Close()
	}
}

// reportWriteAmplification converts the device counters into the two numbers a
// write-path change is judged by: bytes actually handed to the device per
// published generation, and how many generations shared one durability fence.
func reportWriteAmplification(b *testing.B, stats Stats, base Stats, puts int) {
	b.Helper()
	if puts == 0 {
		return
	}
	b.ReportMetric(float64(stats.DeviceBytes-base.DeviceBytes)/float64(puts), "devB/put")
	commits := stats.DeviceCommits - base.DeviceCommits
	if commits != 0 {
		b.ReportMetric(float64(puts)/float64(commits), "puts/fsync")
	}
}

// BenchmarkFileStorePutCommitBuffers sweeps the commit-buffer pool against the
// writer shape that is most sensitive to it: one serialized writer, which can
// only overlap a Put with the durability fence of an *earlier* Put.
//
// The knob is not really "how many buffers"; it is "how many generations may
// be in flight at once". One transaction reserves the worst-case page count for
// the configured single-document geometry plus a root buffer, so the
// achievable depth is
// BufferCount/(singleDocumentTransactionPages+1). At depth one a serialized
// writer must wait for its predecessor's fence before it can even begin, which
// is why the puts/fsync column and the ns/op column move together here.
//
// It reports CommitCapacityBytes so the throughput gain is never quoted
// without the off-heap memory it was bought with.
func BenchmarkFileStorePutCommitBuffers(b *testing.B) {
	options := benchWriteOptions()
	normalized, err := options.normalized()
	if err != nil {
		b.Fatal(err)
	}
	minimum := 1
	for minimum <= normalized.singleDocumentTransactionPages {
		minimum <<= 1
	}
	bufferCounts := []int{0, minimum}
	for buffers := minimum * 2; buffers <= minimum*4 && buffers <= maxCollectionBuffers; buffers *= 2 {
		bufferCounts = append(bufferCounts, buffers)
	}
	for _, buffers := range bufferCounts {
		name := fmt.Sprintf("buffers=%d", buffers)
		if buffers == 0 {
			name = "buffers=default"
		}
		b.Run(name, func(b *testing.B) {
			options := benchWriteOptions()
			options.BufferCount = buffers
			collection, done := openBenchCollection(b, options)
			defer done()
			base := collection.Stats()
			// Unique keys formatted into a reused []byte so the timed loop pays
			// no per-op key allocation (the store borrows the key).
			var keyBuf []byte
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", i)
				if _, err := collection.Put(keyBuf, benchDocument(i)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
			stats := collection.Stats()
			reportWriteAmplification(b, stats, base, b.N)
			b.ReportMetric(float64(stats.CommitCapacityBytes)/(1<<20), "stageMiB")
			b.ReportMetric(float64(stats.LargestCommitGroup), "maxgroup")
		})
	}
}

// BenchmarkFileStorePutDeviceBytes measures write amplification per Put at
// several store sizes. A directory whose depth does not track the store's size
// shows up here as a constant, size-independent surcharge on every Put.
func BenchmarkFileStorePutDeviceBytes(b *testing.B) {
	for _, existing := range []int{0, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("existing=%d", existing), func(b *testing.B) {
			if existing >= 100_000 && testing.Short() {
				b.Skip("100k-document prefill is too slow for -short")
			}
			options := benchWriteOptions()
			collection, done := openBenchCollection(b, options)
			defer done()
			for i := range existing {
				if _, err := collection.Put([]byte(fmt.Sprintf("key-%09d", i)), benchDocument(i)); err != nil {
					b.Fatal(err)
				}
			}
			base := collection.Stats()
			var keyBuf []byte
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", existing+i)
				if _, err := collection.Put(keyBuf, benchDocument(existing+i)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportWriteAmplification(b, collection.Stats(), base, b.N)
		})
	}
}

// BenchmarkFileStorePutDurabilitySync measures the latency a durable caller sees.
// Concurrency is the variable that matters: one serialized writer can never
// group-commit, so its cost is one full double-fence per Put no matter how the
// group-commit knobs are set.
func BenchmarkFileStorePutDurabilitySync(b *testing.B) {
	for _, writers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			options := benchWriteOptions()
			options.Durability = DurabilitySync
			collection, done := openBenchCollection(b, options)
			defer done()
			base := collection.Stats()
			b.ResetTimer()
			runCollectionWriters(b, collection, writers)
			b.StopTimer()
			stats := collection.Stats()
			reportWriteAmplification(b, stats, base, b.N)
			b.ReportMetric(float64(stats.LargestCommitGroup), "maxgroup")
		})
	}
}

// BenchmarkFileStorePutChunkGeometry attributes a Put's device bytes to the
// chunk page by varying only how many documents share one.
//
// It exists to settle a plausible-sounding theory rather than to tune a knob.
// One Put rebuilds every record in its chunk, so writing one document appears to
// rewrite sixty-four — but the page-size ladder starts at PageSize, and
// sixty-four ordinary documents still fit one 4 KiB extent, so the rebuild costs
// exactly the same single page a one-document chunk would. Shrinking the chunk
// does not shrink the write; it multiplies the chunk directory instead. Any
// future change that claims to reduce write amplification by narrowing the chunk
// has to move this number.
func BenchmarkFileStorePutChunkGeometry(b *testing.B) {
	for _, documents := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("chunk=%d", documents), func(b *testing.B) {
			options := benchWriteOptions()
			options.Collection.ChunkDocuments = documents
			options.MaxBatchDocuments = 1
			collection, done := openBenchCollection(b, options)
			defer done()
			for i := range 2_000 {
				if _, err := collection.Put([]byte(fmt.Sprintf("key-%09d", i)), benchDocument(i)); err != nil {
					b.Fatal(err)
				}
			}
			base := collection.Stats()
			var keyBuf []byte
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				keyBuf = fmt.Appendf(keyBuf[:0], "key-%09d", 2_000+i)
				if _, err := collection.Put(keyBuf, benchDocument(i)); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportWriteAmplification(b, collection.Stats(), base, b.N)
		})
	}
}
