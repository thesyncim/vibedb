package durable

import (
	"fmt"
	"testing"
)

// Each operation scans one complete corpus, split into disjoint key ranges.
// Every worker owns a snapshot and reconstruction scratch. The fixture is
// immutable, so all workers read the same generation. All returned bytes are
// consumed; the reported throughput is corpus bytes / whole-operation time.
func BenchmarkUnifiedPartitionedScanAllBytes(b *testing.B) {
	benchmarkUnifiedPartitionedScanAllBytes(b, 100_000, false)
}

func BenchmarkUnifiedPartitionedScanAllBytesMillion(b *testing.B) {
	benchmarkUnifiedPartitionedScanAllBytes(b, 1_000_000, false)
}

func BenchmarkUnifiedPartitionedScanAllBytesHighCardinality(b *testing.B) {
	benchmarkUnifiedPartitionedScanAllBytes(b, 100_000, true)
}

func benchmarkUnifiedPartitionedScanAllBytes(b *testing.B, rows int, high bool) {
	keys, documents := unifiedCompetitiveCorpus(rows, high)
	collection := unifiedBenchStore(b, keys, documents, unifiedBenchOptions())
	for _, workers := range []int{1, 2, 4, 8, 12, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			type worker struct {
				snapshot              *Snapshot
				lower, upper, scratch []byte
				start                 chan bool
				done                  chan struct{}
				count                 int
				bytes                 int64
				sink                  byte
				err                   error
			}
			pool := make([]worker, workers)
			var expectedBytes int64
			var expectedSink byte
			for _, doc := range documents {
				expectedBytes += int64(len(doc))
				expectedSink ^= touchUnifiedScanAllBytes(doc)
			}
			for i := range pool {
				w := &pool[i]
				var err error
				w.snapshot, err = collection.Snapshot()
				if err != nil {
					b.Fatal(err)
				}
				w.lower = []byte(keys[i*rows/workers])
				if i+1 < workers {
					w.upper = []byte(keys[(i+1)*rows/workers])
				} else {
					w.upper = []byte(keys[rows-1] + "\x00")
				}
				w.start = make(chan bool)
				w.done = make(chan struct{})
				go func() {
					visit := func(_, value []byte) error {
						w.count++
						w.bytes += int64(len(value))
						w.sink ^= touchUnifiedScanAllBytes(value)
						return nil
					}
					for range w.start {
						w.count, w.bytes, w.sink = 0, 0, 0
						w.scratch, w.err = w.snapshot.RangeBoundsRawBuffer(w.lower, w.upper, w.scratch, false, visit)
						w.done <- struct{}{}
					}
					close(w.done)
				}()
			}
			defer func() {
				for i := range pool {
					close(pool[i].start)
				}
				for i := range pool {
					<-pool[i].done
					if err := pool[i].snapshot.Close(); err != nil {
						b.Error(err)
					}
				}
			}()
			run := func() {
				for i := range pool {
					pool[i].start <- true
				}
				count, size, sink := 0, int64(0), byte(0)
				for i := range pool {
					w := &pool[i]
					<-w.done
					if w.err != nil {
						b.Fatal(w.err)
					}
					count += w.count
					size += w.bytes
					sink ^= w.sink
				}
				if count != rows || size != expectedBytes || sink != expectedSink {
					b.Fatalf("got rows/bytes/checksum %d/%d/%d, want %d/%d/%d", count, size, sink, rows, expectedBytes, expectedSink)
				}
			}
			run()
			b.SetBytes(expectedBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
		})
	}
}

// Narrow ranges guard lookup-sized scans while the partitioned benchmark
// measures long ranges. Setup and warmup remain outside the timed loop.
func BenchmarkUnifiedBoundedScanWidths(b *testing.B) {
	keys, documents := unifiedCompetitiveCorpus(10_000, false)
	collection := unifiedBenchStore(b, keys, documents, unifiedBenchOptions())
	for _, width := range []int{1, 16, 256, 4096} {
		b.Run(fmt.Sprintf("rows=%d", width), func(b *testing.B) {
			snapshot, err := collection.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			defer snapshot.Close()
			lower, upper := []byte(keys[17]), []byte(keys[17+width])
			var scratch []byte
			count := 0
			var sink byte
			visit := func(_, value []byte) error { count++; sink ^= touchUnifiedScanAllBytes(value); return nil }
			run := func() {
				count = 0
				scratch, err = snapshot.RangeBoundsRawBuffer(lower, upper, scratch, false, visit)
				if err != nil || count != width {
					b.Fatalf("rows=%d want=%d: %v", count, width, err)
				}
			}
			run()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
			benchScanSink = sink
		})
	}
}
