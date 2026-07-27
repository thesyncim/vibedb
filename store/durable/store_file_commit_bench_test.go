package durable

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runCollectionWriters drives exactly writers concurrent Put loops over b.N
// operations. It does not use RunParallel, because SetParallelism multiplies by
// GOMAXPROCS and so cannot express the one case the durability trade turns on:
// a single serialized writer, which never has a second generation queued to
// group with and therefore pays any coalescing window as pure added latency.
func runCollectionWriters(b *testing.B, collection *Collection, writers int) {
	b.Helper()
	var next atomic.Int64
	var group sync.WaitGroup
	limit := int64(b.N)
	for range writers {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				i := next.Add(1) - 1
				if i >= limit {
					return
				}
				if _, err := collection.Put(fmt.Sprintf("key-%09d", i), benchDocument(int(i))); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	group.Wait()
}

// BenchmarkFileStoreCommitGrouping sweeps the group-commit knobs against the
// two shapes of writer that behave differently under them. It exists to
// justify the defaults rather than to track a number, because the answer is a
// policy choice about latency rather than a throughput fact.
func BenchmarkFileStoreCommitGrouping(b *testing.B) {
	type knob struct {
		name     string
		queue    int
		group    int
		coalesce time.Duration
	}
	knobs := []knob{
		{"default", 0, 0, 0},
		{"group=2", 4, 2, 0},
		{"group=64", 64, 64, 0},
		{"coalesce=1ms", 0, 0, time.Millisecond},
		{"group=64+coalesce=1ms", 64, 64, time.Millisecond},
	}
	for _, synchronous := range []bool{false, true} {
		for _, writers := range []int{1, 8, 64} {
			for _, k := range knobs {
				name := fmt.Sprintf("sync=%v/writers=%d/%s", synchronous, writers, k.name)
				b.Run(name, func(b *testing.B) {
					options := benchWriteOptions()
					if synchronous {
						options.Durability = DurabilitySync
					} else {
						options.Durability = DurabilityAsyncVisible
					}
					options.QueueSlots = k.queue
					options.GroupLimit = k.group
					options.CommitCoalesce = k.coalesce
					collection, done := openBenchCollection(b, options)
					defer done()
					base := collection.Stats()
					b.ResetTimer()
					runCollectionWriters(b, collection, writers)
					b.StopTimer()
					if err := collection.Flush(); err != nil {
						b.Fatal(err)
					}
					stats := collection.Stats()
					reportWriteAmplification(b, stats, base, b.N)
					b.ReportMetric(float64(stats.LargestCommitGroup), "maxgroup")
				})
			}
		}
	}
}

// BenchmarkFileStoreAutomaticMutationCombining isolates the work saved before
// persistence grouping: concurrent ordinary Put calls either rebuild one
// generation each or share the same batched page/tree materializer. A single
// writer is the no-backlog control and should remain on the direct path.
func BenchmarkFileStoreAutomaticMutationCombining(b *testing.B) {
	for _, writers := range []int{1, 8, 64} {
		for _, disabled := range []bool{true, false} {
			name := fmt.Sprintf("writers=%d/enabled=%v", writers, !disabled)
			b.Run(name, func(b *testing.B) {
				options := benchWriteOptions()
				options.DisableMutationCombining = disabled
				collection, done := openBenchCollection(b, options)
				defer done()
				base := collection.Stats()
				b.ResetTimer()
				runCollectionWriters(b, collection, writers)
				b.StopTimer()
				if err := collection.Flush(); err != nil {
					b.Fatal(err)
				}
				stats := collection.Stats()
				reportWriteAmplification(b, stats, base, b.N)
				b.ReportMetric(
					float64(stats.LargestAutomaticMutationGroup),
					"maxapply",
				)
				if stats.AutomaticMutationGroups != 0 {
					b.ReportMetric(
						float64(stats.AutomaticMutationRequests)/
							float64(stats.AutomaticMutationGroups),
						"calls/apply",
					)
				}
			})
		}
	}
}
