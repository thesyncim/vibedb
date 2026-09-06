package storeio

import (
	"fmt"
	"testing"
)

// BenchmarkPageCacheDirtyBudget measures the per-mutation dirty-budget check
// against cache size. The writer runs this once per Put, so anything that
// scales with the frame count here is a size-dependent tax on a bound that is
// O(1) by construction. Stats is measured beside it because that is what the
// check used to call.
func BenchmarkPageCacheDirtyBudget(b *testing.B) {
	for _, megabytes := range []int64{1, 16, 64} {
		file, storeID, _ := newPageCacheFixture(b, 1)
		cache, err := NewPageCache(file, PageCacheOptions{
			PageSize: pageCacheTestPageSize, ResidentBytes: megabytes << 20,
			StoreID: storeID, PrefetchQueue: 4,
		})
		if err != nil {
			b.Fatal(err)
		}
		name := fmt.Sprintf("resident=%dMiB", megabytes)
		b.Run(name+"/DirtyCapacityAvailable", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink += cache.DirtyCapacityAvailable()
			}
		})
		b.Run(name+"/Stats", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink += cache.Stats().DirtyBytes
			}
		})
		if err := cache.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPageCacheWarmAcquire isolates the resident lease cost paid once per
// durable radix level. It keeps the route pages distinct so a future routing
// optimization cannot hide a deep page walk behind one repeatedly acquired
// frame.
func BenchmarkPageCacheWarmAcquire(b *testing.B) {
	for _, depth := range []int{1, 3, 5} {
		file, storeID, refs := newPageCacheFixture(b, depth)
		cache, err := NewPageCache(file, PageCacheOptions{
			PageSize:      pageCacheTestPageSize,
			ResidentBytes: int64(depth+1) * pageCacheTestPageSize,
			StoreID:       storeID,
			PrefetchQueue: 4,
		})
		if err != nil {
			b.Fatal(err)
		}
		for _, ref := range refs {
			lease, acquireErr := cache.Acquire(ref)
			if acquireErr != nil {
				b.Fatal(acquireErr)
			}
			lease.Release()
		}
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, ref := range refs {
					lease, acquireErr := cache.Acquire(ref)
					if acquireErr != nil {
						b.Fatal(acquireErr)
					}
					sink += uint64(lease.Payload()[0])
					lease.Release()
				}
			}
			b.ReportMetric(
				float64(b.Elapsed().Nanoseconds())/
					float64(max(1, b.N*depth)),
				"ns/acquire",
			)
		})
		if err := cache.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPageCacheParallelAcquire hammers one resident frame from all
// threads. The pin path is the hottest shared-memory operation in the read
// path, so this measures the contention ceiling directly: a frame-lock pin
// serializes here, while the lock-free pin scales.
func BenchmarkPageCacheParallelAcquire(b *testing.B) {
	file, storeID, refs := newPageCacheFixture(b, 1)
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: pageCacheTestPageSize, ResidentBytes: 2 * pageCacheTestPageSize,
		StoreID: storeID, PrefetchQueue: 4,
	})
	if err != nil {
		b.Fatal(err)
	}
	ref := refs[0]
	lease, err := cache.Acquire(ref)
	if err != nil {
		b.Fatal(err)
	}
	lease.Release()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local uint64
		for pb.Next() {
			l, acquireErr := cache.Acquire(ref)
			if acquireErr != nil {
				b.Error(acquireErr)
				return
			}
			local += uint64(l.Payload()[0])
			l.Release()
		}
		_ = local
	})
	if err := cache.Close(); err != nil {
		b.Fatal(err)
	}
}

var sink uint64
