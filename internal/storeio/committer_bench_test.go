package storeio

import "testing"

// BenchmarkCommitterBufferPool records both the former per-index reservation
// shape and the bulk operation Committer.Begin/Batch.ResizePages now use.
func BenchmarkCommitterBufferPool(b *testing.B) {
	const count = 601
	indexes := make([]uint32, count)
	b.Run("per-index", func(b *testing.B) {
		pool := newIndexPool(count)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			for i := range indexes {
				index, ok := pool.pop()
				if !ok {
					b.Fatal("pop failed")
				}
				indexes[i] = index
			}
			for _, index := range indexes {
				pool.push(index)
			}
		}
	})
	b.Run("bulk", func(b *testing.B) {
		pool := newIndexPool(count)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if !pool.popN(indexes) {
				b.Fatal("popN failed")
			}
			pool.pushN(indexes)
		}
	})
}
