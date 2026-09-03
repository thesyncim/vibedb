package planner

import (
	"math/rand/v2"
	"slices"
	"testing"
)

func TestDistinctSketchUnionMatchesExactSet(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	for _, k := range []int{16, 64, 2048} {
		merged := distinctSample{capacity: k}
		all := make(map[uint64]bool)
		var scratch []uint64
		for shard := range 20 {
			part := distinctSample{capacity: k}
			for i := range shard*31 + 1 {
				h := rng.Uint64()
				if i%3 == 0 {
					h = uint64(i)
				}
				if i%11 == 0 {
					h = ^uint64(0)
				}
				part.add(h)
				all[h] = true
			}
			before := slices.Clone(part.hashes)
			scratch = merged.merge(part, scratch)
			want := make([]uint64, 0, len(all))
			for h := range all {
				want = append(want, h)
			}
			slices.Sort(want)
			if !slices.Equal(merged.hashes, want[:min(k, len(want))]) || merged.full != (len(want) > k) {
				t.Fatalf("capacity=%d shard=%d union differs from exact set", k, shard)
			}
			if !slices.Equal(part.hashes, before) {
				t.Fatal("merge changed source")
			}
		}
	}
	// A duplicate at the threshold does not turn a census into an estimate.
	exact := distinctSample{capacity: 16}
	for i := range 16 {
		exact.add(uint64(i))
	}
	exact.add(15)
	if exact.full {
		t.Fatal("duplicate threshold marked truncated")
	}
	copyOfExact := distinctSample{capacity: 16}
	copyOfExact.merge(exact, nil)
	copyOfExact.merge(exact, nil)
	if copyOfExact.full || len(copyOfExact.hashes) != 16 {
		t.Fatal("identical unions marked truncated")
	}
}

func BenchmarkDistinctSketchUnion(b *testing.B) {
	const k = 2048
	rng := rand.New(rand.NewPCG(3, 5))
	a, other := distinctSample{capacity: k}, distinctSample{capacity: k}
	for range 10000 {
		a.add(rng.Uint64())
		other.add(rng.Uint64())
	}
	for _, linear := range []bool{false, true} {
		name := "insert-sorted"
		if linear {
			name = "linear-union"
		}
		b.Run(name, func(b *testing.B) {
			work := distinctSample{capacity: k, hashes: make([]uint64, k), full: true}
			scratch := make([]uint64, 0, k)
			b.ReportAllocs()
			for b.Loop() {
				copy(work.hashes, a.hashes)
				if linear {
					scratch = work.merge(other, scratch)
				} else {
					for _, h := range other.hashes {
						work.add(h)
					}
				}
			}
		})
	}
}
