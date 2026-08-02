package distribution

// ResolvePoint sits on the routing hot path of every point lookup, and the
// design claims O(log shard_count) with no allocation. These benchmarks make
// that shape observable — the same resolve at 8, 64, 512, and 4096 shards, so
// near-flat/logarithmic ns/op growth is visible against the 512x shard-count
// increase — and TestZeroAllocationResolvePoint pins the zero-allocation
// contract as a regression gate, matching the allocation-gated benchmark style
// the append hot path already uses.

import (
	"fmt"
	"math"
	"testing"
)

var (
	benchSinkShard  ShardID
	benchSinkShards []ShardID
	benchSinkTarget Target
)

// benchShardCounts is the geometric sweep that exposes the resolver's growth
// shape.
var benchShardCounts = []int{8, 64, 512, 4096}

// benchProbePoints spreads n points across the whole keyspace so a benchmark
// exercises every branch of the search rather than one hot path.
func benchProbePoints(n int) []KeyspacePoint {
	pts := make([]KeyspacePoint, n)
	step := uint64(math.MaxUint64) / uint64(n)
	for i := range pts {
		pts[i] = pt(uint64(i) * step)
	}
	return pts
}

func BenchmarkResolvePoint(b *testing.B) {
	const probes = 1024
	pts := benchProbePoints(probes)
	for _, shardCount := range benchShardCounts {
		m := manifestFromBounds(b, evenlySpacedBounds(shardCount)...)
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				benchSinkShard, _ = m.ResolvePoint(pts[i&(probes-1)])
				i++
			}
		})
	}
}

func BenchmarkResolvePointTarget(b *testing.B) {
	const probes = 1024
	pts := benchProbePoints(probes)
	for _, shardCount := range benchShardCounts {
		m := manifestFromBounds(b, evenlySpacedBounds(shardCount)...)
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				benchSinkTarget, _ = m.ResolvePointTarget(pts[i&(probes-1)])
				i++
			}
		})
	}
}

func BenchmarkResolveRange(b *testing.B) {
	const probes = 1024
	starts := benchProbePoints(probes)
	span := uint64(math.MaxUint64) / 8 // spans roughly an eighth of the keyspace
	for _, shardCount := range benchShardCounts {
		m := manifestFromBounds(b, evenlySpacedBounds(shardCount)...)
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				sv := ptVal(starts[i&(probes-1)])
				end := kend(sv + span)
				if sv+span < sv { // saturating: keep the range valid at the top
					end = maxKeyEnd
				}
				benchSinkShards, _ = m.ResolveRange(KeyRange{Start: pt(sv), End: end})
				i++
			}
		})
	}
}

// TestZeroAllocationResolvePoint pins the promised zero-allocation point
// resolver: a closure-free binary search over an array-compared key must not
// allocate, even at the largest benchmarked shard count.
func TestZeroAllocationResolvePoint(t *testing.T) {
	m := manifestFromBounds(t, evenlySpacedBounds(4096)...)
	pts := benchProbePoints(1024)
	i := 0
	allocations := testing.AllocsPerRun(1000, func() {
		benchSinkShard, _ = m.ResolvePoint(pts[i&1023])
		i++
	})
	if allocations != 0 {
		t.Fatalf("ResolvePoint allocations = %v, want 0", allocations)
	}
}

func TestZeroAllocationResolvePointTarget(t *testing.T) {
	m := manifestFromBounds(t, evenlySpacedBounds(4096)...)
	pts := benchProbePoints(1024)
	i := 0
	allocations := testing.AllocsPerRun(1000, func() {
		var ok bool
		benchSinkTarget, ok = m.ResolvePointTarget(pts[i&1023])
		if !ok || benchSinkTarget.Shard == "" ||
			benchSinkTarget.Endpoint == "" ||
			benchSinkTarget.Role != RoleLeader {
			t.Fatal("ResolvePointTarget returned an incomplete leader target")
		}
		i++
	})
	if allocations != 0 {
		t.Fatalf("ResolvePointTarget allocations = %v, want 0", allocations)
	}
}
