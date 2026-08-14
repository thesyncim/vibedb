package autosplit

import (
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

func BenchmarkSketchObservePoint(b *testing.B) {
	sketch, _ := NewSketch(testSource(balancedRange()), 1)
	point := testPoint(0x7f00112233445566)
	load := LoadVector{ResourceWriteCPU: 1, ResourceRequests: 1, ResourceLiveBytes: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sketch.ObservePoint(point, load, 1)
	}
	b.ReportMetric(float64(unsafe.Sizeof(Sketch{})), "sketch-B")
}

func BenchmarkRecommend64Bins(b *testing.B) {
	sketch, _ := balancedSketch(b)
	capacities := balancedCapacities()
	policy := Policy{TriggerPressurePPM: 1, MinBenefitPPM: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = Recommend(sketch, capacities, policy)
	}
	b.ReportMetric(float64(unsafe.Sizeof(Tracker{})), "tracker-B")
}

func balancedRange() distribution.KeyRange {
	return distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
}
