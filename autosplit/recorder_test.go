package autosplit

import (
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestRecorderRotatesOneExactConcurrentWindow(t *testing.T) {
	source := testSource(balancedRange())
	recorder, err := NewRecorder(source, 7, 8)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	const perWorker = 2_000
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Go(func() {
			point := testPoint(uint64(worker+1) << (64 - source.BucketBits))
			load := LoadVector{ResourceWriteCPU: 3, ResourceRequests: 1, ResourceLiveBytes: 2}
			for range perWorker {
				if !recorder.ObservePoint(point, load, 1) {
					t.Errorf("worker %d observation rejected", worker)
					return
				}
			}
		})
	}
	wait.Wait()
	window, err := recorder.Rotate(8)
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(workers * perWorker)
	if window.sequence != 7 || window.samples != want ||
		window.total[ResourceWriteCPU] != 3*want ||
		window.total[ResourceRequests] != want || window.hotTotal != want {
		t.Fatalf("window = sequence %d samples %d total %+v hot %d",
			window.sequence, window.samples, window.total, window.hotTotal)
	}
	if next, err := recorder.Rotate(9); err != nil || next.sequence != 8 || next.samples != 0 {
		t.Fatalf("empty next window = %+v, %v", next, err)
	}
	if _, err := recorder.Rotate(9); err == nil {
		t.Fatal("non-increasing rotation sequence accepted")
	}
}

func TestRecorderMergeRetainsHotBucketAndFanoutEvidence(t *testing.T) {
	source := testSource(balancedRange())
	recorder, _ := NewRecorder(source, 1, 4)
	hot := testPoint(uint64(91)<<(64-source.BucketBits) | 13)
	for range 100 {
		recorder.ObservePoint(hot, LoadVector{ResourceWriteCPU: 10, ResourceRequests: 1}, 10)
	}
	recorder.ObserveSpan(
		testPoint(0), distribution.KeyspaceEnd{Point: testPoint(uint64(1) << 63)}, 7,
	)
	recorder.ObserveUnbounded(3)
	window, err := recorder.Rotate(2)
	if err != nil {
		t.Fatal(err)
	}
	hitter, ok := strongestCelebrity(window, 500_000)
	if !ok || !distribution.VirtualBucketBoundary(hitter.point, source.BucketBits) {
		t.Fatalf("merged heavy hitter = %+v,%v", hitter, ok)
	}
	if window.bounded != 7 || window.unbounded != 3 {
		t.Fatalf("fanout evidence = bounded %d unbounded %d", window.bounded, window.unbounded)
	}
}

func TestRecorderCompositeObservationsRetainOneSample(t *testing.T) {
	source := testSource(balancedRange())
	recorder, _ := NewRecorder(source, 1, 1)
	bucket, _ := distribution.VirtualBucketRange(91, source.BucketBits)
	malformed := bucket.Start
	malformed[7] = 1
	if recorder.ObserveBucket(
		malformed, LoadVector{ResourceRequests: 99}, 99, 1,
	) {
		t.Fatal("non-canonical bucket observation accepted")
	}
	if !recorder.ObserveBucket(
		bucket.Start, LoadVector{ResourceRequests: 1}, 7, 1,
	) || !recorder.ObserveUnknown(LoadVector{ResourceRequests: 1}, 1) {
		t.Fatal("composite observation rejected")
	}
	window, err := recorder.Rotate(2)
	if err != nil {
		t.Fatal(err)
	}
	if window.samples != 2 || window.total[ResourceRequests] != 2 ||
		window.hotTotal != 7 || window.bounded != 1 || window.unbounded != 1 {
		t.Fatalf("composite window = %+v", window.Pulse())
	}
}

func TestRecorderUniformObservationPreservesExactTotalsWithoutHotBucket(t *testing.T) {
	source := testSource(balancedRange())
	recorder, _ := NewRecorder(source, 3, 2)
	load := LoadVector{ResourceIO: 67, ResourceRequests: 1, ResourceLatencyDebt: 131}
	if !recorder.ObserveUniform(load) {
		t.Fatal("uniform observation rejected")
	}
	window, err := recorder.Rotate(4)
	if err != nil {
		t.Fatal(err)
	}
	if window.samples != 1 || window.total != [ResourceCount]uint64{
		ResourceIO: 67, ResourceRequests: 1, ResourceLatencyDebt: 131,
	} {
		t.Fatalf("uniform totals = samples %d total %+v", window.samples, window.total)
	}
	for resource := range ResourceCount {
		var attributed uint64
		for bin := range BinCount {
			attributed += uint64(window.bins[bin][resource])
		}
		if attributed != 0 || window.uniform[resource] != window.total[resource] {
			t.Fatalf("resource %d attributed %d uniform %d total %d",
				resource, attributed, window.uniform[resource], window.total[resource])
		}
	}
	if window.hotTotal != 0 {
		t.Fatalf("uniform observation manufactured hot weight %d", window.hotTotal)
	}
}

func TestRecorderRejectsInvalidConstructionAndBoundsSpace(t *testing.T) {
	source := testSource(balancedRange())
	if _, err := NewRecorder(source, 0, 1); err == nil {
		t.Fatal("zero sequence accepted")
	}
	if _, err := NewRecorder(source, 1, MaxRecorderLanes+1); err == nil {
		t.Fatal("oversized lane count accepted")
	}
	unaligned := source
	unaligned.Range.Start[7] = 1
	if _, err := NewRecorder(unaligned, 1, 1); err == nil {
		t.Fatal("unaligned serving range accepted")
	}
	recorder, err := NewRecorder(source, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if recorder.RetainedBytes() > 32<<10 {
		t.Fatalf("default recorder retained %d bytes, want <= 32 KiB", recorder.RetainedBytes())
	}
}
