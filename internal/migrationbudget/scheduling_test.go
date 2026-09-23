package migrationbudget

import (
	"math"
	"testing"
	"time"
)

// CPU contention slows migration to its floor but never pauses it: on a host
// where foreground load alone saturates the cores, a pause would starve the
// migration forever. Quiet samples then recover the rate.
func TestSchedulingPressureDownshiftsWithoutPausing(t *testing.T) {
	budget, err := New(pressureTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	budget.ApplyPressure(PressureSample{Sequence: 1, Initial: true, QueueCapacity: 8})
	contended := uint64(80 * time.Millisecond)
	for sequence := uint64(2); sequence <= 12; sequence++ {
		budget.ApplyPressure(PressureSample{Sequence: sequence, QueueCapacity: 8, SchedulingLatencyNanos: contended})
	}
	state := budget.Pressure()
	if state.Paused || state.ScalePPM != DefaultPressureConfig().MinimumScalePPM ||
		state.SchedulingPressurePPM != PressureScaleMax || state.SevereWindows != 0 {
		t.Fatalf("contended state = %+v, want floor without pause", state)
	}
	// Below the high threshold is quiet.
	quiet := uint64(time.Millisecond)
	for sequence := uint64(13); sequence <= 13+3*8; sequence++ {
		budget.ApplyPressure(PressureSample{Sequence: sequence, QueueCapacity: 8, SchedulingLatencyNanos: quiet})
	}
	if recovered := budget.Pressure(); recovered.ScalePPM != PressureScaleMax {
		t.Fatalf("quiet scheduling did not recover full rate: %+v", recovered)
	}
}

func TestSchedulingPressureConfigIsAPair(t *testing.T) {
	config := pressureTestConfig()
	config.Pressure.SevereSchedulingNanos = 0
	if err := config.Validate(); err == nil {
		t.Fatal("half-configured scheduling thresholds accepted")
	}
	config.Pressure.HighSchedulingNanos, config.Pressure.SevereSchedulingNanos = 0, 0
	if err := config.Validate(); err != nil {
		t.Fatalf("disabled scheduling signal rejected: %v", err)
	}
	config.Pressure.HighSchedulingNanos, config.Pressure.SevereSchedulingNanos = 10, 5
	if err := config.Validate(); err == nil {
		t.Fatal("inverted scheduling thresholds accepted")
	}
}

func TestHistogramQuantileUsesBucketUpperBound(t *testing.T) {
	buckets := []float64{0, 1e-6, 1e-3, 1e-2, math.Inf(1)}
	counts := []uint64{90, 9, 1, 0}
	if got := histogramQuantileNanos(counts, buckets, 100, 99); got != uint64(time.Millisecond) {
		t.Fatalf("p99=%d, want 1ms bucket bound", got)
	}
	if got := histogramQuantileNanos(counts, buckets, 100, 50); got != uint64(time.Microsecond) {
		t.Fatalf("p50=%d, want 1us bucket bound", got)
	}
	if got := histogramQuantileNanos([]uint64{0, 0, 0, 1}, buckets, 1, 99); got != uint64(10*time.Millisecond) {
		t.Fatalf("open-ended bucket=%d, want its lower bound", got)
	}
	if got := histogramQuantileNanos(counts, buckets, 0, 99); got != 0 {
		t.Fatalf("empty interval=%d", got)
	}
}

func TestSchedulingLatencyProbeReadsRuntime(t *testing.T) {
	probe := NewSchedulingLatencyProbe()
	if probe == nil {
		t.Skip("runtime does not export scheduling latency")
	}
	done := make(chan struct{})
	for range 64 {
		go func() { <-done }()
	}
	close(done)
	time.Sleep(10 * time.Millisecond)
	_ = probe.P99Nanos() // must not panic; value is environment dependent
}
