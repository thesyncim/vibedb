package migrationbudget

import (
	"context"
	"errors"
	"testing"
	"time"
)

func pressureTestConfig() Config {
	config := testConfig()
	config.Pressure = DefaultPressureConfig()
	return config
}

func TestPressureDownshiftsPausesAndRecoversWithHysteresis(t *testing.T) {
	budget, err := New(pressureTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	budget.ApplyPressure(PressureSample{Sequence: 1, Initial: true, QueueCapacity: 8})
	budget.ApplyPressure(PressureSample{Sequence: 2, QueueDepth: 7, QueueCapacity: 8})
	if got := budget.Pressure(); got.ScalePPM != 500_000 || got.Paused {
		t.Fatalf("high pressure state = %+v", got)
	}
	budget.ApplyPressure(PressureSample{Sequence: 3, QueueDepth: 8, QueueCapacity: 8})
	budget.ApplyPressure(PressureSample{Sequence: 4, QueueDepth: 8, QueueCapacity: 8})
	paused := budget.Pressure()
	if !paused.Paused || paused.ScalePPM != 125_000 {
		t.Fatalf("severe pressure state = %+v", paused)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := budget.WaitPressure(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("paused wait = %v", err)
	}
	// Duplicate and stale samples cannot accidentally recover a paused budget.
	budget.ApplyPressure(PressureSample{Sequence: 4, QueueDepth: 0, QueueCapacity: 8})
	if !budget.Pressure().Paused {
		t.Fatal("stale pressure sample changed pause state")
	}
	for sequence := uint64(5); sequence <= 7; sequence++ {
		budget.ApplyPressure(PressureSample{Sequence: sequence, QueueCapacity: 8})
	}
	recovered := budget.Pressure()
	if recovered.Paused || recovered.ScalePPM <= paused.ScalePPM || recovered.RecoverySteps == 0 {
		t.Fatalf("quiet recovery state = %+v", recovered)
	}
}

func TestPressureZeroCapacityAndCounterResetAreSafe(t *testing.T) {
	budget, err := New(pressureTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	budget.ApplyPressure(PressureSample{Sequence: 1, Initial: true, QueueDepth: 99})
	budget.ApplyPressure(PressureSample{Sequence: 2, QueueDepth: ^uint64(0), QueueCapacity: ^uint64(0), ReadyQueueWaitNanos: ^uint64(0), ReadySubmissions: 1})
	status := budget.Pressure()
	if status.QueuePressurePPM == 0 || status.WaitPressurePPM == 0 {
		t.Fatalf("large pressure sample was not bounded: %+v", status)
	}
	// A restarted sequencer reports a fresh interval baseline; no cumulative
	// counter wrap is treated as a pressure event by the node sampler.
	budget.ApplyPressure(PressureSample{Sequence: 3, QueueCapacity: 8})
	if budget.Pressure().BackpressureSubmissions != 0 {
		t.Fatal("counter reset produced a pressure event")
	}
}
