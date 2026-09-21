//go:build linux

package gatewayruntime

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestSeamlessScaleSQLRecoveryUsesElapsedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		calls := 0
		result, retries, err := retrySeamlessScaleSQL(t.Context(), false, func(context.Context) (fusedPGResult, error) {
			calls++
			if time.Since(started) < time.Second {
				return fusedPGResult{code: "40001", message: "transaction aborted"}, nil
			}
			return fusedPGResult{tag: "INSERT 0 1"}, nil
		})
		if err != nil || result.tag != "INSERT 0 1" || retries < 4 || calls != int(retries)+1 {
			t.Fatalf("result=%+v retries=%d calls=%d err=%v", result, retries, calls, err)
		}
	})
}

func TestSeamlessScaleSQLNeverReplaysAmbiguousOrPermanentWrites(t *testing.T) {
	for _, response := range []fusedPGResult{
		{code: "40003", message: "write outcome unknown: no reachable leader"},
		{code: "XX000", message: "no reachable leader"},
		{code: "42501", message: "unauthorized"},
		{code: "23505", message: "duplicate key"},
	} {
		calls := 0
		result, retries, err := retrySeamlessScaleSQL(t.Context(), false, func(context.Context) (fusedPGResult, error) { calls++; return response, nil })
		if err != nil || result.code != response.code || retries != 0 || calls != 1 {
			t.Fatalf("response=%+v retries=%d calls=%d err=%v", response, retries, calls, err)
		}
	}
	for _, readOnly := range []bool{false, true} {
		calls := 0
		_, retries, err := retrySeamlessScaleSQL(t.Context(), readOnly, func(context.Context) (fusedPGResult, error) { calls++; return fusedPGResult{}, io.EOF })
		if !errors.Is(err, io.EOF) || retries != 0 || calls != 1 {
			t.Fatal("transport error retried")
		}
	}
}

func TestSeamlessScaleSQLRetryStopsAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		_, retries, err := retrySeamlessScaleSQL(t.Context(), true, func(context.Context) (fusedPGResult, error) {
			return fusedPGResult{code: "XX000", message: "no reachable leader"}, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || retries == 0 || time.Since(started) != seamlessScaleRecoveryBudget {
			t.Fatalf("elapsed=%s retries=%d err=%v", time.Since(started), retries, err)
		}
	})
}

func TestSeamlessScaleFaultWindowsPreserveEverySampleAndSteadyContinuity(t *testing.T) {
	start := time.Unix(100, 0)
	workload := &seamlessScaleWorkload{history: make(map[string][]seamlessScaleWindow)}
	for i := range 5 {
		from := start.Add(time.Duration(i) * 10 * time.Second)
		samples := []seamlessScaleSample{
			{Scheduled: from, Started: from.Add(time.Millisecond), Completed: from.Add(2 * time.Millisecond), Latency: 2 * time.Millisecond, QueueLag: time.Millisecond},
			{Scheduled: from.Add(time.Second), Started: from.Add(time.Second + time.Millisecond), Completed: from.Add(time.Second + 2*time.Millisecond), Latency: 2 * time.Millisecond, QueueLag: time.Millisecond},
		}
		evidence := workload.phaseEvidence(seamlessScalePhaseDuring, from, from.Add(10*time.Second), 2, 0, samples)
		workload.history[seamlessScalePhaseDuring] = append(workload.history[seamlessScalePhaseDuring], seamlessScaleWindow{evidence: evidence, samples: samples})
	}
	workload.MarkFault(start.Add(11 * time.Second))
	steady, recovery, normalWindows, faultWindows, injections := workload.FaultTimingEvidence()
	if normalWindows != 3 || faultWindows != 2 || injections != 1 || steady.Scheduled != 6 || recovery.Scheduled != 4 ||
		steady.DurationNS != uint64(30*time.Second) || recovery.DurationNS != uint64(20*time.Second) ||
		steady.CompletionGapNS != uint64(9*time.Second) {
		t.Fatalf("lost sample or fabricated gap: steady=%+v recovery=%+v windows=%d/%d injections=%d", steady, recovery, normalWindows, faultWindows, injections)
	}
}
