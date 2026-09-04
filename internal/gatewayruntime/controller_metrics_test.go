package gatewayruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

func TestGatewayControllerMetricsObserveActualPasses(t *testing.T) {
	metrics := new(gatewayControllerMetrics)
	metrics.observeMove(rebalanceexec.ControllerPass{Moves: 3, Advanced: 2, Completed: 1}, nil, 10*time.Nanosecond)
	metrics.observeMove(rebalanceexec.ControllerPass{Moves: 4, Advanced: 1}, errors.New("fault"), 20*time.Nanosecond)
	metrics.observeSplit(splitcontroller.ControllerPass{Discovered: 5, Triggered: 3, Completed: 2}, nil, 30*time.Nanosecond)
	metrics.observeSplit(splitcontroller.ControllerPass{}, context.Canceled, time.Nanosecond)
	got := metrics.Snapshot()
	if got.MovePasses != 2 || got.MoveDiscovered != 7 || got.MoveAdvanced != 3 || got.MoveCompleted != 1 ||
		got.MoveFaults != 1 || got.MoveDurationNS != 30 || got.MoveDurationMaxNS != 20 ||
		got.SplitPasses != 2 || got.SplitDiscovered != 5 || got.SplitTriggered != 3 || got.SplitCompleted != 2 ||
		got.SplitFaults != 0 || got.SplitDurationNS != 31 || got.SplitDurationMaxNS != 30 {
		t.Fatalf("metrics=%+v", got)
	}
}

func BenchmarkGatewayControllerMetricsSnapshot(b *testing.B) {
	metrics := new(gatewayControllerMetrics)
	b.ReportAllocs()
	for b.Loop() {
		_ = metrics.Snapshot()
	}
}
