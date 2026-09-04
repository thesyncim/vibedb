package gatewayruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type gatewayControllerMetrics struct {
	movePasses, moveDiscovered, moveAdvanced, moveCompleted, moveFaults       atomic.Uint64
	moveDurationNS, moveDurationMaxNS                                         atomic.Uint64
	splitPasses, splitDiscovered, splitTriggered, splitCompleted, splitFaults atomic.Uint64
	splitDurationNS, splitDurationMaxNS                                       atomic.Uint64
}

type gatewayControllerMetricsSnapshot struct {
	MovePasses, MoveDiscovered, MoveAdvanced, MoveCompleted, MoveFaults       uint64
	MoveDurationNS, MoveDurationMaxNS                                         uint64
	SplitPasses, SplitDiscovered, SplitTriggered, SplitCompleted, SplitFaults uint64
	SplitDurationNS, SplitDurationMaxNS                                       uint64
}

type gatewayControllerMetricsKey struct{}

func withGatewayControllerMetrics(ctx context.Context, metrics *gatewayControllerMetrics) context.Context {
	if ctx == nil || metrics == nil {
		return ctx
	}
	return context.WithValue(ctx, gatewayControllerMetricsKey{}, metrics)
}
func gatewayControllerMetricsFromContext(ctx context.Context) *gatewayControllerMetrics {
	metrics, _ := ctx.Value(gatewayControllerMetricsKey{}).(*gatewayControllerMetrics)
	return metrics
}

func (metrics *gatewayControllerMetrics) observeMove(pass rebalanceexec.ControllerPass, err error, elapsed time.Duration) {
	if metrics == nil {
		return
	}
	metrics.movePasses.Add(1)
	metrics.moveDiscovered.Add(uint64(pass.Moves))
	metrics.moveAdvanced.Add(uint64(pass.Advanced))
	metrics.moveCompleted.Add(uint64(pass.Completed))
	if err != nil && !errors.Is(err, context.Canceled) {
		metrics.moveFaults.Add(1)
	}
	metrics.moveDurationNS.Add(uint64(max(elapsed, 0)))
	atomicMax(&metrics.moveDurationMaxNS, uint64(max(elapsed, 0)))
}
func (metrics *gatewayControllerMetrics) observeSplit(pass splitcontroller.ControllerPass, err error, elapsed time.Duration) {
	if metrics == nil {
		return
	}
	metrics.splitPasses.Add(1)
	metrics.splitDiscovered.Add(uint64(pass.Discovered))
	metrics.splitTriggered.Add(uint64(pass.Triggered))
	metrics.splitCompleted.Add(uint64(pass.Completed))
	if err != nil && !errors.Is(err, context.Canceled) {
		metrics.splitFaults.Add(1)
	}
	metrics.splitDurationNS.Add(uint64(max(elapsed, 0)))
	atomicMax(&metrics.splitDurationMaxNS, uint64(max(elapsed, 0)))
}
func atomicMax(value *atomic.Uint64, candidate uint64) {
	for current := value.Load(); candidate > current && !value.CompareAndSwap(current, candidate); current = value.Load() {
	}
}
func (metrics *gatewayControllerMetrics) Snapshot() gatewayControllerMetricsSnapshot {
	if metrics == nil {
		return gatewayControllerMetricsSnapshot{}
	}
	return gatewayControllerMetricsSnapshot{MovePasses: metrics.movePasses.Load(), MoveDiscovered: metrics.moveDiscovered.Load(), MoveAdvanced: metrics.moveAdvanced.Load(), MoveCompleted: metrics.moveCompleted.Load(), MoveFaults: metrics.moveFaults.Load(), MoveDurationNS: metrics.moveDurationNS.Load(), MoveDurationMaxNS: metrics.moveDurationMaxNS.Load(), SplitPasses: metrics.splitPasses.Load(), SplitDiscovered: metrics.splitDiscovered.Load(), SplitTriggered: metrics.splitTriggered.Load(), SplitCompleted: metrics.splitCompleted.Load(), SplitFaults: metrics.splitFaults.Load(), SplitDurationNS: metrics.splitDurationNS.Load(), SplitDurationMaxNS: metrics.splitDurationMaxNS.Load()}
}
