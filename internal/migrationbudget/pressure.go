package migrationbudget

import (
	"context"
	"sync"
)

// pressureController owns only bounded atomics-like state under a short
// mutex. It never calls into a host, Raft, SQL, or authority lock. The mutex
// is held while changing a state sample and released before any migration
// waiter wakes.
type pressureController struct {
	mu     sync.Mutex
	config PressureConfig
	state  PressureStatus
	wake   chan struct{}
}

func newPressureController(config PressureConfig) *pressureController {
	return &pressureController{config: config, state: PressureStatus{ScalePPM: pressureScaleMax}, wake: make(chan struct{})}
}

// apply returns the old and new scale so the caller can clamp all token
// buckets outside this controller's mutex.
func (controller *pressureController) apply(sample PressureSample) (uint32, uint32) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if sample.Sequence == 0 || sample.Sequence <= controller.state.Sequence {
		return controller.state.ScalePPM, controller.state.ScalePPM
	}
	queuePressure := pressureRatio(sample.QueueDepth, sample.QueueCapacity)
	waitPressure := uint32(0)
	if sample.ReadySubmissions != 0 {
		averageWait := sample.ReadyQueueWaitNanos / sample.ReadySubmissions
		waitPressure = waitRatio(averageWait, controller.config.SevereWaitNanos)
	}
	if waitPressure > pressureScaleMax {
		waitPressure = pressureScaleMax
	}
	if sample.Initial {
		controller.state.Sequence = sample.Sequence
		controller.state.Timestamp = sample.Timestamp
		controller.state.QueuePressurePPM = queuePressure
		controller.state.WaitPressurePPM = waitPressure
		controller.state.BackpressureSubmissions = sample.BackpressureSubmissions
		controller.state.BackpressureTotal = sample.BackpressureSubmissions
		if controller.state.ScalePPM == 0 {
			controller.state.ScalePPM = pressureScaleMax
		}
		return controller.state.ScalePPM, controller.state.ScalePPM
	}

	previousScale := controller.state.ScalePPM
	if previousScale == 0 {
		previousScale = pressureScaleMax
	}
	oldScale := previousScale
	controller.state.Sequence = sample.Sequence
	controller.state.Timestamp = sample.Timestamp
	controller.state.QueuePressurePPM = queuePressure
	controller.state.WaitPressurePPM = waitPressure
	controller.state.BackpressureSubmissions = sample.BackpressureSubmissions
	if sample.BackpressureSubmissions > ^uint64(0)-controller.state.BackpressureTotal {
		controller.state.BackpressureTotal = ^uint64(0)
	} else {
		controller.state.BackpressureTotal += sample.BackpressureSubmissions
	}

	high := sample.BackpressureSubmissions != 0 ||
		queuePressure >= controller.config.HighQueuePPM ||
		waitPressure >= pressureRatio(controller.config.HighWaitNanos, controller.config.SevereWaitNanos)
	severe := sample.BackpressureSubmissions != 0 ||
		queuePressure >= controller.config.SevereQueuePPM ||
		waitPressure >= pressureScaleMax
	low := sample.BackpressureSubmissions == 0 &&
		queuePressure <= controller.config.LowQueuePPM &&
		waitPressure < pressureRatio(controller.config.HighWaitNanos, controller.config.SevereWaitNanos)

	if high {
		if controller.state.HighWindows < ^uint32(0) {
			controller.state.HighWindows++
		}
		controller.state.LowWindows = 0
		if previousScale > controller.config.MinimumScalePPM {
			previousScale /= 2
			if previousScale < controller.config.MinimumScalePPM {
				previousScale = controller.config.MinimumScalePPM
			}
			controller.state.Downshifts++
		}
	} else {
		controller.state.HighWindows = 0
	}
	if severe {
		if controller.state.SevereWindows < ^uint32(0) {
			controller.state.SevereWindows++
		}
	} else {
		controller.state.SevereWindows = 0
	}

	if controller.state.SevereWindows >= controller.config.PauseWindows {
		controller.state.Paused = true
	}
	if low {
		if controller.state.LowWindows < ^uint32(0) {
			controller.state.LowWindows++
		}
		if controller.state.LowWindows >= controller.config.RecoveryWindows {
			controller.state.LowWindows = 0
			if previousScale < pressureScaleMax {
				previousScale += controller.config.RecoveryStepPPM
				if previousScale > pressureScaleMax {
					previousScale = pressureScaleMax
				}
				controller.state.RecoverySteps++
			}
			if controller.state.Paused {
				controller.state.Paused = false
				close(controller.wake)
				controller.wake = make(chan struct{})
			}
		}
	} else if !high {
		controller.state.LowWindows = 0
	}
	controller.state.ScalePPM = previousScale
	return oldScale, previousScale
}

func (controller *pressureController) status() PressureStatus {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.state
}

func (controller *pressureController) wait(ctx context.Context, closed <-chan struct{}) error {
	for {
		controller.mu.Lock()
		paused := controller.state.Paused
		wake := controller.wake
		controller.mu.Unlock()
		if !paused {
			select {
			case <-closed:
				return ErrClosed
			default:
				return nil
			}
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return ErrClosed
		}
	}
}

func pressureRatio(numerator, denominator uint64) uint32 {
	if denominator == 0 {
		return 0
	}
	if numerator >= denominator {
		return pressureScaleMax
	}
	const maxUint64 = ^uint64(0)
	if numerator <= maxUint64/uint64(pressureScaleMax) {
		return uint32((numerator * uint64(pressureScaleMax)) / denominator)
	}
	// The ratio is only a scheduling signal. Floating-point division avoids an
	// overflowing intermediate for a very large synthetic gauge while retaining
	// the same bounded [0, pressureScaleMax) result.
	result := uint32(float64(numerator) / float64(denominator) * float64(pressureScaleMax))
	if result >= pressureScaleMax {
		return pressureScaleMax - 1
	}
	return result
}

func waitRatio(averageNanos, severeNanos uint64) uint32 {
	if severeNanos == 0 || averageNanos >= severeNanos {
		if averageNanos == 0 {
			return 0
		}
		return pressureScaleMax
	}
	const maxUint64 = ^uint64(0)
	if averageNanos <= maxUint64/uint64(pressureScaleMax) {
		return uint32((averageNanos * uint64(pressureScaleMax)) / severeNanos)
	}
	result := uint32(float64(averageNanos) / float64(severeNanos) * float64(pressureScaleMax))
	if result >= pressureScaleMax {
		return pressureScaleMax - 1
	}
	return result
}
