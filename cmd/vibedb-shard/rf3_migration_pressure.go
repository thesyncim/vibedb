package main

import (
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
)

const rf3MigrationPressureSampleInterval = 250 * time.Millisecond

func (owner *rf3NodeOwner) startMigrationPressureSampler(budget *migrationbudget.Budget) {
	if owner == nil || owner.sequencer == nil || budget == nil || owner.pressureDone != nil {
		return
	}
	owner.migrationBudget = budget
	owner.pressureStop = make(chan struct{})
	owner.pressureDone = make(chan struct{})
	stop, done, sequencer := owner.pressureStop, owner.pressureDone, owner.sequencer
	go func() {
		defer close(done)
		ticker := time.NewTicker(rf3MigrationPressureSampleInterval)
		defer ticker.Stop()
		baseline := sequencer.Stats()
		sequence := uint64(1)
		budget.ApplyPressure(migrationbudget.PressureSample{
			Sequence: sequence, Timestamp: time.Now(), QueueDepth: baseline.QueueDepth,
			QueueCapacity: baseline.QueueCapacity, Initial: true,
		})
		for {
			select {
			case <-ticker.C:
				current := sequencer.Stats()
				sequence++
				budget.ApplyPressure(migrationbudget.PressureSample{
					Sequence: sequence, Timestamp: time.Now(), QueueDepth: current.QueueDepth,
					QueueCapacity:           current.QueueCapacity,
					BackpressureSubmissions: counterDelta(current.BackpressureSubmissions, baseline.BackpressureSubmissions),
					ReadyQueueWaitNanos:     counterDelta(current.ReadyQueueWaitNanos, baseline.ReadyQueueWaitNanos),
					ReadySubmissions:        counterDelta(current.ReadySubmissions, baseline.ReadySubmissions),
				})
				baseline = current
			case <-stop:
				return
			}
		}
	}()
}

func (owner *rf3NodeOwner) stopMigrationPressureSampler() {
	if owner == nil {
		return
	}
	owner.pressureStopOnce.Do(func() {
		if owner.pressureStop != nil {
			close(owner.pressureStop)
			<-owner.pressureDone
		}
	})
}

func counterDelta(current, prior uint64) uint64 {
	if current < prior {
		// A restarted/replaced sequencer establishes a new baseline. Treating a
		// wrapped counter as a pressure spike would pause all migration work.
		return 0
	}
	return current - prior
}
