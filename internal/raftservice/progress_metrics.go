package raftservice

import (
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

// ProgressMetrics is the bounded process-incarnation counter seam for shipped
// RF3 owner loops. It observes already-produced fixed-width progress after a
// Host turn; no formatting, labels, clocks, maps, or callbacks enter Raft.
type ProgressMetrics struct {
	proposalCommands  atomic.Uint64
	proposalBytes     atomic.Uint64
	appliedEntries    atomic.Uint64
	readyPersisted    atomic.Uint64
	snapshotsFinished atomic.Uint64
	readCompletions   atomic.Uint64
	faults            atomic.Uint64
}

// ProgressMetricsSnapshot is a detached, consistent-enough counter cut. Every
// field is cumulative for one process incarnation.
type ProgressMetricsSnapshot struct {
	ProposalCommands  uint64
	ProposalBytes     uint64
	AppliedEntries    uint64
	ReadyPersisted    uint64
	SnapshotsFinished uint64
	ReadCompletions   uint64
	Faults            uint64
}

func (metrics *ProgressMetrics) observeProgress(progress multiraft.Progress, done bool, err error) {
	if metrics == nil {
		return
	}
	if progress.ProposalCount > 0 {
		metrics.proposalCommands.Add(uint64(progress.ProposalCount))
		metrics.proposalBytes.Add(uint64(progress.ProposalBytes))
	}
	if progress.AppliedCount > 0 {
		metrics.appliedEntries.Add(uint64(progress.AppliedCount))
	}
	switch progress.ReadyKind {
	case raftmember.DrivePersisted:
		metrics.readyPersisted.Add(1)
	case raftmember.DriveSnapshotFinished:
		metrics.snapshotsFinished.Add(1)
	case raftmember.DriveReadStatesFinished:
		if count := len(progress.ReadOutcomes); count != 0 {
			metrics.readCompletions.Add(uint64(count))
		}
	}
	if err != nil && done {
		metrics.faults.Add(1)
	}
}

func (metrics *ProgressMetrics) Snapshot() ProgressMetricsSnapshot {
	if metrics == nil {
		return ProgressMetricsSnapshot{}
	}
	return ProgressMetricsSnapshot{
		ProposalCommands:  metrics.proposalCommands.Load(),
		ProposalBytes:     metrics.proposalBytes.Load(),
		AppliedEntries:    metrics.appliedEntries.Load(),
		ReadyPersisted:    metrics.readyPersisted.Load(),
		SnapshotsFinished: metrics.snapshotsFinished.Load(),
		ReadCompletions:   metrics.readCompletions.Load(),
		Faults:            metrics.faults.Load(),
	}
}
