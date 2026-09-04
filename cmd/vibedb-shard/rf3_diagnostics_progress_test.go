package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
)

func TestRF3DiagnosticCanaryCountersMapExactSnapshots(t *testing.T) {
	var snapshot rf3DiagnosticSnapshot
	applyRF3DiagnosticProgress(&snapshot, raftservice.ProgressMetricsSnapshot{
		ProposalBatches:    1,
		ProposalCommands:   2,
		ProposalBytes:      3,
		ApplyBatches:       4,
		AppliedEntries:     5,
		CommitAdvancements: 6,
		CommittedEntries:   7,
		ReadyPersisted:     8,
	})
	if snapshot.RaftProposalBatches != 1 || snapshot.RaftProposalCommands != 2 ||
		snapshot.RaftProposalBytes != 3 || snapshot.RaftApplyBatches != 4 ||
		snapshot.RaftAppliedEntries != 5 || snapshot.RaftCommitAdvancements != 6 ||
		snapshot.RaftCommittedEntries != 7 || snapshot.RaftReadyPersisted != 8 {
		t.Fatalf("progress counters were not copied exactly: %+v", snapshot)
	}

	applyRF3DiagnosticSequencer(&snapshot, raftstore.NodeSubmissionSequencerStats{
		ReadySubmissions:          11,
		ReadyQueueWaitNanos:       12,
		ReadyWavesAttempted:       13,
		ReadyPersistAttempts:      14,
		ReadyPersistSuccesses:     15,
		ReadyPersistFailures:      16,
		ReadyWavesFailed:          17,
		ReadyPersistDurationNanos: 18,
		ReadyWaveDurationNanos:    19,
	})
	if snapshot.ReadySubmissions != 11 || snapshot.ReadyQueueWaitNs != 12 ||
		snapshot.ReadyWavesAttempted != 13 || snapshot.ReadyPersistAttempts != 14 ||
		snapshot.ReadyPersistSuccesses != 15 || snapshot.ReadyPersistFailures != 16 ||
		snapshot.ReadyWavesFailed != 17 || snapshot.ReadyPersistDurationNs != 18 ||
		snapshot.ReadyWaveDurationNs != 19 {
		t.Fatalf("sequencer counters were not copied exactly: %+v", snapshot)
	}

	applyRF3DiagnosticProgress(nil, raftservice.ProgressMetricsSnapshot{})
	applyRF3DiagnosticSequencer(nil, raftstore.NodeSubmissionSequencerStats{})
}
