package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
)

func TestRF3DiagnosticCanaryCountersMapExactSnapshots(t *testing.T) {
	snapshot := rf3DiagnosticSnapshot{
		ReadyWaveHistogram:              make([]uint64, raftstore.MaxPersistGroupBatches+1),
		ReadySeriesHistogram:            make([]uint64, raftstore.MaxReadySeries+1),
		ReadyDurableHistogram:           make([]uint64, raftstore.MaxReadySeries+1),
		RaftProposalQueueDepthHistogram: make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalEntriesPerReady:     make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalBytesPerReady:       make([]uint64, raftservice.ProposalBytesHistogramBuckets),
	}
	applyRF3DiagnosticProgress(&snapshot, raftservice.ProgressMetricsSnapshot{
		ProposalBatches:             1,
		ProposalCommands:            2,
		ProposalBytes:               3,
		ApplyBatches:                4,
		AppliedEntries:              5,
		CommitAdvancements:          6,
		CommittedEntries:            7,
		ReadyPersisted:              8,
		ProposalWindowQueued:        9,
		LateJoinUsed:                10,
		LateJoinMissed:              11,
		LateJoinEntries:             12,
		ProposalQueueDepthHistogram: [raftservice.ProposalEntryHistogramBuckets]uint64{2: 13},
		ProposalEntriesPerReady:     [raftservice.ProposalEntryHistogramBuckets]uint64{2: 14},
		ProposalBytesPerReady:       [raftservice.ProposalBytesHistogramBuckets]uint64{1: 15},
	})
	if snapshot.RaftProposalBatches != 1 || snapshot.RaftProposalCommands != 2 ||
		snapshot.RaftProposalBytes != 3 || snapshot.RaftApplyBatches != 4 ||
		snapshot.RaftAppliedEntries != 5 || snapshot.RaftCommitAdvancements != 6 ||
		snapshot.RaftCommittedEntries != 7 || snapshot.RaftReadyPersisted != 8 ||
		snapshot.RaftProposalWindowQueued != 9 || snapshot.RaftLateJoinUsed != 10 ||
		snapshot.RaftLateJoinMissed != 11 || snapshot.RaftLateJoinEntries != 12 ||
		snapshot.RaftProposalQueueDepthHistogram[2] != 13 ||
		snapshot.RaftProposalEntriesPerReady[2] != 14 ||
		snapshot.RaftProposalBytesPerReady[1] != 15 {
		t.Fatalf("progress counters were not copied exactly: %+v", snapshot)
	}

	applyRF3DiagnosticSequencer(&snapshot, raftstore.NodeSubmissionSequencerStats{
		ReadySubmissions:                11,
		ReadyQueueWaitNanos:             12,
		ReadyWavesAttempted:             13,
		ReadyPersistAttempts:            14,
		ReadyPersistSuccesses:           15,
		ReadyPersistFailures:            16,
		ReadyWavesFailed:                17,
		ReadyPersistDurationNanos:       18,
		ReadyWaveDurationNanos:          19,
		ReadyLogicalBatches:             20,
		ReadySeriesSubmissions:          21,
		ReadySingletonSeriesSubmissions: 22,
		ReadyMultiSeriesSubmissions:     23,
		ReadySeriesHistogram:            [raftstore.MaxReadySeries + 1]uint64{2: 24},
		ReadyDurableLogicalBatches:      25,
		ReadyDurableSeriesSubmissions:   26,
		ReadyDurableSeriesHistogram:     [raftstore.MaxReadySeries + 1]uint64{2: 27},
	})
	if snapshot.ReadySubmissions != 11 || snapshot.ReadyQueueWaitNs != 12 ||
		snapshot.ReadyWavesAttempted != 13 || snapshot.ReadyPersistAttempts != 14 ||
		snapshot.ReadyPersistSuccesses != 15 || snapshot.ReadyPersistFailures != 16 ||
		snapshot.ReadyWavesFailed != 17 || snapshot.ReadyPersistDurationNs != 18 ||
		snapshot.ReadyWaveDurationNs != 19 || snapshot.ReadyLogicalBatches != 20 ||
		snapshot.ReadySeriesSubmissions != 21 || snapshot.ReadySingletonSeries != 22 ||
		snapshot.ReadyMultiSeries != 23 || snapshot.ReadySeriesHistogram[2] != 24 ||
		snapshot.ReadyDurableLogical != 25 || snapshot.ReadyDurableSeries != 26 ||
		snapshot.ReadyDurableHistogram[2] != 27 {
		t.Fatalf("sequencer counters were not copied exactly: %+v", snapshot)
	}

	applyRF3DiagnosticProgress(nil, raftservice.ProgressMetricsSnapshot{})
	applyRF3DiagnosticSequencer(nil, raftstore.NodeSubmissionSequencerStats{})
}
