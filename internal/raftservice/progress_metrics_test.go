package raftservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

func TestProgressMetricsCountsExistingOwnerSeamsExactly(t *testing.T) {
	metrics := new(ProgressMetrics)
	metrics.observeProgress(multiraft.Progress{
		Kind: multiraft.ProgressProposal, ProposalCount: 3, ProposalBytes: 19,
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{
		ReadyKind: raftmember.DrivePersisted, AppliedCount: 2,
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{
		ReadyKind: raftmember.DriveSnapshotFinished,
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{
		ReadyKind:    raftmember.DriveReadStatesFinished,
		ReadOutcomes: make([]raftmodel.ReadOutcome, 2),
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{Kind: multiraft.ProgressFault}, true, errors.New("disk"))
	want := ProgressMetricsSnapshot{ProposalCommands: 3, ProposalBytes: 19,
		AppliedEntries: 2, ReadyPersisted: 1, SnapshotsFinished: 1,
		ReadCompletions: 2, Faults: 1}
	if got := metrics.Snapshot(); got != want {
		t.Fatalf("metrics=%+v want=%+v", got, want)
	}
}

func BenchmarkProgressMetricsObserveProposal(b *testing.B) {
	metrics := new(ProgressMetrics)
	progress := multiraft.Progress{Kind: multiraft.ProgressProposal,
		ProposalCount: 1, ProposalBytes: 128}
	b.ReportAllocs()
	for b.Loop() {
		metrics.observeProgress(progress, true, nil)
	}
}
