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
		CommitAdvancements: 1, CommittedEntries: 4,
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{
		ReadyKind: raftmember.DriveSnapshotFinished,
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{
		ReadyKind:    raftmember.DriveReadStatesFinished,
		ReadOutcomes: make([]raftmodel.ReadOutcome, 2),
	}, true, nil)
	metrics.observeProgress(multiraft.Progress{Kind: multiraft.ProgressFault}, true, errors.New("disk"))
	want := ProgressMetricsSnapshot{ProposalBatches: 1, ProposalCommands: 3, ProposalBytes: 19,
		ApplyBatches: 1, AppliedEntries: 2, ReadyPersisted: 1, SnapshotsFinished: 1,
		CommitAdvancements: 1, CommittedEntries: 4,
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

func TestProgressMetricsGroupDirectoryIsExactAndBounded(t *testing.T) {
	groupA := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	groupB := groupA
	groupB.GroupID[0] = 6
	metrics := new(ProgressMetrics)
	if err := metrics.ConfigureGroups([]raftmember.RuntimeIdentity{
		{Group: groupA, MemberID: 7}, {Group: groupB, MemberID: 8},
	}); err != nil {
		t.Fatal(err)
	}
	metrics.observeProgress(multiraft.Progress{Group: groupA, ProposalCount: 2, ProposalBytes: 11,
		CommitAdvancements: 3, CommittedEntries: 7}, true, nil)
	identity, got, found := metrics.GroupProgressMetrics(groupA)
	if !found || identity.MemberID != 7 || got.ProposalCommands != 2 || got.ProposalBytes != 11 ||
		got.CommitAdvancements != 3 || got.CommittedEntries != 7 {
		t.Fatalf("identity=%+v metrics=%+v found=%v", identity, got, found)
	}
	if _, got, found = metrics.GroupProgressMetrics(groupB); !found || got != (ProgressMetricsSnapshot{}) {
		t.Fatalf("unobserved group metrics=%+v found=%v", got, found)
	}
	unknown := groupA
	unknown.GroupID[0] = 9
	if _, _, found = metrics.GroupProgressMetrics(unknown); found {
		t.Fatal("unknown group found")
	}
	if err := metrics.ConfigureGroups([]raftmember.RuntimeIdentity{{Group: groupA, MemberID: 9}}); err == nil {
		t.Fatal("reconfiguration accepted")
	}
}

func BenchmarkProgressMetricsObserveConfiguredGroup(b *testing.B) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	metrics := new(ProgressMetrics)
	if err := metrics.ConfigureGroups([]raftmember.RuntimeIdentity{{Group: group, MemberID: 7}}); err != nil {
		b.Fatal(err)
	}
	progress := multiraft.Progress{Group: group, Kind: multiraft.ProgressProposal,
		ProposalCount: 1, ProposalBytes: 128}
	b.ReportAllocs()
	for b.Loop() {
		metrics.observeProgress(progress, true, nil)
	}
}
