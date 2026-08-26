package rebalanceexec

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type moveControllerJournal struct {
	records map[[32]byte]gateway.ReplicatedOperationRecord
	extra   map[[32]byte]gateway.ReplicatedOperationRecord
}

func (journal *moveControllerJournal) ids() [][32]byte {
	ids := make([][32]byte, 0, len(journal.records)+len(journal.extra))
	for id := range journal.records {
		ids = append(ids, id)
	}
	for id := range journal.extra {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool {
		return string(ids[left][:]) < string(ids[right][:])
	})
	return ids
}

func (journal *moveControllerJournal) ReadOperationIDs(context.Context) ([][32]byte, error) {
	return journal.ids(), nil
}

func (journal *moveControllerJournal) ReadOperation(
	_ context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	if record, ok := journal.records[id]; ok {
		return record, nil
	}
	if record, ok := journal.extra[id]; ok {
		return record, nil
	}
	return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
}

func (journal *moveControllerJournal) SubmitOperation(
	_ context.Context, record gateway.ReplicatedOperationRecord,
) error {
	if _, exists := journal.records[record.ID]; exists {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.records[record.ID] = record
	return nil
}

func (journal *moveControllerJournal) PublishOperation(
	_ context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	current, exists := journal.records[record.ID]
	if !exists || current.Revision != expected || record.Revision != expected+1 {
		return gateway.ErrReplicatedCatalogConflict
	}
	journal.records[record.ID] = record
	return nil
}

func (journal *moveControllerJournal) DeleteOperation(
	_ context.Context, id [32]byte, expected uint64,
) error {
	current, exists := journal.records[id]
	if !exists {
		return nil
	}
	if current.Revision != expected || current.State != gateway.ReplicatedOperationComplete {
		return gateway.ErrReplicatedCatalogConflict
	}
	delete(journal.records, id)
	return nil
}

func (*moveControllerJournal) RetryPending(context.Context) error { return nil }

func TestControllerDiscoversOnlyMovesAndResumesFromJournal(t *testing.T) {
	plan, fixture := newExecutorFixture(t)
	executor, err := New(Options{
		Routes: fixture, Grants: fixture, Membership: fixture, Snapshots: fixture,
		Bootstrap: fixture, Awaiter: fixture, Ownership: fixture, Catalog: fixture,
		Drainer: fixture, Retirer: fixture,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &controllerObserver{cut: rebalance.ReplicatedMoveCut{Observation: rebalance.Observation{
		Catalog: fixture.cut.Catalog,
		Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4,
			ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}},
		},
		LeaderStatus: leaderStatusForController(1, 5),
	}}}
	journal := &moveControllerJournal{
		records: make(map[[32]byte]gateway.ReplicatedOperationRecord),
		extra: map[[32]byte]gateway.ReplicatedOperationRecord{
			{1}: {ID: [32]byte{1}, Kind: gateway.ReplicatedOperationSplit},
		},
	}
	controller, err := NewController(journal, journal, observer, executor)
	if err != nil {
		t.Fatal(err)
	}
	action, err := controller.Submit(context.Background(), plan)
	if err != nil || action.Kind != rebalance.ActionAddLearner ||
		len(fixture.membershipRequests) != 1 {
		t.Fatalf("submit action=%+v membership=%d err=%v",
			action, len(fixture.membershipRequests), err)
	}
	observer.cut.Publication = raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6,
		ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}, Learners: []uint64{2}},
	}
	observer.cut.LeaderStatus.Commit = 6
	observer.cut.LeaderStatus.Applied = 6
	pass, err := controller.RunPass(context.Background())
	if err != nil || pass.Discovered != 2 || pass.Moves != 1 || pass.Advanced != 1 ||
		pass.Completed != 0 || len(fixture.snapshotRequests) != 1 {
		t.Fatalf("pass=%+v snapshots=%d err=%v", pass, len(fixture.snapshotRequests), err)
	}
}

func TestControllerRejectsIncompleteComposition(t *testing.T) {
	if _, err := NewController(nil, nil, nil, nil); !errors.Is(err, ErrControllerConfig) {
		t.Fatalf("NewController error = %v", err)
	}
	var controller *Controller
	if _, err := controller.RunPass(context.Background()); !errors.Is(err, ErrControllerConfig) {
		t.Fatalf("nil RunPass error = %v", err)
	}
}

func leaderStatusForController(member, applied uint64) raftmember.RuntimeStatus {
	return raftmember.RuntimeStatus{
		MemberID: member, LeaderID: member, Term: 3, Commit: applied, Applied: applied,
		RaftState: raft.StateLeader,
	}
}
