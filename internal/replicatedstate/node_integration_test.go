package replicatedstate

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftsim"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

func driveReplicatedStateNode(t testing.TB, node *raftmodel.Node) {
	t.Helper()
	var workspace raftmodel.NormalApplyBatchWorkspace
	for {
		has, err := node.HasReady()
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			return
		}
		captured, err := node.CaptureReady()
		if err != nil || !captured {
			t.Fatalf("CaptureReady = %v,%v", captured, err)
		}
		if err := node.PersistReady(); err != nil {
			t.Fatalf("PersistReady: %v", err)
		}
		if err := node.DrainMessages(func(*pb.Message) error { return nil }); err != nil {
			t.Fatalf("DrainMessages: %v", err)
		}
		if err := node.InstallSnapshot(); err != nil {
			t.Fatalf("InstallSnapshot: %v", err)
		}
		if err := node.ApplyCommitted(
			&workspace, func(raftmodel.AppliedNormalBatch) error { return nil },
		); err != nil {
			t.Fatalf("ApplyCommitted: %v", err)
		}
		if _, err := node.RecordReadStates(); err != nil {
			t.Fatalf("RecordReadStates: %v", err)
		}
		if err := node.AdvanceReady(); err != nil {
			t.Fatalf("AdvanceReady: %v", err)
		}
	}
}

func TestRaftModelNodeRestartUsesMachineAppliedWatermark(t *testing.T) {
	fixture := newMachineFixture(t)
	stable, err := raftsim.NewMemoryStore([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := stable.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	machine, err := Open(
		fixture.binding, bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	node, err := raftmodel.NewNode(1, 1, stable, machine)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Campaign(); err != nil {
		t.Fatal(err)
	}
	driveReplicatedStateNode(t, node)
	prototype := commandValue(fixture.binding, 1)
	open := encodeCommand(t, sessionOpenFor(prototype))
	if err := node.Propose(open); err != nil {
		t.Fatal(err)
	}
	driveReplicatedStateNode(t, node)
	openLookup, err := machine.LookupCompletion(open)
	if err != nil {
		t.Fatal(err)
	}
	openCompletion, err := replication.OpenCompletion(openLookup.Bytes)
	if err != nil || openCompletion.ResultCode != ResultSessionOpened ||
		openCompletion.ClientEpoch != openLookup.AppliedSequence ||
		machine.state.SessionEpochHighWater != openCompletion.ClientEpoch {
		t.Fatalf("node session open = %+v lookup=%+v state=%+v err=%v",
			openCompletion, openLookup, machine.state, err)
	}
	prototype.ClientEpoch = openCompletion.ClientEpoch
	prototype.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
		Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`),
	}}}}
	command := encodeCommand(t, prototype)
	if err := node.Propose(command); err != nil {
		t.Fatal(err)
	}
	driveReplicatedStateNode(t, node)
	before := machine.Published()
	if before.Applied != openCompletion.ClientEpoch+1 || machine.state.SessionCount != 1 ||
		machine.state.SessionSlotCount != 2 ||
		machine.state.SessionEpochHighWater != openCompletion.ClientEpoch {
		t.Fatalf("publication before restart=%+v state=%+v", before, machine.state)
	}
	first, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	firstCompletion, err := replication.OpenCompletion(first.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	firstRows, err := OpenMutationCompletionResult(
		firstCompletion.ResultCode, firstCompletion.InlineResult,
	)
	if err != nil || firstRows != 1 {
		t.Fatalf("pre-restart affected rows=%d err=%v", firstRows, err)
	}

	reopened, err := Open(
		fixture.binding, bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := raftmodel.NewNode(1, 2, stable, reopened)
	if err != nil {
		t.Fatal(err)
	}
	driveReplicatedStateNode(t, restarted)
	after := reopened.Published()
	if after.Applied != before.Applied || after.DataChainDigest != before.DataChainDigest ||
		reopened.state.SessionCount != 1 || reopened.state.SessionSlotCount != 2 ||
		reopened.state.SessionEpochHighWater != openCompletion.ClientEpoch {
		t.Fatalf("restart replayed publication: before=%+v after=%+v state=%+v", before, after, reopened.state)
	}
	second, err := reopened.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	if second.AppliedSequence != first.AppliedSequence || string(second.Bytes) != string(first.Bytes) {
		t.Fatalf("restart rewrote completion: before=%+v after=%+v", first, second)
	}
	secondCompletion, err := replication.OpenCompletion(second.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	secondRows, err := OpenMutationCompletionResult(
		secondCompletion.ResultCode, secondCompletion.InlineResult,
	)
	if err != nil || secondRows != firstRows {
		t.Fatalf("restart affected rows=%d, want %d, err=%v", secondRows, firstRows, err)
	}
}
