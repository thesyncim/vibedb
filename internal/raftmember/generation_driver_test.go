package raftmember

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRuntimeWALGenerationDriverRepeatedCompactionAndRestart(t *testing.T) {
	fixture := newRuntimeFixture(t, 247, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.ConfigureWALGeneration(WALGenerationDriverOptions{
		IntervalTicks: 1,
		Key:           fixture.walKey,
	}); err != nil {
		t.Fatal(err)
	}

	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	for sequence := uint64(2); sequence <= 3; sequence++ {
		key, _ := orderedkey.AppendJSONString(nil, []byte{byte('a' + sequence - 2)}, orderedkey.Ascending)
		command := testApplyCommand(
			fixture.base, epoch, sequence, key,
			[]byte(`{"id":"generation-driver","value":1}`),
		)
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatal(err)
		}
		drainRuntime(t, fixture.runtime, nil)
		fixture.runtime.tickWALGeneration()
		info, err := fixture.wal.GenerationInfo()
		if err != nil || info.Generation != sequence-1 {
			t.Fatalf("generation after sequence %d = %+v, %v", sequence, info, err)
		}
	}
	before, err := fixture.wal.GenerationInfo()
	if err != nil {
		t.Fatal(err)
	}
	// An idle cadence must not replace the active inode or mint a generation.
	fixture.runtime.tickWALGeneration()
	after, err := fixture.wal.GenerationInfo()
	if err != nil || after != before {
		t.Fatalf("idle cadence changed generation: before=%+v after=%+v err=%v", before, after, err)
	}

	if err := fixture.runtime.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedWAL, err := raftstore.Open(
		fixture.walPath, fixture.walID, testTopologyRecoveryEpoch,
		fixture.walKey, fixture.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	reopenedDB, reopenedApply, err := OpenBoundSQLWithApplyRecoveringGeneration(
		fixture.sqlPath, reopenedWAL, testAuthorityProfile(), fixture.base, fixture.applyID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopenedApply.Applied(); got < 4 {
		t.Fatalf("restarted apply index = %d, want compacted commands", got)
	}
	if info, err := reopenedWAL.GenerationInfo(); err != nil || info != before {
		t.Fatalf("restarted generation = %+v, want %+v, err=%v", info, before, err)
	}
	if err := reopenedApply.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopenedDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopenedWAL.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveringOpenCommitsSelectedGenerationBeforeDeletingSource(t *testing.T) {
	identity := testWALIdentity(248)
	walPath, wal, key, options := createWAL(t, identity)
	authority := testAuthorityProfile()
	sqlPath, database, _ := prepareSQLRoot(t, identity, "generation-driver-recovery")
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind generation recovery SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	apply, applyID, err := OpenPreparedApply(wal, database, authority, base, testApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	index, term := uint64(2), uint64(2)
	entryType := pb.EntryNormal
	if err := wal.Persist(raftmodel.PersistBatch{
		NodeIncarnation: incarnation,
		ReadyID:         1,
		HardState:       &pb.HardState{Term: &term, Commit: &index},
		Entries: []*pb.Entry{{
			Term: &term, Index: &index, Type: &entryType,
		}},
		MustSync: true,
	}); err != nil {
		t.Fatal(err)
	}
	preparation, err := apply.CaptureWALBase(sqldriver.WALBaseCaptureOptions{
		Workspace: make([]byte, 0, replicatedstate.MaxSnapshotArtifactChunkBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := PrepareWALGeneration(wal, apply, preparation, key)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishWALGeneration(wal, apply, preparation, builder); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(candidate.Path); err != nil {
		t.Fatalf("selected candidate disappeared before settlement: %v", err)
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := apply.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	selected, err := raftstore.Open(walPath, identity, testTopologyRecoveryEpoch, key, options)
	if err != nil {
		t.Fatal(err)
	}
	recoveredDB, recoveredApply, err := OpenBoundSQLWithApplyRecoveringGeneration(
		sqlPath, selected, authority, base, applyID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(candidate.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate sibling remains after durable settlement: %v", err)
	}
	if _, _, err := selected.InitialState(); err != nil {
		t.Fatalf("recovered generation remains fenced: %v", err)
	}
	_ = recoveredApply.Close()
	_ = recoveredDB.Close()
	_ = selected.Close()
}
