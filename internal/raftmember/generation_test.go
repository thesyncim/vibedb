package raftmember

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestWALGenerationActivationSurvivesEveryRestartBoundary(t *testing.T) {
	identity := testWALIdentity(252)
	walPath, wal, key, options := createWAL(t, identity)
	authority := testAuthorityProfile()
	sqlPath, database, _ := prepareSQLRoot(t, identity, "wal-generation")
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind generation SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	apply, applyIdentity, err := OpenPreparedApply(
		wal, database, authority, base, testApplyOptions(),
	)
	skipIfStrictAllocationUnsupported(t, "open generation apply", err)
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
	index, term, entryType := uint64(2), uint64(2), pb.EntryNormal
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
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishWALGeneration(wal, apply, preparation, builder); err != nil {
		t.Fatal(err)
	}
	if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, nil); !errors.Is(err, sqldriver.ErrReplicatedApplyBusy) {
		t.Fatalf("committed suffix crossed selected base fence: %v", err)
	}
	if _, _, err := wal.InitialState(); !errors.Is(err, raftstore.ErrGenerationActivationPending) {
		t.Fatalf("selected source remained serving: %v", err)
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

	selected, err := raftstore.Open(
		walPath, identity, testTopologyRecoveryEpoch, key, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if ordinary, ordinaryApply, err := OpenBoundSQLWithApply(
		sqlPath, selected, authority, base, applyIdentity,
	); ordinary != nil || ordinaryApply != nil || !errors.Is(err, ErrWALUnavailable) {
		t.Fatalf("ordinary open crossed pending generation = %v/%v/%v",
			ordinary, ordinaryApply, err)
	}
	recoveredDB, recoveredApply, err := OpenBoundSQLWithApplyForGenerationActivation(
		sqlPath, selected, authority, base, applyIdentity,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveredApply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, nil); !errors.Is(err, sqldriver.ErrReplicatedApplyBusy) {
		t.Fatalf("special activation open served before WAL commit: %v", err)
	}
	recoveredApply.CompleteGenerationActivation(raftstore.GenerationActivationCompletion{})
	if _, err := recoveredApply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, nil); !errors.Is(err, sqldriver.ErrReplicatedApplyBusy) {
		t.Fatalf("zero completion released activation fence: %v", err)
	}
	if err := selected.CommitGenerationSelection(recoveredApply); err != nil {
		t.Fatalf("settle selected generation: %v", err)
	}
	publication, err := recoveredApply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, nil)
	if err != nil || publication.Applied != 2 {
		t.Fatalf("replay retained suffix after settlement = %+v, %v", publication, err)
	}
	if _, _, err := selected.InitialState(); err != nil {
		t.Fatalf("settled generation remained fenced: %v", err)
	}
	activeInfo, err := selected.GenerationInfo()
	if err != nil || activeInfo.BindingDigest != candidate.Info.BindingDigest ||
		activeInfo.SourceCutDigest != candidate.Info.SourceCutDigest {
		t.Fatalf("active generation = %+v, candidate = %+v, err=%v",
			activeInfo, candidate.Info, err)
	}
	if err := recoveredApply.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recoveredDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(candidate.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("activated candidate sibling survived replacement: %v", err)
	}

	reopenedWAL, err := raftstore.Open(
		walPath, identity, testTopologyRecoveryEpoch, key, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	reopenedDB, reopenedApply, err := OpenBoundSQLWithApply(
		sqlPath, reopenedWAL, authority, base, applyIdentity,
	)
	if err != nil {
		t.Fatalf("ordinary open after settlement: %v", err)
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

func TestWALGenerationCompletionKeepsOpenApplyLive(t *testing.T) {
	identity := testWALIdentity(254)
	walPath, wal, key, options := createWAL(t, identity)
	authority := testAuthorityProfile()
	_, database, _ := prepareSQLRoot(t, identity, "wal-generation-live-apply")
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind live generation SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	apply, _, err := OpenPreparedApply(
		wal, database, authority, base, testApplyOptions(),
	)
	skipIfStrictAllocationUnsupported(t, "open live generation apply", err)
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
	index, term, entryType := uint64(2), uint64(2), pb.EntryNormal
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
	defer builder.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if err := PublishWALGeneration(wal, apply, preparation, builder); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	selected, err := raftstore.Open(
		walPath, identity, testTopologyRecoveryEpoch, key, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	apply.CompleteGenerationActivation(raftstore.GenerationActivationCompletion{})
	if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, nil); !errors.Is(err, sqldriver.ErrReplicatedApplyBusy) {
		t.Fatalf("zero completion released live apply fence: %v", err)
	}
	if err := selected.CommitGenerationSelection(apply); err != nil {
		t.Fatalf("settle generation with live apply: %v", err)
	}
	publication, err := apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, nil)
	if err != nil || publication.Applied != 2 {
		t.Fatalf("live apply remained fenced = %+v, %v", publication, err)
	}
	if err := apply.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareWALGenerationRejectsCrossShardApplyBeforeBuild(t *testing.T) {
	authority := testAuthorityProfile()
	firstIdentity := testWALIdentity(253)
	_, firstWAL, firstKey, _ := createWAL(t, firstIdentity)
	_, firstDatabase, _ := prepareSQLRoot(t, firstIdentity, "generation-binding-first")
	firstBase, err := BindPreparedSQL(firstWAL, firstDatabase, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind first generation root", err)
	if err != nil {
		t.Fatal(err)
	}
	firstApply, _, err := OpenPreparedApply(
		firstWAL, firstDatabase, authority, firstBase, testApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstBootstrap, err := firstWAL.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstApply.InstallSnapshot(firstBootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err := firstWAL.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}

	secondIdentity := firstIdentity
	secondIdentity.Shard = "8000-ffff"
	secondIdentity.AllocationGeneration++
	secondIdentity.ShardIncarnation[0] ^= 0x80
	secondIdentity.GroupID[0] ^= 0x40
	secondIdentity.StoreID[0] ^= 0x20
	_, secondWAL, _, _ := createWAL(t, secondIdentity)
	_, secondDatabase, _ := prepareSQLRoot(t, secondIdentity, "generation-binding-second")
	secondBase, err := BindPreparedSQL(secondWAL, secondDatabase, authority, "docs")
	if err != nil {
		t.Fatal(err)
	}
	secondApply, _, err := OpenPreparedApply(
		secondWAL, secondDatabase, authority, secondBase, testApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	secondBootstrap, err := secondWAL.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secondApply.InstallSnapshot(secondBootstrap); err != nil {
		t.Fatal(err)
	}
	preparation, err := secondApply.CaptureWALBase(sqldriver.WALBaseCaptureOptions{
		Workspace: make([]byte, 0, replicatedstate.MaxSnapshotArtifactChunkBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	if builder, err := PrepareWALGeneration(
		firstWAL, secondApply, preparation, firstKey,
	); builder != nil || !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("cross-shard generation prepare = %v, %v", builder, err)
	}
}
