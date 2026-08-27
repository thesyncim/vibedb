package raftmember

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
	var driverErr error
	if err := fixture.runtime.ConfigureWALGeneration(WALGenerationDriverOptions{
		IntervalTicks: 1,
		Key:           fixture.walKey,
		OnError:       func(err error) { driverErr = err },
	}); err != nil {
		t.Fatal(err)
	}

	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	for sequence := uint64(2); sequence <= 3; sequence++ {
		beforeApplied := fixture.apply.Applied()
		key, document := generationDriverMutation(t, sequence)
		command := testApplyCommand(
			fixture.base, epoch, sequence, key, document,
		)
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatal(err)
		}
		drainRuntime(t, fixture.runtime, nil)
		if fixture.apply.Applied() != beforeApplied+1 {
			t.Fatalf("sequence %d apply index=%d, want %d", sequence, fixture.apply.Applied(), beforeApplied+1)
		}
		// Inspect the journal-only cut before any completion snapshot. A lookup
		// can materialize system parents and legitimately checkpoint the group,
		// which would erase the exact compaction-stall precondition under test.
		if sequence == 3 && fixture.apply.CheckpointAppliedIndex() != beforeApplied {
			t.Fatalf("test requires a journal-durable suffix above the previous generation: checkpoint=%d previous=%d applied=%d",
				fixture.apply.CheckpointAppliedIndex(), beforeApplied, fixture.apply.Applied())
		}
		info, err := awaitWALGeneration(t, fixture.runtime, fixture.wal, sequence-1)
		if err != nil || info.Generation != sequence-1 || info.BaseIndex != beforeApplied+1 {
			t.Fatalf("generation after sequence %d = %+v, %v; driver=%v", sequence, info, err, driverErr)
		}
		lookup, lookupErr := fixture.apply.LookupCompletion(command)
		if lookupErr != nil {
			t.Fatal(lookupErr)
		}
		completion, completionErr := replication.OpenCompletion(lookup.Bytes)
		if completionErr != nil || completion.ResultCode != replicatedstate.ResultApplied ||
			completion.AppliedSequence != beforeApplied+1 || fixture.apply.Applied() != beforeApplied+1 {
			t.Fatalf("sequence %d did not durably apply: %+v err=%v applied=%d", sequence, completion, completionErr, fixture.apply.Applied())
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

func generationDriverMutation(t testing.TB, sequence uint64) ([]byte, []byte) {
	t.Helper()
	id := byte('a' + sequence - 2)
	key, ok := orderedkey.AppendJSONString(nil, []byte{'"', id, '"'}, orderedkey.Ascending)
	if !ok {
		t.Fatal("invalid JSON primary key")
	}
	document := []byte(`{"id":"a","value":1}`)
	document[7] = id
	return key, document
}

func TestGenerationDriverMutationPreflight(t *testing.T) {
	for sequence := uint64(2); sequence <= 3; sequence++ {
		key, document := generationDriverMutation(t, sequence)
		if len(key) == 0 || document[7] != byte('a'+sequence-2) {
			t.Fatalf("invalid generation fixture: key=%x document=%s", key, document)
		}
	}
}

func awaitWALGeneration(
	t testing.TB, runtimeOwner *Runtime, wal *raftstore.Store, generation uint64,
) (raftstore.GenerationInfo, error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		runtimeOwner.tickWALGeneration()
		info, err := wal.GenerationInfo()
		if err == nil && info.Generation == generation {
			return info, nil
		}
		runtime.Gosched()
	}
	return wal.GenerationInfo()
}

func TestRuntimeWALGenerationBuildDoesNotBlockRaftProgress(t *testing.T) {
	fixture := newRuntimeFixture(t, 249, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.ConfigureWALGeneration(WALGenerationDriverOptions{
		IntervalTicks: 1, Key: fixture.walKey,
	}); err != nil {
		t.Fatal(err)
	}
	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	firstKey, _ := orderedkey.AppendJSONString(nil, []byte(`"first"`), orderedkey.Ascending)
	first := testApplyCommand(fixture.base, epoch, 2, firstKey, []byte(`{"id":"first"}`))
	if err := fixture.runtime.Propose(first); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	previousBuild := walGenerationBuild
	var releaseOnce sync.Once
	walGenerationBuild = func(builder *raftstore.GenerationBuilder) error {
		close(started)
		<-release
		err := previousBuild(builder)
		close(finished)
		return err
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		fixture.runtime.walGeneration.stopAndWait()
		walGenerationBuild = previousBuild
	})
	fixture.runtime.tickWALGeneration()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("background generation build did not start")
	}
	if err := fixture.runtime.Tick(); err != nil {
		t.Fatalf("Tick blocked or failed during generation build: %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)

	secondKey, _ := orderedkey.AppendJSONString(nil, []byte(`"second"`), orderedkey.Ascending)
	second := testApplyCommand(fixture.base, epoch, 3, secondKey, []byte(`{"id":"second"}`))
	if err := fixture.runtime.Propose(second); err != nil {
		t.Fatalf("proposal blocked by generation build: %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)
	if publication := fixture.apply.Published(); publication.Applied < 5 {
		t.Fatalf("apply did not progress while build blocked: %+v", publication)
	}
	select {
	case <-finished:
		t.Fatal("generation build was not held by the test barrier")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("background generation build did not finish")
	}
	// The intervening apply stales the candidate. Owner-lane revalidation must
	// discard it without selecting or deleting the serving source.
	sourceFile, err := os.Stat(fixture.walPath)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for fixture.runtime.walGeneration.building && time.Now().Before(deadline) {
		fixture.runtime.tickWALGeneration()
		runtime.Gosched()
	}
	if fixture.runtime.walGeneration.building || fixture.runtime.walGeneration.activationPending {
		t.Fatal("stale candidate was not consumed and discarded")
	}
	if _, err := fixture.wal.GenerationInfo(); !errors.Is(err, raftstore.ErrGenerationCandidate) {
		t.Fatalf("stale off-lane build was published: %v", err)
	}
	afterFile, err := os.Stat(fixture.walPath)
	if err != nil || !os.SameFile(sourceFile, afterFile) {
		t.Fatalf("stale build replaced source inode: %v", err)
	}
	hard, _, err := fixture.wal.InitialState()
	if err != nil || hard.GetCommit() < fixture.apply.Published().Applied {
		t.Fatalf("source stopped serving acknowledged apply: hard=%v err=%v", hard, err)
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
