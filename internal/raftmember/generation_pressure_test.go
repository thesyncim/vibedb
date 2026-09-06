package raftmember

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestRuntimeWALPressureCompactsBeforePeriodicMaintenance(t *testing.T) {
	options := testWALOptions()
	options.MaxRecords = 16
	options.MaxFileBytes = 256 << 20
	options.MaxLiveBytes = 2 * raftstore.MinimumReadyLiveBytes
	fixture := newRuntimeFixtureWithOptions(t, 245, nil, options)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.ConfigureWALGeneration(WALGenerationDriverOptions{
		IntervalTicks: 12000, Key: fixture.walKey,
	}); err != nil {
		t.Fatal(err)
	}
	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	key, document := generationDriverMutation(t, 2)
	command := testApplyCommand(fixture.base, epoch, 2, key, document)
	// Exact retries still consume Raft log records, as catalog control traffic
	// does. Exhaust several generations before even one periodic tick fires.
	for index := range 50 {
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatalf("proposal %d exhausted WAL before maintenance: %v", index, err)
		}
		drainRuntime(t, fixture.runtime, nil)
	}
	info, err := fixture.wal.GenerationInfo()
	if err != nil || info.Generation < 2 {
		t.Fatalf("pressure did not advance generations: %+v err=%v", info, err)
	}
	if err := fixture.runtime.Tick(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
}

func TestRuntimeWALPressureRecoveryAfterRestart(t *testing.T) {
	options := testWALOptions()
	options.MaxRecords = 16
	options.MaxFileBytes = 256 << 20
	options.MaxLiveBytes = 2 * raftstore.MinimumReadyLiveBytes
	fixture := newRuntimeFixtureWithOptions(t, 244, nil, options)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	key, document := generationDriverMutation(t, 2)
	command := testApplyCommand(fixture.base, epoch, 2, key, document)
	for index := 0; ; index++ {
		if err := fixture.wal.ReserveReady(); errors.Is(err, raftstore.ErrFull) {
			break
		} else if err != nil || index == 50 {
			t.Fatalf("failed to create bounded full WAL: index=%d err=%v", index, err)
		}
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatal(err)
		}
		drainRuntime(t, fixture.runtime, nil)
	}
	before := fixture.apply.Applied()
	if err := fixture.runtime.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := raftstore.Open(fixture.walPath, fixture.walID, testTopologyRecoveryEpoch, fixture.walKey, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	db, apply, err := OpenBoundSQLWithApplyRecoveringGeneration(fixture.sqlPath, wal, testAuthorityProfile(), fixture.base, fixture.applyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = apply.Close(); _ = db.Close() })
	owner, err := AdoptRuntime(wal, db, apply)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := owner.ConfigureWALGeneration(WALGenerationDriverOptions{IntervalTicks: 12000, Key: fixture.walKey}); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, owner, nil)
	if err := owner.Tick(); err != nil {
		t.Fatalf("full WAL could not restart before maintenance: %v", err)
	}
	drainRuntime(t, owner, nil)
	if err := wal.ReserveReady(); err != nil {
		t.Fatalf("restarted WAL has no headroom: %v", err)
	}
	if apply.Applied() != before {
		t.Fatalf("compaction changed applied index: before=%d after=%d", before, apply.Applied())
	}
	read, err := apply.PointReadInto(1, key, before, replication.MaxMutationValueBytes, nil)
	if err != nil || !read.Found || !bytes.Equal(read.Value, document) {
		t.Fatalf("recovered document=%q found=%v err=%v", read.Value, read.Found, err)
	}
	lookup, err := apply.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("recovered completion=%+v err=%v", completion, err)
	}
}

func TestPipelinedWALPressureDrainsUncapturedReady(t *testing.T) {
	options := testWALOptions()
	options.MaxRecords = 16
	options.MaxFileBytes = 256 << 20
	options.MaxLiveBytes = 2 * raftstore.MinimumReadyLiveBytes
	fixture := newRuntimeFixtureWithPipeline(t, 243, nil, options, true)
	drain := func() {
		t.Helper()
		var workspace ReadyWorkspace
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if fixture.runtime.walGenerationQuiescent() {
				return
			}
			result, err := fixture.runtime.DriveReady(&workspace, func(OutboundMessage) error { return nil }, settleTestApplied)
			if err != nil && !errors.Is(err, raftstore.ErrFull) {
				t.Fatalf("pipelined pressure drain: %v", err)
			}
			if fixture.runtime.walGenerationQuiescent() {
				return
			}
			if !result.Progressed() {
				time.Sleep(100 * time.Microsecond)
			}
		}
		t.Fatal("pipelined pressure drain stalled")
	}
	drain()
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drain()
	open := testApplySessionOpen(fixture.base)
	if err := fixture.runtime.Propose(open); err != nil {
		t.Fatal(err)
	}
	drain()
	lookup, err := fixture.apply.LookupCompletion(open)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, document := generationDriverMutation(t, 2)
	command := testApplyCommand(fixture.base, completion.ClientEpoch, 2, key, document)
	// Fill the WAL with maintenance disabled, stopping at the settled cut
	// before an idle DriveReady can run admission maintenance.
	for i := 0; ; i++ {
		if err := fixture.wal.ReserveReady(); errors.Is(err, raftstore.ErrFull) {
			break
		} else if err != nil || i == 50 {
			t.Fatalf("fill WAL: i=%d err=%v", i, err)
		}
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatal(err)
		}
		drain()
	}
	if err := fixture.runtime.ConfigureWALGeneration(WALGenerationDriverOptions{IntervalTicks: 12000, Key: fixture.walKey}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatal(err)
	}
	ready, err := fixture.runtime.node.HasReady()
	if err != nil || !ready || !fixture.runtime.pipelined.quiescent() {
		t.Fatalf("expected uncaptured Ready at settled full WAL: ready=%t err=%v", ready, err)
	}
	fixture.runtime.pipelined.admission = 0
	drain()
	for i := 0; i < 50; i++ {
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatalf("proposal %d: %v", i, err)
		}
		drain()
	}
	info, err := fixture.wal.GenerationInfo()
	if err != nil || info.Generation < 2 {
		t.Fatalf("generation=%+v err=%v", info, err)
	}
}
