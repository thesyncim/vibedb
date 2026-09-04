package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	"go.etcd.io/raft/v3/raftpb"
)

func newDiagnosticsNodeStore(t *testing.T, groups int) *NodeStore {
	t.Helper()
	bootstraps := make([]NodeBootstrap, groups)
	for i := range bootstraps {
		group := uint64(i + 1)
		bootstraps[i] = NodeBootstrap{
			Descriptor: testGroupDescriptor(group),
			Snapshot:   nodeSnapshot(group, 1, 1),
		}
	}
	options := NodeStoreOptions{
		MaxWaveBytes: 1 << 20, MaxSegmentEvents: 256, RecentWaves: 64,
		MaxEntriesPerGroup: 64, ReaderSlots: 1,
	}
	store, err := CreateNodeStore(filepath.Join(t.TempDir(), "node"), testNodeIdentity(), testKey(), bootstraps, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	groupIDs := make([]uint64, groups)
	for i := range groupIDs {
		groupIDs[i] = uint64(i + 1)
	}
	if _, err := store.BeginIncarnations(groupIDs); err != nil {
		t.Fatal(err)
	}
	return store
}

func diagnosticsReady(t *testing.T, group, readyID uint64) *Submission {
	t.Helper()
	submission := preparedSubmission(t, group, readyID)
	submission.Ready.Batch.Entries = []*raftpb.Entry{typedEntry(2, 2, raftpb.EntryNormal, string([]byte{byte(group)}))}
	submission.Ready.Batch.HardState = hard(2, 2)
	return submission
}

func TestNodeSubmissionSequencerStatsObserveRealDurableWaves(t *testing.T) {
	store := newDiagnosticsNodeStore(t, 3)
	var syncCalls atomic.Uint64
	store.SetDataSyncForTesting(func(file *os.File) error {
		syncCalls.Add(1)
		return file.Sync()
	})
	entered, release := make(chan struct{}), make(chan struct{})
	var engineCalls atomic.Uint64
	store.persistWaveTest = func(wave seglog.Wave) error {
		if engineCalls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return store.engine.PersistWave(wave)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })

	submissions := []*Submission{
		diagnosticsReady(t, 1, 1), diagnosticsReady(t, 2, 1), diagnosticsReady(t, 3, 1),
	}
	for i, submission := range submissions {
		if _, err := sequencer.TrySubmit(submission); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			<-entered
		}
	}
	close(release)
	for _, submission := range submissions {
		if _, err := submission.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	stats := sequencer.Stats()
	if stats.SubmissionAttempts != 3 || stats.AcceptedSubmissions != 3 || stats.ReadySubmissions != 3 ||
		stats.RejectedSubmissions != 0 || stats.BackpressureSubmissions != 0 {
		t.Fatalf("submission stats=%+v", stats)
	}
	if stats.ReadyWavesAttempted != 2 || stats.ReadyPersistAttempts != 2 ||
		stats.ReadyWavesSucceeded != 2 || stats.ReadyWavesFailed != 0 ||
		stats.ReadyPersistSuccesses != 2 || stats.ReadyPersistFailures != 0 ||
		stats.FailedWaves != 0 {
		t.Fatalf("persistence stats=%+v", stats)
	}
	if stats.ReadyDurableWaves != 2 || stats.ReadyWaveGroupHistogram[1] != 1 ||
		stats.ReadyWaveGroupHistogram[2] != 1 || stats.MultiGroupWaves != 1 {
		t.Fatalf("durable wave stats=%+v", stats)
	}
	if stats.ObservedAppendBarriers != 2 || stats.ReadyObservedAppendBarriers != 2 ||
		stats.ControlObservedAppendBarriers != 0 || engineCalls.Load() != 2 || syncCalls.Load() != 2 {
		t.Fatalf("engine sync stats=%+v engineCalls=%d syncCalls=%d", stats, engineCalls.Load(), syncCalls.Load())
	}
	if stats.ReadyPersistDurationNanos == 0 || stats.ReadyWaveDurationNanos == 0 {
		t.Fatalf("missing duration diagnostics=%+v", stats)
	}
}

func TestNodeSubmissionSequencerStatsSeparateIdempotentRetryFromEngineSync(t *testing.T) {
	store := newDiagnosticsNodeStore(t, 1)
	var syncCalls atomic.Uint64
	store.SetDataSyncForTesting(func(file *os.File) error {
		syncCalls.Add(1)
		return file.Sync()
	})
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })

	first, retry := diagnosticsReady(t, 1, 1), diagnosticsReady(t, 1, 1)
	for _, submission := range []*Submission{first, retry} {
		if _, err := sequencer.TrySubmit(submission); err != nil {
			t.Fatal(err)
		}
		if _, err := submission.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	stats := sequencer.Stats()
	if stats.ReadyWavesAttempted != 2 || stats.ReadyPersistAttempts != 2 || stats.ReadyWavesSucceeded != 2 ||
		stats.ReadyPersistSuccesses != 2 || stats.ReadyWavesFailed != 0 {
		t.Fatalf("retry persistence stats=%+v", stats)
	}
	if stats.ReadyDurableWaves != 1 || stats.ReadyObservedAppendBarriers != 1 || stats.ObservedAppendBarriers != 1 ||
		stats.ReadyWaveGroupHistogram[1] != 1 || stats.MultiGroupWaves != 0 || syncCalls.Load() != 1 {
		t.Fatalf("retry engine stats=%+v syncCalls=%d", stats, syncCalls.Load())
	}
}

func TestNodeSubmissionSequencerStatsExcludeRetryFromAppendedGroupCount(t *testing.T) {
	store := newDiagnosticsNodeStore(t, 3)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Uint64
	store.persistWaveTest = func(wave seglog.Wave) error {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		return store.engine.PersistWave(wave)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	first := diagnosticsReady(t, 1, 1)
	if _, err := sequencer.TrySubmit(first); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	blocker := diagnosticsReady(t, 3, 1)
	if _, err := sequencer.TrySubmit(blocker); err != nil {
		t.Fatal(err)
	}
	<-entered
	// Both tickets enter one wave, but the store omits the exact retry from
	// the engine frame. Only group 2 contributes to the next append barrier.
	retry, fresh := diagnosticsReady(t, 1, 1), diagnosticsReady(t, 2, 1)
	for _, submission := range []*Submission{retry, fresh} {
		if _, err := sequencer.TrySubmit(submission); err != nil {
			t.Fatal(err)
		}
	}
	releaseOnce.Do(func() { close(release) })
	for _, submission := range []*Submission{blocker, retry, fresh} {
		if _, err := submission.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	stats := sequencer.Stats()
	if calls.Load() != 3 || stats.ReadyPersistAttempts != 3 || stats.ReadySubmissions != 4 ||
		stats.ReadyDurableWaves != 3 || stats.ReadyObservedAppendBarriers != 3 ||
		stats.ReadyWaveGroupHistogram[1] != 3 || stats.ReadyWaveGroupHistogram[2] != 0 || stats.MultiGroupWaves != 0 {
		t.Fatalf("retry counted as an appended group: stats=%+v calls=%d", stats, calls.Load())
	}
}

func TestNodeSubmissionSequencerStatsCountPersistenceRetryOutcomes(t *testing.T) {
	store := newDiagnosticsNodeStore(t, 1)
	var attempts atomic.Uint64
	store.persistWaveTest = func(wave seglog.Wave) error {
		if attempts.Add(1) == 1 {
			return seglog.ErrBackpressure
		}
		return store.engine.PersistWave(wave)
	}
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	submission := diagnosticsReady(t, 1, 1)
	if _, err := sequencer.TrySubmit(submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); err != nil {
		t.Fatal(err)
	}
	stats := sequencer.Stats()
	if stats.ReadyWavesAttempted != 1 || stats.ReadyWavesSucceeded != 1 || stats.ReadyWavesFailed != 0 ||
		stats.ReadyPersistAttempts != 2 || stats.ReadyPersistSuccesses != 1 || stats.ReadyPersistFailures != 1 ||
		stats.ReadyObservedAppendBarriers != 1 || stats.FailedWaves != 0 {
		t.Fatalf("persistence retry outcomes=%+v", stats)
	}
}

func TestNodeSubmissionSequencerStatsCountPanickingPersistenceAsFailure(t *testing.T) {
	sequencer := newTestSequencer(t, 8, func([]NodeReady) error { panic("persist failed") })
	submission := preparedSubmission(t, 1, 1)
	if _, err := sequencer.TrySubmit(submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); !errors.Is(err, ErrSubmissionPanic) || !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("panic completion=%v", err)
	}
	stats := sequencer.Stats()
	if stats.ReadyPersistAttempts != 1 || stats.ReadyPersistSuccesses != 0 || stats.ReadyPersistFailures != 1 ||
		stats.ReadyWavesAttempted != 1 || stats.ReadyWavesSucceeded != 0 || stats.ReadyWavesFailed != 1 || stats.FailedWaves != 1 ||
		stats.ObservedAppendBarriers != 0 {
		t.Fatalf("panicking persistence outcomes=%+v", stats)
	}
}

func TestNodeSubmissionSequencerStatsSeparateControlFromReady(t *testing.T) {
	store := newDiagnosticsNodeStore(t, 1)
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	var submission Submission
	if err := submission.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err := submission.PrepareBeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.TrySubmit(&submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); err != nil {
		t.Fatal(err)
	}
	incarnation := submission.Incarnations()[0]
	if err := submission.PreparePersistIncarnations([]GroupIncarnation{incarnation}); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.TrySubmit(&submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := submission.PrepareBeginIncarnations([]uint64{99}); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.TrySubmit(&submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown group control result=%v", err)
	}
	stats := sequencer.Stats()
	if stats.ControlSubmissions != 3 || stats.ReadySubmissions != 0 ||
		stats.ControlWavesAttempted != 3 || stats.ControlWavesSucceeded != 2 || stats.ControlWavesFailed != 1 ||
		stats.ControlPersistAttempts != 3 || stats.ControlPersistSuccesses != 2 || stats.ControlPersistFailures != 1 ||
		stats.ControlObservedAppendBarriers != 1 || stats.ReadyObservedAppendBarriers != 0 ||
		stats.ObservedAppendBarriers != 1 || stats.ReadyDurableWaves != 0 || stats.FailedWaves != 1 {
		t.Fatalf("control diagnostics=%+v", stats)
	}
}

func TestNodeSubmissionSequencerStatsDoNotInferBarrierFromAmbiguousSync(t *testing.T) {
	store := newDiagnosticsNodeStore(t, 1)
	failure := errors.New("ambiguous data sync result")
	var syncs atomic.Uint64
	store.SetDataSyncForTesting(func(file *os.File) error {
		if err := file.Sync(); err != nil {
			return err
		}
		syncs.Add(1)
		return failure
	})
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	submission := diagnosticsReady(t, 1, 1)
	if _, err := sequencer.TrySubmit(submission); err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(); !errors.Is(err, ErrPersistenceUnknown) || !errors.Is(err, failure) {
		t.Fatalf("ambiguous persistence result=%v", err)
	}
	stats := sequencer.Stats()
	if syncs.Load() != 1 || stats.ReadyPersistAttempts != 1 || stats.ReadyPersistFailures != 1 ||
		stats.ReadyWavesFailed != 1 || stats.ObservedAppendBarriers != 0 || stats.ReadyDurableWaves != 0 {
		t.Fatalf("ambiguous sync inferred an append witness: stats=%+v syncs=%d", stats, syncs.Load())
	}
}

func TestNodeSubmissionSequencerStatsQueueDepthBoundedDuringCursorProgress(t *testing.T) {
	// Model legal reserve/pop transitions without persistence latency hiding
	// the cursor interleaving. Begin near wrap to cover modular subtraction.
	sequencer := &NodeSubmissionSequencer{ring: make([]submissionRingSlot, 8)}
	base := ^uint64(0) - 32
	sequencer.head.value.Store(base)
	sequencer.tail.value.Store(base)
	var stop atomic.Bool
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		for position := base; !stop.Load(); position++ {
			sequencer.tail.value.Store(position + 1)
			sequencer.head.value.Store(position + 1)
		}
	}()
	defer func() { stop.Store(true); worker.Wait() }()
	for i := 0; i < 10000; i++ {
		stats := sequencer.Stats()
		if stats.QueueDepth > stats.QueueCapacity {
			t.Fatalf("queue depth exceeds capacity during concurrent progress: %d > %d", stats.QueueDepth, stats.QueueCapacity)
		}
	}
}

func TestNodeSubmissionSequencerStatsClassifiesBeforeCompletionReuse(t *testing.T) {
	sequencer := newTestSequencer(t, 8, func([]NodeReady) error { return nil })
	cell := preparedSubmission(t, 1, 1)
	reused := make(chan error)
	sequencer.completeHookTest = func(submission *Submission, _ uint32) {
		if _, done, _ := submission.Poll(); !done {
			reused <- errors.New("completion hook did not observe completion")
			return
		}
		// Poll transfers the completed cell back to its owner, even when the
		// producer has not yet returned from the publishing TrySubmit call.
		reused <- submission.PrepareBeginIncarnations([]uint64{1})
	}
	const submissions = 10000
	for readyID := uint64(1); readyID <= submissions; readyID++ {
		if err := cell.Prepare(NodeReady{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: readyID}}); err != nil {
			t.Fatal(err)
		}
		if _, err := sequencer.TrySubmit(cell); err != nil {
			t.Fatal(err)
		}
		if err := <-reused; err != nil {
			t.Fatal(err)
		}
	}
	stats := sequencer.Stats()
	if stats.ReadySubmissions != submissions || stats.ControlSubmissions != 0 {
		t.Fatalf("classified a reused cell instead of its submitted value: %+v", stats)
	}
}

func TestNodeSubmissionSequencerStatsConcurrentSnapshotsAreAllocationFree(t *testing.T) {
	sequencer := newTestSequencer(t, 128, func([]NodeReady) error {
		time.Sleep(time.Microsecond)
		return nil
	})
	cell := new(Submission)
	if err := cell.Initialize(); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var snapshots atomic.Uint64
	var snapshotWG sync.WaitGroup
	snapshotWG.Add(1)
	go func() {
		defer snapshotWG.Done()
		for !stop.Load() {
			_ = sequencer.Stats()
			snapshots.Add(1)
		}
	}()
	for readyID := uint64(1); readyID <= 128; readyID++ {
		if err := cell.Prepare(NodeReady{GroupID: 1, Batch: raftmodel.PersistBatch{NodeIncarnation: 1, ReadyID: readyID}}); err != nil {
			t.Fatal(err)
		}
		if _, err := sequencer.TrySubmit(cell); err != nil {
			t.Fatal(err)
		}
		if _, err := cell.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	snapshotWG.Wait()
	if snapshots.Load() == 0 {
		t.Fatal("snapshot goroutine did not run")
	}
	allocs := testing.AllocsPerRun(1000, func() { _ = sequencer.Stats() })
	if allocs != 0 {
		t.Fatalf("Stats allocations=%f", allocs)
	}
}
