package durable

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func durabilityTestState(generation uint64) *fileStoreState {
	return &fileStoreState{
		root: storeio.StateRoot{Generation: generation},
	}
}

func TestFileStoreDurabilityModeSafeZeroAndExplicitAsync(t *testing.T) {
	zero, err := (Options{}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if zero.Durability != DurabilitySync {
		t.Fatalf("zero durability = %d, want DurabilitySync", zero.Durability)
	}
	asynchronous, err := (Options{
		Durability: DurabilityAsyncVisible,
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if asynchronous.Durability != DurabilityAsyncVisible {
		t.Fatalf("async durability = %d", asynchronous.Durability)
	}
	for _, mode := range []WriteMode{WriteDirectTry, WriteDirectRequire} {
		if _, err := (Options{
			MaterializationDamageGranule: 512,
			WriteMode:                    mode,
		}).normalized(); err == nil {
			t.Fatalf(
				"materialization with direct write mode %d was accepted",
				mode,
			)
		}
	}
}

func TestFileStoreAsyncCreatedSyncReopenChainFenceProgressAndRecovery(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "chain-fence-reopen-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"keep":   `{"value":"before"}`,
		"remove": `{"value":"delete-me"}`,
	} {
		if _, err := collection.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("seed Put(%q): %v", key, err)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("seed Flush: %v", err)
	}
	if err := collection.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}

	options.Durability = DurabilitySync
	collection, err = Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if !collection.chainFenceSync() || collection.journalEnabled() {
		_ = collection.Close()
		t.Fatal("async-created store did not reopen on the journal-less sync lane")
	}

	type mutationResult struct {
		changed bool
		err     error
	}
	runMutation := func(name string, mutate func() (bool, error)) bool {
		t.Helper()
		result := make(chan mutationResult, 1)
		go func() {
			changed, mutationErr := mutate()
			result <- mutationResult{changed: changed, err: mutationErr}
		}()
		select {
		case outcome := <-result:
			if outcome.err != nil {
				t.Fatalf("%s: %v", name, outcome.err)
			}
			return outcome.changed
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not cross the synchronous root fence", name)
			return false
		}
	}

	if created := runMutation("Put", func() (bool, error) {
		return collection.Put([]byte("keep"), []byte(`{"value":"after"}`))
	}); created {
		t.Fatal("replacement Put reported an insert")
	}
	if got, found, err := collection.AppendRaw(nil, []byte("keep")); err != nil || !found || string(got) != `{"value":"after"}` {
		t.Fatalf("visible replacement = (%s, %v, %v)", got, found, err)
	}
	if deleted := runMutation("Delete", func() (bool, error) {
		return collection.Delete([]byte("remove"))
	}); !deleted {
		t.Fatal("Delete reported the existing key absent")
	}
	if got, found, err := collection.AppendRaw(nil, []byte("remove")); err != nil || found || len(got) != 0 {
		t.Fatalf("visible deletion = (%s, %v, %v)", got, found, err)
	}
	if collection.Generation() != collection.DurableGeneration() {
		t.Fatalf(
			"sync generation = %d, durable = %d",
			collection.Generation(), collection.DurableGeneration(),
		)
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("chain-fence Flush: %v", err)
	}
	if err := collection.Close(); err != nil {
		t.Fatalf("chain-fence Close: %v", err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, found, err := reopened.AppendRaw(nil, []byte("keep")); err != nil || !found || string(got) != `{"value":"after"}` {
		t.Fatalf("reopened replacement = (%s, %v, %v)", got, found, err)
	}
	if got, found, err := reopened.AppendRaw(nil, []byte("remove")); err != nil || found || len(got) != 0 {
		t.Fatalf("reopened deletion = (%s, %v, %v)", got, found, err)
	}
}

func TestFileStoreAsyncCreatedSyncReopenChainFenceFailureFailsClosed(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "chain-fence-failure-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	const key = "stable"
	const before = `{"value":"before"}`
	if _, err := collection.Put([]byte(key), []byte(before)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	options.Durability = DurabilitySync
	controller := &faultController{plan: storeio.FaultPlan{
		Commit: 0, Phase: storeio.FaultENOSPCData, DataIndex: 0,
	}}
	previousFactory := storeCommitterFactory
	storeCommitterFactory = controller.factory()
	t.Cleanup(func() { storeCommitterFactory = previousFactory })
	collection, err = Open(file, options)
	if err != nil {
		storeCommitterFactory = previousFactory
		t.Fatal(err)
	}
	if !collection.chainFenceSync() {
		storeCommitterFactory = previousFactory
		_ = collection.Close()
		t.Fatal("async-created store did not reopen on chain-fence sync lane")
	}
	if got, found, err := collection.AppendRaw(nil, []byte(key)); err != nil || !found || string(got) != before {
		t.Fatalf("chain-fence baseline = (%s, %v, %v), want %s", got, found, err, before)
	}
	_, putErr := collection.Put(
		[]byte(key), []byte(`{"value":"rejected"}`),
	)
	if putErr == nil {
		storeCommitterFactory = previousFactory
		_ = collection.Close()
		t.Fatal("faulted chain-fence Put succeeded")
	}
	if controller.device == nil || !controller.device.Faulted() {
		storeCommitterFactory = previousFactory
		_ = collection.Close()
		t.Fatal("programmed chain-fence device fault did not fire")
	}
	if got, found, readErr := collection.AppendRaw(nil, []byte(key)); readErr == nil || found || len(got) != 0 {
		storeCommitterFactory = previousFactory
		_ = collection.Close()
		t.Fatalf(
			"read after failed fence = (%s, %v, %v), want fail-closed; Put=%v PersistenceError=%v generation=%d durable=%d",
			got, found, readErr, putErr, collection.PersistenceError(),
			collection.Generation(), collection.DurableGeneration(),
		)
	}
	_ = collection.Close()
	storeCommitterFactory = previousFactory

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, found, err := reopened.AppendRaw(nil, []byte(key)); err != nil || !found || string(got) != before {
		t.Fatalf("recovered rejected fence = (%s, %v, %v), want %s", got, found, err, before)
	}
}

func TestFileStoreDurablePromotionSelectsNewestGroupedGeneration(t *testing.T) {
	collection := &Collection{
		options:        normalizedFileStoreOptions{Options: Options{Durability: DurabilitySync}},
		pendingVisible: make([]filePendingState, 4),
	}
	initial := durabilityTestState(1)
	collection.initializeFileState(initial)
	second := durabilityTestState(2)
	third := durabilityTestState(3)
	collection.pendingVisible[2] = filePendingState{generation: 2, state: second}
	collection.pendingVisible[3] = filePendingState{generation: 3, state: third}

	collection.promoteDurableStateLocked(3)
	if got := collection.visibleState.Load(); got != third {
		t.Fatalf("visible state = %p, want grouped latest %p", got, third)
	}
	if got := collection.durableState.Load(); got != third {
		t.Fatalf("durable state = %p, want grouped latest %p", got, third)
	}
	for index, pending := range collection.pendingVisible {
		if pending.state != nil {
			t.Fatalf("pending slot %d retained generation %d", index, pending.generation)
		}
	}
}

func TestFileStoreDurablePromotionGuardSkipsUnchangedWatermark(t *testing.T) {
	collection := &Collection{
		options: normalizedFileStoreOptions{
			Options: Options{Durability: DurabilityBufferedVisible},
		},
		pendingVisible: make([]filePendingState, 1_024),
	}
	initial := durabilityTestState(10)
	collection.initializeFileState(initial)
	next := durabilityTestState(11)

	collection.recordPendingFileStateLocked(next, 10)
	if got := collection.visibleState.Load(); got != next {
		t.Fatalf("visible state = %p, want published %p", got, next)
	}
	if got := collection.durableState.Load(); got != initial {
		t.Fatalf("durable state = %p, want unchanged %p", got, initial)
	}
	slot := next.root.Generation & uint64(len(collection.pendingVisible)-1)
	pending := collection.pendingVisible[slot]
	if pending.generation != 11 || pending.state != next {
		t.Fatalf(
			"pending slot = generation %d state %p, want 11/%p",
			pending.generation, pending.state, next,
		)
	}
	if scanned := collection.promoteDurableStateIfAdvancedLocked(10); scanned {
		t.Fatal("unchanged durable watermark scanned the visibility ring")
	}
	if pending = collection.pendingVisible[slot]; pending.state != next {
		t.Fatal("guard changed pending publication at unchanged watermark")
	}
}

func TestFileStorePublishRecordsPendingBeforePromotion(t *testing.T) {
	collection := &Collection{
		options: normalizedFileStoreOptions{
			// With no journal, synchronous visibility is gated by the physical
			// committer watermark. The state can become visible only if the
			// promotion sees the pending entry recorded by this same call.
			Options: Options{Durability: DurabilitySync},
		},
		pendingVisible: make([]filePendingState, 4),
	}
	initial := durabilityTestState(1)
	collection.initializeFileState(initial)
	second := durabilityTestState(2)

	// The supplied physical watermark already covers generation 2. Recording
	// second before the guarded scan is what lets this single call promote it.
	collection.recordPendingFileStateLocked(second, 2)
	if got := collection.durableState.Load(); got != second {
		t.Fatalf("durable state = %p, want just-published %p", got, second)
	}
	if got := collection.visibleState.Load(); got != second {
		t.Fatalf("visible state = %p, want just-published %p", got, second)
	}
	slot := second.root.Generation & uint64(len(collection.pendingVisible)-1)
	if pending := collection.pendingVisible[slot]; pending.state != nil {
		t.Fatalf(
			"promoted pending slot retained generation %d state %p",
			pending.generation, pending.state,
		)
	}
}

func TestFileStoreDurablePromotionGuardHandlesGroupedGap(t *testing.T) {
	collection := &Collection{
		options: normalizedFileStoreOptions{
			Options: Options{Durability: DurabilityBufferedVisible},
		},
		pendingVisible: make([]filePendingState, 8),
	}
	initial := durabilityTestState(1)
	collection.initializeFileState(initial)
	second := durabilityTestState(2)
	third := durabilityTestState(3)
	fourth := durabilityTestState(4)

	collection.recordPendingFileStateLocked(second, 1)
	collection.recordPendingFileStateLocked(third, 1)
	collection.recordPendingFileStateLocked(fourth, 1)
	if got := collection.durableState.Load(); got != initial {
		t.Fatalf("pre-fence durable state = %p, want %p", got, initial)
	}
	if scanned := collection.promoteDurableStateIfAdvancedLocked(3); !scanned {
		t.Fatal("advanced grouped watermark skipped promotion scan")
	}
	if got := collection.durableState.Load(); got != third {
		t.Fatalf("grouped durable state = %p, want generation-3 %p", got, third)
	}
	for _, generation := range []uint64{2, 3} {
		slot := generation & uint64(len(collection.pendingVisible)-1)
		if pending := collection.pendingVisible[slot]; pending.state != nil {
			t.Fatalf("eligible generation %d remained pending", generation)
		}
	}
	fourthSlot := fourth.root.Generation &
		uint64(len(collection.pendingVisible)-1)
	if pending := collection.pendingVisible[fourthSlot]; pending.state != fourth {
		t.Fatalf("future generation 4 was cleared by generation-3 fence")
	}
	if scanned := collection.promoteDurableStateIfAdvancedLocked(4); !scanned {
		t.Fatal("second advanced watermark skipped promotion scan")
	}
	if got := collection.durableState.Load(); got != fourth {
		t.Fatalf("final durable state = %p, want generation-4 %p", got, fourth)
	}
}

func BenchmarkFileStoreDurablePromotionGuard(b *testing.B) {
	newCollection := func() *Collection {
		collection := &Collection{
			pendingVisible: make([]filePendingState, 1_024),
		}
		collection.initializeFileState(durabilityTestState(1))
		collection.pendingVisible[2] = filePendingState{
			generation: 2, state: durabilityTestState(2),
		}
		return collection
	}
	b.Run("unchanged-guard", func(b *testing.B) {
		collection := newCollection()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if collection.promoteDurableStateIfAdvancedLocked(1) {
				b.Fatal("unchanged watermark scanned")
			}
		}
	})
	b.Run("full-scan-control", func(b *testing.B) {
		collection := newCollection()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			collection.promoteDurableStateLocked(1)
		}
	})
}

func TestFileStoreAsyncFailureRollsBackOrRejectsCanonicalView(t *testing.T) {
	initial := durabilityTestState(1)
	volatile := durabilityTestState(2)

	copyOnWrite := &Collection{
		options: normalizedFileStoreOptions{
			Options: Options{Durability: DurabilityAsyncVisible},
		},
		pendingVisible: make([]filePendingState, 4),
	}
	copyOnWrite.initializeFileState(initial)
	copyOnWrite.visibleState.Store(volatile)
	copyOnWrite.poisonPersistence(nil)
	if got := copyOnWrite.visibleState.Load(); got != initial {
		t.Fatalf("copy-on-write failure view = %p, want durable %p", got, initial)
	}

	canonical := &Collection{
		options: normalizedFileStoreOptions{
			Options: Options{
				Durability:                   DurabilityAsyncVisible,
				MaterializationDamageGranule: 512,
			},
		},
		pendingVisible: make([]filePendingState, 4),
	}
	canonical.initializeFileState(initial)
	canonical.visibleState.Store(volatile)
	canonical.poisonPersistence(nil)
	if got := canonical.visibleState.Load(); got != nil {
		t.Fatalf("canonical failure retained unsafe reader state %p", got)
	}
}

func TestFileStoreStickyFailureRejectsNoOpMutations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "sticky-failure-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, _ = collection.Put([]byte("trigger"), []byte(`{"value":1}`))
	deadline := time.Now().Add(5 * time.Second)
	for collection.PersistenceError() == nil && time.Now().Before(deadline) {
		time.Sleep(100 * time.Microsecond)
	}
	persistErr := collection.PersistenceError()
	if persistErr == nil {
		t.Fatal("closed commit descriptor did not poison persistence")
	}
	if deleted, err := collection.Delete([]byte("missing")); deleted ||
		!errors.Is(err, persistErr) {
		t.Fatalf(
			"missing Delete after failure = (%v, %v), want sticky %v",
			deleted, err, persistErr,
		)
	}
	if err := collection.Update(func(*WriteBatch) error {
		return nil
	}); !errors.Is(err, persistErr) {
		t.Fatalf("empty Update after failure = %v, want %v", err, persistErr)
	}
	if err := collection.Close(); !errors.Is(err, persistErr) {
		t.Fatalf("Close after failure = %v, want %v", err, persistErr)
	}
}

func TestFileStoreSyncFailureNeverExposesRejectedMutation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "sync-failure-visibility-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilitySync
	// The sync primary lane's durability fence is a synced recovery-journal record
	// appended before the mutation is applied and published. That append is the
	// device write a failure must be injected into: the chunk-era trick of closing
	// the store descriptor no longer touches a sync Put, which writes only the
	// sibling journal. Arm the journal write seam so the rejected commit's append
	// fails with ENOSPC.
	get, restore := installJournalFaultSeam(t)
	defer restore()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	fj := get()
	if fj == nil {
		t.Fatal("sync primary store minted no journal to fault")
	}
	const key = "stable"
	before := []byte(`{"value":"durable"}`)
	after := []byte(`{"value":"rejected"}`)
	if _, err := collection.Put([]byte(key), before); err != nil {
		t.Fatal(err)
	}
	// A sync Put acknowledges through the journal and folds its root at the next
	// checkpoint, so the published generation leads the checkpointed
	// DurableGeneration until then. Checkpoint here so the baseline has both
	// aligned; the assertion that a rejected commit advances neither is the point.
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	generation := collection.Generation()
	durableGeneration := collection.DurableGeneration()
	if generation != durableGeneration {
		t.Fatalf(
			"baseline generation = %d, durable = %d",
			generation, durableGeneration,
		)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Fail the next journal append — the replacement Put's durability record —
	// before it can cross the fence and be applied or published.
	fj.Program(storeio.JournalFaultPlan{
		Phase: storeio.JournalFaultENOSPCAppend, AppendIndex: fj.Appends(),
	})
	if created, err := collection.Put([]byte(key), after); created || err == nil {
		t.Fatalf("rejected replacement = (%v, %v), want false and failure", created, err)
	}
	if !fj.Faulted() {
		t.Fatal("programmed journal append fault never fired")
	}
	persistErr := collection.PersistenceError()
	if persistErr == nil {
		t.Fatal("synchronous device failure did not become sticky")
	}
	got, found, readErr := collection.AppendRaw(nil, []byte(key))
	if readErr != nil || !found || string(got) != string(before) {
		t.Fatalf(
			"reader after rejected commit = (%q, %v, %v), want durable %q",
			got, found, readErr, before,
		)
	}
	if collection.Generation() != generation ||
		collection.DurableGeneration() != durableGeneration {
		t.Fatalf(
			"rejected commit advanced generation = %d/%d, want %d/%d",
			collection.Generation(), collection.DurableGeneration(),
			generation, durableGeneration,
		)
	}
	// The journal poison is die-don't-retry: every later mutation is refused with
	// the sticky error rather than retried against an uncertain journal.
	if _, err := collection.Put([]byte(key), after); !errors.Is(err, persistErr) {
		t.Fatalf("mutation after sync failure = %v, want sticky %v", err, persistErr)
	}
	// An active snapshot keeps resource teardown retryable even on the poisoned
	// path. Once it releases, Close reports the sticky failure but still releases
	// every owned resource and the writer lock.
	if err := collection.Close(); !errors.Is(err, persistErr) ||
		!errors.Is(err, storeio.ErrLeasesActive) {
		t.Fatalf("Close with snapshot after sync failure = %v, want %v and %v",
			err, persistErr, storeio.ErrLeasesActive)
	}
	if collection.CloseCompleted() {
		t.Fatal("CloseCompleted reported terminal cleanup while a snapshot lease remained")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	closeErr := collection.Close()
	if !errors.Is(closeErr, persistErr) {
		t.Fatalf("Close after snapshot release = %v, want sticky %v",
			closeErr, persistErr)
	}
	if !collection.CloseCompleted() {
		t.Fatal("CloseCompleted did not report teardown after terminal persistence error")
	}
	if repeated := collection.Close(); repeated != closeErr {
		t.Fatalf("idempotent Close = %v, want cached exact %v", repeated, closeErr)
	}
	// Recovery on reopen, rather than a retry on the poisoned handle, must find
	// the durable baseline and none of the rejected mutation.
	reopenedFile, err := os.OpenFile(file.Name(), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatalf("reopen after sync failure: %v", err)
	}
	defer reopened.Close()
	if got, found, err := reopened.AppendRaw(nil, []byte(key)); err != nil || !found ||
		string(got) != string(before) {
		t.Fatalf("reopened value = (%q, %v, %v), want durable baseline %q", got, found, err, before)
	}
}
