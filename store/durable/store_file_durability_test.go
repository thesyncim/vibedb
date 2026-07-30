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
		super: storeio.Superblock{
			Generation: generation,
		},
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
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); !errors.Is(err, persistErr) {
		t.Fatalf("Close after snapshot release = %v, want sticky %v",
			err, persistErr)
	}
	if err := collection.Close(); err != nil {
		t.Fatalf("idempotent Close = %v", err)
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
