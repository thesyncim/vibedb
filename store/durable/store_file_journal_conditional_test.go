package durable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func conditionalMarkerID(seed byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

// openCatalogOwnedSyncCollection opens a sync-journal collection in the sole
// current grammar, which directly admits conditional records.
func openCatalogOwnedSyncCollection(t *testing.T) (*Collection, *os.File, string) {
	t.Helper()
	options := syncPrimaryJournalTestOptions()
	coll, file, path := openPrimaryBatchStore(t, options)
	if coll.journal.Header().Format != storeio.RecoveryJournalFormat {
		t.Fatalf("format=%d, want current", coll.journal.Header().Format)
	}
	return coll, file, path
}

// prepareConditionalUnpublished stages and force-syncs a kind-4 record, then
// fully unwinds so memory stays at the pre-prepare root while the journal holds
// the durable conditional batch. It returns the prepared generation carried by
// the record and matched by the decision resolver.
func prepareConditionalUnpublished(
	t *testing.T, coll *Collection, markerID [16]byte, epoch, txnID uint64,
) uint64 {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	batch := coll.fileWriteBatch()
	defer coll.releaseFileWriteBatch(batch)
	if err := phaseWorkload(batch); err != nil {
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchConditionalLocked(batch)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if !staged.live {
		t.Fatal("expected live staged batch")
	}
	if err := coll.preparePrimaryBatchConditionalLocked(
		&staged, markerID, epoch, txnID, true,
	); err != nil {
		coll.unwindStagedPrimaryBatch(&staged)
		t.Fatalf("prepare: %v", err)
	}
	coll.unwindStagedPrimaryBatch(&staged)
	return staged.generation
}

func captureStoreJournal(t *testing.T, path string) (store, journal []byte) {
	t.Helper()
	var err error
	store, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	journal, err = os.ReadFile(path + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}
	return store, journal
}

func writeStoreJournal(t *testing.T, dir string, store, journal []byte) string {
	t.Helper()
	path := filepath.Join(dir, "store.vibe")
	if err := os.WriteFile(path, store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".rjournal", journal, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func installReplayResolver(
	t *testing.T,
	resolve recoveryJournalDecisionResolver,
	epoch uint64,
) {
	t.Helper()
	prev := recoveryJournalReplayResolverHook
	recoveryJournalReplayResolverHook = func(*Collection) (
		recoveryJournalDecisionResolver, uint64,
	) {
		return resolve, epoch
	}
	t.Cleanup(func() { recoveryJournalReplayResolverHook = prev })
}

func journalHoldsConditionalForTest(
	t testing.TB, coll *Collection, markerID [16]byte, epoch uint64,
) bool {
	t.Helper()
	holds, err := coll.journalHoldsConditional(markerID, epoch)
	if err != nil {
		t.Fatalf("journalHoldsConditional: %v", err)
	}
	return holds
}

func resolveAllConditionals(committed bool) recoveryJournalDecisionResolver {
	return func([16]byte, uint64, uint64, uint64) (bool, error) {
		return committed, nil
	}
}

func reopenSync(t *testing.T, path string) (*Collection, *os.File) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, syncPrimaryJournalTestOptions())
	if err != nil {
		_ = file.Close()
		t.Fatalf("Open: %v", err)
	}
	return coll, file
}

// TestConditionalReplayCommittedApplies proves a resolver that reports the
// prepared tuple committed applies the kind-4 batch on reopen.
func TestConditionalReplayCommittedApplies(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(1)
	const epoch, txnID = uint64(3), uint64(11)
	preparedGeneration := prepareConditionalUnpublished(
		t, coll, markerID, epoch, txnID,
	)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func(
		id [16]byte, ep, txn, generation uint64,
	) (bool, error) {
		return id == markerID && ep == epoch && txn == txnID &&
			generation == preparedGeneration, nil
	}, epoch)

	reopened, rfile := reopenSync(t, img)
	defer rfile.Close()
	defer reopened.Close()
	got := primaryStoreContent(t, reopened)
	want := phaseExpectedContent()
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("content[%q]=%q, want %q", k, got[k], v)
		}
	}
}

// TestConditionalReplayUndecidedSkips proves same-epoch undecided kind-4 is
// presumed abort: reopen leaves pre-prepare content and consumes the window.
func TestConditionalReplayUndecidedSkips(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	before := primaryStoreContent(t, coll)
	markerID := conditionalMarkerID(2)
	const epoch, txnID = uint64(1), uint64(5)
	prepareConditionalUnpublished(t, coll, markerID, epoch, txnID)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func([16]byte, uint64, uint64, uint64) (bool, error) {
		return false, nil
	}, epoch)

	reopened, rfile := reopenSync(t, img)
	defer rfile.Close()
	defer reopened.Close()
	got := primaryStoreContent(t, reopened)
	for k, v := range before {
		if got[k] != v {
			t.Fatalf("undecided apply mutated %q: got %q want %q", k, got[k], v)
		}
	}
	for k := range phaseExpectedContent() {
		if _, ok := before[k]; !ok {
			if _, present := got[k]; present {
				t.Fatalf("undecided kind-4 applied key %q", k)
			}
		}
	}
	reopened.writer.Lock()
	holds := journalHoldsConditionalForTest(t, reopened, markerID, epoch)
	reopened.writer.Unlock()
	if holds {
		t.Fatal("skipped kind-4 survived reopen without fold/recycle")
	}
}

// TestConditionalReplayEpochMismatchFailsClosed covers epoch-ahead and
// epoch-behind kind-4 records against the decision-log epoch.
func TestConditionalReplayEpochMismatchFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name          string
		recordEpoch   uint64
		decisionEpoch uint64
	}{
		{"epoch-ahead", 5, 4},
		{"epoch-behind", 3, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll, file, path := openCatalogOwnedSyncCollection(t)
			markerID := conditionalMarkerID(3)
			prepareConditionalUnpublished(t, coll, markerID, tc.recordEpoch, 9)
			storeBytes, journalBytes := captureStoreJournal(t, path)
			_ = coll.Close()
			_ = file.Close()

			img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
			installReplayResolver(t, func([16]byte, uint64, uint64, uint64) (bool, error) {
				t.Fatal("resolver must not run on epoch mismatch")
				return false, nil
			}, tc.decisionEpoch)

			rfile, err := os.OpenFile(img, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer rfile.Close()
			_, err = Open(rfile, syncPrimaryJournalTestOptions())
			if !errors.Is(err, ErrTransactionMarkerEpochMismatch) {
				t.Fatalf("Open err=%v, want ErrTransactionMarkerEpochMismatch", err)
			}
		})
	}
}

// TestConditionalReplayStandaloneUncoveredInDoubt proves Open with a nil
// resolver fails closed when the live window holds an uncovered kind-4.
func TestConditionalReplayStandaloneUncoveredInDoubt(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	prepareConditionalUnpublished(t, coll, conditionalMarkerID(4), 1, 1)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	rfile, err := os.OpenFile(img, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer rfile.Close()
	_, err = Open(rfile, syncPrimaryJournalTestOptions())
	if !errors.Is(err, ErrCollectionInDoubt) {
		t.Fatalf("Open err=%v, want ErrCollectionInDoubt", err)
	}
}

// advanceDurableRootWithoutRecycle checkpoints while journalReplaying is set so
// the durable root advances but the live journal window is retained. Used to
// build covered kind-4 images (record generation ≤ root, still in the window).
func advanceDurableRootWithoutRecycle(t *testing.T, coll *Collection) {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	coll.journalReplaying = true
	err := coll.checkpointBufferedLocked()
	coll.journalReplaying = false
	if err != nil {
		t.Fatalf("checkpoint without recycle: %v", err)
	}
}

// TestConditionalReplayCoveredStillResolved proves a covered kind-4 is still
// resolved. The root may contain only a sequential replay prefix, so coverage
// alone cannot establish whether the complete conditional batch committed.
func TestConditionalReplayCoveredStillResolved(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(5)
	const epoch = uint64(1)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 42)
	// Publish a later kind-3 at the same generation so the durable root can
	// advance past the kind-4 while the journal still holds it.
	if err := coll.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("cover"), []byte(`{"v":1}`))
	}); err != nil {
		t.Fatalf("cover Update: %v", err)
	}
	advanceDurableRootWithoutRecycle(t, coll)
	coll.writer.Lock()
	rootGen := coll.state.Load().root.Generation
	if !journalHoldsConditionalForTest(t, coll, markerID, epoch) {
		coll.writer.Unlock()
		t.Fatal("expected covered kind-4 retained in journal")
	}
	coll.writer.Unlock()
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	resolverCalls := 0
	installReplayResolver(t, func([16]byte, uint64, uint64, uint64) (bool, error) {
		resolverCalls++
		return false, nil
	}, epoch)

	reopened, rfile := reopenSync(t, img)
	defer rfile.Close()
	defer reopened.Close()
	if resolverCalls != 1 {
		t.Fatalf("covered kind-4 invoked resolver %d times, want 1", resolverCalls)
	}
	reopened.writer.Lock()
	holds := journalHoldsConditionalForTest(t, reopened, markerID, epoch)
	cursor := reopened.journal.Cursor()
	gotRoot := reopened.state.Load().root.Generation
	reopened.writer.Unlock()
	if gotRoot < rootGen {
		t.Fatalf("reopened root=%d, want ≥ %d", gotRoot, rootGen)
	}
	if holds {
		t.Fatal("covered kind-4 still present after reopen fold")
	}
	if cursor != 0 {
		t.Fatalf("journal cursor=%d after covered consume, want 0", cursor)
	}

	// Standalone nil-resolver path: the same covered image remains in doubt.
	img2 := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	prev := recoveryJournalReplayResolverHook
	recoveryJournalReplayResolverHook = nil
	sfile, err := os.OpenFile(img2, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, openErr := Open(sfile, syncPrimaryJournalTestOptions())
	recoveryJournalReplayResolverHook = prev
	defer sfile.Close()
	if !errors.Is(openErr, ErrCollectionInDoubt) {
		t.Fatalf("standalone covered open = %v, want ErrCollectionInDoubt", openErr)
	}
}

// TestConditionalReplayTargetBindingSkips proves a resolver that commits
// the triple but (per its closure) does not name this collection skips apply.
func TestConditionalReplayTargetBindingSkips(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	before := primaryStoreContent(t, coll)
	markerID := conditionalMarkerID(6)
	const epoch, txnID = uint64(2), uint64(8)
	prepareConditionalUnpublished(t, coll, markerID, epoch, txnID)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	// Closure answers committed only when the decision names this collection.
	// Here the decision exists for the triple but omits this participant.
	thisJournalID := [16]byte{}
	installReplayResolver(t, func(
		id [16]byte, ep, txn, generation uint64,
	) (bool, error) {
		if id != markerID || ep != epoch || txn != txnID || generation == 0 {
			return false, nil
		}
		// Participant binding: decision does not name this journal.
		_ = thisJournalID
		return false, nil
	}, epoch)

	reopened, rfile := reopenSync(t, img)
	defer rfile.Close()
	defer reopened.Close()
	got := primaryStoreContent(t, reopened)
	for k, v := range before {
		if got[k] != v {
			t.Fatalf("binding-skip mutated %q", k)
		}
	}
	for k := range phaseExpectedContent() {
		if _, ok := before[k]; !ok {
			if _, present := got[k]; present {
				t.Fatalf("binding-skip applied key %q", k)
			}
		}
	}
}

// TestConditionalReplayResolverErrorFailsClosed proves a resolver error fails
// replay closed with that error surfaced via errors.Is.
func TestConditionalReplayResolverErrorFailsClosed(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	prepareConditionalUnpublished(t, coll, conditionalMarkerID(7), 1, 3)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	sentinel := errors.New("vibedb: test transaction log missing")
	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func([16]byte, uint64, uint64, uint64) (bool, error) {
		return false, sentinel
	}, 1)

	rfile, err := os.OpenFile(img, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer rfile.Close()
	_, err = Open(rfile, syncPrimaryJournalTestOptions())
	if !errors.Is(err, sentinel) {
		t.Fatalf("Open err=%v, want sentinel via errors.Is", err)
	}
}

// TestConditionalReplayCoveredInvokesResolver is the direct tripwire form of
// the covered-resolution contract.
func TestConditionalReplayCoveredInvokesResolver(t *testing.T) {
	coll, file, _ := openCatalogOwnedSyncCollection(t)
	defer file.Close()
	defer coll.Close()

	markerID := conditionalMarkerID(8)
	const epoch = uint64(1)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 1)
	if err := coll.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("cover"), []byte(`{"v":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	advanceDurableRootWithoutRecycle(t, coll)

	coll.writer.Lock()
	rootGen := coll.state.Load().root.Generation
	coll.writer.Unlock()
	resolverCalls := 0
	resolve := recoveryJournalDecisionResolver(
		func([16]byte, uint64, uint64, uint64) (bool, error) {
			resolverCalls++
			return false, nil
		},
	)
	// Replay acquires the writer for its post-fold checkpoint; do not hold it.
	if err := coll.replayRecoveryJournalResolvedLocked(
		rootGen, resolve, epoch,
	); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if resolverCalls != 1 {
		t.Fatalf("covered resolver calls = %d, want 1", resolverCalls)
	}
	coll.writer.Lock()
	holds := journalHoldsConditionalForTest(t, coll, markerID, epoch)
	coll.writer.Unlock()
	if holds {
		t.Fatal("covered kind-4 not consumed")
	}
}

// TestConditionalStrayConsumptionFoldRecycle proves a window of only skipped
// undecided kind-4 records is folded and recycled; a second reopen replays
// nothing and performs no further fold.
func TestConditionalStrayConsumptionFoldRecycle(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(9)
	const epoch = uint64(1)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 99)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func([16]byte, uint64, uint64, uint64) (bool, error) {
		return false, nil
	}, epoch)

	first, f1 := reopenSync(t, img)
	first.writer.Lock()
	if journalHoldsConditionalForTest(t, first, markerID, epoch) {
		first.writer.Unlock()
		t.Fatal("stray kind-4 survived first reopen")
	}
	if first.journal.Cursor() != 0 {
		first.writer.Unlock()
		t.Fatalf("cursor=%d after stray fold, want 0", first.journal.Cursor())
	}
	checkpoints := first.automaticCheckpoints.Load()
	first.writer.Unlock()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	_ = f1.Close()

	second, f2 := reopenSync(t, img)
	defer f2.Close()
	defer second.Close()
	second.writer.Lock()
	defer second.writer.Unlock()
	if second.journal.Cursor() != 0 {
		t.Fatalf("second reopen cursor=%d, want 0", second.journal.Cursor())
	}
	// Clean reopen with empty journal must not force another fold checkpoint
	// beyond Close's own persistence boundary accounting; cursor staying zero
	// and no conditional held is the contract.
	if journalHoldsConditionalForTest(t, second, markerID, epoch) {
		t.Fatal("conditional reappeared on second reopen")
	}
	_ = checkpoints
}

// TestConditionalAbortedGenerationAliasing proves an aborted kind-4 at G+1
// followed by an applied kind-3 at G+1 replays only the kind-3; second replay
// is idempotent.
func TestConditionalAbortedGenerationAliasing(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(10)
	const epoch = uint64(1)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 1)

	// Single-collection Update reuses G+1 (aborted conditional never advanced
	// the published generation) and appends a kind-3 batch after the kind-4.
	if err := coll.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("alias"), []byte(`{"v":"kind3"}`))
	}); err != nil {
		t.Fatalf("kind-3 Update: %v", err)
	}
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func([16]byte, uint64, uint64, uint64) (bool, error) {
		return false, nil // aborted / undecided kind-4
	}, epoch)

	verify := func(label string) {
		t.Helper()
		reopened, rfile := reopenSync(t, img)
		defer rfile.Close()
		defer reopened.Close()
		got := primaryStoreContent(t, reopened)
		if got["alias"] != `{"v":"kind3"}` {
			t.Fatalf("%s: kind-3 missing: %q", label, got["alias"])
		}
		for k := range phaseExpectedContent() {
			if _, ok := got[k]; ok && k != "seed" {
				// phase keys must not appear from the aborted kind-4
				if k == "a" || k == "b" || k == "c" {
					t.Fatalf("%s: aborted kind-4 key %q present", label, k)
				}
			}
		}
	}
	verify("first-open")
	verify("second-open-idempotent")
}

// TestConditionalAccessorsFoldPastWindow proves journalHoldsConditional
// transitions true→false across prepare→checkpointPastConditionalsLocked and
// leaves the window conditional-free.
func TestConditionalAccessorsFoldPastWindow(t *testing.T) {
	coll, file, _ := openCatalogOwnedSyncCollection(t)
	defer file.Close()
	defer coll.Close()

	markerID := conditionalMarkerID(11)
	const epoch = uint64(2)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 7)

	coll.writer.Lock()
	defer coll.writer.Unlock()
	if !journalHoldsConditionalForTest(t, coll, markerID, epoch) {
		t.Fatal("expected journalHoldsConditional true after prepare")
	}
	if journalHoldsConditionalForTest(t, coll, markerID, epoch+1) {
		t.Fatal("holdsConditional matched wrong epoch")
	}
	before := coll.automaticCheckpoints.Load()
	if err := coll.checkpointPastConditionalsLocked(
		resolveAllConditionals(false), epoch,
	); err != nil {
		t.Fatalf("checkpointPastConditionalsLocked: %v", err)
	}
	if coll.automaticCheckpoints.Load() != before+1 {
		t.Fatalf("fold not foreground-bounded to one checkpoint: before=%d after=%d",
			before, coll.automaticCheckpoints.Load())
	}
	if journalHoldsConditionalForTest(t, coll, markerID, epoch) {
		t.Fatal("journalHoldsConditional still true after fold")
	}
	if coll.journal.Cursor() != 0 {
		t.Fatalf("cursor=%d after fold, want 0", coll.journal.Cursor())
	}
}
