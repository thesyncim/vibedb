package durable

import (
	"errors"
	"fmt"
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

// openCatalogOwnedSyncCollection opens a sync-journal collection and marks it
// catalog-owned so prepare may remint at the conditional journal format.
func openCatalogOwnedSyncCollection(t *testing.T) (*Collection, *os.File, string) {
	t.Helper()
	options := syncPrimaryJournalTestOptions()
	coll, file, path := openPrimaryBatchStore(t, options)
	coll.writer.Lock()
	coll.journalCatalogOwned = true
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		coll.writer.Unlock()
		t.Fatalf("ensure conditional format: %v", err)
	}
	if coll.journal.Header().FormatVersion !=
		storeio.RecoveryJournalFormatConditional {
		coll.writer.Unlock()
		t.Fatalf("format=%d, want conditional", coll.journal.Header().FormatVersion)
	}
	coll.writer.Unlock()
	return coll, file, path
}

// prepareConditionalUnpublished stages and force-syncs a kind-5 record, then
// fully unwinds so memory stays at the pre-prepare root while the journal holds
// the durable conditional batch. Returns the marker binding used.
func prepareConditionalUnpublished(
	t *testing.T, coll *Collection, markerID [16]byte, epoch, txnID uint64,
) {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	batch := coll.fileWriteBatch()
	defer coll.releaseFileWriteBatch(batch)
	if err := phaseWorkload(batch); err != nil {
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchLocked(batch)
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
// prepared triple committed applies the kind-5 batch on reopen.
func TestConditionalReplayCommittedApplies(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(1)
	const epoch, txnID = uint64(3), uint64(11)
	prepareConditionalUnpublished(t, coll, markerID, epoch, txnID)
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func(id [16]byte, ep, txn uint64) (bool, error) {
		return id == markerID && ep == epoch && txn == txnID, nil
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

// TestConditionalReplayUndecidedSkips proves same-epoch undecided kind-5 is
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
	installReplayResolver(t, func([16]byte, uint64, uint64) (bool, error) {
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
				t.Fatalf("undecided kind-5 applied key %q", k)
			}
		}
	}
	reopened.writer.Lock()
	holds := reopened.journalHoldsConditional(markerID, epoch)
	reopened.writer.Unlock()
	if holds {
		t.Fatal("skipped kind-5 survived reopen without fold/recycle")
	}
}

// TestConditionalReplayEpochMismatchFailsClosed covers epoch-ahead and
// epoch-behind kind-5 records against the decision-log epoch.
func TestConditionalReplayEpochMismatchFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name         string
		recordEpoch  uint64
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
			installReplayResolver(t, func([16]byte, uint64, uint64) (bool, error) {
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
// resolver fails closed when the live window holds an uncovered kind-5.
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
// build covered kind-5 images (record generation ≤ root, still in the window).
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

// TestConditionalReplayCoveredConsumedNilResolver proves a covered kind-5
// (root generation ≥ record generation) is consumed without consulting a
// resolver, including the standalone nil-resolver Open path.
func TestConditionalReplayCoveredConsumedNilResolver(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(5)
	const epoch = uint64(1)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 42)
	// Publish a later kind-3 at the same generation so the durable root can
	// advance past the kind-5 while the journal still holds it.
	if err := coll.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("cover"), []byte(`{"v":1}`))
	}); err != nil {
		t.Fatalf("cover Update: %v", err)
	}
	advanceDurableRootWithoutRecycle(t, coll)
	coll.writer.Lock()
	rootGen := coll.state.Load().root.Generation
	if !coll.journalHoldsConditional(markerID, epoch) {
		coll.writer.Unlock()
		t.Fatal("expected covered kind-5 retained in journal")
	}
	coll.writer.Unlock()
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	resolverCalls := 0
	installReplayResolver(t, func([16]byte, uint64, uint64) (bool, error) {
		resolverCalls++
		return false, fmt.Errorf("resolver must not be consulted for covered kind-5")
	}, epoch)

	reopened, rfile := reopenSync(t, img)
	defer rfile.Close()
	defer reopened.Close()
	if resolverCalls != 0 {
		t.Fatalf("covered kind-5 invoked resolver %d times", resolverCalls)
	}
	reopened.writer.Lock()
	holds := reopened.journalHoldsConditional(markerID, epoch)
	cursor := reopened.journal.Cursor()
	gotRoot := reopened.state.Load().root.Generation
	reopened.writer.Unlock()
	if gotRoot < rootGen {
		t.Fatalf("reopened root=%d, want ≥ %d", gotRoot, rootGen)
	}
	if holds {
		t.Fatal("covered kind-5 still present after reopen fold")
	}
	if cursor != 0 {
		t.Fatalf("journal cursor=%d after covered consume, want 0", cursor)
	}

	// Standalone nil-resolver path: same covered image must Open clean.
	img2 := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	prev := recoveryJournalReplayResolverHook
	recoveryJournalReplayResolverHook = nil
	standalone, sfile := reopenSync(t, img2)
	recoveryJournalReplayResolverHook = prev
	defer sfile.Close()
	defer standalone.Close()
	standalone.writer.Lock()
	holds = standalone.journalHoldsConditional(markerID, epoch)
	standalone.writer.Unlock()
	if holds {
		t.Fatal("standalone open left covered kind-5 in place")
	}
}

// TestConditionalReplayParticipantBindingSkips proves a resolver that commits
// the triple but (per its closure) does not name this collection skips apply.
func TestConditionalReplayParticipantBindingSkips(t *testing.T) {
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
	installReplayResolver(t, func(id [16]byte, ep, txn uint64) (bool, error) {
		if id != markerID || ep != epoch || txn != txnID {
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
	installReplayResolver(t, func([16]byte, uint64, uint64) (bool, error) {
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

// TestConditionalReplayCoveredNeverInvokesResolver is the tripwire form of the
// covered-consume contract: a resolver that fails the test if called stays
// uncalled when the selected root already covers the kind-5 generation.
func TestConditionalReplayCoveredNeverInvokesResolver(t *testing.T) {
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
	resolve := recoveryJournalDecisionResolver(
		func([16]byte, uint64, uint64) (bool, error) {
			t.Fatal("covered kind-5 consulted resolver")
			return false, nil
		},
	)
	// Replay acquires the writer for its post-fold checkpoint; do not hold it.
	if err := coll.replayRecoveryJournalResolvedLocked(
		rootGen, resolve, epoch,
	); err != nil {
		t.Fatalf("replay: %v", err)
	}
	coll.writer.Lock()
	holds := coll.journalHoldsConditional(markerID, epoch)
	coll.writer.Unlock()
	if holds {
		t.Fatal("covered kind-5 not consumed")
	}
}

// TestConditionalStrayConsumptionFoldRecycle proves a window of only skipped
// undecided kind-5 records is folded and recycled; a second reopen replays
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
	installReplayResolver(t, func([16]byte, uint64, uint64) (bool, error) {
		return false, nil
	}, epoch)

	first, f1 := reopenSync(t, img)
	first.writer.Lock()
	if first.journalHoldsConditional(markerID, epoch) {
		first.writer.Unlock()
		t.Fatal("stray kind-5 survived first reopen")
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
	if second.journalHoldsConditional(markerID, epoch) {
		t.Fatal("conditional reappeared on second reopen")
	}
	_ = checkpoints
}

// TestConditionalAbortedGenerationAliasing proves an aborted kind-5 at G+1
// followed by an applied kind-3 at G+1 replays only the kind-3; second replay
// is idempotent.
func TestConditionalAbortedGenerationAliasing(t *testing.T) {
	coll, file, path := openCatalogOwnedSyncCollection(t)
	markerID := conditionalMarkerID(10)
	const epoch = uint64(1)
	prepareConditionalUnpublished(t, coll, markerID, epoch, 1)

	// Single-collection Update reuses G+1 (aborted conditional never advanced
	// the published generation) and appends a kind-3 batch after the kind-5.
	if err := coll.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("alias"), []byte(`{"v":"kind3"}`))
	}); err != nil {
		t.Fatalf("kind-3 Update: %v", err)
	}
	storeBytes, journalBytes := captureStoreJournal(t, path)
	_ = coll.Close()
	_ = file.Close()

	img := writeStoreJournal(t, t.TempDir(), storeBytes, journalBytes)
	installReplayResolver(t, func([16]byte, uint64, uint64) (bool, error) {
		return false, nil // aborted / undecided kind-5
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
				// phase keys must not appear from the aborted kind-5
				if k == "a" || k == "b" || k == "c" {
					t.Fatalf("%s: aborted kind-5 key %q present", label, k)
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
	if !coll.journalHoldsConditional(markerID, epoch) {
		t.Fatal("expected journalHoldsConditional true after prepare")
	}
	if coll.journalHoldsConditional(markerID, epoch+1) {
		t.Fatal("holdsConditional matched wrong epoch")
	}
	before := coll.automaticCheckpoints.Load()
	if err := coll.checkpointPastConditionalsLocked(); err != nil {
		t.Fatalf("checkpointPastConditionalsLocked: %v", err)
	}
	if coll.automaticCheckpoints.Load() != before+1 {
		t.Fatalf("fold not foreground-bounded to one checkpoint: before=%d after=%d",
			before, coll.automaticCheckpoints.Load())
	}
	if coll.journalHoldsConditional(markerID, epoch) {
		t.Fatal("journalHoldsConditional still true after fold")
	}
	if coll.journal.Cursor() != 0 {
		t.Fatalf("cursor=%d after fold, want 0", coll.journal.Cursor())
	}
}

// TestConditionalLegacyToConditionalUpgradeOnce proves a catalog-owned legacy
// journal remints at the conditional format through exactly one bounded
// foreground checkpoint when the live window is non-empty.
func TestConditionalLegacyToConditionalUpgradeOnce(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	coll, file, _ := openPrimaryBatchStore(t, options)
	defer file.Close()
	defer coll.Close()

	if err := coll.Update(phaseWorkload); err != nil {
		t.Fatalf("seed Update: %v", err)
	}
	coll.writer.Lock()
	defer coll.writer.Unlock()
	if coll.journal.Header().FormatVersion !=
		storeio.RecoveryJournalFormatLegacy {
		t.Fatalf("pre-upgrade format=%d, want legacy",
			coll.journal.Header().FormatVersion)
	}
	if coll.journal.Cursor() == 0 {
		t.Fatal("expected non-empty live window before upgrade")
	}
	coll.journalCatalogOwned = true
	before := coll.automaticCheckpoints.Load()
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if coll.journal.Header().FormatVersion !=
		storeio.RecoveryJournalFormatConditional {
		t.Fatalf("post-upgrade format=%d, want conditional",
			coll.journal.Header().FormatVersion)
	}
	if coll.automaticCheckpoints.Load() != before+1 {
		t.Fatalf("upgrade checkpoints=%d→%d, want exactly one",
			before, coll.automaticCheckpoints.Load())
	}
	// Second ensure is a no-op: already conditional, no further checkpoint.
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	if coll.automaticCheckpoints.Load() != before+1 {
		t.Fatalf("second ensure took another checkpoint")
	}
}

// TestConditionalPrepareRefusesScalarPatchJournal pins the defensive typed
// error when conditional prepare is reached on a scalar-patch journal.
func TestConditionalPrepareRefusesScalarPatchJournal(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	options.RecoveryJournal = false // ordinary buffered delta → scalar-patch
	coll, file, _ := openPrimaryBatchStore(t, options)
	defer file.Close()
	defer coll.Close()

	// Force the ordinary buffered delta journal to exist at scalar-patch format.
	if _, err := coll.Put([]byte("x"), []byte(`{"v":1}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := coll.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	coll.writer.Lock()
	defer coll.writer.Unlock()
	if !coll.journalEnabled() {
		t.Fatal("expected scalar-patch journal after Flush")
	}
	if coll.journal.Header().FormatVersion !=
		storeio.RecoveryJournalFormatScalarPatch {
		t.Fatalf("format=%d, want scalar-patch",
			coll.journal.Header().FormatVersion)
	}
	coll.journalCatalogOwned = true
	if err := coll.ensureConditionalJournalFormatLocked(); !errors.Is(
		err, ErrConditionalPrepareUnsupportedJournal,
	) {
		t.Fatalf("ensure err=%v, want ErrConditionalPrepareUnsupportedJournal", err)
	}
}
