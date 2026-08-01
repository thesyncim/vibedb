package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

// TestFileStoreForegroundHolePunchSurvivesCrashReopen destroys every range the
// production durability hook declares free, then opens a copied crash image and
// performs a complete scan. Writing zeros rather than calling the host punch
// primitive makes the proof deterministic on filesystems that do not support
// sparse deallocation: if the candidate union contains one live byte, reopen or
// the scan fails exactly as it would after a real punched hole.
func TestFileStoreForegroundHolePunchSurvivesCrashReopen(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "hole-punch-source.vibe")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Durability = DurabilitySync
	options.ResidentBytes = 16 << 20
	options.BufferCount = 2048
	options.MaxRetiredExtents = 4096
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	zero := make([]byte, options.PageSize)
	type punchedRange struct{ offset, length uint64 }
	var punched []punchedRange
	var orderingErr error
	collection.holePunch = func(
		file *os.File, offset, length uint64,
	) (bool, error) {
		if collection.freeImageScratchInUse {
			orderingErr = errors.New("hole punch ran while fold scratch was live")
			return true, orderingErr
		}
		if collection.journal != nil &&
			collection.journal.BaseGeneration() <
				collection.committer.DurableGeneration() {
			orderingErr = fmt.Errorf(
				"punch before recycle: journal base=%d durable=%d",
				collection.journal.BaseGeneration(),
				collection.committer.DurableGeneration(),
			)
			return true, orderingErr
		}
		for written := uint64(0); written < length; {
			chunk := min(uint64(len(zero)), length-written)
			if _, err := file.WriteAt(
				zero[:chunk], int64(offset+written),
			); err != nil {
				return true, err
			}
			written += chunk
		}
		punched = append(punched, punchedRange{offset, length})
		return true, nil
	}

	const (
		keys   = 48
		rounds = 12
	)
	oracle := make(map[string][]byte, keys)
	for round := range rounds {
		// Force a real fold after punching has already borrowed and reset the
		// same fixed arena. The subsequent crash scan proves that the fold rebuilt
		// its image rather than observing stale live-length state from the hook.
		if round == rounds/2 {
			collection.freeFoldRequired = true
		}
		freeChurnRound(t, collection, keys, round)
		for key := range keys {
			name := fmt.Sprintf("key-%02d", key)
			padding := strings.Repeat("x", 120+(round*37+key*53)%300)
			document := []byte(fmt.Sprintf(
				`{"round":%d,"key":%d,"status":%q,"padding":%q}`,
				round, key,
				[3]string{"active", "idle", "paused"}[(round+key)%3],
				padding,
			))
			canonical, err := vibejson.AppendCanonicalize(nil, document)
			if err != nil {
				t.Fatal(err)
			}
			oracle[name] = canonical
		}
		for key := round % 3; key < keys; key += 3 {
			delete(oracle, fmt.Sprintf("key-%02d", key))
		}
		before := len(punched)
		if err := collection.Flush(); err != nil {
			t.Fatalf("round %d Flush: %v", round, err)
		}
		if delta := len(punched) - before; delta > fileStoreHolePunchMaxCalls {
			t.Fatalf(
				"round %d punched %d ranges, cap %d",
				round, delta, fileStoreHolePunchMaxCalls,
			)
		}
	}
	if orderingErr != nil {
		t.Fatal(orderingErr)
	}
	if len(punched) == 0 {
		t.Fatal("churn produced no foreground hole-punch candidates")
	}
	stats := collection.Stats()
	var punchedBytes uint64
	for _, extent := range punched {
		punchedBytes += extent.length
	}
	if stats.HolePunchRanges != uint64(len(punched)) ||
		stats.HolePunchBytes != punchedBytes ||
		stats.HolePunchErrors != 0 || stats.HolePunchUnsupported != 0 {
		t.Fatalf("hole-punch stats = %+v, calls=%d bytes=%d",
			stats, len(punched), punchedBytes)
	}

	// Capture the durable files before clean Close: this is the image a process
	// crash immediately after the foreground deallocation would leave.
	crashPath := filepath.Join(directory, "hole-punch-crash.vibe")
	copyFileForCrash(t, path, crashPath)
	copyFileForCrash(t, path+".rjournal", crashPath+".rjournal")
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	crashFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer crashFile.Close()
	reopened, err := Open(crashFile, options)
	if err != nil {
		t.Fatalf("reopen punched crash image: %v", err)
	}
	snapshot, err := reopened.Snapshot()
	if err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	seen := make(map[string][]byte, len(oracle))
	err = snapshot.RangeRaw(func(key, value []byte) error {
		seen[string(key)] = append([]byte(nil), value...)
		return nil
	})
	if closeErr := snapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = reopened.Close()
		t.Fatalf("scan punched crash image: %v", err)
	}
	if len(seen) != len(oracle) {
		_ = reopened.Close()
		t.Fatalf("scan rows = %d, want %d", len(seen), len(oracle))
	}
	for key, want := range oracle {
		if got, ok := seen[key]; !ok || !bytes.Equal(got, want) {
			_ = reopened.Close()
			t.Fatalf("scan %q = (%q,%v), want %q", key, got, ok, want)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func testHolePunchDisjointRanges(count int) []storeio.FreeExtent {
	const page = uint64(4 << 10)
	ranges := make([]storeio.FreeExtent, 0, count)
	for pageIndex := uint64(2); len(ranges) < count; pageIndex += 2 {
		ranges = append(ranges, storeio.FreeExtent{
			Offset:            pageIndex * page,
			Length:            page,
			RetiredGeneration: 7,
		})
	}
	return ranges
}

func TestFileStoreHolePunchExtentTracksCompletionAndReretirement(
	t *testing.T,
) {
	ranges := testHolePunchDisjointRanges(3)
	collection := &Collection{}
	var called []storeio.FreeExtent
	collection.holePunch = func(
		_ *os.File, offset, length uint64,
	) (bool, error) {
		for _, extent := range ranges {
			if extent.Offset == offset && extent.Length == length {
				called = append(called, extent)
				return true, nil
			}
		}
		t.Fatalf("unexpected punch [%d,%d)", offset, offset+length)
		return true, nil
	}

	for _, extent := range ranges {
		if !collection.punchFileStoreExtent(extent) {
			t.Fatalf("initial punch failed for %+v", extent)
		}
	}
	if got := collection.holePunchRanges.Load(); got != uint64(len(ranges)) {
		t.Fatalf("successful ranges = %d, want %d", got, len(ranges))
	}
	for _, extent := range ranges {
		if !collection.punchFileStoreExtent(extent) {
			t.Fatalf("completion hit failed for %+v", extent)
		}
	}
	if got := len(called); got != len(ranges) {
		t.Fatalf("completion cache repeated %d calls", got-len(ranges))
	}

	// Reusing then re-retiring identical physical geometry changes only the
	// generation. Exact completion state must not suppress that new lifetime.
	rereturned := ranges[len(ranges)/2]
	rereturned.RetiredGeneration++
	if !collection.punchFileStoreExtent(rereturned) {
		t.Fatal("higher-generation re-retirement failed")
	}
	if got := len(called); got != len(ranges)+1 {
		t.Fatalf("higher-generation re-retirement calls = %d, want %d", got, len(ranges)+1)
	}
}

func TestFileStoreHolePunchCursorConvergesThroughCompletionCollisions(t *testing.T) {
	const collisionCount = fileStoreHolePunchCompletionWays + 3
	var bySet [fileStoreHolePunchCompletionSlots / fileStoreHolePunchCompletionWays][]storeio.FreeExtent
	var ranges []storeio.FreeExtent
	for page := uint64(2); len(ranges) == 0; page += 2 {
		extent := storeio.FreeExtent{
			Offset: page * 4096, Length: 4096, RetiredGeneration: 11,
		}
		set := fileStoreHolePunchCompletionSet(extent) /
			fileStoreHolePunchCompletionWays
		bySet[set] = append(bySet[set], extent)
		if len(bySet[set]) == collisionCount {
			ranges = bySet[set]
		}
	}
	collection := &Collection{}
	calls := 0
	collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
		calls++
		return true, nil
	}
	cursor := uint64(0)
	for range len(ranges) {
		var candidate [1]storeio.FreeExtent
		window, next := appendHolePunchOffsetWindow(
			candidate[:0], ranges, cursor, 1,
		)
		if len(window) != 1 || !collection.punchFileStoreExtent(window[0]) {
			t.Fatalf("cursor failed at %d with %d candidates", cursor, len(window))
		}
		cursor = next
	}
	if calls != len(ranges) || cursor != fileStoreHolePunchOffsetSweepDone {
		t.Fatalf("collision sweep calls=%d/%d cursor=%d", calls, len(ranges), cursor)
	}
	for range 10 {
		var candidate [1]storeio.FreeExtent
		window, next := appendHolePunchOffsetWindow(
			candidate[:0], ranges, cursor, 1,
		)
		if len(window) != 0 || next != fileStoreHolePunchOffsetSweepDone {
			t.Fatalf("completed collision sweep resumed: len=%d next=%d", len(window), next)
		}
	}
	if calls != len(ranges) {
		t.Fatalf("stable collision sweep repeated %d calls", calls-len(ranges))
	}
}

func TestFileStoreHolePunchFragmentedSourceConvergesAndReretires(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "hole-punch-converges-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilitySync
	options.ResidentBytes = 16 << 20
	options.BufferCount = 2048
	options.MaxRetiredExtents = 4096
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	originalState := collection.durableState.Load()
	originalReusable := append([]storeio.FreeExtent(nil), collection.reusable...)
	if cap(collection.reusable) < 300 {
		_ = collection.Close()
		t.Fatalf("reusable capacity = %d, want at least 300", cap(collection.reusable))
	}
	layout, err := storeio.MutableStoreLayout(originalState.root.PageSize)
	if err != nil {
		_ = collection.Close()
		t.Fatal(err)
	}
	pageSize := uint64(originalState.root.PageSize)
	collection.reusable = collection.reusable[:300]
	for index := range collection.reusable {
		collection.reusable[index] = storeio.FreeExtent{
			Offset: layout.DataStart + uint64(2*index)*pageSize,
			Length: pageSize, RetiredGeneration: 1,
		}
	}
	syntheticState := *originalState
	syntheticState.fileEnd = collection.reusable[len(collection.reusable)-1].Offset + pageSize
	collection.durableState.Store(&syntheticState)
	clear(collection.holePunchCompletions[:])
	collection.holePunchCompletionVictim = 0
	collection.holePunchReusableCursor = 0
	collection.holePunchPendingCursor = storeio.PunchableExtentCursor{}
	collection.holePunchAbsorbedCursor = 0
	collection.holePunchCandidateSource = 0

	var calls []storeio.FreeExtent
	collection.holePunch = func(_ *os.File, offset, length uint64) (bool, error) {
		for _, extent := range collection.reusable {
			if extent.Offset == offset && extent.Length == length {
				calls = append(calls, extent)
				return true, nil
			}
		}
		t.Fatalf("unexpected punch [%d,%d)", offset, offset+length)
		return true, nil
	}
	punchBoundary := func() {
		t.Helper()
		before := len(calls)
		collection.writer.Lock()
		punchErr := collection.punchDurableFreeExtentsLocked()
		collection.writer.Unlock()
		if punchErr != nil {
			t.Fatal(punchErr)
		}
		if delta := len(calls) - before; delta > fileStoreHolePunchMaxCalls {
			t.Fatalf("foreground boundary issued %d calls, cap %d",
				delta, fileStoreHolePunchMaxCalls)
		}
	}

	for range len(collection.reusable) + 8 {
		punchBoundary()
	}
	if len(calls) != len(collection.reusable) {
		t.Fatalf("stable fragmented sweep calls = %d, want %d",
			len(calls), len(collection.reusable))
	}
	converged := len(calls)
	for range 12 {
		punchBoundary()
	}
	if len(calls) != converged {
		t.Fatalf("stable fragmented source repeated %d calls after convergence",
			len(calls)-converged)
	}

	// Simulate reuse followed by a later retirement of the exact same physical
	// geometry. mergeReusable calls this rewind at the first affected offset;
	// the newer generation must not be suppressed by the old exact completion.
	reretired := collection.reusable[17]
	reretired.RetiredGeneration++
	collection.reusable[17] = reretired
	collection.rewindHolePunchReusable(reretired.Offset)
	for range len(collection.reusable) + 8 {
		punchBoundary()
	}
	foundReretired := false
	for _, extent := range calls[converged:] {
		if extent == reretired {
			foundReretired = true
			break
		}
	}
	if !foundReretired {
		t.Fatalf("higher-generation re-retirement was suppressed: %+v", reretired)
	}
	secondConvergence := len(calls)
	for range 12 {
		punchBoundary()
	}
	if len(calls) != secondConvergence {
		t.Fatalf("mutated fragmented source repeated %d calls after convergence",
			len(calls)-secondConvergence)
	}

	collection.writer.Lock()
	clear(collection.reusable)
	collection.reusable = collection.reusable[:len(originalReusable)]
	copy(collection.reusable, originalReusable)
	collection.durableState.Store(originalState)
	collection.holePunch = nil
	collection.writer.Unlock()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreHolePunchSchedulerOptionalFailuresDoNotPoison(t *testing.T) {
	ranges := testHolePunchDisjointRanges(3)

	t.Run("unsupported disables once", func(t *testing.T) {
		collection := &Collection{}
		calls := 0
		collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
			calls++
			return false, nil
		}
		collection.punchFileStoreExtent(ranges[0])
		collection.punchFileStoreExtent(ranges[0])
		if calls != 1 || !collection.holePunchDisabled ||
			collection.holePunchUnsupported.Load() != 1 ||
			collection.holePunchErrors.Load() != 0 {
			t.Fatalf(
				"unsupported state: calls=%d disabled=%v unsupported=%d errors=%d",
				calls, collection.holePunchDisabled,
				collection.holePunchUnsupported.Load(),
				collection.holePunchErrors.Load(),
			)
		}
	})

	t.Run("error disables once", func(t *testing.T) {
		collection := &Collection{}
		injected := errors.New("injected hole-punch failure")
		calls := 0
		collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
			calls++
			return true, injected
		}
		collection.punchFileStoreExtent(ranges[0])
		if calls != 1 || !collection.holePunchDisabled ||
			collection.holePunchErrors.Load() != 1 ||
			collection.holePunchRanges.Load() != 0 {
			t.Fatalf(
				"error state: calls=%d disabled=%v errors=%d successes=%d",
				calls, collection.holePunchDisabled,
				collection.holePunchErrors.Load(),
				collection.holePunchRanges.Load(),
			)
		}
		collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
			calls++
			return true, nil
		}
		collection.punchFileStoreExtent(ranges[0])
		if calls != 1 || collection.holePunchRanges.Load() != 0 {
			t.Fatalf("disabled error state: calls=%d successes=%d",
				calls, collection.holePunchRanges.Load())
		}
	})
}

func TestFileStorePinnedSnapshotNarrowsForegroundHolePunch(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "hole-punch-reader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilitySync
	options.ResidentBytes = 16 << 20
	options.BufferCount = 2048
	options.MaxRetiredExtents = 4096
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := collection.Close(); closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	}()

	type punchedRange struct{ offset, length uint64 }
	var punched []punchedRange
	collection.holePunch = func(
		_ *os.File, offset, length uint64,
	) (bool, error) {
		punched = append(punched, punchedRange{offset, length})
		return true, nil
	}
	for round := range 6 {
		freeChurnRound(t, collection, 32, round)
		if err := collection.Flush(); err != nil {
			t.Fatalf("warmup Flush %d: %v", round, err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	pinnedGeneration := snapshot.Generation()
	defer snapshot.Close()

	freeChurnRound(t, collection, 32, 6)

	// Forget prior successful syscalls so this pass must revisit the already-safe
	// reusable set. The active snapshot must narrow only pending candidates; it
	// must not veto the pass as a whole.
	clear(collection.holePunchCompletions[:])
	collection.holePunchCompletionVictim = 0
	punched = punched[:0]
	if err := collection.Flush(); err != nil {
		t.Fatalf("Flush with pinned snapshot: %v", err)
	}
	if len(punched) == 0 {
		t.Fatal("pinned snapshot vetoed every older-safe hole-punch candidate")
	}
	readerReachable := collection.reclaimer.AppendPending(nil)
	readerReachable = slices.DeleteFunc(readerReachable, func(extent storeio.FreeExtent) bool {
		return extent.RetiredGeneration < pinnedGeneration
	})
	if len(readerReachable) == 0 {
		t.Fatal("Flush lost every reader-reachable retirement")
	}
	for _, call := range punched {
		callEnd := call.offset + call.length
		for _, pinned := range readerReachable {
			pinnedEnd := pinned.Offset + pinned.Length
			if call.offset < pinnedEnd && pinned.Offset < callEnd {
				t.Fatalf(
					"punched reader-reachable extent: call=[%d,%d) pinned=%+v snapshotGeneration=%d",
					call.offset, callEnd, pinned, pinnedGeneration,
				)
			}
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreHolePunchFailureIsOptionalAtFlush(t *testing.T) {
	for _, test := range []struct {
		name        string
		unsupported bool
	}{
		{name: "filesystem error"},
		{name: "unsupported", unsupported: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "hole-punch-optional-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			options := testFileStoreOptions()
			options.Durability = DurabilitySync
			options.ResidentBytes = 16 << 20
			options.BufferCount = 2048
			options.MaxRetiredExtents = 4096
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			injected := errors.New("injected filesystem deallocation failure")
			collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
				calls++
				if test.unsupported {
					return false, nil
				}
				return true, injected
			}
			for round := 0; round < 10 && calls == 0; round++ {
				freeChurnRound(t, collection, 24, round)
				if err := collection.Flush(); err != nil {
					t.Fatalf("Flush surfaced optional failure: %v", err)
				}
			}
			if calls == 0 {
				t.Fatal("churn produced no hole-punch attempt")
			}
			if failure := collection.PersistenceError(); failure != nil {
				t.Fatalf("optional failure poisoned collection: %v", failure)
			}
			callsAfterFailure := calls
			if _, err := collection.Put(
				[]byte("still-writable"), []byte(`{"ok":true}`),
			); err != nil {
				t.Fatalf("mutation after optional failure: %v", err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatalf("Flush after optional failure: %v", err)
			}
			if calls != callsAfterFailure || !collection.holePunchDisabled {
				t.Fatalf(
					"disabled retry state: calls=%d before=%d disabled=%v",
					calls, callsAfterFailure, collection.holePunchDisabled,
				)
			}
			if test.unsupported && collection.holePunchUnsupported.Load() != 1 {
				t.Fatalf("unsupported count = %d, want 1",
					collection.holePunchUnsupported.Load())
			}
			if !test.unsupported && collection.holePunchErrors.Load() != 1 {
				t.Fatalf("error count = %d, want 1",
					collection.holePunchErrors.Load())
			}
			// Close takes the same bounded foreground path and must remain
			// best-effort under the injected failure too.
			if err := collection.Close(); err != nil {
				t.Fatalf("Close surfaced optional failure: %v", err)
			}
		})
	}
}

func TestFileStoreHolePunchCandidatePreparationIsFixedWindow(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "hole-punch-window-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.ResidentBytes = 16 << 20
	options.BufferCount = 2048
	options.MaxRetiredExtents = 4096
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	selectionChecks := 0
	var fenceOrderingErr error
	collection.holePunchCandidateFenced = func() {
		selectionChecks++
		if !collection.readEpochs.Diverted() {
			fenceOrderingErr = errors.New(
				"candidate selection ran without direct-reader fence",
			)
			return
		}
		if collection.snapshotGate.TryRLock() {
			collection.snapshotGate.RUnlock()
			fenceOrderingErr = errors.New(
				"candidate selection ran without snapshot gate",
			)
		}
	}

	originalState := collection.durableState.Load()
	originalReusableLen := len(collection.reusable)
	if cap(collection.reusable) < fileStoreHolePunchCandidateWindow+1 {
		_ = collection.Close()
		t.Fatalf("reusable capacity = %d, want at least %d",
			cap(collection.reusable), fileStoreHolePunchCandidateWindow+1)
	}
	layout, err := storeio.MutableStoreLayout(originalState.root.PageSize)
	if err != nil {
		_ = collection.Close()
		t.Fatal(err)
	}
	pageSize := uint64(originalState.root.PageSize)
	collection.reusable = collection.reusable[:fileStoreHolePunchCandidateWindow+1]
	for index := range collection.reusable {
		collection.reusable[index] = storeio.FreeExtent{
			Offset: layout.DataStart + uint64(2*index)*pageSize,
			Length: pageSize, RetiredGeneration: 1,
		}
	}
	// A corrupt entry immediately outside the first fixed window proves that
	// the first pass neither copied, sorted, nor validated the unbounded tail.
	collection.reusable[fileStoreHolePunchCandidateWindow].Length = 0
	syntheticState := *originalState
	syntheticState.fileEnd = collection.reusable[len(collection.reusable)-1].Offset + pageSize
	collection.durableState.Store(&syntheticState)

	calls := 0
	collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
		if collection.readEpochs.Diverted() {
			fenceOrderingErr = errors.New(
				"hole-punch syscall ran under direct-reader fence",
			)
			return true, fenceOrderingErr
		}
		if !collection.snapshotGate.TryRLock() {
			fenceOrderingErr = errors.New(
				"hole-punch syscall ran under snapshot gate",
			)
			return true, fenceOrderingErr
		}
		collection.snapshotGate.RUnlock()
		calls++
		return true, nil
	}
	collection.writer.Lock()
	err = collection.punchDurableFreeExtentsLocked()
	collection.writer.Unlock()
	if err != nil {
		t.Fatalf("first bounded pass: %v", err)
	}
	if fenceOrderingErr != nil {
		t.Fatal(fenceOrderingErr)
	}
	if selectionChecks != 1 {
		t.Fatalf("fenced candidate selections = %d, want 1", selectionChecks)
	}
	if calls != fileStoreHolePunchMaxCalls {
		t.Fatalf("first bounded pass calls = %d, want %d",
			calls, fileStoreHolePunchMaxCalls)
	}
	firstCalls := calls
	collection.writer.Lock()
	err = collection.punchDurableFreeExtentsLocked()
	collection.writer.Unlock()
	if !errors.Is(err, storeio.ErrFreeLogCorrupt) {
		t.Fatalf("second rotating window error = %v, want %v",
			err, storeio.ErrFreeLogCorrupt)
	}
	if calls != firstCalls {
		t.Fatalf("validation issued destructive calls: before=%d after=%d",
			firstCalls, calls)
	}
	if fenceOrderingErr != nil {
		t.Fatal(fenceOrderingErr)
	}
	if selectionChecks != 2 {
		t.Fatalf("fenced candidate selections = %d, want 2", selectionChecks)
	}

	collection.writer.Lock()
	clear(collection.reusable[originalReusableLen:])
	collection.reusable = collection.reusable[:originalReusableLen]
	collection.durableState.Store(originalState)
	collection.holePunchCandidateFenced = nil
	collection.writer.Unlock()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreAsyncAndChainFenceForegroundHolePunchHooks(t *testing.T) {
	for _, test := range []struct {
		name       string
		chainFence bool
		close      bool
	}{
		{name: "async flush"},
		{name: "chain-fence flush", chainFence: true},
		{name: "async close", close: true},
		{name: "chain-fence close", chainFence: true, close: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "hole-punch-lane-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			options := testFileStoreOptions()
			options.Durability = DurabilityAsyncVisible
			options.ResidentBytes = 16 << 20
			options.BufferCount = 2048
			options.MaxRetiredExtents = 4096
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			if test.chainFence {
				if err := collection.Close(); err != nil {
					t.Fatal(err)
				}
				options.Durability = DurabilitySync
				collection, err = Open(file, options)
				if err != nil {
					t.Fatal(err)
				}
				if !collection.chainFenceSync() {
					_ = collection.Close()
					t.Fatal("async-created store did not reopen on chain-fence sync lane")
				}
			}
			if collection.journalEnabled() {
				_ = collection.Close()
				t.Fatal("async/chain-fence lane unexpectedly opened a journal")
			}

			originalState := collection.durableState.Load()
			originalReusableLen := len(collection.reusable)
			if cap(collection.reusable) == 0 {
				_ = collection.Close()
				t.Fatal("no reusable candidate storage")
			}
			layout, err := storeio.MutableStoreLayout(originalState.root.PageSize)
			if err != nil {
				_ = collection.Close()
				t.Fatal(err)
			}
			pageSize := uint64(originalState.root.PageSize)
			collection.reusable = collection.reusable[:originalReusableLen+1]
			collection.reusable[originalReusableLen] = storeio.FreeExtent{
				Offset: layout.DataStart, Length: pageSize,
				RetiredGeneration: originalState.root.Generation,
			}
			syntheticState := *originalState
			syntheticState.fileEnd = max(
				syntheticState.fileEnd, layout.DataStart+pageSize,
			)
			collection.durableState.Store(&syntheticState)

			calls := 0
			var orderingErr error
			collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
				if collection.writer.TryLock() {
					collection.writer.Unlock()
					orderingErr = errors.New("hole punch ran without writer lock")
					return true, orderingErr
				}
				state := collection.durableState.Load()
				durable := collection.committer.DurableGeneration()
				if state == nil || state.root.Generation < durable ||
					collection.committer.FallbackGeneration() == 0 {
					orderingErr = fmt.Errorf(
						"unsettled hook state=%v durable=%d fallback=%d",
						state, durable, collection.committer.FallbackGeneration(),
					)
					return true, orderingErr
				}
				calls++
				return true, nil
			}

			if test.close {
				if err := collection.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := collection.Flush(); err != nil {
					_ = collection.Close()
					t.Fatal(err)
				}
				collection.writer.Lock()
				clear(collection.reusable[originalReusableLen:])
				collection.reusable = collection.reusable[:originalReusableLen]
				collection.durableState.Store(originalState)
				collection.writer.Unlock()
				if err := collection.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if orderingErr != nil {
				t.Fatal(orderingErr)
			}
			if calls != 1 {
				t.Fatalf("foreground hook calls = %d, want 1", calls)
			}
		})
	}
}
