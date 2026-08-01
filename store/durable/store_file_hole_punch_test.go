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
	attempts := 0
	injectedSecondFailure := errors.New("injected second hole-punch failure")
	collection.holePunch = func(
		file *os.File, offset, length uint64,
	) (bool, error) {
		attempts++
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
		// The first range is really destroyed; the second call then fails. Flush
		// must keep that partial optional success non-poisoning, and the crash copy
		// below must still reopen and scan exactly.
		if attempts == 2 {
			return true, injectedSecondFailure
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
		if delta := len(punched) - before; delta > fileStoreHolePunchSelectedCalls {
			t.Fatalf(
				"round %d punched %d ranges, cap %d",
				round, delta, fileStoreHolePunchSelectedCalls,
			)
		}
	}
	if orderingErr != nil {
		t.Fatal(orderingErr)
	}
	if len(punched) == 0 {
		t.Fatal("churn produced no foreground hole-punch candidates")
	}
	if attempts != 2 {
		t.Fatalf("hole-punch attempts = %d, want one success then one failure", attempts)
	}
	stats := collection.Stats()
	var punchedBytes uint64
	for _, extent := range punched {
		punchedBytes += extent.length
	}
	if stats.HolePunchRanges != uint64(len(punched)) ||
		stats.HolePunchBytes != punchedBytes ||
		stats.HolePunchErrors != 1 || stats.HolePunchUnsupported != 0 {
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

func testHolePunchSchedulerCollection(t *testing.T) (*os.File, *Collection) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "hole-punch-scheduler-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.ResidentBytes = 16 << 20
	options.BufferCount = 2048
	options.MaxRetiredExtents = 4096
	collection, err := Create(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
		return true, nil
	}
	for generation := range 2 {
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("scheduler-seed-%d", generation)),
			[]byte(fmt.Sprintf(`{"generation":%d}`, generation)),
		); err != nil {
			_ = collection.Close()
			_ = file.Close()
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			_ = collection.Close()
			_ = file.Close()
			t.Fatal(err)
		}
	}
	reclaimer, err := storeio.NewExtentReclaimer(
		collection.leases,
		storeio.ExtentReclaimerOptions{
			MaxRetiredExtents: 4096,
			Epochs:            collection.readEpochs,
		},
	)
	if err != nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	collection.reclaimer = reclaimer
	clear(collection.retirementAbsorbed)
	collection.retirementAbsorbed = collection.retirementAbsorbed[:0]
	collection.holePunch = nil
	return file, collection
}

func closeSyntheticHolePunchCollection(
	t *testing.T, file *os.File, collection *Collection,
) {
	t.Helper()
	collection.writer.Lock()
	collection.holePunchDisabled = true
	collection.writer.Unlock()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreHolePunchFairSourcesAdvanceIndependently(t *testing.T) {
	file, collection := testHolePunchSchedulerCollection(t)
	defer closeSyntheticHolePunchCollection(t, file, collection)

	state := collection.durableState.Load()
	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(state.root.PageSize)
	const reusableCount = fileStoreHolePunchCandidateRuns + 8
	if cap(collection.reusable) < reusableCount || cap(collection.retirementAbsorbed) == 0 {
		t.Fatalf("scheduler scratch capacities reusable=%d absorbed=%d",
			cap(collection.reusable), cap(collection.retirementAbsorbed))
	}
	reclaimer, err := storeio.NewExtentReclaimer(
		collection.leases,
		storeio.ExtentReclaimerOptions{
			MaxRetiredExtents: 32,
			Epochs:            collection.readEpochs,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	collection.reclaimer = reclaimer
	collection.reusable = collection.reusable[:reusableCount]
	for rank := range collection.reusable {
		collection.reusable[rank] = storeio.FreeExtent{
			Offset: layout.DataStart + uint64(rank*2)*pageSize,
			Length: pageSize, RetiredGeneration: 1,
		}
	}
	pending := storeio.FreeExtent{
		Offset: layout.DataStart + uint64(reusableCount*2+4)*pageSize,
		Length: pageSize, RetiredGeneration: 1,
	}
	if err := reclaimer.RetireBatch([]storeio.FreeExtent{pending}); err != nil {
		t.Fatal(err)
	}
	absorbed := storeio.FreeExtent{
		Offset: pending.Offset + 4*pageSize,
		Length: pageSize, RetiredGeneration: 1,
	}
	collection.retirementAbsorbed = append(
		collection.retirementAbsorbed[:0], absorbed,
	)
	synthetic := *state
	synthetic.fileEnd = absorbed.Offset + absorbed.Length
	collection.durableState.Store(&synthetic)
	collection.holePunchReusableCursor = 0
	collection.holePunchPendingCursor = storeio.PunchableExtentCursor{}
	collection.holePunchAbsorbedCursor = 0
	collection.holePunchCandidateSource = 0
	clear(collection.holePunchCompletions[:])
	clear(collection.holePunchPartials[:])

	var calls []storeio.FreeExtent
	collection.holePunch = func(_ *os.File, offset, length uint64) (bool, error) {
		calls = append(calls, storeio.FreeExtent{Offset: offset, Length: length})
		return true, nil
	}
	collection.writer.Lock()
	err = collection.punchDurableFreeExtentsLocked()
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) > fileStoreHolePunchSelectedCalls {
		t.Fatalf("fair pass calls=%d cap=%d", len(calls), fileStoreHolePunchSelectedCalls)
	}
	foundPending, foundAbsorbed := false, false
	for _, call := range calls {
		foundPending = foundPending || call.Offset == pending.Offset
		foundAbsorbed = foundAbsorbed || call.Offset == absorbed.Offset
	}
	if !foundPending || !foundAbsorbed {
		t.Fatalf("fragmented reusable source starved peers: pending=%v absorbed=%v calls=%+v",
			foundPending, foundAbsorbed, calls)
	}
	if collection.holePunchReusableCursor != 0 ||
		collection.holePunchAbsorbedCursor != 1 {
		t.Fatalf("independent cursors reusable=%d absorbed=%d",
			collection.holePunchReusableCursor, collection.holePunchAbsorbedCursor)
	}

	// Removing the completion optimization must not make either completed peer
	// reappear: their source cursors advanced even though reusable stayed pinned.
	clear(collection.holePunchCompletions[:])
	calls = calls[:0]
	collection.writer.Lock()
	err = collection.punchDurableFreeExtentsLocked()
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		if call.Offset == pending.Offset || call.Offset == absorbed.Offset {
			t.Fatalf("advanced peer cursor repeated %+v", call)
		}
	}
}

func TestFileStoreHolePunchSoleSourceUsesFullDiscoveryBudget(t *testing.T) {
	file, collection := testHolePunchSchedulerCollection(t)
	defer closeSyntheticHolePunchCollection(t, file, collection)

	state := collection.durableState.Load()
	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(state.root.PageSize)
	if cap(collection.reusable) < fileStoreHolePunchCandidateWindow {
		t.Fatalf("reusable capacity=%d want=%d",
			cap(collection.reusable), fileStoreHolePunchCandidateWindow)
	}
	collection.reusable = collection.reusable[:fileStoreHolePunchCandidateWindow]
	const identitiesPerRun = fileStoreHolePunchCandidateWindow /
		fileStoreHolePunchCandidateRuns
	for rank := range collection.reusable {
		run := rank / identitiesPerRun
		page := rank % identitiesPerRun
		collection.reusable[rank] = storeio.FreeExtent{
			Offset: layout.DataStart +
				uint64(run*(identitiesPerRun+1)+page)*pageSize,
			Length: pageSize, RetiredGeneration: 1,
		}
	}
	// Corruption in the final identity of run 64 is a deterministic witness that
	// discovery reached both hard ceilings. The former fixed-third partition saw
	// only 342 identities/22 runs and therefore could not observe this entry.
	collection.reusable[len(collection.reusable)-1].Length = 0
	last := collection.reusable[len(collection.reusable)-1]
	synthetic := *state
	synthetic.fileEnd = last.Offset + pageSize
	collection.durableState.Store(&synthetic)
	collection.holePunchReusableCursor = 0
	collection.holePunchPendingCursor = storeio.PunchableExtentCursor{}
	collection.holePunchAbsorbedCursor = 0
	collection.holePunchCandidateSource = 0
	clear(collection.holePunchCompletions[:])
	clear(collection.holePunchPartials[:])

	calls := 0
	collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
		calls++
		return true, nil
	}
	collection.writer.Lock()
	err = collection.punchDurableFreeExtentsLocked()
	collection.writer.Unlock()
	if !errors.Is(err, storeio.ErrFreeLogCorrupt) {
		t.Fatalf("full-window tail validation error=%v", err)
	}
	if calls != 0 {
		t.Fatalf("tail validation issued %d destructive calls", calls)
	}
}

func TestFileStoreHolePunchRedistributesShortPeerQuota(t *testing.T) {
	file, collection := testHolePunchSchedulerCollection(t)
	defer closeSyntheticHolePunchCollection(t, file, collection)

	state := collection.durableState.Load()
	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(state.root.PageSize)
	const reusableCount = fileStoreHolePunchCandidateWindow - 1
	if cap(collection.reusable) < reusableCount ||
		cap(collection.retirementAbsorbed) == 0 {
		t.Fatalf("scheduler capacities reusable=%d absorbed=%d",
			cap(collection.reusable), cap(collection.retirementAbsorbed))
	}
	collection.reusable = collection.reusable[:reusableCount]
	for rank := range collection.reusable {
		collection.reusable[rank] = storeio.FreeExtent{
			Offset: layout.DataStart + uint64(rank)*pageSize,
			Length: pageSize, RetiredGeneration: 1,
		}
	}
	peer := storeio.FreeExtent{
		Offset: layout.DataStart + uint64(reusableCount+2)*pageSize,
		Length: pageSize, RetiredGeneration: 1,
	}
	collection.retirementAbsorbed = append(
		collection.retirementAbsorbed[:0], peer,
	)
	synthetic := *state
	synthetic.fileEnd = peer.Offset + peer.Length
	collection.durableState.Store(&synthetic)
	collection.holePunchReusableCursor = 0
	collection.holePunchPendingCursor = storeio.PunchableExtentCursor{}
	collection.holePunchAbsorbedCursor = 0
	collection.holePunchCandidateSource = 0
	clear(collection.holePunchCompletions[:])
	clear(collection.holePunchPartials[:])

	peerCalled := false
	collection.holePunch = func(_ *os.File, offset, _ uint64) (bool, error) {
		peerCalled = peerCalled || offset == peer.Offset
		return true, nil
	}
	collection.writer.Lock()
	err = collection.punchDurableFreeExtentsLocked()
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !peerCalled {
		t.Fatal("short peer was not punched")
	}
	if collection.holePunchReusableCursor != fileStoreHolePunchOffsetSweepDone ||
		collection.holePunchAbsorbedCursor != 1 {
		t.Fatalf("redistributed cursors reusable=%d absorbed=%d",
			collection.holePunchReusableCursor, collection.holePunchAbsorbedCursor)
	}
}

func TestFileStoreHolePunchOversizedIdentityChunksToCompletion(t *testing.T) {
	file, collection := testHolePunchSchedulerCollection(t)
	defer closeSyntheticHolePunchCollection(t, file, collection)

	state := collection.durableState.Load()
	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(state.root.PageSize)
	parent := storeio.FreeExtent{
		Offset:            layout.DataStart,
		Length:            fileStoreHolePunchSelectedBytes + 3*pageSize,
		RetiredGeneration: 1,
	}
	collection.reusable = append(collection.reusable[:0], parent)
	synthetic := *state
	synthetic.fileEnd = parent.Offset + parent.Length
	collection.durableState.Store(&synthetic)
	collection.holePunchReusableCursor = 0
	collection.holePunchPendingCursor = storeio.PunchableExtentCursor{}
	collection.holePunchAbsorbedCursor = 0
	collection.holePunchCandidateSource = 0
	clear(collection.holePunchCompletions[:])
	clear(collection.holePunchPartials[:])

	var calls []storeio.FreeExtent
	collection.holePunch = func(_ *os.File, offset, length uint64) (bool, error) {
		calls = append(calls, storeio.FreeExtent{Offset: offset, Length: length})
		return true, nil
	}
	for boundary := 0; boundary < 8 &&
		collection.holePunchReusableCursor != fileStoreHolePunchOffsetSweepDone; boundary++ {
		before := len(calls)
		collection.writer.Lock()
		err = collection.punchDurableFreeExtentsLocked()
		collection.writer.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		var boundaryBytes uint64
		for _, call := range calls[before:] {
			boundaryBytes += call.Length
		}
		if len(calls)-before > fileStoreHolePunchSelectedCalls ||
			boundaryBytes > fileStoreHolePunchSelectedBytes {
			t.Fatalf("boundary calls=%d bytes=%d", len(calls)-before, boundaryBytes)
		}
		if boundary == 0 && collection.holePunchReusableCursor != 0 {
			t.Fatalf("partial identity advanced cursor=%d",
				collection.holePunchReusableCursor)
		}
	}
	if collection.holePunchReusableCursor != fileStoreHolePunchOffsetSweepDone {
		t.Fatalf("oversized identity did not converge: cursor=%d partial=%+v",
			collection.holePunchReusableCursor, collection.holePunchPartials[0])
	}
	var punched uint64
	next := parent.Offset
	for _, call := range calls {
		if call.Offset != next || call.Length == 0 {
			t.Fatalf("non-contiguous oversized chunks: next=%d call=%+v", next, call)
		}
		next += call.Length
		punched += call.Length
	}
	if punched != parent.Length || collection.holePunchPartials[0] != (fileStoreHolePunchPartial{}) ||
		collection.holePunchSkippedRanges.Load() != 0 {
		t.Fatalf("oversized completion bytes=%d/%d partial=%+v skipped=%d",
			punched, parent.Length, collection.holePunchPartials[0],
			collection.holePunchSkippedRanges.Load())
	}
}

func TestFileStoreHolePunchRecordsPlannerAuthorityExactly(t *testing.T) {
	file, collection := testHolePunchSchedulerCollection(t)
	defer closeSyntheticHolePunchCollection(t, file, collection)

	current := collection.committer.DurableGeneration()
	if current < 2 {
		t.Fatalf("durable generation=%d, want >=2", current)
	}
	state := collection.durableState.Load()
	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint64(state.root.PageSize)
	collection.reusable = append(collection.reusable[:0], storeio.FreeExtent{
		Offset: layout.DataStart, RetiredGeneration: 1,
	})
	synthetic := *state
	synthetic.fileEnd = layout.DataStart + pageSize
	collection.durableState.Store(&synthetic)
	collection.holePunchGeneration = 0
	collection.holePunchReusableCursor = 0
	clear(collection.holePunchCompletions[:])

	calls := 0
	collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
		calls++
		return true, nil
	}
	collection.writer.Lock()
	err = collection.punchNewPhysicalGenerationLocked(current - 1)
	collection.writer.Unlock()
	if !errors.Is(err, storeio.ErrFreeLogCorrupt) ||
		collection.holePunchGeneration != 0 || calls != 0 {
		t.Fatalf("hard planner error=%v authority=%d calls=%d",
			err, collection.holePunchGeneration, calls)
	}
	collection.reusable[0].Length = pageSize
	collection.writer.Lock()
	err = collection.punchNewPhysicalGenerationLocked(current - 1)
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if collection.holePunchGeneration != current || calls != 1 {
		t.Fatalf("recorded authority=%d current=%d calls=%d",
			collection.holePunchGeneration, current, calls)
	}
	collection.writer.Lock()
	err = collection.punchNewPhysicalGenerationLocked(current)
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("same planner authority repeated: calls=%d", calls)
	}
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
	const collisionCount = 7
	var bySet [fileStoreHolePunchCompletionSlots][]storeio.FreeExtent
	var ranges []storeio.FreeExtent
	for page := uint64(2); len(ranges) == 0; page += 2 {
		extent := storeio.FreeExtent{
			Offset: page * 4096, Length: 4096, RetiredGeneration: 11,
		}
		set := fileStoreHolePunchCompletionSet(extent)
		bySet[set] = append(bySet[set], extent)
		if len(bySet[set]) == collisionCount {
			ranges = bySet[set]
		}
	}
	collection := &Collection{}
	calls := 0
	collection.holePunch = func(_ *os.File, offset, length uint64) (bool, error) {
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
	fencedSelections := 0
	collection.holePunchCandidateFenced = func() {
		fencedSelections++
	}
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
		if delta := len(calls) - before; delta > fileStoreHolePunchSelectedCalls {
			t.Fatalf("foreground boundary issued %d calls, cap %d",
				delta, fileStoreHolePunchSelectedCalls)
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
	collection.holePunchCandidateFenced = nil
	collection.writer.Unlock()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreHolePunchCoalescedBudgetCursorAndReretirement(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "hole-punch-coalesced-*")
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
	const (
		runCount    = fileStoreHolePunchCandidateRuns + 2
		pagesPerRun = 3
	)
	identityCount := runCount * pagesPerRun
	if cap(collection.reusable) < identityCount {
		_ = collection.Close()
		t.Fatalf("reusable capacity = %d, want at least %d",
			cap(collection.reusable), identityCount)
	}
	layout, err := storeio.MutableStoreLayout(originalState.root.PageSize)
	if err != nil {
		_ = collection.Close()
		t.Fatal(err)
	}
	pageSize := uint64(originalState.root.PageSize)
	collection.reusable = collection.reusable[:identityCount]
	for run := range runCount {
		runStart := layout.DataStart + uint64(run*(pagesPerRun+1))*pageSize
		for page := range pagesPerRun {
			collection.reusable[run*pagesPerRun+page] = storeio.FreeExtent{
				Offset: runStart + uint64(page)*pageSize,
				Length: pageSize, RetiredGeneration: 1,
			}
		}
	}
	syntheticState := *originalState
	last := collection.reusable[len(collection.reusable)-1]
	syntheticState.fileEnd = last.Offset + last.Length
	collection.durableState.Store(&syntheticState)
	clear(collection.holePunchCompletions[:])
	collection.holePunchCompletionVictim = 0
	collection.holePunchReusableCursor = 0
	collection.holePunchPendingCursor = storeio.PunchableExtentCursor{}
	collection.holePunchAbsorbedCursor = 0
	collection.holePunchCandidateSource = 0

	var calls []storeio.FreeExtent
	fencedSelections := 0
	collection.holePunchCandidateFenced = func() {
		fencedSelections++
	}
	collection.holePunch = func(_ *os.File, offset, length uint64) (bool, error) {
		calls = append(calls, storeio.FreeExtent{Offset: offset, Length: length})
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
		if delta := len(calls) - before; delta > fileStoreHolePunchSelectedCalls {
			t.Fatalf("foreground boundary issued %d calls, cap %d",
				delta, fileStoreHolePunchSelectedCalls)
		}
	}

	punchBoundary()
	if len(calls) != fileStoreHolePunchSelectedCalls {
		t.Fatalf("first boundary calls = %d, want %d",
			len(calls), fileStoreHolePunchSelectedCalls)
	}
	if collection.holePunchReusableCursor != 0 {
		t.Fatalf("partial window advanced cursor to %d",
			collection.holePunchReusableCursor)
	}
	for index, call := range calls {
		wantOffset := collection.reusable[index*pagesPerRun].Offset
		if call.Offset != wantOffset || call.Length != pagesPerRun*pageSize {
			t.Fatalf("coalesced call %d = %+v, want offset=%d length=%d",
				index, call, wantOffset, pagesPerRun*pageSize)
		}
	}

	for len(calls) < runCount {
		punchBoundary()
	}
	if collection.holePunchReusableCursor != fileStoreHolePunchOffsetSweepDone {
		t.Fatalf("convergence calls=%d/%d cursor=%d",
			len(calls), runCount, collection.holePunchReusableCursor)
	}
	converged := len(calls)
	punchBoundary()
	if len(calls) != converged {
		t.Fatalf("settled source repeated %d calls", len(calls)-converged)
	}

	// Re-retire the middle identity of the final range. Its physical union has
	// already been punched, but the newer lifetime must rewind inside that union
	// and issue a fresh call rather than being hidden by synthetic completion.
	rerank := (runCount-1)*pagesPerRun + 1
	rereturned := collection.reusable[rerank]
	rereturned.RetiredGeneration++
	collection.reusable[rerank] = rereturned
	collection.rewindHolePunchReusable(rereturned.Offset)
	punchBoundary()
	if len(calls) != converged+1 {
		t.Fatalf("coalesced re-retirement calls = %d, want %d",
			len(calls), converged+1)
	}
	lastCall := calls[len(calls)-1]
	if lastCall.Offset != rereturned.Offset || lastCall.Length != 2*pageSize {
		t.Fatalf("coalesced re-retirement call = %+v, want offset=%d length=%d",
			lastCall, rereturned.Offset, 2*pageSize)
	}
	punchBoundary()
	if len(calls) != converged+1 {
		t.Fatalf("re-retired source repeated %d calls", len(calls)-(converged+1))
	}

	collection.writer.Lock()
	clear(collection.reusable)
	collection.reusable = collection.reusable[:len(originalReusable)]
	copy(collection.reusable, originalReusable)
	collection.durableState.Store(originalState)
	collection.holePunch = nil
	collection.holePunchCandidateFenced = nil
	collection.writer.Unlock()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreHolePunchPendingPrefixRefetchResumesExactly(t *testing.T) {
	leases, err := storeio.NewGenerationLeases(
		storeio.GenerationLeaseOptions{MaxLeases: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := storeio.NewExtentReclaimer(
		leases, storeio.ExtentReclaimerOptions{MaxRetiredExtents: 512},
	)
	if err != nil {
		t.Fatal(err)
	}
	const (
		runs        = fileStoreHolePunchCandidateRuns + 2
		pagesPerRun = 3
		pageSize    = uint64(4 << 10)
	)
	extents := make([]storeio.FreeExtent, 0, runs*pagesPerRun)
	for run := range runs {
		start := uint64(2+run*(pagesPerRun+1)) * pageSize
		for page := range pagesPerRun {
			extents = append(extents, storeio.FreeExtent{
				Offset: start + uint64(page)*pageSize,
				Length: pageSize, RetiredGeneration: 1,
			})
		}
	}
	if err := reclaimer.RetireBatch(extents); err != nil {
		t.Fatal(err)
	}

	var cursor storeio.PunchableExtentCursor
	window := make([]storeio.FreeExtent, 0, fileStoreHolePunchCandidateWindow)
	fetched, _, _ := reclaimer.AppendPunchableAfter(
		window, 10, 10, cursor, cap(window),
	)
	kept, physicalRanges := fileStoreHolePunchCandidatePrefix(
		fetched, make([]bool, len(fetched)), fileStoreHolePunchCandidateRuns,
	)
	wantKept := fileStoreHolePunchCandidateRuns * pagesPerRun
	if kept != wantKept || physicalRanges != fileStoreHolePunchCandidateRuns {
		t.Fatalf("pending prefix kept=%d ranges=%d, want %d/%d",
			kept, physicalRanges, wantKept, fileStoreHolePunchCandidateRuns)
	}
	// Production discards the over-fetched suffix and refetches exactly the
	// assigned prefix from the old opaque cursor. The returned cursor must then
	// resume at run six, neither revisiting nor skipping an identity.
	assigned, cursor, done := reclaimer.AppendPunchableAfter(
		window[:0], 10, 10, cursor, kept,
	)
	if done || !slices.Equal(assigned, extents[:kept]) {
		t.Fatalf("assigned pending prefix done=%v len=%d want=%d",
			done, len(assigned), kept)
	}
	remainder, _, done := reclaimer.AppendPunchableAfter(
		window[:0], 10, 10, cursor, cap(window),
	)
	if !done || !slices.Equal(remainder, extents[kept:]) {
		t.Fatalf("pending remainder done=%v len=%d want=%d",
			done, len(remainder), len(extents)-kept)
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
			Offset: layout.DataStart + uint64(index)*pageSize,
			Length: pageSize, RetiredGeneration: 1,
		}
	}
	// The first fixed window is one physically adjacent range. A corrupt entry
	// immediately after it proves that candidate preparation coalesces the
	// bounded prefix without copying, sorting, or validating the unbounded tail.
	collection.reusable[fileStoreHolePunchCandidateWindow].Length = 0
	syntheticState := *originalState
	syntheticState.fileEnd = collection.reusable[len(collection.reusable)-1].Offset + pageSize
	collection.durableState.Store(&syntheticState)

	calls := 0
	var calledOffset, calledLength uint64
	collection.holePunch = func(
		_ *os.File, offset, length uint64,
	) (bool, error) {
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
		calledOffset, calledLength = offset, length
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
	if calls != 1 {
		t.Fatalf("first bounded adjacent pass calls = %d, want 1", calls)
	}
	if calledOffset != layout.DataStart ||
		calledLength != uint64(fileStoreHolePunchCandidateWindow)*pageSize {
		t.Fatalf("coalesced call = [%d,%d), want [%d,%d)",
			calledOffset, calledOffset+calledLength, layout.DataStart,
			layout.DataStart+uint64(fileStoreHolePunchCandidateWindow)*pageSize)
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
