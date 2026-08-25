package durable

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// TestFilePrimaryUnifiedNativeFoldCrashBoundary exercises the checkpoint shape
// the native class-5 patcher accepts: several same-shape, same-size integer
// replacements in one durable leaf. Before Flush a copied store must recover
// the sealed base; after Flush a reopen must recover every patched canonical
// row. This pins the fresh-page + root publication boundary independently of
// the codec's byte-identity differential.
func TestFilePrimaryUnifiedNativeFoldCrashBoundary(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-fold.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}

	state := collection.state.Load()
	firstRoute, err := collection.currentPrimaryResidentRoute(
		state, []byte(keys[0]),
	)
	if err != nil {
		t.Fatal(err)
	}
	var selected []int
	for i := range keys {
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(keys[i]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket == firstRoute.Bucket {
			selected = append(selected, i)
			if len(selected) == 4 {
				break
			}
		}
	}
	if len(selected) != 4 {
		t.Fatalf("fixture provided %d same-leaf rows, want 4", len(selected))
	}

	updated := make(map[int][]byte, len(selected))
	for _, index := range selected {
		value := append([]byte(nil), values[index]...)
		at := bytes.Index(value, []byte(`"group":`))
		if at < 0 {
			t.Fatalf("row %d has no group scalar", index)
		}
		at += len(`"group":`)
		if value[at] == '9' {
			value[at] = '8'
		} else {
			value[at]++
		}
		if len(value) != len(values[index]) ||
			bytes.Equal(value, values[index]) {
			t.Fatalf("row %d replacement is not fixed-size", index)
		}
		updated[index] = value
		if created, putErr := collection.Put(
			[]byte(keys[index]), value,
		); putErr != nil || created {
			t.Fatalf("Put row %d = %v,%v", index, created, putErr)
		}
	}

	before := clonePrimaryCrashFile(t, file, "before-native-fold.vibe")
	beforeCollection, err := Open(before, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range selected {
		assertPrimaryRaw(t, beforeCollection, keys[index], values[index], true)
	}
	if err := beforeCollection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}

	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, index := range selected {
		assertPrimaryRaw(t, reopened, keys[index], updated[index], true)
	}
}

// seedBufferedInlinePrimaryLeaf takes the structural buffered-COW primitive
// directly so a test can start from an inline, cache-only leaf above the
// checkpoint base. Ordinary inline Put intentionally chooses the row overlay.
func seedBufferedInlinePrimaryLeaf(
	t *testing.T,
	collection *Collection,
	key, value []byte,
) storeio.PageRef {
	t.Helper()
	var ref storeio.PageRef
	target := func() uint64 {
		collection.writer.Lock()
		defer collection.writer.Unlock()
		if err := collection.repartitionPrimaryForExactIndexLocked(context.Background()); err != nil {
			t.Fatal(err)
		}
		state := collection.state.Load()
		route, err := collection.currentPrimaryResidentRoute(state, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := collection.ensureBufferedPrimaryMutationCapacity(
			route, len(value),
		); err != nil {
			t.Fatal(err)
		}
		lease, err := collection.primaryRouter.Load().AcquireLeaf(
			collection.cache, route,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release()
		leaf, err := storeio.AdmittedPrimaryLeafForMutationWithScratch(
			lease.Page(), collection.storeID, route.Bucket,
			collection.primaryLeafBounds(state),
			collection.primaryLeafMutationScratch,
		)
		if err != nil {
			t.Fatal(err)
		}
		slot, _, _, found := leaf.LookupRawHashed(route.Hash, key)
		if !found {
			t.Fatal("seed row is missing")
		}
		ref, _, _, err = collection.cowBufferedPrimaryMutation(
			state, key, value, false, true, slot, route, &leaf, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		journalTarget, err := collection.journalDepositLocked(
			storeio.RecoveryRecordKindPut,
			state.root.Generation+1, key, value,
		)
		if err != nil {
			t.Fatal(err)
		}
		return journalTarget
	}()
	if target != 0 {
		if err := collection.journalGroupAwait(target); err != nil {
			t.Fatal(err)
		}
	}
	return ref
}

func TestFilePrimaryCollisionGapIsCompactAndSnapshotSafe(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-compact-collision-gap.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	// Make the exceptional leaf layout transition before measuring the pending
	// parent's reservation. seedBufferedInlinePrimaryLeaf repeats this call as a
	// no-op, matching the ordinary indexed fallback without mixing structural
	// conversion bytes into the collision-gap measurement.
	collection.writer.Lock()
	err = collection.repartitionPrimaryForExactIndexLocked(context.Background())
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	beforeState := collection.state.Load()
	beforeStats := collection.Stats()
	beforeInfo, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	updated := append([]byte(nil), values[0]...)
	at := bytes.Index(updated, []byte(`"group":`))
	if at < 0 {
		t.Fatal("fixture row has no group scalar")
	}
	at += len(`"group":`)
	if updated[at] == '9' {
		updated[at] = '8'
	} else {
		updated[at]++
	}
	ref := seedBufferedInlinePrimaryLeaf(
		t, collection, []byte(keys[0]), updated,
	)
	wantOffset := beforeState.fileEnd + collection.options.maxTransactionBytes
	if ref.Offset != wantOffset {
		t.Fatalf("first volatile offset = %d, want compact boundary %d", ref.Offset, wantOffset)
	}
	oldOffset := beforeState.fileEnd +
		uint64(collection.options.maxTransactionPages)*
			uint64(collection.options.MaxPageSize)
	if ref.Offset >= oldOffset {
		t.Fatalf("compact volatile offset = %d, old fixed-frame boundary %d", ref.Offset, oldOffset)
	}
	afterInfo, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if afterInfo.Size() != beforeInfo.Size() {
		t.Fatalf("buffered frame changed apparent device file: %d -> %d", beforeInfo.Size(), afterInfo.Size())
	}
	afterStats := collection.Stats()
	if afterStats.DeviceBytes != beforeStats.DeviceBytes {
		t.Fatalf("buffered frame changed device bytes: %d -> %d", beforeStats.DeviceBytes, afterStats.DeviceBytes)
	}
	assertPrimaryRaw(t, collection, keys[0], updated, true)
	got, found, err := snapshot.AppendRaw(nil, []byte(keys[0]))
	if err != nil || !found || !bytes.Equal(got, values[0]) {
		t.Fatalf("pinned snapshot before checkpoint = (%q,%v,%v), want old row", got, found, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	got, found, err = snapshot.AppendRaw(nil, []byte(keys[0]))
	if err != nil || !found || !bytes.Equal(got, values[0]) {
		t.Fatalf("pinned snapshot after checkpoint = (%q,%v,%v), want old row", got, found, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertPrimaryRaw(t, reopened, keys[0], updated, true)
}

func forcePrimaryOverlayPressureFold(
	t *testing.T,
	collection *Collection,
	requireNative bool,
) {
	t.Helper()
	collection.writer.Lock()
	fullScratch := collection.primaryLeafMutationScratch
	if requireNative {
		// RenderRecordsWithScratch(nil) fails closed. Success therefore proves
		// the materializer never reached the full-planner fallback.
		collection.primaryLeafMutationScratch = nil
	}
	err := collection.materializePrimaryOverlayPressureLocked()
	collection.primaryLeafMutationScratch = fullScratch
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// TestFilePrimaryUnifiedNativeFoldAcrossVolatileCut proves the native patch
// path is not restricted to a leaf already present on device. The first
// pressure fold stages a fresh class-5 leaf in the manual committer; the second
// fold patches that never-durable page, cancels its queued write through the
// retirement handoff, and removes its cache frame. Nil-ing the full-render
// scratch around each fold makes any planner fallback fail closed, so two
// successful folds also pin fast-path selection rather than just final values.
func TestFilePrimaryUnifiedNativeFoldAcrossVolatileCut(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability:      DurabilityBufferedVisible,
		RecoveryJournal: true,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-volatile-fold.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}

	const widthIndex = 63 // group 63 -> 64 grows its encoded zigzag varint.
	state := collection.state.Load()
	firstRoute, err := collection.currentPrimaryResidentRoute(
		state, []byte(keys[widthIndex]),
	)
	if err != nil {
		t.Fatal(err)
	}
	selected := make([]int, 1, 5)
	selected[0] = widthIndex
	for i := range keys {
		if i == widthIndex {
			continue
		}
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(keys[i]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket == firstRoute.Bucket {
			selected = append(selected, i)
			if len(selected) == cap(selected) {
				break
			}
		}
	}
	if len(selected) != cap(selected) {
		t.Fatalf("fixture provided %d same-leaf rows, want %d",
			len(selected), cap(selected))
	}
	targets := selected[:4]

	mutate := func(round, index int, source []byte) []byte {
		t.Helper()
		value := append([]byte(nil), source...)
		at := bytes.Index(value, []byte(`"group":`))
		if at < 0 {
			t.Fatal("row has no group scalar")
		}
		at += len(`"group":`)
		if index == widthIndex {
			from, to := []byte("63"), []byte("64")
			if round&1 == 0 {
				from, to = to, from
			}
			if !bytes.Equal(value[at:at+len(from)], from) {
				t.Fatalf("round %d width scalar = %q, want %q",
					round, value[at:at+len(from)], from)
			}
			copy(value[at:at+len(to)], to)
			return value
		}
		before := value[at]
		if (int(before-'0')+round)&1 == 0 {
			value[at] = '7'
		} else {
			value[at] = '8'
		}
		if value[at] == before {
			value[at] = '6'
		}
		if len(value) != len(source) || bytes.Equal(value, source) {
			t.Fatalf("round %d replacement is not fixed-size", round)
		}
		return value
	}
	putRound := func(round int, source map[int][]byte) map[int][]byte {
		t.Helper()
		next := make(map[int][]byte, len(targets))
		for _, index := range targets {
			value := mutate(round, index, source[index])
			next[index] = value
			created, putErr := collection.Put([]byte(keys[index]), value)
			if putErr != nil || created {
				t.Fatalf("round %d Put row %d = %v,%v",
					round, index, created, putErr)
			}
		}
		return next
	}
	current := make(map[int][]byte, len(targets))
	for _, index := range targets {
		current[index] = values[index]
	}
	// Seed the pending-parent bridge with a genuine cache-only leaf above the
	// checkpoint base. Normal inline replacements choose the row overlay, so the
	// test invokes the same bounded COW primitive directly to isolate this source
	// shape without adding an insert/delete/overflow fallback to the scenario.
	seedIndex := selected[4]
	seedValue := mutate(1, seedIndex, values[seedIndex])
	seedRef := seedBufferedInlinePrimaryLeaf(
		t, collection, []byte(keys[seedIndex]), seedValue,
	)
	baseline := collection.Stats()
	initialDurableGeneration := collection.committer.DurableGeneration()
	checkpointBase := collection.primaryCheckpointBaseState()
	if checkpointBase == nil {
		t.Fatal("seed has no checkpoint base")
	}
	if seedRef.Offset < checkpointBase.fileEnd {
		t.Fatalf("seed ref %+v is not above checkpoint base fileEnd %d",
			seedRef, checkpointBase.fileEnd)
	}

	current = putRound(1, current)
	beforeFirstFold, err := collection.currentPrimaryResidentRoute(
		collection.state.Load(), []byte(keys[targets[0]]),
	)
	if err != nil {
		t.Fatal(err)
	}
	if beforeFirstFold.Ref != seedRef {
		t.Fatalf("first pressure source = %+v, want volatile %+v",
			beforeFirstFold.Ref, seedRef)
	}
	forcePrimaryOverlayPressureFold(t, collection, false)
	firstFoldRoute, err := collection.currentPrimaryResidentRoute(
		collection.state.Load(), []byte(keys[targets[0]]),
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstFoldRoute.Ref.Generation <= initialDurableGeneration {
		t.Fatalf("first fold ref generation = %d, durable base = %d",
			firstFoldRoute.Ref.Generation, initialDurableGeneration)
	}
	if got := collection.committer.DurableGeneration(); got != initialDurableGeneration {
		t.Fatalf("device-silent fold advanced durable generation to %d, want %d",
			got, initialDurableGeneration)
	}

	current = putRound(2, current)
	forcePrimaryOverlayPressureFold(t, collection, false)
	secondFoldRoute, err := collection.currentPrimaryResidentRoute(
		collection.state.Load(), []byte(keys[targets[0]]),
	)
	if err != nil {
		t.Fatal(err)
	}
	if secondFoldRoute.Ref == firstFoldRoute.Ref {
		t.Fatalf("second fold retained source ref %+v", firstFoldRoute.Ref)
	}
	retiredNeverDurable := false
	for _, extent := range collection.retirementAbsorbed {
		if extent.Offset == firstFoldRoute.Ref.Offset &&
			extent.Length == uint64(firstFoldRoute.Ref.Length) {
			retiredNeverDurable = true
			break
		}
	}
	if !retiredNeverDurable {
		t.Fatalf("never-durable source %+v was not extracted for reuse",
			firstFoldRoute.Ref)
	}
	if lease, acquireErr := collection.cache.Acquire(firstFoldRoute.Ref); acquireErr == nil {
		lease.Release()
		t.Fatalf("never-durable source %+v remained cache-reachable",
			firstFoldRoute.Ref)
	}
	stats := collection.Stats()
	if got := stats.PrimaryOverlayFolds - baseline.PrimaryOverlayFolds; got != 2 {
		t.Fatalf("pressure folds = %d, want 2", got)
	}
	if stats.JournalDeltaFullFallbacks != baseline.JournalDeltaFullFallbacks {
		t.Fatalf("journal fallback count = %d, want %d",
			stats.JournalDeltaFullFallbacks,
			baseline.JournalDeltaFullFallbacks)
	}

	// The exact pre-Flush image contains the synced recovery records but none of
	// the manual committer's device-silent pages. Reopen therefore has to replay
	// both mutation windows from the sealed durable base.
	crash := clonePrimaryCrashFile(t, file, "volatile-fold-crash.vibe")
	crashCollection, err := Open(crash, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range targets {
		assertPrimaryRaw(t, crashCollection, keys[index], current[index], true)
	}
	assertPrimaryRaw(t, crashCollection, keys[seedIndex], seedValue, true)
	if err := crashCollection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := crash.Close(); err != nil {
		t.Fatal(err)
	}

	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, index := range targets {
		assertPrimaryRaw(t, reopened, keys[index], current[index], true)
	}
	assertPrimaryRaw(t, reopened, keys[seedIndex], seedValue, true)
}

// TestFilePrimaryUnifiedNativeFoldPinsVolatileSource proves a direct reader
// admitted before publication keeps the cache-only source leaf alive across a
// native fold. Once the epoch exits, the ordinary deferred-retirement cleanup
// makes that exact generation unreachable.
func TestFilePrimaryUnifiedNativeFoldPinsVolatileSource(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability:      DurabilityBufferedVisible,
		RecoveryJournal: true,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-pinned-source.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const widthIndex = 63
	state := collection.state.Load()
	targetRoute, err := collection.currentPrimaryResidentRoute(
		state, []byte(keys[widthIndex]),
	)
	if err != nil {
		t.Fatal(err)
	}
	seedIndex := -1
	var seedRoute storeio.ResidentPrimaryRoute
	for i := range keys {
		if i == widthIndex {
			continue
		}
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(keys[i]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket == targetRoute.Bucket {
			seedIndex, seedRoute = i, route
			break
		}
	}
	if seedIndex < 0 {
		t.Fatal("fixture has no second row in target bucket")
	}
	seedValue := append([]byte(nil), values[seedIndex]...)
	seedScalar := bytes.Index(seedValue, []byte(`"group":`)) + len(`"group":`)
	if seedScalar < len(`"group":`) {
		t.Fatal("seed row has no group scalar")
	}
	if seedValue[seedScalar] == '8' {
		seedValue[seedScalar] = '7'
	} else {
		seedValue[seedScalar] = '8'
	}
	seedRef := seedBufferedInlinePrimaryLeaf(
		t, collection, []byte(keys[seedIndex]), seedValue,
	)
	base := collection.primaryCheckpointBaseState()
	if base == nil || seedRef.Offset < base.fileEnd {
		t.Fatalf("seed ref %+v is not cache-only above the checkpoint base", seedRef)
	}
	targetValue := bytes.Replace(
		append([]byte(nil), values[widthIndex]...),
		[]byte(`"group":63`), []byte(`"group":64`), 1,
	)
	if bytes.Equal(targetValue, values[widthIndex]) {
		t.Fatal("width-changing target mutation was not applied")
	}
	if created, putErr := collection.Put(
		[]byte(keys[widthIndex]), targetValue,
	); putErr != nil || created {
		t.Fatalf("overlay Put = %v,%v", created, putErr)
	}

	pinnedView, epoch, entered := collection.enterReadEpoch()
	if !entered {
		t.Fatal("could not pin direct reader")
	}
	released := false
	defer func() {
		if !released {
			epoch.Exit()
		}
	}()
	pinnedState := *pinnedView.state
	pinnedState.root.Generation = pinnedView.generation
	pinnedState.root.DocumentCount = pinnedView.documentCount
	pinnedRoute, err := collection.currentPrimaryResidentRoute(
		&pinnedState, []byte(keys[seedIndex]),
	)
	if err != nil || pinnedRoute.Ref != seedRef {
		t.Fatalf("pinned source route = %+v,%v want %+v",
			pinnedRoute.Ref, err, seedRef)
	}
	forcePrimaryOverlayPressureFold(t, collection, false)

	deferred := false
	for _, ref := range collection.primaryVolatileRetired {
		if ref == seedRef {
			deferred = true
			break
		}
	}
	if !deferred {
		t.Fatalf("reader-pinned source %+v was not deferred", seedRef)
	}
	lease, err := collection.cache.Acquire(seedRef)
	if err != nil {
		t.Fatalf("reader-pinned source is not cache-readable: %v", err)
	}
	unified, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), collection.storeID, seedRoute.Bucket,
	)
	if !ok {
		lease.Release()
		t.Fatal("reader-pinned source is not an admitted compact stripe")
	}
	rank, found := unified.FindKey([]byte(keys[seedIndex]))
	_, overflow := unified.OverflowRef(rank)
	got, decoded := unified.AppendValue(nil, rank)
	lease.Release()
	if !found || overflow || !decoded || !bytes.Equal(got, seedValue) {
		t.Fatalf("reader-pinned source row = %q,%v,%v want %q",
			got, found, overflow, seedValue)
	}

	epoch.Exit()
	released = true
	collection.writer.Lock()
	collection.clearPrimaryVolatileRetiredLocked()
	collection.writer.Unlock()
	if lease, acquireErr := collection.cache.Acquire(seedRef); acquireErr == nil {
		lease.Release()
		t.Fatalf("released reader left source %+v cache-reachable", seedRef)
	}
}

// TestFilePrimaryUnifiedVolatileOverflowForcesFullFold pins the safety side of
// native-fold admission. A cache-only source with a volatile overflow chain
// must take the full renderer so the chain is re-minted into the checkpoint;
// copying its old descriptor would publish references to pages no commit owns.
func TestFilePrimaryUnifiedVolatileOverflowForcesFullFold(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability:      DurabilityBufferedVisible,
		RecoveryJournal: true,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-volatile-overflow-fold.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	state := collection.state.Load()
	seedRoute, err := collection.currentPrimaryResidentRoute(
		state, []byte(keys[0]),
	)
	if err != nil {
		t.Fatal(err)
	}
	targetIndex := -1
	for i := 1; i < len(keys); i++ {
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(keys[i]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket == seedRoute.Bucket {
			targetIndex = i
			break
		}
	}
	if targetIndex < 0 {
		t.Fatal("fixture has no inline target in overflow bucket")
	}
	large := append([]byte(nil), `{"payload":"`...)
	large = append(large, bytes.Repeat([]byte{'x'}, 160<<10)...)
	large = append(large, `"}`...)
	canonicalLarge := canonicalDocs(t, [][]byte{large})[0]
	base := collection.primaryCheckpointBaseState()
	if base == nil {
		t.Fatal("overflow source has no checkpoint base")
	}
	if created, putErr := collection.Put([]byte(keys[0]), large); putErr != nil || created {
		t.Fatalf("overflow Put = %v,%v", created, putErr)
	}
	sourceState := collection.state.Load()
	sourceRoute, err := collection.currentPrimaryResidentRoute(
		sourceState, []byte(keys[0]),
	)
	if err != nil {
		t.Fatal(err)
	}
	if sourceRoute.Ref.Offset < base.fileEnd {
		t.Fatalf("overflow source %+v is not above base fileEnd %d",
			sourceRoute.Ref, base.fileEnd)
	}
	lease, err := collection.cache.Acquire(sourceRoute.Ref)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), collection.storeID, sourceRoute.Bucket,
	)
	if !ok {
		lease.Release()
		t.Fatal("overflow source is not an admitted compact stripe")
	}
	sourceRank, found := source.FindKey([]byte(keys[0]))
	oldHead, overflow := source.OverflowRef(sourceRank)
	lease.Release()
	if !found || !overflow || oldHead == (storeio.PageRef{}) ||
		oldHead.Offset < base.fileEnd {
		t.Fatalf("source overflow = %+v,%v,%v", oldHead, found, overflow)
	}
	oldExtents, err := collection.collectPrimaryOverflowExtents(
		nil, oldHead, collection.primaryLeafBounds(sourceState),
	)
	if err != nil || len(oldExtents) < 2 {
		t.Fatalf("volatile overflow extents = %d,%v, want a chain",
			len(oldExtents), err)
	}

	targetValue := append([]byte(nil), values[targetIndex]...)
	targetScalar := bytes.Index(targetValue, []byte(`"group":`)) + len(`"group":`)
	if targetScalar < len(`"group":`) {
		t.Fatal("inline target has no group scalar")
	}
	if targetValue[targetScalar] == '8' {
		targetValue[targetScalar] = '7'
	} else {
		targetValue[targetScalar] = '8'
	}
	if created, putErr := collection.Put(
		[]byte(keys[targetIndex]), targetValue,
	); putErr != nil || created {
		t.Fatalf("inline overlay Put = %v,%v", created, putErr)
	}
	baselineFolds := collection.Stats().PrimaryOverlayFolds
	forcePrimaryOverlayPressureFold(t, collection, false)
	if got := collection.Stats().PrimaryOverlayFolds; got != baselineFolds+1 {
		t.Fatalf("pressure folds = %d, want %d", got, baselineFolds+1)
	}

	foldedState := collection.state.Load()
	foldedRoute, err := collection.currentPrimaryResidentRoute(
		foldedState, []byte(keys[0]),
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err = collection.cache.Acquire(foldedRoute.Ref)
	if err != nil {
		t.Fatal(err)
	}
	folded, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), collection.storeID, foldedRoute.Bucket,
	)
	if !ok {
		lease.Release()
		t.Fatal("folded overflow leaf is not admitted")
	}
	foldedRank, found := folded.FindKey([]byte(keys[0]))
	newHead, overflow := folded.OverflowRef(foldedRank)
	targetRank, targetFound := folded.FindKey([]byte(keys[targetIndex]))
	_, targetOverflow := folded.OverflowRef(targetRank)
	gotTarget, targetDecoded := folded.AppendValue(nil, targetRank)
	lease.Release()
	if !found || !overflow || newHead == (storeio.PageRef{}) || newHead == oldHead {
		t.Fatalf("reminted overflow head = %+v,%v,%v; old=%+v",
			newHead, found, overflow, oldHead)
	}
	if !targetFound || targetOverflow || !targetDecoded || !bytes.Equal(gotTarget, targetValue) {
		t.Fatalf("folded inline target = %q,%v,%v want %q",
			gotTarget, targetFound, targetOverflow, targetValue)
	}
	for _, ref := range append(oldExtents, sourceRoute.Ref) {
		if stale, acquireErr := collection.cache.Acquire(ref); acquireErr == nil {
			stale.Release()
			t.Fatalf("superseded volatile ref %+v remained cache-reachable", ref)
		}
	}

	crash := clonePrimaryCrashFile(t, file, "volatile-overflow-crash.vibe")
	crashCollection, err := Open(crash, options)
	if err != nil {
		t.Fatal(err)
	}
	assertPrimaryRaw(t, crashCollection, keys[0], canonicalLarge, true)
	assertPrimaryRaw(t, crashCollection, keys[targetIndex], targetValue, true)
	if err := crashCollection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := crash.Close(); err != nil {
		t.Fatal(err)
	}
}
