package durable

import (
	"bytes"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func primaryNativeFoldTestReplacement(t *testing.T, src []byte) []byte {
	t.Helper()
	value := append([]byte(nil), src...)
	at := bytes.Index(value, []byte(`"group":`))
	if at < 0 {
		t.Fatal("row has no group scalar")
	}
	at += len(`"group":`)
	if value[at] == '9' {
		value[at] = '8'
	} else {
		value[at]++
	}
	if len(value) != len(src) || bytes.Equal(value, src) {
		t.Fatal("replacement is not a fixed-size change")
	}
	return value
}

// primaryNativeFoldVisibleState adapts the physical root pointer to the
// reader-visible logical cut. These tests intentionally call fold internals
// directly; ordinary production entry points perform this adaptation before
// invoking the planner when a packed overlay suffix is pending.
func primaryNativeFoldVisibleState(
	t *testing.T, collection *Collection,
) *fileStoreState {
	t.Helper()
	view, err := collection.visibleLogicalView()
	if err != nil || view.state == nil {
		t.Fatalf("visible logical state: state=%v err=%v", view.state, err)
	}
	state := *view.state
	state.root.Generation = view.generation
	state.root.DocumentCount = view.documentCount
	return &state
}

func primaryNativeFoldDistinctRows(
	t *testing.T,
	collection *Collection,
	keys []string,
	want int,
) []int {
	t.Helper()
	state := primaryNativeFoldVisibleState(t, collection)
	seen := make(map[storeio.BucketID]struct{}, want)
	selected := make([]int, 0, want)
	for index := range keys {
		route, err := collection.currentPrimaryResidentRoute(
			state, []byte(keys[index]),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := seen[route.Bucket]; ok {
			continue
		}
		seen[route.Bucket] = struct{}{}
		selected = append(selected, index)
		if len(selected) == want {
			return selected
		}
	}
	t.Fatalf("fixture provided %d distinct leaves, want %d", len(selected), want)
	return nil
}

func TestPrimaryMutationScratchAccountsCompactColumnPlanner(t *testing.T) {
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "compact-column-scratch-accounting.vibe",
	)
	defer file.Close()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	before := collection.Stats()
	value := primaryNativeFoldTestReplacement(t, values[0])
	created, err := collection.Put([]byte(keys[0]), value)
	if err != nil || created {
		t.Fatalf("Put = created %v, err %v", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	after := collection.Stats()
	if after.PrimaryCompactColumnPatches <= before.PrimaryCompactColumnPatches {
		t.Fatalf(
			"compact patches = %d, started at %d",
			after.PrimaryCompactColumnPatches, before.PrimaryCompactColumnPatches,
		)
	}
	if after.PrimaryMutationScratchBytes <= before.PrimaryMutationScratchBytes {
		t.Fatalf(
			"mutation scratch = %d, started at %d; compact planner growth was not charged",
			after.PrimaryMutationScratchBytes, before.PrimaryMutationScratchBytes,
		)
	}
}

// TestFilePrimaryNativeFoldForegroundOverlap uses a worker-side barrier to
// prove two independently routed native leaves are inside their codec lane at
// the same time. The materializer itself runs in a third goroutine because
// context zero is deliberately useful work on the calling goroutine. Reaching
// both hook calls before release is impossible for a serial implementation.
func TestFilePrimaryNativeFoldForegroundOverlap(t *testing.T) {
	t.Skip("compact stream folds use the deterministic foreground encoder")
	previousProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previousProcs)

	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-parallel-overlap.vibe",
	)
	defer file.Close()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if len(collection.primaryNativeFoldContexts) < 2 {
		t.Fatalf("native fold contexts = %d, want at least 2",
			len(collection.primaryNativeFoldContexts))
	}

	// Five leaves force two complete waves plus a one-leaf final wave at
	// GOMAXPROCS=2. The first wave is held at the hook; later calls only count,
	// so the test also proves workers are reused rather than recreated or
	// stranded at either the mixed middle wave or the short final wave.
	selected := primaryNativeFoldDistinctRows(t, collection, keys, 5)
	updated := make(map[int][]byte, len(selected))
	for _, index := range selected {
		updated[index] = primaryNativeFoldTestReplacement(t, values[index])
		created, putErr := collection.Put(
			[]byte(keys[index]), updated[index],
		)
		if putErr != nil || created {
			t.Fatalf("Put row %d = %v,%v", index, created, putErr)
		}
	}
	deletedIndex := selected[2]
	deleted, deleteErr := collection.Delete([]byte(keys[deletedIndex]))
	if deleteErr != nil || !deleted {
		t.Fatalf("Delete row %d = %v,%v", deletedIndex, deleted, deleteErr)
	}

	entered := make(chan storeio.BucketID, 2)
	release := make(chan struct{})
	var hookCalls atomic.Uint32
	collection.primaryNativeFoldPrecomputeHook = func(bucket storeio.BucketID) {
		if hookCalls.Add(1) <= 2 {
			entered <- bucket
			<-release
		}
	}
	done := make(chan error, 1)
	go func() {
		collection.writer.Lock()
		err := collection.materializePrimaryParentsLocked()
		collection.writer.Unlock()
		done <- err
	}()

	var first, second storeio.BucketID
	for index := 0; index < 2; index++ {
		select {
		case bucket := <-entered:
			if index == 0 {
				first = bucket
			} else {
				second = bucket
			}
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("native fold workers did not overlap")
		}
	}
	if first == second {
		close(release)
		t.Fatalf("overlap hooks named one bucket %d twice", first)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native fold did not finish after hook release")
	}
	collection.primaryNativeFoldPrecomputeHook = nil
	if got, want := hookCalls.Load(), uint32(len(selected)-1); got != want {
		t.Fatalf("native precompute hooks = %d, want %d", got, want)
	}
	for _, index := range selected {
		if index == deletedIndex {
			assertPrimaryRaw(t, collection, keys[index], nil, false)
		} else {
			assertPrimaryRaw(t, collection, keys[index], updated[index], true)
		}
	}
}

// TestFilePrimaryNativeFoldPrecomputeMatchesSerial pins both outcomes of the
// seam: a successful worker image is byte-identical to the established serial
// codec call, while a final tombstone is declined by both and reaches the full
// planner unchanged.
func TestFilePrimaryNativeFoldPrecomputeMatchesSerial(t *testing.T) {
	t.Skip("compact stream folds use the deterministic foreground encoder")
	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-parallel-identity.vibe",
	)
	defer file.Close()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if len(collection.primaryNativeFoldContexts) == 0 {
		t.Fatal("native fold context pool is empty")
	}
	coordinator := &collection.primaryNativeFoldContexts[0]
	if len(coordinator.page) == 0 || len(collection.primaryLeafScratch) == 0 ||
		&coordinator.page[0] != &collection.primaryLeafScratch[0] {
		t.Fatal("coordinator did not borrow writer page scratch")
	}
	coordinatorReplacements := coordinator.replacements[:1]
	writerReplacements := collection.primaryUnifiedReplacementScratch[:1]
	if &coordinatorReplacements[0] != &writerReplacements[0] {
		t.Fatal("coordinator did not borrow writer replacement scratch")
	}

	selected := primaryNativeFoldDistinctRows(t, collection, keys, 2)
	for _, index := range selected {
		value := primaryNativeFoldTestReplacement(t, values[index])
		created, putErr := collection.Put([]byte(keys[index]), value)
		if putErr != nil || created {
			t.Fatalf("Put row %d = %v,%v", index, created, putErr)
		}
	}

	collection.writer.Lock()
	state := primaryNativeFoldVisibleState(t, collection)
	base := collection.primaryCheckpointBaseState()
	if state == nil || base == nil {
		collection.writer.Unlock()
		t.Fatal("missing fold state")
	}
	if err := collection.preparePrimaryUnifiedOverlayParentsLocked(state); err != nil {
		collection.writer.Unlock()
		t.Fatal(err)
	}
	route, err := collection.currentPrimaryResidentRoute(
		state, []byte(keys[selected[0]]),
	)
	if err != nil {
		collection.writer.Unlock()
		t.Fatal(err)
	}
	pendingIndex := collection.primaryPendingParentIndex(route.Bucket)
	if pendingIndex < 0 {
		collection.writer.Unlock()
		t.Fatal("native bucket has no pending parent")
	}
	pending := &collection.primaryPendingParents[pendingIndex]
	worker := &collection.primaryNativeFoldContexts[0]
	collection.preparePrimaryNativeFold(
		worker, pending, base, state, state.root.Generation,
	)
	if worker.err != nil || !worker.native || !worker.inspected ||
		!worker.allPuts || !worker.sourceSafe {
		collection.writer.Unlock()
		t.Fatalf(
			"worker native result = native=%v inspected=%v all-puts=%v source-safe=%v err=%v",
			worker.native, worker.inspected, worker.allPuts,
			worker.sourceSafe, worker.err,
		)
	}
	workerImage := append([]byte(nil), worker.image...)

	lease, err := collection.cache.Acquire(pending.volatileRef)
	if err != nil {
		collection.writer.Unlock()
		t.Fatal(err)
	}
	unified, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		lease.Page(), collection.storeID, pending.leafRoute.Bucket,
		collection.primaryLeafBounds(state),
	)
	if !ok {
		lease.Release()
		collection.writer.Unlock()
		t.Fatal("serial source leaf is not admitted unified")
	}
	replacements, allPuts, err :=
		collection.primaryUnifiedOverlay.primaryUnifiedFixedReplacements(
			collection.primaryUnifiedReplacementScratch[:0],
			pending.leafRoute.Bucket, state.root.Generation,
		)
	if err != nil || !allPuts {
		lease.Release()
		collection.writer.Unlock()
		t.Fatalf("serial replacements = %v,%v", allPuts, err)
	}
	header := unified.Header()
	header.Generation = state.root.Generation
	serialImage, serialNative, err := unified.PatchPlanStableReplacements(
		collection.primaryLeafScratch, header, replacements,
		collection.primaryUnifiedBuilder,
	)
	lease.Release()
	if err != nil || !serialNative {
		collection.writer.Unlock()
		t.Fatalf("serial native result = %v,%v", serialNative, err)
	}
	if !bytes.Equal(workerImage, serialImage) {
		collection.writer.Unlock()
		t.Fatal("parallel precompute image differs from serial codec image")
	}
	collection.writer.Unlock()

	deleted, err := collection.Delete([]byte(keys[selected[1]]))
	if err != nil || !deleted {
		t.Fatalf("Delete row %d = %v,%v", selected[1], deleted, err)
	}
	collection.writer.Lock()
	state = primaryNativeFoldVisibleState(t, collection)
	base = collection.primaryCheckpointBaseState()
	route, err = collection.currentPrimaryResidentRoute(
		state, []byte(keys[selected[1]]),
	)
	if err != nil {
		collection.writer.Unlock()
		t.Fatal(err)
	}
	pendingIndex = collection.primaryPendingParentIndex(route.Bucket)
	if pendingIndex < 0 {
		collection.writer.Unlock()
		t.Fatal("deleted bucket has no pending parent")
	}
	pending = &collection.primaryPendingParents[pendingIndex]
	collection.preparePrimaryNativeFold(
		worker, pending, base, state, state.root.Generation,
	)
	if worker.err != nil || worker.native || !worker.inspected ||
		worker.allPuts || worker.sourceSafe {
		collection.writer.Unlock()
		t.Fatalf(
			"delete worker fallback = native=%v inspected=%v all-puts=%v source-safe=%v err=%v",
			worker.native, worker.inspected, worker.allPuts,
			worker.sourceSafe, worker.err,
		)
	}
	if err := collection.materializePrimaryParentsLocked(); err != nil {
		collection.writer.Unlock()
		t.Fatal(err)
	}
	collection.writer.Unlock()
	for index := range collection.primaryNativeFoldContexts {
		context := &collection.primaryNativeFoldContexts[index]
		if context.image != nil || context.native || context.inspected ||
			context.allPuts || context.sourceSafe || context.retrySerial ||
			context.err != nil ||
			len(context.replacements) != 0 {
			t.Fatalf("native context %d retained result: %+v", index, context)
		}
	}
	assertPrimaryRaw(t, collection, keys[selected[1]], nil, false)
}

// TestFilePrimaryNativeFoldWorkerCountIdentity compares complete staged leaves,
// page references, and the published primary root between coordinator-only and
// four-context folds. It pins the crucial property that parallelism changes
// only when CPU work completes, never allocation order or physical output.
func TestFilePrimaryNativeFoldWorkerCountIdentity(t *testing.T) {
	t.Skip("compact stream folds use the deterministic foreground encoder")
	previousProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previousProcs)

	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	parallelFile := createPrimaryPointFile(
		t, built, options, "unified-native-workers-four.vibe",
	)
	defer parallelFile.Close()
	serialFile := clonePrimaryCrashFile(
		t, parallelFile, "unified-native-workers-one.vibe",
	)
	defer serialFile.Close()
	parallel, err := Open(parallelFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer parallel.Close()
	serial, err := Open(serialFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	if len(parallel.primaryNativeFoldContexts) < 4 ||
		len(serial.primaryNativeFoldContexts) < 4 {
		t.Fatalf("fold context pools = %d,%d, want at least 4",
			len(parallel.primaryNativeFoldContexts),
			len(serial.primaryNativeFoldContexts))
	}
	serial.primaryNativeFoldContexts = serial.primaryNativeFoldContexts[:1]

	selected := primaryNativeFoldDistinctRows(t, parallel, keys, 9)
	for _, index := range selected {
		value := primaryNativeFoldTestReplacement(t, values[index])
		for _, collection := range []*Collection{serial, parallel} {
			created, putErr := collection.Put([]byte(keys[index]), value)
			if putErr != nil || created {
				t.Fatalf("Put row %d = %v,%v", index, created, putErr)
			}
		}
	}
	parallelStateBefore := primaryNativeFoldVisibleState(t, parallel)
	targetRoute, err := parallel.currentPrimaryResidentRoute(
		parallelStateBefore, []byte(keys[selected[0]]),
	)
	if err != nil {
		t.Fatal(err)
	}
	var targetAcquireCalls atomic.Uint32
	var retryPinnedPages atomic.Uint64
	parallel.primaryNativeFoldAcquire = func(
		ref storeio.PageRef,
	) (storeio.PageLease, error) {
		if ref == targetRoute.Ref {
			call := targetAcquireCalls.Add(1)
			if call == 1 {
				return storeio.PageLease{}, storeio.ErrPageCachePinned
			}
			if call == 2 {
				retryPinnedPages.Store(parallel.cache.Stats().PinnedPages)
			}
		}
		return parallel.cache.Acquire(ref)
	}
	fold := func(collection *Collection) error {
		collection.writer.Lock()
		defer collection.writer.Unlock()
		return collection.materializePrimaryParentsLocked()
	}
	if err := fold(serial); err != nil {
		t.Fatal(err)
	}
	if err := fold(parallel); err != nil {
		t.Fatal(err)
	}
	parallel.primaryNativeFoldAcquire = nil
	if calls := targetAcquireCalls.Load(); calls != 2 {
		t.Fatalf("pinned target acquire calls = %d, want 2", calls)
	}
	if pinned := retryPinnedPages.Load(); pinned != 0 {
		t.Fatalf("serial retry observed %d worker-pinned pages", pinned)
	}
	serialState, parallelState := serial.state.Load(), parallel.state.Load()
	if serialState == nil || parallelState == nil {
		t.Fatal("fold published a nil state")
	}
	if serialState.root != parallelState.root ||
		serialState.fileEnd != parallelState.fileEnd {
		t.Fatalf("worker-count states differ:\nserial=%+v end=%d\nparallel=%+v end=%d",
			serialState.root, serialState.fileEnd,
			parallelState.root, parallelState.fileEnd)
	}
	for _, index := range selected {
		serialRoute, routeErr := serial.currentPrimaryResidentRoute(
			serialState, []byte(keys[index]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		parallelRoute, routeErr := parallel.currentPrimaryResidentRoute(
			parallelState, []byte(keys[index]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if serialRoute.Ref != parallelRoute.Ref {
			t.Fatalf("row %d refs differ: serial=%+v parallel=%+v",
				index, serialRoute.Ref, parallelRoute.Ref)
		}
		serialLease, acquireErr := serial.cache.Acquire(serialRoute.Ref)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		parallelLease, acquireErr := parallel.cache.Acquire(parallelRoute.Ref)
		if acquireErr != nil {
			serialLease.Release()
			t.Fatal(acquireErr)
		}
		equal := bytes.Equal(serialLease.Page(), parallelLease.Page())
		serialLease.Release()
		parallelLease.Release()
		if !equal {
			t.Fatalf("row %d leaf image differs by worker count", index)
		}
	}
}

func TestFilePrimaryNativeFoldLaterWaveErrorAbortsAndRetries(t *testing.T) {
	t.Skip("compact stream folds use the deterministic foreground encoder")
	previousProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previousProcs)

	built, keys, values := buildRedundantPrimaryCorpus(t, 2_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "unified-native-later-wave-abort.vibe",
	)
	defer file.Close()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if len(collection.primaryNativeFoldContexts) < 2 {
		t.Fatalf("native fold contexts = %d, want at least 2",
			len(collection.primaryNativeFoldContexts))
	}

	selected := primaryNativeFoldDistinctRows(t, collection, keys, 4)
	updated := make(map[int][]byte, len(selected))
	var target storeio.ResidentPrimaryRoute
	for _, index := range selected {
		value := primaryNativeFoldTestReplacement(t, values[index])
		updated[index] = value
		created, putErr := collection.Put([]byte(keys[index]), value)
		if putErr != nil || created {
			t.Fatalf("Put row %d = %v,%v", index, created, putErr)
		}
		route, routeErr := collection.currentPrimaryResidentRoute(
			primaryNativeFoldVisibleState(t, collection), []byte(keys[index]),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if target.Ref == (storeio.PageRef{}) || route.Bucket > target.Bucket {
			target = route
		}
	}

	injected := errors.New("injected later native-fold wave failure")
	var successfulImages atomic.Uint32
	var targetCalls atomic.Uint32
	collection.primaryNativeFoldPrecomputeHook = func(storeio.BucketID) {
		successfulImages.Add(1)
	}
	collection.primaryNativeFoldAcquire = func(
		ref storeio.PageRef,
	) (storeio.PageLease, error) {
		if ref == target.Ref && targetCalls.Add(1) == 1 {
			return storeio.PageLease{}, injected
		}
		return collection.cache.Acquire(ref)
	}
	before := collection.state.Load()
	beforePinned := collection.cache.Stats().PinnedPages
	collection.writer.Lock()
	err = collection.materializePrimaryParentsLocked()
	collection.writer.Unlock()
	if !errors.Is(err, injected) {
		t.Fatalf("materialize error = %v, want %v", err, injected)
	}
	if got := successfulImages.Load(); got < 2 {
		t.Fatalf("only %d images completed before later-wave failure", got)
	}
	if collection.state.Load() != before {
		t.Fatal("aborted native fold published a state")
	}
	if got := collection.cache.Stats().PinnedPages; got != beforePinned {
		t.Fatalf("aborted native fold retained %d pins, started with %d",
			got, beforePinned)
	}
	for index := range collection.primaryNativeFoldContexts {
		context := &collection.primaryNativeFoldContexts[index]
		if context.image != nil || context.native || context.inspected ||
			context.allPuts || context.sourceSafe || context.retrySerial ||
			context.err != nil ||
			len(context.replacements) != 0 {
			t.Fatalf("aborted context %d retained result: %+v", index, context)
		}
	}

	collection.primaryNativeFoldAcquire = nil
	collection.primaryNativeFoldPrecomputeHook = nil
	collection.writer.Lock()
	err = collection.materializePrimaryParentsLocked()
	collection.writer.Unlock()
	if err != nil {
		t.Fatalf("retry materialize: %v", err)
	}
	for _, index := range selected {
		assertPrimaryRaw(t, collection, keys[index], updated[index], true)
	}
}
