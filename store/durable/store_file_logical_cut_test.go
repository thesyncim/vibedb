package durable

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFileLogicalCutEncodingBoundaries(t *testing.T) {
	for _, test := range []struct {
		name       string
		generation uint64
		delta      int
		ok         bool
	}{
		{name: "generation-zero", generation: 0, ok: false},
		{name: "generation-one", generation: 1, ok: true},
		{name: "maximum-generation", generation: fileLogicalCutGenerationMask, ok: true},
		{name: "generation-overflow", generation: fileLogicalCutGenerationMask + 1, ok: false},
		{name: "minimum-delta", generation: 1, delta: fileLogicalCutMinDelta, ok: true},
		{name: "maximum-delta", generation: 1, delta: fileLogicalCutMaxDelta, ok: true},
		{name: "delta-underflow", generation: 1, delta: fileLogicalCutMinDelta - 1, ok: false},
		{name: "delta-overflow", generation: 1, delta: fileLogicalCutMaxDelta + 1, ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cut, ok := packFileLogicalCut(test.generation, test.delta)
			if ok != test.ok {
				t.Fatalf("pack ok = %v, want %v", ok, test.ok)
			}
			if !ok {
				return
			}
			if got := fileLogicalCutGeneration(cut); got != test.generation {
				t.Fatalf("generation = %d, want %d", got, test.generation)
			}
			if got := fileLogicalCutDelta(cut); got != test.delta {
				t.Fatalf("delta = %d, want %d", got, test.delta)
			}
		})
	}
}

func TestFileLogicalDocumentCountBounds(t *testing.T) {
	maximum := ^uint64(0)
	for _, test := range []struct {
		name  string
		base  uint64
		delta int
		want  uint64
		ok    bool
	}{
		{name: "zero", base: 7, delta: 0, want: 7, ok: true},
		{name: "decrement", base: 7, delta: -3, want: 4, ok: true},
		{name: "underflow", base: 2, delta: -3, ok: false},
		{name: "increment", base: 7, delta: 3, want: 10, ok: true},
		{name: "maximum", base: maximum - 1, delta: 1, want: maximum, ok: true},
		{name: "overflow", base: maximum, delta: 1, ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := fileLogicalDocumentCount(test.base, test.delta)
			if got != test.want || ok != test.ok {
				t.Fatalf("document count = %d,%v, want %d,%v",
					got, ok, test.want, test.ok)
			}
		})
	}
}

func TestFileLogicalCutFoldTransitionModel(t *testing.T) {
	collection := &Collection{
		primaryConcurrentContexts: &primaryConcurrentContextPool{},
	}
	oldPhysical := &fileStoreState{}
	oldPhysical.root.Generation = 10
	oldPhysical.root.DocumentCount = 100
	newPhysical := &fileStoreState{}
	newPhysical.root.Generation = 12
	newPhysical.root.DocumentCount = 101
	oldCut, _ := packFileLogicalCut(12, 1)
	resetCut, _ := packFileLogicalCut(12, 0)

	assertView := func(
		name string, physical *fileStoreState, cut uint64,
		wantGeneration, wantCount uint64,
	) {
		t.Helper()
		view, ok := collection.logicalViewOf(physical, cut)
		if !ok {
			t.Fatalf("%s: invalid view", name)
		}
		if view.generation != wantGeneration ||
			view.documentCount != wantCount {
			t.Fatalf("%s: view = (%d,%d), want (%d,%d)", name,
				view.generation, view.documentCount,
				wantGeneration, wantCount)
		}
	}
	assertView("old-physical-old-cut", oldPhysical, oldCut, 12, 101)
	assertView("new-physical-old-cut", newPhysical, oldCut, 12, 101)
	assertView("new-physical-reset-cut", newPhysical, resetCut, 12, 101)
	// This pair never returns from the stable loader: the second state load sees
	// the physical pointer change and retries. Its raw interpretation demonstrates
	// why that retry is load-bearing.
	assertView("old-physical-reset-requires-retry", oldPhysical, resetCut, 12, 100)

	collection.visibleState.Store(oldPhysical)
	collection.logicalCut.Store(oldCut)
	previous := fileLogicalViewAfterStateLoadHook
	var once sync.Once
	fileLogicalViewAfterStateLoadHook = func() {
		once.Do(func() {
			collection.visibleState.Store(newPhysical)
			collection.logicalCut.Store(resetCut)
		})
	}
	t.Cleanup(func() { fileLogicalViewAfterStateLoadHook = previous })
	view := collection.visibleLogicalViewNoError()
	if view.state != newPhysical || view.generation != 12 ||
		view.documentCount != 101 {
		t.Fatalf("retried view = (%p,%d,%d), want (%p,12,101)",
			view.state, view.generation, view.documentCount, newPhysical)
	}
}

func TestPackedLogicalCutFoldPublishesPhysicalBeforeReset(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	key := []byte(fixture.keys[0])
	physicalBefore := collection.state.Load()
	countBefore := collection.Len()
	if deleted, err := collection.Delete(key); err != nil || !deleted {
		t.Fatalf("Delete = %v,%v", deleted, err)
	}
	if collection.state.Load() != physicalBefore {
		t.Fatal("ordinary Delete allocated/published a physical state")
	}
	oldCut := collection.logicalCut.Load()
	if got := fileLogicalCutDelta(oldCut); got != -1 {
		t.Fatalf("cut delta = %d, want -1", got)
	}

	previous := fileLogicalCutBeforeResetHook
	var observed atomic.Bool
	var observationErr error
	fileLogicalCutBeforeResetHook = func(next *fileStoreState) {
		if !observed.CompareAndSwap(false, true) {
			return
		}
		if collection.state.Load() != next || collection.visibleState.Load() != next {
			observationErr = fmt.Errorf("physical pointers were not published first")
			return
		}
		if collection.logicalCut.Load() != oldCut {
			observationErr = fmt.Errorf("cut reset before physical observation")
			return
		}
		view, ok := collection.logicalViewOf(next, oldCut)
		if !ok || view.generation != next.root.Generation ||
			view.documentCount != next.root.DocumentCount {
			observationErr = fmt.Errorf(
				"equal-generation fold double-applied delta: view=(%d,%d) physical=(%d,%d)",
				view.generation, view.documentCount,
				next.root.Generation, next.root.DocumentCount,
			)
		}
	}
	t.Cleanup(func() { fileLogicalCutBeforeResetHook = previous })
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if observationErr != nil {
		t.Fatal(observationErr)
	}
	if !observed.Load() {
		t.Fatal("fold publication boundary was not observed")
	}
	if snapshot.Len() != countBefore-1 ||
		snapshot.Generation() != fileLogicalCutGeneration(oldCut) {
		t.Fatalf("snapshot cut = (%d,%d), want (%d,%d)",
			snapshot.Generation(), snapshot.Len(),
			fileLogicalCutGeneration(oldCut), countBefore-1)
	}
	if got := fileLogicalCutDelta(collection.logicalCut.Load()); got != 0 {
		t.Fatalf("reset delta = %d, want 0", got)
	}
}

func TestPackedLogicalCutMaximumDeltaDeclinesInsert(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	physical := collection.state.Load()
	baseGeneration := physical.root.Generation
	fakeGeneration := baseGeneration + 1
	cut, ok := packFileLogicalCut(fakeGeneration, fileLogicalCutMaxDelta)
	if !ok {
		t.Fatal("pack maximum delta")
	}
	collection.primaryRouter.Load().AdvanceGeneration(fakeGeneration)
	collection.logicalCut.Store(cut)

	existing := []byte(fixture.keys[0])
	if handled, created, err := collection.tryConcurrentPrimaryPut(
		existing, []byte(`{"boundary":"replacement"}`),
	); err != nil || !handled || created {
		t.Fatalf("replacement prefix = %v,%v,%v", handled, created, err)
	}
	prefixCut := collection.logicalCut.Load()
	if fileLogicalCutDelta(prefixCut) != fileLogicalCutMaxDelta ||
		fileLogicalCutGeneration(prefixCut) != fakeGeneration+1 {
		t.Fatalf("prefix cut = generation %d delta %d",
			fileLogicalCutGeneration(prefixCut), fileLogicalCutDelta(prefixCut))
	}
	records := collection.primaryUnifiedOverlay.count.Load()
	if handled, created, err := collection.tryConcurrentPrimaryPut(
		[]byte("logical-cut-overflow-key"),
		[]byte(`{"boundary":"insert"}`),
	); err != nil || handled || created {
		t.Fatalf("overflow insert = %v,%v,%v, want exclusive fallback",
			handled, created, err)
	}
	if got := collection.primaryUnifiedOverlay.count.Load(); got != records {
		t.Fatalf("overflow insert published record count %d, want %d", got, records)
	}
	if collection.logicalCut.Load() != prefixCut ||
		!collection.packedLogicalCutPending() {
		t.Fatal("overflow insert changed/saturated the packed cut")
	}

	// Restore the fixture's real physical cut before its registered Close.
	collection.primaryUnifiedOverlay.markFolded(fakeGeneration+1, true)
	collection.primaryRouter.Load().AdvanceGeneration(baseGeneration)
	collection.pageValidator.update(physical)
	baseCut, _ := packFileLogicalCut(baseGeneration, 0)
	collection.logicalCut.Store(baseCut)
}

func TestPackedLogicalCutMinimumDeltaDeclinesDelete(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	physical := collection.state.Load()
	baseGeneration := physical.root.Generation
	originalCount := physical.root.DocumentCount
	physical.root.DocumentCount = uint64(-fileLogicalCutMinDelta) + 512
	defer func() { physical.root.DocumentCount = originalCount }()
	fakeGeneration := baseGeneration + 1
	cut, ok := packFileLogicalCut(fakeGeneration, fileLogicalCutMinDelta)
	if !ok {
		t.Fatal("pack minimum delta")
	}
	collection.primaryRouter.Load().AdvanceGeneration(fakeGeneration)
	collection.logicalCut.Store(cut)

	records := collection.primaryUnifiedOverlay.count.Load()
	if handled, deleted, err := collection.tryConcurrentPrimaryDelete(
		[]byte(fixture.keys[0]),
	); err != nil || handled || deleted {
		t.Fatalf("underflow delete = %v,%v,%v, want exclusive fallback",
			handled, deleted, err)
	}
	if got := collection.primaryUnifiedOverlay.count.Load(); got != records {
		t.Fatalf("underflow delete published record count %d, want %d", got, records)
	}
	if collection.logicalCut.Load() != cut ||
		!collection.packedLogicalCutPending() {
		t.Fatal("underflow delete changed/wrapped the packed cut")
	}

	collection.primaryRouter.Load().AdvanceGeneration(baseGeneration)
	collection.pageValidator.update(physical)
	baseCut, _ := packFileLogicalCut(baseGeneration, 0)
	collection.logicalCut.Store(baseCut)
}

func TestPackedLogicalCutLeasedReadTerminalWriterFence(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	key := []byte(fixture.keys[0])
	values := [][]byte{
		[]byte(`{"terminal":"aaaaaaaa"}`),
		[]byte(`{"terminal":"bbbbbbbb"}`),
	}
	previous := liveReadLeasedPinnedHook
	var calls int
	var hookErr error
	liveReadLeasedPinnedHook = func(attempt int) {
		if hookErr != nil {
			return
		}
		if attempt != calls {
			hookErr = fmt.Errorf("hook attempt %d, want %d", attempt, calls)
			return
		}
		created, err := collection.Put(key, values[attempt&1])
		if err != nil || created {
			hookErr = fmt.Errorf("publish %d = %v,%v", attempt, created, err)
			return
		}
		calls++
	}
	t.Cleanup(func() { liveReadLeasedPinnedHook = previous })
	got, found, err := collection.appendRawLeased(nil, key)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err != nil || !found || calls != liveReadSupersededRetries+1 {
		t.Fatalf("terminal read = %q,%v,%v after %d publications",
			got, found, err, calls)
	}
	want := values[liveReadSupersededRetries&1]
	if !bytes.Equal(got, want) {
		t.Fatalf("terminal read = %q, want latest %q", got, want)
	}
}

func TestPackedLogicalCutWarmedPointMutationsAllocateNothing(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	key := []byte(fixture.keys[0])
	values := [][]byte{
		[]byte(`{"allocation":"aaaaaaaa"}`),
		[]byte(`{"allocation":"bbbbbbbb"}`),
	}
	if created, err := collection.Put(key, values[0]); err != nil || created {
		t.Fatalf("seed replacement = %v,%v", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	flip := 0
	if created, err := collection.Put(key, values[1]); err != nil || created {
		t.Fatalf("warm replacement = %v,%v", created, err)
	}
	replaceAllocs := testing.AllocsPerRun(100, func() {
		flip ^= 1
		if created, err := collection.Put(key, values[flip]); err != nil || created {
			panic("packed replacement failed")
		}
	})
	if replaceAllocs != 0 {
		t.Fatalf("warmed packed Put allocations = %.2f, want 0", replaceAllocs)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	deleteRestoreAllocs := testing.AllocsPerRun(100, func() {
		if deleted, err := collection.Delete(key); err != nil || !deleted {
			panic("packed delete failed")
		}
		if created, err := collection.Put(key, values[0]); err != nil || !created {
			panic("packed restore failed")
		}
	})
	if deleteRestoreAllocs != 0 {
		t.Fatalf("warmed packed Delete+Put allocations = %.2f, want 0",
			deleteRestoreAllocs)
	}
}

func TestPackedLogicalCutConcurrentReadFoldModel(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	const writes = 200
	var workers sync.WaitGroup
	errCh := make(chan error, 4)
	workers.Add(3)
	go func() {
		defer workers.Done()
		for i := 0; i < writes; i++ {
			key := []byte(fixture.keys[i%32])
			value := []byte(fmt.Sprintf(`{"race":%d,"lane":"writer"}`, i))
			if created, err := collection.Put(key, value); err != nil || created {
				errCh <- fmt.Errorf("Put %d = %v,%w", i, created, err)
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < writes; i++ {
			key := []byte(fixture.keys[i%32])
			if _, found, err := collection.AppendRaw(nil, key); err != nil || !found {
				errCh <- fmt.Errorf("AppendRaw %d = %v,%w", i, found, err)
				return
			}
			if collection.Generation() == 0 || collection.Len() == 0 {
				errCh <- fmt.Errorf("zero logical metadata at read %d", i)
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 20; i++ {
			snapshot, err := collection.Snapshot()
			if err != nil {
				errCh <- fmt.Errorf("Snapshot %d: %w", i, err)
				return
			}
			if snapshot.Generation() == 0 || snapshot.Len() == 0 {
				_ = snapshot.Close()
				errCh <- fmt.Errorf("zero snapshot metadata at %d", i)
				return
			}
			_ = snapshot.Close()
		}
	}()
	workers.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestPackedLogicalCutUpdateAfterSuffix(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	first := []byte(fixture.keys[0])
	second := []byte(fixture.keys[1])
	before := collection.Generation()
	if created, err := collection.Put(
		first, []byte(`{"cut":"overlay"}`),
	); err != nil || created {
		t.Fatalf("packed prefix = %v,%v", created, err)
	}
	if !collection.packedLogicalCutPending() {
		t.Fatal("packed prefix did not leave a logical suffix")
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put(second, []byte(`{"cut":"batch"}`)); err != nil {
			return err
		}
		return batch.Delete(first)
	}); err != nil {
		t.Fatal(err)
	}
	if collection.Generation() != before+2 ||
		fileLogicalCutDelta(collection.logicalCut.Load()) != 0 {
		t.Fatalf("post-batch cut = generation %d delta %d, want %d,0",
			collection.Generation(),
			fileLogicalCutDelta(collection.logicalCut.Load()), before+2)
	}
	if _, found, err := collection.AppendRaw(nil, first); err != nil || found {
		t.Fatalf("deleted prefix key = %v,%v", found, err)
	}
	if got, found, err := collection.AppendRaw(nil, second); err != nil ||
		!found || !bytes.Equal(got, []byte(`{"cut":"batch"}`)) {
		t.Fatalf("batch key = %q,%v,%v", got, found, err)
	}
}

func TestPackedLogicalCutFinalDeleteFallsBackAfterFold(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 2, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	before := collection.Generation()
	physical := collection.state.Load()
	if deleted, err := collection.Delete(
		[]byte(fixture.keys[0]),
	); err != nil || !deleted {
		t.Fatalf("first Delete = %v,%v", deleted, err)
	}
	if collection.state.Load() != physical || !collection.packedLogicalCutPending() {
		t.Fatal("first Delete did not remain on packed suffix")
	}
	if deleted, err := collection.Delete(
		[]byte(fixture.keys[1]),
	); err != nil || !deleted {
		t.Fatalf("final Delete = %v,%v", deleted, err)
	}
	if collection.Len() != 0 || collection.Generation() != before+2 {
		t.Fatalf("empty cut = generation %d length %d, want %d,0",
			collection.Generation(), collection.Len(), before+2)
	}
	if collection.state.Load() == physical ||
		fileLogicalCutDelta(collection.logicalCut.Load()) != 0 ||
		collection.primaryRouter.Load().Generation() != collection.Generation() {
		t.Fatal("final Delete did not fold before structural fallback publication")
	}
}

func TestPackedLogicalCutCheapFlushRetainsReadableCut(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	key := []byte(fixture.keys[0])
	want := []byte(`{"flush":"journal-delta"}`)
	physical := collection.state.Load()
	if created, err := collection.Put(key, want); err != nil || created {
		t.Fatalf("Put = %v,%v", created, err)
	}
	target := collection.Generation()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if collection.DurableGeneration() != target {
		t.Fatalf("durable generation = %d, want %d",
			collection.DurableGeneration(), target)
	}
	if collection.state.Load() != physical {
		t.Fatal("cheap Flush unexpectedly folded the physical graph")
	}
	if got, found, err := collection.AppendRaw(nil, key); err != nil ||
		!found || !bytes.Equal(got, want) {
		t.Fatalf("post-Flush read = %q,%v,%v", got, found, err)
	}
	if collection.Generation() != target || collection.Len() != 512 {
		t.Fatalf("post-Flush metadata = (%d,%d), want (%d,512)",
			collection.Generation(), collection.Len(), target)
	}
}
