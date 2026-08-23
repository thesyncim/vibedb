package durable

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func bufferedCurrentScanOracle(
	fixture concurrentPrimaryTestFixture,
) map[string][]byte {
	oracle := make(map[string][]byte, len(fixture.keys))
	for index, key := range fixture.keys {
		oracle[key] = bytes.Clone(fixture.values[index])
	}
	return oracle
}

func putBufferedCurrentScanRow(
	t *testing.T,
	collection *Collection,
	oracle map[string][]byte,
	key string,
	value []byte,
	wantCreated bool,
) {
	t.Helper()
	canonical := canonicalConcurrentPrimaryValue(t, value)
	created, err := collection.Put([]byte(key), value)
	if err != nil || created != wantCreated {
		t.Fatalf(
			"Put(%q) = created %v, err %v; want created %v",
			key, created, err, wantCreated,
		)
	}
	oracle[key] = canonical
}

func deleteBufferedCurrentScanRow(
	t *testing.T,
	collection *Collection,
	oracle map[string][]byte,
	key string,
) {
	t.Helper()
	deleted, err := collection.Delete([]byte(key))
	if err != nil || !deleted {
		t.Fatalf("Delete(%q) = deleted %v, err %v", key, deleted, err)
	}
	delete(oracle, key)
}

func assertBufferedCurrentScan(
	t *testing.T,
	collection *Collection,
	oracle map[string][]byte,
) {
	t.Helper()
	wantKeys := make([]string, 0, len(oracle))
	wantBytes := 0
	for key, value := range oracle {
		wantKeys = append(wantKeys, key)
		wantBytes += len(key) + len(value)
	}
	slices.Sort(wantKeys)

	at := 0
	gotBytes := 0
	scratch, err := collection.RangeRawCurrentBuffer(
		nil,
		func(key, value []byte) error {
			if at >= len(wantKeys) {
				return fmt.Errorf("unexpected trailing row %q", key)
			}
			wantKey := wantKeys[at]
			if string(key) != wantKey {
				return fmt.Errorf(
					"row %d key = %q, want %q", at, key, wantKey,
				)
			}
			if want := oracle[wantKey]; !bytes.Equal(value, want) {
				return fmt.Errorf(
					"row %d value = %s, want %s", at, value, want,
				)
			}
			gotBytes += len(key) + len(value)
			at++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("RangeRawCurrentBuffer: %v", err)
	}
	if at != len(wantKeys) || gotBytes != wantBytes {
		t.Fatalf(
			"RangeRawCurrentBuffer visited %d rows/%d bytes, want %d/%d (scratch cap %d)",
			at, gotBytes, len(wantKeys), wantBytes, cap(scratch),
		)
	}
}

func verifyBufferedCurrentScanWithHook(
	collection *Collection,
	oracle map[string][]byte,
	hook func(key []byte) error,
) error {
	wantKeys := make([]string, 0, len(oracle))
	wantBytes := 0
	for key, value := range oracle {
		wantKeys = append(wantKeys, key)
		wantBytes += len(key) + len(value)
	}
	slices.Sort(wantKeys)
	at := 0
	gotBytes := 0
	_, err := collection.RangeRawCurrentBuffer(
		nil,
		func(key, value []byte) error {
			if at >= len(wantKeys) {
				return fmt.Errorf("unexpected trailing row %q", key)
			}
			wantKey := wantKeys[at]
			if string(key) != wantKey || !bytes.Equal(value, oracle[wantKey]) {
				return fmt.Errorf(
					"row %d = %q/%s, want %q/%s",
					at, key, value, wantKey, oracle[wantKey],
				)
			}
			gotBytes += len(key) + len(value)
			at++
			if hook != nil {
				return hook(key)
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	if at != len(wantKeys) || gotBytes != wantBytes {
		return fmt.Errorf(
			"visited %d rows/%d bytes, want %d/%d",
			at, gotBytes, len(wantKeys), wantBytes,
		)
	}
	return nil
}

func TestBufferedUnifiedRangeRawCurrentBufferMergesOverlayInOrder(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 250, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracle := bufferedCurrentScanOracle(fixture)

	putBufferedCurrentScanRow(
		t, collection, oracle, fixture.keys[0],
		[]byte(`{"scan":"replacement","id":0}`), false,
	)
	deleteBufferedCurrentScanRow(t, collection, oracle, fixture.keys[1])
	putBufferedCurrentScanRow(
		t, collection, oracle, "primary-key-000000010-current-insert",
		[]byte(`{"scan":"insert"}`), true,
	)
	if len(collection.primaryPendingParents) != 0 ||
		!collection.primaryUnifiedOverlay.hasPending() {
		t.Fatalf(
			"current scan setup = %d pending parents, overlay pending %v; want 0/true",
			len(collection.primaryPendingParents),
			collection.primaryUnifiedOverlay.hasPending(),
		)
	}

	beforeState := collection.state.Load()
	beforePublished := collection.committer.PublishedGeneration()
	beforeFolded := collection.primaryUnifiedOverlay.folded.Load()
	beforeRecords := collection.primaryUnifiedOverlay.count.Load()
	assertBufferedCurrentScan(t, collection, oracle)
	if got := collection.state.Load(); got != beforeState {
		t.Fatalf("current scan changed logical state %p -> %p", beforeState, got)
	}
	if got := collection.committer.PublishedGeneration(); got != beforePublished {
		t.Fatalf(
			"current scan published physical generation %d, want unchanged %d",
			got, beforePublished,
		)
	}
	if got := collection.primaryUnifiedOverlay.folded.Load(); got != beforeFolded {
		t.Fatalf("current scan advanced folded generation %d -> %d", beforeFolded, got)
	}
	if got := collection.primaryUnifiedOverlay.count.Load(); got != beforeRecords {
		t.Fatalf("current scan changed overlay records %d -> %d", beforeRecords, got)
	}
	if !collection.primaryUnifiedOverlay.hasPending() {
		t.Fatal("current scan physically folded the row overlay")
	}

	// The current-scan API returns its only variable-width reconstruction
	// scratch. Once the largest base row has warmed that buffer, merging inline
	// overlay values must remain allocation-free across repeated scans.
	wantRows := len(oracle)
	wantBytes := 0
	for key, value := range oracle {
		wantBytes += len(key) + len(value)
	}
	visited, visitedBytes := 0, 0
	visit := func(key, value []byte) error {
		visited++
		visitedBytes += len(key) + len(value)
		return nil
	}
	var scratch []byte
	var err error
	scratch, err = collection.RangeRawCurrentBuffer(scratch, visit)
	if err != nil || visited != wantRows || visitedBytes != wantBytes {
		t.Fatalf(
			"warm current scan = %d rows/%d bytes, err %v; want %d/%d",
			visited, visitedBytes, err, wantRows, wantBytes,
		)
	}
	allocs := testing.AllocsPerRun(100, func() {
		visited, visitedBytes = 0, 0
		var runErr error
		scratch, runErr = collection.RangeRawCurrentBuffer(scratch[:0], visit)
		if runErr != nil || visited != wantRows || visitedBytes != wantBytes {
			panic("buffered current scan failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed RangeRawCurrentBuffer allocated %.2f times, want 0", allocs)
	}
}

func TestBufferedUnifiedRangeRawCurrentBufferLargeUnindexedLeaf(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 1000, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracle := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracle, fixture.keys[500],
		[]byte(`{"scan":"large-unindexed-replacement","id":500}`), false,
	)
	deleteBufferedCurrentScanRow(t, collection, oracle, fixture.keys[700])
	assertBufferedCurrentScan(t, collection, oracle)
}

func TestBufferedUnifiedRangeRawCurrentBufferUnindexed256Insert(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, storeio.CommonPrimaryLeafWideSlots, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracle := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracle, "primary-key-000000128-inserted",
		[]byte(`{"scan":"unindexed-256-insert"}`), true,
	)
	assertBufferedCurrentScan(t, collection, oracle)
}

func TestBufferedUnifiedRangeRawCurrentBufferPinsGenerationDuringScan(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 250, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracleAtStart := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracleAtStart, fixture.keys[8],
		[]byte(`{"scan":"before-future-write","id":8}`), false,
	)
	deleteBufferedCurrentScanRow(t, collection, oracleAtStart, fixture.keys[9])

	futureKey := "primary-key-000000200-mid-scan-future"
	futureValue := []byte(`{"scan":"future-insert"}`)
	mutationStart := make(chan struct{})
	mutationDone := make(chan concurrentPrimaryPutResult, 1)
	go func() {
		<-mutationStart
		created, err := collection.Put([]byte(futureKey), futureValue)
		mutationDone <- concurrentPrimaryPutResult{created: created, err: err}
	}()

	wantKeys := make([]string, 0, len(oracleAtStart))
	for key := range oracleAtStart {
		wantKeys = append(wantKeys, key)
	}
	slices.Sort(wantKeys)
	at := 0
	triggered := false
	_, err := collection.RangeRawCurrentBuffer(
		nil,
		func(key, value []byte) error {
			if at >= len(wantKeys) {
				return fmt.Errorf("future scan emitted trailing row %q", key)
			}
			wantKey := wantKeys[at]
			if string(key) != wantKey ||
				!bytes.Equal(value, oracleAtStart[wantKey]) {
				return fmt.Errorf(
					"start-cut row %d = %q/%s, want %q/%s",
					at, key, value, wantKey, oracleAtStart[wantKey],
				)
			}
			at++
			if !triggered && string(key) == fixture.keys[5] {
				triggered = true
				close(mutationStart)
				result := awaitConcurrentPrimary(
					t, mutationDone, "mid-scan future mutation",
				)
				if result.err != nil || !result.created {
					return fmt.Errorf(
						"mid-scan Put = created %v, err %v",
						result.created, result.err,
					)
				}
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("start-cut RangeRawCurrentBuffer: %v", err)
	}
	if !triggered || at != len(wantKeys) {
		t.Fatalf(
			"start-cut scan triggered %v and visited %d rows, want true/%d",
			triggered, at, len(wantKeys),
		)
	}
	if len(collection.primaryPendingParents) != 0 ||
		!collection.primaryUnifiedOverlay.hasPending() {
		t.Fatalf(
			"future mutation = %d pending parents, overlay pending %v; want 0/true",
			len(collection.primaryPendingParents),
			collection.primaryUnifiedOverlay.hasPending(),
		)
	}

	nextOracle := make(map[string][]byte, len(oracleAtStart)+1)
	for key, value := range oracleAtStart {
		nextOracle[key] = bytes.Clone(value)
	}
	nextOracle[futureKey] = canonicalConcurrentPrimaryValue(t, futureValue)
	assertBufferedCurrentScan(t, collection, nextOracle)
}

func TestBufferedUnifiedRangeRawCurrentBufferCallbackErrorReleasesFlushFence(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracle := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracle, fixture.keys[15],
		[]byte(`{"scan":"callback-error","id":15}`), false,
	)

	callbackErr := errors.New("stop current scan")
	callbacks := 0
	_, err := collection.RangeRawCurrentBuffer(
		nil,
		func(_, _ []byte) error {
			callbacks++
			return callbackErr
		},
	)
	if !errors.Is(err, callbackErr) {
		t.Fatalf("RangeRawCurrentBuffer error = %v, want %v", err, callbackErr)
	}
	if callbacks != 1 {
		t.Fatalf("callback count = %d, want 1", callbacks)
	}
	if collection.readEpochs.AnyActive() {
		t.Fatal("callback error leaked the current scan's direct reader epoch")
	}
	if active := collection.leases.Stats(collection.Generation()).Active; active != 0 {
		t.Fatalf("callback error leaked %d generation leases", active)
	}

	flushAttempt := make(chan struct{}, 1)
	previousHook := concurrentPrimaryExclusiveWaitHook
	concurrentPrimaryExclusiveWaitHook = func(name string) {
		if name == "flush" {
			flushAttempt <- struct{}{}
		}
	}
	t.Cleanup(func() { concurrentPrimaryExclusiveWaitHook = previousHook })
	flushDone := make(chan error, 1)
	go func() { flushDone <- collection.Flush() }()
	awaitConcurrentPrimary(t, flushAttempt, "Flush after callback error")
	if flushErr := awaitConcurrentPrimary(
		t, flushDone, "Flush fence release after callback error",
	); flushErr != nil {
		t.Fatal(flushErr)
	}
	if got, want := collection.DurableGeneration(), collection.Generation(); got != want {
		t.Fatalf("durable generation = %d, want current %d", got, want)
	}
}

func TestBufferedUnifiedRangeRawCurrentBufferStructuralMutationStartsNextCut(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracleAtStart := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracleAtStart, fixture.keys[8],
		[]byte(`{"scan":"captured-overlay","id":8}`), false,
	)
	deleteBufferedCurrentScanRow(t, collection, oracleAtStart, fixture.keys[9])

	futureKey := fixture.keys[200]
	futureValue := fmt.Appendf(
		nil, `{"scan":"structural-future","payload":%q}`,
		strings.Repeat("x", collection.options.InlineValueBytes+1),
	)
	futureCanonical := canonicalConcurrentPrimaryValue(t, futureValue)
	mutated := false
	scanDone := make(chan error, 1)
	go func() {
		scanErr := verifyBufferedCurrentScanWithHook(
			collection, oracleAtStart,
			func(key []byte) error {
				if mutated || string(key) != fixture.keys[5] {
					return nil
				}
				mutated = true
				created, err := collection.Put([]byte(futureKey), futureValue)
				if err != nil || created {
					return fmt.Errorf(
						"structural callback Put = created %v, err %v",
						created, err,
					)
				}
				return nil
			},
		)
		if scanErr == nil && !mutated {
			scanErr = errors.New("structural callback was not reached")
		}
		scanDone <- scanErr
	}()
	if err := awaitConcurrentPrimary(
		t, scanDone, "current scan with structural callback mutation",
	); err != nil {
		t.Fatal(err)
	}

	// The structural replacement completed before the first cursor reached this
	// later key, but belongs to the next visible cut only. The first scan checked
	// the old oracle; a fresh scan must now expose the overflow value.
	nextOracle := make(map[string][]byte, len(oracleAtStart))
	for key, value := range oracleAtStart {
		nextOracle[key] = bytes.Clone(value)
	}
	nextOracle[futureKey] = futureCanonical
	assertBufferedCurrentScan(t, collection, nextOracle)
}

func TestRangeRawCurrentDeferredSnapshotFallbackCountsOneFullScan(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 250, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	key := fixture.keys[8]
	value := fmt.Appendf(
		nil, `{"scan":"deferred-fallback","pad":%q}`,
		strings.Repeat("x", collection.options.InlineValueBytes+1),
	)
	created, err := collection.Put([]byte(key), value)
	if err != nil || created {
		t.Fatalf("replace with overflow row: created=%v err=%v", created, err)
	}

	// A replacement beyond the inline budget takes the deferred COW lane and
	// leaves a valid pending parent for checkpoint folding. RangeRawCurrent must
	// use its Snapshot fallback here and still account for one public scan, not
	// two.
	if len(collection.primaryPendingParents) == 0 {
		t.Fatal("deferred fallback fixture has no prepared parent")
	}

	before := collection.Stats().SnapshotFullScanCalls
	rows := 0
	if _, err := collection.RangeRawCurrentBuffer(nil, func(_, _ []byte) error {
		rows++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows != len(fixture.keys) {
		t.Fatalf("deferred fallback rows = %d, want %d", rows, len(fixture.keys))
	}
	if got := collection.Stats().SnapshotFullScanCalls; got != before+1 {
		t.Fatalf("deferred fallback full scans = %d, want %d", got, before+1)
	}
}

func TestBufferedUnifiedRangeRawCurrentBufferDoesNotBlockFlushCallback(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracle := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracle, fixture.keys[18],
		[]byte(`{"scan":"flush-overlap","id":18}`), false,
	)
	if !collection.primaryUnifiedOverlay.hasPending() {
		t.Fatal("flush-overlap fixture has no pending row overlay")
	}
	physicalGeneration := collection.state.Load().root.Generation
	logicalGeneration := collection.Generation()
	if logicalGeneration <= physicalGeneration {
		t.Fatalf("scan fixture cut = logical %d physical %d, want pending suffix",
			logicalGeneration, physicalGeneration)
	}

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseCallback) }) })
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- verifyBufferedCurrentScanWithHook(
			collection, oracle,
			func(key []byte) error {
				if string(key) == fixture.keys[5] {
					enteredOnce.Do(func() { close(callbackEntered) })
					<-releaseCallback
				}
				return nil
			},
		)
	}()
	awaitConcurrentPrimary(t, callbackEntered, "blocked current-scan callback")
	if got := collection.readEpochs.Minimum(logicalGeneration); got != physicalGeneration {
		t.Fatalf("blocked scan retention generation = %d, want physical %d",
			got, physicalGeneration)
	}

	flushAttempt := make(chan struct{}, 1)
	previousHook := concurrentPrimaryExclusiveWaitHook
	concurrentPrimaryExclusiveWaitHook = func(name string) {
		if name == "flush" {
			flushAttempt <- struct{}{}
		}
	}
	t.Cleanup(func() { concurrentPrimaryExclusiveWaitHook = previousHook })
	flushDone := make(chan error, 1)
	go func() { flushDone <- collection.Flush() }()
	awaitConcurrentPrimary(t, flushAttempt, "Flush during blocked scan callback")
	if err := awaitConcurrentPrimary(
		t, flushDone, "Flush completion during blocked scan callback",
	); err != nil {
		t.Fatal(err)
	}
	concurrentPrimaryExclusiveWaitHook = previousHook
	if got, want := collection.DurableGeneration(), collection.Generation(); got != want {
		t.Fatalf("overlap durable generation = %d, want %d", got, want)
	}
	// Snapshot forces the journal-delta suffix into a new physical graph while
	// the callback still walks the old one. Then an overflow replacement takes a
	// second physical publication through the allocator/reclaimer. The original
	// root's retirements must remain pinned at physicalGeneration throughout.
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	overflowValue := fmt.Appendf(
		nil, `{"scan":"later-checkpoint","payload":%q}`,
		strings.Repeat("x", collection.options.InlineValueBytes+1),
	)
	if created, err := collection.Put(
		[]byte(fixture.keys[200]), overflowValue,
	); err != nil || created {
		t.Fatalf("later checkpoint Put = created %v, err %v", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	// Rotate the two-root recovery fence beyond the scan's physical base. At
	// that point the active B epoch is the only reason B-retired extents cannot
	// enter the reusable set.
	for attempt := 0; collection.committer.FallbackGeneration() <= physicalGeneration &&
		attempt < 4; attempt++ {
		nextOverflow := fmt.Appendf(
			nil, `{"scan":"reuse-boundary-%d","payload":%q}`,
			attempt, strings.Repeat("y", collection.options.InlineValueBytes+1),
		)
		if created, err := collection.Put(
			[]byte(fixture.keys[200]), nextOverflow,
		); err != nil || created {
			t.Fatalf("recovery rotation Put %d = created %v, err %v",
				attempt, created, err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if fallback := collection.committer.FallbackGeneration(); fallback <= physicalGeneration {
		t.Fatalf("fallback generation = %d, want beyond pinned physical %d",
			fallback, physicalGeneration)
	}
	collection.writer.Lock()
	refreshErr := collection.refreshReusable(collection.state.Load())
	collection.writer.Unlock()
	if refreshErr != nil {
		t.Fatal(refreshErr)
	}
	pinnedRetired := collection.reclaimer.Stats()
	if pinnedRetired.Pending == 0 ||
		pinnedRetired.OldestRetired != physicalGeneration {
		t.Fatalf("pinned retirements = %+v, want oldest physical generation %d",
			pinnedRetired, physicalGeneration)
	}
	releaseOnce.Do(func() { close(releaseCallback) })
	if err := awaitConcurrentPrimary(
		t, scanDone, "current scan after overlapping Flush",
	); err != nil {
		t.Fatal(err)
	}
	collection.writer.Lock()
	refreshErr = collection.refreshReusable(collection.state.Load())
	collection.writer.Unlock()
	if refreshErr != nil {
		t.Fatal(refreshErr)
	}
	afterRelease := collection.reclaimer.Stats()
	if afterRelease.Pending >= pinnedRetired.Pending ||
		afterRelease.OldestRetired == physicalGeneration {
		t.Fatalf("retirements did not cross reuse boundary after scan release: before %+v after %+v",
			pinnedRetired, afterRelease)
	}
}

func TestBufferedUnifiedRangeRawCurrentBufferPanicReleasesReadersAndPagePins(t *testing.T) {
	for _, forceLease := range []bool{false, true} {
		name := "direct-epoch"
		if forceLease {
			name = "generation-lease"
		}
		t.Run(name, func(t *testing.T) {
			fixture := openConcurrentPrimaryTestFixture(
				t, 256, concurrentPrimaryTestOptions(),
			)
			collection := fixture.collection
			oracle := bufferedCurrentScanOracle(fixture)
			putBufferedCurrentScanRow(
				t, collection, oracle, fixture.keys[25],
				[]byte(`{"scan":"panic-release","id":25}`), false,
			)

			var held []storeio.ReadEpoch
			if forceLease {
				for len(held) < 64 {
					_, epoch, ok := collection.enterReadEpoch()
					if !ok {
						break
					}
					held = append(held, epoch)
				}
				if len(held) == 0 || len(held) == 64 {
					t.Fatalf("failed to saturate direct epoch table with %d claims", len(held))
				}
			}
			t.Cleanup(func() {
				for _, epoch := range held {
					epoch.Exit()
				}
			})
			beforePins := collection.Stats().PinnedPages
			panicValue := &struct{ label string }{label: name}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, _ = collection.RangeRawCurrentBuffer(
					nil,
					func(_, _ []byte) error { panic(panicValue) },
				)
			}()
			if recovered != panicValue {
				t.Fatalf("recovered panic = %#v, want %#v", recovered, panicValue)
			}
			if active := collection.leases.Stats(collection.Generation()).Active; active != 0 {
				t.Fatalf("panic leaked %d generation leases", active)
			}
			for _, epoch := range held {
				epoch.Exit()
			}
			held = held[:0]
			if collection.readEpochs.AnyActive() {
				t.Fatal("panic leaked a direct reader epoch")
			}

			// Stats takes writer exclusively. Completing it proves panic unwound the
			// short structural hold; its pin count also proves the current leaf lease
			// was released while the callback unwound.
			statsDone := make(chan Stats, 1)
			go func() { statsDone <- collection.Stats() }()
			after := awaitConcurrentPrimary(
				t, statsDone, "Stats after current-scan callback panic",
			)
			if after.PinnedPages != beforePins {
				t.Fatalf("panic changed pinned pages %d -> %d", beforePins, after.PinnedPages)
			}
		})
	}
}

func TestCollectionCloseAdmissionRejectsNewReadersWhileCurrentScanPinned(
	t *testing.T,
) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	oracle := bufferedCurrentScanOracle(fixture)
	putBufferedCurrentScanRow(
		t, collection, oracle, fixture.keys[18],
		[]byte(`{"scan":"close-admission","id":18}`), false,
	)

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseCallback) }) })
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- verifyBufferedCurrentScanWithHook(
			collection, oracle,
			func([]byte) error {
				enteredOnce.Do(func() { close(callbackEntered) })
				<-releaseCallback
				return nil
			},
		)
	}()
	awaitConcurrentPrimary(t, callbackEntered, "Close-admission current scan")

	closeDone := make(chan error, 1)
	go func() { closeDone <- collection.Close() }()
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for (!collection.leases.Closing() || !collection.readEpochs.Diverted()) &&
		time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !collection.leases.Closing() || !collection.readEpochs.Diverted() {
		t.Fatal("Close did not establish sticky reader admission")
	}

	assertClosedPromptly := func(name string, operation func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- operation() }()
		select {
		case err := <-done:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("post-admission %s = %v, want ErrClosed", name, err)
			}
		case <-time.After(concurrentPrimaryTestTimeout):
			t.Fatalf("post-admission %s did not return promptly", name)
		}
	}
	assertClosedPromptly("AppendRaw", func() error {
		_, _, err := collection.AppendRaw(nil, []byte(fixture.keys[0]))
		return err
	})
	assertClosedPromptly("Snapshot", func() error {
		snapshot, err := collection.Snapshot()
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return err
	})
	assertClosedPromptly("RangeRawCurrent", func() error {
		return collection.RangeRawCurrent(func([]byte, []byte) error { return nil })
	})

	if err := awaitConcurrentPrimary(
		t, closeDone, "Close with pinned current scan",
	); !errors.Is(err, storeio.ErrLeasesActive) {
		t.Fatalf("Close with pinned current scan = %v, want ErrLeasesActive", err)
	}
	releaseOnce.Do(func() { close(releaseCallback) })
	if err := awaitConcurrentPrimary(
		t, scanDone, "pinned current scan after Close admission",
	); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}
