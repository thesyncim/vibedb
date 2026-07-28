package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestFilePrimaryPutDeleteSnapshotCOW(t *testing.T) {
	for _, durability := range []DurabilityMode{
		DurabilitySync,
		DurabilityBufferedVisible,
	} {
		t.Run(fmt.Sprint(durability), func(t *testing.T) {
			built, keys, values := buildFilePrimaryCorpus(t, 1_000)
			options := Options{
				Backend: BackendPortable, ResidentBytes: 32 << 20,
				Durability: durability,
			}
			file := createPrimaryPointFile(
				t, built, options, "primary-mutate.vibe",
			)
			collection, err := Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			snapshot, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()

			updated := []byte(`{"id":500,"updated":true}`)
			if created, err := collection.Put(
				keys[500], updated,
			); err != nil || created {
				t.Fatalf("update = %v,%v", created, err)
			}
			insertedKey := "primary-key-000000500-extra"
			inserted := []byte(`{"inserted":true}`)
			if created, err := collection.Put(
				insertedKey, inserted,
			); err != nil || !created {
				t.Fatalf("insert = %v,%v", created, err)
			}
			if deleted, err := collection.Delete(
				keys[250],
			); err != nil || !deleted {
				t.Fatalf("delete = %v,%v", deleted, err)
			}
			if collection.Len() != 1_000 {
				t.Fatalf("Len = %d, want 1000", collection.Len())
			}
			assertPrimaryRaw(t, collection, keys[500], updated, true)
			assertPrimaryRaw(t, collection, insertedKey, inserted, true)
			assertPrimaryRaw(t, collection, keys[250], nil, false)

			buffer := make([]byte, 0, 128)
			got, ok, err := snapshot.AppendRaw(buffer[:0], keys[500])
			if err != nil || !ok || !bytes.Equal(got, values[500]) {
				t.Fatalf("snapshot update = %q,%v,%v", got, ok, err)
			}
			got, ok, err = snapshot.AppendRaw(buffer[:0], insertedKey)
			if err != nil || ok || len(got) != 0 {
				t.Fatalf("snapshot insert = %q,%v,%v", got, ok, err)
			}
			got, ok, err = snapshot.AppendRaw(buffer[:0], keys[250])
			if err != nil || !ok || !bytes.Equal(got, values[250]) {
				t.Fatalf("snapshot delete = %q,%v,%v", got, ok, err)
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := collection.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertPrimaryRaw(t, reopened, keys[500], updated, true)
			assertPrimaryRaw(t, reopened, insertedKey, inserted, true)
			assertPrimaryRaw(t, reopened, keys[250], nil, false)
		})
	}
}

func TestFilePrimaryBufferedCanonicalFrame(t *testing.T) {
	built, keys, values := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-buffered.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	first := []byte(`{"id":500,"hot":"first"}`)
	second := []byte(`{"id":500,"hot":"other"}`)
	third := []byte(`{"id":500,"hot":"third"}`)
	fourth := []byte(`{"id":500,"hot":"final"}`)
	if len(first) != len(second) || len(second) != len(third) ||
		len(third) != len(fourth) {
		t.Fatal("same-size fixture drift")
	}
	if created, err := collection.Put(keys[500], first); err != nil || created {
		t.Fatalf("first update = %v,%v", created, err)
	}
	afterFirst := collection.Stats()
	if afterFirst.BufferedInplaceUpdates != 0 {
		t.Fatalf(
			"first-touch in-place updates = %d, want 0",
			afterFirst.BufferedInplaceUpdates,
		)
	}
	if created, err := collection.Put(keys[500], second); err != nil || created {
		t.Fatalf("second update = %v,%v", created, err)
	}
	afterSecond := collection.Stats()
	if afterSecond.BufferedInplaceUpdates != 0 {
		t.Fatalf(
			"same-size first-touch in-place updates = %d, want 0",
			afterSecond.BufferedInplaceUpdates,
		)
	}
	if created, err := collection.Put(keys[500], third); err != nil || created {
		t.Fatalf("third update = %v,%v", created, err)
	}
	afterThird := collection.Stats()
	if afterThird.BufferedInplaceUpdates != 1 {
		t.Fatalf(
			"same-size second-touch in-place updates = %d, want 1",
			afterThird.BufferedInplaceUpdates,
		)
	}
	assertPrimaryRaw(t, collection, keys[500], third, true)
	state := collection.state.Load()
	buffer := make([]byte, 0, 128)
	got, ok, err := collection.resolvePrimaryGraphPageWalk(
		buffer[:0], state, keys[500],
	)
	if err != nil || !ok || !bytes.Equal(got, values[500]) {
		t.Fatalf(
			"pre-checkpoint page-walk = %q,%v,%v, want sealed %q",
			got, ok, err, values[500],
		)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if created, err := collection.Put(keys[500], fourth); err != nil || created {
		t.Fatalf("snapshot-forced COW = %v,%v", created, err)
	}
	assertPrimaryRaw(t, collection, keys[500], fourth, true)
	got, ok, err = snapshot.AppendRaw(buffer[:0], keys[500])
	if err != nil || !ok || !bytes.Equal(got, third) {
		t.Fatalf("snapshot after forced COW = %q,%v,%v", got, ok, err)
	}
	if collection.Stats().BufferedInplaceFallbacks == 0 {
		t.Fatal("snapshot-forced COW did not count an in-place fallback")
	}
}

func TestFilePrimaryBufferedCrashBoundary(t *testing.T) {
	built, keys, values := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-crash-source.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	sealedRoot := collection.state.Load().root.PrimaryRoot
	sealedSize, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	sealedRetired := collection.Stats().PendingRetiredExtents
	updated := []byte(`{"id":500,"crash":"buffered"}`)
	if created, err := collection.Put(
		keys[500], updated,
	); err != nil || created {
		t.Fatalf("buffered update = %v,%v", created, err)
	}
	assertPrimaryRaw(t, collection, keys[500], updated, true)
	if got := collection.state.Load().root.PrimaryRoot; got != sealedRoot {
		t.Fatalf(
			"acknowledgement materialized primary root %v, want sealed %v",
			got, sealedRoot,
		)
	}
	if got := len(collection.primaryPendingParents); got != 1 {
		t.Fatalf("pending parents = %d, want 1", got)
	}
	if got := collection.Stats().PendingRetiredExtents; got != sealedRetired {
		t.Fatalf(
			"pre-checkpoint retired extents = %d, want %d",
			got, sealedRetired,
		)
	}
	afterAckSize, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if afterAckSize.Size() != sealedSize.Size() {
		t.Fatalf(
			"acknowledgement file size = %d, want sealed %d",
			afterAckSize.Size(), sealedSize.Size(),
		)
	}

	before := clonePrimaryCrashFile(t, file, "before-checkpoint.vibe")
	beforeCollection, err := Open(before, options)
	if err != nil {
		t.Fatal(err)
	}
	assertPrimaryRaw(t, beforeCollection, keys[500], values[500], true)
	if err := beforeCollection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}

	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(collection.primaryPendingParents) != 0 {
		t.Fatal("checkpoint retained pending primary parents")
	}
	after := clonePrimaryCrashFile(t, file, "after-checkpoint.vibe")
	afterCollection, err := Open(after, options)
	if err != nil {
		t.Fatal(err)
	}
	assertPrimaryRaw(t, afterCollection, keys[500], updated, true)
	if err := afterCollection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := after.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFilePrimaryBufferedParentChainOncePerCheckpoint(t *testing.T) {
	built, keys, values := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-deferred-parents.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	state := collection.state.Load()
	firstIndex := 500
	firstKey := keys[firstIndex]
	firstResident, err := collection.currentPrimaryResidentRoute(
		state, []byte(firstKey),
	)
	if err != nil {
		t.Fatal(err)
	}
	secondIndex := -1
	var secondResident storeio.ResidentPrimaryRoute
	for index, candidate := range keys {
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(candidate),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket != firstResident.Bucket {
			secondIndex = index
			secondResident = route
			break
		}
	}
	if secondIndex < 0 {
		t.Fatal("fixture did not span two routed leaves")
	}
	secondKey := keys[secondIndex]
	sealedRefs := make([]storeio.PageRef, 0, 12)
	addSealed := func(ref storeio.PageRef) {
		if slices.Contains(sealedRefs, ref) {
			return
		}
		sealedRefs = append(sealedRefs, ref)
	}
	for _, selected := range []struct {
		key      string
		resident storeio.ResidentPrimaryRoute
	}{
		{key: firstKey, resident: firstResident},
		{key: secondKey, resident: secondResident},
	} {
		var path filePrimaryMutationPath
		if err := collection.acquirePrimaryMutationPath(
			&path, state, []byte(selected.key),
			selected.resident,
		); err != nil {
			t.Fatal(err)
		}
		addSealed(path.leafRoute.Ref)
		addSealed(path.anchorRoute.Ref)
		addSealed(path.tabletRoute.Ref)
		addSealed(path.catalogRef)
		if path.hasBranch {
			addSealed(path.branchRef)
		}
		addSealed(state.root.PrimaryRoot)
		path.Release()
	}
	wantRetired := uint64(len(sealedRefs))
	sealedRoot := state.root.PrimaryRoot
	before := collection.Stats()
	updates := []struct {
		key     string
		value   []byte
		pending int
	}{
		{
			key:     firstKey,
			value:   []byte(`{"value":"first deferred value"}`),
			pending: 1,
		},
		{
			key:     secondKey,
			value:   []byte(`{"value":"second deferred leaf"}`),
			pending: 2,
		},
		{
			key:     firstKey,
			value:   []byte(`{"value":"final"}`),
			pending: 2,
		},
	}
	for index, update := range updates {
		if created, putErr := collection.Put(
			update.key, update.value,
		); putErr != nil || created {
			t.Fatalf(
				"Put %d = %v,%v", index, created, putErr,
			)
		}
		if got := collection.state.Load().root.PrimaryRoot; got != sealedRoot {
			t.Fatalf(
				"Put %d rewrote root %v, want %v",
				index, got, sealedRoot,
			)
		}
		if got := len(collection.primaryPendingParents); got != update.pending {
			t.Fatalf(
				"Put %d pending parents = %d, want %d",
				index, got, update.pending,
			)
		}
		assertPrimaryRaw(
			t, collection, update.key, update.value, true,
		)
	}
	pageWalk, ok, err := collection.resolvePrimaryGraphPageWalk(
		nil, collection.state.Load(), firstKey,
	)
	if err != nil || !ok ||
		!bytes.Equal(pageWalk, values[firstIndex]) {
		t.Fatalf(
			"pre-checkpoint walk = %q,%v,%v, want sealed %q",
			pageWalk, ok, err, values[firstIndex],
		)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	after := collection.Stats()
	if got := after.PendingRetiredExtents -
		before.PendingRetiredExtents; got != wantRetired {
		t.Fatalf(
			"checkpoint retired extents = %d, want one chain (%d)",
			got, wantRetired,
		)
	}
	if after.PublishedGeneration != collection.Generation() ||
		after.DurableGeneration != collection.Generation() {
		t.Fatalf(
			"checkpoint generations = published %d durable %d visible %d",
			after.PublishedGeneration, after.DurableGeneration,
			collection.Generation(),
		)
	}
	pageWalk, ok, err = collection.resolvePrimaryGraphPageWalk(
		pageWalk[:0], collection.state.Load(), firstKey,
	)
	if err != nil || !ok ||
		!bytes.Equal(pageWalk, updates[len(updates)-1].value) {
		t.Fatalf(
			"checkpoint walk = %q,%v,%v, want %q",
			pageWalk, ok, err, updates[len(updates)-1].value,
		)
	}
	pageWalk, ok, err = collection.resolvePrimaryGraphPageWalk(
		pageWalk[:0], collection.state.Load(), secondKey,
	)
	if err != nil || !ok ||
		!bytes.Equal(pageWalk, updates[1].value) {
		t.Fatalf(
			"second checkpoint walk = %q,%v,%v, want %q",
			pageWalk, ok, err, updates[1].value,
		)
	}
}

func TestFilePrimaryBufferedPendingCapacityForcesCheckpoint(t *testing.T) {
	built, keys, _ := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-pending-capacity.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	collection.primaryPendingParents = make(
		[]filePrimaryPendingParent, 0, 1,
	)

	state := collection.state.Load()
	first := keys[0]
	firstRoute, err := collection.currentPrimaryResidentRoute(
		state, []byte(first),
	)
	if err != nil {
		t.Fatal(err)
	}
	second := ""
	for _, candidate := range keys[1:] {
		route, routeErr := collection.currentPrimaryResidentRoute(
			state, []byte(candidate),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Bucket != firstRoute.Bucket {
			second = candidate
			break
		}
	}
	if second == "" {
		t.Fatal("fixture did not span two routed leaves")
	}
	firstValue := []byte(`{"capacity":"first"}`)
	secondValue := []byte(`{"capacity":"second"}`)
	if _, err := collection.Put(first, firstValue); err != nil {
		t.Fatal(err)
	}
	firstGeneration := collection.Generation()
	before := collection.Stats()
	if _, err := collection.Put(second, secondValue); err != nil {
		t.Fatal(err)
	}
	after := collection.Stats()
	if after.AutomaticCheckpoints != before.AutomaticCheckpoints+1 {
		t.Fatalf(
			"automatic checkpoints = %d, want %d",
			after.AutomaticCheckpoints,
			before.AutomaticCheckpoints+1,
		)
	}
	if after.DurableGeneration != firstGeneration {
		t.Fatalf(
			"capacity durable generation = %d, want first cut %d",
			after.DurableGeneration, firstGeneration,
		)
	}
	if len(collection.primaryPendingParents) != 1 {
		t.Fatalf(
			"new checkpoint window pending parents = %d, want 1",
			len(collection.primaryPendingParents),
		)
	}
	assertPrimaryRaw(t, collection, first, firstValue, true)
	assertPrimaryRaw(t, collection, second, secondValue, true)
}

func TestFilePrimaryLeafSplitSignal(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "primary-split.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	// Routed contract: with putPrimaryWithSplit live, a full leaf performs a
	// bounded structural split and the mutation succeeds. Insert well past a
	// single narrow leaf's ~195-live capacity so several splits fire, and assert
	// every Put is accepted -- ErrPrimaryLeafSplitRequired must never surface to
	// the caller at leaf scale. The only remaining signal is
	// ErrPrimaryMacroSplitRequired, which needs a tablet's full 4096 local IDs
	// (~400k keys) and so is a bound, not a leaf-scale event.
	const inserts = 2048
	oracle := make(map[string][]byte, inserts)
	for at := range inserts {
		key := fmt.Sprintf("key-%04d", at)
		value := []byte(fmt.Sprintf(`{"v":%d}`, at))
		created, putErr := collection.Put(key, value)
		if errors.Is(putErr, ErrPrimaryLeafSplitRequired) {
			t.Fatalf("routed put %q surfaced a leaf-split signal", key)
		}
		if putErr != nil || !created {
			t.Fatalf("routed put %q = created %v, err %v", key, created, putErr)
		}
		oracle[key] = value
	}
	if got := collection.Stats().PrimaryLeafSplits; got == 0 {
		t.Fatal("no leaf split fired across 2048 inserts into one tablet")
	}
	if got := collection.Stats().PrimaryMacroSplitRequired; got != 0 {
		t.Fatalf("unexpected macro-split at leaf scale: %d", got)
	}

	// A completed structural transaction is a checkpoint, and Flush drains any
	// buffered puts trailing the last split, so no pending parents survive it.
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(collection.primaryPendingParents) != 0 {
		t.Fatalf(
			"pending parents after routed splits and Flush = %d, want 0",
			len(collection.primaryPendingParents),
		)
	}
	if after := collection.Stats(); after.DurableGeneration != collection.Generation() {
		t.Fatalf(
			"routed splits left durable %d behind visible %d",
			after.DurableGeneration, collection.Generation(),
		)
	}

	buf := make([]byte, 0, 32)
	for key, want := range oracle {
		got, ok, readErr := collection.AppendRaw(buf[:0], key)
		if readErr != nil || !ok || !bytes.Equal(got, want) {
			t.Fatalf(
				"routed split readback %q = %q,%v,%v want %q",
				key, got, ok, readErr, want,
			)
		}
		buf = got
	}
	if got, ok, readErr := collection.AppendRaw(
		buf[:0], "seed",
	); readErr != nil || !ok || !bytes.Equal(got, []byte(`{"v":0}`)) {
		t.Fatalf("seed readback across splits = %q,%v,%v", got, ok, readErr)
	}
}

func TestFilePrimaryEmptyLeafAccounting(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"v":0}`)
	if err := builder.Append("seed", value); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "primary-empty.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if deleted, err := collection.Delete("seed"); err != nil || !deleted {
		t.Fatalf("delete = %v,%v", deleted, err)
	}
	if collection.Stats().PrimaryEmptyLeaves != 1 {
		t.Fatalf(
			"empty leaves = %d, want 1",
			collection.Stats().PrimaryEmptyLeaves,
		)
	}
	if created, err := collection.Put("seed", value); err != nil || !created {
		t.Fatalf("refill = %v,%v", created, err)
	}
	if collection.Stats().PrimaryEmptyLeaves != 0 {
		t.Fatalf(
			"empty leaves after refill = %d, want 0",
			collection.Stats().PrimaryEmptyLeaves,
		)
	}
}

func TestFilePrimaryMutationDifferentialSmoke(t *testing.T) {
	runPrimaryMutationDifferential(
		t, DurabilityBufferedVisible, 1_000, 512,
	)
	runPrimaryMutationDifferential(
		t, DurabilitySync, 1_000, 64,
	)
}

func TestFilePrimaryConcurrentRouterPublication(t *testing.T) {
	built, keys, values := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-concurrent.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	key := keys[500]
	first := []byte(`{"v":"first"}`)
	second := []byte(`{"v":"a-longer-second"}`)
	var (
		done      atomic.Bool
		writerWG  sync.WaitGroup
		writerErr error
	)
	writerWG.Go(func() {
		defer done.Store(true)
		for at := range 200 {
			value := first
			if at&1 != 0 {
				value = second
			}
			if _, putErr := collection.Put(key, value); putErr != nil {
				writerErr = putErr
				return
			}
		}
	})
	currentBuffer := make([]byte, 0, 128)
	snapshotBuffer := make([]byte, 0, 128)
	for !done.Load() {
		current, ok, readErr := collection.AppendRaw(
			currentBuffer[:0], key,
		)
		if readErr != nil || !ok ||
			!bytes.Equal(current, values[500]) &&
				!bytes.Equal(current, first) &&
				!bytes.Equal(current, second) {
			t.Fatalf("current read = %q,%v,%v", current, ok, readErr)
		}
		pinned, pinnedOK, pinnedErr := snapshot.AppendRaw(
			snapshotBuffer[:0], key,
		)
		if pinnedErr != nil || !pinnedOK ||
			!bytes.Equal(pinned, values[500]) {
			t.Fatalf(
				"snapshot read = %q,%v,%v",
				pinned, pinnedOK, pinnedErr,
			)
		}
		currentBuffer = current
		snapshotBuffer = pinned
	}
	writerWG.Wait()
	if writerErr != nil {
		t.Fatal(writerErr)
	}
}

func TestFilePrimaryMutationDifferential10KAnd100K(t *testing.T) {
	if os.Getenv("VIBEJSON_PRIMARY_LONG") == "" {
		t.Skip("set VIBEJSON_PRIMARY_LONG=1 for 10k/100k mutation traces")
	}
	for _, operations := range []int{10_000, 100_000} {
		for _, durability := range []DurabilityMode{
			DurabilityBufferedVisible,
			DurabilitySync,
		} {
			t.Run(
				fmt.Sprintf("%d/%d", operations, durability),
				func(t *testing.T) {
					runPrimaryMutationDifferential(
						t, durability, 10_000, operations,
					)
				},
			)
		}
	}
}

func runPrimaryMutationDifferential(
	t testing.TB,
	durability DurabilityMode,
	count, operations int,
) {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, count)
	oracle := make(map[string][]byte, count)
	for at := range count {
		key := fmt.Sprintf("trace-key-%06d", at)
		value := []byte(fmt.Sprintf(`{"v":"%06d","p":"a"}`, at))
		keys[at] = key
		oracle[key] = append([]byte(nil), value...)
		if err := builder.Append(key, value); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability: durability,
	}
	file := createPrimaryPointFile(t, built, options, "primary-trace.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	random := uint64(0x9e3779b97f4a7c15)
	buffer := make([]byte, 0, 64)
	oracleBuffer := make([]byte, 0, 64)
	for operation := range operations {
		random ^= random << 7
		random ^= random >> 9
		random ^= random << 8
		key := keys[int(random%uint64(len(keys)))]
		if random>>63 == 0 {
			deleted, deleteErr := collection.Delete(key)
			_, existed := oracle[key]
			if deleteErr != nil || deleted != existed {
				t.Fatalf(
					"delete %d %q = %v,%v, want %v,nil",
					operation, key, deleted, deleteErr, existed,
				)
			}
			delete(oracle, key)
		} else {
			value := []byte(fmt.Sprintf(
				`{"v":"%06d","p":"b"}`, operation%1_000_000,
			))
			_, existed := oracle[key]
			created, putErr := collection.Put(key, value)
			if putErr != nil || created == existed {
				t.Fatalf(
					"put %d %q = %v,%v, existed=%v",
					operation, key, created, putErr, existed,
				)
			}
			oracle[key] = append(oracle[key][:0], value...)
		}
		want, wantOK := oracle[key]
		got, ok, readErr := collection.AppendRaw(buffer[:0], key)
		if readErr != nil || ok != wantOK || !bytes.Equal(got, want) {
			t.Fatalf(
				"read %d %q = %q,%v,%v; want %q,%v,nil",
				operation, key, got, ok, readErr, want, wantOK,
			)
		}
		if durability != DurabilityBufferedVisible ||
			operation%64 == 63 {
			if durability == DurabilityBufferedVisible {
				if flushErr := collection.Flush(); flushErr != nil {
					t.Fatalf(
						"checkpoint %d: %v", operation, flushErr,
					)
				}
			}
			state := collection.state.Load()
			pageWalk, pageWalkOK, pageWalkErr :=
				collection.resolvePrimaryGraphPageWalk(
					oracleBuffer[:0], state, key,
				)
			if pageWalkErr != nil || pageWalkOK != wantOK ||
				!bytes.Equal(pageWalk, want) {
				t.Fatalf(
					"page-walk %d %q = %q,%v,%v; want %q,%v,nil",
					operation, key, pageWalk, pageWalkOK,
					pageWalkErr, want, wantOK,
				)
			}
			oracleBuffer = pageWalk
		}
		buffer = got
	}
	if collection.Len() != uint64(len(oracle)) {
		t.Fatalf(
			"Len = %d, want %d",
			collection.Len(), len(oracle),
		)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
}

func assertPrimaryRaw(
	t testing.TB,
	collection *Collection,
	key string,
	want []byte,
	wantOK bool,
) {
	t.Helper()
	got, ok, err := collection.AppendRaw(
		make([]byte, 0, max(len(want), 1)), key,
	)
	if err != nil || ok != wantOK || !bytes.Equal(got, want) {
		t.Fatalf(
			"AppendRaw(%q) = %q,%v,%v; want %q,%v,nil",
			key, got, ok, err, want, wantOK,
		)
	}
}

func clonePrimaryCrashFile(
	t testing.TB,
	source *os.File,
	name string,
) *os.File {
	t.Helper()
	image, err := os.ReadFile(source.Name())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// TestFilePrimaryRoutedSplitDifferential net-grows a primary graph from a single
// seed to 30k keys in shuffled order so leaf splits fire continuously through the
// routed Put path, validating every insert against a map oracle and then the full
// ordered contents. Short mode grows to 10k. This is the routed-split correctness
// gate: the structural transactions must preserve exact point and ordered-scan
// semantics against the oracle.
func TestFilePrimaryRoutedSplitDifferential(t *testing.T) {
	target := 30_000
	if testing.Short() {
		target = 10_000
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "routed-split-diff.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	// Shuffle the insert order (xorshift Fisher-Yates, no import) so splits land
	// across the whole keyspace instead of always at the tail.
	order := make([]int, target)
	for i := range order {
		order[i] = i
	}
	rng := uint64(0x9e3779b97f4a7c15)
	for i := target - 1; i > 0; i-- {
		rng ^= rng << 7
		rng ^= rng >> 9
		rng ^= rng << 8
		j := int(rng % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	oracle := map[string][]byte{"seed": []byte(`{"v":0}`)}
	buffer := make([]byte, 0, 64)
	for at, id := range order {
		key := fmt.Sprintf("rk-%08d", id)
		value := []byte(fmt.Sprintf(`{"id":%d,"n":%d}`, id, at))
		created, putErr := collection.Put(key, value)
		if putErr != nil || !created {
			t.Fatalf("insert %d %q = created %v, err %v", at, key, created, putErr)
		}
		oracle[key] = value
		got, ok, readErr := collection.AppendRaw(buffer[:0], key)
		if readErr != nil || !ok || !bytes.Equal(got, value) {
			t.Fatalf("post-insert read %q = %q,%v,%v want %q", key, got, ok, readErr, value)
		}
		buffer = got
		if at%256 == 255 {
			if flushErr := collection.Flush(); flushErr != nil {
				t.Fatalf("checkpoint at insert %d: %v", at, flushErr)
			}
		}
	}
	if flushErr := collection.Flush(); flushErr != nil {
		t.Fatal(flushErr)
	}
	if stats := collection.Stats(); stats.PrimaryLeafSplits == 0 {
		t.Fatalf("net growth to %d keys fired no leaf splits", target)
	}
	if got := int(collection.Len()); got != len(oracle) {
		t.Fatalf("Len = %d, want %d", got, len(oracle))
	}

	// Point differential: every oracle key resolves byte-exact.
	for key, want := range oracle {
		got, ok, readErr := collection.AppendRaw(buffer[:0], key)
		if readErr != nil || !ok || !bytes.Equal(got, want) {
			t.Fatalf("point differential %q = %q,%v,%v want %q", key, got, ok, readErr, want)
		}
		buffer = got
	}

	// Ordered-scan differential: the full scan yields exactly the oracle keys in
	// strict lexical order with byte-exact values.
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	seen := 0
	var prevKey string
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		if seen > 0 && string(key) <= prevKey {
			t.Fatalf("scan order violation: %q after %q", key, prevKey)
		}
		prevKey = string(key)
		want, ok := oracle[string(key)]
		if !ok || !bytes.Equal(value, want) {
			t.Fatalf("scan differential %q = %q, want %q (present=%v)", key, value, want, ok)
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(oracle) {
		t.Fatalf("scan visited %d keys, want %d", seen, len(oracle))
	}
}
