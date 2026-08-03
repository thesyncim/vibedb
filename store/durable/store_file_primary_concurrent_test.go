package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

const concurrentPrimaryTestTimeout = 10 * time.Second

type concurrentPrimaryTestFixture struct {
	collection *Collection
	file       *os.File
	path       string
	keys       []string
	values     [][]byte
}

type concurrentPrimaryTestBucket struct {
	id      storeio.BucketID
	indices []int
}

type concurrentPrimaryPutResult struct {
	created bool
	err     error
}

type concurrentPrimaryDeleteResult struct {
	deleted bool
	err     error
}

func concurrentPrimaryTestOptions() Options {
	return Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability:         DurabilityBufferedVisible,
		CheckpointStrength: CheckpointFilesystem,
	}
}

func openConcurrentPrimaryTestFixture(
	t *testing.T, count int, options Options,
) concurrentPrimaryTestFixture {
	return openConcurrentPrimaryTestFixtureMode(t, count, options, true)
}

func openConcurrentPrimaryTestFixtureMode(
	t *testing.T, count int, options Options, rootJournal bool,
) concurrentPrimaryTestFixture {
	t.Helper()
	built, keys, values := buildFilePrimaryCorpus(t, count)
	file := createPrimaryPointFile(
		t, built, options, "concurrent-primary.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	collection.writer.Lock()
	if err := collection.repartitionPrimaryForExactIndexLocked(context.Background()); err != nil {
		collection.writer.Unlock()
		t.Fatal(err)
	}
	collection.writer.Unlock()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if rootJournal {
		// Production bulk images intentionally omit the ordinary buffered journal.
		// Root that one-time lazy identity before measuring the concurrent lane,
		// then clear the priming mutation's public fast-path counters.
		if created, err := collection.Put([]byte(keys[0]), values[0]); err != nil || created {
			t.Fatalf("prime lazy journal: created=%v err=%v", created, err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		collection.concurrentPrimaryReplaces.Store(0)
		collection.concurrentPrimaryFallbacks.Store(0)
		collection.concurrentPrimaryPublishGroups.Store(0)
		collection.concurrentPrimaryLargestPublishGroup.Store(0)
	}
	t.Cleanup(func() {
		if err := collection.Close(); err != nil {
			t.Errorf("close concurrent primary fixture: %v", err)
		}
	})
	return concurrentPrimaryTestFixture{
		collection: collection, file: file, path: file.Name(),
		keys: keys, values: values,
	}
}

func openConcurrentPrimarySeededTestFixture(
	t *testing.T, options Options,
) concurrentPrimaryTestFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "concurrent-primary-seeded.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := collection.Close(); err != nil {
			t.Errorf("close seeded concurrent primary fixture: %v", err)
		}
	})
	key := "schema-key"
	value := canonicalConcurrentPrimaryValue(t, []byte(`{"id":0,"seed":true}`))
	created, err := collection.Put([]byte(key), value)
	if err != nil || !created {
		t.Fatalf("seed schema fixture: created=%v err=%v", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	return concurrentPrimaryTestFixture{
		collection: collection, file: file, path: path,
		keys: []string{key}, values: [][]byte{value},
	}
}

func concurrentPrimaryTestBuckets(
	t *testing.T, fixture concurrentPrimaryTestFixture,
) []concurrentPrimaryTestBucket {
	t.Helper()
	router := fixture.collection.primaryRouter.Load()
	if router == nil {
		t.Fatal("primary router is unavailable")
	}
	byID := make(map[storeio.BucketID]int)
	buckets := make([]concurrentPrimaryTestBucket, 0, router.Len())
	for i, key := range fixture.keys {
		route, ok := router.Route([]byte(key))
		if !ok {
			t.Fatalf("route existing key %q", key)
		}
		bucketAt, exists := byID[route.Bucket]
		if !exists {
			lease, err := router.AcquireLeaf(fixture.collection.cache, route)
			if err != nil {
				t.Fatalf("acquire bucket %d: %v", route.Bucket, err)
			}
			unified := storeio.PrimaryLeafClass(lease.Page()) ==
				storeio.CommonPrimaryLeafCompact
			lease.Release()
			if !unified {
				continue
			}
			bucketAt = len(buckets)
			byID[route.Bucket] = bucketAt
			buckets = append(buckets, concurrentPrimaryTestBucket{id: route.Bucket})
		}
		buckets[bucketAt].indices = append(buckets[bucketAt].indices, i)
	}
	return buckets
}

func concurrentPrimaryTestTargets(
	t *testing.T, fixture concurrentPrimaryTestFixture,
) (same [2]int, disjoint [2]int) {
	t.Helper()
	buckets := concurrentPrimaryTestBuckets(t, fixture)
	sameFound := false
	disjointFirstFound := false
	for _, bucket := range buckets {
		if len(bucket.indices) >= 2 && !sameFound {
			same = [2]int{bucket.indices[0], bucket.indices[1]}
			sameFound = true
		}
		if len(bucket.indices) == 0 {
			continue
		}
		if !disjointFirstFound {
			disjoint[0] = bucket.indices[0]
			disjointFirstFound = true
			continue
		}
		firstRoute, _ := fixture.collection.primaryRouter.Load().Route(
			[]byte(fixture.keys[disjoint[0]]),
		)
		if bucket.id != firstRoute.Bucket &&
			primaryConcurrentStripeIndex(bucket.id) !=
				primaryConcurrentStripeIndex(firstRoute.Bucket) {
			disjoint[1] = bucket.indices[0]
			break
		}
	}
	if !sameFound {
		t.Fatalf("%d keys supplied no compact bucket with two rows", len(fixture.keys))
	}
	if disjoint[1] == 0 {
		t.Fatalf("%d keys supplied no two compact buckets on distinct stripes", len(fixture.keys))
	}
	return same, disjoint
}

func canonicalConcurrentPrimaryValue(t *testing.T, src []byte) []byte {
	t.Helper()
	out, err := vibejson.AppendCanonicalize(nil, src)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func awaitConcurrentPrimary[T any](
	t *testing.T, channel <-chan T, label string,
) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(concurrentPrimaryTestTimeout):
		var zero T
		t.Fatalf("timed out waiting for %s", label)
		return zero
	}
}

func assertConcurrentPrimaryRaw(
	t *testing.T, collection *Collection, key, want []byte,
) {
	t.Helper()
	got, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found {
		t.Fatalf("AppendRaw(%q): found=%v err=%v", key, found, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("AppendRaw(%q) = %s, want %s", key, got, want)
	}
}

func TestConcurrentPrimaryPublisherPreservesNonPressureErrorForSuffix(
	t *testing.T,
) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	route, ok := coll.primaryRouter.Load().Route(key)
	if !ok {
		t.Fatal("route existing key")
	}
	baseGeneration := coll.Generation()
	baseRecords := coll.primaryUnifiedOverlay.count.Load()

	requests := [2]primaryConcurrentPublishRequest{}
	for index := range requests {
		requests[index] = primaryConcurrentPublishRequest{
			key: key, canonical: []byte(`{"publisher":"suffix"}`),
			route: route, kind: primaryUnifiedOverlayPut,
			countDelta: 0,
			signal:     make(chan primaryConcurrentPublishSignal, 1),
		}
	}
	// This reaches prepareWithLeafReservation and fails its exact delta
	// validation. The second request is deliberately otherwise valid: it proves
	// the original non-capacity failure is copied to the untouched suffix.
	requests[0].countDelta = 2
	batch := []*primaryConcurrentPublishRequest{&requests[0], &requests[1]}
	coll.writer.RLock()
	coll.publishConcurrentPrimaryMutations(batch)
	coll.writer.RUnlock()

	for index := range requests {
		signal := <-requests[index].signal
		if signal.handled || !errors.Is(signal.err, storeio.ErrInvalidWrite) ||
			errors.Is(signal.err, errConcurrentPrimaryPressure) {
			t.Fatalf(
				"request %d result = handled=%v err=%v, want direct invalid-write error",
				index, signal.handled, signal.err,
			)
		}
	}
	if got := coll.Generation(); got != baseGeneration {
		t.Fatalf("generation = %d, want unchanged %d", got, baseGeneration)
	}
	if got := coll.primaryUnifiedOverlay.count.Load(); got != baseRecords {
		t.Fatalf("overlay records = %d, want unchanged %d", got, baseRecords)
	}
}

func TestConcurrentPrimaryReplaceReusesJournalCoveredArena(t *testing.T) {
	options := concurrentPrimaryTestOptions()
	fixture := openConcurrentPrimaryTestFixture(t, 512, options)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	first := []byte(`{"arena":"aaaa","revision":1}`)
	second := []byte(`{"arena":"bbbb","revision":2}`)
	if len(first) != len(second) {
		t.Fatal("test values are not the same size")
	}
	if created, err := coll.Put(key, first); err != nil || created {
		t.Fatalf("first Put = %v,%v", created, err)
	}
	firstGeneration := coll.Generation()
	used := coll.primaryUnifiedOverlay.used.Load()
	count := coll.primaryUnifiedOverlay.count.Load()
	physical := coll.committer.DurableGeneration()
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := coll.journalDeltaGeneration.Load(); got != firstGeneration {
		t.Fatalf("journal generation = %d, want %d", got, firstGeneration)
	}
	if got := coll.committer.DurableGeneration(); got != physical {
		t.Fatalf("journal-only Flush advanced physical generation %d -> %d",
			physical, got)
	}
	old := coll.primaryUnifiedOverlay.records[count-1]

	if created, err := coll.Put(key, second); err != nil || created {
		t.Fatalf("second Put = %v,%v", created, err)
	}
	if got := coll.primaryUnifiedOverlay.used.Load(); got != used {
		t.Fatalf("arena used = %d, want unchanged %d", got, used)
	}
	if got := coll.primaryUnifiedOverlay.count.Load(); got != count+1 {
		t.Fatalf("record count = %d, want %d", got, count+1)
	}
	latest := coll.primaryUnifiedOverlay.records[count]
	if latest.keyOffset != old.keyOffset ||
		latest.valueOff != old.valueOff {
		t.Fatalf("latest offsets = %d/%d, want reused %d/%d",
			latest.keyOffset, latest.valueOff,
			old.keyOffset, old.valueOff)
	}
	assertConcurrentPrimaryRaw(t, coll, key, second)

	entries, complete, err := coll.primaryUnifiedOverlay.checkpointEntries(
		make([]storeio.RecoveryBatchEntry, 0, 1),
		firstGeneration, coll.Generation(),
	)
	if err != nil || !complete || len(entries) != 1 ||
		!bytes.Equal(entries[0].Key, key) ||
		!bytes.Equal(entries[0].Value, second) {
		t.Fatalf("post-reuse journal entries = %#v complete=%v err=%v",
			entries, complete, err)
	}

	// The newer replacement is still volatile. A crash image must recover the
	// first value from the already-synced journal bytes, proving that overwriting
	// its in-memory arena copy did not weaken the previous durability cut.
	image := captureJournalImage(t, fixture.path)
	recoveryPath := filepath.Join(t.TempDir(), "arena-reuse-recovery.vibe")
	if err := os.WriteFile(recoveryPath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		recoveryPath+".rjournal", image.journal, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	recoveryFile, err := os.OpenFile(recoveryPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveryFile.Close()
	recovered, err := Open(recoveryFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	assertConcurrentPrimaryRaw(t, recovered, key, first)
}

func TestConcurrentPrimaryReplaceRepeatedArenaReuseFoldsAndReopens(t *testing.T) {
	options := concurrentPrimaryTestOptions()
	fixture := openConcurrentPrimaryTestFixture(t, 512, options)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	values := [][]byte{
		[]byte(`{"arena":"aaaa","revision":1}`),
		[]byte(`{"arena":"bbbb","revision":2}`),
		[]byte(`{"arena":"cccc","revision":3}`),
		[]byte(`{"arena":"dddd","revision":4}`),
		[]byte(`{"arena":"eeee","revision":5}`),
	}
	for index := 1; index < len(values); index++ {
		if len(values[index]) != len(values[0]) {
			t.Fatalf("test value %d length = %d, want %d",
				index, len(values[index]), len(values[0]))
		}
	}
	physicalBase := coll.committer.DurableGeneration()
	if created, err := coll.Put(key, values[0]); err != nil || created {
		t.Fatalf("initial Put = %v,%v", created, err)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := coll.committer.DurableGeneration(); got != physicalBase {
		t.Fatalf("initial journal cut advanced physical generation %d -> %d",
			physicalBase, got)
	}
	used := coll.primaryUnifiedOverlay.used.Load()
	firstRecord := coll.primaryUnifiedOverlay.records[0]

	for index, value := range values[1:] {
		count := coll.primaryUnifiedOverlay.count.Load()
		if created, err := coll.Put(key, value); err != nil || created {
			t.Fatalf("reuse Put %d = %v,%v", index+1, created, err)
		}
		if got := coll.primaryUnifiedOverlay.used.Load(); got != used {
			t.Fatalf("reuse %d arena used = %d, want unchanged %d",
				index+1, got, used)
		}
		if got := coll.primaryUnifiedOverlay.count.Load(); got != count+1 {
			t.Fatalf("reuse %d record count = %d, want %d",
				index+1, got, count+1)
		}
		latest := coll.primaryUnifiedOverlay.records[count]
		if latest.keyOffset != firstRecord.keyOffset ||
			latest.valueOff != firstRecord.valueOff {
			t.Fatalf("reuse %d offsets = %d/%d, want %d/%d",
				index+1, latest.keyOffset, latest.valueOff,
				firstRecord.keyOffset, firstRecord.valueOff)
		}
		if err := coll.Flush(); err != nil {
			t.Fatalf("journal cut %d: %v", index+1, err)
		}
		if got, want := coll.journalDeltaGeneration.Load(),
			coll.Generation(); got != want {
			t.Fatalf("journal cut %d watermark = %d, want %d",
				index+1, got, want)
		}
		if got := coll.committer.DurableGeneration(); got != physicalBase {
			t.Fatalf("journal cut %d advanced physical generation %d -> %d",
				index+1, physicalBase, got)
		}
		assertConcurrentPrimaryRaw(t, coll, key, value)
	}

	// Recovery after several overwrite/sync cycles must use the newest journal
	// bytes, even though every metadata generation still aliases one arena slot.
	image := captureJournalImage(t, fixture.path)
	recoveryPath := filepath.Join(t.TempDir(), "repeated-arena-reuse.vibe")
	if err := os.WriteFile(recoveryPath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		recoveryPath+".rjournal", image.journal, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	recoveryFile, err := os.OpenFile(recoveryPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(recoveryFile, options)
	if err != nil {
		_ = recoveryFile.Close()
		t.Fatal(err)
	}
	assertConcurrentPrimaryRaw(t, recovered, key, values[len(values)-1])
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recoveryFile.Close(); err != nil {
		t.Fatal(err)
	}

	// Folding the full alias chain must preserve generation order and aggregate
	// the initial size delta only once. Reopen the actual file after that device
	// checkpoint, rather than relying solely on the journal recovery copy above.
	finalGeneration := coll.Generation()
	if err := flushPhysicalForTest(coll); err != nil {
		t.Fatal(err)
	}
	if got := coll.committer.DurableGeneration(); got != finalGeneration {
		t.Fatalf("physical generation = %d, want %d", got, finalGeneration)
	}
	if got := coll.primaryUnifiedOverlay.count.Load(); got != 0 {
		t.Fatalf("folded overlay record count = %d, want recycled", got)
	}
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(fixture.file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Generation(); got != finalGeneration {
		t.Fatalf("reopened generation = %d, want %d", got, finalGeneration)
	}
	assertConcurrentPrimaryRaw(t, reopened, key, values[len(values)-1])
}

func TestConcurrentPrimaryIdenticalReplaceReservesActualLeafBytes(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	router := coll.primaryRouter.Load()
	route, ok := router.Route(key)
	if !ok {
		t.Fatal("route disappeared")
	}
	if route.Ref.Length >= uint32(coll.options.MaxPageSize) {
		t.Fatalf("fixture leaf extent = %d, want compact extent below %d",
			route.Ref.Length, coll.options.MaxPageSize)
	}
	if created, err := coll.Put(key, fixture.values[0]); err != nil || created {
		t.Fatalf("identical Put = %v,%v", created, err)
	}
	slot, found := coll.primaryUnifiedOverlay.bucketSlot(route.Bucket)
	if !found {
		t.Fatal("identical replacement did not publish an overlay bucket")
	}
	wantExact := route.Ref.Length +
		coll.options.primaryUnifiedOverlayParentBytes
	if got := coll.primaryUnifiedOverlay.buckets[slot].reservedBytes.Load(); got != wantExact {
		t.Fatalf("certified bucket reservation = %d, want route %d + parents %d = %d",
			got, route.Ref.Length,
			coll.options.primaryUnifiedOverlayParentBytes, wantExact)
	}

	// Any content change is not yet a compact-stream extent certificate: even a
	// same-length integer can widen a leaf-wide FOR/delta stream. It must
	// conservatively upgrade the already-dirty bucket to MaxPageSize.
	integer := bytes.Replace(
		fixture.values[0], []byte(`"group":0`), []byte(`"group":1`), 1,
	)
	if bytes.Equal(integer, fixture.values[0]) {
		t.Fatalf("fixture value has no one-digit group: %s", fixture.values[0])
	}
	if created, err := coll.Put(key, integer); err != nil || created {
		t.Fatalf("changed integer Put = %v,%v", created, err)
	}
	wantWide := uint32(coll.options.MaxPageSize) +
		coll.options.primaryUnifiedOverlayParentBytes
	if got := coll.primaryUnifiedOverlay.buckets[slot].reservedBytes.Load(); got != wantWide {
		t.Fatalf("changed integer reservation = %d, want conservative %d", got, wantWide)
	}

	// A dictionary-eligible string change remains conservatively wide.
	stringChange := bytes.Replace(
		integer, []byte(`primary row 0`), []byte(`xrimary row 0`), 1,
	)
	if bytes.Equal(stringChange, integer) {
		t.Fatalf("fixture value has no mutable string: %s", integer)
	}
	if created, err := coll.Put(key, stringChange); err != nil || created {
		t.Fatalf("uncertified string Put = %v,%v", created, err)
	}
	if got := coll.primaryUnifiedOverlay.buckets[slot].reservedBytes.Load(); got != wantWide {
		t.Fatalf("uncertified bucket reservation = %d, want max leaf + parents %d",
			got, wantWide)
	}
}

func TestConcurrentPrimaryDeleteRestoreDowngradesFinalReservation(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	targets, _ := concurrentPrimaryTestTargets(t, fixture)
	keys := [2][]byte{
		[]byte(fixture.keys[targets[0]]),
		[]byte(fixture.keys[targets[1]]),
	}
	values := [2][]byte{
		fixture.values[targets[0]], fixture.values[targets[1]],
	}
	route, ok := coll.primaryRouter.Load().Route(keys[0])
	if !ok {
		t.Fatal("route first restore key")
	}
	other, ok := coll.primaryRouter.Load().Route(keys[1])
	if !ok || other.Bucket != route.Bucket || other.Ref != route.Ref {
		t.Fatalf("restore routes differ: first=%+v second=%+v ok=%v",
			route, other, ok)
	}
	if route.Ref.Length >= uint32(coll.options.MaxPageSize) {
		t.Fatalf("fixture leaf extent = %d, want compact extent below %d",
			route.Ref.Length, coll.options.MaxPageSize)
	}

	wantWide := uint32(coll.options.MaxPageSize) +
		coll.options.primaryUnifiedOverlayParentBytes
	wantFixed := route.Ref.Length +
		coll.options.primaryUnifiedOverlayParentBytes
	for i := range keys {
		deleted, err := coll.Delete(keys[i])
		if err != nil || !deleted {
			t.Fatalf("Delete %d = %v,%v", i, deleted, err)
		}
		slot, found := coll.primaryUnifiedOverlay.bucketSlot(route.Bucket)
		if !found {
			t.Fatal("delete did not publish overlay bucket")
		}
		if got := coll.primaryUnifiedOverlay.buckets[slot].wideKeys.Load(); got != int32(i+1) {
			t.Fatalf("wide keys after delete %d = %d, want %d", i, got, i+1)
		}
		if got := coll.primaryUnifiedOverlay.buckets[slot].reservedBytes.Load(); got != wantWide {
			t.Fatalf("reservation after delete %d = %d, want %d", i, got, wantWide)
		}
	}

	for i := range keys {
		created, err := coll.Put(keys[i], values[i])
		if err != nil || !created {
			t.Fatalf("certified restore %d = %v,%v", i, created, err)
		}
		slot, _ := coll.primaryUnifiedOverlay.bucketSlot(route.Bucket)
		wantWideKeys := int32(1 - i)
		if got := coll.primaryUnifiedOverlay.buckets[slot].wideKeys.Load(); got != wantWideKeys {
			t.Fatalf("wide keys after restore %d = %d, want %d",
				i, got, wantWideKeys)
		}
		wantReservation := wantWide
		if wantWideKeys == 0 {
			wantReservation = wantFixed
		}
		if got := coll.primaryUnifiedOverlay.buckets[slot].reservedBytes.Load(); got != wantReservation {
			t.Fatalf("reservation after restore %d = %d, want %d",
				i, got, wantReservation)
		}
	}
	if got := coll.primaryUnifiedOverlay.dirtyBytes.Load(); got != uint64(wantFixed) {
		t.Fatalf("final dirty bytes = %d, want %d", got, wantFixed)
	}
	for i := 0; i < 2; i++ {
		record := &coll.primaryUnifiedOverlay.records[i]
		if record.kind != primaryUnifiedOverlayDelete || record.reservationWide != 1 {
			t.Fatalf("old tombstone %d mutated: kind=%d wide=%d",
				i, record.kind, record.reservationWide)
		}
	}
}

func TestConcurrentPrimaryDeleteRestoreWithActiveSnapshot(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	want := canonicalConcurrentPrimaryValue(t, fixture.values[0])
	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	baseGeneration := coll.Generation()
	baseFolds := coll.Stats().PrimaryOverlayFolds
	var staged atomic.Int64
	previous := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		staged.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })

	deleted, err := coll.Delete(key)
	if err != nil || !deleted {
		t.Fatalf("snapshot-overlap Delete = %v,%v", deleted, err)
	}
	if got, found, readErr := coll.AppendRaw(nil, key); readErr != nil || found {
		t.Fatalf("current deleted read = %s,%v,%v", got, found, readErr)
	}
	if got, found, readErr := snapshot.AppendRaw(nil, key); readErr != nil ||
		!found || !bytes.Equal(got, want) {
		t.Fatalf("snapshot after delete = %s,%v,%v want %s",
			got, found, readErr, want)
	}
	created, err := coll.Put(key, fixture.values[0])
	if err != nil || !created {
		t.Fatalf("snapshot-overlap restore = %v,%v", created, err)
	}
	if got, found, readErr := snapshot.AppendRaw(nil, key); readErr != nil ||
		!found || !bytes.Equal(got, want) {
		t.Fatalf("snapshot after restore = %s,%v,%v want %s",
			got, found, readErr, want)
	}
	if got := staged.Load(); got != 2 {
		t.Fatalf("snapshot-overlap concurrent stages = %d, want 2", got)
	}
	if got := coll.Generation(); got != baseGeneration+2 {
		t.Fatalf("snapshot-overlap generation = %d, want %d",
			got, baseGeneration+2)
	}
	if got := coll.Stats().PrimaryOverlayFolds; got != baseFolds {
		t.Fatalf("snapshot-overlap folds = %d, baseline %d", got, baseFolds)
	}
}

func TestConcurrentPrimaryDeleteRestoreWithPinnedDirectEpoch(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	want := canonicalConcurrentPrimaryValue(t, fixture.values[0])
	view, epoch, ok := coll.enterReadEpoch()
	if !ok {
		t.Fatal("pin direct read epoch")
	}
	defer epoch.Exit()
	baseGeneration := view.generation
	baseFolds := coll.Stats().PrimaryOverlayFolds
	var staged atomic.Int64
	previous := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		staged.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })

	deleted, err := coll.Delete(key)
	if err != nil || !deleted {
		t.Fatalf("epoch-overlap Delete = %v,%v", deleted, err)
	}
	if got, found, readErr := coll.resolvePrimaryGraph(nil, view.state, key); readErr != nil ||
		!found || !bytes.Equal(got, want) {
		t.Fatalf("pinned generation after delete = %s,%v,%v want %s",
			got, found, readErr, want)
	}
	created, err := coll.Put(key, fixture.values[0])
	if err != nil || !created {
		t.Fatalf("epoch-overlap restore = %v,%v", created, err)
	}
	if got, found, readErr := coll.resolvePrimaryGraph(nil, view.state, key); readErr != nil ||
		!found || !bytes.Equal(got, want) {
		t.Fatalf("pinned generation after restore = %s,%v,%v want %s",
			got, found, readErr, want)
	}
	if got := staged.Load(); got != 2 {
		t.Fatalf("epoch-overlap concurrent stages = %d, want 2", got)
	}
	if got := coll.Generation(); got != baseGeneration+2 {
		t.Fatalf("epoch-overlap generation = %d, want %d",
			got, baseGeneration+2)
	}
	if got := coll.Stats().PrimaryOverlayFolds; got != baseFolds {
		t.Fatalf("epoch-overlap folds = %d, baseline %d", got, baseFolds)
	}
}

func TestConcurrentPrimaryActiveSnapshotPressureIsBounded(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	wantSnapshot := canonicalConcurrentPrimaryValue(t, fixture.values[0])
	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	baseGeneration := coll.Generation()
	baseStats := coll.Stats()
	values := [2][]byte{
		[]byte(`{"pressure":100000}`),
		[]byte(`{"pressure":200000}`),
	}
	for generation := range primaryUnifiedOverlayRecords {
		created, putErr := coll.Put(key, values[generation&1])
		if putErr != nil || created {
			t.Fatalf("pressure fill %d = %v,%v", generation, created, putErr)
		}
	}
	if got := coll.primaryUnifiedOverlay.count.Load(); got != primaryUnifiedOverlayRecords {
		t.Fatalf("pressure overlay records = %d, want %d",
			got, primaryUnifiedOverlayRecords)
	}
	final := values[primaryUnifiedOverlayRecords&1]
	created, err := coll.Put(key, final)
	if err != nil || created {
		t.Fatalf("bounded pressure Put = %v,%v", created, err)
	}
	if got := coll.Generation(); got != baseGeneration+primaryUnifiedOverlayRecords+1 {
		t.Fatalf("pressure generation = %d, want %d",
			got, baseGeneration+primaryUnifiedOverlayRecords+1)
	}
	after := coll.Stats()
	if got := after.PrimaryOverlayFolds - baseStats.PrimaryOverlayFolds; got != 1 {
		t.Fatalf("snapshot pressure folds = %d, want 1", got)
	}
	if got := after.ConcurrentPrimaryFallbacks - baseStats.ConcurrentPrimaryFallbacks; got != 1 {
		t.Fatalf("snapshot pressure fallbacks = %d, want 1", got)
	}
	if got, found, readErr := snapshot.AppendRaw(nil, key); readErr != nil ||
		!found || !bytes.Equal(got, wantSnapshot) {
		t.Fatalf("snapshot after pressure = %s,%v,%v want %s",
			got, found, readErr, wantSnapshot)
	}
	assertConcurrentPrimaryRaw(
		t, coll, key, canonicalConcurrentPrimaryValue(t, final),
	)
}

func TestConcurrentPrimarySameBucketCompetingInsertsCrashRecover(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	router := coll.primaryRouter.Load()
	var (
		keys  [2][]byte
		route storeio.ResidentPrimaryRoute
	)
	foundCandidates := false
	for _, bucket := range concurrentPrimaryTestBuckets(t, fixture) {
		if len(bucket.indices) < 3 {
			continue
		}
		base := fixture.keys[bucket.indices[len(bucket.indices)/2]]
		first := []byte(base + "-insert-a")
		second := []byte(base + "-insert-b")
		firstRoute, firstOK := router.Route(first)
		secondRoute, secondOK := router.Route(second)
		if firstOK && secondOK && firstRoute.Bucket == secondRoute.Bucket &&
			firstRoute.Ref == secondRoute.Ref {
			keys = [2][]byte{first, second}
			route = firstRoute
			foundCandidates = true
			break
		}
	}
	if !foundCandidates {
		t.Fatal("no same-bucket insert candidates")
	}
	values := [2][]byte{
		canonicalConcurrentPrimaryValue(t, []byte(`{"insert":"alpha","n":1}`)),
		canonicalConcurrentPrimaryValue(t, []byte(`{"insert":"bravo","n":2}`)),
	}
	for _, key := range keys {
		if _, found, err := coll.AppendRaw(nil, key); err != nil || found {
			t.Fatalf("candidate %q already present: found=%v err=%v", key, found, err)
		}
	}
	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	baseGeneration := coll.Generation()
	baseCount := coll.Len()
	baseFolds := coll.Stats().PrimaryOverlayFolds

	entered := make(chan struct{}, 1)
	contended := make(chan struct{}, 1)
	release := make(chan struct{})
	var (
		blocked atomic.Bool
		staged  atomic.Int64
	)
	previousStaged := concurrentPrimaryReplaceStagedHook
	previousPublish := concurrentPrimaryReplacePublishHook
	previousContended := concurrentPrimaryStripeContendedHook
	concurrentPrimaryReplaceStagedHook = func(bucket storeio.BucketID) {
		if bucket == route.Bucket {
			staged.Add(1)
		}
	}
	concurrentPrimaryReplacePublishHook = func(bucket storeio.BucketID, _ uint64) {
		if bucket == route.Bucket && blocked.CompareAndSwap(false, true) {
			entered <- struct{}{}
			<-release
		}
	}
	concurrentPrimaryStripeContendedHook = func(bucket storeio.BucketID) {
		if bucket == route.Bucket {
			contended <- struct{}{}
		}
	}
	t.Cleanup(func() {
		concurrentPrimaryReplaceStagedHook = previousStaged
		concurrentPrimaryReplacePublishHook = previousPublish
		concurrentPrimaryStripeContendedHook = previousContended
	})

	results := make(chan concurrentPrimaryPutResult, 2)
	go func() {
		created, putErr := coll.Put(keys[0], values[0])
		results <- concurrentPrimaryPutResult{created: created, err: putErr}
	}()
	awaitConcurrentPrimary(t, entered, "first insert publisher")
	go func() {
		created, putErr := coll.Put(keys[1], values[1])
		results <- concurrentPrimaryPutResult{created: created, err: putErr}
	}()
	awaitConcurrentPrimary(t, contended, "second same-bucket insert")
	close(release)
	for range 2 {
		result := awaitConcurrentPrimary(t, results, "same-bucket insert result")
		if result.err != nil || !result.created {
			t.Fatalf("same-bucket insert = %v,%v", result.created, result.err)
		}
	}
	if got := staged.Load(); got != 2 {
		t.Fatalf("concurrent insert stages = %d, want 2", got)
	}
	if got := coll.Generation(); got != baseGeneration+2 {
		t.Fatalf("insert generation = %d, want %d", got, baseGeneration+2)
	}
	if got := coll.Len(); got != baseCount+2 {
		t.Fatalf("insert count = %d, want %d", got, baseCount+2)
	}
	var slots [2]uint8
	for i := range keys {
		assertConcurrentPrimaryRaw(t, coll, keys[i], values[i])
		if got, present, readErr := snapshot.AppendRaw(nil, keys[i]); readErr != nil || present {
			t.Fatalf("old snapshot insert %d = %s,%v,%v", i, got, present, readErr)
		}
		insertRoute, routeOK := router.Route(keys[i])
		if !routeOK {
			t.Fatalf("insert route %d disappeared", i)
		}
		value, disposition, slot := coll.primaryUnifiedOverlay.lookup(
			insertRoute.Bucket, insertRoute.Hash, keys[i], coll.Generation(),
		)
		if disposition != primaryUnifiedOverlayValue || !bytes.Equal(value, values[i]) {
			t.Fatalf("insert overlay %d = %s,%d", i, value, disposition)
		}
		slots[i] = slot
	}
	if slots[0] == slots[1] {
		t.Fatalf("competing inserts claimed slot %d", slots[0])
	}
	if got := coll.Stats().PrimaryOverlayFolds; got != baseFolds {
		t.Fatalf("competing insert folds = %d, baseline %d", got, baseFolds)
	}
	if got := len(coll.primaryPendingParents); got != 0 {
		t.Fatalf("competing inserts created %d structural parents", got)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	image := captureJournalImage(t, fixture.path)
	recoveryPath := filepath.Join(t.TempDir(), "concurrent-insert-recovery.vibe")
	if err := os.WriteFile(recoveryPath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recoveryPath+".rjournal", image.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	recoveryFile, err := os.OpenFile(recoveryPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveryFile.Close()
	recovered, err := Open(recoveryFile, concurrentPrimaryTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if got := recovered.Generation(); got != baseGeneration+2 {
		t.Fatalf("recovered insert generation = %d, want %d", got, baseGeneration+2)
	}
	if got := recovered.Len(); got != baseCount+2 {
		t.Fatalf("recovered insert count = %d, want %d", got, baseCount+2)
	}
	for i := range keys {
		assertConcurrentPrimaryRaw(t, recovered, keys[i], values[i])
	}
}

func TestConcurrentPrimaryReplaceLateReaderDisablesArenaReuse(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	key := []byte(fixture.keys[0])
	first := []byte(`{"arena":"left","revision":1}`)
	second := []byte(`{"arena":"next","revision":2}`)
	if len(first) != len(second) {
		t.Fatal("test values are not the same size")
	}
	if created, err := coll.Put(key, first); err != nil || created {
		t.Fatalf("first Put = %v,%v", created, err)
	}
	firstGeneration := coll.Generation()
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	used := coll.primaryUnifiedOverlay.used.Load()

	staged := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	previous := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		close(staged)
		<-release
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })
	result := make(chan concurrentPrimaryPutResult, 1)
	go func() {
		created, err := coll.Put(key, second)
		result <- concurrentPrimaryPutResult{created: created, err: err}
	}()
	awaitConcurrentPrimary(t, staged, "same-size replacement staging")
	epoch, ok := coll.readEpochs.Enter(firstGeneration)
	if !ok {
		t.Fatal("could not pin late direct-read epoch")
	}
	defer epoch.Exit()
	releaseOnce.Do(func() { close(release) })
	put := awaitConcurrentPrimary(t, result, "reader-fenced replacement")
	if put.err != nil || put.created {
		t.Fatalf("second Put = %v,%v", put.created, put.err)
	}
	if got, want := coll.primaryUnifiedOverlay.used.Load(),
		used+uint32(len(key)+len(second)); got != want {
		t.Fatalf("reader-fenced arena used = %d, want fresh-copy %d", got, want)
	}
	router := coll.primaryRouter.Load()
	route, ok := router.Route(key)
	if !ok {
		t.Fatal("route disappeared")
	}
	old, disposition, _ := coll.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, firstGeneration,
	)
	if disposition != primaryUnifiedOverlayValue || !bytes.Equal(old, first) {
		t.Fatalf("old reader generation = %q,%d, want %s",
			old, disposition, first)
	}
	assertConcurrentPrimaryRaw(t, coll, key, second)
}

func TestConcurrentPrimaryReplaceDisjointStripesStageBeforePublish(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 4096, concurrentPrimaryTestOptions(),
	)
	_, targets := concurrentPrimaryTestTargets(t, fixture)
	baseGeneration := fixture.collection.Generation()
	baseStats := fixture.collection.Stats()

	staged := make(chan storeio.BucketID, 2)
	published := make(chan uint64, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	previousStaged := concurrentPrimaryReplaceStagedHook
	previousPublish := concurrentPrimaryReplacePublishHook
	concurrentPrimaryReplaceStagedHook = func(bucket storeio.BucketID) {
		staged <- bucket
		<-release
	}
	concurrentPrimaryReplacePublishHook = func(_ storeio.BucketID, generation uint64) {
		published <- generation
	}
	t.Cleanup(func() {
		concurrentPrimaryReplaceStagedHook = previousStaged
		concurrentPrimaryReplacePublishHook = previousPublish
	})

	values := [][]byte{
		canonicalConcurrentPrimaryValue(t, []byte(`{ "lane": 1, "state": "left" }`)),
		canonicalConcurrentPrimaryValue(t, []byte(`{ "lane": 2, "state": "right" }`)),
	}
	results := make(chan concurrentPrimaryPutResult, 2)
	for lane, target := range targets {
		go func() {
			created, err := fixture.collection.Put(
				[]byte(fixture.keys[target]), values[lane],
			)
			results <- concurrentPrimaryPutResult{created: created, err: err}
		}()
	}
	first := awaitConcurrentPrimary(t, staged, "first staged replacement")
	second := awaitConcurrentPrimary(t, staged, "second staged replacement")
	if first == second {
		t.Fatalf("both disjoint replacements routed to bucket %d", first)
	}
	if primaryConcurrentStripeIndex(first) == primaryConcurrentStripeIndex(second) {
		t.Fatalf(
			"buckets %d and %d unexpectedly share stripe %d",
			first, second, primaryConcurrentStripeIndex(first),
		)
	}
	// Both calls have completed canonicalization, routing, lookup, and their
	// independent bucket-stripe acquisition. Neither can reach publication
	// until this release, proving useful work overlaps before the publisher.
	releaseOnce.Do(func() { close(release) })
	for range 2 {
		result := awaitConcurrentPrimary(t, results, "disjoint replacement")
		if result.err != nil || result.created {
			t.Fatalf("disjoint replacement: created=%v err=%v", result.created, result.err)
		}
	}
	for offset := range 2 {
		if got := awaitConcurrentPrimary(t, published, "published replacement generation"); got != baseGeneration+uint64(offset)+1 {
			t.Fatalf(
				"publish hook generation %d = %d, want %d",
				offset, got, baseGeneration+uint64(offset)+1,
			)
		}
	}
	for lane, target := range targets {
		assertConcurrentPrimaryRaw(
			t, fixture.collection, []byte(fixture.keys[target]), values[lane],
		)
	}
	if got := fixture.collection.Generation(); got != baseGeneration+2 {
		t.Fatalf("generation = %d, want %d", got, baseGeneration+2)
	}
	if got := fixture.collection.Stats().ConcurrentPrimaryReplaces -
		baseStats.ConcurrentPrimaryReplaces; got != 2 {
		t.Fatalf("concurrent replacements = %d, want 2", got)
	}
}

func TestConcurrentPrimaryDeleteRestoreKeepsStableSlotAndExactCuts(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	coll := fixture.collection
	at := len(fixture.keys) / 2
	key := []byte(fixture.keys[at])
	want := canonicalConcurrentPrimaryValue(t, fixture.values[at])
	route, ok := coll.primaryRouter.Load().Route(key)
	if !ok {
		t.Fatalf("route existing key %q", key)
	}
	lease, err := coll.primaryRouter.Load().AcquireLeaf(coll.cache, route)
	if err != nil {
		t.Fatal(err)
	}
	leaf, admitted := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), coll.storeID, route.Bucket,
	)
	if !admitted {
		lease.Release()
		t.Fatal("target is not an admitted unified leaf")
	}
	rank, found := leaf.FindKey(key)
	baseSlot, slotOK := leaf.PostingSlot(rank)
	_, overflow := leaf.OverflowRef(rank)
	if !found || overflow {
		lease.Release()
		t.Fatalf("base lookup = found %v overflow %v", found, overflow)
	}
	if !slotOK {
		lease.Release()
		t.Fatal("base lookup has no posting slot")
	}
	baseValue, decoded := leaf.AppendValue(nil, rank)
	if !decoded {
		lease.Release()
		t.Fatal("base lookup value did not decode")
	}
	baseLen := len(baseValue)
	lease.Release()

	baseGeneration := coll.Generation()
	baseCount := coll.Len()
	baseOverlayCount := coll.primaryUnifiedOverlay.count.Load()
	baseFolds := coll.Stats().PrimaryOverlayFolds
	var staged atomic.Int64
	previousStaged := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		staged.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previousStaged })

	deleted, err := coll.Delete(key)
	if err != nil || !deleted {
		t.Fatalf("Delete = %v,%v", deleted, err)
	}
	if got := coll.Generation(); got != baseGeneration+1 {
		t.Fatalf("delete generation = %d, want %d", got, baseGeneration+1)
	}
	if got := coll.Len(); got != baseCount-1 {
		t.Fatalf("delete count = %d, want %d", got, baseCount-1)
	}
	if got, present, readErr := coll.AppendRaw(nil, key); readErr != nil || present {
		t.Fatalf("deleted read = %s,%v,%v", got, present, readErr)
	}
	deleteRecord := coll.primaryUnifiedOverlay.records[baseOverlayCount]
	if deleteRecord.kind != primaryUnifiedOverlayDelete ||
		deleteRecord.countDelta != -1 || deleteRecord.slot != baseSlot ||
		int(deleteRecord.rawDelta) !=
			-storeio.CommonPrimaryUnifiedInsertedTrivialBytes(key, baseLen) {
		t.Fatalf("delete record = %#v", deleteRecord)
	}
	if _, disposition, slot := coll.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, baseGeneration+1,
	); disposition != primaryUnifiedOverlayDeleted || slot != baseSlot {
		t.Fatalf("delete cut = disposition %d slot %d, want deleted/%d",
			disposition, slot, baseSlot)
	}

	// A repeated Delete is a leaf-local no-op: no record or generation appears.
	deleted, err = coll.Delete(key)
	if err != nil || deleted {
		t.Fatalf("second Delete = %v,%v", deleted, err)
	}
	if got := coll.Generation(); got != baseGeneration+1 {
		t.Fatalf("no-op delete generation = %d, want %d", got, baseGeneration+1)
	}
	if got := coll.primaryUnifiedOverlay.count.Load(); got != baseOverlayCount+1 {
		t.Fatalf("no-op delete overlay count = %d, want %d", got, baseOverlayCount+1)
	}

	created, err := coll.Put(key, fixture.values[at])
	if err != nil || !created {
		t.Fatalf("restore Put = %v,%v", created, err)
	}
	if got := coll.Generation(); got != baseGeneration+2 {
		t.Fatalf("restore generation = %d, want %d", got, baseGeneration+2)
	}
	if got := coll.Len(); got != baseCount {
		t.Fatalf("restore count = %d, want %d", got, baseCount)
	}
	assertConcurrentPrimaryRaw(t, coll, key, want)
	restoreRecord := coll.primaryUnifiedOverlay.records[baseOverlayCount+1]
	if restoreRecord.kind != primaryUnifiedOverlayPut ||
		restoreRecord.countDelta != 1 || restoreRecord.slot != baseSlot ||
		int(restoreRecord.rawDelta) !=
			storeio.CommonPrimaryUnifiedInsertedTrivialBytes(key, len(want)) {
		t.Fatalf("restore record = %#v", restoreRecord)
	}
	if int(deleteRecord.rawDelta)+int(restoreRecord.rawDelta) != 0 {
		t.Fatalf("round-trip raw delta = %d + %d, want zero",
			deleteRecord.rawDelta, restoreRecord.rawDelta)
	}
	if value, disposition, slot := coll.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, baseGeneration+2,
	); disposition != primaryUnifiedOverlayValue || slot != baseSlot ||
		!bytes.Equal(value, want) {
		t.Fatalf("restore cut = %s,%d,%d, want %s,value,%d",
			value, disposition, slot, want, baseSlot)
	}
	if _, disposition, _ := coll.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, baseGeneration+1,
	); disposition != primaryUnifiedOverlayDeleted {
		t.Fatalf("historical delete cut disposition = %d", disposition)
	}
	if raw, rows := coll.primaryUnifiedOverlay.pendingBucketDeltas(
		route.Bucket,
	); raw != 0 || rows != 0 {
		t.Fatalf("round-trip bucket deltas = %d,%d, want 0,0", raw, rows)
	}
	if got := staged.Load(); got != 2 {
		t.Fatalf("concurrent stages = %d, want delete+restore", got)
	}
	if got := len(coll.primaryPendingParents); got != 0 {
		t.Fatalf("delete+restore created %d structural parents", got)
	}
	if got := coll.Stats().PrimaryOverlayFolds; got != baseFolds {
		t.Fatalf("delete+restore folds = %d, baseline %d", got, baseFolds)
	}
	if current, routeOK := coll.primaryRouter.Load().Route(key); !routeOK ||
		current.Ref != route.Ref {
		t.Fatalf("resident route changed from %v to %v,%v", route.Ref, current.Ref, routeOK)
	}

	entries, complete, err := coll.primaryUnifiedOverlay.checkpointEntries(
		make([]storeio.RecoveryBatchEntry, 0, 2),
		baseGeneration, baseGeneration+2,
	)
	if err != nil || !complete || len(entries) != 2 ||
		entries[0].Kind != storeio.RecoveryRecordKindDelete ||
		entries[1].Kind != storeio.RecoveryRecordKindPut ||
		!bytes.Equal(entries[1].Value, want) {
		t.Fatalf("checkpoint entries = %#v complete=%v err=%v",
			entries, complete, err)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := coll.DurableGeneration(); got != baseGeneration+2 {
		t.Fatalf("durable generation = %d, want %d", got, baseGeneration+2)
	}
	image := captureJournalImage(t, fixture.path)
	recoveryPath := filepath.Join(t.TempDir(), "delete-restore-recovery.vibe")
	if err := os.WriteFile(recoveryPath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		recoveryPath+".rjournal", image.journal, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	recoveryFile, err := os.OpenFile(recoveryPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveryFile.Close()
	recovered, err := Open(recoveryFile, concurrentPrimaryTestOptions())
	if err != nil {
		_ = recoveryFile.Close()
		t.Fatal(err)
	}
	defer recovered.Close()
	if got := recovered.Generation(); got != baseGeneration+2 {
		t.Fatalf("recovered generation = %d, want %d", got, baseGeneration+2)
	}
	if got := recovered.Len(); got != baseCount {
		t.Fatalf("recovered count = %d, want %d", got, baseCount)
	}
	assertConcurrentPrimaryRaw(t, recovered, key, want)
}

func TestConcurrentPrimaryDeleteRestoreDisjointStripesStageInParallel(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 4096, concurrentPrimaryTestOptions(),
	)
	_, targets := concurrentPrimaryTestTargets(t, fixture)
	baseGeneration := fixture.collection.Generation()
	baseCount := fixture.collection.Len()
	baseFolds := fixture.collection.Stats().PrimaryOverlayFolds

	staged := make(chan storeio.BucketID, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	previousStaged := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(bucket storeio.BucketID) {
		staged <- bucket
		<-release
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previousStaged })

	deleteResults := make(chan concurrentPrimaryDeleteResult, 2)
	for _, target := range targets {
		go func() {
			deleted, err := fixture.collection.Delete(
				[]byte(fixture.keys[target]),
			)
			deleteResults <- concurrentPrimaryDeleteResult{
				deleted: deleted, err: err,
			}
		}()
	}
	first := awaitConcurrentPrimary(t, staged, "first staged delete")
	second := awaitConcurrentPrimary(t, staged, "second staged delete")
	if first == second ||
		primaryConcurrentStripeIndex(first) == primaryConcurrentStripeIndex(second) {
		t.Fatalf("delete buckets are not disjoint stripes: %d/%d", first, second)
	}
	releaseOnce.Do(func() { close(release) })
	for range 2 {
		result := awaitConcurrentPrimary(t, deleteResults, "disjoint delete")
		if result.err != nil || !result.deleted {
			t.Fatalf("disjoint Delete = %v,%v", result.deleted, result.err)
		}
	}
	if got := fixture.collection.Generation(); got != baseGeneration+2 {
		t.Fatalf("delete generation = %d, want %d", got, baseGeneration+2)
	}
	if got := fixture.collection.Len(); got != baseCount-2 {
		t.Fatalf("delete count = %d, want %d", got, baseCount-2)
	}

	putResults := make(chan concurrentPrimaryPutResult, 2)
	for _, target := range targets {
		go func() {
			created, err := fixture.collection.Put(
				[]byte(fixture.keys[target]), fixture.values[target],
			)
			putResults <- concurrentPrimaryPutResult{
				created: created, err: err,
			}
		}()
	}
	// release is already closed; both resurrection calls still report their
	// completed leaf-local inspection through the same deterministic seam.
	first = awaitConcurrentPrimary(t, staged, "first staged restore")
	second = awaitConcurrentPrimary(t, staged, "second staged restore")
	if first == second ||
		primaryConcurrentStripeIndex(first) == primaryConcurrentStripeIndex(second) {
		t.Fatalf("restore buckets are not disjoint stripes: %d/%d", first, second)
	}
	for range 2 {
		result := awaitConcurrentPrimary(t, putResults, "disjoint restore")
		if result.err != nil || !result.created {
			t.Fatalf("disjoint restore = %v,%v", result.created, result.err)
		}
	}
	if got := fixture.collection.Generation(); got != baseGeneration+4 {
		t.Fatalf("round-trip generation = %d, want %d", got, baseGeneration+4)
	}
	if got := fixture.collection.Len(); got != baseCount {
		t.Fatalf("round-trip count = %d, want %d", got, baseCount)
	}
	for _, target := range targets {
		assertConcurrentPrimaryRaw(
			t, fixture.collection, []byte(fixture.keys[target]),
			canonicalConcurrentPrimaryValue(t, fixture.values[target]),
		)
	}
	if got := len(fixture.collection.primaryPendingParents); got != 0 {
		t.Fatalf("parallel churn created %d structural parents", got)
	}
	if got := fixture.collection.Stats().PrimaryOverlayFolds; got != baseFolds {
		t.Fatalf("parallel churn folds = %d, baseline %d", got, baseFolds)
	}
	entries, complete, err :=
		fixture.collection.primaryUnifiedOverlay.checkpointEntries(
			make([]storeio.RecoveryBatchEntry, 0, 4),
			baseGeneration, baseGeneration+4,
		)
	if err != nil || !complete || len(entries) != 4 ||
		entries[0].Kind != storeio.RecoveryRecordKindDelete ||
		entries[1].Kind != storeio.RecoveryRecordKindDelete ||
		entries[2].Kind != storeio.RecoveryRecordKindPut ||
		entries[3].Kind != storeio.RecoveryRecordKindPut {
		t.Fatalf("parallel churn journal = %#v complete=%v err=%v",
			entries, complete, err)
	}
}

func TestConcurrentPrimaryDeleteRestoreSameBucketNoLostCount(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 4096, concurrentPrimaryTestOptions(),
	)
	targets, _ := concurrentPrimaryTestTargets(t, fixture)
	keys := [2][]byte{
		[]byte(fixture.keys[targets[0]]),
		[]byte(fixture.keys[targets[1]]),
	}
	values := [2][]byte{
		fixture.values[targets[0]], fixture.values[targets[1]],
	}
	route, ok := fixture.collection.primaryRouter.Load().Route(keys[0])
	if !ok {
		t.Fatal("route first same-bucket key")
	}
	other, ok := fixture.collection.primaryRouter.Load().Route(keys[1])
	if !ok || other.Bucket != route.Bucket {
		t.Fatalf("same-bucket routes = %d/%d, ok=%v", route.Bucket, other.Bucket, ok)
	}
	baseGeneration := fixture.collection.Generation()
	baseCount := fixture.collection.Len()

	var phase atomic.Int32
	var blocked [2]atomic.Bool
	var stages [2]atomic.Int64
	entered := [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}
	contended := [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	previousStaged := concurrentPrimaryReplaceStagedHook
	previousPublish := concurrentPrimaryReplacePublishHook
	previousContended := concurrentPrimaryStripeContendedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		stages[phase.Load()].Add(1)
	}
	concurrentPrimaryReplacePublishHook = func(
		bucket storeio.BucketID, _ uint64,
	) {
		at := phase.Load()
		if bucket == route.Bucket && blocked[at].CompareAndSwap(false, true) {
			entered[at] <- struct{}{}
			<-release[at]
		}
	}
	concurrentPrimaryStripeContendedHook = func(bucket storeio.BucketID) {
		if bucket == route.Bucket {
			contended[phase.Load()] <- struct{}{}
		}
	}
	t.Cleanup(func() {
		concurrentPrimaryReplaceStagedHook = previousStaged
		concurrentPrimaryReplacePublishHook = previousPublish
		concurrentPrimaryStripeContendedHook = previousContended
	})

	deleteResults := make(chan concurrentPrimaryDeleteResult, 2)
	go func() {
		deleted, err := fixture.collection.Delete(keys[0])
		deleteResults <- concurrentPrimaryDeleteResult{deleted: deleted, err: err}
	}()
	awaitConcurrentPrimary(t, entered[0], "blocked same-bucket delete publisher")
	go func() {
		deleted, err := fixture.collection.Delete(keys[1])
		deleteResults <- concurrentPrimaryDeleteResult{deleted: deleted, err: err}
	}()
	awaitConcurrentPrimary(t, contended[0], "same-bucket delete contention")
	close(release[0])
	for range 2 {
		result := awaitConcurrentPrimary(t, deleteResults, "same-bucket delete")
		if result.err != nil || !result.deleted {
			t.Fatalf("same-bucket Delete = %v,%v", result.deleted, result.err)
		}
	}
	if got := fixture.collection.Generation(); got != baseGeneration+2 {
		t.Fatalf("same-bucket delete generation = %d, want %d", got, baseGeneration+2)
	}
	if got := fixture.collection.Len(); got != baseCount-2 {
		t.Fatalf("same-bucket delete count = %d, want %d", got, baseCount-2)
	}
	if got := stages[0].Load(); got != 2 {
		t.Fatalf("same-bucket fast deletes = %d, want both serialized", got)
	}

	phase.Store(1)
	putResults := make(chan concurrentPrimaryPutResult, 2)
	go func() {
		created, err := fixture.collection.Put(keys[0], values[0])
		putResults <- concurrentPrimaryPutResult{created: created, err: err}
	}()
	awaitConcurrentPrimary(t, entered[1], "blocked same-bucket restore publisher")
	go func() {
		created, err := fixture.collection.Put(keys[1], values[1])
		putResults <- concurrentPrimaryPutResult{created: created, err: err}
	}()
	awaitConcurrentPrimary(t, contended[1], "same-bucket restore contention")
	close(release[1])
	for range 2 {
		result := awaitConcurrentPrimary(t, putResults, "same-bucket restore")
		if result.err != nil || !result.created {
			t.Fatalf("same-bucket restore = %v,%v", result.created, result.err)
		}
	}
	if got := fixture.collection.Generation(); got != baseGeneration+4 {
		t.Fatalf("same-bucket round-trip generation = %d, want %d", got, baseGeneration+4)
	}
	if got := fixture.collection.Len(); got != baseCount {
		t.Fatalf("same-bucket round-trip count = %d, want %d", got, baseCount)
	}
	if got := stages[1].Load(); got != 2 {
		t.Fatalf("same-bucket fast restores = %d, want both serialized", got)
	}
	for i := range keys {
		assertConcurrentPrimaryRaw(
			t, fixture.collection, keys[i],
			canonicalConcurrentPrimaryValue(t, values[i]),
		)
	}
}

func TestConcurrentPrimaryDeleteFallbackShapes(t *testing.T) {
	type fallbackCase struct {
		name         string
		count        int
		options      func(*testing.T) Options
		open         func(*testing.T, Options) concurrentPrimaryTestFixture
		seedOverflow bool
	}
	baseOptions := func(*testing.T) Options { return concurrentPrimaryTestOptions() }
	cases := []fallbackCase{
		{
			name:    "last-row",
			count:   1,
			options: baseOptions,
		},
		{
			name:  "recovery-journal",
			count: 256,
			options: func(*testing.T) Options {
				options := concurrentPrimaryTestOptions()
				options.RecoveryJournal = true
				return options
			},
		},
		{
			name:  "exact-index",
			count: 256,
			options: func(*testing.T) Options {
				return primaryExactOverlayTestOptions("/group")
			},
		},
		{
			name:  "schema",
			count: 1,
			options: func(t *testing.T) Options {
				schema, err := store.CompileSchema(store.SchemaDefinition{
					Root: store.SchemaObject,
					Fields: []store.SchemaField{{
						Path: "/id", Types: store.SchemaInteger, Required: true,
					}},
				})
				if err != nil {
					t.Fatal(err)
				}
				options := concurrentPrimaryTestOptions()
				options.Collection.Schema = schema
				return options
			},
			open: openConcurrentPrimarySeededTestFixture,
		},
		{
			name:         "overflow",
			count:        256,
			seedOverflow: true,
			options: func(*testing.T) Options {
				options := concurrentPrimaryTestOptions()
				options.InlineValueBytes = 64
				options.MaxDocumentBytes = 2048
				return options
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options := test.options(t)
			var fixture concurrentPrimaryTestFixture
			if test.open != nil {
				fixture = test.open(t, options)
			} else {
				fixture = openConcurrentPrimaryTestFixture(t, test.count, options)
			}
			key := []byte(fixture.keys[0])
			if test.seedOverflow {
				overflowValue := fmt.Appendf(
					nil, `{"overflow":%q}`, bytes.Repeat([]byte("x"), 512),
				)
				created, err := fixture.collection.Put(key, overflowValue)
				if err != nil || created {
					t.Fatalf("seed overflow = %v,%v", created, err)
				}
			}
			baseGeneration := fixture.collection.Generation()
			baseCount := fixture.collection.Len()
			var staged atomic.Int64
			previousStaged := concurrentPrimaryReplaceStagedHook
			concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
				staged.Add(1)
			}
			t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previousStaged })
			deleted, err := fixture.collection.Delete(key)
			if err != nil || !deleted {
				t.Fatalf("fallback Delete = %v,%v", deleted, err)
			}
			if got := staged.Load(); got != 0 {
				t.Fatalf("fallback shape staged %d concurrent deletes", got)
			}
			if got := fixture.collection.Generation(); got <= baseGeneration {
				t.Fatalf("fallback generation = %d, want > %d", got, baseGeneration)
			}
			if got := fixture.collection.Len(); got != baseCount-1 {
				t.Fatalf("fallback count = %d, want %d", got, baseCount-1)
			}
			if got, present, readErr := fixture.collection.AppendRaw(nil, key); readErr != nil || present {
				t.Fatalf("fallback deleted read = %s,%v,%v", got, present, readErr)
			}
		})
	}
}

func TestConcurrentPrimaryDeleteRestoreMultiOwnerStress(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 4096, concurrentPrimaryTestOptions(),
	)
	const (
		workers = 8
		loops   = 16
	)
	targets := make([]int, 0, workers)
	seenStripes := make(map[uint32]struct{}, workers)
	for _, bucket := range concurrentPrimaryTestBuckets(t, fixture) {
		if len(bucket.indices) < 2 {
			continue
		}
		stripe := primaryConcurrentStripeIndex(bucket.id)
		if _, exists := seenStripes[stripe]; exists {
			continue
		}
		seenStripes[stripe] = struct{}{}
		targets = append(targets, bucket.indices[0])
		if len(targets) == workers {
			break
		}
	}
	if len(targets) != workers {
		t.Fatalf("found %d distinct dense stripes, want %d", len(targets), workers)
	}
	baseGeneration := fixture.collection.Generation()
	baseCount := fixture.collection.Len()
	baseFolds := fixture.collection.Stats().PrimaryOverlayFolds
	var staged atomic.Int64
	previousStaged := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		staged.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previousStaged })

	start := make(chan struct{})
	errorsOut := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for _, target := range targets {
		go func() {
			defer wait.Done()
			<-start
			key := []byte(fixture.keys[target])
			value := fixture.values[target]
			for iteration := range loops {
				deleted, err := fixture.collection.Delete(key)
				if err != nil || !deleted {
					errorsOut <- fmt.Errorf(
						"worker %d delete %d = %v,%v",
						target, iteration, deleted, err,
					)
					return
				}
				created, err := fixture.collection.Put(key, value)
				if err != nil || !created {
					errorsOut <- fmt.Errorf(
						"worker %d restore %d = %v,%v",
						target, iteration, created, err,
					)
					return
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
	wantMutations := uint64(2 * workers * loops)
	if got := fixture.collection.Generation(); got != baseGeneration+wantMutations {
		t.Fatalf("stress generation = %d, want %d",
			got, baseGeneration+wantMutations)
	}
	if got := fixture.collection.Len(); got != baseCount {
		t.Fatalf("stress count = %d, want %d", got, baseCount)
	}
	if got := staged.Load(); got != int64(wantMutations) {
		t.Fatalf("stress concurrent stages = %d, want %d", got, wantMutations)
	}
	if got := fixture.collection.Stats().PrimaryOverlayFolds; got != baseFolds {
		t.Fatalf("stress folds = %d, baseline %d", got, baseFolds)
	}
	for _, target := range targets {
		assertConcurrentPrimaryRaw(
			t, fixture.collection, []byte(fixture.keys[target]),
			canonicalConcurrentPrimaryValue(t, fixture.values[target]),
		)
	}
}

func TestConcurrentPrimaryDeletePressurePublishesPrefixAndRetries(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 4096, concurrentPrimaryTestOptions(),
	)
	_, targets := concurrentPrimaryTestTargets(t, fixture)
	fillKey := []byte(fixture.keys[targets[0]])
	for generation := range primaryUnifiedOverlayRecords - 1 {
		value := fmt.Appendf(nil, `{"pressure":%d}`, generation)
		created, err := fixture.collection.Put(fillKey, value)
		if err != nil || created {
			t.Fatalf("fill replacement %d = %v,%v", generation, created, err)
		}
	}
	if got := fixture.collection.primaryUnifiedOverlay.count.Load(); got != primaryUnifiedOverlayRecords-1 {
		t.Fatalf("pre-pressure overlay count = %d, want %d",
			got, primaryUnifiedOverlayRecords-1)
	}
	baseGeneration := fixture.collection.Generation()
	baseCount := fixture.collection.Len()
	baseStats := fixture.collection.Stats()
	var staged atomic.Int64
	previousStaged := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		staged.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previousStaged })

	results := make(chan concurrentPrimaryDeleteResult, 2)
	for _, target := range targets {
		go func() {
			deleted, err := fixture.collection.Delete(
				[]byte(fixture.keys[target]),
			)
			results <- concurrentPrimaryDeleteResult{deleted: deleted, err: err}
		}()
	}
	for range 2 {
		result := awaitConcurrentPrimary(t, results, "pressure delete")
		if result.err != nil || !result.deleted {
			t.Fatalf("pressure Delete = %v,%v", result.deleted, result.err)
		}
	}
	if got := fixture.collection.Generation(); got != baseGeneration+2 {
		t.Fatalf("pressure delete generation = %d, want %d", got, baseGeneration+2)
	}
	if got := fixture.collection.Len(); got != baseCount-2 {
		t.Fatalf("pressure delete count = %d, want %d", got, baseCount-2)
	}
	afterDelete := fixture.collection.Stats()
	if got := afterDelete.PrimaryOverlayFolds - baseStats.PrimaryOverlayFolds; got != 1 {
		t.Fatalf("pressure delete folds = %d, want one foreground fold", got)
	}
	if got := afterDelete.AutomaticCheckpoints - baseStats.AutomaticCheckpoints; got != 0 {
		t.Fatalf("pressure delete automatic checkpoints = %d, want zero", got)
	}
	if got := staged.Load(); got < 3 {
		t.Fatalf("pressure delete stages = %d, want prefix plus pressure retry", got)
	}

	for _, target := range targets {
		created, err := fixture.collection.Put(
			[]byte(fixture.keys[target]), fixture.values[target],
		)
		if err != nil || !created {
			t.Fatalf("post-pressure restore %d = %v,%v", target, created, err)
		}
	}
	if got := fixture.collection.Generation(); got != baseGeneration+4 {
		t.Fatalf("pressure round-trip generation = %d, want %d", got, baseGeneration+4)
	}
	if got := fixture.collection.Len(); got != baseCount {
		t.Fatalf("pressure round-trip count = %d, want %d", got, baseCount)
	}
	for _, target := range targets {
		assertConcurrentPrimaryRaw(
			t, fixture.collection, []byte(fixture.keys[target]),
			canonicalConcurrentPrimaryValue(t, fixture.values[target]),
		)
	}
}

func TestConcurrentPrimaryReplaceSameBucketNoLostUpdates(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 4096, concurrentPrimaryTestOptions(),
	)
	targets, _ := concurrentPrimaryTestTargets(t, fixture)
	router := fixture.collection.primaryRouter.Load()
	firstRoute, _ := router.Route([]byte(fixture.keys[targets[0]]))
	secondRoute, _ := router.Route([]byte(fixture.keys[targets[1]]))
	if firstRoute.Bucket != secondRoute.Bucket {
		t.Fatalf("selected keys do not share a bucket: %d != %d", firstRoute.Bucket, secondRoute.Bucket)
	}
	baseGeneration := fixture.collection.Generation()
	baseStats := fixture.collection.Stats()

	publisherEntered := make(chan struct{})
	releasePublisher := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releasePublisher) })
	var publishCalls atomic.Int64
	previous := concurrentPrimaryReplacePublishHook
	concurrentPrimaryReplacePublishHook = func(bucket storeio.BucketID, _ uint64) {
		if bucket == firstRoute.Bucket && publishCalls.Add(1) == 1 {
			close(publisherEntered)
			<-releasePublisher
		}
	}
	t.Cleanup(func() { concurrentPrimaryReplacePublishHook = previous })

	values := [][]byte{
		canonicalConcurrentPrimaryValue(t, []byte(`{"same_bucket":1,"value":"alpha"}`)),
		canonicalConcurrentPrimaryValue(t, []byte(`{"same_bucket":2,"value":"bravo-bravo"}`)),
	}
	results := make(chan concurrentPrimaryPutResult, 2)
	go func() {
		created, err := fixture.collection.Put(
			[]byte(fixture.keys[targets[0]]), values[0],
		)
		results <- concurrentPrimaryPutResult{created: created, err: err}
	}()
	awaitConcurrentPrimary(t, publisherEntered, "first same-bucket publisher")
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		created, err := fixture.collection.Put(
			[]byte(fixture.keys[targets[1]]), values[1],
		)
		results <- concurrentPrimaryPutResult{created: created, err: err}
	}()
	awaitConcurrentPrimary(t, secondStarted, "second same-bucket caller")
	releaseOnce.Do(func() { close(releasePublisher) })
	for range 2 {
		result := awaitConcurrentPrimary(t, results, "same-bucket replacement")
		if result.err != nil || result.created {
			t.Fatalf("same-bucket replacement: created=%v err=%v", result.created, result.err)
		}
	}
	for lane, target := range targets {
		assertConcurrentPrimaryRaw(
			t, fixture.collection, []byte(fixture.keys[target]), values[lane],
		)
	}
	if got := fixture.collection.Generation(); got != baseGeneration+2 {
		t.Fatalf("generation = %d, want %d", got, baseGeneration+2)
	}
	if got := fixture.collection.Stats().ConcurrentPrimaryReplaces -
		baseStats.ConcurrentPrimaryReplaces; got < 1 || got > 2 {
		t.Fatalf("concurrent replacements = %d, want one or two", got)
	}
}

func TestConcurrentPrimaryReplaceReadersSeeCompleteCanonicalValues(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	key := []byte(fixture.keys[0])
	first := canonicalConcurrentPrimaryValue(
		t, []byte(`{ "reader" : "first", "sequence" : 1 }`),
	)
	second := canonicalConcurrentPrimaryValue(
		t, []byte("{\n\t\"sequence\": 2, \"reader\": \"second\"\n}"),
	)
	allowed := [][]byte{fixture.values[0], first, second}
	baseGeneration := fixture.collection.Generation()
	baseStats := fixture.collection.Stats()

	firstStaged := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFirst) })
	var stagedCalls atomic.Int64
	previousStaged := concurrentPrimaryReplaceStagedHook
	previousPublish := concurrentPrimaryReplacePublishHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		if stagedCalls.Add(1) == 1 {
			close(firstStaged)
			<-releaseFirst
		}
	}
	concurrentPrimaryReplacePublishHook = func(storeio.BucketID, uint64) {
		runtime.Gosched()
	}
	t.Cleanup(func() {
		concurrentPrimaryReplaceStagedHook = previousStaged
		concurrentPrimaryReplacePublishHook = previousPublish
	})

	const writes = 64
	writerDone := make(chan error, 1)
	go func() {
		for i := range writes {
			value := first
			if i&1 != 0 {
				value = second
			}
			created, err := fixture.collection.Put(key, value)
			if err != nil {
				writerDone <- err
				return
			}
			if created {
				writerDone <- fmt.Errorf("write %d unexpectedly inserted the key", i)
				return
			}
		}
		writerDone <- nil
	}()
	awaitConcurrentPrimary(t, firstStaged, "first staged reader-test write")

	stop := make(chan struct{})
	readerFailure := make(chan error, 1)
	changed := make(chan struct{})
	var changedOnce sync.Once
	var readers sync.WaitGroup
	const readerCount = 4
	readers.Add(readerCount)
	ready := make(chan struct{}, readerCount)
	for reader := range readerCount {
		go func() {
			defer readers.Done()
			announced := false
			for {
				got, found, err := fixture.collection.AppendRaw(nil, key)
				if !announced {
					announced = true
					ready <- struct{}{}
				}
				if err != nil || !found {
					select {
					case readerFailure <- fmt.Errorf(
						"reader %d: found=%v err=%v", reader, found, err,
					):
					default:
					}
					return
				}
				valid := false
				for _, candidate := range allowed {
					if bytes.Equal(got, candidate) {
						valid = true
						if !bytes.Equal(got, fixture.values[0]) {
							changedOnce.Do(func() { close(changed) })
						}
						break
					}
				}
				if !valid {
					select {
					case readerFailure <- fmt.Errorf(
						"reader %d observed partial/noncanonical value %q", reader, got,
					):
					default:
					}
					return
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	for range readerCount {
		awaitConcurrentPrimary(t, ready, "reader startup")
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	if err := awaitConcurrentPrimary(t, writerDone, "reader-test writer"); err != nil {
		close(stop)
		readers.Wait()
		t.Fatal(err)
	}
	select {
	case <-changed:
	case err := <-readerFailure:
		close(stop)
		readers.Wait()
		t.Fatal(err)
	case <-time.After(concurrentPrimaryTestTimeout):
		close(stop)
		readers.Wait()
		t.Fatal("readers never observed a published replacement")
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-readerFailure:
		t.Fatal(err)
	default:
	}
	assertConcurrentPrimaryRaw(t, fixture.collection, key, second)
	if got := fixture.collection.Generation(); got != baseGeneration+writes {
		t.Fatalf("generation = %d, want %d", got, baseGeneration+writes)
	}
	// The first replacement is deliberately staged before the readers start.
	// Direct-read epochs do not veto append-only immutable overlay publication,
	// so every later replacement must retain the concurrent lane while readers
	// verify complete canonical values.
	staged := stagedCalls.Load()
	if staged != writes {
		t.Fatalf("staged calls = %d, want %d", staged, writes)
	}
	if got := fixture.collection.Stats().ConcurrentPrimaryReplaces -
		baseStats.ConcurrentPrimaryReplaces; got != uint64(staged) {
		t.Fatalf("concurrent replacements = %d, want staged count %d", got, staged)
	}
}

func TestConcurrentPrimaryExclusiveOperationsWaitAndIncludeCut(t *testing.T) {
	for _, operation := range []string{"snapshot", "flush"} {
		t.Run(operation, func(t *testing.T) {
			fixture := openConcurrentPrimaryTestFixture(
				t, 512, concurrentPrimaryTestOptions(),
			)
			key := []byte(fixture.keys[0])
			value := canonicalConcurrentPrimaryValue(
				t, fmt.Appendf(nil, `{ "exclusive": %q, "cut": 1 }`, operation),
			)
			baseGeneration := fixture.collection.Generation()

			staged := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			exclusiveAttempt := make(chan struct{}, 1)
			var stagedOnce sync.Once
			previousStaged := concurrentPrimaryReplaceStagedHook
			previousExclusive := concurrentPrimaryExclusiveWaitHook
			concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
				stagedOnce.Do(func() { close(staged) })
				<-release
			}
			concurrentPrimaryExclusiveWaitHook = func(name string) {
				if name == operation {
					exclusiveAttempt <- struct{}{}
				}
			}
			t.Cleanup(func() {
				concurrentPrimaryReplaceStagedHook = previousStaged
				concurrentPrimaryExclusiveWaitHook = previousExclusive
			})

			putDone := make(chan concurrentPrimaryPutResult, 1)
			go func() {
				created, err := fixture.collection.Put(key, value)
				putDone <- concurrentPrimaryPutResult{created: created, err: err}
			}()
			awaitConcurrentPrimary(t, staged, "staged writer before exclusive operation")

			type exclusiveResult struct {
				snapshot *Snapshot
				err      error
			}
			exclusiveDone := make(chan exclusiveResult, 1)
			go func() {
				if operation == "snapshot" {
					snapshot, err := fixture.collection.Snapshot()
					exclusiveDone <- exclusiveResult{snapshot: snapshot, err: err}
					return
				}
				exclusiveDone <- exclusiveResult{err: fixture.collection.Flush()}
			}()
			awaitConcurrentPrimary(t, exclusiveAttempt, operation+" exclusive attempt")
			if fixture.collection.writer.TryLock() {
				fixture.collection.writer.Unlock()
				t.Fatal("exclusive writer gate acquired while staged fast writer held its read side")
			}
			releaseOnce.Do(func() { close(release) })
			putResult := awaitConcurrentPrimary(t, putDone, "writer before "+operation)
			if putResult.err != nil || putResult.created {
				t.Fatalf("replacement: created=%v err=%v", putResult.created, putResult.err)
			}
			result := awaitConcurrentPrimary(t, exclusiveDone, operation)
			if result.err != nil {
				t.Fatal(result.err)
			}
			wantGeneration := baseGeneration + 1
			if operation == "snapshot" {
				if result.snapshot == nil {
					t.Fatal("Snapshot returned nil")
				}
				if result.snapshot.Generation() != wantGeneration {
					t.Fatalf(
						"snapshot generation = %d, want %d",
						result.snapshot.Generation(), wantGeneration,
					)
				}
				got, found, err := result.snapshot.AppendRaw(nil, key)
				if err != nil || !found || !bytes.Equal(got, value) {
					t.Fatalf("snapshot cut: found=%v value=%s err=%v", found, got, err)
				}
				if err := result.snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}

			if got := fixture.collection.DurableGeneration(); got != wantGeneration {
				t.Fatalf("durable generation = %d, want %d", got, wantGeneration)
			}
			image := captureJournalImage(t, fixture.path)
			recoveryPath := filepath.Join(t.TempDir(), "recovered.vibe")
			if err := os.WriteFile(recoveryPath, image.store, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(recoveryPath+".rjournal", image.journal, 0o600); err != nil {
				t.Fatal(err)
			}
			recoveryFile, err := os.OpenFile(recoveryPath, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer recoveryFile.Close()
			recovered, err := Open(recoveryFile, concurrentPrimaryTestOptions())
			if err != nil {
				t.Fatal(err)
			}
			assertConcurrentPrimaryRaw(t, recovered, key, value)
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConcurrentPrimaryPutCanonicalizationDoesNotConvoyExclusiveWriter(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	prepareKey := []byte(fixture.keys[0])
	prepareValue := []byte(`{ "flush_cut": 1, "prepared": true }`)
	if created, err := collection.Put(prepareKey, prepareValue); err != nil || created {
		t.Fatalf("prepare replacement: created=%v err=%v", created, err)
	}
	preparedGeneration := collection.Generation()

	candidateKey := []byte(fixture.keys[1])
	candidateValue := []byte(`{ "candidate": true, "order": [3, 2, 1] }`)
	want := canonicalConcurrentPrimaryValue(t, candidateValue)
	canonicalEntered := make(chan struct{})
	releaseCanonical := make(chan struct{})
	canonicalComplete := make(chan struct{})
	flushHoldingWriter := make(chan struct{})
	releaseFlush := make(chan struct{})
	staged := make(chan struct{})
	var canonicalEnteredOnce sync.Once
	var canonicalCompleteOnce sync.Once
	var flushHoldingOnce sync.Once
	var stagedOnce sync.Once
	var releaseCanonicalOnce sync.Once
	var releaseFlushOnce sync.Once
	defer releaseCanonicalOnce.Do(func() { close(releaseCanonical) })
	defer releaseFlushOnce.Do(func() { close(releaseFlush) })

	previousCanonical := concurrentPrimaryPutCanonicalizeHook
	previousPreSync := recoveryJournalDeltaPreSyncHook
	previousStaged := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryPutCanonicalizeHook = func(done bool) {
		if !done {
			canonicalEnteredOnce.Do(func() { close(canonicalEntered) })
			<-releaseCanonical
			return
		}
		canonicalCompleteOnce.Do(func() { close(canonicalComplete) })
	}
	recoveryJournalDeltaPreSyncHook = func(uint64) {
		flushHoldingOnce.Do(func() { close(flushHoldingWriter) })
		<-releaseFlush
	}
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		stagedOnce.Do(func() { close(staged) })
	}
	t.Cleanup(func() {
		concurrentPrimaryPutCanonicalizeHook = previousCanonical
		recoveryJournalDeltaPreSyncHook = previousPreSync
		concurrentPrimaryReplaceStagedHook = previousStaged
	})

	putDone := make(chan concurrentPrimaryPutResult, 1)
	go func() {
		created, err := collection.Put(candidateKey, candidateValue)
		putDone <- concurrentPrimaryPutResult{created: created, err: err}
	}()
	awaitConcurrentPrimary(t, canonicalEntered, "private canonicalization entry")

	flushDone := make(chan error, 1)
	go func() { flushDone <- collection.Flush() }()
	// The Flush hook runs only after Flush owns writer exclusively. If the Put
	// took writer.RLock before private canonicalization, this wait deadlocks and
	// the test times out exactly at the convoy regression.
	awaitConcurrentPrimary(t, flushHoldingWriter, "Flush writer acquisition")
	releaseCanonicalOnce.Do(func() { close(releaseCanonical) })
	awaitConcurrentPrimary(t, canonicalComplete, "private canonicalization completion")
	select {
	case <-staged:
		t.Fatal("candidate routed or staged while Flush still held writer")
	default:
	}
	releaseFlushOnce.Do(func() { close(releaseFlush) })
	if err := awaitConcurrentPrimary(t, flushDone, "exclusive Flush"); err != nil {
		t.Fatal(err)
	}
	result := awaitConcurrentPrimary(t, putDone, "post-Flush candidate")
	if result.err != nil || result.created {
		t.Fatalf("candidate replacement: created=%v err=%v",
			result.created, result.err)
	}
	awaitConcurrentPrimary(t, staged, "candidate shared staging")
	if got := collection.Generation(); got != preparedGeneration+1 {
		t.Fatalf("generation = %d, want %d", got, preparedGeneration+1)
	}
	if got := collection.DurableGeneration(); got != preparedGeneration {
		t.Fatalf("durable generation = %d, want flushed cut %d",
			got, preparedGeneration)
	}
	assertConcurrentPrimaryRaw(t, collection, candidateKey, want)
}

func TestConcurrentPrimaryPutPreflightDefersErrorsUntilLaneRecheck(t *testing.T) {
	persistenceFailure := errors.New("preflight persistence failure")
	tests := []struct {
		name      string
		configure func(*Collection) func()
		wantErr   error
	}{
		{
			name: "closed wins",
			configure: func(collection *Collection) func() {
				collection.closed = true
				return func() { collection.closed = false }
			},
			wantErr: ErrClosed,
		},
		{
			name: "persistence failure wins",
			configure: func(collection *Collection) func() {
				collection.journalFailure.Store(&journalFailureBox{
					err: persistenceFailure,
				})
				return func() { collection.journalFailure.Store(nil) }
			},
			wantErr: persistenceFailure,
		},
		{
			name: "lane change wins",
			configure: func(collection *Collection) func() {
				collection.onlineIndexBuild.Store(true)
				return func() { collection.onlineIndexBuild.Store(false) }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := openConcurrentPrimaryTestFixture(
				t, 256, concurrentPrimaryTestOptions(),
			)
			collection := fixture.collection
			canonicalEntered := make(chan struct{})
			releaseCanonical := make(chan struct{})
			var enteredOnce sync.Once
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(releaseCanonical) })
			previousCanonical := concurrentPrimaryPutCanonicalizeHook
			concurrentPrimaryPutCanonicalizeHook = func(done bool) {
				if done {
					return
				}
				enteredOnce.Do(func() { close(canonicalEntered) })
				<-releaseCanonical
			}
			t.Cleanup(func() {
				concurrentPrimaryPutCanonicalizeHook = previousCanonical
			})

			type tryResult struct {
				handled bool
				created bool
				err     error
			}
			resultChannel := make(chan tryResult, 1)
			go func() {
				handled, created, err := collection.tryConcurrentPrimaryPut(
					[]byte(fixture.keys[0]), []byte(`{"unfinished":`),
				)
				resultChannel <- tryResult{
					handled: handled, created: created, err: err,
				}
			}()
			awaitConcurrentPrimary(t, canonicalEntered, "preflight canonicalization")
			collection.writer.Lock()
			restore := test.configure(collection)
			collection.writer.Unlock()
			restored := false
			defer func() {
				if restored {
					return
				}
				collection.writer.Lock()
				restore()
				collection.writer.Unlock()
			}()
			releaseOnce.Do(func() { close(releaseCanonical) })
			result := awaitConcurrentPrimary(t, resultChannel, "preflight result")
			collection.writer.Lock()
			restore()
			collection.writer.Unlock()
			restored = true
			if result.handled || result.created {
				t.Fatalf("preflight result handled/created = %v/%v, want false/false",
					result.handled, result.created)
			}
			if test.wantErr == nil {
				if result.err != nil {
					t.Fatalf("lane recheck error = %v, want nil", result.err)
				}
				return
			}
			if !errors.Is(result.err, test.wantErr) {
				t.Fatalf("preflight error = %v, want %v", result.err, test.wantErr)
			}
		})
	}
}

func TestConcurrentPrimaryReplaceUnsupportedShapesFallBack(t *testing.T) {
	type fallbackCase struct {
		name    string
		options func(*testing.T) Options
		open    func(*testing.T, Options) concurrentPrimaryTestFixture
		mutate  func(*testing.T, concurrentPrimaryTestFixture)
	}
	replace := func(value []byte) func(*testing.T, concurrentPrimaryTestFixture) {
		return func(t *testing.T, fixture concurrentPrimaryTestFixture) {
			created, err := fixture.collection.Put([]byte(fixture.keys[0]), value)
			if err != nil || created {
				t.Fatalf("replacement: created=%v err=%v", created, err)
			}
			assertConcurrentPrimaryRaw(
				t, fixture.collection, []byte(fixture.keys[0]),
				canonicalConcurrentPrimaryValue(t, value),
			)
		}
	}
	cases := []fallbackCase{
		{
			name: "sync-mode",
			options: func(*testing.T) Options {
				return Options{Backend: BackendPortable, ResidentBytes: 64 << 20}
			},
			mutate: replace([]byte(`{"mode":"sync","id":0}`)),
		},
		{
			name: "recovery-journal",
			options: func(*testing.T) Options {
				options := concurrentPrimaryTestOptions()
				options.RecoveryJournal = true
				return options
			},
			mutate: replace([]byte(`{"mode":"journal","id":0}`)),
		},
		{
			name: "exact-index",
			options: func(*testing.T) Options {
				return primaryExactOverlayTestOptions("/group")
			},
			mutate: replace([]byte(`{"id":0,"group":41,"name":"indexed"}`)),
		},
		{
			name: "schema",
			options: func(t *testing.T) Options {
				schema, err := store.CompileSchema(store.SchemaDefinition{
					Root: store.SchemaObject,
					Fields: []store.SchemaField{{
						Path: "/id", Types: store.SchemaInteger, Required: true,
					}},
				})
				if err != nil {
					t.Fatal(err)
				}
				options := concurrentPrimaryTestOptions()
				options.Collection.Schema = schema
				return options
			},
			open:   openConcurrentPrimarySeededTestFixture,
			mutate: replace([]byte(`{"id":0,"schema":"checked"}`)),
		},
		{
			name: "overflow-replacement",
			options: func(*testing.T) Options {
				options := concurrentPrimaryTestOptions()
				options.InlineValueBytes = 64
				options.MaxDocumentBytes = 2048
				return options
			},
			mutate: replace(fmt.Appendf(
				nil, `{"id":0,"overflow":%q}`, bytes.Repeat([]byte("x"), 512),
			)),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			options := test.options(t)
			var fixture concurrentPrimaryTestFixture
			if test.open != nil {
				fixture = test.open(t, options)
			} else {
				fixture = openConcurrentPrimaryTestFixture(t, 256, options)
			}
			baseGeneration := fixture.collection.Generation()
			baseStats := fixture.collection.Stats()
			var stagedCalls atomic.Int64
			previous := concurrentPrimaryReplaceStagedHook
			concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
				stagedCalls.Add(1)
			}
			t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })

			test.mutate(t, fixture)
			if got := stagedCalls.Load(); got != 0 {
				t.Fatalf("unsupported shape reached fast-path staged hook %d times", got)
			}
			if got := fixture.collection.Stats().ConcurrentPrimaryReplaces -
				baseStats.ConcurrentPrimaryReplaces; got != 0 {
				t.Fatalf("unsupported shape recorded %d concurrent replacements", got)
			}
			if got := fixture.collection.Generation(); got != baseGeneration+1 {
				t.Fatalf("generation = %d, want %d", got, baseGeneration+1)
			}
		})
	}
}

func TestConcurrentPrimaryReplaceGenerationIncrementsExactly(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	key := []byte(fixture.keys[0])
	baseGeneration := fixture.collection.Generation()
	baseStats := fixture.collection.Stats()
	var stagedCalls atomic.Int64
	previous := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		stagedCalls.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })

	const writes = 32
	var final []byte
	for i := range writes {
		value := fmt.Appendf(nil, `{ "generation": %d, "stable": true }`, i)
		final = canonicalConcurrentPrimaryValue(t, value)
		created, err := fixture.collection.Put(key, value)
		if err != nil || created {
			t.Fatalf("write %d: created=%v err=%v", i, created, err)
		}
		if got := fixture.collection.Generation(); got != baseGeneration+uint64(i)+1 {
			t.Fatalf(
				"after write %d generation = %d, want %d",
				i, got, baseGeneration+uint64(i)+1,
			)
		}
	}
	assertConcurrentPrimaryRaw(t, fixture.collection, key, final)
	if got := stagedCalls.Load(); got != writes {
		t.Fatalf("staged calls = %d, want %d", got, writes)
	}
	if got := fixture.collection.Stats().ConcurrentPrimaryReplaces -
		baseStats.ConcurrentPrimaryReplaces; got != writes {
		t.Fatalf("concurrent replacements = %d, want %d", got, writes)
	}
}

func TestConcurrentPrimaryScratchIsFixedAndLargeInputFallsBack(t *testing.T) {
	options := concurrentPrimaryTestOptions()
	options.InlineValueBytes = storeio.CommonPrimaryLeafMaxExtentBytes
	fixture := openConcurrentPrimaryTestFixture(t, 256, options)
	pool := fixture.collection.primaryConcurrentContexts
	if pool == nil || pool.rawLimit != primaryConcurrentRawScratchLimit {
		t.Fatalf("concurrent raw scratch limit = %v, want %d", pool, primaryConcurrentRawScratchLimit)
	}
	before := fixture.collection.Stats()
	if before.ConcurrentPrimaryScratchBytes == 0 {
		t.Fatal("eligible store reports no concurrent-primary scratch")
	}

	key := []byte(fixture.keys[0])
	src := []byte(strings.Repeat(" ", primaryConcurrentRawScratchLimit+1) +
		`{"bounded":true}`)
	want := canonicalConcurrentPrimaryValue(t, src)
	created, err := fixture.collection.Put(key, src)
	if err != nil || created {
		t.Fatalf("large bounded fallback: created=%v err=%v", created, err)
	}
	assertConcurrentPrimaryRaw(t, fixture.collection, key, want)
	after := fixture.collection.Stats()
	if got := after.ConcurrentPrimaryReplaces - before.ConcurrentPrimaryReplaces; got != 0 {
		t.Fatalf("large input used concurrent lane %d times, want fallback", got)
	}
	if after.ConcurrentPrimaryScratchBytes != before.ConcurrentPrimaryScratchBytes {
		t.Fatalf(
			"concurrent scratch grew from %d to %d bytes",
			before.ConcurrentPrimaryScratchBytes,
			after.ConcurrentPrimaryScratchBytes,
		)
	}
}

func TestConcurrentPrimaryContextBackingIsLazy(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixtureMode(
		t, 256, concurrentPrimaryTestOptions(), false,
	)
	pool := fixture.collection.primaryConcurrentContexts
	if pool == nil {
		t.Fatal("concurrent context pool is nil")
	}
	if pool.initialized.Load() {
		t.Fatal("read-only open initialized concurrent context backing")
	}
	for i := range pool.contexts {
		context := &pool.contexts[i]
		if context.index != nil || context.patchSpans != nil ||
			context.canonical != nil || context.value != nil ||
			context.publish.signal != nil {
			t.Fatalf("context %d allocated backing before first use", i)
		}
	}
	created, err := fixture.collection.Put(
		[]byte(fixture.keys[0]), []byte(`{"lazy":true}`),
	)
	if err != nil || created {
		t.Fatalf("first replacement = created %v, err %v", created, err)
	}
	if !pool.initialized.Load() {
		t.Fatal("first eligible mutation did not initialize context backing")
	}
	for i := range pool.contexts {
		context := &pool.contexts[i]
		if cap(context.index) == 0 || cap(context.patchSpans) == 0 ||
			cap(context.canonical) == 0 || cap(context.value) == 0 ||
			context.publish.signal == nil {
			t.Fatalf("context %d backing is incomplete after first use", i)
		}
	}
}

func TestConcurrentPrimaryContextPoolExhaustionWakesWithoutLoss(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	pool := fixture.collection.primaryConcurrentContexts
	if pool == nil || len(pool.contexts) < 2 {
		t.Fatalf("concurrent context pool = %v, want at least two slots", pool)
	}
	held := make([]*primaryConcurrentContext, len(pool.contexts))
	for i := range held {
		held[i] = pool.acquire()
	}
	if free := pool.free.Load(); free != 0 {
		t.Fatalf("free mask after exhaustion = %#x, want zero", free)
	}

	const waiting = 64
	acquired := make(chan uint8, waiting)
	for range waiting {
		go func() {
			context := pool.acquire()
			acquired <- context.poolSlot
			pool.release(context)
		}()
	}
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for pool.waiters.Load() != waiting && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := pool.waiters.Load(); got != waiting {
		t.Fatalf("blocked waiters = %d, want %d", got, waiting)
	}

	// One returned slot must hand off through every waiter. Each release wakes
	// the next, so a missed exhaustion wake strands this loop deterministically.
	pool.release(held[0])
	held[0] = nil
	for i := range waiting {
		select {
		case slot := <-acquired:
			if slot != 0 {
				t.Fatalf("waiter %d acquired slot %d, want the sole free slot 0", i, slot)
			}
		case <-time.After(concurrentPrimaryTestTimeout):
			t.Fatalf("waiter %d remained blocked after chained releases", i)
		}
	}
	for _, context := range held[1:] {
		pool.release(context)
	}
	if got := pool.waiters.Load(); got != 0 {
		t.Fatalf("waiters after drain = %d, want zero", got)
	}
	wantFree := primaryConcurrentContextMask
	if len(pool.contexts) < primaryConcurrentContextLimit {
		wantFree = (uint64(1) << uint(len(pool.contexts))) - 1
	}
	if got := pool.free.Load(); got != wantFree {
		t.Fatalf("free mask after drain = %#x, want %#x", got, wantFree)
	}
}

func TestConcurrentPrimaryContextPoolCloseRejectsSaturatedOperations(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	collection := fixture.collection
	pool := collection.primaryConcurrentContexts
	if pool == nil {
		t.Fatal("concurrent context pool is nil")
	}
	held := make([]*primaryConcurrentContext, len(pool.contexts))
	for index := range held {
		held[index] = pool.acquire()
		if held[index] == nil {
			t.Fatalf("context %d was not acquired", index)
		}
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- collection.Close() }()
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for pool.free.Load()&primaryConcurrentPoolClosed == 0 &&
		time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if pool.free.Load()&primaryConcurrentPoolClosed == 0 {
		t.Fatal("Close did not close the saturated context pool")
	}

	type operationResult struct {
		name string
		err  error
	}
	operations := make(chan operationResult, 2)
	go func() {
		_, err := collection.Put(
			[]byte(fixture.keys[0]), []byte(`{"closed":"put"}`),
		)
		operations <- operationResult{name: "Put", err: err}
	}()
	go func() {
		_, err := collection.Delete([]byte(fixture.keys[1]))
		operations <- operationResult{name: "Delete", err: err}
	}()
	for range 2 {
		select {
		case result := <-operations:
			if !errors.Is(result.err, ErrClosed) {
				t.Fatalf("post-Close %s error = %v, want ErrClosed",
					result.name, result.err)
			}
		case <-time.After(concurrentPrimaryTestTimeout):
			t.Fatal("post-Close operation blocked on saturated context pool")
		}
	}

	for _, context := range held {
		pool.release(context)
	}
	if err := awaitConcurrentPrimary(t, closeDone, "saturated-pool Close"); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentPrimaryContextPoolCASMaintainsUniqueOwnership(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 256, concurrentPrimaryTestOptions(),
	)
	pool := fixture.collection.primaryConcurrentContexts
	if pool == nil {
		t.Fatal("concurrent context pool is nil")
	}
	var owners [primaryConcurrentContextLimit]atomic.Int32
	start := make(chan struct{})
	failure := make(chan string, 1)
	const (
		workers    = 128
		iterations = 500
	)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			for range iterations {
				context := pool.acquire()
				slot := int(context.poolSlot)
				if slot >= len(pool.contexts) || &pool.contexts[slot] != context {
					select {
					case failure <- fmt.Sprintf("foreign context slot %d", slot):
					default:
					}
					return
				}
				if owners[slot].Add(1) != 1 {
					select {
					case failure <- fmt.Sprintf("slot %d acquired concurrently", slot):
					default:
					}
					owners[slot].Add(-1)
					pool.release(context)
					return
				}
				runtime.Gosched()
				owners[slot].Add(-1)
				pool.release(context)
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("contended context pool did not drain")
	}
	select {
	case message := <-failure:
		t.Fatal(message)
	default:
	}
	if got := pool.waiters.Load(); got != 0 {
		t.Fatalf("waiters after stress = %d, want zero", got)
	}
}

func TestConcurrentPrimaryReplacePressureFallsBackWithoutDeadlock(t *testing.T) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	key := []byte(fixture.keys[0])
	baseGeneration := fixture.collection.Generation()
	baseStats := fixture.collection.Stats()
	var stagedCalls atomic.Int64
	previous := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) {
		stagedCalls.Add(1)
	}
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })

	const writes = primaryUnifiedOverlayRecords + 1
	writeDone := make(chan error, 1)
	var final []byte
	for i := range writes {
		final = fmt.Appendf(final[:0], `{"pressure":%d}`, i)
	}
	go func() {
		for i := range writes {
			value := fmt.Appendf(nil, `{"pressure":%d}`, i)
			created, err := fixture.collection.Put(key, value)
			if err != nil {
				writeDone <- fmt.Errorf("write %d: %w", i, err)
				return
			}
			if created {
				writeDone <- fmt.Errorf("write %d unexpectedly inserted the key", i)
				return
			}
		}
		writeDone <- nil
	}()
	if err := awaitConcurrentPrimary(t, writeDone, "pressure fallback writes"); err != nil {
		t.Fatal(err)
	}
	final = canonicalConcurrentPrimaryValue(t, final)
	assertConcurrentPrimaryRaw(t, fixture.collection, key, final)
	if got := fixture.collection.Generation(); got != baseGeneration+writes {
		t.Fatalf("generation = %d, want %d", got, baseGeneration+writes)
	}
	if got := stagedCalls.Load(); got < writes {
		t.Fatalf("staged calls = %d, want at least %d including pressure retry", got, writes)
	}
	after := fixture.collection.Stats()
	if after.PrimaryOverlayFolds <= baseStats.PrimaryOverlayFolds {
		t.Fatalf(
			"primary overlay folds = %d, baseline %d",
			after.PrimaryOverlayFolds, baseStats.PrimaryOverlayFolds,
		)
	}
	if got := after.ConcurrentPrimaryReplaces - baseStats.ConcurrentPrimaryReplaces; got != primaryUnifiedOverlayRecords {
		t.Fatalf(
			"concurrent replacements = %d, want %d fast successes plus one fallback",
			got, primaryUnifiedOverlayRecords,
		)
	}
	if got := after.ConcurrentPrimaryFallbacks - baseStats.ConcurrentPrimaryFallbacks; got != 1 {
		t.Fatalf("coordinated concurrent fallbacks = %d, want 1", got)
	}
	if got := after.ConcurrentPrimaryReplaces - baseStats.ConcurrentPrimaryReplaces +
		after.ConcurrentPrimaryFallbacks - baseStats.ConcurrentPrimaryFallbacks; got != writes {
		t.Fatalf("accounted pressure writes = %d, want %d", got, writes)
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- fixture.collection.Flush() }()
	if err := awaitConcurrentPrimary(t, flushDone, "post-pressure Flush"); err != nil {
		t.Fatal(err)
	}
	if got := fixture.collection.DurableGeneration(); got != baseGeneration+writes {
		t.Fatalf("durable generation = %d, want %d", got, baseGeneration+writes)
	}
}
