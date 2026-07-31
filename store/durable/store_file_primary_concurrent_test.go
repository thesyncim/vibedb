package durable

import (
	"bytes"
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
	t.Helper()
	built, keys, values := buildFilePrimaryCorpus(t, count)
	file := createPrimaryPointFile(
		t, built, options, "concurrent-primary.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
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
				storeio.CommonPrimaryLeafUnified
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
		t.Fatalf("%d keys supplied no unified bucket with two rows", len(fixture.keys))
	}
	if disjoint[1] == 0 {
		t.Fatalf("%d keys supplied no two unified buckets on distinct stripes", len(fixture.keys))
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
	// Later calls may sample an occupied direct-read epoch and take the
	// established snapshot-safe fallback, or land between epochs and retain the
	// concurrent lane; both schedules must preserve complete canonical values.
	staged := stagedCalls.Load()
	if staged < 1 || staged > writes {
		t.Fatalf("staged calls = %d, want between 1 and %d", staged, writes)
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

func TestConcurrentPrimaryReplaceUnsupportedShapesFallBack(t *testing.T) {
	type fallbackCase struct {
		name    string
		options func(*testing.T) Options
		open    func(*testing.T, Options) concurrentPrimaryTestFixture
		mutate  func(*testing.T, concurrentPrimaryTestFixture)
	}
	baseOptions := func(*testing.T) Options { return concurrentPrimaryTestOptions() }
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
			name:    "insert",
			options: baseOptions,
			mutate: func(t *testing.T, fixture concurrentPrimaryTestFixture) {
				key := []byte("new-concurrent-primary-key")
				value := canonicalConcurrentPrimaryValue(t, []byte(`{ "inserted": true }`))
				created, err := fixture.collection.Put(key, value)
				if err != nil || !created {
					t.Fatalf("insert: created=%v err=%v", created, err)
				}
				assertConcurrentPrimaryRaw(t, fixture.collection, key, value)
			},
		},
		{
			name:    "delete",
			options: baseOptions,
			mutate: func(t *testing.T, fixture concurrentPrimaryTestFixture) {
				key := []byte(fixture.keys[0])
				deleted, err := fixture.collection.Delete(key)
				if err != nil || !deleted {
					t.Fatalf("delete: deleted=%v err=%v", deleted, err)
				}
				if got, found, err := fixture.collection.AppendRaw(nil, key); err != nil || found {
					t.Fatalf("deleted read: found=%v value=%s err=%v", found, got, err)
				}
			},
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
