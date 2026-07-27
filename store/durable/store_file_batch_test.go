package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func testBatchOptions(batchDocuments int) Options {
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 8
	options.MaxBatchDocuments = batchDocuments
	options.BufferCount = 0
	options.MaxRetiredExtents = 1 << 15
	options.ResidentBytes = 16 << 20
	return options
}

func openBatchCollection(t *testing.T, options Options) (*Collection, *os.File) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "file-store-batch-*")
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Create(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = collection.Close()
		_ = file.Close()
	})
	return collection, file
}

// TestCollectionUpdateMatchesSequentialMutations is the differential oracle for
// the batched write path. One collection receives every mutation through Put
// and Delete, the other receives the same mutations grouped into Update calls,
// and the two must be indistinguishable afterwards: same length, same document
// for every key, and same exact-index answers.
func TestCollectionUpdateMatchesSequentialMutations(t *testing.T) {
	options := testBatchOptions(24)
	options.Indexes = []store.IndexDefinition{
		{Name: "status", Paths: []string{"/status"}},
		{Name: "pair", Paths: []string{"/status", "/kind"}},
	}
	sequential, _ := openBatchCollection(t, options)
	batched, _ := openBatchCollection(t, options)

	random := rand.New(rand.NewSource(0xba7c4))
	// The last status is deliberately longer than a leaf can carry as an exact
	// representative. Such a value is still routed by its tuple hash but records
	// no certificate, and that is a distinct encoding the shorter values never
	// reach.
	statuses := []string{"active", "idle", "paused", strings.Repeat("verbose", 300)}
	live := map[string][]byte{}
	for round := range 30 {
		count := 1 + random.Intn(20)
		type mutation struct {
			key    string
			value  []byte
			remove bool
		}
		planned := make([]mutation, 0, count)
		for range count {
			key := fmt.Sprintf("key-%03d", random.Intn(120))
			_, present := live[key]
			if present && random.Intn(4) == 0 {
				planned = append(planned, mutation{key: key, remove: true})
				continue
			}
			value := fmt.Appendf(nil, `{"round":%d,"status":%q,"kind":%q,"n":%d,"pad":%q}`,
				round, statuses[random.Intn(len(statuses))],
				[]string{"a", "b"}[random.Intn(2)], random.Intn(1000),
				strings.Repeat("p", random.Intn(700)))
			planned = append(planned, mutation{key: key, value: value})
		}
		for _, m := range planned {
			if m.remove {
				if _, err := sequential.Delete(m.key); err != nil {
					t.Fatalf("round %d Delete: %v", round, err)
				}
				delete(live, m.key)
				continue
			}
			if _, err := sequential.Put(m.key, m.value); err != nil {
				t.Fatalf("round %d Put: %v", round, err)
			}
			live[m.key] = m.value
		}
		if err := batched.Update(func(b *WriteBatch) error {
			for _, m := range planned {
				if m.remove {
					if err := b.Delete(m.key); err != nil {
						return err
					}
					continue
				}
				if err := b.Put(m.key, m.value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("round %d Update: %v", round, err)
		}
		assertCollectionsAgree(t, fmt.Sprintf("round %d", round), sequential, batched, 120)
		for _, status := range statuses {
			want := 0
			for _, value := range live {
				if bytes.Contains(value, fmt.Appendf(nil, `"status":%q`, status)) {
					want++
				}
			}
			label := fmt.Sprintf("round %d status=%s", round, status)
			assertIndexRowCount(t, label+" sequential", sequential, "status", status, want)
			assertIndexRowCount(t, label+" batched", batched, "status", status, want)
		}
	}
}

// assertIndexRowCount resolves one exact index value and counts the rows behind
// the masks. Comparing counts rather than masks is deliberate: a batch that
// replaces a key the same batch deleted keeps the row in place, while the
// sequential equivalent frees the slot and appends, so the two collections hold
// identical documents at deliberately different coordinates.
func assertIndexRowCount(t *testing.T, label string, collection *Collection, name, value string, want int) {
	t.Helper()
	src := fmt.Appendf(nil, "%q", value)
	needed, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		t.Fatal(err)
	}
	index, err := vibejson.BuildIndex(src, make([]vibejson.IndexEntry, needed))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	masks, err := snapshot.AppendIndexMasks(nil, name, index)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	rows := 0
	for _, mask := range masks {
		rows += popcount(mask.Bits)
	}
	if rows != want {
		t.Fatalf("%s: index resolved %d rows, want %d", label, rows, want)
	}
}

func assertCollectionsAgree(t *testing.T, label string, want, got *Collection, keys int) {
	t.Helper()
	if want.Len() != got.Len() {
		t.Fatalf("%s: length = %d, want %d", label, got.Len(), want.Len())
	}
	for i := range keys {
		key := fmt.Sprintf("key-%03d", i)
		wantRaw, wantOK, err := want.AppendRaw(nil, key)
		if err != nil {
			t.Fatalf("%s: sequential AppendRaw(%s): %v", label, key, err)
		}
		gotRaw, gotOK, err := got.AppendRaw(nil, key)
		if err != nil {
			t.Fatalf("%s: batched AppendRaw(%s): %v", label, key, err)
		}
		if wantOK != gotOK || string(wantRaw) != string(gotRaw) {
			t.Fatalf("%s: %s = (%q,%v), want (%q,%v)", label, key, gotRaw, gotOK, wantRaw, wantOK)
		}
	}
}

// TestCollectionUpdatePublishesOneGenerationPerBatch is the whole point of the
// API: a batch must cost one publication and one durability fence, not one per
// document. A regression here is invisible to every correctness test.
func TestCollectionUpdatePublishesOneGenerationPerBatch(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(32))
	before := collection.Generation()
	beforeStats := collection.Stats()
	if err := collection.Update(func(b *WriteBatch) error {
		for i := range 32 {
			if err := b.Put(fmt.Sprintf("key-%03d", i), fmt.Appendf(nil, `{"i":%d}`, i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := collection.Generation() - before; got != 1 {
		t.Fatalf("batch published %d generations, want 1", got)
	}
	if collection.Len() != 32 {
		t.Fatalf("length = %d, want 32", collection.Len())
	}
	afterStats := collection.Stats()
	if commits := afterStats.DeviceCommits - beforeStats.DeviceCommits; commits != 1 {
		t.Fatalf("batch cost %d device commits, want 1", commits)
	}
}

// TestCollectionUpdateRollsBackOnError proves the batch is failure-atomic at
// its own boundary: a closure that fails, and a document the applier rejects,
// must both leave the published generation untouched.
func TestCollectionUpdateRollsBackOnError(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(16))
	if _, err := collection.Put("seed", []byte(`{"seed":true}`)); err != nil {
		t.Fatal(err)
	}
	generation := collection.Generation()
	sentinel := errors.New("closure failed")
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put("added", []byte(`{"a":1}`)); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("closure error = %v, want %v", err, sentinel)
	}
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put("added", []byte(`{"a":1}`)); err != nil {
			return err
		}
		return b.Put("broken", []byte(`{"a":`))
	}); err == nil {
		t.Fatal("malformed document accepted")
	}
	if collection.Generation() != generation {
		t.Fatalf("generation = %d, want %d", collection.Generation(), generation)
	}
	if collection.Len() != 1 {
		t.Fatalf("length = %d, want 1", collection.Len())
	}
	if _, ok, err := collection.AppendRaw(nil, "added"); err != nil || ok {
		t.Fatalf("aborted batch published a row: (%v,%v)", ok, err)
	}
	// The collection must still be writable: an aborted transaction that leaked
	// its reservation or its retirement claim would fail the next commit.
	if _, err := collection.Put("after", []byte(`{"a":2}`)); err != nil {
		t.Fatalf("write after aborted batch: %v", err)
	}
}

// TestCollectionUpdateDeduplicatesRepeatedKeys pins the last-write-wins
// contract. Two rows for one key inside a single chunk rebuild would corrupt
// the page, so the deduplication is a correctness requirement, not a
// convenience.
func TestCollectionUpdateDeduplicatesRepeatedKeys(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(16))
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put("k", []byte(`{"v":1}`)); err != nil {
			return err
		}
		if err := b.Put("k", []byte(`{"v":2}`)); err != nil {
			return err
		}
		if err := b.Put("gone", []byte(`{"v":3}`)); err != nil {
			return err
		}
		if err := b.Delete("gone"); err != nil {
			return err
		}
		// The single-document path accepts the empty key, so the batch must
		// too: a bound one API enforces and the other does not is the kind of
		// divergence a caller only finds in production.
		return b.Put("", []byte(`{"v":4}`))
	}); err != nil {
		t.Fatal(err)
	}
	if collection.Len() != 2 {
		t.Fatalf("length = %d, want 2", collection.Len())
	}
	if raw, ok, err := collection.AppendRaw(nil, ""); err != nil || !ok || string(raw) != `{"v":4}` {
		t.Fatalf("empty key = (%q,%v,%v)", raw, ok, err)
	}
	raw, ok, err := collection.AppendRaw(nil, "k")
	if err != nil || !ok || string(raw) != `{"v":2}` {
		t.Fatalf("k = (%q,%v,%v), want the second document", raw, ok, err)
	}
	if _, ok, err := collection.AppendRaw(nil, "gone"); err != nil || ok {
		t.Fatalf("gone = (%v,%v), want absent", ok, err)
	}
}

// TestCollectionUpdateRejectsOversizedBatch keeps the reservation honest. A
// batch that outgrew its bound must be refused before anything is published
// rather than silently split across two generations.
func TestCollectionUpdateRejectsOversizedBatch(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(4))
	generation := collection.Generation()
	err := collection.Update(func(b *WriteBatch) error {
		for i := range 5 {
			if err := b.Put(fmt.Sprintf("key-%d", i), fmt.Appendf(nil, `{"i":%d}`, i)); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("oversized batch = %v, want ErrBatchTooLarge", err)
	}
	if collection.Generation() != generation || collection.Len() != 0 {
		t.Fatalf("oversized batch published generation %d length %d", collection.Generation(), collection.Len())
	}
}

func TestCollectionUpdateBoundsRepeatedKeyArenaAndTotalBytes(t *testing.T) {
	options := testBatchOptions(4)
	options.MaxDocumentBytes = 1024
	options.MaxBatchBytes = options.MaxDocumentBytes +
		options.MaxBatchDocuments*options.MaxKeyBytes
	collection, _ := openBatchCollection(t, options)
	if got := collection.MaxBatchBytes(); got != options.MaxBatchBytes {
		t.Fatalf("MaxBatchBytes = %d, want %d", got, options.MaxBatchBytes)
	}

	large := bytes.Repeat([]byte("x"), 900)
	large[0], large[len(large)-1] = '{', '}'
	var arenaCapacity int
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put("same", large); err != nil {
			return err
		}
		for i := range 10_000 {
			if i&1 == 0 {
				if err := b.Delete("same"); err != nil {
					return err
				}
			} else if err := b.Put("same", []byte(`{"final":false}`)); err != nil {
				return err
			}
			arenaCapacity = max(arenaCapacity, cap(b.values))
		}
		return b.Put("same", []byte(`{"final":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	if arenaCapacity > 2048 {
		t.Fatalf("repeated-key value arena capacity = %d, want bounded near largest value", arenaCapacity)
	}
	got, ok, err := collection.AppendRaw(nil, "same")
	if err != nil || !ok || string(got) != `{"final":true}` {
		t.Fatalf("final repeated value = (%q,%v,%v)", got, ok, err)
	}

	generation := collection.Generation()
	err = collection.Update(func(b *WriteBatch) error {
		if err := b.Put("first", large); err != nil {
			return err
		}
		return b.Put("second", large)
	})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("aggregate byte overflow = %v, want ErrBatchTooLarge", err)
	}
	if collection.Generation() != generation {
		t.Fatalf("aggregate byte overflow published generation %d, want %d", collection.Generation(), generation)
	}
}

// TestCollectionUpdateBatchIsSingleUse stops a caller from retaining the
// pooled batch: the next Update would otherwise find another caller's
// mutations already recorded.
func TestCollectionUpdateBatchIsSingleUse(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	var retained *WriteBatch
	if err := collection.Update(func(b *WriteBatch) error {
		retained = b
		return b.Put("k", []byte(`{"v":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := retained.Put("late", []byte(`{"v":2}`)); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("retained batch Put = %v, want ErrBatchClosed", err)
	}
	if err := retained.Delete("k"); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("retained batch Delete = %v, want ErrBatchClosed", err)
	}
	if collection.Len() != 1 {
		t.Fatalf("length = %d, want 1", collection.Len())
	}
}

// TestCollectionUpdateSurvivesReopen checks that a batched generation is a
// complete durable state and not merely a correct in-memory one.
func TestCollectionUpdateSurvivesReopen(t *testing.T) {
	options := testBatchOptions(40)
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	file, err := os.CreateTemp(t.TempDir(), "file-store-batch-reopen-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Update(func(b *WriteBatch) error {
		for i := range 40 {
			if err := b.Put(fmt.Sprintf("key-%03d", i), fmt.Appendf(nil,
				`{"i":%d,"status":%q,"pad":%q}`, i, []string{"a", "b"}[i%2],
				strings.Repeat("q", i*13))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := collection.Update(func(b *WriteBatch) error {
		for i := 0; i < 40; i += 3 {
			if err := b.Delete(fmt.Sprintf("key-%03d", i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := range 40 {
		if i%3 == 0 {
			continue
		}
		key := fmt.Sprintf("key-%03d", i)
		raw, ok, rawErr := collection.AppendRaw(nil, key)
		if rawErr != nil || !ok {
			t.Fatalf("%s = (%v,%v)", key, ok, rawErr)
		}
		want[key] = string(raw)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if int(reopened.Len()) != len(want) {
		t.Fatalf("reopened length = %d, want %d", reopened.Len(), len(want))
	}
	for key, value := range want {
		raw, ok, rawErr := reopened.AppendRaw(nil, key)
		if rawErr != nil || !ok || string(raw) != value {
			t.Fatalf("reopened %s = (%q,%v,%v), want %q", key, raw, ok, rawErr, value)
		}
	}
	for i := 0; i < 40; i += 3 {
		key := fmt.Sprintf("key-%03d", i)
		if _, ok, rawErr := reopened.AppendRaw(nil, key); rawErr != nil || ok {
			t.Fatalf("reopened deleted %s = (%v,%v)", key, ok, rawErr)
		}
	}
	// A batched generation must leave the free set replayable and consistent
	// with what the published root still reaches.
	if err := reopened.refreshReusable(reopened.state.Load()); err != nil {
		t.Fatalf("free-log replay after batched generations: %v", err)
	}
	assertFreeSetMirror(t, reopened, "after batched generations")
}

// TestCollectionUpdateWritesFewerDeviceBytesThanPuts is the space half of the
// claim. Grouping mutations must not merely save fences: it must publish fewer
// pages, because one chunk rebuild and one directory descent replace many.
func TestCollectionUpdateWritesFewerDeviceBytesThanPuts(t *testing.T) {
	options := testBatchOptions(64)
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	document := func(i int) []byte {
		return fmt.Appendf(nil, `{"i":%d,"status":%q}`, i, fmt.Sprintf("s%02d", i%7))
	}

	sequential, _ := openBatchCollection(t, options)
	base := sequential.Stats()
	for i := range 64 {
		if _, err := sequential.Put(fmt.Sprintf("key-%03d", i), document(i)); err != nil {
			t.Fatal(err)
		}
	}
	sequentialBytes := sequential.Stats().DeviceBytes - base.DeviceBytes

	batched, _ := openBatchCollection(t, options)
	base = batched.Stats()
	if err := batched.Update(func(b *WriteBatch) error {
		for i := range 64 {
			if err := b.Put(fmt.Sprintf("key-%03d", i), document(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	batchedBytes := batched.Stats().DeviceBytes - base.DeviceBytes
	if batchedBytes*4 >= sequentialBytes {
		t.Fatalf("batched wrote %d device bytes, sequential wrote %d", batchedBytes, sequentialBytes)
	}
	assertCollectionsAgree(t, "device bytes", sequential, batched, 64)
}

// TestCollectionUpdateKeepsPostingsWhenAcceleratorsAreDropped covers the one
// place the batched path deliberately does less than the single-document path:
// a bulk-built collection's compact index-group catalog and dense float64
// projection are released rather than maintained. Releasing them may cost query
// speed; it must never cost an answer, which means the exact postings the
// batch does maintain have to still resolve every value.
func TestCollectionUpdateKeepsPostingsWhenAcceleratorsAreDropped(t *testing.T) {
	options := testBatchOptions(16)
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	options.Float64Columns = []string{"/score"}
	source, err := store.New(options.Collection)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 24 {
		if _, err := source.Put(fmt.Sprintf("key-%03d", i), fmt.Appendf(nil,
			`{"i":%d,"status":%q,"score":%d}`, i, []string{"a", "b"}[i%2], i)); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "file-store-batch-bulk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if collection.state.Load().root.IndexGroupHead == (storeio.PageRef{}) {
		t.Fatal("bulk build produced no index-group catalog to release")
	}
	if err := collection.Update(func(b *WriteBatch) error {
		for i := 24; i < 32; i++ {
			if err := b.Put(fmt.Sprintf("key-%03d", i), fmt.Appendf(nil,
				`{"i":%d,"status":"c","score":%d}`, i, i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if head := collection.state.Load().root.IndexGroupHead; head != (storeio.PageRef{}) {
		t.Fatalf("index-group catalog survived a batch: %+v", head)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, value := range []string{`"a"`, `"b"`, `"c"`} {
		src := []byte(value)
		needed, err := vibejson.RequiredIndexEntries(src)
		if err != nil {
			t.Fatal(err)
		}
		index, err := vibejson.BuildIndex(src, make([]vibejson.IndexEntry, needed))
		if err != nil {
			t.Fatal(err)
		}
		masks, err := snapshot.AppendIndexMasks(nil, "status", index)
		if err != nil {
			t.Fatalf("probe %s after accelerator release: %v", value, err)
		}
		rows := 0
		for _, mask := range masks {
			rows += popcount(mask.Bits)
		}
		want := 12
		if value == `"c"` {
			want = 8
		}
		if rows != want {
			t.Fatalf("status=%s resolved %d rows, want %d", value, rows, want)
		}
	}
}

func popcount(bits uint64) int {
	count := 0
	for bits != 0 {
		bits &= bits - 1
		count++
	}
	return count
}

// TestCollectionUpdateCrashImagesRecoverWholeBatch is the durability oracle for
// the batched write path. Every prefix of the commit's positional writes, and
// every prefix of the superblock that follows them, is reopened and must yield
// one whole generation: either every mutation the batch made or none of them.
//
// A batch is where a partial recovery would be most visible and least
// detectable — a torn commit that published some of twelve documents would read
// back as a perfectly valid, quietly wrong collection — so the assertion is on
// the entire mutated key set rather than on one probe key.
func TestCollectionUpdateCrashImagesRecoverWholeBatch(t *testing.T) {
	options := testBatchOptions(24)
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	file, err := os.CreateTemp(t.TempDir(), "file-store-batch-crash-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	seeded := map[string]string{}
	for i := range 12 {
		key := fmt.Sprintf("seed-%02d", i)
		value := fmt.Sprintf(`{"i":%d,"status":"old","pad":%q}`, i, strings.Repeat("a", i*60))
		if _, err := collection.Put(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
		seeded[key] = value
	}
	oldGeneration := collection.Generation()
	before, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	updated := map[string]string{}
	for key, value := range seeded {
		updated[key] = value
	}
	if err := collection.Update(func(b *WriteBatch) error {
		for i := range 10 {
			key := fmt.Sprintf("added-%02d", i)
			value := fmt.Sprintf(`{"i":%d,"status":"new","pad":%q}`, i, strings.Repeat("z", i*90))
			if err := b.Put(key, []byte(value)); err != nil {
				return err
			}
			updated[key] = value
		}
		for i := range 3 {
			key := fmt.Sprintf("seed-%02d", i)
			value := fmt.Sprintf(`{"i":%d,"status":"replaced","pad":"r"}`, i)
			if err := b.Put(key, []byte(value)); err != nil {
				return err
			}
			updated[key] = value
		}
		for i := 9; i < 12; i++ {
			key := fmt.Sprintf("seed-%02d", i)
			if err := b.Delete(key); err != nil {
				return err
			}
			delete(updated, key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	newGeneration := collection.Generation()
	if newGeneration != oldGeneration+1 {
		t.Fatalf("batch published generations %d..%d", oldGeneration, newGeneration)
	}
	after, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	pageSize := options.PageSize
	dataStart := testMutableDataStart(pageSize)
	var recoveredOld, recoveredNew int
	for _, cut := range changedPageCrashCuts(before, after, dataStart, pageSize) {
		image := make([]byte, max(len(before), len(after)))
		copy(image, before)
		copy(image[dataStart:dataStart+cut], after[dataStart:dataStart+cut])
		assertBatchCrashImage(t, image, options, oldGeneration, newGeneration,
			seeded, updated, fmt.Sprintf("data-cut-%d", cut),
			&recoveredOld, &recoveredNew)
	}
	rootOffset := int((newGeneration-1)&1) * pageSize
	for cut := 0; cut <= storeio.InlineSuperblockSize; cut++ {
		image := append([]byte(nil), after...)
		copy(image[rootOffset:rootOffset+pageSize], before[rootOffset:rootOffset+pageSize])
		copy(image[rootOffset:rootOffset+cut], after[rootOffset:rootOffset+cut])
		assertBatchCrashImage(t, image, options, oldGeneration, newGeneration,
			seeded, updated, fmt.Sprintf("root-cut-%d", cut),
			&recoveredOld, &recoveredNew)
	}
	// A sweep in which every image recovered the same generation would pass
	// without ever exercising the boundary it exists to police, so require both
	// outcomes to have actually occurred.
	if recoveredOld == 0 || recoveredNew == 0 {
		t.Fatalf("crash sweep recovered %d old and %d new generations; both must occur",
			recoveredOld, recoveredNew)
	}
}

func assertBatchCrashImage(
	t *testing.T, image []byte, options Options,
	oldGeneration, newGeneration uint64,
	seeded, updated map[string]string, name string,
	recoveredOld, recoveredNew *int,
) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatalf("%s recovery: %v", name, err)
	}
	defer collection.Close()
	want := seeded
	switch collection.Generation() {
	case oldGeneration:
		*recoveredOld++
	case newGeneration:
		*recoveredNew++
		want = updated
	default:
		t.Fatalf("%s recovered generation %d, want %d or %d",
			name, collection.Generation(), oldGeneration, newGeneration)
	}
	if int(collection.Len()) != len(want) {
		t.Fatalf("%s recovered %d documents, want %d", name, collection.Len(), len(want))
	}
	for key, value := range want {
		raw, ok, rawErr := collection.AppendRaw(nil, key)
		if rawErr != nil || !ok || string(raw) != value {
			t.Fatalf("%s: %s = (%q,%v,%v), want %q", name, key, raw, ok, rawErr, value)
		}
	}
	for _, key := range []string{"added-00", "added-09", "seed-09", "seed-11"} {
		_, present := want[key]
		_, ok, rawErr := collection.AppendRaw(nil, key)
		if rawErr != nil || ok != present {
			t.Fatalf("%s: %s presence = (%v,%v), want %v", name, key, ok, rawErr, present)
		}
	}
	if err := collection.refreshReusable(collection.state.Load()); err != nil {
		t.Fatalf("%s free-log replay after recovery: %v", name, err)
	}
	assertFreeSetMirror(t, collection, name+" after recovery")
}

// TestCollectionWideIndexedBatchSustainsFreeLogFolds covers the interaction
// between a wide random batch and durable free-space accounting. Such a batch
// can retire pages from far more than the single-document fold baseline's
// sixteen image segments in one generation; the transaction must scale its
// fold reservation rather than fail after the delta chain reaches its first
// fold.
func TestCollectionWideIndexedBatchSustainsFreeLogFolds(t *testing.T) {
	const (
		rows        = 4096
		batchSize   = 64
		generations = 256
	)
	options := testBatchOptions(batchSize)
	options.Durability = DurabilityAsyncVisible
	options.ResidentBytes = 128 << 20
	options.Indexes = []store.IndexDefinition{
		{Name: "status", Paths: []string{"/status"}},
	}
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.freeFoldLimit <= storeio.FreeLogMaxFoldSegments {
		t.Fatalf(
			"wide batch fold limit = %d, want more than baseline %d",
			normalized.freeFoldLimit, storeio.FreeLogMaxFoldSegments,
		)
	}

	source, err := store.New(options.Collection)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if _, err := source.Put(
			fmt.Sprintf("key-%04d", i),
			fmt.Appendf(nil, `{"id":%d,"status":"state-0"}`, i),
		); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "wide-indexed-fold-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if collection != nil {
			_ = collection.Close()
		}
	}()
	versions := make([]uint8, rows)
	for generation := range generations {
		start := generation * 811 & (rows - 1)
		if err := collection.Update(func(batch *WriteBatch) error {
			for j := range batchSize {
				index := (start + j*1597) & (rows - 1)
				versions[index] ^= 1
				if err := batch.Put(
					fmt.Sprintf("key-%04d", index),
					fmt.Appendf(
						nil, `{"id":%d,"status":"state-%d"}`,
						index, versions[index],
					),
				); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("generation %d: %v", generation, err)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := collection.Stats()
	if stats.DocumentCount != rows || stats.AbandonedExtents != 0 {
		t.Fatalf(
			"sustained batch stats = documents %d abandoned %d",
			stats.DocumentCount, stats.AbandonedExtents,
		)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	collection = nil
	collection, err = Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if collection.Len() != rows {
		t.Fatalf("reopened length = %d, want %d", collection.Len(), rows)
	}
	for i := 0; i < rows; i += 257 {
		want := fmt.Sprintf(`{"id":%d,"status":"state-%d"}`, i, versions[i])
		got, ok, err := collection.AppendRaw(nil, fmt.Sprintf("key-%04d", i))
		if err != nil || !ok || string(got) != want {
			t.Fatalf(
				"reopened key %d = (%q,%v,%v), want %q",
				i, got, ok, err, want,
			)
		}
	}
}

// TestCollectionUpdateNoOpBatchPublishesNothing covers the case where every
// recorded mutation resolves away: deletes of keys the collection does not
// hold. Nothing may be published, and — because these options are synchronous —
// the caller must not be left waiting on a generation the committer never
// received.
func TestCollectionUpdateNoOpBatchPublishesNothing(t *testing.T) {
	options := testBatchOptions(8)
	if options.Durability == DurabilityAsyncVisible {
		t.Fatal("this test only means something with synchronous durability")
	}
	collection, _ := openBatchCollection(t, options)
	if _, err := collection.Put("present", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	generation := collection.Generation()
	stats := collection.Stats()
	if err := collection.Update(func(b *WriteBatch) error {
		for i := range 4 {
			if err := b.Delete(fmt.Sprintf("absent-%d", i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("no-op batch: %v", err)
	}
	if err := collection.Update(func(b *WriteBatch) error { return nil }); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if collection.Generation() != generation {
		t.Fatalf("generation = %d, want %d", collection.Generation(), generation)
	}
	if got := collection.Stats().DeviceCommits; got != stats.DeviceCommits {
		t.Fatalf("device commits = %d, want %d", got, stats.DeviceCommits)
	}
	if _, err := collection.Put("after", []byte(`{"v":2}`)); err != nil {
		t.Fatalf("write after no-op batch: %v", err)
	}
}

// TestCollectionUpdateIndexesValuesWithoutCertificates covers the indexed value
// that is too long for a leaf's exact-value representative. Such a value is
// still routed by its tuple hash, but its posting carries no certificate, and a
// zero-length certificate span is only well formed at offset zero — so a batch
// that met one after any other certificate used to be rejected outright.
func TestCollectionUpdateIndexesValuesWithoutCertificates(t *testing.T) {
	options := testBatchOptions(8)
	options.MaxDocumentBytes = 1 << 16
	options.InlineValueBytes = 4096
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	collection, _ := openBatchCollection(t, options)
	long := strings.Repeat("L", 2000)
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put("short", fmt.Appendf(nil, `{"status":%q}`, "s")); err != nil {
			return err
		}
		return b.Put("long", fmt.Appendf(nil, `{"status":%q}`, long))
	}); err != nil {
		t.Fatalf("batch with an uncertifiable indexed value: %v", err)
	}
	if collection.Len() != 2 {
		t.Fatalf("length = %d, want 2", collection.Len())
	}
	assertIndexRowCount(t, "short value", collection, "status", "s", 1)
	assertIndexRowCount(t, "long value", collection, "status", long, 1)
}
