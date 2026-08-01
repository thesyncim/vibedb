package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

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

// TestCollectionUpdateMaintainsIndexedPrimaryAtomically pins the public batch
// contract for indexed collections: primary rows and exact postings move in one
// publication, while a snapshot retained before the batch keeps the old pair.
func TestCollectionUpdateMaintainsIndexedPrimaryAtomically(t *testing.T) {
	options := testBatchOptions(24)
	options.Indexes = []store.IndexDefinition{
		{Name: "status", Paths: []string{"/status"}},
		{Name: "pair", Paths: []string{"/status", "/kind"}},
	}
	collection, _ := openBatchCollection(t, options)
	if _, err := collection.Put([]byte("seed"), []byte(`{"status":"active","kind":"a"}`)); err != nil {
		t.Fatal(err)
	}
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte("k"), []byte(`{"status":"idle","kind":"b"}`)); err != nil {
			return err
		}
		if err := b.Put([]byte("other"), []byte(`{"status":"idle","kind":"c"}`)); err != nil {
			return err
		}
		return b.Delete([]byte("seed"))
	}); err != nil {
		t.Fatalf("indexed Update: %v", err)
	}
	idle := primaryExactTestNeedle(t, `"idle"`)
	active := primaryExactTestNeedle(t, `"active"`)
	if got := primaryExactTestKeys(t, collection, "status", idle); len(got) != 2 ||
		got[0] != "k" || got[1] != "other" {
		t.Fatalf("live idle postings = %v", got)
	}
	if got := primaryExactTestKeys(t, collection, "status", active); len(got) != 0 {
		t.Fatalf("live active postings = %v", got)
	}
	if got := primaryExactSnapshotKeys(t, before, "status", active); len(got) != 1 ||
		got[0] != "seed" {
		t.Fatalf("old active postings = %v", got)
	}
}

func assertCollectionsAgree(t *testing.T, label string, want, got *Collection, keys int) {
	t.Helper()
	if want.Len() != got.Len() {
		t.Fatalf("%s: length = %d, want %d", label, got.Len(), want.Len())
	}
	for i := range keys {
		key := fmt.Sprintf("key-%03d", i)
		wantRaw, wantOK, err := want.AppendRaw(nil, []byte(key))
		if err != nil {
			t.Fatalf("%s: sequential AppendRaw(%s): %v", label, key, err)
		}
		gotRaw, gotOK, err := got.AppendRaw(nil, []byte(key))
		if err != nil {
			t.Fatalf("%s: batched AppendRaw(%s): %v", label, key, err)
		}
		if wantOK != gotOK || string(wantRaw) != string(gotRaw) {
			t.Fatalf("%s: %s = (%q,%v), want (%q,%v)", label, key, gotRaw, gotOK, wantRaw, wantOK)
		}
	}
}

// TestCollectionUpdatePublishesOneDurabilityFencePerBatch is the whole point of
// the API: a batch of N documents must cost one publication and one durability
// fence, not one per document. A regression here is invisible to every
// correctness test.
//
// Generation() delta is the wrong observable for this on the ordered primary:
// the batch stamps its published root at the highest generation any single leaf
// folded to, so when many mutations crowd one leaf the raw generation number
// advances by more than one even though the whole group is published atomically.
// The right observable is the durability fence itself — one journal
// acknowledgement covers the whole batch where the identical sequential
// mutations take one each.
func TestCollectionUpdatePublishesOneDurabilityFencePerBatch(t *testing.T) {
	const n = 32
	document := func(i int) []byte { return fmt.Appendf(nil, `{"i":%d}`, i) }

	sequential, _ := openBatchCollection(t, testBatchOptions(n))
	seqBefore := sequential.Stats().JournalAcks
	for i := range n {
		if _, err := sequential.Put([]byte(fmt.Sprintf("key-%03d", i)), document(i)); err != nil {
			t.Fatal(err)
		}
	}
	if acks := sequential.Stats().JournalAcks - seqBefore; acks != n {
		t.Fatalf("sequential mutations took %d journal acknowledgements, want %d", acks, n)
	}

	batched, _ := openBatchCollection(t, testBatchOptions(n))
	batchBefore := batched.Stats().JournalAcks
	beforeGen := batched.Generation()
	if err := batched.Update(func(b *WriteBatch) error {
		for i := range n {
			if err := b.Put([]byte(fmt.Sprintf("key-%03d", i)), document(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if acks := batched.Stats().JournalAcks - batchBefore; acks != 1 {
		t.Fatalf("batch took %d journal acknowledgements, want 1", acks)
	}
	if batched.Generation() <= beforeGen {
		t.Fatalf("batch did not publish a new generation: %d -> %d",
			beforeGen, batched.Generation())
	}
	if batched.Len() != n {
		t.Fatalf("length = %d, want %d", batched.Len(), n)
	}
	assertCollectionsAgree(t, "one-fence batch", sequential, batched, n)
}

// TestCollectionUpdateRollsBackOnError proves the batch is failure-atomic at
// its own boundary: a closure that fails, and a document the applier rejects,
// must both leave the published generation untouched.
func TestCollectionUpdateRollsBackOnError(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(16))
	if _, err := collection.Put([]byte("seed"), []byte(`{"seed":true}`)); err != nil {
		t.Fatal(err)
	}
	generation := collection.Generation()
	sentinel := errors.New("closure failed")
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte("added"), []byte(`{"a":1}`)); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("closure error = %v, want %v", err, sentinel)
	}
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte("added"), []byte(`{"a":1}`)); err != nil {
			return err
		}
		return b.Put([]byte("broken"), []byte(`{"a":`))
	}); err == nil {
		t.Fatal("malformed document accepted")
	}
	if collection.Generation() != generation {
		t.Fatalf("generation = %d, want %d", collection.Generation(), generation)
	}
	if collection.Len() != 1 {
		t.Fatalf("length = %d, want 1", collection.Len())
	}
	if _, ok, err := collection.AppendRaw(nil, []byte("added")); err != nil || ok {
		t.Fatalf("aborted batch published a row: (%v,%v)", ok, err)
	}
	// The collection must still be writable: an aborted transaction that leaked
	// its reservation or its retirement claim would fail the next commit.
	if _, err := collection.Put([]byte("after"), []byte(`{"a":2}`)); err != nil {
		t.Fatalf("write after aborted batch: %v", err)
	}
}

// TestCollectionUpdateDeduplicatesRepeatedKeys pins the last-write-wins
// contract. Two rows for one key inside a single leaf rewrite would corrupt the
// page, so the deduplication is a correctness requirement, not a convenience.
func TestCollectionUpdateDeduplicatesRepeatedKeys(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(16))
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte("k"), []byte(`{"v":1}`)); err != nil {
			return err
		}
		if err := b.Put([]byte("k"), []byte(`{"v":2}`)); err != nil {
			return err
		}
		if err := b.Put([]byte("gone"), []byte(`{"v":3}`)); err != nil {
			return err
		}
		return b.Delete([]byte("gone"))
	}); err != nil {
		t.Fatal(err)
	}
	if collection.Len() != 1 {
		t.Fatalf("length = %d, want 1", collection.Len())
	}
	raw, ok, err := collection.AppendRaw(nil, []byte("k"))
	if err != nil || !ok || string(raw) != `{"v":2}` {
		t.Fatalf("k = (%q,%v,%v), want the second document", raw, ok, err)
	}
	if _, ok, err := collection.AppendRaw(nil, []byte("gone")); err != nil || ok {
		t.Fatalf("gone = (%v,%v), want absent", ok, err)
	}
	// The empty key is out of bounds on the ordered primary for the batch and the
	// single-document path alike (both fail ErrKeyTooLarge), so the two APIs agree
	// by refusing it rather than one accepting what the other rejects; the refusal
	// rejects the whole batch and publishes nothing new.
	generation := collection.Generation()
	if err := collection.Update(func(b *WriteBatch) error {
		return b.Put([]byte(""), []byte(`{"v":4}`))
	}); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("empty-key batch = %v, want ErrKeyTooLarge", err)
	}
	if collection.Generation() != generation {
		t.Fatalf("rejected empty-key batch published generation %d, want %d",
			collection.Generation(), generation)
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
			if err := b.Put([]byte(fmt.Sprintf("key-%d", i)), fmt.Appendf(nil, `{"i":%d}`, i)); err != nil {
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
		if err := b.Put([]byte("same"), large); err != nil {
			return err
		}
		for i := range 10_000 {
			if i&1 == 0 {
				if err := b.Delete([]byte("same")); err != nil {
					return err
				}
			} else if err := b.Put([]byte("same"), []byte(`{"final":false}`)); err != nil {
				return err
			}
			arenaCapacity = max(arenaCapacity, cap(b.values))
		}
		return b.Put([]byte("same"), []byte(`{"final":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	if arenaCapacity > 2048 {
		t.Fatalf("repeated-key value arena capacity = %d, want bounded near largest value", arenaCapacity)
	}
	got, ok, err := collection.AppendRaw(nil, []byte("same"))
	if err != nil || !ok || string(got) != `{"final":true}` {
		t.Fatalf("final repeated value = (%q,%v,%v)", got, ok, err)
	}

	generation := collection.Generation()
	err = collection.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte("first"), large); err != nil {
			return err
		}
		return b.Put([]byte("second"), large)
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
		return b.Put([]byte("k"), []byte(`{"v":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := retained.Put([]byte("late"), []byte(`{"v":2}`)); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("retained batch Put = %v, want ErrBatchClosed", err)
	}
	if err := retained.Delete([]byte("k")); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("retained batch Delete = %v, want ErrBatchClosed", err)
	}
	if collection.Len() != 1 {
		t.Fatalf("length = %d, want 1", collection.Len())
	}
}

// TestCollectionUpdateSurvivesReopen checks that a batched generation is a
// complete durable state and not merely a correct in-memory one.
func TestCollectionUpdateSurvivesReopen(t *testing.T) {
	// This broad row-shape recovery cover runs without indexes; indexed batch
	// reopen and crash recovery are pinned by the exact batch and journal suites.
	options := testBatchOptions(40)
	file, err := os.CreateTemp(t.TempDir(), "file-store-batch-reopen-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	// One Update seeds all forty keys into the single root leaf, so the corpus is
	// sized to fold within one leaf: a batch cannot split a fresh leaf mid-fold,
	// which bounds the bytes one Update may land on it.
	if err := collection.Update(func(b *WriteBatch) error {
		for i := range 40 {
			if err := b.Put([]byte(fmt.Sprintf("key-%03d", i)), fmt.Appendf(nil,
				`{"i":%d,"status":%q,"pad":%q}`, i, []string{"a", "b"}[i%2],
				strings.Repeat("q", i*3))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := collection.Update(func(b *WriteBatch) error {
		for i := 0; i < 40; i += 3 {
			if err := b.Delete([]byte(fmt.Sprintf("key-%03d", i))); err != nil {
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
		raw, ok, rawErr := collection.AppendRaw(nil, []byte(key))
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
		raw, ok, rawErr := reopened.AppendRaw(nil, []byte(key))
		if rawErr != nil || !ok || string(raw) != value {
			t.Fatalf("reopened %s = (%q,%v,%v), want %q", key, raw, ok, rawErr, value)
		}
	}
	for i := 0; i < 40; i += 3 {
		key := fmt.Sprintf("key-%03d", i)
		if _, ok, rawErr := reopened.AppendRaw(nil, []byte(key)); rawErr != nil || ok {
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
// claim. A batch folds every mutation that lands in a leaf into one rewrite of
// that leaf, so checkpointing a batch of N documents that crowd a few leaves
// reaches the device with far fewer bytes than checkpointing the same N
// mutations one at a time — one leaf rebuild and one root descent replace many.
// The comparison is against per-mutation checkpointing on purpose: the deferred
// lane already coalesces a run of plain Puts into a single checkpoint, so it is
// the naive one-fence-per-document baseline the batch has to beat.
func TestCollectionUpdateWritesFewerDeviceBytesThanPuts(t *testing.T) {
	const n = 64
	document := func(i int) []byte {
		return fmt.Appendf(nil, `{"i":%d,"tag":%q}`, i, fmt.Sprintf("s%02d", i%7))
	}

	sequential, _ := openBatchCollection(t, testBatchOptions(n))
	base := sequential.Stats()
	for i := range n {
		if _, err := sequential.Put([]byte(fmt.Sprintf("key-%03d", i)), document(i)); err != nil {
			t.Fatal(err)
		}
		// Checkpoint each mutation so its leaf rewrite reaches the device on its own.
		if err := sequential.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	sequentialBytes := sequential.Stats().DeviceBytes - base.DeviceBytes

	batched, _ := openBatchCollection(t, testBatchOptions(n))
	base = batched.Stats()
	if err := batched.Update(func(b *WriteBatch) error {
		for i := range n {
			if err := b.Put([]byte(fmt.Sprintf("key-%03d", i)), document(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := batched.Flush(); err != nil {
		t.Fatal(err)
	}
	batchedBytes := batched.Stats().DeviceBytes - base.DeviceBytes
	if batchedBytes*4 >= sequentialBytes {
		t.Fatalf("batched wrote %d device bytes, per-mutation sequential wrote %d",
			batchedBytes, sequentialBytes)
	}
	assertCollectionsAgree(t, "device bytes", sequential, batched, n)
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
	if _, err := collection.Put([]byte("present"), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	generation := collection.Generation()
	stats := collection.Stats()
	if err := collection.Update(func(b *WriteBatch) error {
		for i := range 4 {
			if err := b.Delete([]byte(fmt.Sprintf("absent-%d", i))); err != nil {
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
	if _, err := collection.Put([]byte("after"), []byte(`{"v":2}`)); err != nil {
		t.Fatalf("write after no-op batch: %v", err)
	}
}

// TestCollectionUpdateLargeDocuments covers the batch path's document-size
// boundary now that overflow-on-Put exists. A batch commits large inline
// documents that nearly fill the inline leaf budget, atomically, and reads them
// back across a reopen. A document past that budget is the single-document
// overflow path's domain — the batch does not yet stage overflow chains, so it
// refuses an over-budget value whole with ErrDocumentTooLarge rather than
// truncating it or splitting the group, and the refusal publishes nothing.
func TestCollectionUpdateLargeDocuments(t *testing.T) {
	options := testBatchOptions(8)
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 64 << 10
	file, err := os.CreateTemp(t.TempDir(), "file-store-batch-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	// Two large inline documents that each fill most of the inline budget, plus a
	// small one — sized so the three fold within the single fresh root leaf.
	want := map[string]string{
		"big-a": fmt.Sprintf(`{"kind":"a","pad":%q}`, strings.Repeat("A", 1800)),
		"big-b": fmt.Sprintf(`{"kind":"b","pad":%q}`, strings.Repeat("B", 1850)),
		"small": `{"kind":"small"}`,
	}
	if err := collection.Update(func(b *WriteBatch) error {
		for _, key := range []string{"big-a", "big-b", "small"} {
			if err := b.Put([]byte(key), []byte(want[key])); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("large-inline batch: %v", err)
	}
	if collection.Len() != uint64(len(want)) {
		t.Fatalf("length = %d, want %d", collection.Len(), len(want))
	}

	// A document past the inline budget is refused whole, publishing nothing.
	generation := collection.Generation()
	overflow := fmt.Appendf(nil, `{"pad":%q}`, strings.Repeat("O", options.InlineValueBytes))
	if err := collection.Update(func(b *WriteBatch) error {
		if err := b.Put([]byte("ok"), []byte(`{"v":1}`)); err != nil {
			return err
		}
		return b.Put([]byte("too-big"), overflow)
	}); !errors.Is(err, ErrDocumentTooLarge) {
		t.Fatalf("over-budget batch = %v, want ErrDocumentTooLarge", err)
	}
	if collection.Generation() != generation {
		t.Fatalf("refused over-budget batch published generation %d, want %d",
			collection.Generation(), generation)
	}
	if _, ok, _ := collection.AppendRaw(nil, []byte("ok")); ok {
		t.Fatal("refused over-budget batch made its sibling visible")
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Len() != uint64(len(want)) {
		t.Fatalf("reopened length = %d, want %d", reopened.Len(), len(want))
	}
	for key, value := range want {
		got, ok, err := reopened.AppendRaw(nil, []byte(key))
		if err != nil || !ok || string(got) != value {
			t.Fatalf("reopened %s = (%q,%v,%v), want %q", key, got, ok, err, value)
		}
	}
}
