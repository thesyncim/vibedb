package store

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math/bits"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/storekey"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

func TestStoreChunkVectorSparseTraversal(t *testing.T) {
	var vector storeChunkVector
	keep := map[uint32]bool{0: true, 31: true, 32: true, 1023: true, 1024: true, 1099: true}
	for range uint32(1100) {
		vector, _ = vector.append(&Chunk{})
	}
	for id := range uint32(1100) {
		if !keep[id] {
			vector = vector.set(id, nil)
		}
	}
	var got []uint32
	vector.Each(func(id uint32, _ *Chunk) bool {
		got = append(got, id)
		return true
	})
	want := []uint32{0, 31, 32, 1023, 1024, 1099}
	if !slices.Equal(got, want) {
		t.Fatalf("sparse traversal = %v, want %v", got, want)
	}
	for from := uint64(0); from < uint64(vector.Count); from++ {
		wantID, found := uint32(0), false
		for _, id := range want {
			if uint64(id) >= from {
				wantID, found = id, true
				break
			}
		}
		id, chunk, ok := vector.next(from)
		if ok != found || ok && (id != wantID || chunk == nil) {
			t.Fatalf("next(%d) = (%d,%p,%v), want id=%d found=%v", from, id, chunk, ok, wantID, found)
		}
	}
	if id, chunk, ok := vector.next(uint64(vector.Count)); ok || chunk != nil || id != 0 {
		t.Fatalf("terminal next = (%d,%p,%v), want zero miss", id, chunk, ok)
	}
}

func TestStoreCompiledKeyAcrossSnapshotsAndStores(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2, ShapeTapes: true}}
	for key, doc := range map[string]string{
		"":  `{"v":"empty"}`,
		"a": `{"v":"old"}`,
		"b": `{"v":"other"}`,
	} {
		if _, err := collection.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := collection.Snapshot()
	compiled := old.CompileKey("a")
	empty := old.CompileKey("")
	absent := old.CompileKey("later")
	if raw, ok := old.GetRawKey(compiled); !ok || string(raw.Bytes()) != `{"v":"old"}` {
		t.Fatalf("compiled old read = (%q,%v)", raw.Bytes(), ok)
	}
	if raw, ok := old.GetRawKey(empty); !ok || string(raw.Bytes()) != `{"v":"empty"}` {
		t.Fatalf("compiled empty-key read = (%q,%v)", raw.Bytes(), ok)
	}

	if _, err := collection.Put("a", []byte(`{"v":"new"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("a", []byte(`{"v":"moved"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("later", []byte(`{"v":"appeared"}`)); err != nil {
		t.Fatal(err)
	}
	current, _ := collection.Snapshot()
	if raw, ok := current.GetRawKey(compiled); !ok || string(raw.Bytes()) != `{"v":"moved"}` {
		t.Fatalf("compiled moved read = (%q,%v)", raw.Bytes(), ok)
	}
	if raw, ok := current.GetRawKey(absent); !ok || string(raw.Bytes()) != `{"v":"appeared"}` {
		t.Fatalf("compiled absent-then-present read = (%q,%v)", raw.Bytes(), ok)
	}
	if raw, ok := old.GetRawKey(compiled); !ok || string(raw.Bytes()) != `{"v":"old"}` {
		t.Fatalf("compiled retained-snapshot read = (%q,%v)", raw.Bytes(), ok)
	}

	other := &Collection{Options: Options{}}
	if _, err := other.Put("a", []byte(`{"v":"other-store"}`)); err != nil {
		t.Fatal(err)
	}
	if raw, ok := other.GetRawKey(compiled); !ok || string(raw.Bytes()) != `{"v":"other-store"}` {
		t.Fatalf("cross-collection fallback = (%q,%v)", raw.Bytes(), ok)
	}
}

func TestStoreCompiledKeySteadyAllocs(t *testing.T) {
	collection := &Collection{Options: Options{ShapeTapes: true}}
	if _, err := collection.Put("key", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := collection.Snapshot()
	key := snapshot.CompileKey("key")
	if allocs := testing.AllocsPerRun(1000, func() {
		if raw, ok := snapshot.GetRawKey(key); !ok || len(raw.Bytes()) == 0 {
			panic("compiled key miss")
		}
	}); allocs != 0 {
		t.Fatalf("Snapshot.GetRawKey allocated %.2f times, want 0", allocs)
	}
}

func TestStoreCompiledKeyMutationDifferential(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 7, ShapeTapes: true}}
	const keyCount = 96
	keys := make([]string, keyCount)
	compiled := make([]CompiledKey, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
		if i%3 != 0 {
			if _, err := collection.Put(keys[i], []byte(fmt.Sprintf(`{"step":0,"key":%d}`, i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := range keys {
		compiled[i] = collection.CompileKey(keys[i])
	}

	rng := rand.New(rand.NewSource(23))
	for step := 1; step <= 4000; step++ {
		i := rng.Intn(keyCount)
		doc := fmt.Sprintf(`{"step":%d,"key":%d}`, step, i)
		if _, err := collection.Put(keys[i], []byte(doc)); err != nil {
			t.Fatal(err)
		}
		if step%37 == 0 {
			j := rng.Intn(keyCount)
			compiled[j] = collection.CompileKey(keys[j])
		}
		if step%19 != 0 {
			continue
		}
		snapshot, _ := collection.Snapshot()
		for j := range keys {
			plain, plainOK := snapshot.GetRaw(keys[j])
			fast, fastOK := snapshot.GetRawKey(compiled[j])
			if fastOK != plainOK || fastOK && string(fast.Bytes()) != string(plain.Bytes()) {
				t.Fatalf("step %d key %q: GetRawKey=(%q,%v), GetRaw=(%q,%v)",
					step, keys[j], fast.Bytes(), fastOK, plain.Bytes(), plainOK)
			}
		}
	}
}

func TestStoreCoverageSparsePages(t *testing.T) {
	ids := []uint32{1, (1 << storeCoveragePageShift) - 1, 1 << 31, ^uint32(0)}
	var coverage storeCoverage
	for _, id := range ids {
		if !coverage.mark(id) || coverage.mark(id) {
			t.Fatalf("mark(%d) did not have set semantics", id)
		}
	}
	if len(coverage.pages) != 3 {
		t.Fatalf("sparse coverage pages = %d, want 3", len(coverage.pages))
	}
	for _, id := range ids {
		if !coverage.has(id) {
			t.Fatalf("coverage lost %d", id)
		}
	}
	for _, id := range ids {
		if !coverage.unmark(id) || coverage.unmark(id) {
			t.Fatalf("unmark(%d) did not have set semantics", id)
		}
	}
	if coverage.pages != nil {
		t.Fatalf("empty coverage retained %d pages", len(coverage.pages))
	}
}

func TestStoreMutationSnapshotDifferential(t *testing.T) {
	for _, options := range []Options{
		{ChunkDocuments: 1},
		{ChunkDocuments: 3, IndexOptions: document.IndexOptions{HashKeys: true}},
		{ChunkDocuments: 17, ShapeTapes: true, ValueDict: true, Postings: true},
		{ChunkDocuments: 64, ShapeTapes: true, IndexOptions: document.IndexOptions{HashKeys: true}},
	} {
		t.Run(fmt.Sprintf("chunk=%d/shape=%v/post=%v/dict=%v", options.ChunkDocuments, options.ShapeTapes, options.Postings, options.ValueDict), func(t *testing.T) {
			collection := &Collection{Options: options}
			want := make(map[string]string)
			rng := rand.New(rand.NewSource(7))
			var held []Snapshot
			for step := range 4000 {
				key := fmt.Sprintf("key-%03d", rng.Intn(300))
				if step%251 == 0 {
					before, _ := collection.Snapshot()
					held = append(held, before)
				}
				doc := fmt.Sprintf(`{"id":%d,"key":%q,"g":%d,"value":"v-%03d"}`, step, key, step%11, rng.Intn(80))
				created, err := collection.Put(key, []byte(doc))
				if err != nil {
					t.Fatal(err)
				}
				if created != (want[key] == "") {
					t.Fatalf("step %d Put(%q) created=%v, existed=%v", step, key, created, want[key] != "")
				}
				want[key] = doc
				if step%97 == 0 {
					snap51, _ := collection.Snapshot()
					checkCollectionSnapshot(t, snap51, want)
				}
			}
			snap50, _ := collection.Snapshot()
			checkCollectionSnapshot(t, snap50, want)
			for _, snapshot := range held {
				// Holding old versions while the writer churns is the lifetime
				// assertion; a traversal under checkptr/race must remain sound.
				count := 0
				snapshot.Range(func(_ string, value vibejson.RawValue) bool {
					if !vibejson.Valid(value.Bytes()) {
						t.Fatal("held snapshot contains invalid JSON")
					}
					count++
					return true
				})
				if count != snapshot.Len() {
					t.Fatalf("held snapshot Range count = %d, Len = %d", count, snapshot.Len())
				}
			}
		})
	}
}

func TestStoreMutationReusesOnlyLiveImmutableStorage(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 8, ShapeTapes: true}}
	for i := range 8 {
		doc := fmt.Sprintf(`{"id":%d,"group":1,"active":true,"name":"old"}`, i)
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}

	lookupSource := func(key string) []byte {
		state := collection.state.Load()
		loc, ok := storekey.Lookup(state.keys, maphash.String(state.seed, key), key)
		if !ok {
			t.Fatalf("missing key %q", key)
		}
		chunk := state.Chunks.Get(loc.Chunk)
		return chunk.Docs.RawAt(int(chunk.Ord[loc.Slot]))
	}
	before, _ := collection.Snapshot()
	beforeChunk := before.state.Chunks.Get(0)
	beforeSources := make([][]byte, 8)
	for i := range beforeSources {
		beforeSources[i] = lookupSource(fmt.Sprintf("k%d", i))
		ord := int(beforeChunk.Ord[i])
		ref := beforeChunk.Docs.ShapeTapeRefAt(ord)
		if ref.Rec == nil || !ref.Narrow {
			t.Fatalf("slot %d was not promoted to a narrow shape tape", i)
		}
		if cap(beforeChunk.Docs.docs[ord].Entries) != 0 {
			t.Fatalf("slot %d narrow tape retained %d unused classic entries", i, cap(beforeChunk.Docs.docs[ord].Entries))
		}
		if cap(ref.Rec.Fields) != len(ref.Rec.Fields) || cap(ref.Rec.table) != len(ref.Rec.table) {
			t.Fatalf("slot %d shape retained oversized fields/table: %d/%d and %d/%d",
				i, len(ref.Rec.Fields), cap(ref.Rec.Fields), len(ref.Rec.table), cap(ref.Rec.table))
		}
	}
	// The ingest cache is released at seal; dedup is proven through the
	// rows instead: eight documents over one layout share one record.
	seen := make(map[*ShapeRecord]struct{})
	for i := range beforeSources {
		if rec := beforeChunk.Docs.ShapeTapeRefAt(int(beforeChunk.Ord[i])).Rec; rec != nil {
			seen[rec] = struct{}{}
		}
	}
	if len(seen) != 1 {
		t.Fatalf("distinct shape records = %d, want 1", len(seen))
	}

	replacement := []byte(`{"id":3,"group":9,"active":false,"name":"new"}`)
	if created, err := collection.Put("k3", replacement); err != nil || created {
		t.Fatalf("Put update = (%v,%v), want (false,nil)", created, err)
	}
	replacement[7] = '8'
	after, _ := collection.Snapshot()
	afterChunk := after.state.Chunks.Get(0)
	for i := range 8 {
		current := lookupSource(fmt.Sprintf("k%d", i))
		if i == 3 {
			if &current[0] == &beforeSources[i][0] {
				t.Fatal("replacement reused the old source")
			}
			continue
		}
		if &current[0] != &beforeSources[i][0] {
			t.Fatalf("unchanged slot %d copied its source", i)
		}
	}
	if &afterChunk.Docs.narrow[0] == &beforeChunk.Docs.narrow[0] {
		t.Fatal("new chunk reused the published narrow-value slab")
	}
	if raw, ok := before.GetRaw("k3"); !ok || string(raw.Bytes()) != `{"id":3,"group":1,"active":true,"name":"old"}` {
		t.Fatalf("old snapshot changed after update: %q, %v", raw.Bytes(), ok)
	}
	if raw, ok := after.GetRaw("k3"); !ok || string(raw.Bytes()) != `{"id":3,"group":9,"active":false,"name":"new"}` {
		t.Fatalf("new snapshot aliases caller input: %q, %v", raw.Bytes(), ok)
	}

	// Replace the complete live A layout with B. The final cache must contain
	// only B: sharing live immutable records cannot turn into shape history.
	churn := &Collection{Options: Options{ChunkDocuments: 3, ShapeTapes: true}}
	for i := range 3 {
		_, _ = churn.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"a":%d,"x":true}`, i)))
	}
	oldRec := churn.state.Load().Chunks.Get(0).Docs.ShapeTapeRefAt(0).Rec
	if oldRec == nil {
		t.Fatal("layout A row carries no shape record")
	}
	for i := range 3 {
		if _, err := churn.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"b":%d,"y":false}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	// The ingest cache is released at seal, so the no-history invariant is
	// proven through the live rows: exactly one layout B record, never A's.
	afterChurn := churn.state.Load().Chunks.Get(0)
	current := make(map[*ShapeRecord]struct{})
	for i := 0; i < afterChurn.Docs.Len(); i++ {
		if rec := afterChurn.Docs.ShapeTapeRefAt(i).Rec; rec != nil {
			current[rec] = struct{}{}
		}
	}
	if len(current) != 1 {
		t.Fatalf("live rows reference %d distinct shape records, want 1", len(current))
	}
	for rec := range current {
		if rec == oldRec {
			t.Fatalf("live rows still reference obsolete layout A record %p", oldRec)
		}
	}
}

func checkCollectionSnapshot(t testing.TB, snapshot Snapshot, want map[string]string) {
	t.Helper()
	if snapshot.Len() != len(want) {
		t.Fatalf("Snapshot.Len = %d, want %d", snapshot.Len(), len(want))
	}
	seen := make(map[string]bool, len(want))
	snapshot.Range(func(key string, value vibejson.RawValue) bool {
		if seen[key] {
			t.Fatalf("Range repeated %q", key)
		}
		seen[key] = true
		if string(value.Bytes()) != want[key] {
			t.Fatalf("Range(%q) = %s, want %s", key, value.Bytes(), want[key])
		}
		return true
	})
	for key, doc := range want {
		raw, ok := snapshot.GetRaw(key)
		if !ok || string(raw.Bytes()) != doc {
			t.Fatalf("GetRaw(%q) = (%s,%v), want (%s,true)", key, raw.Bytes(), ok, doc)
		}
		index, ok := snapshot.Get(key)
		if !ok || string(index.Root().Raw().Bytes()) != doc {
			t.Fatalf("Get(%q) mismatch", key)
		}
	}
}

func TestStoreInvalidPutRollback(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 4, ShapeTapes: true, Postings: true}}
	if _, err := collection.Put("bad", []byte(`{"x":`)); err == nil {
		t.Fatal("invalid Put succeeded")
	}
	if collection.Len() != 0 || collection.Generation() != 0 {
		t.Fatalf("failed Put changed visible state: Len=%d Generation=%d", collection.Len(), collection.Generation())
	}
	for i := range 100 {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	// A rolled-back Put leaves the collection able to keep inserting, and a
	// snapshot taken now keeps its exact 100-row view while the writer churns on.
	old, _ := collection.Snapshot()
	for i := range 100 {
		if _, err := collection.Put(fmt.Sprintf("r%d", i), []byte(fmt.Sprintf(`{"n":%d}`, -i))); err != nil {
			t.Fatal(err)
		}
	}
	if old.Len() != 100 {
		t.Fatalf("old snapshot Len = %d, want 100", old.Len())
	}
	if collection.Len() != 200 {
		t.Fatalf("Len after churn = %d, want 200", collection.Len())
	}
}

func TestStoreAddressSpaceOverflow(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 1}}
	collection.mu.Lock()
	state, err := collection.initLocked()
	if err != nil {
		t.Fatal(err)
	}
	next := *state
	next.Chunks.Count = ^uint32(0)
	collection.state.Store(&next)
	collection.mu.Unlock()
	if _, err := collection.Put("overflow", []byte(`{"v":1}`)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Put overflow error = %v, want %v", err, ErrTooLarge)
	}
}

func TestStoreSnapshotReadSteadyAllocs(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 8, ShapeTapes: true}}
	for i := range 32 {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"n":%d,"s":"x"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, _ := collection.Snapshot()
	if _, ok := snapshot.GetRaw("k17"); !ok {
		t.Fatal("warm GetRaw miss")
	}
	allocs := testing.AllocsPerRun(100, func() {
		if raw, ok := snapshot.GetRaw("k17"); !ok || len(raw.Bytes()) == 0 {
			panic("GetRaw miss")
		}
	})
	if allocs != 0 {
		t.Fatalf("Snapshot.GetRaw allocated %.2f times, want 0", allocs)
	}
	allocs = testing.AllocsPerRun(100, func() {
		count := 0
		snapshot.Range(func(_ string, _ vibejson.RawValue) bool {
			count++
			return true
		})
		if count != snapshot.Len() {
			panic("Range count")
		}
	})
	if allocs != 0 {
		t.Fatalf("Snapshot.Range allocated %.2f times, want 0", allocs)
	}

	var stats Stats
	allocs = testing.AllocsPerRun(100, func() {
		stats = collection.Stats()
	})
	if allocs != 0 || uint64(stats.Keys) != collection.Len() {
		t.Fatalf("Collection.Stats = (%+v, %.2f allocs), want current zero-allocation counters", stats, allocs)
	}
}

func TestStoreConcurrentSnapshots(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 8, ShapeTapes: true, ValueDict: true, Postings: true}}
	for i := range 64 {
		_, _ = collection.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"n":%d,"s":"same"}`, i)))
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 1000 {
				snapshot, _ := collection.Snapshot()
				if raw, ok := snapshot.GetRaw(fmt.Sprintf("k%d", i%64)); ok && !vibejson.Valid(raw.Bytes()) {
					t.Error("reader observed invalid JSON")
				}
			}
		})
	}
	for i := range 1000 {
		key := fmt.Sprintf("k%d", i%64)
		if _, err := collection.Put(key, []byte(fmt.Sprintf(`{"n":%d,"s":"same"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestStoreBornWithPostingsProbes(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 4, ShapeTapes: true, Postings: true}}
	for i := range 14 {
		_, _ = collection.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"g":%d,"v":%d}`, i%3, i)))
	}
	needle := refIndex(t, `1`)
	wantContains := []string{"k1", "k4", "k7", "k10", "k13"}
	prefix := []string{"keep"}
	if got, err := collection.AppendWhereContainsKeys(prefix, "g", []byte(`{"bad":`)); err == nil || !slices.Equal(got, prefix) {
		t.Fatalf("invalid contains = (%v,%v), want unchanged prefix and error", got, err)
	}
	snap, _ := collection.Snapshot()
	// A collection born with postings carries the physical index on every chunk,
	// so the containment probe is served from postings rather than an exact scan.
	snap.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		if !chunk.Docs.Postings {
			t.Fatal("born-with-postings collection left an uncovered chunk")
		}
		return true
	})
	contains := make([]string, 0, snap.Len())
	contains = snap.AppendWhereContainsIndexKeys(contains, "g", needle)
	if !slices.Equal(contains, wantContains) {
		t.Fatalf("postings contains = %v, want %v", contains, wantContains)
	}
	keys := make([]string, 0, snap.Len())
	keys = snap.AppendWhereExistsKeys(keys, "v")
	if len(keys) != snap.Len() {
		t.Fatalf("exists keys = %d, want %d", len(keys), snap.Len())
	}
}

func TestStoreIndexedSnapshotProbeSteadyAllocs(t *testing.T) {
	seed := func(options Options) Snapshot {
		collection := &Collection{Options: options}
		for i := range 32 {
			if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf(`{"g":%d,"v":%d}`, i%3, i))); err != nil {
				t.Fatal(err)
			}
		}
		snap, _ := collection.Snapshot()
		return snap
	}
	needle := refIndex(t, `1`)
	// The exact-scan fallback and the postings-served path must both probe with
	// zero steady-state allocation. The scan snapshot carries no postings; the
	// indexed snapshot is born with them on every chunk.
	for _, test := range []struct {
		name     string
		snapshot Snapshot
	}{
		{"scan", seed(Options{ChunkDocuments: 4, ShapeTapes: true})},
		{"indexed", seed(Options{ChunkDocuments: 4, ShapeTapes: true, Postings: true})},
	} {
		t.Run(test.name, func(t *testing.T) {
			exists := make([]string, 0, test.snapshot.Len())
			contains := make([]string, 0, test.snapshot.Len())
			exists = test.snapshot.AppendWhereExistsKeys(exists, "v")
			contains = test.snapshot.AppendWhereContainsIndexKeys(contains, "g", needle)
			if len(exists) != test.snapshot.Len() || len(contains) == 0 {
				t.Fatalf("warm probes returned exists=%d contains=%d", len(exists), len(contains))
			}
			if allocs := testing.AllocsPerRun(100, func() {
				exists = test.snapshot.AppendWhereExistsKeys(exists[:0], "v")
			}); allocs != 0 {
				t.Fatalf("AppendWhereExistsKeys allocated %.2f times, want 0", allocs)
			}
			if allocs := testing.AllocsPerRun(100, func() {
				contains = test.snapshot.AppendWhereContainsIndexKeys(contains[:0], "g", needle)
			}); allocs != 0 {
				t.Fatalf("AppendWhereContainsIndexKeys allocated %.2f times, want 0", allocs)
			}
		})
	}
}

func TestStoreOwnershipOptionsAndEmptyKey(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2, ShapeTapes: true}}
	src := []byte(`{"v":"owned"}`)
	created, err := collection.Put("", src)
	if err != nil || !created {
		t.Fatalf("Put empty key = (%v,%v), want (true,nil)", created, err)
	}
	for i := range src {
		src[i] = 'x'
	}
	raw, ok := collection.GetRaw("")
	if !ok || string(raw.Bytes()) != `{"v":"owned"}` {
		t.Fatalf("collection retained caller source: (%s,%v)", raw.Bytes(), ok)
	}

	// Options are a construction-time policy. Mutating the public field after
	// initialization must not change the representation of later chunks.
	collection.Options.ChunkDocuments = 64
	for i := range 4 {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(`{"v":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	stats := collection.Stats()
	if stats.ChunkDocuments != 2 || stats.Chunks != 3 {
		t.Fatalf("frozen options stats = %+v, want chunk size 2 and 3 chunks", stats)
	}

	invalid := &Collection{Options: Options{ChunkDocuments: MaxChunkDocuments + 1}}
	if _, err := invalid.Put("k", []byte(`null`)); err == nil {
		t.Fatal("Put accepted invalid Options")
	}
	if _, err := invalid.CreateIndex(IndexDefinition{Name: "i", Paths: []string{"/v"}}); err == nil {
		t.Fatal("CreateIndex accepted invalid Options")
	}
	if invalid.Len() != 0 || invalid.Generation() != 0 {
		t.Fatalf("invalid options published state: Len=%d Generation=%d", invalid.Len(), invalid.Generation())
	}
}

// A published chunk is immutable and every edit rebuilds it by copy, so the
// working state ingest leaves behind — buildDoc's spill build buffer, the
// arena headroom a spilled document's replacement tape would carry, and the
// value dictionary's first-sighting gate — has no consumer once the chunk is
// live and would otherwise stay pinned for the whole generation's lifetime.
// The release is a space property only, so every assertion here is paired with
// a differential over the exact bytes written.
func TestStoreChunkRetainsNoIngestScratch(t *testing.T) {
	// Seventeen entries — a root plus eight key/value pairs — exceed the
	// sixteen-entry first entry arena initChunkSegment gives a collection chunk, so
	// each document is guaranteed to take buildDoc's spill path rather than
	// building in the arena tail. The repeated eighteen-byte string span clears
	// the dictionary's length floor and recurs, so the sighting gate is
	// guaranteed to be populated before the chunk is sealed.
	const label = `"aaaaaaaaaaaaaaaa"`
	const tapeEntries = 17
	doc := func(i int) string {
		return fmt.Sprintf(
			`{"id":%d,"tag":%s,"dup":%s,"a":1,"b":2,"c":3,"d":4,"e":5}`,
			i, label, label,
		)
	}
	checkSealed := func(t *testing.T, collection *Collection, want map[string]string) {
		t.Helper()
		state := collection.state.Load()
		for id := uint32(0); id < state.Chunks.Count; id++ {
			chunk := state.Chunks.Get(id)
			if chunk == nil {
				continue
			}
			if chunk.Docs.scratch != nil {
				t.Fatalf("chunk %d retained a %d-entry spill build buffer",
					id, cap(chunk.Docs.scratch))
			}
			if chunk.Docs.valueSeen != nil {
				t.Fatalf("chunk %d retained a %d-hash first-sighting gate",
					id, len(chunk.Docs.valueSeen))
			}
			if chunk.Docs.values.Len() == 0 {
				t.Fatalf("chunk %d interned no value: the sighting gate the "+
					"test means to observe was never populated", id)
			}
			// No document here is stored as a template, so nothing may be
			// appended to the entry arena after the one replacement is parsed:
			// a chunk holding one is holding entries no tape aliases.
			if cap(chunk.Docs.entryChunk) != 0 {
				t.Fatalf("chunk %d retained a %d-entry entry arena holding %d "+
					"committed entries", id,
					cap(chunk.Docs.entryChunk), len(chunk.Docs.entryChunk))
			}
			for left := chunk.Live; left != 0; left &= left - 1 {
				ord := int(chunk.Ord[bits.TrailingZeros64(left)])
				entries := chunk.Docs.docs[ord].Entries
				if len(entries) != tapeEntries {
					t.Fatalf("chunk %d ordinal %d tape = %d entries, want %d",
						id, ord, len(entries), tapeEntries)
				}
				if cap(entries) != len(entries) {
					t.Fatalf("chunk %d ordinal %d retained %d unwritten tape "+
						"entries", id, ord, cap(entries)-len(entries))
				}
			}
		}
		snapshot, _ := collection.Snapshot()
		checkCollectionSnapshot(t, snapshot, want)
	}

	schema, err := CompileSchema(SchemaDefinition{
		Root:   SchemaObject,
		Fields: []SchemaField{{Path: "/id", Types: SchemaInteger, Required: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Both build paths, because buildDocSchema specializes buildDoc rather than
	// branching inside it and so owns a second copy of the spill tail.
	for _, variant := range []struct {
		name    string
		options Options
	}{
		{"schemaless", Options{ChunkDocuments: 4, ValueDict: true}},
		{"schema", Options{ChunkDocuments: 4, ValueDict: true, Schema: schema}},
	} {
		t.Run(variant.name, func(t *testing.T) {
			collection := &Collection{Options: variant.options}
			want := make(map[string]string, 10)
			for i := range 10 {
				key := fmt.Sprintf("k%d", i)
				if _, err := collection.Put(key, []byte(doc(i))); err != nil {
					t.Fatal(err)
				}
				want[key] = doc(i)
			}
			checkSealed(t, collection, want)

			// A replacement is a rebuild that parses the one new document; the
			// surviving rows it carries in by reference must keep the exact tapes
			// they were given, and the published chunk must still be sealed.
			if created, err := collection.Put("k3", []byte(doc(300))); err != nil || created {
				t.Fatalf("Put update = (%v,%v), want (false,nil)", created, err)
			}
			want["k3"] = doc(300)
			checkSealed(t, collection, want)
		})
	}

	// A Builder page shares initChunkSegment but parses every one of its
	// rows, so it keeps the arena growth policy and seals at flush instead —
	// the point past which the page takes no further row. Its documents are
	// compacted into pointer-free owned blocks by Build, so only the ingest
	// state is observable here.
	t.Run("builder", func(t *testing.T) {
		builder, err := NewBuilder(Options{ChunkDocuments: 4, ValueDict: true})
		if err != nil {
			t.Fatal(err)
		}
		want := make(map[string]string, 10)
		for i := range 10 {
			key := fmt.Sprintf("k%d", i)
			if err := builder.Append(key, []byte(doc(i))); err != nil {
				t.Fatal(err)
			}
			want[key] = doc(i)
		}
		collection, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		state := collection.state.Load()
		for id := uint32(0); id < state.Chunks.Count; id++ {
			chunk := state.Chunks.Get(id)
			if chunk == nil {
				continue
			}
			if chunk.Docs.scratch != nil {
				t.Fatalf("page %d retained a %d-entry spill build buffer",
					id, cap(chunk.Docs.scratch))
			}
			if chunk.Docs.valueSeen != nil {
				t.Fatalf("page %d retained a %d-hash first-sighting gate",
					id, len(chunk.Docs.valueSeen))
			}
			if chunk.Docs.values.Len() == 0 {
				t.Fatalf("page %d interned no value: the sighting gate the "+
					"test means to observe was never populated", id)
			}
		}
		snapshot, _ := collection.Snapshot()
		checkCollectionSnapshot(t, snapshot, want)
	})
}
