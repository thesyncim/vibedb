package store

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/thesyncim/vibejson"
)

func testScalarIndex(t testing.TB, src string) vibejson.Index {
	t.Helper()
	entries, err := vibejson.RequiredIndexEntries([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	index, err := vibejson.BuildIndex([]byte(src), make([]vibejson.IndexEntry, entries))
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func TestStoreExactCompoundIndexLifecycle(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2, ShapeTapes: true}}
	for _, row := range []struct{ key, doc string }{
		{"a", `{"tenant":"acme","status":"active","n":1}`},
		{"b", `{"tenant":"acme","status":"idle","n":1.0}`},
		{"c", `{"tenant":"other","status":"active","n":2}`},
	} {
		key, doc := row.key, row.doc
		if _, err := collection.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	def := IndexDefinition{Name: "tenant_status", Paths: []string{"/tenant", "/status"}}
	info, err := collection.CreateIndex(def)
	if err != nil || info.State != IndexBuilding || info.ColumnCount != 2 {
		t.Fatalf("CreateIndex = (%+v,%v)", info, err)
	}
	// Caller mutation cannot alter the compiled definition or published info.
	def.Paths[0] = "/wrong"
	snap23, _ := collection.Snapshot()
	infos := snap23.AppendIndexes(nil)
	if len(infos) != 1 || infos[0].Columns[0] != "/tenant" || infos[0].Columns[1] != "/status" {
		t.Fatalf("published definition = %+v", infos)
	}

	// Building is an operational state, never a correctness precondition.
	got, err := collection.IndexRawKeys("tenant_status", []byte(`"acme"`), []byte(`"active"`))
	if err != nil || !slices.Equal(got, []string{"a"}) {
		t.Fatalf("building lookup = (%v,%v)", got, err)
	}
	info, err = collection.BackfillIndex("tenant_status", 1)
	if err != nil || info.CoveredChunks != 1 || info.State != IndexBuilding {
		t.Fatalf("partial backfill = (%+v,%v)", info, err)
	}
	info, err = collection.BackfillIndex("tenant_status", 0)
	if err != nil || info.State != IndexReady || info.CoveredChunks != info.TotalChunks {
		t.Fatalf("complete backfill = (%+v,%v)", info, err)
	}

	before, _ := collection.Snapshot()
	if _, err := collection.Put("a", []byte(`{"tenant":"acme","status":"idle","n":1}`)); err != nil {
		t.Fatal(err)
	}
	del22, _ := collection.Delete("b")
	if !del22 {
		t.Fatal("Delete(b) missed")
	}
	if _, err := collection.Put("d", []byte(`{"tenant":"acme","status":"active","n":3}`)); err != nil {
		t.Fatal(err)
	}

	active := []vibejson.Index{testScalarIndex(t, `"acme"`), testScalarIndex(t, `"active"`)}
	snap28, _ := collection.Snapshot()
	got, err = snap28.AppendIndexKeys(nil, "tenant_status", active...)
	if err != nil || !slices.Equal(got, []string{"d"}) {
		t.Fatalf("current active lookup = (%v,%v)", got, err)
	}
	idle := testScalarIndex(t, `"idle"`)
	snap27, _ := collection.Snapshot()
	got, err = snap27.AppendIndexKeys(nil, "tenant_status", active[0], idle)
	if err != nil || !slices.Equal(got, []string{"a"}) {
		t.Fatalf("current idle lookup = (%v,%v)", got, err)
	}
	got, err = before.AppendIndexKeys(nil, "tenant_status", active...)
	if err != nil || !slices.Equal(got, []string{"a"}) {
		t.Fatalf("retained snapshot lookup = (%v,%v)", got, err)
	}
}

func TestStoreExactIndexPhysicalAliasDeduplication(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2}}
	for _, row := range []struct{ key, doc string }{
		{"a", `{"tenant":"acme","status":"active"}`},
		{"b", `{"tenant":"acme","status":"idle"}`},
		{"c", `{"tenant":"other","status":"active"}`},
	} {
		if _, err := collection.Put(row.key, []byte(row.doc)); err != nil {
			t.Fatal(err)
		}
	}
	paths := []string{"/tenant", "/status"}
	primary, err := collection.CreateIndex(IndexDefinition{Name: "primary", Paths: paths})
	if err != nil {
		t.Fatal(err)
	}
	alias, err := collection.CreateIndex(IndexDefinition{Name: "alias", Paths: paths})
	if err != nil {
		t.Fatal(err)
	}
	if primary.Name != "primary" || alias.Name != "alias" {
		t.Fatalf("logical names = (%q,%q)", primary.Name, alias.Name)
	}
	if collection.indexes["primary"] != collection.indexes["alias"] {
		t.Fatal("identical ordered paths did not share one physical build")
	}
	if collection.exactAliases != 1 || collection.indexes["primary"].refs != 2 {
		t.Fatalf("alias accounting = (%d,%d)", collection.exactAliases, collection.indexes["primary"].refs)
	}
	if _, err := collection.CreateIndex(IndexDefinition{
		Name: "reverse", Paths: []string{"/status", "/tenant"},
	}); err != nil {
		t.Fatal(err)
	}
	if collection.indexes["primary"] == collection.indexes["reverse"] {
		t.Fatal("order-sensitive compound indexes were incorrectly aliased")
	}
	if stats := collection.Stats(); stats.Indexes != 3 || stats.PhysicalIndexes != 2 {
		t.Fatalf("deduplicated Stats = %+v", stats)
	}

	info, err := collection.BackfillIndex("alias", 1)
	if err != nil || info.Name != "alias" || info.CoveredChunks != 1 {
		t.Fatalf("alias partial backfill = (%+v,%v)", info, err)
	}
	if got := storeLogicalIndexInfo("primary", collection.indexes["primary"]); got.CoveredChunks != 1 {
		t.Fatalf("primary coverage after alias backfill = %+v", got)
	}
	if info, err = collection.BackfillIndex("primary", 0); err != nil || info.State != IndexReady {
		t.Fatalf("primary complete backfill = (%+v,%v)", info, err)
	}
	if _, err = collection.BackfillIndex("reverse", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("a", []byte(`{"tenant":"acme","status":"idle"}`)); err != nil {
		t.Fatal(err)
	}
	if deleted, err := collection.Delete("b"); err != nil || !deleted {
		t.Fatalf("Delete(b) = (%v,%v)", deleted, err)
	}
	if _, err := collection.Put("d", []byte(`{"tenant":"acme","status":"active"}`)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"primary", "alias"} {
		got, err := collection.IndexRawKeys(name, []byte(`"acme"`), []byte(`"active"`))
		if err != nil || !slices.Equal(got, []string{"d"}) {
			t.Fatalf("%s lookup = (%v,%v)", name, got, err)
		}
	}

	retained, _ := collection.Snapshot()
	if err := collection.DropIndex("primary"); err != nil {
		t.Fatal(err)
	}
	if collection.exactAliases != 0 || collection.indexes["alias"].refs != 1 {
		t.Fatalf("post-drop alias accounting = (%d,%d)", collection.exactAliases, collection.indexes["alias"].refs)
	}
	if _, err := collection.IndexRawKeys("primary", []byte(`"acme"`), []byte(`"active"`)); err != ErrIndexNotFound {
		t.Fatalf("dropped primary lookup error = %v", err)
	}
	if got, err := collection.IndexRawKeys("alias", []byte(`"acme"`), []byte(`"active"`)); err != nil || !slices.Equal(got, []string{"d"}) {
		t.Fatalf("surviving alias lookup = (%v,%v)", got, err)
	}
	if got, err := retained.AppendIndexKeys(nil, "primary",
		testScalarIndex(t, `"acme"`), testScalarIndex(t, `"active"`),
	); err != nil || !slices.Equal(got, []string{"d"}) {
		t.Fatalf("retained primary lookup = (%v,%v)", got, err)
	}
	if info, err := collection.CreateIndex(IndexDefinition{Name: "alias2", Paths: paths}); err != nil || info.State != IndexReady {
		t.Fatalf("create ready alias = (%+v,%v)", info, err)
	}
	if stats := collection.Stats(); stats.Indexes != 3 || stats.PhysicalIndexes != 2 {
		t.Fatalf("post-drop/create Stats = %+v", stats)
	}
}

func TestStoreBuilderExactIndexPhysicalAliasDeduplication(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range []IndexDefinition{
		{Name: "a", Paths: []string{"/value"}},
		{Name: "b", Paths: []string{"/value"}},
		{Name: "other", Paths: []string{"/other"}},
	} {
		if err := builder.CreateIndex(def); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Append("one", []byte(`{"value":1,"other":2}`)); err != nil {
		t.Fatal(err)
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if collection.indexes["a"] != collection.indexes["b"] {
		t.Fatal("Builder duplicated identical physical indexes")
	}
	if collection.indexes["a"] == collection.indexes["other"] {
		t.Fatal("Builder merged different physical indexes")
	}
	infos, _ := collection.Snapshot()
	published := infos.AppendIndexes(nil)
	if len(published) != 3 || published[0].Name != "a" ||
		published[1].Name != "b" || published[2].Name != "other" {
		t.Fatalf("logical index catalog = %+v", published)
	}
}

func TestStoreExactIndexNestedFields(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2, ShapeTapes: true, ValueDict: true}}
	docs := []string{
		`{"profile":{"geo":{"country":"PT"},"a/b":{"~tag":"blue"}},"items":[{"sku":"A"}]}`,
		`{"profile":{"geo":{"country":"US"},"a/b":{"~tag":"blue"}},"items":[{"sku":"B"}]}`,
		`{"profile":{"geo":{"country":"PT"},"a/b":{"~tag":"red"}},"items":[{"sku":"B"}]}`,
		`{"profile":{"geo":{}},"items":[]}`,
	}
	for i, doc := range docs {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	for _, def := range []IndexDefinition{
		{Name: "country", Paths: []string{"/profile/geo/country"}},
		{Name: "escaped", Paths: []string{"/profile/a~1b/~0tag"}},
		{Name: "country_sku", Paths: []string{"/profile/geo/country", "/items/0/sku"}},
	} {
		info, err := collection.CreateIndex(def)
		if err != nil {
			t.Fatal(err)
		}
		info, err = collection.BackfillIndex(def.Name, 0)
		if err != nil || info.State != IndexReady {
			t.Fatalf("BackfillIndex(%s) = (%+v,%v)", def.Name, info, err)
		}
	}

	for _, test := range []struct {
		name   string
		values []string
		want   []string
	}{
		{"country", []string{`"PT"`}, []string{"k0", "k2"}},
		{"escaped", []string{`"blue"`}, []string{"k0", "k1"}},
		{"country_sku", []string{`"PT"`, `"B"`}, []string{"k2"}},
	} {
		values := make([][]byte, len(test.values))
		for i := range test.values {
			values[i] = []byte(test.values[i])
		}
		got, err := collection.IndexRawKeys(test.name, values...)
		if err != nil || !slices.Equal(got, test.want) {
			t.Errorf("%s lookup = (%v,%v), want %v", test.name, got, err, test.want)
		}
	}
}

func TestStoreExactIndexMutationDifferential(t *testing.T) {
	for _, chunkDocuments := range []int{1, 3, 8, 64} {
		t.Run(fmt.Sprintf("chunk=%d", chunkDocuments), func(t *testing.T) {
			collection := &Collection{Options: Options{ChunkDocuments: chunkDocuments, ShapeTapes: true}}
			for i := range 97 {
				doc := fmt.Sprintf(`{"tenant":"t%d","profile":{"bucket":%d},"seq":%d}`, i%7, i%11, i)
				if _, err := collection.Put(fmt.Sprintf("k%03d", i), []byte(doc)); err != nil {
					t.Fatal(err)
				}
			}
			info, err := collection.CreateIndex(IndexDefinition{
				Name: "tenant_bucket", Paths: []string{"/tenant", "/profile/bucket"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if info, err = collection.BackfillIndex(info.Name, 2); err != nil {
				t.Fatal(err)
			}
			snap26, _ := collection.Snapshot()
			checkCollectionExactIndexDifferential(t, snap26, info.Name)
			if info, err = collection.BackfillIndex(info.Name, 0); err != nil || info.State != IndexReady {
				t.Fatalf("complete backfill = (%+v,%v)", info, err)
			}

			retained, _ := collection.Snapshot()
			for step := range 240 {
				i := (step*37 + 13) % 131
				key := fmt.Sprintf("k%03d", i)
				if step%9 == 0 {
					collection.Delete(key)
				} else {
					doc := fmt.Sprintf(`{"tenant":"t%d","profile":{"bucket":%d},"seq":%d}`, (i+step)%7, (i*3+step)%11, step)
					if _, err := collection.Put(key, []byte(doc)); err != nil {
						t.Fatal(err)
					}
				}
				if step%17 == 0 {
					snap25, _ := collection.Snapshot()
					checkCollectionExactIndexDifferential(t, snap25, info.Name)
					checkCollectionExactIndexDifferential(t, retained, info.Name)
				}
			}
			snap24, _ := collection.Snapshot()
			checkCollectionExactIndexDifferential(t, snap24, info.Name)
			checkCollectionExactIndexDifferential(t, retained, info.Name)
		})
	}
}

func checkCollectionExactIndexDifferential(t testing.TB, snapshot Snapshot, name string) {
	t.Helper()
	tenantPath := vibejson.MustCompilePointer("/tenant")
	bucketPath := vibejson.MustCompilePointer("/profile/bucket")
	for tenant := range 7 {
		for bucket := range 11 {
			want := make([]string, 0)
			tenantNeedle := testScalarIndex(t, fmt.Sprintf(`"t%d"`, tenant)).Root().Raw()
			bucketNeedle := testScalarIndex(t, fmt.Sprint(bucket)).Root().Raw()
			snapshot.Range(func(key string, _ vibejson.RawValue) bool {
				doc, _ := snapshot.Get(key)
				tenantNode, tenantOK, tenantErr := doc.PointerCompiled(tenantPath)
				bucketNode, bucketOK, bucketErr := doc.PointerCompiled(bucketPath)
				if tenantErr != nil || bucketErr != nil {
					t.Fatalf("reference pointer: tenant=%v bucket=%v", tenantErr, bucketErr)
				}
				if tenantOK && bucketOK && storeIndexScalarEqual(tenantNode.Raw(), tenantNeedle) && storeIndexScalarEqual(bucketNode.Raw(), bucketNeedle) {
					want = append(want, key)
				}
				return true
			})
			got, err := snapshot.IndexRawKeys(name, tenantNeedle.Bytes(), bucketNeedle.Bytes())
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("%s/%d = (%v,%v), want %v", tenantNeedle.Bytes(), bucket, got, err, want)
			}
		}
	}
}

func TestStoreExactIndexScalarSemantics(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 1}}
	docs := []string{
		`{"v":1}`,
		`{"v":1.0}`,
		`{"v":"a"}`,
		`{"v":"\u0061"}`,
		`{"v":1e100000}`,
		`{"v":2e100000}`,
		`{"v":null}`,
		`{"v":[1]}`,
		`{}`,
	}
	for i, doc := range docs {
		if _, err := collection.Put(string(rune('a'+i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}})
	if err != nil {
		t.Fatal(err)
	}
	for info.State != IndexReady {
		info, err = collection.BackfillIndex("v", 2)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		needle string
		want   []string
	}{
		{`1e0`, []string{"a", "b"}},
		{`"a"`, []string{"c", "d"}},
		{`1e100000`, []string{"e"}}, // wide-number bucket collision is rechecked
		{`2e100000`, []string{"f"}},
		{`null`, []string{"g"}},
	} {
		got, err := collection.IndexRawKeys("v", []byte(test.needle))
		if err != nil || !slices.Equal(got, test.want) {
			t.Errorf("lookup %s = (%v,%v), want %v", test.needle, got, err, test.want)
		}
	}
	if _, err := collection.IndexRawKeys("v", []byte(`[1]`)); err != ErrIndexScalar {
		t.Fatalf("container lookup error = %v, want %v", err, ErrIndexScalar)
	}
}

func TestStoreIndexCandidateMasksAreSoundSuperset(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 1}}
	for i, doc := range []string{
		`{"v":1e100000}`,
		`{"v":2e100000}`,
		`{"v":3}`,
	} {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}})
	if err != nil {
		t.Fatal(err)
	}
	if info, err = collection.BackfillIndex(info.Name, 0); err != nil || info.State != IndexReady {
		t.Fatalf("BackfillIndex = (%+v,%v)", info, err)
	}
	needle := testScalarIndex(t, `1e100000`)
	exact, err := collection.AppendIndexMasks(nil, "v", needle)
	if err != nil || len(exact) != 1 || exact[0].Chunk != 0 {
		t.Fatalf("exact masks = (%v,%v), want only chunk 0", exact, err)
	}
	candidates, err := collection.AppendIndexCandidateMasks(nil, "v", needle)
	if err != nil || len(candidates) != 2 ||
		candidates[0].Chunk != 0 || candidates[1].Chunk != 1 {
		t.Fatalf("candidate masks = (%v,%v), want sound wide-number bucket", candidates, err)
	}
}

func TestStoreExactIndexDefinitionErrors(t *testing.T) {
	collection := new(Collection)
	for _, def := range []IndexDefinition{
		{},
		{Name: "x"},
		{Name: "x", Paths: []string{"not-a-pointer"}},
		{Name: "x", Paths: []string{"/a", "/a"}},
		{Name: "x", Paths: []string{"/a", "/b", "/c", "/d", "/e"}},
	} {
		if _, err := collection.CreateIndex(def); err == nil {
			t.Fatalf("CreateIndex(%+v) succeeded", def)
		}
	}
	if _, err := collection.CreateIndex(IndexDefinition{Name: "x", Paths: []string{"/a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.CreateIndex(IndexDefinition{Name: "x", Paths: []string{"/b"}}); err != ErrIndexExists {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := collection.IndexRawKeys("x"); err != ErrIndexArity {
		t.Fatalf("arity error = %v", err)
	}
}

func TestStoreIndexMasksPersistentPromotion(t *testing.T) {
	var masks storeIndexMasks
	for _, id := range []uint32{9, 1, 5, 2} {
		masks = masks.set(id, uint64(1)<<id)
	}
	inline := masks
	masks = masks.set(33, 7)
	if masks.wide.root == nil || inline.wide.root != nil {
		t.Fatal("fifth chunk did not promote without modifying old value")
	}
	var ids []uint32
	masks.each(func(id uint32, _ uint64) bool {
		ids = append(ids, id)
		return true
	})
	if !slices.Equal(ids, []uint32{1, 2, 5, 9, 33}) {
		t.Fatalf("iteration order = %v", ids)
	}
	before := masks
	masks = masks.set(5, 0)
	if before.get(5) == 0 || masks.get(5) != 0 {
		t.Fatal("persistent delete changed old bitmap or retained new bit")
	}
	if masks.wide.root != nil || masks.n != storeIndexInlineMasks {
		t.Fatal("four-word posting did not demote to compact inline storage")
	}

	var vector storeIndexMaskVector
	vector = vector.set(1, 1)
	vector = vector.set(1<<30, 2)
	deep := vector
	vector = vector.set(1<<30, 0)
	if vector.depth != storeIndexMaskMinDepth || vector.get(1) != 1 || deep.get(1<<30) != 2 {
		t.Fatal("radix vector did not shrink without changing retained root")
	}
}

func TestStoreIndexMaskIteratorOrderedAndAllocationFree(t *testing.T) {
	entries := []storeIndexChunkMask{
		{chunk: 0, mask: 1},
		{chunk: 31, mask: 2},
		{chunk: 32, mask: 4},
		{chunk: 1 << 10, mask: 8},
		{chunk: 1 << 20, mask: 16},
		{chunk: 1 << 30, mask: 32},
		{chunk: ^uint32(0), mask: 64},
	}
	masks := storeIndexMasksFromSorted(entries)
	check := func() {
		it := masks.iterator()
		for i, want := range entries {
			chunk, mask, ok := it.next()
			if !ok || chunk != want.chunk || mask != want.mask {
				t.Fatalf("iterator[%d] = (%d,%d,%v), want (%d,%d,true)",
					i, chunk, mask, ok, want.chunk, want.mask)
			}
		}
		if _, _, ok := it.next(); ok {
			t.Fatal("iterator returned a tail entry")
		}
	}
	check()
	if allocs := testing.AllocsPerRun(1000, check); allocs != 0 {
		t.Fatalf("iterator allocated %.2f times, want 0", allocs)
	}
	for _, from := range []uint64{0, 1, 32, 33, 1 << 19, 1 << 30, uint64(^uint32(0)), uint64(^uint32(0)) + 1} {
		want := len(entries)
		for i := range entries {
			if uint64(entries[i].chunk) >= from {
				want = i
				break
			}
		}
		chunk, mask, ok := masks.next(from)
		if want == len(entries) {
			if ok {
				t.Fatalf("next(%d) = (%d,%d,true), want exhausted", from, chunk, mask)
			}
			continue
		}
		if !ok || chunk != entries[want].chunk || mask != entries[want].mask {
			t.Fatalf("next(%d) = (%d,%d,%v), want (%d,%d,true)",
				from, chunk, mask, ok, entries[want].chunk, entries[want].mask)
		}
	}
}

// TestStoreIndexMaskRadixFootprint pins the level split of the posting radix.
// A level-tagged node carrying a child array and a word array pays for both at
// every level while the level decides which one a walk can reach, so half of
// every node was unreachable storage — 49.1% of a whole 1000-distinct-value
// index in the profile that motivated the split. The shape below is fixed: 256
// single-document chunks and four values, so every posting spans 64 chunk ids
// under 256 and therefore one branch over eight 32-chunk leaves. The bound is
// what an unsplit node would have cost for those same nodes, and IndexStats
// accounts for strictly more than them, so staying under it can only mean the
// leaves stopped carrying child pointers.
func TestStoreIndexMaskRadixFootprint(t *testing.T) {
	collection, err := New(Options{ChunkDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}}); err != nil {
		t.Fatal(err)
	}
	for i := range 256 {
		if _, err := collection.Put(strconv.Itoa(i), []byte(`{"v":`+strconv.Itoa(i%4)+`}`)); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := collection.IndexStats("v")
	if err != nil {
		t.Fatal(err)
	}
	// Four postings, each one branch plus eight leaves. Asserting the shape
	// first keeps the byte bound meaningful: a degenerate index would pass a
	// size bound by holding nothing.
	if stats.Fingerprints != 4 || stats.BitmapNodes != 4*(1+8) {
		t.Fatalf("radix shape = %d fingerprints, %d bitmap nodes; want 4 and 36",
			stats.Fingerprints, stats.BitmapNodes)
	}
	pointer := reflect.TypeFor[*storeIndexMaskBranch]().Size()
	unsplit := uint64(stats.BitmapNodes) * uint64(32*8+32*uint64(pointer))
	if stats.EstimatedBytes >= unsplit {
		t.Fatalf("index footprint %d B is not below the %d B an unsplit radix "+
			"would spend on its %d nodes alone", stats.EstimatedBytes, unsplit, stats.BitmapNodes)
	}
}

// TestStoreIndexMasksWideWithinOneLeaf covers the posting shape the radix no
// longer has a special case for: wide, but addressing only chunks below 32. It
// used to be a bare level-zero root, and making the wide root always a branch
// is what keeps a second typed pointer out of every posting. Every reader must
// agree on that shape, so all four are exercised against the same posting.
func TestStoreIndexMasksWideWithinOneLeaf(t *testing.T) {
	entries := []storeIndexChunkMask{
		{chunk: 0, mask: 1},
		{chunk: 3, mask: 2},
		{chunk: 7, mask: 4},
		{chunk: 18, mask: 8},
		{chunk: 31, mask: 16},
	}
	for _, masks := range []storeIndexMasks{
		storeIndexMasksFromSorted(entries),
		func() storeIndexMasks {
			var m storeIndexMasks
			for _, e := range entries {
				m = m.set(e.chunk, e.mask)
			}
			return m
		}(),
	} {
		if masks.wide.root == nil {
			t.Fatal("a five-word posting stayed inline")
		}
		if masks.wide.depth < storeIndexMaskMinDepth {
			t.Fatalf("wide depth = %d, want at least %d", masks.wide.depth, storeIndexMaskMinDepth)
		}
		for _, e := range entries {
			if got := masks.get(e.chunk); got != e.mask {
				t.Fatalf("get(%d) = %d, want %d", e.chunk, got, e.mask)
			}
		}
		if got := masks.get(1); got != 0 {
			t.Fatalf("get(1) = %d, want 0 for an absent chunk", got)
		}
		var seen []storeIndexChunkMask
		masks.each(func(chunk uint32, mask uint64) bool {
			seen = append(seen, storeIndexChunkMask{chunk: chunk, mask: mask})
			return true
		})
		if !slices.Equal(seen, entries) {
			t.Fatalf("each = %v, want %v", seen, entries)
		}
		seen = seen[:0]
		it := masks.iterator()
		for {
			chunk, mask, ok := it.next()
			if !ok {
				break
			}
			seen = append(seen, storeIndexChunkMask{chunk: chunk, mask: mask})
		}
		if !slices.Equal(seen, entries) {
			t.Fatalf("iterator = %v, want %v", seen, entries)
		}
		for from := uint64(0); from <= 32; from++ {
			want := len(entries)
			for i := range entries {
				if uint64(entries[i].chunk) >= from {
					want = i
					break
				}
			}
			chunk, mask, ok := masks.next(from)
			if want == len(entries) {
				if ok {
					t.Fatalf("next(%d) = (%d,%d,true), want exhausted", from, chunk, mask)
				}
				continue
			}
			if !ok || chunk != entries[want].chunk || mask != entries[want].mask {
				t.Fatalf("next(%d) = (%d,%d,%v), want (%d,%d,true)",
					from, chunk, mask, ok, entries[want].chunk, entries[want].mask)
			}
		}
		// Dropping back to four words must demote to inline storage, which is
		// the path that reads the wide vector out through each.
		demoted := masks.set(18, 0)
		if demoted.wide.root != nil || demoted.n != storeIndexInlineMasks {
			t.Fatalf("demoted posting = %d inline words, wide=%v", demoted.n, demoted.wide.root != nil)
		}
		if masks.get(18) != 8 {
			t.Fatal("demotion mutated the retained posting")
		}
	}
}

func TestStoreIndexMergeBulkMasksInterleaved(t *testing.T) {
	const words = 2048
	currentEntries := make([]storeIndexChunkMask, words)
	changes := make([]storeIndexChunkMask, words)
	for i := range words {
		currentEntries[i] = storeIndexChunkMask{chunk: uint32(i * 2), mask: 1}
		changes[i] = storeIndexChunkMask{chunk: uint32(i*2 + 1), mask: 2}
	}
	merged := storeIndexMergeBulkMasks(storeIndexMasksFromSorted(currentEntries), changes)
	it := merged.iterator()
	for i := range words * 2 {
		chunk, mask, ok := it.next()
		wantMask := uint64(1)
		if i&1 != 0 {
			wantMask = 2
		}
		if !ok || chunk != uint32(i) || mask != wantMask {
			t.Fatalf("merged[%d] = (%d,%d,%v), want (%d,%d,true)",
				i, chunk, mask, ok, i, wantMask)
		}
	}
	if _, _, ok := it.next(); ok {
		t.Fatal("interleaved merge returned a tail entry")
	}

	overlap := storeIndexMergeBulkMasks(
		storeIndexMasksFromSorted([]storeIndexChunkMask{{chunk: 7, mask: 1}}),
		[]storeIndexChunkMask{{chunk: 7, mask: 2}},
	)
	if got := overlap.get(7); got != 3 {
		t.Fatalf("overlap mask = %d, want 3", got)
	}
}

func TestStoreIndexMergeBulkMasksDifferential(t *testing.T) {
	state := uint64(0x4d595df4d0f33173)
	random := func() uint64 {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		return state * 0x2545f4914f6cdd1d
	}
	for trial := range 500 {
		currentMap := make(map[uint32]uint64)
		changeMap := make(map[uint32]uint64)
		for len(currentMap) < 1+int(random()%160) {
			currentMap[uint32(random()%4096)] |= uint64(1) << (random() & 63)
		}
		for len(changeMap) < 1+int(random()%160) {
			changeMap[uint32(random()%4096)] |= uint64(1) << (random() & 63)
		}
		currentEntries := make([]storeIndexChunkMask, 0, len(currentMap))
		changes := make([]storeIndexChunkMask, 0, len(changeMap))
		for chunk, mask := range currentMap {
			currentEntries = append(currentEntries, storeIndexChunkMask{chunk: chunk, mask: mask})
		}
		for chunk, mask := range changeMap {
			changes = append(changes, storeIndexChunkMask{chunk: chunk, mask: mask})
		}
		slices.SortFunc(currentEntries, func(a, b storeIndexChunkMask) int {
			return int(a.chunk) - int(b.chunk)
		})
		slices.SortFunc(changes, func(a, b storeIndexChunkMask) int {
			return int(a.chunk) - int(b.chunk)
		})

		want := make(map[uint32]uint64, len(currentMap)+len(changeMap))
		for chunk, mask := range currentMap {
			want[chunk] = mask
		}
		for chunk, mask := range changeMap {
			want[chunk] |= mask
		}
		got := storeIndexMergeBulkMasks(storeIndexMasksFromSorted(currentEntries), changes)
		it := got.iterator()
		seen := 0
		var previous uint32
		for {
			chunk, mask, ok := it.next()
			if !ok {
				break
			}
			if seen != 0 && chunk <= previous {
				t.Fatalf("trial %d produced non-ascending chunks %d then %d", trial, previous, chunk)
			}
			if mask != want[chunk] {
				t.Fatalf("trial %d chunk %d mask = %#x, want %#x", trial, chunk, mask, want[chunk])
			}
			previous = chunk
			seen++
		}
		if seen != len(want) {
			t.Fatalf("trial %d produced %d chunks, want %d", trial, seen, len(want))
		}
	}
}

func TestStoreIndexPostingBulkBuild(t *testing.T) {
	pending := make(map[uint64][]storeIndexChunkMask, 2048)
	for i := range uint64(2048) {
		// Hold the first radix digit constant and vary later digits. This is
		// the adversarial ordering for a builder that incorrectly sorts by the
		// ordinary high-to-low integer order.
		hash := i<<5 | 7
		pending[hash] = []storeIndexChunkMask{{chunk: uint32(i & 63), mask: uint64(1) << (i & 63)}}
	}
	root := storeIndexBuildBulk(pending)
	for hash, want := range pending {
		got, ok := storeIndexPostingLookup(root, hash)
		if !ok || got.get(want[0].chunk) != want[0].mask {
			t.Fatalf("lookup %#x missed bulk-built posting", hash)
		}
	}
}

func TestStoreExactIndexSteadyLookupAllocs(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 8, ShapeTapes: true}}
	for i := range 64 {
		doc := []byte(`{"tenant":"acme","bucket":3}`)
		if _, err := collection.Put(string(rune(i+1)), doc); err != nil {
			t.Fatal(err)
		}
	}
	info, err := collection.CreateIndex(IndexDefinition{Name: "tb", Paths: []string{"/tenant", "/bucket"}})
	if err != nil {
		t.Fatal(err)
	}
	for info.State != IndexReady {
		info, err = collection.BackfillIndex("tb", 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	snapshot, _ := collection.Snapshot()
	tenant := testScalarIndex(t, `"acme"`)
	bucket := testScalarIndex(t, `3`)
	dst := make([]string, 0, snapshot.Len())
	dst, err = snapshot.AppendIndexKeys(dst[:0], "tb", tenant, bucket)
	if err != nil || len(dst) != snapshot.Len() {
		t.Fatalf("warm lookup = (%d,%v)", len(dst), err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var runErr error
		dst, runErr = snapshot.AppendIndexKeys(dst[:0], "tb", tenant, bucket)
		if runErr != nil || len(dst) != snapshot.Len() {
			panic("lookup failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("AppendIndexKeys allocated %.2f times, want 0", allocs)
	}
	rawTenant, rawBucket := []byte(`"acme"`), []byte(`3`)
	allocs = testing.AllocsPerRun(100, func() {
		var runErr error
		dst, runErr = snapshot.AppendIndexRawKeys(dst[:0], "tb", rawTenant, rawBucket)
		if runErr != nil || len(dst) != snapshot.Len() {
			panic("raw lookup failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("AppendIndexRawKeys allocated %.2f times, want 0", allocs)
	}
}

func TestStoreExactIndexStats(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2}}
	for i := range 10 {
		doc := fmt.Sprintf(`{"v":%d}`, i&1)
		if _, err := collection.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}})
	if err != nil {
		t.Fatal(err)
	}
	if info, err = collection.BackfillIndex(info.Name, 0); err != nil || info.State != IndexReady {
		t.Fatalf("BackfillIndex = (%+v,%v)", info, err)
	}
	stats, err := collection.IndexStats(info.Name)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fingerprints != 2 || stats.ChunkWords != 10 || stats.CandidateRows != 10 ||
		stats.EstimatedBytes == 0 || stats.PackedBytes == 0 || stats.DirectoryNodes != 0 {
		t.Fatalf("IndexStats = %+v", stats)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var runErr error
		stats, runErr = collection.IndexStats(info.Name)
		if runErr != nil || stats.CandidateRows != 10 {
			panic("IndexStats failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("IndexStats allocated %.2f times, want 0", allocs)
	}
	if _, err := collection.IndexStats("missing"); err != ErrIndexNotFound {
		t.Fatalf("missing IndexStats error = %v", err)
	}
}
