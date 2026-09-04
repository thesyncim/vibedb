package store

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

func TestStoreBuilderEquivalentAndMutable(t *testing.T) {
	for _, options := range []Options{
		{ChunkDocuments: 1},
		{ChunkDocuments: 7, ShapeTapes: true},
		{ChunkDocuments: 64, ShapeTapes: true, Postings: true, ValueDict: true,
			IndexOptions: document.IndexOptions{HashKeys: true}},
	} {
		t.Run(fmt.Sprintf("chunk=%d/shape=%v/postings=%v", options.ChunkDocuments, options.ShapeTapes, options.Postings), func(t *testing.T) {
			builder, err := NewBuilder(options)
			if err != nil {
				t.Fatal(err)
			}
			want := make(map[string]string)
			for i := range 257 {
				key := fmt.Sprintf("key-%03d", i)
				doc := fmt.Sprintf(`{"id":%d,"profile":{"geo":{"country":"c%d"}},"active":%t}`, i, i%9, i%2 == 0)
				input := []byte(doc)
				if err := builder.Append(key, input); err != nil {
					t.Fatal(err)
				}
				clear(input)
				want[key] = doc
			}
			if builder.Len() != len(want) {
				t.Fatalf("builder Len = %d, want %d", builder.Len(), len(want))
			}
			collection, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if collection.Len() != uint64(len(want)) || collection.Generation() != 1 {
				t.Fatalf("built collection Len/Generation = %d/%d", collection.Len(), collection.Generation())
			}
			snap4, _ := collection.Snapshot()
			checkCollectionSnapshot(t, snap4, want)

			// A built collection is still Put-mutable — the SQL driver stages
			// transient point-lookup rows into one — and a snapshot retained across
			// that Put keeps its original bytes.
			before, _ := collection.Snapshot()
			if _, err := collection.Put("key-003", []byte(`{"id":3,"profile":{"geo":{"country":"new"}}}`)); err != nil {
				t.Fatal(err)
			}
			if raw, ok := before.GetRaw("key-003"); !ok || string(raw.Bytes()) != want["key-003"] {
				t.Fatal("post-build mutation changed retained snapshot")
			}
			if raw, ok := collection.GetRaw("key-003"); !ok || string(raw.Bytes()) != `{"id":3,"profile":{"geo":{"country":"new"}}}` {
				t.Fatalf("post-build Put not visible: (%q,%v)", raw.Bytes(), ok)
			}
		})
	}
}

const storeBuilderTemplateRows = 32

func buildNestedTemplateCollection(t *testing.T) *Collection {
	t.Helper()
	builder, err := NewBuilder(Options{ChunkDocuments: 8, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range storeBuilderTemplateRows {
		doc := fmt.Sprintf(`{"id":%d,"profile":{"geo":{"country":"c%d"}},"active":%t}`, i, i%4, i&1 == 0)
		if err := builder.Append(fmt.Sprintf("k%02d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	// The exact index over the interned nested templates is declared online on
	// the built collection; Builder no longer declares indexes itself.
	if _, err := collection.CreateIndex(IndexDefinition{Name: "country", Paths: []string{"/profile/geo/country"}}); err != nil {
		t.Fatal(err)
	}
	for {
		info, err := collection.BackfillIndex("country", 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.State == IndexReady {
			break
		}
	}
	return collection
}

func TestStoreBuilderInternsNestedStructuralTemplates(t *testing.T) {
	const rows = storeBuilderTemplateRows
	collection := buildNestedTemplateCollection(t)
	state := collection.state.Load()
	if state.mappedDocs == nil || len(state.mappedDocs.templates) != 1 {
		t.Fatalf("nested template catalog = %+v", state.mappedDocs)
	}
	state.Chunks.Each(func(id uint32, chunk *Chunk) bool {
		for row := 0; row < chunk.Docs.Len(); row++ {
			ref := state.mappedDocs.refAt(chunk.Docs.mappedBase + uint64(row))
			if !storeOwnedDocIsTemplate(ref.kind) || ref.shapeID != 0 || len(chunk.Docs.DocAt(row).Entries) != 0 {
				t.Fatalf("chunk %d row %d ref/index = %+v/%+v", id, row, ref, chunk.Docs.DocAt(row))
			}
		}
		return true
	})

	pointer := vibejson.MustCompilePointer("/profile/geo/country")
	values := make([]vibejson.RawValue, 0, rows)
	snap10, _ := collection.Snapshot()
	values, err := snap10.AppendPointer(values, pointer)
	if err != nil || len(values) != rows || string(values[7].Bytes()) != `"c3"` {
		t.Fatalf("template pointer = (%d,%q,%v)", len(values), values[7].Bytes(), err)
	}
	keys := make([]string, 0, rows)
	snap9, _ := collection.Snapshot()
	keys, err = snap9.AppendIndexRawKeys(keys, "country", []byte(`"c3"`))
	if err != nil || len(keys) != rows/4 {
		t.Fatalf("template exact index = (%d,%v)", len(keys), err)
	}
	index, ok := collection.Get("k07")
	if !ok {
		t.Fatal("navigable template Get missed")
	}
	node, ok, err := index.PointerCompiled(pointer)
	if err != nil || !ok || string(node.Raw().Bytes()) != `"c3"` {
		t.Fatalf("widened template pointer = (%q,%v,%v)", node.Raw().Bytes(), ok, err)
	}
	before, _ := collection.Snapshot()
	if _, err := collection.Put("k07", []byte(`{"id":7,"profile":{"geo":{"country":"new"}},"active":false}`)); err != nil {
		t.Fatal(err)
	}
	if raw, ok := before.GetRaw("k07"); !ok || !bytes.Contains(raw.Bytes(), []byte(`"c3"`)) {
		t.Fatalf("retained template snapshot = (%q,%v)", raw.Bytes(), ok)
	}
	snap8, _ := collection.Snapshot()
	if keys, err = snap8.AppendIndexRawKeys(keys[:0], "country", []byte(`"new"`)); err != nil || len(keys) != 1 || keys[0] != "k07" {
		t.Fatalf("mutated template index = (%v,%v)", keys, err)
	}
}

func TestStoreBuilderNestedStructuralTemplateAllocs(t *testing.T) {
	collection := buildNestedTemplateCollection(t)
	pointer := vibejson.MustCompilePointer("/profile/geo/country")
	values := make([]vibejson.RawValue, 0, storeBuilderTemplateRows)
	var err error
	snap7, _ := collection.Snapshot()
	values, err = snap7.AppendPointer(values, pointer)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		snap6, _ := collection.Snapshot()
		values, err = snap6.AppendPointer(values[:0], pointer)
	})
	if err != nil || allocs != 0 {
		t.Fatalf("template pointer allocs/error = %.2f/%v", allocs, err)
	}

	keys := make([]string, 0, storeBuilderTemplateRows)
	snap20, _ := collection.Snapshot()
	keys, err = snap20.AppendIndexRawKeys(keys, "country", []byte(`"c3"`))
	if err != nil {
		t.Fatal(err)
	}
	allocs = testing.AllocsPerRun(100, func() {
		snap19, _ := collection.Snapshot()
		keys, err = snap19.AppendIndexRawKeys(keys[:0], "country", []byte(`"c3"`))
	})
	if err != nil || allocs != 0 {
		t.Fatalf("template exact index allocs/error = %.2f/%v", allocs, err)
	}
}

func TestStoreBuilderCompactsKeyDirectory(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ key, json string }{
		{"", `0`},
		{"alpha", `1`},
		{"a-key-longer-than-the-first-arena-growth", `2`},
	} {
		if err := builder.Append(row.key, []byte(row.json)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	state := collection.state.Load()
	if state.keys != nil || state.baseKeys == nil || state.baseKeys.count != 3 ||
		state.baseKeys.sourceBlock == nil {
		t.Fatalf("published key directory retained builder graph: %+v", state)
	}
	if state.baseKeys.dense == nil || state.baseKeys.compact != nil || state.baseKeys.refs != nil ||
		state.baseKeys.denseShift != 1 {
		t.Fatalf("power-of-two chunks did not select dense key refs: %+v", state.baseKeys)
	}
	state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		if chunk.keys != nil || chunk.keyBytes != nil || chunk.mappedKeys != state.baseKeys {
			t.Fatalf("chunk retained heap key storage: %+v", chunk)
		}
		return true
	})
	for key, want := range map[string]string{
		"": `0`, "alpha": `1`, "a-key-longer-than-the-first-arena-growth": `2`,
	} {
		if raw, ok := collection.GetRaw(key); !ok || string(raw.Bytes()) != want {
			t.Fatalf("GetRaw(%q) = (%q,%v), want (%q,true)", key, raw.Bytes(), ok, want)
		}
	}
	if state.baseKeys.sourceBlock.OutsideHeap() && collection.Stats().ExternalKeyBytes == 0 {
		t.Fatal("owned external key bytes not reported")
	}

	before, _ := collection.Snapshot()
	if created, err := collection.Put("later", []byte(`3`)); err != nil || !created {
		t.Fatalf("Put(later) into compact base = (%v,%v)", created, err)
	}
	if raw, ok := before.GetRaw("alpha"); !ok || string(raw.Bytes()) != `1` {
		t.Fatalf("retained compact snapshot = (%q,%v)", raw.Bytes(), ok)
	}
	if raw, ok := collection.GetRaw("later"); !ok || string(raw.Bytes()) != `3` {
		t.Fatalf("Put(later) into compact base not visible: (%q,%v)", raw.Bytes(), ok)
	}
}

func TestStoreBuilderCompactsNonPowerOfTwoKeyDirectory(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 3})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 7 {
		if err := builder.Append(fmt.Sprintf("key-%d", row), []byte(fmt.Sprintf(`{"n":%d}`, row))); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	base := collection.state.Load().baseKeys
	if base == nil || base.compact == nil || base.dense != nil || base.refs != nil {
		t.Fatalf("non-power-of-two chunks did not select explicit compact refs: %+v", base)
	}
	for row := range 7 {
		key := fmt.Sprintf("key-%d", row)
		if raw, ok := collection.GetRaw(key); !ok || string(raw.Bytes()) != fmt.Sprintf(`{"n":%d}`, row) {
			t.Fatalf("GetRaw(%q) = (%q, %v)", key, raw.Bytes(), ok)
		}
	}
}

func TestStoreBuilderKeyTableCollisionAndAllocs(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"alpha", "beta", "gamma"} {
		if err := builder.Append(key, []byte(`0`)); err != nil {
			t.Fatal(err)
		}
	}

	// A single artificial fingerprint and probe bucket exercises the exact-key
	// comparison rather than relying on maphash collisions occurring by chance.
	const collision = uint64(0x123456789abcdef0)
	builder.keyTable = storeBuilderKeyTable{}
	builder.keyTable.reserve(builder, 3)
	for row := range uint64(3) {
		builder.keyTable.insert(collision, row)
	}
	for _, key := range []string{"alpha", "beta", "gamma"} {
		if !builder.keyTable.contains(builder, collision, key) {
			t.Fatalf("collision chain missed %q", key)
		}
	}
	if builder.keyTable.contains(builder, collision, "delta") {
		t.Fatal("collision chain accepted a different key")
	}

	allocs := testing.AllocsPerRun(100, func() {
		clear(builder.keyTable.slots)
		for row := range uint64(3) {
			builder.keyTable.insert(collision, row)
		}
		for _, key := range []string{"alpha", "beta", "gamma"} {
			if !builder.keyTable.contains(builder, collision, key) {
				panic("key table lookup invariant")
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("reserved key insert/lookup allocations = %.2f, want 0", allocs)
	}
}

func TestStoreBuilderSharesImmutableShapesAcrossChunks(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 2, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		if err := builder.Append(string(rune('a'+i)), []byte(`{"a":1,"b":"x"}`)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	var shared *ShapeRecord
	collection.state.Load().Chunks.Each(func(id uint32, chunk *Chunk) bool {
		// The ingest cache is released when chunks publish: shapes live in
		// mappedShapes and the rows' Rec pointers, never in heap tables.
		if len(chunk.Docs.mappedShapes) != 1 || chunk.Docs.shapes != nil {
			t.Fatalf("chunk %d mapped shapes = %d, heap cache released = %v, want 1/true",
				id, len(chunk.Docs.mappedShapes), chunk.Docs.shapes == nil)
		}
		rec := chunk.Docs.mappedShapes[0]
		if shared == nil {
			shared = rec
		} else if rec != shared {
			t.Fatalf("chunk %d recompiled immutable shape", id)
		}
		for row := 0; row < chunk.Docs.Len(); row++ {
			if chunk.Docs.ShapeTapeRefAt(row).Rec != shared {
				t.Fatalf("chunk %d row %d did not use shared shape", id, row)
			}
		}
		return true
	})
}

func TestStoreBuilderReservesOneBoundedSourceArena(t *testing.T) {
	const rows = 64
	builder, err := NewBuilder(Options{ChunkDocuments: rows, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"a":"0123456789abcdef","b":123456789}`)
	for i := range rows {
		if err := builder.Append(string(rune(i+1)), document); err != nil {
			t.Fatal(err)
		}
	}
	chunk := builder.chunks.Get(0)
	if chunk == nil || len(chunk.Docs.srcChunk) != rows*len(document) {
		t.Fatalf("source arena length = %d, want %d", len(chunk.Docs.srcChunk), rows*len(document))
	}
	if cap(chunk.Docs.srcChunk) < rows*len(document) || cap(chunk.Docs.srcChunk) > rows*len(document)*5/4 {
		t.Fatalf("source arena capacity = %d, want [%d,%d]", cap(chunk.Docs.srcChunk), rows*len(document), rows*len(document)*5/4)
	}
}

func TestStoreBuilderErrorsAndEmptyStore(t *testing.T) {
	if _, err := NewBuilder(Options{ChunkDocuments: 65}); err == nil {
		t.Fatal("invalid chunk bound accepted")
	}
	var nilBuilder *Builder
	if !errors.Is(nilBuilder.Append("k", []byte(`null`)), ErrBuilderClosed) {
		t.Fatal("nil Append error")
	}
	if _, err := nilBuilder.Build(); !errors.Is(err, ErrBuilderClosed) {
		t.Fatal("nil Build error")
	}

	builder, err := NewBuilder(Options{ChunkDocuments: 3, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("bad", []byte(`{"broken"`)); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if builder.Len() != 0 {
		t.Fatalf("invalid append changed Len to %d", builder.Len())
	}
	if err := builder.Append("bad", []byte(`{"valid":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("bad", []byte(`{"duplicate":true}`)); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate error = %v", err)
	}
	if builder.Len() != 1 {
		t.Fatalf("duplicate changed Len to %d", builder.Len())
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("later", []byte(`null`)); !errors.Is(err, ErrBuilderClosed) {
		t.Fatalf("Append after Build error = %v", err)
	}
	if _, err := builder.Build(); !errors.Is(err, ErrBuilderClosed) {
		t.Fatalf("second Build error = %v", err)
	}
	if raw, ok := collection.GetRaw("bad"); !ok || string(raw.Bytes()) != `{"valid":true}` {
		t.Fatalf("built value = (%q,%v)", raw.Bytes(), ok)
	}

	emptyBuilder, err := NewBuilder(Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := emptyBuilder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if empty.Len() != 0 || empty.Generation() != 0 {
		t.Fatalf("empty Len/Generation = %d/%d", empty.Len(), empty.Generation())
	}
	if _, err := empty.Put("first", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
}
