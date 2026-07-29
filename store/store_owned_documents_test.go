package store

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibejson"
)

func TestStoreBuilderSelectsOwnedNarrowTapeWidths(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		kind uint8
	}{
		{name: "8-bit coordinates", doc: `{"a":1,"b":"x"}`, kind: storeOwnedDocNarrow8},
		{
			name: "9-bit start",
			doc:  fmt.Sprintf(`{"a":"%s","b":"%s"}`, strings.Repeat("a", 128), strings.Repeat("b", 128)),
			kind: storeOwnedDocNarrow9,
		},
		{
			name: "16-bit start",
			doc: fmt.Sprintf(`{"a":"%s","b":"%s","c":"%s","d":"%s"}`,
				strings.Repeat("a", 128), strings.Repeat("b", 128),
				strings.Repeat("c", 128), strings.Repeat("d", 128)),
			kind: storeOwnedDocNarrowLength8,
		},
		{
			name: "wide value",
			doc:  fmt.Sprintf(`{"a":"%s"}`, strings.Repeat("x", 300)),
			kind: storeOwnedDocNarrow,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collection := buildOwnedLayoutCollection(t, tc.doc, 2)
			state := collection.state.Load()
			if state.mappedDocs.compactRefs == nil {
				t.Fatal("small owned publication did not select compact row refs")
			}
			for row := range uint64(2) {
				ref := state.mappedDocs.refAt(row)
				if ref.kind != tc.kind {
					t.Fatalf("row %d tape kind = %d, want %d", row, ref.kind, tc.kind)
				}
			}
			assertOwnedCollection(t, collection, "k1", tc.doc, "/a")
		})
	}
}

func TestStoreBuilderSelectsOwnedTemplateSpanWidths(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		kind uint8
	}{
		{
			name: "8-bit coordinates",
			doc:  `{"a":{"x":"short"},"b":{"y":1}}`,
			kind: storeOwnedDocTemplate8,
		},
		{
			name: "16-bit start",
			doc: fmt.Sprintf(`{"a":{"x":"%s"},"b":{"y":"%s"},"c":{"z":"%s"}}`,
				strings.Repeat("a", 110), strings.Repeat("b", 110), strings.Repeat("c", 30)),
			kind: storeOwnedDocTemplateLength8,
		},
		{
			name: "wide child",
			doc:  fmt.Sprintf(`{"a":{"x":"%s"}}`, strings.Repeat("x", 300)),
			kind: storeOwnedDocTemplate,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collection := buildOwnedLayoutCollection(t, tc.doc, 2)
			state := collection.state.Load()
			if len(state.mappedDocs.templates) != 1 {
				t.Fatalf("template count = %d, want 1", len(state.mappedDocs.templates))
			}
			for row := range uint64(2) {
				ref := state.mappedDocs.refAt(row)
				if ref.kind != tc.kind || ref.shapeID != 0 {
					t.Fatalf("row %d ref = %+v, want kind %d template 0", row, ref, tc.kind)
				}
			}
			assertOwnedCollection(t, collection, "k1", tc.doc, "/a/x")
		})
	}
}

func TestStoreBuilderTemplatesComposeWithValueDictionary(t *testing.T) {
	const rows = 16
	const want = `"a-repeated-value-longer-than-the-default-floor"`
	doc := `{"profile":{"label":` + want + `},"active":true}`
	builder, err := NewBuilder(Options{ChunkDocuments: 8, ShapeTapes: true, ValueDict: true})
	if err != nil {
		t.Fatal(err)
	}
	for row := range rows {
		if err := builder.Append(fmt.Sprintf("k%d", row), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(collection.state.Load().mappedDocs.templates) != 1 {
		t.Fatal("value dictionary disabled structural templates")
	}
	pointer := vibejson.MustCompilePointer("/profile/label")
	values := make([]vibejson.RawValue, 0, rows)
	snap39, _ := collection.Snapshot()
	values, err = snap39.AppendPointer(values, pointer)
	if err != nil || len(values) != rows {
		t.Fatalf("template dictionary pointer = (%d, %v)", len(values), err)
	}
	if got := string(values[rows-1].Bytes()); got != want {
		t.Fatalf("template dictionary pointer = %q, want %q", got, want)
	}
	allocs := testing.AllocsPerRun(100, func() {
		snap38, _ := collection.Snapshot()
		values, err = snap38.AppendPointer(values[:0], pointer)
	})
	if err != nil || allocs != 0 {
		t.Fatalf("template dictionary pointer allocs/error = %.2f/%v", allocs, err)
	}
	assertOwnedCollection(t, collection, "k7", doc, "/profile/label")
}

func TestStoreBuilderCompactRefsRecoverTrimmedRoot(t *testing.T) {
	for _, tc := range []struct {
		name, source, root string
	}{
		{name: "shape", source: " \n {\"a\":1,\"b\":2}\t ", root: `{"a":1,"b":2}`},
		{name: "template", source: "\t {\"nested\":{\"a\":1}} \r\n", root: `{"nested":{"a":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collection := buildOwnedLayoutCollection(t, tc.source, 2)
			if collection.state.Load().mappedDocs.compactRefs == nil {
				t.Fatal("small publication did not select compact refs")
			}
			index, ok := collection.Get("k1")
			if !ok || string(index.Root().Raw().Bytes()) != tc.root {
				t.Fatalf("trimmed root = (%q, %v), want %q", index.Root().Raw().Bytes(), ok, tc.root)
			}
			if raw, ok := collection.GetRaw("k1"); !ok || string(raw.Bytes()) != tc.source {
				t.Fatalf("exact source = (%q, %v), want %q", raw.Bytes(), ok, tc.source)
			}
			snap37, _ := collection.Snapshot()
			roots, err := snap37.AppendPointer(nil, vibejson.MustCompilePointer(""))
			if err != nil || len(roots) != 2 || string(roots[1].Bytes()) != tc.root {
				t.Fatalf("root column = (%q, %v), want %q", roots[1].Bytes(), err, tc.root)
			}
		})
	}
}

// TestStorePutRetainsMappedDocumentOwner is the regression test for a
// use-after-free. A chunk rebuild carries surviving rows by reference out of
// the mapped source block (appendStoreDoc copies nothing), and those borrowed
// pointers are invisible to the garbage collector. An earlier design detached
// the state-level owner once every chunk had been rebuilt; the finalizer then
// unmapped bytes the rebuilt chunks still aliased, and reading a carried row
// after GC faulted. The owner must therefore survive every rebuild, exactly
// like baseKeys does on the key side.
func TestStorePutRetainsMappedDocumentOwner(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 2, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	docs := make(map[string]string, 4)
	for row := range 4 {
		key := fmt.Sprintf("k%d", row)
		docs[key] = fmt.Sprintf(`{"n":%d,"pad":"%032d"}`, row, row)
		if err := builder.Append(key, []byte(docs[key])); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	retained, _ := collection.Snapshot()
	// One Put per chunk rebuilds every chunk; k1 and k3 are carried by
	// reference out of the mapped block into the rebuilt chunks.
	if _, err := collection.Put("k0", []byte(`{"n":10,"pad":"replacement-payload-0"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("k2", []byte(`{"n":12,"pad":"replacement-payload-2"}`)); err != nil {
		t.Fatal(err)
	}
	if state := collection.state.Load(); state.mappedDocs == nil {
		t.Fatal("rebuilding every chunk dropped the mapped document owner")
	}
	// Give the collector every chance to run the owner's finalizer if the
	// owner edge were broken; the reads below would then fault or return
	// garbage rather than the original bytes.
	for range 6 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	for _, key := range []string{"k1", "k3"} {
		if raw, ok := collection.GetRaw(key); !ok || string(raw.Bytes()) != docs[key] {
			t.Fatalf("carried row %s after GC = (%q, %v), want %q", key, raw.Bytes(), ok, docs[key])
		}
	}
	for _, key := range []string{"k0", "k2"} {
		if raw, ok := retained.GetRaw(key); !ok || string(raw.Bytes()) != docs[key] {
			t.Fatalf("retained %s after GC = (%q, %v), want %q", key, raw.Bytes(), ok, docs[key])
		}
	}
}

func buildOwnedLayoutCollection(t *testing.T, doc string, rows int) *Collection {
	t.Helper()
	builder, err := NewBuilder(Options{ChunkDocuments: 8, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for row := range rows {
		if err := builder.Append(fmt.Sprintf("k%d", row), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func assertOwnedCollection(t *testing.T, collection *Collection, key, want, pointer string) {
	t.Helper()
	index, ok := collection.Get(key)
	if !ok || string(index.Root().Raw().Bytes()) != want {
		t.Fatalf("Get(%q) = (%q, %v)", key, index.Root().Raw().Bytes(), ok)
	}
	node, ok, err := index.Pointer(pointer)
	if err != nil || !ok || len(node.Raw().Bytes()) == 0 {
		t.Fatalf("Pointer(%q) = (%q, %v, %v)", pointer, node.Raw().Bytes(), ok, err)
	}
}
