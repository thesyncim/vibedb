package store

import (
	"testing"
)

func putDoc(t *testing.T, c *Collection, key, doc string) {
	t.Helper()
	if _, err := c.Put(key, []byte(doc)); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}

func TestTinIndexLifecycle(t *testing.T) {
	c := &Collection{}
	putDoc(t, c, "a", `{"title":"fuji apple pie","n":1}`)
	putDoc(t, c, "b", `{"title":"apple fuji tart","n":2}`)
	putDoc(t, c, "c", `{"title":"lazy dog","n":3}`)
	putDoc(t, c, "d", `{"title":42}`)
	putDoc(t, c, "e", `{"other":"apple fuji"}`)

	info, err := c.CreateIndex(IndexDefinition{Name: "title_tin", Paths: []string{"/title"}, Kind: IndexTin})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if info.Kind != IndexTin || info.Name != "title_tin" || info.ColumnCount != 1 || info.Columns[0] != "/title" {
		t.Fatalf("info = %+v", info)
	}

	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ix, err := c.TinIndex(snap, "title_tin")
	if err != nil {
		t.Fatalf("TinIndex: %v", err)
	}
	q, err := ix.ParseTINQL(`"fuji apple"`)
	if err != nil {
		t.Fatal(err)
	}
	// DocIDs pack chunk<<32|slot; with few docs there is one chunk, so
	// matching docs are exactly the two title hits (d is numeric, e has no
	// title path).
	matched := ix.Match(q, nil)
	if len(matched) != 1 {
		t.Fatalf("phrase matched %v, want 1 doc (chunk0 slot0)", matched)
	}

	// The snapshot catalog advertises the tin index for binders.
	var infos []IndexInfo
	infos = snap.AppendIndexes(infos)
	found := false
	for _, in := range infos {
		if in.Name == "title_tin" && in.Kind == IndexTin {
			found = true
		}
	}
	if !found {
		t.Fatalf("catalog = %+v, want title_tin", infos)
	}

	// Writes publish a new state; the sidecar rebuilds for it.
	putDoc(t, c, "f", `{"title":"fuji apple crumble"}`)
	snap2, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ix2, err := c.TinIndex(snap2, "title_tin")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(ix2.Match(q, nil)); got != 2 {
		t.Fatalf("after write matched %d, want 2", got)
	}

	// Drop removes the definition; the old snapshot's index still answers
	// through its own reference.
	if err := c.DropIndex("title_tin"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if _, err := c.TinIndex(snap2, "title_tin"); err != ErrIndexNotFound {
		t.Fatalf("TinIndex after drop = %v", err)
	}
	if got := len(ix2.Match(q, nil)); got != 2 {
		t.Fatalf("dropped snapshot index matched %d, want 2", got)
	}
}

func TestTinIndexValidation(t *testing.T) {
	c := &Collection{}
	for _, def := range []IndexDefinition{
		{Name: "", Paths: []string{"/t"}, Kind: IndexTin},
		{Name: "x", Paths: nil, Kind: IndexTin},
		{Name: "x", Paths: []string{"/a", "/b"}, Kind: IndexTin},
		{Name: "x", Paths: []string{"/t"}, Kind: IndexTin, Unique: true},
		{Name: "x", Paths: []string{"/~x"}, Kind: IndexTin},
	} {
		if _, err := c.CreateIndex(def); err == nil {
			t.Fatalf("CreateIndex(%+v) succeeded", def)
		}
	}
	if _, err := c.CreateIndex(IndexDefinition{Name: "ok", Paths: []string{"/t"}, Kind: IndexTin}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateIndex(IndexDefinition{Name: "ok", Paths: []string{"/t"}}); err != ErrIndexExists {
		t.Fatalf("exact over tin name: %v", err)
	}
	if _, err := c.CreateIndex(IndexDefinition{Name: "ok", Paths: []string{"/t"}, Kind: IndexTin}); err != ErrIndexExists {
		t.Fatalf("tin over tin name: %v", err)
	}
	if _, err := c.BackfillIndex("ok", 0); err != nil {
		t.Fatalf("BackfillIndex: %v", err)
	}
	if _, err := c.BackfillIndex("missing", 0); err != ErrIndexNotFound {
		t.Fatalf("BackfillIndex missing: %v", err)
	}
}

func TestTinIndexExpansions(t *testing.T) {
	c := &Collection{}
	putDoc(t, c, "a", `{"title":"apple application"}`)
	putDoc(t, c, "b", `{"title":"peach"}`)
	if _, err := c.CreateIndex(IndexDefinition{Name: "t", Paths: []string{"/title"}, Kind: IndexTin}); err != nil {
		t.Fatal(err)
	}
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ix, err := c.TinIndex(snap, "t")
	if err != nil {
		t.Fatal(err)
	}
	for input, want := range map[string]int{
		`appl*`: 1, `apple~1`: 1, `aardvark TO peach`: 2, `MATCHES peach`: 1,
	} {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("%s: %v", input, err)
		}
		if got := len(ix.Match(q, nil)); got != want {
			t.Fatalf("%s matched %d, want %d", input, got, want)
		}
	}
}
