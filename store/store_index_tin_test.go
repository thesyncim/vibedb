package store

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/tin"
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

func TestTinSearchGoAPI(t *testing.T) {
	c := &Collection{}
	putDoc(t, c, "a", `{"title":"fuji apple pie"}`)
	putDoc(t, c, "b", `{"title":"apple fuji tart"}`)
	putDoc(t, c, "c", `{"title":"lazy dog"}`)
	if _, err := c.CreateIndex(IndexDefinition{
		Name: "title_tin", Paths: []string{"/title"}, Kind: IndexTin,
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	hits, err := c.TinSearch(snap, "/title", "apple", 10)
	if err != nil {
		t.Fatalf("TinSearch: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %+v, want 2", hits)
	}
	keys := map[string]float64{hits[0].Key: hits[0].Score, hits[1].Key: hits[1].Score}
	if _, ok := keys["a"]; !ok {
		t.Fatalf("hits = %+v, want key a", hits)
	}
	if _, ok := keys["b"]; !ok {
		t.Fatalf("hits = %+v, want key b", hits)
	}
	for _, h := range hits {
		if h.Score <= 0 {
			t.Fatalf("hit %+v has non-positive score", h)
		}
	}
	if hits[0].Score < hits[1].Score {
		t.Fatalf("hits not score-ordered: %+v", hits)
	}
	if one, err := c.TinSearch(snap, "/title", "apple", 1); err != nil || len(one) != 1 {
		t.Fatalf("topK=1 = (%+v, %v), want 1 hit", one, err)
	}
	if none, err := c.TinSearch(snap, "/title", "apple", 0); err != nil || none != nil {
		t.Fatalf("topK=0 = (%+v, %v), want nil", none, err)
	}
	if _, err := c.TinSearch(snap, "/missing", "apple", 10); err != ErrIndexNotFound {
		t.Fatalf("unindexed path = %v, want %v", err, ErrIndexNotFound)
	}
	if _, err := c.TinSearch(snap, "/title", `"unclosed`, 10); err == nil {
		t.Fatal("invalid TINQL = nil, want parse error")
	}
	// Snapshot-pinned builds: the old snapshot never sees the new doc.
	putDoc(t, c, "d", `{"title":"apple turnover"}`)
	fresh, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if stale, err := c.TinSearch(snap, "/title", "apple", 10); err != nil || len(stale) != 2 {
		t.Fatalf("stale snapshot = (%+v, %v), want 2 hits", stale, err)
	}
	if grew, err := c.TinSearch(fresh, "/title", "apple", 10); err != nil || len(grew) != 3 {
		t.Fatalf("fresh snapshot = (%+v, %v), want 3 hits", grew, err)
	}
}

// Two tin definitions over one path hold identical content, but path
// resolution must still pick one deterministically: Go map iteration order
// is random, so TinIndexForPath resolves to the smallest catalog name,
// matching the name-sorted catalog. Resolving a hundred times must yield
// one index object: the "aa_tin" build, never a "zz_tin" twin.
func TestTinIndexForPathResolvesSmallestName(t *testing.T) {
	c := &Collection{}
	putDoc(t, c, "a", `{"title":"fuji apple pie"}`)
	for _, name := range []string{"zz_tin", "aa_tin"} {
		if _, err := c.CreateIndex(IndexDefinition{
			Name: name, Paths: []string{"/title"}, Kind: IndexTin,
		}); err != nil {
			t.Fatalf("CreateIndex(%s): %v", name, err)
		}
	}
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want, err := c.TinIndex(snap, "aa_tin")
	if err != nil {
		t.Fatalf("TinIndex(aa_tin): %v", err)
	}
	for range 100 {
		got, err := c.TinIndexForPath(snap, "/title")
		if err != nil {
			t.Fatalf("TinIndexForPath: %v", err)
		}
		if got != want {
			t.Fatal("TinIndexForPath resolved outside the smallest catalog name")
		}
	}
}

// TestTinSegmentsUnion proves sidecar segments union exactly: the same
// corpus searched through four segment indexes matches the single index
// hit for hit and score for score, across deletes, non-string bodies,
// ties, and every match shape. It also pins the per-State cache (second
// call shares the builds) and the clamp of oversized segment counts.
func TestTinSegmentsUnion(t *testing.T) {
	c := &Collection{}
	for i := range 200 {
		key := fmt.Sprintf("k%04d", i)
		// alpha everywhere; beta in every 5th (selective, like the
		// SQL bench's zipf); gamma ties pair the residues.
		var body string
		switch i % 5 {
		case 0:
			body = `"alpha beta"`
		case 1, 2:
			body = `"alpha gamma"`
		default:
			body = `"alpha"`
		}
		if i%9 == 0 {
			body = `42`
		}
		putDoc(t, c, key, `{"title":`+body+`}`)
	}
	for i := 0; i < 200; i += 11 {
		if _, err := c.Delete(fmt.Sprintf("k%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateIndex(IndexDefinition{Name: "title_tin", Paths: []string{"/title"}, Kind: IndexTin}); err != nil {
		t.Fatal(err)
	}
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	segs, err := c.TinSegmentsForPath(snap, "/title", 4)
	if err != nil {
		t.Fatalf("TinSegmentsForPath: %v", err)
	}
	if len(segs) != 4 {
		t.Fatalf("segments = %d, want 4 (200 docs over 64-doc chunks)", len(segs))
	}
	for i, s := range segs {
		if s == nil {
			t.Fatalf("segment %d is nil", i)
		}
	}
	single, err := c.TinIndexForPath(snap, "/title")
	if err != nil {
		t.Fatalf("TinIndexForPath: %v", err)
	}
	shards := make([]tin.Shard, len(segs))
	for i := range segs {
		shards[i].Ix = segs[i]
	}
	mustParse := func(pattern string) tin.Query {
		t.Helper()
		q, err := single.ParseTINQL(pattern)
		if err != nil {
			t.Fatalf("parse %s: %v", pattern, err)
		}
		return q
	}
	for _, pattern := range []string{
		"alpha", "beta AND alpha", "gamma OR beta", `"alpha beta"`,
		"alpha NEAR/1 beta", "alph*", "alpha~1", "MATCHES al.*a",
		"aab TO aac", "missing",
	} {
		q := mustParse(pattern)
		got := tin.MatchGathered(shards, q, nil)
		want := single.Match(q, nil)
		if len(got) != len(want) {
			t.Fatalf("%s: %d docs, want %d", pattern, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s doc %d = %d, want %d", pattern, i, got[i], want[i])
			}
		}
	}
	for _, pattern := range []string{"alpha", "beta AND alpha", "alpha^2", "missing"} {
		q := mustParse(pattern)
		for _, topK := range []int{1, 5, 25} {
			got, ok := tin.ScoreSegmented(shards, q, topK, nil)
			if !ok {
				t.Fatalf("%s topK=%d declined", pattern, topK)
			}
			want := single.Score(q, topK, nil)
			if len(got) != len(want) {
				t.Fatalf("%s topK=%d: %d hits, want %d", pattern, topK, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("%s topK=%d hit %d = %+v, want %+v", pattern, topK, i, got[i], want[i])
				}
			}
		}
	}
	again, err := c.TinSegmentsForPath(snap, "/title", 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := range segs {
		if again[i] != segs[i] {
			t.Fatalf("segment %d not shared across calls", i)
		}
	}
	clamped, err := c.TinSegmentsForPath(snap, "/title", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(clamped) != len(segs) {
		t.Fatalf("clamped segments = %d, want %d", len(clamped), len(segs))
	}
}
