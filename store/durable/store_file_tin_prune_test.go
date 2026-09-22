package durable

import (
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
)

// TestTinBuildMasksRoundTrip proves the whole probe against a live router:
// Match ordinals through AppendCandidateMasks visit exactly the TinSearch
// key set, masks ascend strictly, and the key join published a table.
// Twelve filler documents keep 2 hits under the quarter decline.
func TestTinBuildMasksRoundTrip(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	options := testDatabaseOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	}
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, docs, "one", `{"body":"luxury goods and vintage watches"}`)
	mustPut(t, docs, "three", `{"body":"luxury watches","id":3}`)
	for i := 0; i < 10; i++ {
		mustPut(t, docs, fmt.Sprintf("filler%02d", i), `{"body":"cheap ordinary goods"}`)
	}
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	build, err := snap.TinBuildForPath("/body")
	if err != nil {
		t.Fatal(err)
	}
	if build.Index() == nil {
		t.Fatal("nil index")
	}
	q, err := build.Index().ParseTINQL("luxury")
	if err != nil {
		t.Fatal(err)
	}
	ids := build.Index().Match(q, nil)
	if len(ids) != 2 {
		t.Fatalf("Match(luxury) = %d ids, want 2", len(ids))
	}
	masks, hits, ok := build.AppendCandidateMasks(snap.PrimaryRouter(), ids, nil, nil)
	if !ok {
		t.Fatal("probe declined a prunable build")
	}
	if len(masks) == 0 {
		t.Fatal("no masks for 2 hits")
	}
	for i := 1; i < len(masks); i++ {
		if masks[i].Chunk <= masks[i-1].Chunk {
			t.Fatalf("masks not strictly ascending: %+v", masks)
		}
	}
	var keys []string
	if err := snap.RangeMasksRaw(masks, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "one" || keys[1] != "three" {
		t.Fatalf("masked keys = %q, want [one three]", keys)
	}
	// Empty matches visit nothing with ok=true: bounded nothing-matches.
	none, err := build.Index().ParseTINQL("aardvark")
	if err != nil {
		t.Fatal(err)
	}
	masks, _, ok = build.AppendCandidateMasks(
		snap.PrimaryRouter(), build.Index().Match(none, nil), nil, nil,
	)
	if !ok || len(masks) != 0 {
		t.Fatalf("empty match = %+v, %v; want no masks, ok", masks, ok)
	}
	// A full-corpus match declines: masking everything costs more than the
	// sequential scan it replaces.
	all, err := build.Index().ParseTINQL("*")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok = build.AppendCandidateMasks(
		snap.PrimaryRouter(), build.Index().Match(all, nil), nil, nil,
	); ok {
		t.Fatal("full-corpus match did not decline")
	}
	// Missing declaration reports ErrIndexNotFound, never a nil build.
	if _, err := snap.TinBuildForPath("/nope"); !errors.Is(err, store.ErrIndexNotFound) {
		t.Fatalf("missing path err = %v, want ErrIndexNotFound", err)
	}
	// Warmed probe allocates nothing: table lookups index retained
	// arrays, the fold sorts in place, masks append into dst.
	dst, hits, ok := build.AppendCandidateMasks(snap.PrimaryRouter(), ids, nil, hits[:0])
	if !ok {
		t.Fatal("warmup declined")
	}
	if n := testing.AllocsPerRun(20, func() {
		dst, hits, _ = build.AppendCandidateMasks(snap.PrimaryRouter(), ids, dst[:0], hits[:0])
	}); n != 0 {
		t.Fatalf("probe allocated %v per run, want 0", n)
	}
	_ = dst
	_ = hits
}

// TestTinBuildDeclinesDenseHits proves the cost heuristic: 2 hits over 3
// rows exceed a quarter of the corpus, so the probe declines and the scan
// runs whole.
func TestTinBuildDeclinesDenseHits(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	build, err := snap.TinBuildForPath("/body")
	if err != nil {
		t.Fatal(err)
	}
	q, err := build.Index().ParseTINQL("luxury")
	if err != nil {
		t.Fatal(err)
	}
	ids := build.Index().Match(q, nil)
	if len(ids) != 2 {
		t.Fatalf("Match(luxury) = %d ids, want 2", len(ids))
	}
	if _, _, ok := build.AppendCandidateMasks(snap.PrimaryRouter(), ids, nil, nil); ok {
		t.Fatal("dense hit set did not decline")
	}
}

// TestTinAppendMasksDeclines pins every fail-closed path that needs no
// router: nil builds, unmapped builds, out-of-range ordinals, and
// out-of-order ordinals all decline to the full scan.
func TestTinAppendMasksDeclines(t *testing.T) {
	_, docs := openTinDatabase(t)
	snap, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	router := snap.PrimaryRouter()
	table := &TinBuild{slots: &tinSlotMap{
		slots: []TinSlotHit{{Chunk: 0, Bit: 1}, {Chunk: 0, Bit: 3}},
		rows:  2,
	}}
	// 2 hits over 2 rows trips the quarter decline first.
	if _, _, ok := table.AppendCandidateMasks(router,
		[]tin.DocID{0, 1}, nil, nil,
	); ok {
		t.Fatal("dense synthetic hit set did not decline")
	}
	wide := &TinBuild{slots: &tinSlotMap{
		slots: make([]TinSlotHit, 12),
		rows:  12,
	}}
	for i := range wide.slots.slots {
		wide.slots.slots[i] = TinSlotHit{Chunk: uint32(i), Bit: 0}
	}
	if _, _, ok := wide.AppendCandidateMasks(router,
		[]tin.DocID{0, 11, 12}, nil, nil,
	); ok {
		t.Fatal("out-of-range ordinal did not decline")
	}
	if _, _, ok := wide.AppendCandidateMasks(router,
		[]tin.DocID{5, 2}, nil, nil,
	); ok {
		t.Fatal("out-of-order ordinals did not decline")
	}
	var nilBuild *TinBuild
	if _, _, ok := nilBuild.AppendCandidateMasks(router,
		[]tin.DocID{0}, nil, nil,
	); ok {
		t.Fatal("nil build did not decline")
	}
	if _, _, ok := table.AppendCandidateMasks(nil,
		[]tin.DocID{0}, nil, nil,
	); ok {
		t.Fatal("nil router did not decline")
	}
}
