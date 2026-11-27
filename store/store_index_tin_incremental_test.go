package store

import (
	"fmt"
	"testing"
)

func TestHeapTinIncrementalMaintenanceMatchesFullBuild(t *testing.T) {
	c := &Collection{}
	if _, err := c.CreateIndex(IndexDefinition{
		Name: "body_tin", Paths: []string{"/body"}, Kind: IndexTin,
	}); err != nil {
		t.Fatal(err)
	}
	// Populate across multiple 64-slot chunks so single-row writes touch only 1 chunk.
	const initialDocs = 320
	for i := 0; i < initialDocs; i++ {
		body := fmt.Sprintf("ordinary catalog item %d", i)
		if i%40 == 0 {
			body = fmt.Sprintf("luxury vintage watch item %d", i)
		}
		if _, err := c.Put(fmt.Sprintf("k%04d", i), []byte(fmt.Sprintf(`{"body":%q}`, body))); err != nil {
			t.Fatal(err)
		}
	}

	snap0, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	hits0, err := c.TinSearch(snap0, "/body", "luxury", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits0) != initialDocs/40 {
		t.Fatalf("initial hits = %d, want %d", len(hits0), initialDocs/40)
	}
	if c.tinFullBuilds != 1 || c.tinIncrementalBuilds != 0 {
		t.Fatalf("after initial build: full=%d inc=%d, want 1/0", c.tinFullBuilds, c.tinIncrementalBuilds)
	}

	// 1. Insert a new document.
	if _, err := c.Put("k9001", []byte(`{"body":"luxury sapphire watch"}`)); err != nil {
		t.Fatal(err)
	}
	snap1, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// 2. Update an existing document's text.
	if _, err := c.Put("k0000", []byte(`{"body":"ordinary replaced item"}`)); err != nil {
		t.Fatal(err)
	}
	snap2, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// 3. Replace a string field with non-string (must remove from tin index).
	if _, err := c.Put("k0040", []byte(`{"body":12345}`)); err != nil {
		t.Fatal(err)
	}
	snap3, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// 4. Delete an existing matching document.
	if _, err := c.Delete("k0080"); err != nil {
		t.Fatal(err)
	}
	snap4, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	for idx, snap := range []Snapshot{snap1, snap2, snap3, snap4} {
		for _, q := range []string{"luxury", "sapphire", "replaced", "vintage AND watch"} {
			got, err := c.TinSearch(snap, "/body", q, 50)
			if err != nil {
				t.Fatalf("snap%d TinSearch(%q): %v", idx+1, q, err)
			}
			// Compare against a fresh full build on the exact same snapshot state.
			freshIx, err := buildTinIndex(snap.state, c.tinDefs["body_tin"])
			if err != nil {
				t.Fatalf("snap%d buildTinIndex: %v", idx+1, err)
			}
			parsed, err := freshIx.ParseTINQL(q)
			if err != nil {
				t.Fatalf("snap%d ParseTINQL(%q): %v", idx+1, q, err)
			}
			wantScored := freshIx.Score(parsed, 50, nil)
			if len(got) != len(wantScored) {
				t.Fatalf("snap%d query %q: got %d hits, want %d", idx+1, q, len(got), len(wantScored))
			}
			for i := range got {
				chunk := snap.state.Chunks.Get(uint32(wantScored[i].Doc >> 32))
				slot := int(wantScored[i].Doc & 0xffffffff)
				wantKey := chunk.Key(slot)
				if got[i].Key != wantKey || got[i].Score != wantScored[i].Score {
					t.Fatalf("snap%d query %q hit %d: got=%+v want={Key:%q Score:%v}",
						idx+1, q, i, got[i], wantKey, wantScored[i].Score)
				}
			}
		}
	}

	if c.tinFullBuilds != 1 {
		t.Fatalf("tinFullBuilds = %d, want 1 (all later generations should be incremental)", c.tinFullBuilds)
	}
	if c.tinIncrementalBuilds != 4 {
		t.Fatalf("tinIncrementalBuilds = %d, want 4", c.tinIncrementalBuilds)
	}

	// Old pinned snapshot must remain isolated and return its original hits.
	hits0Again, err := c.TinSearch(snap0, "/body", "luxury", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits0Again) != len(hits0) {
		t.Fatalf("snap0 hits changed: got %d, want %d", len(hits0Again), len(hits0))
	}
}
