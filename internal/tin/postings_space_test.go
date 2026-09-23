package tin

import (
	"fmt"
	"testing"
)

// TestPostingsSpaceFootprint pins the columnar postings layout: ids (8B) +
// off boundaries (4B) + pos (4B), with term frequency derived from off
// boundaries so no frequency array may exist. The small fixture pins the
// exact total; the wide fixture shows the per-occurrence cost converges to
// 16 bytes as the one-extra-boundary-per-term amortizes away. If either
// fails, postings regressed in space.
func TestPostingsSpaceFootprint(t *testing.T) {
	ix := NewIndex()
	ix.Add(1, "a b c")
	ix.Add(2, "a b")
	ix.Add(3, "a")
	ix.ensureSorted()

	var total uint64
	for _, p := range ix.post {
		if uint64(len(p.off)) != uint64(len(p.ids))+1 {
			t.Fatalf("off boundaries %d != ids %d + 1", len(p.off), len(p.ids))
		}
		total += 8*uint64(len(p.ids)) + 4*uint64(len(p.off)) + 4*uint64(len(p.pos))
	}
	// a: 8*3+4*4+4*3=52; b: 8*2+4*3+4*2=36; c: 8+8+4=20.
	if total != 108 {
		t.Fatalf("postings footprint = %d bytes, want 108", total)
	}

	wide := NewIndex()
	for i := 1; i <= 100; i++ {
		wide.Add(DocID(i), fmt.Sprintf("common t%d", i))
	}
	wide.ensureSorted()
	var wids, woff, wpos uint64
	for _, p := range wide.post {
		// Find the "common" list: 100 documents, 100 occurrences.
		if len(p.ids) != 100 {
			continue
		}
		wids += uint64(len(p.ids))
		woff += uint64(len(p.off))
		wpos += uint64(len(p.pos))
	}
	if wpos != 100 {
		t.Fatalf("common occurrences = %d, want 100", wpos)
	}
	if per := (8*wids + 4*woff + 4*wpos) / wpos; per != 16 {
		t.Fatalf("wide bytes per occurrence = %d, want 16", per)
	}
}
