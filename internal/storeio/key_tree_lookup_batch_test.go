package storeio

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func seedKeyTreeLookupBatch(t testing.TB, count int) (*keyTreeHarness, []KeyTreeEdit) {
	t.Helper()
	h := newKeyTreeHarnessPages(t, 1024, 2048)
	edits := make([]KeyTreeEdit, count)
	for i := range edits {
		edits[i] = KeyTreeEdit{
			Key: fmt.Appendf(
				nil, "%04d-%s", i,
				strings.Repeat(string(rune('a'+i%26)), 40+i%180),
			),
			Location: KeyLocation{
				Chunk: uint32(i), Slot: uint8(i % 64),
			},
		}
	}
	h.batchMutate(edits, 1024)
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	return h, edits
}

// TestLookupKeyTreeBatchMatchesPointLookup is the differential oracle for
// batched resolution. Present keys and gaps spanning the whole tree must agree
// with the unchanged point-lookup path, including keys outside the tree's
// inclusive lower and upper bounds.
func TestLookupKeyTreeBatchMatchesPointLookup(t *testing.T) {
	h, edits := seedKeyTreeLookupBatch(t, 320)
	keys := make([]string, 0, 2*len(edits)+2)
	keys = append(keys, "", "zzzz-after-tree")
	for i, edit := range edits {
		keys = append(keys, string(edit.Key), fmt.Sprintf("%04d-missing", i))
	}
	sort.Strings(keys)
	keys = compactStrings(keys)
	lookups := make([]KeyTreeLookup, len(keys))
	for i := range keys {
		lookups[i].Key = []byte(keys[i])
	}
	if err := LookupKeyTreeBatch(h.cache, h.root, lookups, h.bounds); err != nil {
		t.Fatal(err)
	}
	for i, lookup := range lookups {
		want, wantOK := h.lookup(keys[i])
		if lookup.Found != wantOK || lookup.Location != want {
			t.Fatalf(
				"LookupKeyTreeBatch(%q) = (%+v,%v), want (%+v,%v)",
				keys[i], lookup.Location, lookup.Found, want, wantOK,
			)
		}
	}
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	at := 1
	for _, value := range values[1:] {
		if value == values[at-1] {
			continue
		}
		values[at] = value
		at++
	}
	return values[:at]
}

// TestLookupKeyTreeBatchAcquiresSharedPagesOnce pins the performance invariant
// independently of wall-clock noise. Once resident, point lookup acquires one
// root-to-leaf path per key; batch lookup acquires every covered page once.
func TestLookupKeyTreeBatchAcquiresSharedPagesOnce(t *testing.T) {
	h, edits := seedKeyTreeLookupBatch(t, 256)
	lookups := make([]KeyTreeLookup, len(edits))
	for i := range edits {
		lookups[i].Key = edits[i].Key
		if _, ok := h.lookup(string(edits[i].Key)); !ok {
			t.Fatalf("warm lookup %d missing", i)
		}
	}

	beforeBatch := h.cache.Stats().CacheHits
	if err := LookupKeyTreeBatch(h.cache, h.root, lookups, h.bounds); err != nil {
		t.Fatal(err)
	}
	batchHits := h.cache.Stats().CacheHits - beforeBatch

	beforePoints := h.cache.Stats().CacheHits
	for _, edit := range edits {
		if _, ok := h.lookup(string(edit.Key)); !ok {
			t.Fatalf("point lookup %q missing", edit.Key)
		}
	}
	pointHits := h.cache.Stats().CacheHits - beforePoints
	if batchHits >= pointHits/4 {
		t.Fatalf(
			"batch acquired %d resident pages, point lookups acquired %d",
			batchHits, pointHits,
		)
	}
	if pinned := h.cache.Stats().PinnedPages; pinned != 0 {
		t.Fatalf("batch left %d pages pinned", pinned)
	}
}

func TestLookupKeyTreeBatchRejectsUnsortedOrDuplicateKeys(t *testing.T) {
	h, _ := seedKeyTreeLookupBatch(t, 8)
	for name, lookups := range map[string][]KeyTreeLookup{
		"unsorted":  {{Key: []byte("b")}, {Key: []byte("a")}},
		"duplicate": {{Key: []byte("a")}, {Key: []byte("a")}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := LookupKeyTreeBatch(h.cache, h.root, lookups, h.bounds); err == nil {
				t.Fatalf("%s lookup batch accepted", name)
			}
		})
	}
}

func TestLookupKeyTreeBatchWarmSteadyAllocation(t *testing.T) {
	h, edits := seedKeyTreeLookupBatch(t, 64)
	lookups := make([]KeyTreeLookup, len(edits))
	for i := range edits {
		lookups[i].Key = edits[i].Key
	}
	if err := LookupKeyTreeBatch(h.cache, h.root, lookups, h.bounds); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := LookupKeyTreeBatch(h.cache, h.root, lookups, h.bounds); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm key-tree batch lookup allocations = %g, want 0", allocs)
	}
}

// BenchmarkLookupKeyTreeBatch isolates mutation planning from page rewriting:
// both arms resolve the same resident keys, and only the number of shared tree
// descents differs.
func BenchmarkLookupKeyTreeBatch(b *testing.B) {
	h, edits := seedKeyTreeLookupBatch(b, 512)
	lookups := make([]KeyTreeLookup, len(edits))
	for i := range edits {
		lookups[i].Key = edits[i].Key
	}
	b.Run("point-loop", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for i := range lookups {
				location, found, err := LookupKeyTree(
					h.cache, h.root, lookups[i].Key, h.bounds,
				)
				if err != nil || !found || location != edits[i].Location {
					b.Fatalf("lookup %d = (%+v,%v,%v)", i, location, found, err)
				}
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*len(lookups)),
			"ns/key",
		)
	})
	b.Run("batch", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := LookupKeyTreeBatch(h.cache, h.root, lookups, h.bounds); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*len(lookups)),
			"ns/key",
		)
	})
}
