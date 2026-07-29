package durable

import (
	"fmt"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Spanned term-leaf coverage (docs/design/indexed-write-path.md §6): fold
// identity ACROSS CUT BOUNDARIES on a corpus large enough that the exact
// root v1 catalog genuinely spans — giant terms as stripe pieces and a
// multi-run packed index — plus the dirty-leaf fold's page-reuse contract
// (untouched leaves keep their durable pages; only dirty ones re-stage) and
// the stripe-patch locality of a still-giant term.

// spannedTestDoc carries two indexed fields: kind has three values (giant
// terms — one term's postings are a third of the corpus, far past one 64 KiB
// leaf at this scale) and group has 240 (a packed multi-run leaf set).
func spannedTestDoc(at, kindOf int) []byte {
	return fmt.Appendf(nil,
		`{"id":%d,"kind":"k%d","group":%d,"pad":"xxxxxxxxxxxxxxxx"}`,
		at, kindOf, at%240)
}

func spannedTestKey(at int) string {
	return fmt.Sprintf("sp-key-%08d", at)
}

func spannedTestOptions() Options {
	return Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "kind", Paths: []string{"/kind"}},
			{Name: "group", Paths: []string{"/group"}},
		},
	}
}

// forceSpannedTestBudget narrows the in-memory cutter/staging budget after the
// fixed-format 64 KiB bulk build. The durable root still advertises and admits
// the format's 64 KiB maximum; the next dirty fold simply chooses smaller,
// fully valid leaf extents. Touching all low-cardinality terms below forces
// their complete run set through this budget without a 250k-row fixture.
func forceSpannedTestBudget(coll *Collection) {
	coll.options.MaxPageSize = 4096
}

// spannedLeafShape summarizes one index's resident leaf set.
func spannedLeafShape(r *primaryExactResident) (leaves, pieces int) {
	for l := range r.leaves {
		if r.leaves[l].piece {
			pieces++
		}
	}
	return len(r.leaves), pieces
}

// spannedLeafRefs snapshots the durable page refs of one index's leaves.
func spannedLeafRefs(r *primaryExactResident) []storeio.PageRef {
	refs := make([]storeio.PageRef, len(r.leaves))
	for l := range r.leaves {
		refs[l] = r.leaves[l].ref
	}
	return refs
}

func TestPrimaryExactQuietStagingMetadataIsPrivate(t *testing.T) {
	oldLeaf := storeio.PageRef{
		Offset: 4096, LogicalID: 10, Generation: 2,
		Length: 4096, Kind: storeio.PagePrimaryExactLeaf,
	}
	oldCatalog := storeio.PageRef{
		Offset: 8192, LogicalID: 11, Generation: 2,
		Length: 4096, Kind: storeio.PagePrimaryExactCatalog,
	}
	installed := []primaryExactResident{{
		leaves:  []primaryExactLeaf{{ref: oldLeaf}},
		catalog: []storeio.PageRef{oldCatalog},
	}}
	staged := clonePrimaryExactResidentsForStaging(installed)
	newLeaf, newCatalog := oldLeaf, oldCatalog
	newLeaf.LogicalID, newLeaf.Offset = 20, 12288
	newCatalog.LogicalID, newCatalog.Offset = 21, 16384
	staged[0].leaves[0].ref = newLeaf
	staged[0].catalog[0] = newCatalog
	if installed[0].leaves[0].ref != oldLeaf ||
		installed[0].catalog[0] != oldCatalog {
		t.Fatal("fallible staging mutated installed exact metadata")
	}
	installPrimaryExactDurableMetadata(installed, staged)
	if installed[0].leaves[0].ref != newLeaf ||
		installed[0].catalog[0] != newCatalog {
		t.Fatal("published staging metadata was not installed")
	}
}

// TestFilePrimaryIndexedSpannedFoldIdentity is the P1 identity gate: a
// corpus whose low-cardinality index fails closed under the v0 single-leaf
// format builds spanned, answers probes mid-window and post-fold against an
// oracle, and every fold — including dirty-leaf folds that carry most leaves
// by reference — produces the byte-identical spanned leaf set a from-scratch
// graph rebuild produces, live and after reopen.
func TestFilePrimaryIndexedSpannedFoldIdentity(t *testing.T) {
	const documents = 30_000
	options := spannedTestOptions()
	docs := make(map[string][]byte, documents)
	kinds := make(map[string]int, documents)
	groups := make(map[string]int, documents)
	for at := 0; at < documents; at++ {
		key := spannedTestKey(at)
		docs[key] = spannedTestDoc(at, at%3)
		kinds[key] = at % 3
		groups[key] = at % 240
	}
	coll := buildIndexedPrimaryFile(t, t.TempDir(), "spanned-*", docs, options)
	for at := 0; at < 240; at++ {
		key := spannedTestKey(at)
		nextKind := (at + 1) % 3
		nextGroup := (at + 1) % 240
		value := fmt.Appendf(nil,
			`{"id":%d,"kind":"k%d","group":%d,"pad":"xxxxxxxxxxxxxxxx"}`,
			at, nextKind, nextGroup)
		if _, err := coll.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
		docs[key] = value
		kinds[key] = nextKind
		groups[key] = nextGroup
	}
	forceSpannedTestBudget(coll)
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}

	kindID := coll.options.indexNameIDs["kind"]
	groupID := coll.options.indexNameIDs["group"]
	kindLeaves, kindPieces := spannedLeafShape(
		&coll.primaryEpoch.exact[kindID],
	)
	groupLeaves, _ := spannedLeafShape(
		&coll.primaryEpoch.exact[groupID],
	)
	if kindPieces == 0 {
		t.Fatalf("low-cardinality index emitted no stripe pieces (%d leaves); the giant-term path is uncovered", kindLeaves)
	}
	if groupLeaves < 2 {
		t.Fatalf("group index spans %d leaves; the packed multi-leaf path is uncovered", groupLeaves)
	}
	assertFoldMatchesRebuild(t, coll, "bulk build")

	checkKind := func(when string) {
		t.Helper()
		for kind := 0; kind < 3; kind++ {
			got := primaryExactTestKeys(
				t, coll, "kind",
				primaryExactTestNeedle(t, fmt.Sprintf(`"k%d"`, kind)),
			)
			slices.Sort(got)
			want := make([]string, 0, documents/3)
			for key, value := range kinds {
				if value == kind {
					want = append(want, key)
				}
			}
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: kind=k%d got %d keys want %d",
					when, kind, len(got), len(want))
			}
		}
		for _, group := range []int{0, 7, 100, 239} {
			got := primaryExactTestKeys(
				t, coll, "group",
				primaryExactTestNeedle(t, fmt.Sprintf("%d", group)),
			)
			slices.Sort(got)
			want := make([]string, 0, documents/240)
			for key, value := range groups {
				if value == group {
					want = append(want, key)
				}
			}
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: group=%d got %d keys want %d",
					when, group, len(got), len(want))
			}
		}
	}
	checkKind("post-build")

	// Buffered mutations across both indexes: value moves between giant
	// terms, deletes, and inserts into fresh groups. Probe mid-window (the
	// overlay merged path over spanned leaves), then fold and pin identity.
	put := func(key string, at, kindOf, group int) {
		t.Helper()
		doc := fmt.Appendf(nil,
			`{"id":%d,"kind":"k%d","group":%d,"pad":"xxxxxxxxxxxxxxxx"}`,
			at, kindOf, group)
		if _, err := coll.Put([]byte(key), doc); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		kinds[key] = kindOf
		groups[key] = group
	}
	for at := 0; at < 300; at += 3 {
		put(spannedTestKey(at), at, (at+1)%3, (at*7)%240)
	}
	for at := 1; at < 200; at += 13 {
		if _, err := coll.Delete([]byte(spannedTestKey(at))); err != nil {
			t.Fatal(err)
		}
		delete(kinds, spannedTestKey(at))
		delete(groups, spannedTestKey(at))
	}
	for at := documents; at < documents+50; at++ {
		put(spannedTestKey(at), at, at%3, 500+at%10)
	}
	if coll.primaryEpoch.overlayEmpty() {
		t.Fatal("mutations emitted no overlay records")
	}
	midWindow := probeKeysMidWindow(t, coll, "kind", `"k1"`)
	wantMid := make([]string, 0, documents/3)
	for key, value := range kinds {
		if value == 1 {
			wantMid = append(wantMid, key)
		}
	}
	slices.Sort(wantMid)
	if !slices.Equal(midWindow, wantMid) {
		t.Fatalf("mid-window kind=k1: got %d keys want %d",
			len(midWindow), len(wantMid))
	}

	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if !coll.primaryEpoch.overlayEmpty() {
		t.Fatal("fold left overlay records behind")
	}
	assertFoldMatchesRebuild(t, coll, "post-fold")
	checkKind("post-fold")

	// Dirty-leaf page reuse: a single localized mutation folds by carrying
	// almost every leaf's durable page and re-staging only the dirty ones.
	beforeKind := spannedLeafRefs(&coll.primaryEpoch.exact[kindID])
	beforeGroup := spannedLeafRefs(&coll.primaryEpoch.exact[groupID])
	put(spannedTestKey(42), 42, (kinds[spannedTestKey(42)]+1)%3, 41)
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	assertFoldMatchesRebuild(t, coll, "post-localized-fold")
	afterKind := spannedLeafRefs(&coll.primaryEpoch.exact[kindID])
	afterGroup := spannedLeafRefs(&coll.primaryEpoch.exact[groupID])
	carried := func(before, after []storeio.PageRef) (kept, total int) {
		seen := make(map[storeio.PageRef]bool, len(before))
		for _, ref := range before {
			seen[ref] = true
		}
		for _, ref := range after {
			if seen[ref] {
				kept++
			}
		}
		return kept, len(after)
	}
	keptKind, totalKind := carried(beforeKind, afterKind)
	keptGroup, totalGroup := carried(beforeGroup, afterGroup)
	if keptKind == 0 || keptKind == totalKind {
		t.Fatalf("kind fold carried %d of %d leaf pages; want a proper dirty subset",
			keptKind, totalKind)
	}
	if keptGroup == 0 {
		t.Fatalf("group fold carried %d of %d leaf pages", keptGroup, totalGroup)
	}

	// Reopen: the v1 root and spanned catalogs rehydrate to the identical
	// resident leaf set and keep answering.
	file := coll.file
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen spanned store: %v", err)
	}
	defer reopened.Close()
	for indexID := range reopened.primaryEpoch.exact {
		fresh := &reopened.primaryEpoch.exact[indexID]
		old := &coll.primaryEpoch.exact[indexID]
		if len(fresh.leaves) != len(old.leaves) {
			t.Fatalf("index %d reopened with %d leaves, had %d",
				indexID, len(fresh.leaves), len(old.leaves))
		}
		for l := range fresh.leaves {
			if !slices.Equal(fresh.leaves[l].encoded, old.leaves[l].encoded) {
				t.Fatalf("index %d leaf %d bytes differ after reopen", indexID, l)
			}
		}
	}
	coll = reopened
	checkKind("reopened")
}

// TestFilePrimaryIndexedSpannedStripePatch pins the giant-term stripe patch:
// a mutation touching one stripe of a term that stays giant re-encodes only
// that stripe's piece leaves — every other piece keeps its bytes AND its
// durable page — and the patched index still byte-matches the from-scratch
// rebuild.
func TestFilePrimaryIndexedSpannedStripePatch(t *testing.T) {
	const documents = 30_000
	options := spannedTestOptions()
	options.Indexes = options.Indexes[:1] // kind only: all giant terms
	docs := make(map[string][]byte, documents)
	for at := 0; at < documents; at++ {
		docs[spannedTestKey(at)] = spannedTestDoc(at, at%3)
	}
	coll := buildIndexedPrimaryFile(
		t, t.TempDir(), "spanned-stripe-*", docs, options,
	)
	for at := 0; at < 3; at++ {
		if _, err := coll.Put(
			[]byte(spannedTestKey(at)),
			spannedTestDoc(at, (at+1)%3),
		); err != nil {
			t.Fatal(err)
		}
	}
	forceSpannedTestBudget(coll)
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	resident := &coll.primaryEpoch.exact[0]
	leaves, pieces := spannedLeafShape(resident)
	if pieces < 6 {
		t.Fatalf("corpus produced %d pieces over %d leaves; stripe patch needs several stripes", pieces, leaves)
	}
	before := spannedLeafRefs(resident)
	beforeBytes := make([][]byte, len(resident.leaves))
	for l := range resident.leaves {
		beforeBytes[l] = resident.leaves[l].encoded
	}

	// One value move between two giant terms: both terms stay giant, so the
	// fold must take the stripe-patch path, dirtying at most the pieces of
	// the one stripe the mutated row's bucket belongs to per term.
	key := []byte(spannedTestKey(9_000))
	doc := fmt.Appendf(nil,
		`{"id":%d,"kind":"k1","group":%d,"pad":"xxxxxxxxxxxxxxxx"}`,
		9_000, 9_000%240)
	if _, err := coll.Put(key, doc); err != nil {
		t.Fatal(err)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	assertFoldMatchesRebuild(t, coll, "post-stripe-patch")

	fresh := &coll.primaryEpoch.exact[0]
	seen := make(map[storeio.PageRef]bool, len(before))
	for _, ref := range before {
		seen[ref] = true
	}
	carried, restaged := 0, 0
	for l := range fresh.leaves {
		if seen[fresh.leaves[l].ref] {
			carried++
		} else {
			restaged++
		}
	}
	if restaged == 0 || restaged > 8 {
		t.Fatalf("stripe patch re-staged %d of %d leaves (carried %d); want a small dirty set",
			restaged, len(fresh.leaves), carried)
	}
	// Untouched pieces kept their exact bytes by reference, not by
	// re-encoding to equal bytes.
	byteCarried := 0
	for l := range fresh.leaves {
		for _, old := range beforeBytes {
			if len(old) == len(fresh.leaves[l].encoded) &&
				&old[0] == &fresh.leaves[l].encoded[0] {
				byteCarried++
				break
			}
		}
	}
	if byteCarried != carried {
		t.Fatalf("%d leaves carried pages but %d carried byte slices",
			carried, byteCarried)
	}
}
