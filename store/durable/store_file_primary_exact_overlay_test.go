package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Overlay-specific coverage for the O(delta) indexed write path
// (docs/design/indexed-write-path.md §3, §7): the rebase group a
// slot-reassigning de-template emits, the mid-window read rule over a
// populated overlay, crash-replay across a rebase-before-fold window, the
// zero-allocation contract of the per-mutation record path, and the
// mid-window probe cost gate.

// primaryExactOverlayTestOptions builds a buffered-visible indexed
// configuration over the given index paths.
func primaryExactOverlayTestOptions(paths ...string) Options {
	return Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "group", Paths: paths},
		},
	}
}

// templateHeavyOverlayDoc is the redundant document shape that reliably
// template-compresses at bulk build (same shape the template-leaf test
// uses), with the indexed scalar drawn from a small group space so terms
// carry several postings.
func templateHeavyOverlayDoc(at int) []byte {
	return fmt.Appendf(nil,
		`{"id":%d,"kind":"document","group":%d,"active":%t,`+
			`"tier":"standard","region":"eu-west-1","name":"row %d"}`,
		at, at%37, at%3 == 0, at)
}

func templateHeavyOverlayKey(at int) string {
	return fmt.Sprintf("primary-key-%09d", at)
}

// buildTemplateHeavyOverlayCollection bulk-builds the redundant corpus and
// proves it actually produced at least one template leaf, so the de-template
// rebase path is genuinely exercised.
func buildTemplateHeavyOverlayCollection(
	t *testing.T, dir string, documents int, options Options,
) *Collection {
	t.Helper()
	docs := make(map[string][]byte, documents)
	for at := range documents {
		docs[templateHeavyOverlayKey(at)] = templateHeavyOverlayDoc(at)
	}
	coll := buildIndexedPrimaryFile(t, dir, "overlay-rebase-*", docs, options)
	router := coll.primaryRouter.Load()
	templates := 0
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatal("router row missing")
		}
		lease, err := router.AcquireLeaf(coll.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		if storeio.PrimaryLeafClass(lease.Page()) ==
			storeio.CommonPrimaryLeafTemplate {
			templates++
		}
		lease.Release()
	}
	if templates == 0 {
		t.Fatal("redundant corpus produced no template leaves; the rebase path would go uncovered")
	}
	return coll
}

// findTemplateLeafKeys returns the bucket and lexical keys of one currently
// template-classed leaf, so a mutation against any of them forces the
// de-template + slot reassignment the rebase group exists for.
func findTemplateLeafKeys(
	t *testing.T, coll *Collection,
) (storeio.BucketID, []string) {
	t.Helper()
	state := coll.state.Load()
	router := coll.primaryRouter.Load()
	bounds := coll.primaryLeafBounds(state)
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatal("router row missing")
		}
		lease, err := router.AcquireLeaf(coll.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		if storeio.PrimaryLeafClass(lease.Page()) !=
			storeio.CommonPrimaryLeafTemplate {
			lease.Release()
			continue
		}
		var keys []string
		_, err = storeio.VisitPrimaryLeafPostingRows(
			lease.Page(), coll.storeID, route.Bucket, bounds, nil,
			func(_ uint8, key, _ []byte, _ bool) error {
				keys = append(keys, string(key))
				return nil
			},
		)
		lease.Release()
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) >= 3 {
			return route.Bucket, keys
		}
	}
	t.Fatal("no template leaf found")
	return 0, nil
}

// rebuildPrimaryExactLeavesFromGraph re-derives every physical index's
// spanned canonical term-leaf set from the current graph alone — router,
// leaves, stable slots — through the same VisitPrimaryLeafPostingRows + term
// canonicalization + content-defined cutter + AppendIndexTermLeaf pipeline a
// bulk build uses. It is the from-scratch oracle the fold's byte-identity
// anchor (invariant 4) is asserted against: the fold's leaf set — including
// every cut boundary — must equal it exactly, independent of the mutation
// history that produced the graph. Per index it returns the ordered encoded
// leaves.
func rebuildPrimaryExactLeavesFromGraph(
	t testing.TB, coll *Collection,
) [][][]byte {
	t.Helper()
	state := coll.state.Load()
	router := coll.primaryRouter.Load()
	bounds := coll.primaryLeafBounds(state)
	live := make(map[uint32]*[storeio.TermPostingTileChunks]uint64)
	byIndex := make([]map[string]map[uint32]uint64, len(coll.options.indexes))
	for i := range byIndex {
		byIndex[i] = make(map[string]map[uint32]uint64)
	}
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	var scratch []byte
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatal("router row missing")
		}
		lease, err := router.AcquireLeaf(coll.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		scratch, err = storeio.VisitPrimaryLeafPostingRows(
			lease.Page(), coll.storeID, route.Bucket, bounds, scratch,
			func(slot uint8, _, raw []byte, overflow bool) error {
				if overflow {
					t.Fatal("overlay test corpus must stay inline")
				}
				tileID := uint32(route.Bucket)<<2 | uint32(slot>>6)
				bit := uint64(1) << uint(slot&63)
				mask := live[tileID]
				if mask == nil {
					mask = new([storeio.TermPostingTileChunks]uint64)
					live[tileID] = mask
				}
				mask[0] |= bit
				for indexID, exact := range coll.options.indexes {
					key, present, termErr := appendPrimaryExactDocumentTerm(
						canonical[:0], components[:], exact, raw,
					)
					if termErr != nil {
						return termErr
					}
					if !present {
						continue
					}
					tiles := byIndex[indexID][string(key)]
					if tiles == nil {
						tiles = make(map[uint32]uint64)
						byIndex[indexID][string(key)] = tiles
					}
					tiles[tileID] |= bit
				}
				return nil
			},
		)
		lease.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	table := newPrimaryLiveTable(live)
	out := make([][][]byte, len(byIndex))
	for i := range byIndex {
		leaves, err := coll.encodePrimaryExactLeaves(
			byIndex[i], live, table.lookup,
		)
		if err != nil {
			t.Fatal(err)
		}
		encoded := make([][]byte, len(leaves))
		for l := range leaves {
			encoded[l] = leaves[l].encoded
		}
		out[i] = encoded
	}
	return out
}

// assertFoldMatchesRebuild byte-compares the resident fold base — every
// spanned leaf, in catalog order — against the from-scratch graph rebuild.
func assertFoldMatchesRebuild(t *testing.T, coll *Collection, when string) {
	t.Helper()
	rebuilt := rebuildPrimaryExactLeavesFromGraph(t, coll)
	resident := coll.primaryEpoch.exact
	if len(rebuilt) != len(resident) {
		t.Fatalf("%s: index count %d vs rebuilt %d",
			when, len(resident), len(rebuilt))
	}
	for i := range resident {
		if len(resident[i].leaves) != len(rebuilt[i]) {
			t.Fatalf("%s: index %d spans %d leaves, graph rebuild %d",
				when, i, len(resident[i].leaves), len(rebuilt[i]))
		}
		for l := range resident[i].leaves {
			if !slices.Equal(resident[i].leaves[l].encoded, rebuilt[i][l]) {
				t.Fatalf("%s: index %d leaf %d diverges from graph rebuild: %d vs %d bytes",
					when, i, l, len(resident[i].leaves[l].encoded),
					len(rebuilt[i][l]))
			}
		}
	}
}

// probeKeysMidWindow probes without Collection.Snapshot, which would
// materialize pending parents and fold the very overlay under test;
// pinSnapshot captures the mid-window epoch + generation directly.
func probeKeysMidWindow(
	t *testing.T, coll *Collection, name, rawNeedle string,
) []string {
	t.Helper()
	snapshot, err := coll.pinSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	needle := primaryExactTestNeedle(t, rawNeedle)
	masks, err := snapshot.AppendIndexMasks(nil, name, needle)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, primaryExactMaskRows(masks))
	if err := snapshot.RangeMasksRaw(masks, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

// oracleGroupKeys computes the expected sorted keys per group value.
func oracleGroupKeys(oracle map[string]int, group int) []string {
	keys := make([]string, 0, 8)
	for key, value := range oracle {
		if value == group {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// TestFilePrimaryIndexedRebaseGroupFoldIdentity drives a de-template
// (slot-reassigning) mutation mid-window — surrounded by ordinary overlay
// records before and after it, including post-rebase records on the rebased
// bucket's own tiles — and proves the §3 read rule answers correctly against
// an oracle before any fold, then that the fold output is byte-identical to
// a from-scratch rebuild of the final graph, live and after reopen.
func TestFilePrimaryIndexedRebaseGroupFoldIdentity(t *testing.T) {
	const documents = 4000
	options := primaryExactOverlayTestOptions("/group")
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), documents, options,
	)
	oracle := make(map[string]int, documents)
	for at := range documents {
		oracle[templateHeavyOverlayKey(at)] = at % 37
	}
	put := func(key string, group int) {
		t.Helper()
		doc := fmt.Appendf(nil,
			`{"id":0,"kind":"document","group":%d,"active":false,`+
				`"tier":"standard","region":"eu-west-1","name":"moved"}`, group)
		if _, err := coll.Put([]byte(key), doc); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		oracle[key] = group
	}

	// Ordinary pre-rebase records first (they may themselves de-template
	// their own leaves), then pick a leaf that is STILL template-classed so
	// the next Put provably takes the rebase path.
	put(templateHeavyOverlayKey(0), 100)
	put(templateHeavyOverlayKey(1), 101)
	templateBucket, templateKeys := findTemplateLeafKeys(t, coll)

	// The rebase: a value-changed Put on a template leaf de-templates it,
	// reassigning every slot — the write rule must emit the 4-tile rebase
	// group plus absolute term records for every term in the bucket.
	put(templateKeys[0], 102)
	if coll.primaryEpoch.rebaseFloor(
		uint32(templateBucket)<<2, ^uint64(0),
	) == 0 {
		t.Fatal("de-templating Put emitted no rebase group")
	}
	// Post-rebase records over the rebased bucket's tiles: a newer term
	// record must supersede the rebase group's absolute record, and a delete
	// must overlay the rebased tile's liveness.
	put(templateKeys[1], 103)
	if _, err := coll.Delete([]byte(templateKeys[2])); err != nil {
		t.Fatal(err)
	}
	delete(oracle, templateKeys[2])

	if coll.primaryEpoch.overlayEmpty() {
		t.Fatal("scenario emitted no overlay records; the mid-window read rule is not being exercised")
	}
	// Mid-window correctness through the read rule, before any fold.
	for _, group := range []int{100, 101, 102, 103, 0, 1, 5, 36} {
		got := probeKeysMidWindow(t, coll, "group", fmt.Sprintf("%d", group))
		want := oracleGroupKeys(oracle, group)
		if !slices.Equal(got, want) {
			t.Fatalf("mid-window group=%d: got %d keys want %d\n got=%v\nwant=%v",
				group, len(got), len(want), got, want)
		}
	}

	// Fold and pin the identity anchor: resident fold output must equal the
	// from-scratch rebuild of the final graph, byte for byte.
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if !coll.primaryEpoch.overlayEmpty() {
		t.Fatal("fold left overlay records behind")
	}
	assertFoldMatchesRebuild(t, coll, "post-fold")

	for _, group := range []int{100, 101, 102, 103, 5} {
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%d", group))
		got := primaryExactTestKeys(t, coll, "group", needle)
		slices.Sort(got)
		want := oracleGroupKeys(oracle, group)
		if !slices.Equal(got, want) {
			t.Fatalf("post-fold group=%d: got %v want %v", group, got, want)
		}
	}
}

// TestSyncPrimaryIndexedRebaseCrashReplay crashes the journal-backed sync
// lane in the window between a de-template rebase mutation and its fold,
// replays the journal on the crash image, and proves the recovered fold is
// byte-identical to a from-scratch rebuild of the recovered graph — the
// crash-between-rebase-and-fold case §7 names.
func TestSyncPrimaryIndexedRebaseCrashReplay(t *testing.T) {
	const documents = 4000
	options := syncPrimaryJournalTestOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "group", Paths: []string{"/group"}},
	}
	dir := t.TempDir()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, documents)
	for at := range documents {
		keys = append(keys, templateHeavyOverlayKey(at))
	}
	slices.Sort(keys)
	order := make(map[string]int, documents)
	for at := range documents {
		order[templateHeavyOverlayKey(at)] = at
	}
	for _, key := range keys {
		if err := builder.Append(key, templateHeavyOverlayDoc(order[key])); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "store.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	oracle := make(map[string]int, documents)
	for at := range documents {
		oracle[templateHeavyOverlayKey(at)] = at % 37
	}

	// A pre-rebase journaled mutation first, then pick a leaf that is still
	// template-classed, and arm the crash capture on the de-templating Put's
	// own journal sync: the captured image holds the rebase acknowledgement
	// in the journal with no fold behind it.
	movedDoc := func(group int) []byte {
		return fmt.Appendf(nil,
			`{"id":0,"kind":"document","group":%d,"active":false,`+
				`"tier":"standard","region":"eu-west-1","name":"moved"}`, group)
	}
	if _, err := coll.Put(
		[]byte(templateHeavyOverlayKey(0)), movedDoc(100),
	); err != nil {
		t.Fatal(err)
	}
	oracle[templateHeavyOverlayKey(0)] = 100
	_, templateKeys := findTemplateLeafKeys(t, coll)

	crashStore := filepath.Join(dir, "crash.vibe")
	captured := false
	recoveryJournalPostSyncHook = func() {
		if captured {
			return
		}
		captured = true
		copyFileForCrash(t, path, crashStore)
		copyFileForCrash(t, path+".rjournal", crashStore+".rjournal")
	}
	defer func() { recoveryJournalPostSyncHook = nil }()
	if _, err := coll.Put([]byte(templateKeys[0]), movedDoc(102)); err != nil {
		t.Fatal(err)
	}
	if !captured {
		t.Fatal("post-sync seam never fired for the rebase mutation")
	}
	oracle[templateKeys[0]] = 102
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	// Reopen the crash image: replay drives the same mutation path, so the
	// rebase group regenerates from the journaled Put and the replay's
	// closing checkpoint folds it. The recovered fold must byte-match a
	// from-scratch rebuild of the recovered graph and answer the oracle.
	cf, err := os.OpenFile(crashStore, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	recovered, err := Open(cf, options)
	if err != nil {
		t.Fatalf("reopen crash image: %v", err)
	}
	defer recovered.Close()
	assertFoldMatchesRebuild(t, recovered, "crash-replay")
	for _, group := range []int{100, 102, 0, 5, 36} {
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%d", group))
		got := primaryExactTestKeys(t, recovered, "group", needle)
		slices.Sort(got)
		want := oracleGroupKeys(oracle, group)
		if !slices.Equal(got, want) {
			t.Fatalf("recovered group=%d: got %v want %v", group, got, want)
		}
	}
}

// TestFilePrimaryIndexedMutationAllocations is the zero-GC gate for the
// per-mutation write rule in isolation from checkpoint folds: after the
// one-time de-template transient, an indexed buffered Put — value unchanged
// or changed — allocates exactly what the unindexed base lane allocates
// (the overlay records come from the epoch's bump arenas, entries and
// interned bytes from pre-sized slabs).
func TestFilePrimaryIndexedMutationAllocations(t *testing.T) {
	measure := func(indexed bool) (unchanged, changed float64) {
		options := Options{
			Backend: BackendPortable, ResidentBytes: 32 << 20,
			Durability: DurabilityBufferedVisible,
		}
		if indexed {
			options.Indexes = []store.IndexDefinition{
				{Name: "country", Paths: []string{"/country"}},
			}
		}
		builder, err := store.NewBuilder(store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		for row := range 2000 {
			raw := fmt.Appendf(nil, `{"country":"c%03d","row":%d}`, row%100, row)
			if err := builder.Append(fmt.Sprintf("k%05d", row), raw); err != nil {
				t.Fatal(err)
			}
		}
		source, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.CreateTemp(t.TempDir(), "overlay-alloc-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, err := CreateFromPrimary(source, file, options); err != nil {
			t.Fatal(err)
		}
		coll, err := Open(file, options)
		if err != nil {
			t.Fatal(err)
		}
		defer coll.Close()
		// Touch every key once so any template/compact leaf pays its one
		// de-template rebase before measurement, then fold to an empty window.
		for row := range 2000 {
			raw := fmt.Appendf(nil, `{"country":"c%03d","row":%d}`, row%100, row+10000)
			if _, err := coll.Put(fmt.Appendf(nil, "k%05d", row), raw); err != nil {
				t.Fatal(err)
			}
		}
		if err := coll.Flush(); err != nil {
			t.Fatal(err)
		}
		key := []byte("k00042")
		same := []byte(`{"country":"c042","row":10042}`)
		other := []byte(`{"country":"c999","row":10042}`)
		unchanged = testing.AllocsPerRun(200, func() {
			if _, putErr := coll.Put(key, same); putErr != nil {
				panic(putErr)
			}
		})
		if err := coll.Flush(); err != nil {
			t.Fatal(err)
		}
		flip := 0
		changed = testing.AllocsPerRun(200, func() {
			flip++
			doc := same
			if flip&1 == 0 {
				doc = other
			}
			if _, putErr := coll.Put(key, doc); putErr != nil {
				panic(putErr)
			}
		})
		return unchanged, changed
	}
	baseUnchanged, baseChanged := measure(false)
	unchanged, changed := measure(true)
	if unchanged > baseUnchanged || changed > baseChanged {
		t.Fatalf(
			"indexed mutation allocates beyond the unindexed lane: unchanged %.2f vs %.2f, changed %.2f vs %.2f",
			unchanged, baseUnchanged, changed, baseChanged,
		)
	}
	// The base lane's own budget is one allocation per buffered Put; the
	// exact-index write rule must not add to it.
	if changed > 1 || unchanged > 1 {
		t.Fatalf("buffered indexed Put allocates %.2f/%.2f per run, want ≤ 1",
			unchanged, changed)
	}
}

// BenchmarkPrimaryExactProbeMidWindow is the P0 mid-window probe gate: a
// probe against a snapshot whose overlay carries 64 pending record-emitting
// mutations, every one of them touching the probed term, so the chain walk,
// newest-wins de-duplication, sort, and base merge are all on the measured
// path. Gate: ≤ 1.5 µs, 0 allocs/op (the post-fold gate stays with
// BenchmarkPrimaryExactLookupIteration).
func BenchmarkPrimaryExactProbeMidWindow(b *testing.B) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	const documents = 10_000
	for row := range documents {
		raw := fmt.Appendf(nil, `{"country":"c%03d","row":%d}`, row%100, row)
		if err := builder.Append(fmt.Sprintf("k%05d", row), raw); err != nil {
			b.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	file, err := os.CreateTemp(b.TempDir(), "primary-exact-midwindow-*")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		b.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		b.Fatal(err)
	}
	defer collection.Close()
	// Warm the whole corpus so template/compact leaves de-template outside
	// the window under test, then fold to a clean base.
	for row := range documents {
		raw := fmt.Appendf(nil, `{"country":"c%03d","row":%d}`, row%100, row+100000)
		if _, err := collection.Put(fmt.Appendf(nil, "k%05d", row), raw); err != nil {
			b.Fatal(err)
		}
	}
	if err := collection.Flush(); err != nil {
		b.Fatal(err)
	}
	// 64 pending mutations, all moving rows INTO the probed term c042 from
	// distinct buckets: the worst chain the checkpoint cadence admits for
	// one term.
	for i := range 64 {
		row := i*151 + 3 // spread across leaves; none originally c042
		if row%100 == 42 {
			row++
		}
		raw := fmt.Appendf(nil, `{"country":"c042","row":%d}`, row+200000)
		if _, err := collection.Put(fmt.Appendf(nil, "k%05d", row), raw); err != nil {
			b.Fatal(err)
		}
	}
	if collection.primaryEpoch.overlayEmpty() {
		b.Fatal("window is empty; benchmark would measure the post-fold path")
	}
	// pinSnapshot, not Collection.Snapshot: the latter materializes pending
	// parents, folding away the overlay this benchmark exists to price.
	snapshot, err := collection.pinSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	needle := primaryExactTestNeedle(b, `"c042"`)
	var workspace IndexWorkspace
	masks := make([]store.Mask, 0, 256)
	masks, err = snapshot.AppendIndexMasksInto(
		masks, &workspace, "country", needle,
	)
	if err != nil {
		b.Fatal(err)
	}
	if primaryExactMaskRows(masks) != documents/100+64 {
		b.Fatalf("mid-window probe sees %d rows, want %d",
			primaryExactMaskRows(masks), documents/100+64)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		masks, err = snapshot.AppendIndexMasksInto(
			masks[:0], &workspace, "country", needle,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	primaryExactBenchmarkRows = primaryExactMaskRows(masks)
}
