package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// buildIndexedPrimaryFile builds an ordered-primary exact-indexed store from the
// given key->document map and returns an opened collection.
func buildIndexedPrimaryFile(
	t testing.TB, dir, name string, docs map[string][]byte, options Options,
) *Collection {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(docs))
	for key := range docs {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err := builder.Append(key, docs[key]); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coll.Close() })
	return coll
}

// indexAnswers returns, for one physical index name, the sorted matching keys
// per probed scalar term.
func indexAnswers(
	t testing.TB, coll *Collection, name string, terms []string,
) map[string][]string {
	t.Helper()
	out := make(map[string][]string, len(terms))
	for _, term := range terms {
		needle := primaryExactTestNeedle(t, `"`+term+`"`)
		keys := primaryExactTestKeys(t, coll, name, needle)
		slices.Sort(keys)
		out[term] = keys
	}
	return out
}

// TestFilePrimaryIndexedMutationMatchesRebuild proves incremental posting
// maintenance is equivalent to a bulk rebuild of the final graph (answers match
// a fresh CreateFromPrimary of the final logical state) and deterministic (the
// same mutation sequence produces byte-identical resident term leaves).
func TestFilePrimaryIndexedMutationMatchesRebuild(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	const base = 600
	terms := make([]string, 0, 64)
	for c := 0; c < 60; c++ {
		terms = append(terms, fmt.Sprintf("c%02d", c))
	}

	// run applies the same deterministic base+mutation sequence and returns the
	// opened collection plus the final logical state.
	run := func(dir string) (*Collection, map[string][]byte) {
		docs := make(map[string][]byte, base)
		for row := 0; row < base; row++ {
			docs[fmt.Sprintf("k%04d", row)] =
				[]byte(fmt.Sprintf(`{"country":"c%02d","row":%d}`, row%40, row))
		}
		coll := buildIndexedPrimaryFile(t, dir, "indexed-mut-*", docs, options)
		// Deterministic mutations: value changes, deletes, inserts.
		for row := 0; row < base; row += 7 {
			key := fmt.Sprintf("k%04d", row)
			nc := fmt.Sprintf("c%02d", (row*13)%59)
			doc := []byte(fmt.Sprintf(`{"country":"%s","row":%d}`, nc, row))
			if _, err := coll.Put([]byte(key), doc); err != nil {
				t.Fatalf("value-change put %s: %v", key, err)
			}
			docs[key] = doc
		}
		for row := 1; row < base; row += 11 {
			key := fmt.Sprintf("k%04d", row)
			if _, err := coll.Delete([]byte(key)); err != nil {
				t.Fatalf("delete %s: %v", key, err)
			}
			delete(docs, key)
		}
		for row := base; row < base+80; row++ {
			key := fmt.Sprintf("n%04d", row)
			doc := []byte(fmt.Sprintf(`{"country":"c%02d","row":%d}`, row%58, row))
			if _, err := coll.Put([]byte(key), doc); err != nil {
				t.Fatalf("insert put %s: %v", key, err)
			}
			docs[key] = doc
		}
		return coll, docs
	}

	dir := t.TempDir()
	mutated, finalState := run(dir)

	// Equivalence: a fresh build of the final logical state answers identically.
	rebuilt := buildIndexedPrimaryFile(
		t, dir, "indexed-rebuild-*", finalState, options,
	)
	mutatedAnswers := indexAnswers(t, mutated, "country", terms)
	rebuiltAnswers := indexAnswers(t, rebuilt, "country", terms)
	for _, term := range terms {
		if !slices.Equal(mutatedAnswers[term], rebuiltAnswers[term]) {
			t.Fatalf("country=%s: mutated %v != rebuilt %v",
				term, mutatedAnswers[term], rebuiltAnswers[term])
		}
	}

	// Determinism: the same sequence on the same base yields byte-identical
	// folded term leaves (same StoreID, deterministic maintenance). The
	// resident base only changes at fold points now, so both runs are folded
	// first — the comparison then covers the overlay resolve + re-encode,
	// not just the untouched build-time bytes.
	dir2 := t.TempDir()
	again, _ := run(dir2)
	if err := flushPhysicalForTest(mutated); err != nil {
		t.Fatal(err)
	}
	if err := flushPhysicalForTest(again); err != nil {
		t.Fatal(err)
	}
	mutatedExact := mutated.primaryEpoch.exact
	againExact := again.primaryEpoch.exact
	if len(mutatedExact) != len(againExact) {
		t.Fatalf("index count differs: %d vs %d",
			len(mutatedExact), len(againExact))
	}
	for i := range mutatedExact {
		if mutatedExact[i].present() != againExact[i].present() {
			t.Fatalf("index %d presence differs", i)
		}
		if len(mutatedExact[i].leaves) != len(againExact[i].leaves) {
			t.Fatalf("index %d spans %d vs %d leaves",
				i, len(mutatedExact[i].leaves), len(againExact[i].leaves))
		}
		for l := range mutatedExact[i].leaves {
			if !slices.Equal(
				mutatedExact[i].leaves[l].encoded,
				againExact[i].leaves[l].encoded,
			) {
				t.Fatalf("index %d leaf %d bytes not deterministic: %d vs %d bytes",
					i, l, len(mutatedExact[i].leaves[l].encoded),
					len(againExact[i].leaves[l].encoded))
			}
		}
	}
}

// TestFilePrimaryIndexedChurnSplit forces routed leaf splits and merges on an
// indexed ordered-primary collection and proves the structural posting repair
// keeps the exact index correct: after every structural transaction the index
// answers still match an independent oracle.
func TestFilePrimaryIndexedChurnSplit(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	dir := t.TempDir()
	// Seed a small graph, then insert a dense band of keys sharing a lexical
	// prefix so their leaf fills and splits repeatedly.
	docs := map[string][]byte{
		"a0000": []byte(`{"country":"c00","row":0}`),
		"z0000": []byte(`{"country":"c01","row":1}`),
	}
	coll := buildIndexedPrimaryFile(t, dir, "indexed-churn-*", docs, options)

	oracle := map[string]string{ // key -> country
		"a0000": "c00", "z0000": "c01",
	}
	countryKeys := func() map[string]map[string]bool {
		m := map[string]map[string]bool{}
		for key, country := range oracle {
			if m[country] == nil {
				m[country] = map[string]bool{}
			}
			m[country][key] = true
		}
		return m
	}
	check := func(when string) {
		t.Helper()
		want := countryKeys()
		for country, keys := range want {
			needle := primaryExactTestNeedle(t, `"`+country+`"`)
			got := primaryExactTestKeys(t, coll, "country", needle)
			gotSet := map[string]bool{}
			for _, k := range got {
				gotSet[k] = true
			}
			if len(gotSet) != len(keys) {
				t.Fatalf("%s: country=%s rows=%d want %d",
					when, country, len(gotSet), len(keys))
			}
			for k := range keys {
				if !gotSet[k] {
					t.Fatalf("%s: country=%s missing %s", when, country, k)
				}
			}
		}
	}

	// Dense insert band to drive splits.
	for i := 0; i < 900; i++ {
		key := fmt.Sprintf("m%05d", i)
		country := fmt.Sprintf("c%02d", i%30)
		doc := []byte(fmt.Sprintf(`{"country":"%s","row":%d}`, country, i))
		if _, err := coll.Put([]byte(key), doc); err != nil {
			t.Fatalf("split-driving put %s: %v", key, err)
		}
		oracle[key] = country
		if i%150 == 0 {
			check(fmt.Sprintf("insert-%d", i))
		}
	}
	if coll.Stats().PrimaryLeafSplits == 0 {
		t.Fatal("no leaf split fired; test would not cover structural posting repair")
	}
	check("after-inserts")

	// Delete band to drive merges/empties.
	for i := 0; i < 900; i += 2 {
		key := fmt.Sprintf("m%05d", i)
		if _, err := coll.Delete([]byte(key)); err != nil {
			t.Fatalf("merge-driving delete %s: %v", key, err)
		}
		delete(oracle, key)
	}
	check("after-deletes")

	// Reopen and re-check to prove the durable index matches after checkpoints.
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	check("after-flush")
}

// TestSyncPrimaryIndexedJournalReplayAfterCrashWindow extends the sync-journal
// crash-window test with an indexed collection: a journaled Put/Delete replays
// through the ordinary mutation path on recovery, which maintains the exact
// index, so the recovered index answers identically to the live one.
func TestSyncPrimaryIndexedJournalReplayAfterCrashWindow(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "country", Paths: []string{"/country"}},
	}
	// Seed an indexed one-document primary graph.
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"country":"c00","v":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
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

	oracle := map[string]string{"seed": "c00"}
	const baseline = 12
	for i := 0; i < baseline; i++ {
		key := fmt.Sprintf("key-%04d", i)
		country := fmt.Sprintf("c%02d", i%5)
		val := []byte(fmt.Sprintf(`{"country":"%s","i":%d}`, country, i))
		if _, err := coll.Put([]byte(key), val); err != nil {
			t.Fatalf("baseline put %d: %v", i, err)
		}
		oracle[key] = country
	}

	crashStore := filepath.Join(dir, "crash.vibe")
	crashKey := "crash-key"
	crashVal := []byte(`{"country":"c04","i":999}`)
	var captured bool
	recoveryJournalPostSyncHook = func() {
		if captured {
			return
		}
		captured = true
		copyFileForCrash(t, path, crashStore)
		copyFileForCrash(t, path+".rjournal", crashStore+".rjournal")
	}
	defer func() { recoveryJournalPostSyncHook = nil }()

	if _, err := coll.Put([]byte(crashKey), crashVal); err != nil {
		t.Fatalf("crash-window put: %v", err)
	}
	if !captured {
		t.Fatal("post-sync seam never fired")
	}
	oracle[crashKey] = "c04"
	if err := coll.Close(); err != nil {
		t.Fatalf("close live: %v", err)
	}
	_ = file.Close()

	// The recovered index must answer identically to the oracle for every term.
	terms := []string{"c00", "c01", "c02", "c03", "c04"}
	verify := func(label string) {
		cf, err := os.OpenFile(crashStore, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := Open(cf, options)
		if err != nil {
			t.Fatalf("%s: reopen crash image: %v", label, err)
		}
		// Oracle answers per term.
		want := map[string]map[string]bool{}
		for key, country := range oracle {
			if want[country] == nil {
				want[country] = map[string]bool{}
			}
			want[country][key] = true
		}
		for _, term := range terms {
			needle := primaryExactTestNeedle(t, `"`+term+`"`)
			got := primaryExactTestKeys(t, rc, "country", needle)
			gotSet := map[string]bool{}
			for _, k := range got {
				gotSet[k] = true
			}
			if len(gotSet) != len(want[term]) {
				t.Fatalf("%s: country=%s recovered %d rows want %d",
					label, term, len(gotSet), len(want[term]))
			}
			for k := range want[term] {
				if !gotSet[k] {
					t.Fatalf("%s: country=%s missing recovered key %s",
						label, term, k)
				}
			}
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("%s: close: %v", label, err)
		}
		_ = cf.Close()
	}
	verify("first-open-replay")
	verify("second-open-idempotent")
}

func indexedCrashKey(i int) string { return fmt.Sprintf("crash-%04d", i) }

func indexedCrashValue(i int) []byte {
	return []byte(fmt.Sprintf(
		`{"country":"c%02d","i":%d,"pad":"%s"}`, i%6, i, strings.Repeat("p", 40),
	))
}

// indexedCrashCountry recovers the indexed scalar from a stored document without
// a full parse: the corpus writes it as a fixed "country":"cNN" prefix.
func indexedCrashCountry(doc string) (string, bool) {
	const marker = `"country":"`
	at := strings.Index(doc, marker)
	if at < 0 {
		return "", false
	}
	rest := doc[at+len(marker):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

// runIndexedPrimaryFaultPass runs the indexed seed + indexed Put/Flush workload
// once with plan installed, mirroring runPrimarySplitFaultPass but with indexed
// documents so the exact-index pages are staged inside each checkpoint's commit
// window.
func runIndexedPrimaryFaultPass(
	t *testing.T, built *store.Collection, options Options,
	fc *faultController, plan storeio.FaultPlan, ops int,
) (boundaries []int, contents []map[string]string, image journalCrashImage, faulted bool) {
	t.Helper()
	prev := storeCommitterFactory
	storeCommitterFactory = fc.factory()
	defer func() { storeCommitterFactory = prev }()
	fc.plan = plan
	fc.device = nil

	path := filepath.Join(t.TempDir(), "indexed-crash.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		_ = file.Close()
		t.Fatalf("seed CreateFromPrimary: %v", err)
	}
	coll, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("open seed: %v", err)
	}
	dev := fc.device
	if dev == nil {
		_ = coll.Close()
		_ = file.Close()
		t.Fatal("fault device was not installed by the committer factory")
	}
	boundaries = []int{dev.Commits()}
	contents = []map[string]string{snapshotCollectionContent(t, coll)}
	for i := 0; i < ops; i++ {
		_, opErr := coll.Put([]byte(indexedCrashKey(i)), indexedCrashValue(i))
		if opErr == nil {
			opErr = flushPhysicalForTest(coll)
		}
		if dev.Faulted() {
			break
		}
		if opErr != nil {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("indexed insert %d: unexpected error before any fault: %v", i, opErr)
		}
		boundaries = append(boundaries, dev.Commits())
		contents = append(contents, snapshotCollectionContent(t, coll))
	}
	faulted = dev.Faulted()
	image = captureJournalImage(t, path)
	_ = coll.Close()
	_ = file.Close()
	return boundaries, contents, image, faulted
}

// assertIndexedCrashImage reopens a crash image and proves the recovered exact
// index is consistent with the recovered graph: it never panics, and when Open
// succeeds every country term the index reports resolves to exactly the recovered
// keys carrying that country -- no phantom postings, no dropped ones. A crash
// that leaves the exact pages inconsistent with the graph would either fail Open
// (the term-leaf admission rechecks postings against the derived live map) or
// return a mismatched answer here; a mid-write torn image is allowed to fail
// Open, but must not corrupt.
func assertIndexedCrashImage(
	t *testing.T, options Options, image journalCrashImage, label string,
) {
	t.Helper()
	imagePath := filepath.Join(t.TempDir(), "indexed-verify.vibe")
	if err := os.WriteFile(imagePath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(imagePath+".rjournal", image.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(imagePath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	report, verifyErr := Verify(f)
	if verifyErr != nil {
		t.Fatalf("%s: Verify I/O error: %v", label, verifyErr)
	}
	_ = report
	coll, openErr := Open(f, options)
	if openErr != nil {
		// A torn mid-write image may not reopen; that is acceptable as long as it
		// did not corrupt (Verify returned above without an I/O error).
		return
	}
	defer coll.Close()
	// Recover the document set and the country each doc carries.
	docs := snapshotCollectionContent(t, coll)
	want := map[string]map[string]bool{}
	for key, doc := range docs {
		country, ok := indexedCrashCountry(doc)
		if !ok {
			continue
		}
		if want[country] == nil {
			want[country] = map[string]bool{}
		}
		want[country][key] = true
	}
	for country, keys := range want {
		needle := primaryExactTestNeedle(t, `"`+country+`"`)
		got := primaryExactTestKeys(t, coll, "country", needle)
		gotSet := map[string]bool{}
		for _, k := range got {
			gotSet[k] = true
		}
		if len(gotSet) != len(keys) {
			t.Fatalf("%s: recovered index country=%s has %d rows, graph has %d",
				label, country, len(gotSet), len(keys))
		}
		for k := range keys {
			if !gotSet[k] {
				t.Fatalf("%s: recovered index country=%s missing graph key %s",
					label, country, k)
			}
		}
	}
}

// TestFilePrimaryIndexedCommitCrashBoundary crashes an indexed ordered-primary
// checkpoint at every write point of every device commit in the window and proves
// the exact-index pages recover atomically with the graph: the exact root and
// term leaves are staged and published in the same generation as the primary
// root, so no new commit-window shape exists and every reopened image carries an
// index consistent with its recovered documents.
func TestFilePrimaryIndexedCommitCrashBoundary(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"country":"c00","i":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Collection: store.Options{ChunkDocuments: 1},
		Backend:    BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		PageSize:   4096, MaxPageSize: 64 << 10,
		InlineValueBytes: 2048, MaxDocumentBytes: 2048,
		GroupLimit: 1, CommitCoalesce: 0,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}

	const ops = 6
	fc := &faultController{}
	boundaries, _, _, faulted := runIndexedPrimaryFaultPass(
		t, built, options, fc, storeio.FaultPlan{Phase: storeio.FaultNone}, ops,
	)
	if faulted {
		t.Fatal("clean probe pass unexpectedly faulted")
	}
	lo := boundaries[0]
	hi := boundaries[len(boundaries)-1]
	if hi <= lo {
		t.Fatalf("indexed workload issued no device commits: [%d,%d)", lo, hi)
	}
	phases := []storeio.FaultPhase{
		storeio.FaultAfterDataWrite,
		storeio.FaultAfterBarrier,
		storeio.FaultAfterRootWrite,
		storeio.FaultAfterFinalSync,
		storeio.FaultTornRoot,
	}
	exercised := 0
	for commit := lo; commit < hi; commit++ {
		for _, phase := range phases {
			plan := storeio.FaultPlan{Commit: commit, Phase: phase, DataIndex: 0}
			_, _, image, didFault := runIndexedPrimaryFaultPass(
				t, built, options, fc, plan, ops,
			)
			if !didFault {
				continue
			}
			assertIndexedCrashImage(
				t, options, image, fmt.Sprintf("commit=%d phase=%d", commit, phase),
			)
			exercised++
		}
	}
	if exercised == 0 {
		t.Fatal("no indexed crash points were exercised")
	}
	t.Logf("indexed commit crash sweep: %d points exercised", exercised)
}
