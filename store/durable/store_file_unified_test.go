package durable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func createUnifiedPrimary(t *testing.T, path string, keys []string, docs [][]byte) *Collection {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		ResidentBytes: 16 << 20, Backend: BackendPortable,
	}
	if _, err := CreateFromPrimary(unifiedPrimarySource(t, keys, docs), file, options); err != nil {
		t.Fatalf("CreateFromPrimary(unified): %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(mustOpen(t, path))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("Verify findings: %+v", report.Findings)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen without the option: the reader dispatches on the class byte and
	// must never need to know which representation wrote the file.
	collection, err := Open(reopened, Options{ResidentBytes: 16 << 20, Backend: BackendPortable})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = collection.Close()
		_ = reopened.Close()
	})
	return collection
}

func canonicalDocs(t *testing.T, docs [][]byte) [][]byte {
	t.Helper()
	out := make([][]byte, len(docs))
	for i := range docs {
		canonical, err := vibejson.AppendCanonicalize(nil, docs[i])
		if err != nil {
			t.Fatalf("AppendCanonicalize: %v", err)
		}
		out[i] = canonical
	}
	return out
}

// TestUnifiedPrimaryCanonicalReads pins the whole unified read surface: a
// bulk-built unified store returns exactly the canonical spelling of every
// document on point reads and ordered scans, and the build stages
// only class-5 leaves.
func TestUnifiedPrimaryCanonicalReads(t *testing.T) {
	for _, high := range []bool{false, true} {
		name := "low"
		if high {
			name = "high"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			keys, docs := unifiedPrimaryCorpus(2500, high)
			want := canonicalDocs(t, docs)
			unified := createUnifiedPrimary(t, filepath.Join(dir, "unified.vibe"), keys, docs)

			counts := primaryLeafClassCounts(t, unified)
			if counts[storeio.CommonPrimaryLeafCompact] == 0 {
				t.Fatalf("unified build produced no class-5 leaves: %v", counts)
			}
			for class, n := range counts {
				if class != storeio.CommonPrimaryLeafCompact && n != 0 {
					t.Fatalf("unified build staged class %d leaves: %v", class, counts)
				}
			}
			t.Logf("[%s] unified leaf classes: %v; disk=%d",
				name, counts, fileSize(t, filepath.Join(dir, "unified.vibe")))

			snapshot, err := unified.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			for i := range keys {
				got, ok, err := snapshot.AppendRaw(nil, []byte(keys[i]))
				if err != nil || !ok {
					t.Fatalf("AppendRaw(%q) = (%v,%v)", keys[i], ok, err)
				}
				if !bytes.Equal(got, want[i]) {
					t.Fatalf("point read %q:\n got %q\nwant %q", keys[i], got, want[i])
				}
			}
			rows := 0
			previous := ""
			if err := snapshot.RangeRaw(func(k, v []byte) error {
				if string(k) <= previous {
					return fmt.Errorf("scan order violation at %q", k)
				}
				previous = string(k)
				i := 0
				if _, err := fmt.Sscanf(string(k), "doc:%08d", &i); err != nil {
					return err
				}
				if !bytes.Equal(v, want[i]) {
					return fmt.Errorf("scan value mismatch for %q", k)
				}
				rows++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if rows != len(keys) {
				t.Fatalf("scanned %d rows want %d", rows, len(keys))
			}
		})
	}
}

// TestUnifiedPrimaryDeterminism pins that the unified bulk build is
// byte-for-byte reproducible from the same source.
func TestUnifiedPrimaryDeterminism(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedPrimaryCorpus(1500, false)
	build := func(name string) []byte {
		path := filepath.Join(dir, name)
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		// AsyncVisible mints no recovery journal, so the file carries no random
		// journal identity and a reproducible build is byte-for-byte identical.
		if _, err := CreateFromPrimary(unifiedPrimarySource(t, keys, docs), file,
			Options{
				ResidentBytes: 16 << 20, Backend: BackendPortable,
				Durability: DurabilityAsyncVisible,
			}); err != nil {
			t.Fatalf("CreateFromPrimary: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	a, b := build("a.vibe"), build("b.vibe")
	if !bytes.Equal(a, b) {
		t.Fatalf("unified bulk build is not deterministic: %d vs %d bytes", len(a), len(b))
	}
}

// TestUnifiedPrimaryMutation drives Put/Delete against a unified collection.
// Inline replacements, inserts, and deletes share the bounded class-5 overlay;
// structural pressure and overflow retain the bridge. Every read must remain
// exact against the mutating reference.
func TestUnifiedPrimaryMutation(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedPrimaryCorpus(1200, false)
	canonical := canonicalDocs(t, docs)
	unified := createUnifiedPrimary(t, filepath.Join(dir, "unified.vibe"), keys, docs)
	want := make(map[string][]byte, len(keys))
	for i := range keys {
		want[keys[i]] = canonical[i]
	}

	origN := len(keys)
	rng := rand.New(rand.NewPCG(0x99, 0x11))
	for round := range 400 {
		i := rng.IntN(origN)
		key := keys[i]
		switch round % 4 {
		case 0, 1: // update
			val := fmt.Appendf(nil, `{"rev":%d,"key":"%s","note":"mutated"}`, round, key)
			if _, err := unified.Put([]byte(key), val); err != nil {
				t.Fatalf("put %q: %v", key, err)
			}
			canonicalValue, err := vibejson.AppendCanonicalize(nil, val)
			if err != nil {
				t.Fatal(err)
			}
			want[key] = canonicalValue
		case 2: // delete then reinsert the canonical original
			if _, err := unified.Delete([]byte(key)); err != nil {
				t.Fatalf("delete %q: %v", key, err)
			}
			if _, err := unified.Put([]byte(key), canonical[i]); err != nil {
				t.Fatalf("reinsert %q: %v", key, err)
			}
			want[key] = canonical[i]
		case 3: // insert a fresh key
			nk := fmt.Sprintf("new:%08d", round)
			val := fmt.Appendf(nil, `{"fresh":%d}`, round)
			if _, err := unified.Put([]byte(nk), val); err != nil {
				t.Fatalf("insert %q: %v", nk, err)
			}
			keys = append(keys, nk)
			want[nk] = val
		}
	}
	snapshot, err := unified.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, key := range keys {
		got, ok, err := snapshot.AppendRaw(nil, []byte(key))
		if err != nil || !ok {
			t.Fatalf("AppendRaw(%q) = (%v,%v)", key, ok, err)
		}
		if !bytes.Equal(got, want[key]) {
			t.Fatalf("read %q:\n got %q\nwant %q", key, got, want[key])
		}
	}
}

// TestUnifiedPrimaryOverlayReplaceFold pins the row-overlay mutation lane: one
// equal-size replacement publishes without de-templating the leaf or creating
// a pending raw parent, remains snapshot-isolated by generation, and folds
// through the class-5 bulk encoder when a scan-capable snapshot is requested.
func TestUnifiedPrimaryOverlayReplaceFold(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedPrimaryCorpus(1200, false)
	unified := createUnifiedPrimary(
		t, filepath.Join(dir, "unified-overlay.vibe"), keys, docs,
	)
	if unified.primaryUnifiedOverlay == nil {
		t.Fatal("unified row overlay was not budgeted")
	}

	key := []byte(keys[0])
	before := canonicalDocs(t, docs[:1])[0]
	after := bytes.Replace(
		append([]byte(nil), before...),
		[]byte(`"active":true`), []byte(`"active":null`), 1,
	)
	if bytes.Equal(after, before) || len(after) != len(before) {
		t.Fatal("test replacement must change content without changing size")
	}
	oldSnapshot, err := unified.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()

	state := unified.state.Load()
	beforeRoute, err := unified.currentPrimaryResidentRoute(state, key)
	if err != nil {
		t.Fatal(err)
	}
	created, err := unified.Put(key, after)
	if err != nil || created {
		t.Fatalf("overlay Put = created %v, err %v", created, err)
	}
	if !unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("equal-size class-5 replacement did not enter overlay")
	}
	if len(unified.primaryPendingParents) != 0 {
		t.Fatalf(
			"overlay replacement created %d raw pending parents",
			len(unified.primaryPendingParents),
		)
	}
	state = unified.state.Load()
	overlaidRoute, err := unified.currentPrimaryResidentRoute(state, key)
	if err != nil {
		t.Fatal(err)
	}
	if overlaidRoute.Ref != beforeRoute.Ref {
		t.Fatalf(
			"overlay replaced base leaf before fold: %v -> %v",
			beforeRoute.Ref, overlaidRoute.Ref,
		)
	}
	got, ok, err := unified.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, after) {
		t.Fatalf("live overlay read = %q,%v,%v want %q", got, ok, err, after)
	}
	got, ok, err = oldSnapshot.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, before) {
		t.Fatalf("old snapshot read = %q,%v,%v want %q", got, ok, err, before)
	}

	folded, err := unified.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer folded.Close()
	if unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("snapshot left class-5 overlay dirty")
	}
	state = unified.state.Load()
	foldedRoute, err := unified.currentPrimaryResidentRoute(state, key)
	if err != nil {
		t.Fatal(err)
	}
	if foldedRoute.Ref == beforeRoute.Ref {
		t.Fatal("snapshot did not checkpoint the overlaid leaf")
	}
	lease, err := unified.cache.Acquire(foldedRoute.Ref)
	if err != nil {
		t.Fatal(err)
	}
	class := storeio.PrimaryLeafClass(lease.Page())
	lease.Release()
	if class != storeio.CommonPrimaryLeafCompact {
		t.Fatalf("overlay fold produced class %d, want class 5", class)
	}
	got, ok, err = folded.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, after) {
		t.Fatalf("folded snapshot read = %q,%v,%v want %q", got, ok, err, after)
	}
}

// TestUnifiedPrimaryOverlayGrowingInsertDelete pins the complete unindexed
// class-5 mutation surface. A growing replacement, an insertion, and a
// tombstone all publish without rewriting the resident leaf; the fold preserves
// the insertion's stable slot, lexical order, document count, and old snapshot.
func TestUnifiedPrimaryOverlayGrowingInsertDelete(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedPrimaryCorpus(1200, false)
	unified := createUnifiedPrimary(
		t, filepath.Join(dir, "unified-mutations.vibe"), keys, docs,
	)
	oldSnapshot, err := unified.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()

	initialCount := unified.state.Load().root.DocumentCount
	growingKey := []byte(keys[0])
	deletedKey := []byte(keys[1])
	insertedKey := []byte(keys[0] + "+")
	small := []byte(`{"revision":0}`)
	growing := fmt.Appendf(
		nil, `{"key":"%s","payload":"%s","revision":1}`,
		growingKey, bytes.Repeat([]byte("g"), 96),
	)
	growing, err = vibejson.AppendCanonicalize(nil, growing)
	if err != nil {
		t.Fatal(err)
	}
	inserted := []byte(`{"fresh":true,"payload":"overlay insert"}`)

	created, err := unified.Put(growingKey, small)
	if err != nil || created {
		t.Fatalf("shrinking Put = created %v, err %v", created, err)
	}
	created, err = unified.Put(growingKey, growing)
	if err != nil || created {
		t.Fatalf("growing Put = created %v, err %v", created, err)
	}
	if !unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("growing replacement did not enter class-5 overlay")
	}
	created, err = unified.Put(insertedKey, inserted)
	if err != nil || !created {
		t.Fatalf("insert Put = created %v, err %v", created, err)
	}
	if !unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("insert did not enter class-5 overlay")
	}
	deleted, err := unified.Delete(deletedKey)
	if err != nil || !deleted {
		t.Fatalf("Delete = deleted %v, err %v", deleted, err)
	}
	if !unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("delete did not enter class-5 overlay")
	}
	beforeNoopGeneration := unified.state.Load().root.Generation
	deleted, err = unified.Delete(deletedKey)
	if err != nil || deleted {
		t.Fatalf("second Delete = deleted %v, err %v", deleted, err)
	}
	if got := unified.state.Load().root.Generation; got != beforeNoopGeneration {
		t.Fatalf("absent delete advanced generation: %d -> %d",
			beforeNoopGeneration, got)
	}
	if !unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("class-5 mutation sequence did not remain in overlay")
	}
	if got := len(unified.primaryPendingParents); got != 0 {
		t.Fatalf("overlay mutations created %d structural parents", got)
	}
	if got := unified.state.Load().root.DocumentCount; got != initialCount {
		t.Fatalf("document count = %d, want %d", got, initialCount)
	}

	insertRoute, err := unified.currentPrimaryResidentRoute(
		unified.state.Load(), insertedKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, disposition, insertedSlot := unified.primaryUnifiedOverlay.lookup(
		insertRoute.Bucket, insertRoute.Hash, insertedKey,
		unified.state.Load().root.Generation,
	)
	if disposition != primaryUnifiedOverlayValue {
		t.Fatalf("insert overlay disposition = %v", disposition)
	}
	for _, probe := range []struct {
		key   []byte
		value []byte
		found bool
	}{
		{growingKey, growing, true},
		{insertedKey, inserted, true},
		{deletedKey, nil, false},
	} {
		got, found, readErr := unified.AppendRaw(nil, probe.key)
		if readErr != nil || found != probe.found ||
			probe.found && !bytes.Equal(got, probe.value) {
			t.Fatalf("live read %q = %q,%v,%v want %q,%v",
				probe.key, got, found, readErr, probe.value, probe.found)
		}
	}
	oldGrowing, found, err := oldSnapshot.AppendRaw(nil, growingKey)
	if err != nil || !found ||
		!bytes.Equal(oldGrowing, canonicalDocs(t, docs[:1])[0]) {
		t.Fatalf("old snapshot growing row = %q,%v,%v", oldGrowing, found, err)
	}
	if _, found, err := oldSnapshot.AppendRaw(nil, insertedKey); err != nil || found {
		t.Fatalf("old snapshot insert = %v,%v", found, err)
	}
	oldDeleted, found, err := oldSnapshot.AppendRaw(nil, deletedKey)
	if err != nil || !found ||
		!bytes.Equal(oldDeleted, canonicalDocs(t, docs[1:2])[0]) {
		t.Fatalf("old snapshot deleted row = %q,%v,%v", oldDeleted, found, err)
	}

	folded, err := unified.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer folded.Close()
	if unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("snapshot left insert/delete overlay dirty")
	}
	foldedRoute, err := unified.currentPrimaryResidentRoute(
		unified.state.Load(), insertedKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := unified.cache.Acquire(foldedRoute.Ref)
	if err != nil {
		t.Fatal(err)
	}
	view, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), unified.storeID, foldedRoute.Bucket,
	)
	if !ok {
		lease.Release()
		t.Fatal("folded insert leaf is not admitted compact")
	}
	rank, found := view.FindKey(insertedKey)
	foldedSlot, slotOK := view.PostingSlot(rank)
	_, overflow := view.OverflowRef(rank)
	lease.Release()
	if !found || overflow ||
		view.Len() <= storeio.CommonPrimaryLeafWideSlots &&
			(!slotOK || foldedSlot != insertedSlot) {
		t.Fatalf("folded insert slot = %d,%v,%v want %d",
			foldedSlot, overflow, found, insertedSlot)
	}

	seenInserted := false
	seenDeleted := false
	if err := folded.RangeRaw(func(key, value []byte) error {
		if bytes.Equal(key, insertedKey) {
			seenInserted = bytes.Equal(value, inserted)
		}
		if bytes.Equal(key, deletedKey) {
			seenDeleted = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !seenInserted || seenDeleted {
		t.Fatalf("folded scan inserted=%v deleted=%v", seenInserted, seenDeleted)
	}
}

// TestUnifiedPrimaryOverlayJournalReplay captures the synchronous crash window
// after the replacement's redo record is durable but before the overlay head is
// published. Reopen must replay, fold, and preserve the class-5 representation.
func TestUnifiedPrimaryOverlayJournalReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unified-live.vibe")
	keys, docs := unifiedPrimaryCorpus(300, false)
	options := syncPrimaryJournalTestOptions()

	file, err := os.OpenFile(
		path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(
		unifiedPrimarySource(t, keys, docs), file, options,
	); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	live, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte(keys[0])
	before := canonicalDocs(t, docs[:1])[0]
	after := bytes.Replace(
		append([]byte(nil), before...),
		[]byte(`"active":true`), []byte(`"active":null`), 1,
	)
	crashPath := filepath.Join(dir, "unified-crash.vibe")
	captured := false
	recoveryJournalPostSyncHook = func() {
		if captured {
			return
		}
		captured = true
		copyFileForCrash(t, path, crashPath)
		copyFileForCrash(
			t, path+".rjournal", crashPath+".rjournal",
		)
	}
	defer func() { recoveryJournalPostSyncHook = nil }()
	if _, err := live.Put(key, after); err != nil {
		t.Fatal(err)
	}
	if !captured {
		t.Fatal("class-5 replacement did not cross journal-before-publish seam")
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	for attempt := range 2 {
		recoveredFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		recovered, err := Open(recoveredFile, options)
		if err != nil {
			_ = recoveredFile.Close()
			t.Fatalf("reopen %d: %v", attempt, err)
		}
		got, ok, readErr := recovered.AppendRaw(nil, key)
		if readErr != nil || !ok || !bytes.Equal(got, after) {
			t.Fatalf(
				"reopen %d read = %q,%v,%v want %q",
				attempt, got, ok, readErr, after,
			)
		}
		counts := primaryLeafClassCounts(t, recovered)
		if counts[storeio.CommonPrimaryLeafCompact] == 0 {
			t.Fatalf("reopen %d lost class-5 leaves: %v", attempt, counts)
		}
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}
		if err := recoveredFile.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestUnifiedPrimaryOverlayFoldWithPinnedReader fills the fixed record/arena
// window, holds an epoch reader across the pressure fold, and proves both sides
// of the reuse rule: the old generation still resolves through retained records
// while the new generation may fall back structurally without being shadowed
// by those records; once the reader exits, another class-5 route recycles the
// window and resumes overlay publication.
func TestUnifiedPrimaryOverlayFoldWithPinnedReader(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedPrimaryCorpus(1200, false)
	unified := createUnifiedPrimary(
		t, filepath.Join(dir, "unified-pinned.vibe"), keys, docs,
	)
	key := []byte(keys[0])
	before := canonicalDocs(t, docs[:1])[0]
	after := bytes.Replace(
		append([]byte(nil), before...),
		[]byte(`"active":true`), []byte(`"active":null`), 1,
	)
	if len(after) != len(before) {
		t.Fatalf("fixed test values differ in size: before=%d after=%d",
			len(before), len(after))
	}
	// Keep this reader-lifetime test bounded independently of the production
	// record and byte windows. Reslicing the already fixed backing store changes
	// only the test collection's pressure threshold; all publication, fold,
	// retained-history, and recycle paths remain the production implementation.
	const window = 512
	unified.primaryUnifiedOverlay.ensureBacking()
	if len(unified.primaryUnifiedOverlay.records) < window {
		t.Fatalf("overlay records = %d, want at least %d",
			len(unified.primaryUnifiedOverlay.records), window)
	}
	unified.primaryUnifiedOverlay.records =
		unified.primaryUnifiedOverlay.records[:window]
	recordBytes := len(key) + len(after)
	for i := range window {
		value := after
		if i&1 != 0 || i == window-1 {
			value = before
		}
		if _, err := unified.Put(key, value); err != nil {
			t.Fatalf("fill overlay %d: %v", i, err)
		}
	}
	if got := unified.primaryUnifiedOverlay.count.Load(); got != uint32(window) {
		t.Fatalf("overlay count = %d, want %d", got, window)
	}
	if got, want := unified.primaryUnifiedOverlay.used.Load(),
		uint32(window*recordBytes); got != want {
		t.Fatalf("overlay used bytes = %d, want %d", got, want)
	}
	pinnedView, epoch, entered := unified.enterReadEpoch()
	if !entered {
		t.Fatal("could not pin test epoch reader")
	}
	pinnedState := *pinnedView.state
	pinnedState.root.Generation = pinnedView.generation
	pinnedState.root.DocumentCount = pinnedView.documentCount
	route, err := unified.currentPrimaryResidentRoute(&pinnedState, key)
	if err != nil {
		epoch.Exit()
		t.Fatal(err)
	}
	if _, err := unified.Put(key, after); err != nil {
		epoch.Exit()
		t.Fatalf("pressure Put: %v", err)
	}
	oldValue, disposition, _ := unified.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, pinnedView.generation,
	)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(oldValue, before) {
		epoch.Exit()
		t.Fatalf(
			"pinned generation overlay = %q,%v want %q",
			oldValue, disposition, before,
		)
	}
	got, ok, err := unified.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, after) {
		epoch.Exit()
		t.Fatalf("new generation read = %q,%v,%v want %q", got, ok, err, after)
	}
	epoch.Exit()

	resumeKey := []byte(keys[len(keys)-1])
	resumeBefore := canonicalDocs(t, docs[len(docs)-1:])[0]
	resumeAfter := bytes.Replace(
		append([]byte(nil), resumeBefore...),
		[]byte(`"active":false`), []byte(`"active":10e+0`), 1,
	)
	if bytes.Equal(resumeAfter, resumeBefore) {
		resumeAfter = bytes.Replace(
			resumeAfter,
			[]byte(`"active":true`), []byte(`"active":null`), 1,
		)
	}
	if _, err := unified.Put(resumeKey, resumeAfter); err != nil {
		t.Fatalf("resume overlay Put: %v", err)
	}
	if !unified.primaryUnifiedOverlay.hasPending() {
		t.Fatal("overlay did not recycle and resume after pinned reader exited")
	}
}

// TestUnifiedPrimaryWithIndexes pins that exact indexes built beside a
// unified graph stay consistent with the leaves' stable hash slots — the
// one-slot-discipline invariant (posting slot == read slot), exercised
// through the indexed scan path.
func TestUnifiedPrimaryWithIndexes(t *testing.T) {
	dir := t.TempDir()
	keys, docs := unifiedPrimaryCorpus(2000, false)
	want := canonicalDocs(t, docs)
	path := filepath.Join(dir, "unified-indexed.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		ResidentBytes: 16 << 20, Backend: BackendPortable,
		Indexes: []store.IndexDefinition{{Name: "country", Paths: []string{"/country"}}},
	}
	if _, err := CreateFromPrimary(unifiedPrimarySource(t, keys, docs), file, options); err != nil {
		t.Fatalf("CreateFromPrimary(unified, indexed): %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(mustOpen(t, path))
	if err != nil || !report.OK() {
		t.Fatalf("Verify: %v %+v", err, report.Findings)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(reopened, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		_ = collection.Close()
		_ = reopened.Close()
	}()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	expected := map[string][]byte{}
	for i := range keys {
		if bytes.Contains(docs[i], []byte(`"country":"PT"`)) {
			expected[keys[i]] = want[i]
		}
	}
	needle, err := vibejson.BuildIndex([]byte(`"PT"`), make([]vibejson.IndexEntry, 4))
	if err != nil {
		t.Fatal(err)
	}
	masks, err := snapshot.AppendIndexMasks(nil, "country", needle)
	if err != nil {
		t.Fatalf("AppendIndexMasks: %v", err)
	}
	got := map[string][]byte{}
	if err := snapshot.RangeMasksRaw(masks, func(k, v []byte) error {
		got[string(k)] = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("RangeMasksRaw: %v", err)
	}
	if len(got) != len(expected) || len(expected) == 0 {
		t.Fatalf("index scan matched %d rows want %d", len(got), len(expected))
	}
	for key, value := range expected {
		if !bytes.Equal(got[key], value) {
			t.Fatalf("indexed row %q mismatch:\n got %q\nwant %q", key, got[key], value)
		}
	}
}

// TestUnifiedPrimaryIndexedOverlayMutations proves class-5 and exact-index
// overlays publish as one generation: update, insert, and delete stay off the
// structural leaf path, then fold to the same point and index answers.
func TestUnifiedPrimaryIndexedOverlayMutations(t *testing.T) {
	keys, raw := unifiedPrimaryCorpus(1200, false)
	docs := make(map[string][]byte, len(keys))
	var ptKeys []string
	for i := range keys {
		docs[keys[i]] = raw[i]
		if bytes.Contains(raw[i], []byte(`"country":"PT"`)) &&
			len(ptKeys) < 2 {
			ptKeys = append(ptKeys, keys[i])
		}
	}
	if len(ptKeys) != 2 {
		t.Fatal("test corpus has fewer than two PT rows")
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	collection := buildIndexedPrimaryFile(
		t, t.TempDir(), "unified-indexed-mutation-*", docs, options,
	)
	initialCount := collection.state.Load().root.DocumentCount
	updatedKey := []byte(ptKeys[0])
	deletedKey := []byte(ptKeys[1])
	insertedKey := []byte(ptKeys[0] + "+")
	updated := []byte(`{"country":"ZZ","kind":"updated"}`)
	inserted := []byte(`{"country":"ZZ","kind":"inserted"}`)

	created, err := collection.Put(updatedKey, updated)
	if err != nil || created {
		t.Fatalf("indexed update = created %v, err %v", created, err)
	}
	created, err = collection.Put(insertedKey, inserted)
	if err != nil || !created {
		t.Fatalf("indexed insert = created %v, err %v", created, err)
	}
	deleted, err := collection.Delete(deletedKey)
	if err != nil || !deleted {
		t.Fatalf("indexed delete = deleted %v, err %v", deleted, err)
	}
	if !collection.primaryUnifiedOverlay.hasPending() {
		t.Fatal("indexed class-5 mutations left the primary overlay")
	}
	if collection.primaryEpoch == nil || collection.primaryEpoch.overlayEmpty() {
		t.Fatal("indexed class-5 mutations did not stage exact-index deltas")
	}
	if got := len(collection.primaryPendingParents); got != 0 {
		t.Fatalf("indexed overlay mutations created %d structural parents", got)
	}
	if got := collection.state.Load().root.DocumentCount; got != initialCount {
		t.Fatalf("document count = %d, want %d", got, initialCount)
	}

	zz := primaryExactTestKeys(
		t, collection, "country", primaryExactTestNeedle(t, `"ZZ"`),
	)
	if len(zz) != 2 ||
		zz[0] != string(updatedKey) || zz[1] != string(insertedKey) {
		t.Fatalf("country=ZZ keys = %v", zz)
	}
	pt := primaryExactTestKeys(
		t, collection, "country", primaryExactTestNeedle(t, `"PT"`),
	)
	for _, key := range pt {
		if key == string(updatedKey) || key == string(deletedKey) {
			t.Fatalf("country=PT retained mutated key %q in %v", key, pt)
		}
	}
	if collection.primaryUnifiedOverlay.hasPending() ||
		!collection.primaryEpoch.overlayEmpty() {
		t.Fatal("indexed snapshot did not fold both overlays")
	}
	if _, found, err := collection.AppendRaw(nil, deletedKey); err != nil || found {
		t.Fatalf("deleted point read = %v,%v", found, err)
	}
}
