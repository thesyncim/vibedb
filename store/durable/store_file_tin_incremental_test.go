package durable

import (
	"fmt"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestDurableTinIncrementalMaintenanceMatchesFullBuild(t *testing.T) {
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

	// Populate enough rows to span multiple primary leaves so single-row
	// mutations rewrite only one leaf and trigger incremental maintenance.
	const initialDocs = 600
	for i := 0; i < initialDocs; i++ {
		body := fmt.Sprintf("ordinary catalog record number %04d with filler text", i)
		if i%60 == 0 {
			body = fmt.Sprintf("luxury vintage timepiece number %04d", i)
		}
		mustPut(t, docs, fmt.Sprintf("doc-%04d", i), fmt.Sprintf(`{"body":%q}`, body))
	}

	snap0, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap0.Close()

	hits0, err := docs.TinSearch(snap0, "/body", "luxury", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits0) != initialDocs/60 {
		t.Fatalf("initial hits = %d, want %d", len(hits0), initialDocs/60)
	}
	if docs.tinFullBuilds != 1 || docs.tinIncrementalBuilds != 0 {
		t.Fatalf("after initial search: full=%d inc=%d, want 1/0", docs.tinFullBuilds, docs.tinIncrementalBuilds)
	}
	fullScansAfterInitial := docs.snapshotFullScanCalls.Load()

	// Step 1: insert a new document.
	mustPut(t, docs, "doc-9001", `{"body":"luxury sapphire timepiece"}`)
	snap1, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap1.Close()
	assertDurableTinMatchesFresh(t, docs, snap1, "step1-insert")

	// Step 2: update an existing matching document's text.
	mustPut(t, docs, "doc-0000", `{"body":"ordinary replaced record"}`)
	snap2, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap2.Close()
	assertDurableTinMatchesFresh(t, docs, snap2, "step2-update-text")

	// Step 3: update a matching document's /body to a non-string (must disappear from tin).
	mustPut(t, docs, "doc-0060", `{"body":42}`)
	snap3, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap3.Close()
	assertDurableTinMatchesFresh(t, docs, snap3, "step3-non-string")

	// Step 4: delete an existing matching document.
	if _, err := docs.Delete([]byte("doc-0120")); err != nil {
		t.Fatal(err)
	}
	snap4, err := docs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap4.Close()
	assertDurableTinMatchesFresh(t, docs, snap4, "step4-delete")

	// Verify every post-initial generation was maintained incrementally without
	// any additional full RangeRaw collection scan.
	if docs.tinFullBuilds != 1 {
		t.Fatalf("tinFullBuilds = %d, want 1", docs.tinFullBuilds)
	}
	if docs.tinIncrementalBuilds != 4 {
		t.Fatalf("tinIncrementalBuilds = %d, want 4", docs.tinIncrementalBuilds)
	}
	// Note: assertDurableTinMatchesFresh calls buildTinSnapshot directly (which
	// increments snapshotFullScanCalls by 1 per step). Verify the collection's
	// own tinBuild never added extra full scans beyond those 4 reference checks.
	if got := docs.snapshotFullScanCalls.Load(); got != fullScansAfterInitial+4 {
		t.Fatalf("snapshotFullScanCalls = %d, want %d (only the 4 test reference builds)", got, fullScansAfterInitial+4)
	}

	// Verify old pinned snapshot still sees its exact original hits.
	hits0Again, err := docs.TinSearch(snap0, "/body", "luxury", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits0Again) != len(hits0) {
		t.Fatalf("snap0 hits changed: got %d, want %d", len(hits0Again), len(hits0))
	}
}

func assertDurableTinMatchesFresh(t *testing.T, docs *Collection, snap *Snapshot, label string) {
	t.Helper()
	paths, err := compileSnapshotTinPaths(snap)
	if err != nil {
		t.Fatalf("%s compileSnapshotTinPaths: %v", label, err)
	}
	fresh, err := buildTinSnapshot(snap, paths)
	if err != nil {
		t.Fatalf("%s buildTinSnapshot: %v", label, err)
	}
	build, err := snap.TinBuildForPath("/body")
	if err != nil {
		t.Fatalf("%s TinBuildForPath: %v", label, err)
	}
	for _, query := range []string{"luxury", "sapphire", "replaced", "vintage AND timepiece"} {
		got, err := docs.TinSearch(snap, "/body", query, 50)
		if err != nil {
			t.Fatalf("%s TinSearch(%q): %v", label, query, err)
		}
		freshIx := fresh.indexes["body_tin"]
		qf, err := freshIx.ParseTINQL(query)
		if err != nil {
			t.Fatalf("%s fresh ParseTINQL(%q): %v", label, query, err)
		}
		wantScored := freshIx.Score(qf, 50, nil)
		if len(got) != len(wantScored) {
			t.Fatalf("%s query %q: got %d hits, want %d", label, query, len(got), len(wantScored))
		}
		gotMap := make(map[string]float64, len(got))
		for _, h := range got {
			gotMap[h.Key] = h.Score
		}
		for _, w := range wantScored {
			wKey := string(fresh.keys[uint64(w.Doc)])
			gScore, ok := gotMap[wKey]
			if !ok {
				t.Fatalf("%s query %q: missing key %q in incremental hits", label, query, wKey)
			}
			if gScore != w.Score {
				t.Fatalf("%s query %q key %q: got score %v, want %v", label, query, wKey, gScore, w.Score)
			}
		}

		// Also verify AppendCandidateMasks + RangeMasksRaw pruning matches exact hit keys!
		qi, err := build.Index().ParseTINQL(query)
		if err != nil {
			t.Fatalf("%s build ParseTINQL(%q): %v", label, query, err)
		}
		ids := build.Index().Match(qi, nil)
		masks, _, ok := build.AppendCandidateMasks(snap.PrimaryRouter(), ids, nil, nil)
		if !ok {
			t.Fatalf("%s query %q: AppendCandidateMasks declined unexpectedly", label, query)
		}
		var maskedKeys []string
		if err := snap.RangeMasksRaw(masks, func(key, _ []byte) error {
			maskedKeys = append(maskedKeys, string(key))
			return nil
		}); err != nil {
			t.Fatalf("%s query %q RangeMasksRaw: %v", label, query, err)
		}
		wantKeys := make([]string, 0, len(got))
		for _, h := range got {
			wantKeys = append(wantKeys, h.Key)
		}
		sort.Strings(wantKeys)
		sort.Strings(maskedKeys)
		if len(maskedKeys) != len(wantKeys) {
			t.Fatalf("%s query %q: maskedKeys=%v wantKeys=%v", label, query, maskedKeys, wantKeys)
		}
		for i := range wantKeys {
			if maskedKeys[i] != wantKeys[i] {
				t.Fatalf("%s query %q: maskedKeys=%v wantKeys=%v", label, query, maskedKeys, wantKeys)
			}
		}
	}
}
