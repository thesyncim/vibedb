package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestDurableTinPersistsAcrossReopenZeroScanAndIncrementalCatchup(t *testing.T) {
	dir := t.TempDir()
	db1, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	options := testDatabaseOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
	}
	docs1, err := db1.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	const initialDocs = 500
	for i := 0; i < initialDocs; i++ {
		body := fmt.Sprintf("ordinary archive document %04d", i)
		if i%50 == 0 {
			body = fmt.Sprintf("luxury vintage timepiece %04d", i)
		}
		mustPut(t, docs1, fmt.Sprintf("doc-%04d", i), fmt.Sprintf(`{"body":%q}`, body))
	}

	snap1, err := docs1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	hits1, err := docs1.TinSearch(snap1, "/body", "luxury", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits1) != initialDocs/50 {
		t.Fatalf("initial hits = %d, want %d", len(hits1), initialDocs/50)
	}
	if err := snap1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen #1: exact generation match -> ZERO full scans, loaded straight from sidecar!
	db2, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	docs2, ok := db2.Collection("docs")
	if !ok {
		t.Fatal("missing docs collection after reopen")
	}
	snap2, err := docs2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	hits2, err := docs2.TinSearch(snap2, "/body", "luxury", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits2) != len(hits1) {
		t.Fatalf("reopened hits = %d, want %d", len(hits2), len(hits1))
	}
	if docs2.tinPersistedLoads != 1 || docs2.tinFullBuilds != 0 || docs2.snapshotFullScanCalls.Load() != 0 {
		t.Fatalf("reopen #1 expected zero-scan persisted load: persisted=%d full=%d fullScans=%d",
			docs2.tinPersistedLoads, docs2.tinFullBuilds, docs2.snapshotFullScanCalls.Load())
	}
	if err := snap2.Close(); err != nil {
		t.Fatal(err)
	}

	// Write a new document BEFORE closing (without searching first!), so the
	// on-disk sidecar is 1 generation behind the collection on reopen #2.
	mustPut(t, docs2, "doc-9001", `{"body":"luxury sapphire timepiece"}`)
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen #2: sidecar is behind by 1 write -> loads sidecar and catches up
	// incrementally with ZERO full collection scan!
	db3, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	docs3, ok := db3.Collection("docs")
	if !ok {
		t.Fatal("missing docs collection after reopen #2")
	}
	snap3, err := docs3.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	hits3, err := docs3.TinSearch(snap3, "/body", "sapphire", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits3) != 1 || hits3[0].Key != "doc-9001" {
		t.Fatalf("reopen #2 sapphire hits = %+v, want [doc-9001]", hits3)
	}
	if docs3.tinPersistedLoads != 1 || docs3.tinIncrementalBuilds != 1 || docs3.tinFullBuilds != 0 || docs3.snapshotFullScanCalls.Load() != 0 {
		t.Fatalf("reopen #2 expected incremental catchup from sidecar: persisted=%d inc=%d full=%d fullScans=%d",
			docs3.tinPersistedLoads, docs3.tinIncrementalBuilds, docs3.tinFullBuilds, docs3.snapshotFullScanCalls.Load())
	}
	if err := snap3.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db3.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen #3 with a corrupted .tin sidecar -> must fail closed to a full
	// rebuild and return exact results!
	matches, err := filepath.Glob(filepath.Join(dir, "*.tin"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected 1 .tin sidecar file, got %v (err=%v)", matches, err)
	}
	if err := os.WriteFile(matches[0], []byte("corrupted-sidecar-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	db4, err := OpenDatabase(dir, DatabaseOptions{Options: testDatabaseOptions()})
	if err != nil {
		t.Fatal(err)
	}
	defer db4.Close()
	docs4, ok := db4.Collection("docs")
	if !ok {
		t.Fatal("missing docs collection after reopen #3")
	}
	snap4, err := docs4.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap4.Close()
	hits4, err := docs4.TinSearch(snap4, "/body", "sapphire", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits4) != 1 || hits4[0].Key != "doc-9001" {
		t.Fatalf("reopen #3 after corruption sapphire hits = %+v, want [doc-9001]", hits4)
	}
	if docs4.tinFullBuilds != 1 {
		t.Fatalf("expected fallback full build after corruption, got tinFullBuilds=%d", docs4.tinFullBuilds)
	}
	// DropCollection must remove the .tin sidecar cleanly.
	if err := snap4.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db4.DropCollection("docs"); err != nil {
		t.Fatal(err)
	}
	remaining, _ := filepath.Glob(filepath.Join(dir, "*.tin"))
	if len(remaining) != 0 {
		t.Fatalf("DropCollection left .tin sidecar(s) behind: %v", remaining)
	}
}
