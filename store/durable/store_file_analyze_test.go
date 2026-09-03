package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/planner"
)

func TestDurableSnapshotAnalyze(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "stats.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	for _, doc := range []struct{ k, v string }{{"a", `{"v":1}`}, {"b", `{"v":1.0}`}, {"c", `{"v":2}`}} {
		if _, err := collection.Put([]byte(doc.k), []byte(doc.v)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	analysis, err := snapshot.Analyze(t.Context(), 1, "t", "s", []string{"/v"}, nil, planner.AnalyzeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := planner.MergePartitionStatistics(t.Context(), analysis)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows.Value != 3 || stats.Columns[0].Distinct.Value != 2 {
		t.Fatalf("statistics %+v", stats)
	}
}
