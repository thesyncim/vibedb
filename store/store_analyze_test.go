package store

import (
	"testing"

	"github.com/thesyncim/vibedb/planner"
)

func TestSnapshotAnalyzePinsRowsAndPreservesNumericEquality(t *testing.T) {
	collection := &Collection{}
	for key, doc := range map[string]string{"a": `{"v":1,"n":null}`, "b": `{"v":1.0}`, "c": `{"v":2,"n":3}`} {
		if _, err := collection.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("d", []byte(`{"v":4}`)); err != nil {
		t.Fatal(err)
	}
	analyzed, err := snapshot.Analyze(t.Context(), 1, "t", "shard", []string{"/v", "/n"}, [][]string{{"/v", "/n"}}, planner.AnalyzeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := planner.MergePartitionStatistics(t.Context(), analyzed)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows.Value != 3 || stats.Columns[0].Distinct.Value != 2 || stats.Columns[1].NullFraction != 2.0/3 || stats.Groups[2].Distinct.Value != 2 {
		t.Fatalf("statistics %+v", stats)
	}
}
