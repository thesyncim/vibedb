package gateway

import (
	"errors"
	"testing"

	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestReviewSharedPlanProfileAdmission(t *testing.T) {
	client, _ := newTwoShardCluster(t, 3)
	t.Cleanup(func() { _ = client.Close() })
	profiles := DefaultProfiles()
	low := profiles[ClassInteractive]
	low.MaxAggregateBytes = 1024
	profiles[ClassInteractive] = low
	executor := NewExecutor(
		client, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{Profiles: profiles},
	)
	q := Query{SQL: `SELECT n FROM messages WHERE tenant_id = 'a1'`, Class: ClassInteractive}
	if _, err := executor.Query(t.Context(), q); !errors.Is(err, queryplanner.ErrNoPlan) {
		t.Fatalf("cold low-profile query error = %v, want planner.ErrNoPlan", err)
	}
	q.Class = ClassBatch
	if _, err := executor.Query(t.Context(), q); err != nil {
		t.Fatalf("batch query: %v", err)
	}
	q.Class = ClassInteractive
	if _, err := executor.Query(t.Context(), q); !errors.Is(err, queryplanner.ErrNoPlan) {
		t.Fatalf("warm low-profile query error = %v, want planner.ErrNoPlan", err)
	}
}

func TestReviewSharedPlanPartitionAdmission(t *testing.T) {
	c := newE2ECluster(t)
	stats := []TableStatistics{{
		Table: "messages", Rows: Estimate{Value: 100000, Upper: 100000},
		RowBytes: Estimate{Value: 128, Upper: 128}, Partitions: []PartitionStatistics{
			{Partition: string(c.shards[0].id), Rows: Estimate{Value: 10, Upper: 10}},
			{Partition: string(c.shards[1].id), Rows: Estimate{Value: 90000, Upper: 90000}},
		},
	}}
	profiles := DefaultProfiles()
	low := profiles[ClassInteractive]
	low.MaxAggregateBytes = 128 << 10
	profiles[ClassInteractive] = low
	executor := NewExecutor(
		c.client,
		NewCatalogHolder(c.snapshotWithStatistics(t, 1, stats)),
		Options{Profiles: profiles},
	)
	q := Query{
		SQL:    `SELECT n FROM messages WHERE tenant_id = ?`,
		Class:  ClassInteractive,
		Params: stringParams(c.shards[1].keys[0]),
	}
	if _, err := executor.Query(t.Context(), q); !errors.Is(err, queryplanner.ErrNoPlan) {
		t.Fatalf("cold large-partition query error = %v, want planner.ErrNoPlan", err)
	}
	q.Params = stringParams(c.shards[0].keys[0])
	if _, err := executor.Query(t.Context(), q); err != nil {
		t.Fatalf("small-partition query: %v", err)
	}
	q.Params = stringParams(c.shards[1].keys[0])
	if _, err := executor.Query(t.Context(), q); !errors.Is(err, queryplanner.ErrNoPlan) {
		t.Fatalf("warm large-partition query error = %v, want planner.ErrNoPlan", err)
	}
}

func TestReviewSharedPlanSameTargetParameterAdmission(t *testing.T) {
	client, _ := newTwoShardCluster(t, 3)
	t.Cleanup(func() { _ = client.Close() })
	executor := NewExecutor(client, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})
	q := Query{
		SQL:   `SELECT n FROM messages WHERE tenant_id = ? ORDER BY n LIMIT ?`,
		Class: ClassInteractive,
		Params: []shardservice.Param{
			shardservice.StringParam("a1"), shardservice.NumberParam("1"),
		},
	}
	first, err := executor.Query(t.Context(), q)
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if got := decodeInts(t, first.Rows); !equalInts(got, []int64{1}) {
		t.Fatalf("first rows = %v, want [1]", got)
	}
	executor.planMu.RLock()
	entry, ok := executor.planCache[q.SQL]
	executor.planMu.RUnlock()
	if !ok || entry.physical == nil {
		t.Fatal("first query did not publish a physical plan")
	}
	physical := entry.physical

	// Both values route to the same target, while the predicate and LIMIT
	// change the bound execution. The shared physical plan may be reused only
	// after the exact target and admission context have been revalidated.
	q.Params = []shardservice.Param{
		shardservice.StringParam("a3"), shardservice.NumberParam("0"),
	}
	second, err := executor.Query(t.Context(), q)
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	if got := decodeInts(t, second.Rows); len(got) != 0 {
		t.Fatalf("second rows = %v, want no rows", got)
	}
	executor.planMu.RLock()
	again, ok := executor.planCache[q.SQL]
	executor.planMu.RUnlock()
	if !ok || again.physical != physical {
		t.Fatal("same-target predicate/LIMIT query re-optimized instead of reusing the physical plan")
	}
}
