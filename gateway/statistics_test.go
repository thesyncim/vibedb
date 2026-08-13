package gateway

import (
	"errors"
	"path/filepath"
	"testing"

	queryplanner "github.com/thesyncim/vibedb/planner"
)

func gatewayTestStatistics() []TableStatistics {
	return []TableStatistics{{
		Table:    "messages",
		Rows:     Estimate{Value: 10_000, Lower: 9_000, Upper: 12_000, Confidence: .95},
		RowBytes: Estimate{Value: 200, Lower: 100, Upper: 256, Confidence: .9},
		Partitions: []PartitionStatistics{
			{Partition: "-80", Rows: Estimate{Value: 5_500, Upper: 7_000, Confidence: .9}},
			{Partition: "80-", Rows: Estimate{Value: 4_500, Upper: 5_000, Confidence: .9}},
		},
		Columns: []ColumnStatistics{{
			Path: "/tenant_id", Distinct: Estimate{Value: 1000, Upper: 1200, Confidence: .9},
			MostCommon: []ValueFrequency{{Value: `"acme"`, Frequency: .1}},
		}},
	}}
}

func TestSnapshotStatisticsPersistAndPlan(t *testing.T) {
	snapshot, err := NewSnapshotWithPlannerMetadata(
		testConfig(t), testEndpoints(), 7, nil, gatewayTestStatistics(),
	)
	if err != nil {
		t.Fatal(err)
	}
	stats, ok := snapshot.Statistics("messages")
	if !ok || stats.Rows().Value != 10_000 || stats.RowBytes().Upper != 256 {
		t.Fatalf("statistics = %+v/%v", stats, ok)
	}
	if snapshot.PlannerStatisticsBytes() == 0 {
		t.Fatal("statistics metadata reports zero retained bytes")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		table, _ := snapshot.Statistics("messages")
		column, _ := table.Column("/tenant_id")
		_ = column.EqualitySelectivity(`"acme"`)
	}); allocs != 0 {
		t.Fatalf("snapshot statistics lookup allocations = %v, want 0", allocs)
	}

	executor := newRouteExecutor(t, snapshot)
	physical, err := routeSQL(t, executor, snapshot, `SELECT n FROM messages`, ClassBatch)
	if err != nil {
		t.Fatal(err)
	}
	if physical.physical == nil || physical.physical.Expression.Op != queryplanner.OpGather ||
		len(physical.physical.Children) != 1 || physical.physical.Children[0].Expression.Op != queryplanner.OpRemoteQuery {
		t.Fatalf("physical plan:\n%s", physical.physical)
	}
	// All shards are selected, so the risk-aware remote IO estimate uses the
	// table's upper row and width bounds: 12,000 * 256.
	if got, want := physical.physical.Children[0].Cost.IO, float64(12_000*256); got != want {
		t.Fatalf("remote IO cost = %v, want %v", got, want)
	}

	prepared, err := snapshot.Prepare(t.Context(),
		`SELECT n FROM messages WHERE tenant_id = 'acme'`)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	route := routeBoundPlan(t, bound)
	scanRows, outputRows, width := distributedEstimates(snapshot, bound, route)
	wantScan := 0.0
	for _, target := range route.Targets {
		partition, ok := stats.PartitionRows(string(target.Shard))
		if !ok {
			t.Fatalf("missing partition statistics for %s", target.Shard)
		}
		wantScan += partition.Upper
	}
	if scanRows != wantScan || outputRows != min(wantScan, 1_200.0) || width != 256 {
		t.Fatalf("predicate estimates = scan=%v output=%v width=%v, want scan=%v output=%v width=256",
			scanRows, outputRows, width, wantScan, min(wantScan, 1_200.0))
	}

	emptyPrepared, err := snapshot.Prepare(t.Context(),
		`SELECT n FROM messages WHERE tenant_id = 'a' AND tenant_id = 'b'`)
	if err != nil {
		t.Fatal(err)
	}
	emptyBound, err := emptyPrepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyRoute := routeBoundPlan(t, emptyBound)
	if scan, output, _ := distributedEstimates(snapshot, emptyBound, emptyRoute); scan != 0 || output != 0 {
		t.Fatalf("empty-route estimates = scan=%v output=%v, want zero", scan, output)
	}

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	loadedStats, ok := loaded.Statistics("messages")
	if !ok || loadedStats.Rows().Value != 10_000 || loadedStats.RowBytes().Upper != 256 {
		t.Fatalf("loaded statistics = %+v/%v", loadedStats, ok)
	}
	if partition, ok := loadedStats.PartitionRows("-80"); !ok || partition.Upper != 7_000 {
		t.Fatalf("loaded partition statistics = %+v/%v", partition, ok)
	}
}

func TestSnapshotStatisticsRejectUnplacedTable(t *testing.T) {
	statistics := gatewayTestStatistics()
	statistics[0].Table = "missing"
	_, err := NewSnapshotWithPlannerMetadata(testConfig(t), testEndpoints(), 1, nil, statistics)
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("error = %v, want ErrInvalidCatalog", err)
	}
}

func TestSnapshotStatisticsRejectInactivePartition(t *testing.T) {
	statistics := gatewayTestStatistics()
	statistics[0].Partitions[0].Partition = "retired"
	_, err := NewSnapshotWithPlannerMetadata(testConfig(t), testEndpoints(), 1, nil, statistics)
	if !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("error = %v, want ErrInvalidCatalog", err)
	}
}
