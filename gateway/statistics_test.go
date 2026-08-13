package gateway

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
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
	scanRows, scanBytes, outputRows, width := distributedEstimates(snapshot, bound, route)
	wantScan := 0.0
	for _, target := range route.Targets {
		partition, ok := stats.PartitionRows(string(target.Shard))
		if !ok {
			t.Fatalf("missing partition statistics for %s", target.Shard)
		}
		wantScan += partition.Upper
	}
	if scanRows != wantScan || scanBytes != wantScan*256 ||
		outputRows != min(wantScan, 1_200.0) || width != 256 {
		t.Fatalf("predicate estimates = scan=%v bytes=%v output=%v width=%v, want scan=%v bytes=%v output=%v width=256",
			scanRows, scanBytes, outputRows, width, wantScan, wantScan*256, min(wantScan, 1_200.0))
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
	if scan, bytes, output, _ := distributedEstimates(snapshot, emptyBound, emptyRoute); scan != 0 || bytes != 0 || output != 0 {
		t.Fatalf("empty-route estimates = scan=%v bytes=%v output=%v, want zero", scan, bytes, output)
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

func TestDistributedEstimatesAccountForEveryColocatedJoinTable(t *testing.T) {
	config := testConfig(t)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "users", Distribution: "tenant_data", Columns: []string{"/tenant_id"},
	})
	statistics := gatewayTestStatistics()
	statistics = append(statistics, TableStatistics{
		Table:    "users",
		Rows:     Estimate{Value: 1_800, Lower: 1_500, Upper: 2_000, Confidence: .9},
		RowBytes: Estimate{Value: 48, Lower: 32, Upper: 64, Confidence: .9},
		Partitions: []PartitionStatistics{
			{Partition: "-80", Rows: Estimate{Value: 1_000, Upper: 1_200, Confidence: .9}},
			{Partition: "80-", Rows: Estimate{Value: 800, Upper: 800, Confidence: .9}},
		},
	})
	snapshot, err := NewSnapshotWithPlannerMetadata(config, testEndpoints(), 1, nil, statistics)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := snapshot.Prepare(t.Context(), `
		SELECT messages.n
		FROM messages JOIN users ON messages.tenant_id = users.tenant_id`)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.tables) != 2 || bound.tables[0] != "messages" || bound.tables[1] != "users" {
		t.Fatalf("physical relation list = %v", bound.tables)
	}
	route := routeBoundPlan(t, bound)
	if err := bound.ValidateRoute(route); err != nil {
		t.Fatal(err)
	}
	scanRows, scanBytes, outputRows, width := distributedEstimates(snapshot, bound, route)
	const (
		wantScanRows   = 14_000.0
		wantScanBytes  = 12_000.0*256 + 2_000.0*64
		wantOutputRows = 12_000.0 * 2_000.0
		wantWidth      = 320.0
	)
	if scanRows != wantScanRows || scanBytes != wantScanBytes ||
		outputRows != wantOutputRows || width != wantWidth {
		t.Fatalf("join estimates = rows=%v bytes=%v output=%v width=%v, want %v/%v/%v/%v",
			scanRows, scanBytes, outputRows, width,
			wantScanRows, wantScanBytes, wantOutputRows, wantWidth)
	}
	physical, _, err := optimizeDistributedPlan(
		t.Context(), snapshot, bound, route, DefaultProfiles()[ClassBatch],
	)
	if err != nil {
		t.Fatal(err)
	}
	if physical.Expression.Op != queryplanner.OpGather || len(physical.Children) != 1 ||
		physical.Children[0].Cost.IO != wantScanBytes {
		t.Fatalf("join physical cost did not retain all scan bytes:\n%s", physical)
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

func BenchmarkDistributedEstimatesSkewedPointRoute(b *testing.B) {
	snapshot, err := NewSnapshotWithPlannerMetadata(
		testConfig(b), testEndpoints(), 7, nil, gatewayTestStatistics(),
	)
	if err != nil {
		b.Fatal(err)
	}
	prepared, err := snapshot.Prepare(b.Context(),
		`SELECT n FROM messages WHERE tenant_id = 'acme'`)
	if err != nil {
		b.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		b.Fatal(err)
	}
	route := routeBoundPlan(b, bound)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, _, _ = distributedEstimates(snapshot, bound, route)
	}
}
