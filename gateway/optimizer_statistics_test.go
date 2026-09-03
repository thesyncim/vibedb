package gateway

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestDistributedGroupCountsSeparatePartialAndFinal(t *testing.T) {
	statistics := gatewayTestStatistics()
	statistics[0].Groups = []GroupStatistics{{Paths: []string{"/n"}, Distinct: queryplanner.ExactEstimate(10)}}
	for i := range statistics[0].Partitions {
		statistics[0].Partitions[i].Groups = []GroupStatistics{{Paths: []string{"/n"}, Distinct: queryplanner.ExactEstimate(10)}}
	}
	snapshot, err := NewSnapshotWithPlannerMetadata(testConfig(t), testEndpoints(), 1, nil, statistics)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := snapshot.Prepare(t.Context(), `SELECT n, COUNT(*) FROM messages GROUP BY n`)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	route := routeBoundPlan(t, bound)
	partial, final, busiest, known := distributedGroupEstimates(snapshot, bound, route, 12000)
	if !known || partial != 20 || final != 10 || busiest != 10 {
		t.Fatalf("groups=%v/%v/%v known=%v", partial, final, busiest, known)
	}
	physical, _, err := optimizeDistributedPlan(t.Context(), snapshot, bound, route, DefaultProfiles()[ClassBatch])
	if err != nil {
		t.Fatal(err)
	}
	if physical.Expression.Op != queryplanner.OpFinalAggregate || physical.Cost.Network != 20*256 {
		t.Fatalf("wrong partial traffic:\n%s", physical)
	}
}

func TestDistributedFiltersUseNonRoutingStatistics(t *testing.T) {
	statistics := gatewayTestStatistics()
	statistics[0].Columns = append(statistics[0].Columns, ColumnStatistics{Path: "/n", Distinct: queryplanner.ExactEstimate(100), MostCommon: []ValueFrequency{{Value: "7", Frequency: .25}}})
	statistics[0].Groups = []GroupStatistics{{Paths: []string{"/tenant_id", "/n"}, Distinct: queryplanner.ExactEstimate(1000), MostCommon: []TupleFrequency{{Values: []string{`"acme"`, "7"}, Frequency: queryplanner.ExactEstimate(.08)}}}}
	snapshot, err := NewSnapshotWithPlannerMetadata(testConfig(t), testEndpoints(), 1, nil, statistics)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		sql  string
		want float64
	}{
		{`SELECT n FROM messages WHERE n = 7`, 3000},
		{`SELECT n FROM messages WHERE tenant_id = 'acme' AND n = 7`, 960},
	} {
		prepared, err := snapshot.Prepare(t.Context(), tc.sql)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := prepared.Bind(nil)
		if err != nil {
			t.Fatal(err)
		}
		route := routeBoundPlan(t, bound)
		_, _, rows, _ := distributedEstimates(snapshot, bound, route)
		if math.Abs(rows-tc.want) > .001 {
			t.Fatalf("%s: rows=%v want %v", tc.sql, rows, tc.want)
		}
	}
}

func TestDistributionKeyGroupingEliminatesFinalAggregate(t *testing.T) {
	snapshot := twoShardSnapshot(t, 1, 1)
	prepared, err := snapshot.Prepare(t.Context(), `SELECT tenant_id, COUNT(*) FROM messages GROUP BY tenant_id ORDER BY tenant_id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	physical, _, err := optimizeDistributedPlan(t.Context(), snapshot, bound, routeBoundPlan(t, bound), DefaultProfiles()[ClassBatch])
	if err != nil {
		t.Fatal(err)
	}
	if !bound.groupLocal || physicalPlanContains(physical, queryplanner.OpFinalAggregate) || physicalPlanContains(physical, queryplanner.OpRepartition) {
		t.Fatalf("lost locality proof:\n%s", physical)
	}
	if physicalPlanContains(physical, queryplanner.OpMergeGather) {
		t.Fatalf("partial shards falsely advertise ordering:\n%s", physical)
	}
}

func TestStatisticsBindingTracksOnlyQueryPredicates(t *testing.T) {
	stats := gatewayTestStatistics()
	for i := range 128 {
		stats[0].Columns = append(stats[0].Columns, ColumnStatistics{Path: fmt.Sprintf("/extra%d", i), Distinct: queryplanner.ExactEstimate(10)})
	}
	snapshot, err := NewSnapshotWithPlannerMetadata(testConfig(t), testEndpoints(), 1, nil, stats)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		sql   string
		paths int
	}{
		{`SELECT n FROM messages`, 0},
		{`SELECT n FROM messages WHERE extra1 = 1 AND extra2 IN (1, 2)`, 2},
		{`SELECT n FROM messages WHERE extra1 = 1 AND extra1 IN (1, 2)`, 1},
		{`SELECT n FROM messages WHERE extra1 = 1 OR extra2 = 2`, 0},
	} {
		prepared, err := snapshot.Prepare(t.Context(), tc.sql)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := prepared.Bind(nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(bound.statPaths) != tc.paths || len(bound.statConstraints) != tc.paths {
			t.Fatalf("%s: bound %d paths/%d domains, want %d", tc.sql, len(bound.statPaths), len(bound.statConstraints), tc.paths)
		}
	}
}

func TestE2ELocalGroupsEqualCentralCombiner(t *testing.T) {
	c := newE2ECluster(t)
	executor := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	// Compile one plan then run both physical execution strategies on the same
	// fenced data. This checks COUNT/SUM/MIN/MAX plus global ordering and offset.
	q := Query{SQL: `SELECT tenant_id, COUNT(*), SUM(n), MIN(n), MAX(n) FROM messages GROUP BY tenant_id ORDER BY tenant_id LIMIT 4 OFFSET 1`, Class: ClassBatch}
	snapshot := c.snapshot(t, 1)
	prepared, err := snapshot.Prepare(t.Context(), q.SQL)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	pl, err := executor.route(snapshot, &q, bound, DefaultProfiles()[ClassBatch])
	if err != nil {
		t.Fatal(err)
	}
	if !pl.groupLocal {
		t.Fatal("local grouping not selected")
	}
	local, err := executor.dispatch(t.Context(), pl, DefaultProfiles()[ClassBatch])
	if err != nil {
		t.Fatal(err)
	}
	pl.groupLocal = false
	central, err := executor.dispatch(t.Context(), pl, DefaultProfiles()[ClassBatch])
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Rows) != 4 || len(local.Rows) != len(central.Rows) {
		t.Fatalf("row counts %d/%d", len(local.Rows), len(central.Rows))
	}
	for i, row := range local.Rows {
		for j, cell := range row {
			want := central.Rows[i][j]
			if cell.Null != want.Null || string(cell.Bytes) != string(want.Bytes) {
				t.Fatalf("row %d col %d = %+v want %+v", i, j, cell, want)
			}
		}
	}
}

func TestGroupedBatchOwnsData(t *testing.T) {
	source := [][]shardservice.Cell{
		{{Bytes: []byte("tenant")}, {Bytes: []byte("17")}},
		{{Null: true}, {Bytes: []byte("23")}},
	}
	owned := appendGroupedBatch(nil, source)
	source[0][0].Bytes[0] = 'X'
	source[1][0].Null = false
	if string(owned[0][0].Bytes) != "tenant" || !owned[1][0].Null {
		t.Fatal("batch retains borrowed data")
	}
	// Appending to a cell or row must not overwrite an adjacent arena entry.
	owned[0][0].Bytes = append(owned[0][0].Bytes, 'x')
	owned[0] = append(owned[0], shardservice.Cell{Bytes: []byte("new")})
	if string(owned[0][1].Bytes) != "17" || !owned[1][0].Null || string(owned[1][1].Bytes) != "23" {
		t.Fatal("arena capacities permit cross-row mutation")
	}
}

func TestHashReducerLoadAndMemoryBudget(t *testing.T) {
	if got := hashPartitionGroupUpper(1, 64); got != 1 {
		t.Fatalf("one group peak=%v", got)
	}
	if got := hashPartitionGroupUpper(100000, 4); got <= 25000 || got >= 100000 {
		t.Fatalf("peak=%v", got)
	}
	profile := DefaultProfiles()[ClassBatch]
	profile.MaxAggregateBytes = 40 << 20
	profile.MaxWorkerAggregateBytes = 3 << 20
	limits, err := makeGroupedExchangeLimits(profile, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if limits.reducerMemory != 3<<20 {
		t.Fatalf("worker cap=%v", limits.reducerMemory)
	}
}

func TestGlobalIndexSelectionUsesSkew(t *testing.T) {
	snapshot := costedGlobalIndexSnapshot(t)
	prepared, err := snapshot.Prepare(t.Context(), `SELECT id FROM messages WHERE email = 'hot' AND id = 'needle'`)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	if bound.globalIndex == nil || bound.globalIndex.program.metadata.Name != "z_by_id" {
		t.Fatalf("selective global index not chosen: %+v", bound.globalIndex)
	}
}
func costedGlobalIndexSnapshot(t testing.TB) *Snapshot {
	t.Helper()
	config, endpoints := globalIndexCatalog(t)
	first := testGlobalIndexDescriptor()
	first.Flags &^= IndexUnique
	second := first
	second.IndexID++
	second.Name = "z_by_id"
	second.Relation = "messages_id_index"
	second.Paths = []string{"/id"}
	config.Placements = append(config.Placements, distribution.TablePlacement{Table: second.Relation, Distribution: "message_email_index", Columns: []string{"/id"}})
	statistics := []TableStatistics{{Table: "messages", Rows: queryplanner.ExactEstimate(1_000_000), RowBytes: queryplanner.ExactEstimate(128), Columns: []ColumnStatistics{
		{Path: "/email", Distinct: queryplanner.ExactEstimate(2), MostCommon: []ValueFrequency{{Value: `"hot"`, Frequency: .9}}},
		{Path: "/id", Distinct: queryplanner.ExactEstimate(1_000_000)},
	}}}
	snapshot, err := NewSnapshotWithPlannerMetadata(config, endpoints, 1, []IndexDescriptor{first, second}, statistics)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestUpdateIndexDependenciesKeepUnrelatedWritesLocal(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		set         string
		maintenance bool
	}{
		{`keep = 2`, false}, {`keep = keep + 1`, false}, {`email = 'new'`, true}, {`id = 'new'`, true}, {`tenant_id = 'new'`, true},
	} {
		prepared, err := snapshot.Prepare(t.Context(), `UPDATE messages SET `+tc.set+` WHERE tenant_id = 'tenant-7'`)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(prepared.writeGlobalIndexes) != 0; got != tc.maintenance {
			t.Fatalf("SET %s maintenance=%v want %v", tc.set, got, tc.maintenance)
		}
	}
	// Replacing a parent of an indexed nested path must retain maintenance;
	// escaped top-level names are compared as decoded path tokens.
	metadata, _ := snapshot.Index("messages", "by_email")
	for _, path := range []string{"/payload/email", "/pay~1load/email", "/pay~0load/email"} {
		metadata.Paths[0] = path
		column := strings.Split(path[1:], "/")[0]
		column = strings.ReplaceAll(strings.ReplaceAll(column, "~1", "/"), "~0", "~")
		prepared, err := snapshot.Prepare(t.Context(), `UPDATE messages SET "`+column+`" = 1 WHERE tenant_id = 'tenant-7'`)
		if err != nil {
			t.Fatal(err)
		}
		if !updateMayChangeGlobalIndex(&prepared.statement, metadata) {
			t.Fatalf("missed parent dependency %s", path)
		}
	}
}

func BenchmarkCostedGlobalIndexBind(b *testing.B) {
	snapshot := costedGlobalIndexSnapshot(b)
	prepared, err := snapshot.Prepare(b.Context(), `SELECT id FROM messages WHERE email = ? AND id = ?`)
	if err != nil {
		b.Fatal(err)
	}
	args := []any{"hot", "needle"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := prepared.Bind(args); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGroupedLocalityFinalization(b *testing.B) {
	const n = 10000
	rows := make([][]shardservice.Cell, n)
	for i := range rows {
		rows[i] = []shardservice.Cell{{Bytes: []byte(fmt.Sprint(i))}, {Bytes: []byte("1")}}
	}
	for _, local := range []bool{false, true} {
		name := "central-combine"
		if local {
			name = "local-gather"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(n, "groups/op")
			for b.Loop() {
				if local {
					var out [][]shardservice.Cell
					for start := 0; start < len(rows); start += int(distributedBatchRows) {
						out = appendGroupedBatch(out, rows[start:min(start+int(distributedBatchRows), len(rows))])
					}
					if len(out) != n {
						b.Fatal(len(out))
					}
				} else {
					merger, err := newGroupedAggregateMerger([]sqlast.AggKind{sqlast.AggNone, sqlast.AggCount}, []int{0}, 256<<20)
					if err != nil {
						b.Fatal(err)
					}
					for _, row := range rows {
						if err := merger.add(row); err != nil {
							b.Fatal(err)
						}
					}
					out, err := merger.finish()
					if err != nil || len(out) != n {
						b.Fatalf("groups=%d: %v", len(out), err)
					}
				}
			}
		})
	}
}

func BenchmarkMultiIndexDocumentRouting(b *testing.B) {
	snapshot := costedGlobalIndexSnapshot(b)
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		b.Fatal(err)
	}
	document := []byte(`{"tenant_id":"tenant-7","id":"message-9","email":"hot","payload":"` + strings.Repeat("x", 8192) + `"}`)
	for _, shared := range []bool{false, true} {
		name := "parse-per-index"
		if shared {
			name = "parse-once"
		}
		b.Run(name, func(b *testing.B) {
			var workspace GlobalIndexWorkspace
			b.ReportAllocs()
			b.SetBytes(int64(len(document)))
			for b.Loop() {
				if shared {
					index, err := workspace.indexDocument(document)
					if err != nil {
						b.Fatal(err)
					}
					for range 16 {
						if _, err := program.routeIndexedDocument(index, len(document), &workspace); err != nil {
							b.Fatal(err)
						}
					}
				} else {
					for range 16 {
						if _, err := program.RouteDocument(document, &workspace); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}
