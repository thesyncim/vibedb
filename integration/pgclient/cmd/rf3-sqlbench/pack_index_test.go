package main

import (
	"strings"
	"testing"
	"time"
)

func TestPackIndexDDLAndSharedValues(t *testing.T) {
	leading := config{indexes: indexModePackLeading, sharedBytes: 256, sharedCardinality: 8}
	if tableDDL(leading, "rf3_sql_bench") != "CREATE TABLE IF NOT EXISTS rf3_sql_bench (id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, score INTEGER NOT NULL, payload TEXT NOT NULL, shared TEXT NOT NULL, a TEXT NOT NULL, b TEXT NOT NULL)" {
		t.Fatalf("unexpected table ddl: %s", tableDDL(leading, "rf3_sql_bench"))
	}
	got := indexDDLs(leading, "rf3_sql_bench")
	want := []string{
		"CREATE INDEX IF NOT EXISTS rf3_sql_bench_pack_a ON rf3_sql_bench (bucket, shared, a)",
		"CREATE INDEX IF NOT EXISTS rf3_sql_bench_pack_b ON rf3_sql_bench (bucket, shared, b)",
	}
	if strings.Join(got, ";") != strings.Join(want, ";") {
		t.Fatalf("leading indexes: %v", got)
	}
	nonleading := leading
	nonleading.indexes = indexModePackNonleading
	got = indexDDLs(nonleading, "rf3_sql_bench")
	if !strings.Contains(got[0], "(bucket, a, shared)") || !strings.Contains(got[1], "(bucket, b, shared)") {
		t.Fatalf("nonleading indexes: %v", got)
	}
	if indexDDLs(config{indexes: indexModeNone}, "rf3_sql_bench") != nil {
		t.Fatal("none must not create indexes")
	}
	first := packSharedValue(leading, 0)
	again := packSharedValue(leading, 0)
	other := packSharedValue(leading, 1)
	if len(first) != 256 || first != again {
		t.Fatalf("shared value length/stability: %d %q", len(first), first)
	}
	if first == other {
		t.Fatal("adjacent rows unexpectedly share the same shared value")
	}
	seen := map[string]struct{}{}
	for row := 0; row < 4096; row++ {
		seen[packSharedValue(leading, row)] = struct{}{}
	}
	if len(seen) != 8 {
		t.Fatalf("cardinality 8 produced %d values", len(seen))
	}
}

func TestPackInsertAndVerifySQL(t *testing.T) {
	c := config{indexes: indexModePackLeading, sharedBytes: 16, sharedCardinality: 8, payloadMode: defaultPayloadMode}
	var sql strings.Builder
	sql.WriteString("INSERT INTO t " + insertColumns(c) + " VALUES ")
	appendInsertRow(&sql, c, 0)
	text := sql.String()
	if !strings.Contains(text, "shared") || !strings.Contains(text, packA(0)) || !strings.Contains(text, packB(0)) {
		t.Fatalf("insert missing pack columns: %s", text)
	}
	if verifyColumnCount(c) != 7 || !strings.Contains(verifySelectSQL(c, "t"), "shared,a,b") {
		t.Fatalf("verify sql: %s", verifySelectSQL(c, "t"))
	}
	none := config{}
	if verifyColumnCount(none) != 4 || insertColumns(none) != "(id,bucket,score,payload)" {
		t.Fatal("pk-only schema changed")
	}
}

func TestTenMillionRowsAreAdmitted(t *testing.T) {
	c := config{
		engine: "vibedb", url: "postgresql://local@127.0.0.1:1/db?sslmode=disable",
		output: t.TempDir() + "/unused.json", phase: "run", rows: maxRows,
		operations: 2, scans: 1, repetitions: 1, seedBatch: 64, timeout: 45 * time.Second,
		clients: "1", tables: defaultTable, workloads: "point_hit",
		groupDistribution: "uniform", skewPercent: 80, indexes: indexModePackLeading,
		sharedBytes: 256, sharedCardinality: 8, payloadMode: defaultPayloadMode,
	}
	if err := run(c); err == nil {
		t.Fatal("connection unexpectedly succeeded")
	} else if strings.Contains(err.Error(), "invalid benchmark configuration") {
		t.Fatalf("10M pack-leading config rejected: %v", err)
	}
}
