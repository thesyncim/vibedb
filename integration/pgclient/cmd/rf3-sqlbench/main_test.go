package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInvalidEndpointDoesNotLeakCredentials(t *testing.T) {
	for _, endpoint := range []string{"postgresql://alice:swordfish@localhost:70000/db", "https://alice:swordfish@localhost/db", "postgresql://alice:swordfish@localhost:invalid/db"} {
		_, _, err := parseURLs(endpoint, "")
		if err == nil || strings.Contains(err.Error(), "swordfish") || strings.Contains(err.Error(), "alice") {
			t.Fatalf("unsafe endpoint error: %v", err)
		}
	}
	err := fmt.Errorf("connect user=alice password=swordfish postgresql://alice:swordfish@localhost/db")
	if text := safeError(err, []string{"postgresql://alice:swordfish@localhost/db"}); strings.Contains(text, "swordfish") || strings.Contains(text, "alice") {
		t.Fatalf("unsafe connection error: %s", text)
	}
}

func TestClientsRejectDuplicateTrials(t *testing.T) {
	for _, raw := range []string{"1,1", "0", "16", "", "1,8,8"} {
		if _, err := parseClients(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func TestEarlyConnectionFailureRetainsStructuredReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	c := config{engine: "vibedb", url: "postgresql://alice:swordfish@127.0.0.1:1/db?sslmode=disable", output: path,
		phase: "run", rows: 64, operations: 2, scans: 1, repetitions: 1, clients: "1",
		tables: defaultTable, workloads: "point_hit", groupDistribution: "uniform", skewPercent: 80}
	if err := run(c); err == nil {
		t.Fatal("connection unexpectedly succeeded")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r report
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if r.SchemaVersion != 2 || r.Status != "failed" || r.VerificationError == "" || len(r.Results) != 0 || strings.Contains(string(raw), "swordfish") {
		t.Fatalf("invalid failure report: %s", raw)
	}
	before := string(raw)
	if err := run(c); err == nil {
		t.Fatal("reused output path")
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != before {
		t.Fatalf("existing evidence was changed: %v", err)
	}
}

func TestAtomicCheckpointRetainsCompletedTrialDuringNextTrial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	r := report{SchemaVersion: 2, Status: "incomplete", VerificationError: "benchmark did not finish", Results: []result{{Repetition: 1, Verified: true}},
		ActiveTrial: &trialRecord{Workload: "point_hit", Clients: 8, Repetition: 2, Phase: "preparing-or-measuring"}}
	if err := writeReport(path, r); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(raw, &got); err != nil || len(got.Results) != 1 || !got.Results[0].Verified || got.ActiveTrial.Repetition != 2 {
		t.Fatalf("checkpoint lost trial state: %s, %v", raw, err)
	}
}

func TestStreamsDoNotPartitionGroupsOrKeysByClient(t *testing.T) {
	c := config{groupDistribution: "uniform"}
	var counts [8][4][2]int
	var keyBuckets [8][16]int
	for ordinal := 0; ordinal < 40000; ordinal++ {
		client := ordinal % 8
		op := 0
		if mixedRead(ordinal) {
			op = 1
		}
		counts[client][groupFor(c, 4, ordinal)][op]++
		keyBuckets[client][readKeyFor(8192, 1, ordinal)%16]++
	}
	for client := range counts {
		for group := range counts[client] {
			for op, count := range counts[client][group] {
				if count < 450 || count > 800 {
					t.Fatalf("client %d group %d operation %d count %d", client, group, op, count)
				}
			}
		}
		for bucket, count := range keyBuckets[client] {
			if count < 200 || count > 440 {
				t.Fatalf("client %d key bucket %d count %d", client, bucket, count)
			}
		}
	}
}

func TestParseTables(t *testing.T) {
	got, err := parseTables("rf3_sql_bench,orders_01")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "rf3_sql_bench,orders_01" {
		t.Fatalf("tables = %v", got)
	}
	for _, input := range []string{"", "RF3", "rf3_sql_bench,rf3_sql_bench", "table-name"} {
		if _, err := parseTables(input); err == nil {
			t.Fatalf("parseTables(%q) accepted invalid input", input)
		}
	}
}

func TestParseWorkloads(t *testing.T) {
	got, err := parseWorkloads("point_hit,mixed,update_existing")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "point_hit,mixed_read_update,update_existing" {
		t.Fatalf("workloads = %v", got)
	}
	for _, input := range []string{"", "point_hit,point_hit", "delete_all"} {
		if _, err := parseWorkloads(input); err == nil {
			t.Fatalf("parseWorkloads(%q) accepted invalid input", input)
		}
	}
}

func TestParseURLsKeepsCredentialsOutOfLabels(t *testing.T) {
	endpoints, labels, err := parseURLs(
		"postgresql://alice:secret@127.0.0.1:5432/vibedb?sslmode=disable",
		"postgresql://alice:secret@127.0.0.1:5432/vibedb?sslmode=disable,postgresql://alice:secret@127.0.0.1:5433/vibedb?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 2 || labels[0] != "127.0.0.1:5432" || labels[1] != "127.0.0.1:5433" {
		t.Fatalf("endpoints=%v labels=%v", endpoints, labels)
	}
	if strings.Contains(strings.Join(labels, ","), "secret") {
		t.Fatalf("endpoint labels contain credentials: %v", labels)
	}
	for _, raw := range []string{"", "http://127.0.0.1:5432/db", "postgresql://127.0.0.1:70000/db", "postgresql://127.0.0.1:5432/db,postgresql://127.0.0.1:5432/other"} {
		if _, _, err := parseURLs("postgresql://127.0.0.1:5432/db", raw); raw == "" {
			if err != nil {
				t.Fatalf("single base URL rejected: %v", err)
			}
		} else if err == nil {
			t.Fatalf("parseURLs accepted invalid input %q", raw)
		}
	}
}

func TestGroupForUniformAndSkewed(t *testing.T) {
	uniform := config{groupDistribution: "uniform"}
	counts := make([]int, 4)
	for ordinal := 0; ordinal < 4000; ordinal++ {
		counts[groupFor(uniform, len(counts), ordinal)]++
	}
	for group, count := range counts {
		if count < 850 || count > 1150 {
			t.Fatalf("uniform group %d count = %d, want an approximately uniform count", group, count)
		}
	}
	skewed := config{groupDistribution: "skewed", skewPercent: 80}
	counts = make([]int, 4)
	for ordinal := 0; ordinal < 4000; ordinal++ {
		counts[groupFor(skewed, len(counts), ordinal)]++
	}
	if counts[0] < 2800 || counts[0] > 3600 {
		t.Fatalf("skewed first group count = %d, want a hot majority", counts[0])
	}
	if counts[1]+counts[2]+counts[3] < 400 {
		t.Fatalf("skewed tail count = %d, want cold groups to remain active", counts[1]+counts[2]+counts[3])
	}

	// The operation stream is independent of placement: every group must see
	// both halves of mixed read/update traffic over a real trial-sized sample.
	for group := 0; group < 4; group++ {
		reads, writes := 0, 0
		for ordinal := 0; ordinal < 4000; ordinal++ {
			if groupFor(uniform, 4, ordinal) != group {
				continue
			}
			if mixedRead(ordinal) {
				reads++
			} else {
				writes++
			}
		}
		if reads == 0 || writes == 0 {
			t.Fatalf("group %d mixed operations reads=%d writes=%d", group, reads, writes)
		}
	}
}

func TestTimingFieldsUseStableJSONNames(t *testing.T) {
	anchor := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(result{
		MeasurementStartedUTC: anchor,
		Samples:               []sample{{Ordinal: 0, Client: 0, NS: 10, StartOffsetNS: 2, Group: 1, Table: "orders_01"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, field := range []string{"\"measurement_started_utc\"", "\"start_offset_ns\"", "\"group\"", "\"table\"", "\"endpoint\""} {
		if !strings.Contains(text, field) {
			t.Fatalf("JSON %q missing %s", text, field)
		}
	}
}
