package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func diagnosticFixture(t *testing.T, node string, pid int, serial, count uint64) []byte {
	t.Helper()
	value := map[string]any{"node_id": node, "pid": pid, "serial": serial, "utc": "2026-09-04T00:00:00Z", "event": "snapshot",
		"ready_wave_group_histogram": []uint64{0, count}, "native_active": 0}
	for _, key := range diagnosticCounters {
		value[key] = count
	}
	raw, err := json.MarshalIndent(value, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDiagnosticCaptureIgnoresStaleProcessFileAndArchivesExactBytes(t *testing.T) {
	directory := t.TempDir()
	node := strings.Repeat("01", 16)
	target := diagnosticTarget{NodeID: node, PID: 202, Path: filepath.Join(directory, "latest.json")}
	if err := os.WriteFile(target.Path, diagnosticFixture(t, node, 101, 99, 2), 0600); err != nil {
		t.Fatal(err)
	}
	fresh := diagnosticFixture(t, node, target.PID, 1, 7)
	control := diagnosticControl{targets: []diagnosticTarget{target}, directory: directory,
		check:  func(diagnosticTarget) error { return nil },
		signal: func(diagnosticTarget) error { return os.WriteFile(target.Path, fresh, 0600) }}
	records, err := control.capture(context.Background(), "point_hit", 8, 1, "before")
	if err != nil || len(records) != 1 || records[0].PID != 202 || records[0].Serial != 1 {
		t.Fatalf("records=%v error=%v", records, err)
	}
	archived, err := os.ReadFile(filepath.Join(directory, filepath.Base(records[0].File)))
	if err != nil || string(archived) != string(fresh) {
		t.Fatalf("archive differs from exact snapshot: %v", err)
	}
}

func TestDiagnosticCaptureRequiresNewMatchingAcknowledgement(t *testing.T) {
	for _, mismatch := range []string{"serial", "node", "pid"} {
		t.Run(mismatch, func(t *testing.T) {
			directory := t.TempDir()
			node := strings.Repeat("02", 16)
			target := diagnosticTarget{NodeID: node, PID: 202, Path: filepath.Join(directory, "latest.json")}
			if err := os.WriteFile(target.Path, diagnosticFixture(t, node, target.PID, 5, 7), 0600); err != nil {
				t.Fatal(err)
			}
			control := diagnosticControl{targets: []diagnosticTarget{target}, directory: directory,
				check: func(diagnosticTarget) error { return nil },
				signal: func(diagnosticTarget) error {
					pid, serial, nextNode := target.PID, uint64(6), node
					switch mismatch {
					case "serial":
						serial = 5
					case "node":
						nextNode = strings.Repeat("03", 16)
					case "pid":
						pid = 203
					}
					return os.WriteFile(target.Path, diagnosticFixture(t, nextNode, pid, serial, 9), 0600)
				}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if records, err := control.capture(ctx, "point_hit", 8, 1, "before"); err == nil || len(records) != 0 {
				t.Fatalf("accepted stale or mismatched acknowledgement: %v %v", records, err)
			}
		})
	}
}

func TestDiagnosticTargetsRejectLegacyParentExecutable(t *testing.T) {
	directory := t.TempDir()
	var targets []diagnosticTarget
	for i := 1; i <= 3; i++ {
		targets = append(targets, diagnosticTarget{NodeID: strings.Repeat(string('a'+rune(i)), 32), PID: 100 + i,
			Path: filepath.Join(directory, string('a'+rune(i)), "rf3-diagnostics.json"), Executable: "/bench/parent-vibedb-shard"})
	}
	raw, _ := json.Marshal(targets)
	path := filepath.Join(directory, "targets.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDiagnosticControl(path, directory); err == nil {
		t.Fatal("accepted a legacy executable that does not handle SIGUSR1")
	}
}

func TestDiagnosticDeltasKeepUint64PrecisionAndRejectReset(t *testing.T) {
	node := strings.Repeat("01", 16)
	before, _ := decodeDiagnostic(diagnosticFixture(t, node, 202, 1, 1<<60))
	after, _ := decodeDiagnostic(diagnosticFixture(t, node, 202, 2, (1<<60)+3))
	deltas, err := diagnosticDeltas([]diagnosticRecord{before}, []diagnosticRecord{after})
	if err != nil || len(deltas) != 1 || deltas[0].Counters["gateway_local_calls"] != 3 ||
		deltas[0].Counters["raft_proposal_bytes"] != 3 || deltas[0].Counters["ready_queue_wait_ns"] != 3 ||
		deltas[0].Histogram[1] != 3 {
		t.Fatalf("delta lost precision: %v %v", deltas, err)
	}
	reset, _ := decodeDiagnostic(diagnosticFixture(t, node, 202, 3, 1))
	if _, err := diagnosticDeltas([]diagnosticRecord{before}, []diagnosticRecord{reset}); err == nil {
		t.Fatal("counter reset was accepted")
	}
	after.PID++
	if _, err := diagnosticDeltas([]diagnosticRecord{before}, []diagnosticRecord{after}); err == nil {
		t.Fatal("process change was accepted")
	}
}
