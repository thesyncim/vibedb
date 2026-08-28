package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestParseClientLevelsRequiresExactDoubling(t *testing.T) {
	got, err := parseClientLevels("1,2,4,8")
	if err != nil || len(got) != 4 || got[3] != 8 {
		t.Fatalf("levels=%v err=%v", got, err)
	}
	for _, value := range []string{"1,2", "1,2,3", "0,1,2", "1,,4", "1,2,2"} {
		if _, err := parseClientLevels(value); err == nil {
			t.Fatalf("accepted client levels %q", value)
		}
	}
}

func TestClassifySaturationRequiresConsecutivePlateaus(t *testing.T) {
	levels := func(values ...uint64) []levelSummary {
		result := make([]levelSummary, len(values))
		for i, value := range values {
			result[i] = levelSummary{clients: 1 << i, throughputMedian: value}
		}
		return result
	}
	if clients, ok := classifySaturation(levels(100, 180, 184, 183), 500, 2); !ok || clients != 4 {
		t.Fatalf("saturation=%d,%t want 4,true", clients, ok)
	}
	if clients, ok := classifySaturation(levels(100, 104, 200, 204), 500, 2); ok || clients != 0 {
		t.Fatalf("nonconsecutive saturation=%d,%t", clients, ok)
	}
}

func TestParseMixedOutputChecksMatchedShapeAndExactLatency(t *testing.T) {
	cfg := config{engine: "vibedb", workload: "churn", durability: "buffered-visible",
		cardinality: "low", documentShape: "inline", corpus: 100, operations: 200,
		warmup: 20, checkpointMutations: 64}
	const output = `engine durability workload card document-shape docs measured warmup checkpoint forced-cp exact-indexes clients operation calls p50-us p95-us p99-us p99.9-us max-us total-ops/s
vibedb/bulk-unified buffered-visible churn low inline 100 200 20 64 0 0 4 read 100 1.000 2.000 3.000 4.125 5.250 900
vibedb/bulk-unified buffered-visible churn low inline 100 200 20 64 0 0 4 update 100 1.000 2.000 3.000 6.500 8.750 900
`
	throughput, p999, maximum, err := parseMixedOutput(cfg, 4, []byte(output))
	if err != nil || throughput != 900 || p999 != 6500 || maximum != 8750 {
		t.Fatalf("parsed=%d/%d/%d err=%v", throughput, p999, maximum, err)
	}
	if _, _, _, err := parseMixedOutput(cfg, 8, []byte(output)); err == nil {
		t.Fatal("accepted wrong client configuration")
	}
	forced := strings.Replace(output, " 64 0 0 4 read", " 64 1 0 4 read", 1)
	if _, _, _, err := parseMixedOutput(cfg, 4, []byte(forced)); err == nil {
		t.Fatal("accepted a forced checkpoint")
	}
}

func TestEnvironmentWithRemovesAmbiguousDuplicates(t *testing.T) {
	got := environmentWith([]string{"KEEP=yes", "LANG=old", "LC_ALL=old"}, "LC_ALL=C", "LANG=C")
	want := []string{"KEEP=yes", "LC_ALL=C", "LANG=C"}
	if !slices.Equal(got, want) {
		t.Fatalf("environment=%v want=%v", got, want)
	}
}

func TestRunWritesExactEvidenceAndFindsPlateau(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("helper uses a POSIX shell")
	}
	dir := t.TempDir()
	helper := filepath.Join(dir, "mixed-helper")
	const script = `#!/bin/sh
clients=
for arg in "$@"; do
	case "$arg" in
		-clients=*) clients=${arg#-clients=} ;;
	esac
done
printf '%s\n' 'engine durability workload card document-shape docs measured warmup checkpoint forced-cp exact-indexes clients operation calls p50-us p95-us p99-us p99.9-us max-us total-ops/s'
printf 'vibedb buffered-visible churn low inline 64 64 0 64 0 0 %s read 64 1.000 2.000 3.000 4.000 5.000 1000\n' "$clients"
`
	if err := os.WriteFile(helper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	err := run([]string{"-mixed-bin=" + helper, "-output=-", "-engine=vibedb",
		"-clients=1,2,4", "-repetitions=3", "-conditioning=false", "-corpus=64",
		"-operations=64", "-warmup=0"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	evidence := stdout.String()
	for _, token := range []string{
		"schema\tvibedb.saturation-evidence\t1\n",
		"config\tdurability\tbuffered-visible\n",
		"config\texact-indexes\t0\n",
		"raw_header\trepetition\tposition\tclients",
		"decision\tsaturation-observed\ttrue\tsaturation-clients\t2",
	} {
		if !strings.Contains(evidence, token) {
			t.Fatalf("evidence omitted %q:\n%s", token, evidence)
		}
	}
	if got := strings.Count(evidence, "\nraw\t"); got != 9 {
		t.Fatalf("raw rows=%d want 9", got)
	}
}
