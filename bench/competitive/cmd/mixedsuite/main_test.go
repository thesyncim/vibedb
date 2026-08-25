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

func TestLatinSquareScheduleIsDeterministicAndPositionBalanced(t *testing.T) {
	engines := []string{"a", "b", "c", "d", "e"}
	first := latinSquareSchedule(engines, 10, 99)
	second := latinSquareSchedule(engines, 10, 99)
	if !slices.EqualFunc(first, second, slices.Equal[[]string]) {
		t.Fatal("same seed produced different schedules")
	}
	for repetition, row := range first {
		if len(row) != len(engines) {
			t.Fatalf("row %d has %d engines", repetition, len(row))
		}
		sorted := slices.Clone(row)
		slices.Sort(sorted)
		if !slices.Equal(sorted, engines) {
			t.Fatalf("row %d is not a permutation: %v", repetition, row)
		}
	}
	for position := range engines {
		counts := make(map[string]int)
		for _, row := range first {
			counts[row[position]]++
		}
		for _, engine := range engines {
			if counts[engine] != 2 {
				t.Fatalf(
					"position %d engine %s appeared %d times, want 2",
					position, engine, counts[engine],
				)
			}
		}
	}
	if slices.Equal(first[0], latinSquareSchedule(engines, 10, 100)[0]) {
		t.Fatal("different seed retained the same shuffled base row")
	}
}

func TestParseEnginesRejectsAmbiguousLists(t *testing.T) {
	if got, err := parseEngines(" vibedb, bbolt "); err != nil {
		t.Fatal(err)
	} else if want := []string{"vibedb", "bbolt"}; !slices.Equal(got, want) {
		t.Fatalf("engines = %v, want %v", got, want)
	}
	for _, value := range []string{"vibedb", "a,,b", "a,b,a"} {
		if _, err := parseEngines(value); err == nil {
			t.Fatalf("parseEngines(%q) succeeded", value)
		}
	}
}

func TestMarshalArgvJSONIsExactAndCanonical(t *testing.T) {
	args := []string{"mixedsuite", "--seed=7", "line\nquote\"", "<html>"}
	want := `["mixedsuite","--seed=7","line\nquote\"","\u003chtml\u003e"]`
	if first, second := marshalArgvJSON(args), marshalArgvJSON(args); first != want || second != first {
		t.Fatalf("argv JSON = %q / %q, want %q", first, second, want)
	}
}

func TestEnvironmentWithReplacesExistingValues(t *testing.T) {
	got := environmentWith(
		[]string{"KEEP=yes", "LANG=old", "VIBEDB_MIXED_INTERNAL_STATS="},
		"LANG=C", "VIBEDB_MIXED_INTERNAL_STATS=1",
	)
	want := []string{"KEEP=yes", "LANG=C", "VIBEDB_MIXED_INTERNAL_STATS=1"}
	if !slices.Equal(got, want) {
		t.Fatalf("environment = %v, want %v", got, want)
	}
}

func TestSummarizeReportsMedianMADQuartilesAndRange(t *testing.T) {
	got := summarize([]float64{100, 1, 2})
	if got.n != 3 || got.median != 2 || got.mad != 1 ||
		got.q1 != 1.5 || got.q3 != 51 || got.iqr != 49.5 ||
		got.min != 1 || got.max != 100 {
		t.Fatalf("summary = %+v", got)
	}
}

func TestParseMixedOutputRequiresMachineReadableShape(t *testing.T) {
	const valid = `engine durability workload card document-shape docs measured warmup checkpoint forced-cp exact-indexes clients operation calls p50-us p95-us p99-us p99.9-us max-us total-ops/s disk-MiB alloc-MiB heap-MiB runtime-MiB peak-rss-MiB durability-payload-known logical-write-B durability-payload-B durability-payload/logical
vibedb buffered-visible ycsb-a low inline 10 20 2 64 0 0 1 read 10 1 2 3 4 5 1000 1 1 2 3 4 true 100 200 2
`
	header, rows, err := parseMixedOutput([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if len(header) != 29 || len(rows) != 1 {
		t.Fatalf("header=%d rows=%d", len(header), len(rows))
	}
	if _, _, err := parseMixedOutput([]byte("engine workload\nvibe ycsb-a\n")); err == nil {
		t.Fatal("output without latency and throughput columns was accepted")
	}
}

func TestValidateMixedRowsChecksRequestedConfiguration(t *testing.T) {
	header, rows, err := parseMixedOutput([]byte(
		`engine durability workload card document-shape docs measured warmup checkpoint forced-cp exact-indexes clients operation calls p50-us p95-us p99-us p99.9-us max-us total-ops/s disk-MiB alloc-MiB heap-MiB runtime-MiB peak-rss-MiB durability-payload-known logical-write-B durability-payload-B durability-payload/logical
vibedb/bulk-unified buffered-visible ycsb-a low inline 10 20 2 64 0 0 1 read 10 1 2 3 4 5 1000 1 1 2 3 4 true 100 200 2
`,
	))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		durability: "buffered-visible", workload: "ycsb-a",
		cardinality: "low", documentShape: "inline", corpus: 10, operations: 20, warmup: 2,
		checkpointMutations: 64, clients: 1,
	}
	if err := validateMixedRows(cfg, "vibedb", header, rows); err != nil {
		t.Fatal(err)
	}
	if err := validateMixedRows(cfg, "bbolt", header, rows); err == nil {
		t.Fatal("accepted a row from the wrong engine")
	}
	cfg.operations++
	if err := validateMixedRows(cfg, "vibedb", header, rows); err == nil {
		t.Fatal("accepted a row with the wrong measured-operation count")
	}
}

func TestParseFlagsRequiresPublishableRepetitionCountByDefault(t *testing.T) {
	args := []string{
		"-mixed-bin=" + os.Args[0],
		"-engines=a,b",
		"-repetitions=8",
	}
	if _, err := parseFlags(args, &bytes.Buffer{}); err == nil {
		t.Fatal("accepted fewer than nine repetitions without diagnostic override")
	}
	args = append(args, "-allow-diagnostic")
	if got, err := parseFlags(args, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	} else if !got.allowDiagnostic {
		t.Fatal("diagnostic override was not retained")
	}
}

func TestMaximumForcedCheckpointsFailsClosedOnMissingOrInvalidColumn(t *testing.T) {
	header := []string{"engine", "forced-cp"}
	records := []runRecord{{row: rawRow{values: []string{"vibe", "2"}}}}
	if got, err := maximumNumericColumn(header, records, "forced-cp"); err != nil {
		t.Fatal(err)
	} else if got != 2 {
		t.Fatalf("maximum forced checkpoints = %v, want 2", got)
	}
	if _, err := maximumNumericColumn(
		[]string{"engine"}, records, "forced-cp",
	); err == nil {
		t.Fatal("missing forced checkpoint column was accepted")
	}
}

func TestCheckpointCadenceFailsClosedUnlessDiagnostic(t *testing.T) {
	if err := checkpointCadenceError(0, false); err != nil {
		t.Fatal(err)
	}
	if err := checkpointCadenceError(1, false); err == nil {
		t.Fatal("publishable suite accepted a forced checkpoint")
	}
	if err := checkpointCadenceError(1, true); err != nil {
		t.Fatalf("diagnostic override rejected: %v", err)
	}
}

func TestRunDiscardsConditioningAndRecordsRawAndSummaryRows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper uses a POSIX shell")
	}
	dir := t.TempDir()
	logPath := filepath.Join(dir, "invocations")
	t.Setenv("MIXEDSUITE_TEST_LOG", logPath)
	helper := filepath.Join(dir, "mixed-helper")
	const script = `#!/bin/sh
engine=
available=false
for arg in "$@"; do
	case "$arg" in
		-engine=*) engine=${arg#-engine=} ;;
	esac
done
[ "$engine" = vibedb ] && available=true
printf '%s\n' "$engine" >> "$MIXEDSUITE_TEST_LOG"
printf '%s\n' 'engine durability workload card document-shape docs measured warmup checkpoint forced-cp exact-indexes clients operation calls p50-us p95-us p99-us p99.9-us max-us total-ops/s disk-MiB alloc-MiB heap-MiB runtime-MiB peak-rss-MiB durability-payload-known logical-write-B durability-payload-B durability-payload/logical'
printf '%s\n' "$engine buffered-visible ycsb-a low inline 10 20 2 64 0 0 1 read 10 1 2 3 4 5 1000 1 1 2 3 4 $available 100 200 2"
printf 'mixed-telemetry-json\t%s\n' "{\"schema\":1,\"engine\":\"$engine\",\"clients\":1,\"durable_stats_available\":$available,\"runtime_total_alloc_bytes\":100,\"runtime_mallocs\":10,\"scalar_patch_attempts\":20,\"scalar_patch_accepts\":19,\"publish_groups\":5,\"publish_group_max\":4,\"journal_acks\":8,\"journal_syncs\":2,\"journal_group_max\":4,\"journal_delta_records\":7,\"journal_delta_bytes\":4096,\"journal_delta_fallbacks\":1,\"durability_payload_known\":true,\"durability_payload_bytes\":8192}" >&2
`
	if err := os.WriteFile(helper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-mixed-bin=" + helper,
		"-engines=vibedb,b",
		"-repetitions=2",
		"-allow-diagnostic",
		"-conditioning=true",
		"-corpus=10",
		"-operations=20",
		"-warmup=2",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	logged, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	invocations := strings.Fields(string(logged))
	if len(invocations) != 6 {
		t.Fatalf("child invocations = %v, want conditioning 2 + recorded 4", invocations)
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	raw, summaries, telemetryRows, telemetrySummaries := 0, 0, 0, 0
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "raw\t"):
			raw++
		case strings.HasPrefix(line, "telemetry\t"):
			telemetryRows++
		case strings.HasPrefix(line, "telemetry-summary\t"):
			telemetrySummaries++
		case strings.HasPrefix(line, "summary\t"):
			summaries++
			if !strings.Contains(line, "\t2\t") {
				t.Fatalf("summary does not contain two recorded samples: %q", line)
			}
		}
	}
	if raw != 4 {
		t.Fatalf("raw rows = %d, want 4; output:\n%s", raw, stdout.String())
	}
	if summaries == 0 {
		t.Fatalf("no summary rows; output:\n%s", stdout.String())
	}
	if telemetryRows == 0 || telemetrySummaries == 0 {
		t.Fatalf(
			"telemetry rows/summaries = %d/%d; output:\n%s",
			telemetryRows, telemetrySummaries, stdout.String(),
		)
	}
	if !strings.Contains(
		stdout.String(), "\tvibedb\t1\ttrue\tvibedb\tscalar-patch-attempts\t20\n",
	) {
		t.Fatalf("VibeDB patch telemetry missing:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "meta\tconditioning\tone discarded pass") {
		t.Fatalf("conditioning metadata missing:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "meta\tpublishable-suite\tfalse") {
		t.Fatalf("short diagnostic was not marked non-publishable:\n%s", stdout.String())
	}
}
