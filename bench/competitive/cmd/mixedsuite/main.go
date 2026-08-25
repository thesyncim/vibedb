// Command mixedsuite runs cmd/mixed in isolated, sequential child processes
// using a deterministic Latin-square engine order. It preserves every raw row
// and appends robust cross-process summaries.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/bench/competitive/cmd/internal/mixedtelemetry"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	defaultEngines = "vibedb,bbolt,badger,pebble,sqlite"
	defaultSeed    = int64(0x51C5B)
)

type config struct {
	mixedBin            string
	engines             []string
	repetitions         int
	conditioning        bool
	allowDiagnostic     bool
	seed                int64
	timeout             time.Duration
	output              string
	workload            string
	corpus              int
	operations          int
	warmup              int
	durability          string
	checkpointMutations int
	indexed             bool
	cardinality         string
	clients             int
}

type rawRow struct {
	values []string
}

type runRecord struct {
	repetition int
	position   int
	requested  string
	row        rawRow
}

type telemetryRunRecord struct {
	repetition int
	position   int
	requested  string
	record     mixedtelemetry.Record
}

type summary struct {
	n                        int
	median, mad, q1, q3, iqr float64
	min, max                 float64
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "mixedsuite:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) (result error) {
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	started := time.Now()
	// Capture source state before creating an output file inside the worktree;
	// the result artifact itself must not make a clean checkout look dirty.
	metadata := collectMetadata(cfg, args, started)
	out, closeOutput, err := outputWriter(cfg.output, stdout)
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, closeOutput())
	}()

	for _, key := range sortedKeys(metadata) {
		writeMetadata(out, key, metadata[key])
	}

	schedule := latinSquareSchedule(cfg.engines, cfg.repetitions, cfg.seed)
	base := schedule[0]
	if cfg.conditioning {
		writeMetadata(out, "conditioning", "one discarded pass")
		writeMetadata(out, "conditioning-order", strings.Join(base, ","))
		for position, engine := range base {
			fmt.Fprintf(
				stderr, "conditioning %d/%d: %s\n",
				position+1, len(base), engine,
			)
			childHeader, rows, _, err := executeMixed(cfg, engine)
			if err != nil {
				return fmt.Errorf("conditioning %s: %w", engine, err)
			}
			if err := validateMixedRows(cfg, engine, childHeader, rows); err != nil {
				return fmt.Errorf("conditioning %s: %w", engine, err)
			}
		}
	} else {
		writeMetadata(out, "conditioning", "disabled")
	}

	var (
		header           []string
		records          []runRecord
		telemetryRecords []telemetryRunRecord
		wroteRawHead     bool
	)
	for repetition, order := range schedule {
		writeMetadata(
			out,
			fmt.Sprintf("recorded-order-%02d", repetition+1),
			strings.Join(order, ","),
		)
		for position, engine := range order {
			fmt.Fprintf(
				stderr, "repetition %d/%d, position %d/%d: %s\n",
				repetition+1, cfg.repetitions, position+1, len(order), engine,
			)
			childHeader, rows, telemetry, err := executeMixed(cfg, engine)
			if err != nil {
				return fmt.Errorf(
					"repetition %d position %d engine %s: %w",
					repetition+1, position+1, engine, err,
				)
			}
			if err := validateMixedRows(cfg, engine, childHeader, rows); err != nil {
				return fmt.Errorf(
					"repetition %d position %d engine %s: %w",
					repetition+1, position+1, engine, err,
				)
			}
			if header == nil {
				header = childHeader
			} else if !slices.Equal(header, childHeader) {
				return fmt.Errorf(
					"engine %s output header changed:\n got %v\nwant %v",
					engine, childHeader, header,
				)
			}
			if !wroteRawHead {
				fmt.Fprintf(
					out, "record\trepetition\tposition\trequested-engine\t%s\n",
					strings.Join(header, "\t"),
				)
				wroteRawHead = true
			}
			for _, row := range rows {
				record := runRecord{
					repetition: repetition + 1,
					position:   position + 1,
					requested:  engine,
					row:        row,
				}
				records = append(records, record)
				fmt.Fprintf(
					out, "raw\t%d\t%d\t%s\t%s\n",
					record.repetition, record.position, record.requested,
					strings.Join(record.row.values, "\t"),
				)
			}
			telemetryRecords = append(telemetryRecords, telemetryRunRecord{
				repetition: repetition + 1,
				position:   position + 1,
				requested:  engine,
				record:     telemetry,
			})
		}
	}
	if len(records) == 0 {
		return errors.New("mixed children produced no rows")
	}
	forced, err := maximumNumericColumn(header, records, "forced-cp")
	if err != nil {
		return err
	}
	writeMetadata(
		out, "publishable-checkpoint-cadence",
		strconv.FormatBool(forced == 0),
	)
	writeMetadata(
		out, "publishable-repetition-count",
		strconv.FormatBool(cfg.repetitions >= 9),
	)
	writeMetadata(
		out, "publishable-suite",
		strconv.FormatBool(forced == 0 && cfg.repetitions >= 9),
	)
	writeMetadata(out, "maximum-forced-checkpoints", formatNumber(forced))
	writeMetadata(out, "mixed-telemetry-schema", strconv.Itoa(mixedtelemetry.Schema))
	writeTelemetryRows(out, telemetryRecords)
	if err := writeSummaries(out, header, records); err != nil {
		return err
	}
	writeTelemetrySummaries(out, telemetryRecords)
	writeMetadata(out, "finished-utc", time.Now().UTC().Format(time.RFC3339Nano))
	writeMetadata(out, "elapsed", time.Since(started).String())
	return checkpointCadenceError(forced, cfg.allowDiagnostic)
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("mixedsuite", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		mixedBin     = fs.String("mixed-bin", "", "path to a prebuilt cmd/mixed binary")
		engineNames  = fs.String("engines", defaultEngines, "comma-separated file-backed engines")
		repetitions  = fs.Int("repetitions", 10, "recorded repetitions (10 is two complete five-engine Latin squares)")
		conditioning = fs.Bool("conditioning", true, "run and discard one process per engine before recording")
		diagnostic   = fs.Bool("allow-diagnostic", false, "allow fewer than 9 repetitions or forced checkpoints, marking output non-publishable")
		seed         = fs.Int64("seed", defaultSeed, "deterministic base-order shuffle seed")
		timeout      = fs.Duration("timeout", 15*time.Minute, "timeout for each isolated child process")
		output       = fs.String("output", "-", "output path, created exclusively; - writes stdout")
		workload     = fs.String("workload", "ycsb-a", "mixed workload")
		corpus       = fs.Int("corpus", 10_000, "documents in the shared corpus")
		operations   = fs.Int("operations", 20_000, "measured user operations")
		warmup       = fs.Int("warmup", 2_000, "unmeasured warmup operations")
		durability   = fs.String("durability", "buffered-visible", "explicit durability lane")
		checkpoint   = fs.Int("checkpoint-mutations", 64, "checkpoint cadence in acknowledged mutations")
		indexed      = fs.Bool("indexed", false, "maintain the country secondary index")
		cardinality  = fs.String("cardinality", "low", "low or high corpus cardinality")
		clients      = fs.Int("clients", 1, "concurrent worker goroutines per child (passed to cmd/mixed)")
	)
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *mixedBin == "" {
		return config{}, errors.New("-mixed-bin is required")
	}
	resolvedBin, err := exec.LookPath(*mixedBin)
	if err != nil {
		return config{}, fmt.Errorf("resolve -mixed-bin: %w", err)
	}
	resolvedBin, err = filepath.Abs(resolvedBin)
	if err != nil {
		return config{}, fmt.Errorf("absolute -mixed-bin: %w", err)
	}
	engines, err := parseEngines(*engineNames)
	if err != nil {
		return config{}, err
	}
	if *repetitions < 1 || *timeout <= 0 || *corpus < 2 ||
		*operations < 1 || *warmup < 0 || *checkpoint < 0 || *clients < 1 {
		return config{}, errors.New(
			"-repetitions>=1, -timeout>0, -corpus>=2, -operations>=1, " +
				"-warmup>=0, -checkpoint-mutations>=0, and -clients>=1 are required",
		)
	}
	if *repetitions < 9 && !*diagnostic {
		return config{}, errors.New(
			"-repetitions must be at least 9 for a publishable suite " +
				"(use -allow-diagnostic for a shorter diagnostic)",
		)
	}
	return config{
		mixedBin: resolvedBin, engines: engines,
		repetitions: *repetitions, conditioning: *conditioning,
		allowDiagnostic: *diagnostic,
		seed:            *seed, timeout: *timeout, output: *output,
		workload: *workload, corpus: *corpus, operations: *operations,
		warmup: *warmup, durability: *durability,
		checkpointMutations: *checkpoint, indexed: *indexed,
		cardinality: *cardinality, clients: *clients,
	}, nil
}

func parseEngines(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	engines := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		engine := strings.TrimSpace(part)
		if engine == "" {
			return nil, errors.New("-engines contains an empty name")
		}
		if _, exists := seen[engine]; exists {
			return nil, fmt.Errorf("-engines contains duplicate %q", engine)
		}
		seen[engine] = struct{}{}
		engines = append(engines, engine)
	}
	if len(engines) < 2 {
		return nil, errors.New("-engines must contain at least two engines")
	}
	return engines, nil
}

// latinSquareSchedule first shuffles one base row deterministically, then uses
// cyclic rotations. Every block of len(engines) repetitions puts every engine
// in every position exactly once.
func latinSquareSchedule(engines []string, repetitions int, seed int64) [][]string {
	base := slices.Clone(engines)
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(base), func(i, j int) {
		base[i], base[j] = base[j], base[i]
	})
	schedule := make([][]string, repetitions)
	for repetition := range repetitions {
		row := make([]string, len(base))
		for position := range base {
			row[position] = base[(position+repetition)%len(base)]
		}
		schedule[repetition] = row
	}
	return schedule
}

func executeMixed(
	cfg config,
	engine string,
) ([]string, []rawRow, mixedtelemetry.Record, error) {
	args := []string{
		"-header",
		"-engine=" + engine,
		"-workload=" + cfg.workload,
		"-corpus=" + strconv.Itoa(cfg.corpus),
		"-operations=" + strconv.Itoa(cfg.operations),
		"-warmup=" + strconv.Itoa(cfg.warmup),
		"-durability=" + cfg.durability,
		"-checkpoint-mutations=" + strconv.Itoa(cfg.checkpointMutations),
		"-indexed=" + strconv.FormatBool(cfg.indexed),
		"-cardinality=" + cfg.cardinality,
		"-clients=" + strconv.Itoa(cfg.clients),
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, cfg.mixedBin, args...)
	cmd.Env = environmentWith(
		os.Environ(),
		"LC_ALL=C", "LANG=C", "VIBEDB_MIXED_INTERNAL_STATS=1",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return nil, nil, mixedtelemetry.Record{}, fmt.Errorf(
			"timed out after %s; stderr: %s",
			cfg.timeout, strings.TrimSpace(stderr.String()),
		)
	}
	if err != nil {
		return nil, nil, mixedtelemetry.Record{}, fmt.Errorf(
			"%w; stderr: %s", err, strings.TrimSpace(stderr.String()),
		)
	}
	header, rows, err := parseMixedOutput(stdout.Bytes())
	if err != nil {
		return nil, nil, mixedtelemetry.Record{}, err
	}
	telemetry, err := mixedtelemetry.Parse(stderr.Bytes())
	if err != nil {
		return nil, nil, mixedtelemetry.Record{}, fmt.Errorf(
			"parse successful child telemetry: %w; stderr: %s",
			err, strings.TrimSpace(stderr.String()),
		)
	}
	if telemetry.Engine != engine {
		return nil, nil, mixedtelemetry.Record{}, fmt.Errorf(
			"child telemetry engine = %q, want %q", telemetry.Engine, engine,
		)
	}
	if telemetry.Clients != cfg.clients {
		return nil, nil, mixedtelemetry.Record{}, fmt.Errorf(
			"child telemetry clients = %d, want %d", telemetry.Clients, cfg.clients,
		)
	}
	if telemetry.Available != (engine == "vibedb") {
		return nil, nil, mixedtelemetry.Record{}, fmt.Errorf(
			"child telemetry durable-stats availability = %t for engine %s",
			telemetry.Available, engine,
		)
	}
	return header, rows, telemetry, nil
}

// environmentWith applies deterministic overrides without leaving duplicate
// keys whose first/last-wins behavior can vary between child runtimes.
func environmentWith(base []string, overrides ...string) []string {
	keys := make(map[string]struct{}, len(overrides))
	for _, override := range overrides {
		key, _, _ := strings.Cut(override, "=")
		keys[key] = struct{}{}
	}
	environment := make([]string, 0, len(base)+len(overrides))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := keys[key]; !replaced {
			environment = append(environment, entry)
		}
	}
	return append(environment, overrides...)
}

func parseMixedOutput(src []byte) ([]string, []rawRow, error) {
	scanner := bufio.NewScanner(bytes.NewReader(src))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var (
		header []string
		rows   []rawRow
	)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		if header == nil {
			header = fields
			continue
		}
		if len(fields) != len(header) {
			return nil, nil, fmt.Errorf(
				"mixed row has %d fields, header has %d: %q",
				len(fields), len(header), scanner.Text(),
			)
		}
		rows = append(rows, rawRow{values: fields})
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	if len(header) == 0 || len(rows) == 0 {
		return nil, nil, errors.New("mixed output omitted its header or data rows")
	}
	required := []string{
		"engine", "durability", "workload", "operation",
		"forced-cp", "total-ops/s", "p50-us", "p95-us", "p99-us",
	}
	index := headerIndex(header)
	for _, name := range required {
		if _, ok := index[name]; !ok {
			return nil, nil, fmt.Errorf("mixed output omits required column %q", name)
		}
	}
	return header, rows, nil
}

func validateMixedRows(cfg config, requested string, header []string, rows []rawRow) error {
	index := headerIndex(header)
	expected := map[string]string{
		"durability": cfg.durability,
		"workload":   cfg.workload,
		"card":       cfg.cardinality,
		"docs":       strconv.Itoa(cfg.corpus),
		"measured":   strconv.Itoa(cfg.operations),
		"warmup":     strconv.Itoa(cfg.warmup),
		"checkpoint": strconv.Itoa(cfg.checkpointMutations),
		"indexed":    strconv.FormatBool(cfg.indexed),
		"clients":    strconv.Itoa(cfg.clients),
	}
	for name := range expected {
		if _, ok := index[name]; !ok {
			return fmt.Errorf("mixed output omits requested-configuration column %q", name)
		}
	}
	engineAt, ok := index["engine"]
	if !ok {
		return errors.New("mixed output omits engine column")
	}
	for _, row := range rows {
		gotEngine := row.values[engineAt]
		engineMatches := gotEngine == requested
		if requested == "vibedb" {
			engineMatches = engineMatches ||
				strings.HasPrefix(gotEngine, requested+"/")
		}
		if !engineMatches {
			return fmt.Errorf(
				"child requested as %s returned engine %s",
				requested, gotEngine,
			)
		}
		for name, want := range expected {
			if got := row.values[index[name]]; got != want {
				return fmt.Errorf(
					"engine %s returned %s=%q, want %q",
					requested, name, got, want,
				)
			}
		}
	}
	return nil
}

func writeTelemetryRows(w io.Writer, records []telemetryRunRecord) {
	fmt.Fprintln(
		w,
		"record\trepetition\tposition\trequested-engine\ttelemetry-schema\t"+
			"durable-stats\tscope\tmetric\tvalue",
	)
	for _, run := range records {
		for _, metric := range run.record.Metrics() {
			fmt.Fprintf(
				w, "telemetry\t%d\t%d\t%s\t%d\t%t\t%s\t%s\t%d\n",
				run.repetition, run.position, run.requested,
				run.record.Schema, run.record.Available,
				metric.Scope, metric.Name, metric.Value,
			)
		}
	}
}

func writeTelemetrySummaries(w io.Writer, records []telemetryRunRecord) {
	type group struct {
		engine, scope, metric string
		values                []float64
	}
	groups := make(map[string]*group)
	for _, run := range records {
		for _, metric := range run.record.Metrics() {
			key := strings.Join([]string{run.requested, metric.Scope, metric.Name}, "\x00")
			g := groups[key]
			if g == nil {
				g = &group{engine: run.requested, scope: metric.Scope, metric: metric.Name}
				groups[key] = g
			}
			g.values = append(g.values, float64(metric.Value))
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	fmt.Fprintln(
		w,
		"record\trequested-engine\tscope\tmetric\tsamples\tmedian\tmad\t"+
			"q1\tq3\tiqr\tmin\tmax",
	)
	for _, key := range keys {
		g := groups[key]
		stats := summarize(g.values)
		fmt.Fprintf(
			w, "telemetry-summary\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			g.engine, g.scope, g.metric, stats.n,
			formatNumber(stats.median), formatNumber(stats.mad),
			formatNumber(stats.q1), formatNumber(stats.q3),
			formatNumber(stats.iqr), formatNumber(stats.min),
			formatNumber(stats.max),
		)
	}
}

func writeSummaries(w io.Writer, header []string, records []runRecord) error {
	index := headerIndex(header)
	groupColumns := []string{"engine", "durability", "workload", "card", "operation"}
	for _, name := range groupColumns {
		if _, ok := index[name]; !ok {
			return fmt.Errorf("cannot summarize without column %q", name)
		}
	}
	excluded := map[string]bool{
		"engine": true, "durability": true, "workload": true,
		"card": true, "docs": true, "measured": true, "warmup": true,
		"checkpoint": true, "indexed": true, "operation": true,
		"clients": true,
	}
	var metrics []string
	for _, name := range header {
		if !excluded[name] {
			metrics = append(metrics, name)
		}
	}
	type group struct {
		fields  []string
		samples map[string][]float64
	}
	groups := make(map[string]*group)
	for _, record := range records {
		fields := make([]string, len(groupColumns))
		for i, name := range groupColumns {
			fields[i] = record.row.values[index[name]]
		}
		key := strings.Join(fields, "\x00")
		g := groups[key]
		if g == nil {
			g = &group{fields: fields, samples: make(map[string][]float64)}
			groups[key] = g
		}
		for _, metric := range metrics {
			value, err := strconv.ParseFloat(record.row.values[index[metric]], 64)
			if err != nil {
				return fmt.Errorf(
					"parse %s=%q for %s: %w",
					metric, record.row.values[index[metric]], key, err,
				)
			}
			g.samples[metric] = append(g.samples[metric], value)
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	fmt.Fprintf(
		w,
		"record\tengine\tdurability\tworkload\tcard\toperation\tmetric\t"+
			"samples\tmedian\tmad\tq1\tq3\tiqr\tmin\tmax\n",
	)
	for _, key := range keys {
		g := groups[key]
		for _, metric := range metrics {
			stats := summarize(g.samples[metric])
			fmt.Fprintf(
				w, "summary\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				strings.Join(g.fields, "\t"), metric, stats.n,
				formatNumber(stats.median), formatNumber(stats.mad),
				formatNumber(stats.q1), formatNumber(stats.q3),
				formatNumber(stats.iqr), formatNumber(stats.min),
				formatNumber(stats.max),
			)
		}
	}
	return nil
}

func summarize(values []float64) summary {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	median := quantile(sorted, 0.5)
	deviations := make([]float64, len(sorted))
	for i, value := range sorted {
		deviations[i] = math.Abs(value - median)
	}
	slices.Sort(deviations)
	q1 := quantile(sorted, 0.25)
	q3 := quantile(sorted, 0.75)
	return summary{
		n: len(sorted), median: median, mad: quantile(deviations, 0.5),
		q1: q1, q3: q3, iqr: q3 - q1,
		min: sorted[0], max: sorted[len(sorted)-1],
	}
}

// quantile uses linear interpolation at q*(n-1), the same definition used by
// common statistical packages for descriptive sample quantiles.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	position := q * float64(len(sorted)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}
	weight := position - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func headerIndex(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[name] = i
	}
	return index
}

func maximumNumericColumn(header []string, records []runRecord, name string) (float64, error) {
	at, ok := headerIndex(header)[name]
	if !ok {
		return 0, fmt.Errorf("mixed output omits required column %q", name)
	}
	maximum := 0.0
	for _, record := range records {
		value, err := strconv.ParseFloat(record.row.values[at], 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s=%q: %w", name, record.row.values[at], err)
		}
		if value > maximum {
			maximum = value
		}
	}
	return maximum, nil
}

func checkpointCadenceError(forced float64, allowDiagnostic bool) error {
	if forced == 0 || allowDiagnostic {
		return nil
	}
	return fmt.Errorf(
		"recorded rows report up to %s forced checkpoints; "+
			"same-cadence suite rejected (use -allow-diagnostic to retain a non-publishable diagnostic)",
		formatNumber(forced),
	)
}

func outputWriter(path string, stdout io.Writer) (io.Writer, func() error, error) {
	if path == "-" {
		return stdout, func() error { return nil }, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return file, file.Close, nil
}

func collectMetadata(cfg config, args []string, started time.Time) map[string]string {
	metadata := map[string]string{
		"format-version":       "0",
		"started-utc":          started.UTC().Format(time.RFC3339Nano),
		"argv-json":            marshalArgvJSON(args),
		"mixed-binary":         cfg.mixedBin,
		"mixed-binary-sha256":  fileHash(cfg.mixedBin),
		"suite-go-version":     runtime.Version(),
		"goos":                 runtime.GOOS,
		"goarch":               runtime.GOARCH,
		"gomaxprocs":           strconv.Itoa(runtime.GOMAXPROCS(0)),
		"logical-cpus":         strconv.Itoa(runtime.NumCPU()),
		"hostname":             hostname(),
		"cpu":                  cpuModel(),
		"uname":                commandOutput("uname", "-a"),
		"temp-directory":       os.TempDir(),
		"temp-filesystem":      filesystemType(os.TempDir()),
		"temp-filesystem-df":   commandOutput("df", "-P", os.TempDir()),
		"seed":                 strconv.FormatInt(cfg.seed, 10),
		"repetitions":          strconv.Itoa(cfg.repetitions),
		"allow-diagnostic":     strconv.FormatBool(cfg.allowDiagnostic),
		"engines":              strings.Join(cfg.engines, ","),
		"workload":             cfg.workload,
		"corpus":               strconv.Itoa(cfg.corpus),
		"operations":           strconv.Itoa(cfg.operations),
		"warmup":               strconv.Itoa(cfg.warmup),
		"durability":           cfg.durability,
		"checkpoint-mutations": strconv.Itoa(cfg.checkpointMutations),
		"indexed":              strconv.FormatBool(cfg.indexed),
		"cardinality":          cfg.cardinality,
		"clients":              strconv.Itoa(cfg.clients),
	}
	addBuildMetadata(metadata, "mixed", cfg.mixedBin)
	if suiteBin, err := os.Executable(); err == nil {
		metadata["suite-binary"] = suiteBin
		metadata["suite-binary-sha256"] = fileHash(suiteBin)
		addBuildMetadata(metadata, "suite", suiteBin)
	}
	if root := commandOutput("git", "rev-parse", "--show-toplevel"); root != "" {
		metadata["git-root"] = root
		metadata["git-commit"] = commandOutput("git", "-C", root, "rev-parse", "HEAD")
		metadata["git-branch"] = commandOutput(
			"git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD",
		)
		status := commandOutput(
			"git", "-C", root, "status", "--porcelain", "--untracked-files=normal",
		)
		metadata["git-dirty"] = strconv.FormatBool(status != "")
		metadata["git-status"] = status
		diff, err := exec.Command(
			"git", "-C", root, "diff", "--binary", "HEAD", "--",
		).Output()
		if err == nil {
			sum := sha256.Sum256(diff)
			metadata["git-tracked-diff-sha256"] = hex.EncodeToString(sum[:])
		}
	}
	return metadata
}

func addBuildMetadata(metadata map[string]string, prefix, path string) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return
	}
	metadata[prefix+"-go-version"] = info.GoVersion
	metadata[prefix+"-package"] = info.Path
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs", "vcs.revision", "vcs.time", "vcs.modified",
			"GOOS", "GOARCH", "GOAMD64", "GOARM64", "-race":
			metadata[prefix+"-build-"+setting.Key] = setting.Value
		}
	}
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func writeMetadata(w io.Writer, key, value string) {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\t", "\\t")
	value = strings.ReplaceAll(value, "\r", "\\r")
	value = strings.ReplaceAll(value, "\n", "\\n")
	fmt.Fprintf(w, "meta\t%s\t%s\n", key, value)
}

func fileHash(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "error: " + err.Error()
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var argvEncoder = mustCompileArgvEncoder()

func mustCompileArgvEncoder() vibejson.Encoder[[]string] {
	encoder, err := vibejson.CompileEncoder[[]string](vibejson.EncoderOptions{})
	if err != nil {
		panic(err)
	}
	return encoder
}

func marshalArgvJSON(value []string) string {
	data, err := argvEncoder.AppendJSON(make([]byte, 0, 256), &value)
	if err != nil {
		return "error: " + err.Error()
	}
	return string(data)
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil {
		return "error: " + err.Error()
	}
	return value
}

func cpuModel() string {
	if runtime.GOOS == "darwin" {
		if value := commandOutput("sysctl", "-n", "machdep.cpu.brand_string"); value != "" {
			return value
		}
	}
	if file, err := os.Open("/proc/cpuinfo"); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			for _, prefix := range []string{"model name", "Hardware"} {
				if strings.HasPrefix(line, prefix) {
					if _, value, ok := strings.Cut(line, ":"); ok {
						return strings.TrimSpace(value)
					}
				}
			}
		}
	}
	return "unknown"
}

func filesystemType(path string) string {
	switch runtime.GOOS {
	case "darwin", "freebsd", "openbsd", "netbsd":
		return commandOutput("stat", "-f", "%T", path)
	default:
		return commandOutput("stat", "-f", "-c", "%T", path)
	}
}

func commandOutput(name string, args ...string) string {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func formatNumber(value float64) string {
	return strconv.FormatFloat(value, 'g', 12, 64)
}
