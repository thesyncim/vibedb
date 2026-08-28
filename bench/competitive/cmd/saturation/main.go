// Command saturation runs one mixed-workload engine in isolated child
// processes at a fixed geometric client sweep. It records raw rows and applies
// one explicit, integer-only throughput-plateau rule; it does not publish an
// environment-independent capacity claim.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

const (
	schemaVersion       = 1
	maximumClientLevels = 16
	maximumRepetitions  = 31
	maximumChildOutput  = 1 << 20
)

type config struct {
	mixedBin, output, engine, workload, durability, cardinality, documentShape string
	clientLevels                                                               []int
	repetitions, corpus, operations, warmup, checkpointMutations, exactIndexes int
	minimumGainBPS, plateauWindows                                             uint
	conditioning                                                               bool
	timeout                                                                    time.Duration
}

type observation struct {
	clients, repetition, position int
	elapsedNS                     uint64
	throughput                    uint64
	maximumP999MilliUS            uint64
	maximumLatencyMilliUS         uint64
}

type levelSummary struct {
	clients                     int
	throughputMedian            uint64
	maximumP999MedianMilliUS    uint64
	maximumLatencyMedianMilliUS uint64
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	remaining := buffer.limit - buffer.Len()
	if remaining <= 0 {
		return 0, errors.New("output exceeded hard bound")
	}
	if len(value) > remaining {
		written, _ := buffer.Buffer.Write(value[:remaining])
		return written, errors.New("output exceeded hard bound")
	}
	return buffer.Buffer.Write(value)
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "saturation:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	cleanup, err := prepareMixedBinary(&cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	var observations []observation
	if cfg.conditioning {
		for _, clients := range cfg.clientLevels {
			if _, err := executeMixed(cfg, clients, 0, 0); err != nil {
				return fmt.Errorf("conditioning clients=%d: %w", clients, err)
			}
		}
	}
	for repetition := range cfg.repetitions {
		for position := range cfg.clientLevels {
			clients := cfg.clientLevels[(position+repetition)%len(cfg.clientLevels)]
			fmt.Fprintf(stderr, "saturation repetition %d/%d position %d/%d clients=%d\n",
				repetition+1, cfg.repetitions, position+1, len(cfg.clientLevels), clients)
			observed, err := executeMixed(cfg, clients, repetition+1, position+1)
			if err != nil {
				return fmt.Errorf("repetition=%d position=%d clients=%d: %w",
					repetition+1, position+1, clients, err)
			}
			observations = append(observations, observed)
		}
	}
	summaries, err := summarizeLevels(cfg.clientLevels, cfg.repetitions, observations)
	if err != nil {
		return err
	}
	saturationClients, saturated := classifySaturation(summaries, cfg.minimumGainBPS, cfg.plateauWindows)
	var encoded bytes.Buffer
	writeEvidence(&encoded, cfg, observations, summaries, saturationClients, saturated)
	if err := writeOutput(cfg.output, stdout, encoded.Bytes()); err != nil {
		return err
	}
	if !saturated {
		return errors.New("fixed client sweep did not observe the configured consecutive throughput plateau")
	}
	return nil
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("saturation", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cfg config
	clients := fs.String("clients", "1,2,4,8,16,32,64", "strictly increasing geometric client levels")
	fs.StringVar(&cfg.mixedBin, "mixed-bin", "", "prebuilt cmd/mixed executable")
	fs.StringVar(&cfg.output, "output", "", "new absolute canonical TSV path; - writes stdout")
	fs.StringVar(&cfg.engine, "engine", "", "one file-backed engine")
	fs.IntVar(&cfg.repetitions, "repetitions", 7, "recorded cyclic-order repetitions")
	fs.BoolVar(&cfg.conditioning, "conditioning", true, "discard one child at every client level before recording")
	fs.DurationVar(&cfg.timeout, "timeout", 5*time.Minute, "hard timeout for each child")
	fs.UintVar(&cfg.minimumGainBPS, "minimum-gain-bps", 500, "gain at or below this basis-point threshold is a plateau")
	fs.UintVar(&cfg.plateauWindows, "plateau-windows", 2, "required consecutive plateau transitions")
	fs.StringVar(&cfg.workload, "workload", "churn", "mixed workload")
	fs.IntVar(&cfg.corpus, "corpus", 10_000, "documents")
	fs.IntVar(&cfg.operations, "operations", 20_000, "measured operations per child")
	fs.IntVar(&cfg.warmup, "warmup", 2_000, "unmeasured operations per child")
	fs.StringVar(&cfg.durability, "durability", "buffered-visible", "matched durability")
	fs.IntVar(&cfg.checkpointMutations, "checkpoint-mutations", 64, "matched checkpoint cadence")
	fs.IntVar(&cfg.exactIndexes, "exact-indexes", 0, "matched simultaneous exact index count (0-3)")
	fs.StringVar(&cfg.documentShape, "document-shape", "inline", "inline, mixed, or overflow-heavy")
	fs.StringVar(&cfg.cardinality, "cardinality", "low", "low or high")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	levels, err := parseClientLevels(*clients)
	if err != nil {
		return cfg, err
	}
	cfg.clientLevels = levels
	if cfg.output == "" || (cfg.output != "-" && !filepath.IsAbs(cfg.output)) || cfg.engine == "" ||
		cfg.repetitions < len(levels) || cfg.repetitions%len(levels) != 0 || cfg.repetitions > maximumRepetitions || cfg.timeout <= 0 ||
		cfg.minimumGainBPS > 10_000 || cfg.plateauWindows < 1 || int(cfg.plateauWindows) >= len(levels) ||
		cfg.corpus < levels[len(levels)-1] || cfg.operations < levels[len(levels)-1] || cfg.warmup < 0 ||
		cfg.checkpointMutations < 0 || cfg.exactIndexes < 0 || cfg.exactIndexes > int(competitive.MaximumExactIndexes) {
		return cfg, errors.New("invalid output, engine, repetition, timeout, plateau, corpus, operation, or matched-index configuration")
	}
	if cfg.output != "-" {
		if _, err := os.Stat(cfg.output); err == nil || !os.IsNotExist(err) {
			return cfg, errors.New("output path must not exist")
		}
	}
	if _, ok := competitive.FactoryNamed(cfg.engine); !ok {
		return cfg, fmt.Errorf("unknown engine %q", cfg.engine)
	}
	if cfg.exactIndexes != 0 && !competitive.IndexCapable(cfg.engine) {
		return cfg, fmt.Errorf("%s has no native secondary index", cfg.engine)
	}
	if mode, err := competitive.ParseDurabilityMode(cfg.durability); err != nil {
		return cfg, err
	} else if _, err := competitive.ResolveDurabilityMode(cfg.engine, mode); err != nil {
		return cfg, err
	}
	if _, err := competitive.ParseCardinality(cfg.cardinality); err != nil {
		return cfg, err
	}
	if _, err := competitive.ParseDocumentShape(cfg.documentShape); err != nil {
		return cfg, err
	}
	if _, ok := saturationWorkload(cfg.workload); !ok {
		return cfg, fmt.Errorf("unknown workload %q", cfg.workload)
	}
	return cfg, nil
}

func prepareMixedBinary(cfg *config) (func(), error) {
	if cfg.mixedBin != "" {
		resolved, err := exec.LookPath(cfg.mixedBin)
		if err != nil {
			return nil, fmt.Errorf("resolve -mixed-bin: %w", err)
		}
		cfg.mixedBin = resolved
		return func() {}, nil
	}
	moduleFile, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return nil, fmt.Errorf("locate competitive module: %w", err)
	}
	moduleFile = bytes.TrimSpace(moduleFile)
	if len(moduleFile) == 0 || string(moduleFile) == os.DevNull {
		return nil, errors.New("saturation must run inside the competitive module or receive -mixed-bin")
	}
	temporary, err := os.MkdirTemp("", "vibedb-saturation-")
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(temporary) }
	binary := filepath.Join(temporary, "mixed")
	command := exec.Command("go", "build", "-o", binary, "./cmd/mixed")
	command.Dir = filepath.Dir(string(moduleFile))
	var output boundedBuffer
	output.limit = maximumChildOutput
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		cleanup()
		return nil, fmt.Errorf("build cmd/mixed: %w: %s", err, strings.TrimSpace(output.String()))
	}
	cfg.mixedBin = binary
	return cleanup, nil
}

func parseClientLevels(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	if len(parts) < 3 || len(parts) > maximumClientLevels {
		return nil, fmt.Errorf("-clients requires 3-%d levels", maximumClientLevels)
	}
	levels := make([]int, len(parts))
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 || (i != 0 && n != levels[i-1]*2) {
			return nil, errors.New("-clients must be a strictly increasing doubling sequence")
		}
		levels[i] = n
	}
	return levels, nil
}

func saturationWorkload(name string) (struct{}, bool) {
	switch name {
	case "ycsb-b", "ycsb-a", "ycsb-f", "write", "churn", "scan":
		return struct{}{}, true
	default:
		return struct{}{}, false
	}
}

func executeMixed(cfg config, clients, repetition, position int) (observation, error) {
	args := []string{
		"-header", "-engine=" + cfg.engine, "-workload=" + cfg.workload,
		"-corpus=" + strconv.Itoa(cfg.corpus), "-operations=" + strconv.Itoa(cfg.operations),
		"-warmup=" + strconv.Itoa(cfg.warmup), "-durability=" + cfg.durability,
		"-checkpoint-mutations=" + strconv.Itoa(cfg.checkpointMutations),
		"-exact-indexes=" + strconv.Itoa(cfg.exactIndexes), "-document-shape=" + cfg.documentShape,
		"-cardinality=" + cfg.cardinality, "-clients=" + strconv.Itoa(clients),
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	command := exec.CommandContext(ctx, cfg.mixedBin, args...)
	command.Env = environmentWith(os.Environ(), "LC_ALL=C", "LANG=C")
	output := boundedBuffer{limit: maximumChildOutput}
	command.Stdout = &output
	stderr := boundedBuffer{limit: maximumChildOutput}
	command.Stderr = &stderr
	started := time.Now()
	err := command.Run()
	elapsed := uint64(max(time.Since(started), time.Nanosecond))
	if ctx.Err() != nil {
		return observation{}, fmt.Errorf("child timed out after %s", cfg.timeout)
	}
	if err != nil {
		return observation{}, fmt.Errorf("child failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	throughput, p999, maximum, err := parseMixedOutput(cfg, clients, output.Bytes())
	if err != nil {
		return observation{}, err
	}
	return observation{clients: clients, repetition: repetition, position: position,
		elapsedNS: elapsed, throughput: throughput, maximumP999MilliUS: p999,
		maximumLatencyMilliUS: maximum}, nil
}

func parseMixedOutput(cfg config, clients int, value []byte) (uint64, uint64, uint64, error) {
	scanner := bufio.NewScanner(bytes.NewReader(value))
	scanner.Buffer(make([]byte, 4096), maximumChildOutput)
	if !scanner.Scan() {
		return 0, 0, 0, errors.New("mixed child omitted header")
	}
	header := strings.Fields(scanner.Text())
	index := make(map[string]int, len(header))
	for i, field := range header {
		index[field] = i
	}
	required := []string{"engine", "durability", "workload", "card", "document-shape", "docs", "measured", "warmup", "checkpoint", "forced-cp", "exact-indexes", "clients", "operation", "calls", "p99.9-us", "max-us", "total-ops/s"}
	for _, field := range required {
		if _, ok := index[field]; !ok {
			return 0, 0, 0, fmt.Errorf("mixed child omitted %q", field)
		}
	}
	wants := map[string]string{"durability": cfg.durability, "workload": cfg.workload,
		"card": cfg.cardinality, "document-shape": cfg.documentShape,
		"docs": strconv.Itoa(cfg.corpus), "measured": strconv.Itoa(cfg.operations),
		"warmup": strconv.Itoa(cfg.warmup), "checkpoint": strconv.Itoa(cfg.checkpointMutations),
		"exact-indexes": strconv.Itoa(cfg.exactIndexes), "clients": strconv.Itoa(clients)}
	var throughput, p999, maximum uint64
	rows, operationCalls := 0, uint64(0)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != len(header) {
			return 0, 0, 0, errors.New("mixed child row/header width mismatch")
		}
		if got := fields[index["engine"]]; got != cfg.engine && !strings.HasPrefix(got, cfg.engine+"/") {
			return 0, 0, 0, fmt.Errorf("mixed child engine=%q want=%q", got, cfg.engine)
		}
		for name, want := range wants {
			if fields[index[name]] != want {
				return 0, 0, 0, fmt.Errorf("mixed child %s=%q want=%q", name, fields[index[name]], want)
			}
		}
		if fields[index["forced-cp"]] != "0" {
			return 0, 0, 0, errors.New("mixed child reported a forced checkpoint")
		}
		calls, err := strconv.ParseUint(fields[index["calls"]], 10, 64)
		if err != nil || calls == 0 {
			return 0, 0, 0, errors.New("mixed child calls are invalid")
		}
		if fields[index["operation"]] != "checkpoint" {
			operationCalls += calls
		}
		rowThroughput, err := strconv.ParseUint(fields[index["total-ops/s"]], 10, 64)
		if err != nil || rowThroughput == 0 || (rows != 0 && rowThroughput != throughput) {
			return 0, 0, 0, errors.New("mixed child throughput is invalid or inconsistent")
		}
		rowP999, err := parseFixedMilli(fields[index["p99.9-us"]])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("parse p99.9: %w", err)
		}
		rowMax, err := parseFixedMilli(fields[index["max-us"]])
		if err != nil || rowMax < rowP999 {
			return 0, 0, 0, errors.New("mixed child maximum latency is invalid")
		}
		throughput = rowThroughput
		p999 = max(p999, rowP999)
		maximum = max(maximum, rowMax)
		rows++
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, 0, err
	}
	if rows == 0 {
		return 0, 0, 0, errors.New("mixed child omitted rows")
	}
	if operationCalls != uint64(cfg.operations) {
		return 0, 0, 0, fmt.Errorf("mixed child operation calls=%d want=%d", operationCalls, cfg.operations)
	}
	return throughput, p999, maximum, nil
}

func environmentWith(base []string, overrides ...string) []string {
	replaced := make(map[string]struct{}, len(overrides))
	for _, override := range overrides {
		name, _, _ := strings.Cut(override, "=")
		replaced[name] = struct{}{}
	}
	result := make([]string, 0, len(base)+len(overrides))
	for _, item := range base {
		name, _, _ := strings.Cut(item, "=")
		if _, ok := replaced[name]; !ok {
			result = append(result, item)
		}
	}
	return append(result, overrides...)
}

func parseFixedMilli(value string) (uint64, error) {
	whole, fraction, found := strings.Cut(value, ".")
	if !found || len(fraction) != 3 {
		return 0, errors.New("latency must use exact three-decimal microseconds")
	}
	w, err := strconv.ParseUint(whole, 10, 64)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseUint(fraction, 10, 64)
	if err != nil || w > (^uint64(0)-f)/1000 {
		return 0, errors.New("latency is invalid or overflows")
	}
	return w*1000 + f, nil
}

func summarizeLevels(levels []int, repetitions int, observations []observation) ([]levelSummary, error) {
	if len(observations) != len(levels)*repetitions {
		return nil, errors.New("incomplete saturation observation matrix")
	}
	result := make([]levelSummary, 0, len(levels))
	for _, clients := range levels {
		var throughput, p999, maximum []uint64
		for _, observed := range observations {
			if observed.clients == clients {
				throughput = append(throughput, observed.throughput)
				p999 = append(p999, observed.maximumP999MilliUS)
				maximum = append(maximum, observed.maximumLatencyMilliUS)
			}
		}
		if len(throughput) != repetitions {
			return nil, fmt.Errorf("clients=%d has %d samples, want %d", clients, len(throughput), repetitions)
		}
		result = append(result, levelSummary{clients: clients, throughputMedian: integerMedian(throughput),
			maximumP999MedianMilliUS: integerMedian(p999), maximumLatencyMedianMilliUS: integerMedian(maximum)})
	}
	return result, nil
}

func integerMedian(values []uint64) uint64 {
	values = slices.Clone(values)
	slices.Sort(values)
	return values[len(values)/2]
}

func classifySaturation(summaries []levelSummary, gainBPS uint, windows uint) (int, bool) {
	consecutive := uint(0)
	first := 0
	for i := 1; i < len(summaries); i++ {
		previous, current := summaries[i-1].throughputMedian, summaries[i].throughputMedian
		increment := previous/10_000*uint64(gainBPS) + (previous%10_000)*uint64(gainBPS)/10_000
		ceiling := previous + increment
		if ceiling < previous {
			ceiling = ^uint64(0)
		}
		plateau := previous != 0 && current <= ceiling
		if plateau {
			if consecutive == 0 {
				first = summaries[i].clients
			}
			consecutive++
			if consecutive >= windows {
				return first, true
			}
		} else {
			consecutive, first = 0, 0
		}
	}
	return 0, false
}

func writeEvidence(w io.Writer, cfg config, observations []observation, summaries []levelSummary, clients int, saturated bool) {
	fmt.Fprintf(w, "schema\tvibedb.saturation-evidence\t%d\n", schemaVersion)
	fmt.Fprintf(w, "config\tengine\t%s\nconfig\tworkload\t%s\nconfig\tdurability\t%s\n", cfg.engine, cfg.workload, cfg.durability)
	fmt.Fprintf(w, "config\texact-indexes\t%d\nconfig\tdocument-shape\t%s\nconfig\tcardinality\t%s\n", cfg.exactIndexes, cfg.documentShape, cfg.cardinality)
	fmt.Fprintf(w, "config\tcorpus\t%d\nconfig\toperations\t%d\nconfig\twarmup\t%d\nconfig\tcheckpoint-mutations\t%d\n", cfg.corpus, cfg.operations, cfg.warmup, cfg.checkpointMutations)
	fmt.Fprintf(w, "config\tclient-levels\t%s\nconfig\trepetitions\t%d\nconfig\tconditioning\t%t\n", joinInts(cfg.clientLevels), cfg.repetitions, cfg.conditioning)
	fmt.Fprintf(w, "config\tminimum-gain-bps\t%d\nconfig\tplateau-windows\t%d\n", cfg.minimumGainBPS, cfg.plateauWindows)
	fmt.Fprintln(w, "raw_header\trepetition\tposition\tclients\telapsed_ns\tthroughput_ops_s\tmaximum_operation_p999_milli_us\tmaximum_operation_latency_milli_us")
	for _, observed := range observations {
		fmt.Fprintf(w, "raw\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", observed.repetition, observed.position, observed.clients,
			observed.elapsedNS, observed.throughput, observed.maximumP999MilliUS, observed.maximumLatencyMilliUS)
	}
	fmt.Fprintln(w, "summary_header\tclients\tsamples\tthroughput_median_ops_s\tmaximum_operation_p999_median_milli_us\tmaximum_operation_latency_median_milli_us")
	for _, summary := range summaries {
		fmt.Fprintf(w, "summary\t%d\t%d\t%d\t%d\t%d\n", summary.clients, cfg.repetitions,
			summary.throughputMedian, summary.maximumP999MedianMilliUS, summary.maximumLatencyMedianMilliUS)
	}
	fmt.Fprintf(w, "decision\tsaturation-observed\t%t\tsaturation-clients\t%d\trule\t%d-consecutive-throughput-gains-at-or-below-%d-bps\n",
		saturated, clients, cfg.plateauWindows, cfg.minimumGainBPS)
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}

func writeOutput(path string, stdout io.Writer, value []byte) error {
	if path == "-" {
		return writeExact(stdout, value)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeExact(file, value)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	return errors.Join(writeErr, file.Close())
}

func writeExact(destination io.Writer, value []byte) error {
	for len(value) != 0 {
		written, err := destination.Write(value)
		if written > 0 {
			value = value[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
