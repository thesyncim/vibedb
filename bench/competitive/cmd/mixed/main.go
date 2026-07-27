// Command mixed runs one isolated mixed workload and prints operation latency,
// aggregate throughput, retained Go memory, peak RSS, and disk footprint. Run
// one engine per process; RSS and runtime memory are process-global.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

const (
	opRead = iota
	opUpdate
	opReadModifyWrite
	opChurn
	opScan
	opKinds
)

var opNames = [opKinds]string{"read", "update", "read-modify-write", "delete+restore", "ordered-scan"}

type workload struct {
	name                         string
	reads, updates, readModifies int
	churns, scans                int
}

func (w workload) total() int {
	return w.reads + w.updates + w.readModifies + w.churns + w.scans
}

func workloadNamed(name string) (workload, bool) {
	switch name {
	case "ycsb-b":
		return workload{name: name, reads: 950, updates: 50}, true
	case "ycsb-a":
		return workload{name: name, reads: 500, updates: 500}, true
	case "ycsb-f":
		return workload{name: name, reads: 500, readModifies: 500}, true
	case "churn":
		return workload{name: name, reads: 700, updates: 250, churns: 50}, true
	case "scan":
		return workload{name: name, reads: 799, updates: 150, churns: 50, scans: 1}, true
	default:
		return workload{}, false
	}
}

func operationTrace(w workload) []int {
	cycle := make([]int, 0, w.total())
	appendN := func(op, n int) {
		for range n {
			cycle = append(cycle, op)
		}
	}
	appendN(opRead, w.reads)
	appendN(opUpdate, w.updates)
	appendN(opReadModifyWrite, w.readModifies)
	appendN(opChurn, w.churns)
	appendN(opScan, w.scans)
	rng := rand.New(rand.NewSource(0xA11CE))
	trace := make([]int, 0, 64*len(cycle))
	for range 64 {
		rng.Shuffle(len(cycle), func(i, j int) {
			cycle[i], cycle[j] = cycle[j], cycle[i]
		})
		trace = append(trace, cycle...)
	}
	return trace
}

func main() {
	engineName := flag.String("engine", "", "engine name (see -list)")
	workloadName := flag.String("workload", "churn", "ycsb-b, ycsb-a, ycsb-f, churn, or scan")
	corpusSize := flag.Int("corpus", competitive.CorpusSize, "documents in the shared corpus")
	operations := flag.Int("operations", 100_000, "measured user operations")
	warmup := flag.Int("warmup", 10_000, "unmeasured warmup operations")
	durabilityName := flag.String(
		"durability", "default",
		"default, volatile, buffered-visible, async-stable-in-flight, ordinary-sync, or power-safe",
	)
	checkpointMutations := flag.Int(
		"checkpoint-mutations", 0,
		"checkpoint after this many acknowledged state changes; 0 checkpoints once after all measured mutations",
	)
	indexed := flag.Bool("indexed", false, "maintain the country secondary index")
	putloop := flag.Bool("putloop", false, "store/durable only: load by replaying Put")
	compact := flag.Bool("compact", false, "store/durable only: explicitly use compact bulk documents")
	card := flag.String("cardinality", "low", "low or high corpus cardinality")
	list := flag.Bool("list", false, "list engines and workloads")
	header := flag.Bool("header", false, "print the table header first")
	flag.Parse()

	if *list {
		fmt.Println("workloads: ycsb-b ycsb-a ycsb-f churn scan")
		fmt.Print("engines:")
		for _, factory := range competitive.Factories() {
			fmt.Print(" ", factory.Name)
		}
		fmt.Println()
		return
	}
	if *engineName == "" || *corpusSize < 2 || *operations < 1 ||
		*warmup < 0 || *checkpointMutations < 0 {
		fail("mixed: -engine, -corpus>=2, -operations>=1, -warmup>=0, and -checkpoint-mutations>=0 are required")
	}
	if *compact && (*engineName != "vibejson-durable" || *putloop) {
		fail("mixed: -compact requires vibejson-durable bulk mode")
	}
	factory, ok := competitive.FactoryNamed(*engineName)
	if !ok {
		fail("mixed: unknown engine %q", *engineName)
	}
	mix, ok := workloadNamed(*workloadName)
	if !ok || mix.total() != 1000 {
		fail("mixed: unknown or malformed workload %q", *workloadName)
	}
	if *indexed && !competitive.IndexCapable(factory.Name) {
		fail("mixed: %s has no native secondary index", factory.Name)
	}
	cardinality, err := competitive.ParseCardinality(*card)
	check(err)
	durability, err := competitive.ParseDurabilityMode(*durabilityName)
	check(err)

	docs := competitive.CorpusOf(*corpusSize, cardinality)
	dir, err := os.MkdirTemp("", "vibebench-mixed-")
	check(err)
	defer os.RemoveAll(dir)
	engine, err := factory.New(competitive.Config{
		Dir:        dir,
		Durability: durability,
		Indexed:    *indexed,
		CacheBytes: competitive.DefaultCacheBytes,
		PutLoop:    *putloop,
		Compact:    *compact,
	})
	check(err)
	defer engine.Close()
	check(engine.Load(docs))
	// Start every workload from a recoverable baseline. This is a logical
	// durability fence, not forced representation maintenance: Pebble uses its
	// documented WAL sequence fence rather than an unnecessary memtable flush.
	check(engine.Checkpoint())

	maxInt := int(^uint(0) >> 1)
	if *warmup > maxInt-*operations {
		fail("mixed: -operations plus -warmup overflows int")
	}
	keyTraceSize := *operations + *warmup
	trace := make([]int, keyTraceSize)
	rng := rand.New(rand.NewSource(0x51C5B))
	zipf := rand.NewZipf(rng, 1.01, 1, uint64(len(docs)-1))
	probe := rand.New(rand.NewSource(7)).Perm(len(docs))
	for i := range trace {
		trace[i] = probe[int(zipf.Uint64())]
	}
	probe = nil
	choices := operationTrace(mix)

	var readScratch, replacement []byte
	updated := make([]bool, len(docs))
	run := func(choice, idx int) (kind, mutations int, err error) {
		key := docs[idx].Key
		switch choice {
		case opRead:
			out, err := engine.Get(readScratch[:0], key)
			readScratch = out
			return opRead, 0, err
		case opUpdate:
			if updated[idx] {
				replacement = append(replacement[:0], docs[idx].JSON...)
			} else {
				replacement = competitive.AppendSameSizeUpdatedJSON(replacement[:0], docs, idx)
			}
			err := engine.Put(key, replacement)
			if err == nil {
				updated[idx] = !updated[idx]
			}
			return opUpdate, mutationCount(choice), err
		case opReadModifyWrite:
			out, err := engine.Get(readScratch[:0], key)
			readScratch = out
			if err != nil {
				return opReadModifyWrite, 0, err
			}
			if updated[idx] {
				replacement = append(replacement[:0], docs[idx].JSON...)
			} else {
				replacement = competitive.AppendSameSizeUpdatedJSON(replacement[:0], docs, idx)
			}
			err = engine.Put(key, replacement)
			if err == nil {
				updated[idx] = !updated[idx]
			}
			return opReadModifyWrite, mutationCount(choice), err
		case opChurn:
			if err := engine.Delete(key); err != nil {
				return opChurn, 0, err
			}
			err := engine.Upsert(key, docs[idx].JSON)
			if err == nil {
				updated[idx] = false
			}
			return opChurn, mutationCount(choice), err
		case opScan:
			n, err := engine.ScanAllBytes()
			if err == nil && n != len(docs) {
				err = fmt.Errorf("ordered scan visited %d documents, want %d", n, len(docs))
			}
			return opScan, 0, err
		default:
			return 0, 0, fmt.Errorf("unknown mixed operation %d", choice)
		}
	}

	warmupCheckpoints := checkpointSchedule{every: *checkpointMutations}
	for i := 0; i < *warmup; i++ {
		_, mutations, err := run(
			choices[i%len(choices)], trace[i%len(trace)],
		)
		check(err)
		if warmupCheckpoints.Add(mutations) {
			check(engine.Checkpoint())
			warmupCheckpoints.Mark()
		}
	}
	// Warmup must not consume the measured loss window or leave stable work
	// queued for the timed operations to inherit.
	check(engine.Checkpoint())
	automaticCheckpointStart := automaticCheckpointCount(engine)

	latencies := [opKinds][]int64{}
	latencies[opRead] = make([]int64, 0, *operations*mix.reads/mix.total()+1)
	latencies[opUpdate] = make([]int64, 0, *operations*mix.updates/mix.total()+1)
	latencies[opReadModifyWrite] = make([]int64, 0, *operations*mix.readModifies/mix.total()+1)
	latencies[opChurn] = make([]int64, 0, *operations*mix.churns/mix.total()+1)
	latencies[opScan] = make([]int64, 0, *operations*mix.scans/mix.total()+1)
	checkpointCapacity := 1
	if *checkpointMutations > 0 {
		checkpointCapacity += *operations / *checkpointMutations
	}
	checkpointLatencies := make([]int64, 0, checkpointCapacity)
	checkpoints := checkpointSchedule{every: *checkpointMutations}
	throughputStart := time.Now()
	for i := 0; i < *operations; i++ {
		sequence := *warmup + i
		start := time.Now()
		kind, mutations, err := run(
			choices[sequence%len(choices)],
			trace[sequence%len(trace)],
		)
		elapsed := time.Since(start).Nanoseconds()
		check(err)
		latencies[kind] = append(latencies[kind], elapsed)
		if checkpoints.Add(mutations) {
			start = time.Now()
			check(engine.Checkpoint())
			checkpointLatencies = append(
				checkpointLatencies, time.Since(start).Nanoseconds(),
			)
			checkpoints.Mark()
		}
	}
	// A finite run must not hide its final durability work after the throughput
	// timer. With -checkpoint-mutations=0 this is the one explicit checkpoint.
	if checkpoints.Pending() != 0 {
		start := time.Now()
		check(engine.Checkpoint())
		checkpointLatencies = append(
			checkpointLatencies, time.Since(start).Nanoseconds(),
		)
	}
	measuredNanos := time.Since(throughputStart).Nanoseconds()
	automaticCheckpoints := automaticCheckpointCount(engine) -
		automaticCheckpointStart

	seen := make([]bool, len(docs))
	var expected []byte
	check(engine.Visit(func(key string, value []byte) error {
		const prefix = "doc:"
		if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
			return fmt.Errorf("malformed final key %q", key)
		}
		ord, err := strconv.Atoi(key[len(prefix):])
		if err != nil || ord < 0 || ord >= len(docs) {
			return fmt.Errorf("malformed final key %q", key)
		}
		if seen[ord] {
			return fmt.Errorf("duplicate final key %q", key)
		}
		seen[ord] = true
		if updated[ord] {
			expected = competitive.AppendSameSizeUpdatedJSON(expected[:0], docs, ord)
		} else {
			expected = append(expected[:0], docs[ord].JSON...)
		}
		if !bytes.Equal(value, expected) {
			return fmt.Errorf("final value mismatch for %q", key)
		}
		return nil
	}))
	visited := 0
	for _, ok := range seen {
		if ok {
			visited++
		}
	}
	if visited != len(docs) {
		fail("mixed: final scan visited %d documents, want %d", visited, len(docs))
	}

	type summary struct {
		calls         int
		p50, p95, p99 int64
	}
	summaries := [opKinds]summary{}
	for kind, samples := range latencies {
		if len(samples) == 0 {
			continue
		}
		slices.Sort(samples)
		summaries[kind] = summary{
			calls: len(samples),
			p50:   percentile(samples, 0.50),
			p95:   percentile(samples, 0.95),
			p99:   percentile(samples, 0.99),
		}
	}
	var checkpointSummary summary
	if len(checkpointLatencies) != 0 {
		slices.Sort(checkpointLatencies)
		checkpointSummary = summary{
			calls: len(checkpointLatencies),
			p50:   percentile(checkpointLatencies, 0.50),
			p95:   percentile(checkpointLatencies, 0.95),
			p99:   percentile(checkpointLatencies, 0.99),
		}
	}

	// Release harness storage before the retained-memory reading.
	trace = nil
	choices = nil
	latencies = [opKinds][]int64{}
	checkpointLatencies = nil
	readScratch = nil
	replacement = nil
	expected = nil
	seen = nil
	updated = nil
	docs = nil
	fp, err := competitive.Measure(engine, dir)
	check(err)

	reportName := factory.Name
	if factory.Name == "vibejson-durable" {
		if *putloop {
			reportName += "/put"
		} else if *compact {
			reportName += "/bulk-compact"
		} else {
			reportName += "/bulk-verbatim"
		}
	}
	if *header {
		printHeader(os.Stdout)
	}
	throughput := float64(*operations) * float64(time.Second) / float64(measuredNanos)
	printResult := func(operation string, result summary) {
		fmt.Printf("%-20s %-24s %-8s %-4s %7d %9d %7d %10d %9d %7v %-18s %10d %11.3f %11.3f %11.3f %12.0f %10.1f %10.1f %10.1f %11.1f %12.1f\n",
			reportName, engine.DurabilityMode(), mix.name, cardinality,
			*corpusSize, *operations, *warmup, *checkpointMutations,
			automaticCheckpoints, *indexed, operation, result.calls,
			micros(result.p50), micros(result.p95), micros(result.p99), throughput,
			mib(fp.DiskBytes), mib(fp.DiskAllocatedBytes), mib(int64(fp.HeapAlloc)),
			mib(int64(fp.RuntimeResident)), mib(fp.MaxRSSBytes()))
	}
	for kind, result := range summaries {
		if result.calls == 0 {
			continue
		}
		printResult(opNames[kind], result)
	}
	if checkpointSummary.calls != 0 {
		printResult("checkpoint", checkpointSummary)
	}
}

func printHeader(w io.Writer) {
	fmt.Fprintf(w, "%-20s %-24s %-8s %-4s %7s %9s %7s %10s %9s %7s %-18s %10s %11s %11s %11s %12s %10s %10s %10s %11s %12s\n",
		"engine", "durability", "workload", "card", "docs", "measured",
		"warmup", "checkpoint", "forced-cp", "indexed", "operation", "calls",
		"p50-us", "p95-us", "p99-us", "total-ops/s", "disk-MiB", "alloc-MiB",
		"heap-MiB", "runtime-MiB", "peak-rss-MiB")
}

type automaticCheckpointReporter interface {
	AutomaticCheckpoints() uint64
}

func automaticCheckpointCount(engine competitive.Engine) uint64 {
	reporter, ok := engine.(automaticCheckpointReporter)
	if !ok {
		return 0
	}
	return reporter.AutomaticCheckpoints()
}

func mutationCount(operation int) int {
	switch operation {
	case opUpdate, opReadModifyWrite:
		return 1
	case opChurn:
		return 2
	default:
		return 0
	}
}

type checkpointSchedule struct {
	every   int
	pending int
}

func (s *checkpointSchedule) Add(mutations int) bool {
	s.pending += mutations
	return s.every > 0 && s.pending >= s.every
}

func (s *checkpointSchedule) Mark() { s.pending = 0 }

func (s *checkpointSchedule) Pending() int { return s.pending }

func percentile(sorted []int64, quantile float64) int64 {
	at := int(quantile*float64(len(sorted)-1) + 0.5)
	return sorted[at]
}

func micros(nanos int64) float64 { return float64(nanos) / float64(time.Microsecond) }
func mib(bytes int64) float64    { return float64(bytes) / (1 << 20) }

func check(err error) {
	if err != nil {
		fail("mixed: %v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
