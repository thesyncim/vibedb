// Command mixed runs one isolated mixed workload and prints operation latency,
// aggregate throughput, retained Go memory, peak RSS, and disk footprint. Run
// one engine per process; RSS and runtime memory are process-global.
//
// With -clients=N the operation stream is partitioned across N worker goroutines
// that share one engine handle, so every engine can be measured under concurrent
// load. N=1 reproduces the single-client protocol the published tables use, so
// its rows stay directly comparable; see the -clients flag and the partitioning
// helpers below for how the two paths stay identical at N=1.
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
	"sync"
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

// The trace and choice seeds are fixed so a run reproduces byte-for-byte. Under
// -clients=N each client derives its own seeds from these bases (see
// clientState.build), and client 0 at N=1 gets exactly these bases, which is why
// the single-client stream is identical to the pre-concurrency harness.
const (
	baseKeySeed         = int64(0x51C5B)
	baseProbeSeed       = int64(7)
	choiceSeed          = int64(0xA11CE)
	perClientSeedStride = int64(0x27BB2EE687B0B0FD) // large odd multiplier, decorrelates client seeds
)

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
	case "write":
		return workload{name: name, updates: 1000}, true
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
	rng := rand.New(rand.NewSource(choiceSeed))
	trace := make([]int, 0, 64*len(cycle))
	for range 64 {
		rng.Shuffle(len(cycle), func(i, j int) {
			cycle[i], cycle[j] = cycle[j], cycle[i]
		})
		trace = append(trace, cycle...)
	}
	return trace
}

// clientState is one worker's private slice of the run: a disjoint corpus shard
// it alone mutates (so the content oracle survives concurrency without a lock),
// its own scratch buffers and engine session (so the hot loop stays
// zero-allocation and race-free), and a pre-sized per-operation latency array it
// merges into the aggregate after the timed loop.
type clientState struct {
	id      int
	session competitive.EngineSession

	base, span  int // corpus shard [base, base+span)
	choiceOff   int // phase offset into the shared choices cycle
	warmupOps   int
	measuredOps int
	keyTrace    []int // length warmupOps+measuredOps, global doc indices in the shard

	readScratch []byte
	replacement []byte
	latencies   [opKinds][]int64
	err         error
}

// evenSplit partitions total items across clients deterministically: the first
// total%clients workers take one extra, so the parts sum back to total and the
// N=1 part is the whole. It sizes both the corpus shards and the op counts.
func evenSplit(total, clients, id int) (offset, size int) {
	q, r := total/clients, total%clients
	if id < r {
		return id * (q + 1), q + 1
	}
	return r*(q+1) + (id-r)*q, q
}

// buildKeyTrace generates a client's Zipfian key stream over its own shard. At
// N=1 the shard is the whole corpus and the seeds are the bases, so
// base(=0)+probe[zipf()] is exactly the pre-concurrency global trace; the
// single-client row therefore reproduces the published protocol. Each client's
// keys stay inside [base, base+span) so no two clients ever mutate the same key.
func buildKeyTrace(base, span, length int, keySeed, probeSeed int64) []int {
	rng := rand.New(rand.NewSource(keySeed))
	zipf := rand.NewZipf(rng, 1.01, 1, uint64(span-1))
	probe := rand.New(rand.NewSource(probeSeed)).Perm(span)
	trace := make([]int, length)
	for i := range trace {
		trace[i] = base + probe[int(zipf.Uint64())]
	}
	return trace
}

func main() {
	engineName := flag.String("engine", "", "engine name (see -list)")
	workloadName := flag.String("workload", "churn", "ycsb-b, ycsb-a, ycsb-f, write, churn, or scan")
	corpusSize := flag.Int("corpus", competitive.CorpusSize, "documents in the shared corpus")
	operations := flag.Int("operations", 100_000, "measured user operations")
	warmup := flag.Int("warmup", 10_000, "unmeasured warmup operations")
	clients := flag.Int("clients", 1, "concurrent worker goroutines sharing one engine handle")
	durabilityName := flag.String(
		"durability", "buffered-visible",
		"buffered-visible, async-stable-in-flight, ordinary-sync, or power-safe",
	)
	checkpointMutations := flag.Int(
		"checkpoint-mutations", 0,
		"checkpoint after this many acknowledged state changes; 0 checkpoints once after all measured mutations",
	)
	indexed := flag.Bool("indexed", false, "maintain the country secondary index")
	putloop := flag.Bool("putloop", false, "store/durable only: load by replaying Put")
	card := flag.String("cardinality", "low", "low or high corpus cardinality")
	list := flag.Bool("list", false, "list engines and workloads")
	header := flag.Bool("header", false, "print the table header first")
	flag.Parse()

	if *list {
		fmt.Println("workloads: ycsb-b ycsb-a ycsb-f write churn scan")
		fmt.Print("engines:")
		for _, factory := range competitive.Factories() {
			fmt.Print(" ", factory.Name)
		}
		fmt.Println()
		return
	}
	if *engineName == "" || *corpusSize < 2 || *operations < 1 ||
		*warmup < 0 || *checkpointMutations < 0 || *clients < 1 {
		fail("mixed: -engine, -corpus>=2, -operations>=1, -warmup>=0, -checkpoint-mutations>=0, and -clients>=1 are required")
	}
	// Each client owns a corpus shard and measures at least one operation; the
	// shard partition (and its per-client Zipf) needs a non-empty shard, and a
	// client with no measured work would only dilute the schedule.
	if *clients > *corpusSize {
		fail("mixed: -clients=%d exceeds -corpus=%d (each client needs its own key shard)", *clients, *corpusSize)
	}
	if *clients > *operations {
		fail("mixed: -clients=%d exceeds -operations=%d (each client needs at least one measured op)", *clients, *operations)
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
	choices := operationTrace(mix)

	// Build the per-client state: disjoint corpus shard, per-client op counts,
	// per-client Zipf key trace, decorrelated choice phase, and a session over
	// the shared handle. updated is shared but each index is written by exactly
	// one client (the owner of its shard), so the final content oracle needs no
	// lock.
	updated := make([]bool, len(docs))
	states := make([]*clientState, *clients)
	choiceStride := len(choices) / *clients // spreads clients across the op cycle so they are not in lockstep
	for id := range states {
		base, span := evenSplit(len(docs), *clients, id)
		_, warmupOps := evenSplit(*warmup, *clients, id)
		_, measuredOps := evenSplit(*operations, *clients, id)
		st := &clientState{
			id:          id,
			session:     engine.Session(id),
			base:        base,
			span:        span,
			choiceOff:   id * choiceStride,
			warmupOps:   warmupOps,
			measuredOps: measuredOps,
			keyTrace: buildKeyTrace(
				base, span, warmupOps+measuredOps,
				baseKeySeed+int64(id)*perClientSeedStride,
				baseProbeSeed+int64(id)*perClientSeedStride,
			),
		}
		// Pre-size each latency array to the exact per-kind count the client will
		// record, so append never reallocates inside the timed loop (zero
		// allocation, matching the harness's discipline). The exact count is
		// known because the client walks a fixed slice of the choices cycle.
		for k := range st.latencies {
			st.latencies[k] = make([]int64, 0, st.measuredKindCount(choices, k))
		}
		states[id] = st
	}

	// A whole-store scan cannot assert an exact document count under concurrent
	// churn: a delete+restore on another client's shard transiently hides a key.
	// At N=1 the count is exact; at N>1 the scan may only be short, never long.
	strictScan := *clients == 1

	runClient := func(st *clientState, coord *checkpointCoordinator, measured bool) {
		count := st.warmupOps
		if measured {
			count = st.measuredOps
		}
		for s := 0; s < count; s++ {
			seq := s
			if measured {
				seq = st.warmupOps + s
			}
			choice := choices[(st.choiceOff+seq)%len(choices)]
			idx := st.keyTrace[seq]
			var elapsed int64
			start := time.Now()
			kind, mutations, err := st.run(docs, updated, strictScan, choice, idx)
			elapsed = time.Since(start).Nanoseconds()
			if err != nil {
				st.err = err
				return
			}
			if measured {
				st.latencies[kind] = append(st.latencies[kind], elapsed)
			}
			if err := coord.add(mutations); err != nil {
				st.err = err
				return
			}
		}
	}

	runPhase := func(coord *checkpointCoordinator, measured bool) {
		var wg sync.WaitGroup
		for _, st := range states {
			wg.Add(1)
			go func(st *clientState) {
				defer wg.Done()
				runClient(st, coord, measured)
			}(st)
		}
		wg.Wait()
		for _, st := range states {
			check(st.err)
			st.err = nil
		}
	}

	// Warmup (unmeasured), partitioned across clients, checkpointed on the same
	// cadence but with latencies discarded.
	warmupCoord := newCheckpointCoordinator(engine, *checkpointMutations, false)
	runPhase(warmupCoord, false)
	// Warmup must not consume the measured loss window or leave stable work
	// queued for the timed operations to inherit.
	check(engine.Checkpoint())
	automaticCheckpointStart := automaticCheckpointCount(engine)

	// Measured phase. total ops/s = total measured ops / wall time; the timer
	// brackets every worker plus the final durability fence.
	measuredCoord := newCheckpointCoordinator(engine, *checkpointMutations, true)
	throughputStart := time.Now()
	runPhase(measuredCoord, true)
	// A finite run must not hide its final durability work after the throughput
	// timer. With -checkpoint-mutations=0 this is the one explicit checkpoint.
	check(measuredCoord.finalFlush())
	measuredNanos := time.Since(throughputStart).Nanoseconds()
	automaticCheckpoints := automaticCheckpointCount(engine) - automaticCheckpointStart

	// Release each session's read state (a cached snapshot or held read
	// transaction) before the final single-threaded oracle and footprint reading.
	for _, st := range states {
		competitive.ReleaseSession(st.session)
	}

	// Merge the per-client latency arrays into one aggregate per operation kind,
	// then summarize. The merge is off the timed path, so its allocation is free.
	latencies := [opKinds][]int64{}
	for k := range latencies {
		total := 0
		for _, st := range states {
			total += len(st.latencies[k])
		}
		if total == 0 {
			continue
		}
		merged := make([]int64, 0, total)
		for _, st := range states {
			merged = append(merged, st.latencies[k]...)
		}
		latencies[k] = merged
	}
	checkpointLatencies := measuredCoord.latencies

	seen := make([]bool, len(docs))
	var expected, submitted []byte
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
			submitted = competitive.AppendSameSizeUpdatedJSON(submitted[:0], docs, ord)
		} else {
			submitted = append(submitted[:0], docs[ord].JSON...)
		}
		expected, err = competitive.AppendExpectedStoredJSON(expected[:0], *engineName, submitted)
		if err != nil {
			return fmt.Errorf("final value canonicalization for %q: %w", key, err)
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
	choices = nil
	states = nil
	latencies = [opKinds][]int64{}
	checkpointLatencies = nil
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
		} else {
			reportName += "/bulk-unified"
		}
	}
	if *header {
		printHeader(os.Stdout)
	}
	throughput := float64(*operations) * float64(time.Second) / float64(measuredNanos)
	printResult := func(operation string, result summary) {
		fmt.Printf("%-20s %-24s %-8s %-4s %7d %9d %7d %10d %9d %7v %7d %-18s %10d %11.3f %11.3f %11.3f %12.0f %10.1f %10.1f %10.1f %11.1f %12.1f\n",
			reportName, engine.DurabilityMode(), mix.name, cardinality,
			*corpusSize, *operations, *warmup, *checkpointMutations,
			automaticCheckpoints, *indexed, *clients, operation, result.calls,
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

// run executes one operation against the client's session, updating the client's
// own scratch and the shard-owned entries of the shared updated slice. It is the
// per-operation body of both the warmup and measured loops.
func (st *clientState) run(docs []competitive.Doc, updated []bool, strictScan bool, choice, idx int) (kind, mutations int, err error) {
	key := docs[idx].Key
	switch choice {
	case opRead:
		out, err := st.session.Get(st.readScratch[:0], key)
		st.readScratch = out
		return opRead, 0, err
	case opUpdate:
		if updated[idx] {
			st.replacement = append(st.replacement[:0], docs[idx].JSON...)
		} else {
			st.replacement = competitive.AppendSameSizeUpdatedJSON(st.replacement[:0], docs, idx)
		}
		err := st.session.Put(key, st.replacement)
		if err == nil {
			updated[idx] = !updated[idx]
		}
		return opUpdate, mutationCount(choice), err
	case opReadModifyWrite:
		out, err := st.session.Get(st.readScratch[:0], key)
		st.readScratch = out
		if err != nil {
			return opReadModifyWrite, 0, err
		}
		if updated[idx] {
			st.replacement = append(st.replacement[:0], docs[idx].JSON...)
		} else {
			st.replacement = competitive.AppendSameSizeUpdatedJSON(st.replacement[:0], docs, idx)
		}
		err = st.session.Put(key, st.replacement)
		if err == nil {
			updated[idx] = !updated[idx]
		}
		return opReadModifyWrite, mutationCount(choice), err
	case opChurn:
		if err := st.session.Delete(key); err != nil {
			return opChurn, 0, err
		}
		err := st.session.Upsert(key, docs[idx].JSON)
		if err == nil {
			updated[idx] = false
		}
		return opChurn, mutationCount(choice), err
	case opScan:
		n, err := st.session.ScanAllBytes()
		if err == nil {
			if strictScan && n != len(docs) {
				err = fmt.Errorf("ordered scan visited %d documents, want %d", n, len(docs))
			} else if !strictScan && n > len(docs) {
				err = fmt.Errorf("ordered scan visited %d documents, want <= %d", n, len(docs))
			}
		}
		return opScan, 0, err
	default:
		return 0, 0, fmt.Errorf("unknown mixed operation %d", choice)
	}
}

// measuredKindCount is the exact number of operations of one kind this client
// will record in the timed loop, computed by walking its fixed choice slice. It
// sizes the latency array so the hot loop never reallocates.
func (st *clientState) measuredKindCount(choices []int, kind int) int {
	n := 0
	for s := 0; s < st.measuredOps; s++ {
		if choices[(st.choiceOff+st.warmupOps+s)%len(choices)] == kind {
			n++
		}
	}
	return n
}

func printHeader(w io.Writer) {
	fmt.Fprintf(w, "%-20s %-24s %-8s %-4s %7s %9s %7s %10s %9s %7s %7s %-18s %10s %11s %11s %11s %12s %10s %10s %10s %11s %12s\n",
		"engine", "durability", "workload", "card", "docs", "measured",
		"warmup", "checkpoint", "forced-cp", "indexed", "clients", "operation", "calls",
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

// checkpointCoordinator is the run's single checkpointer. The scheduled
// checkpoint every N mutations must stay exactly one caller for cross-client
// comparability — the engine's checkpoint is a stop-the-world exclusive gate, so
// concurrent callers would serialize anyway — so a mutex both counts global
// mutations (via the tested checkpointSchedule) and guards the one Checkpoint
// call. Reads (mutations==0) never touch the lock. Holding the lock across the
// Checkpoint is deliberate: it stalls other mutators exactly as the engine's own
// gate would, and guarantees no second concurrent checkpoint.
type checkpointCoordinator struct {
	engine competitive.Engine
	record bool

	mu        sync.Mutex
	schedule  checkpointSchedule // guarded by mu
	latencies []int64            // guarded by mu
}

func newCheckpointCoordinator(engine competitive.Engine, every int, record bool) *checkpointCoordinator {
	return &checkpointCoordinator{
		engine:   engine,
		record:   record,
		schedule: checkpointSchedule{every: every},
	}
}

func (c *checkpointCoordinator) add(mutations int) error {
	if mutations == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.schedule.Add(mutations) {
		return nil
	}
	err := c.checkpointLocked()
	c.schedule.Mark()
	return err
}

// finalFlush performs the trailing checkpoint if any mutation remains unfenced,
// matching the single-client "checkpoint once after all measured mutations"
// contract. It runs after every worker has joined.
func (c *checkpointCoordinator) finalFlush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schedule.Pending() == 0 {
		return nil
	}
	err := c.checkpointLocked()
	c.schedule.Mark()
	return err
}

func (c *checkpointCoordinator) checkpointLocked() error {
	var start time.Time
	if c.record {
		start = time.Now()
	}
	err := c.engine.Checkpoint()
	if c.record {
		c.latencies = append(c.latencies, time.Since(start).Nanoseconds())
	}
	return err
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
