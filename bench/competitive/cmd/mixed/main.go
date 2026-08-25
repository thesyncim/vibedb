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
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	"github.com/thesyncim/vibedb/bench/competitive/cmd/internal/mixedtelemetry"
	"github.com/thesyncim/vibedb/store/durable"
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
	exactIndexes := flag.Int("exact-indexes", 0, "number of simultaneous exact indexes (0-3)")
	documentShape := flag.String("document-shape", "inline", "inline, mixed, or overflow-heavy")
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
	if *exactIndexes < 0 || *exactIndexes > int(competitive.MaximumExactIndexes) {
		fail("mixed: -exact-indexes must be in [0,%d]", competitive.MaximumExactIndexes)
	}
	if *exactIndexes != 0 && !competitive.IndexCapable(factory.Name) {
		fail("mixed: %s has no native secondary index", factory.Name)
	}
	cardinality, err := competitive.ParseCardinality(*card)
	check(err)
	shape, err := competitive.ParseDocumentShape(*documentShape)
	check(err)
	durability, err := competitive.ParseDurabilityMode(*durabilityName)
	check(err)

	docs := competitive.CorpusOfShape(*corpusSize, cardinality, shape)
	dir, err := os.MkdirTemp("", "vibebench-mixed-")
	check(err)
	defer os.RemoveAll(dir)
	engine, err := factory.New(competitive.Config{
		Dir:              dir,
		Durability:       durability,
		ExactIndexes:     uint8(*exactIndexes),
		MaxDocumentBytes: shape.MaxDocumentBytes(),
		CacheBytes:       competitive.DefaultCacheBytes,
		PutLoop:          *putloop,
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
	logicalMutationBytes := measuredLogicalMutationBytes(states, choices, docs)

	// A whole-store scan cannot assert an exact document count under concurrent
	// churn: each other client can transiently hide at most one key between its
	// delete and restore. A client's own operations are serial, so the one-client
	// case remains exact.
	minimumScanDocuments := len(docs)
	if *clients > 1 {
		minimumScanDocuments -= *clients - 1
	}

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
			mutations := mutationCount(choice)
			start := time.Now()
			kind, actualMutations, err := st.run(
				docs, updated, minimumScanDocuments, coord, choice, idx,
			)
			elapsed := time.Since(start).Nanoseconds()
			if err == nil && actualMutations != mutations {
				err = fmt.Errorf(
					"operation %s reported %d mutations after reserving %d",
					opNames[choice], actualMutations, mutations,
				)
			}
			if err != nil {
				st.err = err
				return
			}
			if measured {
				st.latencies[kind] = append(st.latencies[kind], elapsed)
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
	diagnosticStats := os.Getenv("VIBEDB_MIXED_INTERNAL_STATS") != ""
	var (
		runtimeBefore runtime.MemStats
		durableBefore durable.Stats
		durableOK     bool
	)
	durableBefore, durableOK = durableStats(engine)
	if diagnosticStats {
		runtime.ReadMemStats(&runtimeBefore)
	}

	// Measured phase. total ops/s = total measured ops / wall time; the timer
	// brackets every worker plus the final durability fence. Per-operation
	// latency starts before mutation admission and ends after any checkpoint the
	// operation was elected to perform, so it describes acknowledgement latency
	// rather than only the engine call hidden inside that acknowledgement.
	measuredCoord := newCheckpointCoordinator(engine, *checkpointMutations, true)
	throughputStart := time.Now()
	runPhase(measuredCoord, true)
	// A finite run must not hide its final durability work after the throughput
	// timer. With -checkpoint-mutations=0 this is the one explicit checkpoint.
	check(measuredCoord.finalFlush())
	measuredNanos := time.Since(throughputStart).Nanoseconds()
	automaticCheckpoints := automaticCheckpointCount(engine) - automaticCheckpointStart
	var (
		runtimeAfter runtime.MemStats
		durableAfter durable.Stats
	)
	if durableOK {
		durableAfter, durableOK = durableStats(engine)
	}
	if diagnosticStats {
		runtime.ReadMemStats(&runtimeAfter)
	}

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
	validateFinal := func(key string, ord int, value []byte) error {
		var err error
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
	}
	if shape != competitive.InlineDocuments && *engineName == "vibedb" {
		// VibeDB's current full-scan reconstruction is independently qualified
		// for inline rows. Overflow reconstruction is not yet a benchmark
		// contract, so keep the untimed final oracle on exact point reads instead
		// of silently certifying malformed scan bytes.
		var point []byte
		for ord := range docs {
			point, err = engine.Get(point[:0], docs[ord].Key)
			check(err)
			check(validateFinal(docs[ord].Key, ord, point))
		}
	} else {
		check(engine.Visit(func(key string, value []byte) error {
			const prefix = "doc:"
			if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
				return fmt.Errorf("malformed final key %q", key)
			}
			ord, err := strconv.Atoi(key[len(prefix):])
			if err != nil || ord < 0 || ord >= len(docs) {
				return fmt.Errorf("malformed final key %q", key)
			}
			return validateFinal(key, ord, value)
		}))
	}
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
		calls               int
		p50, p95, p99, p999 int64
		max                 int64
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
			p999:  percentile(samples, 0.999),
			max:   samples[len(samples)-1],
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
			p999:  percentile(checkpointLatencies, 0.999),
			max:   checkpointLatencies[len(checkpointLatencies)-1],
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
	if factory.Name == "vibedb" {
		if *putloop {
			reportName += "/put"
		} else if shape != competitive.InlineDocuments {
			reportName += "/put-overflow"
		} else {
			reportName += "/bulk-unified"
		}
	}
	if *header {
		printHeader(os.Stdout)
	}
	throughput := float64(*operations) * float64(time.Second) / float64(measuredNanos)
	payloadDelta, payloadMonotonic := counterDeltaKnown(
		durableBefore.DeviceBytes, durableAfter.DeviceBytes,
	)
	payloadKnown := durableOK && payloadMonotonic &&
		(engine.DurabilityMode() == competitive.DurabilityOrdinarySync ||
			engine.DurabilityMode() == competitive.DurabilityPowerSafe)
	durabilityPayloadBytes := uint64(0)
	payloadRatio := 0.0
	if payloadKnown {
		durabilityPayloadBytes = payloadDelta
		if logicalMutationBytes != 0 {
			payloadRatio = float64(durabilityPayloadBytes) / float64(logicalMutationBytes)
		}
	}
	printResult := func(operation string, result summary) {
		fmt.Printf("%-20s %-24s %-8s %-4s %-14s %7d %9d %7d %10d %9d %13d %7d %-18s %10d %11.3f %11.3f %11.3f %11.3f %11.3f %12.0f %10.1f %10.1f %10.1f %11.1f %12.1f %11t %14d %14d %13.4f\n",
			reportName, engine.DurabilityMode(), mix.name, cardinality, shape,
			*corpusSize, *operations, *warmup, *checkpointMutations,
			automaticCheckpoints, *exactIndexes, *clients, operation, result.calls,
			micros(result.p50), micros(result.p95), micros(result.p99), micros(result.p999), micros(result.max), throughput,
			mib(fp.DiskBytes), mib(fp.DiskAllocatedBytes), mib(int64(fp.HeapAlloc)),
			mib(int64(fp.RuntimeResident)), mib(fp.MaxRSSBytes()), payloadKnown,
			logicalMutationBytes, durabilityPayloadBytes, payloadRatio)
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
	if diagnosticStats {
		telemetry := buildTelemetryRecord(
			factory.Name, *clients,
			runtimeBefore, runtimeAfter,
			durableBefore, durableAfter, durableOK,
		)
		check(mixedtelemetry.Write(os.Stderr, telemetry))
		fmt.Fprintf(
			os.Stderr,
			"mixed-runtime engine=%s clients=%d total-alloc-bytes=%d mallocs=%d\n",
			factory.Name, *clients,
			runtimeAfter.TotalAlloc-runtimeBefore.TotalAlloc,
			runtimeAfter.Mallocs-runtimeBefore.Mallocs,
		)
		if durableOK {
			fmt.Fprintf(
				os.Stderr,
				"vibedb-internal clients=%d overlay-folds=%d pressure-fallbacks=%d fast-replaces=%d publish-groups=%d automatic-checkpoints=%d journal-delta-checkpoints=%d journal-delta-records=%d journal-delta-bytes=%d journal-full-fallbacks=%d durability-payload-known=%t durability-payload-bytes=%d leaf-splits=%d empty-reclaims=%d\n",
				*clients,
				durableAfter.PrimaryOverlayFolds-durableBefore.PrimaryOverlayFolds,
				durableAfter.ConcurrentPrimaryFallbacks-durableBefore.ConcurrentPrimaryFallbacks,
				durableAfter.ConcurrentPrimaryReplaces-durableBefore.ConcurrentPrimaryReplaces,
				durableAfter.ConcurrentPrimaryPublishGroups-durableBefore.ConcurrentPrimaryPublishGroups,
				durableAfter.AutomaticCheckpoints-durableBefore.AutomaticCheckpoints,
				durableAfter.JournalDeltaCheckpoints-durableBefore.JournalDeltaCheckpoints,
				durableAfter.JournalDeltaRecords-durableBefore.JournalDeltaRecords,
				durableAfter.JournalDeltaBytes-durableBefore.JournalDeltaBytes,
				durableAfter.JournalDeltaFullFallbacks-durableBefore.JournalDeltaFullFallbacks,
				payloadMonotonic, payloadDelta,
				durableAfter.PrimaryLeafSplits-durableBefore.PrimaryLeafSplits,
				durableAfter.PrimaryEmptyReclaims-durableBefore.PrimaryEmptyReclaims,
			)
		}
	}
}

// run executes one operation against the client's session, updating the client's
// own scratch and the shard-owned entries of the shared updated slice. It is the
// per-operation body of both the warmup and measured loops.
func (st *clientState) run(
	docs []competitive.Doc,
	updated []bool,
	minimumScanDocuments int,
	coord *checkpointCoordinator,
	choice, idx int,
) (kind, mutations int, err error) {
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
		admission, err := coord.admit(1)
		if err != nil {
			return opUpdate, 0, err
		}
		operationErr := st.session.Put(key, st.replacement)
		err = completeMutation(coord, admission, operationErr)
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
		admission, err := coord.admit(1)
		if err != nil {
			return opReadModifyWrite, 0, err
		}
		operationErr := st.session.Put(key, st.replacement)
		err = completeMutation(coord, admission, operationErr)
		if err == nil {
			updated[idx] = !updated[idx]
		}
		return opReadModifyWrite, mutationCount(choice), err
	case opChurn:
		deleteAdmission, err := coord.admit(1)
		if err != nil {
			return opChurn, 0, err
		}
		deleteErr := st.session.Delete(key)
		if err := completeMutation(coord, deleteAdmission, deleteErr); err != nil {
			return opChurn, 1, err
		}
		restoreAdmission, err := coord.admit(1)
		if err != nil {
			return opChurn, 1, err
		}
		restoreErr := st.session.Upsert(key, docs[idx].JSON)
		err = completeMutation(coord, restoreAdmission, restoreErr)
		if err == nil {
			updated[idx] = false
		}
		return opChurn, mutationCount(choice), err
	case opScan:
		n, err := st.session.ScanAllBytes()
		if err == nil && (n < minimumScanDocuments || n > len(docs)) {
			err = fmt.Errorf(
				"ordered scan visited %d documents, want [%d,%d]",
				n, minimumScanDocuments, len(docs),
			)
		}
		return opScan, 0, err
	default:
		return 0, 0, fmt.Errorf("unknown mixed operation %d", choice)
	}
}

// completeMutation releases exactly one state-change admission after its
// engine call. The common success path returns one of the existing errors
// directly; errors.Join is reserved for the exceptional case where both the
// engine mutation and its elected checkpoint fail.
func completeMutation(
	coord *checkpointCoordinator,
	admission mutationAdmission,
	operationErr error,
) error {
	checkpointErr := coord.complete(admission)
	if operationErr == nil {
		return checkpointErr
	}
	if checkpointErr == nil {
		return operationErr
	}
	return errors.Join(operationErr, checkpointErr)
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
	// Keep the established latency column names for mixedsuite compatibility.
	// Their samples are end-to-end acknowledgement latencies: mutation admission,
	// the engine call, and any elected checkpoint are all inside the timer.
	fmt.Fprintf(w, "%-20s %-24s %-8s %-4s %-14s %7s %9s %7s %10s %9s %13s %7s %-18s %10s %11s %11s %11s %11s %11s %12s %10s %10s %10s %11s %12s %11s %14s %14s %13s\n",
		"engine", "durability", "workload", "card", "document-shape", "docs", "measured",
		"warmup", "checkpoint", "forced-cp", "exact-indexes", "clients", "operation", "calls",
		"p50-us", "p95-us", "p99-us", "p99.9-us", "max-us", "total-ops/s", "disk-MiB", "alloc-MiB",
		"heap-MiB", "runtime-MiB", "peak-rss-MiB", "durability-payload-known",
		"logical-write-B", "durability-payload-B", "durability-payload/logical")
}

// measuredLogicalMutationBytes is the byte-exact denominator for the engine's
// issued durability-payload ratio. It counts submitted key and value bytes for successful
// measured mutations; same-size updates make the result independent of each
// key's current toggle state. Checkpoint and journal metadata are deliberately
// excluded from the denominator and remain charged in the issued-payload numerator.
func measuredLogicalMutationBytes(states []*clientState, choices []int, docs []competitive.Doc) uint64 {
	var total uint64
	for _, state := range states {
		for operation := 0; operation < state.measuredOps; operation++ {
			sequence := state.warmupOps + operation
			choice := choices[(state.choiceOff+sequence)%len(choices)]
			doc := docs[state.keyTrace[sequence]]
			switch choice {
			case opUpdate, opReadModifyWrite:
				total += uint64(len(doc.Key) + len(doc.JSON))
			case opChurn:
				total += uint64(2*len(doc.Key) + len(doc.JSON))
			}
		}
	}
	return total
}

type automaticCheckpointReporter interface {
	AutomaticCheckpoints() uint64
}

type durableStatsReporter interface {
	DurableStats() durable.Stats
}

func durableStats(engine competitive.Engine) (durable.Stats, bool) {
	reporter, ok := engine.(durableStatsReporter)
	if !ok {
		return durable.Stats{}, false
	}
	return reporter.DurableStats(), true
}

// buildTelemetryRecord turns the two point-in-time snapshots into a versioned
// transport record. Monotonic counters are deltas over the measured phase.
// Largest-group fields are high-waters rather than counters, so retaining both
// samples avoids the false precision of subtracting them.
func buildTelemetryRecord(
	engine string,
	clients int,
	runtimeBefore, runtimeAfter runtime.MemStats,
	durableBefore, durableAfter durable.Stats,
	durableOK bool,
) mixedtelemetry.Record {
	record := mixedtelemetry.Record{
		Engine:                 engine,
		Clients:                clients,
		Available:              durableOK,
		RuntimeTotalAllocBytes: counterDelta(runtimeBefore.TotalAlloc, runtimeAfter.TotalAlloc),
		RuntimeMallocs:         counterDelta(runtimeBefore.Mallocs, runtimeAfter.Mallocs),
	}
	if !durableOK {
		return record
	}
	record.ScalarPatchAttempts = counterDelta(
		durableBefore.ConcurrentPrimaryScalarPatchAttempts,
		durableAfter.ConcurrentPrimaryScalarPatchAttempts,
	)
	record.ScalarPatchAccepts = counterDelta(
		durableBefore.ConcurrentPrimaryScalarPatches,
		durableAfter.ConcurrentPrimaryScalarPatches,
	)
	record.CompactPatchAttempts = counterDelta(
		durableBefore.PrimaryCompactColumnPatchAttempts,
		durableAfter.PrimaryCompactColumnPatchAttempts,
	)
	record.CompactPatchAccepts = counterDelta(
		durableBefore.PrimaryCompactColumnPatches,
		durableAfter.PrimaryCompactColumnPatches,
	)
	record.OverlayFolds = counterDelta(
		durableBefore.PrimaryOverlayFolds,
		durableAfter.PrimaryOverlayFolds,
	)
	record.OverlayFoldAttempts = counterDelta(
		durableBefore.PrimaryOverlayMaterializationAttempts,
		durableAfter.PrimaryOverlayMaterializationAttempts,
	)
	record.OverlayMaterializations = counterDelta(
		durableBefore.PrimaryOverlayMaterializations,
		durableAfter.PrimaryOverlayMaterializations,
	)
	record.OverlayMaterializationFailures = counterDelta(
		durableBefore.PrimaryOverlayMaterializationFailures,
		durableAfter.PrimaryOverlayMaterializationFailures,
	)
	record.OverlayPressureFolds = counterDelta(
		durableBefore.PrimaryOverlayPressureFolds,
		durableAfter.PrimaryOverlayPressureFolds,
	)
	record.OverlaySnapshotFolds = counterDelta(
		durableBefore.PrimaryOverlaySnapshotFolds,
		durableAfter.PrimaryOverlaySnapshotFolds,
	)
	record.OverlayBarrierFolds = counterDelta(
		durableBefore.PrimaryOverlayBarrierFolds,
		durableAfter.PrimaryOverlayBarrierFolds,
	)
	record.OverlayCheckpointFolds = counterDelta(
		durableBefore.PrimaryOverlayCheckpointFolds,
		durableAfter.PrimaryOverlayCheckpointFolds,
	)
	record.ConcurrentReplaces = counterDelta(
		durableBefore.ConcurrentPrimaryReplaces,
		durableAfter.ConcurrentPrimaryReplaces,
	)
	record.ConcurrentFallbacks = counterDelta(
		durableBefore.ConcurrentPrimaryFallbacks,
		durableAfter.ConcurrentPrimaryFallbacks,
	)
	record.PublishGroups = counterDelta(
		durableBefore.ConcurrentPrimaryPublishGroups,
		durableAfter.ConcurrentPrimaryPublishGroups,
	)
	record.PublishGroupMaxBefore = durableBefore.ConcurrentPrimaryLargestPublishGroup
	record.PublishGroupMax = durableAfter.ConcurrentPrimaryLargestPublishGroup
	record.AutomaticCheckpoints = counterDelta(
		durableBefore.AutomaticCheckpoints,
		durableAfter.AutomaticCheckpoints,
	)
	record.OverlayArenaBytesBefore = durableBefore.PrimaryOverlayArenaBytes
	record.OverlayArenaBytes = durableAfter.PrimaryOverlayArenaBytes
	record.OverlayRetainedRecordsBefore = durableBefore.PrimaryOverlayRetainedRecords
	record.OverlayRetainedRecords = durableAfter.PrimaryOverlayRetainedRecords
	record.OverlayDirtyBucketsBefore = durableBefore.PrimaryOverlayDirtyBuckets
	record.OverlayDirtyBuckets = durableAfter.PrimaryOverlayDirtyBuckets
	record.OverlayReservedFoldBytesBefore = durableBefore.PrimaryOverlayReservedFoldBytes
	record.OverlayReservedFoldBytes = durableAfter.PrimaryOverlayReservedFoldBytes
	record.OverlayDirtyBucketLimit = durableAfter.PrimaryOverlayDirtyBucketLimit
	record.OverlayDirtyByteLimit = durableAfter.PrimaryOverlayDirtyByteLimit
	record.JournalAcks = counterDelta(durableBefore.JournalAcks, durableAfter.JournalAcks)
	record.ChainAcks = counterDelta(durableBefore.ChainAcks, durableAfter.ChainAcks)
	record.JournalSyncs = counterDelta(durableBefore.JournalSyncs, durableAfter.JournalSyncs)
	record.JournalGroupMaxBefore = uint64(durableBefore.JournalLargestGroup)
	record.JournalGroupMax = uint64(durableAfter.JournalLargestGroup)
	record.JournalStrictSyncs = counterDelta(
		durableBefore.JournalStrictSyncs,
		durableAfter.JournalStrictSyncs,
	)
	record.JournalStrictRecords = counterDelta(
		durableBefore.JournalStrictRecords,
		durableAfter.JournalStrictRecords,
	)
	record.JournalStrictMutations = counterDelta(
		durableBefore.JournalStrictMutations,
		durableAfter.JournalStrictMutations,
	)
	record.JournalStrictBytes = counterDelta(
		durableBefore.JournalStrictBytes,
		durableAfter.JournalStrictBytes,
	)
	record.JournalDeltaCheckpoints = counterDelta(
		durableBefore.JournalDeltaCheckpoints,
		durableAfter.JournalDeltaCheckpoints,
	)
	record.JournalDeltaRecords = counterDelta(
		durableBefore.JournalDeltaRecords,
		durableAfter.JournalDeltaRecords,
	)
	record.JournalDeltaBytes = counterDelta(
		durableBefore.JournalDeltaBytes,
		durableAfter.JournalDeltaBytes,
	)
	record.JournalDeltaFallbacks = counterDelta(
		durableBefore.JournalDeltaFullFallbacks,
		durableAfter.JournalDeltaFullFallbacks,
	)
	record.DurabilityPayloadBytes, record.DurabilityPayloadKnown = counterDeltaKnown(
		durableBefore.DeviceBytes, durableAfter.DeviceBytes,
	)
	record.LeafSplits = counterDelta(
		durableBefore.PrimaryLeafSplits,
		durableAfter.PrimaryLeafSplits,
	)
	record.EmptyReclaims = counterDelta(
		durableBefore.PrimaryEmptyReclaims,
		durableAfter.PrimaryEmptyReclaims,
	)
	record.Histograms = map[string]mixedtelemetry.Histogram{
		"primary-overlay-fold-ns": histogramDelta(
			durableBefore.PrimaryOverlayFoldNS,
			durableAfter.PrimaryOverlayFoldNS,
		),
		"concurrent-stripe-wait-ns": histogramDelta(
			durableBefore.ConcurrentPrimaryStripeWaitNS,
			durableAfter.ConcurrentPrimaryStripeWaitNS,
		),
		"concurrent-publish-group-size": histogramDelta(
			durableBefore.ConcurrentPrimaryPublishGroupSize,
			durableAfter.ConcurrentPrimaryPublishGroupSize,
		),
		"journal-group-records": histogramDelta(
			durableBefore.JournalGroupRecords,
			durableAfter.JournalGroupRecords,
		),
		"journal-group-mutations": histogramDelta(
			durableBefore.JournalGroupMutations,
			durableAfter.JournalGroupMutations,
		),
		"journal-group-bytes": histogramDelta(
			durableBefore.JournalGroupBytes,
			durableAfter.JournalGroupBytes,
		),
		"journal-group-sync-ns": histogramDelta(
			durableBefore.JournalGroupSyncNS,
			durableAfter.JournalGroupSyncNS,
		),
		"journal-strict-sync-ns": histogramDelta(
			durableBefore.JournalStrictSyncNS,
			durableAfter.JournalStrictSyncNS,
		),
		"journal-delta-batch-records": histogramDelta(
			durableBefore.JournalDeltaBatchRecords,
			durableAfter.JournalDeltaBatchRecords,
		),
		"journal-delta-batch-bytes": histogramDelta(
			durableBefore.JournalDeltaBatchBytes,
			durableAfter.JournalDeltaBatchBytes,
		),
		"journal-delta-sync-ns": histogramDelta(
			durableBefore.JournalDeltaSyncNS,
			durableAfter.JournalDeltaSyncNS,
		),
	}
	return record
}

func histogramDelta(
	before, after durable.StatsHistogram,
) mixedtelemetry.Histogram {
	buckets := make([]uint64, durable.StatsHistogramBuckets)
	for index := range buckets {
		buckets[index] = counterDelta(before.Buckets[index], after.Buckets[index])
	}
	return mixedtelemetry.Histogram{
		Count:     counterDelta(before.Count, after.Count),
		Sum:       counterDelta(before.Sum, after.Sum),
		MaxBefore: before.Max,
		Max:       after.Max,
		Buckets:   buckets,
	}
}

// counterDelta fails closed if a future reporter resets a counter between
// snapshots; wrapping uint64 arithmetic would manufacture a huge metric.
func counterDelta(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func counterDeltaKnown(before, after uint64) (uint64, bool) {
	if after < before {
		return 0, false
	}
	return after - before, true
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

// checkpointer is the narrow part of competitive.Engine the epoch coordinator
// needs. Keeping it narrow makes the concurrency protocol independently
// testable without a benchmark engine fixture.
type checkpointer interface{ Checkpoint() error }

// mutationAdmission is one state-change reservation in a durability epoch.
// coordinated is false when checkpointing is disabled or for a read, keeping
// both paths off the epoch atomic state.
type mutationAdmission struct {
	coordinated bool
	epoch       uint64
	mutations   int
}

const checkpointCountShift = 32

func packCheckpointCounts(admitted, active uint32) uint64 {
	return uint64(admitted)<<checkpointCountShift | uint64(active)
}

func unpackCheckpointCounts(state uint64) (admitted, active uint32) {
	return uint32(state >> checkpointCountShift), uint32(state)
}

type checkpointFailure struct{ err error }

// checkpointCoordinator is the run's single durability-epoch coordinator.
// Immediately before each engine mutation starts, admit reserves one state
// change in the current epoch. Once the configured budget is full, no mutation
// in the next epoch may enter until every active mutation in this one has
// returned and the elected last completer has checkpointed it. This establishes
// an exact cut for every workload, instead of allowing N-1 already-acknowledged
// writes to race across a checkpoint while waiting to update an after-the-fact
// counter.
// Delete+restore acquires two consecutive one-change admissions, so a checkpoint
// may correctly fall between its two independent engine mutations. Reads never
// touch the coordinator and can proceed during a checkpoint.
type checkpointCoordinator struct {
	engine checkpointer
	record bool

	// state packs admitted in the high word and active in the low word. Ordinary
	// admission/completion changes only this word; the transition mutex is used
	// solely by callers waiting on a full epoch and by the elected checkpoint
	// reset. A full, drained epoch remains (every,0) until Checkpoint returns.
	state atomic.Uint64
	epoch atomic.Uint64
	every uint32

	mu      sync.Mutex
	cond    *sync.Cond
	waiters atomic.Int32
	failure atomic.Pointer[checkpointFailure]

	// latencies has one writer at a time under mu. The benchmark reads it only
	// after all clients join.
	latencies []int64

	// pendingFinal is used only by checkpoint-mutations=0. It preserves the
	// original final-only contract without putting its hot mutation path through
	// epoch admission.
	pendingFinal atomic.Uint64
}

func newCheckpointCoordinator(engine checkpointer, every int, record bool) *checkpointCoordinator {
	c := &checkpointCoordinator{
		engine: engine, record: record,
	}
	c.cond = sync.NewCond(&c.mu)
	if every < 0 || uint64(every) > uint64(^uint32(0)) {
		c.failure.Store(&checkpointFailure{err: fmt.Errorf(
			"checkpoint epoch size %d exceeds packed counter capacity", every,
		)})
	} else {
		c.every = uint32(every)
	}
	return c
}

func (c *checkpointCoordinator) failureErr() error {
	if failure := c.failure.Load(); failure != nil {
		return failure.err
	}
	return nil
}

// failLocked publishes the first terminal coordinator error and wakes every
// full-epoch waiter. c.mu must be held.
func (c *checkpointCoordinator) failLocked(err error) error {
	if err == nil {
		return nil
	}
	failure := &checkpointFailure{err: err}
	if !c.failure.CompareAndSwap(nil, failure) {
		err = c.failure.Load().err
	}
	c.cond.Broadcast()
	return err
}

func (c *checkpointCoordinator) admit(mutations int) (mutationAdmission, error) {
	if mutations == 0 {
		return mutationAdmission{}, nil
	}
	if mutations != 1 {
		return mutationAdmission{}, fmt.Errorf(
			"checkpoint epoch requires one state change per admission, got %d",
			mutations,
		)
	}
	if c.every == 0 {
		if err := c.failureErr(); err != nil {
			return mutationAdmission{}, err
		}
		return mutationAdmission{mutations: mutations}, nil
	}
	// Do not barge past callers already queued at a full epoch. The zero-waiter
	// path below is the ordinary allocation-free pair of atomic operations.
	if c.waiters.Load() == 0 {
		if admission, admitted := c.tryAdmit(); admitted {
			return admission, nil
		}
	}
	return c.waitAndAdmit()
}

// tryAdmit reserves one mutation when the current epoch still has capacity.
// The successful active increment pins the epoch until this admission is
// completed, so reading epoch after the CAS cannot cross a checkpoint reset.
func (c *checkpointCoordinator) tryAdmit() (mutationAdmission, bool) {
	for {
		state := c.state.Load()
		admitted, active := unpackCheckpointCounts(state)
		if admitted >= c.every {
			return mutationAdmission{}, false
		}
		next := packCheckpointCounts(admitted+1, active+1)
		if !c.state.CompareAndSwap(state, next) {
			continue
		}
		return mutationAdmission{
			coordinated: true,
			epoch:       c.epoch.Load(),
			mutations:   1,
		}, true
	}
}

// waitAndAdmit is entered only after an epoch fills or when an older waiter is
// already queued. The waiter count closes the barging gate before taking mu;
// after a reset, broadcast waiters fill the new epoch before fresh fast-path
// callers can bypass them.
func (c *checkpointCoordinator) waitAndAdmit() (
	mutationAdmission, error,
) {
	c.waiters.Add(1)
	defer c.waiters.Add(-1)
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if err := c.failureErr(); err != nil {
			return mutationAdmission{}, err
		}
		admission, admitted := c.tryAdmit()
		if admitted {
			return admission, nil
		}
		c.cond.Wait()
	}
}

// complete releases one admitted mutation. The last active mutation in a full
// epoch is elected to checkpoint; its caller does not return, and the next
// epoch is not admitted, until the checkpoint finishes.
func (c *checkpointCoordinator) complete(admission mutationAdmission) error {
	if admission.mutations == 0 {
		return nil
	}
	if !admission.coordinated {
		c.pendingFinal.Add(uint64(admission.mutations))
		return nil
	}
	currentEpoch := c.epoch.Load()
	for {
		state := c.state.Load()
		admitted, active := unpackCheckpointCounts(state)
		if admission.epoch != currentEpoch || active == 0 {
			return fmt.Errorf(
				"checkpoint epoch completion out of order: admission=%d current=%d active=%d",
				admission.epoch, currentEpoch, active,
			)
		}
		next := packCheckpointCounts(admitted, active-1)
		if !c.state.CompareAndSwap(state, next) {
			continue
		}
		if active == 1 && admitted == c.every {
			return c.checkpointFullEpoch(admission.epoch)
		}
		return nil
	}
}

// checkpointFullEpoch is called only by the CAS that changes (every,1) to
// (every,0). The closed packed state excludes next-epoch admission until the
// engine checkpoint returns and resetLocked publishes epoch+1 followed by 0/0.
func (c *checkpointCoordinator) checkpointFullEpoch(epoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkpointStateLocked(epoch, c.every)
}

// checkpointStateLocked validates and checkpoints one fully drained state.
// For a trailing partial epoch it first atomically closes admission by changing
// (admitted,0) to (every,0). c.mu must be held.
func (c *checkpointCoordinator) checkpointStateLocked(
	epoch uint64, admitted uint32,
) error {
	state := c.state.Load()
	stateAdmitted, active := unpackCheckpointCounts(state)
	if epoch != c.epoch.Load() || admitted == 0 ||
		stateAdmitted != admitted || active != 0 {
		return c.failLocked(fmt.Errorf(
			"invalid checkpoint epoch state: admission=%d current=%d admitted=%d/%d active=%d",
			epoch, c.epoch.Load(), admitted, stateAdmitted, active,
		))
	}
	if admitted < c.every && !c.state.CompareAndSwap(
		state, packCheckpointCounts(c.every, 0),
	) {
		return c.failLocked(fmt.Errorf(
			"checkpoint trailing epoch changed during close",
		))
	}
	return c.checkpointAndResetLocked(epoch)
}

// checkpointAndResetLocked keeps the transition mutex only around the one
// engine fence and reset. The per-mutation path never takes it.
func (c *checkpointCoordinator) checkpointAndResetLocked(epoch uint64) error {
	var start time.Time
	if c.record {
		start = time.Now()
	}
	err := c.engine.Checkpoint()
	if c.record {
		c.latencies = append(c.latencies, time.Since(start).Nanoseconds())
	}
	if err != nil {
		return c.failLocked(err)
	}
	c.epoch.Store(epoch + 1)
	c.state.Store(0)
	c.cond.Broadcast()
	return nil
}

// finalFlush performs the trailing checkpoint if any mutation remains unfenced,
// matching the single-client "checkpoint once after all measured mutations"
// contract. It runs after every worker has joined.
func (c *checkpointCoordinator) finalFlush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.failureErr(); err != nil {
		return err
	}
	if c.every == 0 {
		if c.pendingFinal.Swap(0) == 0 {
			return nil
		}
		var start time.Time
		if c.record {
			start = time.Now()
		}
		err := c.engine.Checkpoint()
		if c.record {
			c.latencies = append(c.latencies, time.Since(start).Nanoseconds())
		}
		return c.failLocked(err)
	}
	state := c.state.Load()
	admitted, active := unpackCheckpointCounts(state)
	if active != 0 {
		return c.failLocked(fmt.Errorf(
			"checkpoint final flush has %d active mutations", active,
		))
	}
	if admitted == 0 {
		return nil
	}
	return c.checkpointStateLocked(c.epoch.Load(), admitted)
}

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
