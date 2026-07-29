package competitive

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestPhase0Diag attributes the buffered churn/update per-mutation CPU on the
// exact harness shapes: bulk-load the low-cardinality 10k corpus, then replay
// the mixed churn trace (Zipfian keys, checkpoint every 64 mutations) under a
// CPU profile while reading the template de-template / merge-eval counters. It
// is a diagnostic, run explicitly with -run TestPhase0Diag, never in CI.
func TestPhase0Diag(t *testing.T) {
	if os.Getenv("PHASE0") == "" {
		t.Skip("set PHASE0=1 to run the phase-0 diagnostic")
	}
	const (
		corpusSize          = 10_000
		operations          = 60_000
		warmup              = 10_000
		checkpointMutations = 64
	)
	docs := CorpusOf(corpusSize, LowCardinality)

	opts := durable.Options{
		ResidentBytes:      DefaultCacheBytes,
		Durability:         durable.DurabilityBufferedVisible,
		Backend:            durable.BackendPortable,
		CheckpointStrength: durable.CheckpointFilesystem,
		MaxBatchDocuments:  1,
		MaxDocumentBytes:   1 << 10,
		BufferCount:        1024,
		QueueSlots:         1024,
		GroupLimit:         64,
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "vibejson.db")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range docs {
		if err := builder.Append(docs[i].Key, docs[i].JSON); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	// The chunk builder (durable.CreateFrom) and its PHASE0_CHUNK diagnostic path
	// were removed with the chunk store; every durable store is now an ordered
	// primary graph built by CreateFromPrimary.
	if _, err := durable.CreateFromPrimary(built, f, opts); err != nil {
		t.Fatal(err)
	}
	coll, err := durable.Open(f, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer coll.Close()
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}

	// Census: touch every key once. Each still-templated leaf de-templates
	// exactly once, so the de-template delta is the count of TC leaves that
	// bulk build actually adopted for this corpus.
	if os.Getenv("PHASE0_CENSUS") != "" {
		durable.ResetPrimaryTemplateDiag()
		cs0 := coll.Stats()
		var scr []byte
		for i := range docs {
			scr = AppendSameSizeUpdatedJSON(scr[:0], docs, i)
			if _, err := coll.Put([]byte(docs[i].Key), scr); err != nil {
				t.Fatal(err)
			}
		}
		if err := coll.Flush(); err != nil {
			t.Fatal(err)
		}
		cs1 := coll.Stats()
		fmt.Printf("[census] touch-all-keys detemplate=%d replan=%d (=> TC leaves adopted)\n",
			cs1.PrimaryTemplateDetemplateEvents-cs0.PrimaryTemplateDetemplateEvents,
			cs1.PrimaryTemplateReplanEvents-cs0.PrimaryTemplateReplanEvents)
		return
	}

	// Zipf trace, identical to cmd/mixed.
	rng := rand.New(rand.NewSource(0x51C5B))
	zipf := rand.NewZipf(rng, 1.01, 1, uint64(len(docs)-1))
	probe := rand.New(rand.NewSource(7)).Perm(len(docs))
	trace := make([]int, operations+warmup)
	for i := range trace {
		trace[i] = probe[int(zipf.Uint64())]
	}
	choices := operationTraceLocal(700, 250, 50) // churn workload

	var readScratch, replacement []byte
	updated := make([]bool, len(docs))
	run := func(choice, idx int) (kind int, mutations int) {
		key := docs[idx].Key
		switch choice {
		case 0: // read
			out, _, err := coll.AppendRaw(readScratch[:0], []byte(key))
			if err != nil {
				t.Fatal(err)
			}
			readScratch = out
			return 0, 0
		case 1: // update
			if updated[idx] {
				replacement = append(replacement[:0], docs[idx].JSON...)
			} else {
				replacement = AppendSameSizeUpdatedJSON(replacement[:0], docs, idx)
			}
			if _, err := coll.Put([]byte(key), replacement); err != nil {
				t.Fatal(err)
			}
			updated[idx] = !updated[idx]
			return 1, 1
		case 3: // churn: delete + restore
			if _, err := coll.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			if _, err := coll.Put([]byte(key), docs[idx].JSON); err != nil {
				t.Fatal(err)
			}
			updated[idx] = false
			return 3, 2
		}
		return 0, 0
	}

	// Warmup (unmeasured), checkpoint every 64 mutations.
	pending := 0
	for i := 0; i < warmup; i++ {
		_, m := run(choices[i%len(choices)], trace[i%len(trace)])
		pending += m
		if pending >= checkpointMutations {
			if err := coll.Flush(); err != nil {
				t.Fatal(err)
			}
			pending = 0
		}
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}

	before := coll.Stats()
	fmt.Printf("\n[after warmup] cumulative detemplate=%d replan=%d mergeEval=%d splits=%d merges=%d reclass=%d inplaceUpd=%d len=%d\n",
		before.PrimaryTemplateDetemplateEvents, before.PrimaryTemplateReplanEvents,
		before.MergeReclassEvaluations, before.PrimaryLeafSplits, before.PrimaryLeafMerges,
		before.PrimaryLeafReclass, before.BufferedInplaceUpdates, coll.Len())
	var updateLat, churnLat []int64

	profPath := filepath.Join(os.Getenv("PHASE0_OUT"), "phase0-churn-cpu.prof")
	if os.Getenv("PHASE0_OUT") == "" {
		profPath = filepath.Join(dir, "phase0-churn-cpu.prof")
	}
	pf, err := os.Create(profPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(pf); err != nil {
		t.Fatal(err)
	}

	pending = 0
	for i := 0; i < operations; i++ {
		seq := warmup + i
		choice := choices[seq%len(choices)]
		idx := trace[seq%len(trace)]
		start := time.Now()
		kind, m := run(choice, idx)
		elapsed := time.Since(start).Nanoseconds()
		switch kind {
		case 1:
			updateLat = append(updateLat, elapsed)
		case 3:
			churnLat = append(churnLat, elapsed)
		}
		pending += m
		if pending >= checkpointMutations {
			if err := coll.Flush(); err != nil {
				t.Fatal(err)
			}
			pending = 0
		}
	}
	pprof.StopCPUProfile()
	pf.Close()
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	after := coll.Stats()

	p50 := func(s []int64) float64 {
		if len(s) == 0 {
			return 0
		}
		slices.Sort(s)
		return float64(s[int(0.5*float64(len(s)-1)+0.5)]) / 1000.0
	}
	p99 := func(s []int64) float64 {
		if len(s) == 0 {
			return 0
		}
		slices.Sort(s)
		return float64(s[int(0.99*float64(len(s)-1)+0.5)]) / 1000.0
	}

	d := func(a, b uint64) uint64 { return b - a }

	fmt.Printf("\n===== PHASE 0 DIAGNOSTIC (churn workload, buffered-visible, cp=64) =====\n")
	fmt.Printf("update  count=%d  p50=%.2fus  p99=%.2fus\n", len(updateLat), p50(updateLat), p99(updateLat))
	fmt.Printf("churn   count=%d  p50=%.2fus  p99=%.2fus\n", len(churnLat), p50(churnLat), p99(churnLat))
	fmt.Printf("--- counter deltas over the measured loop ---\n")
	fmt.Printf("template detemplate events   = %d\n", d(before.PrimaryTemplateDetemplateEvents, after.PrimaryTemplateDetemplateEvents))
	fmt.Printf("template replan (encode)     = %d\n", d(before.PrimaryTemplateReplanEvents, after.PrimaryTemplateReplanEvents))
	fmt.Printf("merge/reclass evaluations    = %d\n", d(before.MergeReclassEvaluations, after.MergeReclassEvaluations))
	fmt.Printf("merge/reclass warranted      = %d\n", d(before.MergeReclassWarranted, after.MergeReclassWarranted))
	fmt.Printf("merge/reclass commits        = %d\n", d(before.MergeReclassCommits, after.MergeReclassCommits))
	fmt.Printf("merge/reclass aborts         = %d\n", d(before.MergeReclassAborts, after.MergeReclassAborts))
	fmt.Printf("merge/reclass skips (hyst.)  = %d\n", d(before.MergeReclassSkips, after.MergeReclassSkips))
	fmt.Printf("primary leaf splits          = %d\n", d(before.PrimaryLeafSplits, after.PrimaryLeafSplits))
	fmt.Printf("primary leaf merges          = %d\n", d(before.PrimaryLeafMerges, after.PrimaryLeafMerges))
	fmt.Printf("primary leaf reclass         = %d\n", d(before.PrimaryLeafReclass, after.PrimaryLeafReclass))
	fmt.Printf("buffered inplace attempts    = %d\n", d(before.BufferedInplaceAttempts, after.BufferedInplaceAttempts))
	fmt.Printf("buffered inplace updates     = %d\n", d(before.BufferedInplaceUpdates, after.BufferedInplaceUpdates))
	fmt.Printf("buffered inplace fallbacks   = %d\n", d(before.BufferedInplaceFallbacks, after.BufferedInplaceFallbacks))
	fmt.Printf("automatic checkpoints        = %d\n", d(before.AutomaticCheckpoints, after.AutomaticCheckpoints))
	fmt.Printf("journal acks                 = %d\n", d(before.JournalAcks, after.JournalAcks))
	fmt.Printf("chain acks                   = %d\n", d(before.ChainAcks, after.ChainAcks))
	fmt.Printf("cpu profile written to       = %s\n", profPath)
	fmt.Printf("=======================================================================\n")
}

func operationTraceLocal(reads, updates, churns int) []int {
	cycle := make([]int, 0, reads+updates+churns)
	for range reads {
		cycle = append(cycle, 0)
	}
	for range updates {
		cycle = append(cycle, 1)
	}
	for range churns {
		cycle = append(cycle, 3)
	}
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
