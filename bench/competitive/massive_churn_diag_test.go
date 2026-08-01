package competitive

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestMassiveChurnDiag profiles uniform fixed-live-set replacement and
// delete/reinsert churn after the primary graph grows beyond its configured
// resident cache. It is an explicit research tool, not a CI test.
func TestMassiveChurnDiag(t *testing.T) {
	if os.Getenv("MASSIVE_CHURN") == "" {
		t.Skip("set MASSIVE_CHURN=1 to run the massive-churn diagnostic")
	}
	corpusSize := massiveDiagInt(t, "MASSIVE_CORPUS", 1_000_000)
	operations := massiveDiagInt(t, "MASSIVE_OPERATIONS", 250_000)
	warmup := massiveDiagInt(t, "MASSIVE_WARMUP", 25_000)
	deletePercent := massiveDiagInt(t, "MASSIVE_DELETE_PERCENT", 20)
	checkpointMutations := massiveDiagInt(t, "MASSIVE_CHECKPOINT", 64)
	cacheMiB := massiveDiagInt(t, "MASSIVE_CACHE_MIB", DefaultCacheBytes>>20)
	if corpusSize < 1 || operations < 1 || warmup < 0 ||
		deletePercent < 0 || deletePercent > 100 || checkpointMutations < 1 ||
		cacheMiB < 1 {
		t.Fatal("invalid MASSIVE_* diagnostic configuration")
	}
	cardinality := HighCardinality
	if os.Getenv("MASSIVE_CARDINALITY") == "low" {
		cardinality = LowCardinality
	}
	docs := CorpusOf(corpusSize, cardinality)

	opts := durable.Options{
		ResidentBytes:      int64(cacheMiB) << 20,
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
	path := filepath.Join(dir, "vibedb.db")
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
	bulkOpts := opts
	bulkOpts.BufferCount = 0
	bulkOpts.QueueSlots = 0
	if _, err := durable.CreateFromPrimary(built, f, bulkOpts); err != nil {
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

	rng := rand.New(rand.NewSource(0xC11D15C))
	updated := make([]bool, len(docs))
	var replacement []byte
	pending := 0
	mutate := func() int {
		i := rng.Intn(len(docs))
		key := []byte(docs[i].Key)
		if rng.Intn(100) < deletePercent {
			if updated[i] {
				replacement = AppendSameSizeUpdatedJSON(replacement[:0], docs, i)
			} else {
				replacement = append(replacement[:0], docs[i].JSON...)
			}
			if deleted, err := coll.Delete(key); err != nil || !deleted {
				t.Fatalf("delete %q = %t, %v", key, deleted, err)
			}
			if _, err := coll.Put(key, replacement); err != nil {
				t.Fatalf("restore %q: %v", key, err)
			}
			return 2
		}
		if updated[i] {
			replacement = append(replacement[:0], docs[i].JSON...)
		} else {
			replacement = AppendSameSizeUpdatedJSON(replacement[:0], docs, i)
		}
		if _, err := coll.Put(key, replacement); err != nil {
			t.Fatalf("replace %q: %v", key, err)
		}
		updated[i] = !updated[i]
		return 1
	}
	checkpoint := func(changes int) {
		pending += changes
		if pending >= checkpointMutations {
			if err := coll.Flush(); err != nil {
				t.Fatal(err)
			}
			pending = 0
		}
	}
	for range warmup {
		checkpoint(mutate())
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	pending = 0

	profilePath := os.Getenv("MASSIVE_PROFILE")
	if profilePath == "" {
		profilePath = filepath.Join(dir, "massive-churn-cpu.prof")
	}
	profile, err := os.Create(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(profile); err != nil {
		profile.Close()
		t.Fatal(err)
	}

	before := coll.Stats()
	latencies := make([]int64, 0, operations)
	stateChanges := 0
	start := time.Now()
	for range operations {
		operationStart := time.Now()
		changes := mutate()
		checkpoint(changes)
		latencies = append(latencies, time.Since(operationStart).Nanoseconds())
		stateChanges += changes
	}
	if pending != 0 {
		if err := coll.Flush(); err != nil {
			pprof.StopCPUProfile()
			profile.Close()
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	pprof.StopCPUProfile()
	if err := profile.Close(); err != nil {
		t.Fatal(err)
	}
	after := coll.Stats()
	slices.Sort(latencies)
	percentile := func(p float64) float64 {
		at := int(p*float64(len(latencies)-1) + 0.5)
		return float64(latencies[at]) / 1_000
	}
	delta := func(a, b uint64) uint64 { return b - a }
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("\n===== MASSIVE CHURN DIAGNOSTIC =====\n")
	fmt.Printf("corpus=%d cardinality=%s operations=%d stateChanges=%d deletePercent=%d checkpoint=%d cacheMiB=%d\n",
		corpusSize, cardinality, operations, stateChanges, deletePercent, checkpointMutations, cacheMiB)
	fmt.Printf("throughput=%.0f stateChanges/s p50=%.2fus p99=%.2fus p99.9=%.2fus\n",
		float64(stateChanges)/elapsed.Seconds(), percentile(0.50), percentile(0.99), percentile(0.999))
	fmt.Printf("pageReads=%d readMiB=%.2f hits=%d misses=%d evictions=%d\n",
		delta(before.PageReads, after.PageReads), float64(delta(before.ReadBytes, after.ReadBytes))/(1<<20),
		delta(before.CacheHits, after.CacheHits), delta(before.CacheMisses, after.CacheMisses),
		delta(before.Evictions, after.Evictions))
	fmt.Printf("materialize attempts=%d updates=%d fallbacks=%d overlayFolds=%d forcedCP=%d\n",
		delta(before.MaterializationAttempts, after.MaterializationAttempts),
		delta(before.MaterializationUpdates, after.MaterializationUpdates),
		delta(before.MaterializationFallbacks, after.MaterializationFallbacks),
		delta(before.PrimaryOverlayFolds, after.PrimaryOverlayFolds),
		delta(before.AutomaticCheckpoints, after.AutomaticCheckpoints))
	fmt.Printf("deviceMiB=%.2f reusableMiB=%.2f pendingRetiredMiB=%.2f fileMiB=%.2f\n",
		float64(delta(before.DeviceBytes, after.DeviceBytes))/(1<<20),
		float64(after.ReusableBytes)/(1<<20), float64(after.PendingRetiredBytes)/(1<<20),
		float64(info.Size())/(1<<20))
	fmt.Printf("cpu profile=%s\n", profilePath)
	fmt.Printf("====================================\n")
}

func massiveDiagInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, value, err)
	}
	return parsed
}
