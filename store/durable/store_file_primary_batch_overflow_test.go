package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func primaryBatchOverflowDocument(tag string, fill byte, size int) []byte {
	return fmt.Appendf(nil, `{"tag":%q,"pad":%q}`,
		tag, strings.Repeat(string(fill), size))
}

func requirePrimaryBatchRaw(t testing.TB, collection *Collection, key string, want []byte) {
	t.Helper()
	got, ok, err := collection.AppendRaw(nil, []byte(key))
	if err != nil || !ok || !bytes.Equal(got, want) {
		t.Fatalf("AppendRaw(%q) = (%q,%t,%v), want %q", key, got, ok, err, want)
	}
}

func requirePrimaryBatchMissing(t testing.TB, collection *Collection, key string) {
	t.Helper()
	got, ok, err := collection.AppendRaw(nil, []byte(key))
	if err != nil || ok {
		t.Fatalf("AppendRaw(%q) = (%q,%t,%v), want missing", key, got, ok, err)
	}
}

func primaryBatchOverflowOptions(durability DurabilityMode) Options {
	options := testBatchOptions(64)
	options.Durability = durability
	options.RecoveryJournal = true
	options.InlineValueBytes = 512
	options.MaxDocumentBytes = 32 << 10
	options.Indexes = []store.IndexDefinition{{Name: "tag", Paths: []string{"/tag"}}}
	return options
}

// TestCollectionUpdateOverflowMixedReplaceDeleteSnapshotAndReopen exercises the
// complete lifecycle in both journal lanes: mixed inline/overflow inserts,
// replacement of a durable chain, deletion of a newer volatile chain,
// duplicate-resolved keys, exact indexes, snapshots, checkpoint materialization,
// and reopen.
func TestCollectionUpdateOverflowMixedReplaceDeleteSnapshotAndReopen(t *testing.T) {
	for _, lane := range []struct {
		name       string
		durability DurabilityMode
	}{
		{name: "sync-journal", durability: DurabilitySync},
		{name: "buffered-journal", durability: DurabilityBufferedVisible},
	} {
		t.Run(lane.name, func(t *testing.T) {
			options := primaryBatchOverflowOptions(lane.durability)
			path := filepath.Join(t.TempDir(), "overflow-batch.vibe")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}

			durableOld := primaryBatchOverflowDocument("durable-old", 'D', 9<<10)
			volatileOld := primaryBatchOverflowDocument("volatile-old", 'V', 5<<10)
			if err := collection.Update(func(batch *WriteBatch) error {
				if err := batch.Put([]byte("durable"), durableOld); err != nil {
					return err
				}
				return batch.Put([]byte("inline-seed"), []byte(`{"tag":"seed"}`))
			}); err != nil {
				t.Fatalf("seed durable overflow: %v", err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatalf("materialize durable overflow: %v", err)
			}
			if err := collection.Update(func(batch *WriteBatch) error {
				return batch.Put([]byte("volatile"), volatileOld)
			}); err != nil {
				t.Fatalf("seed volatile overflow: %v", err)
			}
			before, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}

			newOverflow := primaryBatchOverflowDocument("new", 'N', 13<<10)
			duplicateDiscarded := primaryBatchOverflowDocument("discarded", 'X', 7<<10)
			duplicateFinal := primaryBatchOverflowDocument("final", 'F', 6<<10)
			if err := collection.Update(func(batch *WriteBatch) error {
				if err := batch.Put([]byte("durable"), []byte(`{"tag":"now-inline"}`)); err != nil {
					return err
				}
				if err := batch.Delete([]byte("volatile")); err != nil {
					return err
				}
				if err := batch.Put([]byte("new-overflow"), newOverflow); err != nil {
					return err
				}
				if err := batch.Put([]byte("duplicate"), duplicateDiscarded); err != nil {
					return err
				}
				return batch.Put([]byte("duplicate"), duplicateFinal)
			}); err != nil {
				t.Fatalf("mixed overflow batch: %v", err)
			}

			requirePrimaryBatchRaw(t, collection, "durable", []byte(`{"tag":"now-inline"}`))
			requirePrimaryBatchMissing(t, collection, "volatile")
			requirePrimaryBatchRaw(t, collection, "new-overflow", newOverflow)
			requirePrimaryBatchRaw(t, collection, "duplicate", duplicateFinal)
			if got := primaryExactTestKeys(
				t, collection, "tag", primaryExactTestNeedle(t, `"final"`),
			); len(got) != 1 || got[0] != "duplicate" {
				t.Fatalf("final exact postings = %v", got)
			}
			if got := primaryExactTestKeys(
				t, collection, "tag", primaryExactTestNeedle(t, `"discarded"`),
			); len(got) != 0 {
				t.Fatalf("discarded duplicate exact postings = %v", got)
			}
			oldDurable, ok, err := before.AppendRaw(nil, []byte("durable"))
			if err != nil || !ok || !bytes.Equal(oldDurable, durableOld) {
				t.Fatalf("snapshot durable = (%q,%t,%v)", oldDurable, ok, err)
			}
			oldVolatile, ok, err := before.AppendRaw(nil, []byte("volatile"))
			if err != nil || !ok || !bytes.Equal(oldVolatile, volatileOld) {
				t.Fatalf("snapshot volatile = (%q,%t,%v)", oldVolatile, ok, err)
			}
			if err := before.Close(); err != nil {
				t.Fatal(err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatalf("post-batch checkpoint: %v", err)
			}
			if err := collection.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(file, options)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			requirePrimaryBatchRaw(t, reopened, "durable", []byte(`{"tag":"now-inline"}`))
			requirePrimaryBatchMissing(t, reopened, "volatile")
			requirePrimaryBatchRaw(t, reopened, "new-overflow", newOverflow)
			requirePrimaryBatchRaw(t, reopened, "duplicate", duplicateFinal)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestCollectionUpdateOverflowJournalCrashReopen proves the batch record replays
// as one group with overflow payloads in both supported journal lanes. The sync
// case captures the post-sync/pre-publication image; the buffered case captures
// the acknowledged journal image without closing or checkpointing the store.
func TestCollectionUpdateOverflowJournalCrashReopen(t *testing.T) {
	for _, lane := range []struct {
		name        string
		durability  DurabilityMode
		captureHook bool
	}{
		{name: "sync-journal", durability: DurabilitySync, captureHook: true},
		{name: "buffered-journal", durability: DurabilityBufferedVisible},
	} {
		t.Run(lane.name, func(t *testing.T) {
			options := primaryBatchOverflowOptions(lane.durability)
			path := filepath.Join(t.TempDir(), "overflow-crash.vibe")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]string{
				"inline":     string([]byte(`{"tag":"inline"}`)),
				"overflow-a": string(primaryBatchOverflowDocument("a", 'A', 10<<10)),
				"overflow-b": string(primaryBatchOverflowDocument("b", 'B', 14<<10)),
			}
			var image journalCrashImage
			captured := false
			if lane.captureHook {
				previous := recoveryJournalPostSyncHook
				recoveryJournalPostSyncHook = func() {
					if !captured {
						image = captureJournalImage(t, path)
						captured = true
					}
				}
				defer func() { recoveryJournalPostSyncHook = previous }()
			}
			if err := collection.Update(func(batch *WriteBatch) error {
				for _, key := range []string{"inline", "overflow-a", "overflow-b"} {
					if err := batch.Put([]byte(key), []byte(values[key])); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("overflow crash batch: %v", err)
			}
			if !lane.captureHook {
				image = captureJournalImage(t, path)
				captured = true
			}
			if !captured {
				t.Fatal("journal crash boundary was not captured")
			}
			outcome := verifyJournalCrashImage(
				t, options, image, []map[string]string{values}, lane.name,
			)
			if outcome != "recovered" {
				t.Fatalf("captured batch outcome = %q, want recovered", outcome)
			}
			_ = collection.Close()
			_ = file.Close()
		})
	}
}

// TestCollectionUpdateOverflowWALFailureUnadmitsAllFrames drives a failure after
// every overflow page and leaf has been admitted but before publication. Dirty
// capacity, visible rows, and crash recovery must all remain at the pre-batch
// state.
func TestCollectionUpdateOverflowWALFailureUnadmitsAllFrames(t *testing.T) {
	getFault, restore := installJournalFaultSeam(t)
	defer restore()
	options := primaryBatchOverflowOptions(DurabilitySync)
	path := filepath.Join(t.TempDir(), "overflow-wal-failure.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam not installed")
	}
	before := collection.Stats()
	fault.Program(storeio.JournalFaultPlan{
		Phase: storeio.JournalFaultENOSPCAppend, AppendIndex: fault.Appends(),
	})
	err = collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put(
			[]byte("overflow-a"), primaryBatchOverflowDocument("a", 'A', 12<<10),
		); err != nil {
			return err
		}
		return batch.Put(
			[]byte("overflow-b"), primaryBatchOverflowDocument("b", 'B', 8<<10),
		)
	})
	if err == nil || !fault.Faulted() {
		t.Fatalf("faulted overflow batch = %v, fired=%t", err, fault.Faulted())
	}
	requirePrimaryBatchMissing(t, collection, "overflow-a")
	requirePrimaryBatchMissing(t, collection, "overflow-b")
	after := collection.Stats()
	if after.DirtyBytes != before.DirtyBytes {
		t.Fatalf("failed batch dirty bytes = %d, want %d", after.DirtyBytes, before.DirtyBytes)
	}
	image := captureJournalImage(t, path)
	_ = collection.Close()
	_ = file.Close()
	if outcome := verifyJournalCrashImage(
		t, options, image, []map[string]string{{}}, "overflow WAL failure",
	); outcome != "recovered" {
		t.Fatalf("failed-batch crash outcome = %q, want recovered empty state", outcome)
	}
}

// TestCollectionUpdateOverflowTopologyRetry inserts enough compact overflow
// descriptors to force class-5 subdivision. The content-equivalent topology
// generation may publish first, but the logical rows still appear all-or-none in
// the following batch generation and survive checkpoint/reopen.
func TestCollectionUpdateOverflowTopologyRetry(t *testing.T) {
	const documents = 280
	options := primaryBatchOverflowOptions(DurabilitySync)
	options.MaxBatchDocuments = documents
	options.MaxBatchBytes = 4 << 20
	options.Indexes = nil
	collection, file := openBatchCollection(t, options)
	values := make([][]byte, documents)
	if err := collection.Update(func(batch *WriteBatch) error {
		for i := range documents {
			values[i] = primaryBatchOverflowDocument("split", byte('A'+i%26), 3<<10)
			key := fmt.Appendf(nil, "overflow-topology-%04d-%s", i,
				strings.Repeat("k", 80))
			if err := batch.Put(key, values[i]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("topology overflow batch: %v", err)
	}
	if collection.Len() != documents {
		t.Fatalf("length = %d, want %d", collection.Len(), documents)
	}
	for _, i := range []int{0, documents / 2, documents - 1} {
		key := fmt.Sprintf("overflow-topology-%04d-%s", i, strings.Repeat("k", 80))
		requirePrimaryBatchRaw(t, collection, key, values[i])
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Len() != documents {
		t.Fatalf("reopened length = %d, want %d", reopened.Len(), documents)
	}
}

// TestCollectionUpdateOverflowRejectsOnlyMaxDocumentBoundary proves the inline
// threshold is representation selection, not an Update admission limit.
func TestCollectionUpdateOverflowRejectsOnlyMaxDocumentBoundary(t *testing.T) {
	options := primaryBatchOverflowOptions(DurabilitySync)
	collection, _ := openBatchCollection(t, options)
	valid := primaryBatchOverflowDocument("valid", 'V', options.InlineValueBytes+1)
	if err := collection.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("valid"), valid)
	}); err != nil {
		t.Fatalf("valid overflow: %v", err)
	}
	requirePrimaryBatchRaw(t, collection, "valid", valid)
	generation := collection.Generation()
	tooLarge := primaryBatchOverflowDocument("large", 'L', options.MaxDocumentBytes)
	err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("sibling"), []byte(`{"tag":"sibling"}`)); err != nil {
			return err
		}
		return batch.Put([]byte("too-large"), tooLarge)
	})
	if !errors.Is(err, ErrDocumentTooLarge) {
		t.Fatalf("over-MaxDocumentBytes batch = %v", err)
	}
	if collection.Generation() != generation {
		t.Fatal("rejected oversized batch published")
	}
	requirePrimaryBatchMissing(t, collection, "sibling")
}

// TestPrimaryBatchOverflowLayoutBounds pins the exact extent transitions and
// overflow-safe high-water arithmetic independently of publication. The same
// planner is used by point and batch writes, so these checks also guard 386.
func TestPrimaryBatchOverflowLayoutBounds(t *testing.T) {
	options := primaryBatchOverflowOptions(DurabilityBufferedVisible)
	options.RecoveryJournal = false
	options.Indexes = nil
	options.MaxDocumentBytes = 2 * options.MaxPageSize
	collection, _ := openBatchCollection(t, options)
	perPage := collection.options.MaxPageSize - primaryOverflowPageOverhead
	baseOffset := uint64(64 * collection.options.PageSize)
	for _, size := range []int{1, perPage, perPage + 1, options.MaxDocumentBytes} {
		value := bytes.Repeat([]byte{'x'}, size)
		head, total, pages, err := collection.planBufferedPrimaryOverflowChain(
			value, 7, baseOffset, 100,
		)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if pages != collection.primaryOverflowPageCount(size) {
			t.Fatalf("size %d pages = %d, want %d", size, pages,
				collection.primaryOverflowPageCount(size))
		}
		if head.Offset != baseOffset || head.LogicalID != 100 ||
			head.Generation != 7 || head.Kind != storeio.PageOverflow || total == 0 {
			t.Fatalf("size %d layout = head %+v total %d", size, head, total)
		}
	}
	quantum := uint64(collection.options.PageSize)
	value := bytes.Repeat([]byte{'x'}, perPage+1)
	if _, _, _, err := collection.planBufferedPrimaryOverflowChain(
		value, 7, ^uint64(0)-quantum+1, 100,
	); !errors.Is(err, storeio.ErrInvalidWrite) {
		t.Fatalf("FileEnd overflow = %v", err)
	}
	if _, _, _, err := collection.planBufferedPrimaryOverflowChain(
		value, 7, baseOffset, ^uint64(0)-1,
	); !errors.Is(err, storeio.ErrInvalidWrite) {
		t.Fatalf("NextLogicalID overflow = %v", err)
	}
}

// TestCollectionUpdateOverflowSnapshotRetirementPressureIsFailureAtomic fills
// the bounded volatile-retirement table with real superseded chains while a
// snapshot holds their generation. The first over-capacity batch and its retry
// must publish neither the replacement nor its sibling, stage no lasting dirty
// frame, and consume no additional dirty capacity. Releasing the snapshot makes
// the identical batch immediately retryable.
func TestCollectionUpdateOverflowSnapshotRetirementPressureIsFailureAtomic(t *testing.T) {
	options := primaryBatchOverflowOptions(DurabilityBufferedVisible)
	options.RecoveryJournal = false
	options.Indexes = nil
	options.MaxBatchDocuments = 4
	options.MaxRetiredExtents = 512
	options.ResidentBytes = 128 << 20
	collection, _ := openBatchCollection(t, options)
	if _, err := collection.Put([]byte("baseline"), []byte(`{"tag":"base"}`)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	held, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	current := primaryBatchOverflowDocument("current", 'A', 24<<10)
	if err := collection.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("hot"), current)
	}); err != nil {
		t.Fatal(err)
	}
	retirePerReplacement := collection.primaryOverflowPageCount(len(current)) + 1
	for iteration := 0; len(collection.primaryVolatileRetired)+
		retirePerReplacement <= collection.options.MaxRetiredExtents; iteration++ {
		if iteration > collection.options.MaxRetiredExtents {
			t.Fatal("snapshot did not retain superseded overflow frames")
		}
		beforeRetired := len(collection.primaryVolatileRetired)
		candidate := primaryBatchOverflowDocument(
			"current", byte('A'+(iteration+1)%26), 24<<10,
		)
		if err := collection.Update(func(batch *WriteBatch) error {
			return batch.Put([]byte("hot"), candidate)
		}); err != nil {
			t.Fatalf("fill retirement table at %d/%d: %v",
				len(collection.primaryVolatileRetired),
				collection.options.MaxRetiredExtents, err)
		}
		if len(collection.primaryVolatileRetired) <= beforeRetired {
			t.Fatalf("retirement table did not grow: before=%d after=%d",
				beforeRetired, len(collection.primaryVolatileRetired))
		}
		current = candidate
	}
	failed := primaryBatchOverflowDocument("failed", 'Z', 24<<10)
	apply := func() error {
		return collection.Update(func(batch *WriteBatch) error {
			if err := batch.Put([]byte("hot"), failed); err != nil {
				return err
			}
			return batch.Put([]byte("failure-sibling"), []byte(`{"tag":"sibling"}`))
		})
	}
	if err := apply(); !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("snapshot-pinned retirement pressure = %v", err)
	}
	if len(collection.batchPrimaryAdmitted) != 0 {
		t.Fatalf("failed batch retained %d admitted frames",
			len(collection.batchPrimaryAdmitted))
	}
	requirePrimaryBatchRaw(t, collection, "hot", current)
	requirePrimaryBatchMissing(t, collection, "failure-sibling")
	available := collection.cache.DirtyCapacityAvailable()
	if err := apply(); !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("repeated snapshot-pinned retirement pressure = %v", err)
	}
	if len(collection.batchPrimaryAdmitted) != 0 {
		t.Fatalf("repeated failed batch retained %d admitted frames",
			len(collection.batchPrimaryAdmitted))
	}
	if got := collection.cache.DirtyCapacityAvailable(); got != available {
		t.Fatalf("repeated failure dirty capacity = %d, want %d", got, available)
	}
	requirePrimaryBatchRaw(t, collection, "hot", current)
	requirePrimaryBatchMissing(t, collection, "failure-sibling")
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := apply(); err != nil {
		t.Fatalf("retry after snapshot release: %v", err)
	}
	requirePrimaryBatchRaw(t, collection, "hot", failed)
	requirePrimaryBatchRaw(t, collection, "failure-sibling", []byte(`{"tag":"sibling"}`))
}

func BenchmarkCollectionUpdateOverflowBatch(b *testing.B) {
	for _, shape := range []struct {
		name       string
		firstSize  int
		secondSize int
	}{
		{name: "InlineReplacementControl", firstSize: 32, secondSize: 32},
		{name: "MixedOverflowReplacement", firstSize: 32, secondSize: 4 << 10},
	} {
		b.Run(shape.name, func(b *testing.B) {
			options := primaryBatchOverflowOptions(DurabilityBufferedVisible)
			options.RecoveryJournal = false
			options.Indexes = nil
			file, err := os.CreateTemp(b.TempDir(), "overflow-benchmark-*")
			if err != nil {
				b.Fatal(err)
			}
			collection, err := Create(file, options)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				_ = collection.Close()
				_ = file.Close()
			})
			a := primaryBatchOverflowDocument("a", 'A', shape.firstSize)
			c := primaryBatchOverflowDocument("b", 'B', shape.secondSize)
			apply := func() error {
				return collection.Update(func(batch *WriteBatch) error {
					if err := batch.Put([]byte("a"), a); err != nil {
						return err
					}
					return batch.Put([]byte("b"), c)
				})
			}
			if err := apply(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := apply(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
