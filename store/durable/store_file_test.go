package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func testFileStoreOptions() Options {
	return Options{
		Collection: store.Options{ChunkDocuments: 4},
		PageSize:   4096, MaxPageSize: 64 << 10, ResidentBytes: 8 << 20,
		MaxDocumentBytes: 64 << 10, MaxKeyBytes: 128, InlineValueBytes: 512,
		ReadConcurrency: 2, PrefetchQueue: 8, BufferCount: 1024,
		QueueSlots: 4, GroupLimit: 2, Backend: BackendPortable,
		Durability:        DurabilitySync,
		MaxSnapshotLeases: 8, MaxRetiredExtents: 1024,
		// These tests exercise the single-document path and pin deliberately
		// tight buffer, retirement, and residency bounds. A batch reservation
		// wide enough for the default sixty-four-document Update would not fit
		// any of them, and widening them here would stop testing the pressure
		// they were written for.
		MaxBatchDocuments: 1,
	}
}

func TestFileStoreDirtyBudgetUsesExtentSizes(t *testing.T) {
	options := testFileStoreOptions()
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	oldFixedFrameBound := uint64(normalized.maxTransactionPages * normalized.MaxPageSize)
	if normalized.maxTransactionBytes >= oldFixedFrameBound {
		t.Fatalf("packed dirty bound = %d, fixed-frame bound %d", normalized.maxTransactionBytes, oldFixedFrameBound)
	}
	// ResidentBytes now also narrows the adaptive dirty-leaf overlay window.
	// Converge to its one-bucket floor before checking the exact transaction
	// boundary; using the original wider window's byte bound would legitimately
	// select a smaller transaction on the second normalization.
	for {
		options.ResidentBytes = int64(normalized.maxTransactionBytes)
		next, nextErr := options.normalized()
		if nextErr != nil {
			t.Fatalf("exact adaptive dirty budget rejected: %v", nextErr)
		}
		if next.maxTransactionBytes == normalized.maxTransactionBytes {
			normalized = next
			break
		}
		normalized = next
	}
	options.ResidentBytes = int64(normalized.maxTransactionBytes)
	options.ResidentBytes--
	if _, err := options.normalized(); err == nil {
		t.Fatal("undersized dirty budget accepted")
	}
	options = testFileStoreOptions()
	options.MaxDocumentBytes = int(^uint(0) >> 1)
	if _, err := options.normalized(); err == nil {
		t.Fatal("overflowing transaction geometry accepted")
	}
	options = testFileStoreOptions()
	options.ReadMode = ReadMode(255)
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid direct-read mode accepted")
	}
	options = testFileStoreOptions()
	options.WriteMode = WriteMode(255)
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid direct-write mode accepted")
	}
	options = testFileStoreOptions()
	options.ReadConcurrency = -1
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid read concurrency accepted")
	}
	options = testFileStoreOptions()
	options.ReadQueueDepth = -1
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid read queue depth accepted")
	}
	options = testFileStoreOptions()
	options.PrefetchQueue = 32769
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid prefetch queue accepted")
	}
	options = testFileStoreOptions()
	options.QueueSlots = int(^uint(0) >> 1)
	if _, err := options.normalized(); err == nil {
		t.Fatal("overflowing commit queue accepted")
	}
	options = testFileStoreOptions()
	options.MaxRetiredExtents = 1<<24 + 1
	if _, err := options.normalized(); err == nil {
		t.Fatal("retirement capacity beyond allocator limit accepted")
	}
	options = testFileStoreOptions()
	options.MaxRetiredExtents = 1 << 24
	options.ResidentBytes = 128 << 20
	options.BufferCount = 32_768
	if _, err := options.normalized(); err != nil {
		t.Fatalf("100M retirement capacity rejected: %v", err)
	}
	options = testFileStoreOptions()
	options.CommitCoalesce = time.Second + 1
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid commit coalescing window accepted")
	}
	options = testFileStoreOptions()
	options.MaxBatchBytes = options.MaxDocumentBytes
	if _, err := options.normalized(); err == nil {
		t.Fatal("batch byte bound that cannot hold every key accepted")
	}
}

func TestFileStoreDirectReadModeAndCallerDescriptorLifetime(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-direct-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ReadMode = ReadDirectTry
	options.WriteMode = WriteDirectTry
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put([]byte("direct:key"), []byte(`{"mode":"observable"}`)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reopened.AppendRaw(make([]byte, 0, 64), []byte("direct:key"))
	if err != nil || !ok || string(got) != `{"mode":"observable"}` {
		t.Fatalf("direct-mode read = (%q,%v,%v)", got, ok, err)
	}
	stats := reopened.Stats()
	if stats.PageReads == 0 {
		t.Fatalf("direct-mode reopen performed no cache-miss read: %+v", stats)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// collection owns only independently reopened direct descriptors. Closing
	// them must never close or alter the caller-owned descriptor.
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil {
		t.Fatalf("caller descriptor after Collection.Close: %v", err)
	}
}

func TestFileStoreCreateOpenAndSnapshotLifetime(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs.Stats().CommitCapacityBytes,
		uint64(min(options.BufferCount, 16)*options.MaxPageSize); got != want {
		t.Fatalf("commit capacity = %d, want %d", got, want)
	}
	reusableCapacity := options.MaxRetiredExtents +
		min(options.MaxRetiredExtents, freeReclaimBatch)
	if got, want := fs.Stats().ReusableCapacityBytes, uint64(reusableCapacity)*uint64(unsafe.Sizeof(storeio.FreeExtent{})); got != want {
		t.Fatalf("reusable capacity = %d, want %d", got, want)
	}
	if got, want := fs.Stats().ReusableIndexBytes,
		uint64(storeio.FreeExtentIndexCapacity(reusableCapacity))*8; got != want {
		t.Fatalf("reusable index = %d, want %d", got, want)
	}
	if got, want := fs.Stats().RetiredIntervalIndexBytes,
		uint64(storeio.RetiredIntervalIndexStorageBytes(
			options.MaxRetiredExtents,
		)); got != want {
		t.Fatalf("retired interval index = %d, want %d", got, want)
	}
	if got, want := fs.Stats().RetiredExtentArenaBytes,
		uint64(storeio.RetiredExtentStorageBytes(
			options.MaxRetiredExtents,
		)); got != want {
		t.Fatalf("retired extent arena = %d, want %d", got, want)
	}
	if fs.reusableBlock.OutsideHeap() &&
		fs.Stats().RetiredIntervalIndexExternalBytes !=
			fs.Stats().RetiredIntervalIndexBytes {
		t.Fatalf("retired interval index external accounting = %+v", fs.Stats())
	}
	if fs.reusableBlock.OutsideHeap() &&
		fs.Stats().RetiredExtentArenaExternalBytes !=
			fs.Stats().RetiredExtentArenaBytes {
		t.Fatalf("retired extent arena external accounting = %+v", fs.Stats())
	}
	if fs.Len() != 0 || fs.Generation() != 1 || fs.DurableGeneration() != 1 {
		t.Fatalf("created state = len %d generation %d durable %d", fs.Len(), fs.Generation(), fs.DurableGeneration())
	}
	if got, ok, err := fs.AppendRaw(nil, []byte("missing")); err != nil || ok || got != nil {
		t.Fatalf("AppendRaw missing = (%q,%v,%v)", got, ok, err)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Len() != 0 || snapshot.Generation() != 1 {
		t.Fatalf("snapshot = len %d generation %d", snapshot.Len(), snapshot.Generation())
	}
	if err := fs.Close(); !errors.Is(err, storeio.ErrLeasesActive) {
		t.Fatalf("Close with snapshot = %v, want %v", err, storeio.ErrLeasesActive)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Len() != 0 || reopened.Generation() != 1 || reopened.DurableGeneration() != 1 {
		t.Fatalf("reopened state = len %d generation %d durable %d", reopened.Len(), reopened.Generation(), reopened.DurableGeneration())
	}
}

func newFileStoreWithPendingRetirement(
	t *testing.T,
) (*Collection, *os.File) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "file-fs-close-stats-*")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := Create(file, testFileStoreOptions())
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if created, err := fs.Put([]byte("held"), []byte(`{"value":1}`)); err != nil || !created {
		_ = fs.Close()
		_ = file.Close()
		t.Fatalf("initial Put = (%v,%v)", created, err)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		_ = fs.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	if created, err := fs.Put([]byte("held"), []byte(`{"value":2}`)); err != nil || created {
		_ = snapshot.Close()
		_ = fs.Close()
		_ = file.Close()
		t.Fatalf("replacement Put = (%v,%v)", created, err)
	}
	if stats := fs.Stats(); stats.PendingRetiredExtents == 0 {
		_ = snapshot.Close()
		_ = fs.Close()
		_ = file.Close()
		t.Fatal("replacement did not leave a snapshot-fenced retirement")
	}
	if err := snapshot.Close(); err != nil {
		_ = fs.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	if stats := fs.Stats(); stats.PendingRetiredExtents == 0 {
		_ = fs.Close()
		_ = file.Close()
		t.Fatal("closing snapshot unexpectedly drained retirement metadata")
	}
	return fs, file
}

func TestFileStoreStatsAfterCloseDetachesRetirementArenas(t *testing.T) {
	fs, file := newFileStoreWithPendingRetirement(t)
	defer file.Close()
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	if fs.reclaimer != nil || fs.reusableBlock != nil {
		t.Fatal("Close retained a view into retirement metadata")
	}
	if stats := fs.Stats(); stats != (Stats{}) {
		t.Fatalf("Stats after Close = %+v, want zero", stats)
	}
}

func TestFileStoreStatsConcurrentWithCloseAndPendingRetirements(t *testing.T) {
	fs, file := newFileStoreWithPendingRetirement(t)
	defer file.Close()

	const readers = 16
	start := make(chan struct{})
	stop := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(readers)
	done.Add(readers)
	for range readers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
					_ = fs.Stats()
				}
			}
		}()
	}
	ready.Wait()
	close(start)
	if err := fs.Close(); err != nil {
		close(stop)
		done.Wait()
		t.Fatal(err)
	}
	close(stop)
	done.Wait()
	if stats := fs.Stats(); stats != (Stats{}) {
		t.Fatalf("Stats after concurrent Close = %+v, want zero", stats)
	}
}

func TestFileStorePublishedStateStaysCompact(t *testing.T) {
	const maxPublishedStateBytes = 640
	if size := unsafe.Sizeof(fileStoreState{}); size > maxPublishedStateBytes {
		t.Fatalf("published state is %d bytes, want at most %d", size, maxPublishedStateBytes)
	}
}

func TestFileStoreExclusiveWriterLease(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-writer-lock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	// Advisory locks commonly permit a second acquisition through the same
	// descriptor. The in-process registry must reject that case too.
	if _, err := Open(file, options); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("same-descriptor second writer = %v, want %v", err, ErrWriterLocked)
	}
	second, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := Open(second, options); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("second-descriptor writer = %v, want %v", err, ErrWriterLocked)
	}

	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(second, options)
	if err != nil {
		t.Fatalf("writer lease remained after Close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateFileStoreRequiresEmptyFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-nonempty-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("occupied")); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(file, testFileStoreOptions()); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("Create = %v, want %v", err, ErrNotEmpty)
	}
}

func TestFileStoreMutationsOverflowSnapshotAndReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-mutations-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string)
	for i := range 10 {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf(`{"key":%q,"value":%d}`, key, i)
		created, putErr := fs.Put([]byte(key), []byte(value))
		if putErr != nil || !created {
			t.Fatalf("Put(%q) = (%v,%v)", key, created, putErr)
		}
		want[key] = value
	}
	if fs.Len() != uint64(len(want)) {
		t.Fatalf("Len = %d, want %d", fs.Len(), len(want))
	}

	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	old := want["key-01"]
	large := `{"payload":"` + strings.Repeat("large-value-", 400) + `"}`
	created, err := fs.Put([]byte("key-01"), []byte(large))
	if err != nil || created {
		t.Fatalf("update = (%v,%v), want existing", created, err)
	}
	want["key-01"] = large
	if got, ok, err := snapshot.AppendRaw(nil, []byte("key-01")); err != nil || !ok || string(got) != old {
		t.Fatalf("old snapshot = (%q,%v,%v), want %q", got, ok, err, old)
	}
	if got, ok, err := fs.AppendRaw(nil, []byte("key-01")); err != nil || !ok || string(got) != large {
		t.Fatalf("current overflow = (%d bytes,%v,%v), want %d bytes", len(got), ok, err, len(large))
	}
	deleted, err := fs.Delete([]byte("key-02"))
	if err != nil || !deleted {
		t.Fatalf("Delete existing = (%v,%v)", deleted, err)
	}
	delete(want, "key-02")
	if deleted, err := fs.Delete([]byte("key-02")); err != nil || deleted {
		t.Fatalf("Delete missing = (%v,%v)", deleted, err)
	}
	if got, ok, err := snapshot.AppendRaw(nil, []byte("key-02")); err != nil || !ok || string(got) == "" {
		t.Fatalf("snapshot deleted key = (%q,%v,%v)", got, ok, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Len() != uint64(len(want)) {
		t.Fatalf("reopened Len = %d, want %d", reopened.Len(), len(want))
	}
	queued, err := reopened.PrefetchKeys([][]byte{[]byte("key-09"), []byte("key-00"), []byte("missing"), []byte("key-05"), []byte("key-01")})
	if err != nil || queued == 0 {
		t.Fatalf("PrefetchKeys = (%d,%v)", queued, err)
	}
	if stats := reopened.Stats(); stats.PrefetchQueued < uint64(queued) || stats.CapacityBytes == 0 || stats.DocumentCount != uint64(len(want)) {
		t.Fatalf("Stats after prefetch = %+v", stats)
	}
	for key, value := range want {
		got, ok, getErr := reopened.AppendRaw(nil, []byte(key))
		if getErr != nil || !ok || string(got) != value {
			t.Fatalf("reopened %q = (%q,%v,%v), want %q", key, got, ok, getErr, value)
		}
	}
	if got, ok, err := reopened.AppendRaw(nil, []byte("key-02")); err != nil || ok || got != nil {
		t.Fatalf("reopened deleted key = (%q,%v,%v)", got, ok, err)
	}
}

func TestFileStoreRejectsInvalidMutationWithoutPublishing(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-invalid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	generation := fs.Generation()
	if _, err := fs.Put([]byte("bad"), []byte(`{"unterminated":`)); err == nil {
		t.Fatal("Put invalid JSON succeeded")
	}
	if fs.Generation() != generation || fs.Len() != 0 {
		t.Fatalf("invalid Put published generation %d len %d", fs.Generation(), fs.Len())
	}
	if _, err := fs.Put([]byte(strings.Repeat("k", fs.options.MaxKeyBytes+1)), []byte(`null`)); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversize key = %v, want %v", err, ErrKeyTooLarge)
	}
}

func TestFileStoreReusesExtentsWithoutViolatingSnapshots(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-reuse-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	// This test reads super.FileEnd directly to observe physical copy-on-write
	// reuse. The synchronous primary lane stages each Put in a volatile append
	// region far past the durable FileEnd and only materializes it at a checkpoint,
	// so its visible FileEnd swings and never plateaus. The async lane's committer
	// writes each generation to the device, so its FileEnd is the physical
	// high-water this test is written against.
	options.Durability = DurabilityAsyncVisible
	options.MaxRetiredExtents = 1024
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if _, err := fs.Put([]byte("hot"), []byte(`{"version":0}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	beforePinned := fs.state.Load().fileEnd
	for version := 1; version <= 20; version++ {
		if _, err := fs.Put([]byte("hot"), []byte(fmt.Sprintf(`{"version":%d}`, version))); err != nil {
			t.Fatal(err)
		}
	}
	afterPinned := fs.state.Load().fileEnd
	if afterPinned <= beforePinned {
		t.Fatalf("active snapshot did not fence reuse: fileEnd %d -> %d", beforePinned, afterPinned)
	}
	if got, ok, err := snapshot.AppendRaw(nil, []byte("hot")); err != nil || !ok || string(got) != `{"version":0}` {
		t.Fatalf("pinned value after churn = (%q,%v,%v)", got, ok, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	for version := 21; version <= 40; version++ {
		if _, err := fs.Put([]byte("hot"), []byte(fmt.Sprintf(`{"version":%d}`, version))); err != nil {
			t.Fatal(err)
		}
	}
	plateau := fs.state.Load().fileEnd
	for version := 41; version <= 80; version++ {
		if _, err := fs.Put([]byte("hot"), []byte(fmt.Sprintf(`{"version":%d}`, version))); err != nil {
			t.Fatal(err)
		}
	}
	if got := fs.state.Load().fileEnd; got != plateau {
		t.Fatalf("copy-on-write file did not plateau: %d -> %d", plateau, got)
	}
	if got, ok, err := fs.AppendRaw(nil, []byte("hot")); err != nil || !ok || string(got) != `{"version":80}` {
		t.Fatalf("latest value = (%q,%v,%v)", got, ok, err)
	}
}

func TestFileStorePersistsReusableExtentsAcrossReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-free-log-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// Physical allocation is what publishes the inline free log and, on a reopened
	// store, triggers the lazy free-log replay. The synchronous lane stages each
	// mutation as a volatile frame and does that allocation at the next checkpoint,
	// so this test checkpoints at each observation point; the reads are of settled
	// durable state, not the volatile append region, and stay deterministic across
	// the whole package (an async committer's plateau does not).
	options := testFileStoreOptions()
	options.MaxRetiredExtents = 1024
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put([]byte("hot"), []byte(`0`)); err != nil {
		t.Fatal(err)
	}
	for version := 1; version <= 30; version++ {
		if _, err := fs.Put([]byte("hot"), []byte(fmt.Sprintf(`%d`, version))); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Flush(); err != nil {
		t.Fatal(err)
	}
	if fs.inlineFree.Len() == 0 {
		t.Fatal("churn did not publish a durable inline free log")
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.freeLoaded {
		t.Fatal("Open eagerly replayed the free log")
	}
	if _, err := reopened.Put([]byte("hot"), []byte(`31`)); err != nil {
		t.Fatal(err)
	}
	// The synchronous lane's first physical allocation is the checkpoint, which
	// draws from free space and lazily replays the bounded free log.
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	if !reopened.freeLoaded {
		t.Fatal("first checkpoint did not lazily replay the bounded free log")
	}
	for version := 32; version <= 50; version++ {
		if _, err := reopened.Put([]byte("hot"), []byte(fmt.Sprintf(`%d`, version))); err != nil {
			t.Fatal(err)
		}
	}
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	plateau := reopened.Stats().FileEnd
	for version := 51; version <= 80; version++ {
		if _, err := reopened.Put([]byte("hot"), []byte(fmt.Sprintf(`%d`, version))); err != nil {
			t.Fatal(err)
		}
	}
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Stats().FileEnd; got != plateau {
		t.Fatalf("reopened allocator did not plateau: %d -> %d", plateau, got)
	}
}

// collectionIndexMasks probes an exact index against a collection's newest
// committed state through a short-lived snapshot. Exact-index probing moved off
// Collection onto Snapshot with the ordered primary, so the convenience the
// chunk store exposed on Collection is reconstructed here for the tests that
// still verify the current state directly.
func collectionIndexMasks(c *Collection, dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	snap, err := c.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	return snap.AppendIndexMasks(dst, name, values...)
}

func TestFileStoreExactIndexesMaintainProbeAndReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-index-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	options.Indexes = []store.IndexDefinition{
		{Name: "status", Paths: []string{"/status"}},
		{Name: "tenant_status", Paths: []string{"/tenant", "/status"}},
	}
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		status := "idle"
		if i%3 == 0 {
			status = "active"
		}
		tenant := "other"
		if i%2 == 0 {
			tenant = "acme"
		}
		doc := fmt.Sprintf(`{"id":%d,"tenant":%q,"status":%q,"padding":%q}`, i, tenant, status, strings.Repeat("x", i*70))
		if i == 9 {
			doc = fmt.Sprintf(`{"id":%d,"tenant":%q,"status":"ac\u0074ive","padding":%q}`, i, tenant, strings.Repeat("x", 900))
		}
		if _, err := fs.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	needle := func(src string) vibejson.Index {
		t.Helper()
		needed, err := vibejson.RequiredIndexEntries([]byte(src))
		if err != nil {
			t.Fatal(err)
		}
		index, err := vibejson.BuildIndex([]byte(src), make([]vibejson.IndexEntry, needed))
		if err != nil {
			t.Fatal(err)
		}
		return index
	}
	active := needle(`"active"`)
	acme := needle(`"acme"`)
	countMasks := func(masks []store.Mask) int {
		count := 0
		for _, mask := range masks {
			count += bits.OnesCount64(mask.Bits)
		}
		return count
	}
	masks, err := collectionIndexMasks(fs, nil, "status", active)
	if err != nil || countMasks(masks) != 4 {
		t.Fatalf("active masks = (%+v,%v), count %d", masks, err, countMasks(masks))
	}
	certifiedSnapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var certifiedWorkspace IndexWorkspace
	masks, err = certifiedSnapshot.AppendIndexMasksInto(
		masks[:0], &certifiedWorkspace, "status", active,
	)
	if err != nil || countMasks(masks) != 4 {
		t.Fatalf("certified active masks = (%+v,%v)", masks, err)
	}
	if stats := certifiedWorkspace.LastProbeStats(); stats.CertificateRows != 4 ||
		stats.DocumentRecheckRows != 0 || stats.MatchedRows != 4 {
		t.Fatalf("online certificate stats = %+v", stats)
	}
	if err := certifiedSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	compound, err := collectionIndexMasks(fs, nil, "tenant_status", acme, active)
	if err != nil || countMasks(compound) != 2 {
		t.Fatalf("compound masks = (%+v,%v), count %d", compound, err, countMasks(compound))
	}
	old, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var indexWorkspace IndexWorkspace
	bufferedMasks := make([]store.Mask, 0, 4)
	bufferedMasks, err = old.AppendIndexMasksInto(
		bufferedMasks[:0], &indexWorkspace, "tenant_status", acme, active,
	)
	if err != nil || countMasks(bufferedMasks) != 2 {
		t.Fatalf("buffered compound masks = (%+v,%v)", bufferedMasks, err)
	}
	bufferedMasks, err = old.AppendIndexCandidateMasksInto(
		bufferedMasks[:0], &indexWorkspace, "tenant_status", acme, active,
	)
	if err != nil || countMasks(bufferedMasks) != 2 {
		t.Fatalf("buffered compound candidates = (%+v,%v)", bufferedMasks, err)
	}
	if _, err := fs.Put([]byte("k00"), []byte(`{"id":0,"tenant":"acme","status":"idle"}`)); err != nil {
		t.Fatal(err)
	}
	if ok, err := fs.Delete([]byte("k06")); err != nil || !ok {
		t.Fatalf("Delete indexed row = (%v,%v)", ok, err)
	}
	masks, err = collectionIndexMasks(fs, masks[:0], "status", active)
	if err != nil || countMasks(masks) != 2 {
		t.Fatalf("updated active masks = (%+v,%v), count %d", masks, err, countMasks(masks))
	}
	oldMasks, err := old.AppendIndexMasks(nil, "status", active)
	if err != nil || countMasks(oldMasks) != 4 {
		t.Fatalf("old snapshot masks = (%+v,%v), count %d", oldMasks, err, countMasks(oldMasks))
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	masks, err = collectionIndexMasks(reopened, nil, "status", active)
	if err != nil || countMasks(masks) != 2 {
		t.Fatalf("reopened active masks = (%+v,%v), count %d", masks, err, countMasks(masks))
	}
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	masks, err = reopenedSnapshot.AppendIndexMasksInto(
		masks[:0], &certifiedWorkspace, "status", active,
	)
	if err != nil || countMasks(masks) != 2 {
		t.Fatalf("reopened certified active masks = (%+v,%v)", masks, err)
	}
	if stats := certifiedWorkspace.LastProbeStats(); stats.CertificateRows != 2 ||
		stats.DocumentRecheckRows != 0 || stats.MatchedRows != 2 {
		t.Fatalf("reopened certificate stats = %+v", stats)
	}
	if err := reopenedSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := options
	wrong.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/tenant"}}, options.Indexes[1]}
	if _, err := Open(file, wrong); err == nil {
		t.Fatal("Open accepted a mismatched index catalog")
	}
}

func TestFileSnapshotRangeMasksRawOrderedAndBuffered(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-mask-range-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	for i := range 10 {
		padding := ""
		if i == 9 {
			padding = strings.Repeat("x", 1024)
		}
		doc := []byte(fmt.Sprintf(`{"id":%d,"padding":%q}`, i, padding))
		if _, err := fs.Put([]byte(fmt.Sprintf("k%02d", i)), doc); err != nil {
			t.Fatal(err)
		}
	}
	if deleted, err := fs.Delete([]byte("k01")); err != nil || !deleted {
		t.Fatalf("Delete(k01) = (%v,%v)", deleted, err)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	// Slots are assigned from the store's random hash seed, so the masks that name
	// k00, k03, and k09 are discovered from the live layout rather than hardcoded.
	// A full-quadrant sweep of the single populated bucket yields every row's
	// stable location; the three wanted keys' locations then build the selecting
	// masks, grouped and ordered by chunk exactly as an index probe emits them.
	locations := make(map[string]store.Location)
	scratch := make([]byte, 0, 2048)
	sweep := []store.Mask{
		{Chunk: 0, Bits: ^uint64(0)}, {Chunk: 1, Bits: ^uint64(0)},
		{Chunk: 2, Bits: ^uint64(0)}, {Chunk: 3, Bits: ^uint64(0)},
	}
	scratch, err = snapshot.RangeMasksRawRowsBuffer(
		sweep, scratch,
		func(row store.Location, key, _ []byte) error {
			locations[string(key)] = row
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	selectMasks := func(keys ...string) []store.Mask {
		byChunk := make(map[uint32]uint64)
		for _, key := range keys {
			loc, ok := locations[key]
			if !ok {
				t.Fatalf("no location for %q", key)
			}
			byChunk[loc.Chunk] |= uint64(1) << loc.Slot
		}
		chunks := make([]uint32, 0, len(byChunk))
		for chunk := range byChunk {
			chunks = append(chunks, chunk)
		}
		slices.Sort(chunks)
		out := make([]store.Mask, 0, len(chunks))
		for _, chunk := range chunks {
			out = append(out, store.Mask{Chunk: chunk, Bits: byChunk[chunk]})
		}
		return out
	}

	var keys []string
	overflowValueLen := 0
	scratch, err = snapshot.RangeMasksRawBuffer(
		selectMasks("k00", "k03", "k09"), scratch[:0],
		func(key, value []byte) error {
			keys = append(keys, string(key))
			if len(value) == 0 {
				t.Fatal("empty value")
			}
			if string(key) == "k09" {
				overflowValueLen = len(value)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(keys, ","), "k00,k03,k09"; got != want {
		t.Fatalf("masked key order = %q, want %q", got, want)
	}
	if overflowValueLen < 1024 {
		t.Fatalf("resolved overflow value length = %d, want at least 1024", overflowValueLen)
	}

	var serialKeys []string
	scratch, err = snapshot.RangeRawBuffer(scratch[:0], func(key, _ []byte) error {
		serialKeys = append(serialKeys, string(key))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeReadAhead := fs.Stats()
	var readAheadKeys []string
	scratch, err = snapshot.RangeRawBuffer(scratch[:0], func(key, _ []byte) error {
		readAheadKeys = append(readAheadKeys, string(key))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(readAheadKeys, ","), strings.Join(serialKeys, ","); got != want {
		t.Fatalf("read-ahead order = %q, want %q", got, want)
	}
	if after := fs.Stats(); after.PrefetchQueued != beforeReadAhead.PrefetchQueued {
		t.Fatalf("buffered read-ahead should use the serial kernel-readahead lane: before=%+v after=%+v", beforeReadAhead, after)
	}
	if err := snapshot.RangeMasksRaw(
		[]store.Mask{{Chunk: 2, Bits: 1}, {Chunk: 2, Bits: 2}},
		func(_, _ []byte) error { return nil },
	); !errors.Is(err, store.ErrMaskOrder) {
		t.Fatalf("duplicate chunk error = %v, want %v", err, store.ErrMaskOrder)
	}
	if err := snapshot.RangeMasksRaw(
		[]store.Mask{{Chunk: 99, Bits: 1}},
		func(_, _ []byte) error { return nil },
	); !errors.Is(err, store.ErrMaskChunk) {
		t.Fatalf("unknown chunk error = %v, want %v", err, store.ErrMaskChunk)
	}

	steady := selectMasks("k00", "k05", "k09")
	visitBytes := 0
	visit := func(key, value []byte) error {
		visitBytes += len(key) + len(value)
		return nil
	}
	scratch, err = snapshot.RangeMasksRawBuffer(steady, scratch[:0], visit)
	if err != nil {
		t.Fatal(err)
	}
	if cap(scratch) < 2048 || visitBytes == 0 {
		t.Fatalf("masked steady scan returned scratch capacity %d and visited %d bytes", cap(scratch), visitBytes)
	}
}

func TestFileStoreExactIndexWorkspaceAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-index-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.BufferCount = 1024
	options.Indexes = []store.IndexDefinition{
		{Name: "tenant_status", Paths: []string{"/tenant", "/status"}},
	}
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	for row := range 8 {
		document := fmt.Appendf(nil, `{"tenant":"acme","status":"active","row":%d}`, row)
		if _, err := fs.Put([]byte(fmt.Sprintf("k%d", row)), document); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	needle := func(src string) vibejson.Index {
		needed, err := vibejson.RequiredIndexEntries([]byte(src))
		if err != nil {
			t.Fatal(err)
		}
		index, err := vibejson.BuildIndex([]byte(src), make([]vibejson.IndexEntry, needed))
		if err != nil {
			t.Fatal(err)
		}
		return index
	}
	acme, active := needle(`"acme"`), needle(`"active"`)
	var workspace IndexWorkspace
	masks := make([]store.Mask, 0, 2)
	masks, err = snapshot.AppendIndexMasksInto(masks, &workspace, "tenant_status", acme, active)
	if err != nil || len(masks) == 0 {
		t.Fatalf("warm exact probe = (%+v,%v)", masks, err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var runErr error
		masks, runErr = snapshot.AppendIndexMasksInto(masks[:0], &workspace, "tenant_status", acme, active)
		if runErr != nil || len(masks) == 0 {
			panic("exact probe failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed AppendIndexMasksInto allocated %.2f times, want 0", allocs)
	}
	// CandidateChunks counts the succinct candidate groups the probe walked. On the
	// ordered primary those groups are hash-distributed, so how many the eight
	// matching rows fall across depends on the per-process hash seed and is not a
	// deterministic property of the query. The row-level counts and posting-page
	// count are exact and are what the probe must get right.
	if stats := workspace.LastProbeStats(); stats.CandidateRows != 8 ||
		stats.CertificateRows != 8 || stats.DocumentRecheckRows != 0 ||
		stats.MatchedRows != 8 || stats.PostingPages != 1 {
		t.Fatalf("exact probe stats = %+v", stats)
	}
	masks, err = snapshot.AppendIndexCandidateMasksInto(masks[:0], &workspace, "tenant_status", acme, active)
	if err != nil || len(masks) == 0 {
		t.Fatalf("warm candidate probe = (%+v,%v)", masks, err)
	}
	allocs = testing.AllocsPerRun(100, func() {
		var runErr error
		masks, runErr = snapshot.AppendIndexCandidateMasksInto(masks[:0], &workspace, "tenant_status", acme, active)
		if runErr != nil || len(masks) == 0 {
			panic("candidate probe failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed AppendIndexCandidateMasksInto allocated %.2f times, want 0", allocs)
	}
	// CandidateChunks is hash-seed-dependent on the ordered primary (see the exact
	// probe assertion above), so only the exact row and posting-page counts are
	// pinned.
	if stats := workspace.LastProbeStats(); stats.CandidateRows != 8 ||
		stats.PostingPages != 1 {
		t.Fatalf("candidate probe stats = %+v", stats)
	}
}

func TestFileSnapshotRangeBufferAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-range-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	// The masked range needs real stable-slot masks. On the ordered primary a row's
	// slot is its key hash, not a chunk ordinal, so hand-built chunk coordinates
	// address nothing; the masks are taken from an exact index query instead.
	options.Indexes = []store.IndexDefinition{{Name: "grp", Paths: []string{"/grp"}}}
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	for row := range 10 {
		padding := ""
		if row == 9 {
			padding = strings.Repeat("x", 1024)
		}
		document := fmt.Appendf(nil, `{"row":%d,"grp":"g","padding":%q}`, row, padding)
		if _, err := fs.Put([]byte(fmt.Sprintf("k%02d", row)), document); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	// Every row shares grp="g", so the index resolves masks covering all ten rows.
	needed, err := vibejson.RequiredIndexEntries([]byte(`"g"`))
	if err != nil {
		t.Fatal(err)
	}
	grp, err := vibejson.BuildIndex([]byte(`"g"`), make([]vibejson.IndexEntry, needed))
	if err != nil {
		t.Fatal(err)
	}
	masks, err := snapshot.AppendIndexMasks(nil, "grp", grp)
	if err != nil || len(masks) == 0 {
		t.Fatalf("index masks = (%+v,%v)", masks, err)
	}
	scratch := make([]byte, 0, 2048)
	visitBytes := 0
	visit := func(key, value []byte) error {
		visitBytes += len(key) + len(value)
		return nil
	}
	scratch, err = snapshot.RangeMasksRawBuffer(masks, scratch, visit)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		visitBytes = 0
		var runErr error
		scratch, runErr = snapshot.RangeMasksRawBuffer(masks, scratch[:0], visit)
		if runErr != nil || visitBytes == 0 {
			panic("masked range failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed RangeMasksRawBuffer allocated %.2f times, want 0", allocs)
	}
	if err := snapshot.RangeMasksRaw(masks, visit); err != nil {
		t.Fatal(err)
	}
	allocs = testing.AllocsPerRun(100, func() {
		visitBytes = 0
		if runErr := snapshot.RangeMasksRaw(masks, visit); runErr != nil ||
			visitBytes == 0 {
			panic("masked convenience range failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed RangeMasksRaw allocated %.2f times, want 0", allocs)
	}
	allocs = testing.AllocsPerRun(100, func() {
		visitBytes = 0
		var runErr error
		scratch, runErr = snapshot.RangeRawBuffer(scratch[:0], visit)
		if runErr != nil || visitBytes == 0 {
			panic("read-ahead range failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed RangeRawBuffer allocated %.2f times, want 0", allocs)
	}
}

// Given documents just past the inline threshold, when they are written, then
// each overflow extent is sized to its piece rather than to MaxPageSize.
//
// A value one byte over InlineValueBytes needs a single overflow piece of ~513
// bytes. Allocating MaxPageSize for it — which is what the writer did before
// overflowPageSize existed — spent a 64 KiB extent on 577 bytes of payload, so
// the file grew by 64 KiB per document on exactly the sizes that overflow
// first. The bound below is deliberately far tighter than MaxPageSize and far
// looser than the payload, so it fails on a regression to fixed-size extents
// without pinning the test to incidental metadata growth.
func TestFileStoreOverflowExtentsMatchTheirPiece(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-overflow-size-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	before, err := file.Seek(0, 2)
	if err != nil {
		t.Fatal(err)
	}

	// One byte past InlineValueBytes, so the value takes the overflow path by
	// the narrowest possible margin.
	const documents = 8
	body := strings.Repeat("v", options.InlineValueBytes+1-len(`{"v":""}`))
	value := []byte(`{"v":"` + body + `"}`)
	if len(value) <= options.InlineValueBytes {
		t.Fatalf("fixture value is %d bytes, not past the %d-byte inline threshold",
			len(value), options.InlineValueBytes)
	}
	for i := range documents {
		if _, err := fs.Put([]byte(fmt.Sprintf("k%02d", i)), value); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	after, err := file.Seek(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	grown := after - before
	fixedFrame := int64(documents) * int64(options.MaxPageSize)
	t.Logf("%d overflow documents of %d bytes grew the file by %d bytes (%d per document); "+
		"a fixed MaxPageSize extent each would be %d",
		documents, len(value), grown, grown/documents, fixedFrame)
	if grown >= fixedFrame {
		t.Fatalf("file grew %d bytes for %d overflow documents; a fixed MaxPageSize extent "+
			"per document would be %d, so the extents are not sized to their piece",
			grown, documents, fixedFrame)
	}

	// Every document must still read back exactly, so the smaller extent is a
	// space change and not a truncation.
	for i := range documents {
		got, ok, err := fs.AppendRaw(nil, []byte(fmt.Sprintf("k%02d", i)))
		if err != nil || !ok {
			t.Fatalf("AppendRaw(k%02d) = (%v,%v)", i, ok, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("AppendRaw(k%02d) returned %d bytes, want %d", i, len(got), len(value))
		}
	}
}

// Given bounded write-resource pressure while a snapshot pins both historical
// cache frames and the reclamation floor, when the snapshot is released, then
// writes recover.
//
// Refusing a write while its old graph remains observable is correct bounded
// backpressure. Depending on the exact cache/free-arena geometry, either the
// retirement metadata or the page cache can report its limit first. The defect
// was that the collection never recovered after the pin went away: reclamation
// declined the entire batch whenever the pending set exceeded the room left in
// the reusable arena, removing the one process that could create room. Only a
// restart recovered the collection, abandoning every pending extent.
//
// The unpinned warm-up leaves free extents resident so the integration test
// still traverses that stalled-drain geometry even when cache admission is the
// first component to surface the shared snapshot pin.
func TestFileStoreRecoversAfterPinnedResourcePressureClears(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-reclaim-pressure-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.MaxRetiredExtents = 1024
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	body := strings.Repeat("x", 512)
	put := func(round, i int) error {
		_, err := fs.Put([]byte(fmt.Sprintf("k%02d", i)), fmt.Appendf(nil,
			`{"round":%d,"v":%q}`, round, body))
		return err
	}
	const keys = 16
	for round := range 200 {
		for i := range keys {
			if err := put(round, i); err != nil {
				t.Fatalf("warm-up Put failed at round %d: %v", round, err)
			}
		}
	}
	if len(fs.reusable) == 0 {
		t.Skip("warm-up left no resident free extents; the arena geometry no longer reproduces the defect")
	}

	// Churn under a pinned snapshot until one bounded resource is exhausted.
	// Reaching that point is expected backpressure, not the defect under test.
	pinned, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	pressured := false
	var pressureErr error
	for round := 200; round < 900 && !pressured; round++ {
		for i := range keys {
			if err := put(round, i); err != nil {
				if !errors.Is(err, storeio.ErrRetiredExtentCapacity) &&
					!errors.Is(err, storeio.ErrPageCachePinned) {
					_ = pinned.Close()
					t.Fatalf("unexpected Put failure under a pinned snapshot: %v", err)
				}
				pressured = true
				pressureErr = err
				break
			}
		}
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if !pressured {
		t.Skip("pinned churn reached no bounded-resource pressure; the arena geometry no longer reproduces it")
	}
	t.Logf("pinned write pressure: %v", pressureErr)

	// The pin is gone, so cache eviction and reclamation must resume and writes
	// must succeed. Before the fix every one of these failed permanently.
	for round := 1000; round < 1016; round++ {
		for i := range keys {
			if err := put(round, i); err != nil {
				t.Fatalf("Put still failing at round %d after the snapshot was released: %v; "+
					"bounded resources did not recover once the pin cleared", round, err)
			}
		}
	}

	// Every document must still read back, so the resumed reclamation did not
	// hand out space that was still live.
	for i := range keys {
		key := fmt.Sprintf("k%02d", i)
		got, ok, err := fs.AppendRaw(nil, []byte(key))
		if err != nil || !ok {
			t.Fatalf("AppendRaw(%s) = (%v,%v)", key, ok, err)
		}
		if !bytes.Contains(got, []byte(`"round":1015`)) {
			t.Fatalf("AppendRaw(%s) returned a stale document: %s", key, got)
		}
	}
}
