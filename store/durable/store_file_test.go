package durable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func testFileStoreOptions() Options {
	return Options{
		Collection: store.Options{ChunkDocuments: 4},
		PageSize:   4096, MaxPageSize: 64 << 10, ResidentBytes: 4 << 20,
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
	options.ResidentBytes = int64(normalized.maxTransactionBytes)
	if _, err := options.normalized(); err != nil {
		t.Fatalf("exact dirty budget rejected: %v", err)
	}
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
	options.MaxRetiredExtents = 1<<24 + 1
	if _, err := options.normalized(); err == nil {
		t.Fatal("retirement capacity beyond allocator limit accepted")
	}
	options = testFileStoreOptions()
	options.MaxRetiredExtents = 1<<24
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
	options = testFileStoreOptions()
	options.Float64Columns = []string{"/score", "/score"}
	if _, err := options.normalized(); err == nil {
		t.Fatal("duplicate float64 covering column accepted")
	}
	options = testFileStoreOptions()
	options.Float64Columns = []string{"not-an-rfc6901-pointer"}
	if _, err := options.normalized(); err == nil {
		t.Fatal("invalid float64 covering path accepted")
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
	if _, err := fs.Put("direct:key", []byte(`{"mode":"observable"}`)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reopened.AppendRaw(make([]byte, 0, 64), "direct:key")
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
	if got, ok, err := fs.AppendRaw(nil, "missing"); err != nil || ok || got != nil {
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

func TestFileStoreOpenDiscoversPageSize(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-page-discovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.PageSize = 16 << 10
	options.ResidentBytes = 64 << 20
	options.MaxRetiredExtents = 4096
	options.BufferCount = 4096
	created, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.Put("discovered", []byte(`{"page":"size"}`)); err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}

	reopen := options
	reopen.PageSize = 0
	opened, err := Open(file, reopen)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	got, ok, err := opened.AppendRaw(nil, "discovered")
	if err != nil || !ok || string(got) != `{"page":"size"}` {
		t.Fatalf("discovered page-size read = (%q, %v, %v)", got, ok, err)
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
	if created, err := fs.Put("held", []byte(`{"value":1}`)); err != nil || !created {
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
	if created, err := fs.Put("held", []byte(`{"value":2}`)); err != nil || created {
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

// Given concurrent synchronous writers, when they all return, then every write
// is durable, the published and durable generations agree, and the elided root
// writes are accounted for exactly — and across a few attempts at least one
// batch is observed coalescing.
//
// The split between what is checked every attempt and what is only required
// once is the point. Durability, the generation count, and the root-write
// accounting are invariants: they hold whatever the scheduler does, so a single
// violation is a bug. Coalescing is not an invariant. Two writers share a device
// commit only if they are both inside the commit queue at the same moment, and
// nothing in Put makes that happen — under GOMAXPROCS=1, or on a loaded machine
// where each goroutine is descheduled before the next is admitted, sixteen
// writers legitimately produce sixteen groups of one.
//
// Asserting the optimisation per attempt made this test fail roughly one run in
// four under load, and because it failed before its Close the leaked writer
// lease then failed every later durable test in the package. So the optimisation
// is asserted over the run rather than over one attempt: it still fails if
// grouping is impossible, and it no longer fails because grouping did not happen
// to occur.
func TestFileStoreDurabilitySyncWritersShareFence(t *testing.T) {
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 1
	options.BufferCount = 1024
	options.QueueSlots = 32
	options.GroupLimit = 16
	options.CommitCoalesce = 10 * time.Millisecond
	const (
		writers  = 16
		attempts = 8
	)
	grouped, largest := 0, uint32(0)
	for attempt := range attempts {
		file, err := os.CreateTemp(t.TempDir(), "file-fs-sync-group-*")
		if err != nil {
			t.Fatal(err)
		}
		fs, err := Create(file, options)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		// Closing through cleanup rather than at the end of the body is what keeps
		// a failure here local: the writer lease is process-global, so a store left
		// open by a t.Fatal takes every later test in the package with it.
		closed := false
		t.Cleanup(func() {
			if !closed {
				_ = fs.Close()
			}
			_ = file.Close()
		})

		start := make(chan struct{})
		errs := make(chan error, writers)
		for writer := range writers {
			go func() {
				<-start
				key := fmt.Sprintf("writer:%02d", writer)
				created, putErr := fs.Put(key, []byte(fmt.Sprintf(`{"writer":%d}`, writer)))
				if putErr != nil || !created {
					errs <- fmt.Errorf("Put(%s) = (%v,%v)", key, created, putErr)
					return
				}
				errs <- nil
			}()
		}
		close(start)
		for range writers {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		stats := fs.Stats()
		if stats.DocumentCount != writers || stats.CommittedBatches != writers+1 ||
			stats.DurableGeneration != stats.PublishedGeneration ||
			stats.SuppressedRootBytes !=
				stats.SuppressedRootWrites*uint64(options.PageSize) {
			t.Fatalf("attempt %d: synchronous group commit did not converge: %+v",
				attempt, stats)
		}
		// State lives inside the one selected fixed root, so grouping no
		// longer stages intermediate PageStateRoot writes to suppress.
		if stats.SuppressedRootWrites != 0 || stats.SuppressedRootBytes != 0 {
			t.Fatalf("attempt %d: inline roots reported suppressed state pages: %+v",
				attempt, stats)
		}
		if stats.LargestCommitGroup >= 2 {
			grouped++
		}
		largest = max(largest, stats.LargestCommitGroup)

		if err := fs.Close(); err != nil {
			t.Fatal(err)
		}
		closed = true
		reopened, err := Open(file, options)
		if err != nil {
			t.Fatal(err)
		}
		if reopened.Len() != writers {
			_ = reopened.Close()
			t.Fatalf("attempt %d: reopened documents = %d, want %d",
				attempt, reopened.Len(), writers)
		}
		for writer := range writers {
			key := fmt.Sprintf("writer:%02d", writer)
			got, ok, readErr := reopened.AppendRaw(nil, key)
			if readErr != nil || !ok || string(got) != fmt.Sprintf(`{"writer":%d}`, writer) {
				_ = reopened.Close()
				t.Fatalf("attempt %d: %s after reopen = (%q,%v,%v)",
					attempt, key, got, ok, readErr)
			}
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Coalescing needs two writers inside the commit queue at the same moment,
	// and with a single P nothing can put them there: a writer that publishes then
	// waits hands the P back, and whether another writer is admitted before the
	// committer drains depends entirely on what else is runnable. Measured on a
	// sixteen-core machine under a 40-process CPU load: at GOMAXPROCS=16 every one
	// of fifteen runs coalesced on all eight attempts, while at GOMAXPROCS=1 a
	// full shuffled package run coalesced on none of eight in four runs out of
	// six. So the requirement is stated against the configuration that can satisfy
	// it, and the invariants above are still checked at any GOMAXPROCS.
	if runtime.GOMAXPROCS(0) < 2 {
		t.Logf("GOMAXPROCS is 1, so two writers cannot be in the commit queue at "+
			"once; checked the durability invariants over %d attempts without "+
			"requiring coalescing", attempts)
		return
	}
	if grouped == 0 {
		t.Fatalf("no attempt out of %d coalesced two synchronous writers into one "+
			"device commit (largest group seen: %d), so group commit is not merely "+
			"unlucky here — it is not happening", attempts, largest)
	}
	t.Logf("%d of %d attempts coalesced, largest group %d", grouped, attempts, largest)
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
		created, putErr := fs.Put(key, []byte(value))
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
	created, err := fs.Put("key-01", []byte(large))
	if err != nil || created {
		t.Fatalf("update = (%v,%v), want existing", created, err)
	}
	want["key-01"] = large
	if got, ok, err := snapshot.AppendRaw(nil, "key-01"); err != nil || !ok || string(got) != old {
		t.Fatalf("old snapshot = (%q,%v,%v), want %q", got, ok, err, old)
	}
	if got, ok, err := fs.AppendRaw(nil, "key-01"); err != nil || !ok || string(got) != large {
		t.Fatalf("current overflow = (%d bytes,%v,%v), want %d bytes", len(got), ok, err, len(large))
	}
	deleted, err := fs.Delete("key-02")
	if err != nil || !deleted {
		t.Fatalf("Delete existing = (%v,%v)", deleted, err)
	}
	delete(want, "key-02")
	if deleted, err := fs.Delete("key-02"); err != nil || deleted {
		t.Fatalf("Delete missing = (%v,%v)", deleted, err)
	}
	if got, ok, err := snapshot.AppendRaw(nil, "key-02"); err != nil || !ok || string(got) == "" {
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
	queued, err := reopened.PrefetchKeys([]string{"key-09", "key-00", "missing", "key-05", "key-01"})
	if err != nil || queued == 0 {
		t.Fatalf("PrefetchKeys = (%d,%v)", queued, err)
	}
	if stats := reopened.Stats(); stats.PrefetchQueued < uint64(queued) || stats.CapacityBytes == 0 || stats.DocumentCount != uint64(len(want)) {
		t.Fatalf("Stats after prefetch = %+v", stats)
	}
	for key, value := range want {
		got, ok, getErr := reopened.AppendRaw(nil, key)
		if getErr != nil || !ok || string(got) != value {
			t.Fatalf("reopened %q = (%q,%v,%v), want %q", key, got, ok, getErr, value)
		}
	}
	if got, ok, err := reopened.AppendRaw(nil, "key-02"); err != nil || ok || got != nil {
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
	if _, err := fs.Put("bad", []byte(`{"unterminated":`)); err == nil {
		t.Fatal("Put invalid JSON succeeded")
	}
	if fs.Generation() != generation || fs.Len() != 0 {
		t.Fatalf("invalid Put published generation %d len %d", fs.Generation(), fs.Len())
	}
	if _, err := fs.Put(strings.Repeat("k", fs.options.MaxKeyBytes+1), []byte(`null`)); !errors.Is(err, ErrKeyTooLarge) {
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
	options.MaxRetiredExtents = 1024
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if _, err := fs.Put("hot", []byte(`{"version":0}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	beforePinned := fs.state.Load().super.FileEnd
	for version := 1; version <= 20; version++ {
		if _, err := fs.Put("hot", []byte(fmt.Sprintf(`{"version":%d}`, version))); err != nil {
			t.Fatal(err)
		}
	}
	afterPinned := fs.state.Load().super.FileEnd
	if afterPinned <= beforePinned {
		t.Fatalf("active snapshot did not fence reuse: fileEnd %d -> %d", beforePinned, afterPinned)
	}
	if got, ok, err := snapshot.AppendRaw(nil, "hot"); err != nil || !ok || string(got) != `{"version":0}` {
		t.Fatalf("pinned value after churn = (%q,%v,%v)", got, ok, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	for version := 21; version <= 40; version++ {
		if _, err := fs.Put("hot", []byte(fmt.Sprintf(`{"version":%d}`, version))); err != nil {
			t.Fatal(err)
		}
	}
	plateau := fs.state.Load().super.FileEnd
	for version := 41; version <= 80; version++ {
		if _, err := fs.Put("hot", []byte(fmt.Sprintf(`{"version":%d}`, version))); err != nil {
			t.Fatal(err)
		}
	}
	if got := fs.state.Load().super.FileEnd; got != plateau {
		t.Fatalf("copy-on-write file did not plateau: %d -> %d", plateau, got)
	}
	if got, ok, err := fs.AppendRaw(nil, "hot"); err != nil || !ok || string(got) != `{"version":80}` {
		t.Fatalf("latest value = (%q,%v,%v)", got, ok, err)
	}
}

func TestFileStorePersistsReusableExtentsAcrossReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-free-log-*")
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
	if _, err := fs.Put("hot", []byte(`0`)); err != nil {
		t.Fatal(err)
	}
	for version := 1; version <= 30; version++ {
		if _, err := fs.Put("hot", []byte(fmt.Sprintf(`%d`, version))); err != nil {
			t.Fatal(err)
		}
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
	if _, err := reopened.Put("hot", []byte(`31`)); err != nil {
		t.Fatal(err)
	}
	if !reopened.freeLoaded {
		t.Fatal("first mutation did not lazily replay the bounded free log")
	}
	for version := 32; version <= 50; version++ {
		if _, err := reopened.Put("hot", []byte(fmt.Sprintf(`%d`, version))); err != nil {
			t.Fatal(err)
		}
	}
	plateau := reopened.Stats().FileEnd
	for version := 51; version <= 80; version++ {
		if _, err := reopened.Put("hot", []byte(fmt.Sprintf(`%d`, version))); err != nil {
			t.Fatal(err)
		}
	}
	if got := reopened.Stats().FileEnd; got != plateau {
		t.Fatalf("reopened allocator did not plateau: %d -> %d", plateau, got)
	}
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
		if _, err := fs.Put(fmt.Sprintf("k%02d", i), []byte(doc)); err != nil {
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
	masks, err := fs.AppendIndexMasks(nil, "status", active)
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
	compound, err := fs.AppendIndexMasks(nil, "tenant_status", acme, active)
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
	if _, err := fs.Put("k00", []byte(`{"id":0,"tenant":"acme","status":"idle"}`)); err != nil {
		t.Fatal(err)
	}
	if ok, err := fs.Delete("k06"); err != nil || !ok {
		t.Fatalf("Delete indexed row = (%v,%v)", ok, err)
	}
	masks, err = fs.AppendIndexMasks(masks[:0], "status", active)
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
	masks, err = reopened.AppendIndexMasks(nil, "status", active)
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

func TestFileIndexTupleCertificateSemantics(t *testing.T) {
	leftValues := []vibejson.RawValue{
		{Src: []byte(`"active"`)},
		{Src: []byte(`1`)},
	}
	rightValues := []vibejson.RawValue{
		{Src: []byte(`"ac\u0074ive"`)},
		{Src: []byte(`1.0`)},
	}
	left, ok := appendFileIndexCertificate(nil, leftValues, 4096)
	if !ok || !fileIndexCertificateValid(left, 2) {
		t.Fatalf("left certificate = (%x,%v)", left, ok)
	}
	right, ok := appendFileIndexCertificate(nil, rightValues, 4096)
	if !ok || !fileIndexCertificateValid(right, 2) {
		t.Fatalf("right certificate = (%x,%v)", right, ok)
	}
	if !fileIndexCertificatesEqual(left, right, 2) {
		t.Fatal("semantically equal tuple certificates compared unequal")
	}
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
	if !fileIndexCertificateMatches(
		left, []vibejson.Index{needle(`"ac\u0074ive"`), needle(`1e0`)}, 2,
	) {
		t.Fatal("tuple certificate did not match equivalent query scalars")
	}
	different, ok := appendFileIndexCertificate(
		nil, []vibejson.RawValue{{Src: []byte(`"active"`)}, {Src: []byte(`2`)}}, 4096,
	)
	if !ok || fileIndexCertificatesEqual(left, different, 2) {
		t.Fatal("different tuple certificates compared equal")
	}
	corrupt := append([]byte(nil), left...)
	corrupt[4] = 0xff
	corrupt[5] = 0xff
	if fileIndexCertificateValid(corrupt, 2) ||
		fileIndexCertificateMatches(corrupt, []vibejson.Index{needle(`"active"`), needle(`1`)}, 2) {
		t.Fatal("malformed tuple certificate was accepted")
	}
	prefix := []byte("prefix")
	if got, ok := appendFileIndexCertificate(prefix, leftValues, 4); ok ||
		string(got) != "prefix" {
		t.Fatalf("bounded certificate append = (%q,%v)", got, ok)
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
		if _, err := fs.Put(fmt.Sprintf("k%02d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	if deleted, err := fs.Delete("k01"); err != nil || !deleted {
		t.Fatalf("Delete(k01) = (%v,%v)", deleted, err)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	masks := []store.Mask{
		{Chunk: 0, Bits: 1<<0 | 1<<1 | 1<<3 | 1<<63},
		{Chunk: 2, Bits: 1 << 1},
	}
	var keys []string
	scratch := make([]byte, 0, 2048)
	scratch, err = snapshot.RangeMasksRawBuffer(masks, scratch, func(key, value []byte) error {
		keys = append(keys, string(key))
		if len(value) == 0 {
			t.Fatal("empty value")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(keys, ","), "k00,k03,k09"; got != want {
		t.Fatalf("masked key order = %q, want %q", got, want)
	}
	if cap(scratch) < 1024 {
		t.Fatalf("caller overflow scratch capacity = %d, want at least 1024", cap(scratch))
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
	scratch, err = snapshot.RangeRawReadAheadBuffer(scratch[:0], func(key, _ []byte) error {
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

	steady := []store.Mask{{Chunk: 0, Bits: 1<<0 | 1<<3}, {Chunk: 2, Bits: 1 << 1}}
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
		if _, err := fs.Put(fmt.Sprintf("k%d", row), document); err != nil {
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
	if stats := workspace.LastProbeStats(); stats != (IndexProbeStats{
		CandidateRows: 8, CertificateRows: 8,
		MatchedRows: 8, CandidateChunks: 2, PostingPages: 1,
	}) {
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
	if stats := workspace.LastProbeStats(); stats != (IndexProbeStats{
		CandidateRows: 8, CandidateChunks: 2, PostingPages: 1,
	}) {
		t.Fatalf("candidate probe stats = %+v", stats)
	}
}

func TestFileStoreFloat64ColumnsMutationSnapshotReopenAndAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-float64-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Float64Columns = []string{"/score", "/nested/value"}
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for key, document := range map[string]string{
		"k0": `{"score":1.5,"nested":{"value":10}}`,
		"k1": `{"score":2,"nested":{"value":"not numeric"}}`,
		"k2": `{"score":"not numeric","nested":{"value":-3}}`,
		"k3": `{"score":1e999,"nested":null}`,
	} {
		if _, err := fs.Put(key, []byte(document)); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}
	if got := fs.Stats().Float64ScratchBytes; got != 2*(8+64*8) {
		t.Fatalf("Float64ScratchBytes = %d, want %d", got, 2*(8+64*8))
	}
	old, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertAggregate := func(snapshot *Snapshot, path string, want store.Float64Aggregate) {
		t.Helper()
		got, covered, err := snapshot.ReduceFloat64Path(path)
		if err != nil || !covered || got != want {
			t.Fatalf("ReduceFloat64Path(%q) = (%+v,%v,%v), want (%+v,true,nil)", path, got, covered, err, want)
		}
	}
	assertAggregate(old, "/score", store.Float64Aggregate{Count: 2, Sum: 3.5, Min: 1.5, Max: 2})
	assertAggregate(old, "/nested/value", store.Float64Aggregate{Count: 2, Sum: 7, Min: -3, Max: 10})
	if !old.HasFloat64Path("/nested/value") || old.HasFloat64Path("/missing") {
		t.Fatal("covering-column catalog lookup mismatch")
	}
	if got, covered, err := old.ReduceFloat64Path("/missing"); err != nil || covered || got != (store.Float64Aggregate{}) {
		t.Fatalf("unconfigured reduction = (%+v,%v,%v), want zero,false,nil", got, covered, err)
	}

	if created, err := fs.Put("k0", []byte(`{"score":4}`)); err != nil || created {
		t.Fatalf("update k0 = (%v,%v), want (false,nil)", created, err)
	}
	if deleted, err := fs.Delete("k1"); err != nil || !deleted {
		t.Fatalf("delete k1 = (%v,%v), want (true,nil)", deleted, err)
	}
	if created, err := fs.Put("k4", []byte(`{"score":-1,"nested":{"value":8}}`)); err != nil || !created {
		t.Fatalf("insert k4 = (%v,%v), want (true,nil)", created, err)
	}
	current, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertAggregate(current, "/score", store.Float64Aggregate{Count: 2, Sum: 3, Min: -1, Max: 4})
	assertAggregate(current, "/nested/value", store.Float64Aggregate{Count: 2, Sum: 5, Min: -3, Max: 8})
	paths := []string{"/score", "/nested/value"}
	totals := make([]store.Float64Aggregate, len(paths))
	if covered, err := current.ReduceFloat64PathsInto(totals, paths); err != nil || !covered ||
		totals[0] != (store.Float64Aggregate{Count: 2, Sum: 3, Min: -1, Max: 4}) ||
		totals[1] != (store.Float64Aggregate{Count: 2, Sum: 5, Min: -3, Max: 8}) {
		t.Fatalf("fused covering reductions = (%+v,%v,%v)", totals, covered, err)
	}
	// Copy-on-write publication keeps the old page and its typed columns
	// coherent for readers that already hold the preceding generation.
	assertAggregate(old, "/score", store.Float64Aggregate{Count: 2, Sum: 3.5, Min: 1.5, Max: 2})

	if _, _, err := current.ReduceFloat64Path("/score"); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		got, covered, runErr := current.ReduceFloat64Path("/score")
		if runErr != nil || !covered || got.Count != 2 || got.Sum != 3 {
			panic("covered reduction failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed ReduceFloat64Path allocated %.2f times, want 0", allocs)
	}
	allocs = testing.AllocsPerRun(100, func() {
		covered, runErr := current.ReduceFloat64PathsInto(totals, paths)
		if runErr != nil || !covered || totals[0].Sum != 3 || totals[1].Sum != 5 {
			panic("fused covered reduction failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed ReduceFloat64PathsInto allocated %.2f times, want 0", allocs)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
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
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertAggregate(reopenedSnapshot, "/score", store.Float64Aggregate{Count: 2, Sum: 3, Min: -1, Max: 4})
	assertAggregate(reopenedSnapshot, "/nested/value", store.Float64Aggregate{Count: 2, Sum: 5, Min: -3, Max: 8})
	if err := reopenedSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := options
	wrong.Float64Columns = []string{"/score"}
	if _, err := Open(file, wrong); err == nil {
		t.Fatal("Open accepted a mismatched float64 covering catalog")
	}
}

func recoveredFileDocumentRef(t *testing.T, file *os.File, options Options, chunk uint32) storeio.PageRef {
	t.Helper()
	rootScratch := make([]byte, options.PageSize)
	inline, root, _, err := storeio.RecoverInlineStateRoot(file, uint32(options.PageSize), rootScratch)
	if err != nil {
		t.Fatal(err)
	}
	ref := root.ChunkDirectory
	for {
		page := make([]byte, ref.Length)
		if _, err := file.ReadAt(page, int64(ref.Offset)); err != nil {
			t.Fatal(err)
		}
		view, err := storeio.OpenChunkDirectoryPage(page, inline.FileEnd, root.NextLogicalID)
		if err != nil {
			t.Fatal(err)
		}
		child, ok := view.Lookup(chunk)
		if !ok {
			t.Fatalf("chunk %d is absent from the recovered directory", chunk)
		}
		if view.Header().Shift == 0 {
			return child
		}
		ref = child
	}
}

func TestFileStoreFloat64ColumnRejectsResealedCorruptionOnAdmission(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-float64-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Float64Columns = []string{"/score"}
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put("key", []byte(`{"score":1.5}`)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	documentRef := recoveredFileDocumentRef(t, file, options, 0)
	page := make([]byte, documentRef.Length)
	if _, err := file.ReadAt(page, int64(documentRef.Offset)); err != nil {
		t.Fatal(err)
	}
	payloadStart := storeio.PageHeaderSize
	count := int(page[payloadStart+20])
	dataStart := payloadStart + storeio.DocumentPagePayloadHeaderSize +
		count*storeio.DocumentPageRecordSize
	dataLength := int(binary.LittleEndian.Uint32(page[payloadStart+16 : payloadStart+20]))
	valueOffset := dataStart + dataLength + 8 // Skip the first column's stable-slot mask.
	if valueOffset+8 > len(page) {
		t.Fatal("encoded float64 covering value is outside the document page")
	}
	binary.LittleEndian.PutUint64(page[valueOffset:valueOffset+8], math.Float64bits(math.Inf(1)))
	if _, err := storeio.SealPage(page); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(page, int64(documentRef.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("Open returned a fs for a corrupt append document page")
	}
	if !errors.Is(err, storeio.ErrDocumentPageCorrupt) {
		t.Fatalf("Open resealed covering corruption = %v, want document corruption", err)
	}
}

func TestFileStoreRejectsResealedInvalidInlineJSONOnAdmission(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-json-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"ok":true}`)
	if _, err := fs.Put("key", document); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	documentRef := recoveredFileDocumentRef(t, file, options, 0)
	page := make([]byte, documentRef.Length)
	if _, err := file.ReadAt(page, int64(documentRef.Offset)); err != nil {
		t.Fatal(err)
	}
	position := bytes.Index(page, document)
	if position < 0 {
		t.Fatal("inline JSON is absent from recovered document page")
	}
	copy(page[position:], `{"ok":xxxx}`)
	if _, err := storeio.SealPage(page); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(page, int64(documentRef.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("Open returned a fs for invalid inline JSON")
	}
	if !errors.Is(err, storeio.ErrDocumentPageCorrupt) {
		t.Fatalf("Open resealed invalid JSON = %v, want document corruption", err)
	}
}

func TestFileSnapshotRejectsResealedCrossChunkDocument(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-cross-chunk-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for row := range options.Collection.ChunkDocuments + 1 {
		if _, err := fs.Put(fmt.Sprintf("key-%d", row), []byte(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	documentRef := recoveredFileDocumentRef(t, file, options, 0)
	page := make([]byte, documentRef.Length)
	if _, err := file.ReadAt(page, int64(documentRef.Offset)); err != nil {
		t.Fatal(err)
	}
	// Chunk one exists, so the typed page validator accepts this in-range
	// identity. The selecting chunk-tree edge must still reject the mismatch.
	binary.LittleEndian.PutUint32(page[storeio.PageHeaderSize+4:storeio.PageHeaderSize+8], 1)
	if _, err := storeio.SealPage(page); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(page, int64(documentRef.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if _, _, err := snapshot.AppendRaw(nil, "key-0"); !errors.Is(err, storeio.ErrDocumentPageCorrupt) {
		t.Fatalf("AppendRaw cross-chunk document = %v, want document corruption", err)
	}
}

func TestFileSnapshotRangeBufferAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-range-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	for row := range 10 {
		padding := ""
		if row == 9 {
			padding = strings.Repeat("x", 1024)
		}
		document := fmt.Appendf(nil, `{"row":%d,"padding":%q}`, row, padding)
		if _, err := fs.Put(fmt.Sprintf("k%02d", row), document); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	masks := []store.Mask{{Chunk: 0, Bits: 1<<0 | 1<<3}, {Chunk: 2, Bits: 1 << 1}}
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
	allocs = testing.AllocsPerRun(100, func() {
		visitBytes = 0
		var runErr error
		scratch, runErr = snapshot.RangeRawReadAheadBuffer(scratch[:0], visit)
		if runErr != nil || visitBytes == 0 {
			panic("read-ahead range failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed RangeRawReadAheadBuffer allocated %.2f times, want 0", allocs)
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
		if _, err := fs.Put(fmt.Sprintf("k%02d", i), value); err != nil {
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
		got, ok, err := fs.AppendRaw(nil, fmt.Sprintf("k%02d", i))
		if err != nil || !ok {
			t.Fatalf("AppendRaw(k%02d) = (%v,%v)", i, ok, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("AppendRaw(k%02d) returned %d bytes, want %d", i, len(got), len(value))
		}
	}
}

// Given retirement pressure that exhausts reclamation metadata while a snapshot
// pins the reclamation floor, when the snapshot is released, then writes
// recover.
//
// Refusing a write while nothing is reclaimable is correct backpressure: a
// pinned snapshot holds the floor down, so retired extents legally cannot be
// reused yet. The defect was that the collection never recovered afterwards.
// Reclamation declined the entire batch whenever the pending set exceeded the
// room left in the reusable arena, and since that call is the only drain of the
// pending set, declining removed the one process that would have created the
// room. The condition could not clear itself, so releasing the snapshot changed
// nothing and every subsequent write failed. Only restarting the process
// recovered the collection, abandoning every pending extent.
//
// Reaching the defect needs free extents already resident in the arena: the
// guard compared the pending count against the room left, so with an empty
// arena it could never trip before the pending set hit its own capacity. Hence
// the unpinned warm-up, which leaves ~200 extents resident before the pinned
// churn begins.
func TestFileStoreRecoversAfterRetirementPressureClears(t *testing.T) {
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
		_, err := fs.Put(fmt.Sprintf("k%02d", i), fmt.Appendf(nil,
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

	// Churn under a pinned snapshot until reclamation metadata is exhausted.
	// Reaching that point is expected backpressure, not the defect under test.
	pinned, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	exhausted := false
	for round := 200; round < 900 && !exhausted; round++ {
		for i := range keys {
			if err := put(round, i); err != nil {
				if !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
					pinned.Close()
					t.Fatalf("unexpected Put failure under a pinned snapshot: %v", err)
				}
				exhausted = true
				break
			}
		}
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if !exhausted {
		t.Skip("retirement pressure never reached capacity; the arena geometry no longer reproduces it")
	}

	// The floor is free again, so reclamation must resume and writes must
	// succeed. Before the fix every one of these failed, permanently.
	for round := 1000; round < 1016; round++ {
		for i := range keys {
			if err := put(round, i); err != nil {
				t.Fatalf("Put still failing at round %d after the snapshot was released: %v; "+
					"reclamation did not resume once the floor advanced", round, err)
			}
		}
	}

	// Every document must still read back, so the resumed reclamation did not
	// hand out space that was still live.
	for i := range keys {
		key := fmt.Sprintf("k%02d", i)
		got, ok, err := fs.AppendRaw(nil, key)
		if err != nil || !ok {
			t.Fatalf("AppendRaw(%s) = (%v,%v)", key, ok, err)
		}
		if !bytes.Contains(got, []byte(`"round":1015`)) {
			t.Fatalf("AppendRaw(%s) returned a stale document: %s", key, got)
		}
	}
}
