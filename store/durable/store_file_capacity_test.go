package durable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func cappedAsyncFileStoreOptions(t testing.TB) (Options, uint64) {
	t.Helper()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.RecoveryJournal = false
	options.PhysicalCapacityBytes = 0
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	initial, err := initialCollectionPhysicalFileEnd(normalized)
	if err != nil {
		t.Fatal(err)
	}
	options.PhysicalCapacityBytes = initial + 256*uint64(options.PageSize)
	return options, initial
}

func TestPhysicalCapacityOptionValidation(t *testing.T) {
	base, initial := cappedAsyncFileStoreOptions(t)
	for name, mutate := range map[string]func(*Options){
		"unaligned": func(options *Options) {
			options.PhysicalCapacityBytes++
		},
		"below initial graph": func(options *Options) {
			options.PhysicalCapacityBytes = initial - uint64(options.PageSize)
		},
		"sync lane": func(options *Options) {
			options.Durability = DurabilitySync
		},
		"buffered lane": func(options *Options) {
			options.Durability = DurabilityBufferedVisible
		},
		"recovery journal": func(options *Options) {
			options.RecoveryJournal = true
		},
		"canonical materialization": func(options *Options) {
			options.MaterializationDamageGranule = 512
		},
	} {
		t.Run(name, func(t *testing.T) {
			options := base
			mutate(&options)
			if _, err := options.normalized(); !errors.Is(err, ErrPhysicalCapacity) {
				t.Fatalf("normalized = %v, want %v", err, ErrPhysicalCapacity)
			}
		})
	}
}

func TestInitialCollectionPhysicalFileEndMatchesCreate(t *testing.T) {
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	indexed := testFileStoreOptions()
	indexed.Indexes = []store.IndexDefinition{{Name: "id", Paths: []string{"/id"}}}
	for _, tc := range []struct {
		name    string
		options Options
	}{
		{name: "plain", options: testFileStoreOptions()},
		{name: "schema catalog", options: func() Options {
			options := testFileStoreOptions()
			options.Collection.Schema = testDurableStoreSchema(t)
			return options
		}()},
		{name: "indexed catalog", options: indexed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := tc.options
			options.Durability = DurabilityAsyncVisible
			options.RecoveryJournal = false
			normalized, err := options.normalized()
			if err != nil {
				t.Fatal(err)
			}
			want, err := initialCollectionPhysicalFileEnd(normalized)
			if err != nil {
				t.Fatal(err)
			}
			options.PhysicalCapacityBytes = want + 32*uint64(options.PageSize)
			file, err := os.CreateTemp(t.TempDir(), "physical-capacity-initial-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			if got := collection.visibleState.Load().fileEnd; got != want {
				t.Fatalf("Create high-water = %d, helper = %d", got, want)
			}
			if got := collection.PhysicalHighWaterBytes(); got != want {
				t.Fatalf("physical certificate = %d, helper = %d", got, want)
			}
			if err := collection.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPhysicalCapacityCreateAndEnsureWithFaults(t *testing.T) {
	options, initial := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	allocations := 0
	syncs := 0
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		allocations++
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error {
		syncs++
		return file.Sync()
	}

	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if got := uint64(info.Size()); got != initial || got == options.PhysicalCapacityBytes {
		t.Fatalf("created apparent EOF = %d, want initial %d below ceiling %d", got, initial, options.PhysicalCapacityBytes)
	}
	if collection.PhysicalCapacityBytes() != options.PhysicalCapacityBytes ||
		collection.PhysicalHighWaterBytes() != initial {
		t.Fatalf("capacity accessors = (%d,%d), want (%d,%d)", collection.PhysicalCapacityBytes(), collection.PhysicalHighWaterBytes(), options.PhysicalCapacityBytes, initial)
	}
	stats := collection.Stats()
	if stats.PhysicalCapacityBytes != options.PhysicalCapacityBytes ||
		stats.PhysicalHighWaterBytes != initial || !collection.holePunchDisabled {
		t.Fatalf("capacity profile = (%d,%d) holeDisabled=%v", stats.PhysicalCapacityBytes, stats.PhysicalHighWaterBytes, collection.holePunchDisabled)
	}
	punchCalls := 0
	collection.holePunch = func(*os.File, uint64, uint64) (bool, error) {
		punchCalls++
		return true, nil
	}
	if collection.punchFileStoreExtent(storeio.FreeExtent{
		Offset: initial, Length: uint64(options.PageSize), RetiredGeneration: 1,
	}) || punchCalls != 0 {
		t.Fatalf("capped collection hole punch = calls %d", punchCalls)
	}

	target := initial + 4*uint64(options.PageSize)
	failedSync := errors.New("allocation sync failed")
	fileStoreCapacityOps.sync = func(*os.File) error {
		syncs++
		return failedSync
	}
	if err := collection.EnsurePhysicalAllocation(target); !errors.Is(err, failedSync) || !errors.Is(err, ErrPhysicalCapacity) {
		t.Fatalf("failed Ensure = %v, want typed sync failure", err)
	}
	if collection.PhysicalHighWaterBytes() != initial {
		t.Fatalf("failed Ensure advanced certificate to %d", collection.PhysicalHighWaterBytes())
	}
	info, err = file.Stat()
	if err != nil || uint64(info.Size()) != target {
		t.Fatalf("orphan apparent EOF = (%d,%v), want %d", info.Size(), err, target)
	}

	allocationsBeforeRetry := allocations
	fileStoreCapacityOps.sync = func(file *os.File) error {
		syncs++
		return file.Sync()
	}
	if err := collection.EnsurePhysicalAllocation(initial + uint64(options.PageSize)); err != nil {
		t.Fatal(err)
	}
	if allocations != allocationsBeforeRetry+1 || collection.PhysicalHighWaterBytes() != target {
		t.Fatalf("smaller retry = allocations %d->%d high-water %d, want one repeat and %d", allocationsBeforeRetry, allocations, collection.PhysicalHighWaterBytes(), target)
	}

	if err := collection.EnsurePhysicalAllocation(options.PhysicalCapacityBytes + uint64(options.PageSize)); !errors.Is(err, ErrPhysicalCapacity) {
		t.Fatalf("over-ceiling Ensure = %v, want %v", err, ErrPhysicalCapacity)
	}
}

func TestPhysicalCapacityAllocatorPartialGrowthRetryAndReopen(t *testing.T) {
	options, initial := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-partial-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	pageSize := uint64(options.PageSize)
	requested := initial + 4*pageSize
	partial := initial + 2*pageSize + 1
	allocatorErr := syscall.ENOSPC
	allocationCalls := 0
	fileStoreCapacityOps.allocate = func(file *os.File, _, _ int64) error {
		allocationCalls++
		if err := file.Truncate(int64(partial)); err != nil {
			return err
		}
		return allocatorErr
	}
	if err := collection.EnsurePhysicalAllocation(requested); !errors.Is(err, ErrPhysicalCapacity) ||
		!errors.Is(err, allocatorErr) {
		t.Fatalf("partial allocator failure = %v, want typed ENOSPC", err)
	}
	if got := collection.PhysicalHighWaterBytes(); got != initial {
		t.Fatalf("partial allocator failure advanced certificate to %d, want %d", got, initial)
	}
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		allocationCalls++
		return file.Truncate(target)
	}
	if err := collection.EnsurePhysicalAllocation(initial + pageSize); err != nil {
		t.Fatal(err)
	}
	rounded := initial + 3*pageSize
	if got := collection.PhysicalHighWaterBytes(); got != rounded {
		t.Fatalf("partial allocator retry certificate = %d, want rounded %d", got, rounded)
	}
	if allocationCalls != 2 {
		t.Fatalf("partial allocator retry calls = %d, want 2", allocationCalls)
	}

	// Repeat the partial extension and close without an in-process retry. Open
	// must repair, fence, and adopt the rounded apparent prefix before recovery.
	orphan := rounded + pageSize + 1
	fileStoreCapacityOps.allocate = func(file *os.File, _, _ int64) error {
		if err := file.Truncate(int64(orphan)); err != nil {
			return err
		}
		return allocatorErr
	}
	if err := collection.EnsurePhysicalAllocation(rounded + 3*pageSize); !errors.Is(err, allocatorErr) {
		t.Fatalf("reopen orphan fixture = %v, want ENOSPC", err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	reopened, err := Open(file, Options{Durability: DurabilityAsyncVisible})
	if err != nil {
		t.Fatal(err)
	}
	wantReopened := rounded + 2*pageSize
	if got := reopened.PhysicalHighWaterBytes(); got != wantReopened {
		t.Fatalf("reopened orphan certificate = %d, want %d", got, wantReopened)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSettleApparentPhysicalHighWaterRejectsBelowBoundAndRoundedCeiling(t *testing.T) {
	const pageSize = uint64(4096)
	for _, tc := range []struct {
		name                 string
		apparent, lower, cap uint64
	}{
		{name: "below certificate", apparent: 2 * pageSize, lower: 3 * pageSize, cap: 8 * pageSize},
		{name: "round past ceiling", apparent: 8*pageSize - 1, lower: 3 * pageSize, cap: 8*pageSize - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := settleApparentPhysicalHighWater(
				tc.apparent, tc.lower, tc.cap, pageSize,
			); !errors.Is(err, ErrPhysicalCapacity) {
				t.Fatalf("settle = %v, want %v", err, ErrPhysicalCapacity)
			}
		})
	}
}

func TestPhysicalCapacityCreateAllocationFailurePublishesNothing(t *testing.T) {
	options, _ := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-enospc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(*os.File, int64, int64) error {
		return syscall.ENOSPC
	}
	if _, err := Create(file, options); !errors.Is(err, ErrPhysicalCapacity) || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Create = %v, want typed ENOSPC", err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("allocation failure changed file size to %d", info.Size())
	}
}

func TestPhysicalCapacityBulkOrdersProvisioningBeforePublication(t *testing.T) {
	options, _ := cappedAsyncFileStoreOptions(t)
	records := []PrimaryBulkRecord{{
		Key: "bulk-capacity", Value: []byte(`{"value":"sealed"}`),
	}}
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fenceErr := errors.New("bulk allocation fence failed")
	fileStoreCapacityOps.sync = func(*os.File) error { return fenceErr }
	failed, err := os.CreateTemp(t.TempDir(), "physical-capacity-bulk-failed-*")
	if err != nil {
		t.Fatal(err)
	}
	defer failed.Close()
	if _, err := CreateFromRecords(records, failed, options); !errors.Is(err, ErrPhysicalCapacity) || !errors.Is(err, fenceErr) {
		t.Fatalf("bulk sync failure = %v, want typed fence failure", err)
	}
	rootPrefix := make([]byte, 2*options.PageSize)
	if _, err := failed.ReadAt(rootPrefix, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rootPrefix, make([]byte, len(rootPrefix))) {
		t.Fatal("bulk allocation sync failure published a root")
	}
	layout, err := storeio.MutableStoreLayout(uint32(options.PageSize))
	if err != nil {
		t.Fatal(err)
	}
	catalogPrefix := make([]byte, options.PageSize)
	if _, err := failed.ReadAt(catalogPrefix, int64(layout.DataStart)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(catalogPrefix, make([]byte, len(catalogPrefix))) {
		t.Fatal("bulk allocation sync failure wrote the catalog")
	}

	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	success, err := os.CreateTemp(t.TempDir(), "physical-capacity-bulk-success-*")
	if err != nil {
		t.Fatal(err)
	}
	defer success.Close()
	fileEnd, err := CreateFromRecords(records, success, options)
	if err != nil {
		t.Fatal(err)
	}
	if fileEnd <= 0 || uint64(fileEnd) >= options.PhysicalCapacityBytes {
		t.Fatalf("bulk fileEnd = %d, ceiling %d", fileEnd, options.PhysicalCapacityBytes)
	}
	reopened, err := Open(success, Options{Durability: DurabilityAsyncVisible})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := reopened.AppendRaw(nil, []byte("bulk-capacity"))
	if err != nil || !found || string(got) != `{"value":"sealed"}` {
		t.Fatalf("bulk reopen = (%q,%v,%v)", got, found, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalCapacityOpenRestoresCertificateAndRejectsMismatch(t *testing.T) {
	options, initial := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-open-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	target := initial + 8*uint64(options.PageSize)
	if err := collection.EnsurePhysicalAllocation(target); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	allocationCalls := 0
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		allocationCalls++
		return file.Truncate(target)
	}
	if _, err := Open(file, Options{}); !errors.Is(err, ErrPhysicalCapacity) {
		t.Fatalf("default-lane Open = %v, want %v", err, ErrPhysicalCapacity)
	}
	if allocationCalls != 0 {
		t.Fatalf("wrong-lane Open performed %d allocation operations", allocationCalls)
	}

	mismatch := options
	mismatch.PhysicalCapacityBytes += uint64(options.PageSize)
	if _, err := Open(file, mismatch); err == nil {
		t.Fatal("Open accepted a mismatched sealed ceiling")
	}
	reopened, err := Open(file, Options{Durability: DurabilityAsyncVisible})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.PhysicalCapacityBytes() != options.PhysicalCapacityBytes ||
		reopened.PhysicalHighWaterBytes() != target {
		t.Fatalf("reopened capacity = (%d,%d), want (%d,%d)", reopened.PhysicalCapacityBytes(), reopened.PhysicalHighWaterBytes(), options.PhysicalCapacityBytes, target)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalCapacityOpenRejectsJournaledRootBeforeAllocation(t *testing.T) {
	options, _ := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-journal-root-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	layout, err := storeio.MutableStoreLayout(uint32(options.PageSize))
	if err != nil {
		t.Fatal(err)
	}
	const (
		inlineStateAt       = 96
		stateJournalIDAt    = 144
		inlineChecksumBytes = 8
	)
	for _, offset := range layout.RootOffsets {
		inline := make([]byte, storeio.InlineSuperblockSize)
		if _, err := file.ReadAt(inline, int64(offset)); err != nil {
			t.Fatal(err)
		}
		inline[inlineStateAt+stateJournalIDAt] = 1
		trailer := len(inline) - inlineChecksumBytes
		checksum := storeio.PageChecksum(inline[:trailer])
		binary.LittleEndian.PutUint32(inline[trailer:trailer+4], checksum)
		binary.LittleEndian.PutUint32(inline[trailer+4:], ^checksum)
		if _, err := file.WriteAt(inline, int64(offset)); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	allocationCalls := 0
	fileStoreCapacityOps.allocate = func(*os.File, int64, int64) error {
		allocationCalls++
		return nil
	}
	if _, err := Open(file, Options{Durability: DurabilityAsyncVisible}); err == nil {
		t.Fatal("Open accepted a capped root with a recovery-journal identity")
	}
	if allocationCalls != 0 {
		t.Fatalf("invalid journaled capped root performed %d allocation calls", allocationCalls)
	}
}

func TestEnsurePhysicalAllocationRejectsElasticCollection(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-elastic-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if err := collection.EnsurePhysicalAllocation(uint64(options.PageSize)); !errors.Is(err, ErrPhysicalCapacity) {
		t.Fatalf("elastic Ensure = %v, want %v", err, ErrPhysicalCapacity)
	}
}

func TestPhysicalCapacityPlatformSupportIsExplicit(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux has strict allocation support")
	}
	options, _ := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-platform-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := Create(file, options); !errors.Is(err, ErrPhysicalCapacity) {
		t.Fatalf("sealed Create on %s = %v, want %v", runtime.GOOS, err, ErrPhysicalCapacity)
	}
}

func TestCappedRootedMutationRefusesWithoutStateChange(t *testing.T) {
	options, initial := cappedAsyncFileStoreOptions(t)
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-mutation-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	production := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = production }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	generation, documents := collection.Generation(), collection.Len()
	if _, err := collection.Put([]byte("capped"), []byte(`{"value":1}`)); !errors.Is(err, ErrPhysicalCapacity) {
		t.Fatalf("Put without Ensure = %v, want %v", err, ErrPhysicalCapacity)
	}
	if collection.Generation() != generation || collection.Len() != documents ||
		collection.PhysicalHighWaterBytes() != initial {
		t.Fatalf("capacity refusal mutated state: generation=%d documents=%d high-water=%d", collection.Generation(), collection.Len(), collection.PhysicalHighWaterBytes())
	}
	reserve := initial + uint64(collection.options.maxTransactionPages)*
		uint64(collection.options.MaxPageSize)
	if reserve > options.PhysicalCapacityBytes {
		reserve = options.PhysicalCapacityBytes
	}
	if err := collection.EnsurePhysicalAllocation(reserve); err != nil {
		t.Fatal(err)
	}
	if created, err := collection.Put([]byte("capped"), []byte(`{"value":1}`)); err != nil || !created {
		t.Fatalf("Put after Ensure = (%v,%v)", created, err)
	}
}

func TestEnsurePhysicalAllocationDrainsInflightCommit(t *testing.T) {
	options, _ := cappedAsyncFileStoreOptions(t)
	options.CommitCoalesce = time.Second
	file, err := os.CreateTemp(t.TempDir(), "physical-capacity-inflight-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	productionCapacity := fileStoreCapacityOps
	defer func() { fileStoreCapacityOps = productionCapacity }()
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		return file.Truncate(target)
	}
	fileStoreCapacityOps.sync = func(file *os.File) error { return file.Sync() }
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	pageSize := uint64(options.PageSize)
	if err := collection.EnsurePhysicalAllocation(
		options.PhysicalCapacityBytes - pageSize,
	); err != nil {
		t.Fatal(err)
	}
	allocationEntered := make(chan struct{}, 1)
	allocationBeforeDurable := errors.New("allocation ran before published cut was durable")
	var published uint64
	fileStoreCapacityOps.allocate = func(file *os.File, _, target int64) error {
		allocationEntered <- struct{}{}
		if collection.committer.DurableGeneration() < published {
			return allocationBeforeDurable
		}
		return file.Truncate(target)
	}
	if _, err := collection.Put([]byte("inflight"), []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	published = collection.committer.PublishedGeneration()
	if durable := collection.committer.DurableGeneration(); durable >= published {
		t.Fatalf(
			"coalesced commit settled before Ensure fixture: durable=%d published=%d",
			durable, published,
		)
	}
	target := collection.PhysicalHighWaterBytes() + pageSize
	done := make(chan error, 1)
	go func() { done <- collection.EnsurePhysicalAllocation(target) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Ensure did not finish after coalesced commit settled")
	}
	select {
	case <-allocationEntered:
	default:
		t.Fatal("Ensure never entered strict allocation after the flush")
	}
	if collection.PhysicalHighWaterBytes() != target {
		t.Fatalf("post-drain high-water = %d, want %d", collection.PhysicalHighWaterBytes(), target)
	}
}
