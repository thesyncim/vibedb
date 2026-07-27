package storeio

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCommitterPortableLifecycle(t *testing.T) {
	committer, file, pageSize := newPortableCommitter(t, 6, 2)

	page0 := []byte("generation-one-page")
	root0 := []byte("generation-one-root")
	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: page0}}, 0, root0)
	page1 := []byte("generation-two-page")
	root1 := []byte("generation-two-root")
	publishTestGeneration(t, committer, 2,
		[]testPage{{offset: int64(2 * pageSize), data: page1}}, 0, root1)
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := committer.PublishedGeneration(); got != 2 {
		t.Fatalf("published generation = %d, want 2", got)
	}
	if got := committer.DurableGeneration(); got != 2 {
		t.Fatalf("durable generation = %d, want 2", got)
	}
	stats := committer.Stats()
	if stats.Backend != BackendPortable || stats.CommittedBatches != 2 ||
		stats.DeviceCommits < 1 || stats.DeviceCommits > 2 || stats.LargestGroup < 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	assertFileBytes(t, file, int64(pageSize), page0)
	assertFileBytes(t, file, int64(2*pageSize), page1)
	assertFileBytes(t, file, 0, root1)

	page2 := []byte("generation-three-page")
	root2 := []byte("generation-three-root")
	publishTestGeneration(t, committer, 3,
		[]testPage{{offset: int64(3 * pageSize), data: page2}}, 0, root2)
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	assertFileBytes(t, file, int64(3*pageSize), page2)
	assertFileBytes(t, file, 0, root2)
	if err := committer.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := committer.Begin(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Begin after Close = %v, want %v", err, ErrClosed)
	}
}

func TestCommitterOptionsValidation(t *testing.T) {
	normalized, err := (CommitterOptions{QueueSlots: 3}).normalized(8)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.QueueSlots != 4 || normalized.MaxPagesPerBatch != 7 || normalized.GroupLimit != 4 {
		t.Fatalf("normalized options = %+v", normalized)
	}
	tests := []CommitterOptions{
		{QueueSlots: -1},
		{QueueSlots: 1<<16 + 1},
		{MaxPagesPerBatch: -1},
		{MaxPagesPerBatch: 8},
		{QueueSlots: 1 << 16, MaxPagesPerBatch: 17},
		{QueueSlots: 4, GroupLimit: -1},
		{QueueSlots: 4, GroupLimit: 5},
		{ManualCheckpoint: true, MaterializationDamageGranule: 512},
	}
	for _, options := range tests {
		if _, err := options.normalized(8); err == nil {
			t.Fatalf("options %+v unexpectedly accepted", options)
		}
	}
}

func TestCommitterInitializeRecoveredGeneration(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 2, 0)
	defer committer.Close()
	if err := committer.InitializeGeneration(41); err != nil {
		t.Fatal(err)
	}
	if committer.PublishedGeneration() != 41 || committer.DurableGeneration() != 41 {
		t.Fatalf("initialized generations = %d/%d", committer.PublishedGeneration(), committer.DurableGeneration())
	}
	if err := committer.InitializeGeneration(42); !errors.Is(err, ErrGenerationOrder) {
		t.Fatalf("second InitializeGeneration = %v, want %v", err, ErrGenerationOrder)
	}
	batch, err := committer.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := batch.RootBuffer()
	clear(root)
	root[0] = 1
	if err := batch.SetRoot(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(42); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(42); err != nil {
		t.Fatal(err)
	}
}

func TestCommitterAlternatesFromRecoveredPhysicalRootAcrossGroupedGenerations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "grouped-root-slots")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device := newRecordingDevice(4, pageSize)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 4, BufferSize: pageSize,
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: 0, GroupLimit: 4},
		func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	if err := committer.InitializeGenerationAt(1, 0); err != nil {
		t.Fatal(err)
	}

	publish := func(generation uint64) {
		t.Helper()
		batch, beginErr := committer.Begin(0)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		state := []byte{byte(generation)}
		root := testSuperblock(
			generation, testMutableStoreDataStart(uint32(pageSize)), state,
		)
		root.PageSize = uint32(pageSize)
		root.FileEnd = root.StateOffset + uint64(pageSize)
		if setErr := batch.SetSuperblock(root); setErr != nil {
			t.Fatal(setErr)
		}
		if publishErr := batch.Publish(generation); publishErr != nil {
			t.Fatal(publishErr)
		}
	}

	// Generation two starts a physical commit in slot one. While it is held,
	// generations three and four queue and collapse under generation four.
	// The grouped root must use slot zero even though generation four's parity
	// would select slot one and destroy the only durable fallback.
	publish(2)
	<-device.firstStarted
	publish(3)
	publish(4)
	close(device.releaseFirst)
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 2 {
		t.Fatalf("device commits = %d, want 2", len(commits))
	}
	if got, want := commits[0].rootOffset, int64(pageSize); got != want {
		t.Fatalf("generation two root offset = %d, want %d", got, want)
	}
	if got := commits[1].rootOffset; got != 0 {
		t.Fatalf("grouped generation four root offset = %d, want 0", got)
	}
	if got := committer.FallbackGeneration(); got != 2 {
		t.Fatalf("grouped generation four fallback = %d, want 2", got)
	}
}

func TestCommitterGroupedSuperblockTearRecoversPreviousPhysicalCommit(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "grouped-root-recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 4, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 1, GroupLimit: 4,
		CoalesceDelay: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	publish := func(generation uint64) {
		t.Helper()
		batch, beginErr := committer.Begin(1)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		statePage, bufferErr := batch.PageBuffer(0)
		if bufferErr != nil {
			t.Fatal(bufferErr)
		}
		stateOffset := testMutableStoreDataStart(uint32(pageSize)) +
			(generation-1)*uint64(pageSize)
		fileEnd := stateOffset + uint64(pageSize)
		state := StateRoot{
			StoreID: testStoreID, Generation: generation, PageSize: uint32(pageSize),
			MaxPageSize: uint32(pageSize), NextLogicalID: 2, ChunkDocuments: 64,
		}
		if _, encodeErr := EncodeStateRootPage(statePage, state, fileEnd); encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if setErr := batch.setStorePage(
			0, int64(stateOffset), pageSize, PageStateRoot,
		); setErr != nil {
			t.Fatal(setErr)
		}
		super := Superblock{
			StoreID: testStoreID, Generation: generation,
			StateOffset: stateOffset, StateLength: uint32(pageSize),
			StateChecksum: PageChecksum(statePage[:pageSize]),
			FileEnd:       fileEnd, PageSize: uint32(pageSize),
		}
		if setErr := batch.SetSuperblock(super); setErr != nil {
			t.Fatal(setErr)
		}
		if publishErr := batch.Publish(generation); publishErr != nil {
			t.Fatal(publishErr)
		}
	}

	publish(1)
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	publish(2)
	publish(3)
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := committer.FallbackGeneration(); got != 1 {
		t.Fatalf("grouped generation three fallback = %d, want 1", got)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}

	scratch := make([]byte, pageSize)
	_, newest, slot, fallbackGeneration, err := RecoverStateRootWithFallback(
		file, uint32(pageSize), scratch,
	)
	if err != nil || newest.Generation != 3 || slot != 1 || fallbackGeneration != 1 {
		t.Fatalf(
			"recover grouped root = generation %d slot %d fallback %d err %v",
			newest.Generation, slot, fallbackGeneration, err,
		)
	}
	var corrupt [1]byte
	if _, err := file.ReadAt(corrupt[:], int64(pageSize)); err != nil {
		t.Fatal(err)
	}
	corrupt[0] ^= 1
	writeAtTest(t, file, corrupt[:], int64(pageSize))
	_, recovered, slot, fallbackGeneration, err := RecoverStateRootWithFallback(
		file, uint32(pageSize), scratch,
	)
	if err != nil || recovered.Generation != 1 || slot != 0 || fallbackGeneration != 1 {
		t.Fatalf(
			"recover torn grouped root = generation %d slot %d fallback %d err %v",
			recovered.Generation, slot, fallbackGeneration, err,
		)
	}
}

func TestCommitterBatchValidationRetainsOwnership(t *testing.T) {
	committer, _, pageSize := newPortableCommitter(t, 3, 1)
	defer committer.Close()

	batch, err := committer.Begin(1)
	if err != nil {
		t.Fatal(err)
	}
	page, err := batch.PageBuffer(0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := batch.RootBuffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(page, "page")
	copy(root, "root")
	if err := batch.SetPage(0, int64(pageSize), 0); err != nil {
		t.Fatal(err)
	}
	if err := batch.SetRoot(0, 4); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("invalid Publish = %v, want %v", err, ErrInvalidWrite)
	}
	if err := batch.SetPage(0, int64(pageSize), 4); err != nil {
		t.Fatalf("batch ownership lost after validation error: %v", err)
	}
	if err := batch.Publish(1); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.RootBuffer(); !errors.Is(err, ErrBatchState) {
		t.Fatalf("published RootBuffer = %v, want %v", err, ErrBatchState)
	}

	repeated, err := committer.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	root, err = repeated.RootBuffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(root, "next")
	if err := repeated.SetRoot(0, 4); err != nil {
		t.Fatal(err)
	}
	if err := repeated.Publish(1); !errors.Is(err, ErrGenerationOrder) {
		t.Fatalf("repeated generation = %v, want %v", err, ErrGenerationOrder)
	}
	if err := repeated.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := repeated.Abort(); !errors.Is(err, ErrBatchState) {
		t.Fatalf("second Abort = %v, want %v", err, ErrBatchState)
	}
	if _, err := committer.Begin(2); !errors.Is(err, ErrTooManyPages) {
		t.Fatalf("oversize Begin = %v, want %v", err, ErrTooManyPages)
	}
}

func TestCommitterBatchResizeReturnsUnusedBuffers(t *testing.T) {
	committer, _, pageSize := newPortableCommitter(t, 4, 3)
	defer committer.Close()
	batch, err := committer.Begin(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.ResizePages(1); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.PageBuffer(1); !errors.Is(err, ErrBatchState) {
		t.Fatalf("released PageBuffer = %v, want %v", err, ErrBatchState)
	}
	page, err := batch.PageBuffer(0)
	if err != nil {
		t.Fatal(err)
	}
	clear(page)
	page[0] = 1
	if err := batch.SetPage(0, int64(2*pageSize), pageSize); err != nil {
		t.Fatal(err)
	}
	root, err := batch.RootBuffer()
	if err != nil {
		t.Fatal(err)
	}
	clear(root)
	root[0] = 2
	if err := batch.SetRoot(0, 128); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(1); err != nil {
		t.Fatal(err)
	}
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	// The two returned page buffers plus the completed batch capacity make a
	// worst-case reservation available again.
	next, err := committer.Begin(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitterStickyFailure(t *testing.T) {
	committer, file, pageSize := newPortableCommitter(t, 2, 1)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte("page")}}, 0, []byte("root"))
	waitErr := committer.Wait(1)
	if waitErr == nil {
		t.Fatal("Wait succeeded after closing persistence file")
	}
	if _, err := committer.Begin(0); err != waitErr {
		t.Fatalf("Begin error = %v, want sticky %v", err, waitErr)
	}
	if err := committer.Close(); err != waitErr {
		t.Fatalf("Close error = %v, want sticky %v", err, waitErr)
	}
}

func TestCommitterConcurrentPublishAndClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		committer, file, _ := newPortableCommitter(t, 1, 0)
		batch, err := committer.Begin(0)
		if err != nil {
			t.Fatal(err)
		}
		root, err := batch.RootBuffer()
		if err != nil {
			t.Fatal(err)
		}
		copy(root, "root")
		if err := batch.SetRoot(0, 4); err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- committer.Close() }()
		publishErr := batch.Publish(1)
		closeErr := <-closed
		if publishErr == nil {
			if closeErr != nil {
				t.Fatalf("iteration %d Close: %v", iteration, closeErr)
			}
			if got := committer.DurableGeneration(); got != 1 {
				t.Fatalf("iteration %d durable = %d after accepted Publish", iteration, got)
			}
			assertFileBytes(t, file, 0, []byte("root"))
			continue
		}
		if !errors.Is(publishErr, ErrClosed) {
			t.Fatalf("iteration %d Publish = %v, want nil or %v", iteration, publishErr, ErrClosed)
		}
		if err := batch.Abort(); err != nil {
			t.Fatalf("iteration %d Abort: %v", iteration, err)
		}
		if closeErr != nil {
			t.Fatalf("iteration %d Close: %v", iteration, closeErr)
		}
	}
}

func TestCommitterBackpressureWakesOnRecycle(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 1, 0)
	defer committer.Close()
	held, err := committer.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	type beginResult struct {
		batch *Batch
		err   error
	}
	started := make(chan struct{})
	result := make(chan beginResult, 1)
	go func() {
		close(started)
		batch, err := committer.Begin(0)
		result <- beginResult{batch: batch, err: err}
	}()
	<-started
	select {
	case got := <-result:
		t.Fatalf("Begin bypassed exhausted buffer budget: %+v", got)
	case <-time.After(10 * time.Millisecond):
	}
	if err := held.Abort(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if err := got.batch.Abort(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Begin did not wake after buffer recycle")
	}
}

func TestCommitterGroupsQueuedGenerationsUnderLatestRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "group-commit")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device := newRecordingDevice(6, pageSize)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 6, BufferSize: pageSize,
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: 1, GroupLimit: 4},
		func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte("page-1")}}, 0, []byte("root-1"))
	<-device.firstStarted
	publishTestGeneration(t, committer, 2,
		[]testPage{{offset: int64(2 * pageSize), data: []byte("page-2")}}, 0, []byte("root-2"))
	publishTestGeneration(t, committer, 3,
		[]testPage{{offset: int64(3 * pageSize), data: []byte("page-3")}}, 0, []byte("root-3"))
	close(device.releaseFirst)
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}

	commits := device.snapshot()
	if len(commits) != 2 {
		t.Fatalf("device commits = %d, want 2", len(commits))
	}
	if commits[0].root != "root-1" || len(commits[0].pages) != 1 {
		t.Fatalf("first commit = %+v", commits[0])
	}
	if commits[1].root != "root-3" || len(commits[1].pages) != 2 {
		t.Fatalf("grouped commit = %+v", commits[1])
	}
	if commits[1].pages[0].Offset != int64(2*pageSize) ||
		commits[1].pages[1].Offset != int64(3*pageSize) {
		t.Fatalf("grouped writes not physically ordered: %+v", commits[1].pages)
	}
	stats := committer.Stats()
	if stats.DeviceCommits != 2 || stats.CommittedBatches != 3 || stats.LargestGroup != 2 {
		t.Fatalf("unexpected grouped stats: %+v", stats)
	}
}

func TestCommitterCoalesceWindowGroupsActiveProducer(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "coalesce-commit")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device := newRecordingDevice(8, pageSize)
	close(device.releaseFirst)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 8, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 8, MaxPagesPerBatch: 1, GroupLimit: 4,
		CoalesceDelay: 10 * time.Millisecond,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	for generation := uint64(1); generation <= 3; generation++ {
		publishTestGeneration(t, committer, generation,
			[]testPage{{offset: int64(generation+1) * int64(pageSize), data: []byte{byte(generation)}}},
			0, []byte(fmt.Sprintf("root-%d", generation)))
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 || commits[0].root != "root-3" || len(commits[0].pages) != 3 {
		t.Fatalf("coalesced commits = %+v, want one three-generation commit", commits)
	}
}

func TestCommitterCoalesceSuppressesIntermediateStatePages(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "coalesce-state-pages")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device := newRecordingDevice(12, pageSize)
	close(device.releaseFirst)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 12, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 2, GroupLimit: 4,
		CoalesceDelay: 10 * time.Millisecond,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	storeID := [16]byte{1}
	for generation := uint64(1); generation <= 3; generation++ {
		batch, beginErr := committer.Begin(2)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		data, bufferErr := batch.PageBuffer(0)
		if bufferErr != nil {
			t.Fatal(bufferErr)
		}
		copy(data, []byte{byte(generation)})
		if err := batch.SetPage(
			0, int64((generation*2+8)*uint64(pageSize)), 1,
		); err != nil {
			t.Fatal(err)
		}
		state, bufferErr := batch.PageBuffer(1)
		if bufferErr != nil {
			t.Fatal(bufferErr)
		}
		if _, initErr := InitPage(state, PageHeader{
			StoreID: storeID, Generation: generation,
			LogicalID: StateRootLogicalID,
			PageSize:  uint32(pageSize), Kind: PageStateRoot,
		}); initErr != nil {
			t.Fatal(initErr)
		}
		if _, sealErr := SealPage(state[:pageSize]); sealErr != nil {
			t.Fatal(sealErr)
		}
		if err := batch.setStorePage(
			1, int64((generation*2+9)*uint64(pageSize)),
			pageSize, PageStateRoot,
		); err != nil {
			t.Fatal(err)
		}
		root, bufferErr := batch.RootBuffer()
		if bufferErr != nil {
			t.Fatal(bufferErr)
		}
		root[0] = byte(generation)
		if err := batch.SetRoot(0, 1); err != nil {
			t.Fatal(err)
		}
		if err := batch.Publish(generation); err != nil {
			t.Fatal(err)
		}
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 || len(commits[0].pages) != 4 {
		t.Fatalf(
			"coalesced state-page writes = %+v, want 3 data + 1 state",
			commits,
		)
	}
	statePages := 0
	for _, write := range commits[0].pages {
		if write.kind == PageStateRoot {
			statePages++
		}
	}
	if statePages != 1 {
		t.Fatalf("durable state pages = %d, want 1", statePages)
	}
	stats := committer.Stats()
	if stats.SuppressedRootWrites != 2 ||
		stats.SuppressedRootBytes != 2*uint64(pageSize) {
		t.Fatalf("suppressed state-page stats = %+v", stats)
	}
}

type testPage struct {
	offset int64
	data   []byte
}

type recordedCommit struct {
	pages      []Write
	root       string
	rootOffset int64
}

type recordingDevice struct {
	buffers      [][]byte
	bufferSize   int
	seen         []uint64
	firstStarted chan struct{}
	releaseFirst chan struct{}
	mu           sync.Mutex
	commits      []recordedCommit
}

func newRecordingDevice(bufferCount, bufferSize int) *recordingDevice {
	buffers := make([][]byte, bufferCount)
	for i := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}
	return &recordingDevice{
		buffers:      buffers,
		bufferSize:   bufferSize,
		seen:         make([]uint64, (bufferCount+63)/64),
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
}

func (*recordingDevice) Backend() Backend { return BackendPortable }

func (d *recordingDevice) Buffer(index int) ([]byte, error) {
	if index < 0 || index >= len(d.buffers) {
		return nil, ErrInvalidWrite
	}
	return d.buffers[index], nil
}

func (d *recordingDevice) Commit(pages []Write, root Write) error {
	if err := validateCommit(len(d.buffers), d.bufferSize, d.seen, pages, root); err != nil {
		return err
	}
	d.mu.Lock()
	call := len(d.commits)
	record := recordedCommit{
		pages:      append([]Write(nil), pages...),
		root:       string(d.buffers[root.Buffer][:root.Length]),
		rootOffset: root.Offset,
	}
	d.commits = append(d.commits, record)
	d.mu.Unlock()
	if call == 0 {
		close(d.firstStarted)
		<-d.releaseFirst
	}
	return nil
}

func (*recordingDevice) Close() error { return nil }

func (d *recordingDevice) snapshot() []recordedCommit {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]recordedCommit(nil), d.commits...)
}

func newPortableCommitter(t *testing.T, buffers, maxPages int) (*Committer, *os.File, int) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "committer")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	pageSize := os.Getpagesize()
	bufferSize := max(pageSize, 2*int(testSuperblockPageSize))
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: buffers, BufferSize: bufferSize,
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: maxPages, GroupLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	return committer, file, pageSize
}

func publishTestGeneration(t *testing.T, committer *Committer, generation uint64,
	pages []testPage, rootOffset int64, root []byte,
) {
	t.Helper()
	batch, err := committer.Begin(len(pages))
	if err != nil {
		t.Fatal(err)
	}
	for i, page := range pages {
		buffer, err := batch.PageBuffer(i)
		if err != nil {
			t.Fatal(err)
		}
		copy(buffer, page.data)
		if err := batch.SetPage(i, page.offset, len(page.data)); err != nil {
			t.Fatal(err)
		}
	}
	buffer, err := batch.RootBuffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(buffer, root)
	if err := batch.SetRoot(rootOffset, len(root)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Publish(generation); err != nil {
		t.Fatal(err)
	}
}
