package storeio

import (
	"errors"
	"os"
	"slices"
	"testing"
	"time"
)

func TestCommitterManualCheckpointIssuesNoDeviceIOBeforeFlush(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-checkpoint-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newRecordingDevice(8, pageSize)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 8, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}

	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte("page-1")}},
		0, []byte("root-1"))
	publishTestGeneration(t, committer, 2,
		[]testPage{{offset: int64(2 * pageSize), data: []byte("page-2")}},
		0, []byte("root-2"))

	select {
	case <-device.firstStarted:
		close(device.releaseFirst)
		t.Fatal("manual committer touched the device before Flush")
	case <-time.After(20 * time.Millisecond):
	}
	if got := committer.DurableGeneration(); got != 0 {
		t.Fatalf("durable generation before Flush = %d, want 0", got)
	}

	close(device.releaseFirst)
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 {
		t.Fatalf("device commits after Flush = %d, want 1", len(commits))
	}
	if commits[0].root != "root-2" || len(commits[0].pages) != 2 {
		t.Fatalf("checkpoint commit = %+v, want both pages under root-2", commits[0])
	}
	if got := committer.DurableGeneration(); got != 2 {
		t.Fatalf("durable generation after Flush = %d, want 2", got)
	}

	publishTestGeneration(t, committer, 3,
		[]testPage{{offset: int64(3 * pageSize), data: []byte("page-3")}},
		0, []byte("root-3"))
	if len(device.snapshot()) != 1 {
		t.Fatal("manual committer touched the device after Flush without a new checkpoint")
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	commits = device.snapshot()
	if len(commits) != 2 || commits[1].root != "root-3" {
		t.Fatalf("Close checkpoint commits = %+v, want root-3", commits)
	}
}

func TestCommitterManualCheckpointReturnsBoundedPressure(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-checkpoint-pressure-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newRecordingDevice(2, pageSize)
	close(device.releaseFirst)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 2, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	publishTestGeneration(t, committer, 1, nil, 0, []byte("root-1"))
	publishTestGeneration(t, committer, 2, nil, 0, []byte("root-2"))
	published := committer.PublishedGeneration()
	if _, err := committer.Begin(0); !errors.Is(err, ErrCheckpointRequired) {
		t.Fatalf("Begin at bounded dirty capacity = %v, want %v", err, ErrCheckpointRequired)
	}
	if got := committer.PublishedGeneration(); got != published {
		t.Fatalf("rejected admission published generation %d, want %d", got, published)
	}
	if len(device.snapshot()) != 0 {
		t.Fatal("staging pressure caused an implicit device checkpoint")
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(device.snapshot()) != 1 {
		t.Fatalf("device commits after pressure Flush = %d, want 1", len(device.snapshot()))
	}
	if committer.NeedsCheckpointFor(1) {
		t.Fatal("Flush returned before checkpoint staging was reusable")
	}
}

func TestCommitterManualCheckpointSupersedesUnreachableRootBuffers(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-root-supersession-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newRecordingDevice(2, pageSize)
	close(device.releaseFirst)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 8, GroupLimit: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	for generation := uint64(1); generation <= 8; generation++ {
		publishTestGeneration(
			t, committer, generation, nil, 0, []byte{byte(generation)},
		)
	}
	stats := committer.Stats()
	if stats.SupersededRootWrites != 7 ||
		stats.SupersededRootBytes != 7 {
		t.Fatalf("root supersession stats = %+v, want seven one-byte roots", stats)
	}
	if got := committer.freeBuffers.availableCount(); got != 1 {
		t.Fatalf(
			"free staging buffers before checkpoint = %d, want one surviving root",
			got,
		)
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 || commits[0].root != string([]byte{8}) ||
		len(commits[0].pages) != 0 {
		t.Fatalf("checkpoint commits = %+v, want only generation-eight root", commits)
	}
	stats = committer.Stats()
	if stats.CommittedBatches != 8 || stats.LargestGroup != 8 {
		t.Fatalf("checkpoint group stats = %+v, want one eight-generation cut", stats)
	}
	if got := committer.freeBuffers.availableCount(); got != 2 {
		t.Fatalf("free staging buffers after checkpoint = %d, want 2", got)
	}
}

func TestCommitterManualCheckpointSupersedesExactRetiredPages(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-page-supersession-*")
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
		QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte("old")}},
		0, []byte("root-1"))
	retirement := FreeExtent{
		Offset: uint64(pageSize), Length: 3, RetiredGeneration: 1,
	}
	superseded := publishTestGenerationRetiringCollect(t, committer, 2,
		[]testPage{{offset: int64(2 * pageSize), data: []byte("new")}},
		0, []byte("root-2"), []PageRef{
			// Overlap and same-offset/different-length are not proofs.
			{Offset: uint64(pageSize) + 1, Length: 2, Generation: 1},
			{Offset: uint64(pageSize), Length: 2, Generation: 1},
			// A duplicate exact proof must recycle the buffer only once.
			{Offset: uint64(pageSize), Length: 3, Generation: 1},
			{Offset: uint64(pageSize), Length: 3, Generation: 1},
		}, []FreeExtent{retirement}, make([]FreeExtent, 0, 2))
	if !slices.Equal(superseded, []FreeExtent{retirement}) {
		t.Fatalf(
			"superseded retirement proof = %+v, want %+v",
			superseded, retirement,
		)
	}
	stats := committer.Stats()
	if stats.SupersededPageWrites != 1 ||
		stats.SupersededPageBytes != 3 {
		t.Fatalf("page supersession stats = %+v, want one three-byte page", stats)
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 || commits[0].root != "root-2" ||
		len(commits[0].pages) != 1 ||
		commits[0].pages[0].Offset != int64(2*pageSize) {
		t.Fatalf("checkpoint = %+v, want only the newer page", commits)
	}
	if got := committer.freeBuffers.availableCount(); got != 8 {
		t.Fatalf("free buffers after checkpoint = %d, want 8", got)
	}
}

func TestCommitterManualCheckpointRetainsAndLaterReleasesTailWitness(t *testing.T) {
	t.Run("submitted", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "manual-tail-witness-*")
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
			QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
		}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer committer.Close()

		publishTestGeneration(t, committer, 1,
			[]testPage{{offset: int64(3 * pageSize), data: []byte("tail")}},
			0, []byte("root-1"))
		tailRetirement := FreeExtent{
			Offset: uint64(3 * pageSize), Length: 4,
			RetiredGeneration: 1,
		}
		superseded := publishTestGenerationRetiringCollect(t, committer, 2,
			[]testPage{{offset: int64(2 * pageSize), data: []byte("low")}},
			0, []byte("root-2"), []PageRef{{
				Offset: uint64(3 * pageSize), Length: 4, Generation: 1,
			}}, []FreeExtent{tailRetirement}, make([]FreeExtent, 0, 1))
		if len(superseded) != 0 {
			t.Fatalf("tail witness reported reusable: %+v", superseded)
		}
		if stats := committer.Stats(); stats.TailWitnessWrites != 0 {
			t.Fatalf("provisional witness counted before submission: %+v", stats)
		}
		if err := committer.Flush(); err != nil {
			t.Fatal(err)
		}
		commits := device.snapshot()
		if len(commits) != 1 || len(commits[0].pages) != 2 {
			t.Fatalf("tail checkpoint = %+v, want retained tail and low page", commits)
		}
		stats := committer.Stats()
		if stats.TailWitnessWrites != 1 || stats.TailWitnessBytes != 4 ||
			stats.SupersededPageWrites != 0 {
			t.Fatalf("submitted tail witness stats = %+v", stats)
		}
	})

	t.Run("later-extension", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "manual-tail-release-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		pageSize := os.Getpagesize()
		device := newRecordingDevice(10, pageSize)
		close(device.releaseFirst)
		committer, err := newCommitter(file, DeviceOptions{
			Backend: BackendPortable, BufferCount: 10, BufferSize: pageSize,
		}, CommitterOptions{
			QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
		}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer committer.Close()

		publishTestGeneration(t, committer, 1,
			[]testPage{{offset: int64(3 * pageSize), data: []byte("tail")}},
			0, []byte("root-1"))
		tailRetirement := FreeExtent{
			Offset: uint64(3 * pageSize), Length: 4,
			RetiredGeneration: 1,
		}
		firstProof := publishTestGenerationRetiringCollect(t, committer, 2,
			[]testPage{{offset: int64(2 * pageSize), data: []byte("low")}},
			0, []byte("root-2"), []PageRef{{
				Offset: uint64(3 * pageSize), Length: 4, Generation: 1,
			}}, []FreeExtent{tailRetirement}, make([]FreeExtent, 0, 1))
		if len(firstProof) != 0 {
			t.Fatalf("active tail witness reported reusable: %+v", firstProof)
		}
		// The non-nil empty proof says this publication also observed no active
		// snapshot. Its farther page makes generation one's retained tail
		// witness unnecessary.
		releasedProof := publishTestGenerationRetiringCollect(t, committer, 3,
			[]testPage{{offset: int64(4 * pageSize), data: []byte("high")}},
			0, []byte("root-3"), []PageRef{}, nil,
			make([]FreeExtent, 0, 1))
		if !slices.Equal(releasedProof, []FreeExtent{tailRetirement}) {
			t.Fatalf(
				"released tail proof = %+v, want %+v",
				releasedProof, tailRetirement,
			)
		}
		if err := committer.Flush(); err != nil {
			t.Fatal(err)
		}
		commits := device.snapshot()
		if len(commits) != 1 || len(commits[0].pages) != 2 {
			t.Fatalf("released-tail checkpoint = %+v, want low and high pages", commits)
		}
		for _, write := range commits[0].pages {
			if write.Offset == int64(3*pageSize) {
				t.Fatal("former tail witness reached the device")
			}
		}
		stats := committer.Stats()
		if stats.SupersededPageWrites != 1 ||
			stats.SupersededPageBytes != 4 ||
			stats.TailWitnessWrites != 0 {
			t.Fatalf("released tail stats = %+v", stats)
		}
	})
}

func TestCommitterManualPageSupersessionDoesNotCrossCheckpointCut(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-page-cut-*")
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
		QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte("old")}},
		0, []byte("root-1"))
	// Model a checkpoint request that captured generation one but whose worker
	// has not dequeued it yet. Generation two is outside that cut and cannot
	// remove a page root-one may publish.
	committer.checkpointThrough.Store(1)
	protectedRetirement := FreeExtent{
		Offset: uint64(pageSize), Length: 3, RetiredGeneration: 1,
	}
	superseded := publishTestGenerationRetiringCollect(t, committer, 2,
		[]testPage{{offset: int64(2 * pageSize), data: []byte("new")}},
		0, []byte("root-2"), []PageRef{{
			Offset: uint64(pageSize), Length: 3, Generation: 1,
		}}, []FreeExtent{protectedRetirement}, make([]FreeExtent, 0, 1))
	if len(superseded) != 0 {
		t.Fatalf("checkpoint-cut retirement reported reusable: %+v", superseded)
	}
	if got := committer.Stats().SupersededPageWrites; got != 0 {
		t.Fatalf("checkpoint cut superseded page: count=%d", got)
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 || len(commits[0].pages) != 2 {
		t.Fatalf("checkpoint = %+v, want both generations' pages", commits)
	}
}

func TestCommitterManualRootSupersessionRecyclesBuffersAfterFailure(t *testing.T) {
	persistErr := errors.New("injected checkpoint failure")
	file, err := os.CreateTemp(t.TempDir(), "manual-root-failure-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newPhaseFailureDevice(
		2, pageSize, failDataWrite, persistErr,
	)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 2, GroupLimit: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}

	publishTestGeneration(t, committer, 1, nil, 0, []byte("a"))
	publishTestGeneration(t, committer, 2, nil, 0, []byte("b"))
	if err := committer.Flush(); !errors.Is(err, persistErr) {
		t.Fatalf("Flush = %v, want %v", err, persistErr)
	}
	if got := committer.freeBuffers.availableCount(); got != 2 {
		t.Fatalf("free staging buffers after failed checkpoint = %d, want 2", got)
	}
	if stats := committer.Stats(); stats.SupersededRootWrites != 1 {
		t.Fatalf("root supersession stats after failure = %+v, want one", stats)
	}
	if err := committer.Close(); !errors.Is(err, persistErr) {
		t.Fatalf("Close = %v, want %v", err, persistErr)
	}
}

func TestCommitterManualCheckpointCapacityPreflight(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-checkpoint-capacity-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newRecordingDevice(4, pageSize)
	close(device.releaseFirst)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 4, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 3, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	if committer.NeedsCheckpointFor(3) {
		t.Fatal("empty manual committer reported checkpoint pressure")
	}
	held, err := committer.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	if !committer.NeedsCheckpointFor(3) {
		t.Fatal("preflight admitted a worst-case transaction without enough buffers")
	}
	if err := held.Abort(); err != nil {
		t.Fatal(err)
	}
	if committer.NeedsCheckpointFor(3) {
		t.Fatal("preflight did not observe recycled buffers")
	}
}

func TestCommitterManualCheckpointExcludesPublicationsAfterCut(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-checkpoint-cut-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newRecordingDevice(8, pageSize)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 8, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte("page-1")}},
		0, []byte("root-1"))
	publishTestGeneration(t, committer, 2,
		[]testPage{{offset: int64(2 * pageSize), data: []byte("page-2")}},
		0, []byte("root-2"))
	if got := committer.Stats().SupersededRootWrites; got != 1 {
		t.Fatalf("superseded roots before checkpoint cut = %d, want 1", got)
	}

	flushed := make(chan error, 1)
	go func() { flushed <- committer.Flush() }()
	select {
	case <-device.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("checkpoint did not reach the device")
	}
	// Flush captured generation two before the worker entered this blocked
	// commit. Generation three is accepted while that cut is still in flight
	// and must remain entirely outside it.
	publishTestGeneration(t, committer, 3,
		[]testPage{{offset: int64(3 * pageSize), data: []byte("page-3")}},
		0, []byte("root-3"))
	if got := committer.Stats().SupersededRootWrites; got != 1 {
		t.Fatalf("checkpoint cut allowed root-two supersession: count=%d", got)
	}
	close(device.releaseFirst)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	commits := device.snapshot()
	if len(commits) != 1 || commits[0].root != "root-2" {
		t.Fatalf("first checkpoint commits = %+v, want only cut through root-2", commits)
	}
	if got := committer.DurableGeneration(); got != 2 {
		t.Fatalf("durable generation after first cut = %d, want 2", got)
	}
	if err := committer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits = device.snapshot()
	if len(commits) != 2 || commits[1].root != "root-3" {
		t.Fatalf("second checkpoint commits = %+v, want root-3", commits)
	}
}

func TestCommitterManualCheckpointCaptureLinearizesWithSupersession(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-cut-linearization-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device := newRecordingDevice(8, pageSize)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 8, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	firstData := []byte("page-1")
	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: firstData}},
		0, []byte("root-1"))

	second, err := committer.Begin(1)
	if err != nil {
		t.Fatal(err)
	}
	secondData := []byte("page-2")
	secondPage, err := second.PageBuffer(0)
	if err != nil {
		t.Fatal(err)
	}
	copy(secondPage, secondData)
	if err := second.SetPage(0, int64(2*pageSize), len(secondData)); err != nil {
		t.Fatal(err)
	}
	secondRoot, err := second.RootBuffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(secondRoot, "root-2")
	if err := second.SetRoot(0, len("root-2")); err != nil {
		t.Fatal(err)
	}

	// Force checkpoint capture and the retiring publication to contend on the
	// same mutex. Either order is legal, but their outcomes must agree: a cut
	// through generation one protects its page, while a cut through generation
	// two may observe that exact page as superseded.
	committer.manualMu.Lock()
	start := make(chan struct{})
	entered := make(chan struct{}, 2)
	cut := make(chan uint64, 1)
	published := make(chan error, 1)
	go func() {
		entered <- struct{}{}
		<-start
		cut <- committer.requestCurrentCheckpoint()
	}()
	go func() {
		entered <- struct{}{}
		<-start
		published <- committer.publish(second, 2, []PageRef{{
			Offset: uint64(pageSize), Length: uint32(len(firstData)),
			Generation: 1,
		}})
	}()
	<-entered
	<-entered
	close(start)
	committer.manualMu.Unlock()

	generation := <-cut
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	superseded := committer.Stats().SupersededPageWrites
	switch generation {
	case 1:
		if superseded != 0 {
			t.Fatalf("generation-one cut recycled %d protected pages", superseded)
		}
	case 2:
		if superseded != 1 {
			t.Fatalf("generation-two cut superseded %d pages, want one", superseded)
		}
	default:
		t.Fatalf("checkpoint cut = %d, want generation one or two", generation)
	}
	close(device.releaseFirst)
	if err := committer.Wait(generation); err != nil {
		t.Fatal(err)
	}
}

func publishTestGenerationRetiring(
	t *testing.T,
	committer *Committer,
	generation uint64,
	pages []testPage,
	rootOffset int64,
	root []byte,
	retired []PageRef,
) {
	_ = publishTestGenerationRetiringCollect(
		t, committer, generation, pages, rootOffset, root,
		retired, nil, nil,
	)
}

func publishTestGenerationRetiringCollect(
	t *testing.T,
	committer *Committer,
	generation uint64,
	pages []testPage,
	rootOffset int64,
	root []byte,
	retired []PageRef,
	retirements []FreeExtent,
	superseded []FreeExtent,
) []FreeExtent {
	t.Helper()
	batch, err := committer.Begin(len(pages))
	if err != nil {
		t.Fatal(err)
	}
	for i, page := range pages {
		buffer, bufferErr := batch.PageBuffer(i)
		if bufferErr != nil {
			t.Fatal(bufferErr)
		}
		copy(buffer, page.data)
		if setErr := batch.SetPage(i, page.offset, len(page.data)); setErr != nil {
			t.Fatal(setErr)
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
	superseded, err = committer.publishRetiring(
		batch, generation, retired, retirements, superseded,
	)
	if err != nil {
		t.Fatal(err)
	}
	return superseded
}
