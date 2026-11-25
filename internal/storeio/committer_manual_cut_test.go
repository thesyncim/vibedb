package storeio

import (
	"os"
	"testing"
)

// TestCommitterManualCheckpointNeverSplitsSupersededRootCut verifies that when
// multiple buffered generations accumulate more raw page descriptors than
// BufferCount (and older roots/pages have been superseded and recycled),
// ManualCheckpoint flushes the entire authorized cut in one commit rather than
// splitting at cap(commitScratch) and publishing a superseded/recycled root.
func TestCommitterManualCheckpointNeverSplitsSupersededRootCut(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "manual-cut-no-split-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	const bufferCount = 4
	device := newRecordingDevice(bufferCount, pageSize)
	close(device.releaseFirst)
	committer, err := newCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: bufferCount, BufferSize: pageSize,
	}, CommitterOptions{
		QueueSlots: 8, GroupLimit: 1, MaxPagesPerBatch: 1, ManualCheckpoint: true,
	}, func(*os.File, DeviceOptions) (Device, error) { return device, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()

	// Publish 6 generations (6 raw page descriptors > BufferCount=4). Each new
	// generation retires the previous generation's page so buffers are recycled
	// and earlier roots are marked pendingWriteSuperseded.
	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: []byte{1, 1, 1}}},
		0, []byte{1})
	for generation := uint64(2); generation <= 6; generation++ {
		prevOffset := uint64(generation-1) * uint64(pageSize)
		curOffset := int64(generation) * int64(pageSize)
		retirement := FreeExtent{
			Offset: prevOffset, Length: 3, RetiredGeneration: generation - 1,
		}
		publishTestGenerationRetiringCollect(
			t, committer, generation,
			[]testPage{{offset: curOffset, data: []byte{byte(generation), byte(generation), byte(generation)}}},
			0, []byte{byte(generation)},
			[]PageRef{{Offset: prevOffset, Length: 3, Generation: generation - 1}},
			[]FreeExtent{retirement},
			make([]FreeExtent, 0, 1),
		)
	}

	if err := committer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	commits := device.snapshot()
	if len(commits) != 1 {
		t.Fatalf("device commits = %d, want 1 atomic manual cut (commits=%+v)", len(commits), commits)
	}
	if commits[0].root != string([]byte{6}) {
		t.Fatalf("committed root = %x, want 06", []byte(commits[0].root))
	}
	if len(commits[0].pages) != 1 || commits[0].pages[0].Offset != int64(6*pageSize) || commits[0].pages[0].Length != 3 {
		t.Fatalf("committed pages = %+v, want only generation 6 page at offset %d", commits[0].pages, 6*pageSize)
	}
}
