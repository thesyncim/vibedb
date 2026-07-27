package durable

import (
	"math"
	"os"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileFreeScratchIsOneExactPointerFreeArena(t *testing.T) {
	const (
		fenced = 113
		image  = 257
		ranges = 19
		order  = 37
	)
	layout, ok := planFileFreeScratch(fenced, image, ranges, order)
	if !ok || layout.bytes == 0 {
		t.Fatalf("scratch layout = (%+v,%v)", layout, ok)
	}
	block, scratch, err := newFileFreeScratch(fenced, image, ranges, order)
	if err != nil {
		t.Fatal(err)
	}
	defer block.Close()
	if got := uintptr(block.Len()); got != layout.bytes {
		t.Fatalf("scratch bytes = %d, want exact layout %d", got, layout.bytes)
	}
	if len(scratch.fenced) != fenced || len(scratch.image) != image ||
		len(scratch.ranges) != ranges || len(scratch.order) != order {
		t.Fatalf("scratch capacities = (%d,%d,%d,%d)",
			len(scratch.fenced), len(scratch.image),
			len(scratch.ranges), len(scratch.order))
	}
	if uintptr(unsafe.Pointer(unsafe.SliceData(scratch.fenced)))%
		unsafe.Alignof(storeio.FreeExtent{}) != 0 ||
		uintptr(unsafe.Pointer(unsafe.SliceData(scratch.ranges)))%
			unsafe.Alignof([2]int{}) != 0 ||
		uintptr(unsafe.Pointer(unsafe.SliceData(scratch.order)))%
			unsafe.Alignof(freeFoldSlot{}) != 0 {
		t.Fatal("scratch partition is not naturally aligned")
	}
	scratch.fenced[0].Offset = 11
	scratch.image[0].Offset = 22
	scratch.ranges[0] = [2]int{33, 44}
	scratch.order[0].rebuilt = 55
	if scratch.fenced[0].Offset != 11 || scratch.image[0].Offset != 22 ||
		scratch.ranges[0] != [2]int{33, 44} || scratch.order[0].rebuilt != 55 {
		t.Fatal("scratch partitions overlap")
	}
}

func TestFileFreeScratchCapacityArithmeticRejectsOverflow(t *testing.T) {
	if _, _, ok := checkedFileFreeScratchCounts(
		math.MaxInt, 2, 1, 1, 1,
	); ok {
		t.Fatal("free scratch accepted overflowing fold image")
	}
	if _, _, ok := checkedFileFreeScratchCounts(
		1, 1, math.MaxInt, math.MaxInt, 1,
	); ok {
		t.Fatal("free scratch accepted overflowing extent sum")
	}
}

func TestFileStoreReportsExternalFreeScratchCapacity(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-free-scratch-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.MaxBatchDocuments = 1
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	stats := collection.Stats()
	if stats.FreeScratchCapacityBytes == 0 ||
		stats.FreeScratchCapacityBytes != uint64(collection.freeScratchBlock.Len()) {
		t.Fatalf("free scratch capacity = %d, block=%d",
			stats.FreeScratchCapacityBytes, collection.freeScratchBlock.Len())
	}
	if stats.FreeScratchLiveBytes != 0 {
		t.Fatalf("fresh free scratch live bytes = %d, want 0", stats.FreeScratchLiveBytes)
	}
	if collection.freeScratchBlock.OutsideHeap() &&
		stats.FreeScratchExternalBytes != stats.FreeScratchCapacityBytes {
		t.Fatalf("external free scratch = %d, want %d",
			stats.FreeScratchExternalBytes, stats.FreeScratchCapacityBytes)
	}
}
