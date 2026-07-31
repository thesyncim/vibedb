package durable

import (
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/storemem"
	"github.com/thesyncim/vibedb/store"
)

// fileFreeScratch is the pointer-free free-set planner arena. Keeping all four
// slices in one fixed block avoids placing multi-megabyte, differently lived
// objects in Go's size-class heap for every collection.
type fileFreeScratch struct {
	fenced []storeio.FreeExtent
	image  []storeio.FreeExtent
	ranges [][2]int
	order  []freeFoldSlot
}

type fileFreeScratchLayout struct {
	fencedAt, fencedCount uintptr
	imageAt, imageCount   uintptr
	rangesAt, rangesCount uintptr
	orderAt, orderCount   uintptr
	bytes                 uintptr
}

func checkedFileFreeScratchCounts(
	foldLimit, imagePerPage, freeSetLimit, maxRetired, maxTransaction int,
) (fenced, image int, ok bool) {
	foldExtents, ok := checkedFileFreeScratchProduct(foldLimit, imagePerPage)
	if !ok {
		return 0, 0, false
	}
	common, ok := checkedFileFreeScratchSum(
		foldExtents,
		maxRetired,
		maxTransaction,
		fileStorePointPrimaryRetirePages,
		1,
		storeio.FreeLogMaxChainPages,
		storeio.FreeLogMaxIndexPages,
		foldLimit,
	)
	if !ok {
		return 0, 0, false
	}
	image, ok = checkedFileFreeScratchSum(common, freeSetLimit)
	return common, image, ok
}

func checkedFileFreeScratchProduct(left, right int) (int, bool) {
	if left < 0 || right < 0 || left != 0 && right > math.MaxInt/left {
		return 0, false
	}
	return left * right, true
}

func checkedFileFreeScratchSum(parts ...int) (int, bool) {
	total := 0
	for _, part := range parts {
		if part < 0 || part > math.MaxInt-total {
			return 0, false
		}
		total += part
	}
	return total, true
}

func newFileFreeScratch(
	fencedCount, imageCount, rangesCount, orderCount int,
) (*storemem.Block, fileFreeScratch, error) {
	layout, ok := planFileFreeScratch(
		fencedCount, imageCount, rangesCount, orderCount,
	)
	if !ok || layout.bytes > uintptr(math.MaxInt) {
		return nil, fileFreeScratch{}, store.ErrCheckpointTooLarge
	}
	block, err := storemem.Allocate(int(layout.bytes))
	if err != nil {
		return nil, fileFreeScratch{}, err
	}
	bytes := block.Bytes()
	scratch := fileFreeScratch{
		fenced: fileFreeScratchSlice[storeio.FreeExtent](
			bytes, layout.fencedAt, layout.fencedCount,
		),
		image: fileFreeScratchSlice[storeio.FreeExtent](
			bytes, layout.imageAt, layout.imageCount,
		),
		ranges: fileFreeScratchSlice[[2]int](
			bytes, layout.rangesAt, layout.rangesCount,
		),
		order: fileFreeScratchSlice[freeFoldSlot](
			bytes, layout.orderAt, layout.orderCount,
		),
	}
	return block, scratch, nil
}

func planFileFreeScratch(
	fencedCount, imageCount, rangesCount, orderCount int,
) (fileFreeScratchLayout, bool) {
	var layout fileFreeScratchLayout
	appendRegion := func(count int, size, alignment uintptr) (uintptr, uintptr, bool) {
		if count < 0 || size == 0 || alignment == 0 {
			return 0, 0, false
		}
		mask := alignment - 1
		if layout.bytes > ^uintptr(0)-mask {
			return 0, 0, false
		}
		at := (layout.bytes + mask) &^ mask
		if uintptr(count) > (^uintptr(0)-at)/size {
			return 0, 0, false
		}
		layout.bytes = at + uintptr(count)*size
		return at, uintptr(count), true
	}
	var ok bool
	layout.fencedAt, layout.fencedCount, ok = appendRegion(
		fencedCount,
		unsafe.Sizeof(storeio.FreeExtent{}),
		unsafe.Alignof(storeio.FreeExtent{}),
	)
	if !ok {
		return fileFreeScratchLayout{}, false
	}
	layout.imageAt, layout.imageCount, ok = appendRegion(
		imageCount,
		unsafe.Sizeof(storeio.FreeExtent{}),
		unsafe.Alignof(storeio.FreeExtent{}),
	)
	if !ok {
		return fileFreeScratchLayout{}, false
	}
	layout.rangesAt, layout.rangesCount, ok = appendRegion(
		rangesCount,
		unsafe.Sizeof([2]int{}),
		unsafe.Alignof([2]int{}),
	)
	if !ok {
		return fileFreeScratchLayout{}, false
	}
	layout.orderAt, layout.orderCount, ok = appendRegion(
		orderCount,
		unsafe.Sizeof(freeFoldSlot{}),
		unsafe.Alignof(freeFoldSlot{}),
	)
	if !ok {
		return fileFreeScratchLayout{}, false
	}
	return layout, true
}

func fileFreeScratchSlice[T any](bytes []byte, at, count uintptr) []T {
	if count == 0 {
		return nil
	}
	return unsafe.Slice(
		(*T)(unsafe.Pointer(unsafe.SliceData(bytes[at:]))),
		int(count),
	)
}
