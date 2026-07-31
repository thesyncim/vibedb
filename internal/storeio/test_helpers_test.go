package storeio

import (
	"hash/crc32"
	"os"
	"testing"
)

const testSuperblockPageSize = uint32(4096)

var testStoreID = [16]byte{
	0x51, 0x7a, 0x93, 0x11, 0x2c, 0x44, 0x58, 0x61,
	0x70, 0x8d, 0xa2, 0xb5, 0xc9, 0xd0, 0xe4, 0xff,
}

type GlobalTabletCatalogSpace struct {
	Tablets      uint64
	Leaves       uint64
	Documents    uint64
	TabletBytes  uint64
	CatalogBytes uint64
	TotalBytes   uint64
	BytesPerLeaf float64
	BytesPerDoc  float64
}

func GlobalTabletCatalogRoutingSpace(
	documents, rowsPerLeaf, leavesPerTablet, catalogBytes uint64,
) (GlobalTabletCatalogSpace, bool) {
	if documents == 0 || rowsPerLeaf == 0 || leavesPerTablet == 0 {
		return GlobalTabletCatalogSpace{}, false
	}
	leaves := (documents + rowsPerLeaf - 1) / rowsPerLeaf
	tablets := (leaves + leavesPerTablet - 1) / leavesPerTablet
	tabletBytes := tablets * (GlobalTabletCatalogTabletBytes +
		GlobalTabletCatalogLocatorBytes +
		SegmentedTabletRouterMaxPages*SegmentedTabletRouterAnchorPageBytes)
	total := tabletBytes + catalogBytes
	return GlobalTabletCatalogSpace{
		Tablets: tablets, Leaves: leaves, Documents: documents,
		TabletBytes: tabletBytes, CatalogBytes: catalogBytes, TotalBytes: total,
		BytesPerLeaf: float64(total) / float64(leaves),
		BytesPerDoc:  float64(total) / float64(documents),
	}, true
}

func SegmentedTabletRouterRoutingBytesPerDocument(
	leafCount, rowsPerLeaf int,
) float64 {
	if leafCount <= 0 || leafCount > TabletLocalIdentityLocalCount ||
		rowsPerLeaf <= 0 {
		return 0
	}
	pages := (leafCount + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	bytes := SegmentedTabletRouterRootBytes +
		SegmentedTabletRouterLocatorBytes +
		pages*SegmentedTabletRouterAnchorPageBytes
	return float64(bytes) / float64(leafCount*rowsPerLeaf)
}

func (b *Batch) PageBuffer(page int) ([]byte, error) {
	if b == nil || b.state.Load() != batchOwned || b.materialized ||
		page < 0 || page >= len(b.pages) {
		return nil, ErrBatchState
	}
	if b.pages[page].frameNative() {
		cache := b.committer.frameCache.Load()
		if cache == nil || int(b.pages[page].frameIndex) >= len(cache.frames) {
			return nil, ErrBatchState
		}
		return cache.extentBytes(
			int(b.pages[page].frameIndex), b.pages[page].Length,
		), nil
	}
	return b.committer.buffers[b.pages[page].Buffer], nil
}

func (b *Batch) SetPage(page int, offset int64, length int) error {
	if b == nil || b.state.Load() != batchOwned || b.materialized ||
		page < 0 || page >= len(b.pages) {
		return ErrBatchState
	}
	if length < 0 || uint64(length) > uint64(^uint32(0)) {
		return ErrInvalidWrite
	}
	b.pages[page].Offset = offset
	b.pages[page].Length = uint32(length)
	return nil
}

func (b *Batch) RootBuffer() ([]byte, error) {
	if b == nil || b.state.Load() != batchOwned {
		return nil, ErrBatchState
	}
	return b.committer.buffers[b.root.Buffer], nil
}

func (b *Batch) SetRoot(offset int64, length int) error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	if length < 0 || uint64(length) > uint64(^uint32(0)) {
		return ErrInvalidWrite
	}
	b.root.Offset = offset
	b.root.Length = uint32(length)
	b.rootGeneration = 0
	return nil
}

func (b *Batch) Publish(generation uint64) error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	return b.committer.publish(b, generation, nil)
}

func (c *Committer) BeginMaterialized(patchWriteCount int) (*Batch, error) {
	return c.beginHybridMaterialized(0, patchWriteCount)
}

func testMutableStoreDataStart(pageSize uint32) uint64 {
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		panic(err)
	}
	return layout.DataStart
}

func writeAtTest(t *testing.T, file *os.File, data []byte, offset int64) {
	t.Helper()
	n, err := file.WriteAt(data, offset)
	if err != nil || n != len(data) {
		t.Fatalf("WriteAt(%d) = %d, %v", offset, n, err)
	}
}

func TestPageChecksumMatchesStandardLibrary(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	data := make([]byte, (128<<10)+31)
	for i := range data {
		data[i] = byte(i*131 + i>>3)
	}
	for alignment := range 16 {
		for _, size := range []int{
			0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 119, 120, 127,
			128, 255, 256, 257, 511, 512, 1023, 1024, 1025, 4095,
			4096, 4097, 64 << 10, 128 << 10,
		} {
			input := data[alignment : alignment+size]
			if got, want := PageChecksum(input), crc32.Checksum(input, table); got != want {
				t.Fatalf(
					"alignment=%d size=%d: checksum=%08x, want %08x",
					alignment, size, got, want,
				)
			}
		}
	}
}
