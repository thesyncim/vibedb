package storeio

import (
	"errors"
	"os"
	"strconv"
	"testing"
)

func TestDiscoverMutableInlinePageSize(t *testing.T) {
	for _, pageSize := range []uint32{4096, 16 << 10, 64 << 10} {
		t.Run(strconv.FormatUint(uint64(pageSize), 10), func(t *testing.T) {
			file := openDiscoveryFixture(t)
			root := discoveryInlineRoot(t, pageSize, 7, [16]byte{1})
			writeDiscoveryRoot(t, file, root, 0)
			if got, err := DiscoverMutableInlinePageSize(file); err != nil || got != pageSize {
				t.Fatalf("DiscoverMutableInlinePageSize() = %d, %v; want %d", got, err, pageSize)
			}
		})
	}
}

func TestDiscoverMutableInlineBootstrap(t *testing.T) {
	for _, pageSize := range []uint32{4096, 16 << 10, 64 << 10} {
		t.Run(strconv.FormatUint(uint64(pageSize), 10), func(t *testing.T) {
			file := openDiscoveryFixture(t)
			root := discoveryInlineRoot(t, pageSize, 7, [16]byte{7})
			root.State.Options |= StateOptionSchema |
				StateOptionCanonicalMaterialization
			root.State.IndexCatalogHash = 1
			root.State.MaterializationDamageGranule =
				MaterializationJournalMinSectorSize
			root.State.NextLogicalID = 3
			root.State.PageCatalogHead = PageRef{
				Offset: root.FileEnd, LogicalID: 2, Generation: 1,
				Length: pageSize, Kind: PageCatalogSegment,
			}
			root.State.PageCatalogDigest =
				[PageCatalogDigestSize]byte{0x19, 0x82}
			root.State.PageCatalogBytes =
				PageCatalogCanonicalHeaderSize
			root.FileEnd += uint64(pageSize)
			writeDiscoveryRoot(t, file, root, 0)

			got, err := DiscoverMutableInlineBootstrap(file)
			want := MutableInlineBootstrap{
				StoreID:  root.StoreID,
				PageSize: pageSize, MaxPageSize: 64 << 10,
				MaterializationDamageGranule: MaterializationJournalMinSectorSize,
				PageCatalogHead:              root.State.PageCatalogHead,
				PageCatalogDigest:            root.State.PageCatalogDigest,
				PageCatalogBytes:             root.State.PageCatalogBytes,
			}
			if err != nil || got != want {
				t.Fatalf(
					"DiscoverMutableInlineBootstrap() = (%+v,%v), want (%+v,nil)",
					got, err, want,
				)
			}
		})
	}
}

func TestDiscoverMutableInlinePageSizeFromAlternate(t *testing.T) {
	const pageSize = uint32(64 << 10)
	file := openDiscoveryFixture(t)
	root := discoveryInlineRoot(t, pageSize, 9, [16]byte{2})
	writeDiscoveryRoot(t, file, root, int64(pageSize))
	if got, err := DiscoverMutableInlinePageSize(file); err != nil || got != pageSize {
		t.Fatalf("DiscoverMutableInlinePageSize() = %d, %v; want %d", got, err, pageSize)
	}
}

func TestDiscoverMutableInlinePageSizeRejectsConflictingCandidates(t *testing.T) {
	for _, validZero := range []bool{false, true} {
		t.Run(strconv.FormatBool(validZero), func(t *testing.T) {
			file := openDiscoveryFixture(t)
			root4K := discoveryInlineRoot(t, 4096, 3, [16]byte{3})
			root8K := discoveryInlineRoot(t, 8192, 4, [16]byte{4})
			if validZero {
				writeDiscoveryRoot(t, file, root4K, 0)
			} else {
				writeDiscoveryRoot(t, file, root4K, 4096)
			}
			writeDiscoveryRoot(t, file, root8K, 8192)
			if _, err := DiscoverMutableInlinePageSize(file); !errors.Is(err, ErrSuperblockConflict) {
				t.Fatalf("DiscoverMutableInlinePageSize() error = %v; want %v", err, ErrSuperblockConflict)
			}
		})
	}
}

func TestDiscoverMutableInlineBootstrapRejectsImmutableConflicts(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*InlineSuperblock)
	}{
		{"maximum page", func(root *InlineSuperblock) {
			root.State.MaxPageSize = 128 << 10
		}},
		{"damage granule", func(root *InlineSuperblock) {
			root.State.Options |= StateOptionCanonicalMaterialization
			root.State.MaterializationDamageGranule = 1024
		}},
		{"index depth", func(root *InlineSuperblock) {
			root.State.IndexMaxDepth = 17
		}},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			file := openDiscoveryFixture(t)
			first := discoveryInlineRoot(t, 4096, 3, [16]byte{3})
			second := discoveryInlineRoot(t, 4096, 4, [16]byte{3})
			for _, root := range []*InlineSuperblock{&first, &second} {
				root.State.Options |= StateOptionSchema
				root.State.IndexCatalogHash = 1
				root.State.NextLogicalID = 3
				root.State.PageCatalogHead = PageRef{
					Offset: root.FileEnd, LogicalID: 2, Generation: 1,
					Length: root.PageSize, Kind: PageCatalogSegment,
				}
				root.State.PageCatalogDigest =
					[PageCatalogDigestSize]byte{0x71}
				root.State.PageCatalogBytes =
					PageCatalogCanonicalHeaderSize
				root.FileEnd += uint64(root.PageSize)
			}
			if test.name == "damage granule" {
				first.State.Options |= StateOptionCanonicalMaterialization
				first.State.MaterializationDamageGranule =
					MaterializationJournalMinSectorSize
				second.State.Options = first.State.Options
				second.State.MaterializationDamageGranule =
					first.State.MaterializationDamageGranule
			}
			test.mutate(&second)
			writeDiscoveryRoot(t, file, first, 0)
			writeDiscoveryRoot(t, file, second, 4096)
			if _, err := DiscoverMutableInlineBootstrap(file); !errors.Is(
				err, ErrSuperblockConflict,
			) {
				t.Fatalf(
					"DiscoverMutableInlineBootstrap() error = %v; want %v",
					err, ErrSuperblockConflict,
				)
			}
		})
	}
}

func TestDiscoverMutableInlineBootstrapSelectsNewerCatalog(t *testing.T) {
	file := openDiscoveryFixture(t)
	first := discoveryInlineRoot(t, 4096, 3, [16]byte{3})
	second := discoveryInlineRoot(t, 4096, 4, [16]byte{3})
	second.State.IndexCount = 1
	second.State.IndexCatalogHash = 7
	second.State.NextLogicalID = 3
	second.State.PageCatalogHead = PageRef{
		Offset: second.FileEnd, LogicalID: 2, Generation: second.Generation,
		Length: second.PageSize, Kind: PageCatalogSegment,
	}
	second.State.PageCatalogDigest = [PageCatalogDigestSize]byte{0x71}
	second.State.PageCatalogBytes = PageCatalogCanonicalHeaderSize
	second.FileEnd += uint64(second.PageSize)
	writeDiscoveryRoot(t, file, first, 0)
	writeDiscoveryRoot(t, file, second, 4096)

	got, err := DiscoverMutableInlineBootstrap(file)
	if err != nil {
		t.Fatal(err)
	}
	if got.PageCatalogHead != second.State.PageCatalogHead ||
		got.PageCatalogDigest != second.State.PageCatalogDigest ||
		got.PageCatalogBytes != second.State.PageCatalogBytes {
		t.Fatalf("bootstrap selected stale catalog: %+v", got)
	}
}

func TestDiscoverMutableInlinePageSizeRejectsMissingRoots(t *testing.T) {
	file := openDiscoveryFixture(t)
	if err := file.Truncate(128 << 10); err != nil {
		t.Fatal(err)
	}
	if _, err := DiscoverMutableInlinePageSize(file); !errors.Is(err, ErrSuperblockNotFound) {
		t.Fatalf("DiscoverMutableInlinePageSize() error = %v; want %v", err, ErrSuperblockNotFound)
	}
}

func openDiscoveryFixture(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "inline-discovery-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func discoveryInlineRoot(
	t *testing.T, pageSize uint32, generation uint64, storeID [16]byte,
) InlineSuperblock {
	t.Helper()
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	state := StateRoot{
		StoreID: storeID, Generation: generation, PageSize: pageSize,
		MaxPageSize: 64 << 10, NextLogicalID: 2, ChunkDocuments: 64,
	}
	return InlineSuperblock{
		StoreID: storeID, Generation: generation, FileEnd: layout.DataStart,
		PageSize: pageSize, State: state,
		FreeDelta: NewInlineFreeDelta(PageRef{}, PageRef{}),
	}
}

func writeDiscoveryRoot(t *testing.T, file *os.File, root InlineSuperblock, offset int64) {
	t.Helper()
	var page [InlineSuperblockSize]byte
	encoded, err := EncodeInlineSuperblock(page[:], root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(encoded, offset); err != nil {
		t.Fatal(err)
	}
}
