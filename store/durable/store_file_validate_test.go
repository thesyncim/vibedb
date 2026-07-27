package durable

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileStorePageValidatorFailsClosedForEveryPageKind(t *testing.T) {
	validator := newFileStorePageValidator(testAdmissionPageSize, 4, 64)
	validator.fileEnd.Store(64 * testAdmissionPageSize)
	validator.nextLogicalID.Store(100)
	validator.chunkHighWater.Store(20)
	storeID := [16]byte{1, 2, 3}

	for kind := storeio.PageStateRoot; kind <= storeio.PageFingerprintDirectory; kind++ {
		t.Run(pageAdmissionKindName(kind), func(t *testing.T) {
			logicalID := uint64(50)
			if kind == storeio.PageStateRoot {
				logicalID = storeio.StateRootLogicalID
			}
			page := make([]byte, testAdmissionPageSize)
			if _, err := storeio.InitPage(page, storeio.PageHeader{
				StoreID: storeID, Generation: 3, LogicalID: logicalID,
				PageSize: testAdmissionPageSize, Kind: kind,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := storeio.SealPage(page); err != nil {
				t.Fatal(err)
			}
			ref := storeio.PageRef{
				Offset: 8 * testAdmissionPageSize, LogicalID: logicalID,
				Generation: 3, Length: testAdmissionPageSize, Kind: kind,
			}
			err := validator.validate(page, ref)
			if err == nil {
				t.Fatalf("checksum-valid but semantically empty kind %d was admitted", kind)
			}
			if kind != storeio.PageStateRoot &&
				errors.Is(err, storeio.ErrPageCacheReference) {
				t.Fatalf("kind %d reached the unsupported-kind default instead of typed validation: %v",
					kind, err)
			}
		})
	}
}

func TestFileStorePageValidatorCoversMetadataAndOverflowKinds(t *testing.T) {
	const (
		pageSize      = uint32(testAdmissionPageSize)
		fileEnd       = uint64(64 * testAdmissionPageSize)
		nextLogicalID = uint64(100)
	)
	storeID := [16]byte{1, 2, 3}
	validator := newFileStorePageValidator(pageSize, 4, 64)
	validator.fileEnd.Store(fileEnd)
	validator.nextLogicalID.Store(nextLogicalID)
	validator.chunkHighWater.Store(20)

	overflow, err := storeio.EncodeOverflowPage(
		make([]byte, pageSize),
		storeio.OverflowPageHeader{
			StoreID: storeID, Generation: 3, LogicalID: 30, PageSize: pageSize,
			Chunk: 3, Slot: 2, Total: 2,
		},
		[]byte(`{}`), fileEnd, nextLogicalID, pageSize, 20, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	indexDirectory, err := storeio.EncodeIndexDirectoryLeaf(
		make([]byte, pageSize),
		storeio.IndexDirectoryHeader{
			StoreID: storeID, Generation: 3, LogicalID: 31, PageSize: pageSize,
		},
		[]storeio.IndexDirectoryEntry{{
			Key:  storeio.IndexDirectoryKey{IndexID: 1, TupleHash: 7, Chunk: 3},
			Bits: 1 << 2,
		}},
		nil, nextLogicalID, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	posting, err := storeio.EncodePostingPage(
		make([]byte, pageSize),
		storeio.PostingPageHeader{
			StoreID: storeID, Generation: 3, LogicalID: 32, PageSize: pageSize,
			IndexID: 1,
		},
		[]storeio.PostingSegment{{
			StreamID: 1, TupleHash: 7,
			Entries: []storeio.PostingEntry{{Chunk: 3, Bits: 1 << 2}},
		}},
		nextLogicalID, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	freeHeader := func(logicalID uint64) storeio.FreeLogHeader {
		return storeio.FreeLogHeader{
			StoreID: storeID, Generation: 3, LogicalID: logicalID, PageSize: pageSize,
		}
	}
	freeExtent := storeio.FreeExtent{
		Offset: 8 * uint64(pageSize), Length: uint64(pageSize), RetiredGeneration: 2,
	}
	freeImage, err := storeio.EncodeFreeImagePage(
		make([]byte, pageSize), freeHeader(33), []storeio.FreeExtent{freeExtent},
		fileEnd, nextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	freeDelta, err := storeio.EncodeFreeDeltaPage(
		make([]byte, pageSize), freeHeader(34),
		[]storeio.FreeDelta{{Op: storeio.FreeOpSet, Extent: freeExtent}},
		storeio.PageRef{}, storeio.PageRef{}, fileEnd, nextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	freeIndex, err := storeio.EncodeFreeIndexPage(
		make([]byte, pageSize), freeHeader(35),
		[]storeio.FreeSegment{{
			Ref: storeio.PageRef{
				Offset: 20 * uint64(pageSize), LogicalID: 33, Generation: 3,
				Length: pageSize, Kind: storeio.PageFreeImage,
			},
			FirstOffset: freeExtent.Offset, LargestFree: freeExtent.Length, Count: 1,
		}},
		storeio.PageRef{}, fileEnd, nextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		kind    storeio.PageKind
		logical uint64
		page    []byte
		corrupt func([]byte)
		want    error
	}{
		{
			name: "overflow", kind: storeio.PageOverflow, logical: 30, page: overflow,
			corrupt: func(page []byte) {
				binary.LittleEndian.PutUint32(page[storeio.PageHeaderSize+12:], 3)
			},
			want: storeio.ErrOverflowPageCorrupt,
		},
		{
			name: "index directory", kind: storeio.PageIndexDirectory,
			logical: 31, page: indexDirectory,
			corrupt: func(page []byte) {
				binary.LittleEndian.PutUint16(page[storeio.PageHeaderSize+6:], 600)
			},
			want: storeio.ErrIndexDirectoryCorrupt,
		},
		{
			name: "index posting", kind: storeio.PageIndexPosting,
			logical: 32, page: posting,
			corrupt: func(page []byte) {
				binary.LittleEndian.PutUint16(page[storeio.PageHeaderSize+8:], 600)
			},
			want: storeio.ErrPostingPageCorrupt,
		},
		{
			name: "free image", kind: storeio.PageFreeImage, logical: 33, page: freeImage,
			corrupt: func(page []byte) {
				binary.LittleEndian.PutUint16(page[storeio.PageHeaderSize+6:], 600)
			},
			want: storeio.ErrFreeLogCorrupt,
		},
		{
			name: "free delta", kind: storeio.PageFreeDelta, logical: 34, page: freeDelta,
			corrupt: func(page []byte) {
				page[storeio.PageHeaderSize+storeio.FreeDeltaPayloadHeaderSize] = 0xff
			},
			want: storeio.ErrFreeLogCorrupt,
		},
		{
			name: "free index", kind: storeio.PageFreeIndex, logical: 35, page: freeIndex,
			corrupt: func(page []byte) {
				binary.LittleEndian.PutUint32(
					page[storeio.PageHeaderSize+storeio.FreeIndexPayloadHeaderSize+48:], 0,
				)
			},
			want: storeio.ErrFreeLogCorrupt,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ref := storeio.PageRef{
				Offset: (test.logical + 4) * uint64(pageSize), LogicalID: test.logical,
				Generation: 3, Length: pageSize, Kind: test.kind,
			}
			if err := validator.validate(test.page, ref); err != nil {
				t.Fatalf("valid page rejected: %v", err)
			}
			corrupt := append([]byte(nil), test.page...)
			test.corrupt(corrupt)
			if _, err := storeio.SealPage(corrupt); err != nil {
				t.Fatal(err)
			}
			if err := validator.validate(corrupt, ref); !errors.Is(err, test.want) {
				t.Fatalf("corrupt page error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestFileStorePageValidatorExplicitlyRejectsStateRootAdmission(t *testing.T) {
	const (
		pageSize = uint32(testAdmissionPageSize)
		fileEnd  = uint64(64 * testAdmissionPageSize)
	)
	root := storeio.StateRoot{
		StoreID: [16]byte{1, 2, 3}, Generation: 3, PageSize: pageSize,
		MaxPageSize: pageSize, NextLogicalID: 100, ChunkDocuments: 64,
	}
	page, err := storeio.EncodeStateRootPage(make([]byte, pageSize), root, fileEnd)
	if err != nil {
		t.Fatal(err)
	}
	validator := newFileStorePageValidator(pageSize, 4, 64)
	validator.fileEnd.Store(fileEnd)
	validator.nextLogicalID.Store(root.NextLogicalID)
	ref := storeio.PageRef{
		Offset: 4 * uint64(pageSize), LogicalID: storeio.StateRootLogicalID,
		Generation: root.Generation, Length: pageSize, Kind: storeio.PageStateRoot,
	}
	if err := validator.validate(page, ref); !errors.Is(err, storeio.ErrPageCacheReference) {
		t.Fatalf("state-root admission error = %v, want %v", err, storeio.ErrPageCacheReference)
	}
}

func pageAdmissionKindName(kind storeio.PageKind) string {
	return "kind-" + string(rune('A'+kind-1))
}
