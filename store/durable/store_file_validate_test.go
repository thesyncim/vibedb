package durable

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileStorePageValidatorFailsClosedForEveryPageKind(t *testing.T) {
	validator := newFileStorePageValidator(testAdmissionPageSize, 4)
	validator.fileEnd.Store(64 * testAdmissionPageSize)
	validator.nextLogicalID.Store(100)
	storeID := [16]byte{1, 2, 3}

	// The retired chunk kinds (2, 4-6, 8-12, 16) no longer have a typed
	// validator, so they are not swept here; only the durable kinds that
	// survived the chunk deletion are. Each must still fail closed on a
	// checksum-valid but semantically empty page, and only PageStateRoot may
	// reach the unsupported-kind reference default.
	for _, kind := range []storeio.PageKind{
		storeio.PageStateRoot, storeio.PageOverflow,
		storeio.PageFreeImage, storeio.PageFreeDelta, storeio.PageFreeIndex,
	} {
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
	validator := newFileStorePageValidator(pageSize, 4)
	validator.fileEnd.Store(fileEnd)
	validator.nextLogicalID.Store(nextLogicalID)

	// The ordered-primary graph pins the overflow codec's vestigial chunk/slot
	// addressing to the same fixed sentinels the producer and validator use, so the
	// encoded page must too (a primary state root leaves ChunkHighWater zero).
	overflow, err := storeio.EncodeOverflowPage(
		make([]byte, pageSize),
		storeio.OverflowPageHeader{
			StoreID: storeID, Generation: 3, LogicalID: 30, PageSize: pageSize,
			Chunk: 0, Slot: 0, Total: 2,
		},
		[]byte(`{}`), fileEnd, nextLogicalID, pageSize,
		primaryOverflowChunkHighWater, primaryOverflowChunkDocuments,
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
	validator := newFileStorePageValidator(pageSize, 4)
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
