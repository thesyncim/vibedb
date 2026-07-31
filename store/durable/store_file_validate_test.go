package durable

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileStorePageValidatorRejectsEmptyStandaloneKinds(t *testing.T) {
	validator := newFileStorePageValidator(testAdmissionPageSize, 4)
	validator.fileEnd.Store(64 * testAdmissionPageSize)
	validator.nextLogicalID.Store(100)
	storeID := [16]byte{1, 2, 3}

	// Each standalone kind must fail closed on a checksum-valid but
	// semantically empty page. Primary graph and exact-index kinds require
	// linked context and are covered by their graph admission tests.
	for _, kind := range []storeio.PageKind{
		storeio.PageOverflow,
		storeio.PageFreeImage, storeio.PageFreeDelta, storeio.PageFreeIndex,
	} {
		t.Run(pageAdmissionKindName(kind), func(t *testing.T) {
			logicalID := uint64(50)
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
			if errors.Is(err, storeio.ErrPageCacheReference) {
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

	overflow, err := storeio.EncodeOverflowPage(
		make([]byte, pageSize),
		storeio.OverflowPageHeader{
			StoreID: storeID, Generation: 3, LogicalID: 30, PageSize: pageSize,
			Total: 2,
		},
		[]byte(`{}`), fileEnd, nextLogicalID, pageSize,
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

func pageAdmissionKindName(kind storeio.PageKind) string {
	return "kind-" + string(rune('A'+kind-1))
}
