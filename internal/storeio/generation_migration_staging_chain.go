package storeio

import (
	"encoding/binary"
	"fmt"
)

const (
	generationMigrationStagingChainHeaderBytes = 88
	generationMigrationStagingExtentBytes      = 40
)

// GenerationMigrationStagingExtent names one exact incremental reservation.
// Its first physical/logical page is the chain page itself; DataBytes records
// the prefix after that page already filled by the migration. Unused admitted
// bytes remain named and can be retired without scanning their contents.
type GenerationMigrationStagingExtent struct {
	Offset, Length            uint64
	FirstLogicalID            uint64
	LogicalIDCount, DataBytes uint64
}

func (e GenerationMigrationStagingExtent) valid(pageSize uint32) bool {
	quantum := uint64(pageSize)
	return e.Offset != 0 && e.Offset%quantum == 0 && e.Length >= quantum &&
		e.Length%quantum == 0 && e.Offset <= ^uint64(0)-e.Length &&
		e.FirstLogicalID != 0 && e.LogicalIDCount != 0 &&
		e.FirstLogicalID <= ^uint64(0)-e.LogicalIDCount &&
		e.DataBytes <= e.Length-quantum && e.DataBytes%quantum == 0
}

// EncodeGenerationMigrationStagingChainPage seals one immutable backward
// chain link. A bounded number of exact reservations can be coalesced into one
// link; the cumulative witnesses make omission and rollback fail closed.
func EncodeGenerationMigrationStagingChainPage(
	dst []byte,
	storeID, migrationID [16]byte,
	generation, logicalID, sequence uint64,
	previous PageRef,
	cumulativeExtentCount, cumulativeAllocatedBytes, cumulativeUsedBytes uint64,
	extents []GenerationMigrationStagingExtent,
) ([]byte, error) {
	payloadBytes := generationMigrationStagingChainHeaderBytes +
		len(extents)*generationMigrationStagingExtentBytes
	if migrationID == ([16]byte{}) || sequence == 0 || len(extents) == 0 ||
		len(extents) > int(^uint16(0)) || payloadBytes > len(dst)-PageHeaderSize-PageTrailerSize ||
		(previous != (PageRef{}) && (previous.Kind != PageMigrationStagingChain ||
			previous.Generation == 0 || previous.Generation > generation)) {
		return nil, fmt.Errorf("%w: migration staging chain fields", ErrInvalidWrite)
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: storeID, Generation: generation, LogicalID: logicalID,
		PageSize: uint32(len(dst)), PayloadLength: uint32(payloadBytes),
		Kind: PageMigrationStagingChain,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint16(payload[4:6], uint16(len(extents)))
	binary.LittleEndian.PutUint64(payload[8:16], sequence)
	copy(payload[16:32], migrationID[:])
	encodePageRef(payload[32:64], previous)
	binary.LittleEndian.PutUint64(payload[64:72], cumulativeExtentCount)
	binary.LittleEndian.PutUint64(payload[72:80], cumulativeAllocatedBytes)
	binary.LittleEndian.PutUint64(payload[80:88], cumulativeUsedBytes)

	var allocated, used uint64
	for index, extent := range extents {
		if !extent.valid(uint32(len(dst))) ||
			index != 0 && extents[index-1].Offset+extents[index-1].Length > extent.Offset ||
			allocated > ^uint64(0)-extent.Length ||
			used > ^uint64(0)-uint64(len(dst))-extent.DataBytes {
			return nil, fmt.Errorf("%w: migration staging extent", ErrInvalidWrite)
		}
		allocated += extent.Length
		used += uint64(len(dst)) + extent.DataBytes
		at := generationMigrationStagingChainHeaderBytes + index*generationMigrationStagingExtentBytes
		binary.LittleEndian.PutUint64(payload[at:at+8], extent.Offset)
		binary.LittleEndian.PutUint64(payload[at+8:at+16], extent.Length)
		binary.LittleEndian.PutUint64(payload[at+16:at+24], extent.FirstLogicalID)
		binary.LittleEndian.PutUint64(payload[at+24:at+32], extent.LogicalIDCount)
		binary.LittleEndian.PutUint64(payload[at+32:at+40], extent.DataBytes)
	}
	if cumulativeExtentCount < uint64(len(extents)) ||
		cumulativeAllocatedBytes < allocated || cumulativeUsedBytes < used ||
		(previous == (PageRef{}) && (cumulativeExtentCount != uint64(len(extents)) ||
			cumulativeAllocatedBytes != allocated || cumulativeUsedBytes != used)) {
		return nil, fmt.Errorf("%w: migration staging cumulative witnesses", ErrInvalidWrite)
	}
	if _, err := sealInitializedPage(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

type GenerationMigrationStagingChainPageView struct {
	payload                  []byte
	migrationID              [16]byte
	previous                 PageRef
	sequence                 uint64
	cumulativeExtentCount    uint64
	cumulativeAllocatedBytes uint64
	cumulativeUsedBytes      uint64
	count                    uint16
	pageSize                 uint32
}

func OpenGenerationMigrationStagingChainPage(
	src []byte, expected PageRef, storeID, migrationID [16]byte, generation uint64,
) (GenerationMigrationStagingChainPageView, error) {
	var view GenerationMigrationStagingChainPageView
	if expected.Kind != PageMigrationStagingChain || expected.Length != uint32(len(src)) ||
		expected.Generation == 0 || expected.Generation > generation {
		return view, ErrGenerationMigrationManifestCorrupt
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != storeID || header.Generation != expected.Generation ||
		header.LogicalID != expected.LogicalID || header.Kind != expected.Kind ||
		len(payload) < generationMigrationStagingChainHeaderBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != DevelopmentFormatVersion ||
		!allZero(payload[6:8]) {
		return view, ErrGenerationMigrationManifestCorrupt
	}
	view.payload = payload
	view.pageSize = uint32(len(src))
	view.count = binary.LittleEndian.Uint16(payload[4:6])
	view.sequence = binary.LittleEndian.Uint64(payload[8:16])
	copy(view.migrationID[:], payload[16:32])
	view.previous = decodePageRef(payload[32:64])
	view.cumulativeExtentCount = binary.LittleEndian.Uint64(payload[64:72])
	view.cumulativeAllocatedBytes = binary.LittleEndian.Uint64(payload[72:80])
	view.cumulativeUsedBytes = binary.LittleEndian.Uint64(payload[80:88])
	wantBytes := generationMigrationStagingChainHeaderBytes +
		int(view.count)*generationMigrationStagingExtentBytes
	if view.count == 0 || view.sequence == 0 || view.migrationID != migrationID ||
		wantBytes != len(payload) ||
		(view.previous != (PageRef{}) && (view.previous.Kind != PageMigrationStagingChain ||
			view.previous.Generation == 0 || view.previous.Generation > generation)) {
		return GenerationMigrationStagingChainPageView{}, ErrGenerationMigrationManifestCorrupt
	}
	it := view.Iterator()
	var previous GenerationMigrationStagingExtent
	var allocated, used uint64
	for index := 0; index < int(view.count); index++ {
		extent, ok := it.Next()
		if !ok || index != 0 && previous.Offset+previous.Length > extent.Offset ||
			allocated > ^uint64(0)-extent.Length ||
			used > ^uint64(0)-uint64(len(src))-extent.DataBytes {
			return GenerationMigrationStagingChainPageView{}, ErrGenerationMigrationManifestCorrupt
		}
		allocated += extent.Length
		used += uint64(len(src)) + extent.DataBytes
		previous = extent
	}
	if _, ok := it.Next(); ok || view.cumulativeExtentCount < uint64(view.count) ||
		view.cumulativeAllocatedBytes < allocated || view.cumulativeUsedBytes < used ||
		(view.previous == (PageRef{}) && (view.cumulativeExtentCount != uint64(view.count) ||
			view.cumulativeAllocatedBytes != allocated || view.cumulativeUsedBytes != used)) {
		return GenerationMigrationStagingChainPageView{}, ErrGenerationMigrationManifestCorrupt
	}
	return view, nil
}

func (v GenerationMigrationStagingChainPageView) MigrationID() [16]byte { return v.migrationID }
func (v GenerationMigrationStagingChainPageView) Previous() PageRef     { return v.previous }
func (v GenerationMigrationStagingChainPageView) Sequence() uint64      { return v.sequence }
func (v GenerationMigrationStagingChainPageView) CumulativeExtentCount() uint64 {
	return v.cumulativeExtentCount
}
func (v GenerationMigrationStagingChainPageView) CumulativeAllocatedBytes() uint64 {
	return v.cumulativeAllocatedBytes
}
func (v GenerationMigrationStagingChainPageView) CumulativeUsedBytes() uint64 {
	return v.cumulativeUsedBytes
}
func (v GenerationMigrationStagingChainPageView) Iterator() GenerationMigrationStagingExtentIterator {
	return GenerationMigrationStagingExtentIterator{payload: v.payload, remaining: v.count, at: generationMigrationStagingChainHeaderBytes, pageSize: v.pageSize}
}

type GenerationMigrationStagingExtentIterator struct {
	payload   []byte
	remaining uint16
	at        int
	pageSize  uint32
}

func (it *GenerationMigrationStagingExtentIterator) Next() (GenerationMigrationStagingExtent, bool) {
	if it == nil || it.remaining == 0 || it.at+generationMigrationStagingExtentBytes > len(it.payload) {
		return GenerationMigrationStagingExtent{}, false
	}
	record := it.payload[it.at : it.at+generationMigrationStagingExtentBytes]
	extent := GenerationMigrationStagingExtent{
		Offset:         binary.LittleEndian.Uint64(record[0:8]),
		Length:         binary.LittleEndian.Uint64(record[8:16]),
		FirstLogicalID: binary.LittleEndian.Uint64(record[16:24]),
		LogicalIDCount: binary.LittleEndian.Uint64(record[24:32]),
		DataBytes:      binary.LittleEndian.Uint64(record[32:40]),
	}
	if !extent.valid(it.pageSize) {
		it.remaining = 0
		return GenerationMigrationStagingExtent{}, false
	}
	it.at += generationMigrationStagingExtentBytes
	it.remaining--
	return extent, true
}
