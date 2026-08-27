package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	generationMigrationExactRunHeaderBytes = 32
	generationMigrationExactRunRecordBytes = 20
	generationMigrationExactRunLast        = uint32(1)
)

type GenerationMigrationExactRunRecord struct {
	IndexID uint32
	Key     []byte
	TileID  uint32
	Mask    uint64
}

// EncodeGenerationMigrationExactRunPage encodes one ordered external-sort
// page. Keys remain canonical vibejson-derived binary tuples; no string
// conversion or per-record allocation occurs.
func EncodeGenerationMigrationExactRunPage(
	dst []byte,
	storeID [16]byte,
	generation, logicalID, runID uint64,
	pageOrdinal uint32,
	last bool,
	records []GenerationMigrationExactRunRecord,
) ([]byte, error) {
	payloadBytes := generationMigrationExactRunHeaderBytes
	for index := range records {
		payloadBytes += generationMigrationExactRunRecordBytes + len(records[index].Key)
	}
	if len(records) == 0 || payloadBytes > len(dst)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: migration exact run size", ErrInvalidWrite)
	}
	payload, err := InitPage(dst, PageHeader{StoreID: storeID, Generation: generation, LogicalID: logicalID, PageSize: uint32(len(dst)), PayloadLength: uint32(payloadBytes), Kind: PageMigrationExactRun})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint64(payload[8:16], runID)
	binary.LittleEndian.PutUint32(payload[16:20], pageOrdinal)
	binary.LittleEndian.PutUint32(payload[20:24], uint32(len(records)))
	if last {
		binary.LittleEndian.PutUint32(payload[24:28], generationMigrationExactRunLast)
	}
	at := generationMigrationExactRunHeaderBytes
	var previous GenerationMigrationExactRunRecord
	for index := range records {
		record := records[index]
		if record.Mask == 0 || len(record.Key) == 0 || len(record.Key) > IndexTermMaxKeyBytes || !ValidIndexTermKey(record.Key) ||
			index != 0 && compareGenerationMigrationExactRunRecord(previous, record) >= 0 {
			return nil, fmt.Errorf("%w: migration exact run order", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint32(payload[at:at+4], record.IndexID)
		binary.LittleEndian.PutUint32(payload[at+4:at+8], record.TileID)
		binary.LittleEndian.PutUint64(payload[at+8:at+16], record.Mask)
		binary.LittleEndian.PutUint16(payload[at+16:at+18], uint16(len(record.Key)))
		copy(payload[at+generationMigrationExactRunRecordBytes:], record.Key)
		at += generationMigrationExactRunRecordBytes + len(record.Key)
		previous = record
	}
	if _, err := sealInitializedPage(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

type GenerationMigrationExactRunPageView struct {
	payload     []byte
	runID       uint64
	pageOrdinal uint32
	count       uint32
	last        bool
}

func OpenGenerationMigrationExactRunPage(src []byte, expected PageRef, storeID [16]byte, generation uint64) (GenerationMigrationExactRunPageView, error) {
	if expected.Kind != PageMigrationExactRun || expected.Length != uint32(len(src)) || expected.Generation == 0 || expected.Generation > generation {
		return GenerationMigrationExactRunPageView{}, ErrGenerationMigrationManifestCorrupt
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != storeID || header.Generation != expected.Generation || header.LogicalID != expected.LogicalID || header.Kind != PageMigrationExactRun || len(payload) < generationMigrationExactRunHeaderBytes || binary.LittleEndian.Uint32(payload[0:4]) != DevelopmentFormatVersion || !allZero(payload[4:8]) || !allZero(payload[28:32]) {
		return GenerationMigrationExactRunPageView{}, ErrGenerationMigrationManifestCorrupt
	}
	flags := binary.LittleEndian.Uint32(payload[24:28])
	view := GenerationMigrationExactRunPageView{payload: payload, runID: binary.LittleEndian.Uint64(payload[8:16]), pageOrdinal: binary.LittleEndian.Uint32(payload[16:20]), count: binary.LittleEndian.Uint32(payload[20:24]), last: flags == generationMigrationExactRunLast}
	if view.runID == 0 || view.count == 0 || flags&^generationMigrationExactRunLast != 0 {
		return GenerationMigrationExactRunPageView{}, ErrGenerationMigrationManifestCorrupt
	}
	it := view.Iterator()
	var previous GenerationMigrationExactRunRecord
	for index := uint32(0); index < view.count; index++ {
		record, ok := it.Next()
		if !ok || index != 0 && compareGenerationMigrationExactRunRecord(previous, record) >= 0 {
			return GenerationMigrationExactRunPageView{}, ErrGenerationMigrationManifestCorrupt
		}
		previous = record
	}
	if _, ok := it.Next(); ok || it.at != len(payload) {
		return GenerationMigrationExactRunPageView{}, ErrGenerationMigrationManifestCorrupt
	}
	return view, nil
}

func (v GenerationMigrationExactRunPageView) RunID() uint64       { return v.runID }
func (v GenerationMigrationExactRunPageView) PageOrdinal() uint32 { return v.pageOrdinal }
func (v GenerationMigrationExactRunPageView) Len() int            { return int(v.count) }
func (v GenerationMigrationExactRunPageView) Last() bool          { return v.last }
func (v GenerationMigrationExactRunPageView) Iterator() GenerationMigrationExactRunIterator {
	return GenerationMigrationExactRunIterator{payload: v.payload, remaining: v.count, at: generationMigrationExactRunHeaderBytes}
}

type GenerationMigrationExactRunIterator struct {
	payload   []byte
	remaining uint32
	at        int
}

func (it *GenerationMigrationExactRunIterator) Next() (GenerationMigrationExactRunRecord, bool) {
	if it == nil || it.remaining == 0 || it.at+generationMigrationExactRunRecordBytes > len(it.payload) {
		return GenerationMigrationExactRunRecord{}, false
	}
	record := it.payload[it.at:]
	keyBytes := int(binary.LittleEndian.Uint16(record[16:18]))
	end := it.at + generationMigrationExactRunRecordBytes + keyBytes
	if keyBytes == 0 || keyBytes > IndexTermMaxKeyBytes || end > len(it.payload) || !allZero(record[18:20]) {
		it.remaining = 0
		return GenerationMigrationExactRunRecord{}, false
	}
	result := GenerationMigrationExactRunRecord{IndexID: binary.LittleEndian.Uint32(record[0:4]), TileID: binary.LittleEndian.Uint32(record[4:8]), Mask: binary.LittleEndian.Uint64(record[8:16]), Key: it.payload[it.at+generationMigrationExactRunRecordBytes : end]}
	if result.Mask == 0 || !ValidIndexTermKey(result.Key) {
		it.remaining = 0
		return GenerationMigrationExactRunRecord{}, false
	}
	it.at = end
	it.remaining--
	return result, true
}

func compareGenerationMigrationExactRunRecord(a, b GenerationMigrationExactRunRecord) int {
	if a.IndexID < b.IndexID {
		return -1
	}
	if a.IndexID > b.IndexID {
		return 1
	}
	if cmp := bytes.Compare(a.Key, b.Key); cmp != 0 {
		return cmp
	}
	if a.TileID < b.TileID {
		return -1
	}
	if a.TileID > b.TileID {
		return 1
	}
	return 0
}
