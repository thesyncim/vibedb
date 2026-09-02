package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Metadata is deliberately sector/page separated: a torn write to one slot
// cannot share a 4 KiB page with the fallback slot. The catalog begins after a
// second alignment gap so future direct-I/O metadata writers retain alignment.
const (
	metadataSlotBytes      = 4096
	metadataSlot0Offset    = 0
	metadataSlot1Offset    = metadataSlotBytes
	metadataCatalogStart   = 16 << 10
	catalogRecordBytes     = 192
	metadataCatalogRecords = 8192
	metadataCatalogBytes   = metadataCatalogRecords * catalogRecordBytes
	metadataCatalogEnd     = metadataCatalogStart + metadataCatalogBytes

	metadataSlotMACOffset    = metadataSlotBytes - 36
	metadataSlotCRCOffset    = metadataSlotBytes - 4
	catalogMACOffset         = catalogRecordBytes - 36
	catalogCRCOffset         = catalogRecordBytes - 4
	maxRetiredSegments       = 32
	retiredDescriptorBytes   = 104
	retiredDescriptorsOffset = 504
	retiredCheckpointOffset  = 3868
	maxRetiredCheckpoints    = 4
)

// Tests may reduce the logical ring while retaining the production physical
// allocation. It is control-plane state and never read on append hot paths.
var catalogRingRecords = uint64(metadataCatalogRecords)

func catalogPhysicalOffset(cursor uint64) (int64, error) {
	if cursor < metadataCatalogStart || (cursor-metadataCatalogStart)%catalogRecordBytes != 0 || catalogRingRecords == 0 || catalogRingRecords > metadataCatalogRecords {
		return 0, ErrCorrupt
	}
	record := (cursor - metadataCatalogStart) / catalogRecordBytes
	offset := uint64(metadataCatalogStart) + record%catalogRingRecords*catalogRecordBytes
	return int64(offset), nil
}

var (
	metadataMagic = [8]byte{'V', 'D', 'B', 'S', 'M', 'E', 'T', 'A'}
	catalogMagic  = [8]byte{'V', 'D', 'B', 'S', 'C', 'A', 'T', 'A'}
)

type catalogKind uint8

const (
	catalogSeal catalogKind = iota + 1
	catalogAnchor
)

type fileID [16]byte

type activeDescriptor struct {
	FileID         fileID
	ID, Generation uint64
	PreviousID     uint64
	PreviousHash   [32]byte
	Capacity       uint64
}

type pendingDescriptor struct {
	FileID                         fileID
	ID, Generation, Bytes, Records uint64
	PreviousHash, Hash             [32]byte
}

type reserveDescriptor struct {
	FileID   fileID
	Capacity uint64
	Ready    bool
}

type reclaimPhase uint8

const (
	reclaimNone reclaimPhase = iota
	reclaimPrepared
	reclaimDurable
)

type retiredDescriptor struct {
	ID, Generation uint64
	FileID         fileID
	Bytes          uint64
	PreviousHash   [32]byte
	Hash           [32]byte
}

type retiredCheckpointDescriptor struct {
	ID   fileID
	Hash [32]byte
}

type metadataSlot struct {
	Generation                 uint64
	LogID                      [16]byte
	CatalogTail                uint64
	CatalogHash                [32]byte
	CheckpointID               [16]byte
	CheckpointTail             uint64
	CheckpointHash             [32]byte
	PreviousCheckpointID       [16]byte
	PreviousCheckpointTail     uint64
	PreviousCheckpointHash     [32]byte
	AnchorID, AnchorGeneration uint64
	AnchorHash                 [32]byte
	Active                     activeDescriptor
	Pending                    pendingDescriptor
	HasPending                 bool
	Reserves                   [2]reserveDescriptor
	ReclaimPhase               reclaimPhase
	RetiredCount               uint8
	RetiredReserveMask         uint32
	Retired                    [maxRetiredSegments]retiredDescriptor
	RetiredCheckpointCount     uint8
	RetiredCheckpoints         [maxRetiredCheckpoints]retiredCheckpointDescriptor
}

type catalogRecord struct {
	Kind                       catalogKind
	Generation, PreviousTail   uint64
	PreviousHash               [32]byte
	Segment                    SegmentMeta
	AnchorID, AnchorGeneration uint64
	AnchorHash                 [32]byte
	FileID                     fileID
}

// Slot layout (4096 bytes):
//
//	0..184  envelope, catalog/checkpoint cursor, and anchor
//	184..264 active descriptor
//	264..376 optional pending descriptor
//	376..440 two 32-byte reserve descriptors
//	440..496 previous checkpoint descriptor
//	496..504 reclaim phase/count/recycled-reserve mask
//	504..3832 thirty-two 104-byte retired segment descriptors
//	3832..3868 retired-checkpoint count and canonical zero padding
//	3868..4060 four 48-byte retired checkpoint ID/hash descriptors
//	4060..4092 HMAC-SHA256; 4092..4096 CRC32C
func marshalMetadataSlot(dst []byte, slot metadataSlot, key [32]byte) error {
	if len(dst) != metadataSlotBytes || key == ([32]byte{}) || validateMetadataSlot(slot) != nil {
		return ErrBounds
	}
	clear(dst)
	copy(dst[:8], metadataMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], canonicalFormatMarker)
	binary.LittleEndian.PutUint16(dst[10:12], metadataSlotBytes)
	if slot.HasPending {
		binary.LittleEndian.PutUint32(dst[12:16], 1)
	}
	binary.LittleEndian.PutUint64(dst[16:24], slot.Generation)
	copy(dst[24:40], slot.LogID[:])
	binary.LittleEndian.PutUint64(dst[40:48], slot.CatalogTail)
	copy(dst[48:80], slot.CatalogHash[:])
	copy(dst[80:96], slot.CheckpointID[:])
	binary.LittleEndian.PutUint64(dst[96:104], slot.CheckpointTail)
	copy(dst[104:136], slot.CheckpointHash[:])
	binary.LittleEndian.PutUint64(dst[136:144], slot.AnchorID)
	binary.LittleEndian.PutUint64(dst[144:152], slot.AnchorGeneration)
	copy(dst[152:184], slot.AnchorHash[:])
	putActiveDescriptor(dst[184:264], slot.Active)
	if slot.HasPending {
		putPendingDescriptor(dst[264:376], slot.Pending)
	}
	putReserveDescriptor(dst[376:408], slot.Reserves[0])
	putReserveDescriptor(dst[408:440], slot.Reserves[1])
	copy(dst[440:456], slot.PreviousCheckpointID[:])
	binary.LittleEndian.PutUint64(dst[456:464], slot.PreviousCheckpointTail)
	copy(dst[464:496], slot.PreviousCheckpointHash[:])
	dst[496] = byte(slot.ReclaimPhase)
	dst[497] = slot.RetiredCount
	binary.LittleEndian.PutUint32(dst[498:502], slot.RetiredReserveMask)
	for i := 0; i < int(slot.RetiredCount); i++ {
		putRetiredDescriptor(dst[retiredDescriptorsOffset+i*retiredDescriptorBytes:retiredDescriptorsOffset+(i+1)*retiredDescriptorBytes], slot.Retired[i])
	}
	dst[3832] = slot.RetiredCheckpointCount
	for i := 0; i < int(slot.RetiredCheckpointCount); i++ {
		start := retiredCheckpointOffset + i*48
		copy(dst[start:start+16], slot.RetiredCheckpoints[i].ID[:])
		copy(dst[start+16:start+48], slot.RetiredCheckpoints[i].Hash[:])
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(dst[:metadataSlotMACOffset])
	copy(dst[metadataSlotMACOffset:metadataSlotCRCOffset], mac.Sum(nil))
	binary.LittleEndian.PutUint32(dst[metadataSlotCRCOffset:], crc32.Checksum(dst[:metadataSlotCRCOffset], crcTable))
	return nil
}

func unmarshalMetadataSlot(src []byte, key [32]byte) (metadataSlot, error) {
	if len(src) != metadataSlotBytes || key == ([32]byte{}) || string(src[:8]) != string(metadataMagic[:]) || binary.LittleEndian.Uint16(src[8:10]) != canonicalFormatMarker || binary.LittleEndian.Uint16(src[10:12]) != metadataSlotBytes || binary.LittleEndian.Uint32(src[metadataSlotCRCOffset:]) != crc32.Checksum(src[:metadataSlotCRCOffset], crcTable) {
		return metadataSlot{}, ErrCorrupt
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(src[:metadataSlotMACOffset])
	retiredCount := src[497]
	retiredCheckpointCount := src[3832]
	if retiredCount > maxRetiredSegments || retiredCheckpointCount > maxRetiredCheckpoints || !hmac.Equal(src[metadataSlotMACOffset:metadataSlotCRCOffset], mac.Sum(nil)) || !allZero(src[502:retiredDescriptorsOffset]) || !allZero(src[retiredDescriptorsOffset+int(retiredCount)*retiredDescriptorBytes:3832]) || !allZero(src[3833:retiredCheckpointOffset]) || !allZero(src[retiredCheckpointOffset+int(retiredCheckpointCount)*48:metadataSlotMACOffset]) {
		return metadataSlot{}, ErrCorrupt
	}
	flags := binary.LittleEndian.Uint32(src[12:16])
	if flags&^1 != 0 {
		return metadataSlot{}, ErrCorrupt
	}
	slot := metadataSlot{Generation: binary.LittleEndian.Uint64(src[16:24]), CatalogTail: binary.LittleEndian.Uint64(src[40:48]), CheckpointTail: binary.LittleEndian.Uint64(src[96:104]), AnchorID: binary.LittleEndian.Uint64(src[136:144]), AnchorGeneration: binary.LittleEndian.Uint64(src[144:152]), HasPending: flags&1 != 0}
	copy(slot.LogID[:], src[24:40])
	copy(slot.CatalogHash[:], src[48:80])
	copy(slot.CheckpointID[:], src[80:96])
	copy(slot.CheckpointHash[:], src[104:136])
	copy(slot.AnchorHash[:], src[152:184])
	copy(slot.PreviousCheckpointID[:], src[440:456])
	slot.PreviousCheckpointTail = binary.LittleEndian.Uint64(src[456:464])
	copy(slot.PreviousCheckpointHash[:], src[464:496])
	slot.ReclaimPhase, slot.RetiredCount = reclaimPhase(src[496]), retiredCount
	slot.RetiredReserveMask = binary.LittleEndian.Uint32(src[498:502])
	for i := 0; i < int(slot.RetiredCount); i++ {
		slot.Retired[i] = getRetiredDescriptor(src[retiredDescriptorsOffset+i*retiredDescriptorBytes : retiredDescriptorsOffset+(i+1)*retiredDescriptorBytes])
	}
	slot.RetiredCheckpointCount = retiredCheckpointCount
	for i := 0; i < int(retiredCheckpointCount); i++ {
		start := retiredCheckpointOffset + i*48
		copy(slot.RetiredCheckpoints[i].ID[:], src[start:start+16])
		copy(slot.RetiredCheckpoints[i].Hash[:], src[start+16:start+48])
	}
	slot.Active = getActiveDescriptor(src[184:264])
	slot.Pending = getPendingDescriptor(src[264:376])
	if !validReserveDescriptorBytes(src[376:408]) || !validReserveDescriptorBytes(src[408:440]) {
		return metadataSlot{}, ErrCorrupt
	}
	slot.Reserves[0] = getReserveDescriptor(src[376:408])
	slot.Reserves[1] = getReserveDescriptor(src[408:440])
	if validateMetadataSlot(slot) != nil {
		return metadataSlot{}, ErrCorrupt
	}
	return slot, nil
}

func validateMetadataSlot(slot metadataSlot) error {
	if slot.Generation == 0 || slot.LogID == ([16]byte{}) || slot.CatalogTail < metadataCatalogStart || (slot.CatalogTail-metadataCatalogStart)%catalogRecordBytes != 0 || slot.Active.FileID == (fileID{}) || slot.Active.ID == 0 || slot.Active.Generation == 0 || slot.Active.Capacity < segmentHeaderBytes || slot.Active.Capacity >= 1<<32 || slot.Active.PreviousID+1 != slot.Active.ID || !slot.HasPending && slot.Pending != (pendingDescriptor{}) {
		return ErrCorrupt
	}
	if (slot.CatalogTail == metadataCatalogStart) != (slot.CatalogHash == ([32]byte{})) {
		return ErrCorrupt
	}
	checkpoint := slot.CheckpointID != ([16]byte{}) || slot.CheckpointTail != 0 || slot.CheckpointHash != ([32]byte{})
	if checkpoint && (slot.CheckpointID == ([16]byte{}) || slot.CheckpointHash == ([32]byte{}) || slot.CheckpointTail < metadataCatalogStart || slot.CheckpointTail > slot.CatalogTail || (slot.CheckpointTail-metadataCatalogStart)%catalogRecordBytes != 0) {
		return ErrCorrupt
	}
	previousCheckpoint := slot.PreviousCheckpointID != ([16]byte{}) || slot.PreviousCheckpointTail != 0 || slot.PreviousCheckpointHash != ([32]byte{})
	if previousCheckpoint && (slot.PreviousCheckpointID == ([16]byte{}) || slot.PreviousCheckpointID == slot.CheckpointID || slot.PreviousCheckpointHash == ([32]byte{}) || slot.PreviousCheckpointTail < metadataCatalogStart || slot.PreviousCheckpointTail > slot.CheckpointTail || (slot.PreviousCheckpointTail-metadataCatalogStart)%catalogRecordBytes != 0) {
		return ErrCorrupt
	}
	if previousCheckpoint && !checkpoint {
		return ErrCorrupt
	}
	anchor := slot.AnchorID != 0 || slot.AnchorGeneration != 0 || slot.AnchorHash != ([32]byte{})
	if anchor != (slot.AnchorID != 0 && slot.AnchorGeneration != 0 && slot.AnchorHash != ([32]byte{})) {
		return ErrCorrupt
	}
	if slot.Active.ID == 1 {
		if slot.Active.PreviousID != 0 || slot.Active.PreviousHash != ([32]byte{}) {
			return ErrCorrupt
		}
	} else if slot.Active.PreviousID == 0 || slot.Active.PreviousHash == ([32]byte{}) {
		return ErrCorrupt
	}
	if slot.HasPending && (slot.Pending.FileID == (fileID{}) || slot.Pending.ID+1 != slot.Active.ID || slot.Pending.Generation+1 != slot.Active.Generation || slot.Pending.Hash != slot.Active.PreviousHash || slot.Pending.Bytes < segmentHeaderBytes) {
		return ErrCorrupt
	}
	if slot.CatalogTail == metadataCatalogStart {
		lastID, lastGeneration, lastHash := slot.AnchorID, slot.AnchorGeneration, slot.AnchorHash
		if slot.HasPending {
			if slot.Pending.ID != lastID+1 || slot.Pending.Generation != lastGeneration+1 || slot.Pending.PreviousHash != lastHash {
				return ErrCorrupt
			}
		} else if slot.Active.PreviousID != lastID || slot.Active.Generation != lastGeneration+1 || slot.Active.PreviousHash != lastHash {
			return ErrCorrupt
		}
	}
	for i := range slot.Reserves {
		reserve := slot.Reserves[i]
		if reserve.Ready != (reserve.FileID != (fileID{}) && reserve.Capacity != 0) || reserve.Ready && reserve.Capacity != slot.Active.Capacity {
			return ErrCorrupt
		}
		if reserve.Ready && (reserve.FileID == slot.Active.FileID || slot.HasPending && reserve.FileID == slot.Pending.FileID) {
			return ErrCorrupt
		}
	}
	if slot.Reserves[0].Ready && slot.Reserves[1].Ready && slot.Reserves[0].FileID == slot.Reserves[1].FileID {
		return ErrCorrupt
	}
	if slot.ReclaimPhase > reclaimDurable || (slot.ReclaimPhase == reclaimNone) != (slot.RetiredCount == 0) || slot.RetiredCount > maxRetiredSegments || slot.RetiredReserveMask>>slot.RetiredCount != 0 || slot.ReclaimPhase != reclaimDurable && slot.RetiredReserveMask != 0 {
		return ErrCorrupt
	}
	for i := 0; i < int(slot.RetiredCount); i++ {
		retired := slot.Retired[i]
		if retired.ID == 0 || retired.Generation == 0 || retired.FileID == (fileID{}) || retired.Bytes < segmentHeaderBytes+sealedIndexHeaderBytes+segmentFooterBytes || retired.Bytes > slot.Active.Capacity || retired.Hash == ([32]byte{}) || (retired.ID == 1) != (retired.PreviousHash == ([32]byte{})) || i != 0 && (retired.ID != slot.Retired[i-1].ID+1 || retired.Generation != slot.Retired[i-1].Generation+1 || retired.PreviousHash != slot.Retired[i-1].Hash) {
			return ErrCorrupt
		}
		if retired.FileID == slot.Active.FileID || slot.HasPending && retired.FileID == slot.Pending.FileID {
			return ErrCorrupt
		}
		matchedReserve := false
		for j := range slot.Reserves {
			if slot.Reserves[j].Ready && retired.FileID == slot.Reserves[j].FileID {
				matchedReserve = true
			}
		}
		if matchedReserve != (slot.RetiredReserveMask&(uint32(1)<<i) != 0) {
			return ErrCorrupt
		}
		for j := 0; j < i; j++ {
			if retired.FileID == slot.Retired[j].FileID {
				return ErrCorrupt
			}
		}
	}
	if slot.RetiredCount != 0 {
		last := slot.Retired[slot.RetiredCount-1]
		if last.ID != slot.AnchorID || last.Generation != slot.AnchorGeneration || last.Hash != slot.AnchorHash {
			return ErrCorrupt
		}
	}
	if slot.RetiredCheckpointCount > maxRetiredCheckpoints {
		return ErrCorrupt
	}
	for i := 0; i < int(slot.RetiredCheckpointCount); i++ {
		retired := slot.RetiredCheckpoints[i]
		if retired.ID == (fileID{}) || retired.Hash == ([32]byte{}) || retired.ID == fileID(slot.CheckpointID) || retired.ID == fileID(slot.PreviousCheckpointID) {
			return ErrCorrupt
		}
		for j := 0; j < i; j++ {
			if retired.ID == slot.RetiredCheckpoints[j].ID {
				return ErrCorrupt
			}
		}
	}
	return nil
}

func putRetiredDescriptor(dst []byte, d retiredDescriptor) {
	binary.LittleEndian.PutUint64(dst[0:8], d.ID)
	binary.LittleEndian.PutUint64(dst[8:16], d.Generation)
	copy(dst[16:32], d.FileID[:])
	binary.LittleEndian.PutUint64(dst[32:40], d.Bytes)
	copy(dst[40:72], d.PreviousHash[:])
	copy(dst[72:104], d.Hash[:])
}

func getRetiredDescriptor(src []byte) (d retiredDescriptor) {
	d.ID = binary.LittleEndian.Uint64(src[0:8])
	d.Generation = binary.LittleEndian.Uint64(src[8:16])
	copy(d.FileID[:], src[16:32])
	d.Bytes = binary.LittleEndian.Uint64(src[32:40])
	copy(d.PreviousHash[:], src[40:72])
	copy(d.Hash[:], src[72:104])
	return d
}

func putActiveDescriptor(dst []byte, d activeDescriptor) {
	copy(dst[:16], d.FileID[:])
	binary.LittleEndian.PutUint64(dst[16:24], d.ID)
	binary.LittleEndian.PutUint64(dst[24:32], d.Generation)
	binary.LittleEndian.PutUint64(dst[32:40], d.PreviousID)
	copy(dst[40:72], d.PreviousHash[:])
	binary.LittleEndian.PutUint64(dst[72:80], d.Capacity)
}

func getActiveDescriptor(src []byte) (d activeDescriptor) {
	copy(d.FileID[:], src[:16])
	d.ID = binary.LittleEndian.Uint64(src[16:24])
	d.Generation = binary.LittleEndian.Uint64(src[24:32])
	d.PreviousID = binary.LittleEndian.Uint64(src[32:40])
	copy(d.PreviousHash[:], src[40:72])
	d.Capacity = binary.LittleEndian.Uint64(src[72:80])
	return d
}

func putPendingDescriptor(dst []byte, d pendingDescriptor) {
	copy(dst[:16], d.FileID[:])
	binary.LittleEndian.PutUint64(dst[16:24], d.ID)
	binary.LittleEndian.PutUint64(dst[24:32], d.Generation)
	binary.LittleEndian.PutUint64(dst[32:40], d.Bytes)
	binary.LittleEndian.PutUint64(dst[40:48], d.Records)
	copy(dst[48:80], d.PreviousHash[:])
	copy(dst[80:112], d.Hash[:])
}

func getPendingDescriptor(src []byte) (d pendingDescriptor) {
	copy(d.FileID[:], src[:16])
	d.ID = binary.LittleEndian.Uint64(src[16:24])
	d.Generation = binary.LittleEndian.Uint64(src[24:32])
	d.Bytes = binary.LittleEndian.Uint64(src[32:40])
	d.Records = binary.LittleEndian.Uint64(src[40:48])
	copy(d.PreviousHash[:], src[48:80])
	copy(d.Hash[:], src[80:112])
	return d
}

func putReserveDescriptor(dst []byte, d reserveDescriptor) {
	copy(dst[:16], d.FileID[:])
	binary.LittleEndian.PutUint64(dst[16:24], d.Capacity)
	if d.Ready {
		dst[24] = 1
	}
}

func getReserveDescriptor(src []byte) (d reserveDescriptor) {
	copy(d.FileID[:], src[:16])
	d.Capacity = binary.LittleEndian.Uint64(src[16:24])
	d.Ready = src[24] == 1
	return d
}

func validReserveDescriptorBytes(src []byte) bool {
	if len(src) != 32 || src[24] > 1 || !allZero(src[25:32]) {
		return false
	}
	if src[24] == 0 {
		return allZero(src[:24])
	}
	return !allZero(src[:16]) && binary.LittleEndian.Uint64(src[16:24]) != 0
}

// Catalog record layout (192 bytes): 60-byte chain envelope, 96-byte union
// payload, 32-byte HMAC, and 4-byte CRC. A seal record consumes the payload
// exactly: id/gen/fileID/bytes/records/index geometry/hash. Previous segment
// identity is derived from the authenticated catalog chain and segment header.
func marshalCatalogRecord(dst []byte, record catalogRecord, key [32]byte) ([32]byte, error) {
	if len(dst) != catalogRecordBytes || key == ([32]byte{}) || validateCatalogRecord(record) != nil {
		return [32]byte{}, ErrBounds
	}
	clear(dst)
	copy(dst[:8], catalogMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], canonicalFormatMarker)
	dst[10] = byte(record.Kind)
	binary.LittleEndian.PutUint64(dst[12:20], record.Generation)
	binary.LittleEndian.PutUint64(dst[20:28], record.PreviousTail)
	copy(dst[28:60], record.PreviousHash[:])
	switch record.Kind {
	case catalogSeal:
		s := record.Segment
		binary.LittleEndian.PutUint64(dst[60:68], s.ID)
		binary.LittleEndian.PutUint64(dst[68:76], s.Generation)
		copy(dst[76:92], record.FileID[:])
		binary.LittleEndian.PutUint64(dst[92:100], s.Bytes)
		binary.LittleEndian.PutUint64(dst[100:108], s.Records)
		binary.LittleEndian.PutUint64(dst[108:116], s.IndexOffset)
		binary.LittleEndian.PutUint64(dst[116:124], s.IndexBytes)
		copy(dst[124:156], s.Hash[:])
	case catalogAnchor:
		binary.LittleEndian.PutUint64(dst[60:68], record.AnchorID)
		binary.LittleEndian.PutUint64(dst[68:76], record.AnchorGeneration)
		copy(dst[76:108], record.AnchorHash[:])
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(dst[:catalogMACOffset])
	copy(dst[catalogMACOffset:catalogCRCOffset], mac.Sum(nil))
	binary.LittleEndian.PutUint32(dst[catalogCRCOffset:], crc32.Checksum(dst[:catalogCRCOffset], crcTable))
	return sha256.Sum256(dst), nil
}

func validateCatalogRecord(record catalogRecord) error {
	if record.Generation == 0 || record.Kind < catalogSeal || record.Kind > catalogAnchor || record.PreviousTail < metadataCatalogStart || (record.PreviousTail-metadataCatalogStart)%catalogRecordBytes != 0 || (record.PreviousTail == metadataCatalogStart) != (record.PreviousHash == ([32]byte{})) {
		return ErrCorrupt
	}
	switch record.Kind {
	case catalogSeal:
		if record.Segment.ID == 0 || record.Segment.Generation == 0 || record.FileID == (fileID{}) || !validSegmentGeometry(record.Segment) || record.Segment.PreviousHash != ([32]byte{}) || record.AnchorID != 0 || record.AnchorGeneration != 0 || record.AnchorHash != ([32]byte{}) {
			return ErrCorrupt
		}
	case catalogAnchor:
		if record.AnchorID == 0 || record.AnchorGeneration == 0 || record.AnchorHash == ([32]byte{}) || record.Segment != (SegmentMeta{}) || record.FileID != (fileID{}) {
			return ErrCorrupt
		}
	}
	return nil
}

func unmarshalCatalogRecord(src []byte, key [32]byte) (catalogRecord, [32]byte, error) {
	if len(src) != catalogRecordBytes || key == ([32]byte{}) || string(src[:8]) != string(catalogMagic[:]) || binary.LittleEndian.Uint16(src[8:10]) != canonicalFormatMarker || src[11] != 0 || binary.LittleEndian.Uint32(src[catalogCRCOffset:]) != crc32.Checksum(src[:catalogCRCOffset], crcTable) {
		return catalogRecord{}, [32]byte{}, ErrCorrupt
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(src[:catalogMACOffset])
	if !hmac.Equal(src[catalogMACOffset:catalogCRCOffset], mac.Sum(nil)) {
		return catalogRecord{}, [32]byte{}, ErrCorrupt
	}
	r := catalogRecord{Kind: catalogKind(src[10]), Generation: binary.LittleEndian.Uint64(src[12:20]), PreviousTail: binary.LittleEndian.Uint64(src[20:28])}
	copy(r.PreviousHash[:], src[28:60])
	switch r.Kind {
	case catalogSeal:
		r.Segment = SegmentMeta{ID: binary.LittleEndian.Uint64(src[60:68]), Generation: binary.LittleEndian.Uint64(src[68:76]), Bytes: binary.LittleEndian.Uint64(src[92:100]), Records: binary.LittleEndian.Uint64(src[100:108]), IndexOffset: binary.LittleEndian.Uint64(src[108:116]), IndexBytes: binary.LittleEndian.Uint64(src[116:124]), State: SegmentSealed}
		copy(r.FileID[:], src[76:92])
		copy(r.Segment.Hash[:], src[124:156])
		if r.FileID == (fileID{}) || !validSegmentGeometry(r.Segment) {
			return catalogRecord{}, [32]byte{}, ErrCorrupt
		}
	case catalogAnchor:
		r.AnchorID = binary.LittleEndian.Uint64(src[60:68])
		r.AnchorGeneration = binary.LittleEndian.Uint64(src[68:76])
		copy(r.AnchorHash[:], src[76:108])
		if r.AnchorID == 0 || r.AnchorGeneration == 0 || r.AnchorHash == ([32]byte{}) || !allZero(src[108:156]) {
			return catalogRecord{}, [32]byte{}, ErrCorrupt
		}
	default:
		return catalogRecord{}, [32]byte{}, fmt.Errorf("%w: catalog kind", ErrCorrupt)
	}
	if r.Generation == 0 {
		return catalogRecord{}, [32]byte{}, ErrCorrupt
	}
	if validateCatalogRecord(r) != nil {
		return catalogRecord{}, [32]byte{}, ErrCorrupt
	}
	return r, sha256.Sum256(src), nil
}
