package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	KeyDirectoryPayloadHeaderSize = 32
	KeyDirectoryLeafRecordSize    = 16
	KeyDirectoryBranchRecordSize  = 40
	keyDirectoryVersion           = DevelopmentFormatVersion
	keyDirectoryKnownFlags        = uint8(0)
	keyDirectoryMaxLevel          = uint8(10)
)

// ErrKeyDirectoryCorrupt reports a checksum-valid common page whose key
// directory payload is malformed. Complete key spellings are retained in the
// tree, so lookup never trusts a hash or fingerprint collision.
var ErrKeyDirectoryCorrupt = errors.New("vibejson: corrupt Store key directory")

// KeyLocation is the stable Store row address stored in a key-directory leaf.
type KeyLocation struct {
	Chunk uint32
	Slot  uint8
}

// KeyDirectoryEntry is one transient leaf input. Key is borrowed only for the
// encode call. Entries must be strictly ordered by raw key bytes.
type KeyDirectoryEntry struct {
	Key      []byte
	Location KeyLocation
}

// KeyDirectoryChild is one transient branch input. Lower is the inclusive
// lower bound of the child subtree. Children must be strictly ordered by
// Lower. Ref names another key-directory page one level closer to a leaf.
type KeyDirectoryChild struct {
	Lower []byte
	Ref   PageRef
}

// KeyDirectoryHeader describes one immutable B+tree node. Level zero is a
// leaf; higher levels contain lower-bound/child pairs.
type KeyDirectoryHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	Level      uint8
	Flags      uint8
}

// KeyDirectoryView is a checksum-verified borrowed view. It retains one slice
// regardless of entry count and performs allocation-free binary searches.
type KeyDirectoryView struct {
	header    KeyDirectoryHeader
	payload   []byte
	dataStart int
	count     uint16
}

// EncodeKeyDirectoryLeaf writes one complete leaf into dst. Empty keys are
// valid. Locations must fall below chunkHighWater and chunkDocuments.
func EncodeKeyDirectoryLeaf(dst []byte, header KeyDirectoryHeader, entries []KeyDirectoryEntry, nextLogicalID uint64, chunkHighWater uint32, chunkDocuments uint8) ([]byte, error) {
	if header.Level != 0 {
		return nil, fmt.Errorf("%w: leaf level", ErrInvalidWrite)
	}
	if chunkHighWater == 0 || chunkDocuments == 0 || chunkDocuments > 64 {
		return nil, fmt.Errorf("%w: key location bounds", ErrInvalidWrite)
	}
	dataLength := 0
	for i, entry := range entries {
		if i != 0 && bytes.Compare(entries[i-1].Key, entry.Key) >= 0 {
			return nil, fmt.Errorf("%w: unordered or duplicate key", ErrInvalidWrite)
		}
		if entry.Location.Chunk >= chunkHighWater || entry.Location.Slot >= chunkDocuments {
			return nil, fmt.Errorf("%w: key location", ErrInvalidWrite)
		}
		if len(entry.Key) > int(^uint(0)>>1)-dataLength ||
			uint64(dataLength)+uint64(len(entry.Key)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("%w: key bytes", ErrInvalidWrite)
		}
		dataLength += len(entry.Key)
	}
	if err := validateKeyDirectoryHeader(header, len(entries), KeyDirectoryLeafRecordSize, dataLength, nextLogicalID); err != nil {
		return nil, err
	}
	payloadLength := KeyDirectoryPayloadHeaderSize + len(entries)*KeyDirectoryLeafRecordSize + dataLength
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageKeyDirectory,
	})
	if err != nil {
		return nil, err
	}
	encodeKeyDirectoryHeader(payload, header, len(entries), dataLength)
	dataStart := KeyDirectoryPayloadHeaderSize + len(entries)*KeyDirectoryLeafRecordSize
	position := 0
	for i, entry := range entries {
		copy(payload[dataStart+position:], entry.Key)
		position += len(entry.Key)
		record := payload[KeyDirectoryPayloadHeaderSize+i*KeyDirectoryLeafRecordSize:]
		binary.LittleEndian.PutUint32(record[0:4], uint32(position))
		binary.LittleEndian.PutUint32(record[4:8], entry.Location.Chunk)
		record[8] = entry.Location.Slot
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// EncodeKeyDirectoryBranch writes one complete branch into dst. Each child
// must be a base-size key-directory page from the same Store history.
func EncodeKeyDirectoryBranch(dst []byte, header KeyDirectoryHeader, children []KeyDirectoryChild, fileEnd, nextLogicalID uint64) ([]byte, error) {
	if header.Level == 0 {
		return nil, fmt.Errorf("%w: branch level", ErrInvalidWrite)
	}
	if len(children) > 64 {
		return nil, fmt.Errorf("%w: key branch fanout", ErrInvalidWrite)
	}
	dataLength := 0
	var seen chunkDirectoryRefSet
	for i, child := range children {
		if i != 0 && bytes.Compare(children[i-1].Lower, child.Lower) >= 0 {
			return nil, fmt.Errorf("%w: unordered or duplicate lower bound", ErrInvalidWrite)
		}
		if err := validateKeyDirectoryChild(header, child.Ref, fileEnd, nextLogicalID); err != nil {
			return nil, err
		}
		if !seen.add(child.Ref) {
			return nil, fmt.Errorf("%w: duplicate key child", ErrInvalidWrite)
		}
		if len(child.Lower) > int(^uint(0)>>1)-dataLength ||
			uint64(dataLength)+uint64(len(child.Lower)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("%w: lower-bound bytes", ErrInvalidWrite)
		}
		dataLength += len(child.Lower)
	}
	if err := validateKeyDirectoryHeader(header, len(children), KeyDirectoryBranchRecordSize, dataLength, nextLogicalID); err != nil {
		return nil, err
	}
	payloadLength := KeyDirectoryPayloadHeaderSize + len(children)*KeyDirectoryBranchRecordSize + dataLength
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageKeyDirectory,
	})
	if err != nil {
		return nil, err
	}
	encodeKeyDirectoryHeader(payload, header, len(children), dataLength)
	dataStart := KeyDirectoryPayloadHeaderSize + len(children)*KeyDirectoryBranchRecordSize
	position := 0
	for i, child := range children {
		copy(payload[dataStart+position:], child.Lower)
		position += len(child.Lower)
		record := payload[KeyDirectoryPayloadHeaderSize+i*KeyDirectoryBranchRecordSize:]
		binary.LittleEndian.PutUint32(record[0:4], uint32(position))
		encodePageRef(record[8:8+PageRefSize], child.Ref)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func encodeKeyDirectoryHeader(payload []byte, header KeyDirectoryHeader, count, dataLength int) {
	binary.LittleEndian.PutUint32(payload[0:4], keyDirectoryVersion)
	payload[4] = header.Level
	payload[5] = header.Flags
	binary.LittleEndian.PutUint16(payload[6:8], uint16(count))
	binary.LittleEndian.PutUint32(payload[8:12], uint32(dataLength))
}

// OpenKeyDirectoryPage validates a complete leaf or branch once. Physical and
// logical bounds come from the selecting state root.
func OpenKeyDirectoryPage(src []byte, fileEnd, nextLogicalID uint64, chunkHighWater uint32, chunkDocuments uint8) (KeyDirectoryView, error) {
	if chunkHighWater == 0 || chunkDocuments == 0 || chunkDocuments > 64 {
		return KeyDirectoryView{}, fmt.Errorf("%w: location bounds", ErrKeyDirectoryCorrupt)
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return KeyDirectoryView{}, fmt.Errorf("%w: %w", ErrKeyDirectoryCorrupt, err)
	}
	if pageHeader.Kind != PageKeyDirectory || len(payload) < KeyDirectoryPayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != keyDirectoryVersion ||
		!allZero(payload[12:KeyDirectoryPayloadHeaderSize]) {
		return KeyDirectoryView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrKeyDirectoryCorrupt)
	}
	header := KeyDirectoryHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		Level: payload[4], Flags: payload[5],
	}
	count := int(binary.LittleEndian.Uint16(payload[6:8]))
	dataLength64 := uint64(binary.LittleEndian.Uint32(payload[8:12]))
	if dataLength64 > uint64(^uint(0)>>1) {
		return KeyDirectoryView{}, fmt.Errorf("%w: key bytes exceed address space", ErrKeyDirectoryCorrupt)
	}
	dataLength := int(dataLength64)
	recordSize := KeyDirectoryLeafRecordSize
	if header.Level != 0 {
		recordSize = KeyDirectoryBranchRecordSize
	}
	if err := validateKeyDirectoryHeader(header, count, recordSize, dataLength, nextLogicalID); err != nil {
		return KeyDirectoryView{}, fmt.Errorf("%w: %v", ErrKeyDirectoryCorrupt, err)
	}
	dataStart := KeyDirectoryPayloadHeaderSize + count*recordSize
	if dataStart > len(payload) || dataLength != len(payload)-dataStart {
		return KeyDirectoryView{}, fmt.Errorf("%w: payload length", ErrKeyDirectoryCorrupt)
	}
	var previous []byte
	var previousEnd uint32
	var seen chunkDirectoryRefSet
	for i := 0; i < count; i++ {
		record := payload[KeyDirectoryPayloadHeaderSize+i*recordSize:]
		keyEnd := binary.LittleEndian.Uint32(record[0:4])
		if keyEnd < previousEnd || uint64(keyEnd) > uint64(dataLength) {
			return KeyDirectoryView{}, fmt.Errorf("%w: key bounds", ErrKeyDirectoryCorrupt)
		}
		key := payload[dataStart+int(previousEnd) : dataStart+int(keyEnd)]
		if i != 0 && bytes.Compare(previous, key) >= 0 {
			return KeyDirectoryView{}, fmt.Errorf("%w: key order", ErrKeyDirectoryCorrupt)
		}
		if header.Level == 0 {
			if !allZero(record[9:16]) ||
				binary.LittleEndian.Uint32(record[4:8]) >= chunkHighWater || record[8] >= chunkDocuments {
				return KeyDirectoryView{}, fmt.Errorf("%w: leaf location or reserved bytes", ErrKeyDirectoryCorrupt)
			}
		} else {
			if !allZero(record[4:8]) || !pageRefReservedZero(record[8:8+PageRefSize]) {
				return KeyDirectoryView{}, fmt.Errorf("%w: branch reserved bytes", ErrKeyDirectoryCorrupt)
			}
			ref := decodePageRef(record[8 : 8+PageRefSize])
			if err := validateKeyDirectoryChild(header, ref, fileEnd, nextLogicalID); err != nil || !seen.add(ref) {
				return KeyDirectoryView{}, fmt.Errorf("%w: branch child", ErrKeyDirectoryCorrupt)
			}
		}
		previous = key
		previousEnd = keyEnd
	}
	if int(previousEnd) != dataLength {
		return KeyDirectoryView{}, fmt.Errorf("%w: unreferenced key bytes", ErrKeyDirectoryCorrupt)
	}
	return KeyDirectoryView{
		header: header, payload: payload, dataStart: dataStart, count: uint16(count),
	}, nil
}

// AdmittedKeyDirectoryPage reconstructs a view of a page whose CRC32C, common
// header, and complete key-directory payload PageCache already validated at
// admission. Calling it on arbitrary bytes is invalid: the returned view
// indexes the payload with offsets taken from the page, so unvalidated bytes
// produce an out-of-range panic rather than an error.
//
// It exists because OpenKeyDirectoryPage is O(entries) — it re-checksums the
// whole page and re-walks every record's ordering, bounds, and reserved bytes.
// Paying that at every level of every descent made a resident point read cost
// several microseconds, most of it re-proving a fact established once when the
// page was admitted.
func AdmittedKeyDirectoryPage(src []byte) KeyDirectoryView {
	pageHeader, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	header := KeyDirectoryHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		Level: payload[4], Flags: payload[5],
	}
	count := int(binary.LittleEndian.Uint16(payload[6:8]))
	recordSize := KeyDirectoryLeafRecordSize
	if header.Level != 0 {
		recordSize = KeyDirectoryBranchRecordSize
	}
	return KeyDirectoryView{
		header: header, payload: payload,
		dataStart: KeyDirectoryPayloadHeaderSize + count*recordSize,
		count:     uint16(count),
	}
}

// Header returns value-only node identity and level metadata.
func (v KeyDirectoryView) Header() KeyDirectoryHeader { return v.header }

// Len returns the number of leaf entries or branch children.
func (v KeyDirectoryView) Len() int { return int(v.count) }

// Lookup resolves a complete key in a leaf. Branch views return false.
func (v KeyDirectoryView) Lookup(key []byte) (KeyLocation, bool) {
	if v.header.Level != 0 {
		return KeyLocation{}, false
	}
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		got := v.keyAt(middle, KeyDirectoryLeafRecordSize)
		if bytes.Compare(got, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low >= int(v.count) || !bytes.Equal(v.keyAt(low, KeyDirectoryLeafRecordSize), key) {
		return KeyLocation{}, false
	}
	record := v.payload[KeyDirectoryPayloadHeaderSize+low*KeyDirectoryLeafRecordSize:]
	return KeyLocation{
		Chunk: binary.LittleEndian.Uint32(record[4:8]), Slot: record[8],
	}, true
}

// EntryAt returns a borrowed leaf entry at rank.
func (v KeyDirectoryView) EntryAt(rank int) (KeyDirectoryEntry, bool) {
	if v.header.Level != 0 || rank < 0 || rank >= int(v.count) {
		return KeyDirectoryEntry{}, false
	}
	record := v.payload[KeyDirectoryPayloadHeaderSize+rank*KeyDirectoryLeafRecordSize:]
	return KeyDirectoryEntry{
		Key: v.keyAt(rank, KeyDirectoryLeafRecordSize),
		Location: KeyLocation{
			Chunk: binary.LittleEndian.Uint32(record[4:8]), Slot: record[8],
		},
	}, true
}

// Child selects the branch whose inclusive lower bound is greatest but not
// greater than key. A key before the first subtree returns false.
func (v KeyDirectoryView) Child(key []byte) (PageRef, bool) {
	if v.header.Level == 0 || v.count == 0 {
		return PageRef{}, false
	}
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if bytes.Compare(v.keyAt(middle, KeyDirectoryBranchRecordSize), key) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == 0 {
		return PageRef{}, false
	}
	return v.childRefAt(low - 1)
}

// ChildAt returns the borrowed lower bound and physical child at rank.
func (v KeyDirectoryView) ChildAt(rank int) (KeyDirectoryChild, bool) {
	if v.header.Level == 0 || rank < 0 || rank >= int(v.count) {
		return KeyDirectoryChild{}, false
	}
	ref, _ := v.childRefAt(rank)
	return KeyDirectoryChild{Lower: v.keyAt(rank, KeyDirectoryBranchRecordSize), Ref: ref}, true
}

func (v KeyDirectoryView) keyAt(rank, recordSize int) []byte {
	record := v.payload[KeyDirectoryPayloadHeaderSize+rank*recordSize:]
	end := binary.LittleEndian.Uint32(record[0:4])
	start := uint32(0)
	if rank != 0 {
		previous := v.payload[KeyDirectoryPayloadHeaderSize+(rank-1)*recordSize:]
		start = binary.LittleEndian.Uint32(previous[0:4])
	}
	return v.payload[v.dataStart+int(start) : v.dataStart+int(end) : v.dataStart+int(end)]
}

func (v KeyDirectoryView) childRefAt(rank int) (PageRef, bool) {
	if rank < 0 || rank >= int(v.count) {
		return PageRef{}, false
	}
	record := v.payload[KeyDirectoryPayloadHeaderSize+rank*KeyDirectoryBranchRecordSize:]
	return decodePageRef(record[8 : 8+PageRefSize]), true
}

func validateKeyDirectoryHeader(header KeyDirectoryHeader, count, recordSize, dataLength int, nextLogicalID uint64) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) || header.Level > keyDirectoryMaxLevel || header.Flags&^keyDirectoryKnownFlags != 0 ||
		count <= 0 || count > int(^uint16(0)) || dataLength < 0 || uint64(dataLength) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: key-directory identity, count, or flags", ErrInvalidWrite)
	}
	if recordSize == KeyDirectoryBranchRecordSize && count > 64 {
		return fmt.Errorf("%w: key branch fanout", ErrInvalidWrite)
	}
	payloadLength := uint64(KeyDirectoryPayloadHeaderSize) + uint64(count)*uint64(recordSize) + uint64(dataLength)
	if payloadLength > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: key-directory payload does not fit", ErrInvalidWrite)
	}
	return nil
}

func validateKeyDirectoryChild(header KeyDirectoryHeader, ref PageRef, fileEnd, nextLogicalID uint64) error {
	pageSize := uint64(header.PageSize)
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		return err
	}
	if fileEnd < layout.DataStart || fileEnd > maxSuperblockFileOffset || fileEnd%pageSize != 0 ||
		ref.Kind != PageKeyDirectory || ref.Flags != 0 || ref.Aux != 0 || ref.Length != header.PageSize ||
		ref.Generation == 0 || ref.Generation > header.Generation ||
		ref.LogicalID <= StateRootLogicalID || ref.LogicalID >= nextLogicalID || ref.LogicalID == header.LogicalID ||
		ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
		ref.Offset > maxSuperblockFileOffset || ref.Offset > fileEnd-pageSize {
		return fmt.Errorf("%w: invalid key-directory child", ErrInvalidWrite)
	}
	return nil
}
