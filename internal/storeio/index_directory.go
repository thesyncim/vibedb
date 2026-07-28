package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	IndexDirectoryPayloadHeaderSize = 32
	IndexDirectoryLeafRecordSize    = 32
	IndexDirectoryBranchRecordSize  = 48
	indexDirectoryVersion           = DevelopmentFormatVersion
	indexDirectoryKnownFlags        = uint8(0)
	indexDirectoryEntryKnownFlags   = IndexEntryCollision
)

const (
	// IndexEntryCollision marks a certificate whose hash stream contains more
	// than one exact scalar or compound tuple. The representative then proves
	// nothing about the individual rows behind the mask and readers must
	// recheck their documents.
	IndexEntryCollision uint16 = 1 << 0
)

const (
	// IndexEntryInlineMask is the only defined leaf-record encoding: the
	// 64-bit stable-slot mask for one chunk lives in the record itself. Every
	// other Kind is rejected on both encode and decode so that a future
	// out-of-line encoding can never be silently misread as an inline mask by
	// a reader that predates it.
	IndexEntryInlineMask uint8 = 0
)

// ErrIndexDirectoryCorrupt reports malformed durable secondary-index routing
// metadata. Tuple hashes only select candidates; Store query execution still
// performs its ordinary exact scalar recheck.
var ErrIndexDirectoryCorrupt = errors.New("vibejson: corrupt Store index directory")

// IndexDirectoryKey is the canonical routing order for one exact tuple stream.
type IndexDirectoryKey struct {
	IndexID   uint32
	TupleHash uint64
	Chunk     uint32
}

// CertSpan locates one exact-value certificate inside a byte arena. Spans
// rather than slices are the currency of the leaf format because a decoded
// certificate borrows an evictable page frame: an offset cannot outlive its
// arena the way a slice header silently can. Length is bounded by
// IndexDirectoryMaxCertificate and therefore always fits the record's uint16
// field; Offset is deliberately wider than the record field because a caller
// arena accumulates the certificates of many leaves at once.
type CertSpan struct {
	Offset uint32
	Length uint16
}

// IndexDirectoryEntry is one leaf mapping: the chunk slot mask for one
// (index, tuple hash, chunk) triple, inline in the record. Bits is never zero
// — an entry with an empty mask is a live routing key that answers "no rows",
// which is indistinguishable from a correct answer and therefore silently
// loses data. Writers must delete such a key instead.
type IndexDirectoryEntry struct {
	Key   IndexDirectoryKey
	Bits  uint64
	Cert  CertSpan
	Flags uint16
	Kind  uint8
}

// IndexDirectoryChild is one branch lower bound and child page.
type IndexDirectoryChild struct {
	Lower IndexDirectoryKey
	Ref   PageRef
}

// IndexDirectoryHeader describes one immutable B+tree node.
type IndexDirectoryHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	Level      uint8
	Flags      uint8
}

// IndexDirectoryView is one checksum-verified borrowed node.
type IndexDirectoryView struct {
	header    IndexDirectoryHeader
	payload   []byte
	count     uint16
	heapStart uint32
}

// IndexDirectoryMaxCertificate is the largest certificate one leaf record may
// carry. The bound is not about a single record fitting: it is what makes a
// two-way leaf split always possible. A rewritten leaf holds at most one more
// entry than the page budget B admits, and cutting a dedup run in half copies
// one representative twice, so the oversized sequence costs at most
// B+32+2*max. A greedy left prefix keeps more than B-(32+max), leaving a right
// half below 64+4*max, which fits only while max stays under a quarter of the
// budget. Writers whose representative exceeds this simply omit it and accept
// the document recheck, exactly as they already do for containers.
func IndexDirectoryMaxCertificate(pageSize uint32) int {
	budget := indexDirectoryLeafBudget(pageSize)
	if budget <= 96 {
		return 0
	}
	return (budget - 96) / 4
}

// EncodeIndexDirectoryLeaf writes a strictly ordered tuple-routing leaf. Every
// entry's Cert span addresses certificates; the encoder copies the bytes into
// a page-local heap that grows from the end of the record array to the end of
// the payload. Consecutive entries carrying identical representatives share
// one heap region: leaf order is (IndexID, TupleHash, Chunk), so one hash
// stream spanning many chunks stores its representative exactly once.
func EncodeIndexDirectoryLeaf(dst []byte, header IndexDirectoryHeader, entries []IndexDirectoryEntry, certificates []byte, nextLogicalID uint64, indexHighWater uint32) ([]byte, error) {
	if header.Level != 0 || indexHighWater == 0 {
		return nil, fmt.Errorf("%w: index leaf level or bounds", ErrInvalidWrite)
	}
	heapBytes, err := indexDirectoryHeapBytes(entries, certificates, IndexDirectoryMaxCertificate(header.PageSize))
	if err != nil {
		return nil, err
	}
	if err := validateIndexDirectoryHeader(header, len(entries), IndexDirectoryLeafRecordSize, heapBytes, nextLogicalID); err != nil {
		return nil, err
	}
	for i, entry := range entries {
		if entry.Key.IndexID >= indexHighWater || i != 0 && compareIndexDirectoryKey(entries[i-1].Key, entry.Key) >= 0 ||
			entry.Bits == 0 || entry.Kind != IndexEntryInlineMask ||
			entry.Flags&^indexDirectoryEntryKnownFlags != 0 ||
			entry.Flags&IndexEntryCollision != 0 && entry.Cert.Length == 0 {
			return nil, fmt.Errorf("%w: index leaf order, id, mask, kind, or flags", ErrInvalidWrite)
		}
	}
	heapStart := IndexDirectoryPayloadHeaderSize + len(entries)*IndexDirectoryLeafRecordSize
	payloadLength := heapStart + heapBytes
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageIndexDirectory,
	})
	if err != nil {
		return nil, err
	}
	encodeIndexDirectoryHeader(payload, header, len(entries), uint32(heapStart))
	position, shared := heapStart, 0
	var previous []byte
	for i, entry := range entries {
		certificate, _ := indexDirectoryArenaCertificate(certificates, entry.Cert)
		offset := 0
		if len(certificate) != 0 {
			if i != 0 && bytes.Equal(certificate, previous) {
				offset = shared
			} else {
				offset, shared = position, position
				copy(payload[position:position+len(certificate)], certificate)
				position += len(certificate)
			}
		}
		previous = certificate
		record := payload[IndexDirectoryPayloadHeaderSize+i*IndexDirectoryLeafRecordSize:]
		encodeIndexDirectoryKey(record, entry.Key)
		binary.LittleEndian.PutUint64(record[16:24], entry.Bits)
		binary.LittleEndian.PutUint16(record[24:26], uint16(offset))
		binary.LittleEndian.PutUint16(record[26:28], entry.Cert.Length)
		binary.LittleEndian.PutUint16(record[28:30], entry.Flags)
		record[30] = entry.Kind
		record[31] = 0
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// EncodeIndexDirectoryBranch writes a branch with at most 64 children.
func EncodeIndexDirectoryBranch(dst []byte, header IndexDirectoryHeader, children []IndexDirectoryChild, fileEnd, nextLogicalID uint64) ([]byte, error) {
	if header.Level == 0 || len(children) > 64 {
		return nil, fmt.Errorf("%w: index branch level or fanout", ErrInvalidWrite)
	}
	if err := validateIndexDirectoryHeader(header, len(children), IndexDirectoryBranchRecordSize, 0, nextLogicalID); err != nil {
		return nil, err
	}
	var seen chunkDirectoryRefSet
	for i, child := range children {
		if i != 0 && compareIndexDirectoryKey(children[i-1].Lower, child.Lower) >= 0 {
			return nil, fmt.Errorf("%w: index lower-bound order", ErrInvalidWrite)
		}
		if err := validateIndexDirectoryChild(header, child.Ref, fileEnd, nextLogicalID); err != nil || !seen.add(child.Ref) {
			return nil, fmt.Errorf("%w: index branch child", ErrInvalidWrite)
		}
	}
	payloadLength := IndexDirectoryPayloadHeaderSize + len(children)*IndexDirectoryBranchRecordSize
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageIndexDirectory,
	})
	if err != nil {
		return nil, err
	}
	encodeIndexDirectoryHeader(payload, header, len(children), 0)
	for i, child := range children {
		record := payload[IndexDirectoryPayloadHeaderSize+i*IndexDirectoryBranchRecordSize:]
		encodeIndexDirectoryKey(record, child.Lower)
		encodePageRef(record[16:16+PageRefSize], child.Ref)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

func encodeIndexDirectoryHeader(payload []byte, header IndexDirectoryHeader, count int, heapStart uint32) {
	binary.LittleEndian.PutUint32(payload[0:4], indexDirectoryVersion)
	payload[4] = header.Level
	payload[5] = header.Flags
	binary.LittleEndian.PutUint16(payload[6:8], uint16(count))
	binary.LittleEndian.PutUint32(payload[8:12], heapStart)
}

func encodeIndexDirectoryKey(dst []byte, key IndexDirectoryKey) {
	binary.LittleEndian.PutUint32(dst[0:4], key.IndexID)
	binary.LittleEndian.PutUint32(dst[4:8], key.Chunk)
	binary.LittleEndian.PutUint64(dst[8:16], key.TupleHash)
}

func decodeIndexDirectoryKey(src []byte) IndexDirectoryKey {
	return IndexDirectoryKey{
		IndexID:   binary.LittleEndian.Uint32(src[0:4]),
		TupleHash: binary.LittleEndian.Uint64(src[8:16]),
		Chunk:     binary.LittleEndian.Uint32(src[4:8]),
	}
}

// OpenIndexDirectoryPage validates one tuple-routing leaf or branch. A leaf is
// admitted only in its canonical form: the heap is tiled exactly by the spans
// its records name, in order, with shared regions used precisely where two
// neighbours carry equal certificates. Anything looser would let a writer bug
// hide behind a valid checksum.
func OpenIndexDirectoryPage(src []byte, fileEnd, nextLogicalID uint64, indexHighWater uint32) (IndexDirectoryView, error) {
	if indexHighWater == 0 {
		return IndexDirectoryView{}, fmt.Errorf("%w: index bounds", ErrIndexDirectoryCorrupt)
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return IndexDirectoryView{}, fmt.Errorf("%w: %w", ErrIndexDirectoryCorrupt, err)
	}
	if pageHeader.Kind != PageIndexDirectory || len(payload) < IndexDirectoryPayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != indexDirectoryVersion ||
		!allZero(payload[12:IndexDirectoryPayloadHeaderSize]) {
		return IndexDirectoryView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrIndexDirectoryCorrupt)
	}
	header := IndexDirectoryHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		Level: payload[4], Flags: payload[5],
	}
	count := int(binary.LittleEndian.Uint16(payload[6:8]))
	heapStart := binary.LittleEndian.Uint32(payload[8:12])
	if header.Level != 0 {
		if err := validateIndexDirectoryHeader(header, count, IndexDirectoryBranchRecordSize, 0, nextLogicalID); err != nil ||
			heapStart != 0 || len(payload) != IndexDirectoryPayloadHeaderSize+count*IndexDirectoryBranchRecordSize {
			return IndexDirectoryView{}, fmt.Errorf("%w: branch bounds", ErrIndexDirectoryCorrupt)
		}
		if err := validateIndexDirectoryBranchRecords(header, payload, count, fileEnd, nextLogicalID, indexHighWater); err != nil {
			return IndexDirectoryView{}, err
		}
		return IndexDirectoryView{header: header, payload: payload, count: uint16(count)}, nil
	}
	if uint64(heapStart) != uint64(IndexDirectoryPayloadHeaderSize)+uint64(count)*IndexDirectoryLeafRecordSize ||
		uint64(len(payload)) < uint64(heapStart) {
		return IndexDirectoryView{}, fmt.Errorf("%w: leaf heap start", ErrIndexDirectoryCorrupt)
	}
	if err := validateIndexDirectoryHeader(header, count, IndexDirectoryLeafRecordSize, len(payload)-int(heapStart), nextLogicalID); err != nil {
		return IndexDirectoryView{}, fmt.Errorf("%w: leaf bounds", ErrIndexDirectoryCorrupt)
	}
	if err := validateIndexDirectoryLeafRecords(header, payload, count, int(heapStart), indexHighWater); err != nil {
		return IndexDirectoryView{}, err
	}
	return IndexDirectoryView{header: header, payload: payload, count: uint16(count), heapStart: heapStart}, nil
}

func validateIndexDirectoryLeafRecords(header IndexDirectoryHeader, payload []byte, count, heapStart int, indexHighWater uint32) error {
	maxCertificate := IndexDirectoryMaxCertificate(header.PageSize)
	var previousKey IndexDirectoryKey
	var previous []byte
	expected, shared := heapStart, 0
	for i := range count {
		record := payload[IndexDirectoryPayloadHeaderSize+i*IndexDirectoryLeafRecordSize:]
		key := decodeIndexDirectoryKey(record)
		flags := binary.LittleEndian.Uint16(record[28:30])
		certificate := CertSpan{
			Offset: uint32(binary.LittleEndian.Uint16(record[24:26])),
			Length: binary.LittleEndian.Uint16(record[26:28]),
		}
		if key.IndexID >= indexHighWater || i != 0 && compareIndexDirectoryKey(previousKey, key) >= 0 ||
			binary.LittleEndian.Uint64(record[16:24]) == 0 || record[30] != IndexEntryInlineMask ||
			record[31] != 0 || flags&^indexDirectoryEntryKnownFlags != 0 ||
			flags&IndexEntryCollision != 0 && certificate.Length == 0 {
			return fmt.Errorf("%w: leaf key order, mask, kind, or flags", ErrIndexDirectoryCorrupt)
		}
		previousKey = key
		if certificate.Length == 0 {
			// An absent representative must be the canonical zero span; a
			// dangling offset would otherwise survive every bounds check here
			// and reappear the moment a reader stops consulting Length.
			if certificate.Offset != 0 {
				return fmt.Errorf("%w: empty certificate offset", ErrIndexDirectoryCorrupt)
			}
			previous = nil
			continue
		}
		if int(certificate.Length) > maxCertificate ||
			uint64(certificate.Offset)+uint64(certificate.Length) > uint64(len(payload)) ||
			certificate.Offset < uint32(heapStart) {
			return fmt.Errorf("%w: certificate span bounds", ErrIndexDirectoryCorrupt)
		}
		bytesOf := payload[certificate.Offset : uint64(certificate.Offset)+uint64(certificate.Length)]
		if len(previous) != 0 && int(certificate.Offset) == shared && len(bytesOf) == len(previous) {
			previous = bytesOf
			continue
		}
		if int(certificate.Offset) != expected || bytes.Equal(bytesOf, previous) {
			// Either the heap has a gap or an overlap, or the writer stored a
			// second copy of a representative its predecessor already carries.
			// Both break the one-canonical-encoding rule the split and rewrite
			// paths rely on when they recompute a leaf's byte cost.
			return fmt.Errorf("%w: non-canonical certificate heap", ErrIndexDirectoryCorrupt)
		}
		expected, shared = expected+int(certificate.Length), int(certificate.Offset)
		previous = bytesOf
	}
	if expected != len(payload) {
		return fmt.Errorf("%w: certificate heap does not tile the payload", ErrIndexDirectoryCorrupt)
	}
	return nil
}

func validateIndexDirectoryBranchRecords(header IndexDirectoryHeader, payload []byte, count int, fileEnd, nextLogicalID uint64, indexHighWater uint32) error {
	var previousKey IndexDirectoryKey
	var seen chunkDirectoryRefSet
	for i := range count {
		record := payload[IndexDirectoryPayloadHeaderSize+i*IndexDirectoryBranchRecordSize:]
		key := decodeIndexDirectoryKey(record)
		if key.IndexID >= indexHighWater || i != 0 && compareIndexDirectoryKey(previousKey, key) >= 0 ||
			!pageRefReservedZero(record[16:16+PageRefSize]) {
			return fmt.Errorf("%w: branch key order, id, or reserved bytes", ErrIndexDirectoryCorrupt)
		}
		ref := decodePageRef(record[16 : 16+PageRefSize])
		if err := validateIndexDirectoryChild(header, ref, fileEnd, nextLogicalID); err != nil || !seen.add(ref) {
			return fmt.Errorf("%w: branch child", ErrIndexDirectoryCorrupt)
		}
		previousKey = key
	}
	return nil
}

// Header returns value-only node metadata.
func (v IndexDirectoryView) Header() IndexDirectoryHeader { return v.header }

// Len returns the number of entries or children.
func (v IndexDirectoryView) Len() int { return int(v.count) }

// Certificate returns the representative named by span. The slice borrows the
// leaf payload and is therefore valid only while the page lease is held;
// callers that outlive the lease must copy it first.
func (v IndexDirectoryView) Certificate(span CertSpan) []byte {
	if v.header.Level != 0 || span.Length == 0 ||
		uint64(span.Offset) < uint64(v.heapStart) ||
		uint64(span.Offset)+uint64(span.Length) > uint64(len(v.payload)) {
		return nil
	}
	end := int(span.Offset) + int(span.Length)
	return v.payload[span.Offset:end:end]
}

// Lookup resolves one exact routing key in a leaf. The returned Cert span
// addresses this view's payload, not any caller arena.
func (v IndexDirectoryView) Lookup(key IndexDirectoryKey) (IndexDirectoryEntry, bool) {
	if v.header.Level != 0 {
		return IndexDirectoryEntry{}, false
	}
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		record := v.payload[IndexDirectoryPayloadHeaderSize+middle*IndexDirectoryLeafRecordSize:]
		if compareIndexDirectoryKey(decodeIndexDirectoryKey(record), key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low >= int(v.count) {
		return IndexDirectoryEntry{}, false
	}
	entry, _ := v.EntryAt(low)
	if compareIndexDirectoryKey(entry.Key, key) != 0 {
		return IndexDirectoryEntry{}, false
	}
	return entry, true
}

// EntryAt returns one leaf mapping at rank. The returned Cert span addresses
// this view's payload, not any caller arena.
func (v IndexDirectoryView) EntryAt(rank int) (IndexDirectoryEntry, bool) {
	if v.header.Level != 0 || rank < 0 || rank >= int(v.count) {
		return IndexDirectoryEntry{}, false
	}
	record := v.payload[IndexDirectoryPayloadHeaderSize+rank*IndexDirectoryLeafRecordSize:]
	return IndexDirectoryEntry{
		Key:  decodeIndexDirectoryKey(record),
		Bits: binary.LittleEndian.Uint64(record[16:24]),
		Cert: CertSpan{
			Offset: uint32(binary.LittleEndian.Uint16(record[24:26])),
			Length: binary.LittleEndian.Uint16(record[26:28]),
		},
		Flags: binary.LittleEndian.Uint16(record[28:30]),
		Kind:  record[30],
	}, true
}

// Child selects the branch with the greatest lower bound not exceeding key.
func (v IndexDirectoryView) Child(key IndexDirectoryKey) (PageRef, bool) {
	if v.header.Level == 0 || v.count == 0 {
		return PageRef{}, false
	}
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		record := v.payload[IndexDirectoryPayloadHeaderSize+middle*IndexDirectoryBranchRecordSize:]
		if compareIndexDirectoryKey(decodeIndexDirectoryKey(record), key) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == 0 {
		return PageRef{}, false
	}
	record := v.payload[IndexDirectoryPayloadHeaderSize+(low-1)*IndexDirectoryBranchRecordSize:]
	return decodePageRef(record[16 : 16+PageRefSize]), true
}

// ChildAt returns one branch lower bound and child at rank.
func (v IndexDirectoryView) ChildAt(rank int) (IndexDirectoryChild, bool) {
	if v.header.Level == 0 || rank < 0 || rank >= int(v.count) {
		return IndexDirectoryChild{}, false
	}
	record := v.payload[IndexDirectoryPayloadHeaderSize+rank*IndexDirectoryBranchRecordSize:]
	return IndexDirectoryChild{Lower: decodeIndexDirectoryKey(record), Ref: decodePageRef(record[16 : 16+PageRefSize])}, true
}

func compareIndexDirectoryKey(a, b IndexDirectoryKey) int {
	if a.IndexID < b.IndexID {
		return -1
	}
	if a.IndexID > b.IndexID {
		return 1
	}
	if a.TupleHash < b.TupleHash {
		return -1
	}
	if a.TupleHash > b.TupleHash {
		return 1
	}
	if a.Chunk < b.Chunk {
		return -1
	}
	if a.Chunk > b.Chunk {
		return 1
	}
	return 0
}

func validateIndexDirectoryHeader(header IndexDirectoryHeader, count, recordSize, heapBytes int, nextLogicalID uint64) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) || header.Flags&^indexDirectoryKnownFlags != 0 ||
		count <= 0 || count > int(^uint16(0)) || heapBytes < 0 ||
		recordSize == IndexDirectoryBranchRecordSize && count > 64 {
		return fmt.Errorf("%w: index node identity, count, or flags", ErrInvalidWrite)
	}
	payloadLength := uint64(IndexDirectoryPayloadHeaderSize) + uint64(count)*uint64(recordSize) + uint64(heapBytes)
	if payloadLength > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: index-directory payload does not fit", ErrInvalidWrite)
	}
	return nil
}

func validateIndexDirectoryChild(header IndexDirectoryHeader, ref PageRef, fileEnd, nextLogicalID uint64) error {
	return validateIndexPageRef(header, ref, PageIndexDirectory, fileEnd, nextLogicalID)
}

func validateIndexPageRef(header IndexDirectoryHeader, ref PageRef, kind PageKind, fileEnd, nextLogicalID uint64) error {
	pageSize := uint64(header.PageSize)
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		return err
	}
	if fileEnd < layout.DataStart || fileEnd > maxSuperblockFileOffset || fileEnd%pageSize != 0 ||
		ref.Kind != kind || ref.Flags != 0 || ref.Aux != 0 || ref.Length != header.PageSize ||
		ref.Generation == 0 || ref.Generation > header.Generation ||
		ref.LogicalID <= StateRootLogicalID || ref.LogicalID >= nextLogicalID || ref.LogicalID == header.LogicalID ||
		ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
		ref.Offset > maxSuperblockFileOffset || ref.Offset > fileEnd-pageSize {
		return fmt.Errorf("%w: invalid index page reference", ErrInvalidWrite)
	}
	return nil
}

func indexDirectoryArenaCertificate(certificates []byte, span CertSpan) ([]byte, bool) {
	if span.Length == 0 {
		return nil, span.Offset == 0
	}
	end := uint64(span.Offset) + uint64(span.Length)
	if end > uint64(len(certificates)) {
		return nil, false
	}
	return certificates[span.Offset:end:end], true
}

// indexDirectoryHeapBytes returns the canonical certificate-heap size for
// entries, applying exactly the dedup rule the encoder applies. Sizing and
// encoding must never disagree: the leaf split chooses its cut point from this
// number, and an underestimate would overrun the page while an overestimate
// would leave the heap short of the payload end and fail admission.
func indexDirectoryHeapBytes(entries []IndexDirectoryEntry, certificates []byte, maxCertificate int) (int, error) {
	heapBytes := 0
	var previous []byte
	for i := range entries {
		certificate, ok := indexDirectoryArenaCertificate(certificates, entries[i].Cert)
		if !ok || len(certificate) > maxCertificate {
			return 0, fmt.Errorf("%w: index certificate span or length", ErrInvalidWrite)
		}
		if len(certificate) != 0 && (i == 0 || !bytes.Equal(certificate, previous)) {
			heapBytes += len(certificate)
		}
		previous = certificate
	}
	return heapBytes, nil
}

// IndexDirectoryLeafPrefix returns the longest non-empty prefix of entries
// whose records and deduplicated certificate heap fit one leaf page. Packers
// partition a sorted routing sequence with it instead of dividing by a fixed
// record capacity, which stopped being correct once the heap made a leaf's
// cost depend on its contents rather than only on its entry count.
func IndexDirectoryLeafPrefix(entries []IndexDirectoryEntry, certificates []byte, pageSize uint32) (int, error) {
	budget := indexDirectoryLeafBudget(pageSize)
	maxCertificate := IndexDirectoryMaxCertificate(pageSize)
	used, count := 0, 0
	var previous []byte
	for i := range entries {
		certificate, ok := indexDirectoryArenaCertificate(certificates, entries[i].Cert)
		if !ok || len(certificate) > maxCertificate {
			return 0, fmt.Errorf("%w: index certificate span or length", ErrInvalidWrite)
		}
		cost := IndexDirectoryLeafRecordSize
		if len(certificate) != 0 && (i == 0 || !bytes.Equal(certificate, previous)) {
			cost += len(certificate)
		}
		if used+cost > budget {
			break
		}
		used, count, previous = used+cost, i+1, certificate
	}
	if count == 0 {
		return 0, fmt.Errorf("%w: index leaf entry does not fit", ErrInvalidWrite)
	}
	return count, nil
}

func indexDirectoryLeafBudget(pageSize uint32) int {
	return int(pageSize) - PageHeaderSize - PageTrailerSize - IndexDirectoryPayloadHeaderSize
}

// indexDirectoryLeafBytes returns the records-plus-heap cost of entries, the
// quantity the page budget bounds.
func indexDirectoryLeafBytes(entries []IndexDirectoryEntry, certificates []byte, maxCertificate int) (int, error) {
	heapBytes, err := indexDirectoryHeapBytes(entries, certificates, maxCertificate)
	if err != nil {
		return 0, err
	}
	return len(entries)*IndexDirectoryLeafRecordSize + heapBytes, nil
}
