package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"unsafe"
)

// The recovery journal is a bounded redo log kept in a SEPARATE preallocated
// file beside the store. It exists only to make a DurabilitySync in-place value
// replacement durable with one bounded append plus one sync, instead of the
// full copy-on-write chain and two ordered barriers a per-mutation root
// publication pays. Readers never consult it: visibility comes from the
// canonical frames exactly as before, and the journal is write-only until crash
// recovery replays it.
//
// It is a separate file, never an in-store region, because fdatasync flushes
// every dirty page of the file it is called on; a journal region inside the
// store file would drag concurrently pre-written checkpoint pages into every
// acknowledgement sync and make the acknowledgement latency unpredictable. A
// dedicated file keeps the sync domain to the journal's own preallocated pages,
// which is the same reason every production WAL is a separate file.
//
// Lifetime: a checkpoint materializes the dirty frames, publishes the alternate
// root, then recycles the journal head past the checkpointed generation in the
// same publication. Steady state without crashes never reads a record back.
//
// Durability protocol per record:
//
//  1. The mutation is applied to the canonical in-memory frames exactly as
//     buffered-visible mode does (an in-place same-size patch). Only that
//     eligibility class uses the journal; a ref-changing write falls back to
//     the ordinary full-chain sync path.
//  2. One redo record — sequence, generation, key, value, CRC — is appended at
//     the write cursor. The record is padded to the durable damage granule so a
//     torn append can only damage its own tail, never the previous record's
//     already-synced bytes.
//  3. The mode's sync primitive runs on the journal file alone
//     (F_FULLFSYNC-class for the power-safe lane, fdatasync-class for the
//     ordinary-filesystem lane). Acknowledgement returns after that single sync.
//
// Recovery selects the newest valid store root, opens the paired journal, and
// replays every record whose generation is newer than that root's generation
// through the ordinary mutation path, stopping at the first framing-invalid
// record. A torn or reordered tail therefore truncates and never corrupts: the
// root and its graph were never touched before their own two-phase publication.
//
// A batch record (recoveryRecordKindBatch) is the group-commit form of the same
// protocol: one sequence, one generation, and an ordered list of put/delete
// entries, all under a single CRC and made durable by one append plus one sync.
// Because the whole record is framed by that one CRC, a torn append fails
// validation and replay truncates before it, so recovery replays either every
// entry of the group or none — the atomicity a multi-document Update needs, and
// the primitive a future group of independent acknowledgements sharing one sync
// builds on.
const (
	// RecoveryJournalHeaderSize is one full damage-granule-aligned header
	// sector. The header is rewritten only on create and on recycle, never per
	// record, so an acknowledgement sync never commits journal metadata.
	RecoveryJournalHeaderSize = 512
	// recoveryJournalHeaderSlots is the number of alternating header sectors.
	// Recycle rewrites the slot opposite the live one and syncs; a torn recycle
	// therefore leaves the previous header intact, and recovery falls back to it
	// and re-applies the records it still describes idempotently. This mirrors
	// the store's double-root publication for exactly the same reason: a single
	// in-place metadata sector cannot be rewritten crash-atomically.
	recoveryJournalHeaderSlots = 2
	// recoveryJournalRegionStart is the byte offset of the record region, past
	// both header slots.
	recoveryJournalRegionStart = RecoveryJournalHeaderSize * recoveryJournalHeaderSlots
	// RecoveryJournalRecordPrefixSize is the fixed record framing that precedes
	// the variable key and value bytes.
	RecoveryJournalRecordPrefixSize = 32
	// RecoveryJournalRecordTrailerSize is the CRC and its complement.
	RecoveryJournalRecordTrailerSize = 8
	// RecoveryJournalMinSectorSize is the smallest supported append/damage
	// granule. Record starts are aligned to the sector so a torn multi-sector
	// append cannot rewrite an already-synced earlier record.
	RecoveryJournalMinSectorSize = 512
	// RecoveryJournalMaxCapacityBytes is the authoritative upper bound for the
	// preallocated record region and the buffer read by recovery. Keeping it in
	// the format package makes a checksummed hostile header unable to request an
	// unbounded allocation and keeps durable's creation clamp from drifting.
	RecoveryJournalMaxCapacityBytes = uint64(16) << 20

	recoveryJournalMagic   = "RJRNL01\x00"
	recoveryJournalVersion = DevelopmentFormatVersion
	recoveryRecordMagic    = uint32(0x314a5252) // "RRJ1", little-endian.

	// RecordKindPut marks a same-size inline value replacement.
	recoveryRecordKindPut = uint16(1)
	// recoveryRecordKindDelete marks a key removal.
	recoveryRecordKindDelete = uint16(2)
	// recoveryRecordKindBatch marks a group-commit record: a single sequence and
	// generation covering an ordered list of put/delete entries, framed by one
	// CRC. It is the group-commit primitive — the whole record is durable after
	// one sync, and a torn append fails framing so recovery replays either every
	// entry or none. A multi-document Update rides it; a future group of
	// independent acknowledgements sharing one sync is the same record with
	// unrelated entries.
	recoveryRecordKindBatch = uint16(3)

	// RecoveryRecordKindPut, RecoveryRecordKindDelete, and RecoveryRecordKindBatch
	// are the exported record kinds a store passes to Append/AppendBatch and
	// matches on during Replay. The internal values stay unexported so the encoder
	// validates them, but a caller in another package needs a name for the redo
	// verb it is journaling.
	RecoveryRecordKindPut    = recoveryRecordKindPut
	RecoveryRecordKindDelete = recoveryRecordKindDelete
	RecoveryRecordKindBatch  = recoveryRecordKindBatch

	// RecoveryBatchEntryHeaderSize is the fixed per-entry framing inside a batch
	// record that precedes the entry's variable key and value bytes:
	// kind (2) + reserved (2) + keyLen (4) + valueLen (4).
	RecoveryBatchEntryHeaderSize = 12
)

// RecoveryRecordPaddedSize returns the on-disk byte cost of one record whose key
// and value have the given lengths, padded to the sector granule. A store sizes
// its preallocated journal capacity from this so a chosen record budget maps to
// an exact byte reservation. Inputs that cannot be represented by the wire
// format or the current architecture saturate at the largest int, so callers
// that use the result as an upper bound fail closed instead of under-reserving.
func RecoveryRecordPaddedSize(sectorSize uint32, keyLen, valueLen int) int {
	return recoveryRecordPadded(sectorSize, keyLen, valueLen)
}

var (
	// ErrRecoveryJournalCorrupt reports a header whose framing, checksum, or
	// identity is invalid. A corrupt header fails closed: the paired store
	// cannot prove which records belong to it.
	ErrRecoveryJournalCorrupt = errors.New("vibejson: corrupt recovery journal header")
	// ErrRecoveryJournalIdentity reports a header that framed correctly but does
	// not pair with the selected store root (StoreID or JournalID mismatch). A
	// referenced-but-mismatched journal must never be replayed onto the store.
	ErrRecoveryJournalIdentity = errors.New("vibejson: recovery journal identity mismatch")

	// ErrRecoveryJournalGeometry reports a paired journal whose page geometry
	// does not match the store that names it — the bundle halves were built
	// for different stores even though the identities collide.
	ErrRecoveryJournalGeometry = errors.New("vibejson: recovery journal geometry mismatch")

	// ErrRecoveryJournalEpoch reports a journal whose base generation is ahead
	// of the root that selected it: the store file is older than the journal
	// (a mixed-epoch bundle, typically a restored store beside a live
	// journal), so acknowledgements recycled during the gap are gone and the
	// pair must fail closed rather than open with silent loss.
	ErrRecoveryJournalEpoch = errors.New("vibejson: recovery journal is ahead of the store root")
	// ErrRecoveryJournalMissing reports a store root that references a journal
	// (non-zero JournalID) whose file is absent. This fails closed: the store
	// may have acknowledged mutations that only the missing journal records.
	ErrRecoveryJournalMissing = errors.New("vibejson: referenced recovery journal is missing")
	// ErrRecoveryJournalFull reports that the next record does not fit the
	// preallocated capacity. The caller must force a checkpoint, which recycles
	// the journal head, exactly as staging pressure forces one today.
	ErrRecoveryJournalFull = errors.New("vibejson: recovery journal is full")
	// ErrRecoveryJournalRecord reports a record that failed framing, checksum,
	// or monotonic-sequence validation. Replay stops at the first such record;
	// it is the truncation point of a torn or reordered tail, not corruption.
	ErrRecoveryJournalRecord = errors.New("vibejson: invalid recovery journal record")
)

// RecoveryJournalHeader is the pointer-free identity and geometry of one
// journal file. BaseGeneration is the store root generation the current live
// region builds upon: recovery replays only records strictly newer than it.
// BaseSequence anchors monotonic-sequence validation so stale bytes left in the
// preallocated region after a recycle can never be mistaken for live records —
// the first live record must carry exactly BaseSequence+1.
type RecoveryJournalHeader struct {
	StoreID        [16]byte
	JournalID      [16]byte
	PageSize       uint32
	SectorSize     uint32
	BaseGeneration uint64
	BaseSequence   uint64
	Capacity       uint64
	// RecycleCount is strictly monotonic across recycles. Recovery selects the
	// valid header slot with the highest count, so a torn recycle to one slot
	// leaves the previous slot as the recovery point.
	RecycleCount uint64
}

// RecoveryRecord is one decoded redo record. Key and Value borrow the decode
// buffer and must be copied if retained past the next decode.
//
// For a batch record (Kind == RecoveryRecordKindBatch) the logical mutations are
// carried in Entries instead of Key/Value; Sequence and Generation are the
// group's single sequence and generation.
type RecoveryRecord struct {
	Sequence   uint64
	Generation uint64
	Kind       uint16
	Key        []byte
	Value      []byte
	Entries    []RecoveryBatchEntry
}

// RecoveryBatchEntry is one put or delete inside a batch record. Key and Value
// borrow the decode buffer, exactly like RecoveryRecord's own fields.
type RecoveryBatchEntry struct {
	Kind  uint16
	Key   []byte
	Value []byte
}

// recoveryRecordPadded returns the sector-padded on-disk size of a record whose
// key and value have the given lengths.
func recoveryRecordPadded(sectorSize uint32, keyLen, valueLen int) int {
	padded, ok := checkedRecoveryRecordPadded(
		sectorSize, keyLen, valueLen,
	)
	if !ok {
		return maxIntValue
	}
	return padded
}

func checkedRecoveryPadRaw(sectorSize uint32, raw uint64) (int, bool) {
	if sectorSize == 0 {
		return 0, false
	}
	sector := uint64(sectorSize)
	if remainder := raw % sector; remainder != 0 {
		var ok bool
		raw, ok = checkedSizeAdd(
			raw, sector-remainder, uint64(maxIntValue),
		)
		if !ok {
			return 0, false
		}
	}
	return checkedSizeInt(raw, ^uint64(0))
}

func checkedRecoveryRecordPadded(
	sectorSize uint32, keyLen, valueLen int,
) (int, bool) {
	if keyLen < 0 || valueLen < 0 {
		return 0, false
	}
	wireLimit := uint64(^uint32(0))
	keyBytes, valueBytes := uint64(keyLen), uint64(valueLen)
	if keyBytes > wireLimit || valueBytes > wireLimit {
		return 0, false
	}
	raw := uint64(
		RecoveryJournalRecordPrefixSize +
			RecoveryJournalRecordTrailerSize,
	)
	var ok bool
	raw, ok = checkedSizeAdd(raw, keyBytes, uint64(maxIntValue))
	if !ok {
		return 0, false
	}
	raw, ok = checkedSizeAdd(raw, valueBytes, uint64(maxIntValue))
	if !ok {
		return 0, false
	}
	return checkedRecoveryPadRaw(sectorSize, raw)
}

func checkedRecoveryBatchBodyAdd(
	body, keyLen, valueLen uint64,
) (uint64, bool) {
	wireLimit := uint64(^uint32(0))
	if keyLen == 0 || keyLen > wireLimit || valueLen > wireLimit {
		return 0, false
	}
	next, ok := checkedSizeAdd(
		body, RecoveryBatchEntryHeaderSize, wireLimit,
	)
	if !ok {
		return 0, false
	}
	next, ok = checkedSizeAdd(next, keyLen, wireLimit)
	if !ok {
		return 0, false
	}
	return checkedSizeAdd(next, valueLen, wireLimit)
}

// checkedRecoveryBatchBodyLen sums the framed length of every entry in a batch
// record without allowing the uint32 body field or native int to wrap.
func checkedRecoveryBatchBodyLen(
	entries []RecoveryBatchEntry,
) (uint64, bool) {
	if len(entries) == 0 || uint64(len(entries)) > uint64(^uint32(0)) {
		return 0, false
	}
	var body uint64
	for i := range entries {
		var ok bool
		body, ok = checkedRecoveryBatchBodyAdd(
			body,
			uint64(len(entries[i].Key)),
			uint64(len(entries[i].Value)),
		)
		if !ok {
			return 0, false
		}
	}
	return body, true
}

func checkedRecoveryBatchRecordPaddedSize(
	sectorSize uint32, entries []RecoveryBatchEntry,
) (int, bool) {
	body, ok := checkedRecoveryBatchBodyLen(entries)
	if !ok {
		return 0, false
	}
	raw := uint64(
		RecoveryJournalRecordPrefixSize +
			RecoveryJournalRecordTrailerSize,
	)
	raw, ok = checkedSizeAdd(raw, body, uint64(maxIntValue))
	if !ok {
		return 0, false
	}
	return checkedRecoveryPadRaw(sectorSize, raw)
}

func recoveryBatchEntryArenaFits(entryCount uint64) bool {
	_, ok := checkedSizeMul(
		entryCount,
		uint64(unsafe.Sizeof(RecoveryBatchEntry{})),
		uint64(maxIntValue),
	)
	return ok
}

// RecoveryBatchRecordPaddedSize returns the on-disk byte cost of one batch
// record carrying entries, padded to the sector granule. A store sizes a batch's
// journal reservation from this so one Update maps to one bounded append.
// Unrepresentable inputs saturate at the largest int, matching
// RecoveryRecordPaddedSize's fail-closed sizing contract.
func RecoveryBatchRecordPaddedSize(sectorSize uint32, entries []RecoveryBatchEntry) int {
	padded, ok := checkedRecoveryBatchRecordPaddedSize(sectorSize, entries)
	if !ok {
		return maxIntValue
	}
	return padded
}

// validateRecoveryJournalHeader enforces the geometry invariants every header
// must satisfy regardless of provenance.
func validateRecoveryJournalHeader(h RecoveryJournalHeader) error {
	if h.StoreID == ([16]byte{}) || h.JournalID == ([16]byte{}) {
		return fmt.Errorf("%w: zero identity", ErrRecoveryJournalCorrupt)
	}
	if err := validateRecoveryJournalGeometry(h); err != nil {
		return err
	}
	if h.BaseGeneration == 0 {
		return fmt.Errorf("%w: base generation", ErrRecoveryJournalCorrupt)
	}
	if h.Capacity == 0 ||
		h.Capacity > RecoveryJournalMaxCapacityBytes ||
		h.Capacity%uint64(h.SectorSize) != 0 {
		return fmt.Errorf("%w: capacity", ErrRecoveryJournalCorrupt)
	}
	if h.RecycleCount == 0 {
		return fmt.Errorf("%w: recycle count", ErrRecoveryJournalCorrupt)
	}
	return nil
}

func validateRecoveryJournalGeometry(h RecoveryJournalHeader) error {
	if !validPhysicalPageSize(h.PageSize) {
		return fmt.Errorf("%w: page size", ErrRecoveryJournalCorrupt)
	}
	if h.SectorSize < RecoveryJournalMinSectorSize ||
		h.SectorSize&(h.SectorSize-1) != 0 ||
		h.SectorSize > h.PageSize || h.PageSize%h.SectorSize != 0 {
		return fmt.Errorf("%w: sector size", ErrRecoveryJournalCorrupt)
	}
	return nil
}

// EncodeRecoveryJournalHeader writes one sealed header sector into dst.
func EncodeRecoveryJournalHeader(dst []byte, h RecoveryJournalHeader) ([]byte, error) {
	if len(dst) < RecoveryJournalHeaderSize {
		return nil, fmt.Errorf("%w: header buffer has %d bytes", ErrInvalidWrite, len(dst))
	}
	if err := validateRecoveryJournalHeader(h); err != nil {
		return nil, err
	}
	sector := dst[:RecoveryJournalHeaderSize]
	clear(sector)
	copy(sector[0:8], recoveryJournalMagic)
	binary.LittleEndian.PutUint32(sector[8:12], recoveryJournalVersion)
	binary.LittleEndian.PutUint32(sector[12:16], RecoveryJournalHeaderSize)
	copy(sector[16:32], h.StoreID[:])
	copy(sector[32:48], h.JournalID[:])
	binary.LittleEndian.PutUint32(sector[48:52], h.PageSize)
	binary.LittleEndian.PutUint32(sector[52:56], h.SectorSize)
	binary.LittleEndian.PutUint64(sector[56:64], h.BaseGeneration)
	binary.LittleEndian.PutUint64(sector[64:72], h.BaseSequence)
	binary.LittleEndian.PutUint64(sector[72:80], h.Capacity)
	binary.LittleEndian.PutUint64(sector[80:88], h.RecycleCount)
	checksum := PageChecksum(sector[:RecoveryJournalHeaderSize-8])
	binary.LittleEndian.PutUint32(sector[RecoveryJournalHeaderSize-8:RecoveryJournalHeaderSize-4], checksum)
	binary.LittleEndian.PutUint32(sector[RecoveryJournalHeaderSize-4:], ^checksum)
	return sector, nil
}

// DecodeRecoveryJournalHeader validates one header sector and returns its
// value-only identity and geometry. A cleanly-truncated (all-zero) header
// sector reports ErrRecoveryJournalCorrupt with the zero-magic path so callers
// can distinguish an uninitialized file from a live one.
func DecodeRecoveryJournalHeader(src []byte) (RecoveryJournalHeader, error) {
	if len(src) < RecoveryJournalHeaderSize {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: short header", ErrRecoveryJournalCorrupt)
	}
	src = src[:RecoveryJournalHeaderSize]
	if string(src[0:8]) != recoveryJournalMagic {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: magic", ErrRecoveryJournalCorrupt)
	}
	checksum := binary.LittleEndian.Uint32(src[RecoveryJournalHeaderSize-8 : RecoveryJournalHeaderSize-4])
	if binary.LittleEndian.Uint32(src[RecoveryJournalHeaderSize-4:]) != ^checksum ||
		PageChecksum(src[:RecoveryJournalHeaderSize-8]) != checksum {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: checksum", ErrRecoveryJournalCorrupt)
	}
	if binary.LittleEndian.Uint32(src[8:12]) != recoveryJournalVersion ||
		binary.LittleEndian.Uint32(src[12:16]) != RecoveryJournalHeaderSize {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: version or header size", ErrRecoveryJournalCorrupt)
	}
	var h RecoveryJournalHeader
	copy(h.StoreID[:], src[16:32])
	copy(h.JournalID[:], src[32:48])
	h.PageSize = binary.LittleEndian.Uint32(src[48:52])
	h.SectorSize = binary.LittleEndian.Uint32(src[52:56])
	h.BaseGeneration = binary.LittleEndian.Uint64(src[56:64])
	h.BaseSequence = binary.LittleEndian.Uint64(src[64:72])
	h.Capacity = binary.LittleEndian.Uint64(src[72:80])
	h.RecycleCount = binary.LittleEndian.Uint64(src[80:88])
	if err := validateRecoveryJournalHeader(h); err != nil {
		return RecoveryJournalHeader{}, err
	}
	return h, nil
}

// EncodeRecoveryRecord writes one sector-padded record into dst and returns the
// exact padded length. dst must be at least the padded length. The record CRC
// covers the fixed prefix, key, and value; the complement mirrors it so a
// single flipped bit in either fails validation.
func EncodeRecoveryRecord(
	dst []byte, sectorSize uint32, rec RecoveryRecord,
) (int, error) {
	if rec.Sequence == 0 || rec.Generation == 0 {
		return 0, fmt.Errorf("%w: zero sequence or generation", ErrInvalidWrite)
	}
	if rec.Kind != recoveryRecordKindPut && rec.Kind != recoveryRecordKindDelete {
		return 0, fmt.Errorf("%w: record kind", ErrInvalidWrite)
	}
	if len(rec.Key) == 0 {
		return 0, fmt.Errorf("%w: record key or value length", ErrInvalidWrite)
	}
	padded, ok := checkedRecoveryRecordPadded(
		sectorSize, len(rec.Key), len(rec.Value),
	)
	if !ok {
		return 0, fmt.Errorf("%w: record key or value length", ErrInvalidWrite)
	}
	if len(dst) < padded {
		return 0, fmt.Errorf("%w: record buffer has %d bytes, need %d", ErrInvalidWrite, len(dst), padded)
	}
	buf := dst[:padded]
	clear(buf)
	binary.LittleEndian.PutUint32(buf[0:4], recoveryRecordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], rec.Kind)
	// buf[6:8] reserved zero.
	binary.LittleEndian.PutUint64(buf[8:16], rec.Sequence)
	binary.LittleEndian.PutUint64(buf[16:24], rec.Generation)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(rec.Key)))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(len(rec.Value)))
	cursor := RecoveryJournalRecordPrefixSize
	cursor += copy(buf[cursor:], rec.Key)
	cursor += copy(buf[cursor:], rec.Value)
	checksum := PageChecksum(buf[:cursor])
	binary.LittleEndian.PutUint32(buf[cursor:cursor+4], checksum)
	binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], ^checksum)
	return padded, nil
}

// EncodeRecoveryBatchRecord writes one sector-padded batch record into dst and
// returns the exact padded length. The record carries one sequence and
// generation over rec.Entries; a single CRC (and its complement) covers the
// prefix and every framed entry, so a torn append fails validation and recovery
// replays either all entries or none. dst must be at least the padded length.
func EncodeRecoveryBatchRecord(
	dst []byte, sectorSize uint32, rec RecoveryRecord,
) (int, error) {
	if rec.Sequence == 0 || rec.Generation == 0 {
		return 0, fmt.Errorf("%w: zero sequence or generation", ErrInvalidWrite)
	}
	if len(rec.Entries) == 0 ||
		uint64(len(rec.Entries)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("%w: batch entry count", ErrInvalidWrite)
	}
	body, ok := checkedRecoveryBatchBodyLen(rec.Entries)
	if !ok {
		return 0, fmt.Errorf("%w: batch body length", ErrInvalidWrite)
	}
	padded, ok := checkedRecoveryBatchRecordPaddedSize(
		sectorSize, rec.Entries,
	)
	if !ok {
		return 0, fmt.Errorf("%w: batch record length", ErrInvalidWrite)
	}
	if len(dst) < padded {
		return 0, fmt.Errorf("%w: batch record buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), padded)
	}
	buf := dst[:padded]
	clear(buf)
	binary.LittleEndian.PutUint32(buf[0:4], recoveryRecordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], recoveryRecordKindBatch)
	// buf[6:8] reserved zero.
	binary.LittleEndian.PutUint64(buf[8:16], rec.Sequence)
	binary.LittleEndian.PutUint64(buf[16:24], rec.Generation)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(rec.Entries)))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(body))
	cursor := RecoveryJournalRecordPrefixSize
	for i := range rec.Entries {
		entry := rec.Entries[i]
		if entry.Kind != recoveryRecordKindPut &&
			entry.Kind != recoveryRecordKindDelete {
			return 0, fmt.Errorf("%w: batch entry kind", ErrInvalidWrite)
		}
		if len(entry.Key) == 0 ||
			uint64(len(entry.Key)) > uint64(^uint32(0)) ||
			uint64(len(entry.Value)) > uint64(^uint32(0)) {
			return 0, fmt.Errorf("%w: batch entry key or value length", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint16(buf[cursor:cursor+2], entry.Kind)
		// buf[cursor+2:cursor+4] reserved zero.
		binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], uint32(len(entry.Key)))
		binary.LittleEndian.PutUint32(buf[cursor+8:cursor+12], uint32(len(entry.Value)))
		cursor += RecoveryBatchEntryHeaderSize
		cursor += copy(buf[cursor:], entry.Key)
		cursor += copy(buf[cursor:], entry.Value)
	}
	checksum := PageChecksum(buf[:cursor])
	binary.LittleEndian.PutUint32(buf[cursor:cursor+4], checksum)
	binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], ^checksum)
	return padded, nil
}

// DecodeRecoveryRecord validates one record at the start of src against the
// expected sequence and returns the borrowed record and its padded size. A
// record whose magic, framing, checksum, or sequence is wrong returns
// ErrRecoveryJournalRecord: it is the truncation point of a torn or reordered
// tail, not corruption of the store.
func DecodeRecoveryRecord(
	src []byte, sectorSize uint32, expectedSequence uint64,
) (RecoveryRecord, int, error) {
	if len(src) < RecoveryJournalRecordPrefixSize+RecoveryJournalRecordTrailerSize {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: short record", ErrRecoveryJournalRecord)
	}
	if binary.LittleEndian.Uint32(src[0:4]) != recoveryRecordMagic {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: magic", ErrRecoveryJournalRecord)
	}
	kind := binary.LittleEndian.Uint16(src[4:6])
	if binary.LittleEndian.Uint16(src[6:8]) != 0 {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: reserved", ErrRecoveryJournalRecord)
	}
	if kind == recoveryRecordKindBatch {
		return decodeRecoveryBatchRecord(src, sectorSize, expectedSequence)
	}
	if kind != recoveryRecordKindPut && kind != recoveryRecordKindDelete {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: kind", ErrRecoveryJournalRecord)
	}
	sequence := binary.LittleEndian.Uint64(src[8:16])
	generation := binary.LittleEndian.Uint64(src[16:24])
	keyLen := binary.LittleEndian.Uint32(src[24:28])
	valueLen := binary.LittleEndian.Uint32(src[28:32])
	if sequence != expectedSequence || generation == 0 || keyLen == 0 {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: sequence or framing", ErrRecoveryJournalRecord)
	}
	keyStart := uint64(RecoveryJournalRecordPrefixSize)
	valueStart := keyStart + uint64(keyLen)
	bodyEnd := valueStart + uint64(valueLen)
	if bodyEnd+RecoveryJournalRecordTrailerSize > uint64(len(src)) {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: length overruns region", ErrRecoveryJournalRecord)
	}
	checksum := PageChecksum(src[:bodyEnd])
	stored := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	if stored != checksum ||
		binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) != ^checksum {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: checksum", ErrRecoveryJournalRecord)
	}
	padded, ok := checkedRecoveryPadRaw(
		sectorSize, bodyEnd+RecoveryJournalRecordTrailerSize,
	)
	if !ok {
		return RecoveryRecord{}, 0, fmt.Errorf(
			"%w: padded record length", ErrRecoveryJournalRecord,
		)
	}
	rec := RecoveryRecord{
		Sequence:   sequence,
		Generation: generation,
		Kind:       kind,
		Key:        src[keyStart:valueStart],
		Value:      src[valueStart:bodyEnd],
	}
	return rec, padded, nil
}

// decodeRecoveryBatchRecord validates one batch record at the start of src. The
// single CRC over the prefix and every framed entry is what gives the batch its
// all-or-nothing recovery: a torn append damages the record's own tail, the CRC
// fails, and replay truncates before it — so no entry of a half-appended batch
// is ever replayed, and every entry of a fully-synced one is.
func decodeRecoveryBatchRecord(
	src []byte, sectorSize uint32, expectedSequence uint64,
) (RecoveryRecord, int, error) {
	sequence := binary.LittleEndian.Uint64(src[8:16])
	generation := binary.LittleEndian.Uint64(src[16:24])
	entryCount := binary.LittleEndian.Uint32(src[24:28])
	bodyLen := binary.LittleEndian.Uint32(src[28:32])
	if sequence != expectedSequence || generation == 0 || entryCount == 0 {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: sequence or framing", ErrRecoveryJournalRecord)
	}
	bodyEnd := uint64(RecoveryJournalRecordPrefixSize) + uint64(bodyLen)
	if bodyEnd+RecoveryJournalRecordTrailerSize > uint64(len(src)) {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: length overruns region", ErrRecoveryJournalRecord)
	}
	// Every entry has a fixed header and a non-empty key. Prove the peer-chosen
	// count fits both the framed body and native slice capacity before make;
	// otherwise a tiny checksummed body could request a multi-gigabyte arena.
	if uint64(entryCount) >
		uint64(bodyLen)/(RecoveryBatchEntryHeaderSize+1) ||
		uint64(entryCount) > uint64(maxIntValue) {
		return RecoveryRecord{}, 0, fmt.Errorf(
			"%w: batch entry count overruns body",
			ErrRecoveryJournalRecord,
		)
	}
	if !recoveryBatchEntryArenaFits(uint64(entryCount)) {
		return RecoveryRecord{}, 0, fmt.Errorf(
			"%w: batch entry arena overflows address space",
			ErrRecoveryJournalRecord,
		)
	}
	checksum := PageChecksum(src[:bodyEnd])
	stored := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	if stored != checksum ||
		binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) != ^checksum {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: checksum", ErrRecoveryJournalRecord)
	}
	entries := make([]RecoveryBatchEntry, 0, entryCount)
	cursor := uint64(RecoveryJournalRecordPrefixSize)
	for i := uint32(0); i < entryCount; i++ {
		if cursor+RecoveryBatchEntryHeaderSize > bodyEnd {
			return RecoveryRecord{}, 0, fmt.Errorf("%w: batch entry header overruns", ErrRecoveryJournalRecord)
		}
		entryKind := binary.LittleEndian.Uint16(src[cursor : cursor+2])
		if binary.LittleEndian.Uint16(src[cursor+2:cursor+4]) != 0 ||
			(entryKind != recoveryRecordKindPut && entryKind != recoveryRecordKindDelete) {
			return RecoveryRecord{}, 0, fmt.Errorf("%w: batch entry kind or reserved", ErrRecoveryJournalRecord)
		}
		keyLen := uint64(binary.LittleEndian.Uint32(src[cursor+4 : cursor+8]))
		valueLen := uint64(binary.LittleEndian.Uint32(src[cursor+8 : cursor+12]))
		if keyLen == 0 {
			return RecoveryRecord{}, 0, fmt.Errorf("%w: batch entry key length", ErrRecoveryJournalRecord)
		}
		keyStart := cursor + RecoveryBatchEntryHeaderSize
		valueStart := keyStart + keyLen
		entryEnd := valueStart + valueLen
		if entryEnd > bodyEnd {
			return RecoveryRecord{}, 0, fmt.Errorf("%w: batch entry overruns body", ErrRecoveryJournalRecord)
		}
		entries = append(entries, RecoveryBatchEntry{
			Kind:  entryKind,
			Key:   src[keyStart:valueStart],
			Value: src[valueStart:entryEnd],
		})
		cursor = entryEnd
	}
	if cursor != bodyEnd {
		return RecoveryRecord{}, 0, fmt.Errorf("%w: batch body length mismatch", ErrRecoveryJournalRecord)
	}
	padded, ok := checkedRecoveryPadRaw(
		sectorSize, bodyEnd+RecoveryJournalRecordTrailerSize,
	)
	if !ok {
		return RecoveryRecord{}, 0, fmt.Errorf(
			"%w: padded batch length", ErrRecoveryJournalRecord,
		)
	}
	rec := RecoveryRecord{
		Sequence:   sequence,
		Generation: generation,
		Kind:       recoveryRecordKindBatch,
		Entries:    entries,
	}
	return rec, padded, nil
}

// RecoveryJournal is the single-writer file-backed manager. It is owned by the
// collection's serialized writer and is not safe for concurrent use, exactly
// like the durability device it complements.
type RecoveryJournal struct {
	file   *os.File
	header RecoveryJournalHeader
	// cursor is the in-memory byte offset within the record region (0-based
	// from the region start) where the next record will be appended. It is
	// derived by scanning on Open and reset to zero on Recycle; it is never
	// persisted, so an acknowledgement never writes header metadata.
	cursor uint64
	// nextSequence is the sequence the next appended record will carry.
	nextSequence uint64
	// headerSlot is the alternating slot the live header occupies. Recycle
	// writes the opposite slot and flips this after its sync succeeds.
	headerSlot uint32
	scratch    []byte
	// journalSync and journalDataSync are injected so a fault seam can wrap the
	// real barriers; production wires the platform sync helpers.
	journalSync     func(*os.File) error
	journalDataSync func(*os.File) error
	// writeAt is injected so a fault seam can intercept the append write.
	writeAt func(p []byte, off int64) (int, error)
}

// CreateRecoveryJournal preallocates a fresh journal file, writes its header,
// and syncs both. capacity is the record-region byte budget; it is rounded up
// to a whole sector. The file is preallocated to header+capacity so no later
// append ever extends it and thus never commits filesystem metadata under an
// acknowledgement sync.
func CreateRecoveryJournal(
	file *os.File, h RecoveryJournalHeader,
) (*RecoveryJournal, error) {
	if err := validateRecoveryJournalGeometry(h); err != nil {
		return nil, err
	}
	if h.Capacity == 0 || h.Capacity > RecoveryJournalMaxCapacityBytes {
		return nil, fmt.Errorf("%w: capacity", ErrRecoveryJournalCorrupt)
	}
	sector := uint64(h.SectorSize)
	if remainder := h.Capacity % sector; remainder != 0 {
		rounded, ok := checkedSizeAdd(
			h.Capacity,
			sector-remainder,
			RecoveryJournalMaxCapacityBytes,
		)
		if !ok {
			return nil, fmt.Errorf("%w: capacity", ErrRecoveryJournalCorrupt)
		}
		h.Capacity = rounded
	}
	h.RecycleCount = 1
	if err := validateRecoveryJournalHeader(h); err != nil {
		return nil, err
	}
	total := int64(recoveryJournalRegionStart) + int64(h.Capacity)
	if err := recoveryJournalPreallocate(file, total); err != nil {
		return nil, err
	}
	rj := newRecoveryJournalManager(file, h)
	rj.headerSlot = 0
	if err := rj.writeHeader(0, rj.header); err != nil {
		return nil, err
	}
	if err := rj.journalDataSync(file); err != nil {
		return nil, err
	}
	rj.cursor = 0
	rj.nextSequence = h.BaseSequence + 1
	return rj, nil
}

// OpenRecoveryJournal reads and validates the header of an existing journal
// file, then scans the record region to re-derive the append cursor and next
// sequence. Pairing against the store root is the caller's responsibility via
// Pair; a bare Open only proves the header self-consistent.
func OpenRecoveryJournal(file *os.File) (*RecoveryJournal, error) {
	var slots [recoveryJournalHeaderSlots][RecoveryJournalHeaderSize]byte
	selected := -1
	var header RecoveryJournalHeader
	for slot := 0; slot < recoveryJournalHeaderSlots; slot++ {
		off := int64(slot) * RecoveryJournalHeaderSize
		if _, err := readFullAt(file, slots[slot][:], off); err != nil {
			return nil, err
		}
		h, err := DecodeRecoveryJournalHeader(slots[slot][:])
		if err != nil {
			continue
		}
		if selected < 0 || h.RecycleCount > header.RecycleCount {
			selected = slot
			header = h
		}
	}
	if selected < 0 {
		return nil, fmt.Errorf("%w: no valid header slot", ErrRecoveryJournalCorrupt)
	}
	rj := newRecoveryJournalManager(file, header)
	rj.headerSlot = uint32(selected)
	if err := rj.scanTail(); err != nil {
		return nil, err
	}
	return rj, nil
}

func newRecoveryJournalManager(file *os.File, h RecoveryJournalHeader) *RecoveryJournal {
	rj := &RecoveryJournal{
		file:            file,
		header:          h,
		scratch:         make([]byte, RecoveryJournalHeaderSize),
		journalSync:     filesystemSync,
		journalDataSync: dataSync,
	}
	rj.writeAt = file.WriteAt
	return rj
}

// Header returns the value-only journal identity and geometry.
func (rj *RecoveryJournal) Header() RecoveryJournalHeader { return rj.header }

// Pair fails closed unless this journal's header identity matches the selected
// store root. It is the recovery gate that forbids replaying a stray or
// mismatched journal onto a store.
func (rj *RecoveryJournal) Pair(
	storeID, journalID [16]byte, pageSize uint32, rootGeneration uint64,
) error {
	if rj.header.StoreID != storeID || rj.header.JournalID != journalID {
		return ErrRecoveryJournalIdentity
	}
	if rj.header.PageSize != pageSize {
		return ErrRecoveryJournalGeometry
	}
	if rj.header.BaseGeneration > rootGeneration {
		return ErrRecoveryJournalEpoch
	}
	return nil
}

// writeHeader encodes and positionally writes one header slot. It is called
// only on create and recycle, never per record. The header is passed by value
// rather than read from rj.header so Recycle can write a staged candidate and
// commit it to memory only after the device accepted it.
func (rj *RecoveryJournal) writeHeader(slot uint32, header RecoveryJournalHeader) error {
	if _, err := EncodeRecoveryJournalHeader(rj.scratch, header); err != nil {
		return err
	}
	off := int64(slot) * RecoveryJournalHeaderSize
	if _, err := rj.writeAt(rj.scratch[:RecoveryJournalHeaderSize], off); err != nil {
		return err
	}
	return nil
}

// scanTail walks the record region from its start, validating strict-monotonic
// sequence and framing, to re-derive the append cursor after Open. It stops at
// the first invalid record — the torn or reordered tail — leaving the cursor
// and next sequence positioned to overwrite it.
func (rj *RecoveryJournal) scanTail() error {
	region := make([]byte, rj.header.Capacity)
	if _, err := readFullAt(rj.file, region, recoveryJournalRegionStart); err != nil {
		return err
	}
	cursor := uint64(0)
	sequence := rj.header.BaseSequence + 1
	for cursor < rj.header.Capacity {
		rec, padded, err := DecodeRecoveryRecord(
			region[cursor:], rj.header.SectorSize, sequence,
		)
		if err != nil {
			break
		}
		_ = rec
		if cursor+uint64(padded) > rj.header.Capacity {
			break
		}
		cursor += uint64(padded)
		sequence++
	}
	rj.cursor = cursor
	rj.nextSequence = sequence
	return nil
}

// Append writes one redo record at the cursor and advances it. It never extends
// the file: a record that would overrun the preallocated capacity returns
// ErrRecoveryJournalFull so the caller forces a checkpoint. Append does not
// sync; the caller issues the lane's sync exactly once after the append so a
// group of appends can share one fence.
func (rj *RecoveryJournal) Append(kind uint16, generation uint64, key, value []byte) (uint64, error) {
	rec := RecoveryRecord{
		Sequence:   rj.nextSequence,
		Generation: generation,
		Kind:       kind,
		Key:        key,
		Value:      value,
	}
	padded, ok := checkedRecoveryRecordPadded(
		rj.header.SectorSize, len(key), len(value),
	)
	if !ok {
		return 0, fmt.Errorf("%w: record key or value length", ErrInvalidWrite)
	}
	end, ok := checkedSizeAdd(rj.cursor, uint64(padded), ^uint64(0))
	if !ok || end > rj.header.Capacity {
		return 0, ErrRecoveryJournalFull
	}
	if cap(rj.scratch) < padded {
		rj.scratch = make([]byte, padded)
	}
	if _, err := EncodeRecoveryRecord(rj.scratch[:padded], rj.header.SectorSize, rec); err != nil {
		return 0, err
	}
	offset := int64(recoveryJournalRegionStart) + int64(rj.cursor)
	if _, err := rj.writeAt(rj.scratch[:padded], offset); err != nil {
		return 0, err
	}
	rj.cursor += uint64(padded)
	sequence := rj.nextSequence
	rj.nextSequence++
	return sequence, nil
}

// Fits reports whether a record of the given key and value lengths would fit in
// the remaining preallocated capacity without a recycle.
func (rj *RecoveryJournal) Fits(keyLen, valueLen int) bool {
	padded, ok := checkedRecoveryRecordPadded(
		rj.header.SectorSize, keyLen, valueLen,
	)
	if !ok {
		return false
	}
	end, ok := checkedSizeAdd(rj.cursor, uint64(padded), ^uint64(0))
	return ok && end <= rj.header.Capacity
}

// FitsBatch reports whether one batch record carrying entries would fit in the
// remaining preallocated capacity without a recycle.
func (rj *RecoveryJournal) FitsBatch(entries []RecoveryBatchEntry) bool {
	padded, ok := checkedRecoveryBatchRecordPaddedSize(
		rj.header.SectorSize, entries,
	)
	if !ok {
		return false
	}
	end, ok := checkedSizeAdd(rj.cursor, uint64(padded), ^uint64(0))
	return ok && end <= rj.header.Capacity
}

// AppendBatch writes one batch record carrying entries at the cursor and
// advances it, consuming a single sequence number. Like Append it never extends
// the file — a record that would overrun capacity returns ErrRecoveryJournalFull
// so the caller forces a checkpoint — and it does not sync: the caller issues the
// lane's single sync once after the append, which is the whole point of a batch
// record. The group is durable, atomically, after that one sync.
func (rj *RecoveryJournal) AppendBatch(
	generation uint64, entries []RecoveryBatchEntry,
) (uint64, error) {
	padded, ok := checkedRecoveryBatchRecordPaddedSize(
		rj.header.SectorSize, entries,
	)
	if !ok {
		return 0, fmt.Errorf("%w: batch record length", ErrInvalidWrite)
	}
	end, ok := checkedSizeAdd(rj.cursor, uint64(padded), ^uint64(0))
	if !ok || end > rj.header.Capacity {
		return 0, ErrRecoveryJournalFull
	}
	if cap(rj.scratch) < padded {
		rj.scratch = make([]byte, padded)
	}
	rec := RecoveryRecord{
		Sequence:   rj.nextSequence,
		Generation: generation,
		Kind:       recoveryRecordKindBatch,
		Entries:    entries,
	}
	if _, err := EncodeRecoveryBatchRecord(rj.scratch[:padded], rj.header.SectorSize, rec); err != nil {
		return 0, err
	}
	offset := int64(recoveryJournalRegionStart) + int64(rj.cursor)
	if _, err := rj.writeAt(rj.scratch[:padded], offset); err != nil {
		return 0, err
	}
	rj.cursor += uint64(padded)
	sequence := rj.nextSequence
	rj.nextSequence++
	return sequence, nil
}

// Sync issues the lane's sync on the journal file alone. powerSafe selects the
// F_FULLFSYNC-class barrier that survives sudden power loss; otherwise the
// ordinary filesystem-strength barrier (fdatasync on Linux) that survives
// process failure without the device-cache drain.
func (rj *RecoveryJournal) Sync(powerSafe bool) error {
	if powerSafe {
		return rj.journalDataSync(rj.file)
	}
	return rj.journalSync(rj.file)
}

// Recycle advances the journal head past a checkpointed generation. It rewrites
// the header with the new base generation and the current durable sequence,
// syncs, and resets the append cursor to the region start. Stale record bytes
// left in the preallocated region are never zeroed: the new BaseSequence anchor
// makes them fail monotonic-sequence validation, so a later recovery treats the
// recycled region as empty until fresh appends overwrite it.
//
// The header rewrite plus its sync is the journal half of the checkpoint's root
// publication. A crash between the store root publication and this sync leaves
// the old header, whose records recovery re-applies idempotently through the
// ordinary mutation path onto the newer root — the replay filter skips any
// record whose generation the root already covers.
func (rj *RecoveryJournal) Recycle(baseGeneration uint64) error {
	if baseGeneration < rj.header.BaseGeneration {
		return fmt.Errorf("%w: recycle base generation regressed", ErrGenerationOrder)
	}
	// Stage the advanced header and commit it to memory only after the write and
	// its sync both succeed. A failed recycle then leaves the manager describing
	// exactly what is on the device — the old base still guards the < check
	// above, the cursor still appends after the live records — instead of a
	// half-applied header whose in-memory base has moved past the durable one.
	next := rj.header
	next.BaseGeneration = baseGeneration
	next.BaseSequence = rj.nextSequence - 1
	next.RecycleCount++
	slot := rj.headerSlot ^ 1
	if err := rj.writeHeader(slot, next); err != nil {
		return err
	}
	if err := rj.journalDataSync(rj.file); err != nil {
		return err
	}
	rj.header = next
	rj.headerSlot = slot
	rj.cursor = 0
	return nil
}

// BaseGeneration reports the generation the current live region builds upon.
func (rj *RecoveryJournal) BaseGeneration() uint64 { return rj.header.BaseGeneration }

// NextSequence reports the sequence the next appended record will carry.
func (rj *RecoveryJournal) NextSequence() uint64 { return rj.nextSequence }

// Cursor reports the in-region byte offset of the next append. It is exposed
// for tests and diagnostics.
func (rj *RecoveryJournal) Cursor() uint64 { return rj.cursor }

// Replay walks the live record region and invokes fn for every record whose
// generation is strictly newer than baseGeneration, in append order. It stops
// at the first framing-invalid record (the torn or reordered tail) and returns
// nil: a torn tail is a truncation, not an error. fn's error aborts replay.
func (rj *RecoveryJournal) Replay(baseGeneration uint64, fn func(RecoveryRecord) error) error {
	region := make([]byte, rj.header.Capacity)
	if _, err := readFullAt(rj.file, region, recoveryJournalRegionStart); err != nil {
		return err
	}
	cursor := uint64(0)
	sequence := rj.header.BaseSequence + 1
	for cursor < rj.header.Capacity {
		rec, padded, err := DecodeRecoveryRecord(
			region[cursor:], rj.header.SectorSize, sequence,
		)
		if err != nil {
			// First invalid record truncates the replay. Everything before it was
			// individually sync-fenced and is durable; nothing after it can be
			// trusted.
			break
		}
		if cursor+uint64(padded) > rj.header.Capacity {
			break
		}
		if rec.Generation > baseGeneration {
			if err := fn(rec); err != nil {
				return err
			}
		}
		cursor += uint64(padded)
		sequence++
	}
	return nil
}

// Close closes the underlying journal file.
func (rj *RecoveryJournal) Close() error {
	if rj == nil || rj.file == nil {
		return nil
	}
	return rj.file.Close()
}

// readFullAt reads exactly len(buf) bytes at off, treating a short read as a
// corrupt-journal error rather than a silent truncation.
func readFullAt(file *os.File, buf []byte, off int64) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := file.ReadAt(buf[total:], off+int64(total))
		total += n
		if err != nil {
			if total == len(buf) {
				return total, nil
			}
			return total, err
		}
	}
	return total, nil
}
