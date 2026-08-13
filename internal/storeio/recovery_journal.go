package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
// protocol: one sequence, one generation, and an ordered list of put, delete,
// or compact scalar-patch entries, all under a single CRC and made durable by
// one append plus one sync.
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

	recoveryJournalMagic = "RJRNL00\x00"
	recoveryRecordMagic  = uint32(0x304a5252) // "RRJ0", little-endian.

	// RecoveryJournalFormatLegacy is the original put/delete batch format. Its
	// zero value preserves every pre-feature header byte and keeps callers that
	// do not opt in safely on the legacy grammar.
	RecoveryJournalFormatLegacy = uint32(0)
	// RecoveryJournalFormatScalarPatch admits compact scalar-patch batch entries.
	// The feature version occupies the header's former DevelopmentFormatVersion
	// word, so an older binary's existing version==0 check rejects the journal
	// before it can misinterpret kind 4.
	RecoveryJournalFormatScalarPatch = uint32(1)
	// RecoveryJournalFormatConditional admits conditional batch records (kind 5)
	// used by multi-collection prepare. It occupies the same header format word
	// older binaries require to hold a known value, so a legacy or scalar-patch
	// reader rejects the journal before any record decode. Scalar-patch entries
	// (kind 4) remain scalar-patch-format-only: the ordinary buffered delta lane
	// never coexists with conditional records under this format word.
	RecoveryJournalFormatConditional = uint32(2)

	// recoveryJournalFlagSealedCapacity marks Capacity as an immutable physical
	// certificate. It occupies the current header's reserved flags word; every
	// other bit remains invalid.
	recoveryJournalFlagSealedCapacity = uint32(1)

	// RecordKindPut marks a same-size inline value replacement.
	recoveryRecordKindPut = uint16(1)
	// recoveryRecordKindDelete marks a key removal.
	recoveryRecordKindDelete = uint16(2)
	// recoveryRecordKindBatch marks a group-commit record: a single sequence and
	// generation covering an ordered list of logical mutation entries, framed by
	// one CRC. It is the group-commit primitive — the whole record is durable after
	// one sync, and a torn append fails framing so recovery replays either every
	// entry or none. A multi-document Update rides it; a future group of
	// independent acknowledgements sharing one sync is the same record with
	// unrelated entries.
	recoveryRecordKindBatch = uint16(3)
	// recoveryRecordKindScalarPatch marks a compact existing-key replacement
	// carried only as an entry inside a batch record. Its Value is the new
	// canonical integer, bool, or null spelling; ScalarPatch identifies the old
	// spelling in the canonical document and seals the expected complete result.
	// It is deliberately not a top-level record kind.
	recoveryRecordKindScalarPatch = uint16(4)
	// recoveryRecordKindConditionalBatch marks a prepare-time batch whose body is
	// the ordinary batch entry grammar prefixed by a 32-byte conditional header
	// (MarkerID, MarkerEpoch, TxnID). One sequence, one generation, one CRC —
	// identical framing discipline to kind 3. Decide-time resolution is not this
	// package's job; decode surfaces the conditional header to the replay caller.
	recoveryRecordKindConditionalBatch = uint16(5)

	// RecoveryRecordKindPut, RecoveryRecordKindDelete, RecoveryRecordKindBatch,
	// RecoveryRecordKindScalarPatch, and RecoveryRecordKindConditionalBatch are
	// the exported record kinds a store passes to Append/AppendBatch/
	// AppendConditionalBatch and matches on during Replay. ScalarPatch is
	// batch-entry-only; EncodeRecoveryRecord and DecodeRecoveryRecord reject it as
	// a standalone record. ConditionalBatch is top-level and conditional-format-
	// only.
	RecoveryRecordKindPut              = recoveryRecordKindPut
	RecoveryRecordKindDelete           = recoveryRecordKindDelete
	RecoveryRecordKindBatch            = recoveryRecordKindBatch
	RecoveryRecordKindScalarPatch      = recoveryRecordKindScalarPatch
	RecoveryRecordKindConditionalBatch = recoveryRecordKindConditionalBatch

	// RecoveryBatchEntryHeaderSize is the fixed per-entry framing inside a batch
	// record that precedes the entry's variable key and value bytes:
	// kind (2) + reserved (2) + keyLen (4) + valueLen (4).
	RecoveryBatchEntryHeaderSize = 12
	// RecoveryConditionalHeaderSize is the fixed wire prefix of a conditional
	// batch body: MarkerID (16) + MarkerEpoch (8) + TxnID (8).
	RecoveryConditionalHeaderSize = 32
	// RecoveryScalarPatchMetadataSize is the fixed wire framing inserted after a
	// scalar-patch entry header and before its key and new scalar bytes. Byte 3 is
	// reserved zero, keeping the checksum naturally aligned while making future
	// format damage fail closed.
	RecoveryScalarPatchMetadataSize = 8
	// recoveryScalarPatchMaxCanonicalBytes is the widest scalar the compact lane
	// accepts: a sign plus CanonicalIntMaxDigits. Bool and null are shorter.
	recoveryScalarPatchMaxCanonicalBytes = CanonicalIntMaxDigits + 1
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
	ErrRecoveryJournalCorrupt = errors.New("vibedb: corrupt recovery journal header")
	// ErrRecoveryJournalIdentity reports a header that framed correctly but does
	// not pair with the selected store root (StoreID or JournalID mismatch). A
	// referenced-but-mismatched journal must never be replayed onto the store.
	ErrRecoveryJournalIdentity = errors.New("vibedb: recovery journal identity mismatch")

	// ErrRecoveryJournalGeometry reports a paired journal whose page geometry
	// does not match the store that names it — the bundle halves were built
	// for different stores even though the identities collide.
	ErrRecoveryJournalGeometry = errors.New("vibedb: recovery journal geometry mismatch")

	// ErrRecoveryJournalEpoch reports a journal whose base generation is ahead
	// of the root that selected it: the store file is older than the journal
	// (a mixed-epoch bundle, typically a restored store beside a live
	// journal), so acknowledgements recycled during the gap are gone and the
	// pair must fail closed rather than open with silent loss.
	ErrRecoveryJournalEpoch = errors.New("vibedb: recovery journal is ahead of the store root")
	// ErrRecoveryJournalMissing reports a store root that references a journal
	// (non-zero JournalID) whose file is absent. This fails closed: the store
	// may have acknowledged mutations that only the missing journal records.
	ErrRecoveryJournalMissing = errors.New("vibedb: referenced recovery journal is missing")
	// ErrRecoveryJournalFull reports that the next record does not fit the
	// preallocated capacity. The caller must force a checkpoint, which recycles
	// the journal head, exactly as staging pressure forces one today.
	ErrRecoveryJournalFull = errors.New("vibedb: recovery journal is full")
	// ErrRecoveryJournalRecord reports a record that failed framing, checksum,
	// monotonic-sequence, or semantic validation. Framing/checksum-invalid tails
	// truncate replay; checksum-authenticated semantic failures are returned and
	// fail recovery closed.
	ErrRecoveryJournalRecord = errors.New("vibedb: invalid recovery journal record")
	// errRecoveryJournalTruncatableTail distinguishes damage consistent with an
	// incomplete/reordered append from a checksum-authenticated semantic error.
	// Both still match ErrRecoveryJournalRecord for API compatibility; only this
	// private marker may be swallowed by scanTail and Replay.
	errRecoveryJournalTruncatableTail = errors.New("vibedb: truncatable recovery journal tail")
)

func recoveryJournalTailError(reason string) error {
	return fmt.Errorf(
		"%w: %w: %s",
		ErrRecoveryJournalRecord, errRecoveryJournalTruncatableTail, reason,
	)
}

func recoveryJournalSemanticError(reason string) error {
	return fmt.Errorf("%w: semantic: %s", ErrRecoveryJournalRecord, reason)
}

func recoveryJournalFormatSupported(formatVersion uint32) bool {
	return formatVersion == RecoveryJournalFormatLegacy ||
		formatVersion == RecoveryJournalFormatScalarPatch ||
		formatVersion == RecoveryJournalFormatConditional
}

// RecoveryJournalHeader is the pointer-free format, identity, and geometry of
// one journal file. FormatVersion gates the record grammar before scanning;
// BaseGeneration is the store root generation the current live region builds
// upon, and recovery replays only records strictly newer than it.
// BaseSequence anchors monotonic-sequence validation so stale bytes left in the
// preallocated region after a recycle can never be mistaken for live records —
// the first live record must carry exactly BaseSequence+1.
type RecoveryJournalHeader struct {
	FormatVersion  uint32
	StoreID        [16]byte
	JournalID      [16]byte
	PageSize       uint32
	SectorSize     uint32
	BaseGeneration uint64
	BaseSequence   uint64
	Capacity       uint64
	// SealedCapacity requires an exact-size, strictly allocated journal file.
	// The bit is immutable across recycle and disables capacity growth.
	SealedCapacity bool
	// RecycleCount is strictly monotonic across header publications (recycle or
	// compatible capacity growth). Recovery selects the valid header slot with
	// the highest count, so a torn publication leaves the previous slot as the
	// recovery point.
	RecycleCount uint64
}

// RecoveryConditionalHeader is the pointer-free prepare binding carried by one
// RecoveryRecordKindConditionalBatch record. MarkerID identifies the decision
// log; MarkerEpoch is the epoch under which the record was written; TxnID is
// unique within that epoch. Replay callers receive these fields and decide
// apply/skip/fail-closed themselves — this package only decodes.
type RecoveryConditionalHeader struct {
	MarkerID    [16]byte
	MarkerEpoch uint64
	TxnID       uint64
}

// RecoveryRecord is one decoded redo record. Key and Value borrow the decode
// buffer and must be copied if retained past the next decode.
//
// For a batch record (Kind == RecoveryRecordKindBatch) the logical mutations are
// carried in Entries instead of Key/Value; Sequence and Generation are the
// group's single sequence and generation. For a conditional batch
// (Kind == RecoveryRecordKindConditionalBatch) Entries holds the same put/
// delete grammar and Conditional carries the prepare binding; Key/Value stay
// empty.
type RecoveryRecord struct {
	Sequence    uint64
	Generation  uint64
	Kind        uint16
	Key         []byte
	Value       []byte
	Entries     []RecoveryBatchEntry
	Conditional RecoveryConditionalHeader
}

// RecoveryScalarPatchMetadata is the pointer-free certificate carried by one
// RecoveryRecordKindScalarPatch batch entry. CanonicalOffset and
// OldScalarLength select the spelling to replace in the pre-patch canonical
// document. ExpectedResultChecksum is PageChecksum over the complete canonical
// document after replacement, so recovery can reject a stale key, wrong base,
// or damaged patch without materializing a second journal payload.
//
// Its wire representation is fixed at RecoveryScalarPatchMetadataSize bytes;
// the encoder does not serialize Go struct padding.
type RecoveryScalarPatchMetadata struct {
	CanonicalOffset        uint16
	OldScalarLength        uint8
	ExpectedResultChecksum uint32
}

var (
	_ [RecoveryScalarPatchMetadataSize - int(unsafe.Sizeof(RecoveryScalarPatchMetadata{}))]byte
	_ [int(unsafe.Sizeof(RecoveryScalarPatchMetadata{})) - RecoveryScalarPatchMetadataSize]byte
)

// RecoveryBatchEntry is one put, delete, or compact scalar replacement inside
// a batch record. Key and Value borrow the decode buffer, exactly like
// RecoveryRecord's own fields. For RecoveryRecordKindScalarPatch, Value is only
// the new canonical scalar spelling and ScalarPatch carries its patch metadata.
type RecoveryBatchEntry struct {
	Kind        uint16
	Key         []byte
	Value       []byte
	ScalarPatch RecoveryScalarPatchMetadata
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

func recoveryScalarPatchValueValid(value []byte) bool {
	switch len(value) {
	case 4:
		if value[0] == 't' && value[1] == 'r' &&
			value[2] == 'u' && value[3] == 'e' {
			return true
		}
		if value[0] == 'n' && value[1] == 'u' &&
			value[2] == 'l' && value[3] == 'l' {
			return true
		}
	case 5:
		if value[0] == 'f' && value[1] == 'a' && value[2] == 'l' &&
			value[3] == 's' && value[4] == 'e' {
			return true
		}
	}
	_, integer := CanonicalIntValue(value)
	return integer
}

func recoveryScalarPatchValid(
	metadata RecoveryScalarPatchMetadata, value []byte,
) bool {
	oldLength := uint32(metadata.OldScalarLength)
	if oldLength == 0 ||
		oldLength > recoveryScalarPatchMaxCanonicalBytes ||
		len(value) == 0 || len(value) > recoveryScalarPatchMaxCanonicalBytes ||
		!recoveryScalarPatchValueValid(value) {
		return false
	}
	// A uint16 offset can address a byte anywhere in one 64-KiB canonical
	// document. Both the removed and inserted spellings must end within that
	// same representable window; recovery can then use ordinary int slicing
	// without an offset-plus-length wrap or an ambiguous widened coordinate.
	const canonicalWindow = uint32(1) << 16
	offset := uint32(metadata.CanonicalOffset)
	return offset+oldLength <= canonicalWindow &&
		offset+uint32(len(value)) <= canonicalWindow
}

func recoveryBatchEntryMetadataSize(
	entry RecoveryBatchEntry,
) (uint64, bool) {
	switch entry.Kind {
	case recoveryRecordKindPut, recoveryRecordKindDelete:
		if entry.ScalarPatch != (RecoveryScalarPatchMetadata{}) {
			return 0, false
		}
		return 0, true
	case recoveryRecordKindScalarPatch:
		if !recoveryScalarPatchValid(entry.ScalarPatch, entry.Value) {
			return 0, false
		}
		return RecoveryScalarPatchMetadataSize, true
	default:
		return 0, false
	}
}

func recoveryBatchEntriesAllowed(
	formatVersion uint32, entries []RecoveryBatchEntry,
) bool {
	if !recoveryJournalFormatSupported(formatVersion) {
		return false
	}
	for i := range entries {
		switch entries[i].Kind {
		case recoveryRecordKindPut, recoveryRecordKindDelete:
			if entries[i].ScalarPatch != (RecoveryScalarPatchMetadata{}) {
				return false
			}
		case recoveryRecordKindScalarPatch:
			// Scalar-patch remains the scalar-patch format's lane only. The
			// conditional journal format never coexists with it.
			if formatVersion != RecoveryJournalFormatScalarPatch {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func checkedRecoveryBatchBodyAdd(
	body, keyLen, valueLen uint64,
) (uint64, bool) {
	return checkedRecoveryBatchBodyAddMetadata(body, 0, keyLen, valueLen)
}

func checkedRecoveryBatchBodyAddMetadata(
	body, metadataBytes, keyLen, valueLen uint64,
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
	next, ok = checkedSizeAdd(next, metadataBytes, wireLimit)
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
		metadataBytes, valid := recoveryBatchEntryMetadataSize(entries[i])
		if !valid {
			return 0, false
		}
		var ok bool
		body, ok = checkedRecoveryBatchBodyAddMetadata(
			body,
			metadataBytes,
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
	return checkedRecoveryBatchRecordPaddedSizeForBody(sectorSize, body)
}

func checkedRecoveryBatchRecordPaddedSizeForBody(
	sectorSize uint32, body uint64,
) (int, bool) {
	raw := uint64(
		RecoveryJournalRecordPrefixSize +
			RecoveryJournalRecordTrailerSize,
	)
	raw, ok := checkedSizeAdd(raw, body, uint64(maxIntValue))
	if !ok {
		return 0, false
	}
	return checkedRecoveryPadRaw(sectorSize, raw)
}

// RecoveryBatchPlan is an opaque, allocation-free sizing result for one batch
// record. Preparing once lets a caller use the same validated layout for its
// capacity decision, append, and byte accounting instead of rescanning every
// entry at each step. A plan is bound to the journal feature version that
// prepared it, preventing a scalar-enabled plan from crossing into a legacy
// journal. Its fields are deliberately private: only this package can mint a
// layout accepted by AppendPreparedBatch.
type RecoveryBatchPlan struct {
	formatVersion uint32
	sectorSize    uint32
	entryCount    int
	bodyLen       uint64
	padded        int
}

// PaddedSize returns the exact sector-padded bytes the prepared batch consumes.
func (p RecoveryBatchPlan) PaddedSize() int { return p.padded }

func prepareRecoveryBatch(
	sectorSize uint32, entries []RecoveryBatchEntry,
) (RecoveryBatchPlan, bool) {
	return prepareRecoveryBatchForFormat(
		RecoveryJournalFormatScalarPatch, sectorSize, entries,
	)
}

func prepareRecoveryBatchForFormat(
	formatVersion, sectorSize uint32, entries []RecoveryBatchEntry,
) (RecoveryBatchPlan, bool) {
	if !recoveryBatchEntriesAllowed(formatVersion, entries) {
		return RecoveryBatchPlan{}, false
	}
	body, ok := checkedRecoveryBatchBodyLen(entries)
	if !ok {
		return RecoveryBatchPlan{}, false
	}
	padded, ok := checkedRecoveryBatchRecordPaddedSizeForBody(
		sectorSize, body,
	)
	if !ok {
		return RecoveryBatchPlan{}, false
	}
	return RecoveryBatchPlan{
		formatVersion: formatVersion,
		sectorSize:    sectorSize,
		entryCount:    len(entries),
		bodyLen:       body,
		padded:        padded,
	}, true
}

func (p RecoveryBatchPlan) validFor(
	sectorSize uint32, entryCount int,
) bool {
	if !recoveryJournalFormatSupported(p.formatVersion) ||
		p.sectorSize != sectorSize || p.entryCount != entryCount ||
		p.entryCount <= 0 || p.bodyLen > uint64(^uint32(0)) ||
		p.padded <= 0 {
		return false
	}
	padded, ok := checkedRecoveryBatchRecordPaddedSizeForBody(
		p.sectorSize, p.bodyLen,
	)
	return ok && padded == p.padded
}

func (p RecoveryBatchPlan) validForFormat(
	formatVersion, sectorSize uint32, entryCount int,
) bool {
	return p.formatVersion == formatVersion &&
		p.validFor(sectorSize, entryCount)
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

// RecoveryBatchRecordPaddedSizeForPayload returns the exact padded record size
// for a batch with entryCount fixed entry headers and totalPayloadBytes bytes
// after those headers. For legacy put/delete entries that payload is key+value;
// callers estimating metadata-bearing entry kinds must add their metadata bytes
// themselves. Consequently this is not an upper bound for scalar-patch batches
// when passed only their key+value arena size. Invalid or unrepresentable inputs
// saturate at the largest int.
func RecoveryBatchRecordPaddedSizeForPayload(
	sectorSize uint32, entryCount, totalPayloadBytes int,
) int {
	if entryCount <= 0 || totalPayloadBytes < 0 {
		return maxIntValue
	}
	count := uint64(entryCount)
	payload := uint64(totalPayloadBytes)
	wireLimit := uint64(^uint32(0))
	if count > wireLimit {
		return maxIntValue
	}
	body, ok := checkedSizeMul(
		count, RecoveryBatchEntryHeaderSize, wireLimit,
	)
	if !ok {
		return maxIntValue
	}
	body, ok = checkedSizeAdd(body, payload, wireLimit)
	if !ok {
		return maxIntValue
	}
	raw := uint64(
		RecoveryJournalRecordPrefixSize +
			RecoveryJournalRecordTrailerSize,
	)
	raw, ok = checkedSizeAdd(raw, body, uint64(maxIntValue))
	if !ok {
		return maxIntValue
	}
	padded, ok := checkedRecoveryPadRaw(sectorSize, raw)
	if !ok {
		return maxIntValue
	}
	return padded
}

// validateRecoveryJournalHeader enforces the geometry invariants every header
// must satisfy regardless of provenance.
func validateRecoveryJournalHeader(h RecoveryJournalHeader) error {
	if !recoveryJournalFormatSupported(h.FormatVersion) {
		return fmt.Errorf("%w: format version", ErrRecoveryJournalCorrupt)
	}
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
	binary.LittleEndian.PutUint32(sector[8:12], h.FormatVersion)
	binary.LittleEndian.PutUint32(sector[12:16], RecoveryJournalHeaderSize)
	copy(sector[16:32], h.StoreID[:])
	copy(sector[32:48], h.JournalID[:])
	binary.LittleEndian.PutUint32(sector[48:52], h.PageSize)
	binary.LittleEndian.PutUint32(sector[52:56], h.SectorSize)
	binary.LittleEndian.PutUint64(sector[56:64], h.BaseGeneration)
	binary.LittleEndian.PutUint64(sector[64:72], h.BaseSequence)
	binary.LittleEndian.PutUint64(sector[72:80], h.Capacity)
	binary.LittleEndian.PutUint64(sector[80:88], h.RecycleCount)
	if h.SealedCapacity {
		binary.LittleEndian.PutUint32(sector[88:92], recoveryJournalFlagSealedCapacity)
	}
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
	formatVersion := binary.LittleEndian.Uint32(src[8:12])
	if !recoveryJournalFormatSupported(formatVersion) ||
		binary.LittleEndian.Uint32(src[12:16]) != RecoveryJournalHeaderSize {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: version or header size", ErrRecoveryJournalCorrupt)
	}
	h := RecoveryJournalHeader{FormatVersion: formatVersion}
	copy(h.StoreID[:], src[16:32])
	copy(h.JournalID[:], src[32:48])
	h.PageSize = binary.LittleEndian.Uint32(src[48:52])
	h.SectorSize = binary.LittleEndian.Uint32(src[52:56])
	h.BaseGeneration = binary.LittleEndian.Uint64(src[56:64])
	h.BaseSequence = binary.LittleEndian.Uint64(src[64:72])
	h.Capacity = binary.LittleEndian.Uint64(src[72:80])
	h.RecycleCount = binary.LittleEndian.Uint64(src[80:88])
	flags := binary.LittleEndian.Uint32(src[88:92])
	if flags&^recoveryJournalFlagSealedCapacity != 0 {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: header flags", ErrRecoveryJournalCorrupt)
	}
	if !allZero(src[92 : RecoveryJournalHeaderSize-8]) {
		return RecoveryJournalHeader{}, fmt.Errorf("%w: header reserved bytes", ErrRecoveryJournalCorrupt)
	}
	h.SealedCapacity = flags&recoveryJournalFlagSealedCapacity != 0
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

func encodeRecoveryScalarPatchMetadata(
	dst []byte, metadata RecoveryScalarPatchMetadata,
) {
	binary.LittleEndian.PutUint16(dst[0:2], metadata.CanonicalOffset)
	dst[2] = metadata.OldScalarLength
	// dst[3] is reserved zero; the enclosing batch buffer was cleared.
	binary.LittleEndian.PutUint32(dst[4:8], metadata.ExpectedResultChecksum)
}

func decodeRecoveryScalarPatchMetadata(
	src []byte,
) (RecoveryScalarPatchMetadata, bool) {
	if len(src) < RecoveryScalarPatchMetadataSize || src[3] != 0 {
		return RecoveryScalarPatchMetadata{}, false
	}
	return RecoveryScalarPatchMetadata{
		CanonicalOffset:        binary.LittleEndian.Uint16(src[0:2]),
		OldScalarLength:        src[2],
		ExpectedResultChecksum: binary.LittleEndian.Uint32(src[4:8]),
	}, true
}

func encodeRecoveryBatchRecordPrepared(
	dst []byte, sectorSize uint32, rec RecoveryRecord,
	plan RecoveryBatchPlan,
) (int, error) {
	if rec.Sequence == 0 || rec.Generation == 0 {
		return 0, fmt.Errorf("%w: zero sequence or generation", ErrInvalidWrite)
	}
	if !plan.validFor(sectorSize, len(rec.Entries)) {
		return 0, fmt.Errorf("%w: batch plan", ErrInvalidWrite)
	}
	if len(dst) < plan.padded {
		return 0, fmt.Errorf("%w: batch record buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), plan.padded)
	}
	buf := dst[:plan.padded]
	clear(buf)
	binary.LittleEndian.PutUint32(buf[0:4], recoveryRecordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], recoveryRecordKindBatch)
	// buf[6:8] reserved zero.
	binary.LittleEndian.PutUint64(buf[8:16], rec.Sequence)
	binary.LittleEndian.PutUint64(buf[16:24], rec.Generation)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(rec.Entries)))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(plan.bodyLen))
	cursor := uint64(RecoveryJournalRecordPrefixSize)
	bodyEnd := cursor + plan.bodyLen
	for i := range rec.Entries {
		entry := rec.Entries[i]
		if entry.Kind == recoveryRecordKindScalarPatch &&
			plan.formatVersion != RecoveryJournalFormatScalarPatch {
			return 0, fmt.Errorf(
				"%w: scalar-patch entry outside scalar-patch journal",
				ErrInvalidWrite,
			)
		}
		metadataBytes, valid := recoveryBatchEntryMetadataSize(entry)
		if !valid {
			return 0, fmt.Errorf(
				"%w: batch entry kind or scalar-patch metadata",
				ErrInvalidWrite,
			)
		}
		if len(entry.Key) == 0 ||
			uint64(len(entry.Key)) > uint64(^uint32(0)) ||
			uint64(len(entry.Value)) > uint64(^uint32(0)) {
			return 0, fmt.Errorf("%w: batch entry key or value length", ErrInvalidWrite)
		}
		entryEnd := cursor + RecoveryBatchEntryHeaderSize + metadataBytes +
			uint64(len(entry.Key)) + uint64(len(entry.Value))
		if entryEnd > bodyEnd {
			return 0, fmt.Errorf("%w: batch plan no longer matches entries", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint16(buf[cursor:cursor+2], entry.Kind)
		// buf[cursor+2:cursor+4] reserved zero.
		binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], uint32(len(entry.Key)))
		binary.LittleEndian.PutUint32(buf[cursor+8:cursor+12], uint32(len(entry.Value)))
		cursor += RecoveryBatchEntryHeaderSize
		if entry.Kind == recoveryRecordKindScalarPatch {
			encodeRecoveryScalarPatchMetadata(
				buf[cursor:cursor+RecoveryScalarPatchMetadataSize],
				entry.ScalarPatch,
			)
			cursor += RecoveryScalarPatchMetadataSize
		}
		cursor += uint64(copy(buf[cursor:], entry.Key))
		cursor += uint64(copy(buf[cursor:], entry.Value))
	}
	if cursor != bodyEnd {
		return 0, fmt.Errorf("%w: batch plan no longer matches entries", ErrInvalidWrite)
	}
	checksum := PageChecksum(buf[:cursor])
	binary.LittleEndian.PutUint32(buf[cursor:cursor+4], checksum)
	binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], ^checksum)
	return plan.padded, nil
}

// DecodeRecoveryRecord validates one record at the start of src against the
// expected sequence and returns the borrowed record and its padded size. A
// record whose magic, framing, checksum, or sequence is wrong returns a
// truncatable ErrRecoveryJournalRecord. Once a complete record's CRC validates,
// an unknown kind or impossible semantic payload instead returns a hard
// ErrRecoveryJournalRecord that scanTail and Replay propagate.
func DecodeRecoveryRecord(
	src []byte, sectorSize uint32, expectedSequence uint64,
) (RecoveryRecord, int, error) {
	if len(src) < RecoveryJournalRecordPrefixSize+RecoveryJournalRecordTrailerSize {
		return RecoveryRecord{}, 0, recoveryJournalTailError("short record")
	}
	if binary.LittleEndian.Uint32(src[0:4]) != recoveryRecordMagic {
		return RecoveryRecord{}, 0, recoveryJournalTailError("magic")
	}
	kind := binary.LittleEndian.Uint16(src[4:6])
	if binary.LittleEndian.Uint16(src[6:8]) != 0 {
		return RecoveryRecord{}, 0, recoveryJournalTailError("reserved")
	}
	if kind == recoveryRecordKindBatch {
		return decodeRecoveryBatchRecord(src, sectorSize, expectedSequence)
	}
	if kind == recoveryRecordKindConditionalBatch {
		return decodeRecoveryConditionalBatchRecord(
			src, sectorSize, expectedSequence,
		)
	}
	sequence := binary.LittleEndian.Uint64(src[8:16])
	generation := binary.LittleEndian.Uint64(src[16:24])
	keyLen := binary.LittleEndian.Uint32(src[24:28])
	valueLen := binary.LittleEndian.Uint32(src[28:32])
	if sequence != expectedSequence || generation == 0 || keyLen == 0 {
		return RecoveryRecord{}, 0, recoveryJournalTailError("sequence or framing")
	}
	keyStart := uint64(RecoveryJournalRecordPrefixSize)
	valueStart := keyStart + uint64(keyLen)
	bodyEnd := valueStart + uint64(valueLen)
	if bodyEnd+RecoveryJournalRecordTrailerSize > uint64(len(src)) {
		return RecoveryRecord{}, 0, recoveryJournalTailError("length overruns region")
	}
	checksum := PageChecksum(src[:bodyEnd])
	stored := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	if stored != checksum ||
		binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) != ^checksum {
		return RecoveryRecord{}, 0, recoveryJournalTailError("checksum")
	}
	if kind != recoveryRecordKindPut && kind != recoveryRecordKindDelete {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid standalone record kind",
		)
	}
	padded, ok := checkedRecoveryPadRaw(
		sectorSize, bodyEnd+RecoveryJournalRecordTrailerSize,
	)
	if !ok {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid padded record length",
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
		return RecoveryRecord{}, 0, recoveryJournalTailError("batch sequence or framing")
	}
	bodyEnd := uint64(RecoveryJournalRecordPrefixSize) + uint64(bodyLen)
	if bodyEnd+RecoveryJournalRecordTrailerSize > uint64(len(src)) {
		return RecoveryRecord{}, 0, recoveryJournalTailError("batch length overruns region")
	}
	checksum := PageChecksum(src[:bodyEnd])
	stored := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	if stored != checksum ||
		binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) != ^checksum {
		return RecoveryRecord{}, 0, recoveryJournalTailError("batch checksum")
	}
	// Every entry has a fixed header and a non-empty key. Prove the peer-chosen
	// count fits both the framed body and native slice capacity before make;
	// otherwise a tiny checksummed body could request a multi-gigabyte arena.
	if uint64(entryCount) >
		uint64(bodyLen)/(RecoveryBatchEntryHeaderSize+1) ||
		uint64(entryCount) > uint64(maxIntValue) {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid batch entry count overruns body",
		)
	}
	if !recoveryBatchEntryArenaFits(uint64(entryCount)) {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid batch entry arena overflows address space",
		)
	}
	entries := make([]RecoveryBatchEntry, 0, entryCount)
	cursor := uint64(RecoveryJournalRecordPrefixSize)
	for i := uint32(0); i < entryCount; i++ {
		if cursor+RecoveryBatchEntryHeaderSize > bodyEnd {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid batch entry header overruns",
			)
		}
		entryKind := binary.LittleEndian.Uint16(src[cursor : cursor+2])
		if binary.LittleEndian.Uint16(src[cursor+2:cursor+4]) != 0 ||
			(entryKind != recoveryRecordKindPut &&
				entryKind != recoveryRecordKindDelete &&
				entryKind != recoveryRecordKindScalarPatch) {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid batch entry kind or reserved",
			)
		}
		keyLen := uint64(binary.LittleEndian.Uint32(src[cursor+4 : cursor+8]))
		valueLen := uint64(binary.LittleEndian.Uint32(src[cursor+8 : cursor+12]))
		if keyLen == 0 {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid batch entry key length",
			)
		}
		payloadStart := cursor + RecoveryBatchEntryHeaderSize
		var scalarPatch RecoveryScalarPatchMetadata
		if entryKind == recoveryRecordKindScalarPatch {
			metadataEnd := payloadStart + RecoveryScalarPatchMetadataSize
			if metadataEnd > bodyEnd {
				return RecoveryRecord{}, 0, recoveryJournalSemanticError(
					"checksum-valid scalar-patch metadata overruns body",
				)
			}
			var metadataOK bool
			scalarPatch, metadataOK = decodeRecoveryScalarPatchMetadata(
				src[payloadStart:metadataEnd],
			)
			if !metadataOK {
				return RecoveryRecord{}, 0, recoveryJournalSemanticError(
					"checksum-valid scalar-patch metadata",
				)
			}
			payloadStart = metadataEnd
		}
		keyStart := payloadStart
		valueStart := keyStart + keyLen
		entryEnd := valueStart + valueLen
		if entryEnd > bodyEnd {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid batch entry overruns body",
			)
		}
		value := src[valueStart:entryEnd]
		if entryKind == recoveryRecordKindScalarPatch &&
			!recoveryScalarPatchValid(scalarPatch, value) {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid scalar-patch bounds or value",
			)
		}
		entries = append(entries, RecoveryBatchEntry{
			Kind:        entryKind,
			Key:         src[keyStart:valueStart],
			Value:       value,
			ScalarPatch: scalarPatch,
		})
		cursor = entryEnd
	}
	if cursor != bodyEnd {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid batch body length mismatch",
		)
	}
	padded, ok := checkedRecoveryPadRaw(
		sectorSize, bodyEnd+RecoveryJournalRecordTrailerSize,
	)
	if !ok {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid padded batch length",
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

func validateRecoveryRecordForFormat(
	formatVersion uint32, rec RecoveryRecord,
) error {
	if !recoveryJournalFormatSupported(formatVersion) {
		return recoveryJournalSemanticError("unsupported journal format version")
	}
	switch rec.Kind {
	case recoveryRecordKindPut, recoveryRecordKindDelete:
		return nil
	case recoveryRecordKindBatch:
		for i := range rec.Entries {
			if rec.Entries[i].Kind != recoveryRecordKindScalarPatch {
				continue
			}
			if formatVersion != RecoveryJournalFormatScalarPatch {
				return recoveryJournalSemanticError(
					"scalar-patch entry outside scalar-patch journal",
				)
			}
		}
		return nil
	case recoveryRecordKindConditionalBatch:
		if formatVersion != RecoveryJournalFormatConditional {
			return recoveryJournalSemanticError(
				"conditional-batch record outside conditional journal",
			)
		}
		for i := range rec.Entries {
			if rec.Entries[i].Kind == recoveryRecordKindScalarPatch {
				return recoveryJournalSemanticError(
					"scalar-patch entry in conditional journal",
				)
			}
		}
		return nil
	default:
		return recoveryJournalSemanticError("unsupported record kind")
	}
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
	// headerSlot is the alternating slot the live header occupies. Recycle and
	// GrowCapacity write the opposite slot and flip this after sync succeeds.
	headerSlot uint32
	scratch    []byte
	// journalSync and journalDataSync are injected so a fault seam can wrap the
	// real barriers; production wires the platform sync helpers.
	journalSync     func(*os.File) error
	journalDataSync func(*os.File) error
	// writeAt is injected so a fault seam can intercept the append write.
	writeAt func(p []byte, off int64) (int, error)
}

// RecoveryJournalPairing is an external exact identity and recovery-epoch
// assertion. Mutable open validates it immediately after header selection,
// before allocation proof or record scanning.
type RecoveryJournalPairing struct {
	StoreID        [16]byte
	JournalID      [16]byte
	PageSize       uint32
	RootGeneration uint64
}

// RecoveryJournalOpenOptions supplies external exact assertions. A zero
// SealedCapacityBytes requires an ordinary unsealed journal. A non-zero value
// requires the persisted sealed bit and an exact record-region byte match.
// Pairing, when non-nil, is checked before either allocation proof or scan.
type RecoveryJournalOpenOptions struct {
	SealedCapacityBytes uint64
	Pairing             *RecoveryJournalPairing
}

var recoveryJournalScanTail = func(journal *RecoveryJournal) error {
	return journal.scanTail()
}

// RecoveryJournalInspection is a read-only offline scan. It never proves
// physical allocation and exposes no append, sync, recycle, or growth method,
// so it cannot be confused with a qualified mutable journal handle.
type RecoveryJournalInspection struct {
	journal *RecoveryJournal
}

func (i *RecoveryJournalInspection) Header() RecoveryJournalHeader {
	if i == nil || i.journal == nil {
		return RecoveryJournalHeader{}
	}
	return i.journal.Header()
}

func (i *RecoveryJournalInspection) Cursor() uint64 {
	if i == nil || i.journal == nil {
		return 0
	}
	return i.journal.Cursor()
}

func (i *RecoveryJournalInspection) BaseGeneration() uint64 {
	if i == nil || i.journal == nil {
		return 0
	}
	return i.journal.BaseGeneration()
}

func (i *RecoveryJournalInspection) Replay(
	afterGeneration uint64,
	apply func(RecoveryRecord) error,
) error {
	if i == nil || i.journal == nil {
		return fmt.Errorf("%w: closed recovery journal inspection", ErrInvalidWrite)
	}
	return i.journal.Replay(afterGeneration, apply)
}

func (i *RecoveryJournalInspection) Close() error {
	if i == nil || i.journal == nil {
		return nil
	}
	err := i.journal.Close()
	i.journal = nil
	return err
}

// CreateRecoveryJournal preallocates a fresh journal file, writes its header,
// and syncs both. capacity is the record-region byte budget; it is rounded up
// to a whole sector. The file is preallocated to header+capacity so no append
// extends it or commits filesystem metadata under an acknowledgement sync.
// An ordinary journal may explicitly GrowCapacity before a later append
// reaches its point of no return. A sealed journal is immutable.
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
		if h.SealedCapacity {
			return nil, fmt.Errorf(
				"%w: sealed recovery journal capacity is not sector aligned",
				ErrSealedCapacityMismatch,
			)
		}
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
	if h.SealedCapacity {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return nil, fmt.Errorf(
				"%w: sealed journal create requires an empty regular file",
				ErrSealedCapacityMismatch,
			)
		}
		if err := StrictlyAllocateFile(file, total); err != nil {
			return nil, fmt.Errorf("%w: strictly allocate recovery journal: %w", ErrSealedCapacityMismatch, err)
		}
	} else if err := recoveryJournalPreallocate(file, total); err != nil {
		return nil, err
	}
	rj := newRecoveryJournalManager(file, h)
	rj.headerSlot = 0
	if err := rj.writeHeader(0, rj.header); err != nil {
		return nil, err
	}
	syncFile := rj.journalDataSync
	if h.SealedCapacity {
		syncFile = strictAllocationDataSync
	}
	if err := syncFile(file); err != nil {
		return nil, err
	}
	if h.SealedCapacity {
		if err := requireExactRegularFileSize(file, total); err != nil {
			return nil, fmt.Errorf("%w: recovery journal after create sync: %w", ErrSealedCapacityMismatch, err)
		}
	}
	rj.cursor = 0
	rj.nextSequence = h.BaseSequence + 1
	return rj, nil
}

// OpenRecoveryJournal reads and validates the header of an existing journal
// file, then scans the record region to re-derive the append cursor and next
// sequence. Pairing against the store root is the caller's responsibility via
// Pair. A bare Open of a sealed journal also re-proves its self-described
// immutable allocation; GrowCapacity remains unavailable on the returned
// handle.
func OpenRecoveryJournal(file *os.File) (*RecoveryJournal, error) {
	return openRecoveryJournal(file, RecoveryJournalOpenOptions{}, true, false)
}

// InspectRecoveryJournal scans a journal through a read-only descriptor without
// changing or qualifying its physical allocation. The returned inspection does
// not expose mutable journal operations.
func InspectRecoveryJournal(file *os.File) (*RecoveryJournalInspection, error) {
	journal, err := openRecoveryJournal(file, RecoveryJournalOpenOptions{}, true, true)
	if err != nil {
		return nil, err
	}
	return &RecoveryJournalInspection{journal: journal}, nil
}

// OpenRecoveryJournalWithOptions selects and validates the authoritative
// header before proving sealed allocation and before scanning any record byte.
func OpenRecoveryJournalWithOptions(
	file *os.File,
	options RecoveryJournalOpenOptions,
) (*RecoveryJournal, error) {
	return openRecoveryJournal(file, options, false, false)
}

func openRecoveryJournal(
	file *os.File,
	options RecoveryJournalOpenOptions,
	allowSelfDescribedSealed bool,
	inspectionOnly bool,
) (*RecoveryJournal, error) {
	if file == nil {
		return nil, fmt.Errorf("%w: nil recovery journal", ErrInvalidWrite)
	}
	if options.SealedCapacityBytes > RecoveryJournalMaxCapacityBytes {
		return nil, fmt.Errorf("%w: recovery journal capacity", ErrSealedCapacityMismatch)
	}
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
	if options.Pairing != nil {
		if err := pairRecoveryJournalHeader(header, *options.Pairing); err != nil {
			return nil, err
		}
	}
	if (options.SealedCapacityBytes == 0 && header.SealedCapacity &&
		!allowSelfDescribedSealed) ||
		(options.SealedCapacityBytes != 0 &&
			(!header.SealedCapacity || header.Capacity != options.SealedCapacityBytes)) {
		return nil, fmt.Errorf(
			"%w: recovery journal expected=%d actual=%d sealed=%t",
			ErrSealedCapacityMismatch, options.SealedCapacityBytes,
			header.Capacity, header.SealedCapacity,
		)
	}
	if header.SealedCapacity {
		total := int64(recoveryJournalRegionStart) + int64(header.Capacity)
		if err := requireExactRegularFileSize(file, total); err != nil {
			return nil, fmt.Errorf("%w: recovery journal: %w", ErrSealedCapacityMismatch, err)
		}
		if !inspectionOnly {
			if err := StrictlyAllocateFile(file, total); err != nil {
				return nil, fmt.Errorf("%w: reprove recovery journal: %w", ErrSealedCapacityMismatch, err)
			}
			if err := strictAllocationDataSync(file); err != nil {
				return nil, fmt.Errorf("%w: sync recovery journal allocation: %w", ErrSealedCapacityMismatch, err)
			}
			if err := requireExactRegularFileSize(file, total); err != nil {
				return nil, fmt.Errorf("%w: recovery journal after allocation sync: %w", ErrSealedCapacityMismatch, err)
			}
		}
	}
	rj := newRecoveryJournalManager(file, header)
	rj.headerSlot = uint32(selected)
	if err := recoveryJournalScanTail(rj); err != nil {
		return nil, err
	}
	return rj, nil
}

func requireExactRegularFileSize(file *os.File, expected int64) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != expected {
		return fmt.Errorf(
			"file mode=%v size=%d want regular size=%d",
			info.Mode(), info.Size(), expected,
		)
	}
	return nil
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
	return pairRecoveryJournalHeader(rj.header, RecoveryJournalPairing{
		StoreID: storeID, JournalID: journalID, PageSize: pageSize,
		RootGeneration: rootGeneration,
	})
}

func pairRecoveryJournalHeader(
	header RecoveryJournalHeader,
	pairing RecoveryJournalPairing,
) error {
	if header.StoreID != pairing.StoreID ||
		header.JournalID != pairing.JournalID {
		return ErrRecoveryJournalIdentity
	}
	if header.PageSize != pairing.PageSize {
		return ErrRecoveryJournalGeometry
	}
	if header.BaseGeneration > pairing.RootGeneration {
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
	if err := writeRecoveryJournalFull(
		rj.writeAt, rj.scratch[:RecoveryJournalHeaderSize], off,
	); err != nil {
		return err
	}
	return nil
}

// writeRecoveryJournalFull preserves the journal's all-or-nothing cursor
// contract when a faulty or unusual positional writer reports a short write
// without an accompanying error. Callers advance in-memory header or record
// state only after this helper succeeds.
func writeRecoveryJournalFull(
	writeAt func([]byte, int64) (int, error), p []byte, off int64,
) error {
	n, err := writeAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

// scanTail walks the record region from its start, validating strict-monotonic
// sequence and framing, to re-derive the append cursor after Open. It stops only
// at a framing/checksum-invalid tail consistent with a torn or reordered append,
// leaving the cursor and next sequence positioned to overwrite it. A
// checksum-authenticated semantic error is returned and fails Open closed.
func (rj *RecoveryJournal) scanTail() error {
	rj.cursor = 0
	rj.nextSequence = rj.header.BaseSequence + 1
	// A fresh or fully recycled journal begins with a zeroed preallocated
	// region. Bad magic is already the format's authoritative truncatable-tail
	// marker, so prove that case from the first word before materializing the
	// complete bounded region. Read-only opens and clean checkpoints therefore
	// do not allocate Capacity bytes merely to rediscover an empty log.
	if _, err := readFullAt(
		rj.file, rj.scratch[:4], recoveryJournalRegionStart,
	); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(rj.scratch[:4]) != recoveryRecordMagic {
		return nil
	}
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
			if errors.Is(err, errRecoveryJournalTruncatableTail) {
				break
			}
			return err
		}
		if err := validateRecoveryRecordForFormat(
			rj.header.FormatVersion, rec,
		); err != nil {
			return err
		}
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
	if err := writeRecoveryJournalFull(
		rj.writeAt, rj.scratch[:padded], offset,
	); err != nil {
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

// GrowCapacity raises the preallocated record-region capacity to at least
// minimum. It never shrinks the journal and never changes the live record
// cursor, sequence, base generation, or record grammar.
//
// Growth is published with the same alternating-header discipline as Recycle:
// the new file tail is reserved first, then the opposite header slot names it,
// and only a successful sync commits the larger geometry to this manager. A
// crash or error before the header write leaves the previous header
// authoritative. If a header or sync error still persists the new candidate,
// that candidate is also safe because preallocation completed before it could
// name the extension. Existing readers already accept any
// sector-aligned capacity through RecoveryJournalMaxCapacityBytes, so this is
// a compatible geometry change rather than a format revision.
//
// powerSafe selects the same sync strength as Recycle. Callers grow before a
// mutation's point of no return, so allocation or header failures reject that
// mutation without consuming cursor or sequence state.
func (rj *RecoveryJournal) GrowCapacity(
	minimum uint64,
	powerSafe bool,
) error {
	if rj == nil || rj.file == nil {
		return fmt.Errorf("%w: nil recovery journal", ErrInvalidWrite)
	}
	if rj.header.SealedCapacity {
		return fmt.Errorf("%w: recovery journal is sealed", ErrSealedCapacityMismatch)
	}
	if minimum <= rj.header.Capacity {
		return nil
	}
	sector := uint64(rj.header.SectorSize)
	if sector == 0 || minimum > RecoveryJournalMaxCapacityBytes {
		return fmt.Errorf("%w: journal growth capacity", ErrInvalidWrite)
	}
	if remainder := minimum % sector; remainder != 0 {
		var ok bool
		minimum, ok = checkedSizeAdd(
			minimum,
			sector-remainder,
			RecoveryJournalMaxCapacityBytes,
		)
		if !ok {
			return fmt.Errorf("%w: journal growth capacity", ErrInvalidWrite)
		}
	}
	total := int64(recoveryJournalRegionStart) + int64(minimum)
	if err := recoveryJournalPreallocate(rj.file, total); err != nil {
		return err
	}
	next := rj.header
	next.Capacity = minimum
	next.RecycleCount++
	slot := rj.headerSlot ^ 1
	if err := rj.writeHeader(slot, next); err != nil {
		return err
	}
	sync := rj.journalSync
	if powerSafe {
		sync = rj.journalDataSync
	}
	if err := sync(rj.file); err != nil {
		return err
	}
	rj.header = next
	rj.headerSlot = slot
	return nil
}

// FitsBatch reports whether one batch record carrying entries would fit in the
// remaining preallocated capacity without a recycle.
func (rj *RecoveryJournal) FitsBatch(entries []RecoveryBatchEntry) bool {
	plan, err := rj.PrepareBatch(entries)
	if err != nil {
		return false
	}
	return rj.PreparedBatchFits(plan)
}

// PrepareBatch validates and sizes one batch without allocating. The returned
// opaque plan can be reused for the capacity decision, append, and accounting.
// AppendPreparedBatch still validates every entry while encoding: a body-size
// change fails closed before any write, while same-size content changes remain
// safe because framing and CRC are recomputed from the final bytes.
func (rj *RecoveryJournal) PrepareBatch(
	entries []RecoveryBatchEntry,
) (RecoveryBatchPlan, error) {
	plan, ok := prepareRecoveryBatchForFormat(
		rj.header.FormatVersion, rj.header.SectorSize, entries,
	)
	if !ok {
		return RecoveryBatchPlan{}, fmt.Errorf(
			"%w: batch record length", ErrInvalidWrite,
		)
	}
	return plan, nil
}

// PreparedBatchFits reports whether a prepared batch fits at the current
// cursor. A plan minted for another journal geometry fails closed.
func (rj *RecoveryJournal) PreparedBatchFits(plan RecoveryBatchPlan) bool {
	if !plan.validForFormat(
		rj.header.FormatVersion, rj.header.SectorSize, plan.entryCount,
	) {
		return false
	}
	end, ok := checkedSizeAdd(
		rj.cursor, uint64(plan.padded), ^uint64(0),
	)
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
	plan, err := rj.PrepareBatch(entries)
	if err != nil {
		return 0, err
	}
	return rj.AppendPreparedBatch(generation, entries, plan)
}

// AppendPreparedBatch appends a batch using a layout returned by PrepareBatch.
// It preserves AppendBatch's all-or-nothing framing and cursor semantics while
// eliminating repeated entry-length scans in callers that must preflight fit.
func (rj *RecoveryJournal) AppendPreparedBatch(
	generation uint64, entries []RecoveryBatchEntry,
	plan RecoveryBatchPlan,
) (uint64, error) {
	if !plan.validForFormat(
		rj.header.FormatVersion, rj.header.SectorSize, len(entries),
	) {
		return 0, fmt.Errorf("%w: batch plan", ErrInvalidWrite)
	}
	end, ok := checkedSizeAdd(
		rj.cursor, uint64(plan.padded), ^uint64(0),
	)
	if !ok || end > rj.header.Capacity {
		return 0, ErrRecoveryJournalFull
	}
	if cap(rj.scratch) < plan.padded {
		rj.scratch = make([]byte, plan.padded)
	}
	rec := RecoveryRecord{
		Sequence:   rj.nextSequence,
		Generation: generation,
		Kind:       recoveryRecordKindBatch,
		Entries:    entries,
	}
	if _, err := encodeRecoveryBatchRecordPrepared(
		rj.scratch[:plan.padded], rj.header.SectorSize, rec, plan,
	); err != nil {
		return 0, err
	}
	offset := int64(recoveryJournalRegionStart) + int64(rj.cursor)
	if err := writeRecoveryJournalFull(
		rj.writeAt, rj.scratch[:plan.padded], offset,
	); err != nil {
		return 0, err
	}
	rj.cursor = end
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
// syncs at the same strength as the checkpoint that made baseGeneration safe,
// and resets the append cursor to the region start. Stale record bytes left in
// the preallocated region are never zeroed: the new BaseSequence anchor makes
// them fail monotonic-sequence validation, so a later recovery treats the
// recycled region as empty until fresh appends overwrite it.
//
// The header rewrite plus its sync is the journal half of the checkpoint's root
// publication. A crash between the store root publication and this sync leaves
// the old header, whose records recovery re-applies idempotently through the
// ordinary mutation path onto the newer root — the replay filter skips any
// record whose generation the root already covers.
func (rj *RecoveryJournal) Recycle(
	baseGeneration uint64,
	powerSafe bool,
) error {
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
	sync := rj.journalSync
	if powerSafe {
		sync = rj.journalDataSync
	}
	if err := sync(rj.file); err != nil {
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
// at the first framing/checksum-invalid record (the torn or reordered tail) and
// returns nil. A checksum-valid semantic error is not a truncation: it returns
// ErrRecoveryJournalRecord and fails recovery closed. fn's error aborts replay.
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
			if errors.Is(err, errRecoveryJournalTruncatableTail) {
				// A framing/checksum-invalid tail is consistent with an incomplete or
				// reordered append. Everything before it was individually sync-fenced.
				break
			}
			return err
		}
		if err := validateRecoveryRecordForFormat(
			rj.header.FormatVersion, rec,
		); err != nil {
			return err
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
