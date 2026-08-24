package storeio

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

// The transaction decision log (txn.vtm) is a database-scoped sidecar that
// records cross-collection commit decisions and participant retirements. It is
// a sibling of the recovery journal: two alternating, independently checksummed
// 512-byte header sectors, a fixed-capacity preallocated record region,
// positional sector-aligned appends, strict sequence validation, and torn-tail
// truncation at the first invalid record. Recovery selects the valid header
// with the greater recycle count.
//
// Creation is fenced: CreateTxnMarker returns success only after file creation,
// both header sectors, region preallocation, file sync, and parent-directory
// fsync have all completed. A failure anywhere in that sequence is a typed
// error and leaves no usable marker — reopen finds the path absent, a
// valid-empty log, or ErrTxnMarkerNoValidHeader. Policy over that residue
// (fresh creation versus fail-closed) belongs to the durable layer, not this package.
//
// Recycle legality ("no participant journal still holds current-epoch kind-4
// records") is also the caller's rule; this package only provides Recycle.
const (
	// TxnMarkerHeaderSize is one damage-granule-aligned header sector.
	TxnMarkerHeaderSize = 512
	// txnMarkerHeaderSlots is the number of alternating header sectors.
	txnMarkerHeaderSlots = 2
	// txnMarkerRegionStart is the byte offset of the record region.
	txnMarkerRegionStart = TxnMarkerHeaderSize * txnMarkerHeaderSlots
	// TxnMarkerMinSectorSize is the append/damage granule.
	TxnMarkerMinSectorSize = 512
	// TxnMarkerMaxCapacityBytes is the independent decision-log clamp. It does
	// not inherit recovery-journal envelope overhead: decision records have their
	// own fixed participant ceiling, and a checksummed hostile header must not
	// gain allocation authority when the recovery-journal bound changes.
	TxnMarkerMaxCapacityBytes = uint64(16) << 20
	// txnMarkerDefaultCapacityBytes is the create-time default record region.
	txnMarkerDefaultCapacityBytes = uint64(1) << 20

	// TxnMarkerFormat is the sole admitted decision-log grammar.
	TxnMarkerFormat = uint32(0)

	// txnMarkerFlagSealedCapacity marks Capacity as an immutable physical
	// certificate. Every other bit in the current reserved flags word is invalid.
	txnMarkerFlagSealedCapacity = uint32(1)

	txnMarkerMagic       = "VTXNMRK\x00"
	txnMarkerRecordMagic = uint32(0x524d5456) // "VTMR", little-endian.

	// TxnMarkerRecordKindDecision is a committed multi-collection transaction.
	TxnMarkerRecordKindDecision = uint16(1)
	// TxnMarkerRecordKindRetirement covers a StoreID dropped after its
	// conditional records were folded past.
	TxnMarkerRecordKindRetirement = uint16(2)

	// TxnMarkerRecordPrefixSize is the fixed framing before kind-specific body
	// bytes that follow the common sequence field.
	TxnMarkerRecordPrefixSize = 32
	// TxnMarkerRecordTrailerSize is the CRC32C and its complement.
	TxnMarkerRecordTrailerSize = 8
	// TxnParticipantSize is the fixed on-disk participant tuple.
	TxnParticipantSize = 40

	// TxnMarkerMaxParticipants is the hard encode-time participant ceiling for
	// one decision record. Durable TxnLimits.MaxCollections is capped by this.
	TxnMarkerMaxParticipants = 64
)

var (
	// ErrTxnMarkerCorrupt reports a header whose framing, checksum, or
	// geometry is invalid.
	ErrTxnMarkerCorrupt = errors.New("vibedb: corrupt transaction decision log header")
	// ErrTxnMarkerNoValidHeader reports a file whose header slots are all
	// uninitialized or checksum-invalid. A checksum-authenticated semantic error
	// is ErrTxnMarkerCorrupt instead. The durable layer treats no-valid-header as
	// removable creation residue when no journal holds conditional records, and
	// as fail-closed tampering otherwise — that policy is not this package's.
	ErrTxnMarkerNoValidHeader = errors.New("vibedb: transaction decision log has no valid header")
	// ErrTxnMarkerFull reports that the next record does not fit the
	// preallocated capacity.
	ErrTxnMarkerFull = errors.New("vibedb: transaction decision log is full")
	// ErrTxnMarkerRecord reports a record that failed framing, checksum,
	// monotonic-sequence, or semantic validation. Framing/checksum-invalid
	// tails truncate the scan; checksum-authenticated semantic failures fail
	// Open closed.
	ErrTxnMarkerRecord = errors.New("vibedb: invalid transaction decision log record")
	// errTxnMarkerTruncatableTail distinguishes damage consistent with an
	// incomplete append from a checksum-authenticated semantic error.
	errTxnMarkerTruncatableTail = errors.New("vibedb: truncatable transaction decision log tail")
)

// txnMarkerPreallocate is a package seam so crash tests can induce ENOSPC on
// the mint's up-front preallocation. Production keeps the platform preallocator.
var txnMarkerPreallocate = preallocateRecoveryJournal

// txnMarkerParentDirSync is a package seam for the mint's parent-directory
// fsync fence (L2 / W7). Production Syncs the directory through the same
// pinned root used to create the marker.
var txnMarkerParentDirSync = syncTxnMarkerParentDir

// txnMarkerCreateFileSync is a package seam for the mint's file sync. Production
// uses the power-safe barrier: the mint fence is a durability boundary.
var txnMarkerCreateFileSync = dataSync

func txnMarkerTailError(reason string) error {
	return fmt.Errorf(
		"%w: %w: %s",
		ErrTxnMarkerRecord, errTxnMarkerTruncatableTail, reason,
	)
}

func txnMarkerSemanticError(reason string) error {
	return fmt.Errorf("%w: semantic: %s", ErrTxnMarkerRecord, reason)
}

// TxnMarkerHeader is the pointer-free identity and geometry of one decision
// log. Format gates the record grammar; BaseSequence anchors monotonic
// sequence validation so stale bytes left after a recycle can never be
// mistaken for live records.
type TxnMarkerHeader struct {
	Format       uint32
	MarkerID     [16]byte
	Epoch        uint64
	BaseSequence uint64
	Capacity     uint64
	// SealedCapacity requires an exact-size, strictly allocated marker file.
	SealedCapacity bool
	// RecycleCount is strictly monotonic across recycles. Open selects the
	// semantically valid header slot with the highest count. A checksum-invalid
	// torn publication may fall back; authenticated semantic damage fails closed.
	RecycleCount uint64
}

// TxnParticipant is one collection named by a committed decision.
type TxnParticipant struct {
	StoreID            [16]byte
	JournalID          [16]byte
	PreparedGeneration uint64
}

// TxnMarkerOptions configures CreateTxnMarker / OpenTxnMarker. A zero Capacity
// selects the package default at unsealed create time. A non-zero open Capacity
// is an exact assertion. SealedCapacity requires a non-zero, sector-aligned
// Capacity and makes that geometry immutable.
type TxnMarkerOptions struct {
	Capacity       uint64
	SealedCapacity bool
}

// TxnMarkerRecoveryAnchor identifies a clean replacement file for a recovery
// authority that already authenticated the logical marker lineage. Epoch must
// advance that authority's retained epoch. BaseSequence anchors the empty
// replacement record region after its certified prefix.
type TxnMarkerRecoveryAnchor struct {
	MarkerID     [16]byte
	Epoch        uint64
	BaseSequence uint64
}

// TxnMarker is the single-writer file-backed manager for txn.vtm. It is not
// safe for concurrent use.
type TxnMarker struct {
	file *os.File
	root *os.Root
	path string
	// sourceDir is canonicalized once at open/create for diagnostics. The
	// physical directory identity is retained separately so decisions cannot be
	// paired with a collection through a retargeted path.
	sourceDir     string
	sourceDirInfo os.FileInfo
	header        TxnMarkerHeader
	// cursor is the in-region byte offset of the next append.
	cursor uint64
	// nextSequence is the DCSN the next appended record will carry.
	nextSequence uint64
	// lastTxnID is the greatest decision TxnID in the current epoch. Decision
	// records are strictly increasing in DCSN order; retirement records do not
	// reset it.
	lastTxnID uint64
	retired   map[[16]byte]struct{}
	// headerSlot is the alternating slot the live header occupies.
	headerSlot uint32
	scratch    []byte
	// markerSync and writeAt are injected so a fault seam can wrap the real
	// barriers and appends; production wires the platform helpers.
	markerSync func(*os.File) error
	writeAt    func(p []byte, off int64) (int, error)
}

// TxnMarkerInspection is a read-only offline scan. It never proves physical
// allocation and exposes no append, sync, recycle, or removal method.
type TxnMarkerInspection struct {
	marker *TxnMarker
}

func (i *TxnMarkerInspection) Header() TxnMarkerHeader {
	if i == nil || i.marker == nil {
		return TxnMarkerHeader{}
	}
	return i.marker.Header()
}

func (i *TxnMarkerInspection) Cursor() uint64 {
	if i == nil || i.marker == nil {
		return 0
	}
	return i.marker.Cursor()
}

func (i *TxnMarkerInspection) Close() error {
	if i == nil || i.marker == nil {
		return nil
	}
	err := i.marker.Close()
	i.marker = nil
	return err
}

// TxnDecisions is the scan of one open decision log: committed participant
// sets keyed by TxnID within the selected header's epoch, the retired StoreID
// set, and the high-water TxnID / DCSN for counter seeding.
type TxnDecisions struct {
	sourceDir     string
	sourceDirInfo os.FileInfo
	markerID      [16]byte
	epoch         uint64
	decisions     map[uint64][]TxnParticipant
	decisionIDs   []uint64
	retired       map[[16]byte]struct{}
	maxTxnID      uint64
	maxDCSN       uint64
}

// RangeDecisions visits committed decisions in authenticated record order.
// Each participant slice is a defensive copy. Returning false stops iteration.
func (d *TxnDecisions) RangeDecisions(
	visit func(txnID uint64, participants []TxnParticipant) bool,
) {
	if d == nil || visit == nil {
		return
	}
	for _, txnID := range d.decisionIDs {
		participants := append([]TxnParticipant(nil), d.decisions[txnID]...)
		if !visit(txnID, participants) {
			return
		}
	}
}

// SourceDir returns the canonical directory name captured when the decision
// log was opened. It is diagnostic metadata; collection pairing is fenced by
// the retained physical directory identity in MatchesFileDirectory.
func (d *TxnDecisions) SourceDir() string {
	if d == nil {
		return ""
	}
	return d.sourceDir
}

// MatchesFileDirectory proves that file is still the exact entry reached
// through a pinned handle to the same physical directory as this decision
// log. It rejects path retargeting between the caller's open and this check.
func (d *TxnDecisions) MatchesFileDirectory(file *os.File) (bool, error) {
	if d == nil || d.sourceDirInfo == nil || file == nil {
		return false, ErrInvalidWrite
	}
	return matchesTxnMarkerDirectory(d.sourceDirInfo, file)
}

func matchesTxnMarkerDirectory(
	sourceDirInfo os.FileInfo, file *os.File,
) (bool, error) {
	if sourceDirInfo == nil || !sourceDirInfo.IsDir() || file == nil {
		return false, ErrInvalidWrite
	}
	// Open the live parent path directly. OpenRoot pins the directory reached
	// through the caller's current namespace, and the SameFile check below
	// proves that pinned handle is the marker's physical directory. Resolving
	// every path component with EvalSymlinks first adds no identity guarantee:
	// the directory descriptor, not its spelling, is the proof. Avoiding that
	// redundant walk also keeps this per-commit fence allocation-light.
	root, err := os.OpenRoot(filepath.Dir(file.Name()))
	if err != nil {
		return false, err
	}
	defer root.Close()
	dirInfo, err := root.Stat(".")
	if err != nil {
		return false, err
	}
	if !os.SameFile(sourceDirInfo, dirInfo) {
		return false, nil
	}
	entryInfo, err := root.Lstat(filepath.Base(file.Name()))
	if err != nil {
		return false, err
	}
	if !entryInfo.Mode().IsRegular() {
		return false, nil
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return false, err
	}
	return os.SameFile(fileInfo, entryInfo), nil
}

// FileMatchesDirectory proves that file is the exact regular, non-symlink
// entry reached through the same physical directory represented by directory.
// The FileInfo must come from a pinned directory handle's Stat(".").
func FileMatchesDirectory(
	directory os.FileInfo, file *os.File,
) (bool, error) {
	return matchesTxnMarkerDirectory(directory, file)
}

// MarkerID returns the decision-log identity selected at Open.
func (d *TxnDecisions) MarkerID() [16]byte {
	if d == nil {
		return [16]byte{}
	}
	return d.markerID
}

// Epoch returns the decision-log epoch selected at Open.
func (d *TxnDecisions) Epoch() uint64 {
	if d == nil {
		return 0
	}
	return d.epoch
}

// MaxTxnID returns the greatest TxnID observed in a decision record, or zero
// when the log holds none.
func (d *TxnDecisions) MaxTxnID() uint64 {
	if d == nil {
		return 0
	}
	return d.maxTxnID
}

// MaxDCSN returns the greatest durable commit sequence observed, or zero when
// the log holds no records.
func (d *TxnDecisions) MaxDCSN() uint64 {
	if d == nil {
		return 0
	}
	return d.maxDCSN
}

// Lookup reports the committed participant set for (markerID, epoch, txnID).
// A mismatched marker identity or epoch returns false — the record is not
// authoritative for that key.
func (d *TxnDecisions) Lookup(
	markerID [16]byte, epoch, txnID uint64,
) ([]TxnParticipant, bool) {
	if d == nil || d.decisions == nil ||
		markerID != d.markerID || epoch != d.epoch {
		return nil, false
	}
	participants, ok := d.decisions[txnID]
	if !ok {
		return nil, false
	}
	out := make([]TxnParticipant, len(participants))
	copy(out, participants)
	return out, true
}

// Retired reports whether a participant-retired record covers storeID.
func (d *TxnDecisions) Retired(storeID [16]byte) bool {
	if d == nil || d.retired == nil {
		return false
	}
	_, ok := d.retired[storeID]
	return ok
}

// RetirementCount reports how many participant-retirement records survived
// the selected marker epoch. Fixed-membership checkpoint groups reject any
// such record; ordinary transaction recovery retains its existing semantics.
func (d *TxnDecisions) RetirementCount() int {
	if d == nil {
		return 0
	}
	return len(d.retired)
}

func validateTxnMarkerHeader(h TxnMarkerHeader) error {
	if h.Format != TxnMarkerFormat {
		return fmt.Errorf("%w: format", ErrTxnMarkerCorrupt)
	}
	if h.MarkerID == ([16]byte{}) {
		return fmt.Errorf("%w: zero marker identity", ErrTxnMarkerCorrupt)
	}
	if h.Epoch == 0 {
		return fmt.Errorf("%w: zero epoch", ErrTxnMarkerCorrupt)
	}
	if h.Capacity == 0 ||
		h.Capacity > TxnMarkerMaxCapacityBytes ||
		h.Capacity%uint64(TxnMarkerMinSectorSize) != 0 {
		return fmt.Errorf("%w: capacity", ErrTxnMarkerCorrupt)
	}
	if h.RecycleCount == 0 {
		return fmt.Errorf("%w: recycle count", ErrTxnMarkerCorrupt)
	}
	return nil
}

func checkedTxnMarkerPadRaw(raw uint64) (int, bool) {
	return checkedRecoveryPadRaw(TxnMarkerMinSectorSize, raw)
}

func checkedTxnDecisionPaddedSize(participantCount int) (int, bool) {
	if participantCount <= 0 || participantCount > TxnMarkerMaxParticipants {
		return 0, false
	}
	body, ok := checkedSizeMul(
		uint64(participantCount), TxnParticipantSize, uint64(maxIntValue),
	)
	if !ok {
		return 0, false
	}
	raw := uint64(TxnMarkerRecordPrefixSize + TxnMarkerRecordTrailerSize)
	raw, ok = checkedSizeAdd(raw, body, uint64(maxIntValue))
	if !ok {
		return 0, false
	}
	return checkedTxnMarkerPadRaw(raw)
}

// TxnDecisionRecordPaddedSize returns the exact current record-region charge
// for one transaction decision. A false result means the participant count is
// outside the current grammar.
func TxnDecisionRecordPaddedSize(participantCount int) (int, bool) {
	return checkedTxnDecisionPaddedSize(participantCount)
}

func checkedTxnRetirementPaddedSize() (int, bool) {
	raw := uint64(TxnMarkerRecordPrefixSize + TxnMarkerRecordTrailerSize)
	return checkedTxnMarkerPadRaw(raw)
}

// EncodeTxnMarkerHeader writes one sealed header sector into dst.
func EncodeTxnMarkerHeader(dst []byte, h TxnMarkerHeader) ([]byte, error) {
	if len(dst) < TxnMarkerHeaderSize {
		return nil, fmt.Errorf("%w: header buffer has %d bytes", ErrInvalidWrite, len(dst))
	}
	if err := validateTxnMarkerHeader(h); err != nil {
		return nil, err
	}
	sector := dst[:TxnMarkerHeaderSize]
	clear(sector)
	copy(sector[0:8], txnMarkerMagic)
	binary.LittleEndian.PutUint32(sector[8:12], h.Format)
	binary.LittleEndian.PutUint32(sector[12:16], TxnMarkerHeaderSize)
	copy(sector[16:32], h.MarkerID[:])
	binary.LittleEndian.PutUint64(sector[32:40], h.Epoch)
	binary.LittleEndian.PutUint64(sector[40:48], h.BaseSequence)
	binary.LittleEndian.PutUint64(sector[48:56], h.Capacity)
	binary.LittleEndian.PutUint64(sector[56:64], h.RecycleCount)
	if h.SealedCapacity {
		binary.LittleEndian.PutUint32(sector[64:68], txnMarkerFlagSealedCapacity)
	}
	checksum := PageChecksum(sector[:TxnMarkerHeaderSize-8])
	binary.LittleEndian.PutUint32(sector[TxnMarkerHeaderSize-8:TxnMarkerHeaderSize-4], checksum)
	binary.LittleEndian.PutUint32(sector[TxnMarkerHeaderSize-4:], ^checksum)
	return sector, nil
}

// DecodeTxnMarkerHeader validates one header sector.
func DecodeTxnMarkerHeader(src []byte) (TxnMarkerHeader, error) {
	if len(src) < TxnMarkerHeaderSize {
		return TxnMarkerHeader{}, fmt.Errorf("%w: short header", ErrTxnMarkerCorrupt)
	}
	src = src[:TxnMarkerHeaderSize]
	if string(src[0:8]) != txnMarkerMagic {
		return TxnMarkerHeader{}, fmt.Errorf("%w: magic", ErrTxnMarkerCorrupt)
	}
	checksum := binary.LittleEndian.Uint32(src[TxnMarkerHeaderSize-8 : TxnMarkerHeaderSize-4])
	if binary.LittleEndian.Uint32(src[TxnMarkerHeaderSize-4:]) != ^checksum ||
		PageChecksum(src[:TxnMarkerHeaderSize-8]) != checksum {
		return TxnMarkerHeader{}, fmt.Errorf("%w: checksum", ErrTxnMarkerCorrupt)
	}
	if binary.LittleEndian.Uint32(src[8:12]) != TxnMarkerFormat ||
		binary.LittleEndian.Uint32(src[12:16]) != TxnMarkerHeaderSize {
		return TxnMarkerHeader{}, fmt.Errorf("%w: format or header size", ErrTxnMarkerCorrupt)
	}
	h := TxnMarkerHeader{Format: TxnMarkerFormat}
	copy(h.MarkerID[:], src[16:32])
	h.Epoch = binary.LittleEndian.Uint64(src[32:40])
	h.BaseSequence = binary.LittleEndian.Uint64(src[40:48])
	h.Capacity = binary.LittleEndian.Uint64(src[48:56])
	h.RecycleCount = binary.LittleEndian.Uint64(src[56:64])
	flags := binary.LittleEndian.Uint32(src[64:68])
	if flags&^txnMarkerFlagSealedCapacity != 0 {
		return TxnMarkerHeader{}, fmt.Errorf("%w: header flags", ErrTxnMarkerCorrupt)
	}
	if !allZero(src[68 : TxnMarkerHeaderSize-8]) {
		return TxnMarkerHeader{}, fmt.Errorf("%w: header reserved bytes", ErrTxnMarkerCorrupt)
	}
	h.SealedCapacity = flags&txnMarkerFlagSealedCapacity != 0
	if err := validateTxnMarkerHeader(h); err != nil {
		return TxnMarkerHeader{}, err
	}
	return h, nil
}

// txnMarkerHeaderAuthenticated is the marker counterpart to
// recoveryJournalHeaderAuthenticated. A checksum-valid slot is an
// authoritative semantic statement even when its fields are invalid, so
// selection must fail closed instead of resurrecting an older epoch.
func txnMarkerHeaderAuthenticated(src []byte) bool {
	if len(src) < TxnMarkerHeaderSize {
		return false
	}
	src = src[:TxnMarkerHeaderSize]
	checksum := binary.LittleEndian.Uint32(
		src[TxnMarkerHeaderSize-8 : TxnMarkerHeaderSize-4],
	)
	return binary.LittleEndian.Uint32(src[TxnMarkerHeaderSize-4:]) == ^checksum &&
		PageChecksum(src[:TxnMarkerHeaderSize-8]) == checksum
}

func validateTxnParticipants(participants []TxnParticipant) error {
	if len(participants) == 0 || len(participants) > TxnMarkerMaxParticipants {
		return fmt.Errorf("%w: participant count", ErrInvalidWrite)
	}
	for i := range participants {
		if participants[i].StoreID == ([16]byte{}) ||
			participants[i].JournalID == ([16]byte{}) ||
			participants[i].PreparedGeneration == 0 {
			return fmt.Errorf("%w: participant identity or generation", ErrInvalidWrite)
		}
		for previous := 0; previous < i; previous++ {
			if participants[previous].StoreID == participants[i].StoreID ||
				participants[previous].JournalID == participants[i].JournalID {
				return fmt.Errorf("%w: duplicate participant identity", ErrInvalidWrite)
			}
		}
	}
	return nil
}

func encodeTxnDecisionRecord(
	dst []byte, sequence, txnID uint64, participants []TxnParticipant,
) (int, error) {
	if sequence == 0 || txnID == 0 {
		return 0, fmt.Errorf("%w: zero sequence or txn id", ErrInvalidWrite)
	}
	if err := validateTxnParticipants(participants); err != nil {
		return 0, err
	}
	padded, ok := checkedTxnDecisionPaddedSize(len(participants))
	if !ok {
		return 0, fmt.Errorf("%w: decision record length", ErrInvalidWrite)
	}
	if len(dst) < padded {
		return 0, fmt.Errorf(
			"%w: decision buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), padded,
		)
	}
	buf := dst[:padded]
	clear(buf)
	binary.LittleEndian.PutUint32(buf[0:4], txnMarkerRecordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], TxnMarkerRecordKindDecision)
	binary.LittleEndian.PutUint64(buf[8:16], sequence)
	binary.LittleEndian.PutUint64(buf[16:24], txnID)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(participants)))
	cursor := TxnMarkerRecordPrefixSize
	for i := range participants {
		copy(buf[cursor:cursor+16], participants[i].StoreID[:])
		copy(buf[cursor+16:cursor+32], participants[i].JournalID[:])
		binary.LittleEndian.PutUint64(
			buf[cursor+32:cursor+40], participants[i].PreparedGeneration,
		)
		cursor += TxnParticipantSize
	}
	checksum := PageChecksum(buf[:cursor])
	binary.LittleEndian.PutUint32(buf[cursor:cursor+4], checksum)
	binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], ^checksum)
	return padded, nil
}

func encodeTxnRetirementRecord(
	dst []byte, sequence uint64, storeID [16]byte,
) (int, error) {
	if sequence == 0 {
		return 0, fmt.Errorf("%w: zero sequence", ErrInvalidWrite)
	}
	if storeID == ([16]byte{}) {
		return 0, fmt.Errorf("%w: zero store identity", ErrInvalidWrite)
	}
	padded, ok := checkedTxnRetirementPaddedSize()
	if !ok {
		return 0, fmt.Errorf("%w: retirement record length", ErrInvalidWrite)
	}
	if len(dst) < padded {
		return 0, fmt.Errorf(
			"%w: retirement buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), padded,
		)
	}
	buf := dst[:padded]
	clear(buf)
	binary.LittleEndian.PutUint32(buf[0:4], txnMarkerRecordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], TxnMarkerRecordKindRetirement)
	binary.LittleEndian.PutUint64(buf[8:16], sequence)
	copy(buf[16:32], storeID[:])
	checksum := PageChecksum(buf[:TxnMarkerRecordPrefixSize])
	binary.LittleEndian.PutUint32(
		buf[TxnMarkerRecordPrefixSize:TxnMarkerRecordPrefixSize+4], checksum,
	)
	binary.LittleEndian.PutUint32(
		buf[TxnMarkerRecordPrefixSize+4:TxnMarkerRecordPrefixSize+8], ^checksum,
	)
	return padded, nil
}

// txnMarkerRecord is one decoded decision-log record used while scanning.
type txnMarkerRecord struct {
	Sequence     uint64
	Kind         uint16
	TxnID        uint64
	Participants []TxnParticipant
	StoreID      [16]byte
}

func decodeTxnMarkerRecord(
	src []byte, expectedSequence uint64,
) (txnMarkerRecord, int, error) {
	if expectedSequence == 0 {
		return txnMarkerRecord{}, 0, txnMarkerSemanticError(
			"record sequence space exhausted",
		)
	}
	if len(src) < TxnMarkerRecordPrefixSize+TxnMarkerRecordTrailerSize {
		return txnMarkerRecord{}, 0, txnMarkerTailError("short record")
	}
	magic := binary.LittleEndian.Uint32(src[0:4])
	if magic != txnMarkerRecordMagic {
		if txnMarkerRecordHasAuthenticatedKnownLayout(src) {
			return txnMarkerRecord{}, 0, txnMarkerSemanticError(
				"checksum-valid non-current record domain",
			)
		}
		return txnMarkerRecord{}, 0, txnMarkerTailError("magic")
	}
	kind := binary.LittleEndian.Uint16(src[4:6])
	sequence := binary.LittleEndian.Uint64(src[8:16])
	if sequence != expectedSequence && sequence != 0 {
		return txnMarkerRecord{}, 0, txnMarkerTailError("sequence")
	}
	switch kind {
	case TxnMarkerRecordKindDecision:
		rec, padded, err := decodeTxnDecisionRecord(src, sequence)
		if err == nil {
			return rec, padded, nil
		}
		if errors.Is(err, errTxnMarkerTruncatableTail) &&
			txnMarkerRecordHasAuthenticatedRetirementLayout(src) {
			return txnMarkerRecord{}, 0, txnMarkerSemanticError(
				"decision kind authenticates retirement layout",
			)
		}
		return txnMarkerRecord{}, 0, err
	case TxnMarkerRecordKindRetirement:
		rec, padded, err := decodeTxnRetirementRecord(src, sequence)
		if err == nil {
			return rec, padded, nil
		}
		if errors.Is(err, errTxnMarkerTruncatableTail) &&
			txnMarkerRecordHasAuthenticatedDecisionLayout(src) {
			return txnMarkerRecord{}, 0, txnMarkerSemanticError(
				"retirement kind authenticates decision layout",
			)
		}
		return txnMarkerRecord{}, 0, err
	default:
		if txnMarkerRecordHasAuthenticatedKnownLayout(src) {
			return txnMarkerRecord{}, 0, txnMarkerSemanticError(
				"checksum-valid unknown record kind",
			)
		}
		return txnMarkerRecord{}, 0, txnMarkerTailError("kind")
	}
}

// txnMarkerRecordHasAuthenticatedKnownLayout distinguishes a torn/stale tail
// with an arbitrary kind word from a complete checksummed current record whose
// kind was corrupted or forged. The latter is semantic corruption: silently
// truncating it could omit a durable commit decision. Both admitted record
// layouts are bounded and share the current prefix/trailer checksum grammar.
func txnMarkerRecordHasAuthenticatedKnownLayout(src []byte) bool {
	if len(src) < TxnMarkerRecordPrefixSize+TxnMarkerRecordTrailerSize {
		return false
	}
	return txnMarkerRecordHasAuthenticatedRetirementLayout(src) ||
		txnMarkerRecordHasAuthenticatedDecisionLayout(src)
}

func txnMarkerChecksumValidAt(src []byte, bodyEnd uint64) bool {
	if bodyEnd+TxnMarkerRecordTrailerSize > uint64(len(src)) {
		return false
	}
	checksum := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	return binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) == ^checksum &&
		PageChecksum(src[:bodyEnd]) == checksum
}

func txnMarkerRecordHasAuthenticatedRetirementLayout(src []byte) bool {
	return len(src) >= TxnMarkerRecordPrefixSize+TxnMarkerRecordTrailerSize &&
		txnMarkerChecksumValidAt(src, TxnMarkerRecordPrefixSize)
}

func txnMarkerRecordHasAuthenticatedDecisionLayout(src []byte) bool {
	if len(src) < TxnMarkerRecordPrefixSize+TxnMarkerRecordTrailerSize {
		return false
	}
	participantCount := binary.LittleEndian.Uint32(src[24:28])
	if participantCount == 0 {
		return false
	}
	body, ok := checkedSizeMul(
		uint64(participantCount), TxnParticipantSize, uint64(maxIntValue),
	)
	if !ok {
		return false
	}
	bodyEnd, ok := checkedSizeAdd(
		TxnMarkerRecordPrefixSize, body, uint64(maxIntValue),
	)
	return ok && txnMarkerChecksumValidAt(src, bodyEnd)
}

func decodeTxnDecisionRecord(
	src []byte, sequence uint64,
) (txnMarkerRecord, int, error) {
	txnID := binary.LittleEndian.Uint64(src[16:24])
	participantCount := binary.LittleEndian.Uint32(src[24:28])
	body, ok := checkedSizeMul(
		uint64(participantCount), TxnParticipantSize, uint64(maxIntValue),
	)
	if !ok {
		return txnMarkerRecord{}, 0, txnMarkerTailError("decision body")
	}
	crcEnd, ok := checkedSizeAdd(
		uint64(TxnMarkerRecordPrefixSize),
		body+TxnMarkerRecordTrailerSize,
		uint64(maxIntValue),
	)
	if !ok || uint64(len(src)) < crcEnd {
		return txnMarkerRecord{}, 0, txnMarkerTailError("short decision")
	}
	bodyEnd := TxnMarkerRecordPrefixSize + int(body)
	checksum := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	if binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) != ^checksum ||
		PageChecksum(src[:bodyEnd]) != checksum {
		return txnMarkerRecord{}, 0, txnMarkerTailError("checksum")
	}
	if sequence == 0 || txnID == 0 || participantCount == 0 ||
		participantCount > TxnMarkerMaxParticipants ||
		binary.LittleEndian.Uint16(src[6:8]) != 0 ||
		binary.LittleEndian.Uint32(src[28:32]) != 0 {
		return txnMarkerRecord{}, 0, txnMarkerSemanticError(
			"checksum-valid decision framing",
		)
	}
	padded, ok := checkedTxnMarkerPadRaw(crcEnd)
	if !ok || len(src) < padded {
		return txnMarkerRecord{}, 0, txnMarkerTailError("padded length")
	}
	participants := make([]TxnParticipant, participantCount)
	cursor := TxnMarkerRecordPrefixSize
	for i := range participants {
		copy(participants[i].StoreID[:], src[cursor:cursor+16])
		copy(participants[i].JournalID[:], src[cursor+16:cursor+32])
		participants[i].PreparedGeneration = binary.LittleEndian.Uint64(
			src[cursor+32 : cursor+40],
		)
		if participants[i].StoreID == ([16]byte{}) ||
			participants[i].JournalID == ([16]byte{}) ||
			participants[i].PreparedGeneration == 0 {
			return txnMarkerRecord{}, 0, txnMarkerSemanticError(
				"checksum-valid participant identity or generation",
			)
		}
		cursor += TxnParticipantSize
	}
	if err := validateTxnParticipants(participants); err != nil {
		return txnMarkerRecord{}, 0, txnMarkerSemanticError(
			"checksum-valid duplicate participant identity",
		)
	}
	return txnMarkerRecord{
		Sequence:     sequence,
		Kind:         TxnMarkerRecordKindDecision,
		TxnID:        txnID,
		Participants: participants,
	}, padded, nil
}

func decodeTxnRetirementRecord(
	src []byte, sequence uint64,
) (txnMarkerRecord, int, error) {
	var storeID [16]byte
	copy(storeID[:], src[16:32])
	checksum := binary.LittleEndian.Uint32(
		src[TxnMarkerRecordPrefixSize : TxnMarkerRecordPrefixSize+4],
	)
	if binary.LittleEndian.Uint32(
		src[TxnMarkerRecordPrefixSize+4:TxnMarkerRecordPrefixSize+8],
	) != ^checksum ||
		PageChecksum(src[:TxnMarkerRecordPrefixSize]) != checksum {
		return txnMarkerRecord{}, 0, txnMarkerTailError("checksum")
	}
	if sequence == 0 || binary.LittleEndian.Uint16(src[6:8]) != 0 {
		return txnMarkerRecord{}, 0, txnMarkerSemanticError(
			"checksum-valid retirement framing",
		)
	}
	padded, ok := checkedTxnRetirementPaddedSize()
	if !ok || len(src) < padded {
		return txnMarkerRecord{}, 0, txnMarkerTailError("padded length")
	}
	if storeID == ([16]byte{}) {
		return txnMarkerRecord{}, 0, txnMarkerSemanticError(
			"checksum-valid zero store identity",
		)
	}
	return txnMarkerRecord{
		Sequence: sequence,
		Kind:     TxnMarkerRecordKindRetirement,
		StoreID:  storeID,
	}, padded, nil
}

// CreateTxnMarker mints a fresh decision log at path and returns it open only
// after the complete creation fence is durable.
func CreateTxnMarker(path string, opts TxnMarkerOptions) (*TxnMarker, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty decision log path", ErrInvalidWrite)
	}
	sourceDir, err := canonicalTxnMarkerDir(path)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(sourceDir)
	if err != nil {
		return nil, err
	}
	return createTxnMarkerInRoot(
		root, path, filepath.Base(path), sourceDir, opts, nil,
	)
}

// CreateTxnMarkerAt mints name through a child handle of root. The returned
// marker owns that child handle; the caller retains ownership of root.
func CreateTxnMarkerAt(
	root *os.Root, name string, opts TxnMarkerOptions,
) (*TxnMarker, error) {
	if root == nil || !validTxnMarkerName(name) {
		return nil, fmt.Errorf("%w: invalid decision log root or name", ErrInvalidWrite)
	}
	child, err := root.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	sourceDir := filepath.Clean(root.Name())
	return createTxnMarkerInRoot(
		child, filepath.Join(sourceDir, name), name, sourceDir, opts, nil,
	)
}

// CreateTxnMarkerAtRecoveryAnchor creates a clean marker through the ordinary
// exclusive-create and durability fence while retaining an authenticated
// logical marker identity. The caller must prove the anchor from an external
// recovery authority before this call.
func CreateTxnMarkerAtRecoveryAnchor(
	root *os.Root,
	name string,
	opts TxnMarkerOptions,
	anchor TxnMarkerRecoveryAnchor,
) (*TxnMarker, error) {
	if root == nil || !validTxnMarkerName(name) {
		return nil, fmt.Errorf("%w: invalid decision log root or name", ErrInvalidWrite)
	}
	if anchor.Epoch <= 1 {
		return nil, fmt.Errorf(
			"%w: recovery anchor epoch must exceed one", ErrInvalidWrite,
		)
	}
	child, err := root.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	sourceDir := filepath.Clean(root.Name())
	return createTxnMarkerInRoot(
		child, filepath.Join(sourceDir, name), name, sourceDir, opts, &anchor,
	)
}

func createTxnMarkerInRoot(
	root *os.Root,
	path string,
	name string,
	sourceDir string,
	opts TxnMarkerOptions,
	recoveryAnchor *TxnMarkerRecoveryAnchor,
) (*TxnMarker, error) {
	cleanupRoot := true
	defer func() {
		if cleanupRoot {
			_ = root.Close()
		}
	}()
	capacity, err := normalizeTxnMarkerOptionCapacity(opts, true)
	if err != nil {
		return nil, err
	}
	var header TxnMarkerHeader
	if recoveryAnchor != nil {
		header = TxnMarkerHeader{
			Format: TxnMarkerFormat, MarkerID: recoveryAnchor.MarkerID,
			Epoch: recoveryAnchor.Epoch, BaseSequence: recoveryAnchor.BaseSequence,
			Capacity: capacity, SealedCapacity: opts.SealedCapacity, RecycleCount: 1,
		}
		if err := validateTxnMarkerHeader(header); err != nil {
			return nil, err
		}
	}

	file, sourceDirInfo, err := openTxnMarkerEntry(
		root, name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
		}
	}()
	if recoveryAnchor == nil {
		if _, err := rand.Read(header.MarkerID[:]); err != nil {
			return nil, err
		}
		header.Format = TxnMarkerFormat
		header.Epoch = 1
		header.Capacity = capacity
		header.SealedCapacity = opts.SealedCapacity
		header.RecycleCount = 1
		if err := validateTxnMarkerHeader(header); err != nil {
			return nil, err
		}
	}

	total := int64(txnMarkerRegionStart) + int64(capacity)
	if opts.SealedCapacity {
		info, statErr := file.Stat()
		if statErr != nil {
			return nil, statErr
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return nil, fmt.Errorf(
				"%w: sealed transaction marker create requires an empty regular file",
				ErrSealedCapacityMismatch,
			)
		}
		if err := StrictlyAllocateFile(file, total); err != nil {
			return nil, fmt.Errorf("%w: strictly allocate transaction marker: %w", ErrSealedCapacityMismatch, err)
		}
	} else if err := txnMarkerPreallocate(file, total); err != nil {
		return nil, err
	}

	m := newTxnMarkerManager(
		file, root, path, sourceDir, sourceDirInfo, header,
	)
	if err := m.writeHeaderFaultable(0, header); err != nil {
		return nil, err
	}
	if err := m.writeHeaderFaultable(1, header); err != nil {
		return nil, err
	}
	if err := runTxnMarkerCreateFileSync(file); err != nil {
		return nil, err
	}
	if err := runTxnMarkerCreateParentDirSync(root); err != nil {
		return nil, err
	}
	if header.SealedCapacity {
		if err := requireExactRegularFileSize(file, total); err != nil {
			return nil, fmt.Errorf("%w: transaction marker after create sync: %w", ErrSealedCapacityMismatch, err)
		}
	}

	m.headerSlot = 0
	m.cursor = 0
	m.nextSequence = header.BaseSequence + 1
	cleanup = false
	cleanupRoot = false
	return m, nil
}

// OpenTxnMarker opens an existing decision log, selects the semantically valid
// header with the greatest recycle count, scans the live record prefix into
// TxnDecisions, and positions the append cursor at the first truncatable tail.
// A checksum-authenticated invalid header slot fails closed rather than falling
// back to an older epoch.
func OpenTxnMarker(path string, opts TxnMarkerOptions) (*TxnMarker, *TxnDecisions, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("%w: empty decision log path", ErrInvalidWrite)
	}
	sourceDir, err := canonicalTxnMarkerDir(path)
	if err != nil {
		return nil, nil, err
	}
	root, err := os.OpenRoot(sourceDir)
	if err != nil {
		return nil, nil, err
	}
	return openTxnMarkerInRoot(
		root, path, filepath.Base(path), sourceDir, opts,
	)
}

// InspectTxnMarker scans a decision log without changing or qualifying its
// physical allocation. The returned inspection cannot mutate the marker.
func InspectTxnMarker(
	path string,
) (*TxnMarkerInspection, *TxnDecisions, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("%w: empty decision log path", ErrInvalidWrite)
	}
	sourceDir, err := canonicalTxnMarkerDir(path)
	if err != nil {
		return nil, nil, err
	}
	root, err := os.OpenRoot(sourceDir)
	if err != nil {
		return nil, nil, err
	}
	file, sourceDirInfo, err := openTxnMarkerEntry(
		root, filepath.Base(path), os.O_RDONLY, 0,
	)
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = root.Close()
		}
	}()
	header, selected, err := selectTxnMarkerHeader(file)
	if err != nil {
		return nil, nil, err
	}
	if header.SealedCapacity {
		total := int64(txnMarkerRegionStart) + int64(header.Capacity)
		if err := requireExactRegularFileSize(file, total); err != nil {
			return nil, nil, fmt.Errorf("%w: transaction marker inspection: %w", ErrSealedCapacityMismatch, err)
		}
	}
	marker := newTxnMarkerManager(
		file, root, path, sourceDir, sourceDirInfo, header,
	)
	marker.headerSlot = uint32(selected)
	decisions := &TxnDecisions{}
	if err := marker.scanDecisions(decisions); err != nil {
		return nil, nil, err
	}
	cleanup = false
	return &TxnMarkerInspection{marker: marker}, decisions, nil
}

// OpenTxnMarkerAt opens name through a child handle of root. The returned
// marker owns that child handle; the caller retains ownership of root.
func OpenTxnMarkerAt(
	root *os.Root, name string, opts TxnMarkerOptions,
) (*TxnMarker, *TxnDecisions, error) {
	if root == nil || !validTxnMarkerName(name) {
		return nil, nil, fmt.Errorf(
			"%w: invalid decision log root or name", ErrInvalidWrite,
		)
	}
	child, err := root.OpenRoot(".")
	if err != nil {
		return nil, nil, err
	}
	sourceDir := filepath.Clean(root.Name())
	return openTxnMarkerInRoot(
		child, filepath.Join(sourceDir, name), name, sourceDir, opts,
	)
}

func openTxnMarkerInRoot(
	root *os.Root,
	path string,
	name string,
	sourceDir string,
	opts TxnMarkerOptions,
) (*TxnMarker, *TxnDecisions, error) {
	expectedCapacity, err := normalizeTxnMarkerOptionCapacity(opts, false)
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	file, sourceDirInfo, err := openTxnMarkerEntry(
		root, name, os.O_RDWR, 0o600,
	)
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = root.Close()
		}
	}()

	header, selected, err := selectTxnMarkerHeader(file)
	if err != nil {
		return nil, nil, err
	}
	if header.SealedCapacity != opts.SealedCapacity ||
		expectedCapacity != 0 && header.Capacity != expectedCapacity {
		return nil, nil, fmt.Errorf(
			"%w: transaction marker expected=%d sealed=%t actual=%d sealed=%t",
			ErrSealedCapacityMismatch, expectedCapacity, opts.SealedCapacity,
			header.Capacity, header.SealedCapacity,
		)
	}
	if header.SealedCapacity {
		total := int64(txnMarkerRegionStart) + int64(header.Capacity)
		info, statErr := file.Stat()
		if statErr != nil {
			return nil, nil, statErr
		}
		if !info.Mode().IsRegular() || info.Size() != total {
			return nil, nil, fmt.Errorf(
				"%w: transaction marker size=%d want=%d",
				ErrSealedCapacityMismatch, info.Size(), total,
			)
		}
		if err := StrictlyAllocateFile(file, total); err != nil {
			return nil, nil, fmt.Errorf("%w: reprove transaction marker: %w", ErrSealedCapacityMismatch, err)
		}
		if err := strictAllocationDataSync(file); err != nil {
			return nil, nil, fmt.Errorf("%w: sync transaction marker allocation: %w", ErrSealedCapacityMismatch, err)
		}
		if err := requireExactRegularFileSize(file, total); err != nil {
			return nil, nil, fmt.Errorf("%w: transaction marker after allocation sync: %w", ErrSealedCapacityMismatch, err)
		}
	}

	m := newTxnMarkerManager(
		file, root, path, sourceDir, sourceDirInfo, header,
	)
	m.headerSlot = uint32(selected)
	decisions := &TxnDecisions{}
	if err := m.scanDecisions(decisions); err != nil {
		return nil, nil, err
	}
	cleanup = false
	return m, decisions, nil
}

func selectTxnMarkerHeader(file *os.File) (TxnMarkerHeader, int, error) {
	var slots [txnMarkerHeaderSlots][TxnMarkerHeaderSize]byte
	selected := -1
	var header TxnMarkerHeader
	for slot := 0; slot < txnMarkerHeaderSlots; slot++ {
		off := int64(slot) * TxnMarkerHeaderSize
		if _, err := readFullAt(file, slots[slot][:], off); err != nil {
			return TxnMarkerHeader{}, -1, err
		}
		h, err := DecodeTxnMarkerHeader(slots[slot][:])
		if err != nil {
			if txnMarkerHeaderAuthenticated(slots[slot][:]) {
				return TxnMarkerHeader{}, -1, fmt.Errorf(
					"%w: authenticated invalid header slot %d: %v",
					ErrTxnMarkerCorrupt, slot, err,
				)
			}
			continue
		}
		if selected >= 0 && h.RecycleCount == header.RecycleCount && h != header {
			return TxnMarkerHeader{}, -1, fmt.Errorf(
				"%w: conflicting equal-count header slots",
				ErrTxnMarkerCorrupt,
			)
		}
		if selected < 0 || h.RecycleCount > header.RecycleCount {
			selected = slot
			header = h
		}
	}
	if selected < 0 {
		return TxnMarkerHeader{}, -1, ErrTxnMarkerNoValidHeader
	}
	return header, selected, nil
}

func normalizeTxnMarkerOptionCapacity(
	opts TxnMarkerOptions,
	creating bool,
) (uint64, error) {
	capacity := opts.Capacity
	if opts.SealedCapacity && capacity == 0 {
		return 0, fmt.Errorf("%w: sealed transaction marker requires an exact capacity", ErrSealedCapacityMismatch)
	}
	if capacity == 0 {
		if creating {
			return txnMarkerDefaultCapacityBytes, nil
		}
		return 0, nil
	}
	if capacity > TxnMarkerMaxCapacityBytes {
		return 0, fmt.Errorf("%w: capacity", ErrTxnMarkerCorrupt)
	}
	sector := uint64(TxnMarkerMinSectorSize)
	if remainder := capacity % sector; remainder != 0 {
		if opts.SealedCapacity {
			return 0, fmt.Errorf("%w: transaction marker capacity is not sector aligned", ErrSealedCapacityMismatch)
		}
		rounded, ok := checkedSizeAdd(
			capacity, sector-remainder, TxnMarkerMaxCapacityBytes,
		)
		if !ok {
			return 0, fmt.Errorf("%w: capacity", ErrTxnMarkerCorrupt)
		}
		capacity = rounded
	}
	return capacity, nil
}

func newTxnMarkerManager(
	file *os.File,
	root *os.Root,
	path string,
	sourceDir string,
	sourceDirInfo os.FileInfo,
	h TxnMarkerHeader,
) *TxnMarker {
	m := &TxnMarker{
		file:          file,
		root:          root,
		path:          path,
		sourceDir:     sourceDir,
		sourceDirInfo: sourceDirInfo,
		header:        h,
		scratch:       make([]byte, TxnMarkerHeaderSize),
		markerSync:    dataSync,
	}
	m.writeAt = file.WriteAt
	return m
}

func openTxnMarkerEntry(
	root *os.Root, name string, flag int, perm os.FileMode,
) (*os.File, os.FileInfo, error) {
	dirInfo, err := root.Stat(".")
	if err != nil {
		return nil, nil, err
	}
	creating := flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0
	var before os.FileInfo
	if !creating {
		before, err = root.Lstat(name)
		if err != nil {
			return nil, nil, err
		}
		if !before.Mode().IsRegular() {
			return nil, nil, fmt.Errorf(
				"%w: transaction log entry is not a regular non-symlink file",
				ErrInvalidWrite,
			)
		}
	}
	file, err := root.OpenFile(name, flag, perm)
	if err != nil {
		return nil, nil, err
	}
	fileInfo, fileErr := file.Stat()
	entryInfo, entryErr := root.Lstat(name)
	stable := fileErr == nil && entryErr == nil &&
		fileInfo.Mode().IsRegular() && entryInfo.Mode().IsRegular() &&
		os.SameFile(fileInfo, entryInfo)
	if before != nil {
		stable = stable && os.SameFile(before, entryInfo)
	}
	if !stable {
		_ = file.Close()
		if fileErr != nil {
			return nil, nil, fileErr
		}
		if entryErr != nil {
			return nil, nil, entryErr
		}
		return nil, nil, fmt.Errorf(
			"%w: transaction log entry is not a stable regular file",
			ErrInvalidWrite,
		)
	}
	return file, dirInfo, nil
}

func validTxnMarkerName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name
}

func canonicalTxnMarkerDir(path string) (string, error) {
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return "", fmt.Errorf("vibedb: resolve transaction log directory: %w", err)
	}
	dir = filepath.Clean(dir)
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("vibedb: resolve transaction log directory: %w", err)
	}
	return filepath.Clean(resolved), nil
}

// Header returns the value-only decision-log identity and geometry.
func (m *TxnMarker) Header() TxnMarkerHeader { return m.header }

// Path returns the filesystem path of the decision log.
func (m *TxnMarker) Path() string { return m.path }

// MatchesFileDirectory proves that file is still an entry in the same pinned
// physical directory as the open decision log.
func (m *TxnMarker) MatchesFileDirectory(file *os.File) (bool, error) {
	if m == nil || m.sourceDirInfo == nil || file == nil {
		return false, ErrInvalidWrite
	}
	return matchesTxnMarkerDirectory(m.sourceDirInfo, file)
}

// SameFile reports whether other is another live handle to this exact marker
// entry. Directory identity alone is insufficient for a rescan because a
// different valid txn.vtm could be swapped into the same directory.
func (m *TxnMarker) SameFile(other *TxnMarker) (bool, error) {
	if m == nil || other == nil || m.file == nil || other.file == nil {
		return false, ErrInvalidWrite
	}
	left, err := m.file.Stat()
	if err != nil {
		return false, err
	}
	right, err := other.file.Stat()
	if err != nil {
		return false, err
	}
	return os.SameFile(left, right), nil
}

// EntryCurrent proves that the live marker descriptor is still the exact
// regular, non-symlink txn.vtm entry under its pinned root.
func (m *TxnMarker) EntryCurrent() (bool, error) {
	if m == nil || m.file == nil || m.root == nil {
		return false, ErrInvalidWrite
	}
	fileInfo, err := m.file.Stat()
	if err != nil {
		return false, err
	}
	entryInfo, err := m.root.Lstat(filepath.Base(m.path))
	if err != nil {
		return false, err
	}
	return entryInfo.Mode().IsRegular() && os.SameFile(fileInfo, entryInfo), nil
}

// NextSequence reports the DCSN the next appended record will carry.
func (m *TxnMarker) NextSequence() uint64 { return m.nextSequence }

// Cursor reports the in-region byte offset of the next append.
func (m *TxnMarker) Cursor() uint64 { return m.cursor }

// FitsRetirement reports whether one participant-retirement record can consume
// the next sequence and fit at the current cursor without recycling. It is a
// pure preflight used by catalog drop barriers so ordinary pressure remains a
// definite capacity condition rather than being misclassified as a failed
// positional append.
func (m *TxnMarker) FitsRetirement() bool {
	if m == nil || m.nextSequence == 0 {
		return false
	}
	padded, ok := checkedTxnRetirementPaddedSize()
	if !ok {
		return false
	}
	end, ok := checkedSizeAdd(m.cursor, uint64(padded), ^uint64(0))
	return ok && end <= m.header.Capacity
}

func (m *TxnMarker) writeHeader(slot uint32, header TxnMarkerHeader) error {
	if _, err := EncodeTxnMarkerHeader(m.scratch, header); err != nil {
		return err
	}
	off := int64(slot) * TxnMarkerHeaderSize
	return writeTxnMarkerFull(
		m.writeAt, m.scratch[:TxnMarkerHeaderSize], off,
	)
}

func writeTxnMarkerFull(
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

func (m *TxnMarker) writeHeaderFaultable(slot uint32, header TxnMarkerHeader) error {
	if err := txnMarkerCreateHeaderFault(); err != nil {
		return err
	}
	return m.writeHeader(slot, header)
}

func (m *TxnMarker) scanDecisions(dst *TxnDecisions) error {
	m.cursor = 0
	m.nextSequence = m.header.BaseSequence + 1
	*dst = TxnDecisions{
		sourceDir:     m.sourceDir,
		sourceDirInfo: m.sourceDirInfo,
		markerID:      m.header.MarkerID,
		epoch:         m.header.Epoch,
	}
	// A terminal BaseSequence is a valid exhausted log. Recycle leaves the old
	// record bytes intact, and sequence zero is never a legal successor, so do
	// not reinterpret stale current-magic bytes after exhaustion.
	if m.nextSequence == 0 {
		return nil
	}

	// Empty / fully recycled logs begin with a zeroed region. Bad magic is the
	// authoritative truncatable-tail marker, so prove that from the first word
	// before materializing Capacity bytes.
	if _, err := readFullAt(
		m.file, m.scratch[:4], txnMarkerRegionStart,
	); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(m.scratch[:4]) == 0 {
		return nil
	}

	region := make([]byte, m.header.Capacity)
	if _, err := readFullAt(m.file, region, txnMarkerRegionStart); err != nil {
		return err
	}
	cursor := uint64(0)
	sequence := m.header.BaseSequence + 1
	lastTxnID := uint64(0)
	for cursor < m.header.Capacity {
		rec, padded, err := decodeTxnMarkerRecord(region[cursor:], sequence)
		if err != nil {
			if errors.Is(err, errTxnMarkerTruncatableTail) {
				break
			}
			return err
		}
		if cursor+uint64(padded) > m.header.Capacity {
			break
		}
		switch rec.Kind {
		case TxnMarkerRecordKindDecision:
			if rec.TxnID <= lastTxnID {
				return txnMarkerSemanticError(
					"decision txn ids are not strictly increasing",
				)
			}
			if dst.decisions == nil {
				dst.decisions = make(map[uint64][]TxnParticipant)
			}
			for _, participant := range rec.Participants {
				if _, retired := dst.retired[participant.StoreID]; retired {
					return txnMarkerSemanticError(
						"decision names an already-retired participant",
					)
				}
			}
			if _, exists := dst.decisions[rec.TxnID]; exists {
				return txnMarkerSemanticError("duplicate txn id")
			}
			participants := make([]TxnParticipant, len(rec.Participants))
			copy(participants, rec.Participants)
			dst.decisions[rec.TxnID] = participants
			dst.decisionIDs = append(dst.decisionIDs, rec.TxnID)
			dst.maxTxnID = rec.TxnID
			lastTxnID = rec.TxnID
		case TxnMarkerRecordKindRetirement:
			if dst.retired == nil {
				dst.retired = make(map[[16]byte]struct{})
			}
			if _, exists := dst.retired[rec.StoreID]; exists {
				return txnMarkerSemanticError("duplicate participant retirement")
			}
			dst.retired[rec.StoreID] = struct{}{}
		default:
			return txnMarkerSemanticError("unknown record kind")
		}
		if rec.Sequence > dst.maxDCSN {
			dst.maxDCSN = rec.Sequence
		}
		cursor += uint64(padded)
		sequence++
		if sequence == 0 {
			break
		}
	}
	m.cursor = cursor
	m.nextSequence = sequence
	m.lastTxnID = lastTxnID
	m.retired = make(map[[16]byte]struct{}, len(dst.retired))
	for storeID := range dst.retired {
		m.retired[storeID] = struct{}{}
	}
	return nil
}

// AppendDecision appends one committed decision and returns its assigned DCSN.
// It does not sync; the caller issues Sync exactly once after the append.
func (m *TxnMarker) AppendDecision(
	txnID uint64, participants []TxnParticipant,
) (uint64, error) {
	if m.nextSequence == 0 {
		return 0, ErrTxnMarkerFull
	}
	if txnID == 0 || txnID <= m.lastTxnID || txnID == ^uint64(0) {
		return 0, fmt.Errorf("%w: txn id is not a usable strict successor", ErrInvalidWrite)
	}
	for _, participant := range participants {
		if _, retired := m.retired[participant.StoreID]; retired {
			return 0, fmt.Errorf(
				"%w: decision names an already-retired participant", ErrInvalidWrite,
			)
		}
	}
	padded, ok := checkedTxnDecisionPaddedSize(len(participants))
	if !ok {
		return 0, fmt.Errorf("%w: decision record length", ErrInvalidWrite)
	}
	end, ok := checkedSizeAdd(m.cursor, uint64(padded), ^uint64(0))
	if !ok || end > m.header.Capacity {
		return 0, ErrTxnMarkerFull
	}
	if cap(m.scratch) < padded {
		m.scratch = make([]byte, padded)
	}
	if _, err := encodeTxnDecisionRecord(
		m.scratch[:padded], m.nextSequence, txnID, participants,
	); err != nil {
		return 0, err
	}
	offset := int64(txnMarkerRegionStart) + int64(m.cursor)
	if err := writeTxnMarkerFull(m.writeAt, m.scratch[:padded], offset); err != nil {
		return 0, err
	}
	m.cursor = end
	sequence := m.nextSequence
	m.nextSequence++
	m.lastTxnID = txnID
	return sequence, nil
}

// AppendRetirement appends one participant-retired record and returns its
// assigned DCSN. It does not sync.
func (m *TxnMarker) AppendRetirement(storeID [16]byte) (uint64, error) {
	if m.nextSequence == 0 {
		return 0, ErrTxnMarkerFull
	}
	if _, retired := m.retired[storeID]; retired {
		return 0, fmt.Errorf("%w: duplicate participant retirement", ErrInvalidWrite)
	}
	padded, ok := checkedTxnRetirementPaddedSize()
	if !ok {
		return 0, fmt.Errorf("%w: retirement record length", ErrInvalidWrite)
	}
	end, ok := checkedSizeAdd(m.cursor, uint64(padded), ^uint64(0))
	if !ok || end > m.header.Capacity {
		return 0, ErrTxnMarkerFull
	}
	if cap(m.scratch) < padded {
		m.scratch = make([]byte, padded)
	}
	if _, err := encodeTxnRetirementRecord(
		m.scratch[:padded], m.nextSequence, storeID,
	); err != nil {
		return 0, err
	}
	offset := int64(txnMarkerRegionStart) + int64(m.cursor)
	if err := writeTxnMarkerFull(m.writeAt, m.scratch[:padded], offset); err != nil {
		return 0, err
	}
	m.cursor = end
	sequence := m.nextSequence
	m.nextSequence++
	if m.retired == nil {
		m.retired = make(map[[16]byte]struct{})
	}
	m.retired[storeID] = struct{}{}
	return sequence, nil
}

// Sync issues the power-safe barrier on the decision-log file alone. The
// decision sync is the sole atomic commit point for multi-collection commits.
func (m *TxnMarker) Sync() error {
	return m.markerSync(m.file)
}

// Recycle resets the record region for a new epoch. It rewrites the opposite
// header slot with newEpoch, an advanced base sequence, and a bumped recycle
// count, then syncs. Stale record bytes are never zeroed: the new BaseSequence
// anchor makes them fail monotonic-sequence validation.
//
// The in-memory header advances only after the write and sync both succeed.
func (m *TxnMarker) Recycle(newEpoch uint64) error {
	if newEpoch <= m.header.Epoch {
		return fmt.Errorf("%w: recycle epoch did not advance", ErrTxnMarkerCorrupt)
	}
	next := m.header
	next.Epoch = newEpoch
	next.BaseSequence = m.nextSequence - 1
	next.RecycleCount++
	slot := m.headerSlot ^ 1
	if err := m.writeHeader(slot, next); err != nil {
		return err
	}
	if err := m.markerSync(m.file); err != nil {
		return err
	}
	m.header = next
	m.headerSlot = slot
	m.cursor = 0
	m.lastTxnID = 0
	m.retired = nil
	return nil
}

// Close closes the underlying decision-log file and its pinned directory.
func (m *TxnMarker) Close() error {
	if m == nil {
		return nil
	}
	var fileErr, rootErr error
	if m.file != nil {
		fileErr = m.file.Close()
		m.file = nil
	}
	if m.root != nil {
		rootErr = m.root.Close()
		m.root = nil
	}
	return errors.Join(fileErr, rootErr)
}

// Remove verifies the reserved entry against the open descriptor, closes the
// descriptor, unlinks through the retained root, and persists that unlink
// before releasing the root. It is the namespace-safe L4 residue-removal path.
func (m *TxnMarker) Remove() (err error) {
	if m == nil || m.file == nil || m.root == nil {
		return ErrInvalidWrite
	}
	defer func() {
		err = errors.Join(err, m.Close())
	}()
	current, err := m.EntryCurrent()
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf(
			"%w: transaction log entry changed before removal", ErrInvalidWrite,
		)
	}
	name := filepath.Base(m.path)
	closeErr := m.file.Close()
	m.file = nil
	if closeErr != nil {
		return closeErr
	}
	removeErr := m.root.Remove(name)
	var syncErr error
	if removeErr == nil {
		syncErr = syncTxnMarkerParentDir(m.root)
	}
	return errors.Join(removeErr, syncErr)
}

func syncTxnMarkerParentDir(root *os.Root) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if root == nil {
		return ErrInvalidWrite
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
