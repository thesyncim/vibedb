package storeio

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
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
// (re-mint versus fail-closed) belongs to the durable layer, not this package.
//
// Recycle legality ("no participant journal still holds current-epoch kind-5
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
	// TxnMarkerMaxCapacityBytes mirrors RecoveryJournalMaxCapacityBytes so a
	// checksummed hostile header cannot drive an unbounded allocation.
	TxnMarkerMaxCapacityBytes = RecoveryJournalMaxCapacityBytes
	// txnMarkerDefaultCapacityBytes is the create-time default record region.
	txnMarkerDefaultCapacityBytes = uint64(1) << 20

	// TxnMarkerFormatVersion is the only admitted decision-log grammar. It is
	// nonzero so a hypothetical reader that required a zero format word rejects
	// the file before decoding records.
	TxnMarkerFormatVersion = uint32(1)

	txnMarkerMagic       = "VTXNLOG\x00"
	txnMarkerRecordMagic = uint32(0x304e5854) // "TXN0", little-endian.

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
	// invalid. The durable layer treats this as re-mintable mint residue when
	// no journal holds conditional records, and as fail-closed tampering
	// otherwise — that policy is not this package's.
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
// fsync fence (L2 / W7). Production opens the parent and Syncs it.
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
// log. FormatVersion gates the record grammar; BaseSequence anchors monotonic
// sequence validation so stale bytes left after a recycle can never be
// mistaken for live records.
type TxnMarkerHeader struct {
	FormatVersion uint32
	MarkerID      [16]byte
	Epoch         uint64
	BaseSequence  uint64
	Capacity      uint64
	// RecycleCount is strictly monotonic across recycles. Open selects the
	// valid header slot with the highest count.
	RecycleCount uint64
}

// TxnParticipant is one collection named by a committed decision.
type TxnParticipant struct {
	StoreID            [16]byte
	JournalID          [16]byte
	PreparedGeneration uint64
}

// TxnMarkerOptions configures CreateTxnMarker / OpenTxnMarker. A zero Capacity
// selects the package default at create time; Open ignores Capacity and trusts
// the on-disk header (after the hard clamp).
type TxnMarkerOptions struct {
	Capacity uint64
}

// TxnMarker is the single-writer file-backed manager for txn.vtm. It is not
// safe for concurrent use.
type TxnMarker struct {
	file   *os.File
	path   string
	header TxnMarkerHeader
	// cursor is the in-region byte offset of the next append.
	cursor uint64
	// nextSequence is the DCSN the next appended record will carry.
	nextSequence uint64
	// headerSlot is the alternating slot the live header occupies.
	headerSlot uint32
	scratch    []byte
	// markerSync and writeAt are injected so a fault seam can wrap the real
	// barriers and appends; production wires the platform helpers.
	markerSync func(*os.File) error
	writeAt    func(p []byte, off int64) (int, error)
}

// TxnDecisions is the scan of one open decision log: committed participant
// sets keyed by TxnID within the selected header's epoch, the retired StoreID
// set, and the high-water TxnID / DCSN for counter seeding.
type TxnDecisions struct {
	markerID  [16]byte
	epoch     uint64
	decisions map[uint64][]TxnParticipant
	retired   map[[16]byte]struct{}
	maxTxnID  uint64
	maxDCSN   uint64
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

func validateTxnMarkerHeader(h TxnMarkerHeader) error {
	if h.FormatVersion != TxnMarkerFormatVersion {
		return fmt.Errorf("%w: format version", ErrTxnMarkerCorrupt)
	}
	if h.MarkerID == ([16]byte{}) {
		return fmt.Errorf("%w: zero marker identity", ErrTxnMarkerCorrupt)
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
	binary.LittleEndian.PutUint32(sector[8:12], h.FormatVersion)
	binary.LittleEndian.PutUint32(sector[12:16], TxnMarkerHeaderSize)
	copy(sector[16:32], h.MarkerID[:])
	binary.LittleEndian.PutUint64(sector[32:40], h.Epoch)
	binary.LittleEndian.PutUint64(sector[40:48], h.BaseSequence)
	binary.LittleEndian.PutUint64(sector[48:56], h.Capacity)
	binary.LittleEndian.PutUint64(sector[56:64], h.RecycleCount)
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
	if binary.LittleEndian.Uint32(src[8:12]) != TxnMarkerFormatVersion ||
		binary.LittleEndian.Uint32(src[12:16]) != TxnMarkerHeaderSize {
		return TxnMarkerHeader{}, fmt.Errorf("%w: version or header size", ErrTxnMarkerCorrupt)
	}
	h := TxnMarkerHeader{FormatVersion: TxnMarkerFormatVersion}
	copy(h.MarkerID[:], src[16:32])
	h.Epoch = binary.LittleEndian.Uint64(src[32:40])
	h.BaseSequence = binary.LittleEndian.Uint64(src[40:48])
	h.Capacity = binary.LittleEndian.Uint64(src[48:56])
	h.RecycleCount = binary.LittleEndian.Uint64(src[56:64])
	if err := validateTxnMarkerHeader(h); err != nil {
		return TxnMarkerHeader{}, err
	}
	return h, nil
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
	if len(src) < TxnMarkerRecordPrefixSize+TxnMarkerRecordTrailerSize {
		return txnMarkerRecord{}, 0, txnMarkerTailError("short record")
	}
	if binary.LittleEndian.Uint32(src[0:4]) != txnMarkerRecordMagic {
		return txnMarkerRecord{}, 0, txnMarkerTailError("magic")
	}
	kind := binary.LittleEndian.Uint16(src[4:6])
	if binary.LittleEndian.Uint16(src[6:8]) != 0 {
		return txnMarkerRecord{}, 0, txnMarkerTailError("reserved")
	}
	sequence := binary.LittleEndian.Uint64(src[8:16])
	if sequence != expectedSequence {
		return txnMarkerRecord{}, 0, txnMarkerTailError("sequence")
	}
	switch kind {
	case TxnMarkerRecordKindDecision:
		return decodeTxnDecisionRecord(src, sequence)
	case TxnMarkerRecordKindRetirement:
		return decodeTxnRetirementRecord(src, sequence)
	default:
		return txnMarkerRecord{}, 0, txnMarkerTailError("kind")
	}
}

func decodeTxnDecisionRecord(
	src []byte, sequence uint64,
) (txnMarkerRecord, int, error) {
	txnID := binary.LittleEndian.Uint64(src[16:24])
	participantCount := binary.LittleEndian.Uint32(src[24:28])
	if binary.LittleEndian.Uint32(src[28:32]) != 0 {
		return txnMarkerRecord{}, 0, txnMarkerTailError("reserved")
	}
	if txnID == 0 || participantCount == 0 ||
		participantCount > TxnMarkerMaxParticipants {
		return txnMarkerRecord{}, 0, txnMarkerTailError("decision framing")
	}
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
	capacity := opts.Capacity
	if capacity == 0 {
		capacity = txnMarkerDefaultCapacityBytes
	}
	if capacity > TxnMarkerMaxCapacityBytes {
		return nil, fmt.Errorf("%w: capacity", ErrTxnMarkerCorrupt)
	}
	sector := uint64(TxnMarkerMinSectorSize)
	if remainder := capacity % sector; remainder != 0 {
		rounded, ok := checkedSizeAdd(
			capacity, sector-remainder, TxnMarkerMaxCapacityBytes,
		)
		if !ok {
			return nil, fmt.Errorf("%w: capacity", ErrTxnMarkerCorrupt)
		}
		capacity = rounded
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
		}
	}()

	var markerID [16]byte
	if _, err := rand.Read(markerID[:]); err != nil {
		return nil, err
	}
	header := TxnMarkerHeader{
		FormatVersion: TxnMarkerFormatVersion,
		MarkerID:      markerID,
		Epoch:         1,
		BaseSequence:  0,
		Capacity:      capacity,
		RecycleCount:  1,
	}
	if err := validateTxnMarkerHeader(header); err != nil {
		return nil, err
	}

	total := int64(txnMarkerRegionStart) + int64(capacity)
	if err := txnMarkerPreallocate(file, total); err != nil {
		return nil, err
	}

	m := newTxnMarkerManager(file, path, header)
	if err := m.writeHeaderFaultable(0, header); err != nil {
		return nil, err
	}
	if err := m.writeHeaderFaultable(1, header); err != nil {
		return nil, err
	}
	if err := runTxnMarkerCreateFileSync(file); err != nil {
		return nil, err
	}
	if err := runTxnMarkerCreateParentDirSync(path); err != nil {
		return nil, err
	}

	m.headerSlot = 0
	m.cursor = 0
	m.nextSequence = header.BaseSequence + 1
	cleanup = false
	return m, nil
}

// OpenTxnMarker opens an existing decision log, selects the valid header with
// the greatest recycle count, scans the live record prefix into TxnDecisions,
// and positions the append cursor at the first truncatable tail.
func OpenTxnMarker(path string, opts TxnMarkerOptions) (*TxnMarker, *TxnDecisions, error) {
	_ = opts // Open trusts the on-disk header; options reserved for symmetry.
	if path == "" {
		return nil, nil, fmt.Errorf("%w: empty decision log path", ErrInvalidWrite)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
		}
	}()

	var slots [txnMarkerHeaderSlots][TxnMarkerHeaderSize]byte
	selected := -1
	var header TxnMarkerHeader
	for slot := 0; slot < txnMarkerHeaderSlots; slot++ {
		off := int64(slot) * TxnMarkerHeaderSize
		if _, err := readFullAt(file, slots[slot][:], off); err != nil {
			return nil, nil, err
		}
		h, err := DecodeTxnMarkerHeader(slots[slot][:])
		if err != nil {
			continue
		}
		if selected < 0 || h.RecycleCount > header.RecycleCount {
			selected = slot
			header = h
		}
	}
	if selected < 0 {
		return nil, nil, ErrTxnMarkerNoValidHeader
	}

	m := newTxnMarkerManager(file, path, header)
	m.headerSlot = uint32(selected)
	decisions := &TxnDecisions{}
	if err := m.scanDecisions(decisions); err != nil {
		return nil, nil, err
	}
	cleanup = false
	return m, decisions, nil
}

func newTxnMarkerManager(
	file *os.File, path string, h TxnMarkerHeader,
) *TxnMarker {
	m := &TxnMarker{
		file:       file,
		path:       path,
		header:     h,
		scratch:    make([]byte, TxnMarkerHeaderSize),
		markerSync: dataSync,
	}
	m.writeAt = file.WriteAt
	return m
}

// Header returns the value-only decision-log identity and geometry.
func (m *TxnMarker) Header() TxnMarkerHeader { return m.header }

// Path returns the filesystem path of the decision log.
func (m *TxnMarker) Path() string { return m.path }

// NextSequence reports the DCSN the next appended record will carry.
func (m *TxnMarker) NextSequence() uint64 { return m.nextSequence }

// Cursor reports the in-region byte offset of the next append.
func (m *TxnMarker) Cursor() uint64 { return m.cursor }

func (m *TxnMarker) writeHeader(slot uint32, header TxnMarkerHeader) error {
	if _, err := EncodeTxnMarkerHeader(m.scratch, header); err != nil {
		return err
	}
	off := int64(slot) * TxnMarkerHeaderSize
	if _, err := m.writeAt(m.scratch[:TxnMarkerHeaderSize], off); err != nil {
		return err
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
		markerID: m.header.MarkerID,
		epoch:    m.header.Epoch,
	}

	// Empty / fully recycled logs begin with a zeroed region. Bad magic is the
	// authoritative truncatable-tail marker, so prove that from the first word
	// before materializing Capacity bytes.
	if _, err := readFullAt(
		m.file, m.scratch[:4], txnMarkerRegionStart,
	); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(m.scratch[:4]) != txnMarkerRecordMagic {
		return nil
	}

	region := make([]byte, m.header.Capacity)
	if _, err := readFullAt(m.file, region, txnMarkerRegionStart); err != nil {
		return err
	}
	cursor := uint64(0)
	sequence := m.header.BaseSequence + 1
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
			if dst.decisions == nil {
				dst.decisions = make(map[uint64][]TxnParticipant)
			}
			if _, exists := dst.decisions[rec.TxnID]; exists {
				return txnMarkerSemanticError("duplicate txn id")
			}
			participants := make([]TxnParticipant, len(rec.Participants))
			copy(participants, rec.Participants)
			dst.decisions[rec.TxnID] = participants
			if rec.TxnID > dst.maxTxnID {
				dst.maxTxnID = rec.TxnID
			}
		case TxnMarkerRecordKindRetirement:
			if dst.retired == nil {
				dst.retired = make(map[[16]byte]struct{})
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
	}
	m.cursor = cursor
	m.nextSequence = sequence
	return nil
}

// AppendDecision appends one committed decision and returns its assigned DCSN.
// It does not sync; the caller issues Sync exactly once after the append.
func (m *TxnMarker) AppendDecision(
	txnID uint64, participants []TxnParticipant,
) (uint64, error) {
	if txnID == 0 {
		return 0, fmt.Errorf("%w: zero txn id", ErrInvalidWrite)
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
	if _, err := m.writeAt(m.scratch[:padded], offset); err != nil {
		return 0, err
	}
	m.cursor = end
	sequence := m.nextSequence
	m.nextSequence++
	return sequence, nil
}

// AppendRetirement appends one participant-retired record and returns its
// assigned DCSN. It does not sync.
func (m *TxnMarker) AppendRetirement(storeID [16]byte) (uint64, error) {
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
	if _, err := m.writeAt(m.scratch[:padded], offset); err != nil {
		return 0, err
	}
	m.cursor = end
	sequence := m.nextSequence
	m.nextSequence++
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
	return nil
}

// Close closes the underlying decision-log file.
func (m *TxnMarker) Close() error {
	if m == nil || m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

func syncTxnMarkerParentDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
