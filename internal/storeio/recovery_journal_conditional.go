package storeio

import (
	"encoding/binary"
	"fmt"
)

// Conditional batch records (kind 5) are the prepare half of a multi-collection
// commit. On the wire they reuse the kind-3 batch envelope and entry grammar —
// one sequence, one generation, one CRC32C plus complement, sector-padded —
// with a 32-byte conditional header prefixed to the entry body:
//
//	MarkerID    [16]byte
//	MarkerEpoch uint64
//	TxnID       uint64
//
// The single CRC covers the 32-byte envelope, the conditional header, and every
// framed entry. This package decodes and surfaces (MarkerID, MarkerEpoch,
// TxnID, batch); skipping, applying, or failing closed is the durable layer's
// decision. Torn-tail truncation, strict sequence validation, and recycle
// behavior are inherited from the ordinary journal manager unchanged.
//
// The conditional journal format word (RecoveryJournalFormatConditional) is the
// gate: a legacy or scalar-patch reader rejects the header before record
// decode, and a kind-5 record inside a legacy or scalar-patch journal fails
// closed. Conditional journals accept record kinds 1, 2, 3, and 5; scalar-patch
// entries (kind 4) remain scalar-patch-format-only and never coexist with
// conditional records under this format word.

func recoveryConditionalHeaderValid(h RecoveryConditionalHeader) bool {
	return h.MarkerID != ([16]byte{}) && h.MarkerEpoch != 0 && h.TxnID != 0
}

func encodeRecoveryConditionalHeader(dst []byte, h RecoveryConditionalHeader) {
	copy(dst[0:16], h.MarkerID[:])
	binary.LittleEndian.PutUint64(dst[16:24], h.MarkerEpoch)
	binary.LittleEndian.PutUint64(dst[24:32], h.TxnID)
}

func decodeRecoveryConditionalHeader(src []byte) (RecoveryConditionalHeader, bool) {
	if len(src) < RecoveryConditionalHeaderSize {
		return RecoveryConditionalHeader{}, false
	}
	var h RecoveryConditionalHeader
	copy(h.MarkerID[:], src[0:16])
	h.MarkerEpoch = binary.LittleEndian.Uint64(src[16:24])
	h.TxnID = binary.LittleEndian.Uint64(src[24:32])
	if !recoveryConditionalHeaderValid(h) {
		return RecoveryConditionalHeader{}, false
	}
	return h, true
}

func checkedRecoveryConditionalBatchBodyLen(
	entries []RecoveryBatchEntry,
) (uint64, bool) {
	entriesBody, ok := checkedRecoveryBatchBodyLen(entries)
	if !ok {
		return 0, false
	}
	return checkedSizeAdd(
		RecoveryConditionalHeaderSize, entriesBody, uint64(^uint32(0)),
	)
}

func prepareRecoveryConditionalBatch(
	sectorSize uint32, entries []RecoveryBatchEntry,
) (RecoveryBatchPlan, bool) {
	if !recoveryBatchEntriesAllowed(
		RecoveryJournalFormatConditional, entries,
	) {
		return RecoveryBatchPlan{}, false
	}
	body, ok := checkedRecoveryConditionalBatchBodyLen(entries)
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
		formatVersion: RecoveryJournalFormatConditional,
		sectorSize:    sectorSize,
		entryCount:    len(entries),
		bodyLen:       body,
		padded:        padded,
	}, true
}

func encodeRecoveryConditionalBatchRecordPrepared(
	dst []byte, sectorSize uint32, rec RecoveryRecord,
	plan RecoveryBatchPlan,
) (int, error) {
	if rec.Sequence == 0 || rec.Generation == 0 {
		return 0, fmt.Errorf("%w: zero sequence or generation", ErrInvalidWrite)
	}
	if !recoveryConditionalHeaderValid(rec.Conditional) {
		return 0, fmt.Errorf("%w: conditional header", ErrInvalidWrite)
	}
	if plan.formatVersion != RecoveryJournalFormatConditional ||
		!plan.validFor(sectorSize, len(rec.Entries)) {
		return 0, fmt.Errorf("%w: conditional batch plan", ErrInvalidWrite)
	}
	if len(dst) < plan.padded {
		return 0, fmt.Errorf(
			"%w: conditional batch record buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), plan.padded,
		)
	}
	buf := dst[:plan.padded]
	clear(buf)
	binary.LittleEndian.PutUint32(buf[0:4], recoveryRecordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], recoveryRecordKindConditionalBatch)
	// buf[6:8] reserved zero.
	binary.LittleEndian.PutUint64(buf[8:16], rec.Sequence)
	binary.LittleEndian.PutUint64(buf[16:24], rec.Generation)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(rec.Entries)))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(plan.bodyLen))
	cursor := uint64(RecoveryJournalRecordPrefixSize)
	bodyEnd := cursor + plan.bodyLen
	headerEnd := cursor + RecoveryConditionalHeaderSize
	if headerEnd > bodyEnd {
		return 0, fmt.Errorf("%w: conditional batch plan", ErrInvalidWrite)
	}
	encodeRecoveryConditionalHeader(
		buf[cursor:headerEnd], rec.Conditional,
	)
	cursor = headerEnd
	for i := range rec.Entries {
		entry := rec.Entries[i]
		if entry.Kind != recoveryRecordKindPut &&
			entry.Kind != recoveryRecordKindDelete {
			return 0, fmt.Errorf(
				"%w: conditional batch entry kind", ErrInvalidWrite,
			)
		}
		if entry.ScalarPatch != (RecoveryScalarPatchMetadata{}) {
			return 0, fmt.Errorf(
				"%w: scalar-patch metadata in conditional batch",
				ErrInvalidWrite,
			)
		}
		if len(entry.Key) == 0 ||
			uint64(len(entry.Key)) > uint64(^uint32(0)) ||
			uint64(len(entry.Value)) > uint64(^uint32(0)) {
			return 0, fmt.Errorf(
				"%w: conditional batch entry key or value length",
				ErrInvalidWrite,
			)
		}
		entryEnd := cursor + RecoveryBatchEntryHeaderSize +
			uint64(len(entry.Key)) + uint64(len(entry.Value))
		if entryEnd > bodyEnd {
			return 0, fmt.Errorf(
				"%w: conditional batch plan no longer matches entries",
				ErrInvalidWrite,
			)
		}
		binary.LittleEndian.PutUint16(buf[cursor:cursor+2], entry.Kind)
		// buf[cursor+2:cursor+4] reserved zero.
		binary.LittleEndian.PutUint32(
			buf[cursor+4:cursor+8], uint32(len(entry.Key)),
		)
		binary.LittleEndian.PutUint32(
			buf[cursor+8:cursor+12], uint32(len(entry.Value)),
		)
		cursor += RecoveryBatchEntryHeaderSize
		cursor += uint64(copy(buf[cursor:], entry.Key))
		cursor += uint64(copy(buf[cursor:], entry.Value))
	}
	if cursor != bodyEnd {
		return 0, fmt.Errorf(
			"%w: conditional batch plan no longer matches entries",
			ErrInvalidWrite,
		)
	}
	checksum := PageChecksum(buf[:cursor])
	binary.LittleEndian.PutUint32(buf[cursor:cursor+4], checksum)
	binary.LittleEndian.PutUint32(buf[cursor+4:cursor+8], ^checksum)
	return plan.padded, nil
}

// decodeRecoveryConditionalBatchRecord validates one kind-5 record at the start
// of src. Framing, checksum, and sequence failures are truncatable; a
// checksum-authenticated semantic failure (zero conditional identity, illegal
// entry kind, body mismatch) is hard.
func decodeRecoveryConditionalBatchRecord(
	src []byte, sectorSize uint32, expectedSequence uint64,
) (RecoveryRecord, int, error) {
	sequence := binary.LittleEndian.Uint64(src[8:16])
	generation := binary.LittleEndian.Uint64(src[16:24])
	entryCount := binary.LittleEndian.Uint32(src[24:28])
	bodyLen := binary.LittleEndian.Uint32(src[28:32])
	if sequence != expectedSequence || generation == 0 || entryCount == 0 {
		return RecoveryRecord{}, 0, recoveryJournalTailError(
			"conditional batch sequence or framing",
		)
	}
	if uint64(bodyLen) < RecoveryConditionalHeaderSize {
		return RecoveryRecord{}, 0, recoveryJournalTailError(
			"conditional batch body shorter than header",
		)
	}
	bodyEnd := uint64(RecoveryJournalRecordPrefixSize) + uint64(bodyLen)
	if bodyEnd+RecoveryJournalRecordTrailerSize > uint64(len(src)) {
		return RecoveryRecord{}, 0, recoveryJournalTailError(
			"conditional batch length overruns region",
		)
	}
	checksum := PageChecksum(src[:bodyEnd])
	stored := binary.LittleEndian.Uint32(src[bodyEnd : bodyEnd+4])
	if stored != checksum ||
		binary.LittleEndian.Uint32(src[bodyEnd+4:bodyEnd+8]) != ^checksum {
		return RecoveryRecord{}, 0, recoveryJournalTailError(
			"conditional batch checksum",
		)
	}
	conditional, ok := decodeRecoveryConditionalHeader(
		src[RecoveryJournalRecordPrefixSize : RecoveryJournalRecordPrefixSize+RecoveryConditionalHeaderSize],
	)
	if !ok {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid conditional header",
		)
	}
	entriesBody := uint64(bodyLen) - RecoveryConditionalHeaderSize
	if uint64(entryCount) >
		entriesBody/(RecoveryBatchEntryHeaderSize+1) ||
		uint64(entryCount) > uint64(maxIntValue) {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid conditional batch entry count overruns body",
		)
	}
	if !recoveryBatchEntryArenaFits(uint64(entryCount)) {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid conditional batch entry arena overflows address space",
		)
	}
	entries := make([]RecoveryBatchEntry, 0, entryCount)
	cursor := uint64(RecoveryJournalRecordPrefixSize) + RecoveryConditionalHeaderSize
	for i := uint32(0); i < entryCount; i++ {
		if cursor+RecoveryBatchEntryHeaderSize > bodyEnd {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid conditional batch entry header overruns",
			)
		}
		entryKind := binary.LittleEndian.Uint16(src[cursor : cursor+2])
		if binary.LittleEndian.Uint16(src[cursor+2:cursor+4]) != 0 ||
			(entryKind != recoveryRecordKindPut &&
				entryKind != recoveryRecordKindDelete) {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid conditional batch entry kind or reserved",
			)
		}
		keyLen := uint64(binary.LittleEndian.Uint32(src[cursor+4 : cursor+8]))
		valueLen := uint64(binary.LittleEndian.Uint32(src[cursor+8 : cursor+12]))
		if keyLen == 0 {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid conditional batch entry key length",
			)
		}
		keyStart := cursor + RecoveryBatchEntryHeaderSize
		valueStart := keyStart + keyLen
		entryEnd := valueStart + valueLen
		if entryEnd > bodyEnd {
			return RecoveryRecord{}, 0, recoveryJournalSemanticError(
				"checksum-valid conditional batch entry overruns body",
			)
		}
		entries = append(entries, RecoveryBatchEntry{
			Kind:  entryKind,
			Key:   src[keyStart:valueStart],
			Value: src[valueStart:entryEnd],
		})
		cursor = entryEnd
	}
	if cursor != bodyEnd {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid conditional batch body length mismatch",
		)
	}
	padded, ok := checkedRecoveryPadRaw(
		sectorSize, bodyEnd+RecoveryJournalRecordTrailerSize,
	)
	if !ok {
		return RecoveryRecord{}, 0, recoveryJournalSemanticError(
			"checksum-valid padded conditional batch length",
		)
	}
	return RecoveryRecord{
		Sequence:    sequence,
		Generation:  generation,
		Kind:        recoveryRecordKindConditionalBatch,
		Entries:     entries,
		Conditional: conditional,
	}, padded, nil
}

// RecoveryConditionalBatchRecordPaddedSize returns the on-disk byte cost of one
// conditional batch record carrying entries, padded to the sector granule.
// Unrepresentable inputs saturate at the largest int, matching
// RecoveryBatchRecordPaddedSize.
func RecoveryConditionalBatchRecordPaddedSize(
	sectorSize uint32, entries []RecoveryBatchEntry,
) int {
	plan, ok := prepareRecoveryConditionalBatch(sectorSize, entries)
	if !ok {
		return maxIntValue
	}
	return plan.padded
}

// PrepareConditionalBatch validates and sizes one conditional batch without
// allocating. The opaque plan is bound to the conditional format word; it can
// be reused for the capacity decision, append, and accounting.
func (rj *RecoveryJournal) PrepareConditionalBatch(
	entries []RecoveryBatchEntry,
) (RecoveryBatchPlan, error) {
	if rj.header.FormatVersion != RecoveryJournalFormatConditional {
		return RecoveryBatchPlan{}, fmt.Errorf(
			"%w: conditional batch requires conditional journal format",
			ErrInvalidWrite,
		)
	}
	plan, ok := prepareRecoveryConditionalBatch(rj.header.SectorSize, entries)
	if !ok {
		return RecoveryBatchPlan{}, fmt.Errorf(
			"%w: conditional batch record length", ErrInvalidWrite,
		)
	}
	return plan, nil
}

// FitsConditionalBatch reports whether one conditional batch record carrying
// entries would fit in the remaining preallocated capacity without a recycle.
func (rj *RecoveryJournal) FitsConditionalBatch(entries []RecoveryBatchEntry) bool {
	plan, err := rj.PrepareConditionalBatch(entries)
	if err != nil {
		return false
	}
	return rj.PreparedBatchFits(plan)
}

// AppendConditionalBatch writes one kind-5 conditional batch record at the
// cursor and advances it, consuming a single sequence number. Like AppendBatch
// it never extends the file — a record that would overrun capacity returns
// ErrRecoveryJournalFull — and it does not sync: the caller issues the lane's
// sync once after the append. The group is durable, atomically, after that one
// sync. Decide-time resolution of the conditional header is not performed here.
func (rj *RecoveryJournal) AppendConditionalBatch(
	generation uint64,
	markerID [16]byte,
	markerEpoch, txnID uint64,
	entries []RecoveryBatchEntry,
) (uint64, error) {
	plan, err := rj.PrepareConditionalBatch(entries)
	if err != nil {
		return 0, err
	}
	return rj.AppendPreparedConditionalBatch(
		generation, markerID, markerEpoch, txnID, entries, plan,
	)
}

// AppendPreparedConditionalBatch appends a conditional batch using a layout
// returned by PrepareConditionalBatch. It preserves AppendConditionalBatch's
// all-or-nothing framing and cursor semantics while eliminating repeated
// entry-length scans in callers that must preflight fit.
func (rj *RecoveryJournal) AppendPreparedConditionalBatch(
	generation uint64,
	markerID [16]byte,
	markerEpoch, txnID uint64,
	entries []RecoveryBatchEntry,
	plan RecoveryBatchPlan,
) (uint64, error) {
	if rj.header.FormatVersion != RecoveryJournalFormatConditional {
		return 0, fmt.Errorf(
			"%w: conditional batch requires conditional journal format",
			ErrInvalidWrite,
		)
	}
	if !plan.validForFormat(
		RecoveryJournalFormatConditional, rj.header.SectorSize, len(entries),
	) {
		return 0, fmt.Errorf("%w: conditional batch plan", ErrInvalidWrite)
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
		Kind:       recoveryRecordKindConditionalBatch,
		Entries:    entries,
		Conditional: RecoveryConditionalHeader{
			MarkerID:    markerID,
			MarkerEpoch: markerEpoch,
			TxnID:       txnID,
		},
	}
	if _, err := encodeRecoveryConditionalBatchRecordPrepared(
		rj.scratch[:plan.padded], rj.header.SectorSize, rec, plan,
	); err != nil {
		return 0, err
	}
	offset := int64(recoveryJournalRegionStart) + int64(rj.cursor)
	if _, err := rj.writeAt(rj.scratch[:plan.padded], offset); err != nil {
		return 0, err
	}
	rj.cursor = end
	sequence := rj.nextSequence
	rj.nextSequence++
	return sequence, nil
}
