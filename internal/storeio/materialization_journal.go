package storeio

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// The materialization journal is recovery metadata for updating a canonical
// page in place. It is deliberately not a query overlay: readers never inspect
// it, and a writer may overwrite a canonical page only after one complete undo
// capsule has reached stable storage.
//
// The exact patch-only writer protocol is:
//
//  1. Build complete, checksum-valid after-images in memory and capture the
//     corresponding complete before-image sectors here.
//  2. Write the older of two fixed 4-KiB journal slots and synchronize it.
//  3. Positional-write only the captured sector ranges from the after-images,
//     publish TargetGeneration as the alternate root, then synchronize both
//     together.
//  4. Retain both capsules; never clear a slot merely to finish a successful
//     transaction. Recovery clears a failed capsule only after its complete
//     rollback and every exact-target root invalidation have crossed separate
//     durability fences.
//
// A hybrid batch that also contains unjournaled immutable COW pages retains a
// separate data barrier before root publication. The capsule cannot prove
// those full pages complete if their root reaches storage first.
//
// On recovery the newest checksum-valid sequence is resolved against the
// selected root. Therefore:
//
//   - a torn prospective capsule is not a commit and the previous slot remains
//     the recovery point;
//   - a valid capsule is complete before any target can have changed;
//   - when the selected root is older than TargetGeneration, the new root tore
//     and complete before-image sectors restore the canonical pages reachable
//     from the fallback root;
//   - when the selected root equals TargetGeneration, every target must match
//     the strong after-image descriptor before that root is accepted;
//   - when the selected root is newer than TargetGeneration, the capsule is
//     stale and recovery does not inspect extents that later commits may have
//     reused;
//   - resolving the previous completed transaction is harmless when a newer
//     prospective capsule tore before its durability barrier.
//
// The journal is intentionally bounded. A mutation whose changed spans do not
// fit one capsule must use copy-on-write; it must never grow a read-visible
// delta chain. SectorSize is explicit because an undo span must cover the
// device's complete possible power-loss damage granule. A device requiring
// 4-KiB undo cannot fit one full target plus capsule metadata and therefore
// takes the copy-on-write fallback.
const (
	MaterializationJournalSize       = 4096
	MaterializationJournalHeaderSize = 112
	MaterializationTargetRecordSize  = 56
	MaterializationPatchRecordSize   = 16
	// MaterializationJournalMinSectorSize is the smallest supported durable
	// write/damage granule. The capacity constants are exact at that granule:
	// every target owns at least one complete sector.
	MaterializationJournalMinSectorSize = 512
	MaterializationJournalMaxTargets    = 6
	MaterializationJournalMaxPatches    = 7
	MaterializationJournalMaxData       = 3584

	materializationJournalMagic   = "SJMTRL01"
	materializationJournalVersion = DevelopmentFormatVersion
	materializationJournalFlags   = uint32(0)
	materializationJournalCommit  = uint64(0x313054494d4d4f43) // "COMMIT01", little-endian.

	materializationTargetOffset = MaterializationJournalHeaderSize
	materializationTrailerSize  = 8
	materializationTrailerAt    = MaterializationJournalSize - materializationTrailerSize
)

var (
	// ErrMaterializationJournalCorrupt reports a non-empty capsule whose
	// framing, checksum, canonical packing, target references, or patches are
	// invalid.
	ErrMaterializationJournalCorrupt = errors.New("vibejson: corrupt Store materialization journal")
	// ErrMaterializationJournalNotFound reports an empty slot or two slots with
	// no checksum-valid committed capsule. An invalid capsule never authorizes
	// target writes, so SelectMaterializationJournal may safely treat it as an
	// interrupted pre-barrier write.
	ErrMaterializationJournalNotFound = errors.New("vibejson: no valid Store materialization journal")
	// ErrMaterializationJournalConflict reports two individually valid slots
	// that cannot belong to one monotonic Store history.
	ErrMaterializationJournalConflict = errors.New("vibejson: conflicting Store materialization journals")
	// ErrMaterializationTargetDiverged reports target bytes outside every
	// recorded patch that do not match the image from which the capsule was
	// built. Recovery must not write that page.
	ErrMaterializationTargetDiverged = errors.New("vibejson: materialization target diverged")
)

// MaterializationJournalHeader identifies one committed recovery capsule.
// Sequence is strictly monotonic across capsules. TargetGeneration is the new
// Store root generation and acts as the transaction commit marker: recovery
// rolls back only when its selected root generation is lower. SectorSize is the
// device's complete possible power-loss damage granule.
type MaterializationJournalHeader struct {
	StoreID          [16]byte
	Sequence         uint64
	TargetGeneration uint64
	PageSize         uint32
	SectorSize       uint32
}

// MaterializationTarget is one exact canonical page and the bounded integrity
// values needed for idempotent torn-page recovery.
//
// BeforeChecksum is the input image's already-verified common-page checksum.
// ContextChecksum is the checksum of the complete page with every undo sector
// replaced by zero bytes; it proves that all bytes outside the captured damage
// granules still belong to this target even when a torn write makes neither
// full-image checksum match.
// AfterPatchDigest is a 96-bit domain-separated SHA-256 truncation over every
// bounded after-image sector and its geometry. Together with the common page
// checksum it strongly proves the only bytes the commit may have torn, without
// hashing the full canonical page an extra time on the write hot path.
type MaterializationTarget struct {
	// StoreID is carried in memory so Encode can reject a target built from a
	// different Store. It is encoded once in the transaction header rather
	// than repeated in every target record.
	StoreID          [16]byte
	Ref              PageRef
	BeforeChecksum   uint32
	ContextChecksum  uint32
	AfterPatchDigest [12]byte

	// builtAfterChecksum and builtMarker are transient proof metadata. Only
	// BuildMaterializationTarget sets them, Encode never persists them, and
	// decoded journal targets leave them zero. The trusted builder/stager path
	// can therefore reuse the common-page checksum OpenPage already verified
	// without weakening the journal-only API or durable recovery.
	builtAfterChecksum uint32
	builtMarker        uint32
}

// MaterializationPatch carries one or more complete before-image sectors.
// Encode requires patches in (Target, Offset) order, without overlap, aligned
// to Header.SectorSize. Data is copied into the capsule and need not remain
// live afterward.
type MaterializationPatch struct {
	Target uint16
	Offset uint32
	Data   []byte
}

// MaterializationJournalView is a checksum-verified borrowed capsule. It owns
// no heap memory and retains src until the caller discards the view.
type MaterializationJournalView struct {
	src         []byte
	header      MaterializationJournalHeader
	targetCount uint16
	patchCount  uint16
	patchOffset uint32
	dataOffset  uint32
	dataLength  uint32
}

// MaterializationRollbackResult distinguishes a root that committed, an
// already-restored fallback page, and a page that recovery rolled back.
type MaterializationRollbackResult uint8

const (
	MaterializationRollbackApplied MaterializationRollbackResult = iota + 1
	MaterializationAlreadyRolledBack
	MaterializationRollbackNotNeeded
)

// EncodeMaterializationJournal writes one deterministic committed capsule.
// Inputs must not alias dst. The function clears and checksum-seals the full
// 4-KiB slot and allocates no memory.
func EncodeMaterializationJournal(
	dst []byte,
	header MaterializationJournalHeader,
	targets []MaterializationTarget,
	patches []MaterializationPatch,
) ([]byte, error) {
	if len(dst) < MaterializationJournalSize {
		return nil, fmt.Errorf("%w: journal buffer has %d bytes", ErrInvalidWrite, len(dst))
	}
	if err := validateMaterializationJournalHeader(header); err != nil {
		return nil, err
	}
	if len(targets) == 0 || len(targets) > MaterializationJournalMaxTargets ||
		len(patches) == 0 || len(patches) > MaterializationJournalMaxPatches {
		return nil, fmt.Errorf("%w: materialization target or patch count", ErrInvalidWrite)
	}

	patchOffset64 := uint64(materializationTargetOffset) +
		uint64(len(targets))*MaterializationTargetRecordSize
	dataOffset64 := patchOffset64 + uint64(len(patches))*MaterializationPatchRecordSize
	var dataLength64 uint64
	for _, patch := range patches {
		dataLength64 += uint64(len(patch.Data))
	}
	if patchOffset64 > materializationTrailerAt ||
		dataOffset64 > materializationTrailerAt ||
		dataLength64 > materializationTrailerAt-dataOffset64 {
		return nil, fmt.Errorf("%w: materialization capsule capacity", ErrInvalidWrite)
	}
	patchOffset := uint32(patchOffset64)
	dataOffset := uint32(dataOffset64)
	dataLength := uint32(dataLength64)

	if err := validateMaterializationTargetsForEncode(header, targets); err != nil {
		return nil, err
	}
	if err := validateMaterializationPatchesForEncode(header, targets, patches, dataLength); err != nil {
		return nil, err
	}

	slot := dst[:MaterializationJournalSize]
	clear(slot)
	copy(slot[0:8], materializationJournalMagic)
	binary.LittleEndian.PutUint32(slot[8:12], materializationJournalVersion)
	binary.LittleEndian.PutUint32(slot[12:16], MaterializationJournalSize)
	binary.LittleEndian.PutUint64(slot[16:24], materializationJournalCommit)
	binary.LittleEndian.PutUint64(slot[24:32], ^materializationJournalCommit)
	copy(slot[32:48], header.StoreID[:])
	binary.LittleEndian.PutUint64(slot[48:56], header.Sequence)
	binary.LittleEndian.PutUint64(slot[56:64], ^header.Sequence)
	binary.LittleEndian.PutUint64(slot[64:72], header.TargetGeneration)
	binary.LittleEndian.PutUint64(slot[72:80], ^header.TargetGeneration)
	binary.LittleEndian.PutUint32(slot[80:84], header.PageSize)
	binary.LittleEndian.PutUint16(slot[84:86], uint16(len(targets)))
	binary.LittleEndian.PutUint16(slot[86:88], uint16(len(patches)))
	binary.LittleEndian.PutUint32(slot[88:92], materializationTargetOffset)
	binary.LittleEndian.PutUint32(slot[92:96], patchOffset)
	binary.LittleEndian.PutUint32(slot[96:100], dataOffset)
	binary.LittleEndian.PutUint32(slot[100:104], dataLength)
	binary.LittleEndian.PutUint32(slot[104:108], materializationJournalFlags)
	binary.LittleEndian.PutUint32(slot[108:112], header.SectorSize)

	patchRank := 0
	for targetRank, target := range targets {
		record := slot[materializationTargetOffset+targetRank*MaterializationTargetRecordSize:]
		encodePageRef(record[0:PageRefSize], target.Ref)
		binary.LittleEndian.PutUint32(record[32:36], target.BeforeChecksum)
		binary.LittleEndian.PutUint32(record[36:40], target.ContextChecksum)
		copy(record[40:52], target.AfterPatchDigest[:])
		first := patchRank
		for patchRank < len(patches) && int(patches[patchRank].Target) == targetRank {
			patchRank++
		}
		binary.LittleEndian.PutUint16(record[52:54], uint16(first))
		binary.LittleEndian.PutUint16(record[54:56], uint16(patchRank-first))
	}

	dataCursor := uint32(0)
	for rank, patch := range patches {
		record := slot[int(patchOffset)+rank*MaterializationPatchRecordSize:]
		binary.LittleEndian.PutUint16(record[0:2], patch.Target)
		binary.LittleEndian.PutUint16(record[2:4], uint16(len(patch.Data)))
		binary.LittleEndian.PutUint32(record[4:8], patch.Offset)
		binary.LittleEndian.PutUint32(record[8:12], dataCursor)
		copy(slot[int(dataOffset+dataCursor):], patch.Data)
		dataCursor += uint32(len(patch.Data))
	}
	checksum := PageChecksum(slot[:materializationTrailerAt])
	binary.LittleEndian.PutUint32(slot[materializationTrailerAt:materializationTrailerAt+4], checksum)
	binary.LittleEndian.PutUint32(slot[materializationTrailerAt+4:], ^checksum)
	return slot, nil
}

// OpenMaterializationJournal validates one committed capsule. Success borrows
// src and allocates no memory.
func OpenMaterializationJournal(src []byte) (MaterializationJournalView, error) {
	if len(src) < MaterializationJournalSize {
		return MaterializationJournalView{}, fmt.Errorf("%w: short capsule", ErrMaterializationJournalCorrupt)
	}
	src = src[:MaterializationJournalSize:MaterializationJournalSize]
	if string(src[0:8]) != materializationJournalMagic {
		if allZero(src) {
			return MaterializationJournalView{}, ErrMaterializationJournalNotFound
		}
		return MaterializationJournalView{}, fmt.Errorf("%w: magic", ErrMaterializationJournalCorrupt)
	}
	if binary.LittleEndian.Uint32(src[8:12]) != materializationJournalVersion ||
		binary.LittleEndian.Uint32(src[12:16]) != MaterializationJournalSize ||
		binary.LittleEndian.Uint64(src[16:24]) != materializationJournalCommit ||
		binary.LittleEndian.Uint64(src[24:32]) != ^materializationJournalCommit {
		return MaterializationJournalView{}, fmt.Errorf("%w: header or commit marker", ErrMaterializationJournalCorrupt)
	}
	checksum := binary.LittleEndian.Uint32(src[materializationTrailerAt : materializationTrailerAt+4])
	if binary.LittleEndian.Uint32(src[materializationTrailerAt+4:]) != ^checksum ||
		PageChecksum(src[:materializationTrailerAt]) != checksum {
		return MaterializationJournalView{}, fmt.Errorf("%w: checksum", ErrMaterializationJournalCorrupt)
	}

	var header MaterializationJournalHeader
	copy(header.StoreID[:], src[32:48])
	header.Sequence = binary.LittleEndian.Uint64(src[48:56])
	header.TargetGeneration = binary.LittleEndian.Uint64(src[64:72])
	header.PageSize = binary.LittleEndian.Uint32(src[80:84])
	if binary.LittleEndian.Uint64(src[56:64]) != ^header.Sequence ||
		binary.LittleEndian.Uint64(src[72:80]) != ^header.TargetGeneration ||
		binary.LittleEndian.Uint32(src[104:108]) != materializationJournalFlags {
		return MaterializationJournalView{}, fmt.Errorf("%w: complement or flags", ErrMaterializationJournalCorrupt)
	}
	header.SectorSize = binary.LittleEndian.Uint32(src[108:112])
	if err := validateMaterializationJournalHeader(header); err != nil {
		return MaterializationJournalView{}, fmt.Errorf("%w: %v", ErrMaterializationJournalCorrupt, err)
	}

	targetCount := binary.LittleEndian.Uint16(src[84:86])
	patchCount := binary.LittleEndian.Uint16(src[86:88])
	targetOffset := binary.LittleEndian.Uint32(src[88:92])
	patchOffset := binary.LittleEndian.Uint32(src[92:96])
	dataOffset := binary.LittleEndian.Uint32(src[96:100])
	dataLength := binary.LittleEndian.Uint32(src[100:104])
	wantPatchOffset := uint64(materializationTargetOffset) +
		uint64(targetCount)*MaterializationTargetRecordSize
	wantDataOffset := wantPatchOffset + uint64(patchCount)*MaterializationPatchRecordSize
	dataEnd := uint64(dataOffset) + uint64(dataLength)
	if targetCount == 0 || targetCount > MaterializationJournalMaxTargets ||
		patchCount == 0 || patchCount > MaterializationJournalMaxPatches ||
		targetOffset != materializationTargetOffset ||
		uint64(patchOffset) != wantPatchOffset ||
		uint64(dataOffset) != wantDataOffset ||
		dataEnd > materializationTrailerAt ||
		!allZero(src[dataEnd:materializationTrailerAt]) {
		return MaterializationJournalView{}, fmt.Errorf("%w: non-canonical record layout", ErrMaterializationJournalCorrupt)
	}

	view := MaterializationJournalView{
		src: src, header: header, targetCount: targetCount, patchCount: patchCount,
		patchOffset: patchOffset, dataOffset: dataOffset, dataLength: dataLength,
	}
	if err := view.validateRecords(); err != nil {
		return MaterializationJournalView{}, err
	}
	return view, nil
}

// SelectMaterializationJournal returns the newest valid capsule from two
// alternating slots. Invalid slots are ignored: under the required protocol a
// checksum-invalid prospective capsule never passed the barrier that permits a
// target write. If neither slot is valid, there is nothing to replay.
func SelectMaterializationJournal(
	first, second []byte,
) (MaterializationJournalView, int, error) {
	if len(first) < MaterializationJournalSize || len(second) < MaterializationJournalSize {
		return MaterializationJournalView{}, -1,
			fmt.Errorf("%w: short alternating slot", ErrMaterializationJournalCorrupt)
	}
	a, errA := OpenMaterializationJournal(first)
	b, errB := OpenMaterializationJournal(second)
	validA := errA == nil
	validB := errB == nil
	switch {
	case validA && !validB:
		return a, 0, nil
	case !validA && validB:
		return b, 1, nil
	case !validA && !validB:
		return MaterializationJournalView{}, -1, ErrMaterializationJournalNotFound
	}
	if a.header.StoreID != b.header.StoreID ||
		a.header.PageSize != b.header.PageSize ||
		a.header.SectorSize != b.header.SectorSize ||
		a.header.Sequence == b.header.Sequence {
		return MaterializationJournalView{}, -1, ErrMaterializationJournalConflict
	}
	if a.header.Sequence > b.header.Sequence {
		if a.header.TargetGeneration <= b.header.TargetGeneration {
			return MaterializationJournalView{}, -1, ErrMaterializationJournalConflict
		}
		return a, 0, nil
	}
	if b.header.TargetGeneration <= a.header.TargetGeneration {
		return MaterializationJournalView{}, -1, ErrMaterializationJournalConflict
	}
	return b, 1, nil
}

// BuildMaterializationTarget verifies complete old and new page images against
// ref, proves that complete before-image sectors cover every changed byte, and
// computes the descriptor checksums without allocation.
func BuildMaterializationTarget(
	journal MaterializationJournalHeader,
	ref PageRef,
	before, after []byte,
	patches []MaterializationPatch,
	target uint16,
) (MaterializationTarget, error) {
	if err := validateMaterializationJournalHeader(journal); err != nil {
		return MaterializationTarget{}, err
	}
	if err := validateMaterializationTargetRef(journal, ref); err != nil {
		return MaterializationTarget{}, err
	}
	if len(before) != int(ref.Length) || len(after) != int(ref.Length) {
		return MaterializationTarget{}, fmt.Errorf("%w: materialization image length", ErrInvalidWrite)
	}
	beforeHeader, _, err := OpenPage(before)
	if err != nil || !materializationPageHeaderMatchesRef(beforeHeader, ref) {
		return MaterializationTarget{}, fmt.Errorf("%w: materialization before image", ErrInvalidWrite)
	}
	afterHeader, _, err := OpenPage(after)
	if err != nil || afterHeader.StoreID != beforeHeader.StoreID ||
		!materializationPageHeaderMatchesRef(afterHeader, ref) {
		return MaterializationTarget{}, fmt.Errorf("%w: materialization after image", ErrInvalidWrite)
	}
	if beforeHeader.StoreID != journal.StoreID {
		return MaterializationTarget{}, fmt.Errorf("%w: materialization image Store identity", ErrInvalidWrite)
	}
	cursor := uint32(0)
	found := false
	for _, patch := range patches {
		if patch.Target != target {
			continue
		}
		if len(patch.Data) == 0 || len(patch.Data) > int(^uint16(0)) ||
			patch.Offset%journal.SectorSize != 0 ||
			uint32(len(patch.Data))%journal.SectorSize != 0 ||
			found && patch.Offset <= cursor ||
			uint64(patch.Offset)+uint64(len(patch.Data)) > uint64(ref.Length) ||
			!bytes.Equal(before[cursor:patch.Offset], after[cursor:patch.Offset]) ||
			!bytes.Equal(patch.Data, before[patch.Offset:uint32(uint64(patch.Offset)+uint64(len(patch.Data)))]) {
			return MaterializationTarget{}, fmt.Errorf("%w: materialization patch coverage", ErrInvalidWrite)
		}
		cursor = patch.Offset + uint32(len(patch.Data))
		found = true
	}
	if !found || !bytes.Equal(before[cursor:], after[cursor:]) {
		return MaterializationTarget{}, fmt.Errorf("%w: incomplete materialization patch coverage", ErrInvalidWrite)
	}
	context, err := materializationContextChecksumFromInput(before, patches, target)
	if err != nil {
		return MaterializationTarget{}, err
	}
	afterPatchDigest, err := materializationAfterPatchDigestFromInput(
		after, patches, target,
	)
	if err != nil {
		return MaterializationTarget{}, err
	}
	built := MaterializationTarget{
		StoreID:          beforeHeader.StoreID,
		Ref:              ref,
		BeforeChecksum:   binary.LittleEndian.Uint32(before[len(before)-PageTrailerSize:]),
		ContextChecksum:  context,
		AfterPatchDigest: afterPatchDigest,
		builtAfterChecksum: binary.LittleEndian.Uint32(
			after[len(after)-PageTrailerSize:],
		),
	}
	built.builtMarker = ^built.builtAfterChecksum
	return built, nil
}

func materializationTargetDurableEqual(
	first, second MaterializationTarget,
) bool {
	return first.StoreID == second.StoreID &&
		first.Ref == second.Ref &&
		first.BeforeChecksum == second.BeforeChecksum &&
		first.ContextChecksum == second.ContextChecksum &&
		first.AfterPatchDigest == second.AfterPatchDigest
}

// Header returns value-only capsule metadata.
func (v MaterializationJournalView) Header() MaterializationJournalHeader { return v.header }

// Len returns the number of canonical target pages in this transaction.
func (v MaterializationJournalView) Len() int { return int(v.targetCount) }

// PatchLen returns the number of before-image sector spans in this transaction.
func (v MaterializationJournalView) PatchLen() int { return int(v.patchCount) }

// DataLen returns the exact before-image bytes carried by the capsule.
func (v MaterializationJournalView) DataLen() int { return int(v.dataLength) }

// TargetAt returns one value-only target descriptor.
func (v MaterializationJournalView) TargetAt(rank int) (MaterializationTarget, bool) {
	if rank < 0 || rank >= int(v.targetCount) {
		return MaterializationTarget{}, false
	}
	record := v.src[materializationTargetOffset+rank*MaterializationTargetRecordSize:]
	return decodeMaterializationTarget(record, v.header.StoreID), true
}

// PatchAt returns one borrowed before-image sector patch.
func (v MaterializationJournalView) PatchAt(rank int) (MaterializationPatch, bool) {
	if rank < 0 || rank >= int(v.patchCount) {
		return MaterializationPatch{}, false
	}
	record := v.src[int(v.patchOffset)+rank*MaterializationPatchRecordSize:]
	dataStart := v.dataOffset + binary.LittleEndian.Uint32(record[8:12])
	length := uint32(binary.LittleEndian.Uint16(record[2:4]))
	return MaterializationPatch{
		Target: binary.LittleEndian.Uint16(record[0:2]),
		Offset: binary.LittleEndian.Uint32(record[4:8]),
		Data:   v.src[dataStart : dataStart+length : dataStart+length],
	}, true
}

// TargetPatchRange returns the contiguous patch ranks owned by one target.
// Recovery uses it to persist only the sectors Rollback restored.
func (v MaterializationJournalView) TargetPatchRange(targetRank int) (first, count int, ok bool) {
	if targetRank < 0 || targetRank >= int(v.targetCount) {
		return 0, 0, false
	}
	record := v.src[materializationTargetOffset+targetRank*MaterializationTargetRecordSize:]
	return int(binary.LittleEndian.Uint16(record[52:54])),
		int(binary.LittleEndian.Uint16(record[54:56])), true
}

// NeedsRollback reports whether the selected durable root predates this
// capsule's target generation. It performs no page access.
func (v MaterializationJournalView) NeedsRollback(recoveredRootGeneration uint64) (bool, error) {
	if recoveredRootGeneration == 0 {
		return false, fmt.Errorf("%w: zero recovered root generation", ErrInvalidWrite)
	}
	return recoveredRootGeneration < v.header.TargetGeneration, nil
}

// RecoverTarget is the low-level rollback half of recovery. A target/newer
// root returns MaterializationRollbackNotNeeded without inspecting page; the
// root selector must separately call ValidateAfterImage before accepting an
// exact-target root. An older fallback root rolls page back from complete
// before-image sectors.
func (v MaterializationJournalView) RecoverTarget(
	recoveredRootGeneration uint64,
	targetRank int,
	page []byte,
) (MaterializationRollbackResult, error) {
	needs, err := v.NeedsRollback(recoveredRootGeneration)
	if err != nil {
		return 0, err
	}
	if !needs {
		return MaterializationRollbackNotNeeded, nil
	}
	return v.Rollback(targetRank, page)
}

// ValidateAfterImage proves that one root-generation-equal target is the
// complete image described by the durable capsule. Recovery must validate
// every target before accepting a root whose generation exactly equals the
// journal target. A newer root deliberately skips this check because the
// retained capsule can be stale after later safe extent reuse.
func (v MaterializationJournalView) ValidateAfterImage(
	targetRank int,
	page []byte,
) error {
	target, ok := v.TargetAt(targetRank)
	if !ok || len(page) != int(target.Ref.Length) {
		return fmt.Errorf("%w: materialization target rank or length", ErrInvalidWrite)
	}
	digest, err := v.afterPatchDigest(targetRank, page)
	if err != nil {
		return err
	}
	if v.contextChecksum(targetRank, page) != target.ContextChecksum ||
		digest != target.AfterPatchDigest {
		return ErrMaterializationTargetDiverged
	}
	header, _, err := OpenPage(page)
	if err != nil || header.StoreID != v.header.StoreID ||
		!materializationPageHeaderMatchesRef(header, target.Ref) {
		return ErrMaterializationTargetDiverged
	}
	return nil
}

// Rollback restores one target in caller-owned page scratch. It accepts the
// pristine before-image, the complete after-image, or an arbitrary torn
// mixture inside recorded sectors. Bytes outside those sectors must match the
// capsule's context checksum. On error, the scratch must be discarded.
//
// Rollback validates the final common page envelope and exact PageRef identity.
// The caller must persist only the view's recorded patch ranges from page, then
// synchronize all restored targets before exposing the fallback root. Writing
// the whole page would exceed the damage coverage the bounded capsule proves.
// If recovery itself tears, the retained capsule makes the same rollback
// idempotent on the next open. The success path allocates no memory.
func (v MaterializationJournalView) Rollback(
	targetRank int,
	page []byte,
) (MaterializationRollbackResult, error) {
	target, ok := v.TargetAt(targetRank)
	if !ok || len(page) != int(target.Ref.Length) {
		return 0, fmt.Errorf("%w: materialization target rank or length", ErrInvalidWrite)
	}
	first, count, _ := v.TargetPatchRange(targetRank)
	already := true
	for rank := first; rank < first+count; rank++ {
		patch, _ := v.PatchAt(rank)
		if !bytes.Equal(page[patch.Offset:uint32(uint64(patch.Offset)+uint64(len(patch.Data)))], patch.Data) {
			already = false
			break
		}
	}
	if already &&
		binary.LittleEndian.Uint32(page[len(page)-PageTrailerSize:]) == target.BeforeChecksum {
		if err := v.validateRolledBackPage(target.Ref, page); err != nil {
			return 0, err
		}
		return MaterializationAlreadyRolledBack, nil
	}
	if v.contextChecksum(targetRank, page) != target.ContextChecksum {
		return 0, ErrMaterializationTargetDiverged
	}
	for rank := first; rank < first+count; rank++ {
		patch, _ := v.PatchAt(rank)
		copy(page[patch.Offset:], patch.Data)
	}
	if binary.LittleEndian.Uint32(page[len(page)-PageTrailerSize:]) != target.BeforeChecksum {
		return 0, fmt.Errorf("%w: before-image checksum", ErrMaterializationJournalCorrupt)
	}
	if err := v.validateRolledBackPage(target.Ref, page); err != nil {
		return 0, err
	}
	return MaterializationRollbackApplied, nil
}

func validateMaterializationJournalHeader(header MaterializationJournalHeader) error {
	if header.StoreID == ([16]byte{}) || header.Sequence == 0 ||
		header.TargetGeneration == 0 || !validPhysicalPageSize(header.PageSize) ||
		header.SectorSize < MaterializationJournalMinSectorSize ||
		header.SectorSize&(header.SectorSize-1) != 0 ||
		header.SectorSize > header.PageSize || header.PageSize%header.SectorSize != 0 {
		return fmt.Errorf("%w: materialization journal identity", ErrInvalidWrite)
	}
	return nil
}

func validateMaterializationTargetsForEncode(
	header MaterializationJournalHeader,
	targets []MaterializationTarget,
) error {
	var previousEnd uint64
	for rank, target := range targets {
		if target.StoreID != header.StoreID {
			return fmt.Errorf("%w: materialization target Store identity", ErrInvalidWrite)
		}
		if err := validateMaterializationTargetRef(header, target.Ref); err != nil {
			return err
		}
		if rank != 0 && target.Ref.Offset < previousEnd {
			return fmt.Errorf("%w: materialization target order or overlap", ErrInvalidWrite)
		}
		previousEnd = target.Ref.Offset + uint64(target.Ref.Length)
	}
	return nil
}

func validateMaterializationTargetRef(
	header MaterializationJournalHeader,
	ref PageRef,
) error {
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		return err
	}
	if ref.LogicalID <= StateRootLogicalID || ref.Generation == 0 ||
		ref.Generation >= header.TargetGeneration ||
		ref.Kind == PageStateRoot || ref.Kind == PageCatalogSegment ||
		ref.Kind >= PagePrimaryCatalog && ref.Kind <= PagePrimaryLeaf ||
		!validPageKind(ref.Kind) || !validPageFlags(ref.Kind, ref.Flags) ||
		ref.Aux != 0 || ref.Length < header.PageSize ||
		ref.Length%header.PageSize != 0 || !validPageExtentSize(ref.Kind, ref.Length) ||
		ref.Offset < layout.DataStart ||
		ref.Offset%uint64(header.PageSize) != 0 ||
		ref.Offset > maxSuperblockFileOffset ||
		uint64(ref.Length) > maxSuperblockFileOffset-ref.Offset {
		return fmt.Errorf("%w: materialization target reference", ErrInvalidWrite)
	}
	return nil
}

func validateMaterializationPatchesForEncode(
	header MaterializationJournalHeader,
	targets []MaterializationTarget,
	patches []MaterializationPatch,
	dataLength uint32,
) error {
	var dataCursor uint32
	var previousTarget uint16
	var previousEnd uint32
	var seenTargets uint64
	for rank, patch := range patches {
		if int(patch.Target) >= len(targets) ||
			len(patch.Data) == 0 || len(patch.Data) > int(^uint16(0)) ||
			patch.Offset%header.SectorSize != 0 ||
			uint32(len(patch.Data))%header.SectorSize != 0 ||
			uint64(patch.Offset)+uint64(len(patch.Data)) > uint64(targets[patch.Target].Ref.Length) {
			return fmt.Errorf("%w: materialization patch bounds", ErrInvalidWrite)
		}
		if rank != 0 && (patch.Target < previousTarget ||
			patch.Target == previousTarget && patch.Offset <= previousEnd) {
			return fmt.Errorf("%w: materialization patch order, overlap, or adjacency", ErrInvalidWrite)
		}
		if patch.Target != previousTarget {
			previousEnd = 0
		}
		previousTarget = patch.Target
		previousEnd = patch.Offset + uint32(len(patch.Data))
		dataCursor += uint32(len(patch.Data))
		seenTargets |= uint64(1) << patch.Target
	}
	if dataCursor != dataLength {
		return fmt.Errorf("%w: materialization patch data length", ErrInvalidWrite)
	}
	if seenTargets != uint64(1)<<len(targets)-1 {
		return fmt.Errorf("%w: materialization target without patch", ErrInvalidWrite)
	}
	return nil
}

func (v MaterializationJournalView) validateRecords() error {
	var previousEnd uint64
	for rank := 0; rank < int(v.targetCount); rank++ {
		record := v.src[materializationTargetOffset+rank*MaterializationTargetRecordSize:]
		target := decodeMaterializationTarget(record, v.header.StoreID)
		if err := validateMaterializationTargetRef(v.header, target.Ref); err != nil ||
			rank != 0 && target.Ref.Offset < previousEnd {
			return fmt.Errorf("%w: target record", ErrMaterializationJournalCorrupt)
		}
		previousEnd = target.Ref.Offset + uint64(target.Ref.Length)
		first := binary.LittleEndian.Uint16(record[52:54])
		count := binary.LittleEndian.Uint16(record[54:56])
		if count == 0 || int(first)+int(count) > int(v.patchCount) {
			return fmt.Errorf("%w: target patch range", ErrMaterializationJournalCorrupt)
		}
		if rank == 0 && first != 0 {
			return fmt.Errorf("%w: first target patch range", ErrMaterializationJournalCorrupt)
		}
		if rank != 0 {
			previous := v.src[materializationTargetOffset+(rank-1)*MaterializationTargetRecordSize:]
			want := binary.LittleEndian.Uint16(previous[52:54]) +
				binary.LittleEndian.Uint16(previous[54:56])
			if first != want {
				return fmt.Errorf("%w: non-canonical target patch range", ErrMaterializationJournalCorrupt)
			}
		}
		if rank == int(v.targetCount)-1 && int(first)+int(count) != int(v.patchCount) {
			return fmt.Errorf("%w: trailing unowned patch", ErrMaterializationJournalCorrupt)
		}
		for patchRank := int(first); patchRank < int(first)+int(count); patchRank++ {
			patchRecord := v.src[int(v.patchOffset)+patchRank*MaterializationPatchRecordSize:]
			if binary.LittleEndian.Uint16(patchRecord[0:2]) != uint16(rank) {
				return fmt.Errorf("%w: target does not own patch range", ErrMaterializationJournalCorrupt)
			}
		}
	}

	var dataCursor uint32
	var previousTarget uint16
	var previousPatchEnd uint32
	for rank := 0; rank < int(v.patchCount); rank++ {
		record := v.src[int(v.patchOffset)+rank*MaterializationPatchRecordSize:]
		target := binary.LittleEndian.Uint16(record[0:2])
		length := uint32(binary.LittleEndian.Uint16(record[2:4]))
		offset := binary.LittleEndian.Uint32(record[4:8])
		dataAt := binary.LittleEndian.Uint32(record[8:12])
		if int(target) >= int(v.targetCount) || length == 0 ||
			offset%v.header.SectorSize != 0 || length%v.header.SectorSize != 0 ||
			dataAt != dataCursor || uint64(dataAt)+uint64(length) > uint64(v.dataLength) ||
			!allZero(record[12:16]) {
			return fmt.Errorf("%w: patch record", ErrMaterializationJournalCorrupt)
		}
		targetRecord := v.src[materializationTargetOffset+int(target)*MaterializationTargetRecordSize:]
		targetRef := decodePageRef(targetRecord[0:PageRefSize])
		if uint64(offset)+uint64(length) > uint64(targetRef.Length) ||
			rank != 0 && (target < previousTarget ||
				target == previousTarget && offset <= previousPatchEnd) {
			return fmt.Errorf("%w: patch order, overlap, adjacency, or bounds", ErrMaterializationJournalCorrupt)
		}
		if target != previousTarget {
			previousPatchEnd = 0
		}
		previousTarget = target
		previousPatchEnd = offset + length
		dataCursor += length
	}
	if dataCursor != v.dataLength {
		return fmt.Errorf("%w: patch data packing", ErrMaterializationJournalCorrupt)
	}
	return nil
}

func decodeMaterializationTarget(src []byte, storeID [16]byte) MaterializationTarget {
	target := MaterializationTarget{
		StoreID:         storeID,
		Ref:             decodePageRef(src[0:PageRefSize]),
		BeforeChecksum:  binary.LittleEndian.Uint32(src[32:36]),
		ContextChecksum: binary.LittleEndian.Uint32(src[36:40]),
	}
	copy(target.AfterPatchDigest[:], src[40:52])
	return target
}

func materializationPageHeaderMatchesRef(header PageHeader, ref PageRef) bool {
	return header.LogicalID == ref.LogicalID &&
		header.Generation == ref.Generation &&
		header.PageSize == ref.Length &&
		header.Kind == ref.Kind &&
		header.Flags == ref.Flags
}

func (v MaterializationJournalView) validateRolledBackPage(ref PageRef, page []byte) error {
	header, _, err := OpenPage(page)
	if err != nil || header.StoreID != v.header.StoreID ||
		!materializationPageHeaderMatchesRef(header, ref) {
		return fmt.Errorf("%w: rolled-back page identity or envelope", ErrMaterializationJournalCorrupt)
	}
	return nil
}

func (v MaterializationJournalView) contextChecksum(targetRank int, page []byte) uint32 {
	first, count, _ := v.TargetPatchRange(targetRank)
	checksum := uint32(0)
	cursor := uint32(0)
	for rank := first; rank < first+count; rank++ {
		patch, _ := v.PatchAt(rank)
		checksum = crc32.Update(checksum, pageChecksumTable, page[cursor:patch.Offset])
		checksum = materializationUpdateZeroes(checksum, len(patch.Data))
		cursor = patch.Offset + uint32(len(patch.Data))
	}
	return crc32.Update(checksum, pageChecksumTable, page[cursor:])
}

func materializationContextChecksumFromInput(
	page []byte,
	patches []MaterializationPatch,
	target uint16,
) (uint32, error) {
	checksum := uint32(0)
	cursor := uint32(0)
	found := false
	for _, patch := range patches {
		if patch.Target != target {
			continue
		}
		if len(patch.Data) == 0 || found && patch.Offset <= cursor ||
			uint64(patch.Offset)+uint64(len(patch.Data)) > uint64(len(page)) {
			return 0, fmt.Errorf("%w: materialization context spans", ErrInvalidWrite)
		}
		checksum = crc32.Update(checksum, pageChecksumTable, page[cursor:patch.Offset])
		checksum = materializationUpdateZeroes(checksum, len(patch.Data))
		cursor = patch.Offset + uint32(len(patch.Data))
		found = true
	}
	if !found {
		return 0, fmt.Errorf("%w: materialization context without patches", ErrInvalidWrite)
	}
	return crc32.Update(checksum, pageChecksumTable, page[cursor:]), nil
}

const materializationAfterPatchDigestDomain = "MATAP002"

func materializationAfterPatchDigestFromInput(
	page []byte,
	patches []MaterializationPatch,
	target uint16,
) ([12]byte, error) {
	var material [len(materializationAfterPatchDigestDomain) + 4 +
		MaterializationJournalMaxPatches*(8+sha256.Size)]byte
	cursor := copy(material[:], materializationAfterPatchDigestDomain)
	countAt := cursor
	cursor += 4
	count := 0
	dataLength := 0
	for _, patch := range patches {
		if patch.Target != target {
			continue
		}
		end := uint64(patch.Offset) + uint64(len(patch.Data))
		dataLength += len(patch.Data)
		if count >= MaterializationJournalMaxPatches ||
			dataLength > MaterializationJournalMaxData ||
			len(patch.Data) == 0 || end > uint64(len(page)) ||
			cursor+8+sha256.Size > len(material) {
			return [12]byte{}, fmt.Errorf("%w: materialization after-patch digest", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint32(material[cursor:cursor+4], patch.Offset)
		binary.LittleEndian.PutUint32(
			material[cursor+4:cursor+8], uint32(len(patch.Data)),
		)
		cursor += 8
		patchDigest := sha256.Sum256(page[patch.Offset:uint32(end)])
		copy(material[cursor:], patchDigest[:])
		cursor += len(patchDigest)
		count++
	}
	if count == 0 {
		return [12]byte{}, fmt.Errorf("%w: materialization after-patch digest", ErrInvalidWrite)
	}
	binary.LittleEndian.PutUint32(material[countAt:countAt+4], uint32(count))
	sum := sha256.Sum256(material[:cursor])
	var digest [12]byte
	copy(digest[:], sum[:len(digest)])
	return digest, nil
}

func (v MaterializationJournalView) afterPatchDigest(
	targetRank int,
	page []byte,
) ([12]byte, error) {
	var material [len(materializationAfterPatchDigestDomain) + 4 +
		MaterializationJournalMaxPatches*(8+sha256.Size)]byte
	cursor := copy(material[:], materializationAfterPatchDigestDomain)
	countAt := cursor
	cursor += 4
	first, count, ok := v.TargetPatchRange(targetRank)
	if !ok || count == 0 || count > MaterializationJournalMaxPatches {
		return [12]byte{}, fmt.Errorf("%w: materialization after-patch digest", ErrMaterializationJournalCorrupt)
	}
	for rank := first; rank < first+count; rank++ {
		patch, patchOK := v.PatchAt(rank)
		end := uint64(patch.Offset) + uint64(len(patch.Data))
		if !patchOK || end > uint64(len(page)) ||
			cursor+8+sha256.Size > len(material) {
			return [12]byte{}, fmt.Errorf("%w: materialization after-patch digest", ErrMaterializationJournalCorrupt)
		}
		binary.LittleEndian.PutUint32(material[cursor:cursor+4], patch.Offset)
		binary.LittleEndian.PutUint32(
			material[cursor+4:cursor+8], uint32(len(patch.Data)),
		)
		cursor += 8
		patchDigest := sha256.Sum256(page[patch.Offset:uint32(end)])
		copy(material[cursor:], patchDigest[:])
		cursor += len(patchDigest)
	}
	binary.LittleEndian.PutUint32(material[countAt:countAt+4], uint32(count))
	sum := sha256.Sum256(material[:cursor])
	var digest [12]byte
	copy(digest[:], sum[:len(digest)])
	return digest, nil
}

var materializationZeroes [256]byte

func materializationUpdateZeroes(checksum uint32, length int) uint32 {
	for length > len(materializationZeroes) {
		checksum = crc32.Update(checksum, pageChecksumTable, materializationZeroes[:])
		length -= len(materializationZeroes)
	}
	return crc32.Update(checksum, pageChecksumTable, materializationZeroes[:length])
}
