package storeio

// This file contains the exact-root recovery vector format.  It is deliberately
// independent of the existing transaction marker and recovery-journal formats:
// callers must opt into it explicitly, and the ordinary root selector never
// consults this file.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

const (
	// RootVectorBankHeaderBytes leaves enough room for the fixed cut and
	// identity commitments while keeping every member descriptor and root image
	// at a deterministic offset.
	RootVectorBankHeaderBytes = 256
	RootVectorMemberBytes     = 64
	RootVectorRootBytes       = InlineSuperblockSize
	RootVectorBankChecksum    = sha256.Size
	RootVectorMaxMembers      = 60
	RootVectorFormat          = uint16(1)
)

var rootVectorMagic = [8]byte{'V', 'I', 'B', 'E', 'R', 'V', 'C', 0}

var (
	ErrRootVectorCorrupt        = errors.New("vibedb: corrupt exact-root vector bank")
	ErrRootVectorMissing        = errors.New("vibedb: exact-root vector has no complete bank")
	ErrRootVectorIdentity       = errors.New("vibedb: exact-root vector identity mismatch")
	ErrRootVectorSequence       = errors.New("vibedb: exact-root vector sequence mismatch")
	ErrRootVectorMember         = errors.New("vibedb: exact-root vector member mismatch")
	ErrRootVectorCut            = errors.New("vibedb: exact-root vector cut mismatch")
	ErrRootVectorBufferTooSmall = errors.New("vibedb: exact-root vector buffer too small")
)

// RootVectorCut is the authenticated logical cut shared by every member root
// in one bank.  Applied is the state-machine cut; Term and EntryDigest bind it
// to the authenticated Raft history.  Lineage and GroupID fence membership and
// ownership transitions.
type RootVectorCut struct {
	Applied     uint64
	Term        uint64
	EntryDigest [sha256.Size]byte
	Lineage     [sha256.Size]byte
	GroupID     [sha256.Size]byte
}

// RootVectorMember is a complete member descriptor and its exact 4 KiB inline
// root image.  NameDigest is the canonical catalog name commitment; raw names
// are intentionally outside the recovery format.
type RootVectorMember struct {
	NameDigest [sha256.Size]byte
	StoreID    [16]byte
	JournalID  [16]byte
	Root       InlineSuperblock
}

// RootVector is one complete, self-authenticating bank payload.  Sequence is a
// bank publication sequence and is not part of the logical cut.
type RootVector struct {
	Sequence uint64
	Cut      RootVectorCut
	Members  []RootVectorMember
}

// RootVectorMemberFloor is the minimum exact-root generation retained for one
// member across all still-selectable banks.
type RootVectorMemberFloor struct {
	NameDigest [sha256.Size]byte
	StoreID    [16]byte
	Generation uint64
}

const (
	rootVectorBankBytesOffset      = 12
	rootVectorMemberCountOffset    = 16
	rootVectorSequenceOffset       = 20
	rootVectorAppliedOffset        = 28
	rootVectorTermOffset           = 36
	rootVectorEntryDigestOffset    = 44
	rootVectorLineageOffset        = 76
	rootVectorGroupIDOffset        = 108
	rootVectorIdentityOffset       = 140
	rootVectorRootCommitmentOffset = 172
	rootVectorChecksumOffset       = 204
	rootVectorReservedOffset       = rootVectorChecksumOffset + RootVectorBankChecksum
	rootVectorDescriptorOffset     = RootVectorBankHeaderBytes
)

// RootVectorBankBytes returns the exact fixed bank size for memberCount.
func RootVectorBankBytes(memberCount int) (int, error) {
	if memberCount <= 0 || memberCount > RootVectorMaxMembers {
		return 0, fmt.Errorf("%w: member count %d", ErrRootVectorMember, memberCount)
	}
	used := RootVectorBankHeaderBytes + memberCount*(RootVectorMemberBytes+RootVectorRootBytes)
	return (used + InlineSuperblockSize - 1) &^ (InlineSuperblockSize - 1), nil
}

// RootVectorFileBytes returns the two-bank file size for memberCount.
func RootVectorFileBytes(memberCount int) (int, error) {
	bank, err := RootVectorBankBytes(memberCount)
	if err != nil {
		return 0, err
	}
	return 2 * bank, nil
}

// EncodeRootVectorBank writes the canonical complete bank into dst.  The
// caller may provide a larger scratch buffer; only the exact bank size is
// returned and any unused tail is left untouched.
func EncodeRootVectorBank(dst []byte, vector RootVector) ([]byte, error) {
	if err := validateRootVector(vector); err != nil {
		return nil, err
	}
	bankBytes, err := RootVectorBankBytes(len(vector.Members))
	if err != nil {
		return nil, err
	}
	if len(dst) < bankBytes {
		return nil, fmt.Errorf("%w: have=%d need=%d", ErrRootVectorBufferTooSmall, len(dst), bankBytes)
	}
	dst = dst[:bankBytes]
	clear(dst)
	copy(dst[:8], rootVectorMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], RootVectorFormat)
	binary.LittleEndian.PutUint16(dst[10:12], RootVectorBankHeaderBytes)
	binary.LittleEndian.PutUint32(dst[rootVectorBankBytesOffset:rootVectorBankBytesOffset+4], uint32(bankBytes))
	binary.LittleEndian.PutUint16(dst[rootVectorMemberCountOffset:rootVectorMemberCountOffset+2], uint16(len(vector.Members)))
	binary.LittleEndian.PutUint64(dst[rootVectorSequenceOffset:rootVectorSequenceOffset+8], vector.Sequence)
	binary.LittleEndian.PutUint64(dst[rootVectorAppliedOffset:rootVectorAppliedOffset+8], vector.Cut.Applied)
	binary.LittleEndian.PutUint64(dst[rootVectorTermOffset:rootVectorTermOffset+8], vector.Cut.Term)
	copy(dst[rootVectorEntryDigestOffset:rootVectorEntryDigestOffset+sha256.Size], vector.Cut.EntryDigest[:])
	copy(dst[rootVectorLineageOffset:rootVectorLineageOffset+sha256.Size], vector.Cut.Lineage[:])
	copy(dst[rootVectorGroupIDOffset:rootVectorGroupIDOffset+sha256.Size], vector.Cut.GroupID[:])

	identity := rootVectorIdentityCommitment(vector.Members)
	copy(dst[rootVectorIdentityOffset:rootVectorIdentityOffset+sha256.Size], identity[:])
	rootCommitment := rootVectorRootsCommitment(vector)
	copy(dst[rootVectorRootCommitmentOffset:rootVectorRootCommitmentOffset+sha256.Size], rootCommitment[:])

	for index, member := range vector.Members {
		descriptorOffset := rootVectorDescriptorOffset + index*RootVectorMemberBytes
		copy(dst[descriptorOffset:descriptorOffset+32], member.NameDigest[:])
		copy(dst[descriptorOffset+32:descriptorOffset+48], member.StoreID[:])
		copy(dst[descriptorOffset+48:descriptorOffset+64], member.JournalID[:])
		rootOffset := rootVectorDescriptorOffset + len(vector.Members)*RootVectorMemberBytes + index*RootVectorRootBytes
		if _, err := EncodeInlineSuperblock(dst[rootOffset:rootOffset+RootVectorRootBytes], member.Root); err != nil {
			return nil, fmt.Errorf("%w: member %d root: %v", ErrRootVectorCorrupt, index, err)
		}
	}
	checksum := rootVectorChecksum(dst)
	copy(dst[rootVectorChecksumOffset:rootVectorReservedOffset], checksum[:])
	return dst, nil
}

// DecodeRootVectorBank validates one complete bank independently.  A checksum
// valid but semantically mixed bank is rejected before any root image is
// returned.
func DecodeRootVectorBank(src []byte) (RootVector, error) {
	if len(src) < RootVectorBankHeaderBytes+RootVectorBankChecksum {
		return RootVector{}, fmt.Errorf("%w: short bank", ErrRootVectorCorrupt)
	}
	if string(src[:8]) != string(rootVectorMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != RootVectorFormat ||
		binary.LittleEndian.Uint16(src[10:12]) != RootVectorBankHeaderBytes {
		return RootVector{}, fmt.Errorf("%w: magic/version/header", ErrRootVectorCorrupt)
	}
	memberCount := int(binary.LittleEndian.Uint16(src[rootVectorMemberCountOffset : rootVectorMemberCountOffset+2]))
	bankBytes, err := RootVectorBankBytes(memberCount)
	if err != nil {
		return RootVector{}, err
	}
	if len(src) != bankBytes || int(binary.LittleEndian.Uint32(src[rootVectorBankBytesOffset:rootVectorBankBytesOffset+4])) != bankBytes {
		return RootVector{}, fmt.Errorf("%w: bank geometry", ErrRootVectorCorrupt)
	}
	storedChecksum := src[rootVectorChecksumOffset:rootVectorReservedOffset]
	checksum := rootVectorChecksum(src)
	if !bytes.Equal(storedChecksum, checksum[:]) {
		return RootVector{}, fmt.Errorf("%w: bank checksum", ErrRootVectorCorrupt)
	}
	if !allZero(src[18:20]) || !allZero(src[rootVectorReservedOffset:RootVectorBankHeaderBytes]) {
		return RootVector{}, fmt.Errorf("%w: reserved header bytes", ErrRootVectorCorrupt)
	}
	contentEnd := rootVectorDescriptorOffset + memberCount*(RootVectorMemberBytes+RootVectorRootBytes)
	if !allZero(src[contentEnd:]) {
		return RootVector{}, fmt.Errorf("%w: noncanonical bank padding", ErrRootVectorCorrupt)
	}
	vector := RootVector{
		Sequence: binary.LittleEndian.Uint64(src[rootVectorSequenceOffset : rootVectorSequenceOffset+8]),
		Cut: RootVectorCut{
			Applied: binary.LittleEndian.Uint64(src[rootVectorAppliedOffset : rootVectorAppliedOffset+8]),
			Term:    binary.LittleEndian.Uint64(src[rootVectorTermOffset : rootVectorTermOffset+8]),
		},
		Members: make([]RootVectorMember, memberCount),
	}
	copy(vector.Cut.EntryDigest[:], src[rootVectorEntryDigestOffset:rootVectorEntryDigestOffset+sha256.Size])
	copy(vector.Cut.Lineage[:], src[rootVectorLineageOffset:rootVectorLineageOffset+sha256.Size])
	copy(vector.Cut.GroupID[:], src[rootVectorGroupIDOffset:rootVectorGroupIDOffset+sha256.Size])
	if err := validateRootVectorHeader(vector); err != nil {
		return RootVector{}, err
	}
	for index := range vector.Members {
		descriptorOffset := rootVectorDescriptorOffset + index*RootVectorMemberBytes
		member := &vector.Members[index]
		copy(member.NameDigest[:], src[descriptorOffset:descriptorOffset+32])
		copy(member.StoreID[:], src[descriptorOffset+32:descriptorOffset+48])
		copy(member.JournalID[:], src[descriptorOffset+48:descriptorOffset+64])
		rootOffset := rootVectorDescriptorOffset + memberCount*RootVectorMemberBytes + index*RootVectorRootBytes
		member.Root, err = DecodeInlineSuperblock(src[rootOffset : rootOffset+RootVectorRootBytes])
		if err != nil {
			return RootVector{}, fmt.Errorf("%w: member %d root: %v", ErrRootVectorCorrupt, index, err)
		}
		if member.Root.StoreID != member.StoreID || member.Root.State.StoreID != member.StoreID ||
			member.Root.State.JournalID != member.JournalID {
			return RootVector{}, fmt.Errorf("%w: member %d root identity", ErrRootVectorMember, index)
		}
	}
	identity := rootVectorIdentityCommitment(vector.Members)
	if !bytes.Equal(src[rootVectorIdentityOffset:rootVectorIdentityOffset+sha256.Size], identity[:]) {
		return RootVector{}, fmt.Errorf("%w: member identity commitment", ErrRootVectorIdentity)
	}
	rootCommitment := rootVectorRootsCommitment(vector)
	if !bytes.Equal(src[rootVectorRootCommitmentOffset:rootVectorRootCommitmentOffset+sha256.Size], rootCommitment[:]) {
		return RootVector{}, fmt.Errorf("%w: root commitment", ErrRootVectorCorrupt)
	}
	if err := validateRootVector(vector); err != nil {
		return RootVector{}, err
	}
	return vector, nil
}

// SelectRootVectorBanks independently validates both complete banks and picks
// the newest compatible one. A torn newer bank is simply unavailable and the
// older complete bank is returned. Two complete banks with a foreign identity,
// a sequence gap, or a same-sequence disagreement fail closed.
func SelectRootVectorBanks(first, second []byte) (RootVector, int, error) {
	left, leftErr := DecodeRootVectorBank(first)
	right, rightErr := DecodeRootVectorBank(second)
	if leftErr != nil && rightErr != nil {
		return RootVector{}, -1, errors.Join(ErrRootVectorMissing, leftErr, rightErr)
	}
	if leftErr != nil {
		if !rootVectorSequenceMatchesSlot(right.Sequence, 1) {
			return RootVector{}, -1, ErrRootVectorSequence
		}
		return right, 1, nil
	}
	if rightErr != nil {
		if !rootVectorSequenceMatchesSlot(left.Sequence, 0) {
			return RootVector{}, -1, ErrRootVectorSequence
		}
		return left, 0, nil
	}
	if !rootVectorSequenceMatchesSlot(left.Sequence, 0) ||
		!rootVectorSequenceMatchesSlot(right.Sequence, 1) {
		return RootVector{}, -1, ErrRootVectorSequence
	}
	if !rootVectorSameIdentity(left, right) {
		return RootVector{}, -1, ErrRootVectorIdentity
	}
	if left.Sequence == right.Sequence {
		if !rootVectorSameContent(left, right) {
			return RootVector{}, -1, ErrRootVectorSequence
		}
		return left, 0, nil
	}
	if left.Sequence > right.Sequence {
		if left.Sequence != right.Sequence+1 {
			return RootVector{}, -1, ErrRootVectorSequence
		}
		if err := ValidateRootVectorSuccessor(right, left); err != nil {
			return RootVector{}, -1, err
		}
		return left, 0, nil
	}
	if right.Sequence != left.Sequence+1 {
		return RootVector{}, -1, ErrRootVectorSequence
	}
	if err := ValidateRootVectorSuccessor(left, right); err != nil {
		return RootVector{}, -1, err
	}
	return right, 1, nil
}

// RootVectorMemberFloors computes the retained generation floor for each
// member across all independently valid selectable banks. Both banks must
// carry the same fixed identity; a torn bank is ignored as unavailable.
func RootVectorMemberFloors(first, second []byte) ([]RootVectorMemberFloor, error) {
	left, leftErr := DecodeRootVectorBank(first)
	right, rightErr := DecodeRootVectorBank(second)
	if leftErr != nil && rightErr != nil {
		return nil, errors.Join(ErrRootVectorMissing, leftErr, rightErr)
	}
	var vectors []RootVector
	if leftErr == nil {
		vectors = append(vectors, left)
	}
	if rightErr == nil {
		vectors = append(vectors, right)
	}
	if len(vectors) == 2 && !rootVectorSameIdentity(vectors[0], vectors[1]) {
		return nil, ErrRootVectorIdentity
	}
	if len(vectors) == 2 {
		if _, _, err := SelectRootVectorBanks(first, second); err != nil {
			return nil, err
		}
	}
	result := make([]RootVectorMemberFloor, len(vectors[0].Members))
	for i, member := range vectors[0].Members {
		result[i] = RootVectorMemberFloor{
			NameDigest: member.NameDigest,
			StoreID:    member.StoreID,
			Generation: member.Root.Generation,
		}
	}
	if len(vectors) == 2 {
		for i, member := range vectors[1].Members {
			if member.NameDigest != result[i].NameDigest || member.StoreID != result[i].StoreID {
				return nil, ErrRootVectorIdentity
			}
			if member.Root.Generation < result[i].Generation {
				result[i].Generation = member.Root.Generation
			}
		}
	}
	return result, nil
}

// RootVectorBanksConverged reports whether both independently valid banks name
// the same complete logical cut and exact member roots. Different publication
// sequences are expected; any invalid or incompatible pair returns its error.
func RootVectorBanksConverged(first, second []byte) (bool, error) {
	if _, _, err := SelectRootVectorBanks(first, second); err != nil {
		return false, err
	}
	left, leftErr := DecodeRootVectorBank(first)
	right, rightErr := DecodeRootVectorBank(second)
	if leftErr != nil || rightErr != nil {
		return false, nil
	}
	return rootVectorSameContent(left, right), nil
}

func validateRootVectorHeader(vector RootVector) error {
	if vector.Sequence == 0 || vector.Cut.GroupID == ([sha256.Size]byte{}) ||
		vector.Cut.Lineage == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: sequence or cut identity", ErrRootVectorCut)
	}
	if vector.Cut.Applied == 0 {
		if vector.Cut.Term != 0 || vector.Cut.EntryDigest != ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: zero applied cut has Raft identity", ErrRootVectorCut)
		}
	} else if vector.Cut.Term == 0 || vector.Cut.EntryDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: applied cut lacks Raft identity", ErrRootVectorCut)
	}
	return nil
}

func validateRootVector(vector RootVector) error {
	if err := validateRootVectorHeader(vector); err != nil {
		return err
	}
	if len(vector.Members) == 0 || len(vector.Members) > RootVectorMaxMembers {
		return fmt.Errorf("%w: member count %d", ErrRootVectorMember, len(vector.Members))
	}
	ordered := slices.Clone(vector.Members)
	slices.SortFunc(ordered, compareRootVectorMembers)
	for index, member := range vector.Members {
		if member.NameDigest == ([sha256.Size]byte{}) || member.StoreID == ([16]byte{}) ||
			member.Root.StoreID != member.StoreID || member.Root.State.StoreID != member.StoreID ||
			member.Root.State.JournalID != member.JournalID {
			return fmt.Errorf("%w: member %d identity", ErrRootVectorMember, index)
		}
		if member.Root.Generation == 0 {
			return fmt.Errorf("%w: member %d root generation", ErrRootVectorMember, index)
		}
		for previous := 0; previous < index; previous++ {
			other := vector.Members[previous]
			if other.NameDigest == member.NameDigest || other.StoreID == member.StoreID ||
				member.JournalID != ([16]byte{}) && other.JournalID == member.JournalID {
				return fmt.Errorf("%w: duplicate member identity", ErrRootVectorMember)
			}
		}
	}
	for index := range vector.Members {
		if ordered[index].NameDigest != vector.Members[index].NameDigest ||
			ordered[index].StoreID != vector.Members[index].StoreID ||
			ordered[index].JournalID != vector.Members[index].JournalID {
			return fmt.Errorf("%w: noncanonical member order", ErrRootVectorMember)
		}
		if index != 0 && compareRootVectorMembers(vector.Members[index-1], vector.Members[index]) == 0 {
			return fmt.Errorf("%w: duplicate member", ErrRootVectorMember)
		}
	}
	return nil
}

func compareRootVectorMembers(left, right RootVectorMember) int {
	if result := bytes.Compare(left.NameDigest[:], right.NameDigest[:]); result != 0 {
		return result
	}
	if result := bytes.Compare(left.StoreID[:], right.StoreID[:]); result != 0 {
		return result
	}
	return bytes.Compare(left.JournalID[:], right.JournalID[:])
}

func rootVectorIdentityCommitment(members []RootVectorMember) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/exact-root-vector/identity/v1\x00"))
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(members)))
	_, _ = h.Write(scalar[:])
	for _, member := range members {
		_, _ = h.Write(member.NameDigest[:])
		_, _ = h.Write(member.StoreID[:])
		_, _ = h.Write(member.JournalID[:])
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func rootVectorRootsCommitment(vector RootVector) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/exact-root-vector/roots/v1\x00"))
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], vector.Sequence)
	_, _ = h.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], vector.Cut.Applied)
	_, _ = h.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], vector.Cut.Term)
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(vector.Cut.EntryDigest[:])
	_, _ = h.Write(vector.Cut.Lineage[:])
	_, _ = h.Write(vector.Cut.GroupID[:])
	for _, member := range vector.Members {
		_, _ = h.Write(member.NameDigest[:])
		_, _ = h.Write(member.StoreID[:])
		_, _ = h.Write(member.JournalID[:])
		var image [RootVectorRootBytes]byte
		if _, err := EncodeInlineSuperblock(image[:], member.Root); err != nil {
			// validateRootVector has already checked the root. A zero image here
			// makes the commitment deterministic even if a future caller bypasses
			// that check and then observes the resulting validation error.
			clear(image[:])
		}
		_, _ = h.Write(image[:])
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func rootVectorChecksum(bank []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/exact-root-vector/bank/v1\x00"))
	_, _ = h.Write(bank[:rootVectorChecksumOffset])
	var zero [RootVectorBankChecksum]byte
	_, _ = h.Write(zero[:])
	_, _ = h.Write(bank[rootVectorReservedOffset:])
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func rootVectorSequenceMatchesSlot(sequence uint64, slot int) bool {
	return sequence != 0 && slot >= 0 && slot < 2 && int((sequence-1)&1) == slot
}

// ValidateRootVectorSuccessor proves that newer can safely replace older while
// both banks remain selectable. Same-cut physical refreshes are allowed, but a
// Raft cut, term, or member root generation may never move backwards.
func ValidateRootVectorSuccessor(older, newer RootVector) error {
	if !rootVectorSameIdentity(older, newer) {
		return ErrRootVectorIdentity
	}
	if newer.Cut.Applied < older.Cut.Applied || newer.Cut.Term < older.Cut.Term {
		return ErrRootVectorCut
	}
	if newer.Cut.Applied == older.Cut.Applied &&
		(newer.Cut.Term != older.Cut.Term || newer.Cut.EntryDigest != older.Cut.EntryDigest) {
		return ErrRootVectorCut
	}
	for index := range older.Members {
		if newer.Members[index].Root.Generation < older.Members[index].Root.Generation {
			return ErrRootVectorSequence
		}
	}
	return nil
}

func rootVectorSameIdentity(left, right RootVector) bool {
	if left.Cut.GroupID != right.Cut.GroupID || left.Cut.Lineage != right.Cut.Lineage ||
		len(left.Members) != len(right.Members) {
		return false
	}
	for i := range left.Members {
		if left.Members[i].NameDigest != right.Members[i].NameDigest ||
			left.Members[i].StoreID != right.Members[i].StoreID ||
			left.Members[i].JournalID != right.Members[i].JournalID {
			return false
		}
	}
	return true
}

func rootVectorSameContent(left, right RootVector) bool {
	if !rootVectorSameIdentity(left, right) || left.Cut.Applied != right.Cut.Applied ||
		left.Cut.Term != right.Cut.Term || left.Cut.EntryDigest != right.Cut.EntryDigest {
		return false
	}
	for i := range left.Members {
		var l, r [RootVectorRootBytes]byte
		if _, err := EncodeInlineSuperblock(l[:], left.Members[i].Root); err != nil {
			return false
		}
		if _, err := EncodeInlineSuperblock(r[:], right.Members[i].Root); err != nil {
			return false
		}
		if !bytes.Equal(l[:], r[:]) {
			return false
		}
	}
	return true
}
