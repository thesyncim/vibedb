package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	generationSealPayloadBytes = 472
	retainedPayloadHeaderBytes = 24
)

var (
	generationSealMagic = [8]byte{'V', 'D', 'B', 'R', 'G', 'E', 'N', 0}
	retainedMagic       = [8]byte{'V', 'D', 'B', 'R', 'K', 'E', 'E', 'P'}

	generationFamilyDomain   = []byte("vibedb/raft-wal/family/fixed\x00")
	generationIdentityDomain = []byte(
		"vibedb/raft-wal/generation-identity/fixed\x00",
	)
	generationConfDomain = []byte(
		"vibedb/raft-wal/generation-conf-state/fixed\x00",
	)
	generationSuffixDomain = []byte(
		"vibedb/raft-wal/retained-suffix/fixed\x00",
	)
	generationBindingDomain = []byte(
		"vibedb/raft-wal/generation-binding/fixed\x00",
	)
)

// generationSeal is the authenticated, fixed-width handoff from one selected
// source cut to one staged compacted generation. The target static
// header independently seals the complete member identity, bounds, key
// locator, topology epoch, and snapshot base. This record binds those target
// facts to the exact source current slot and retained semantic suffix.
type generationSeal struct {
	familyID                 [16]byte
	generation               uint64
	parentBindingDigest      [sha256.Size]byte
	identityDigest           [sha256.Size]byte
	sourceFileID             [16]byte
	sourceHeaderDigest       [sha256.Size]byte
	sourceCurrentGeneration  uint64
	sourceWALEnd             uint64
	sourceRecordSequence     uint64
	sourceChainDigest        [sha256.Size]byte
	sourceCurrentIncarnation uint64
	topologyRecoveryEpoch    uint64
	baseIndex                uint64
	baseTerm                 uint64
	baseDigest               [sha256.Size]byte
	confDigest               [sha256.Size]byte
	retentionCommitment      [sha256.Size]byte
	hard                     *pb.HardState
	suffixFirst              uint64
	suffixLast               uint64
	suffixCount              uint64
	suffixBytes              uint64
	suffixDigest             [sha256.Size]byte
	sourceFirst              uint64
	sourceLast               uint64
	bindingDigest            [sha256.Size]byte
}

func marshalGenerationSeal(seal generationSeal) ([]byte, error) {
	if err := validateGenerationSealStatic(seal); err != nil {
		return nil, err
	}
	wantBinding := generationBindingDigest(seal)
	if seal.bindingDigest != wantBinding {
		return nil, fmt.Errorf("%w: generation binding digest", ErrInvalid)
	}
	result := make([]byte, 0, generationSealPayloadBytes)
	result = append(result, generationSealMagic[:]...)
	result = appendUint16(result, codecVersion)
	result = appendUint16(result, 0)
	result = append(result, seal.familyID[:]...)
	result = appendUint64(result, seal.generation)
	result = append(result, seal.parentBindingDigest[:]...)
	result = append(result, seal.identityDigest[:]...)
	result = append(result, seal.sourceFileID[:]...)
	result = append(result, seal.sourceHeaderDigest[:]...)
	result = appendUint64(result, seal.sourceCurrentGeneration)
	result = appendUint64(result, seal.sourceWALEnd)
	result = appendUint64(result, seal.sourceRecordSequence)
	result = append(result, seal.sourceChainDigest[:]...)
	result = appendUint64(result, seal.sourceCurrentIncarnation)
	result = appendUint64(result, seal.topologyRecoveryEpoch)
	result = appendUint64(result, seal.baseIndex)
	result = appendUint64(result, seal.baseTerm)
	result = append(result, seal.baseDigest[:]...)
	result = append(result, seal.confDigest[:]...)
	result = append(result, seal.retentionCommitment[:]...)
	result = appendUint64(result, seal.hard.GetTerm())
	result = appendUint64(result, seal.hard.GetVote())
	result = appendUint64(result, seal.hard.GetCommit())
	result = appendUint64(result, seal.suffixFirst)
	result = appendUint64(result, seal.suffixLast)
	result = appendUint64(result, seal.suffixCount)
	result = appendUint64(result, seal.suffixBytes)
	result = append(result, seal.suffixDigest[:]...)
	result = appendUint64(result, seal.sourceFirst)
	result = appendUint64(result, seal.sourceLast)
	result = append(result, seal.bindingDigest[:]...)
	result = appendUint32(result, 0)
	if len(result) != generationSealPayloadBytes {
		return nil, fmt.Errorf("%w: generation seal width %d", ErrInvalid, len(result))
	}
	return result, nil
}

func unmarshalGenerationSeal(data []byte) (generationSeal, error) {
	if len(data) != generationSealPayloadBytes || !bytes.Equal(data[:8], generationSealMagic[:]) {
		return generationSeal{}, fmt.Errorf("%w: generation seal envelope", ErrCorrupt)
	}
	reader := decoder{data: data[8:]}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return generationSeal{}, fmt.Errorf("%w: generation seal version", ErrCorrupt)
	}
	flags, err := reader.u16()
	if err != nil || flags != 0 {
		return generationSeal{}, fmt.Errorf("%w: generation seal flags", ErrCorrupt)
	}
	var seal generationSeal
	if err := readGenerationFixed(&reader, seal.familyID[:]); err != nil {
		return generationSeal{}, err
	}
	seal.generation, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.parentBindingDigest[:]); err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.identityDigest[:]); err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.sourceFileID[:]); err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.sourceHeaderDigest[:]); err != nil {
		return generationSeal{}, err
	}
	seal.sourceCurrentGeneration, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.sourceWALEnd, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.sourceRecordSequence, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.sourceChainDigest[:]); err != nil {
		return generationSeal{}, err
	}
	seal.sourceCurrentIncarnation, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.topologyRecoveryEpoch, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.baseIndex, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.baseTerm, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.baseDigest[:]); err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.confDigest[:]); err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.retentionCommitment[:]); err != nil {
		return generationSeal{}, err
	}
	hardTerm, err := reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	hardVote, err := reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	hardCommit, err := reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.hard = &pb.HardState{
		Term: uint64Pointer(hardTerm), Vote: uint64Pointer(hardVote), Commit: uint64Pointer(hardCommit),
	}
	seal.suffixFirst, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.suffixLast, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.suffixCount, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.suffixBytes, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.suffixDigest[:]); err != nil {
		return generationSeal{}, err
	}
	seal.sourceFirst, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	seal.sourceLast, err = reader.u64()
	if err != nil {
		return generationSeal{}, err
	}
	if err := readGenerationFixed(&reader, seal.bindingDigest[:]); err != nil {
		return generationSeal{}, err
	}
	reserved, err := reader.u32()
	if err != nil || reserved != 0 {
		return generationSeal{}, fmt.Errorf("%w: generation seal reserved", ErrCorrupt)
	}
	if err := reader.done(); err != nil {
		return generationSeal{}, err
	}
	if err := validateGenerationSealStatic(seal); err != nil {
		return generationSeal{}, fmt.Errorf("%w: generation seal: %v", ErrCorrupt, err)
	}
	if seal.bindingDigest != generationBindingDigest(seal) {
		return generationSeal{}, fmt.Errorf("%w: generation binding digest", ErrCorrupt)
	}
	return seal, nil
}

func readGenerationFixed(reader *decoder, destination []byte) error {
	value, err := reader.take(len(destination))
	if err != nil {
		return err
	}
	copy(destination, value)
	return nil
}

func validateGenerationSealStatic(seal generationSeal) error {
	if seal.familyID == ([16]byte{}) || seal.generation == 0 ||
		seal.identityDigest == ([sha256.Size]byte{}) || seal.sourceFileID == ([16]byte{}) ||
		seal.sourceHeaderDigest == ([sha256.Size]byte{}) || seal.sourceCurrentGeneration == 0 ||
		seal.sourceWALEnd < HeaderBytes+recordDamageGranule ||
		seal.sourceRecordSequence == 0 || seal.sourceChainDigest == ([sha256.Size]byte{}) ||
		seal.sourceCurrentIncarnation == 0 || seal.topologyRecoveryEpoch == 0 ||
		seal.baseIndex == 0 || seal.baseIndex == math.MaxUint64 || seal.baseTerm == 0 ||
		seal.baseTerm == math.MaxUint64 || seal.baseDigest == ([sha256.Size]byte{}) ||
		seal.confDigest == ([sha256.Size]byte{}) || seal.retentionCommitment == ([sha256.Size]byte{}) ||
		seal.hard == nil || seal.hard.GetTerm() == 0 || seal.hard.GetTerm() == math.MaxUint64 ||
		seal.hard.GetCommit() < seal.baseIndex || seal.suffixFirst != seal.baseIndex+1 ||
		seal.suffixLast < seal.baseIndex || seal.suffixCount != seal.suffixLast-seal.baseIndex ||
		seal.suffixDigest == ([sha256.Size]byte{}) || seal.sourceFirst == 0 ||
		seal.sourceLast < seal.suffixLast || seal.bindingDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: impossible generation seal", ErrInvalid)
	}
	if seal.generation == FirstWALGeneration {
		if seal.parentBindingDigest != ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: first generation has a parent", ErrInvalid)
		}
	} else if seal.parentBindingDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: later generation lacks a parent", ErrInvalid)
	}
	if seal.hard.GetVote() != 0 && raft.IsLocalMsgTarget(seal.hard.GetVote()) {
		return fmt.Errorf("%w: generation HardState vote", ErrInvalid)
	}
	return nil
}

func generationBindingDigest(seal generationSeal) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(generationBindingDomain)
	_, _ = h.Write(seal.familyID[:])
	writeGenerationU64(h, seal.generation)
	_, _ = h.Write(seal.parentBindingDigest[:])
	_, _ = h.Write(seal.identityDigest[:])
	_, _ = h.Write(seal.sourceFileID[:])
	_, _ = h.Write(seal.sourceHeaderDigest[:])
	writeGenerationU64(h, seal.sourceCurrentGeneration)
	writeGenerationU64(h, seal.sourceWALEnd)
	writeGenerationU64(h, seal.sourceRecordSequence)
	_, _ = h.Write(seal.sourceChainDigest[:])
	writeGenerationU64(h, seal.sourceCurrentIncarnation)
	writeGenerationU64(h, seal.topologyRecoveryEpoch)
	writeGenerationU64(h, seal.baseIndex)
	writeGenerationU64(h, seal.baseTerm)
	_, _ = h.Write(seal.baseDigest[:])
	_, _ = h.Write(seal.confDigest[:])
	_, _ = h.Write(seal.retentionCommitment[:])
	writeGenerationU64(h, seal.hard.GetTerm())
	writeGenerationU64(h, seal.hard.GetVote())
	writeGenerationU64(h, seal.hard.GetCommit())
	writeGenerationU64(h, seal.suffixFirst)
	writeGenerationU64(h, seal.suffixLast)
	writeGenerationU64(h, seal.suffixCount)
	writeGenerationU64(h, seal.suffixBytes)
	_, _ = h.Write(seal.suffixDigest[:])
	writeGenerationU64(h, seal.sourceFirst)
	writeGenerationU64(h, seal.sourceLast)
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func generationIdentityDigest(identity Identity) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(generationIdentityDomain)
	_, _ = h.Write(identity.ClusterID[:])
	_, _ = h.Write(identity.ClusterIncarnation[:])
	writeGenerationString(h, identity.Distribution)
	writeGenerationString(h, identity.Shard)
	writeGenerationU64(h, identity.AllocationGeneration)
	_, _ = h.Write(identity.ShardIncarnation[:])
	_, _ = h.Write(identity.GroupID[:])
	writeGenerationU64(h, identity.MemberID)
	_, _ = h.Write(identity.StoreID[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func generationFamilyID(base string, identity Identity) [16]byte {
	h := sha256.New()
	_, _ = h.Write(generationFamilyDomain)
	writeGenerationString(h, base)
	identityDigest := generationIdentityDigest(identity)
	_, _ = h.Write(identityDigest[:])
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	var result [16]byte
	copy(result[:], digest[:len(result)])
	return result
}

func generationConfDigest(conf *pb.ConfState) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(generationConfDomain)
	writeGenerationIDs(h, conf.GetVoters())
	writeGenerationIDs(h, conf.GetLearners())
	writeGenerationIDs(h, conf.GetVotersOutgoing())
	writeGenerationIDs(h, conf.GetLearnersNext())
	var presence [2]byte
	if conf != nil && conf.AutoLeave != nil {
		presence[0] = 1
		if conf.GetAutoLeave() {
			presence[1] = 1
		}
	}
	_, _ = h.Write(presence[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func writeGenerationIDs(h hash.Hash, values []uint64) {
	writeGenerationU64(h, uint64(len(values)))
	for _, value := range values {
		writeGenerationU64(h, value)
	}
}

func writeGenerationString(h hash.Hash, value string) {
	writeGenerationU64(h, uint64(len(value)))
	_, _ = io.WriteString(h, value)
}

func writeGenerationU64(h hash.Hash, value uint64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	_, _ = h.Write(encoded[:])
}

type retainedSuffixHasher struct {
	hash  hash.Hash
	frame [32]byte
}

func newRetainedSuffixHash(baseIndex, baseTerm uint64) *retainedSuffixHasher {
	h := &retainedSuffixHasher{hash: sha256.New()}
	_, _ = h.hash.Write(generationSuffixDomain)
	writeGenerationU64(h.hash, baseIndex)
	writeGenerationU64(h.hash, baseTerm)
	return h
}

func (h *retainedSuffixHasher) add(entry *pb.Entry) {
	binary.LittleEndian.PutUint32(h.frame[0:4], uint32(entry.GetType()))
	binary.LittleEndian.PutUint64(h.frame[8:16], entry.GetTerm())
	binary.LittleEndian.PutUint64(h.frame[16:24], entry.GetIndex())
	binary.LittleEndian.PutUint64(h.frame[24:32], uint64(len(entry.GetData())))
	_, _ = h.hash.Write(h.frame[:])
	_, _ = h.hash.Write(entry.GetData())
}

func (h *retainedSuffixHasher) finish() [sha256.Size]byte {
	var result [sha256.Size]byte
	_ = h.hash.Sum(result[:0])
	return result
}

func marshalRetainedEntries(entries []*pb.Entry) ([]byte, error) {
	if len(entries) == 0 || len(entries) > MaxReadyEntries {
		return nil, fmt.Errorf("%w: retained entry count", ErrBounds)
	}
	capacity := retainedPayloadHeaderBytes
	var dataBytes uint64
	for _, entry := range entries {
		if entry == nil || len(entry.ProtoReflect().GetUnknown()) != 0 ||
			entry.GetType() < pb.EntryNormal || entry.GetType() > pb.EntryConfChangeV2 ||
			entry.GetIndex() == 0 || entry.GetIndex() == math.MaxUint64 ||
			entry.GetTerm() == 0 || entry.GetTerm() == math.MaxUint64 ||
			len(entry.GetData()) > raftmodel.MaxProposalBytes ||
			capacity > math.MaxInt-32-len(entry.GetData()) {
			return nil, fmt.Errorf("%w: retained entry geometry", ErrBounds)
		}
		capacity += 32 + len(entry.GetData())
		dataBytes += uint64(len(entry.GetData()))
	}
	result := make([]byte, 0, capacity)
	result = append(result, retainedMagic[:]...)
	result = appendUint16(result, codecVersion)
	result = appendUint16(result, 0)
	result = appendUint32(result, uint32(len(entries)))
	result = appendUint64(result, dataBytes)
	for _, entry := range entries {
		result = appendUint32(result, uint32(entry.GetType()))
		result = appendUint32(result, 0)
		result = appendUint64(result, entry.GetTerm())
		result = appendUint64(result, entry.GetIndex())
		result = appendUint32(result, uint32(len(entry.GetData())))
		result = appendUint32(result, 0)
		result = append(result, entry.GetData()...)
	}
	return result, nil
}

func unmarshalRetainedEntries(data []byte, options normalizedOptions) ([]*pb.Entry, error) {
	return unmarshalRetainedEntriesMode(data, options, true)
}

// unmarshalRetainedEntriesView avoids a second payload copy while an offline
// generation validates or compacts its private scratch stream. Returned entry
// data aliases data.
func unmarshalRetainedEntriesView(data []byte, options normalizedOptions) ([]*pb.Entry, error) {
	return unmarshalRetainedEntriesMode(data, options, false)
}

func unmarshalRetainedEntriesMode(
	data []byte,
	options normalizedOptions,
	detach bool,
) ([]*pb.Entry, error) {
	if len(data) < retainedPayloadHeaderBytes || !bytes.Equal(data[:8], retainedMagic[:]) {
		return nil, fmt.Errorf("%w: retained payload envelope", ErrCorrupt)
	}
	reader := decoder{data: data[8:]}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return nil, fmt.Errorf("%w: retained payload version", ErrCorrupt)
	}
	flags, err := reader.u16()
	if err != nil || flags != 0 {
		return nil, fmt.Errorf("%w: retained payload flags", ErrCorrupt)
	}
	count, err := reader.u32()
	if err != nil || count == 0 || count > MaxReadyEntries || uint64(count) > options.maxEntries {
		return nil, fmt.Errorf("%w: retained entry count", ErrCorrupt)
	}
	wantDataBytes, err := reader.u64()
	if err != nil || wantDataBytes > uint64(options.maxLiveBytes) ||
		uint64(count) > uint64(len(data)-retainedPayloadHeaderBytes)/32 {
		return nil, fmt.Errorf("%w: retained payload geometry", ErrCorrupt)
	}
	entries := make([]*pb.Entry, int(count))
	var dataBytes uint64
	for position := range entries {
		entryType, decodeErr := reader.u32()
		if decodeErr != nil {
			return nil, decodeErr
		}
		reservedType, decodeErr := reader.u32()
		if decodeErr != nil || reservedType != 0 || entryType > uint32(pb.EntryConfChangeV2) {
			return nil, fmt.Errorf("%w: retained entry type", ErrCorrupt)
		}
		term, decodeErr := reader.u64()
		if decodeErr != nil {
			return nil, decodeErr
		}
		index, decodeErr := reader.u64()
		if decodeErr != nil {
			return nil, decodeErr
		}
		length, decodeErr := reader.u32()
		if decodeErr != nil || length > raftmodel.MaxProposalBytes ||
			dataBytes > uint64(options.maxLiveBytes)-uint64(length) {
			return nil, fmt.Errorf("%w: retained entry bytes", ErrCorrupt)
		}
		reservedEntry, decodeErr := reader.u32()
		if decodeErr != nil || reservedEntry != 0 {
			return nil, fmt.Errorf("%w: retained entry reserved", ErrCorrupt)
		}
		payload, decodeErr := reader.take(int(length))
		if decodeErr != nil {
			return nil, decodeErr
		}
		typeValue := pb.EntryType(entryType)
		entryData := payload
		if detach {
			entryData = append([]byte(nil), payload...)
		}
		entries[position] = &pb.Entry{
			Type: entryTypePointer(typeValue), Term: uint64Pointer(term), Index: uint64Pointer(index),
			Data: entryData,
		}
		dataBytes += uint64(length)
	}
	if err := reader.done(); err != nil {
		return nil, err
	}
	if dataBytes != wantDataBytes {
		return nil, fmt.Errorf("%w: retained data length", ErrCorrupt)
	}
	return entries, nil
}
