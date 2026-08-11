package raftstore

import (
	"bytes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	staticPrefixBytes           = 64
	currentPrefixBytes          = 96
	sectorChecksumBytes         = 8
	sectorChecksumOffset        = StaticHeaderBytes - sectorChecksumBytes
	cipherSuiteAES256GCM uint16 = 1
)

var (
	staticMagic  = [8]byte{'V', 'D', 'B', 'R', 'W', 'A', 'L', 0}
	currentMagic = [8]byte{'V', 'D', 'B', 'R', 'C', 'U', 'R', 0}
	crcTable     = crc32.MakeTable(crc32.Castagnoli)
)

type headerState struct {
	identity              Identity
	keyID                 string
	wrapped               []byte
	fileID                [16]byte
	headerNonce           [12]byte
	dataKey               [32]byte
	nonceKey              [32]byte
	headerDigest          [32]byte
	reference             snapshotReference
	bounds                formatBounds
	topologyRecoveryEpoch uint64
	snapshot              *pb.Snapshot
	snapshotBytes         []byte
}

type retryKey struct {
	incarnation uint64
	readyID     uint64
}

type currentState struct {
	activeSlot            int
	generation            uint64
	walEnd                int64
	recordSequence        uint64
	chainDigest           [32]byte
	currentIncarnation    uint64
	hard                  *pb.HardState
	first                 uint64
	last                  uint64
	retryPresent          bool
	retry                 retryKey
	retryDigest           [32]byte
	snapshotID            [16]byte
	snapshotIndex         uint64
	snapshotTerm          uint64
	snapshotSize          uint64
	snapshotChunks        uint32
	snapshotDigest        [32]byte
	topologyRecoveryEpoch uint64
}

type slotDecode struct {
	state      currentState
	absent     bool
	torn       bool
	generation uint64
}

func marshalStaticHeader(identity Identity, key Key, bootstrap Bootstrap, options normalizedOptions) ([]byte, headerState, error) {
	if err := validateIdentity(identity); err != nil {
		return nil, headerState{}, err
	}
	if err := validateKey(key, true); err != nil {
		return nil, headerState{}, err
	}
	bootstrapPayload, snapshotBytes, err := marshalBootstrap(bootstrap, identity.MemberID)
	if err != nil {
		return nil, headerState{}, err
	}
	var fileID [16]byte
	var snapshotID [16]byte
	if err := readFresh(options.random, fileID[:]); err != nil {
		return nil, headerState{}, err
	}
	if err := readFresh(options.random, snapshotID[:]); err != nil {
		return nil, headerState{}, err
	}
	fileCrypto, err := makeFileCrypto(key, fileID)
	if err != nil {
		return nil, headerState{}, err
	}
	aead := fileCrypto.aead
	nonce := deriveNonce(fileCrypto.nonceKey, "static-header", 0)
	reference := snapshotReference{
		id: snapshotID, digest: sha256.Sum256(bootstrapPayload), size: uint64(len(bootstrapPayload)),
		index: bootstrap.Snapshot.GetMetadata().GetIndex(), term: bootstrap.Snapshot.GetMetadata().GetTerm(),
	}
	payload, err := marshalIdentity(identity, reference, boundsFromOptions(options))
	if err != nil {
		return nil, headerState{}, err
	}
	cipherLength := len(payload) + aead.Overhead()
	locatorLength := len(key.ID) + len(key.Wrapped)
	if staticPrefixBytes+locatorLength+cipherLength > sectorChecksumOffset {
		return nil, headerState{}, fmt.Errorf("%w: static header payload", ErrBounds)
	}
	header := make([]byte, StaticHeaderBytes)
	copy(header[0:8], staticMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], codecVersion)
	binary.LittleEndian.PutUint16(header[10:12], StaticHeaderBytes)
	binary.LittleEndian.PutUint16(header[16:18], uint16(len(key.ID)))
	binary.LittleEndian.PutUint16(header[18:20], uint16(len(key.Wrapped)))
	binary.LittleEndian.PutUint32(header[20:24], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[24:28], uint32(cipherLength))
	copy(header[28:40], nonce[:])
	copy(header[40:56], fileID[:])
	binary.LittleEndian.PutUint16(header[56:58], staticPrefixBytes)
	binary.LittleEndian.PutUint16(header[58:60], cipherSuiteAES256GCM)
	position := staticPrefixBytes
	copy(header[position:], key.ID)
	position += len(key.ID)
	copy(header[position:], key.Wrapped)
	position += len(key.Wrapped)
	aad := header[:staticPrefixBytes+locatorLength]
	ciphertext := aead.Seal(nil, nonce[:], payload, aad)
	copy(header[position:], ciphertext)
	sealSectorChecksum(header)
	ownedIdentity := identity
	ownedIdentity.Distribution = strings.Clone(identity.Distribution)
	ownedIdentity.Shard = strings.Clone(identity.Shard)
	state := headerState{
		identity: ownedIdentity, keyID: strings.Clone(key.ID), wrapped: slices.Clone(key.Wrapped), fileID: fileID,
		headerNonce: nonce, dataKey: fileCrypto.dataKey, nonceKey: fileCrypto.nonceKey, headerDigest: sha256.Sum256(header), reference: reference,
		bounds:                boundsFromOptions(options),
		topologyRecoveryEpoch: bootstrap.TopologyRecoveryEpoch,
		snapshot:              cloneSnapshot(bootstrap.Snapshot), snapshotBytes: snapshotBytes,
	}
	return header, state, nil
}

func unmarshalStaticHeader(header []byte, expected Identity, key Key, options normalizedOptions) (headerState, cipher.AEAD, error) {
	if len(header) != StaticHeaderBytes {
		return headerState{}, nil, fmt.Errorf("%w: static header length", ErrCorrupt)
	}
	if !validSectorChecksum(header) {
		return headerState{}, nil, fmt.Errorf("%w: static header checksum", ErrCorrupt)
	}
	if !bytes.Equal(header[0:8], staticMagic[:]) || binary.LittleEndian.Uint16(header[8:10]) != codecVersion ||
		binary.LittleEndian.Uint16(header[10:12]) != StaticHeaderBytes || binary.LittleEndian.Uint32(header[12:16]) != 0 ||
		binary.LittleEndian.Uint16(header[56:58]) != staticPrefixBytes || binary.LittleEndian.Uint16(header[58:60]) != cipherSuiteAES256GCM ||
		!allZero(header[60:64]) {
		return headerState{}, nil, fmt.Errorf("%w: static header envelope", ErrCorrupt)
	}
	keyLength := int(binary.LittleEndian.Uint16(header[16:18]))
	wrappedLength := int(binary.LittleEndian.Uint16(header[18:20]))
	plainLength := int(binary.LittleEndian.Uint32(header[20:24]))
	cipherLength := int(binary.LittleEndian.Uint32(header[24:28]))
	if keyLength == 0 || keyLength > MaxKeyIDBytes || wrappedLength == 0 || wrappedLength > MaxWrappedKeyBytes ||
		plainLength <= 0 || cipherLength != plainLength+16 || staticPrefixBytes+keyLength+wrappedLength+cipherLength > sectorChecksumOffset {
		return headerState{}, nil, fmt.Errorf("%w: static header geometry", ErrCorrupt)
	}
	end := staticPrefixBytes + keyLength + wrappedLength + cipherLength
	if !allZero(header[end:sectorChecksumOffset]) {
		return headerState{}, nil, fmt.Errorf("%w: static header padding", ErrCorrupt)
	}
	keyID := string(header[staticPrefixBytes : staticPrefixBytes+keyLength])
	wrappedStart := staticPrefixBytes + keyLength
	wrapped := slices.Clone(header[wrappedStart : wrappedStart+wrappedLength])
	if err := validateKey(key, false); err != nil {
		return headerState{}, nil, err
	}
	if key.ID != keyID || (key.Wrapped != nil && !bytes.Equal(key.Wrapped, wrapped)) {
		return headerState{}, nil, fmt.Errorf("%w: locator metadata", ErrKeyMismatch)
	}
	var nonce [12]byte
	copy(nonce[:], header[28:40])
	var fileID [16]byte
	copy(fileID[:], header[40:56])
	if fileID == ([16]byte{}) {
		return headerState{}, nil, fmt.Errorf("%w: zero file ID", ErrCorrupt)
	}
	fileCrypto, err := makeFileCrypto(key, fileID)
	if err != nil {
		return headerState{}, nil, err
	}
	aead := fileCrypto.aead
	if nonce != deriveNonce(fileCrypto.nonceKey, "static-header", 0) {
		return headerState{}, nil, errors.Join(ErrKeyMismatch, ErrCorrupt, errors.New("invalid header nonce"))
	}
	aadEnd := staticPrefixBytes + keyLength + wrappedLength
	plaintext, err := aead.Open(nil, nonce[:], header[aadEnd:end], header[:aadEnd])
	if err != nil {
		return headerState{}, nil, errors.Join(ErrKeyMismatch, ErrCorrupt, fmt.Errorf("authenticate static header: %w", err))
	}
	if len(plaintext) != plainLength {
		return headerState{}, nil, fmt.Errorf("%w: static plaintext length", ErrCorrupt)
	}
	identity, reference, bounds, err := unmarshalIdentity(plaintext)
	if err != nil {
		return headerState{}, nil, err
	}
	if identity != expected {
		return headerState{}, nil, fmt.Errorf("%w: expected identity differs from sealed identity", ErrIdentityMismatch)
	}
	if bounds != boundsFromOptions(options) {
		return headerState{}, nil, fmt.Errorf("%w: Open Options differ from sealed bounds", ErrBounds)
	}
	state := headerState{
		identity: identity, keyID: keyID, wrapped: wrapped, fileID: fileID, headerNonce: nonce,
		dataKey: fileCrypto.dataKey, nonceKey: fileCrypto.nonceKey, headerDigest: sha256.Sum256(header), reference: reference, bounds: bounds,
	}
	return state, aead, nil
}

func initialCurrent(header headerState, walEnd int64, sequence uint64, chain [32]byte) currentState {
	index := header.reference.index
	term := header.reference.term
	return currentState{
		activeSlot: 0, generation: 1, walEnd: walEnd, recordSequence: sequence, chainDigest: chain,
		hard:  &pb.HardState{Term: uint64Pointer(term), Vote: uint64Pointer(0), Commit: uint64Pointer(index)},
		first: index + 1, last: index, snapshotID: header.reference.id, snapshotIndex: index,
		snapshotTerm: term, snapshotSize: header.reference.size, snapshotChunks: 1,
		snapshotDigest: header.reference.digest, topologyRecoveryEpoch: header.topologyRecoveryEpoch,
	}
}

func marshalCurrentSlot(state currentState, slot int, header headerState) ([]byte, [12]byte, error) {
	if slot < 0 || slot >= CurrentSlotCount || state.generation == 0 || state.generation%CurrentSlotCount != uint64(slot+1)%CurrentSlotCount {
		return nil, [12]byte{}, fmt.Errorf("%w: current slot generation", ErrInvalid)
	}
	payload, err := marshalCurrentPayload(state)
	if err != nil {
		return nil, [12]byte{}, err
	}
	objectTag := makeObjectTag(header.nonceKey, "current-slot", state.generation, currentTagContext(slot, header.fileID), payload)
	aead, err := makeObjectAEAD(header.dataKey, "current-slot", state.generation, objectTag)
	if err != nil {
		return nil, [12]byte{}, err
	}
	nonce := deriveObjectNonce(header.nonceKey, "current-slot", state.generation, objectTag)
	cipherLength := len(payload) + aead.Overhead()
	if currentPrefixBytes+len(header.keyID)+cipherLength > sectorChecksumOffset {
		return nil, [12]byte{}, fmt.Errorf("%w: current slot payload", ErrBounds)
	}
	result := make([]byte, CurrentSlotBytes)
	copy(result[0:8], currentMagic[:])
	binary.LittleEndian.PutUint16(result[8:10], codecVersion)
	binary.LittleEndian.PutUint16(result[10:12], CurrentSlotBytes)
	result[12] = byte(slot)
	binary.LittleEndian.PutUint16(result[14:16], uint16(len(header.keyID)))
	binary.LittleEndian.PutUint32(result[16:20], uint32(cipherLength))
	binary.LittleEndian.PutUint32(result[20:24], uint32(len(payload)))
	binary.LittleEndian.PutUint64(result[24:32], state.generation)
	copy(result[32:44], nonce[:])
	copy(result[44:60], header.fileID[:])
	binary.LittleEndian.PutUint16(result[60:62], currentPrefixBytes)
	binary.LittleEndian.PutUint16(result[62:64], cipherSuiteAES256GCM)
	copy(result[64:96], objectTag[:])
	copy(result[currentPrefixBytes:], header.keyID)
	aadEnd := currentPrefixBytes + len(header.keyID)
	aad := make([]byte, 0, aadEnd+len(header.headerDigest))
	aad = append(aad, result[:aadEnd]...)
	aad = append(aad, header.headerDigest[:]...)
	ciphertext := aead.Seal(nil, nonce[:], payload, aad)
	copy(result[aadEnd:], ciphertext)
	sealSectorChecksum(result)
	return result, nonce, nil
}

func unmarshalCurrentSlot(data []byte, slot int, header headerState) (slotDecode, error) {
	if len(data) != CurrentSlotBytes || slot < 0 || slot >= CurrentSlotCount {
		return slotDecode{}, fmt.Errorf("%w: current slot length", ErrCorrupt)
	}
	if allZero(data) {
		return slotDecode{absent: true}, nil
	}
	if !validSectorChecksum(data) {
		// The complete sector checksum is written last as part of the same
		// positional slot image. Only this state is eligible for crash fallback;
		// any CRC-valid structural or authentication failure below is corruption.
		return slotDecode{torn: true}, nil
	}
	if !bytes.Equal(data[0:8], currentMagic[:]) || binary.LittleEndian.Uint16(data[8:10]) != codecVersion ||
		binary.LittleEndian.Uint16(data[10:12]) != CurrentSlotBytes || int(data[12]) != slot || data[13] != 0 ||
		binary.LittleEndian.Uint16(data[60:62]) != currentPrefixBytes || binary.LittleEndian.Uint16(data[62:64]) != cipherSuiteAES256GCM {
		return slotDecode{}, fmt.Errorf("%w: current slot envelope", ErrCorrupt)
	}
	generation := binary.LittleEndian.Uint64(data[24:32])
	keyLength := int(binary.LittleEndian.Uint16(data[14:16]))
	cipherLength := int(binary.LittleEndian.Uint32(data[16:20]))
	plainLength := int(binary.LittleEndian.Uint32(data[20:24]))
	var fileID [16]byte
	copy(fileID[:], data[44:60])
	geometryOK := generation != 0 && generation%CurrentSlotCount == uint64(slot+1)%CurrentSlotCount && keyLength == len(header.keyID) &&
		keyLength > 0 && cipherLength == plainLength+16 && plainLength > 0 && currentPrefixBytes+keyLength+cipherLength <= sectorChecksumOffset &&
		fileID == header.fileID && string(data[currentPrefixBytes:currentPrefixBytes+keyLength]) == header.keyID
	if !geometryOK {
		return slotDecode{}, fmt.Errorf("%w: current slot geometry", ErrCorrupt)
	}
	end := currentPrefixBytes + keyLength + cipherLength
	if !allZero(data[end:sectorChecksumOffset]) {
		return slotDecode{}, fmt.Errorf("%w: current slot padding", ErrCorrupt)
	}
	var nonce [12]byte
	copy(nonce[:], data[32:44])
	var objectTag [32]byte
	copy(objectTag[:], data[64:96])
	if nonce != deriveObjectNonce(header.nonceKey, "current-slot", generation, objectTag) {
		return slotDecode{}, fmt.Errorf("%w: invalid current nonce", ErrCorrupt)
	}
	aead, err := makeObjectAEAD(header.dataKey, "current-slot", generation, objectTag)
	if err != nil {
		return slotDecode{}, err
	}
	aadEnd := currentPrefixBytes + keyLength
	aad := make([]byte, 0, aadEnd+len(header.headerDigest))
	aad = append(aad, data[:aadEnd]...)
	aad = append(aad, header.headerDigest[:]...)
	plaintext, err := aead.Open(nil, nonce[:], data[aadEnd:end], aad)
	if err != nil {
		return slotDecode{}, errors.Join(ErrKeyMismatch, ErrCorrupt, fmt.Errorf("authenticate current slot: %w", err))
	}
	if len(plaintext) != plainLength {
		return slotDecode{}, fmt.Errorf("%w: current plaintext length", ErrCorrupt)
	}
	if makeObjectTag(header.nonceKey, "current-slot", generation, currentTagContext(slot, header.fileID), plaintext) != objectTag {
		return slotDecode{}, fmt.Errorf("%w: current object tag", ErrCorrupt)
	}
	state, err := unmarshalCurrentPayload(plaintext)
	if err != nil {
		return slotDecode{}, err
	}
	state.activeSlot = slot
	state.generation = generation
	return slotDecode{state: state, generation: generation}, nil
}

func marshalCurrentPayload(state currentState) ([]byte, error) {
	if state.hard == nil || state.walEnd < HeaderBytes || state.first == 0 || state.last == ^uint64(0) ||
		state.snapshotID == ([16]byte{}) || state.snapshotIndex == 0 || state.snapshotTerm == 0 || state.snapshotChunks == 0 ||
		state.topologyRecoveryEpoch == 0 {
		return nil, fmt.Errorf("%w: invalid current state", ErrInvalid)
	}
	flags := uint16(0)
	if state.retryPresent {
		flags = 1
	}
	result := make([]byte, 0, 224)
	result = appendUint16(result, codecVersion)
	result = appendUint16(result, flags)
	result = appendUint64(result, uint64(state.walEnd))
	result = appendUint64(result, state.recordSequence)
	result = append(result, state.chainDigest[:]...)
	result = appendUint64(result, state.currentIncarnation)
	result = appendUint64(result, state.hard.GetTerm())
	result = appendUint64(result, state.hard.GetVote())
	result = appendUint64(result, state.hard.GetCommit())
	result = appendUint64(result, state.first)
	result = appendUint64(result, state.last)
	result = appendUint64(result, state.retry.incarnation)
	result = appendUint64(result, state.retry.readyID)
	result = append(result, state.retryDigest[:]...)
	result = append(result, state.snapshotID[:]...)
	result = appendUint64(result, state.snapshotIndex)
	result = appendUint64(result, state.snapshotTerm)
	result = appendUint64(result, state.snapshotSize)
	result = appendUint32(result, state.snapshotChunks)
	result = appendUint32(result, 0)
	result = append(result, state.snapshotDigest[:]...)
	result = appendUint64(result, state.topologyRecoveryEpoch)
	return result, nil
}

func unmarshalCurrentPayload(data []byte) (currentState, error) {
	reader := decoder{data: data}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return currentState{}, fmt.Errorf("%w: current codec version", ErrCorrupt)
	}
	flags, err := reader.u16()
	if err != nil || flags&^uint16(1) != 0 {
		return currentState{}, fmt.Errorf("%w: current flags", ErrCorrupt)
	}
	walEnd, err := reader.u64()
	if err != nil || walEnd > uint64(^uint64(0)>>1) {
		return currentState{}, fmt.Errorf("%w: current WAL end", ErrCorrupt)
	}
	sequence, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	chain, err := reader.take(32)
	if err != nil {
		return currentState{}, err
	}
	currentIncarnation, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	term, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	vote, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	commit, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	first, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	last, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	incarnation, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	readyID, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	retryDigest, err := reader.take(32)
	if err != nil {
		return currentState{}, err
	}
	snapshotIDBytes, err := reader.take(16)
	if err != nil {
		return currentState{}, err
	}
	snapshotIndex, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	snapshotTerm, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	snapshotSize, err := reader.u64()
	if err != nil {
		return currentState{}, err
	}
	snapshotChunks, err := reader.u32()
	if err != nil {
		return currentState{}, err
	}
	reserved, err := reader.u32()
	if err != nil || reserved != 0 {
		return currentState{}, fmt.Errorf("%w: current reserved field", ErrCorrupt)
	}
	snapshotDigest, err := reader.take(32)
	if err != nil {
		return currentState{}, err
	}
	topologyRecoveryEpoch, err := reader.u64()
	if err != nil || topologyRecoveryEpoch == 0 {
		return currentState{}, fmt.Errorf("%w: current topology epoch", ErrCorrupt)
	}
	if err := reader.done(); err != nil {
		return currentState{}, err
	}
	state := currentState{
		walEnd: int64(walEnd), recordSequence: sequence, currentIncarnation: currentIncarnation,
		hard:  &pb.HardState{Term: uint64Pointer(term), Vote: uint64Pointer(vote), Commit: uint64Pointer(commit)},
		first: first, last: last, retryPresent: flags&1 != 0,
		retry: retryKey{incarnation: incarnation, readyID: readyID}, snapshotIndex: snapshotIndex,
		snapshotTerm: snapshotTerm, snapshotSize: snapshotSize, snapshotChunks: snapshotChunks,
		topologyRecoveryEpoch: topologyRecoveryEpoch,
	}
	copy(state.chainDigest[:], chain)
	copy(state.retryDigest[:], retryDigest)
	copy(state.snapshotID[:], snapshotIDBytes)
	copy(state.snapshotDigest[:], snapshotDigest)
	if !state.retryPresent && (state.retry != (retryKey{}) || state.retryDigest != ([32]byte{})) {
		return currentState{}, fmt.Errorf("%w: absent retry has data", ErrCorrupt)
	}
	return state, nil
}

func sealSectorChecksum(data []byte) {
	checksum := crc32.Checksum(data[:len(data)-sectorChecksumBytes], crcTable)
	binary.LittleEndian.PutUint32(data[len(data)-8:len(data)-4], checksum)
	binary.LittleEndian.PutUint32(data[len(data)-4:], ^checksum)
}

func validSectorChecksum(data []byte) bool {
	if len(data) < sectorChecksumBytes {
		return false
	}
	want := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	complement := binary.LittleEndian.Uint32(data[len(data)-4:])
	return complement == ^want && crc32.Checksum(data[:len(data)-sectorChecksumBytes], crcTable) == want
}

func allZero(data []byte) bool {
	var value byte
	for _, item := range data {
		value |= item
	}
	return value == 0
}

func currentTagContext(slot int, fileID [16]byte) []byte {
	result := make([]byte, 17)
	result[0] = byte(slot)
	copy(result[1:], fileID[:])
	return result
}
