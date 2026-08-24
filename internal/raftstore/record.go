package raftstore

import (
	"bytes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	recordPrefixBytes           = 160
	recordChecksumBytes         = 8
	recordDamageGranule         = 4096
	recordTagContextBytes       = 78
	recordKindBootstrap   uint8 = 1
	recordKindReady       uint8 = 2
	// Retained-entry records carry exact Raft entries into an offline WAL
	// generation. They deliberately have no ReadyID: Ready IDs belong to one
	// process-local Node lifecycle and are not durable log identity.
	recordKindRetainedEntries uint8 = 3
	// A generation seal follows every retained-entry record and binds the
	// complete compacted image to its source current slot and SQL retention
	// commitment. Normal Ready records may follow only after activation and a
	// fresh BeginIncarnation.
	recordKindGenerationSeal uint8 = 4
)

var recordMagic = [8]byte{'V', 'D', 'B', 'R', 'R', 'E', 'C', 0}

type recordEnvelope struct {
	kind          uint8
	flags         uint8
	total         int
	plainLength   int
	cipherLength  int
	sequence      uint64
	incarnation   uint64
	readyID       uint64
	previous      [32]byte
	nonce         [12]byte
	payloadDigest [32]byte
	fileID        [16]byte
}

type decodedRecord struct {
	envelope recordEnvelope
	payload  []byte
	digest   [32]byte
}

type recordDecodeWorkspace struct {
	aad        []byte
	plaintext  []byte
	crypto     objectCryptoWorkspace
	tagContext [recordTagContextBytes]byte
}

func newRecordDecodeWorkspace(header headerState) recordDecodeWorkspace {
	return recordDecodeWorkspace{
		crypto: newObjectCryptoWorkspace(header.dataKey, header.nonceKey),
	}
}

type readyPayload struct {
	hard     *pb.HardState
	entries  []*pb.Entry
	mustSync bool
}

func marshalRecord(kind uint8, flags uint8, sequence, incarnation, readyID uint64, previous [32]byte, payload []byte, header headerState, options normalizedOptions) ([]byte, [32]byte, [12]byte, error) {
	if (kind != recordKindBootstrap && kind != recordKindReady &&
		kind != recordKindRetainedEntries && kind != recordKindGenerationSeal) ||
		sequence == 0 || len(payload) == 0 {
		return nil, [32]byte{}, [12]byte{}, fmt.Errorf("%w: invalid record input", ErrInvalid)
	}
	if kind == recordKindBootstrap && (incarnation != 0 || readyID != 0 || flags != 0) {
		return nil, [32]byte{}, [12]byte{}, fmt.Errorf("%w: invalid bootstrap envelope", ErrInvalid)
	}
	if kind == recordKindReady && (incarnation == 0 || readyID == 0 || flags&^uint8(1) != 0) {
		return nil, [32]byte{}, [12]byte{}, fmt.Errorf("%w: invalid Ready envelope", ErrInvalid)
	}
	if (kind == recordKindRetainedEntries || kind == recordKindGenerationSeal) &&
		(incarnation == 0 || readyID != 0 || flags != 0) {
		return nil, [32]byte{}, [12]byte{}, fmt.Errorf("%w: invalid generation envelope", ErrInvalid)
	}
	cipherLength := len(payload) + 16
	rawLength := recordPrefixBytes + len(header.keyID) + cipherLength + recordChecksumBytes
	total := alignRecordLength(rawLength)
	if total > options.maxRecordBytes || int64(total) > options.maxFileBytes-HeaderBytes {
		return nil, [32]byte{}, [12]byte{}, fmt.Errorf("%w: record length %d", ErrBounds, total)
	}
	envelope := recordEnvelope{kind: kind, flags: flags, total: total, plainLength: len(payload), cipherLength: cipherLength, sequence: sequence, incarnation: incarnation, readyID: readyID, previous: previous, fileID: header.fileID}
	objectTag := makeObjectTag(header.nonceKey, "wal-record", sequence, recordTagContext(envelope), payload)
	aead, err := makeObjectAEAD(header.dataKey, "wal-record", sequence, objectTag)
	if err != nil {
		return nil, [32]byte{}, [12]byte{}, err
	}
	result := make([]byte, total)
	copy(result[0:8], recordMagic[:])
	binary.LittleEndian.PutUint16(result[8:10], codecVersion)
	result[10] = kind
	result[11] = flags
	binary.LittleEndian.PutUint16(result[12:14], recordPrefixBytes)
	binary.LittleEndian.PutUint16(result[14:16], uint16(len(header.keyID)))
	binary.LittleEndian.PutUint32(result[16:20], uint32(total))
	binary.LittleEndian.PutUint32(result[20:24], uint32(len(payload)))
	binary.LittleEndian.PutUint32(result[24:28], uint32(cipherLength))
	binary.LittleEndian.PutUint64(result[32:40], sequence)
	binary.LittleEndian.PutUint64(result[40:48], incarnation)
	binary.LittleEndian.PutUint64(result[48:56], readyID)
	copy(result[56:88], previous[:])
	nonce := deriveObjectNonce(header.nonceKey, "wal-record", sequence, objectTag)
	copy(result[88:100], nonce[:])
	copy(result[100:132], objectTag[:])
	copy(result[132:148], header.fileID[:])
	copy(result[recordPrefixBytes:], header.keyID)
	aadEnd := recordPrefixBytes + len(header.keyID)
	padding := result[aadEnd+cipherLength : len(result)-recordChecksumBytes]
	aad := make([]byte, 0, aadEnd+len(header.headerDigest)+len(padding))
	aad = append(aad, result[:aadEnd]...)
	aad = append(aad, header.headerDigest[:]...)
	aad = append(aad, padding...)
	ciphertext := aead.Seal(nil, nonce[:], payload, aad)
	copy(result[aadEnd:], ciphertext)
	sealRecordChecksum(result)
	digest := sha256.Sum256(result)
	return result, digest, nonce, nil
}

func inspectRecordPrefix(prefix []byte, header headerState, options normalizedOptions) (recordEnvelope, error) {
	return inspectRecordPrefixWithWorkspace(prefix, header, options, nil)
}

// inspectRecordPrefixWithWorkspace is the GenerationBuilder-only variant. Its
// workspace owns mutable keyed HMAC state and therefore belongs to exactly one
// sequential scanner.
func inspectRecordPrefixWithWorkspace(
	prefix []byte,
	header headerState,
	options normalizedOptions,
	workspace *recordDecodeWorkspace,
) (recordEnvelope, error) {
	if len(prefix) != recordPrefixBytes || !bytes.Equal(prefix[0:8], recordMagic[:]) ||
		binary.LittleEndian.Uint16(prefix[8:10]) != codecVersion || binary.LittleEndian.Uint16(prefix[12:14]) != recordPrefixBytes ||
		binary.LittleEndian.Uint32(prefix[28:32]) != 0 || !allZero(prefix[148:160]) {
		return recordEnvelope{}, fmt.Errorf("%w: record envelope", ErrCorrupt)
	}
	envelope := recordEnvelope{
		kind: prefix[10], flags: prefix[11], total: int(binary.LittleEndian.Uint32(prefix[16:20])),
		plainLength: int(binary.LittleEndian.Uint32(prefix[20:24])), cipherLength: int(binary.LittleEndian.Uint32(prefix[24:28])),
		sequence: binary.LittleEndian.Uint64(prefix[32:40]), incarnation: binary.LittleEndian.Uint64(prefix[40:48]),
		readyID: binary.LittleEndian.Uint64(prefix[48:56]),
	}
	copy(envelope.previous[:], prefix[56:88])
	copy(envelope.nonce[:], prefix[88:100])
	copy(envelope.payloadDigest[:], prefix[100:132])
	copy(envelope.fileID[:], prefix[132:148])
	keyLength := int(binary.LittleEndian.Uint16(prefix[14:16]))
	if (envelope.kind != recordKindBootstrap && envelope.kind != recordKindReady &&
		envelope.kind != recordKindRetainedEntries && envelope.kind != recordKindGenerationSeal) ||
		envelope.flags&^uint8(1) != 0 ||
		envelope.sequence == 0 || envelope.fileID != header.fileID || keyLength != len(header.keyID) ||
		envelope.plainLength <= 0 || envelope.cipherLength != envelope.plainLength+16 ||
		envelope.total != alignRecordLength(recordPrefixBytes+keyLength+envelope.cipherLength+recordChecksumBytes) || envelope.total%recordDamageGranule != 0 ||
		envelope.total > options.maxRecordBytes || envelope.total < recordPrefixBytes+recordChecksumBytes {
		return recordEnvelope{}, fmt.Errorf("%w: record geometry", ErrCorrupt)
	}
	if envelope.kind == recordKindBootstrap && (envelope.incarnation != 0 || envelope.readyID != 0 || envelope.flags != 0) {
		return recordEnvelope{}, fmt.Errorf("%w: bootstrap record identity", ErrCorrupt)
	}
	if envelope.kind == recordKindReady && (envelope.incarnation == 0 || envelope.readyID == 0) {
		return recordEnvelope{}, fmt.Errorf("%w: Ready record identity", ErrCorrupt)
	}
	if (envelope.kind == recordKindRetainedEntries || envelope.kind == recordKindGenerationSeal) &&
		(envelope.incarnation == 0 || envelope.readyID != 0 || envelope.flags != 0) {
		return recordEnvelope{}, fmt.Errorf("%w: generation record identity", ErrCorrupt)
	}
	var wantNonce [12]byte
	if workspace != nil && workspace.crypto.authMAC != nil {
		wantNonce = workspace.crypto.deriveObjectNonce(
			"wal-record", envelope.sequence, envelope.payloadDigest,
		)
	} else {
		wantNonce = deriveObjectNonce(
			header.nonceKey, "wal-record", envelope.sequence, envelope.payloadDigest,
		)
	}
	if envelope.nonce != wantNonce {
		return recordEnvelope{}, fmt.Errorf("%w: invalid record nonce", ErrCorrupt)
	}
	return envelope, nil
}

func unmarshalRecord(data []byte, header headerState, options normalizedOptions) (decodedRecord, error) {
	if len(data) < recordPrefixBytes {
		return decodedRecord{}, fmt.Errorf("%w: short record", ErrCorrupt)
	}
	envelope, err := inspectRecordPrefix(data[:recordPrefixBytes], header, options)
	if err != nil {
		return decodedRecord{}, err
	}
	return unmarshalInspectedRecord(data, envelope, header, nil)
}

// unmarshalInspectedRecord avoids re-parsing and re-deriving the authenticated
// prefix when a bounded sequential scanner already inspected these exact
// bytes. workspace is caller-owned and may be reused after the returned payload
// is no longer referenced.
func unmarshalInspectedRecord(
	data []byte,
	envelope recordEnvelope,
	header headerState,
	workspace *recordDecodeWorkspace,
) (decodedRecord, error) {
	if len(data) != envelope.total {
		return decodedRecord{}, fmt.Errorf("%w: record length", ErrCorrupt)
	}
	if !validRecordChecksum(data) {
		return decodedRecord{}, fmt.Errorf("%w: record checksum", ErrCorrupt)
	}
	keyEnd := recordPrefixBytes + len(header.keyID)
	if !bytes.Equal(data[recordPrefixBytes:keyEnd], []byte(header.keyID)) {
		return decodedRecord{}, fmt.Errorf("%w: record key ID", ErrKeyMismatch)
	}
	cipherEnd := keyEnd + envelope.cipherLength
	padding := data[cipherEnd : len(data)-recordChecksumBytes]
	if !allZero(padding) {
		return decodedRecord{}, fmt.Errorf("%w: record padding", ErrCorrupt)
	}
	aadCapacity := keyEnd + len(header.headerDigest) + len(padding)
	var aad []byte
	if workspace == nil {
		aad = make([]byte, 0, aadCapacity)
	} else {
		if cap(workspace.aad) < aadCapacity {
			clear(workspace.aad[:cap(workspace.aad)])
			workspace.aad = make([]byte, 0, aadCapacity)
		} else {
			workspace.aad = workspace.aad[:0]
		}
		aad = workspace.aad
	}
	aad = append(aad, data[:keyEnd]...)
	aad = append(aad, header.headerDigest[:]...)
	aad = append(aad, padding...)
	if workspace != nil {
		workspace.aad = aad
	}
	var aead cipher.AEAD
	var err error
	if workspace != nil && workspace.crypto.objectKeyMAC != nil {
		derived := workspace.crypto.deriveObjectKey(
			"wal-record", envelope.sequence, envelope.payloadDigest,
		)
		aead, err = makeObjectAEADFromKey(derived)
	} else {
		aead, err = makeObjectAEAD(
			header.dataKey, "wal-record", envelope.sequence, envelope.payloadDigest,
		)
	}
	if err != nil {
		return decodedRecord{}, err
	}
	var destination []byte
	if workspace != nil {
		if cap(workspace.plaintext) < envelope.plainLength {
			clear(workspace.plaintext[:cap(workspace.plaintext)])
			workspace.plaintext = make([]byte, 0, envelope.plainLength)
		} else {
			workspace.plaintext = workspace.plaintext[:0]
		}
		destination = workspace.plaintext
	}
	plaintext, err := aead.Open(destination, envelope.nonce[:], data[keyEnd:cipherEnd], aad)
	if err != nil {
		return decodedRecord{}, errors.Join(ErrKeyMismatch, ErrCorrupt, fmt.Errorf("authenticate record: %w", err))
	}
	if workspace != nil {
		workspace.plaintext = plaintext
	}
	var payloadDigest [sha256.Size]byte
	if workspace != nil && workspace.crypto.authMAC != nil {
		payloadDigest = workspace.crypto.makeObjectTag(
			"wal-record", envelope.sequence,
			putRecordTagContext(&workspace.tagContext, envelope), plaintext,
		)
	} else {
		payloadDigest = makeObjectTag(
			header.nonceKey, "wal-record", envelope.sequence,
			recordTagContext(envelope), plaintext,
		)
	}
	if len(plaintext) != envelope.plainLength || payloadDigest != envelope.payloadDigest {
		return decodedRecord{}, fmt.Errorf("%w: record payload digest", ErrCorrupt)
	}
	return decodedRecord{envelope: envelope, payload: plaintext, digest: sha256.Sum256(data)}, nil
}

func marshalReadyPayload(batch raftmodel.PersistBatch) ([]byte, error) {
	flags := uint16(0)
	if batch.MustSync {
		flags = 1
	}
	hardPresent := !isEmptyHardState(batch.HardState)
	capacity := 40
	for _, entry := range batch.Entries {
		if entry == nil || uint64(len(entry.GetData())) > math.MaxUint32 || capacity > math.MaxInt-32-len(entry.GetData()) {
			return nil, fmt.Errorf("%w: Ready entry geometry", ErrBounds)
		}
		capacity += 32 + len(entry.GetData())
	}
	result := make([]byte, 0, capacity)
	result = appendUint16(result, codecVersion)
	result = appendUint16(result, flags)
	result = appendUint32(result, uint32(len(batch.Entries)))
	if hardPresent {
		result = append(result, 1)
	} else {
		result = append(result, 0)
	}
	result = append(result, make([]byte, 7)...)
	result = appendUint64(result, batch.HardState.GetTerm())
	result = appendUint64(result, batch.HardState.GetVote())
	result = appendUint64(result, batch.HardState.GetCommit())
	for _, entry := range batch.Entries {
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

func unmarshalReadyPayload(data []byte, options normalizedOptions) (readyPayload, error) {
	return unmarshalReadyPayloadMode(data, options, true)
}

// unmarshalReadyPayloadView is the bounded-replay variant used while building
// an offline generation. Entry data aliases data and is valid only until the
// caller releases that authenticated record buffer.
func unmarshalReadyPayloadView(data []byte, options normalizedOptions) (readyPayload, error) {
	return unmarshalReadyPayloadMode(data, options, false)
}

func unmarshalReadyPayloadMode(
	data []byte,
	options normalizedOptions,
	detach bool,
) (readyPayload, error) {
	reader := decoder{data: data}
	version, err := reader.u16()
	if err != nil || version != codecVersion {
		return readyPayload{}, fmt.Errorf("%w: Ready codec version", ErrCorrupt)
	}
	flags, err := reader.u16()
	if err != nil || flags&^uint16(1) != 0 {
		return readyPayload{}, fmt.Errorf("%w: Ready flags", ErrCorrupt)
	}
	entryCount, err := reader.u32()
	if err != nil || entryCount > MaxReadyEntries || uint64(entryCount) > options.maxEntries {
		return readyPayload{}, fmt.Errorf("%w: Ready entry count", ErrCorrupt)
	}
	hardPresent, err := reader.u8()
	if err != nil || hardPresent > 1 {
		return readyPayload{}, fmt.Errorf("%w: Ready HardState marker", ErrCorrupt)
	}
	reserved, err := reader.take(7)
	if err != nil || !allZero(reserved) {
		return readyPayload{}, fmt.Errorf("%w: Ready reserved bytes", ErrCorrupt)
	}
	term, err := reader.u64()
	if err != nil {
		return readyPayload{}, err
	}
	vote, err := reader.u64()
	if err != nil {
		return readyPayload{}, err
	}
	commit, err := reader.u64()
	if err != nil {
		return readyPayload{}, err
	}
	if uint64(entryCount) > uint64(len(data)-reader.offset)/32 {
		return readyPayload{}, fmt.Errorf("%w: impossible Ready entry geometry", ErrCorrupt)
	}
	result := readyPayload{mustSync: flags&1 != 0, entries: make([]*pb.Entry, int(entryCount))}
	if hardPresent == 1 {
		result.hard = &pb.HardState{Term: uint64Pointer(term), Vote: uint64Pointer(vote), Commit: uint64Pointer(commit)}
	} else if term != 0 || vote != 0 || commit != 0 {
		return readyPayload{}, fmt.Errorf("%w: absent HardState has data", ErrCorrupt)
	}
	var totalData int64
	for position := range result.entries {
		entryType, decodeErr := reader.u32()
		if decodeErr != nil {
			return readyPayload{}, decodeErr
		}
		reservedType, decodeErr := reader.u32()
		if decodeErr != nil || reservedType != 0 || entryType > uint32(pb.EntryConfChangeV2) {
			return readyPayload{}, fmt.Errorf("%w: entry type", ErrCorrupt)
		}
		entryTerm, decodeErr := reader.u64()
		if decodeErr != nil {
			return readyPayload{}, decodeErr
		}
		entryIndex, decodeErr := reader.u64()
		if decodeErr != nil {
			return readyPayload{}, decodeErr
		}
		dataLength, decodeErr := reader.u32()
		if decodeErr != nil || dataLength > raftmodel.MaxProposalBytes {
			return readyPayload{}, fmt.Errorf("%w: entry bytes", ErrCorrupt)
		}
		reservedEntry, decodeErr := reader.u32()
		if decodeErr != nil || reservedEntry != 0 {
			return readyPayload{}, fmt.Errorf("%w: entry reserved", ErrCorrupt)
		}
		if totalData > options.maxLiveBytes-int64(dataLength) {
			return readyPayload{}, fmt.Errorf("%w: recovered entry bytes", ErrBounds)
		}
		payload, decodeErr := reader.take(int(dataLength))
		if decodeErr != nil {
			return readyPayload{}, decodeErr
		}
		typeValue := pb.EntryType(entryType)
		entryData := payload
		if detach {
			entryData = append([]byte(nil), payload...)
		}
		result.entries[position] = &pb.Entry{
			Type: entryTypePointer(typeValue), Term: uint64Pointer(entryTerm),
			Index: uint64Pointer(entryIndex), Data: entryData,
		}
		totalData += int64(dataLength)
	}
	if err := reader.done(); err != nil {
		return readyPayload{}, err
	}
	return result, nil
}

func sealRecordChecksum(data []byte) {
	checksum := crc32.Checksum(data[:len(data)-recordChecksumBytes], crcTable)
	binary.LittleEndian.PutUint32(data[len(data)-8:len(data)-4], checksum)
	binary.LittleEndian.PutUint32(data[len(data)-4:], ^checksum)
}

func validRecordChecksum(data []byte) bool {
	if len(data) < recordChecksumBytes {
		return false
	}
	want := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	return binary.LittleEndian.Uint32(data[len(data)-4:]) == ^want && crc32.Checksum(data[:len(data)-recordChecksumBytes], crcTable) == want
}

func isEmptyHardState(state *pb.HardState) bool {
	return state == nil || (state.GetTerm() == 0 && state.GetVote() == 0 && state.GetCommit() == 0)
}

func alignRecordLength(length int) int {
	return (length + recordDamageGranule - 1) &^ (recordDamageGranule - 1)
}

func recordTagContext(envelope recordEnvelope) []byte {
	var result [recordTagContextBytes]byte
	return append([]byte(nil), putRecordTagContext(&result, envelope)...)
}

func putRecordTagContext(
	destination *[recordTagContextBytes]byte,
	envelope recordEnvelope,
) []byte {
	destination[0] = envelope.kind
	destination[1] = envelope.flags
	binary.LittleEndian.PutUint32(destination[2:6], uint32(envelope.total))
	binary.LittleEndian.PutUint32(destination[6:10], uint32(envelope.plainLength))
	binary.LittleEndian.PutUint32(destination[10:14], uint32(envelope.cipherLength))
	binary.LittleEndian.PutUint64(destination[14:22], envelope.incarnation)
	binary.LittleEndian.PutUint64(destination[22:30], envelope.readyID)
	copy(destination[30:62], envelope.previous[:])
	copy(destination[62:78], envelope.fileID[:])
	return destination[:]
}
