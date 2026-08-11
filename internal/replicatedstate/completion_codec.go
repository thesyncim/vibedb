package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	completionRecordFormatV1    = uint16(1)
	completionRecordHeaderBytes = 144
)

var (
	completionRecordMagic          = [8]byte{'V', 'D', 'B', 'R', 'C', 'P', 0, 0}
	completionRecordChecksumDomain = []byte(
		"vibedb/replicated-state/completion-record-checksum/v1\x00",
	)
	completionKeyDomain = []byte(
		"vibedb/replicated-state/completion-key/v1\x00",
	)
	commandDigestDomain = []byte(
		"vibedb/replicated-state/exact-command/v1\x00",
	)
)

// CompletionRecordV1 is the collision-verifiable retained request wrapper.
// Completion contains one exact replication.CompletionV1 envelope.
type CompletionRecordV1 struct {
	Tenant         []byte
	ClientID       replication.ID128
	ClientEpoch    uint64
	ClientSequence uint64
	RetryHome      replication.RetryHome
	Fingerprint    replication.Digest
	CommandDigest  [32]byte
	Collection     string
	Completion     []byte
}

// CompletionKeyV1 derives the sole lookup key for a client sequence. RetryHome,
// fingerprint, and command bytes are intentionally verified values, not key
// components.
func CompletionKeyV1(
	tenant []byte,
	clientID replication.ID128,
	clientEpoch uint64,
	clientSequence uint64,
) [32]byte {
	h := sha256.New()
	_, _ = h.Write(completionKeyDomain)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(tenant)))
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(tenant)
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(clientID)))
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(clientID[:])
	binary.LittleEndian.PutUint64(scalar[:], clientEpoch)
	var scalarFrame [16]byte
	binary.LittleEndian.PutUint64(scalarFrame[:8], uint64(len(scalar)))
	copy(scalarFrame[8:], scalar[:])
	_, _ = h.Write(scalarFrame[:])
	binary.LittleEndian.PutUint64(scalar[:], clientSequence)
	copy(scalarFrame[8:], scalar[:])
	_, _ = h.Write(scalarFrame[:])
	var result [32]byte
	_ = h.Sum(result[:0])
	return result
}

func completionStorageKey(digest [32]byte) [33]byte {
	var key [33]byte
	key[0] = 1
	copy(key[1:], digest[:])
	return key
}

// CommandDigestV1 binds retained dedupe state to the exact command envelope.
func CommandDigestV1(command []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(commandDigestDomain)
	_, _ = h.Write(command)
	var result [32]byte
	_ = h.Sum(result[:0])
	return result
}

// AppendCompletionRecordV1 appends one strict retained-request envelope. On
// error dst is unchanged. Input slices and strings must not overlap the
// writable append region in dst's current backing array; such aliases are
// rejected before dst is modified. Aliases into an old backing array are safe
// when append relocates.
func AppendCompletionRecordV1(dst []byte, record CompletionRecordV1) ([]byte, error) {
	if err := validateCompletionRecordV1(record); err != nil {
		return dst, err
	}
	total := completionRecordHeaderBytes + len(record.Tenant) +
		len(record.Collection) + len(record.Completion) + recordChecksumLen
	if total > MaxCompletionRecordBytes {
		return dst, fmt.Errorf("%w: completion record %d", ErrAdmissionBound, total)
	}
	region := writableAppendRegion(dst, total)
	if byteSlicesOverlap(region, record.Tenant) ||
		byteStringOverlap(region, record.Collection) ||
		byteSlicesOverlap(region, record.Completion) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], completionRecordMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], completionRecordFormatV1)
	binary.LittleEndian.PutUint16(frame[12:14], completionRecordHeaderBytes)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(total))
	binary.LittleEndian.PutUint32(frame[20:24], uint32(total-completionRecordHeaderBytes-recordChecksumLen))
	copy(frame[24:40], record.ClientID[:])
	binary.LittleEndian.PutUint64(frame[40:48], record.ClientEpoch)
	binary.LittleEndian.PutUint64(frame[48:56], record.ClientSequence)
	copy(frame[56:64], record.RetryHome[:])
	copy(frame[64:96], record.Fingerprint[:])
	copy(frame[96:128], record.CommandDigest[:])
	binary.LittleEndian.PutUint16(frame[128:130], uint16(len(record.Tenant)))
	binary.LittleEndian.PutUint16(frame[130:132], uint16(len(record.Collection)))
	binary.LittleEndian.PutUint32(frame[132:136], uint32(len(record.Completion)))
	cursor := completionRecordHeaderBytes
	cursor += copy(frame[cursor:], record.Tenant)
	cursor += copy(frame[cursor:], record.Collection)
	copy(frame[cursor:], record.Completion)
	sealRecord(frame, completionRecordChecksumDomain)
	return dst, nil
}

// OpenCompletionRecordV1 strictly decodes a complete retained-request record.
func OpenCompletionRecordV1(src []byte) (CompletionRecordV1, error) {
	if len(src) < completionRecordHeaderBytes+recordChecksumLen ||
		len(src) > MaxCompletionRecordBytes {
		return CompletionRecordV1{}, fmt.Errorf("%w: record length", ErrCompletionCorrupt)
	}
	if !bytes.Equal(src[0:8], completionRecordMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != completionRecordFormatV1 ||
		binary.LittleEndian.Uint16(src[10:12]) != 0 ||
		binary.LittleEndian.Uint16(src[12:14]) != completionRecordHeaderBytes ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 || !allZero(src[136:144]) {
		return CompletionRecordV1{}, fmt.Errorf("%w: record header", ErrCompletionCorrupt)
	}
	total64 := uint64(binary.LittleEndian.Uint32(src[16:20]))
	body64 := uint64(binary.LittleEndian.Uint32(src[20:24]))
	if total64 != uint64(len(src)) ||
		body64 != uint64(len(src)-completionRecordHeaderBytes-recordChecksumLen) ||
		!verifyRecord(src, completionRecordChecksumDomain) {
		return CompletionRecordV1{}, fmt.Errorf("%w: record size or checksum", ErrCompletionCorrupt)
	}
	tenantLen64 := uint64(binary.LittleEndian.Uint16(src[128:130]))
	collectionLen64 := uint64(binary.LittleEndian.Uint16(src[130:132]))
	completionLen64 := uint64(binary.LittleEndian.Uint32(src[132:136]))
	if tenantLen64+collectionLen64+completionLen64 != body64 ||
		tenantLen64 > uint64(len(src)) || collectionLen64 > uint64(len(src)) ||
		completionLen64 > uint64(len(src)) {
		return CompletionRecordV1{}, fmt.Errorf("%w: record body lengths", ErrCompletionCorrupt)
	}
	tenantLen, collectionLen, completionLen := int(tenantLen64), int(collectionLen64), int(completionLen64)
	var record CompletionRecordV1
	copy(record.ClientID[:], src[24:40])
	record.ClientEpoch = binary.LittleEndian.Uint64(src[40:48])
	record.ClientSequence = binary.LittleEndian.Uint64(src[48:56])
	copy(record.RetryHome[:], src[56:64])
	copy(record.Fingerprint[:], src[64:96])
	copy(record.CommandDigest[:], src[96:128])
	cursor := completionRecordHeaderBytes
	record.Tenant = bytes.Clone(src[cursor : cursor+tenantLen])
	cursor += tenantLen
	record.Collection = string(src[cursor : cursor+collectionLen])
	cursor += collectionLen
	record.Completion = bytes.Clone(src[cursor : cursor+completionLen])
	if err := validateCompletionRecordV1(record); err != nil {
		return CompletionRecordV1{}, err
	}
	return record, nil
}

func validateCompletionRecordV1(record CompletionRecordV1) error {
	if len(record.Tenant) == 0 || len(record.Tenant) > replication.MaxIdentityBytes ||
		record.ClientID == (replication.ID128{}) || record.ClientEpoch == 0 ||
		record.ClientSequence == 0 || record.Fingerprint == (replication.Digest{}) ||
		record.Collection == "" || len(record.Collection) > replication.MaxCollectionBytes ||
		!utf8.ValidString(record.Collection) || len(record.Completion) == 0 {
		return fmt.Errorf("%w: invalid retained request", ErrCompletionCorrupt)
	}
	completion, err := replication.OpenCompletionV1(record.Completion)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCompletionCorrupt, err)
	}
	if !bytes.Equal(completion.Tenant, record.Tenant) ||
		completion.ClientID != record.ClientID ||
		completion.ClientEpoch != record.ClientEpoch ||
		completion.ClientSequence != record.ClientSequence ||
		completion.RetryHome != record.RetryHome ||
		completion.Fingerprint != record.Fingerprint {
		return fmt.Errorf("%w: wrapper and completion differ", ErrCompletionCorrupt)
	}
	if completion.Storage != replication.CompletionInline ||
		completion.ResultFormat != ResultFormatMutationV1 || completion.ResultLength != 0 ||
		len(completion.InlineResult) != 0 || completion.ResultCode < ResultApplied ||
		completion.ResultCode > ResultTargetBound {
		return fmt.Errorf("%w: unsupported completion result grammar", ErrCompletionCorrupt)
	}
	return nil
}

func writeHashFrame(h interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func recordMatchesCommand(record CompletionRecordV1, command replication.CommandViewV1) bool {
	return recordTupleMatchesCommand(record, command) &&
		record.RetryHome == command.RetryHome &&
		record.Fingerprint == command.Fingerprint &&
		record.Collection == string(command.Collection) &&
		record.CommandDigest == CommandDigestV1(command.Bytes())
}

func recordTupleMatchesCommand(record CompletionRecordV1, command replication.CommandViewV1) bool {
	return bytes.Equal(record.Tenant, command.Tenant) &&
		record.ClientID == command.ClientID &&
		record.ClientEpoch == command.ClientEpoch &&
		record.ClientSequence == command.ClientSequence
}
