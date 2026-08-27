package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/replication"
)

var ErrTailBatchWire = errors.New("rangesplit: invalid canonical tail batch")

const (
	tailBatchWireFormat           = uint16(0)
	tailBatchWireHeaderBytes      = 440
	tailBatchOperationHeaderBytes = 12
	// MaxTailBatchWireBytes is an exact aggregate ceiling: one admitted source
	// command, one fixed operation header per maximum mutation, the authenticated
	// identity header, and the frame digest. Decoders reject the length before
	// allocating or reading a payload.
	MaxTailBatchWireBytes = tailBatchWireHeaderBytes + replication.MaxCommandBytes +
		replication.MaxMutations*tailBatchOperationHeaderBytes + sha256.Size
)

var (
	tailBatchWireMagic  = [8]byte{'V', 'D', 'B', 'S', 'T', 'A', 'I', 'L'}
	tailBatchWireDomain = []byte("vibedb/range-split/tail-batch-wire\x00")
)

// TailBatchCodecWorkspace retains one SHA-256 state. It is serial and keeps
// warmed encode/decode allocation independent of mutation count and bytes.
type TailBatchCodecWorkspace struct {
	hasher hash.Hash
	digest [sha256.Size]byte
}

func AppendTailBatch(dst []byte, batch TailBatch) ([]byte, error) {
	return AppendTailBatchWithWorkspace(dst, batch, &TailBatchCodecWorkspace{})
}

// AppendTailBatchWithWorkspace appends one canonical child-local batch. Only
// the operations for this child cross the wire; TransitionCount and the global
// TranslationDigest retain the exact source-entry proof without duplicating
// unrelated child documents.
func AppendTailBatchWithWorkspace(
	dst []byte,
	batch TailBatch,
	workspace *TailBatchCodecWorkspace,
) ([]byte, error) {
	if workspace == nil || !validTailBatchWireIdentity(batch) {
		return dst, ErrTailBatchWire
	}
	operationBytes, err := measureTailBatchOperations(batch)
	if err != nil || operationBytes > MaxTailBatchWireBytes-tailBatchWireHeaderBytes-sha256.Size {
		return dst, errors.Join(ErrTailBatchWire, err)
	}
	total := tailBatchWireHeaderBytes + operationBytes + sha256.Size
	if total > MaxTailBatchWireBytes || uint64(total) > math.MaxUint32 {
		return dst, ErrTailBatchWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	clear(frame[:tailBatchWireHeaderBytes])
	copy(frame[:8], tailBatchWireMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], tailBatchWireFormat)
	binary.LittleEndian.PutUint16(frame[10:12], tailBatchWireHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	frame[16] = batch.Child
	values := [...]uint64{
		batch.Applied, batch.Term, batch.TransitionCount, batch.Operations, batch.Bytes,
		batch.BeforeOwnershipEpoch, batch.AfterOwnershipEpoch,
		batch.BeforeRoutingVersion, batch.AfterRoutingVersion,
		batch.BeforeRouteGeneration, batch.AfterRouteGeneration,
	}
	for index, value := range values {
		binary.LittleEndian.PutUint64(frame[24+index*8:], value)
	}
	digests := [...][sha256.Size]byte{
		batch.PlanDigest, batch.PlacementDigest, batch.TranslationDigest, batch.Digest,
		batch.PreviousEntryDigest, batch.EntryDigest, batch.BeforeDataChainDigest,
		batch.AfterDataChainDigest, batch.SourceBaseDigest, batch.ChildBaseDigest,
	}
	for index := range digests {
		copy(frame[112+index*sha256.Size:], digests[index][:])
	}
	binary.LittleEndian.PutUint32(frame[432:436], uint32(operationBytes))
	at := tailBatchWireHeaderBytes
	iterator := batch.Iterator()
	for iterator.Next() {
		operation := iterator.Operation()
		header := frame[at : at+tailBatchOperationHeaderBytes]
		header[0] = byte(operation.Kind)
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(operation.Key)))
		binary.LittleEndian.PutUint32(header[8:12], uint32(len(operation.Value)))
		at += tailBatchOperationHeaderBytes
		copy(frame[at:], operation.Key)
		at += len(operation.Key)
		copy(frame[at:], operation.Value)
		at += len(operation.Value)
	}
	if iterator.wireInvalid || at != len(frame)-sha256.Size {
		return dst[:start], ErrTailBatchWire
	}
	tailBatchWireDigest(workspace, frame[:at])
	copy(frame[at:], workspace.digest[:])
	return dst, nil
}

func OpenTailBatch(raw []byte) (TailBatch, error) {
	return OpenTailBatchWithWorkspace(raw, &TailBatchCodecWorkspace{})
}

// OpenTailBatchWithWorkspace verifies the whole frame before exposing a lazy
// zero-copy operation iterator. raw must remain immutable while the batch is
// consumed.
func OpenTailBatchWithWorkspace(
	raw []byte,
	workspace *TailBatchCodecWorkspace,
) (TailBatch, error) {
	if workspace == nil || len(raw) < tailBatchWireHeaderBytes+sha256.Size ||
		len(raw) > MaxTailBatchWireBytes || !bytes.Equal(raw[:8], tailBatchWireMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != tailBatchWireFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != tailBatchWireHeaderBytes ||
		uint64(binary.LittleEndian.Uint32(raw[12:16])) != uint64(len(raw)) ||
		!allChildArtifactZero(raw[17:24]) || !allChildArtifactZero(raw[436:440]) {
		return TailBatch{}, ErrTailBatchWire
	}
	tailBatchWireDigest(workspace, raw[:len(raw)-sha256.Size])
	if !bytes.Equal(workspace.digest[:], raw[len(raw)-sha256.Size:]) {
		return TailBatch{}, ErrTailBatchWire
	}
	operationBytes := uint64(binary.LittleEndian.Uint32(raw[432:436]))
	if operationBytes != uint64(len(raw)-tailBatchWireHeaderBytes-sha256.Size) {
		return TailBatch{}, ErrTailBatchWire
	}
	values := [11]uint64{}
	for index := range values {
		values[index] = binary.LittleEndian.Uint64(raw[24+index*8:])
	}
	batch := TailBatch{
		Child: raw[16], Applied: values[0], Term: values[1],
		TransitionCount: values[2], Operations: values[3], Bytes: values[4],
		BeforeOwnershipEpoch: values[5], AfterOwnershipEpoch: values[6],
		BeforeRoutingVersion: values[7], AfterRoutingVersion: values[8],
		BeforeRouteGeneration: values[9], AfterRouteGeneration: values[10],
		wireOperations: raw[tailBatchWireHeaderBytes : len(raw)-sha256.Size : len(raw)-sha256.Size],
		wireEncoded:    true, translated: true,
	}
	digests := []*[sha256.Size]byte{
		&batch.PlanDigest, &batch.PlacementDigest, &batch.TranslationDigest, &batch.Digest,
		&batch.PreviousEntryDigest, &batch.EntryDigest, &batch.BeforeDataChainDigest,
		&batch.AfterDataChainDigest, &batch.SourceBaseDigest, &batch.ChildBaseDigest,
	}
	for index, digest := range digests {
		copy(digest[:], raw[112+index*sha256.Size:112+(index+1)*sha256.Size])
	}
	if !validTailBatchWireIdentity(batch) {
		return TailBatch{}, ErrTailBatchWire
	}
	if measured, err := measureTailBatchOperations(batch); err != nil || measured != int(operationBytes) {
		return TailBatch{}, errors.Join(ErrTailBatchWire, err)
	}
	return batch, nil
}

func validTailBatchWireIdentity(batch TailBatch) bool {
	return batch.translated && batch.Child < autosplit.MaxSplitChildren &&
		batch.PlanDigest != ([sha256.Size]byte{}) &&
		batch.PlacementDigest != ([sha256.Size]byte{}) &&
		batch.TranslationDigest != ([sha256.Size]byte{}) && batch.Digest != ([sha256.Size]byte{}) &&
		batch.PreviousEntryDigest != ([sha256.Size]byte{}) && batch.EntryDigest != ([sha256.Size]byte{}) &&
		batch.BeforeDataChainDigest != ([sha256.Size]byte{}) &&
		batch.AfterDataChainDigest != ([sha256.Size]byte{}) &&
		batch.SourceBaseDigest != ([sha256.Size]byte{}) && batch.ChildBaseDigest != ([sha256.Size]byte{}) &&
		batch.Applied > 0 && batch.Applied < math.MaxUint64 && batch.Term > 0 && batch.Term < math.MaxUint64 &&
		batch.TransitionCount <= replication.MaxMutations && batch.Operations <= batch.TransitionCount &&
		batch.Bytes <= replication.MaxCommandBytes && validTailBatchCoordinates(batch)
}

func measureTailBatchOperations(batch TailBatch) (int, error) {
	iterator := batch.Iterator()
	count, bytesCount, encoded := uint64(0), uint64(0), uint64(0)
	var previous []byte
	for iterator.Next() {
		operation := iterator.Operation()
		if len(operation.Key) == 0 || len(operation.Key) > replication.MaxMutationKeyBytes ||
			previous != nil && bytes.Compare(previous, operation.Key) >= 0 {
			return 0, ErrTailBatchWire
		}
		switch operation.Kind {
		case replication.MutationPut:
			if len(operation.Value) == 0 || len(operation.Value) > replication.MaxMutationValueBytes {
				return 0, ErrTailBatchWire
			}
		case replication.MutationDelete:
			if operation.Value != nil {
				return 0, ErrTailBatchWire
			}
		default:
			return 0, ErrTailBatchWire
		}
		payload := uint64(len(operation.Key) + len(operation.Value))
		if count == math.MaxUint64 || bytesCount > math.MaxUint64-payload ||
			encoded > math.MaxUint64-tailBatchOperationHeaderBytes-payload {
			return 0, ErrTailBatchWire
		}
		count++
		bytesCount += payload
		encoded += tailBatchOperationHeaderBytes + payload
		previous = operation.Key
	}
	if iterator.wireInvalid || count != batch.Operations || bytesCount != batch.Bytes ||
		encoded > math.MaxInt {
		return 0, ErrTailBatchWire
	}
	return int(encoded), nil
}

func openTailWireOperation(raw []byte) (TailOperation, []byte, bool) {
	if len(raw) < tailBatchOperationHeaderBytes || !allChildArtifactZero(raw[1:4]) {
		return TailOperation{}, nil, false
	}
	keyBytes := uint64(binary.LittleEndian.Uint32(raw[4:8]))
	valueBytes := uint64(binary.LittleEndian.Uint32(raw[8:12]))
	payload := keyBytes + valueBytes
	if keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes ||
		valueBytes > replication.MaxMutationValueBytes || payload > uint64(len(raw)-tailBatchOperationHeaderBytes) {
		return TailOperation{}, nil, false
	}
	kind := replication.MutationKind(raw[0])
	if (kind == replication.MutationPut && valueBytes == 0) ||
		(kind == replication.MutationDelete && valueBytes != 0) ||
		(kind != replication.MutationPut && kind != replication.MutationDelete) {
		return TailOperation{}, nil, false
	}
	start := tailBatchOperationHeaderBytes
	keyEnd := start + int(keyBytes)
	valueEnd := keyEnd + int(valueBytes)
	operation := TailOperation{Kind: kind, Key: raw[start:keyEnd:keyEnd]}
	if kind == replication.MutationPut {
		operation.Value = raw[keyEnd:valueEnd:valueEnd]
	}
	return operation, raw[valueEnd:], true
}

func tailBatchWireDigest(workspace *TailBatchCodecWorkspace, raw []byte) {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	workspace.hasher.Reset()
	_, _ = workspace.hasher.Write(tailBatchWireDomain)
	_, _ = workspace.hasher.Write(raw)
	_ = workspace.hasher.Sum(workspace.digest[:0])
}
