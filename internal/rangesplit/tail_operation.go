package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

// One witnessed operation is the source capture's 56-byte row grammar with
// an explicit dense relation ID in the reserved word. Old values never cross
// the wire: their exact length, placement point and digest suffice.
const tailBatchOperationHeaderBytes = 56

func tailOperationForChild(transition *TailTransition, route tailRoute, child uint8) (TailOperation, bool) {
	operation := TailOperation{Relation: 1, Key: transition.Key}
	if route.before == child {
		operation.BeforeWitness = route.witness
	}
	if route.after == child {
		operation.Kind, operation.Value = replication.MutationPut, transition.After
		return operation, true
	}
	if route.before == child {
		operation.Kind = replication.MutationDelete
		return operation, true
	}
	return TailOperation{}, false
}

func validTailOperation(operation, previous TailOperation) bool {
	if operation.Relation == 0 || operation.Relation > replication.MaxRelationID ||
		len(operation.Key) == 0 || len(operation.Key) > replication.MaxMutationKeyBytes ||
		len(operation.Value) > replication.MaxMutationValueBytes ||
		previous.Key != nil && (operation.Relation < previous.Relation ||
			operation.Relation == previous.Relation && bytes.Compare(previous.Key, operation.Key) >= 0) {
		return false
	}
	before := operation.BeforeWitness
	if before.Present {
		if before.DocumentBytes == 0 || before.DocumentBytes > replication.MaxMutationValueBytes || before.Digest == ([sha256.Size]byte{}) {
			return false
		}
	} else if before != (TailBeforeWitness{}) {
		return false
	}
	switch operation.Kind {
	case replication.MutationPut:
		return len(operation.Value) != 0
	case replication.MutationDelete:
		return operation.Value == nil && before.Present
	default:
		return false
	}
}

func putTailOperationHeader(header []byte, operation TailOperation) {
	clear(header[:tailBatchOperationHeaderBytes])
	if operation.BeforeWitness.Present {
		header[0] |= sourceCaptureBeforePresent
	}
	if operation.Kind == replication.MutationPut {
		header[0] |= sourceCaptureAfterPresent
	}
	binary.LittleEndian.PutUint16(header[2:4], uint16(operation.Relation))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(operation.Key)))
	binary.LittleEndian.PutUint32(header[8:12], operation.BeforeWitness.DocumentBytes)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(operation.Value)))
	copy(header[16:24], operation.BeforeWitness.Point[:])
	copy(header[24:56], operation.BeforeWitness.Digest[:])
}

func hashWitnessedTailOperation(h hash.Hash, scratch []byte, operation TailOperation) {
	putTailOperationHeader(scratch, operation)
	_, _ = h.Write(scratch[:tailBatchOperationHeaderBytes])
	_, _ = h.Write(operation.Key)
	_, _ = h.Write(operation.Value)
}

func openTailWireOperation(raw []byte) (TailOperation, []byte, bool) {
	if len(raw) < tailBatchOperationHeaderBytes || raw[0] == 0 || raw[0]&^sourceCapturePresenceMask != 0 || raw[1] != 0 {
		return TailOperation{}, nil, false
	}
	keyBytes, beforeBytes, valueBytes := uint64(binary.LittleEndian.Uint32(raw[4:8])), binary.LittleEndian.Uint32(raw[8:12]), uint64(binary.LittleEndian.Uint32(raw[12:16]))
	if keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes || valueBytes > replication.MaxMutationValueBytes ||
		keyBytes+valueBytes > uint64(len(raw)-tailBatchOperationHeaderBytes) ||
		(raw[0]&sourceCaptureAfterPresent != 0) != (valueBytes != 0) {
		return TailOperation{}, nil, false
	}
	operation := TailOperation{Relation: replication.RelationID(binary.LittleEndian.Uint16(raw[2:4])), Kind: replication.MutationDelete,
		BeforeWitness: TailBeforeWitness{Present: raw[0]&sourceCaptureBeforePresent != 0, DocumentBytes: beforeBytes,
			Point: distribution.KeyspacePoint(raw[16:24]), Digest: [sha256.Size]byte(raw[24:56])}}
	keyEnd := tailBatchOperationHeaderBytes + int(keyBytes)
	valueEnd := keyEnd + int(valueBytes)
	operation.Key = raw[tailBatchOperationHeaderBytes:keyEnd:keyEnd]
	if valueBytes != 0 {
		operation.Kind, operation.Value = replication.MutationPut, raw[keyEnd:valueEnd:valueEnd]
	}
	if !validTailOperation(operation, TailOperation{}) {
		return TailOperation{}, nil, false
	}
	return operation, raw[valueEnd:], true
}
