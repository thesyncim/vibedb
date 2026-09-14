package replication

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

// JSONInt64Delta is the native relation mutation used by the prepared direct
// SQL lane for an atomic update of one top-level JSON integer field. The
// payload is an operation descriptor, never a stored document. The state
// machine evaluates it against the row that is current at its Raft index.
// Keeping the operation in the command is what makes retries and failover
// replay the same logical increment instead of replaying a gateway preimage.
const JSONInt64DeltaMagic = "JID1"

const jsonInt64DeltaHeaderBytes = 16 // magic, column length, reserved, delta

// AppendJSONInt64Delta appends one bounded, versioned integer-delta descriptor.
// The column is a decoded top-level SQL identifier. Its UTF-8 bytes are kept
// verbatim so the descriptor remains deterministic across replicas.
func AppendJSONInt64Delta(dst []byte, column string, delta int64) ([]byte, error) {
	if len(column) == 0 || len(column) > MaxIdentityBytes ||
		!utf8.ValidString(column) || len(column) > int(^uint16(0)) ||
		jsonInt64DeltaHeaderBytes+len(column) > MaxMutationValueBytes {
		return dst, semantic("JSON integer delta column length")
	}
	start := len(dst)
	dst = append(dst, JSONInt64DeltaMagic...)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint16(dst[start+4:start+6], uint16(len(column)))
	binary.LittleEndian.PutUint64(dst[start+8:start+16], uint64(delta))
	return append(dst, column...), nil
}

// OpenJSONInt64Delta validates and opens one borrowed operation descriptor.
// The returned column aliases value and remains valid only while value is
// immutable and live. No allocation occurs on either success or failure.
func OpenJSONInt64Delta(value []byte) (column []byte, delta int64, ok bool) {
	if len(value) < jsonInt64DeltaHeaderBytes || len(value) > MaxMutationValueBytes ||
		!bytes.Equal(value[:4], []byte(JSONInt64DeltaMagic)) ||
		binary.LittleEndian.Uint16(value[6:8]) != 0 {
		return nil, 0, false
	}
	columnBytes := int(binary.LittleEndian.Uint16(value[4:6]))
	if columnBytes == 0 || columnBytes > MaxIdentityBytes ||
		jsonInt64DeltaHeaderBytes+columnBytes != len(value) ||
		!utf8.Valid(value[jsonInt64DeltaHeaderBytes:]) {
		return nil, 0, false
	}
	return value[jsonInt64DeltaHeaderBytes:], int64(binary.LittleEndian.Uint64(value[8:16])), true
}
