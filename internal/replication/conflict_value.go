package replication

import (
	"bytes"
	"encoding/binary"
)

// AppendConflictValue encodes a candidate row and a deterministic conflict
// program. This value is command input, never a stored JSON document. VUC2
// fixes the payload grammar independently of the surrounding command envelope.
func AppendConflictValue(dst, candidate, program []byte) ([]byte, error) {
	if len(candidate) == 0 || len(program) == 0 || len(candidate) > MaxMutationValueBytes-8 || len(program) > MaxMutationValueBytes-8-len(candidate) {
		return nil, semantic("conflict value length")
	}
	dst = append(dst, "VUC2"...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(candidate)))
	dst = append(dst, candidate...)
	return append(dst, program...), nil
}

// OpenConflictValue lends capacity-clamped candidate/program slices from one
// bounded payload. The relation validator owns the program's closed grammar.
func OpenConflictValue(value []byte) (candidate, program []byte, ok bool) {
	if len(value) < 10 || len(value) > MaxMutationValueBytes || !bytes.Equal(value[:4], []byte("VUC2")) {
		return nil, nil, false
	}
	n := uint64(binary.LittleEndian.Uint32(value[4:8]))
	if n == 0 || n >= uint64(len(value)-8) {
		return nil, nil, false
	}
	end := 8 + int(n)
	return value[8:end:end], value[end:len(value):len(value)], true
}
