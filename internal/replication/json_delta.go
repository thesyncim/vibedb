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

// JSONInt64DeltasMagic is the plural form of the same apply-time operation.
// It carries two or more independent top-level field deltas for one row. The
// separate descriptor version keeps JID1 byte compatibility for existing
// single-field recipes while allowing the apply contract to reject malformed
// plural payloads before they reach the state machine.
const JSONInt64DeltasMagic = "JID2"

const jsonInt64DeltaHeaderBytes = 16 // magic, column length, reserved, delta

const (
	jsonInt64DeltasHeaderBytes = 8  // magic, field count, reserved
	jsonInt64DeltaFieldBytes   = 10 // column length, signed delta
	// MaxJSONInt64DeltaFields matches the SQL UPDATE assignment bound. Keeping
	// the wire bound here prevents an otherwise valid mutation value from
	// causing an unbounded descriptor walk at apply time.
	MaxJSONInt64DeltaFields = 1024
)

// JSONInt64DeltaField is one descriptor item passed to the plural encoder.
// Column is a decoded SQL identifier and is copied into the command value.
type JSONInt64DeltaField struct {
	Column string
	Delta  int64
}

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

// AppendJSONInt64Deltas appends a bounded JID2 descriptor. The caller must
// provide at least two distinct fields; SQL lowering performs the same check
// before publication, while this encoder also protects non-SQL callers.
func AppendJSONInt64Deltas(dst []byte, fields []JSONInt64DeltaField) ([]byte, error) {
	if len(fields) < 2 || len(fields) > MaxJSONInt64DeltaFields {
		return dst, semantic("JSON integer delta field count")
	}
	start := len(dst)
	dst = append(dst, JSONInt64DeltasMagic...)
	dst = append(dst, 0, 0, 0, 0)
	if len(fields) > int(^uint16(0)) {
		return dst[:start], semantic("JSON integer delta field count")
	}
	binary.LittleEndian.PutUint16(dst[start+4:start+6], uint16(len(fields)))
	for index, field := range fields {
		if len(field.Column) == 0 || len(field.Column) > MaxIdentityBytes ||
			!utf8.ValidString(field.Column) || len(field.Column) > int(^uint16(0)) {
			return dst[:start], semantic("JSON integer delta column length")
		}
		for prior := 0; prior < index; prior++ {
			if fields[prior].Column == field.Column {
				return dst[:start], semantic("JSON integer delta duplicate column")
			}
		}
		if len(dst)-start > MaxMutationValueBytes-jsonInt64DeltaFieldBytes-len(field.Column) {
			return dst[:start], semantic("JSON integer delta descriptor size")
		}
		at := len(dst)
		dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint16(dst[at:at+2], uint16(len(field.Column)))
		binary.LittleEndian.PutUint64(dst[at+2:at+10], uint64(field.Delta))
		dst = append(dst, field.Column...)
	}
	return dst, nil
}

// JSONInt64DeltaIterator opens a borrowed JID2 descriptor without allocating.
// The iterator validates the complete fixed framing and each UTF-8 column at
// open time; Next only returns slices into the immutable command bytes.
type JSONInt64DeltaIterator struct {
	value     []byte
	offset    int
	remaining int
	valid     bool
}

// OpenJSONInt64Deltas validates and opens one borrowed JID2 descriptor.
func OpenJSONInt64Deltas(value []byte) (JSONInt64DeltaIterator, bool) {
	if len(value) < jsonInt64DeltasHeaderBytes || len(value) > MaxMutationValueBytes ||
		!bytes.Equal(value[:4], []byte(JSONInt64DeltasMagic)) ||
		binary.LittleEndian.Uint16(value[6:8]) != 0 {
		return JSONInt64DeltaIterator{}, false
	}
	count := int(binary.LittleEndian.Uint16(value[4:6]))
	if count < 2 || count > MaxJSONInt64DeltaFields {
		return JSONInt64DeltaIterator{}, false
	}
	offset := jsonInt64DeltasHeaderBytes
	for index := 0; index < count; index++ {
		if len(value)-offset < jsonInt64DeltaFieldBytes {
			return JSONInt64DeltaIterator{}, false
		}
		columnBytes := int(binary.LittleEndian.Uint16(value[offset : offset+2]))
		if columnBytes == 0 || columnBytes > MaxIdentityBytes {
			return JSONInt64DeltaIterator{}, false
		}
		end := offset + jsonInt64DeltaFieldBytes + columnBytes
		if end < offset || end > len(value) || !utf8.Valid(value[offset+jsonInt64DeltaFieldBytes:end]) {
			return JSONInt64DeltaIterator{}, false
		}
		offset = end
	}
	if offset != len(value) {
		return JSONInt64DeltaIterator{}, false
	}
	return JSONInt64DeltaIterator{value: value, offset: jsonInt64DeltasHeaderBytes, remaining: count, valid: true}, true
}

// Next returns the next borrowed column and signed delta. It returns ok=false
// after the descriptor is exhausted or if the iterator was zero-valued.
func (iterator *JSONInt64DeltaIterator) Next() (column []byte, delta int64, ok bool) {
	if iterator == nil || !iterator.valid || iterator.remaining == 0 {
		return nil, 0, false
	}
	offset := iterator.offset
	columnBytes := int(binary.LittleEndian.Uint16(iterator.value[offset : offset+2]))
	end := offset + jsonInt64DeltaFieldBytes + columnBytes
	column = iterator.value[offset+jsonInt64DeltaFieldBytes : end]
	delta = int64(binary.LittleEndian.Uint64(iterator.value[offset+2 : offset+10]))
	iterator.offset = end
	iterator.remaining--
	return column, delta, true
}

// Remaining reports the number of fields not yet returned by Next. It is a
// detached parser detail used to size apply scratch; zero-value iterators have
// no remaining fields.
func (iterator JSONInt64DeltaIterator) Remaining() int {
	if !iterator.valid {
		return 0
	}
	return iterator.remaining
}

// ValidJSONInt64Delta accepts either the original JID1 descriptor or the
// versioned JID2 plural descriptor. It additionally rejects duplicate JID2
// field names, which are invalid SQL lowering and would otherwise make one
// command's result depend on descriptor order.
func ValidJSONInt64Delta(value []byte) bool {
	if _, _, ok := OpenJSONInt64Delta(value); ok {
		return true
	}
	iterator, ok := OpenJSONInt64Deltas(value)
	if !ok {
		return false
	}
	count := iterator.Remaining()
	for index := 0; index < count; index++ {
		outer := iterator
		var column []byte
		for skip := 0; skip <= index; skip++ {
			var next bool
			column, _, next = outer.Next()
			if !next {
				return false
			}
		}
		inner := iterator
		for prior := 0; prior < index; prior++ {
			priorColumn, _, next := inner.Next()
			if !next {
				return false
			}
			if bytes.Equal(priorColumn, column) {
				return false
			}
		}
	}
	return true
}
