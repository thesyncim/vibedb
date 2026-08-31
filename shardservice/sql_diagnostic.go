package shardservice

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"unicode/utf8"
)

// SQLDiagnostic is the bounded, transport-neutral part of one SQL failure.
// Position is a zero-based byte offset into the authored SQL text. The pgwire
// boundary converts it to PostgreSQL's one-based character position.
//
// This value is carried inside ErrorMessage instead of adding fields to
// ShardResponse. Successful responses and legacy error frames therefore keep
// their exact layout and encoding.
type SQLDiagnostic struct {
	Code        string
	Message     string
	Hint        string
	Position    int
	HasPosition bool
}

const (
	sqlDiagnosticEnvelopePrefix  = "VDBSQL:"
	sqlDiagnosticEnvelopeVersion = 1
	maxSQLDiagnosticFieldBytes   = 4 << 10

	sqlDiagnosticFlagPosition = 1 << 0
	sqlDiagnosticKnownFlags   = sqlDiagnosticFlagPosition

	// version + SQLSTATE + flags + optional position + two lengths + fields.
	maxSQLDiagnosticRawBytes = 1 + 5 + 1 + 4 + 2 + maxSQLDiagnosticFieldBytes +
		2 + maxSQLDiagnosticFieldBytes
	maxSQLDiagnosticEncodedBytes = (maxSQLDiagnosticRawBytes*8 + 5) / 6
)

var errBadSQLDiagnostic = errors.New("shardservice: frame carries an invalid SQL diagnostic envelope")

// SQLDiagnostic returns the semantic SQL failure carried by r. Legacy error
// messages, non-malformed error kinds, and malformed reserved envelopes do not
// become SQL errors. The ordinary response decoder bounds the opaque text;
// this stricter decoder then keeps malformed reserved text fail closed for both
// network and direct in-process transports.
func (r *ShardResponse) SQLDiagnostic() (SQLDiagnostic, bool) {
	if r == nil || r.Kind != ResponseError || r.ErrorKind != ErrorMalformedRequest {
		return SQLDiagnostic{}, false
	}
	diagnostic, present, err := decodeSQLDiagnostic(r.ErrorMessage)
	return diagnostic, present && err == nil
}

func newSQLDiagnosticResponse(diagnostic SQLDiagnostic) (*ShardResponse, bool) {
	envelope, err := encodeSQLDiagnostic(diagnostic)
	if err != nil {
		return nil, false
	}
	return NewErrorResponse(ErrorMalformedRequest, envelope), true
}

func encodeSQLDiagnostic(diagnostic SQLDiagnostic) (string, error) {
	if !validSQLState(diagnostic.Code) || diagnostic.Message == "" ||
		len(diagnostic.Message) > maxSQLDiagnosticFieldBytes ||
		len(diagnostic.Hint) > maxSQLDiagnosticFieldBytes ||
		!utf8.ValidString(diagnostic.Message) || !utf8.ValidString(diagnostic.Hint) ||
		diagnostic.Position < 0 ||
		uint64(diagnostic.Position) > uint64(^uint32(0)) ||
		(!diagnostic.HasPosition && diagnostic.Position != 0) {
		return "", errBadSQLDiagnostic
	}

	flags := byte(0)
	positionBytes := 0
	if diagnostic.HasPosition {
		flags |= sqlDiagnosticFlagPosition
		positionBytes = 4
	}
	raw := make([]byte, 0, 1+5+1+positionBytes+2+len(diagnostic.Message)+2+len(diagnostic.Hint))
	raw = append(raw, sqlDiagnosticEnvelopeVersion)
	raw = append(raw, diagnostic.Code...)
	raw = append(raw, flags)
	if diagnostic.HasPosition {
		raw = binary.BigEndian.AppendUint32(raw, uint32(diagnostic.Position))
	}
	raw = binary.BigEndian.AppendUint16(raw, uint16(len(diagnostic.Message)))
	raw = append(raw, diagnostic.Message...)
	raw = binary.BigEndian.AppendUint16(raw, uint16(len(diagnostic.Hint)))
	raw = append(raw, diagnostic.Hint...)

	encoded := make([]byte, len(sqlDiagnosticEnvelopePrefix)+base64.RawStdEncoding.EncodedLen(len(raw)))
	copy(encoded, sqlDiagnosticEnvelopePrefix)
	base64.RawStdEncoding.Encode(encoded[len(sqlDiagnosticEnvelopePrefix):], raw)
	return string(encoded), nil
}

func decodeSQLDiagnostic(envelope string) (SQLDiagnostic, bool, error) {
	if !strings.HasPrefix(envelope, sqlDiagnosticEnvelopePrefix) {
		return SQLDiagnostic{}, false, nil
	}
	body := envelope[len(sqlDiagnosticEnvelopePrefix):]
	if body == "" || len(body) > maxSQLDiagnosticEncodedBytes {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	decodedLen := base64.RawStdEncoding.DecodedLen(len(body))
	if decodedLen < 1+5+1+2+2 || decodedLen > maxSQLDiagnosticRawBytes {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	raw := make([]byte, decodedLen)
	n, err := base64.RawStdEncoding.Strict().Decode(raw, []byte(body))
	if err != nil || n != decodedLen {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	raw = raw[:n]

	index := 0
	if raw[index] != sqlDiagnosticEnvelopeVersion {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	index++
	code := string(raw[index : index+5])
	index += 5
	flags := raw[index]
	index++
	if flags & ^byte(sqlDiagnosticKnownFlags) != 0 {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}

	diagnostic := SQLDiagnostic{Code: code}
	if flags&sqlDiagnosticFlagPosition != 0 {
		if len(raw)-index < 4 {
			return SQLDiagnostic{}, true, errBadSQLDiagnostic
		}
		position := binary.BigEndian.Uint32(raw[index : index+4])
		index += 4
		if uint64(position) > uint64(int(^uint(0)>>1)) {
			return SQLDiagnostic{}, true, errBadSQLDiagnostic
		}
		diagnostic.Position = int(position)
		diagnostic.HasPosition = true
	}

	message, next, ok := sqlDiagnosticField(raw, index)
	if !ok {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	index = next
	hint, next, ok := sqlDiagnosticField(raw, index)
	if !ok || next != len(raw) {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	diagnostic.Message = string(message)
	diagnostic.Hint = string(hint)
	if !validSQLState(diagnostic.Code) || diagnostic.Message == "" ||
		!utf8.ValidString(diagnostic.Message) || !utf8.ValidString(diagnostic.Hint) {
		return SQLDiagnostic{}, true, errBadSQLDiagnostic
	}
	return diagnostic, true, nil
}

func sqlDiagnosticField(raw []byte, index int) ([]byte, int, bool) {
	if index < 0 || len(raw)-index < 2 {
		return nil, index, false
	}
	length := int(binary.BigEndian.Uint16(raw[index : index+2]))
	index += 2
	if length > maxSQLDiagnosticFieldBytes || len(raw)-index < length {
		return nil, index, false
	}
	return raw[index : index+length], index + length, true
}

func validSQLState(code string) bool {
	if len(code) != 5 {
		return false
	}
	for i := range code {
		if code[i] < '0' || code[i] > '9' {
			if code[i] < 'A' || code[i] > 'Z' {
				return false
			}
		}
	}
	return true
}
