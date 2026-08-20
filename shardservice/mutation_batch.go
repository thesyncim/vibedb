package shardservice

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibejson/x/byteview"
)

// MutationStatement is one non-row-returning SQL mutation inside a shard-local
// participant batch. SQL remains text because the ordinary local parser is the
// sole plan authority; typed parameter payloads remain borrowed bytes.
type MutationStatement struct {
	SQL    string
	Params []Param
}

const (
	maxMutationStatements = 4096
	mutationBatchHeader   = 8
)

var (
	mutationBatchMagic = [4]byte{'V', 'M', 'B', '1'}
	ErrMutationBatch   = errors.New("shardservice: invalid participant mutation batch")
)

// AppendMutationBatch appends a deterministic compact batch. Ownership and
// execution limits live once in the participant/apply envelopes, so they are
// not repeated for every statement. The participant record's checksum and
// SHA-256 digest protect these bytes durably and end to end.
func AppendMutationBatch(dst []byte, statements []MutationStatement) ([]byte, error) {
	if len(statements) == 0 || len(statements) > maxMutationStatements {
		return dst, ErrMutationBatch
	}
	total := mutationBatchHeader
	for i := range statements {
		statement := &statements[i]
		if statement.SQL == "" || !utf8.ValidString(statement.SQL) || len(statement.Params) > maxParams {
			return dst, ErrMutationBatch
		}
		total += 8 + len(statement.SQL)
		for j := range statement.Params {
			param := &statement.Params[j]
			if !param.Kind.valid() || !param.Valid() {
				return dst, ErrMutationBatch
			}
			total++
			switch param.Kind {
			case ParamBool:
				total++
			case ParamNumber, ParamString, ParamDocument:
				total += 4 + len(param.Bytes)
			}
			if total > distributedtxn.MaxMutationBytes {
				return dst, distributedtxn.ErrTooLarge
			}
		}
	}
	if total > distributedtxn.MaxMutationBytes {
		return dst, distributedtxn.ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, mutationBatchMagic[:]...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statements)))
	for i := range statements {
		statement := &statements[i]
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.SQL)))
		dst = append(dst, statement.SQL...)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.Params)))
		for j := range statement.Params {
			param := &statement.Params[j]
			dst = append(dst, byte(param.Kind))
			switch param.Kind {
			case ParamBool:
				if param.Bool {
					dst = append(dst, 1)
				} else {
					dst = append(dst, 0)
				}
			case ParamNumber, ParamString, ParamDocument:
				dst = binary.LittleEndian.AppendUint32(dst, uint32(len(param.Bytes)))
				dst = append(dst, param.Bytes...)
			}
		}
	}
	if len(dst)-start != total {
		return dst[:start], ErrMutationBatch
	}
	return dst, nil
}

// MutationBatch is a zero-copy cursor over a validated batch. Next allocates
// only the statement's small parameter descriptor slice; SQL and parameter
// bytes alias the retained participant record.
type MutationBatch struct {
	body      []byte
	remaining uint32
}

// OpenMutationBatch validates the complete batch before it can be applied.
// Validation scans without allocating and returns a fresh cursor on success.
func OpenMutationBatch(src []byte) (MutationBatch, error) {
	if len(src) < mutationBatchHeader || len(src) > distributedtxn.MaxMutationBytes ||
		src[0] != mutationBatchMagic[0] || src[1] != mutationBatchMagic[1] ||
		src[2] != mutationBatchMagic[2] || src[3] != mutationBatchMagic[3] {
		return MutationBatch{}, ErrMutationBatch
	}
	count := binary.LittleEndian.Uint32(src[4:8])
	if count == 0 || count > maxMutationStatements {
		return MutationBatch{}, ErrMutationBatch
	}
	batch := MutationBatch{body: src[mutationBatchHeader:], remaining: count}
	validation := batch
	for validation.remaining != 0 {
		if _, err := validation.next(false); err != nil {
			return MutationBatch{}, err
		}
	}
	if len(validation.body) != 0 {
		return MutationBatch{}, ErrMutationBatch
	}
	return batch, nil
}

// Next returns the next statement and whether one was present.
func (b *MutationBatch) Next() (MutationStatement, bool, error) {
	if b == nil || b.remaining == 0 {
		return MutationStatement{}, false, nil
	}
	statement, err := b.next(true)
	return statement, err == nil, err
}

func (b *MutationBatch) next(materialize bool) (MutationStatement, error) {
	if b.remaining == 0 || len(b.body) < 4 {
		return MutationStatement{}, ErrMutationBatch
	}
	sqlBytes, rest, ok := takeMutationBytes(b.body)
	if !ok || len(sqlBytes) == 0 || !utf8.Valid(sqlBytes) || len(rest) < 4 {
		return MutationStatement{}, ErrMutationBatch
	}
	count := binary.LittleEndian.Uint32(rest[:4])
	rest = rest[4:]
	if count > uint32(maxParams) || uint64(count) > uint64(len(rest)) {
		return MutationStatement{}, ErrMutationBatch
	}
	statement := MutationStatement{SQL: byteview.String(sqlBytes)}
	if materialize && count != 0 {
		statement.Params = make([]Param, int(count))
	}
	for i := uint32(0); i < count; i++ {
		if len(rest) == 0 {
			return MutationStatement{}, ErrMutationBatch
		}
		param := Param{Kind: ParamKind(rest[0])}
		rest = rest[1:]
		switch param.Kind {
		case ParamNull:
		case ParamBool:
			if len(rest) == 0 || rest[0] > 1 {
				return MutationStatement{}, ErrMutationBatch
			}
			param.Bool = rest[0] == 1
			rest = rest[1:]
		case ParamNumber, ParamString, ParamDocument:
			var valid bool
			param.Bytes, rest, valid = takeMutationBytes(rest)
			if !valid {
				return MutationStatement{}, ErrMutationBatch
			}
		default:
			return MutationStatement{}, ErrMutationBatch
		}
		if !param.Valid() {
			return MutationStatement{}, ErrMutationBatch
		}
		if materialize {
			statement.Params[i] = param
		}
	}
	b.body = rest
	b.remaining--
	return statement, nil
}

func takeMutationBytes(src []byte) (value, rest []byte, ok bool) {
	if len(src) < 4 {
		return nil, src, false
	}
	length := uint64(binary.LittleEndian.Uint32(src[:4]))
	if length > uint64(len(src)-4) {
		return nil, src, false
	}
	end := 4 + int(length)
	return src[4:end], src[end:], true
}
