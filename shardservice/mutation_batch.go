package shardservice

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// MutationKind identifies one participant-batch entry. The zero value remains
// ordinary SQL so existing constructors and the routed SQL lane stay compact.
// Typed entries use the formerly invalid zero SQL-length sentinel inside the
// same batch format; there is no parallel or named protocol generation.
type MutationKind uint8

const (
	MutationSQL MutationKind = iota
	MutationGlobalIndexPut
	MutationGlobalIndexDelete
)

func (k MutationKind) valid() bool {
	return k >= MutationSQL && k <= MutationGlobalIndexDelete
}

// MutationStatement is one non-row-returning mutation inside a shard-local
// participant batch. Ordinary SQL remains text because the local parser is its
// plan authority. Global-index entries are byte-native: Relation is cold
// identity, EntryKey is the canonical tuple key, and Value is the bounded JSON
// locator array produced with vibejson. All decoded payloads borrow the durable
// participant record.
type MutationStatement struct {
	Kind   MutationKind
	SQL    string
	Params []Param

	Relation     string
	IndexID      uint64
	Incarnation  uint64
	EntryKey     []byte
	Value        []byte
	LocatorCount uint8
	Unique       bool
}

const (
	maxMutationStatements    = 4096
	maxMutationRelationBytes = 1<<16 - 1
	mutationBatchHeader      = 8
	globalMutationFixedBytes = 4 + 1 + 1 + 2 + 8 + 8 + 4 + 4 + 4
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
		if !statement.Kind.valid() {
			return dst, ErrMutationBatch
		}
		if statement.Kind == MutationSQL {
			if statement.SQL == "" || !utf8.ValidString(statement.SQL) ||
				len(statement.Params) > maxParams || statement.Relation != "" ||
				statement.IndexID != 0 || statement.Incarnation != 0 ||
				len(statement.EntryKey) != 0 || len(statement.Value) != 0 ||
				statement.LocatorCount != 0 || statement.Unique {
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
			continue
		}
		if statement.SQL != "" || len(statement.Params) != 0 ||
			statement.Relation == "" ||
			len(statement.Relation) > maxMutationRelationBytes ||
			!utf8.ValidString(statement.Relation) || statement.IndexID == 0 ||
			statement.Incarnation == 0 || len(statement.EntryKey) == 0 ||
			statement.EntryKey[0] == 0 || statement.LocatorCount == 0 ||
			statement.LocatorCount > 8 ||
			len(statement.Value) == 0 || !vibejson.Valid(statement.Value) ||
			(vibejson.RawValue{Src: statement.Value}).Kind() != jsondoc.Array {
			return dst, ErrMutationBatch
		}
		if len(statement.Relation) > distributedtxn.MaxMutationBytes-total-globalMutationFixedBytes ||
			len(statement.EntryKey) > distributedtxn.MaxMutationBytes-total-globalMutationFixedBytes-len(statement.Relation) ||
			len(statement.Value) > distributedtxn.MaxMutationBytes-total-globalMutationFixedBytes-len(statement.Relation)-len(statement.EntryKey) {
			return dst, distributedtxn.ErrTooLarge
		}
		total += globalMutationFixedBytes + len(statement.Relation) +
			len(statement.EntryKey) + len(statement.Value)
		if total > distributedtxn.MaxMutationBytes {
			return dst, distributedtxn.ErrTooLarge
		}
		if statement.Kind == MutationGlobalIndexDelete && statement.Unique {
			// Uniqueness changes PUT conflict semantics; DELETE always compares
			// the expected locator and does not need another wire bit.
			return dst, ErrMutationBatch
		}
		if statement.Kind != MutationGlobalIndexPut && statement.Unique {
			return dst, ErrMutationBatch
		}
		if statement.Kind == MutationGlobalIndexDelete && len(statement.Value) == 0 {
			return dst, ErrMutationBatch
		}
		if statement.Kind != MutationGlobalIndexPut && statement.Kind != MutationGlobalIndexDelete {
			return dst, ErrMutationBatch
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
		if statement.Kind != MutationSQL {
			dst = binary.LittleEndian.AppendUint32(dst, 0)
			dst = append(dst, byte(statement.Kind))
			if statement.Unique {
				dst = append(dst, 1)
			} else {
				dst = append(dst, 0)
			}
			dst = append(dst, statement.LocatorCount, 0)
			dst = binary.LittleEndian.AppendUint64(dst, statement.IndexID)
			dst = binary.LittleEndian.AppendUint64(dst, statement.Incarnation)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.Relation)))
			dst = append(dst, statement.Relation...)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.EntryKey)))
			dst = append(dst, statement.EntryKey...)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.Value)))
			dst = append(dst, statement.Value...)
			continue
		}
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
	if binary.LittleEndian.Uint32(b.body[:4]) == 0 {
		return b.nextGlobalIndex()
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

func (b *MutationBatch) nextGlobalIndex() (MutationStatement, error) {
	rest := b.body[4:]
	if len(rest) < globalMutationFixedBytes-4 || rest[1] > 1 ||
		rest[2] == 0 || rest[2] > 8 || rest[3] != 0 {
		return MutationStatement{}, ErrMutationBatch
	}
	statement := MutationStatement{
		Kind: MutationKind(rest[0]), Unique: rest[1] == 1, LocatorCount: rest[2],
		IndexID:     binary.LittleEndian.Uint64(rest[4:12]),
		Incarnation: binary.LittleEndian.Uint64(rest[12:20]),
	}
	rest = rest[20:]
	relation, rest, ok := takeMutationBytes(rest)
	if !ok || len(relation) == 0 || len(relation) > maxMutationRelationBytes ||
		!utf8.Valid(relation) {
		return MutationStatement{}, ErrMutationBatch
	}
	statement.Relation = byteview.String(relation)
	statement.EntryKey, rest, ok = takeMutationBytes(rest)
	if !ok || len(statement.EntryKey) == 0 || statement.EntryKey[0] == 0 {
		return MutationStatement{}, ErrMutationBatch
	}
	statement.Value, rest, ok = takeMutationBytes(rest)
	if !ok || len(statement.Value) == 0 || !vibejson.Valid(statement.Value) ||
		(vibejson.RawValue{Src: statement.Value}).Kind() != jsondoc.Array ||
		statement.IndexID == 0 || statement.Incarnation == 0 ||
		!statement.Kind.valid() || statement.Kind == MutationSQL ||
		(statement.Kind == MutationGlobalIndexDelete && statement.Unique) {
		return MutationStatement{}, ErrMutationBatch
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
