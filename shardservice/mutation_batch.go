package shardservice

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// MutationKind identifies one target-batch entry. The zero value remains
// ordinary SQL so existing constructors and the routed SQL lane stay compact.
// Typed entries use the formerly invalid zero SQL-length sentinel inside the
// same batch format; there is no parallel or named protocol generation.
type MutationKind uint8

const (
	MutationSQL MutationKind = iota
	MutationGlobalIndexPut
	MutationGlobalIndexDelete
	MutationPrimaryPrecondition
	MutationPrimaryCheck
)

func (k MutationKind) valid() bool {
	return k >= MutationSQL && k <= MutationPrimaryCheck
}

// MutationStatement is one non-row-returning mutation inside a shard-local
// target batch. Ordinary SQL remains text because the local parser is its
// plan authority. Global-index entries are byte-native: Relation is cold
// identity, EntryKey is the canonical tuple key, and Value is the bounded JSON
// locator array produced with vibejson. All decoded payloads borrow the durable
// target record.
type MutationStatement struct {
	Kind       MutationKind
	SQL        string
	Params     []Param
	ParamTypes []sqldriver.ParamType

	Relation     string
	IndexID      uint64
	Incarnation  uint64
	EntryKey     []byte
	Value        []byte
	LocatorCount uint8
	Unique       bool

	// PrimaryPath and the sorted key/digest pairs belong only to a base-row
	// precondition. Keys are native durable primary keys; digests are SHA-256 of
	// the captured canonical documents. Decoded key slices borrow the batch.
	PrimaryPath     []byte
	ExpectedKeys    [][]byte
	ExpectedDigests [][sha256.Size]byte
}

const (
	// MaxMutationStatements is the canonical per-target statement bound.
	// Gateways use the same authority during planning so they reject excess
	// before retaining it or attempting target encoding.
	MaxMutationStatements      = 4096
	maxMutationRelationBytes   = 1<<16 - 1
	mutationBatchHeader        = 8
	globalMutationFixedBytes   = 4 + 1 + 1 + 2 + 8 + 8 + 4 + 4 + 4
	primaryConditionFixedBytes = 4 + 1 + 1 + 2 + 4 + 4 + 4
	mutationParamTypesFlag     = uint32(1) << 31
)

var (
	mutationBatchMagic = [4]byte{'V', 'M', 'B', '1'}
	ErrMutationBatch   = errors.New("shardservice: invalid participant mutation batch")
)

// AppendMutationBatch appends a deterministic compact batch. Ownership and
// execution limits live once in the target/apply envelopes, so they are
// not repeated for every statement. The target record's checksum and
// SHA-256 digest protect these bytes durably and end to end.
func AppendMutationBatch(dst []byte, statements []MutationStatement) ([]byte, error) {
	if len(statements) == 0 || len(statements) > MaxMutationStatements {
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
				!validSQLParameterTypes(statement.Params, statement.ParamTypes) ||
				statement.IndexID != 0 || statement.Incarnation != 0 ||
				len(statement.EntryKey) != 0 || len(statement.Value) != 0 ||
				statement.LocatorCount != 0 || statement.Unique ||
				len(statement.PrimaryPath) != 0 || len(statement.ExpectedKeys) != 0 ||
				len(statement.ExpectedDigests) != 0 {
				return dst, ErrMutationBatch
			}
			total += 8 + len(statement.SQL) + len(statement.ParamTypes)
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
		if statement.Kind == MutationPrimaryPrecondition || statement.Kind == MutationPrimaryCheck {
			if statement.SQL != "" || len(statement.Params) != 0 || len(statement.ParamTypes) != 0 ||
				statement.Relation == "" || len(statement.Relation) > maxMutationRelationBytes ||
				!utf8.ValidString(statement.Relation) || statement.IndexID != 0 ||
				statement.Incarnation != 0 || len(statement.EntryKey) != 0 ||
				len(statement.Value) != 0 || statement.LocatorCount != 0 || statement.Unique ||
				len(statement.PrimaryPath) == 0 || len(statement.PrimaryPath) > maxMutationRelationBytes ||
				!utf8.Valid(statement.PrimaryPath) ||
				len(statement.ExpectedKeys) != len(statement.ExpectedDigests) ||
				len(statement.ExpectedKeys) > maxParams {
				return dst, ErrMutationBatch
			}
			total += primaryConditionFixedBytes + len(statement.Relation) + len(statement.PrimaryPath)
			for j := range statement.ExpectedKeys {
				key := statement.ExpectedKeys[j]
				if len(key) == 0 || (j != 0 && bytes.Compare(statement.ExpectedKeys[j-1], key) >= 0) ||
					len(key) > distributedtxn.MaxMutationBytes-total-4-sha256.Size {
					return dst, ErrMutationBatch
				}
				total += 4 + len(key) + sha256.Size
				if total > distributedtxn.MaxMutationBytes {
					return dst, distributedtxn.ErrTooLarge
				}
			}
			continue
		}
		if statement.SQL != "" || len(statement.Params) != 0 || len(statement.ParamTypes) != 0 ||
			statement.Relation == "" ||
			len(statement.Relation) > maxMutationRelationBytes ||
			!utf8.ValidString(statement.Relation) || statement.IndexID == 0 ||
			statement.Incarnation == 0 || len(statement.EntryKey) == 0 ||
			statement.EntryKey[0] == 0 || statement.LocatorCount == 0 ||
			statement.LocatorCount > 8 ||
			len(statement.Value) == 0 || !vibejson.Valid(statement.Value) ||
			len(statement.PrimaryPath) != 0 || len(statement.ExpectedKeys) != 0 ||
			len(statement.ExpectedDigests) != 0 ||
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
		if statement.Kind == MutationPrimaryPrecondition || statement.Kind == MutationPrimaryCheck {
			dst = binary.LittleEndian.AppendUint32(dst, 0)
			dst = append(dst, byte(statement.Kind), 0, 0, 0)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.Relation)))
			dst = append(dst, statement.Relation...)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.PrimaryPath)))
			dst = append(dst, statement.PrimaryPath...)
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.ExpectedKeys)))
			for j := range statement.ExpectedKeys {
				dst = binary.LittleEndian.AppendUint32(dst, uint32(len(statement.ExpectedKeys[j])))
				dst = append(dst, statement.ExpectedKeys[j]...)
				dst = append(dst, statement.ExpectedDigests[j][:]...)
			}
			continue
		}
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
		paramCount := uint32(len(statement.Params))
		if len(statement.ParamTypes) != 0 {
			paramCount |= mutationParamTypesFlag
		}
		dst = binary.LittleEndian.AppendUint32(dst, paramCount)
		for _, parameterType := range statement.ParamTypes {
			dst = append(dst, byte(parameterType))
		}
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
// bytes alias the retained target record.
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
	if count == 0 || count > MaxMutationStatements {
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
		if len(b.body) < 5 {
			return MutationStatement{}, ErrMutationBatch
		}
		if MutationKind(b.body[4]) == MutationPrimaryPrecondition ||
			MutationKind(b.body[4]) == MutationPrimaryCheck {
			return b.nextPrimaryPrecondition(materialize)
		}
		return b.nextGlobalIndex()
	}
	sqlBytes, rest, ok := takeMutationBytes(b.body)
	if !ok || len(sqlBytes) == 0 || !utf8.Valid(sqlBytes) || len(rest) < 4 {
		return MutationStatement{}, ErrMutationBatch
	}
	rawCount := binary.LittleEndian.Uint32(rest[:4])
	rest = rest[4:]
	hasParameterTypes := rawCount&mutationParamTypesFlag != 0
	count := rawCount &^ mutationParamTypesFlag
	if count > uint32(maxParams) || uint64(count) > uint64(len(rest)) {
		return MutationStatement{}, ErrMutationBatch
	}
	statement := MutationStatement{SQL: byteview.String(sqlBytes)}
	var parameterTypeBytes []byte
	if hasParameterTypes {
		if count == 0 || uint64(count) > uint64(len(rest)) {
			return MutationStatement{}, ErrMutationBatch
		}
		parameterTypeBytes, rest = rest[:count], rest[count:]
		hasConcreteType := false
		for _, encoded := range parameterTypeBytes {
			parameterType := sqldriver.ParamType(encoded)
			if parameterType >= sqldriver.ParamTypeInvalid {
				return MutationStatement{}, ErrMutationBatch
			}
			hasConcreteType = hasConcreteType || parameterType != sqldriver.ParamTypeUnspecified
		}
		if !hasConcreteType {
			return MutationStatement{}, ErrMutationBatch
		}
		if materialize {
			statement.ParamTypes = make([]sqldriver.ParamType, int(count))
			for index, encoded := range parameterTypeBytes {
				statement.ParamTypes[index] = sqldriver.ParamType(encoded)
			}
		}
	}
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
		if param.Kind == ParamDocument && hasParameterTypes &&
			sqldriver.ParamType(parameterTypeBytes[i]) != sqldriver.ParamTypeUnspecified {
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

func (b *MutationBatch) nextPrimaryPrecondition(materialize bool) (MutationStatement, error) {
	rest := b.body[4:]
	if len(rest) < primaryConditionFixedBytes-4 ||
		(MutationKind(rest[0]) != MutationPrimaryPrecondition && MutationKind(rest[0]) != MutationPrimaryCheck) ||
		rest[1] != 0 || rest[2] != 0 || rest[3] != 0 {
		return MutationStatement{}, ErrMutationBatch
	}
	rest = rest[4:]
	relation, rest, ok := takeMutationBytes(rest)
	if !ok || len(relation) == 0 || len(relation) > maxMutationRelationBytes || !utf8.Valid(relation) {
		return MutationStatement{}, ErrMutationBatch
	}
	primaryPath, rest, ok := takeMutationBytes(rest)
	if !ok || len(primaryPath) == 0 || len(primaryPath) > maxMutationRelationBytes ||
		!utf8.Valid(primaryPath) || len(rest) < 4 {
		return MutationStatement{}, ErrMutationBatch
	}
	count := binary.LittleEndian.Uint32(rest[:4])
	rest = rest[4:]
	if count > maxParams || uint64(count)*(4+sha256.Size) > uint64(len(rest)) {
		return MutationStatement{}, ErrMutationBatch
	}
	statement := MutationStatement{
		Kind: MutationKind(b.body[4]), Relation: byteview.String(relation), PrimaryPath: primaryPath,
	}
	if materialize {
		statement.ExpectedKeys = make([][]byte, int(count))
		statement.ExpectedDigests = make([][sha256.Size]byte, int(count))
	}
	var previous []byte
	for i := uint32(0); i < count; i++ {
		key, next, valid := takeMutationBytes(rest)
		if !valid || len(key) == 0 || (i != 0 && bytes.Compare(previous, key) >= 0) ||
			len(next) < sha256.Size {
			return MutationStatement{}, ErrMutationBatch
		}
		if materialize {
			statement.ExpectedKeys[i] = key
			copy(statement.ExpectedDigests[i][:], next[:sha256.Size])
		}
		previous = key
		rest = next[sha256.Size:]
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
