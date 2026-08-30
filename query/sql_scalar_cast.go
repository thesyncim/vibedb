package query

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/pginput"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

var ErrScalarInvalidText = errors.New("query: invalid text representation")

// ScalarInvalidTextError is intentionally value-redacting: CAST input may be
// application data, so neither database/sql nor pgwire should echo it into a
// log or ErrorResponse. Pos identifies the authored CAST, not a byte inside
// the runtime value.
type ScalarInvalidTextError struct {
	Pos    int
	Target string
}

func (e *ScalarInvalidTextError) Error() string {
	return fmt.Sprintf("query: invalid text representation for CAST AS %s at byte %d: %v",
		e.Target, e.Pos, ErrScalarInvalidText)
}
func (e *ScalarInvalidTextError) Unwrap() error { return ErrScalarInvalidText }
func (e *ScalarInvalidTextError) Position() int { return e.Pos }

func (r *statementScalar) evalCast(
	node *statementScalarNode,
	left statementScalarValue,
	arena *[]byte,
	budget *aggregateBudget,
	intermediate *intermediateBudget,
	intermediateCharge *int64,
) (statementScalarValue, error) {
	if left.value.kind == kindNull {
		return statementScalarValue{value: scalar{kind: kindNull}}, nil
	}
	switch node.cast {
	case sqlast.ScalarCastText:
		return castScalarText(left), nil
	case sqlast.ScalarCastBoolean:
		return castScalarBoolean(node.pos, left)
	case sqlast.ScalarCastNumeric:
		return r.castScalarNumeric(node.pos, left, arena, budget)
	case sqlast.ScalarCastJSON:
		return castScalarJSON(node.pos, left, arena, intermediate, intermediateCharge)
	default:
		return statementScalarValue{}, fmt.Errorf("query: invalid scalar CAST target %d", node.cast)
	}
}

func castScalarText(left statementScalarValue) statementScalarValue {
	if left.value.kind == kindString {
		return left
	}
	var text string
	switch left.value.kind {
	case kindBool:
		text = "false"
		if left.value.bval {
			text = "true"
		}
	case kindNumber:
		text = byteview.String(left.value.num)
	default:
		text = byteview.String(left.value.raw)
	}
	return statementScalarValue{value: scalar{kind: kindString, sval: text}}
}

func castScalarBoolean(pos int, left statementScalarValue) (statementScalarValue, error) {
	if left.value.kind == kindBool {
		return left, nil
	}
	if left.value.kind != kindString {
		return statementScalarValue{}, &ScalarTypeError{
			Pos: pos, Operation: "cast to boolean", Left: valueTypeOfScalar(left.value), Right: TypeAny,
		}
	}
	value, ok := pginput.Boolean(left.value.sval)
	if !ok {
		return statementScalarValue{}, &ScalarInvalidTextError{Pos: pos, Target: "BOOLEAN"}
	}
	return statementScalarValue{value: scalar{kind: kindBool, bval: value}}, nil
}

func (r *statementScalar) castScalarNumeric(
	pos int,
	left statementScalarValue,
	arena *[]byte,
	budget *aggregateBudget,
) (statementScalarValue, error) {
	var number []byte
	switch left.value.kind {
	case kindNumber:
		number = left.value.num
	case kindString:
		var ok bool
		*arena, number, ok = appendSQLNumeric(*arena, left.value.sval)
		if !ok {
			return statementScalarValue{}, &ScalarInvalidTextError{Pos: pos, Target: "NUMERIC"}
		}
	default:
		return statementScalarValue{}, &ScalarTypeError{
			Pos: pos, Operation: "cast to numeric", Left: valueTypeOfScalar(left.value), Right: TypeAny,
		}
	}
	start := len(*arena)
	var err error
	*arena, _, err = r.decimal.binary(sqlScalarAdd, number, zeroNumberBytes, pos, *arena, budget)
	if err != nil {
		return statementScalarValue{}, scalarCastNumericError(pos, err)
	}
	return statementScalarValue{value: classifyComputedNumber((*arena)[start:])}, nil
}

func scalarCastNumericError(pos int, err error) error {
	var bounded *ScalarNumericRangeError
	if errors.As(err, &bounded) {
		return &ScalarNumericRangeError{
			Pos: pos, Operation: "cast to numeric", Requested: bounded.Requested, Limit: bounded.Limit,
		}
	}
	var budget *ScalarAggregateBudgetError
	if errors.As(err, &budget) {
		return &ScalarAggregateBudgetError{Pos: pos, Operation: "cast to numeric", Err: budget.Err}
	}
	return err
}

func castScalarJSON(
	pos int,
	left statementScalarValue,
	arena *[]byte,
	intermediate *intermediateBudget,
	intermediateCharge *int64,
) (statementScalarValue, error) {
	// exact marks a value already admitted by CAST AS JSON. In particular, a
	// JSON string's semantic scalar kind is kindString, but casting json to json
	// is identity: reparsing its decoded contents as SQL text would reject
	// CAST(CAST('"x"' AS JSON) AS JSON) and lose authored escapes.
	if left.exact {
		return left, nil
	}
	if left.value.kind != kindString {
		return left, nil
	}
	text := trimSQLSpace(left.value.sval)
	rawText := byteview.Bytes(text)
	if len(rawText) == 0 || vibejson.Validate(rawText) != nil {
		return statementScalarValue{}, &ScalarInvalidTextError{Pos: pos, Target: "JSON"}
	}
	charge := saturatedProduct(int64(len(rawText)), 2)
	if err := intermediate.reserve("scalar CAST JSON workspace", charge); err != nil {
		return statementScalarValue{}, err
	}
	*intermediateCharge = saturatedBytes(*intermediateCharge, charge)

	// Copy the exact JSON spelling into this row's arena, then reserve enough
	// capacity for worst-case string unescaping before classifying it. The
	// second append is capacity-only and is discarded; reconstructing raw by
	// offsets keeps it valid if either append grows the arena.
	start := len(*arena)
	*arena = append(*arena, rawText...)
	end := len(*arena)
	if cap(*arena)-end < len(rawText) {
		*arena = append(*arena, rawText...)
		*arena = (*arena)[:end]
	}
	raw := (*arena)[start:end:end]
	value := classifyRawInto(vibejson.RawValue{Src: raw}, arena)
	return statementScalarValue{value: value, cell: cellFromScalar(value), exact: true}, nil
}

func trimSQLSpace(text string) string {
	start, end := 0, len(text)
	for start < end && sqlSpace(text[start]) {
		start++
	}
	for end > start && sqlSpace(text[end-1]) {
		end--
	}
	return text[start:end]
}

func sqlSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

// appendSQLNumeric accepts the exact decimal grammar SQL users expect from a
// text cast (optional sign, either side of '.', optional exponent), and emits
// a JSON-number spelling for the executor's exact decimal kernel. It strips
// only syntax JSON forbids: a leading plus, redundant integer leading zeroes,
// and an empty fractional side in "1.". No value passes through float64.
func appendSQLNumeric(dst []byte, text string) ([]byte, []byte, bool) {
	text = trimSQLSpace(text)
	if len(text) == 0 {
		return dst, nil, false
	}
	i, negative := 0, false
	if text[i] == '+' || text[i] == '-' {
		negative = text[i] == '-'
		i++
	}
	intStart := i
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	intEnd := i
	fracStart, fracEnd := i, i
	if i < len(text) && text[i] == '.' {
		i++
		fracStart = i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		fracEnd = i
	}
	if intStart == intEnd && fracStart == fracEnd {
		return dst, nil, false
	}
	expStart, expEnd := i, i
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		expStart = i
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		digits := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == digits {
			return dst, nil, false
		}
		expEnd = i
	}
	if i != len(text) {
		return dst, nil, false
	}

	start := len(dst)
	if negative {
		dst = append(dst, '-')
	}
	for intEnd-intStart > 1 && text[intStart] == '0' {
		intStart++
	}
	if intStart == intEnd {
		dst = append(dst, '0')
	} else {
		dst = append(dst, text[intStart:intEnd]...)
	}
	if fracStart != fracEnd {
		dst = append(dst, '.')
		dst = append(dst, text[fracStart:fracEnd]...)
	}
	if expEnd != expStart {
		dst = append(dst, text[expStart:expEnd]...)
	}
	return dst, dst[start:len(dst):len(dst)], true
}
