package query

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// rawTopLevelScalarMatch scans a validated JSON value without building a
// structural tape. It is deliberately narrower than the general query
// parser: it handles an exact top-level key whose spelling is not escaped and
// returns complete=false when an escaped key could affect the answer. The
// durable reader has already validated its bytes, so a root that is not an
// object is a complete non-match; malformed input still falls back to the
// authoritative Segment path.
func rawTopLevelScalarMatch(
	src []byte,
	key string,
	lit scalar,
	text *[]byte,
) (matched, complete bool) {
	i := rawSkipSpace(src, 0)
	if i >= len(src) || src[i] != '{' {
		return false, true
	}
	i++
	for {
		i = rawSkipSpace(src, i)
		if i >= len(src) {
			return false, false
		}
		if src[i] == '}' {
			return matched, true
		}
		if src[i] != '"' {
			return false, false
		}
		keyStart := i
		keyEnd, escaped, ok := rawScanString(src, i)
		if !ok || escaped {
			// An escaped key can decode to key even when its raw spelling does
			// not equal key. Let the structural path handle that exact case.
			return false, false
		}
		isTarget := keyEnd-keyStart == len(key)+2 &&
			rawEqualString(src[keyStart+1:keyEnd-1], key)
		i = rawSkipSpace(src, keyEnd)
		if i >= len(src) || src[i] != ':' {
			return false, false
		}
		i = rawSkipSpace(src, i+1)
		valueStart := i
		valueEnd, ok := rawSkipValue(src, i)
		if !ok {
			return false, false
		}
		if isTarget {
			matched = rawEqualsScalar(
				vibejson.RawValue{Src: src[valueStart:valueEnd]}, lit, text,
			)
		}
		i = rawSkipSpace(src, valueEnd)
		if i >= len(src) {
			return false, false
		}
		switch src[i] {
		case ',':
			i++
		case '}':
			return matched, true
		default:
			return false, false
		}
	}
}

func rawEqualString(value []byte, want string) bool {
	if len(value) != len(want) {
		return false
	}
	wantBytes := byteview.Bytes(want)
	for i, b := range value {
		if b != wantBytes[i] {
			return false
		}
	}
	return true
}

func rawSkipSpace(src []byte, i int) int {
	for i < len(src) {
		switch src[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func rawScanString(src []byte, start int) (end int, escaped, ok bool) {
	for i := start + 1; i < len(src); i++ {
		switch src[i] {
		case '\\':
			escaped = true
			i++
		case '"':
			return i + 1, escaped, true
		}
	}
	return 0, false, false
}

func rawSkipValue(src []byte, start int) (end int, ok bool) {
	if start >= len(src) {
		return 0, false
	}
	switch src[start] {
	case '"':
		end, _, ok = rawScanString(src, start)
		return end, ok
	case '{', '[':
		depth := 0
		for i := start; i < len(src); i++ {
			switch src[i] {
			case '"':
				var next int
				next, _, ok = rawScanString(src, i)
				if !ok {
					return 0, false
				}
				i = next - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
		}
		return 0, false
	default:
		for i := start; i < len(src); i++ {
			switch src[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return i, i > start
			}
		}
		return len(src), len(src) > start
	}
}

// scalarCountPath recognizes the exact plan shape handled by the fused count
// kernels. Keeping this proof in one place prevents a fast lane from silently
// widening to a query whose result or predicate semantics need the general
// executor.
func (p *plan) scalarCountEqualityPath() (compiledPath, scalar, bool) {
	if p.where == nil || p.where.kind != predCmp || p.where.op != Eq ||
		p.grouped || !p.singleRow || p.where.col < 0 ||
		p.where.col >= len(p.valuePaths) {
		return compiledPath{}, scalar{}, false
	}
	for _, col := range p.columns {
		if col.agg != aggCount || col.value >= 0 {
			return compiledPath{}, scalar{}, false
		}
	}
	return p.valuePaths[p.where.col], p.where.lit, true
}

// scalarCountIntegerOrderPath recognizes the strict ordered COUNT shape. The
// storage lane only accepts an actual int64 literal: decimal and floating
// literals retain the generic executor's exact-number semantics.
func (p *plan) scalarCountIntegerOrderPath() (compiledPath, scalar, Op, bool) {
	if p.where == nil || p.where.kind != predCmp ||
		!orderedRangeOp(p.where.op) || p.grouped || !p.singleRow ||
		p.where.col < 0 || p.where.col >= len(p.valuePaths) ||
		p.where.lit.kind != kindNumber || !p.where.lit.isInt {
		return compiledPath{}, scalar{}, 0, false
	}
	for _, col := range p.columns {
		if col.agg != aggCount || col.value >= 0 {
			return compiledPath{}, scalar{}, 0, false
		}
	}
	return p.valuePaths[p.where.col], p.where.lit, p.where.op, true
}

func (p *plan) scalarCountPath() (compiledPath, scalar, bool) {
	path, lit, ok := p.scalarCountEqualityPath()
	if !ok || !path.single {
		return compiledPath{}, scalar{}, false
	}
	return path, lit, true
}

// rawEqualsScalar is the narrow comparison used by the fused scalar-count
// lanes. It preserves the query scalar contract: strings compare by decoded
// content and numbers by exact decimal value. Stored values have already
// passed JSON validation, so defensive failures are simply non-matches.
func rawEqualsScalar(raw vibejson.RawValue, lit scalar, text *[]byte) bool {
	switch lit.kind {
	case kindBool:
		value, ok := raw.Bool()
		return ok && value == lit.bval
	case kindNumber:
		value, ok := raw.NumberBytes()
		return ok && compareNumberBytes(value, lit.num) == 0
	case kindString:
		if value, ok := raw.StringBytes(); ok {
			return equalBytesString(value, lit.sval)
		}
		bytes := raw.Bytes()
		if len(bytes) == 0 || bytes[0] != '"' {
			return false
		}
		decoded, ok, err := raw.AppendText((*text)[:0])
		if err != nil || !ok {
			return false
		}
		*text = decoded
		return equalBytesString(decoded, lit.sval)
	default:
		return false
	}
}

func equalBytesString(value []byte, want string) bool {
	if len(value) != len(want) {
		return false
	}
	wantBytes := byteview.Bytes(want)
	for i, b := range value {
		if b != wantBytes[i] {
			return false
		}
	}
	return true
}
