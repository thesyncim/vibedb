package query

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

func builtinScalarFuncValueType(name string) ValueType {
	switch name {
	case "length", "char_length", "character_length", "octet_length", "strpos", "ascii",
		"abs", "ceil", "ceiling", "floor", "round", "trunc", "power", "pow", "sqrt",
		"mod", "sign", "ln", "log", "log10", "exp", "pi",
		"extract", "json_array_length", "jsonb_array_length",
		"array_length", "cardinality", "regexp_count":
		return TypeNumber
	case "starts_with", "regexp_like":
		return TypeBool
	case "json_build_object", "jsonb_build_object", "json_build_array", "jsonb_build_array",
		"to_json", "to_jsonb", "json_extract_path", "jsonb_extract_path",
		"json_strip_nulls", "jsonb_strip_nulls", "jsonb_set", "json_set", "string_to_array":
		return TypeAny
	default:
		return TypeString
	}
}

func builtinScalarFuncRepresentation(name string) OutputRepresentation {
	switch builtinScalarFuncValueType(name) {
	case TypeNumber:
		return OutputSQLNumber
	case TypeBool:
		return OutputSQLBool
	case TypeString:
		return OutputSQLText
	default:
		return OutputJSON
	}
}

func (r *statementScalar) evalFunc(node *statementScalarNode, arena *[]byte) (statementScalarValue, error) {
	argIdx := node.funcArgs
	argc := len(argIdx)
	name := node.funcName

	// Non-strict functions (handle NULLs themselves).
	switch name {
	case "concat":
		start := len(*arena)
		for _, idx := range argIdx {
			a := r.values[idx]
			if a.isNull() {
				continue
			}
			*arena = appendScalarDisplay(*arena, a)
		}
		return arenaStringValue(arena, start), nil
	case "concat_ws":
		if argc < 1 {
			return statementScalarValue{}, fmt.Errorf("%w: concat_ws requires at least 1 argument", ErrScalarType)
		}
		first := r.values[argIdx[0]]
		if first.isNull() {
			return nullScalarValue(), nil
		}
		var sep string
		if first.value.kind == kindString {
			sep = first.value.sval
		} else {
			sepStart := len(*arena)
			*arena = appendScalarDisplay(*arena, first)
			sep = byteview.String((*arena)[sepStart:len(*arena):len(*arena)])
		}
		start := len(*arena)
		wrote := false
		for _, idx := range argIdx[1:] {
			a := r.values[idx]
			if a.isNull() {
				continue
			}
			if wrote {
				*arena = append(*arena, sep...)
			}
			*arena = appendScalarDisplay(*arena, a)
			wrote = true
		}
		return arenaStringValue(arena, start), nil
	case "now", "current_timestamp", "clock_timestamp", "statement_timestamp":
		start := len(*arena)
		*arena = time.Now().UTC().AppendFormat(*arena, time.RFC3339Nano)
		return arenaStringValue(arena, start), nil
	case "current_date":
		start := len(*arena)
		*arena = time.Now().UTC().AppendFormat(*arena, "2006-01-02")
		return arenaStringValue(arena, start), nil
	case "gen_random_uuid":
		var u [16]byte
		_, _ = rand.Read(u[:])
		u[6] = (u[6] & 0x0f) | 0x40
		u[8] = (u[8] & 0x3f) | 0x80
		start := len(*arena)
		*arena = appendUUID(*arena, u)
		return arenaStringValue(arena, start), nil
	case "pi":
		return statementScalarValue{value: arenaFloatScalar(arena, math.Pi)}, nil
	case "json_build_array", "jsonb_build_array":
		start := len(*arena)
		*arena = append(*arena, '[')
		for i, idx := range argIdx {
			if i > 0 {
				*arena = append(*arena, ',')
			}
			*arena = appendScalarJSON(*arena, r.values[idx])
		}
		*arena = append(*arena, ']')
		return arenaJSONValue(arena, start), nil
	case "json_build_object", "jsonb_build_object":
		if argc%2 != 0 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires an even number of arguments", ErrScalarType, name)
		}
		nPairs := argc / 2
		if name == "jsonb_build_object" && nPairs > 1 {
			var keyStack [16]string
			var keys []string
			if nPairs <= len(keyStack) {
				keys = keyStack[:nPairs]
			} else {
				keys = make([]string, nPairs)
			}
			for i := 0; i < nPairs; i++ {
				k := r.values[argIdx[i*2]]
				if k.isNull() {
					return statementScalarValue{}, fmt.Errorf("%w: %s key must not be null", ErrScalarType, name)
				}
				if k.value.kind == kindString {
					keys[i] = k.value.sval
				} else {
					ks := len(*arena)
					*arena = appendScalarDisplay(*arena, k)
					keys[i] = byteview.String((*arena)[ks:len(*arena):len(*arena)])
				}
			}
			start := len(*arena)
			*arena = append(*arena, '{')
			wrote := false
			for i := 0; i < nPairs; i++ {
				dup := false
				for j := i + 1; j < nPairs; j++ {
					if keys[j] == keys[i] {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				if wrote {
					*arena = append(*arena, ',')
				}
				wrote = true
				*arena = appendJSONString(*arena, keys[i])
				*arena = append(*arena, ':')
				*arena = appendScalarJSON(*arena, r.values[argIdx[i*2+1]])
			}
			*arena = append(*arena, '}')
			return arenaJSONValue(arena, start), nil
		}
		start := len(*arena)
		*arena = append(*arena, '{')
		for i := 0; i < argc; i += 2 {
			k := r.values[argIdx[i]]
			if k.isNull() {
				return statementScalarValue{}, fmt.Errorf("%w: %s key must not be null", ErrScalarType, name)
			}
			if i > 0 {
				*arena = append(*arena, ',')
			}
			if k.value.kind == kindString {
				*arena = appendJSONString(*arena, k.value.sval)
			} else {
				var kbuf [64]byte
				kb := appendScalarDisplay(kbuf[:0], k)
				*arena = appendJSONString(*arena, byteview.String(kb))
			}
			*arena = append(*arena, ':')
			*arena = appendScalarJSON(*arena, r.values[argIdx[i+1]])
		}
		*arena = append(*arena, '}')
		return arenaJSONValue(arena, start), nil
	case "to_json", "to_jsonb":
		if argc != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		start := len(*arena)
		*arena = appendScalarJSON(*arena, r.values[argIdx[0]])
		return arenaJSONValue(arena, start), nil
	case "json_typeof", "jsonb_typeof":
		if argc != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		v := r.values[argIdx[0]]
		if v.isNull() {
			return nullScalarValue(), nil
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: scalarJSONTypeOf(v)}}, nil
	}

	// Strict functions: any NULL argument yields NULL.
	for _, idx := range argIdx {
		if r.values[idx].isNull() {
			return nullScalarValue(), nil
		}
	}

	switch name {
	case "lower":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: appendLower(arena, s)}}, nil
	case "upper":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: appendUpper(arena, s)}}, nil
	case "length", "char_length", "character_length":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaIntScalar(arena, int64(runeCountFast(s)))}, nil
	case "octet_length":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaIntScalar(arena, int64(len(s)))}, nil
	case "trim", "btrim":
		if argc == 1 {
			s, err := requireStringArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimSpace(s)}}, nil
		}
		if argc == 2 {
			s, err := requireStringArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			cut, err := requireStringArg(name, r.values[argIdx[1]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.Trim(s, cut)}}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s takes 1 or 2 arguments", ErrScalarType, name)
	case "ltrim":
		if argc == 1 {
			s, err := requireStringArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: trimLeftSpaceFast(s)}}, nil
		}
		if argc == 2 {
			s, err := requireStringArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			cut, err := requireStringArg(name, r.values[argIdx[1]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimLeft(s, cut)}}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s takes 1 or 2 arguments", ErrScalarType, name)
	case "rtrim":
		if argc == 1 {
			s, err := requireStringArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: trimRightSpaceFast(s)}}, nil
		}
		if argc == 2 {
			s, err := requireStringArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			cut, err := requireStringArg(name, r.values[argIdx[1]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimRight(s, cut)}}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s takes 1 or 2 arguments", ErrScalarType, name)
	case "replace":
		if argc != 3 {
			return statementScalarValue{}, fmt.Errorf("%w: replace requires 3 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		oldS, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		newS, err := requireStringArg(name, r.values[argIdx[2]])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: appendReplaceAll(arena, s, oldS, newS)}}, nil
	case "substring", "substr":
		if argc < 2 || argc > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: %s takes 2 or 3 arguments", ErrScalarType, name)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		startF, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		hasLen := argc == 3
		var nF float64
		if hasLen {
			nF, err = requireNumberArg(name, r.values[argIdx[2]])
			if err != nil {
				return statementScalarValue{}, err
			}
			if nF < 0 {
				return statementScalarValue{}, fmt.Errorf("%w: negative substring length", ErrScalarNumericRange)
			}
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: substrSlice(s, startF, hasLen, nF)}}, nil
	case "left":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: left requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: leftSlice(s, nF)}}, nil
	case "right":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: right requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: rightSlice(s, nF)}}, nil
	case "split_part":
		if argc != 3 {
			return statementScalarValue{}, fmt.Errorf("%w: split_part requires 3 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		delim, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		fieldF, err := requireNumberArg(name, r.values[argIdx[2]])
		if err != nil {
			return statementScalarValue{}, err
		}
		field := clampFloatToInt(fieldF)
		if field == 0 {
			return statementScalarValue{}, fmt.Errorf("%w: field position must not be zero", ErrScalarNumericRange)
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: splitPartSlice(s, delim, field)}}, nil
	case "strpos":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: strpos requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		sub, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		idx := strings.Index(s, sub)
		if idx < 0 {
			return statementScalarValue{value: arenaIntScalar(arena, 0)}, nil
		}
		pos := runeCountFast(s[:idx]) + 1
		return statementScalarValue{value: arenaIntScalar(arena, int64(pos))}, nil
	case "reverse":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: appendReverse(arena, s)}}, nil
	case "repeat":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: repeat requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		if nF <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		if len(s) == 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		if nF > float64((1<<20)/len(s)) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		n := int(nF)
		if n <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		start := len(*arena)
		for i := 0; i < n; i++ {
			*arena = append(*arena, s...)
		}
		return arenaStringValue(arena, start), nil
	case "lpad", "rpad":
		if argc < 2 || argc > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: %s takes 2 or 3 arguments", ErrScalarType, name)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		fill := " "
		if argc == 3 {
			fill, err = requireStringArg(name, r.values[argIdx[2]])
			if err != nil {
				return statementScalarValue{}, err
			}
		}
		if nF <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		if nF > float64(1<<20) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		n := int(nF)
		if n <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		out := appendPad(arena, s, n, fill, name == "lpad")
		return statementScalarValue{value: scalar{kind: kindString, sval: out}}, nil
	case "initcap":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: appendInitcap(arena, s)}}, nil
	case "md5":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		sum := md5.Sum(byteview.Bytes(s))
		var hexBuf [32]byte
		hex.Encode(hexBuf[:], sum[:])
		start := len(*arena)
		*arena = append(*arena, hexBuf[:]...)
		return arenaStringValue(arena, start), nil
	case "ascii":
		s, err := r.requireOneString(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		if s == "" {
			return statementScalarValue{value: arenaIntScalar(arena, 0)}, nil
		}
		ch, _ := utf8.DecodeRuneInString(s)
		return statementScalarValue{value: arenaIntScalar(arena, int64(ch))}, nil
	case "chr":
		nF, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		ch := utf8.RuneError
		if nF >= 0 && nF <= utf8.MaxRune {
			ch = rune(int(nF))
		}
		start := len(*arena)
		*arena = utf8.AppendRune(*arena, ch)
		return arenaStringValue(arena, start), nil
	case "starts_with":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: starts_with requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		pref, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindBool, bval: strings.HasPrefix(s, pref)}}, nil
	case "translate":
		if argc != 3 {
			return statementScalarValue{}, fmt.Errorf("%w: translate requires 3 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		from, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		to, err := requireStringArg(name, r.values[argIdx[2]])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: appendTranslate(arena, s, from, to)}}, nil
	case "abs":
		if argc != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		arg0 := r.values[argIdx[0]]
		if arg0.value.kind == kindNumber && arg0.value.isInt {
			if arg0.value.ival == math.MinInt64 {
				start := len(*arena)
				*arena = append(*arena, "9223372036854775808"...)
				return statementScalarValue{value: scalar{kind: kindNumber, num: (*arena)[start:len(*arena):len(*arena)]}}, nil
			}
			v := arg0.value.ival
			if v < 0 {
				v = -v
			}
			return statementScalarValue{value: arenaIntScalar(arena, v)}, nil
		}
		n, err := requireNumberArg(name, arg0)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Abs(n))}, nil
	case "ceil", "ceiling":
		n, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Ceil(n))}, nil
	case "floor":
		n, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Floor(n))}, nil
	case "round":
		if argc == 1 {
			n, err := requireNumberArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: arenaFloatScalar(arena, math.Round(n))}, nil
		}
		if argc == 2 {
			n, err := requireNumberArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			scale, err := requireNumberArg(name, r.values[argIdx[1]])
			if err != nil {
				return statementScalarValue{}, err
			}
			res, err := roundOrTruncScale(n, scale, true)
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: round takes 1 or 2 arguments", ErrScalarType)
	case "trunc":
		if argc == 1 {
			n, err := requireNumberArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: arenaFloatScalar(arena, math.Trunc(n))}, nil
		}
		if argc == 2 {
			n, err := requireNumberArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			scale, err := requireNumberArg(name, r.values[argIdx[1]])
			if err != nil {
				return statementScalarValue{}, err
			}
			res, err := roundOrTruncScale(n, scale, false)
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: trunc takes 1 or 2 arguments", ErrScalarType)
	case "power", "pow":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 2 arguments", ErrScalarType, name)
		}
		a, err := requireNumberArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		b, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		res := math.Pow(a, b)
		if math.IsNaN(res) || math.IsInf(res, 0) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
	case "sqrt":
		n, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		if n < 0 {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Sqrt(n))}, nil
	case "mod":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: mod requires 2 arguments", ErrScalarType)
		}
		a, err := requireNumberArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		b, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		if b == 0 {
			return statementScalarValue{}, ErrScalarDivisionByZero
		}
		res := math.Mod(a, b)
		if math.IsNaN(res) || math.IsInf(res, 0) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
	case "sign":
		n, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		var out int64
		if n > 0 {
			out = 1
		} else if n < 0 {
			out = -1
		}
		return statementScalarValue{value: arenaIntScalar(arena, out)}, nil
	case "ln":
		n, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		if n <= 0 {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		res := math.Log(n)
		if math.IsNaN(res) || math.IsInf(res, 0) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
	case "log", "log10":
		if argc == 1 {
			n, err := requireNumberArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			if n <= 0 {
				return statementScalarValue{}, ErrScalarNumericRange
			}
			res := math.Log10(n)
			if math.IsNaN(res) || math.IsInf(res, 0) {
				return statementScalarValue{}, ErrScalarNumericRange
			}
			return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
		}
		if argc == 2 && name == "log" {
			base, err := requireNumberArg(name, r.values[argIdx[0]])
			if err != nil {
				return statementScalarValue{}, err
			}
			val, err := requireNumberArg(name, r.values[argIdx[1]])
			if err != nil {
				return statementScalarValue{}, err
			}
			if base <= 0 || base == 1 || val <= 0 {
				return statementScalarValue{}, ErrScalarNumericRange
			}
			res := math.Log(val) / math.Log(base)
			if math.IsNaN(res) || math.IsInf(res, 0) {
				return statementScalarValue{}, ErrScalarNumericRange
			}
			return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s invalid argument count", ErrScalarType, name)
	case "exp":
		n, err := r.requireOneNumber(name, argIdx)
		if err != nil {
			return statementScalarValue{}, err
		}
		res := math.Exp(n)
		if math.IsNaN(res) || math.IsInf(res, 0) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
	case "date_trunc":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: date_trunc requires 2 arguments", ErrScalarType)
		}
		field, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		tsStr, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		tm, err := parseFlexibleTimestamp(tsStr)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %v", ErrScalarInvalidText, err)
		}
		tm = tm.UTC()
		var trunc time.Time
		switch {
		case strings.EqualFold(field, "year"):
			trunc = time.Date(tm.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		case strings.EqualFold(field, "month"):
			trunc = time.Date(tm.Year(), tm.Month(), 1, 0, 0, 0, 0, time.UTC)
		case strings.EqualFold(field, "day"):
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.UTC)
		case strings.EqualFold(field, "hour"):
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), 0, 0, 0, time.UTC)
		case strings.EqualFold(field, "minute"):
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), 0, 0, time.UTC)
		case strings.EqualFold(field, "second"):
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(), 0, time.UTC)
		default:
			return statementScalarValue{}, fmt.Errorf("%w: unsupported date_trunc field %q", ErrScalarInvalidText, field)
		}
		start := len(*arena)
		*arena = trunc.AppendFormat(*arena, time.RFC3339)
		return arenaStringValue(arena, start), nil
	case "extract":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: extract requires 2 arguments", ErrScalarType)
		}
		field, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		tsStr, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		tm, err := parseFlexibleTimestamp(tsStr)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %v", ErrScalarInvalidText, err)
		}
		tm = tm.UTC()
		var out float64
		switch {
		case strings.EqualFold(field, "year"):
			out = float64(tm.Year())
		case strings.EqualFold(field, "month"):
			out = float64(tm.Month())
		case strings.EqualFold(field, "day"):
			out = float64(tm.Day())
		case strings.EqualFold(field, "hour"):
			out = float64(tm.Hour())
		case strings.EqualFold(field, "minute"):
			out = float64(tm.Minute())
		case strings.EqualFold(field, "second"):
			out = float64(tm.Second())
		case strings.EqualFold(field, "dow"):
			out = float64(tm.Weekday())
		case strings.EqualFold(field, "doy"):
			out = float64(tm.YearDay())
		case strings.EqualFold(field, "epoch"):
			out = float64(tm.Unix()) + float64(tm.Nanosecond())/1e9
		default:
			return statementScalarValue{}, fmt.Errorf("%w: unsupported extract field %q", ErrScalarInvalidText, field)
		}
		return statementScalarValue{value: arenaFloatScalar(arena, out)}, nil
	case "json_array_length", "jsonb_array_length", "cardinality":
		if argc != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		raw := scalarJSONBytes(r.values[argIdx[0]], arena)
		var count int64
		if err := vibejson.EachArray(raw, func(_ int, _ vibejson.RawValue) error {
			count++
			return nil
		}); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %s expects a JSON array", ErrScalarType, name)
		}
		return statementScalarValue{value: arenaIntScalar(arena, count)}, nil
	case "array_length":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: array_length requires 2 arguments", ErrScalarType)
		}
		dim, err := requireNumberArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		if int(dim) != 1 {
			return nullScalarValue(), nil
		}
		raw := scalarJSONBytes(r.values[argIdx[0]], arena)
		var count int64
		if err := vibejson.EachArray(raw, func(_ int, _ vibejson.RawValue) error {
			count++
			return nil
		}); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: array_length expects a JSON array", ErrScalarType)
		}
		if count == 0 {
			return nullScalarValue(), nil
		}
		return statementScalarValue{value: arenaIntScalar(arena, count)}, nil
	case "array_to_string":
		if argc < 2 || argc > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: array_to_string takes 2 or 3 arguments", ErrScalarType)
		}
		delim, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		hasNullStr := argc == 3
		nullStr := ""
		if hasNullStr {
			nullStr, err = requireStringArg(name, r.values[argIdx[2]])
			if err != nil {
				return statementScalarValue{}, err
			}
		}
		raw := scalarJSONBytes(r.values[argIdx[0]], arena)
		start := len(*arena)
		wrote := false
		if err := vibejson.EachArray(raw, func(_ int, elem vibejson.RawValue) error {
			if elem.IsNull() {
				if !hasNullStr {
					return nil
				}
				if wrote {
					*arena = append(*arena, delim...)
				}
				*arena = append(*arena, nullStr...)
				wrote = true
				return nil
			}
			if wrote {
				*arena = append(*arena, delim...)
			}
			if elem.Kind() == document.String {
				var ok bool
				var decErr error
				*arena, ok, decErr = elem.AppendText(*arena)
				if !ok || decErr != nil {
					*arena = append(*arena, elem.Bytes()...)
				}
			} else {
				*arena = append(*arena, elem.Bytes()...)
			}
			wrote = true
			return nil
		}); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: array_to_string expects an array", ErrScalarType)
		}
		return arenaStringValue(arena, start), nil
	case "string_to_array":
		if argc != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: string_to_array requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		delim, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		start := len(*arena)
		*arena = append(*arena, '[')
		if delim == "" {
			first := true
			for i := 0; i < len(s); {
				_, size := utf8.DecodeRuneInString(s[i:])
				if !first {
					*arena = append(*arena, ',')
				}
				*arena = appendJSONString(*arena, s[i:i+size])
				first = false
				i += size
			}
		} else {
			rem := s
			first := true
			for {
				if !first {
					*arena = append(*arena, ',')
				}
				first = false
				idx := strings.Index(rem, delim)
				if idx < 0 {
					*arena = appendJSONString(*arena, rem)
					break
				}
				*arena = appendJSONString(*arena, rem[:idx])
				rem = rem[idx+len(delim):]
			}
		}
		*arena = append(*arena, ']')
		return arenaJSONValue(arena, start), nil
	case "json_extract_path", "jsonb_extract_path", "json_extract_path_text", "jsonb_extract_path_text":
		if argc < 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires at least 1 argument", ErrScalarType, name)
		}
		raw := trimASCIIBytes(scalarJSONBytes(r.values[argIdx[0]], arena))
		if !vibejson.Valid(raw) {
			return nullScalarValue(), nil
		}
		cur := vibejson.RawValue{Src: raw}
		for _, pathIdx := range argIdx[1:] {
			key, err := requireStringArg(name, r.values[pathIdx])
			if err != nil {
				return statementScalarValue{}, err
			}
			switch cur.Kind() {
			case document.Object:
				var next vibejson.RawValue
				found := false
				if err := vibejson.EachObject(cur.Bytes(), func(k string, v vibejson.RawValue) error {
					if k == key {
						next = v
						found = true
					}
					return nil
				}); err != nil || !found {
					return nullScalarValue(), nil
				}
				cur = next
			case document.Array:
				idx, convErr := strconv.Atoi(key)
				if convErr != nil || idx < 0 {
					return nullScalarValue(), nil
				}
				var next vibejson.RawValue
				found := false
				if err := vibejson.EachArray(cur.Bytes(), func(i int, v vibejson.RawValue) error {
					if i == idx {
						next = v
						found = true
					}
					return nil
				}); err != nil || !found {
					return nullScalarValue(), nil
				}
				cur = next
			default:
				return nullScalarValue(), nil
			}
		}
		if cur.IsNull() {
			return nullScalarValue(), nil
		}
		if strings.HasSuffix(name, "_text") {
			if cur.Kind() == document.String {
				if content, clean := cur.StringBytes(); clean {
					return statementScalarValue{value: scalar{kind: kindString, sval: byteview.String(content)}}, nil
				}
				start := len(*arena)
				var ok bool
				var err error
				*arena, ok, err = cur.AppendText(*arena)
				if !ok || err != nil {
					*arena = append(*arena, cur.Bytes()...)
				}
				return arenaStringValue(arena, start), nil
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: byteview.String(cur.Bytes())}}, nil
		}
		start := len(*arena)
		*arena = append(*arena, cur.Bytes()...)
		return arenaJSONValue(arena, start), nil
	case "json_strip_nulls", "jsonb_strip_nulls":
		if argc != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		raw := trimASCIIBytes(scalarJSONBytes(r.values[argIdx[0]], arena))
		if err := vibejson.Validate(raw); err != nil {
			return statementScalarValue{}, err
		}
		start := len(*arena)
		var err error
		*arena, err = appendStripJSONNulls(*arena, vibejson.RawValue{Src: raw})
		if err != nil {
			return statementScalarValue{}, err
		}
		return arenaJSONValue(arena, start), nil
	case "jsonb_pretty":
		if argc != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: jsonb_pretty requires 1 argument", ErrScalarType)
		}
		raw := scalarJSONBytes(r.values[argIdx[0]], arena)
		start := len(*arena)
		var err error
		*arena, err = vibejson.AppendIndent(*arena, raw, "", "    ")
		if err != nil {
			return statementScalarValue{}, err
		}
		return arenaStringValue(arena, start), nil
	case "jsonb_set", "json_set":
		if argc < 3 || argc > 4 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 3 or 4 arguments", ErrScalarType, name)
		}
		targetRaw, err := scalarJSONSetTarget(r.values[argIdx[0]], arena)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %s target is not valid JSON", ErrScalarType, name)
		}
		pathStr, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		var pathBuf [16]string
		segments := parsePGJSONPathInto(pathStr, pathBuf[:0], arena)
		newValRaw := scalarJSONSetValue(r.values[argIdx[2]], arena)
		createMissing := true
		if argc == 4 && r.values[argIdx[3]].value.kind == kindBool {
			createMissing = r.values[argIdx[3]].value.bval
		}
		start := len(*arena)
		*arena, err = appendJSONSetPath(*arena, vibejson.RawValue{Src: targetRaw}, segments, newValRaw, createMissing)
		if err != nil {
			return statementScalarValue{}, err
		}
		return arenaJSONValue(arena, start), nil
	case "regexp_like", "regexp_count", "regexp_substr":
		if argc < 2 || argc > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: %s takes 2 or 3 arguments", ErrScalarType, name)
		}
		src, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		pat, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		flags := ""
		if argc == 3 {
			flags, err = requireStringArg(name, r.values[argIdx[2]])
			if err != nil {
				return statementScalarValue{}, err
			}
		}
		re, err := compilePGRegexp(pat, flags)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %v", ErrInvalidPattern, err)
		}
		switch name {
		case "regexp_like":
			return statementScalarValue{value: scalar{kind: kindBool, bval: re.MatchString(src)}}, nil
		case "regexp_count":
			return statementScalarValue{value: arenaIntScalar(arena, int64(len(re.FindAllStringIndex(src, -1))))}, nil
		default:
			loc := re.FindString(src)
			if loc == "" && !re.MatchString(src) {
				return nullScalarValue(), nil
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: loc}}, nil
		}
	case "regexp_replace":
		if argc < 3 || argc > 4 {
			return statementScalarValue{}, fmt.Errorf("%w: regexp_replace takes 3 or 4 arguments", ErrScalarType)
		}
		src, err := requireStringArg(name, r.values[argIdx[0]])
		if err != nil {
			return statementScalarValue{}, err
		}
		pat, err := requireStringArg(name, r.values[argIdx[1]])
		if err != nil {
			return statementScalarValue{}, err
		}
		repl, err := requireStringArg(name, r.values[argIdx[2]])
		if err != nil {
			return statementScalarValue{}, err
		}
		flags := ""
		if argc == 4 {
			flags, err = requireStringArg(name, r.values[argIdx[3]])
			if err != nil {
				return statementScalarValue{}, err
			}
		}
		re, err := compilePGRegexp(pat, flags)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %v", ErrInvalidPattern, err)
		}
		if strings.ContainsRune(flags, 'g') {
			return statementScalarValue{value: scalar{kind: kindString, sval: re.ReplaceAllString(src, repl)}}, nil
		}
		loc := re.FindStringSubmatchIndex(src)
		if loc == nil {
			return statementScalarValue{value: scalar{kind: kindString, sval: src}}, nil
		}
		start := len(*arena)
		*arena = append(*arena, src[:loc[0]]...)
		*arena = re.ExpandString(*arena, repl, src, loc)
		*arena = append(*arena, src[loc[1]:]...)
		return arenaStringValue(arena, start), nil
	default:
		return statementScalarValue{}, fmt.Errorf("%w: unknown scalar function %q", ErrScalarType, name)
	}
}

func nullScalarValue() statementScalarValue {
	return statementScalarValue{value: scalar{kind: kindNull}}
}

func arenaStringValue(arena *[]byte, start int) statementScalarValue {
	return statementScalarValue{
		value: scalar{kind: kindString, sval: byteview.String((*arena)[start:len(*arena):len(*arena)])},
	}
}

func arenaJSONValue(arena *[]byte, start int) statementScalarValue {
	end := len(*arena)
	raw := (*arena)[start:end:end]
	val := classifyRawInto(vibejson.RawValue{Src: raw}, arena)
	return statementScalarValue{value: val, cell: cellFromScalar(val), exact: true}
}

func appendUUID(dst []byte, u [16]byte) []byte {
	const hexDigits = "0123456789abcdef"
	var buf [36]byte
	pos := 0
	for i, b := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			buf[pos] = '-'
			pos++
		}
		buf[pos] = hexDigits[b>>4]
		buf[pos+1] = hexDigits[b&0x0f]
		pos += 2
	}
	return append(dst, buf[:]...)
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func runeCountFast(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return i + utf8.RuneCountInString(s[i:])
		}
	}
	return len(s)
}

func appendLower(arena *[]byte, s string) string {
	needsChange := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			start := len(*arena)
			*arena = append(*arena, s[:i]...)
			if needsChange {
				dst := (*arena)[start : start+i]
				for j := range dst {
					if dst[j] >= 'A' && dst[j] <= 'Z' {
						dst[j] += 'a' - 'A'
					}
				}
			}
			for i < len(s) {
				b := s[i]
				if b < utf8.RuneSelf {
					if b >= 'A' && b <= 'Z' {
						b += 'a' - 'A'
					}
					*arena = append(*arena, b)
					i++
					continue
				}
				ch, size := utf8.DecodeRuneInString(s[i:])
				*arena = utf8.AppendRune(*arena, unicode.ToLower(ch))
				i += size
			}
			return byteview.String((*arena)[start:len(*arena):len(*arena)])
		}
		if c >= 'A' && c <= 'Z' {
			needsChange = true
		}
	}
	if !needsChange {
		return s
	}
	start := len(*arena)
	*arena = append(*arena, s...)
	dst := (*arena)[start:len(*arena)]
	for i := range dst {
		if dst[i] >= 'A' && dst[i] <= 'Z' {
			dst[i] += 'a' - 'A'
		}
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func appendUpper(arena *[]byte, s string) string {
	needsChange := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			start := len(*arena)
			*arena = append(*arena, s[:i]...)
			if needsChange {
				dst := (*arena)[start : start+i]
				for j := range dst {
					if dst[j] >= 'a' && dst[j] <= 'z' {
						dst[j] -= 'a' - 'A'
					}
				}
			}
			for i < len(s) {
				b := s[i]
				if b < utf8.RuneSelf {
					if b >= 'a' && b <= 'z' {
						b -= 'a' - 'A'
					}
					*arena = append(*arena, b)
					i++
					continue
				}
				ch, size := utf8.DecodeRuneInString(s[i:])
				*arena = utf8.AppendRune(*arena, unicode.ToUpper(ch))
				i += size
			}
			return byteview.String((*arena)[start:len(*arena):len(*arena)])
		}
		if c >= 'a' && c <= 'z' {
			needsChange = true
		}
	}
	if !needsChange {
		return s
	}
	start := len(*arena)
	*arena = append(*arena, s...)
	dst := (*arena)[start:len(*arena)]
	for i := range dst {
		if dst[i] >= 'a' && dst[i] <= 'z' {
			dst[i] -= 'a' - 'A'
		}
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func isASCIISpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

func trimLeftSpaceFast(s string) string {
	i := 0
	for i < len(s) {
		c := s[i]
		if c >= utf8.RuneSelf {
			return strings.TrimLeftFunc(s[i:], unicode.IsSpace)
		}
		if !isASCIISpace(c) {
			return s[i:]
		}
		i++
	}
	return ""
}

func trimRightSpaceFast(s string) string {
	i := len(s)
	for i > 0 {
		c := s[i-1]
		if c >= utf8.RuneSelf {
			return strings.TrimRightFunc(s[:i], unicode.IsSpace)
		}
		if !isASCIISpace(c) {
			return s[:i]
		}
		i--
	}
	return ""
}

func appendReplaceAll(arena *[]byte, s, oldS, newS string) string {
	if oldS == newS {
		return s
	}
	if oldS == "" {
		start := len(*arena)
		*arena = append(*arena, newS...)
		for i := 0; i < len(s); {
			_, size := utf8.DecodeRuneInString(s[i:])
			*arena = append(*arena, s[i:i+size]...)
			*arena = append(*arena, newS...)
			i += size
		}
		return byteview.String((*arena)[start:len(*arena):len(*arena)])
	}
	idx := strings.Index(s, oldS)
	if idx < 0 {
		return s
	}
	start := len(*arena)
	pos := 0
	for {
		*arena = append(*arena, s[pos:pos+idx]...)
		*arena = append(*arena, newS...)
		pos += idx + len(oldS)
		idx = strings.Index(s[pos:], oldS)
		if idx < 0 {
			*arena = append(*arena, s[pos:]...)
			break
		}
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func clampFloatToInt(f float64) int {
	if f >= float64(math.MaxInt32) {
		return math.MaxInt32
	}
	if f <= float64(math.MinInt32) {
		return math.MinInt32
	}
	return int(f)
}

func substrSlice(s string, startF float64, hasLen bool, nF float64) string {
	if s == "" {
		return ""
	}
	start := 0
	if startF > 1 {
		if startF > float64(len(s))+1 {
			return ""
		}
		start = int(startF) - 1
	}
	maxLen := len(s)
	limit := maxLen
	if hasLen {
		if nF <= 0 {
			return ""
		}
		if nF < float64(maxLen) {
			limit = int(nF)
		}
	}
	if isASCII(s) {
		if start >= len(s) {
			return ""
		}
		end := len(s)
		if hasLen && start+limit < end {
			end = start + limit
		}
		return s[start:end]
	}
	runeIdx := 0
	byteStart := -1
	byteEnd := len(s)
	targetEnd := -1
	if hasLen {
		targetEnd = start + limit
	}
	for i := 0; i < len(s); {
		if runeIdx == start {
			byteStart = i
		}
		if targetEnd >= 0 && runeIdx == targetEnd {
			byteEnd = i
			break
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
		runeIdx++
	}
	if byteStart < 0 {
		return ""
	}
	return s[byteStart:byteEnd]
}

func leftSlice(s string, nF float64) string {
	if s == "" {
		return ""
	}
	if isASCII(s) {
		n := clampFloatToInt(nF)
		if n < 0 {
			n = len(s) + n
		}
		if n <= 0 {
			return ""
		}
		if n >= len(s) {
			return s
		}
		return s[:n]
	}
	n := clampFloatToInt(nF)
	if n < 0 {
		n = utf8.RuneCountInString(s) + n
	}
	if n <= 0 {
		return ""
	}
	count := 0
	for i := 0; i < len(s); {
		if count == n {
			return s[:i]
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
		count++
	}
	return s
}

func rightSlice(s string, nF float64) string {
	if s == "" {
		return ""
	}
	if isASCII(s) {
		n := clampFloatToInt(nF)
		if n < 0 {
			n = len(s) + n
		}
		if n <= 0 {
			return ""
		}
		if n >= len(s) {
			return s
		}
		return s[len(s)-n:]
	}
	n := clampFloatToInt(nF)
	if n < 0 {
		n = utf8.RuneCountInString(s) + n
	}
	if n <= 0 {
		return ""
	}
	count := 0
	for i := len(s); i > 0; {
		if count == n {
			return s[i:]
		}
		_, size := utf8.DecodeLastRuneInString(s[:i])
		i -= size
		count++
	}
	return s
}

func splitPartSlice(s, delim string, field int) string {
	if delim == "" {
		total := runeCountFast(s)
		if field < 0 {
			field = total + field + 1
		}
		if field < 1 || field > total {
			return ""
		}
		if isASCII(s) {
			return s[field-1 : field]
		}
		idx := 1
		for i := 0; i < len(s); {
			_, size := utf8.DecodeRuneInString(s[i:])
			if idx == field {
				return s[i : i+size]
			}
			i += size
			idx++
		}
		return ""
	}
	if field < 0 {
		total := strings.Count(s, delim) + 1
		field = total + field + 1
		if field < 1 || field > total {
			return ""
		}
	}
	part := 1
	rem := s
	for {
		idx := strings.Index(rem, delim)
		if idx < 0 {
			if part == field {
				return rem
			}
			return ""
		}
		if part == field {
			return rem[:idx]
		}
		rem = rem[idx+len(delim):]
		part++
	}
}

func appendReverse(arena *[]byte, s string) string {
	if len(s) <= 1 {
		return s
	}
	start := len(*arena)
	if isASCII(s) {
		*arena = append(*arena, s...)
		dst := (*arena)[start:len(*arena)]
		for i, j := 0, len(dst)-1; i < j; i, j = i+1, j-1 {
			dst[i], dst[j] = dst[j], dst[i]
		}
		return byteview.String((*arena)[start:len(*arena):len(*arena)])
	}
	for i := len(s); i > 0; {
		_, size := utf8.DecodeLastRuneInString(s[:i])
		*arena = append(*arena, s[i-size:i]...)
		i -= size
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func appendPad(arena *[]byte, s string, n int, fill string, leftPad bool) string {
	runeLen := runeCountFast(s)
	if runeLen >= n {
		return leftSlice(s, float64(n))
	}
	if fill == "" {
		return s
	}
	need := n - runeLen
	start := len(*arena)
	if !leftPad {
		*arena = append(*arena, s...)
	}
	if isASCII(fill) {
		for need > 0 {
			chunk := len(fill)
			if chunk > need {
				chunk = need
			}
			*arena = append(*arena, fill[:chunk]...)
			need -= chunk
		}
	} else {
		fillRunes := runeCountFast(fill)
		full := need / fillRunes
		rem := need % fillRunes
		for i := 0; i < full; i++ {
			*arena = append(*arena, fill...)
		}
		if rem > 0 {
			*arena = append(*arena, leftSlice(fill, float64(rem))...)
		}
	}
	if leftPad {
		*arena = append(*arena, s...)
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func appendInitcap(arena *[]byte, s string) string {
	if s == "" {
		return ""
	}
	start := len(*arena)
	newWord := true
	if isASCII(s) {
		*arena = append(*arena, s...)
		dst := (*arena)[start:len(*arena)]
		for i, c := range dst {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
				if newWord {
					if c >= 'a' && c <= 'z' {
						dst[i] = c - ('a' - 'A')
					}
					newWord = false
				} else if c >= 'A' && c <= 'Z' {
					dst[i] = c + ('a' - 'A')
				}
			} else {
				newWord = true
			}
		}
		return byteview.String((*arena)[start:len(*arena):len(*arena)])
	}
	for _, ch := range s {
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) {
			if newWord {
				*arena = utf8.AppendRune(*arena, unicode.ToUpper(ch))
				newWord = false
			} else {
				*arena = utf8.AppendRune(*arena, unicode.ToLower(ch))
			}
		} else {
			*arena = utf8.AppendRune(*arena, ch)
			newWord = true
		}
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func appendTranslate(arena *[]byte, s, from, to string) string {
	if s == "" || from == "" {
		return s
	}
	start := len(*arena)
	if isASCII(s) && isASCII(from) && isASCII(to) {
		var tbl [128]int16
		for i := range tbl {
			tbl[i] = -1
		}
		for i := 0; i < len(from); i++ {
			ch := from[i]
			if tbl[ch] != -1 {
				continue
			}
			if i < len(to) {
				tbl[ch] = int16(to[i])
			} else {
				tbl[ch] = -2
			}
		}
		for i := 0; i < len(s); i++ {
			mapped := tbl[s[i]]
			if mapped == -2 {
				continue
			}
			if mapped >= 0 {
				*arena = append(*arena, byte(mapped))
			} else {
				*arena = append(*arena, s[i])
			}
		}
		return byteview.String((*arena)[start:len(*arena):len(*arena)])
	}
	for _, ch := range s {
		matchIdx := -1
		fi := 0
		for _, fc := range from {
			if fc == ch {
				matchIdx = fi
				break
			}
			fi++
		}
		if matchIdx < 0 {
			*arena = utf8.AppendRune(*arena, ch)
			continue
		}
		ti := 0
		found := false
		for _, tc := range to {
			if ti == matchIdx {
				*arena = utf8.AppendRune(*arena, tc)
				found = true
				break
			}
			ti++
		}
		if !found {
			// Deleted rune (matchIdx >= len(to runes)).
			continue
		}
	}
	return byteview.String((*arena)[start:len(*arena):len(*arena)])
}

func roundOrTruncScale(n, scale float64, doRound bool) (float64, error) {
	sc := math.Trunc(scale)
	if sc == 0 {
		if doRound {
			return math.Round(n), nil
		}
		return math.Trunc(n), nil
	}
	if sc < 0 {
		pow := math.Pow(10, -sc)
		if math.IsInf(pow, 0) {
			// Huge negative scale: rounds/truncates to 0.
			return 0, nil
		}
		scaled := n / pow
		var res float64
		if doRound {
			res = math.Round(scaled) * pow
		} else {
			res = math.Trunc(scaled) * pow
		}
		if math.IsNaN(res) || math.IsInf(res, 0) {
			return 0, ErrScalarNumericRange
		}
		return res, nil
	}
	pow := math.Pow(10, sc)
	if math.IsInf(pow, 0) {
		// Huge positive scale: float64 already has fewer fractional digits.
		return n, nil
	}
	scaled := n * pow
	if math.IsInf(scaled, 0) {
		return n, nil
	}
	var res float64
	if doRound {
		res = math.Round(scaled) / pow
	} else {
		res = math.Trunc(scaled) / pow
	}
	if math.IsNaN(res) || math.IsInf(res, 0) {
		return 0, ErrScalarNumericRange
	}
	return res, nil
}

func compilePGRegexp(pat, flags string) (*regexp.Regexp, error) {
	return cachedPGRegexp(pat, flags)
}

var arithTimestampLayouts = [...]string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
}

var flexibleTimestampLayouts = [...]string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func evalTimestampIntervalArith(arena *[]byte, tsStr, intervalStr string, subtract bool) (scalar, bool) {
	ts, hasTime, ok := parseTimestampForArith(tsStr)
	if !ok {
		return scalar{}, false
	}
	years, months, days, dur, ok := parsePGInterval(intervalStr)
	if !ok {
		return scalar{}, false
	}
	if subtract {
		years, months, days, dur = -years, -months, -days, -dur
	}
	res := ts.AddDate(years, months, days).Add(dur)
	start := len(*arena)
	if !hasTime && dur == 0 {
		*arena = res.AppendFormat(*arena, "2006-01-02")
	} else {
		*arena = res.UTC().AppendFormat(*arena, time.RFC3339Nano)
	}
	return scalar{kind: kindString, sval: byteview.String((*arena)[start:len(*arena):len(*arena)])}, true
}

func parseTimestampForArith(s string) (time.Time, bool, bool) {
	s = strings.TrimSpace(s)
	for i := range arithTimestampLayouts {
		if t, err := time.Parse(arithTimestampLayouts[i], s); err == nil {
			return t, true, true
		}
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, false, true
	}
	return time.Time{}, false, false
}

func nextSpaceField(s string) (field, rest string, ok bool) {
	i := 0
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i == len(s) {
		return "", "", false
	}
	start := i
	for i < len(s) && !isASCIISpace(s[i]) {
		i++
	}
	return s[start:i], s[i:], true
}

func parsePGInterval(s string) (years, months, days int, dur time.Duration, ok bool) {
	rem := s
	count := 0
	for {
		valTok, r1, ok1 := nextSpaceField(rem)
		if !ok1 {
			break
		}
		unitTok, r2, ok2 := nextSpaceField(r1)
		if !ok2 {
			return 0, 0, 0, 0, false
		}
		rem = r2
		count++
		val, err := strconv.ParseFloat(valTok, 64)
		if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, 0, 0, 0, false
		}
		unit := strings.TrimSuffix(unitTok, ",")
		switch {
		case strings.EqualFold(unit, "year"), strings.EqualFold(unit, "years"),
			strings.EqualFold(unit, "yr"), strings.EqualFold(unit, "yrs"), strings.EqualFold(unit, "y"):
			years += int(val)
		case strings.EqualFold(unit, "month"), strings.EqualFold(unit, "months"),
			strings.EqualFold(unit, "mon"), strings.EqualFold(unit, "mons"):
			months += int(val)
		case strings.EqualFold(unit, "week"), strings.EqualFold(unit, "weeks"), strings.EqualFold(unit, "w"):
			days += int(val) * 7
		case strings.EqualFold(unit, "day"), strings.EqualFold(unit, "days"), strings.EqualFold(unit, "d"):
			days += int(val)
		case strings.EqualFold(unit, "hour"), strings.EqualFold(unit, "hours"),
			strings.EqualFold(unit, "hr"), strings.EqualFold(unit, "hrs"), strings.EqualFold(unit, "h"):
			dur += time.Duration(val * float64(time.Hour))
		case strings.EqualFold(unit, "minute"), strings.EqualFold(unit, "minutes"),
			strings.EqualFold(unit, "min"), strings.EqualFold(unit, "mins"), strings.EqualFold(unit, "m"):
			dur += time.Duration(val * float64(time.Minute))
		case strings.EqualFold(unit, "second"), strings.EqualFold(unit, "seconds"),
			strings.EqualFold(unit, "sec"), strings.EqualFold(unit, "secs"), strings.EqualFold(unit, "s"):
			dur += time.Duration(val * float64(time.Second))
		case strings.EqualFold(unit, "millisecond"), strings.EqualFold(unit, "milliseconds"), strings.EqualFold(unit, "ms"):
			dur += time.Duration(val * float64(time.Millisecond))
		case strings.EqualFold(unit, "microsecond"), strings.EqualFold(unit, "microseconds"), strings.EqualFold(unit, "us"):
			dur += time.Duration(val * float64(time.Microsecond))
		default:
			return 0, 0, 0, 0, false
		}
	}
	if count == 0 {
		return 0, 0, 0, 0, false
	}
	return years, months, days, dur, true
}

func parsePGJSONPathInto(s string, dst []string, arena *[]byte) []string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return dst
		}
		rem := inner
		for {
			idx := strings.IndexByte(rem, ',')
			var part string
			if idx < 0 {
				part = rem
			} else {
				part = rem[:idx]
			}
			part = strings.Trim(strings.TrimSpace(part), `"`)
			dst = append(dst, part)
			if idx < 0 {
				break
			}
			rem = rem[idx+1:]
		}
		return dst
	}
	if len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']' {
		baseLen := len(dst)
		err := vibejson.EachArray(byteview.Bytes(s), func(_ int, elem vibejson.RawValue) error {
			if elem.Kind() != document.String {
				return ErrScalarType
			}
			if content, clean := elem.StringBytes(); clean {
				dst = append(dst, byteview.String(content))
				return nil
			}
			start := len(*arena)
			var ok bool
			var decErr error
			*arena, ok, decErr = elem.AppendText(*arena)
			if !ok || decErr != nil {
				return ErrScalarType
			}
			dst = append(dst, byteview.String((*arena)[start:len(*arena):len(*arena)]))
			return nil
		})
		if err == nil {
			return dst
		}
		dst = dst[:baseLen]
	}
	if s == "" {
		return dst
	}
	return append(dst, s)
}

func trimASCIIBytes(b []byte) []byte {
	start := 0
	for start < len(b) && isASCIISpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isASCIISpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func scalarJSONSetTarget(v statementScalarValue, arena *[]byte) ([]byte, error) {
	if !v.direct && !v.exact && v.value.kind == kindString {
		trimmed := strings.TrimSpace(v.value.sval)
		if len(trimmed) > 0 && vibejson.Valid(byteview.Bytes(trimmed)) {
			return byteview.Bytes(trimmed), nil
		}
		start := len(*arena)
		*arena = appendJSONString(*arena, v.value.sval)
		return (*arena)[start:len(*arena):len(*arena)], nil
	}
	raw := trimASCIIBytes(scalarJSONBytes(v, arena))
	if err := vibejson.Validate(raw); err != nil {
		return nil, err
	}
	rv := vibejson.RawValue{Src: raw}
	if rv.Kind() == document.String {
		if content, clean := rv.StringBytes(); clean {
			trimmed := trimASCIIBytes(content)
			if len(trimmed) > 0 && vibejson.Valid(trimmed) {
				return trimmed, nil
			}
		} else {
			start := len(*arena)
			if decoded, ok, err := rv.AppendText(*arena); ok && err == nil {
				*arena = decoded
				cand := trimASCIIBytes((*arena)[start:len(*arena):len(*arena)])
				if len(cand) > 0 && vibejson.Valid(cand) {
					return cand, nil
				}
			}
		}
	}
	return raw, nil
}

func scalarJSONSetValue(v statementScalarValue, arena *[]byte) []byte {
	if v.direct || v.exact {
		raw := scalarJSONBytes(v, arena)
		if vibejson.Valid(raw) {
			return raw
		}
		start := len(*arena)
		*arena = appendJSONString(*arena, byteview.String(raw))
		return (*arena)[start:len(*arena):len(*arena)]
	}
	if v.value.kind == kindString {
		if vibejson.Valid(byteview.Bytes(v.value.sval)) {
			return byteview.Bytes(v.value.sval)
		}
		start := len(*arena)
		*arena = appendJSONString(*arena, v.value.sval)
		return (*arena)[start:len(*arena):len(*arena)]
	}
	return scalarJSONBytes(v, arena)
}

func appendJSONSetPath(dst []byte, target vibejson.RawValue, path []string, newVal []byte, createMissing bool) ([]byte, error) {
	if len(path) == 0 {
		return append(dst, newVal...), nil
	}
	key := path[0]
	switch target.Kind() {
	case document.Object:
		dst = append(dst, '{')
		first := true
		found := false
		err := vibejson.EachObject(target.Bytes(), func(k string, v vibejson.RawValue) error {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, k)
			dst = append(dst, ':')
			if k == key {
				found = true
				if len(path) == 1 {
					dst = append(dst, newVal...)
					return nil
				}
				var subErr error
				dst, subErr = appendJSONSetPath(dst, v, path[1:], newVal, createMissing)
				return subErr
			}
			dst = append(dst, v.Bytes()...)
			return nil
		})
		if err != nil {
			return dst, err
		}
		if !found && createMissing && len(path) == 1 {
			if !first {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, key)
			dst = append(dst, ':')
			dst = append(dst, newVal...)
		}
		dst = append(dst, '}')
		return dst, nil
	case document.Array:
		idx, convErr := strconv.Atoi(key)
		if convErr != nil {
			return append(dst, target.Bytes()...), nil
		}
		arrLen := 0
		if err := vibejson.EachArray(target.Bytes(), func(_ int, _ vibejson.RawValue) error {
			arrLen++
			return nil
		}); err != nil {
			return dst, err
		}
		if idx < 0 {
			idx = arrLen + idx
		}
		if idx < 0 || idx >= arrLen {
			if !createMissing || len(path) != 1 {
				return append(dst, target.Bytes()...), nil
			}
			dst = append(dst, '[')
			first := true
			if idx < 0 {
				dst = append(dst, newVal...)
				first = false
			}
			if err := vibejson.EachArray(target.Bytes(), func(_ int, v vibejson.RawValue) error {
				if !first {
					dst = append(dst, ',')
				}
				first = false
				dst = append(dst, v.Bytes()...)
				return nil
			}); err != nil {
				return dst, err
			}
			if idx >= arrLen {
				if !first {
					dst = append(dst, ',')
				}
				dst = append(dst, newVal...)
			}
			dst = append(dst, ']')
			return dst, nil
		}
		dst = append(dst, '[')
		err := vibejson.EachArray(target.Bytes(), func(i int, v vibejson.RawValue) error {
			if i > 0 {
				dst = append(dst, ',')
			}
			if i == idx {
				if len(path) == 1 {
					dst = append(dst, newVal...)
					return nil
				}
				var subErr error
				dst, subErr = appendJSONSetPath(dst, v, path[1:], newVal, createMissing)
				return subErr
			}
			dst = append(dst, v.Bytes()...)
			return nil
		})
		if err != nil {
			return dst, err
		}
		dst = append(dst, ']')
		return dst, nil
	default:
		return append(dst, target.Bytes()...), nil
	}
}

func appendStripJSONNulls(dst []byte, raw vibejson.RawValue) ([]byte, error) {
	switch raw.Kind() {
	case document.Object:
		dst = append(dst, '{')
		first := true
		err := vibejson.EachObject(raw.Bytes(), func(k string, v vibejson.RawValue) error {
			if v.IsNull() {
				return nil
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, k)
			dst = append(dst, ':')
			var subErr error
			dst, subErr = appendStripJSONNulls(dst, v)
			return subErr
		})
		if err != nil {
			return dst, err
		}
		dst = append(dst, '}')
		return dst, nil
	case document.Array:
		dst = append(dst, '[')
		err := vibejson.EachArray(raw.Bytes(), func(i int, v vibejson.RawValue) error {
			if i > 0 {
				dst = append(dst, ',')
			}
			var subErr error
			dst, subErr = appendStripJSONNulls(dst, v)
			return subErr
		})
		if err != nil {
			return dst, err
		}
		dst = append(dst, ']')
		return dst, nil
	default:
		return append(dst, raw.Bytes()...), nil
	}
}

func (v statementScalarValue) isNull() bool {
	if v.direct {
		return v.cell.IsNull()
	}
	return v.value.kind == kindNull && !v.exact
}

func arenaIntScalar(arena *[]byte, n int64) scalar {
	start := len(*arena)
	*arena = strconv.AppendInt(*arena, n, 10)
	return scalar{kind: kindNumber, num: (*arena)[start:len(*arena):len(*arena)], isInt: true, ival: n}
}

func arenaFloatScalar(arena *[]byte, f float64) scalar {
	if f == 0 {
		f = 0 // normalize -0 to +0
	}
	if math.Trunc(f) == f && f >= math.MinInt64 && f <= 9223372036854774784 {
		return arenaIntScalar(arena, int64(f))
	}
	start := len(*arena)
	*arena = strconv.AppendFloat(*arena, f, 'f', -1, 64)
	return scalar{kind: kindNumber, num: (*arena)[start:len(*arena):len(*arena)]}
}

func (r *statementScalar) requireOneString(name string, argIdx []int32) (string, error) {
	if len(argIdx) != 1 {
		return "", fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
	}
	return requireStringArg(name, r.values[argIdx[0]])
}

func requireStringArg(name string, v statementScalarValue) (string, error) {
	if v.value.kind == kindString {
		return v.value.sval, nil
	}
	return "", fmt.Errorf("%w: %s requires a string argument", ErrScalarType, name)
}

func (r *statementScalar) requireOneNumber(name string, argIdx []int32) (float64, error) {
	if len(argIdx) != 1 {
		return 0, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
	}
	return requireNumberArg(name, r.values[argIdx[0]])
}

func requireNumberArg(name string, v statementScalarValue) (float64, error) {
	if v.value.kind == kindNumber {
		if v.value.isInt {
			return float64(v.value.ival), nil
		}
		f, err := strconv.ParseFloat(byteview.String(v.value.num), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, ErrScalarNumericRange
		}
		return f, nil
	}
	if v.exact && v.cell.kind == TypeNumber {
		var buf [64]byte
		raw := v.cell.AppendJSON(buf[:0])
		f, err := strconv.ParseFloat(byteview.String(raw), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, ErrScalarNumericRange
		}
		return f, nil
	}
	return 0, fmt.Errorf("%w: %s requires a numeric argument", ErrScalarType, name)
}

func appendScalarDisplay(dst []byte, v statementScalarValue) []byte {
	switch v.value.kind {
	case kindString:
		return append(dst, v.value.sval...)
	case kindBool:
		if v.value.bval {
			return append(dst, trueBytes...)
		}
		return append(dst, falseBytes...)
	case kindNumber:
		return append(dst, v.value.num...)
	default:
		if v.direct || v.exact {
			return v.cell.AppendJSON(dst)
		}
		return dst
	}
}

func appendScalarJSON(dst []byte, v statementScalarValue) []byte {
	if v.isNull() {
		return append(dst, nullBytes...)
	}
	if v.direct || v.exact {
		return v.cell.AppendJSON(dst)
	}
	switch v.value.kind {
	case kindString:
		return appendJSONString(dst, v.value.sval)
	case kindBool:
		if v.value.bval {
			return append(dst, trueBytes...)
		}
		return append(dst, falseBytes...)
	case kindNumber:
		return append(dst, v.value.num...)
	default:
		return append(dst, nullBytes...)
	}
}

func scalarJSONBytes(v statementScalarValue, arena *[]byte) []byte {
	if v.isNull() {
		return nullBytes
	}
	if (v.direct || v.exact) && v.cell.raw != nil {
		return v.cell.raw
	}
	if v.value.kind == kindNumber && v.value.num != nil {
		return v.value.num
	}
	if v.value.kind == kindBool {
		if v.value.bval {
			return trueBytes
		}
		return falseBytes
	}
	start := len(*arena)
	*arena = appendScalarJSON(*arena, v)
	return (*arena)[start:len(*arena):len(*arena)]
}

func jsonTypeFromRaw(raw []byte) string {
	for _, b := range raw {
		if isASCIISpace(b) {
			continue
		}
		switch b {
		case '{':
			return "object"
		case '[':
			return "array"
		case '"':
			return "string"
		case 't', 'f':
			return "boolean"
		case 'n':
			return "null"
		default:
			return "number"
		}
	}
	return "null"
}

func scalarJSONTypeOf(v statementScalarValue) string {
	if v.direct || v.exact {
		switch v.cell.kind {
		case TypeNull:
			return "null"
		case TypeBool:
			return "boolean"
		case TypeNumber:
			return "number"
		case TypeString:
			return "string"
		default:
			return jsonTypeFromRaw(v.cell.raw)
		}
	}
	switch v.value.kind {
	case kindString:
		return "string"
	case kindBool:
		return "boolean"
	case kindNumber:
		return "number"
	case kindNull:
		return "null"
	default:
		return jsonTypeFromRaw(v.value.raw)
	}
}

func parseFlexibleTimestamp(s string) (time.Time, error) {
	for i := range flexibleTimestampLayouts {
		if t, err := time.Parse(flexibleTimestampLayouts[i], s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}
