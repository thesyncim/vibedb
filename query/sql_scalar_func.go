package query

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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
		"json_strip_nulls", "jsonb_strip_nulls", "string_to_array":
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
	args := make([]statementScalarValue, len(node.funcArgs))
	for i, idx := range node.funcArgs {
		args[i] = r.values[idx]
	}
	name := node.funcName

	// Non-strict functions (handle NULLs themselves).
	switch name {
	case "concat":
		var b strings.Builder
		for _, a := range args {
			if a.isNull() {
				continue
			}
			b.WriteString(scalarAsDisplayString(a))
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: b.String()}}, nil
	case "concat_ws":
		if len(args) < 1 {
			return statementScalarValue{}, fmt.Errorf("%w: concat_ws requires at least 1 argument", ErrScalarType)
		}
		if args[0].isNull() {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
		sep := scalarAsDisplayString(args[0])
		var parts []string
		for _, a := range args[1:] {
			if a.isNull() {
				continue
			}
			parts = append(parts, scalarAsDisplayString(a))
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: strings.Join(parts, sep)}}, nil
	case "now", "current_timestamp", "clock_timestamp", "statement_timestamp":
		return statementScalarValue{value: scalar{kind: kindString, sval: time.Now().UTC().Format(time.RFC3339Nano)}}, nil
	case "current_date":
		return statementScalarValue{value: scalar{kind: kindString, sval: time.Now().UTC().Format("2006-01-02")}}, nil
	case "gen_random_uuid":
		var u [16]byte
		_, _ = rand.Read(u[:])
		u[6] = (u[6] & 0x0f) | 0x40
		u[8] = (u[8] & 0x3f) | 0x80
		s := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
		return statementScalarValue{value: scalar{kind: kindString, sval: s}}, nil
	case "pi":
		return statementScalarValue{value: arenaFloatScalar(arena, math.Pi)}, nil
	case "json_build_array", "jsonb_build_array":
		elems := make([]json.RawMessage, len(args))
		for i, a := range args {
			elems[i] = scalarAsJSONRaw(a)
		}
		raw, err := json.Marshal(elems)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "json_build_object", "jsonb_build_object":
		if len(args)%2 != 0 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires an even number of arguments", ErrScalarType, name)
		}
		obj := make(map[string]json.RawMessage, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			if args[i].isNull() {
				return statementScalarValue{}, fmt.Errorf("%w: %s key must not be null", ErrScalarType, name)
			}
			obj[scalarAsDisplayString(args[i])] = scalarAsJSONRaw(args[i+1])
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "to_json", "to_jsonb":
		if len(args) != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		raw := scalarAsJSONRaw(args[0])
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "json_typeof", "jsonb_typeof":
		if len(args) != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		if args[0].isNull() {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: scalarJSONTypeOf(args[0])}}, nil
	}

	// Strict functions: any NULL argument yields NULL.
	for _, a := range args {
		if a.isNull() {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
	}

	switch name {
	case "lower":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: strings.ToLower(s)}}, nil
	case "upper":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: strings.ToUpper(s)}}, nil
	case "length", "char_length", "character_length":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaIntScalar(arena, int64(utf8.RuneCountInString(s)))}, nil
	case "octet_length":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaIntScalar(arena, int64(len(s)))}, nil
	case "trim", "btrim":
		if len(args) == 1 {
			s, err := requireStringArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimSpace(s)}}, nil
		}
		if len(args) == 2 {
			s, err := requireStringArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			cut, err := requireStringArg(name, args[1])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.Trim(s, cut)}}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s takes 1 or 2 arguments", ErrScalarType, name)
	case "ltrim":
		if len(args) == 1 {
			s, err := requireStringArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimLeftFunc(s, unicode.IsSpace)}}, nil
		}
		if len(args) == 2 {
			s, err := requireStringArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			cut, err := requireStringArg(name, args[1])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimLeft(s, cut)}}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s takes 1 or 2 arguments", ErrScalarType, name)
	case "rtrim":
		if len(args) == 1 {
			s, err := requireStringArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimRightFunc(s, unicode.IsSpace)}}, nil
		}
		if len(args) == 2 {
			s, err := requireStringArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			cut, err := requireStringArg(name, args[1])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: strings.TrimRight(s, cut)}}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s takes 1 or 2 arguments", ErrScalarType, name)
	case "replace":
		if len(args) != 3 {
			return statementScalarValue{}, fmt.Errorf("%w: replace requires 3 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		oldS, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		newS, err := requireStringArg(name, args[2])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: strings.ReplaceAll(s, oldS, newS)}}, nil
	case "substring", "substr":
		if len(args) < 2 || len(args) > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: %s takes 2 or 3 arguments", ErrScalarType, name)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		startF, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		runes := []rune(s)
		start := int(startF) - 1
		if start < 0 {
			start = 0
		}
		if start >= len(runes) {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		end := len(runes)
		if len(args) == 3 {
			nF, err := requireNumberArg(name, args[2])
			if err != nil {
				return statementScalarValue{}, err
			}
			if nF < 0 {
				return statementScalarValue{}, fmt.Errorf("%w: negative substring length", ErrScalarNumericRange)
			}
			if start+int(nF) < end {
				end = start + int(nF)
			}
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: string(runes[start:end])}}, nil
	case "left":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: left requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		runes := []rune(s)
		n := int(nF)
		if n < 0 {
			n = len(runes) + n
		}
		if n <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		if n > len(runes) {
			n = len(runes)
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: string(runes[:n])}}, nil
	case "right":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: right requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		runes := []rune(s)
		n := int(nF)
		if n < 0 {
			n = len(runes) + n
		}
		if n <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		if n > len(runes) {
			n = len(runes)
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: string(runes[len(runes)-n:])}}, nil
	case "split_part":
		if len(args) != 3 {
			return statementScalarValue{}, fmt.Errorf("%w: split_part requires 3 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		delim, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		fieldF, err := requireNumberArg(name, args[2])
		if err != nil {
			return statementScalarValue{}, err
		}
		field := int(fieldF)
		if field == 0 {
			return statementScalarValue{}, fmt.Errorf("%w: field position must not be zero", ErrScalarNumericRange)
		}
		parts := strings.Split(s, delim)
		if field < 0 {
			field = len(parts) + field + 1
		}
		if field < 1 || field > len(parts) {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: parts[field-1]}}, nil
	case "strpos":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: strpos requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		sub, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		idx := strings.Index(s, sub)
		if idx < 0 {
			return statementScalarValue{value: arenaIntScalar(arena, 0)}, nil
		}
		pos := utf8.RuneCountInString(s[:idx]) + 1
		return statementScalarValue{value: arenaIntScalar(arena, int64(pos))}, nil
	case "reverse":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: string(runes)}}, nil
	case "repeat":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: repeat requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		n := int(nF)
		if n <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		if len(s)*n > 1<<20 {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: strings.Repeat(s, n)}}, nil
	case "lpad", "rpad":
		if len(args) < 2 || len(args) > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: %s takes 2 or 3 arguments", ErrScalarType, name)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		nF, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		fill := " "
		if len(args) == 3 {
			fill, err = requireStringArg(name, args[2])
			if err != nil {
				return statementScalarValue{}, err
			}
		}
		n := int(nF)
		if n <= 0 {
			return statementScalarValue{value: scalar{kind: kindString, sval: ""}}, nil
		}
		runes := []rune(s)
		if len(runes) >= n {
			return statementScalarValue{value: scalar{kind: kindString, sval: string(runes[:n])}}, nil
		}
		if fill == "" {
			return statementScalarValue{value: scalar{kind: kindString, sval: s}}, nil
		}
		fillRunes := []rune(fill)
		need := n - len(runes)
		pad := make([]rune, need)
		for i := 0; i < need; i++ {
			pad[i] = fillRunes[i%len(fillRunes)]
		}
		if name == "lpad" {
			return statementScalarValue{value: scalar{kind: kindString, sval: string(pad) + s}}, nil
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: s + string(pad)}}, nil
	case "initcap":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		var b strings.Builder
		newWord := true
		for _, ch := range s {
			if unicode.IsLetter(ch) || unicode.IsDigit(ch) {
				if newWord {
					b.WriteRune(unicode.ToUpper(ch))
					newWord = false
				} else {
					b.WriteRune(unicode.ToLower(ch))
				}
			} else {
				b.WriteRune(ch)
				newWord = true
			}
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: b.String()}}, nil
	case "md5":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		sum := md5.Sum([]byte(s))
		return statementScalarValue{value: scalar{kind: kindString, sval: hex.EncodeToString(sum[:])}}, nil
	case "ascii":
		s, err := requireOneString(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		if s == "" {
			return statementScalarValue{value: arenaIntScalar(arena, 0)}, nil
		}
		ch, _ := utf8.DecodeRuneInString(s)
		return statementScalarValue{value: arenaIntScalar(arena, int64(ch))}, nil
	case "chr":
		nF, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: string(rune(int(nF)))}}, nil
	case "starts_with":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: starts_with requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		pref, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindBool, bval: strings.HasPrefix(s, pref)}}, nil
	case "translate":
		if len(args) != 3 {
			return statementScalarValue{}, fmt.Errorf("%w: translate requires 3 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		from, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		to, err := requireStringArg(name, args[2])
		if err != nil {
			return statementScalarValue{}, err
		}
		fromR, toR := []rune(from), []rune(to)
		m := make(map[rune]rune, len(fromR))
		del := make(map[rune]bool)
		for i, ch := range fromR {
			if _, seen := m[ch]; seen || del[ch] {
				continue
			}
			if i < len(toR) {
				m[ch] = toR[i]
			} else {
				del[ch] = true
			}
		}
		var b strings.Builder
		for _, ch := range s {
			if del[ch] {
				continue
			}
			if rep, ok := m[ch]; ok {
				b.WriteRune(rep)
			} else {
				b.WriteRune(ch)
			}
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: b.String()}}, nil
	case "abs":
		n, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Abs(n))}, nil
	case "ceil", "ceiling":
		n, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Ceil(n))}, nil
	case "floor":
		n, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Floor(n))}, nil
	case "round":
		if len(args) == 1 {
			n, err := requireNumberArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: arenaFloatScalar(arena, math.Round(n))}, nil
		}
		if len(args) == 2 {
			n, err := requireNumberArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			scale, err := requireNumberArg(name, args[1])
			if err != nil {
				return statementScalarValue{}, err
			}
			pow := math.Pow(10, math.Trunc(scale))
			return statementScalarValue{value: arenaFloatScalar(arena, math.Round(n*pow)/pow)}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: round takes 1 or 2 arguments", ErrScalarType)
	case "trunc":
		if len(args) == 1 {
			n, err := requireNumberArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			return statementScalarValue{value: arenaFloatScalar(arena, math.Trunc(n))}, nil
		}
		if len(args) == 2 {
			n, err := requireNumberArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			scale, err := requireNumberArg(name, args[1])
			if err != nil {
				return statementScalarValue{}, err
			}
			pow := math.Pow(10, math.Trunc(scale))
			return statementScalarValue{value: arenaFloatScalar(arena, math.Trunc(n*pow)/pow)}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: trunc takes 1 or 2 arguments", ErrScalarType)
	case "power", "pow":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 2 arguments", ErrScalarType, name)
		}
		a, err := requireNumberArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		b, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		res := math.Pow(a, b)
		if math.IsNaN(res) || math.IsInf(res, 0) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
	case "sqrt":
		n, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		if n < 0 {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Sqrt(n))}, nil
	case "mod":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: mod requires 2 arguments", ErrScalarType)
		}
		a, err := requireNumberArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		b, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		if b == 0 {
			return statementScalarValue{}, ErrScalarDivisionByZero
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Mod(a, b))}, nil
	case "sign":
		n, err := requireOneNumber(name, args)
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
		n, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		if n <= 0 {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, math.Log(n))}, nil
	case "log", "log10":
		if len(args) == 1 {
			n, err := requireNumberArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			if n <= 0 {
				return statementScalarValue{}, ErrScalarNumericRange
			}
			return statementScalarValue{value: arenaFloatScalar(arena, math.Log10(n))}, nil
		}
		if len(args) == 2 && name == "log" {
			base, err := requireNumberArg(name, args[0])
			if err != nil {
				return statementScalarValue{}, err
			}
			val, err := requireNumberArg(name, args[1])
			if err != nil {
				return statementScalarValue{}, err
			}
			if base <= 0 || base == 1 || val <= 0 {
				return statementScalarValue{}, ErrScalarNumericRange
			}
			return statementScalarValue{value: arenaFloatScalar(arena, math.Log(val)/math.Log(base))}, nil
		}
		return statementScalarValue{}, fmt.Errorf("%w: %s invalid argument count", ErrScalarType, name)
	case "exp":
		n, err := requireOneNumber(name, args)
		if err != nil {
			return statementScalarValue{}, err
		}
		res := math.Exp(n)
		if math.IsInf(res, 0) {
			return statementScalarValue{}, ErrScalarNumericRange
		}
		return statementScalarValue{value: arenaFloatScalar(arena, res)}, nil
	case "date_trunc":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: date_trunc requires 2 arguments", ErrScalarType)
		}
		field, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		tsStr, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		tm, err := parseFlexibleTimestamp(tsStr)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %v", ErrScalarInvalidText, err)
		}
		tm = tm.UTC()
		var trunc time.Time
		switch strings.ToLower(field) {
		case "year":
			trunc = time.Date(tm.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		case "month":
			trunc = time.Date(tm.Year(), tm.Month(), 1, 0, 0, 0, 0, time.UTC)
		case "day":
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.UTC)
		case "hour":
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), 0, 0, 0, time.UTC)
		case "minute":
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), 0, 0, time.UTC)
		case "second":
			trunc = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(), 0, time.UTC)
		default:
			return statementScalarValue{}, fmt.Errorf("%w: unsupported date_trunc field %q", ErrScalarInvalidText, field)
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: trunc.Format(time.RFC3339)}}, nil
	case "extract":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: extract requires 2 arguments", ErrScalarType)
		}
		field, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		tsStr, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		tm, err := parseFlexibleTimestamp(tsStr)
		if err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %v", ErrScalarInvalidText, err)
		}
		tm = tm.UTC()
		var out float64
		switch strings.ToLower(field) {
		case "year":
			out = float64(tm.Year())
		case "month":
			out = float64(tm.Month())
		case "day":
			out = float64(tm.Day())
		case "hour":
			out = float64(tm.Hour())
		case "minute":
			out = float64(tm.Minute())
		case "second":
			out = float64(tm.Second())
		case "dow":
			out = float64(tm.Weekday())
		case "doy":
			out = float64(tm.YearDay())
		case "epoch":
			out = float64(tm.Unix()) + float64(tm.Nanosecond())/1e9
		default:
			return statementScalarValue{}, fmt.Errorf("%w: unsupported extract field %q", ErrScalarInvalidText, field)
		}
		return statementScalarValue{value: arenaFloatScalar(arena, out)}, nil
	case "json_array_length", "jsonb_array_length", "cardinality":
		if len(args) != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		raw := scalarAsJSONRaw(args[0])
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %s expects a JSON array", ErrScalarType, name)
		}
		return statementScalarValue{value: arenaIntScalar(arena, int64(len(arr)))}, nil
	case "array_length":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: array_length requires 2 arguments", ErrScalarType)
		}
		dim, err := requireNumberArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		if int(dim) != 1 {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
		raw := scalarAsJSONRaw(args[0])
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: array_length expects a JSON array", ErrScalarType)
		}
		if len(arr) == 0 {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
		return statementScalarValue{value: arenaIntScalar(arena, int64(len(arr)))}, nil
	case "array_to_string":
		if len(args) < 2 || len(args) > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: array_to_string takes 2 or 3 arguments", ErrScalarType)
		}
		delim, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		hasNullStr := len(args) == 3
		nullStr := ""
		if hasNullStr {
			nullStr, err = requireStringArg(name, args[2])
			if err != nil {
				return statementScalarValue{}, err
			}
		}
		var arr []any
		if err := json.Unmarshal(scalarAsJSONRaw(args[0]), &arr); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: array_to_string expects an array", ErrScalarType)
		}
		var parts []string
		for _, elem := range arr {
			if elem == nil {
				if hasNullStr {
					parts = append(parts, nullStr)
				}
				continue
			}
			parts = append(parts, fmt.Sprint(elem))
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: strings.Join(parts, delim)}}, nil
	case "string_to_array":
		if len(args) != 2 {
			return statementScalarValue{}, fmt.Errorf("%w: string_to_array requires 2 arguments", ErrScalarType)
		}
		s, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		delim, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		parts := strings.Split(s, delim)
		raw, err := json.Marshal(parts)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "json_extract_path", "jsonb_extract_path", "json_extract_path_text", "jsonb_extract_path_text":
		if len(args) < 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires at least 1 argument", ErrScalarType, name)
		}
		var current any
		if err := json.Unmarshal(scalarAsJSONRaw(args[0]), &current); err != nil {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
		for _, pathArg := range args[1:] {
			key, err := requireStringArg(name, pathArg)
			if err != nil {
				return statementScalarValue{}, err
			}
			switch container := current.(type) {
			case map[string]any:
				next, exists := container[key]
				if !exists {
					return statementScalarValue{value: scalar{kind: kindNull}}, nil
				}
				current = next
			case []any:
				idx, convErr := strconv.Atoi(key)
				if convErr != nil || idx < 0 || idx >= len(container) {
					return statementScalarValue{value: scalar{kind: kindNull}}, nil
				}
				current = container[idx]
			default:
				return statementScalarValue{value: scalar{kind: kindNull}}, nil
			}
		}
		if current == nil {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
		if strings.HasSuffix(name, "_text") {
			if s, ok := current.(string); ok {
				return statementScalarValue{value: scalar{kind: kindString, sval: s}}, nil
			}
			raw, _ := json.Marshal(current)
			return statementScalarValue{value: scalar{kind: kindString, sval: string(raw)}}, nil
		}
		raw, _ := json.Marshal(current)
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "json_strip_nulls", "jsonb_strip_nulls":
		if len(args) != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
		}
		var val any
		if err := json.Unmarshal(scalarAsJSONRaw(args[0]), &val); err != nil {
			return statementScalarValue{}, err
		}
		stripped := stripJSONNulls(val)
		raw, err := json.Marshal(stripped)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "jsonb_pretty":
		if len(args) != 1 {
			return statementScalarValue{}, fmt.Errorf("%w: jsonb_pretty requires 1 argument", ErrScalarType)
		}
		var val any
		if err := json.Unmarshal(scalarAsJSONRaw(args[0]), &val); err != nil {
			return statementScalarValue{}, err
		}
		raw, err := json.MarshalIndent(val, "", "    ")
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{value: scalar{kind: kindString, sval: string(raw)}}, nil
	case "jsonb_set", "json_set":
		if len(args) < 3 || len(args) > 4 {
			return statementScalarValue{}, fmt.Errorf("%w: %s requires 3 or 4 arguments", ErrScalarType, name)
		}
		var target any
		if err := json.Unmarshal(scalarAsJSONRaw(args[0]), &target); err != nil {
			return statementScalarValue{}, fmt.Errorf("%w: %s target is not valid JSON", ErrScalarType, name)
		}
		if s, ok := target.(string); ok {
			var inner any
			if json.Unmarshal([]byte(s), &inner) == nil {
				target = inner
			}
		}
		pathStr, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		segments := parsePGJSONPath(pathStr)
		var newVal any
		if args[2].direct {
			if err := json.Unmarshal(scalarAsJSONRaw(args[2]), &newVal); err != nil {
				newVal = string(scalarAsJSONRaw(args[2]))
			}
		} else if args[2].value.kind == kindString {
			if err := json.Unmarshal([]byte(args[2].value.sval), &newVal); err != nil {
				newVal = args[2].value.sval
			}
		} else {
			_ = json.Unmarshal(scalarAsJSONRaw(args[2]), &newVal)
		}
		createMissing := true
		if len(args) == 4 && args[3].value.kind == kindBool {
			createMissing = args[3].value.bval
		}
		updated := jsonbSetPath(target, segments, newVal, createMissing)
		raw, err := json.Marshal(updated)
		if err != nil {
			return statementScalarValue{}, err
		}
		return statementScalarValue{cell: jsonCellFromBytes(arena, raw), direct: true}, nil
	case "regexp_like", "regexp_count", "regexp_substr":
		if len(args) < 2 || len(args) > 3 {
			return statementScalarValue{}, fmt.Errorf("%w: %s takes 2 or 3 arguments", ErrScalarType, name)
		}
		src, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		pat, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		flags := ""
		if len(args) == 3 {
			flags, err = requireStringArg(name, args[2])
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
				return statementScalarValue{value: scalar{kind: kindNull}}, nil
			}
			return statementScalarValue{value: scalar{kind: kindString, sval: loc}}, nil
		}
	case "regexp_replace":
		if len(args) < 3 || len(args) > 4 {
			return statementScalarValue{}, fmt.Errorf("%w: regexp_replace takes 3 or 4 arguments", ErrScalarType)
		}
		src, err := requireStringArg(name, args[0])
		if err != nil {
			return statementScalarValue{}, err
		}
		pat, err := requireStringArg(name, args[1])
		if err != nil {
			return statementScalarValue{}, err
		}
		repl, err := requireStringArg(name, args[2])
		if err != nil {
			return statementScalarValue{}, err
		}
		flags := ""
		if len(args) == 4 {
			flags, err = requireStringArg(name, args[3])
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
		out := make([]byte, 0, len(src))
		out = append(out, src[:loc[0]]...)
		out = re.ExpandString(out, repl, src, loc)
		out = append(out, src[loc[1]:]...)
		return statementScalarValue{value: scalar{kind: kindString, sval: string(out)}}, nil
	default:
		return statementScalarValue{}, fmt.Errorf("%w: unknown scalar function %q", ErrScalarType, name)
	}
}

func compilePGRegexp(pat, flags string) (*regexp.Regexp, error) {
	return cachedPGRegexp(pat, flags)
}

func parsePGJSONPath(s string) []string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return nil
		}
		parts := strings.Split(inner, ",")
		for i := range parts {
			parts[i] = strings.Trim(strings.TrimSpace(parts[i]), `"`)
		}
		return parts
	}
	if len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']' {
		var arr []string
		if json.Unmarshal([]byte(s), &arr) == nil {
			return arr
		}
	}
	if s == "" {
		return nil
	}
	return []string{s}
}

func jsonbSetPath(target any, path []string, newVal any, createMissing bool) any {
	if len(path) == 0 {
		return newVal
	}
	key := path[0]
	switch container := target.(type) {
	case map[string]any:
		out := make(map[string]any, len(container)+1)
		for k, v := range container {
			out[k] = v
		}
		if len(path) == 1 {
			if _, exists := out[key]; exists || createMissing {
				out[key] = newVal
			}
			return out
		}
		if existing, exists := out[key]; exists {
			out[key] = jsonbSetPath(existing, path[1:], newVal, createMissing)
		}
		return out
	case []any:
		idx, err := strconv.Atoi(key)
		if err != nil {
			return target
		}
		if idx < 0 {
			idx = len(container) + idx
		}
		out := append([]any(nil), container...)
		if idx < 0 || idx >= len(out) {
			if createMissing && len(path) == 1 {
				if idx >= len(out) {
					return append(out, newVal)
				}
				return append([]any{newVal}, out...)
			}
			return out
		}
		if len(path) == 1 {
			out[idx] = newVal
		} else {
			out[idx] = jsonbSetPath(out[idx], path[1:], newVal, createMissing)
		}
		return out
	default:
		return target
	}
}

func stripJSONNulls(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if val == nil {
				continue
			}
			out[k] = stripJSONNulls(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = stripJSONNulls(val)
		}
		return out
	default:
		return v
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
	if math.Trunc(f) == f && f >= math.MinInt64 && f <= 9223372036854774784 {
		return arenaIntScalar(arena, int64(f))
	}
	start := len(*arena)
	*arena = strconv.AppendFloat(*arena, f, 'f', -1, 64)
	return scalar{kind: kindNumber, num: (*arena)[start:len(*arena):len(*arena)]}
}

func jsonCellFromBytes(arena *[]byte, raw []byte) Cell {
	start := len(*arena)
	*arena = append(*arena, raw...)
	cell, err := ParseJSONCell((*arena)[start:len(*arena):len(*arena)])
	if err != nil {
		return NullCell()
	}
	return cell
}

func requireOneString(name string, args []statementScalarValue) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
	}
	return requireStringArg(name, args[0])
}

func requireStringArg(name string, v statementScalarValue) (string, error) {
	if v.value.kind == kindString {
		return v.value.sval, nil
	}
	return "", fmt.Errorf("%w: %s requires a string argument", ErrScalarType, name)
}

func requireOneNumber(name string, args []statementScalarValue) (float64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("%w: %s requires 1 argument", ErrScalarType, name)
	}
	return requireNumberArg(name, args[0])
}

func requireNumberArg(name string, v statementScalarValue) (float64, error) {
	if v.value.kind == kindNumber {
		if v.value.isInt {
			return float64(v.value.ival), nil
		}
		f, err := strconv.ParseFloat(string(v.value.num), 64)
		if err != nil {
			return 0, ErrScalarNumericRange
		}
		return f, nil
	}
	if v.exact {
		f, err := strconv.ParseFloat(string(v.cell.AppendJSON(nil)), 64)
		if err != nil {
			return 0, ErrScalarNumericRange
		}
		return f, nil
	}
	return 0, fmt.Errorf("%w: %s requires a numeric argument", ErrScalarType, name)
}

func scalarAsDisplayString(v statementScalarValue) string {
	switch v.value.kind {
	case kindString:
		return v.value.sval
	case kindBool:
		if v.value.bval {
			return "true"
		}
		return "false"
	case kindNumber:
		return string(v.value.num)
	default:
		if v.direct || v.exact {
			return string(v.cell.AppendJSON(nil))
		}
		return ""
	}
}

func scalarAsJSONRaw(v statementScalarValue) json.RawMessage {
	if v.isNull() {
		return json.RawMessage("null")
	}
	if v.direct || v.exact {
		return json.RawMessage(v.cell.AppendJSON(nil))
	}
	switch v.value.kind {
	case kindString:
		b, _ := json.Marshal(v.value.sval)
		return b
	case kindBool:
		if v.value.bval {
			return json.RawMessage("true")
		}
		return json.RawMessage("false")
	case kindNumber:
		return json.RawMessage(v.value.num)
	default:
		return json.RawMessage("null")
	}
}

func scalarJSONTypeOf(v statementScalarValue) string {
	raw := scalarAsJSONRaw(v)
	if len(raw) == 0 {
		return "null"
	}
	switch raw[0] {
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

func parseFlexibleTimestamp(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}
