package driver

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"unicode/utf8"

	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	vibejsonsimd "github.com/thesyncim/vibejson/simd"
	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibejson/x/scanner"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

const jsonHexLower = "0123456789abcdef"

var (
	jsonNullBytes  = []byte("null")
	jsonTrueBytes  = []byte("true")
	jsonFalseBytes = []byte("false")
)

// appendJSONString appends s as a quoted, HTML-safe RFC 8259 JSON string to dst
// using vibejson's SIMD string scanner for the unescaped prefix fast path.
func appendJSONString(dst []byte, s string) []byte {
	return appendJSONStringBytes(dst, byteview.Bytes(s))
}

// appendJSONStringBytes appends raw as a quoted, HTML-safe RFC 8259 JSON string
// to dst, matching encoding/json.Marshal escaping semantics.
func appendJSONStringBytes(dst, raw []byte) []byte {
	dst = append(dst, '"')
	for cursor := 0; cursor < len(raw); {
		special := scanner.IndexHTMLStringSpecial(raw, cursor)
		dst = append(dst, raw[cursor:special]...)
		if special == len(raw) {
			break
		}
		b := raw[special]
		if b < utf8.RuneSelf {
			switch b {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			case '<':
				dst = append(dst, `\u003c`...)
			case '>':
				dst = append(dst, `\u003e`...)
			case '&':
				dst = append(dst, `\u0026`...)
			default:
				dst = append(dst, '\\', 'u', '0', '0', jsonHexLower[b>>4], jsonHexLower[b&0x0f])
			}
			cursor = special + 1
			continue
		}
		r, width := utf8.DecodeRune(raw[special:])
		switch {
		case r == '\u2028':
			dst = append(dst, `\u2028`...)
		case r == '\u2029':
			dst = append(dst, `\u2029`...)
		case r == utf8.RuneError && width == 1:
			dst = append(dst, `\ufffd`...)
		default:
			dst = append(dst, raw[special:special+width]...)
		}
		cursor = special + width
	}
	return append(dst, '"')
}

// jsonStringEncodedLen returns the exact byte length produced by
// appendJSONStringBytes(nil, raw).
func jsonStringEncodedLen(raw []byte) int {
	total := 2
	for cursor := 0; cursor < len(raw); {
		special := scanner.IndexHTMLStringSpecial(raw, cursor)
		total += special - cursor
		if special == len(raw) {
			break
		}
		b := raw[special]
		if b < utf8.RuneSelf {
			switch b {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				total += 2
			default:
				total += 6
			}
			cursor = special + 1
			continue
		}
		r, width := utf8.DecodeRune(raw[special:])
		if r == '\u2028' || r == '\u2029' || (r == utf8.RuneError && width == 1) {
			total += 6
		} else {
			total += width
		}
		cursor = special + width
	}
	return total
}

// validJSONNumber reports whether value is strictly a single RFC 8259 JSON
// number with no surrounding whitespace or trailing bytes.
func validJSONNumber(value string) bool {
	if value == "" {
		return false
	}
	_, ok := (vibejson.RawValue{Src: byteview.Bytes(value)}).NumberBytes()
	return ok
}

// appendScalarJSON appends a SQL driver scalar value as JSON to dst without
// using encoding/json.
func appendScalarJSON(dst []byte, value any) ([]byte, error) {
	for {
		switch v := value.(type) {
		case *bool:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *int64:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *float64:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *string:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *query.Number:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		default:
			goto encode
		}
	}
encode:
	switch v := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		if v {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	case int:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(dst, v, 10), nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return dst, errors.New("vibedb: numeric parameters must be finite")
		}
		out, ok := vibejsonsimd.AppendFloat32(dst, v)
		if !ok {
			return dst, errors.New("vibedb: numeric parameters must be finite")
		}
		return out, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return dst, errors.New("vibedb: numeric parameters must be finite")
		}
		out, ok := vibejsonsimd.AppendFloat64(dst, v)
		if !ok {
			return dst, errors.New("vibedb: numeric parameters must be finite")
		}
		return out, nil
	case query.Number:
		if !validJSONNumber(string(v)) {
			return dst, errors.New("vibedb: invalid numeric parameter")
		}
		return append(dst, v...), nil
	case vibejson.RawValue:
		number, ok := v.NumberBytes()
		if !ok {
			return dst, errors.New("vibedb: invalid numeric parameter")
		}
		return append(dst, number...), nil
	case []byte:
		if !utf8.Valid(v) {
			return dst, errors.New("vibedb: column string must be valid UTF-8")
		}
		return appendJSONStringBytes(dst, v), nil
	case string:
		if !utf8.ValidString(v) {
			return dst, errors.New("vibedb: column string must be valid UTF-8")
		}
		return appendJSONString(dst, v), nil
	default:
		return appendScalarJSONFallback(dst, value)
	}
}

//go:noinline
func appendScalarJSONFallback(dst []byte, value any) ([]byte, error) {
	encoded, err := vibejson.Marshal(&value)
	if err != nil {
		return dst, fmt.Errorf("vibedb: encode column value: %w", err)
	}
	return append(dst, encoded...), nil
}

type jsonObjectPatch struct {
	column     string
	value      []byte // valid raw JSON bytes
	lastMember int    // -1 = missing (append), -2 = shadowed by later patch, >=0 = replace member index
}

// patchJSONObject applies top-level key patches to a JSON object document using
// zero-copy vibejson object scanning and single-allocation streaming output.
//
// Duplicate keys in document follow last-occurrence-wins semantics: only the
// final occurrence of a matched key is replaced, preserving earlier members.
// Duplicate columns in patches also follow last-patch-wins semantics.
// Escaped keys in document (e.g. "\u0061") are unescaped before matching.
const (
	jsonIndexPoolDefaultCap = 64
	jsonIndexPoolMaxCap     = 4096
)

var jsonIndexPool = sync.Pool{
	New: func() any {
		buf := make([]vibejson.IndexEntry, jsonIndexPoolDefaultCap)
		return &buf
	},
}

// buildPooledJSONIndex validates and indexes document using a pooled entry
// buffer so steady-state callers perform zero heap allocations for indexing.
// Callers must pass the returned pool handle to releasePooledJSONIndex before
// returning, and must not retain references to the returned Index after release.
func buildPooledJSONIndex(document []byte) (vibejson.Index, *[]vibejson.IndexEntry, error) {
	bufPtr := jsonIndexPool.Get().(*[]vibejson.IndexEntry)
	idx, err := vibejson.BuildIndex(document, *bufPtr)
	if err == nil {
		return idx, bufPtr, nil
	}
	if !errors.Is(err, jsondoc.ErrIndexFull) {
		releasePooledJSONIndex(bufPtr)
		return vibejson.Index{}, nil, err
	}
	needed, reqErr := vibejson.RequiredIndexEntries(document)
	if reqErr != nil {
		releasePooledJSONIndex(bufPtr)
		return vibejson.Index{}, nil, reqErr
	}
	if needed <= jsonIndexPoolMaxCap {
		*bufPtr = make([]vibejson.IndexEntry, needed)
		idx, err = vibejson.BuildIndex(document, *bufPtr)
		if err != nil {
			releasePooledJSONIndex(bufPtr)
			return vibejson.Index{}, nil, err
		}
		return idx, bufPtr, nil
	}
	releasePooledJSONIndex(bufPtr)
	large := make([]vibejson.IndexEntry, needed)
	idx, err = vibejson.BuildIndex(document, large)
	if err != nil {
		return vibejson.Index{}, nil, err
	}
	return idx, nil, nil
}

func releasePooledJSONIndex(bufPtr *[]vibejson.IndexEntry) {
	if bufPtr != nil && cap(*bufPtr) <= jsonIndexPoolMaxCap {
		jsonIndexPool.Put(bufPtr)
	}
}

func patchJSONObject(
	document []byte,
	patches []jsonObjectPatch,
	maxDocumentBytes int,
) ([]byte, error) {
	idx, poolBuf, err := buildPooledJSONIndex(document)
	if err != nil {
		return nil, err
	}
	defer releasePooledJSONIndex(poolBuf)
	root := idx.Root()
	iter, ok := root.ObjectIter()
	if !ok {
		return nil, errors.New("vibedb: expected JSON object document")
	}
	if len(patches) == 0 {
		if maxDocumentBytes > 0 && len(document) > maxDocumentBytes {
			return nil, durable.ErrDocumentTooLarge
		}
		cp := make([]byte, len(document))
		copy(cp, document)
		return cp, nil
	}

	// Deduplicate patches so the last patch for any column wins.
	const linearPatchLimit = 8
	var byColumn map[string]int
	if len(patches) <= linearPatchLimit {
		for i := range patches {
			patches[i].lastMember = -1
			for prev := 0; prev < i; prev++ {
				if patches[prev].lastMember != -2 && patches[prev].column == patches[i].column {
					patches[prev].lastMember = -2
				}
			}
		}
	} else {
		byColumn = make(map[string]int, len(patches))
		for i := range patches {
			if prev, exists := byColumn[patches[i].column]; exists {
				patches[prev].lastMember = -2
			}
			patches[i].lastMember = -1
			byColumn[patches[i].column] = i
		}
	}

	memberCount, _ := root.ObjectLen()
	var keyScratch [64]byte
	keyText := keyScratch[:0]
	for member := 0; ; member++ {
		key, _, hasNext := iter.NextRaw()
		if !hasNext {
			break
		}
		var keyBytes []byte
		if raw, clean := key.StringBytes(); clean {
			keyBytes = raw
		} else {
			var textErr error
			keyText, _, textErr = key.AppendText(keyText[:0])
			if textErr != nil {
				return nil, fmt.Errorf("vibedb: decode object key: %w", textErr)
			}
			keyBytes = keyText
		}
		if byColumn == nil {
			for i := range patches {
				if patches[i].lastMember != -2 && string(keyBytes) == patches[i].column {
					patches[i].lastMember = member
					break
				}
			}
		} else if idx, exists := byColumn[string(keyBytes)]; exists {
			patches[idx].lastMember = member
		}
	}

	var replacementAt map[int]int
	missing := 0
	if len(patches) > linearPatchLimit {
		replacementAt = make(map[int]int, len(patches))
	}
	for i := range patches {
		switch patches[i].lastMember {
		case -2:
			continue
		case -1:
			missing++
		default:
			if replacementAt != nil {
				replacementAt[patches[i].lastMember] = i
			}
		}
	}

	// Pre-compute exact compact output size to enforce maxDocumentBytes before allocating.
	updatedBytes := 2 // '{' and '}'
	outputMembers := memberCount + missing
	if outputMembers > 1 {
		updatedBytes += outputMembers - 1 // commas
	}
	iter, _ = root.ObjectIter()
	for member := 0; ; member++ {
		key, value, hasNext := iter.NextRaw()
		if !hasNext {
			break
		}
		valueBytes := value.Bytes()
		if replacementAt != nil {
			if idx, replace := replacementAt[member]; replace {
				valueBytes = patches[idx].value
			}
		} else {
			for i := range patches {
				if patches[i].lastMember == member {
					valueBytes = patches[i].value
					break
				}
			}
		}
		add := len(key.Bytes()) + 1 + len(valueBytes)
		if maxDocumentBytes > 0 && (updatedBytes > maxDocumentBytes || add > maxDocumentBytes-updatedBytes) {
			return nil, durable.ErrDocumentTooLarge
		}
		updatedBytes += add
	}
	for i := range patches {
		if patches[i].lastMember != -1 {
			continue
		}
		add := jsonStringEncodedLen(byteview.Bytes(patches[i].column)) + 1 + len(patches[i].value)
		if maxDocumentBytes > 0 && (updatedBytes > maxDocumentBytes || add > maxDocumentBytes-updatedBytes) {
			return nil, durable.ErrDocumentTooLarge
		}
		updatedBytes += add
	}
	if maxDocumentBytes > 0 && updatedBytes > maxDocumentBytes {
		return nil, durable.ErrDocumentTooLarge
	}

	out := make([]byte, 0, updatedBytes)
	out = append(out, '{')
	wrote := 0
	iter, _ = root.ObjectIter()
	for member := 0; ; member++ {
		key, value, hasNext := iter.NextRaw()
		if !hasNext {
			break
		}
		if wrote != 0 {
			out = append(out, ',')
		}
		out = key.AppendJSON(out)
		out = append(out, ':')
		replaced := false
		if replacementAt != nil {
			if idx, ok := replacementAt[member]; ok {
				out = append(out, patches[idx].value...)
				replaced = true
			}
		} else {
			for i := range patches {
				if patches[i].lastMember == member {
					out = append(out, patches[i].value...)
					replaced = true
					break
				}
			}
		}
		if !replaced {
			out = value.AppendJSON(out)
		}
		wrote++
	}
	for i := range patches {
		if patches[i].lastMember != -1 {
			continue
		}
		if wrote != 0 {
			out = append(out, ',')
		}
		out = appendJSONString(out, patches[i].column)
		out = append(out, ':')
		out = append(out, patches[i].value...)
		wrote++
	}
	out = append(out, '}')
	return out, nil
}
