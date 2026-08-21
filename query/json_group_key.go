package query

import vibejson "github.com/thesyncim/vibejson"

// JSONGroupKeyEncoder appends the query engine's canonical GROUP BY identity
// for validated JSON cells. It retains only reusable decoded-string scratch;
// returned keys belong to the caller's destination. The zero value is ready.
// An encoder is not safe for concurrent use.
type JSONGroupKeyEncoder struct {
	text []byte
}

// Append validates value as exactly one JSON value and appends its canonical
// GROUP BY identity to dst. Numeric spelling variants with the same exact
// value and escaped/unescaped spellings of the same string receive identical
// keys. Containers retain the query engine's exact-source identity.
func (e *JSONGroupKeyEncoder) Append(dst, value []byte) ([]byte, bool) {
	value = trimJSONGroupSpace(value)
	if e == nil || len(value) == 0 || !vibejson.Valid(value) {
		return dst, false
	}
	e.text = e.text[:0]
	scalar := classifyRawInto(vibejson.RawValue{Src: value}, &e.text)
	return appendGroupKey(dst, scalar), true
}

func trimJSONGroupSpace(value []byte) []byte {
	start, end := 0, len(value)
	for start < end && isJSONGroupSpace(value[start]) {
		start++
	}
	for end > start && isJSONGroupSpace(value[end-1]) {
		end--
	}
	return value[start:end]
}

func isJSONGroupSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}
