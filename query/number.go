package query

import (
	"fmt"
)

// Number is an exact JSON number spelling used by the programmatic plan API
// and SQL parameter binding. It preserves values that cannot round-trip
// through float64, including integers beyond 2^53.
//
// Number describes a JSON value, not a query document. SQL remains the textual
// query language; callers normally reach this type through an exact numeric
// driver binding.
type Number string

// validate reports whether n is one JSON number. Parsed SQL literals are
// already valid, but a caller-provided exact parameter is checked before it is
// admitted to a compiled predicate.
func (n Number) validate() error {
	text := string(n)
	if !validJSONNumber(text) {
		return fmt.Errorf("%q is not a JSON number", text)
	}
	return nil
}

// validJSONNumber recognizes exactly the RFC 8259 number grammar. A whole
// document validator would also admit surrounding JSON whitespace, which is
// not part of an exact Number spelling.
func validJSONNumber(text string) bool {
	i := 0
	if i < len(text) && text[i] == '-' {
		i++
	}
	if i == len(text) {
		return false
	}
	switch text[i] {
	case '0':
		i++
		if i < len(text) && text[i] >= '0' && text[i] <= '9' {
			return false
		}
	default:
		if text[i] < '1' || text[i] > '9' {
			return false
		}
		for i++; i < len(text) && text[i] >= '0' && text[i] <= '9'; i++ {
		}
	}
	if i < len(text) && text[i] == '.' {
		i++
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(text)
}
