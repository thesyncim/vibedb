package competitive

import (
	"bytes"

	vibejson "github.com/thesyncim/vibejson"
)

// countryPointer is the compiled RFC 6901 pointer the key/value engines use to
// pull the predicate field out of each stored document.
var countryPointer = mustCompilePointer(FilterPath)

func mustCompilePointer(p string) vibejson.CompiledPointer {
	c, err := vibejson.CompilePointer(p)
	if err != nil {
		panic(err)
	}
	return c
}

// jsonScalarNeedle renders a string value as the JSON text a raw comparison
// can be made against, quotes included.
func jsonScalarNeedle(value string) []byte {
	out := make([]byte, 0, len(value)+2)
	out = append(out, '"')
	out = append(out, value...)
	out = append(out, '"')
	return out
}

// matchesCountry is the per-document predicate the key/value stores are forced
// to run, because none of them can see inside a value.
//
// This is deliberately the *fastest* spelling available, not a typical one:
// a precompiled pointer, an early-exit scan that stops at the first match, and
// the trusted (non-revalidating) variant, comparing raw bytes without decoding
// the string. Every one of those choices favours the key/value engines, since
// it strips their filter cost down to storage plus the minimum possible parse.
// See matchesCountryValidated for the cost of retaining full validation when
// the caller cannot prove that stored bytes were validated at admission.
func matchesCountry(src, needle []byte) (bool, error) {
	rv, ok, err := countryPointer.ScanFirstRawTrusted(src)
	if err != nil || !ok {
		return false, err
	}
	return bytes.Equal(rv.Bytes(), needle), nil
}

// matchesCountryValidated performs the same borrowed raw comparison while
// validating the complete document under a hard nesting bound. The benchmark
// pair therefore isolates the exact cost of trust at the storage boundary,
// without bringing a second JSON implementation or heap-built strings into the
// process.
func matchesCountryValidated(src, needle []byte) (bool, error) {
	rv, ok, err := countryPointer.GetRawOptions(src, vibejson.Options{MaxDepth: 8})
	if err != nil || !ok {
		return false, err
	}
	return bytes.Equal(rv.Bytes(), needle), nil
}
