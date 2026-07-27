package competitive

import (
	"bytes"
	"encoding/json"

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
// See matchesCountryStdlib for what the same predicate costs with the standard
// library, which is what most applications would actually reach for.
func matchesCountry(src, needle []byte) (bool, error) {
	rv, ok, err := countryPointer.ScanFirstRawTrusted(src)
	if err != nil || !ok {
		return false, err
	}
	return bytes.Equal(rv.Bytes(), needle), nil
}

type countryOnly struct {
	Country string `json:"country"`
}

// matchesCountryStdlib is the same predicate through encoding/json. The
// harness runs one engine both ways so the report can separate "what storage
// costs" from "what parsing costs" instead of conflating them.
func matchesCountryStdlib(src []byte, value string) (bool, error) {
	var doc countryOnly
	if err := json.Unmarshal(src, &doc); err != nil {
		return false, err
	}
	return doc.Country == value, nil
}
