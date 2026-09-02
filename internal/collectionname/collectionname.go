// Package collectionname owns the reversible mapping between logical database
// collection names and portable on-disk filenames.
package collectionname

import (
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

const (
	FilePrefix    = "c-"
	PrimarySuffix = ".vjc"
	JournalSuffix = ".rjournal"
	// MaxComponentBytes is the portable filename-component byte bound used by
	// both the catalog codec and the standalone-file preflight.
	MaxComponentBytes = 255
	// MaxNameBytes keeps the complete paired journal filename within the
	// portable 255-byte component bound:
	//
	//   len("c-") + 2*120 + len(".vjc") + len(".rjournal") == 255
	MaxNameBytes = 120
)

// Valid reports whether name has one canonical reversible file encoding.
// NUL is excluded so memory and durable catalogs share one logical namespace.
// Names are application strings rather than path fragments, so separators,
// reserved device spellings, trailing dots/spaces, and distinct Unicode forms
// are all legal and remain distinct after encoding.
func Valid(name string) bool {
	return name != "" && len(name) <= MaxNameBytes && utf8.ValidString(name) &&
		!strings.ContainsRune(name, 0)
}

// Encode returns name's portable primary filename.
func Encode(name string) (string, bool) {
	if !Valid(name) {
		return "", false
	}
	encoded := make([]byte, len(FilePrefix)+hex.EncodedLen(len(name))+len(PrimarySuffix))
	copy(encoded, FilePrefix)
	at := len(FilePrefix)
	hex.Encode(encoded[at:at+hex.EncodedLen(len(name))], []byte(name))
	copy(encoded[len(encoded)-len(PrimarySuffix):], PrimarySuffix)
	return string(encoded), true
}

// Decode reverses an Encode filename. It accepts only the canonical lowercase
// spelling. Directory catalogs must additionally reject [PrimaryCaseAlias]
// before ignoring unknown files: on a case-insensitive filesystem an alias can
// address the canonical path even though Decode correctly refuses its spelling.
func Decode(filename string) (string, bool) {
	if !strings.HasPrefix(filename, FilePrefix) ||
		!strings.HasSuffix(filename, PrimarySuffix) {
		return "", false
	}
	encoded := filename[len(FilePrefix) : len(filename)-len(PrimarySuffix)]
	if encoded == "" || len(encoded)&1 != 0 || strings.ToLower(encoded) != encoded {
		return "", false
	}
	raw := make([]byte, hex.DecodedLen(len(encoded)))
	if _, err := hex.Decode(raw, []byte(encoded)); err != nil {
		return "", false
	}
	name := string(raw)
	if !Valid(name) {
		return "", false
	}
	return name, true
}

// DecodeJournal reverses the canonical journal filename paired with an
// encoded primary. It rejects arbitrary files that merely share the journal
// suffix, so startup cleanup never treats an unrelated sidecar as engine data.
func DecodeJournal(filename string) (string, bool) {
	if !strings.HasSuffix(filename, JournalSuffix) {
		return "", false
	}
	return Decode(strings.TrimSuffix(filename, JournalSuffix))
}

// PrimaryCaseAlias reports a non-canonical case spelling which folds to one
// valid encoded primary filename.
func PrimaryCaseAlias(filename string) bool {
	lower := strings.ToLower(filename)
	if lower == filename {
		return false
	}
	_, ok := Decode(lower)
	return ok
}

// JournalCaseAlias is the journal counterpart to [PrimaryCaseAlias].
func JournalCaseAlias(filename string) bool {
	lower := strings.ToLower(filename)
	if lower == filename {
		return false
	}
	_, ok := DecodeJournal(lower)
	return ok
}
