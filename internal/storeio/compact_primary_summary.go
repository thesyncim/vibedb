package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

const (
	// CompactPrimarySummaryMaxKeyBytes bounds one canonical scalar extremum.
	// Longer values simply make that stripe/path unprunable; row correctness is
	// unchanged and metadata can never scale with an adversarial string.
	CompactPrimarySummaryMaxKeyBytes = 256
	// CompactPrimarySummaryMaxBytes is the complete trailing metadata ceiling
	// per stripe, including entry headers.
	CompactPrimarySummaryMaxBytes = 4 << 10
	compactPrimarySummaryHeader   = 8
	compactPrimarySummaryValid    = byte(1)
	// Raw scalar spellings above this ceiling cannot enter a 256-byte ordered
	// term cheaply enough to justify parsing. The larger fixed probe covers the
	// ordered-key codec's tag, exponent, escaping, and terminator overhead for
	// every admitted source without a transient growth allocation.
	compactPrimarySummaryMaxSourceBytes = 2 * CompactPrimarySummaryMaxKeyBytes
	compactPrimarySummaryProbeBytes     = 4 * CompactPrimarySummaryMaxKeyBytes
)

var compactPrimarySummaryNull = []byte("null")

func (b *UnifiedPrimaryLeafBuilder) resetCompactPrimarySummaries() {
	for i := range b.compactSummaryPointers {
		b.compactSummaryMin[i] = b.compactSummaryMin[i][:0]
		b.compactSummaryMax[i] = b.compactSummaryMax[i][:0]
		b.compactSummaryValid[i] = true
	}
}

func (b *UnifiedPrimaryLeafBuilder) invalidateCompactPrimarySummaries() {
	clear(b.compactSummaryValid)
}

func (b *UnifiedPrimaryLeafBuilder) addCompactPrimarySummaryRow(raw []byte) error {
	for i, pointer := range b.compactSummaryPointers {
		if !b.compactSummaryValid[i] {
			continue
		}
		value, found, err := pointer.GetRawTrusted(raw)
		if err != nil {
			return err
		}
		if !found {
			value = vibejson.RawValue{Src: compactPrimarySummaryNull}
		}
		term, ok := appendCompactPrimarySummaryTerm(b.compactSummaryProbe[:0], value)
		if !ok || len(term) > CompactPrimarySummaryMaxKeyBytes {
			b.compactSummaryValid[i] = false
			continue
		}
		b.compactSummaryProbe = term
		if len(b.compactSummaryMin[i]) == 0 ||
			bytes.Compare(term, b.compactSummaryMin[i]) < 0 {
			b.compactSummaryMin[i] = append(b.compactSummaryMin[i][:0], term...)
		}
		if len(b.compactSummaryMax[i]) == 0 ||
			bytes.Compare(term, b.compactSummaryMax[i]) > 0 {
			b.compactSummaryMax[i] = append(b.compactSummaryMax[i][:0], term...)
		}
	}
	return nil
}

func appendCompactPrimarySummaryTerm(dst []byte, raw vibejson.RawValue) ([]byte, bool) {
	if len(raw.Bytes()) > compactPrimarySummaryMaxSourceBytes {
		return dst, false
	}
	kind := IndexTermInvalid
	switch raw.Kind() {
	case document.Null:
		kind = IndexTermNull
	case document.Bool:
		kind = IndexTermBool
	case document.Number:
		kind = IndexTermNumber
	case document.String:
		kind = IndexTermString
	default:
		return dst, false
	}
	var component [1]IndexTermComponent
	component[0] = IndexTermComponent{
		Kind: kind, Direction: IndexTermAscending, JSON: raw.Bytes(),
	}
	return AppendIndexTermKey(dst, component[:])
}

func appendPreparedCompactPrimarySummaries(
	dst []byte,
	b *UnifiedPrimaryLeafBuilder,
	rowCount int,
) ([]byte, error) {
	start := len(dst)
	for i := range b.compactSummaryPointers {
		valid := rowCount != 0 && b.compactSummaryValid[i] &&
			len(b.compactSummaryMin[i]) != 0 && len(b.compactSummaryMax[i]) != 0
		entryBytes := compactPrimarySummaryHeader
		if valid {
			entryBytes += len(b.compactSummaryMin[i]) + len(b.compactSummaryMax[i])
		}
		if len(dst)-start+entryBytes > CompactPrimarySummaryMaxBytes ||
			entryBytes > int(^uint16(0)) {
			valid = false
			entryBytes = compactPrimarySummaryHeader
		}
		entry := len(dst)
		dst = append(dst, make([]byte, entryBytes)...)
		binary.LittleEndian.PutUint16(dst[entry+2:], uint16(entryBytes))
		if !valid {
			continue
		}
		dst[entry] = compactPrimarySummaryValid
		binary.LittleEndian.PutUint16(dst[entry+4:], uint16(len(b.compactSummaryMin[i])))
		binary.LittleEndian.PutUint16(dst[entry+6:], uint16(len(b.compactSummaryMax[i])))
		cursor := entry + compactPrimarySummaryHeader
		cursor += copy(dst[cursor:], b.compactSummaryMin[i])
		copy(dst[cursor:], b.compactSummaryMax[i])
	}
	return dst, nil
}

func validateCompactPrimarySummaries(raw []byte, count int) error {
	cursor := 0
	for i := 0; i < count; i++ {
		if len(raw)-cursor < compactPrimarySummaryHeader {
			return fmt.Errorf("%w: compact summary header", ErrCommonPrimaryLeafCorrupt)
		}
		flags := raw[cursor]
		reserved := raw[cursor+1]
		entryBytes := int(binary.LittleEndian.Uint16(raw[cursor+2:]))
		minBytes := int(binary.LittleEndian.Uint16(raw[cursor+4:]))
		maxBytes := int(binary.LittleEndian.Uint16(raw[cursor+6:]))
		if flags&^compactPrimarySummaryValid != 0 || reserved != 0 ||
			entryBytes < compactPrimarySummaryHeader || entryBytes > len(raw)-cursor ||
			minBytes > CompactPrimarySummaryMaxKeyBytes ||
			maxBytes > CompactPrimarySummaryMaxKeyBytes ||
			minBytes+maxBytes > entryBytes-compactPrimarySummaryHeader {
			return fmt.Errorf("%w: compact summary geometry", ErrCommonPrimaryLeafCorrupt)
		}
		body := raw[cursor+compactPrimarySummaryHeader : cursor+entryBytes]
		if flags == 0 {
			if minBytes != 0 || maxBytes != 0 || !allZero(body) {
				return fmt.Errorf("%w: compact summary disabled entry", ErrCommonPrimaryLeafCorrupt)
			}
		} else {
			if minBytes == 0 || maxBytes == 0 ||
				!allZero(body[minBytes+maxBytes:]) {
				return fmt.Errorf("%w: compact summary bounds", ErrCommonPrimaryLeafCorrupt)
			}
			minTerm := body[:minBytes]
			maxTerm := body[minBytes : minBytes+maxBytes]
			if !ValidIndexTermKey(minTerm) || !ValidIndexTermKey(maxTerm) ||
				bytes.Compare(minTerm, maxTerm) > 0 {
				return fmt.Errorf("%w: compact summary term", ErrCommonPrimaryLeafCorrupt)
			}
		}
		cursor += entryBytes
	}
	if cursor != len(raw) {
		return fmt.Errorf("%w: compact summary tail", ErrCommonPrimaryLeafCorrupt)
	}
	return nil
}

func compactPrimarySummaryAt(raw []byte, index int) (minTerm, maxTerm []byte, valid bool) {
	_, _, minTerm, maxTerm, valid = compactPrimarySummaryEntryAt(raw, index)
	return minTerm, maxTerm, valid
}

func compactPrimarySummaryEntryAt(
	raw []byte,
	index int,
) (offset, entryBytes int, minTerm, maxTerm []byte, valid bool) {
	cursor := 0
	for i := 0; i <= index; i++ {
		if len(raw)-cursor < compactPrimarySummaryHeader {
			return 0, 0, nil, nil, false
		}
		entryBytes := int(binary.LittleEndian.Uint16(raw[cursor+2:]))
		if entryBytes < compactPrimarySummaryHeader || entryBytes > len(raw)-cursor {
			return 0, 0, nil, nil, false
		}
		if i == index {
			if raw[cursor] != compactPrimarySummaryValid {
				return cursor, entryBytes, nil, nil, false
			}
			minBytes := int(binary.LittleEndian.Uint16(raw[cursor+4:]))
			maxBytes := int(binary.LittleEndian.Uint16(raw[cursor+6:]))
			body := raw[cursor+compactPrimarySummaryHeader : cursor+entryBytes]
			if minBytes == 0 || maxBytes == 0 || minBytes+maxBytes > len(body) {
				return 0, 0, nil, nil, false
			}
			return cursor, entryBytes,
				body[:minBytes], body[minBytes : minBytes+maxBytes], true
		}
		cursor += entryBytes
	}
	return 0, 0, nil, nil, false
}

// prepareCompactPrimarySummaryWiden derives a conservative after-replacement
// image without rescanning untouched rows. Old extrema are retained and new
// values may only widen them. Invalid new values disable that one entry. false
// asks the caller for a full rebuild when a wider spelling cannot fit the
// existing variable-size entry.
func (v *CompactPrimaryStripeView) prepareCompactPrimarySummaryWiden(
	b *UnifiedPrimaryLeafBuilder,
	replacements []CommonPrimaryUnifiedReplacement,
) (bool, error) {
	if v.summaryCount == 0 {
		return true, nil
	}
	if b == nil || len(b.compactSummaryPointers) != v.summaryCount {
		return false, nil
	}
	for summary := 0; summary < v.summaryCount; summary++ {
		_, entryBytes, minimum, maximum, valid := compactPrimarySummaryEntryAt(
			v.summaryRaw, summary,
		)
		b.compactSummaryValid[summary] = valid
		b.compactSummaryMin[summary] = b.compactSummaryMin[summary][:0]
		b.compactSummaryMax[summary] = b.compactSummaryMax[summary][:0]
		if !valid {
			continue
		}
		b.compactSummaryMin[summary] = append(
			b.compactSummaryMin[summary], minimum...,
		)
		b.compactSummaryMax[summary] = append(
			b.compactSummaryMax[summary], maximum...,
		)
		pointer := b.compactSummaryPointers[summary]
		for i := range replacements {
			value, found, err := pointer.GetRawTrusted(replacements[i].Value)
			if err != nil {
				return false, err
			}
			if !found {
				value = vibejson.RawValue{Src: compactPrimarySummaryNull}
			}
			term, ok := appendCompactPrimarySummaryTerm(b.compactSummaryProbe[:0], value)
			if !ok || len(term) > CompactPrimarySummaryMaxKeyBytes {
				b.compactSummaryValid[summary] = false
				break
			}
			b.compactSummaryProbe = term
			if bytes.Compare(term, b.compactSummaryMin[summary]) < 0 {
				b.compactSummaryMin[summary] = append(
					b.compactSummaryMin[summary][:0], term...,
				)
			}
			if bytes.Compare(term, b.compactSummaryMax[summary]) > 0 {
				b.compactSummaryMax[summary] = append(
					b.compactSummaryMax[summary][:0], term...,
				)
			}
		}
		if b.compactSummaryValid[summary] &&
			len(b.compactSummaryMin[summary])+len(b.compactSummaryMax[summary]) >
				entryBytes-compactPrimarySummaryHeader {
			return false, nil
		}
	}
	return true, nil
}

func (v *CompactPrimaryStripeView) rewriteCompactPrimarySummaries(
	raw []byte,
	b *UnifiedPrimaryLeafBuilder,
) bool {
	if len(raw) != len(v.summaryRaw) || b == nil ||
		len(b.compactSummaryPointers) != v.summaryCount {
		return false
	}
	for summary := 0; summary < v.summaryCount; summary++ {
		offset, entryBytes, _, _, _ := compactPrimarySummaryEntryAt(raw, summary)
		if entryBytes < compactPrimarySummaryHeader || offset+entryBytes > len(raw) {
			return false
		}
		entry := raw[offset : offset+entryBytes]
		clear(entry)
		binary.LittleEndian.PutUint16(entry[2:], uint16(entryBytes))
		if !b.compactSummaryValid[summary] {
			continue
		}
		minimum := b.compactSummaryMin[summary]
		maximum := b.compactSummaryMax[summary]
		if len(minimum) == 0 || len(maximum) == 0 ||
			len(minimum)+len(maximum) > entryBytes-compactPrimarySummaryHeader {
			return false
		}
		entry[0] = compactPrimarySummaryValid
		binary.LittleEndian.PutUint16(entry[4:], uint16(len(minimum)))
		binary.LittleEndian.PutUint16(entry[6:], uint16(len(maximum)))
		cursor := compactPrimarySummaryHeader
		cursor += copy(entry[cursor:], minimum)
		copy(entry[cursor:], maximum)
	}
	return true
}
