package storeio

import (
	"bytes"
	"fmt"
)

// The token filter resolves the
// predicate path against each of the leaf's templates once (hole positions
// legitimately differ template to template), locate the needle in the leaf
// dictionary once, then evaluate rows from tokens without rendering — a tag
// walk to the hole plus a one-byte dict-id compare, a zigzag varint decode,
// or a short memcmp. Rows an engaged leaf cannot evaluate from tokens —
// trivial rows, container-target rows, and overflow rows — individually take
// the render-then-filter path at the fallback rate; the lane is per-row, never
// all-or-nothing.

// UnifiedEqFilter is the reusable state of one equality predicate over the
// token lane. The predicate semantics are canonical-spelling equality: the
// value at the path matches iff its canonical spelling equals the needle's
// canonical spelling. For strings, booleans, null, and canonical integers
// integers — every scalar the canonicalizer pins to one spelling per value —
// this is exactly value equality; float spellings compare textually
// ("5.0" != "5"), which is the documented scope of this lane. A filter is
// single-consumer and reusable across scans and snapshots.
type UnifiedEqFilter struct {
	resolver UnifiedHoleResolver
	needle   []byte
	// needleKind/needleInt classify the needle once against the token grammar
	// so the per-row compare never re-parses it: a canonical-int needle
	// compares as a decoded value, true/false/null compare as tag identity,
	// everything else as a spelling memcmp.
	needleKind UnifiedRowTokenKind
	needleInt  int64

	// Per-leaf state, rebuilt by prepareLeaf for each engaged unified leaf.
	prepared      bool
	dict          int16
	templateCount int
	holes         [unifiedMaxTemplates]int16
}

// NewUnifiedEqFilter builds a filter for path (the UnifiedHoleResolver
// syntax) and needle, which must be the JSON spelling of one comparand value;
// it is canonicalized here so callers may pass any valid spelling.
func NewUnifiedEqFilter(path, needle []byte) (*UnifiedEqFilter, error) {
	f := &UnifiedEqFilter{}
	if err := f.resolver.SetPath(path); err != nil {
		return nil, err
	}
	index, err := f.resolver.buildIndex(needle)
	if err != nil {
		return nil, fmt.Errorf("%w: unified filter needle: %w", ErrInvalidWrite, err)
	}
	var ws CanonicalWorkspace
	if IndexIsCanonical(index, &ws) {
		f.needle = append([]byte(nil), needle...)
	} else {
		canonical, err := AppendCanonicalIndexed(nil, index, &ws)
		if err != nil {
			return nil, err
		}
		f.needle = canonical
	}
	f.needleKind = UnifiedRowTokenLiteral
	switch {
	case bytes.Equal(f.needle, []byte("true")):
		f.needleKind = UnifiedRowTokenTrue
	case bytes.Equal(f.needle, []byte("false")):
		f.needleKind = UnifiedRowTokenFalse
	case bytes.Equal(f.needle, []byte("null")):
		f.needleKind = UnifiedRowTokenNull
	default:
		if value, ok := CanonicalIntValue(f.needle); ok {
			f.needleKind = UnifiedRowTokenInt
			f.needleInt = value
		}
	}
	return f, nil
}

// Needle returns the canonical spelling the filter compares against.
func (f *UnifiedEqFilter) Needle() []byte { return f.needle }

// Reset clears per-leaf state so the filter can begin a fresh scan; call it
// before handing the filter to a new cursor.
func (f *UnifiedEqFilter) Reset() { f.prepared = false }

// EvalRendered is the render-path evaluation: resolve the path within one
// rendered canonical document and compare spellings. It serves trivial rows,
// container targets, overflow chains, and rows of non-unified leaves.
func (f *UnifiedEqFilter) EvalRendered(doc []byte) (bool, error) {
	start, end, found, err := f.resolver.PathSpanOf(doc)
	if err != nil || !found {
		return false, err
	}
	return bytes.Equal(doc[start:end], f.needle), nil
}

// prepareLeaf derives the per-leaf token-lane state: one hole resolution per
// template, amortized over the leaf's rows, and one dictionary scan for the
// needle spelling. A dictionary hit turns every
// dict-token compare into a single byte equality; a miss proves no dict token
// can match, because entries store exact canonical spellings.
func (f *UnifiedEqFilter) prepareLeaf(v *CommonPrimaryUnifiedLeafView) {
	f.templateCount = v.templateCount
	for id := 0; id < v.templateCount; id++ {
		hole := f.resolver.Resolve(v, id)
		if hole > 32000 {
			// Unreachable for inline documents (holes < InlineValueBytes), but
			// keeps the int16 cast provably lossless.
			hole = UnifiedHoleContainer
		}
		f.holes[id] = int16(hole)
	}
	f.dict = -1
	for id := 0; id < v.dictionaryCount; id++ {
		entry, ok := v.dictionaryEntry(id)
		if ok && bytes.Equal(entry, f.needle) {
			f.dict = int16(id)
			break
		}
	}
	f.prepared = true
}

// matchBody evaluates one templated/trivial row body from tokens.
// needsRender reports the rows the token lane cannot decide (trivial rows and
// container targets); the caller renders and evaluates those. ok is false
// only on structural corruption (fail closed, never guess).
func (f *UnifiedEqFilter) matchBody(body []byte) (matched, needsRender, ok bool) {
	if len(body) < 2 {
		return false, false, false
	}
	if body[0] == unifiedRowTrivial {
		return false, true, true
	}
	template := int(body[0])
	if template >= f.templateCount {
		return false, false, false
	}
	hole := int(f.holes[template])
	if hole == UnifiedHoleAbsent {
		// Absent from the shape means absent from every row: no row of this
		// template can equal any needle.
		return false, false, true
	}
	if hole < 0 {
		return false, true, true
	}
	cursor := 1
	for {
		if cursor >= len(body) {
			return false, false, false
		}
		tag := body[cursor]
		cursor++
		at := hole == 0
		hole--
		switch {
		case tag < unifiedTokenDictLimit:
			if at {
				// Dictionary ids name exact canonical spellings, so matching
				// canonical spellings, so id equality is spelling equality.
				return int16(tag) == f.dict, false, true
			}
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			if length > len(body)-cursor {
				return false, false, false
			}
			if at {
				return f.needleKind == UnifiedRowTokenLiteral &&
					bytes.Equal(body[cursor:cursor+length], f.needle), false, true
			}
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, lengthOK := readUnifiedTokenUvarint(body[cursor:])
			if !lengthOK || length == 0 || uint64(length) > uint64(len(body)-cursor-n) {
				return false, false, false
			}
			cursor += n
			if at {
				return f.needleKind == UnifiedRowTokenLiteral &&
					bytes.Equal(body[cursor:cursor+int(length)], f.needle), false, true
			}
			cursor += int(length)
		case tag == unifiedTokenTrue:
			if at {
				return f.needleKind == UnifiedRowTokenTrue, false, true
			}
		case tag == unifiedTokenFalse:
			if at {
				return f.needleKind == UnifiedRowTokenFalse, false, true
			}
		case tag == unifiedTokenNull:
			if at {
				return f.needleKind == UnifiedRowTokenNull, false, true
			}
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			if n == 0 {
				return false, false, false
			}
			cursor += n
			if at {
				return f.needleKind == UnifiedRowTokenInt && value == f.needleInt, false, true
			}
		default:
			return false, false, false
		}
	}
}

// UnifiedFilterProgress accumulates one filtered scan's counters: rows
// matched, rows that took the render-then-filter path (the reported fallback
// lane), and rows scanned in total.
type UnifiedFilterProgress struct {
	Matched  int
	Fallback int
	Scanned  int
}

// FilterCountEq drives the filter over the cursor's remaining rows. Unified
// leaves evaluate from tokens; every row the token lane cannot decide renders
// into the cursor's splice scratch and evaluates there. Overflow rows stop the
// drain and return their borrowed key and chain descriptor — exactly the
// VisitInline overflow contract — so the caller can
// resolve the chain, evaluate the document itself, and re-enter; the cursor
// and per-leaf filter state resume where they left off. A nil key with a zero
// PageRef means the scan is complete.
func (c *PrimaryGraphCursor) FilterCountEq(
	f *UnifiedEqFilter, progress *UnifiedFilterProgress,
) ([]byte, PageRef, error) {
	if c == nil || c.done || f == nil || progress == nil {
		return nil, PageRef{}, nil
	}
	for {
		if !f.prepared {
			f.prepareLeaf(&c.unifiedLeaf)
		}
		for {
			key, body, isOverflow, ok := c.rows.NextRawBorrowed()
			if !ok {
				break
			}
			progress.Scanned++
			if isOverflow {
				// Overflow documents are never templated: the caller
				// reassembles the chain and evaluates the rendered bytes.
				progress.Fallback++
				return key, decodePageRef(body), nil
			}
			matched, needsRender, bodyOK := f.matchBody(body)
			if !bodyOK {
				return nil, PageRef{}, fmt.Errorf(
					"%w: unified filter row", ErrCommonPrimaryLeafCorrupt,
				)
			}
			if needsRender {
				progress.Fallback++
				doc := body[1:]
				if body[0] != unifiedRowTrivial {
					c.spliceScratch = c.unifiedLeaf.AppendAdmittedRowBody(
						c.spliceScratch[:0], body,
					)
					doc = c.spliceScratch
				}
				matched, err := f.EvalRendered(doc)
				if err != nil {
					return nil, PageRef{}, err
				}
				if matched {
					progress.Matched++
				}
				continue
			}
			if matched {
				progress.Matched++
			}
		}
		f.prepared = false
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return nil, PageRef{}, err
		}
		if c.done {
			return nil, PageRef{}, nil
		}
	}
}
