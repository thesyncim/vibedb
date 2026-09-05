package storeio

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/thesyncim/vibejson"
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
// token lane. NewUnifiedEqFilter selects canonical-spelling equality;
// NewUnifiedScalarEqFilter selects the query layer's exact numeric value
// equality as well. A filter is single-consumer and reusable across scans and
// snapshots.
type UnifiedEqFilter struct {
	resolver UnifiedHoleResolver
	needle   []byte
	// needleKind/needleInt classify the needle once against the token grammar
	// so the per-row compare never re-parses it: a canonical-int needle
	// compares as a decoded value, true/false/null compare as tag identity,
	// everything else as a spelling memcmp.
	needleKind UnifiedRowTokenKind
	needleInt  int64
	// numbersByValue selects JSON's exact mathematical number equality rather
	// than canonical-spelling equality. It is opt-in so the original filter's
	// documented spelling contract remains unchanged.
	numbersByValue bool
	needleValueInt bool
	needleIntValue int64
	scanScratch    []byte
	numberIDs      []uint64

	// Per-leaf state, rebuilt by prepareLeaf for each engaged unified leaf.
	prepared      bool
	dict          int16
	dictNumbers   [2]uint64
	templateCount int
	holes         [unifiedMaxTemplates]int16
}

// UnifiedIntegerOrderFilter is the strict storage-native predicate for one
// signed integer ordering over compact FOR streams. Unlike UnifiedEqFilter it
// never renders a row: if any present target stream in the snapshot cannot
// answer exactly, the cursor declines the complete scan and the query layer
// uses its generic executor.
type UnifiedIntegerOrderFilter struct {
	resolver UnifiedHoleResolver
	needle   int64
	op       UnifiedIntegerOrder
}

// UnifiedIntegerIntervalFilter is the reusable state of one normalized
// integer interval predicate over compact FOR streams.
type UnifiedIntegerIntervalFilter struct {
	resolver UnifiedHoleResolver
	interval UnifiedIntegerInterval
}

// NewUnifiedIntegerOrderFilter builds an exact ordered integer filter over a
// unified field path. The caller proves the query literal is an int64 before
// constructing this filter; the storage layer still validates the operation.
func NewUnifiedIntegerOrderFilter(
	path []byte, needle int64, op UnifiedIntegerOrder,
) (*UnifiedIntegerOrderFilter, error) {
	if !validUnifiedIntegerOrder(op) {
		return nil, fmt.Errorf("%w: unified integer order", ErrInvalidWrite)
	}
	f := &UnifiedIntegerOrderFilter{needle: needle, op: op}
	if err := f.resolver.SetPath(path); err != nil {
		return nil, err
	}
	return f, nil
}

// NewUnifiedIntegerIntervalFilter builds an exact normalized interval filter
// over a unified field path. The caller proves the query literals are int64;
// the storage layer still validates the path grammar.
func NewUnifiedIntegerIntervalFilter(
	path []byte, interval UnifiedIntegerInterval,
) (*UnifiedIntegerIntervalFilter, error) {
	f := &UnifiedIntegerIntervalFilter{interval: interval}
	if err := f.resolver.SetPath(path); err != nil {
		return nil, err
	}
	return f, nil
}

// NewUnifiedEqFilter builds a filter for path (the UnifiedHoleResolver
// syntax) and needle, which must be the JSON spelling of one comparand value;
// it is canonicalized here so callers may pass any valid spelling.
func NewUnifiedEqFilter(path, needle []byte) (*UnifiedEqFilter, error) {
	return newUnifiedEqFilter(path, needle, false)
}

// NewUnifiedScalarEqFilter is NewUnifiedEqFilter with exact JSON scalar value
// equality for numbers: 1, 1.0, and 1e0 compare equal without float64
// rounding. String, boolean, and null behavior is identical to the spelling
// filter because canonical form already gives those values one spelling.
func NewUnifiedScalarEqFilter(path, needle []byte) (*UnifiedEqFilter, error) {
	return newUnifiedEqFilter(path, needle, true)
}

func newUnifiedEqFilter(path, needle []byte, numbersByValue bool) (*UnifiedEqFilter, error) {
	f := &UnifiedEqFilter{}
	if err := f.resolver.SetPath(path); err != nil {
		return nil, err
	}
	index, err := f.resolver.buildIndex(needle)
	if err != nil {
		return nil, fmt.Errorf("%w: unified filter needle: %w", ErrInvalidWrite, err)
	}
	_, f.numbersByValue = index.Root().NumberBytes()
	f.numbersByValue = f.numbersByValue && numbersByValue
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
	if f.numbersByValue {
		f.needleIntValue, f.needleValueInt = exactDecimalInt64Value(f.needle)
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
	return f.equalLiteral(doc[start:end]), nil
}

func (f *UnifiedEqFilter) equalLiteral(value []byte) bool {
	if !f.numbersByValue {
		return bytes.Equal(value, f.needle)
	}
	// Stored spellings are validated JSON. A number is the only scalar whose
	// first byte can be '-' or a digit, so this guard prevents handing a string
	// or another literal to JSONNumberEqual's validated-number contract.
	if len(value) == 0 ||
		(value[0] != '-' && (value[0] < '0' || value[0] > '9')) {
		return false
	}
	return vibejson.JSONNumberEqual(value, f.needle)
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
	f.dictNumbers = [2]uint64{}
	for id := 0; id < v.dictionaryCount; id++ {
		entry, ok := v.dictionaryEntry(id)
		if !ok {
			continue
		}
		if f.numbersByValue && f.equalLiteral(entry) {
			f.dictNumbers[id>>6] |= uint64(1) << (id & 63)
		} else if !f.numbersByValue && bytes.Equal(entry, f.needle) {
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
				if f.numbersByValue {
					return f.dictNumbers[tag>>6]&(uint64(1)<<(tag&63)) != 0,
						false, true
				}
				// Dictionary ids name exact canonical spellings, so id equality
				// is spelling equality.
				return int16(tag) == f.dict, false, true
			}
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			if length > len(body)-cursor {
				return false, false, false
			}
			if at {
				return (f.numbersByValue || f.needleKind == UnifiedRowTokenLiteral) &&
					f.equalLiteral(body[cursor:cursor+length]), false, true
			}
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, lengthOK := readUnifiedTokenUvarint(body[cursor:])
			if !lengthOK || length == 0 || uint64(length) > uint64(len(body)-cursor-n) {
				return false, false, false
			}
			cursor += n
			if at {
				return (f.numbersByValue || f.needleKind == UnifiedRowTokenLiteral) &&
					f.equalLiteral(body[cursor:cursor+int(length)]), false, true
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
				if f.needleKind == UnifiedRowTokenInt {
					return value == f.needleInt, false, true
				}
				if f.needleValueInt {
					return value == f.needleIntValue, false, true
				}
				if f.numbersByValue {
					var spelling [20]byte
					return vibejson.JSONNumberEqual(
						strconv.AppendInt(spelling[:0], value, 10), f.needle,
					), false, true
				}
				return false, false, true
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
		if c.row == 0 {
			matched, ok := 0, false
			if f.numbersByValue {
				matched, f.scanScratch, f.numberIDs, ok = c.leaf.CountResolvedNumberEqual(
					&f.resolver, f.needle, f.needleIntValue, f.needleValueInt,
					f.scanScratch, f.numberIDs,
				)
			} else if !f.numbersByValue {
				matched, f.scanScratch, ok = c.leaf.CountResolvedSpellingEqual(
					&f.resolver, f.needle, f.scanScratch,
				)
			}
			if ok {
				progress.Scanned += c.leaf.Len()
				progress.Matched += matched
				c.row = c.leaf.Len()
			}
		}
		for c.row < c.leaf.Len() {
			row := c.row
			if ref, overflow := c.leaf.OverflowRef(row); overflow {
				var keyScratch [CommonPrimaryLeafMaxKeyBytes]byte
				key, ok := c.leaf.AppendKey(keyScratch[:0], row)
				if !ok {
					return nil, PageRef{}, fmt.Errorf(
						"%w: compact filter overflow key", ErrCommonPrimaryLeafCorrupt,
					)
				}
				c.row++
				progress.Scanned++
				progress.Fallback++
				return key, ref, nil
			}
			var ok bool
			c.spliceScratch, ok = c.leaf.AppendValue(c.spliceScratch[:0], row)
			if !ok {
				return nil, PageRef{}, fmt.Errorf(
					"%w: compact filter row", ErrCommonPrimaryLeafCorrupt,
				)
			}
			c.row++
			progress.Scanned++
			progress.Fallback++
			matched, err := f.EvalRendered(c.spliceScratch)
			if err != nil {
				return nil, PageRef{}, err
			}
			if matched {
				progress.Matched++
			}
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return nil, PageRef{}, err
		}
		if c.done {
			return nil, PageRef{}, nil
		}
	}
}

// FilterCountIntegerOrdered scans every compact leaf with an all-or-nothing
// FOR ordering counter. A false result means no count is authoritative: the
// caller must discard progress and execute the original predicate generically.
func (c *PrimaryGraphCursor) FilterCountIntegerOrdered(
	f *UnifiedIntegerOrderFilter, progress *UnifiedFilterProgress,
) (supported bool, err error) {
	if c == nil || f == nil || progress == nil {
		return false, nil
	}
	if c.done {
		return true, nil
	}
	for {
		if c.row == 0 {
			matched, ok := c.leaf.CountResolvedIntegerOrdered(
				&f.resolver, f.needle, f.op,
			)
			if !ok {
				return false, nil
			}
			progress.Scanned += c.leaf.Len()
			progress.Matched += matched
			c.row = c.leaf.Len()
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return false, err
		}
		if c.done {
			return true, nil
		}
	}
}

// FilterCountIntegerInterval scans every compact leaf with an all-or-nothing
// FOR interval counter. A false result means no count is authoritative: the
// caller must discard progress and execute the original predicate generically.
func (c *PrimaryGraphCursor) FilterCountIntegerInterval(
	f *UnifiedIntegerIntervalFilter,
	progress *UnifiedFilterProgress,
) (supported bool, err error) {
	if c == nil || f == nil || progress == nil {
		return false, nil
	}
	if c.done {
		return true, nil
	}
	for {
		if c.row == 0 {
			matched, ok := c.leaf.CountResolvedIntegerInterval(
				&f.resolver, f.interval,
			)
			if !ok {
				return false, nil
			}
			progress.Scanned += c.leaf.Len()
			progress.Matched += matched
			c.row = c.leaf.Len()
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return false, err
		}
		if c.done {
			return true, nil
		}
	}
}
