package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	unifiedPatchTrue  = []byte("true")
	unifiedPatchFalse = []byte("false")
	unifiedPatchNull  = []byte("null")
)

// CommonPrimaryUnifiedReplacement is one final same-key overlay value offered
// to the native class-5 checkpoint patcher. Key and Value are borrowed for the
// call; Slot is the posting-stable slot already owned by the key.
type CommonPrimaryUnifiedReplacement struct {
	Key   []byte
	Value []byte
	Slot  uint8
}

// unifiedPrimaryPatchValueDelta is one dictionary-eligible spelling whose
// frequency a replacement changes. The string is a read-only view into either
// the admitted leaf or the caller's replacement arena, both of which remain
// stable for the complete patch call.
type unifiedPrimaryPatchValueDelta struct {
	value string
	delta int
}

func (b *UnifiedPrimaryLeafBuilder) addPatchValue(value []byte, delta int) {
	if !unifiedDictionaryEligible(value) {
		return
	}
	spelling := byteview.String(value)
	for i := range b.patchValues {
		if b.patchValues[i].value == spelling {
			b.patchValues[i].delta += delta
			return
		}
	}
	b.patchValues = append(b.patchValues, unifiedPrimaryPatchValueDelta{
		value: spelling, delta: delta,
	})
}

// PatchPlanStableReplacements is the native class-5 checkpoint fast path.
// It copies an admitted page and rewrites only same-key, same-shape row bodies.
// The returned image is byte-identical to a complete EncodeBest fold for the
// accepted subset, except for the requested generation.
//
// The acceptance certificate is mechanically sound:
//
//   - no insert, delete, overflow, slot change, shape change, row-length change,
//     or encoded-body-length change;
//   - every changed hole has equal dictionary-free typed token cost;
//   - the template census saving is therefore unchanged for every shape;
//   - canonical integers and the one-byte typed spellings are globally
//     dictionary-ineligible, so their common update path is a local proof;
//   - if another scalar changes, one exact leaf census plus the complete delta
//     set must reproduce the admitted dictionary candidate order and IDs;
//   - content bytes and row count are unchanged, hence the smallest winning
//     extent remains this extent.
//
// Any input outside that proof returns ok=false without publishing dst. The
// ordinary full planner remains the complete fallback.
func (v *CommonPrimaryUnifiedLeafView) PatchPlanStableReplacements(
	dst []byte,
	header CommonPrimaryLeafHeader,
	replacements []CommonPrimaryUnifiedReplacement,
	builder *UnifiedPrimaryLeafBuilder,
) (page []byte, ok bool, err error) {
	if v == nil || builder == nil || len(replacements) == 0 {
		return nil, false, nil
	}
	extent := int(v.env.header.PageSize)
	if len(dst) < extent ||
		header.StoreID != v.env.header.StoreID ||
		header.Bucket != v.env.header.Bucket ||
		header.PageSize != v.env.header.PageSize ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 {
		return nil, false, fmt.Errorf("%w: unified native patch identity", ErrInvalidWrite)
	}
	builder.patchValues = builder.patchValues[:0]
	page = dst[:extent]
	copy(page, v.env.page)

	for i := range replacements {
		replacement := &replacements[i]
		rank := v.env.LowerBound(replacement.Key)
		if rank >= v.env.Len() {
			return nil, false, nil
		}
		key, valueStart, valueEnd, boundsOK := v.env.keyBounds(rank)
		if !boundsOK || !bytes.Equal(key, replacement.Key) ||
			v.env.rankOverflow(rank) {
			return nil, false, nil
		}
		slots, slotsOK := v.env.rankSlots()
		if !slotsOK || slots[rank] != replacement.Slot {
			return nil, false, nil
		}
		oldBody := v.env.payload[valueStart:valueEnd:valueEnd]
		newBody, oldCanonicalBytes, stable, bodyErr :=
			v.planFixedReplacementBody(builder, oldBody, replacement.Value)
		if bodyErr != nil {
			return nil, false, bodyErr
		}
		if !stable || oldCanonicalBytes != len(replacement.Value) ||
			len(newBody) != len(oldBody) {
			return nil, false, nil
		}
		copy(page[PageHeaderSize+valueStart:PageHeaderSize+valueEnd], newBody)
	}

	// Integer and tag-only changes leave patchValues empty and take no
	// leaf-wide pass. Other scalar changes must reproduce the exact admitted
	// dictionary after their complete coalesced delta set is applied.
	if len(builder.patchValues) != 0 &&
		!v.patchDictionaryStable(builder) {
		return nil, false, nil
	}

	binary.LittleEndian.PutUint64(page[24:32], header.Generation)
	if _, sealErr := sealInitializedPage(page); sealErr != nil {
		return nil, false, sealErr
	}
	return page, true, nil
}

// planFixedReplacementBody derives one new body against the admitted
// template/dictionary while proving the template-census operands stay equal.
func (v *CommonPrimaryUnifiedLeafView) planFixedReplacementBody(
	b *UnifiedPrimaryLeafBuilder,
	oldBody, canonical []byte,
) ([]byte, int, bool, error) {
	if len(oldBody) == 0 || len(canonical) == 0 {
		return nil, 0, false, nil
	}
	index, err := b.buildIndex(canonical)
	if err != nil {
		return nil, 0, false, err
	}
	if !IndexIsCanonical(index, &b.ws) {
		return nil, 0, false, nil
	}
	b.spans = appendHoleSpans(b.spans[:0], index)
	newSpans := b.spans

	if oldBody[0] == unifiedRowTrivial {
		oldCanonical := oldBody[1:]
		oldIndex, indexErr := b.buildIndex(oldCanonical)
		if indexErr != nil {
			return nil, 0, false, indexErr
		}
		oldSpanStart := len(b.spans)
		b.spans = appendHoleSpans(b.spans, oldIndex)
		oldSpans := b.spans[oldSpanStart:]
		if !unifiedSameStaticShape(oldCanonical, oldSpans, canonical, newSpans) {
			return nil, 0, false, nil
		}
		oldCost, newCost := 0, 0
		for i := range newSpans {
			oldCost += unifiedTypedTokenCost(
				oldCanonical[oldSpans[i].Start:oldSpans[i].End],
			)
			newCost += unifiedTypedTokenCost(
				canonical[newSpans[i].Start:newSpans[i].End],
			)
		}
		if oldCost != newCost {
			return nil, 0, false, nil
		}
		b.heap = append(b.heap[:0], unifiedRowTrivial)
		b.heap = append(b.heap, canonical...)
		// A trivial row contributes no values to the dictionary census. Shape,
		// canonical length, and no-dictionary token cost equality preserve its
		// template admission decision.
		return b.heap, len(oldCanonical), true, nil
	}

	templateID := int(oldBody[0])
	if templateID >= v.templateCount {
		return nil, 0, false, ErrCommonPrimaryLeafCorrupt
	}
	entry := v.admittedTemplateEntry(templateID)
	if !unifiedMatchesTemplate(canonical, newSpans, entry) {
		return nil, 0, false, nil
	}
	b.heap = append(b.heap[:0], oldBody[0])
	cursor := 1
	oldCanonicalBytes := len(entry.static)
	oldTokenCost, newTokenCost := 0, 0
	for hole := range newSpans {
		oldValue, oldNext, tokenOK := v.patchTokenValue(
			oldBody, cursor, b.patchInteger[:0],
		)
		if !tokenOK {
			return nil, 0, false, ErrCommonPrimaryLeafCorrupt
		}
		cursor = oldNext
		newValue := canonical[newSpans[hole].Start:newSpans[hole].End]
		oldCanonicalBytes += len(oldValue)
		oldTokenCost += unifiedTypedTokenCost(oldValue)
		newTokenCost += unifiedTypedTokenCost(newValue)
		if !bytes.Equal(oldValue, newValue) {
			if unifiedTypedTokenCost(oldValue) !=
				unifiedTypedTokenCost(newValue) {
				return nil, 0, false, nil
			}
			b.addPatchValue(oldValue, -1)
			b.addPatchValue(newValue, 1)
		}
		b.heap = v.appendExistingPlanToken(b.heap, newValue)
	}
	if cursor != len(oldBody) || oldTokenCost != newTokenCost {
		return nil, 0, false, nil
	}
	return b.heap, oldCanonicalBytes, true, nil
}

func unifiedSameStaticShape(
	a []byte, aSpans []UnifiedTokenSpan,
	b []byte, bSpans []UnifiedTokenSpan,
) bool {
	if len(aSpans) != len(bSpans) {
		return false
	}
	aPrevious, bPrevious := uint32(0), uint32(0)
	for i := 0; i <= len(aSpans); i++ {
		aEnd, bEnd := uint32(len(a)), uint32(len(b))
		if i < len(aSpans) {
			aEnd, bEnd = aSpans[i].Start, bSpans[i].Start
		}
		if !bytes.Equal(a[aPrevious:aEnd], b[bPrevious:bEnd]) {
			return false
		}
		if i < len(aSpans) {
			aPrevious, bPrevious = aSpans[i].End, bSpans[i].End
		}
	}
	return true
}

func unifiedMatchesTemplate(
	canonical []byte,
	spans []UnifiedTokenSpan,
	entry unifiedTemplateView,
) bool {
	if len(spans) != entry.holes {
		return false
	}
	canonicalPrevious, staticPrevious := uint32(0), uint32(0)
	for i := 0; i <= len(spans); i++ {
		canonicalEnd := uint32(len(canonical))
		if i < len(spans) {
			canonicalEnd = spans[i].Start
		}
		staticEnd := binary.LittleEndian.Uint32(entry.ends[i*4:])
		if !bytes.Equal(
			canonical[canonicalPrevious:canonicalEnd],
			entry.static[staticPrevious:staticEnd],
		) {
			return false
		}
		if i < len(spans) {
			canonicalPrevious = spans[i].End
		}
		staticPrevious = staticEnd
	}
	return true
}

// patchTokenValue returns one admitted token's canonical spelling.
func (v *CommonPrimaryUnifiedLeafView) patchTokenValue(
	body []byte, cursor int, integer []byte,
) ([]byte, int, bool) {
	if cursor >= len(body) {
		return nil, cursor, false
	}
	tag := body[cursor]
	cursor++
	switch {
	case tag < unifiedTokenDictLimit:
		return v.admittedDictionaryEntry(int(tag)), cursor, true
	case tag >= unifiedTokenShortBase &&
		tag < unifiedTokenShortBase+unifiedTokenShortMax:
		length := int(tag-unifiedTokenShortBase) + 1
		if length > len(body)-cursor {
			return nil, cursor, false
		}
		return body[cursor : cursor+length : cursor+length],
			cursor + length, true
	case tag == unifiedTokenLongLiteral:
		length, n, lengthOK := readUnifiedTokenUvarint(body[cursor:])
		if !lengthOK || int(length) > len(body)-cursor-n {
			return nil, cursor, false
		}
		cursor += n
		return body[cursor : cursor+int(length) : cursor+int(length)],
			cursor + int(length), true
	case tag == unifiedTokenTrue:
		return unifiedPatchTrue, cursor, true
	case tag == unifiedTokenFalse:
		return unifiedPatchFalse, cursor, true
	case tag == unifiedTokenNull:
		return unifiedPatchNull, cursor, true
	case tag == unifiedTokenInt:
		value, n := DecodeZigzagVarint(body[cursor:])
		if n == 0 {
			return nil, cursor, false
		}
		return AppendCanonicalInt(integer[:0], value), cursor + n, true
	default:
		return nil, cursor, false
	}
}

func (v *CommonPrimaryUnifiedLeafView) appendExistingPlanToken(
	dst, value []byte,
) []byte {
	for id := 0; id < v.dictionaryCount; id++ {
		if bytes.Equal(value, v.admittedDictionaryEntry(id)) {
			return append(dst, byte(id))
		}
	}
	switch len(value) {
	case 4:
		if string(value) == "true" {
			return append(dst, unifiedTokenTrue)
		}
		if string(value) == "null" {
			return append(dst, unifiedTokenNull)
		}
	case 5:
		if string(value) == "false" {
			return append(dst, unifiedTokenFalse)
		}
	}
	if integer, intOK := CanonicalIntValue(value); intOK {
		dst = append(dst, unifiedTokenInt)
		return AppendZigzagVarint(dst, integer)
	}
	if len(value) <= unifiedTokenShortMax {
		dst = append(dst, unifiedTokenShortBase+byte(len(value)-1))
		return append(dst, value...)
	}
	dst = append(dst, unifiedTokenLongLiteral)
	var lengthBytes [5]byte
	n := putUnifiedTokenUvarint(lengthBytes[:], uint32(len(value)))
	dst = append(dst, lengthBytes[:n]...)
	return append(dst, value...)
}

// patchDictionaryStable reconstructs the exact after-patch dictionary plan.
// It runs only when a dictionary-eligible scalar changed; integer, bool and
// null updates never call it. The scan counts every eligible templated hole,
// applies all coalesced deltas, and then invokes the full planner's own
// candidate selection routine. Exact entry-order equality proves candidate
// membership, ranking, IDs and encoded section bytes all remain unchanged.
func (v *CommonPrimaryUnifiedLeafView) patchDictionaryStable(
	b *UnifiedPrimaryLeafBuilder,
) bool {
	if b.counts == nil {
		b.counts = make(map[string]int, 128)
	} else {
		clear(b.counts)
	}
	for rank := 0; rank < v.env.Len(); rank++ {
		_, body, overflow, rowOK := v.RowRawAt(rank)
		if !rowOK {
			return false
		}
		if overflow || body[0] == unifiedRowTrivial {
			continue
		}
		entry := v.admittedTemplateEntry(int(body[0]))
		cursor := 1
		for range entry.holes {
			value, next, tokenOK := v.patchTokenValue(
				body, cursor, b.patchInteger[:0],
			)
			if !tokenOK {
				return false
			}
			cursor = next
			if unifiedDictionaryEligible(value) {
				b.counts[byteview.String(value)]++
			}
		}
		if cursor != len(body) {
			return false
		}
	}
	for _, delta := range b.patchValues {
		after := b.counts[delta.value] + delta.delta
		if after < 0 {
			return false
		}
		if after == 0 {
			delete(b.counts, delta.value)
		} else {
			b.counts[delta.value] = after
		}
	}
	b.selectUnifiedDictionary()
	if len(b.dictionary) != v.dictionaryCount {
		return false
	}
	for id := range b.dictionary {
		if !bytes.Equal(
			byteview.Bytes(b.dictionary[id].value),
			v.admittedDictionaryEntry(id),
		) {
			return false
		}
	}
	return true
}
