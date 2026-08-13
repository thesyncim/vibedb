package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
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
	Key         []byte
	Value       []byte
	ScalarPatch CommonPrimaryUnifiedScalarPatch
	Slot        uint8
}

const commonPrimaryUnifiedScalarPatchValid = uint8(1 << 7)

// CommonPrimaryUnifiedScalarPatch is a compact, opaque admission certificate
// for either an exact admitted templated row or one dictionary-neutral scalar
// replacement in that row. Its six bytes fit the existing tail padding in a
// durable overlay record. canonicalOffset and canonicalLength always name the
// replacement spelling. The other coordinates are leaf-class-specific: a
// unified row records its encoded body token, while a compact stripe records
// the changed hole ordinal and old scalar spelling length. The high bit of
// bodyLength is the validity bit.
//
// Values are intentionally constructible only by an admitted-leaf verifier.
// A zero or damaged value is declined by the strict parallel planner; the
// broader serial planner can still re-enter its complete generic proof.
type CommonPrimaryUnifiedScalarPatch struct {
	bodyOffset      uint16
	canonicalOffset uint16
	bodyLength      uint8
	canonicalLength uint8
}

// RecoveryCanonicalPatch exposes the certificate's canonical splice
// coordinates. The certificate is relative to its admitted immutable leaf;
// callers must independently prove that the journal's immediate predecessor is
// the same preimage before using these coordinates for sequential redo. The
// opaque body coordinates remain private to the admitted-leaf validator.
// rawDelta derives the replaced scalar's old length. Exact (byte-identical)
// certificates deliberately decline.
func (c CommonPrimaryUnifiedScalarPatch) RecoveryCanonicalPatch(
	rawDelta int32,
) (canonicalOffset uint16, oldLength, newLength uint8, ok bool) {
	if !c.valid() || c.exact() || c.canonicalLength == 0 {
		return 0, 0, 0, false
	}
	old := int64(c.canonicalLength) - int64(rawDelta)
	if old <= 0 || old > int64(CanonicalIntMaxDigits+1) {
		return 0, 0, 0, false
	}
	return c.canonicalOffset, uint8(old), c.canonicalLength, true
}

func (c CommonPrimaryUnifiedScalarPatch) valid() bool {
	return c.bodyLength&commonPrimaryUnifiedScalarPatchValid != 0
}

func (c CommonPrimaryUnifiedScalarPatch) exact() bool {
	return c.valid() &&
		c.bodyLength == commonPrimaryUnifiedScalarPatchValid &&
		c.bodyOffset == 0 && c.canonicalOffset == 0 &&
		c.canonicalLength == 0
}

func (c CommonPrimaryUnifiedScalarPatch) oldBodyLength() int {
	return int(c.bodyLength &^ commonPrimaryUnifiedScalarPatchValid)
}

// unifiedScalarPatchValueClass classifies one changed canonical hole in one
// pass. Scalar is exactly the dictionary-neutral int/bool/null set; cost is its
// admitted non-dictionary token width. Keeping both answers together avoids
// reparsing decimal integers for cost, dictionary eligibility, and compact
// certificate admission.
func unifiedScalarPatchValueClass(value []byte) (cost int, scalar bool) {
	switch len(value) {
	case 4:
		if string(value) == "true" || string(value) == "null" {
			return 1, true
		}
	case 5:
		if string(value) == "false" {
			return 1, true
		}
	}
	if integer, ok := CanonicalIntValue(value); ok {
		return 1 + zigzagVarintLen(integer), true
	}
	if len(value) <= unifiedTokenShortMax {
		return 1 + len(value), false
	}
	return 1 + unifiedTokenUvarintLen(uint32(len(value))) + len(value), false
}

func unifiedScalarPatchToken(tag byte) bool {
	return tag == unifiedTokenTrue || tag == unifiedTokenFalse ||
		tag == unifiedTokenNull || tag == unifiedTokenInt
}

func appendUnifiedScalarPatchValue(dst, value []byte) ([]byte, bool) {
	switch {
	case bytes.Equal(value, unifiedPatchTrue):
		return append(dst, unifiedTokenTrue), true
	case bytes.Equal(value, unifiedPatchFalse):
		return append(dst, unifiedTokenFalse), true
	case bytes.Equal(value, unifiedPatchNull):
		return append(dst, unifiedTokenNull), true
	default:
		integer, integerOK := CanonicalIntValue(value)
		if !integerOK {
			return dst, false
		}
		dst = append(dst, unifiedTokenInt)
		return AppendZigzagVarint(dst, integer), true
	}
}

// unifiedPrimaryPatchValueDelta is one dictionary-eligible spelling whose
// frequency a replacement changes. The string is a read-only view into either
// the admitted leaf or the caller's replacement arena, both of which remain
// stable for the complete patch call.
type unifiedPrimaryPatchValueDelta struct {
	value string
	delta int
}

// PatchStableReplacementKeepsExtent is the allocation-free admission
// certificate for a single native class-5 replacement. It proves the same
// local invariants as PatchPlanStableReplacements without copying the page,
// sealing a checksum, or running a leaf-wide dictionary census. The narrower
// result deliberately rejects every changed dictionary-eligible spelling;
// integer, bool, null, and equal-cost shape-stable changes can therefore prove
// that the current physical extent remains sufficient using only the selected
// row.
//
// indexStorage and spanStorage are caller-owned fixed scratch. Insufficient
// capacity declines the certificate rather than allocating or weakening the
// proof.
func (v *CommonPrimaryUnifiedLeafView) PatchStableReplacementKeepsExtent(
	replacement CommonPrimaryUnifiedReplacement,
	indexStorage []vibejson.IndexEntry,
	workspace *CanonicalWorkspace,
	spanStorage []UnifiedTokenSpan,
) (bool, error) {
	if v == nil || len(replacement.Key) == 0 ||
		len(replacement.Value) == 0 || workspace == nil ||
		cap(indexStorage) == 0 || cap(spanStorage) == 0 {
		return false, nil
	}
	newIndex, err := vibejson.BuildIndex(
		replacement.Value, indexStorage[:cap(indexStorage)],
	)
	if errors.Is(err, document.ErrIndexFull) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	canonical, indexed := CanonicalSpanIndexOf(
		newIndex, workspace, spanStorage,
	)
	if !indexed {
		return false, nil
	}
	return v.PatchStableCanonicalReplacementKeepsExtent(
		replacement.Key, replacement.Slot, canonical,
		indexStorage, workspace,
	)
}

// PatchStableCanonicalReplacementScalarPatch derives the compact fold
// certificate directly from the scalar spans already built by concurrent
// canonical admission. It accepts only templated existing-key rows whose new
// value is byte-exact to the admitted base or changes exactly one integer,
// bool, or null hole. Those spellings never participate in the unified
// dictionary, so changing their encoded width cannot invalidate the admitted
// template or dictionary plan.
//
// keepsExtent retains the deliberately stricter mutation-time reservation
// proof used by the overlay: it is true only when both the encoded body cost
// and canonical trivial-content cost are unchanged. The returned patch remains
// useful to a later fold when a width changes; that fold checks the complete
// bucket's aggregate extent before publication. resolved is true whenever the
// admitted templated row was fully checked, even if the update is not compact-
// scalar-certifiable; callers can then use keepsExtent without repeating the
// same key, slot, template, and token walk through the generic admission proof.
func (v *CommonPrimaryUnifiedLeafView) PatchStableCanonicalReplacementScalarPatch(
	key []byte,
	slot uint8,
	canonical CanonicalSpanIndex,
) (
	patch CommonPrimaryUnifiedScalarPatch,
	keepsExtent bool,
	resolved bool,
	err error,
) {
	if v == nil || len(key) == 0 || !canonical.valid ||
		len(canonical.canonical) == 0 {
		return CommonPrimaryUnifiedScalarPatch{}, false, false, nil
	}
	rank := v.env.LowerBound(key)
	if rank >= v.env.Len() {
		return CommonPrimaryUnifiedScalarPatch{}, false, false, nil
	}
	admittedKey, valueStart, valueEnd, boundsOK := v.env.keyBounds(rank)
	if !boundsOK || !bytes.Equal(admittedKey, key) ||
		v.env.rankOverflow(rank) {
		return CommonPrimaryUnifiedScalarPatch{}, false, false, nil
	}
	slots, slotsOK := v.env.rankSlots()
	if !slotsOK || slots[rank] != slot {
		return CommonPrimaryUnifiedScalarPatch{}, false, false, nil
	}
	oldBody := v.env.payload[valueStart:valueEnd:valueEnd]
	if len(oldBody) == 0 {
		return CommonPrimaryUnifiedScalarPatch{}, false, false,
			ErrCommonPrimaryLeafCorrupt
	}
	if oldBody[0] == unifiedRowTrivial {
		return CommonPrimaryUnifiedScalarPatch{}, false, false, nil
	}
	templateID := int(oldBody[0])
	if templateID >= v.templateCount {
		return CommonPrimaryUnifiedScalarPatch{}, false, false,
			ErrCommonPrimaryLeafCorrupt
	}
	spans := canonical.spans
	entry := v.admittedTemplateEntry(templateID)
	if !unifiedMatchesTemplate(canonical.canonical, spans, entry) {
		return CommonPrimaryUnifiedScalarPatch{}, false, true, nil
	}

	cursor := 1
	oldCanonicalBytes := len(entry.static)
	genericStable := true
	scalarEligible := true
	scalarChanged := false
	var integer [24]byte
	for hole := range spans {
		tokenStart := cursor
		oldValue, next, tokenOK := v.patchTokenValue(
			oldBody, cursor, integer[:0],
		)
		if !tokenOK {
			return CommonPrimaryUnifiedScalarPatch{}, false, false,
				ErrCommonPrimaryLeafCorrupt
		}
		cursor = next
		oldCanonicalBytes += len(oldValue)
		span := spans[hole]
		newValue := canonical.canonical[span.Start:span.End]
		if bytes.Equal(oldValue, newValue) {
			continue
		}
		oldTokenCost, oldScalar := unifiedScalarPatchValueClass(oldValue)
		newTokenCost, newScalar := unifiedScalarPatchValueClass(newValue)
		if oldTokenCost != newTokenCost || !oldScalar || !newScalar {
			genericStable = false
		}
		if !scalarEligible {
			if !genericStable {
				return CommonPrimaryUnifiedScalarPatch{}, false, true, nil
			}
			continue
		}
		if scalarChanged || !unifiedScalarPatchToken(oldBody[tokenStart]) ||
			!oldScalar || !newScalar {
			patch = CommonPrimaryUnifiedScalarPatch{}
			scalarEligible = false
			if !genericStable {
				// The original fixed-extent proof declines at this same first
				// unstable spelling. With no compact certificate still possible,
				// the rest of the row cannot change either answer.
				return CommonPrimaryUnifiedScalarPatch{}, false, true, nil
			}
			continue
		}
		oldTokenBytes := next - tokenStart
		canonicalBytes := int(span.End - span.Start)
		if tokenStart > int(^uint16(0)) ||
			int(span.Start) > int(^uint16(0)) ||
			oldTokenBytes <= 0 || oldTokenBytes >=
			int(commonPrimaryUnifiedScalarPatchValid) ||
			canonicalBytes <= 0 || canonicalBytes > int(^uint8(0)) {
			patch = CommonPrimaryUnifiedScalarPatch{}
			scalarEligible = false
			continue
		}
		patch = CommonPrimaryUnifiedScalarPatch{
			bodyOffset:      uint16(tokenStart),
			canonicalOffset: uint16(span.Start),
			bodyLength: commonPrimaryUnifiedScalarPatchValid |
				uint8(oldTokenBytes),
			canonicalLength: uint8(canonicalBytes),
		}
		scalarChanged = true
	}
	if cursor != len(oldBody) {
		return CommonPrimaryUnifiedScalarPatch{}, false, false,
			ErrCommonPrimaryLeafCorrupt
	}
	if scalarEligible && !scalarChanged {
		patch.bodyLength = commonPrimaryUnifiedScalarPatchValid
	}
	canonicalDelta := len(canonical.canonical) - oldCanonicalBytes
	return patch, genericStable && canonicalDelta == 0, true, nil
}

// PatchStableCanonicalReplacementKeepsExtent is the spanned sibling of
// PatchStableReplacementKeepsExtent. canonical must be the opaque certificate
// returned for the exact replacement value by CanonicalSpanIndexOf or
// AppendCanonicalIndexedSpans. It consumes those already-validated spans and
// therefore avoids rebuilding a tape over a newly rendered canonical value.
//
// indexStorage remains fixed scratch for the admitted old trivial row, when
// one exists. The certificate retains the caller's span capacity after its
// new-value prefix for that old-row comparison; insufficient space declines
// rather than allocating.
func (v *CommonPrimaryUnifiedLeafView) PatchStableCanonicalReplacementKeepsExtent(
	key []byte,
	slot uint8,
	canonical CanonicalSpanIndex,
	indexStorage []vibejson.IndexEntry,
	workspace *CanonicalWorkspace,
) (bool, error) {
	if v == nil || len(key) == 0 || !canonical.valid ||
		len(canonical.canonical) == 0 || workspace == nil ||
		cap(indexStorage) == 0 {
		return false, nil
	}
	rank := v.env.LowerBound(key)
	if rank >= v.env.Len() {
		return false, nil
	}
	admittedKey, valueStart, valueEnd, boundsOK := v.env.keyBounds(rank)
	if !boundsOK || !bytes.Equal(admittedKey, key) ||
		v.env.rankOverflow(rank) {
		return false, nil
	}
	slots, slotsOK := v.env.rankSlots()
	if !slotsOK || slots[rank] != slot {
		return false, nil
	}
	buildIndex := func(src []byte) (vibejson.Index, bool, error) {
		index, err := vibejson.BuildIndex(
			src, indexStorage[:cap(indexStorage)],
		)
		if errors.Is(err, document.ErrIndexFull) {
			return vibejson.Index{}, false, nil
		}
		if err != nil {
			return vibejson.Index{}, false, err
		}
		return index, true, nil
	}
	replacementValue := canonical.canonical
	spans := canonical.spans
	newSpanCount := len(spans)
	oldBody := v.env.payload[valueStart:valueEnd:valueEnd]
	if len(oldBody) == 0 {
		return false, ErrCommonPrimaryLeafCorrupt
	}
	changedValueStable := func(oldValue, newValue []byte) bool {
		if bytes.Equal(oldValue, newValue) {
			return true
		}
		return unifiedTypedTokenCost(oldValue) ==
			unifiedTypedTokenCost(newValue) &&
			!unifiedDictionaryEligible(oldValue) &&
			!unifiedDictionaryEligible(newValue)
	}

	if oldBody[0] == unifiedRowTrivial {
		oldCanonical := oldBody[1:]
		if len(oldCanonical) != len(replacementValue) {
			return false, nil
		}
		oldIndex, oldIndexed, indexErr := buildIndex(oldCanonical)
		if indexErr != nil || !oldIndexed {
			return false, indexErr
		}
		if len(oldIndex.Entries) > cap(spans)-newSpanCount {
			return false, nil
		}
		spans = appendHoleSpans(spans, oldIndex)
		newSpans := spans[:newSpanCount]
		oldSpans := spans[newSpanCount:]
		if !unifiedSameStaticShape(
			oldCanonical, oldSpans, replacementValue, newSpans,
		) {
			return false, nil
		}
		oldCost, newCost := 0, 0
		for i := range newSpans {
			oldValue := oldCanonical[oldSpans[i].Start:oldSpans[i].End]
			newValue := replacementValue[newSpans[i].Start:newSpans[i].End]
			if !changedValueStable(oldValue, newValue) {
				return false, nil
			}
			oldCost += unifiedTypedTokenCost(oldValue)
			newCost += unifiedTypedTokenCost(newValue)
		}
		return oldCost == newCost, nil
	}

	templateID := int(oldBody[0])
	if templateID >= v.templateCount {
		return false, ErrCommonPrimaryLeafCorrupt
	}
	newSpans := spans[:newSpanCount]
	entry := v.admittedTemplateEntry(templateID)
	if !unifiedMatchesTemplate(replacementValue, newSpans, entry) {
		return false, nil
	}
	cursor := 1
	oldCanonicalBytes := len(entry.static)
	oldCost, newCost := 0, 0
	var integer [24]byte
	for hole := range newSpans {
		oldValue, next, tokenOK := v.patchTokenValue(
			oldBody, cursor, integer[:0],
		)
		if !tokenOK {
			return false, ErrCommonPrimaryLeafCorrupt
		}
		cursor = next
		newValue := replacementValue[newSpans[hole].Start:newSpans[hole].End]
		if !changedValueStable(oldValue, newValue) {
			return false, nil
		}
		oldCanonicalBytes += len(oldValue)
		oldCost += unifiedTypedTokenCost(oldValue)
		newCost += unifiedTypedTokenCost(newValue)
	}
	return cursor == len(oldBody) &&
		oldCanonicalBytes == len(replacementValue) &&
		oldCost == newCost, nil
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
// It preserves an admitted leaf's template and dictionary sections while
// replacing same-key, same-shape inline row bodies. Equal-width bodies retain
// the original copy-and-patch path and are byte-identical to a complete
// EncodeBest fold for the previously accepted subset, except for the requested
// generation. When an encoded body changes width, the record heap and its
// succinct boundaries are rebuilt inside the same physical extent.
//
// The acceptance certificate is mechanically sound:
//
//   - no insert, delete, overflow, slot change, or shape change;
//   - canonical integers and the one-byte typed spellings are globally
//     dictionary-ineligible, so width-changing integers never alter the
//     admitted dictionary;
//   - if another scalar changes, one exact leaf census plus the complete delta
//     set must reproduce the admitted dictionary candidate order and IDs;
//   - aggregate encoded growth must fit the admitted physical extent.
//
// A variable-width result deliberately preserves the admitted physical plan;
// a fresh EncodeBest fold is allowed to reconsider template amortization or a
// smaller extent and therefore need not be byte-identical. Both images render
// the same canonical rows.
//
// Any input outside that proof returns ok=false without publishing dst. The
// ordinary full planner remains the complete fallback.
func (v *CommonPrimaryUnifiedLeafView) PatchPlanStableReplacements(
	dst []byte,
	header CommonPrimaryLeafHeader,
	replacements []CommonPrimaryUnifiedReplacement,
	builder *UnifiedPrimaryLeafBuilder,
) (page []byte, ok bool, err error) {
	return v.patchPlanStableReplacements(
		dst, header, replacements, builder, false,
	)
}

// PatchPlanScalarReplacements is the bounded parallel-fold sibling of
// PatchPlanStableReplacements. Every replacement must carry a compact scalar
// certificate and every certificate must validate against the admitted leaf.
// A missing or mismatched certificate declines with ok=false; this strict path
// never enters the generic JSON tape or dictionary-census planner.
func (v *CommonPrimaryUnifiedLeafView) PatchPlanScalarReplacements(
	dst []byte,
	header CommonPrimaryLeafHeader,
	replacements []CommonPrimaryUnifiedReplacement,
	builder *UnifiedPrimaryLeafBuilder,
) (page []byte, ok bool, err error) {
	return v.patchPlanStableReplacements(
		dst, header, replacements, builder, true,
	)
}

func (v *CommonPrimaryUnifiedLeafView) patchPlanStableReplacements(
	dst []byte,
	header CommonPrimaryLeafHeader,
	replacements []CommonPrimaryUnifiedReplacement,
	builder *UnifiedPrimaryLeafBuilder,
	strictScalar bool,
) (page []byte, ok bool, err error) {
	// Reset reusable output state before every guard. In particular, a strict
	// call with a missing certificate or undersized destination must not expose
	// heap/row lengths left by the preceding successful bucket.
	if builder != nil {
		builder.patchValues = builder.patchValues[:0]
		builder.heap = builder.heap[:0]
		builder.spans = builder.spans[:0]
		builder.rows = builder.rows[:0]
	}
	if v == nil || builder == nil || len(replacements) == 0 ||
		len(replacements) > v.env.Len() {
		return nil, false, nil
	}
	if strictScalar && (cap(builder.rows) < len(replacements) ||
		cap(builder.heap) == 0) {
		return nil, false, nil
	}
	extent := int(v.env.header.PageSize)
	if len(dst) < extent ||
		commonPrimaryLeafOverlaps(dst[:extent], v.env.page) ||
		header.StoreID != v.env.header.StoreID ||
		header.Bucket != v.env.header.Bucket ||
		header.PageSize != v.env.header.PageSize ||
		header.Generation <= v.env.header.Generation ||
		header.Generation >= uint64(1)<<48 {
		return nil, false, fmt.Errorf("%w: unified native patch identity", ErrInvalidWrite)
	}
	if strictScalar {
		defer func() {
			if ok {
				return
			}
			// A strict decline is reusable scratch, not a partially planned
			// result. Restore all lengths a worker can observe before it hands
			// this bounded context to the next bucket.
			builder.patchValues = builder.patchValues[:0]
			builder.heap = builder.heap[:0]
			builder.spans = builder.spans[:0]
			builder.rows = builder.rows[:0]
		}()
	}
	var seenRanks [4]uint64
	var patchByRank [CommonPrimaryLeafWideSlots]uint16
	canonicalDelta := 0
	bodyDelta := 0
	fixedBodies := true
	slots, slotsOK := v.env.rankSlots()
	if !slotsOK {
		return nil, false, ErrCommonPrimaryLeafCorrupt
	}

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
		if slots[rank] != replacement.Slot {
			return nil, false, nil
		}
		rankBit := uint64(1) << uint(rank&63)
		if seenRanks[rank>>6]&rankBit != 0 {
			return nil, false, nil
		}
		seenRanks[rank>>6] |= rankBit
		oldBody := v.env.payload[valueStart:valueEnd:valueEnd]
		bodyOffset, bodyBytes, oldCanonicalBytes, stable, bodyErr :=
			v.planScalarPatchReplacementBody(
				builder, oldBody, replacement.Value,
				replacement.ScalarPatch, strictScalar,
			)
		if bodyErr == nil && !stable && !strictScalar {
			// A missing or damaged compact certificate is never an error and
			// never weakens validation. Re-enter the complete stable planner,
			// which reparses and re-certifies the replacement from its bytes.
			bodyOffset, bodyBytes, oldCanonicalBytes, stable, bodyErr =
				v.planStableReplacementBody(
					builder, oldBody, replacement.Value,
				)
		}
		if bodyErr != nil {
			return nil, false, bodyErr
		}
		if !stable {
			return nil, false, nil
		}
		if replacement.ScalarPatch.exact() {
			// The certificate validator compared every static segment and token.
			// An exact row needs no heap entry and no copy back into the page; the
			// generation/CRC update below is the complete physical change.
			continue
		}
		if bodyBytes != len(oldBody) {
			fixedBodies = false
		}
		bodyDelta += bodyBytes - len(oldBody)
		canonicalDelta += len(replacement.Value) - oldCanonicalBytes
		patchByRank[rank] = uint16(len(builder.rows) + 1)
		builder.rows = append(builder.rows, unifiedPrimaryLeafRow{
			heapOff: int32(bodyOffset), length: int32(bodyBytes),
			shape: int32(rank),
		})
	}

	// Integer and tag-only changes leave patchValues empty and take no
	// leaf-wide pass. Dictionary-eligible hole changes must reproduce the exact
	// admitted dictionary after their complete coalesced delta set is applied.
	if len(builder.patchValues) != 0 &&
		!v.patchDictionaryStable(builder) {
		return nil, false, nil
	}
	newPayloadBytes := len(v.env.payload) + bodyDelta
	payloadCapacity := extent - PageHeaderSize - PageTrailerSize
	newTrivialBytes := v.trivialBytes + canonicalDelta
	if newPayloadBytes < v.heapStart || newPayloadBytes > payloadCapacity ||
		newTrivialBytes < commonPrimaryUnifiedHeaderBytes ||
		uint64(newTrivialBytes) > uint64(^uint32(0)) ||
		!CommonPrimaryUnifiedTrivialFits(v.env.Len(), newTrivialBytes) {
		return nil, false, nil
	}
	unifiedStart := v.templateDir - commonPrimaryUnifiedHeaderBytes

	if fixedBodies {
		page = dst[:extent]
		copy(page, v.env.page)
		for i := range builder.rows {
			row := &builder.rows[i]
			_, valueStart, valueEnd, boundsOK := v.env.keyBounds(int(row.shape))
			if !boundsOK || valueEnd-valueStart != int(row.length) {
				return nil, false, ErrCommonPrimaryLeafCorrupt
			}
			body := builder.heap[row.heapOff : row.heapOff+row.length]
			copy(page[PageHeaderSize+valueStart:PageHeaderSize+valueEnd], body)
		}
		binary.LittleEndian.PutUint32(
			page[PageHeaderSize+unifiedStart+12:], uint32(newTrivialBytes),
		)
		binary.LittleEndian.PutUint64(page[24:32], header.Generation)
		if _, sealErr := sealInitializedPage(page); sealErr != nil {
			return nil, false, sealErr
		}
		return page, true, nil
	}

	logicalID, _ := CommonPrimaryLeafLogicalID(header.Bucket)
	payload, initErr := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: logicalID, PageSize: uint32(extent),
		PayloadLength: uint32(newPayloadBytes), Kind: commonPrimaryLeafPageKind,
	})
	if initErr != nil {
		return nil, false, initErr
	}
	copy(payload[:v.heapStart], v.env.payload[:v.heapStart])
	layout := v.env.layout
	clear(payload[layout.lowStart:layout.highStart])
	clear(payload[layout.highStart : layout.highStart+layout.highBytes])
	clear(payload[layout.checkpointStart : layout.checkpointStart+layout.checkpointCount*layout.checkpointWidth])
	binary.LittleEndian.PutUint32(
		payload[unifiedStart+12:], uint32(newTrivialBytes),
	)
	cursor, patched := v.heapStart, 0
	for rank := 0; rank < v.env.Len(); rank++ {
		recordStart, recordEnd, boundsOK := v.env.recordBounds(rank)
		if !boundsOK {
			return nil, false, ErrCommonPrimaryLeafCorrupt
		}
		commonPrimaryLeafPutBoundary(payload, &layout, rank, uint16(cursor))
		patchIndex := int(patchByRank[rank]) - 1
		if patchIndex >= 0 {
			_, valueStart, valueEnd, keyOK := v.env.keyBounds(rank)
			if !keyOK || valueEnd != recordEnd {
				return nil, false, ErrCommonPrimaryLeafCorrupt
			}
			cursor += copy(payload[cursor:], v.env.payload[recordStart:valueStart])
			row := &builder.rows[patchIndex]
			body := builder.heap[row.heapOff : row.heapOff+row.length]
			cursor += copy(payload[cursor:], body)
			patched++
		} else {
			cursor += copy(payload[cursor:], v.env.payload[recordStart:recordEnd])
		}
	}
	if cursor != newPayloadBytes || patched != len(builder.rows) {
		return nil, false, ErrCommonPrimaryLeafCorrupt
	}
	commonPrimaryLeafPutBoundary(
		payload, &layout, v.env.Len(), uint16(cursor),
	)
	commonPrimaryLeafBuildCheckpoints(payload, &layout, v.env.Len()+1)
	page = dst[:extent]
	if _, sealErr := sealInitializedPage(page); sealErr != nil {
		return nil, false, sealErr
	}
	return page, true, nil
}

// planScalarPatchReplacementBody validates and consumes the six-byte compact
// certificate without constructing a JSON tape. Validation reconstructs the
// admitted canonical row from its template and encoded tokens: all static
// bytes and unchanged holes must match, and the one named hole must be an
// actual dictionary-neutral scalar replacement at the certified offsets.
func (v *CommonPrimaryUnifiedLeafView) planScalarPatchReplacementBody(
	b *UnifiedPrimaryLeafBuilder,
	oldBody, canonical []byte,
	patch CommonPrimaryUnifiedScalarPatch,
	bounded bool,
) (bodyOffset, bodyBytes, oldCanonicalBytes int, stable bool, err error) {
	if !patch.valid() || len(oldBody) == 0 || len(canonical) == 0 ||
		oldBody[0] == unifiedRowTrivial {
		return 0, 0, 0, false, nil
	}
	templateID := int(oldBody[0])
	if templateID >= v.templateCount {
		return 0, 0, 0, false, ErrCommonPrimaryLeafCorrupt
	}
	entry := v.admittedTemplateEntry(templateID)
	exact := patch.exact()
	bodyCursor := 1
	canonicalCursor := 0
	staticCursor := uint32(0)
	oldCanonicalBytes = len(entry.static)
	changed := false
	var encodedStorage [11]byte
	var encoded []byte
	var integer [24]byte
	for hole := 0; hole <= entry.holes; hole++ {
		staticEnd := binary.LittleEndian.Uint32(entry.ends[hole*4:])
		if staticEnd < staticCursor || int(staticEnd) > len(entry.static) {
			return 0, 0, 0, false, ErrCommonPrimaryLeafCorrupt
		}
		static := entry.static[staticCursor:staticEnd]
		if len(static) > len(canonical)-canonicalCursor ||
			!bytes.Equal(canonical[canonicalCursor:canonicalCursor+len(static)], static) {
			return 0, 0, 0, false, nil
		}
		canonicalCursor += len(static)
		staticCursor = staticEnd
		if hole == entry.holes {
			break
		}

		tokenStart := bodyCursor
		oldValue, next, tokenOK := v.patchTokenValue(
			oldBody, bodyCursor, integer[:0],
		)
		if !tokenOK {
			return 0, 0, 0, false, ErrCommonPrimaryLeafCorrupt
		}
		bodyCursor = next
		oldCanonicalBytes += len(oldValue)
		if !exact && tokenStart == int(patch.bodyOffset) {
			newBytes := int(patch.canonicalLength)
			if changed || next-tokenStart != patch.oldBodyLength() ||
				canonicalCursor != int(patch.canonicalOffset) ||
				newBytes == 0 || newBytes > len(canonical)-canonicalCursor {
				return 0, 0, 0, false, nil
			}
			newValue := canonical[canonicalCursor : canonicalCursor+newBytes]
			var encodedOK bool
			encoded, encodedOK = appendUnifiedScalarPatchValue(
				encodedStorage[:0], newValue,
			)
			if bytes.Equal(oldValue, newValue) ||
				!unifiedScalarPatchToken(oldBody[tokenStart]) ||
				!encodedOK {
				return 0, 0, 0, false, nil
			}
			canonicalCursor += newBytes
			changed = true
			continue
		}
		if len(oldValue) > len(canonical)-canonicalCursor ||
			!bytes.Equal(
				canonical[canonicalCursor:canonicalCursor+len(oldValue)],
				oldValue,
			) {
			return 0, 0, 0, false, nil
		}
		canonicalCursor += len(oldValue)
	}
	if bodyCursor != len(oldBody) || canonicalCursor != len(canonical) ||
		exact == changed || !exact && !changed {
		return 0, 0, 0, false, nil
	}

	bodyOffset = len(b.heap)
	if exact {
		return bodyOffset, len(oldBody), oldCanonicalBytes, true, nil
	}
	oldStart := int(patch.bodyOffset)
	oldEnd := oldStart + patch.oldBodyLength()
	newStart := int(patch.canonicalOffset)
	newEnd := newStart + int(patch.canonicalLength)
	if oldStart < 1 || oldEnd > len(oldBody) ||
		newStart < 0 || newEnd > len(canonical) {
		return 0, 0, 0, false, nil
	}
	if len(encoded) == 0 {
		return 0, 0, 0, false, nil
	}
	newBodyBytes := len(oldBody) - (oldEnd - oldStart) + len(encoded)
	if bounded && newBodyBytes > cap(b.heap)-len(b.heap) {
		return 0, 0, 0, false, nil
	}
	b.heap = append(b.heap, oldBody[:oldStart]...)
	b.heap = append(b.heap, encoded...)
	b.heap = append(b.heap, oldBody[oldEnd:]...)
	return bodyOffset, len(b.heap) - bodyOffset, oldCanonicalBytes, true, nil
}

// planStableReplacementBody appends one new body against the admitted
// template/dictionary while proving its static shape remains unchanged. The
// caller performs the complete leaf-wide dictionary and extent certificates.
func (v *CommonPrimaryUnifiedLeafView) planStableReplacementBody(
	b *UnifiedPrimaryLeafBuilder,
	oldBody, canonical []byte,
) (bodyOffset, bodyBytes, oldCanonicalBytes int, stable bool, err error) {
	if len(oldBody) == 0 || len(canonical) == 0 {
		return 0, 0, 0, false, nil
	}
	index, err := b.buildIndex(canonical)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !IndexIsCanonical(index, &b.ws) {
		return 0, 0, 0, false, nil
	}
	b.spans = appendHoleSpans(b.spans[:0], index)
	newSpans := b.spans
	bodyOffset = len(b.heap)

	if oldBody[0] == unifiedRowTrivial {
		oldCanonical := oldBody[1:]
		oldIndex, indexErr := b.buildIndex(oldCanonical)
		if indexErr != nil {
			return 0, 0, 0, false, indexErr
		}
		oldSpanStart := len(b.spans)
		b.spans = appendHoleSpans(b.spans, oldIndex)
		oldSpans := b.spans[oldSpanStart:]
		if !unifiedSameStaticShape(oldCanonical, oldSpans, canonical, newSpans) {
			return 0, 0, 0, false, nil
		}
		b.heap = append(b.heap, unifiedRowTrivial)
		b.heap = append(b.heap, canonical...)
		// A preserved trivial row contributes no values to the admitted
		// dictionary census, regardless of its new scalar widths.
		return bodyOffset, len(b.heap) - bodyOffset, len(oldCanonical), true, nil
	}

	templateID := int(oldBody[0])
	if templateID >= v.templateCount {
		return 0, 0, 0, false, ErrCommonPrimaryLeafCorrupt
	}
	entry := v.admittedTemplateEntry(templateID)
	if !unifiedMatchesTemplate(canonical, newSpans, entry) {
		return 0, 0, 0, false, nil
	}
	b.heap = append(b.heap, oldBody[0])
	cursor := 1
	oldCanonicalBytes = len(entry.static)
	for hole := range newSpans {
		tokenStart := cursor
		oldValue, oldNext, tokenOK := v.patchTokenValue(
			oldBody, cursor, b.patchInteger[:0],
		)
		if !tokenOK {
			return 0, 0, 0, false, ErrCommonPrimaryLeafCorrupt
		}
		cursor = oldNext
		newValue := canonical[newSpans[hole].Start:newSpans[hole].End]
		oldCanonicalBytes += len(oldValue)
		if !bytes.Equal(oldValue, newValue) {
			b.addPatchValue(oldValue, -1)
			b.addPatchValue(newValue, 1)
			b.heap = v.appendExistingPlanToken(b.heap, newValue)
		} else {
			// The admitted token is already the canonical spelling under the
			// preserved dictionary plan. Copying its exact bytes avoids a second
			// dictionary scan and re-encoding for every unchanged hole.
			b.heap = append(b.heap, oldBody[tokenStart:cursor]...)
		}
	}
	if cursor != len(oldBody) {
		return 0, 0, 0, false, ErrCommonPrimaryLeafCorrupt
	}
	return bodyOffset, len(b.heap) - bodyOffset, oldCanonicalBytes, true, nil
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
