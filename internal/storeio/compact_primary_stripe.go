package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
)

const (
	CompactPrimaryStripeMaxRows = 4 << 10
	compactPrimaryHeaderBytes   = 40
	compactPrimaryShapeHeader   = 16
	compactPrimaryMagic         = "VCS1"
	compactPrimaryHasOverflow   = 1 << 0
)

type compactPrimaryBuildScratch struct {
	payload      []byte
	shapeOrder   []uint16
	shapeEnds    []uint16
	shapeCodes   []byte
	streamValues [][]byte
	counts       []uint16
	overflow     []byte
	stream       compactStreamScratch
	patchHeap    []byte
	patchEnds    []uint32
	patchValues  [][]byte
	patchMods    []compactPrimaryReplacementPatch
	patchGroups  []compactPrimaryStreamPatch
	patchStreams []byte
	shapeDeltas  []int
}

type compactPrimaryReplacementPatch struct {
	shape   uint16
	hole    uint16
	ordinal uint16
	value   []byte
}

type compactPrimaryStreamPatch struct {
	shape    uint16
	hole     uint16
	offset   int
	oldBytes int
	newStart int
	newEnd   int
}

// BuildCompactPrimaryStripePayload builds the replacement class-6 VCS1 payload.
// It is exposed only inside storeio so the graph planner can measure a prefix
// before reserving its exact 4 KiB-rounded extent. Records are borrowed; the
// returned payload is borrowed from builder until its next compact build.
func BuildCompactPrimaryStripePayload(
	records []CommonPrimaryLeafRecord,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if err := prepareCompactPrimaryStripe(records, builder); err != nil {
		return nil, err
	}
	return buildPreparedCompactPrimaryStripePayload(records, builder)
}

func prepareCompactPrimaryStripe(
	records []CommonPrimaryLeafRecord,
	builder *UnifiedPrimaryLeafBuilder,
) error {
	if builder == nil || len(records) > CompactPrimaryStripeMaxRows {
		return fmt.Errorf("%w: compact stripe input", ErrInvalidWrite)
	}
	for row := range records {
		if len(records[row].Key) == 0 || len(records[row].Key) > CommonPrimaryLeafMaxKeyBytes ||
			(records[row].Value.IsOverflow() == (len(records[row].Value.Inline) != 0)) ||
			row != 0 && bytes.Compare(records[row-1].Key, records[row].Key) >= 0 {
			return fmt.Errorf("%w: compact stripe record", ErrInvalidWrite)
		}
	}
	if err := builder.extract(records); err != nil {
		return err
	}
	return nil
}

func prepareCompactPrimaryGraphStripe(
	records []PrimaryGraphRecord,
	placed bool,
	builder *UnifiedPrimaryLeafBuilder,
) error {
	if builder == nil || len(records) > CompactPrimaryStripeMaxRows {
		return fmt.Errorf("%w: compact graph stripe input", ErrInvalidWrite)
	}
	return builder.extractPrimaryGraph(records, placed)
}

func buildPreparedCompactPrimaryStripePayload(
	records []CommonPrimaryLeafRecord,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if builder == nil || builder.graphRecords != nil ||
		len(records) > len(builder.records) ||
		len(records) > CompactPrimaryStripeMaxRows {
		return nil, fmt.Errorf("%w: prepared compact stripe input", ErrInvalidWrite)
	}
	return buildPreparedCompactPrimaryStripePayloadRows(len(records), builder)
}

func buildPreparedCompactPrimaryGraphStripePayload(
	records []PrimaryGraphRecord,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if builder == nil || builder.graphRecords == nil ||
		len(records) > len(builder.graphRecords) ||
		len(records) > CompactPrimaryStripeMaxRows {
		return nil, fmt.Errorf("%w: prepared compact graph stripe input", ErrInvalidWrite)
	}
	return buildPreparedCompactPrimaryStripePayloadRows(len(records), builder)
}

func buildPreparedCompactPrimaryStripePayloadRows(
	rowCount int,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	shapeCount := 0
	for row := range rowCount {
		shape := int(builder.rows[row].shape)
		if shape >= 0 {
			shapeCount = max(shapeCount, shape+1)
		}
	}
	if shapeCount > rowCount ||
		shapeCount > len(builder.shapes) || shapeCount > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: compact stripe shapes", ErrInvalidWrite)
	}
	overflowCount := 0
	for row := range rowCount {
		if _, overflow := builder.overflowAt(row); overflow {
			overflowCount++
		}
	}
	hasOverflow := overflowCount != 0
	shapeWidth := 0
	if hasOverflow {
		shapeWidth = bits.Len(uint(shapeCount))
	} else if shapeCount != 0 {
		shapeWidth = bits.Len(uint(shapeCount - 1))
	}
	shapeCodeBytes := (rowCount*shapeWidth + 7) / 8
	restarts := (rowCount + compactStreamRestart - 1) / compactStreamRestart
	rankBytes64 := uint64(restarts) * uint64(shapeCount) * 2
	if rankBytes64 > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: compact stripe rank checkpoints", ErrInvalidWrite)
	}
	rankBytes := int(rankBytes64)

	scratch := &builder.compact
	scratch.counts = slices.Grow(scratch.counts[:0], shapeCount)[:shapeCount]
	clear(scratch.counts)
	counts := scratch.counts
	scratch.shapeCodes = slices.Grow(scratch.shapeCodes[:0], shapeCodeBytes)[:shapeCodeBytes]
	clear(scratch.shapeCodes)
	shapeCodes := scratch.shapeCodes
	for row := range rowCount {
		shape := int(builder.rows[row].shape)
		if shape < 0 {
			if _, overflow := builder.overflowAt(row); !overflow {
				return nil, fmt.Errorf("%w: compact stripe row shape", ErrInvalidWrite)
			}
			compactPutBits(shapeCodes, row*shapeWidth, shapeWidth, uint64(shapeCount))
			continue
		}
		if shape >= shapeCount {
			return nil, fmt.Errorf("%w: compact stripe row shape", ErrInvalidWrite)
		}
		compactPutBits(shapeCodes, row*shapeWidth, shapeWidth, uint64(shape))
		counts[shape]++
	}
	scratch.shapeEnds = slices.Grow(scratch.shapeEnds[:0], shapeCount)[:shapeCount]
	shapeEnds := scratch.shapeEnds
	rowsWithShape := uint16(0)
	for shape := range shapeCount {
		rowsWithShape += counts[shape]
		shapeEnds[shape] = rowsWithShape
	}
	scratch.shapeOrder = slices.Grow(
		scratch.shapeOrder[:0], int(rowsWithShape),
	)[:rowsWithShape]
	shapeOrder := scratch.shapeOrder
	for shape := range shapeCount {
		if shape == 0 {
			counts[shape] = 0
		} else {
			counts[shape] = shapeEnds[shape-1]
		}
	}
	for row := range rowCount {
		shape := int(builder.rows[row].shape)
		if shape >= 0 {
			shapeOrder[counts[shape]] = uint16(row)
			counts[shape]++
		}
	}

	scratch.streamValues = slices.Grow(
		scratch.streamValues[:0], rowCount,
	)[:rowCount]
	keys := scratch.streamValues
	for row := range rowCount {
		keys[row] = builder.keyAt(row)
	}

	headerBytes := compactPrimaryHeaderBytes + 4*shapeCount
	scratch.payload = slices.Grow(scratch.payload[:0], headerBytes)[:headerBytes]
	clear(scratch.payload)
	payload := scratch.payload
	copy(payload, compactPrimaryMagic)
	binary.LittleEndian.PutUint32(payload[4:], uint32(rowCount))
	binary.LittleEndian.PutUint16(payload[8:], uint16(shapeCount))
	payload[10] = byte(shapeWidth)
	if hasOverflow {
		payload[11] = compactPrimaryHasOverflow
	}
	keyStart := len(payload)
	keyEncoding := scratch.stream.encode(keys)
	var err error
	payload, err = keyEncoding.appendBinary(payload)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[12:], uint32(len(payload)-keyStart))

	slotStart := len(payload)
	if rowCount <= CommonPrimaryLeafWideSlots {
		anyNonZero := false
		for row := range rowCount {
			anyNonZero = anyNonZero || builder.slotAt(row) != 0
		}
		writeSlots := false
		if anyNonZero {
			for row := range rowCount {
				if builder.slotAt(row) != uint8(row) {
					writeSlots = true
					break
				}
			}
		}
		if writeSlots {
			var used [4]uint64
			for row := range rowCount {
				slot := builder.slotAt(row)
				bit := uint64(1) << uint(slot&63)
				if used[slot>>6]&bit != 0 {
					return nil, fmt.Errorf("%w: compact stripe duplicate slot", ErrInvalidWrite)
				}
				used[slot>>6] |= bit
				payload = append(payload, slot)
			}
		}
	}
	binary.LittleEndian.PutUint32(payload[16:], uint32(shapeCodeBytes))
	binary.LittleEndian.PutUint32(payload[20:], uint32(rankBytes))
	binary.LittleEndian.PutUint32(payload[28:], uint32(len(payload)-slotStart))
	if hasOverflow {
		bitmapBytes := (rowCount + 7) / 8
		scratch.overflow = slices.Grow(scratch.overflow[:0], bitmapBytes)[:bitmapBytes]
		clear(scratch.overflow)
		for row := range rowCount {
			if _, overflow := builder.overflowAt(row); overflow {
				scratch.overflow[row>>3] |= byte(1) << uint(row&7)
			}
		}
		payload = append(payload, scratch.overflow...)
		for row := range rowCount {
			if ref, overflow := builder.overflowAt(row); overflow {
				start := len(payload)
				payload = append(payload, make([]byte, PageRefSize)...)
				encodePageRef(payload[start:], ref)
			}
		}
	}
	payload = append(payload, shapeCodes...)

	clear(scratch.counts)
	for block := 0; block < restarts; block++ {
		for shape := range shapeCount {
			var encoded [2]byte
			binary.LittleEndian.PutUint16(encoded[:], counts[shape])
			payload = append(payload, encoded[:]...)
		}
		first := block * compactStreamRestart
		last := min(first+compactStreamRestart, rowCount)
		for row := first; row < last; row++ {
			shape := int(compactReadBits(shapeCodes, row*shapeWidth, shapeWidth))
			if shape < shapeCount {
				counts[shape]++
			}
		}
	}

	shapeDataStart := len(payload)
	for shape := range shapeCount {
		shapeStart := uint16(0)
		if shape != 0 {
			shapeStart = shapeEnds[shape-1]
		}
		shapeRows := shapeOrder[shapeStart:shapeEnds[shape]]
		entryStart := len(payload)
		payload = append(payload, make([]byte, compactPrimaryShapeHeader)...)
		plan := &builder.shapes[shape]
		binary.LittleEndian.PutUint32(payload[entryStart:], uint32(len(shapeRows)))
		binary.LittleEndian.PutUint16(payload[entryStart+4:], uint16(plan.holes))

		templateStart := len(payload)
		payload = appendCompactPrimaryTemplate(payload, builder, plan)
		templateBytes := len(payload) - templateStart
		if templateBytes != plan.entryBytes {
			return nil, fmt.Errorf("%w: compact stripe template drift", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint32(payload[entryStart+8:], uint32(templateBytes))

		streamsStart := len(payload)
		for _, encodedRow := range shapeRows {
			rowIndex := int(encodedRow)
			row := &builder.rows[rowIndex]
			if int(row.spanEnd-row.spanStart) != plan.holes {
				return nil, fmt.Errorf("%w: compact stripe hole count", ErrInvalidWrite)
			}
		}
		scratch.streamValues = slices.Grow(
			scratch.streamValues[:0], len(shapeRows),
		)[:len(shapeRows)]
		for hole := range plan.holes {
			values := scratch.streamValues
			for ordinal, encodedRow := range shapeRows {
				rowIndex := int(encodedRow)
				row := &builder.rows[rowIndex]
				span := builder.spans[int(row.spanStart)+hole]
				canonical := builder.canonicalOf(rowIndex)
				values[ordinal] = canonical[span.Start:span.End]
			}
			stream := scratch.stream.encodeShape(values, shapeRows, rowCount)
			payload, err = stream.appendBinary(payload)
			if err != nil {
				return nil, err
			}
		}
		binary.LittleEndian.PutUint32(
			payload[entryStart+12:], uint32(len(payload)-streamsStart),
		)
		binary.LittleEndian.PutUint32(
			payload[compactPrimaryHeaderBytes+shape*4:], uint32(len(payload)-shapeDataStart),
		)
	}
	shapeBytes := len(payload) - shapeDataStart
	binary.LittleEndian.PutUint32(payload[24:], uint32(shapeBytes))
	summaryStart := len(payload)
	payload, err = appendPreparedCompactPrimarySummaries(payload, builder, rowCount)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint16(
		payload[32:], uint16(len(builder.compactSummaryPointers)),
	)
	binary.LittleEndian.PutUint32(payload[36:], uint32(len(payload)-summaryStart))
	scratch.payload = payload
	return payload, nil
}

func appendCompactPrimaryTemplate(
	dst []byte,
	builder *UnifiedPrimaryLeafBuilder,
	shape *unifiedPrimaryLeafShape,
) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 8+(shape.holes+1)*4)...)
	binary.LittleEndian.PutUint16(dst[start:], uint16(shape.holes))
	binary.LittleEndian.PutUint32(dst[start+4:], uint32(shape.staticBytes))
	row := shape.firstRow
	canonical := builder.canonicalOf(row)
	spans := builder.spans[builder.rows[row].spanStart:builder.rows[row].spanEnd]
	previous, written := uint32(0), 0
	for segment := 0; segment <= len(spans); segment++ {
		end := uint32(len(canonical))
		if segment < len(spans) {
			end = spans[segment].Start
		}
		dst = append(dst, canonical[previous:end]...)
		written += int(end - previous)
		binary.LittleEndian.PutUint32(dst[start+8+segment*4:], uint32(written))
		if segment < len(spans) {
			previous = spans[segment].End
		}
	}
	return dst
}

func EncodeCompactPrimaryStripe(
	dst []byte,
	header CommonPrimaryLeafHeader,
	records []CommonPrimaryLeafRecord,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	payloadBytes, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		return nil, err
	}
	return encodeCompactPrimaryStripePayload(dst, header, payloadBytes)
}

func encodeCompactPrimaryGraphStripe(
	dst []byte,
	header CommonPrimaryLeafHeader,
	records []PrimaryGraphRecord,
	placed bool,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if err := prepareCompactPrimaryGraphStripe(records, placed, builder); err != nil {
		return nil, err
	}
	payloadBytes, err := buildPreparedCompactPrimaryGraphStripePayload(records, builder)
	if err != nil {
		return nil, err
	}
	return encodeCompactPrimaryStripePayload(dst, header, payloadBytes)
}

func encodeCompactPrimaryStripePayload(
	dst []byte,
	header CommonPrimaryLeafHeader,
	payloadBytes []byte,
) ([]byte, error) {
	extent := int(header.PageSize)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(header.Bucket)
	if !logicalOK || header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		extent < int(physicalPageQuantum) || extent%int(physicalPageQuantum) != 0 ||
		extent > int(MaxPhysicalPageSize) || len(dst) < extent ||
		len(payloadBytes) > extent-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: compact stripe page", ErrInvalidWrite)
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: logicalID, PageSize: header.PageSize,
		PayloadLength: uint32(len(payloadBytes)), Kind: PagePrimaryLeaf,
	})
	if err != nil {
		return nil, err
	}
	copy(payload, payloadBytes)
	page := dst[:extent]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// EncodeBestCompactPrimaryStripe encodes a complete mutation/checkpoint row
// set at its smallest 4 KiB-rounded extent.
func EncodeBestCompactPrimaryStripe(
	dst []byte,
	header CommonPrimaryLeafHeader,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if builder == nil || header.StoreID != seed {
		return nil, fmt.Errorf("%w: compact fold input", ErrInvalidWrite)
	}
	payload, err := BuildCompactPrimaryStripePayload(records, builder)
	if err != nil {
		return nil, err
	}
	need := PageHeaderSize + len(payload) + PageTrailerSize
	extent := (need + int(physicalPageQuantum) - 1) &^ (int(physicalPageQuantum) - 1)
	if extent > CommonPrimaryLeafMaxExtentBytes {
		return nil, ErrCommonPrimaryLeafFull
	}
	if extent > len(dst) {
		return nil, fmt.Errorf("%w: compact fold destination", ErrInvalidWrite)
	}
	header.PageSize = uint32(extent)
	return encodeCompactPrimaryStripePayload(dst[:extent], header, payload)
}

// CloneCompactPrimaryStripeGeneration copies an unchanged compact leaf into a
// newer COW generation and reseals it. It is valid only when row content and
// posting slots are unchanged.
func CloneCompactPrimaryStripeGeneration(
	dst, src []byte,
	generation uint64,
) ([]byte, error) {
	header, _, err := OpenPage(src)
	if err != nil || header.Kind != PagePrimaryLeaf ||
		PrimaryLeafClass(src) != CommonPrimaryLeafCompact ||
		generation <= header.Generation || generation >= uint64(1)<<48 ||
		len(dst) < len(src) || commonPrimaryLeafOverlaps(dst, src) {
		return nil, fmt.Errorf("%w: compact generation clone", ErrInvalidWrite)
	}
	page := dst[:len(src)]
	copy(page, src)
	binary.LittleEndian.PutUint64(page[24:32], generation)
	if _, err := sealPage(page, false); err != nil {
		return nil, err
	}
	return page, nil
}

// PatchStableCanonicalReplacementScalarPatch consumes the scalar spans already
// produced by concurrent canonical admission and certifies an exact compact
// row or one int/bool/null hole change against this immutable stripe. scratch
// is caller-owned decode storage and is returned so a bounded context retains
// any capacity it learned. The certificate deliberately excludes inserts,
// overflow rows, shape changes, and multi-hole changes; those keep using the
// complete compact planner.
func (v *CompactPrimaryStripeView) PatchStableCanonicalReplacementScalarPatch(
	key []byte,
	slot uint8,
	canonical CanonicalSpanIndex,
	scratch []byte,
) (
	patch CommonPrimaryUnifiedScalarPatch,
	out []byte,
	resolved bool,
	err error,
) {
	out = scratch[:0]
	if v == nil || len(key) == 0 || !canonical.valid ||
		len(canonical.canonical) == 0 {
		return CommonPrimaryUnifiedScalarPatch{}, out, false, nil
	}
	rank, found := v.FindKey(key)
	if !found || v.IsOverflow(rank) {
		return CommonPrimaryUnifiedScalarPatch{}, out, false, nil
	}
	if v.rows <= CommonPrimaryLeafWideSlots {
		admittedSlot, slotOK := v.PostingSlot(rank)
		if !slotOK {
			return CommonPrimaryUnifiedScalarPatch{}, out, false,
				fmt.Errorf("%w: compact patch posting slot", ErrCommonPrimaryLeafCorrupt)
		}
		if admittedSlot != slot {
			return CommonPrimaryUnifiedScalarPatch{}, out, false, nil
		}
	}
	shape := v.rowShape(rank)
	entry, entryOK := v.shapeEntry(shape)
	if !entryOK {
		return CommonPrimaryUnifiedScalarPatch{}, out, false,
			fmt.Errorf("%w: compact patch shape entry", ErrCommonPrimaryLeafCorrupt)
	}
	spans := canonical.spans
	if len(spans) != entry.template.holes {
		return CommonPrimaryUnifiedScalarPatch{}, out, true, nil
	}
	ordinal := v.shapeOrdinal(rank, shape)
	if ordinal < 0 || ordinal >= entry.rows {
		return CommonPrimaryUnifiedScalarPatch{}, out, false,
			fmt.Errorf("%w: compact patch shape ordinal", ErrCommonPrimaryLeafCorrupt)
	}

	previousCanonical, previousStatic := uint32(0), uint32(0)
	streamRaw := entry.streamRaw
	changedHole := -1
	for hole := 0; hole < entry.template.holes; hole++ {
		staticEnd := binary.LittleEndian.Uint32(entry.template.ends[hole*4:])
		span := spans[hole]
		if staticEnd < previousStatic || span.Start < previousCanonical ||
			span.End < span.Start || int(staticEnd) > len(entry.template.static) ||
			int(span.End) > len(canonical.canonical) ||
			!bytes.Equal(
				entry.template.static[previousStatic:staticEnd],
				canonical.canonical[previousCanonical:span.Start],
			) {
			return CommonPrimaryUnifiedScalarPatch{}, out, true, nil
		}
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			return CommonPrimaryUnifiedScalarPatch{}, out, false,
				fmt.Errorf("%w: compact patch source stream", ErrCommonPrimaryLeafCorrupt)
		}
		out = out[:0]
		out, admitted = stream.appendValue(out, stream.shapeCoordinate(rank, ordinal))
		if !admitted {
			return CommonPrimaryUnifiedScalarPatch{}, out, false,
				fmt.Errorf("%w: compact patch source value", ErrCommonPrimaryLeafCorrupt)
		}
		newValue := canonical.canonical[span.Start:span.End]
		if !bytes.Equal(out, newValue) {
			if changedHole >= 0 {
				return CommonPrimaryUnifiedScalarPatch{}, out, true, nil
			}
			_, oldScalar := unifiedScalarPatchValueClass(out)
			_, newScalar := unifiedScalarPatchValueClass(newValue)
			if !oldScalar || !newScalar || hole > int(^uint16(0)) ||
				int(span.Start) > int(^uint16(0)) || len(out) == 0 ||
				len(out) >= int(commonPrimaryUnifiedScalarPatchValid) ||
				len(newValue) == 0 || len(newValue) > int(^uint8(0)) {
				return CommonPrimaryUnifiedScalarPatch{}, out, true, nil
			}
			patch = CommonPrimaryUnifiedScalarPatch{
				bodyOffset:      uint16(hole),
				canonicalOffset: uint16(span.Start),
				bodyLength: commonPrimaryUnifiedScalarPatchValid |
					uint8(len(out)),
				canonicalLength: uint8(len(newValue)),
			}
			changedHole = hole
		}
		streamRaw = streamRaw[stream.encoded:]
		previousCanonical = span.End
		previousStatic = staticEnd
	}
	lastStatic := binary.LittleEndian.Uint32(
		entry.template.ends[entry.template.holes*4:],
	)
	if lastStatic < previousStatic || int(lastStatic) != len(entry.template.static) ||
		int(previousCanonical) > len(canonical.canonical) ||
		!bytes.Equal(
			entry.template.static[previousStatic:lastStatic],
			canonical.canonical[previousCanonical:],
		) {
		return CommonPrimaryUnifiedScalarPatch{}, out, true, nil
	}
	if changedHole < 0 {
		patch.bodyLength = commonPrimaryUnifiedScalarPatchValid
	}
	return patch, out, true, nil
}

// PatchCompactPrimaryStripeReplacements is the exact existing-row COW fast
// path for compact stripes. Every replacement must retain its admitted JSON
// shape and change at most one scalar hole. Each affected shape/hole column is
// decoded once and run through the complete compact-stream planner; the page
// is then assembled from the unchanged source ranges and the replanned
// streams. Those certificates make the result identical to a complete compact
// rebuild except for the requested generation. Any shape, slot, overflow, or
// maximum-extent violation declines to the ordinary full planner.
func (v *CompactPrimaryStripeView) PatchCompactPrimaryStripeReplacements(
	dst []byte,
	generation uint64,
	replacements []CommonPrimaryUnifiedReplacement,
	builder *UnifiedPrimaryLeafBuilder,
) (page []byte, ok bool, err error) {
	corrupt := func(what string) error {
		return fmt.Errorf("%w: compact patch %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	if v == nil || builder == nil || len(v.page) == 0 ||
		len(dst) < len(v.page) || len(replacements) == 0 ||
		generation <= v.header.Generation || generation >= uint64(1)<<48 ||
		commonPrimaryLeafOverlaps(dst[:len(v.page)], v.page) {
		return nil, false, nil
	}
	patch := &builder.compact
	patch.patchHeap = patch.patchHeap[:0]
	patch.patchMods = patch.patchMods[:0]
	for replacementIndex := range replacements {
		replacement := &replacements[replacementIndex]
		if len(replacement.Key) == 0 || len(replacement.Value) == 0 {
			return nil, false, nil
		}
		for previous := 0; previous < replacementIndex; previous++ {
			if bytes.Equal(replacements[previous].Key, replacement.Key) {
				return nil, false, nil
			}
		}
		rank, found := v.FindKey(replacement.Key)
		if !found || v.IsOverflow(rank) {
			return nil, false, nil
		}
		if v.rows <= CommonPrimaryLeafWideSlots {
			admittedSlot, slotOK := v.PostingSlot(rank)
			if !slotOK {
				return nil, false, corrupt("posting slot")
			}
			if admittedSlot != replacement.Slot {
				return nil, false, nil
			}
		}
		if replacement.ScalarPatch.valid() {
			patch.patchHeap = patch.patchHeap[:0]
			var decoded bool
			patch.patchHeap, decoded = v.AppendValue(patch.patchHeap, rank)
			if !decoded {
				return nil, false, corrupt("certified base value")
			}
			if replacement.ScalarPatch.exact() {
				if !bytes.Equal(patch.patchHeap, replacement.Value) {
					return nil, false, nil
				}
				continue
			}
			shape := v.rowShape(rank)
			candidate, entryOK := v.shapeEntry(shape)
			hole := int(replacement.ScalarPatch.bodyOffset)
			start := int(replacement.ScalarPatch.canonicalOffset)
			end := start + int(replacement.ScalarPatch.canonicalLength)
			ordinal := v.shapeOrdinal(rank, shape)
			if !entryOK || hole < 0 || hole >= candidate.template.holes ||
				ordinal < 0 || ordinal > int(^uint16(0)) ||
				shape < 0 || shape > int(^uint16(0)) || start < 0 ||
				end <= start || end > len(replacement.Value) {
				return nil, false, nil
			}
			streamRaw := candidate.streamRaw
			var oldStream compactStreamView
			for at := 0; at <= hole; at++ {
				stream, admitted := admittedCompactStream(streamRaw)
				if !admitted {
					return nil, false, corrupt("certified source stream")
				}
				if at == hole {
					oldStream = stream
					break
				}
				streamRaw = streamRaw[stream.encoded:]
			}
			baseBytes := len(patch.patchHeap)
			oldLength := replacement.ScalarPatch.oldBodyLength()
			if oldLength <= 0 || start > baseBytes ||
				oldLength > baseBytes-start || len(replacement.Value) !=
				baseBytes-oldLength+int(replacement.ScalarPatch.canonicalLength) ||
				!bytes.Equal(patch.patchHeap[:start], replacement.Value[:start]) ||
				!bytes.Equal(
					patch.patchHeap[start+oldLength:], replacement.Value[end:],
				) {
				return nil, false, nil
			}
			patch.patchHeap, decoded = oldStream.appendValue(
				patch.patchHeap, oldStream.shapeCoordinate(rank, ordinal),
			)
			oldValue := patch.patchHeap[baseBytes:]
			newValue := replacement.Value[start:end:end]
			_, oldScalar := unifiedScalarPatchValueClass(oldValue)
			_, newScalar := unifiedScalarPatchValueClass(newValue)
			if !decoded || !oldScalar || !newScalar ||
				len(oldValue) != oldLength ||
				!bytes.Equal(oldValue, patch.patchHeap[start:start+oldLength]) ||
				bytes.Equal(oldValue, newValue) {
				return nil, false, nil
			}
			patch.patchMods = append(
				patch.patchMods,
				compactPrimaryReplacementPatch{
					shape: uint16(shape), hole: uint16(hole),
					ordinal: uint16(ordinal), value: newValue,
				},
			)
			continue
		}
		canonical, canonicalOK, canonicalErr :=
			builder.canonicalSpanIndex(replacement.Value)
		if canonicalErr != nil {
			return nil, false, canonicalErr
		}
		if !canonicalOK {
			return nil, false, nil
		}
		shape := v.rowShape(rank)
		candidate, entryOK := v.shapeEntry(shape)
		if !entryOK {
			return nil, false, corrupt("shape entry")
		}
		spans := canonical.spans
		if len(spans) != candidate.template.holes {
			return nil, false, nil
		}
		ordinal := v.shapeOrdinal(rank, shape)
		previousCanonical, previousStatic := uint32(0), uint32(0)
		streamRaw := candidate.streamRaw
		changedHole := -1
		var changedValue []byte
		for hole := 0; hole < candidate.template.holes; hole++ {
			staticEnd := binary.LittleEndian.Uint32(candidate.template.ends[hole*4:])
			span := spans[hole]
			if staticEnd < previousStatic || span.Start < previousCanonical ||
				int(staticEnd) > len(candidate.template.static) ||
				int(span.End) > len(canonical.canonical) ||
				!bytes.Equal(
					candidate.template.static[previousStatic:staticEnd],
					canonical.canonical[previousCanonical:span.Start],
				) {
				return nil, false, nil
			}
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return nil, false, corrupt("source stream")
			}
			start := len(patch.patchHeap)
			patch.patchHeap, admitted = stream.appendValue(patch.patchHeap, stream.shapeCoordinate(rank, ordinal))
			if !admitted {
				return nil, false, corrupt("source value")
			}
			newValue := canonical.canonical[span.Start:span.End]
			if !bytes.Equal(patch.patchHeap[start:], newValue) {
				if changedHole >= 0 {
					return nil, false, nil
				}
				changedHole = hole
				changedValue = newValue
			}
			patch.patchHeap = patch.patchHeap[:start]
			streamRaw = streamRaw[stream.encoded:]
			previousCanonical = span.End
			previousStatic = staticEnd
		}
		lastStatic := binary.LittleEndian.Uint32(
			candidate.template.ends[candidate.template.holes*4:],
		)
		if lastStatic < previousStatic ||
			int(lastStatic) != len(candidate.template.static) ||
			int(previousCanonical) > len(canonical.canonical) ||
			!bytes.Equal(
				candidate.template.static[previousStatic:lastStatic],
				canonical.canonical[previousCanonical:],
			) {
			return nil, false, nil
		}
		if changedHole < 0 {
			continue
		}
		if shape < 0 || shape > int(^uint16(0)) ||
			changedHole > int(^uint16(0)) ||
			ordinal < 0 || ordinal > int(^uint16(0)) {
			return nil, false, corrupt("replacement coordinates")
		}
		patch.patchMods = append(
			patch.patchMods,
			compactPrimaryReplacementPatch{
				shape: uint16(shape), hole: uint16(changedHole),
				ordinal: uint16(ordinal), value: changedValue,
			},
		)
	}
	if len(patch.patchMods) == 0 {
		page, err := CloneCompactPrimaryStripeGeneration(dst, v.page, generation)
		return page, err == nil, err
	}
	if summariesFit, summaryErr := v.prepareCompactPrimarySummaryWiden(
		builder, replacements,
	); summaryErr != nil || !summariesFit {
		return nil, false, summaryErr
	}

	patch.patchGroups = patch.patchGroups[:0]
	for _, modification := range patch.patchMods {
		found := false
		for _, group := range patch.patchGroups {
			if group.shape == modification.shape && group.hole == modification.hole {
				found = true
				break
			}
		}
		if !found {
			patch.patchGroups = append(
				patch.patchGroups,
				compactPrimaryStreamPatch{
					shape: modification.shape, hole: modification.hole,
				},
			)
		}
	}
	slices.SortFunc(patch.patchGroups, func(a, b compactPrimaryStreamPatch) int {
		if a.shape != b.shape {
			return int(a.shape) - int(b.shape)
		}
		return int(a.hole) - int(b.hole)
	})
	patch.patchStreams = patch.patchStreams[:0]
	rankShape := -1
	shapeRanks := patch.shapeOrder[:0]
	for groupIndex := range patch.patchGroups {
		group := &patch.patchGroups[groupIndex]
		shape := int(group.shape)
		entry, entryOK := v.shapeEntry(shape)
		if !entryOK || int(group.hole) >= entry.template.holes {
			return nil, false, corrupt("group shape")
		}
		shapeOffset := 0
		if shape != 0 {
			shapeOffset = int(binary.LittleEndian.Uint32(v.shapeDir[(shape-1)*4:]))
		}
		templateBytes := 8 + (entry.template.holes+1)*4 + len(entry.template.static)
		streamRaw := entry.streamRaw
		oldStreamOffset := 0
		var oldStream compactStreamView
		for hole := 0; hole <= int(group.hole); hole++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return nil, false, corrupt("group stream")
			}
			if hole == int(group.hole) {
				oldStream = stream
				break
			}
			oldStreamOffset += stream.encoded
			streamRaw = streamRaw[stream.encoded:]
		}
		group.offset = v.shapeStart + shapeOffset + compactPrimaryShapeHeader +
			templateBytes + oldStreamOffset
		group.oldBytes = oldStream.encoded
		if group.offset < 0 || group.offset+group.oldBytes > len(v.payload) {
			return nil, false, corrupt("group bounds")
		}
		if !oldStream.matchesShapeRows(entry.rows, v.rows) {
			return nil, false, corrupt("group row coordinates")
		}
		// The rank candidate is only possible for a long sparse shape. Keep
		// this context local so ordinary streams never scan the leaf to build a
		// map that their planner cannot use. An existing rank-affine stream
		// resolves immediately because its physical coordinates are required
		// for decoding; a newly planned candidate resolves only after the
		// prefix parser rejects the local ordinal-affine form and its exact
		// witness checks pass.
		var rankContext compactRankContext
		var ranks []uint16
		rankCandidateEligible := entry.rows >= compactStreamRestart && entry.rows < v.rows
		rankContextNeeded := entry.rows < v.rows &&
			(oldStream.kind == compactStreamRankAffine || rankCandidateEligible)
		if rankContextNeeded {
			rankContext = compactRankContext{
				view: v, shape: shape, storage: shapeRanks,
			}
			if rankShape == shape {
				rankContext.ranks = shapeRanks
				rankContext.resolved = true
			}
			if oldStream.kind == compactStreamRankAffine && !rankContext.resolved {
				ranks = rankContext.resolve()
				if len(ranks) != entry.rows {
					return nil, false, corrupt("group rank map")
				}
				shapeRanks = ranks
				patch.shapeOrder = shapeRanks
				rankShape = shape
			} else {
				ranks = rankContext.ranks
			}
		}
		patch.patchHeap = patch.patchHeap[:0]
		patch.patchEnds = slices.Grow(
			patch.patchEnds[:0], entry.rows,
		)[:entry.rows]
		for row := 0; row < entry.rows; row++ {
			coordinate := row
			if oldStream.kind == compactStreamRankAffine && len(ranks) != 0 {
				coordinate = int(ranks[row])
			}
			var decoded bool
			patch.patchHeap, decoded = oldStream.appendValue(patch.patchHeap, coordinate)
			if !decoded || uint64(len(patch.patchHeap)) > uint64(^uint32(0)) {
				return nil, false, corrupt("group value")
			}
			patch.patchEnds[row] = uint32(len(patch.patchHeap))
		}
		patch.patchValues = slices.Grow(
			patch.patchValues[:0], entry.rows,
		)[:entry.rows]
		start := uint32(0)
		for row, end := range patch.patchEnds {
			patch.patchValues[row] = patch.patchHeap[start:end:end]
			start = end
		}
		for _, modification := range patch.patchMods {
			if modification.shape != group.shape || modification.hole != group.hole {
				continue
			}
			if int(modification.ordinal) >= entry.rows {
				return nil, false, corrupt("group ordinal")
			}
			patch.patchValues[modification.ordinal] = modification.value
		}
		var rankContextPtr *compactRankContext
		if rankCandidateEligible {
			rankContextPtr = &rankContext
		}
		encoded := patch.stream.encodeShapeWithRankContext(
			patch.patchValues, ranks, v.rows, rankContextPtr,
		)
		if rankCandidateEligible && len(rankContext.ranks) != 0 {
			shapeRanks = rankContext.ranks
			patch.shapeOrder = shapeRanks
			rankShape = shape
		}
		group.newStart = len(patch.patchStreams)
		patch.patchStreams, err = encoded.appendBinary(patch.patchStreams)
		if err != nil {
			return nil, false, err
		}
		group.newEnd = len(patch.patchStreams)
	}
	slices.SortFunc(patch.patchGroups, func(a, b compactPrimaryStreamPatch) int {
		return a.offset - b.offset
	})
	delta := 0
	for _, group := range patch.patchGroups {
		delta += group.newEnd - group.newStart - group.oldBytes
	}
	newPayloadBytes := len(v.payload) + delta
	need := PageHeaderSize + newPayloadBytes + PageTrailerSize
	extent := (need + int(physicalPageQuantum) - 1) &^ (int(physicalPageQuantum) - 1)
	if newPayloadBytes < compactPrimaryHeaderBytes ||
		extent > CommonPrimaryLeafMaxExtentBytes || extent > len(dst) {
		return nil, false, nil
	}
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(v.header.Bucket)
	if !logicalOK {
		return nil, false, corrupt("logical identity")
	}
	payload, initErr := InitPage(dst[:extent], PageHeader{
		StoreID: v.header.StoreID, Generation: generation,
		LogicalID: logicalID, PageSize: uint32(extent),
		PayloadLength: uint32(newPayloadBytes), Kind: PagePrimaryLeaf,
	})
	if initErr != nil {
		return nil, false, initErr
	}
	cursor, sourceCursor := 0, 0
	for _, group := range patch.patchGroups {
		if group.offset < sourceCursor || group.offset+group.oldBytes > len(v.payload) {
			return nil, false, corrupt("assembly bounds")
		}
		cursor += copy(payload[cursor:], v.payload[sourceCursor:group.offset])
		cursor += copy(
			payload[cursor:], patch.patchStreams[group.newStart:group.newEnd],
		)
		sourceCursor = group.offset + group.oldBytes
	}
	cursor += copy(payload[cursor:], v.payload[sourceCursor:])
	if cursor != newPayloadBytes {
		return nil, false, corrupt("assembly length")
	}
	if len(patch.patchGroups) != 0 {
		shapeBytes := int64(binary.LittleEndian.Uint32(payload[24:])) + int64(delta)
		if shapeBytes < 0 || shapeBytes > int64(^uint32(0)) {
			return nil, false, corrupt("shape bytes")
		}
		binary.LittleEndian.PutUint32(payload[24:], uint32(shapeBytes))
		patch.shapeDeltas = slices.Grow(
			patch.shapeDeltas[:0], v.shapeCount,
		)[:v.shapeCount]
		clear(patch.shapeDeltas)
		for _, group := range patch.patchGroups {
			patch.shapeDeltas[group.shape] +=
				group.newEnd - group.newStart - group.oldBytes
		}
		cumulative := 0
		for shape := 0; shape < v.shapeCount; shape++ {
			shapeOffset := 0
			if shape != 0 {
				shapeOffset = int(binary.LittleEndian.Uint32(v.shapeDir[(shape-1)*4:]))
			}
			if patch.shapeDeltas[shape] != 0 {
				streamBytesAt := v.shapeStart + shapeOffset + cumulative + 12
				streamBytes := int64(binary.LittleEndian.Uint32(payload[streamBytesAt:])) +
					int64(patch.shapeDeltas[shape])
				if streamBytes < 0 || streamBytes > int64(^uint32(0)) {
					return nil, false, corrupt("stream bytes")
				}
				binary.LittleEndian.PutUint32(
					payload[streamBytesAt:], uint32(streamBytes),
				)
			}
			cumulative += patch.shapeDeltas[shape]
			at := compactPrimaryHeaderBytes + shape*4
			end := int64(binary.LittleEndian.Uint32(v.shapeDir[shape*4:])) +
				int64(cumulative)
			if end <= 0 || end > int64(^uint32(0)) {
				return nil, false, corrupt("shape directory")
			}
			binary.LittleEndian.PutUint32(payload[at:], uint32(end))
		}
	}
	if v.summaryCount != 0 {
		summaryStart := len(payload) - len(v.summaryRaw)
		if summaryStart < 0 || !v.rewriteCompactPrimarySummaries(
			payload[summaryStart:], builder,
		) {
			return nil, false, corrupt("summary rewrite")
		}
	}
	page = dst[:extent]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, false, err
	}
	return page, true, nil
}

type compactPrimaryTemplateView struct {
	holes  int
	ends   []byte
	static []byte
}

type compactPrimaryShapeView struct {
	rows      int
	template  compactPrimaryTemplateView
	streamRaw []byte
}

type CompactPrimaryStripeView struct {
	header       CommonPrimaryLeafHeader
	page         []byte
	payload      []byte
	rows         int
	shapeCount   int
	shapeWidth   int
	key          compactStreamView
	shapeCodes   []byte
	rankTable    []byte
	shapeDir     []byte
	slots        []byte
	overflow     []byte
	overflowRef  []byte
	shapeData    []byte
	shapeStart   int
	summaryRaw   []byte
	summaryCount int
}

func OpenCompactPrimaryStripe(
	src []byte,
	storeID [16]byte,
	bucket BucketID,
	expected PageRef,
	selectingGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) (CompactPrimaryStripeView, error) {
	corrupt := func(what string) (CompactPrimaryStripeView, error) {
		return CompactPrimaryStripeView{}, fmt.Errorf("%w: compact stripe %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	if storeID == ([16]byte{}) ||
		!commonPrimaryLeafValidateExpectedRef(expected, bucket, selectingGeneration, bounds) {
		return corrupt("selector")
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return CompactPrimaryStripeView{}, fmt.Errorf("%w: %w", ErrCommonPrimaryLeafCorrupt, err)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(bucket)
	if pageHeader.StoreID != storeID || pageHeader.LogicalID != logicalID ||
		pageHeader.Generation != expected.Generation || pageHeader.PageSize != expected.Length ||
		pageHeader.Kind != PagePrimaryLeaf || len(payload) < compactPrimaryHeaderBytes ||
		string(payload[:4]) != compactPrimaryMagic {
		return corrupt("identity or magic")
	}
	view, openErr := openCompactPrimaryStripePayload(
		payload, pageHeader, storeID, bucket, bounds, true,
		expected.Offset < bounds.FileEnd,
	)
	view.page = src[:pageHeader.PageSize:pageHeader.PageSize]
	return view, openErr
}

// AdmittedCompactPrimaryStripe opens a page already selected and checksum-
// admitted by the page cache. Selection bounds remain enforced by the cache;
// this function validates the complete compact payload and page identity.
func AdmittedCompactPrimaryStripe(
	src []byte,
	storeID [16]byte,
	bucket BucketID,
) (CompactPrimaryStripeView, bool) {
	pageHeader, payload, err := OpenPage(src)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(bucket)
	if err != nil || !logicalOK || pageHeader.StoreID != storeID ||
		pageHeader.LogicalID != logicalID || pageHeader.Kind != PagePrimaryLeaf ||
		pageHeader.Generation == 0 || len(payload) < compactPrimaryHeaderBytes ||
		string(payload[:4]) != compactPrimaryMagic {
		return CompactPrimaryStripeView{}, false
	}
	view, err := openCompactPrimaryStripePayload(
		payload, pageHeader, storeID, bucket, CommonPrimaryLeafBounds{}, false, false,
	)
	view.page = src[:pageHeader.PageSize:pageHeader.PageSize]
	return view, err == nil
}

// AdmittedCachedCompactPrimaryStripe opens a payload whose enclosing page was
// already checksum- and identity-validated by PageCache. It repeats the compact
// grammar and logical-identity checks but deliberately does not hash the whole
// page again on every warmed point read.
func AdmittedCachedCompactPrimaryStripe(
	pageHeader PageHeader,
	payload []byte,
	storeID [16]byte,
	bucket BucketID,
) (CompactPrimaryStripeView, bool) {
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(bucket)
	if !logicalOK || pageHeader.StoreID != storeID ||
		pageHeader.LogicalID != logicalID || pageHeader.Kind != PagePrimaryLeaf ||
		pageHeader.Generation == 0 ||
		pageHeader.PayloadLength != uint32(len(payload)) ||
		len(payload) < compactPrimaryHeaderBytes ||
		string(payload[:4]) != compactPrimaryMagic {
		return CompactPrimaryStripeView{}, false
	}
	view, err := openCompactPrimaryStripePayload(
		payload, pageHeader, storeID, bucket, CommonPrimaryLeafBounds{}, false, false,
	)
	return view, err == nil
}

func openCompactPrimaryStripePayload(
	payload []byte,
	pageHeader PageHeader,
	storeID [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
	validate bool,
	validateOverflowBounds bool,
) (CompactPrimaryStripeView, error) {
	corrupt := func(what string) (CompactPrimaryStripeView, error) {
		return CompactPrimaryStripeView{}, fmt.Errorf("%w: compact stripe %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	rows := int(binary.LittleEndian.Uint32(payload[4:]))
	shapeCount := int(binary.LittleEndian.Uint16(payload[8:]))
	shapeWidth := int(payload[10])
	flags := payload[11]
	hasOverflow := flags&compactPrimaryHasOverflow != 0
	if flags&^compactPrimaryHasOverflow != 0 ||
		rows < 0 || rows > CompactPrimaryStripeMaxRows ||
		(rows == 0 && (shapeCount != 0 || shapeWidth != 0)) ||
		shapeCount > rows ||
		(!hasOverflow && rows != 0 && (shapeCount < 1 ||
			shapeWidth != bits.Len(uint(shapeCount-1)))) ||
		(hasOverflow && (rows == 0 || shapeWidth != bits.Len(uint(shapeCount)))) {
		return corrupt("header geometry")
	}
	keyBytes := int(binary.LittleEndian.Uint32(payload[12:]))
	shapeCodeBytes := int(binary.LittleEndian.Uint32(payload[16:]))
	rankBytes := int(binary.LittleEndian.Uint32(payload[20:]))
	shapeBytes := int(binary.LittleEndian.Uint32(payload[24:]))
	slotBytes := int(binary.LittleEndian.Uint32(payload[28:]))
	summaryCount := int(binary.LittleEndian.Uint16(payload[32:]))
	summaryBytes := int(binary.LittleEndian.Uint32(payload[36:]))
	if binary.LittleEndian.Uint16(payload[34:]) != 0 ||
		summaryCount > PageCatalogMaxSkipIndexes ||
		(summaryCount == 0) != (summaryBytes == 0) ||
		summaryBytes > CompactPrimarySummaryMaxBytes {
		return corrupt("summary header")
	}
	wantShapeCodes := (rows*shapeWidth + 7) / 8
	restarts := (rows + compactStreamRestart - 1) / compactStreamRestart
	wantRanks := restarts * shapeCount * 2
	overflowBitmapBytes := 0
	if hasOverflow {
		overflowBitmapBytes = (rows + 7) / 8
	}
	prefix64 := uint64(compactPrimaryHeaderBytes) + uint64(4*shapeCount) +
		uint64(keyBytes) + uint64(slotBytes) + uint64(overflowBitmapBytes)
	if prefix64 > uint64(len(payload)) {
		return corrupt("overflow bitmap bounds")
	}
	overflowBitmap := payload[int(prefix64)-overflowBitmapBytes : int(prefix64)]
	overflowCount := 0
	for _, word := range overflowBitmap {
		overflowCount += bits.OnesCount8(word)
	}
	if hasOverflow && (overflowCount == 0 ||
		rows&7 != 0 && overflowBitmap[len(overflowBitmap)-1]>>uint(rows&7) != 0) {
		return corrupt("overflow bitmap")
	}
	overflowRefBytes := overflowCount * PageRefSize
	fixed64 := prefix64 + uint64(overflowRefBytes) +
		uint64(shapeCodeBytes) + uint64(rankBytes) + uint64(shapeBytes) +
		uint64(summaryBytes)
	if shapeCodeBytes != wantShapeCodes || rankBytes != wantRanks ||
		(slotBytes != 0 && (slotBytes != rows || rows > CommonPrimaryLeafWideSlots)) ||
		fixed64 != uint64(len(payload)) {
		return corrupt("section lengths")
	}
	shapeDir := payload[compactPrimaryHeaderBytes : compactPrimaryHeaderBytes+4*shapeCount]
	cursor := compactPrimaryHeaderBytes + 4*shapeCount
	key, err := openCompactStream(payload[cursor : cursor+keyBytes])
	if err != nil || key.encoded != keyBytes || key.count != rows || key.kind == compactStreamRankAffine {
		return corrupt("key stream")
	}
	cursor += keyBytes
	slots := payload[cursor : cursor+slotBytes]
	cursor += slotBytes
	overflowBitmap = payload[cursor : cursor+overflowBitmapBytes]
	cursor += overflowBitmapBytes
	overflowRefs := payload[cursor : cursor+overflowRefBytes]
	cursor += overflowRefBytes
	shapeCodes := payload[cursor : cursor+shapeCodeBytes]
	cursor += shapeCodeBytes
	rankTable := payload[cursor : cursor+rankBytes]
	cursor += rankBytes
	shapeData := payload[cursor : cursor+shapeBytes]
	summaryRaw := payload[cursor+shapeBytes:]
	if err := validateCompactPrimarySummaries(summaryRaw, summaryCount); err != nil {
		return CompactPrimaryStripeView{}, err
	}
	v := CompactPrimaryStripeView{
		header: CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		payload: payload, rows: rows, shapeCount: shapeCount, shapeWidth: shapeWidth,
		key: key, slots: slots, overflow: overflowBitmap, overflowRef: overflowRefs,
		shapeCodes: shapeCodes, rankTable: rankTable,
		shapeDir: shapeDir, shapeData: shapeData, shapeStart: cursor,
		summaryRaw: summaryRaw, summaryCount: summaryCount,
	}
	if validate {
		if err := v.validate(bounds, validateOverflowBounds); err != nil {
			return CompactPrimaryStripeView{}, err
		}
	}
	return v, nil
}

func (v *CompactPrimaryStripeView) validate(
	bounds CommonPrimaryLeafBounds,
	validateOverflowBounds bool,
) error {
	corrupt := func(what string) error {
		return fmt.Errorf("%w: compact stripe %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	shapeRows := make([]int, v.shapeCount)
	var usedSlots [4]uint64
	for row := 0; row < v.rows; row++ {
		if len(v.slots) != 0 {
			slot := v.slots[row]
			bit := uint64(1) << uint(slot&63)
			if usedSlots[slot>>6]&bit != 0 {
				return corrupt("posting slot")
			}
			usedSlots[slot>>6] |= bit
		}
		shape := v.rowShape(row)
		if row%compactStreamRestart == 0 {
			block := row / compactStreamRestart
			for id := range v.shapeCount {
				got := int(binary.LittleEndian.Uint16(v.rankTable[(block*v.shapeCount+id)*2:]))
				if got != shapeRows[id] {
					return corrupt("shape rank checkpoint")
				}
			}
		}
		if v.IsOverflow(row) {
			if shape != v.shapeCount {
				return corrupt("overflow shape code")
			}
			ref, ok := v.OverflowRef(row)
			logicalID, logicalOK := CommonPrimaryLeafLogicalID(v.header.Bucket)
			validRef := ok && logicalOK && ref.Kind == PageOverflow &&
				ref.LogicalID >= CommonPrimaryLeafFirstDynamicLogicalID &&
				ref.LogicalID != logicalID && ref.Generation != 0 &&
				ref.Generation <= v.header.Generation &&
				validPageExtentSize(PageOverflow, ref.Length)
			if validateOverflowBounds {
				validRef = validRef && commonPrimaryLeafValidateOverflow(
					ref, logicalID, v.header.Generation, bounds,
				)
			}
			if !validRef {
				return corrupt(fmt.Sprintf(
					"overflow reference row=%d ref=%+v leaf_generation=%d bounds=%+v",
					row, ref, v.header.Generation, bounds,
				))
			}
			continue
		}
		if shape < 0 || shape >= v.shapeCount {
			return corrupt("shape code")
		}
		shapeRows[shape]++
	}
	previous := uint32(0)
	for shape := 0; shape < v.shapeCount; shape++ {
		end := binary.LittleEndian.Uint32(v.shapeDir[shape*4:])
		if end <= previous || uint64(end) > uint64(len(v.shapeData)) {
			return corrupt("shape directory")
		}
		entry, ok := v.shapeEntry(shape)
		if !ok || entry.rows != shapeRows[shape] {
			return corrupt("shape entry")
		}
		streamRaw := entry.streamRaw
		for range entry.template.holes {
			stream, err := openCompactStream(streamRaw)
			if err != nil || !stream.matchesShapeRows(entry.rows, v.rows) {
				return corrupt("shape stream")
			}
			streamRaw = streamRaw[stream.encoded:]
		}
		if len(streamRaw) != 0 {
			return corrupt("shape stream length")
		}
		previous = end
	}
	if int(previous) != len(v.shapeData) {
		return corrupt("shape section length")
	}
	var previousKey [CommonPrimaryLeafMaxKeyBytes]byte
	previousLength := 0
	var keyScratch [CommonPrimaryLeafMaxKeyBytes]byte
	for row := 0; row < v.rows; row++ {
		key, ok := v.key.appendValue(keyScratch[:0], row)
		if !ok || len(key) == 0 || len(key) > CommonPrimaryLeafMaxKeyBytes ||
			row != 0 && bytes.Compare(previousKey[:previousLength], key) >= 0 {
			return corrupt("key order")
		}
		previousLength = copy(previousKey[:], key)
	}
	return nil
}

func (v *CompactPrimaryStripeView) shapeEntry(shape int) (compactPrimaryShapeView, bool) {
	if v == nil || shape < 0 || shape >= v.shapeCount {
		return compactPrimaryShapeView{}, false
	}
	start := uint32(0)
	if shape != 0 {
		start = binary.LittleEndian.Uint32(v.shapeDir[(shape-1)*4:])
	}
	end := binary.LittleEndian.Uint32(v.shapeDir[shape*4:])
	if end <= start || uint64(end) > uint64(len(v.shapeData)) {
		return compactPrimaryShapeView{}, false
	}
	raw := v.shapeData[start:end]
	if len(raw) < compactPrimaryShapeHeader || raw[6] != 0 || raw[7] != 0 {
		return compactPrimaryShapeView{}, false
	}
	rows := int(binary.LittleEndian.Uint32(raw))
	holes := int(binary.LittleEndian.Uint16(raw[4:]))
	templateBytes := int(binary.LittleEndian.Uint32(raw[8:]))
	streamBytes := int(binary.LittleEndian.Uint32(raw[12:]))
	if holes < 1 || templateBytes < 8+(holes+1)*4 ||
		compactPrimaryShapeHeader+templateBytes+streamBytes != len(raw) {
		return compactPrimaryShapeView{}, false
	}
	template := raw[compactPrimaryShapeHeader : compactPrimaryShapeHeader+templateBytes]
	if int(binary.LittleEndian.Uint16(template)) != holes || template[2] != 0 || template[3] != 0 {
		return compactPrimaryShapeView{}, false
	}
	staticBytes := int(binary.LittleEndian.Uint32(template[4:]))
	endsBytes := (holes + 1) * 4
	if 8+endsBytes+staticBytes != len(template) {
		return compactPrimaryShapeView{}, false
	}
	ends := template[8 : 8+endsBytes]
	static := template[8+endsBytes:]
	previous := uint32(0)
	for segment := 0; segment <= holes; segment++ {
		value := binary.LittleEndian.Uint32(ends[segment*4:])
		if value < previous || uint64(value) > uint64(len(static)) {
			return compactPrimaryShapeView{}, false
		}
		previous = value
	}
	if int(previous) != len(static) {
		return compactPrimaryShapeView{}, false
	}
	return compactPrimaryShapeView{
		rows:      rows,
		template:  compactPrimaryTemplateView{holes: holes, ends: ends, static: static},
		streamRaw: raw[compactPrimaryShapeHeader+templateBytes:],
	}, true
}

func (v *CompactPrimaryStripeView) rowShape(row int) int {
	if v == nil || row < 0 || row >= v.rows {
		return -1
	}
	// A shape-identical stripe has the canonical zero-bit shape code. It is the
	// overwhelmingly common layout for table- and log-like collections, and it
	// does not need to enter the general bit reservoir just to recover zero.
	if v.shapeCount == 1 && len(v.overflow) == 0 {
		return 0
	}
	return int(compactReadBits(v.shapeCodes, row*v.shapeWidth, v.shapeWidth))
}

func (v *CompactPrimaryStripeView) shapeOrdinal(row, shape int) int {
	// With one shape and no overflow, every preceding row contributes exactly
	// one to that shape's ordinal. Avoid rescanning the row's restart block;
	// unlike a stored rank cache this costs no resident or durable bytes.
	if v.shapeCount == 1 && len(v.overflow) == 0 {
		return row
	}
	block := row / compactStreamRestart
	ordinal := int(binary.LittleEndian.Uint16(v.rankTable[(block*v.shapeCount+shape)*2:]))
	if v.shapeWidth == 2 {
		return ordinal + compactCountTwoBitShapePrefix(
			v.shapeCodes, block*compactStreamRestart, row%compactStreamRestart, shape,
		)
	}
	for at := block * compactStreamRestart; at < row; at++ {
		if v.rowShape(at) == shape {
			ordinal++
		}
	}
	return ordinal
}

// compactCountTwoBitShapePrefix counts one shape in a prefix that starts at a
// 64-row restart boundary. Two-bit codes are the common two-to-four-shape
// layout; comparing all four codes in each byte avoids up to 63 independent
// compactReadBits calls on every random point read.
func compactCountTwoBitShapePrefix(
	codes []byte,
	startRow, count, shape int,
) int {
	if count <= 0 {
		return 0
	}
	packed := codes[startRow/4:]
	target := byte(shape) * 0x55
	matched := 0
	whole := count / 4
	if whole >= 8 {
		const lanes = uint64(0x5555555555555555)
		different := binary.LittleEndian.Uint64(packed) ^ uint64(shape)*lanes
		matched += bits.OnesCount64(^(different | different>>1) & lanes)
		packed = packed[8:]
		whole -= 8
	}
	for _, value := range packed[:whole] {
		different := value ^ target
		matched += bits.OnesCount8(^(different | different>>1) & 0x55)
	}
	if trailing := count & 3; trailing != 0 {
		mask := byte(1<<uint(trailing*2)) - 1
		different := (packed[whole] ^ target) & mask
		matched += bits.OnesCount8(
			^(different | different>>1) & 0x55 & mask,
		)
	}
	return matched
}

func (v *CompactPrimaryStripeView) Len() int {
	if v == nil {
		return 0
	}
	return v.rows
}

func (v *CompactPrimaryStripeView) ShapeCount() int {
	if v == nil {
		return 0
	}
	return v.shapeCount
}

func (v *CompactPrimaryStripeView) Header() CommonPrimaryLeafHeader {
	if v == nil {
		return CommonPrimaryLeafHeader{}
	}
	return v.header
}

// SummaryCount is the collection-catalog ordered min/max entry count carried
// by this stripe.
func (v *CompactPrimaryStripeView) SummaryCount() int {
	if v == nil {
		return 0
	}
	return v.summaryCount
}

// Summary returns one borrowed canonical ordered scalar interval. false means
// the entry was deliberately made unprunable by a container, oversized value,
// overflow row, or bounded metadata admission.
func (v *CompactPrimaryStripeView) Summary(index int) (
	minTerm, maxTerm []byte,
	valid bool,
) {
	if v == nil || index < 0 || index >= v.summaryCount {
		return nil, nil, false
	}
	return compactPrimarySummaryAt(v.summaryRaw, index)
}

// RowAt returns the borrowed-through-scratch key at lexical rank.
func (v *CompactPrimaryStripeView) RowAt(rank int) ([]byte, bool) {
	var scratch [CommonPrimaryLeafMaxKeyBytes]byte
	return v.AppendKey(scratch[:0], rank)
}

// RankAtSlot maps indexed compact stripes' posting ordinal to lexical rank.
func (v *CompactPrimaryStripeView) RankAtSlot(slot uint8) (int, bool) {
	if v == nil {
		return 0, false
	}
	if len(v.slots) == 0 {
		if int(slot) >= v.rows {
			return 0, false
		}
		return int(slot), true
	}
	for rank, candidate := range v.slots {
		if candidate == slot {
			return rank, true
		}
	}
	return 0, false
}

func (v *CompactPrimaryStripeView) PostingSlot(rank int) (uint8, bool) {
	if v == nil || rank < 0 || rank >= v.rows || v.rows > CommonPrimaryLeafWideSlots {
		return 0, false
	}
	if len(v.slots) != 0 {
		return v.slots[rank], true
	}
	return uint8(rank), true
}

func (v *CompactPrimaryStripeView) IsOverflow(row int) bool {
	return v != nil && row >= 0 && row < v.rows && len(v.overflow) != 0 &&
		v.overflow[row>>3]&(byte(1)<<uint(row&7)) != 0
}

// HasOverflowRows reports whether any compact row refers to an out-of-line
// value. A false result certifies that cloning or column-patching the stripe
// cannot preserve a volatile overflow reference across a checkpoint.
func (v *CompactPrimaryStripeView) HasOverflowRows() bool {
	return v != nil && len(v.overflow) != 0
}

// OverflowRef returns the out-of-line descriptor at row. Overflow values are
// exceptional, so deriving their compact ordinal with a bitmap popcount keeps
// the common inline format free of another rank table.
func (v *CompactPrimaryStripeView) OverflowRef(row int) (PageRef, bool) {
	if !v.IsOverflow(row) {
		return PageRef{}, false
	}
	ordinal := 0
	whole := row >> 3
	for _, word := range v.overflow[:whole] {
		ordinal += bits.OnesCount8(word)
	}
	ordinal += bits.OnesCount8(v.overflow[whole] & (byte(1)<<uint(row&7) - 1))
	start := ordinal * PageRefSize
	if start < 0 || start+PageRefSize > len(v.overflowRef) {
		return PageRef{}, false
	}
	return decodePageRef(v.overflowRef[start : start+PageRefSize]), true
}

// ChooseInsertSlotHashed selects a temporary posting slot for a delta-overlay
// insert. Base rows occupy their current lexical ordinals; additional carries
// slots already reserved by newer inserts. A checkpoint rebuilds final
// ordinals and exact postings together.
func (v *CompactPrimaryStripeView) ChooseInsertSlotHashed(
	hash uint64,
	additional [4]uint64,
) (uint8, bool) {
	if v == nil || v.rows >= CommonPrimaryLeafWideSlots {
		return 0, false
	}
	start := uint8(hash)
	for step := 0; step < CommonPrimaryLeafWideSlots; step++ {
		slot := start + uint8(step)
		_, baseOccupied := v.RankAtSlot(slot)
		if baseOccupied || additional[slot>>6]&(uint64(1)<<uint(slot&63)) != 0 {
			continue
		}
		return slot, true
	}
	return 0, false
}

func (v *CompactPrimaryStripeView) EncodedPayloadBytes() int {
	if v == nil {
		return 0
	}
	return len(v.payload)
}

func (v *CompactPrimaryStripeView) FirstRankFrom(key []byte) int {
	if v == nil {
		return 0
	}
	var scratch [CommonPrimaryLeafMaxKeyBytes]byte
	lo, hi := 0, v.rows
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		candidate, ok := v.key.appendValue(scratch[:0], mid)
		if !ok || bytes.Compare(candidate, key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (v *CompactPrimaryStripeView) AppendKey(dst []byte, row int) ([]byte, bool) {
	if v == nil {
		return dst, false
	}
	return v.key.appendValue(dst, row)
}

func (v *CompactPrimaryStripeView) AppendValue(dst []byte, row int) ([]byte, bool) {
	if v == nil || row < 0 || row >= v.rows || v.IsOverflow(row) {
		return dst, false
	}
	if v.shapeCount == 1 && len(v.overflow) == 0 {
		return v.appendValueOrdinal(dst, row, 0, row)
	}
	shape := v.rowShape(row)
	return v.appendValueOrdinal(dst, row, shape, v.shapeOrdinal(row, shape))
}

// valueLength computes one inline row's exact JSON length without appending to
// a destination. Projection fallback uses it to reject an undersized bounded
// scratch before AppendValue can grow that scratch behind the work budget.
func (v *CompactPrimaryStripeView) valueLength(row int) (int, bool) {
	if v == nil || row < 0 || row >= v.rows || v.IsOverflow(row) {
		return 0, false
	}
	shape := v.rowShape(row)
	if shape < 0 || shape >= v.shapeCount {
		return 0, false
	}
	entry, ok := v.shapeEntry(shape)
	if !ok || entry.rows < 0 {
		return 0, false
	}
	ordinal := v.shapeOrdinal(row, shape)
	if ordinal < 0 || ordinal >= entry.rows || len(entry.template.ends) < (entry.template.holes+1)*4 {
		return 0, false
	}
	length := 0
	previous := uint32(0)
	streamRaw := entry.streamRaw
	for hole := 0; hole < entry.template.holes; hole++ {
		end := binary.LittleEndian.Uint32(entry.template.ends[hole*4:])
		if end < previous || uint64(end) > uint64(len(entry.template.static)) {
			return 0, false
		}
		part := int(end - previous)
		if length > int(^uint(0)>>1)-part {
			return 0, false
		}
		length += part
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			return 0, false
		}
		part, _, ok = compactProjectionValueLen(stream, stream.shapeCoordinate(row, ordinal))
		if !ok || length > int(^uint(0)>>1)-part {
			return 0, false
		}
		length += part
		streamRaw = streamRaw[stream.encoded:]
		previous = end
	}
	last := binary.LittleEndian.Uint32(entry.template.ends[entry.template.holes*4:])
	if last < previous || uint64(last) > uint64(len(entry.template.static)) {
		return 0, false
	}
	part := int(last - previous)
	if length > int(^uint(0)>>1)-part {
		return 0, false
	}
	return length + part, true
}

// appendValueOrdinal reconstructs a row after a sequential caller has already
// counted this shape's ordinal. Point reads use AppendValue; ordered cursors
// carry the monotonically increasing ordinal instead of rescanning up to one
// restart block of shape codes for every row.
func (v *CompactPrimaryStripeView) appendValueOrdinal(
	dst []byte,
	row, shape, ordinal int,
) ([]byte, bool) {
	if v == nil || row < 0 || row >= v.rows || v.IsOverflow(row) ||
		shape < 0 || shape >= v.shapeCount {
		return dst, false
	}
	entry, ok := v.shapeEntry(shape)
	if !ok || ordinal < 0 || ordinal >= entry.rows {
		return dst, false
	}
	start := len(dst)
	streamRaw := entry.streamRaw
	previous := uint32(0)
	for hole := 0; hole < entry.template.holes; hole++ {
		end := binary.LittleEndian.Uint32(entry.template.ends[hole*4:])
		dst = append(dst, entry.template.static[previous:end]...)
		previous = end
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			return dst[:start], false
		}
		dst, ok = stream.appendValue(dst, stream.shapeCoordinate(row, ordinal))
		if !ok {
			return dst[:start], false
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	return append(dst, entry.template.static[previous:]...), true
}

// AppendRawValue returns either canonical inline JSON or the encoded overflow
// descriptor used by scan callbacks.
func (v *CompactPrimaryStripeView) AppendRawValue(
	dst []byte, row int,
) (out []byte, overflow, ok bool) {
	if ref, found := v.OverflowRef(row); found {
		out = slices.Grow(dst, PageRefSize)
		start := len(out)
		out = out[:start+PageRefSize]
		encodePageRef(out[start:], ref)
		return out, true, true
	}
	out, ok = v.AppendValue(dst, row)
	return out, false, ok
}

// RenderRecordsWithScratch materializes compact rows into the bounded mutation
// workspace. The returned records borrow scratch until its next use.
func (v *CompactPrimaryStripeView) RenderRecordsWithScratch(
	scratch *PrimaryLeafMutationScratch,
) ([]CommonPrimaryLeafRecord, error) {
	if v == nil || !scratch.reset(v.rows) {
		return nil, ErrCommonPrimaryLeafFull
	}
	for row := 0; row < v.rows; row++ {
		start := len(scratch.heap)
		var decoded bool
		scratch.heap, decoded = v.AppendKey(scratch.heap, row)
		if !decoded {
			return nil, ErrCommonPrimaryLeafCorrupt
		}
		keyEnd := len(scratch.heap)
		if !v.IsOverflow(row) {
			scratch.heap, decoded = v.AppendValue(scratch.heap, row)
			if !decoded {
				return nil, ErrCommonPrimaryLeafCorrupt
			}
		}
		scratch.spans[row] = [2]int{start, keyEnd}
	}
	for row := 0; row < v.rows; row++ {
		span := scratch.spans[row]
		end := len(scratch.heap)
		if row+1 < v.rows {
			end = scratch.spans[row+1][0]
		}
		slot, ok := v.PostingSlot(row)
		if !ok && v.rows <= CommonPrimaryLeafWideSlots {
			return nil, ErrCommonPrimaryLeafCorrupt
		}
		value := CommonPrimaryLeafValue{Inline: scratch.heap[span[1]:end]}
		if v.IsOverflow(row) {
			ref, found := v.OverflowRef(row)
			if !found {
				return nil, ErrCommonPrimaryLeafCorrupt
			}
			value = CommonPrimaryLeafValue{Overflow: ref}
		}
		scratch.records = append(scratch.records, CommonPrimaryLeafRecord{
			Key: scratch.heap[span[0]:span[1]], Value: value, Slot: slot,
		})
	}
	return scratch.records, nil
}

func (v *CompactPrimaryStripeView) FindKey(key []byte) (int, bool) {
	if v == nil || len(key) == 0 {
		return 0, false
	}
	if v.key.kind == compactStreamPrefixInt {
		return v.key.findPrefixInteger(key)
	}
	var scratch [CommonPrimaryLeafMaxKeyBytes]byte
	lo, hi := 0, v.rows
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		candidate, ok := v.key.appendValue(scratch[:0], mid)
		if !ok {
			return 0, false
		}
		if bytes.Compare(candidate, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == v.rows {
		return 0, false
	}
	candidate, ok := v.key.appendValue(scratch[:0], lo)
	return lo, ok && bytes.Equal(candidate, key)
}

func (v *CompactPrimaryStripeView) CountDictionaryHoleEqual(
	holes []int,
	needle []byte,
) (matched int, ok bool) {
	if v == nil || len(v.overflow) != 0 || len(holes) != v.shapeCount {
		return 0, false
	}
	for shape, hole := range holes {
		if hole == UnifiedHoleAbsent {
			continue
		}
		entry, found := v.shapeEntry(shape)
		if !found || hole < 0 || hole >= entry.template.holes {
			return 0, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false
			}
			if at == hole {
				count, supported := stream.countDictionaryEqual(needle)
				if !supported {
					return 0, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, true
}

// CountResolvedDictionaryEqual evaluates one scalar equality directly over
// dictionary streams after resolving the path once per shape. It returns
// ok=false when any present shape needs a non-dictionary comparison.
func (v *CompactPrimaryStripeView) CountResolvedDictionaryEqual(
	resolver *UnifiedHoleResolver,
	needle []byte,
) (matched int, ok bool) {
	if v == nil || resolver == nil || len(v.overflow) != 0 {
		return 0, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return 0, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return 0, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false
			}
			if at == hole {
				count, supported := stream.countDictionaryEqual(needle)
				if !supported {
					return 0, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, true
}

// CountResolvedSpellingEqual evaluates exact canonical-spelling equality over
// every compact scalar codec. scratch carries front-coded reconstruction state
// across shapes and scans; it is returned even when ok=false.
func (v *CompactPrimaryStripeView) CountResolvedSpellingEqual(
	resolver *UnifiedHoleResolver,
	needle, scratch []byte,
) (matched int, out []byte, ok bool) {
	if v == nil || resolver == nil || len(v.overflow) != 0 {
		return 0, scratch, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return 0, scratch, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return 0, scratch, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, scratch, false
			}
			if at == hole {
				var count int
				if stream.kind == compactStreamRankAffine {
					count, ok = v.rankAffineShapeSpelling(stream, shape, needle), true
				} else {
					count, scratch, ok = stream.countSpellingEqual(needle, scratch)
				}
				if !ok {
					return 0, scratch, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, scratch, true
}

// CountResolvedIntegerEqual evaluates exact integer equality directly over
// FOR or delta streams after resolving the path once per shape. The stream
// counters consume all encoded rows; no posting list or pruning metadata is
// consulted. ok=false asks the caller to use the general scalar path.
func (v *CompactPrimaryStripeView) CountResolvedIntegerEqual(
	resolver *UnifiedHoleResolver,
	needle int64,
) (matched int, ok bool) {
	if v == nil || resolver == nil || len(v.overflow) != 0 {
		return 0, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return 0, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return 0, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false
			}
			if at == hole {
				var count int
				var supported bool
				if stream.kind == compactStreamRankAffine {
					count = v.rankAffineShapeEqual(stream, shape, needle)
					supported = stream.rankAffineIsNumber()
				} else {
					count, supported = stream.countIntegerEqual(needle)
				}
				if !supported {
					return 0, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, true
}

// CountResolvedIntegerOrdered evaluates one exact signed-integer ordering
// over FOR streams after resolving the path once per shape. It is all-or-
// nothing at stripe granularity: an overflow bitmap, malformed stream, absent
// target container, or non-FOR target returns ok=false and discards any local
// count so the caller can run the authoritative generic executor.
func (v *CompactPrimaryStripeView) CountResolvedIntegerOrdered(
	resolver *UnifiedHoleResolver,
	needle int64,
	op UnifiedIntegerOrder,
) (matched int, ok bool) {
	if v == nil || resolver == nil || !validUnifiedIntegerOrder(op) ||
		len(v.overflow) != 0 {
		return 0, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return 0, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return 0, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false
			}
			if at == hole {
				var count int
				var supported bool
				if stream.kind == compactStreamRankAffine {
					count, supported = v.rankAffineShapeOrdered(stream, shape, needle, op)
				} else if stream.kind == compactStreamFOR {
					count, supported = stream.countIntegerOrdered(needle, op)
				}
				if !supported {
					return 0, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, true
}

// CountResolvedIntegerInterval evaluates one normalized signed interval over
// every resolved compact target stream. It is all-or-nothing at stripe
// granularity: overflow, malformed data, a container target, or a non-FOR
// target discards the local count and asks the caller to use the generic path.
func (v *CompactPrimaryStripeView) CountResolvedIntegerInterval(
	resolver *UnifiedHoleResolver,
	interval UnifiedIntegerInterval,
) (matched int, ok bool) {
	if v == nil || resolver == nil || len(v.overflow) != 0 {
		return 0, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return 0, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return 0, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false
			}
			if at == hole {
				var count int
				var supported bool
				if stream.kind == compactStreamRankAffine {
					count, supported = v.rankAffineShapeInterval(stream, shape, interval)
				} else if stream.kind == compactStreamFOR {
					count, supported = stream.countIntegerInterval(interval)
				}
				if !supported {
					return 0, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, true
}

// CountResolvedIntegerExtrema reduces one resolved path over exact signed
// integer FOR streams. It resolves the path once per shape and validates the
// complete target stream before publishing any result. Overflow rows,
// malformed geometry, non-FOR targets, width-64 lanes, and signed-wrapping
// FOR spans therefore decline the complete stripe atomically.
func (v *CompactPrimaryStripeView) CountResolvedIntegerExtrema(
	resolver *UnifiedHoleResolver,
) (result UnifiedIntegerExtremaResult, ok bool) {
	if v == nil || resolver == nil || len(v.overflow) != 0 {
		return UnifiedIntegerExtremaResult{}, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return UnifiedIntegerExtremaResult{}, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return UnifiedIntegerExtremaResult{}, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return UnifiedIntegerExtremaResult{}, false
			}
			if at == hole {
				var minimum, maximum int64
				var streamFound, supported bool
				if stream.kind == compactStreamRankAffine {
					minimum, maximum, streamFound, supported = v.rankAffineShapeExtrema(stream, shape)
				} else if stream.kind == compactStreamFOR {
					minimum, maximum, streamFound, supported = stream.countIntegerExtrema()
				}
				if !supported {
					return UnifiedIntegerExtremaResult{}, false
				}
				if streamFound {
					if !result.Found || minimum < result.Min {
						result.Min = minimum
					}
					if !result.Found || maximum > result.Max {
						result.Max = maximum
					}
					result.Found = true
				}
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return result, true
}

// CountResolvedNumberEqual evaluates exact JSON decimal equality over compact
// numeric streams. scratch and ids are caller-owned reusable workspaces and
// are returned on every path so the warmed query remains allocation-free.
func (v *CompactPrimaryStripeView) CountResolvedNumberEqual(
	resolver *UnifiedHoleResolver,
	needle []byte,
	needleInt int64,
	needleIsInt bool,
	scratch []byte,
	ids []uint64,
) (matched int, out []byte, outIDs []uint64, ok bool) {
	if v == nil || resolver == nil || len(v.overflow) != 0 {
		return 0, scratch, ids, false
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, found := v.shapeEntry(shape)
		if !found {
			return 0, scratch, ids, false
		}
		hole := resolver.resolveCompactTemplate(entry.template)
		if hole == UnifiedHoleAbsent {
			continue
		}
		if hole < 0 || hole >= entry.template.holes {
			return 0, scratch, ids, false
		}
		streamRaw := entry.streamRaw
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, scratch, ids, false
			}
			if at == hole {
				var count int
				if stream.kind == compactStreamRankAffine {
					ok = stream.rankAffineIsNumber()
					if needleIsInt {
						count = v.rankAffineShapeEqual(stream, shape, needleInt)
					}
				} else {
					count, scratch, ids, ok = stream.countNumberEqual(
						needle, needleInt, needleIsInt, scratch, ids,
					)
				}
				if !ok {
					return 0, scratch, ids, false
				}
				matched += count
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	return matched, scratch, ids, true
}

// AppendResolvedHole appends one scalar hole without reconstructing the rest
// of the document. supported=false asks the caller to take its container path.
func (v *CompactPrimaryStripeView) AppendResolvedHole(
	dst []byte,
	row int,
	resolver *UnifiedHoleResolver,
) (out []byte, found, supported bool) {
	if v == nil || resolver == nil || row < 0 || row >= v.rows {
		return dst, false, false
	}
	shape := v.rowShape(row)
	entry, ok := v.shapeEntry(shape)
	if !ok {
		return dst, false, false
	}
	hole := resolver.resolveCompactTemplate(entry.template)
	if hole == UnifiedHoleAbsent {
		return dst, false, true
	}
	if hole < 0 || hole >= entry.template.holes {
		return dst, false, false
	}
	ordinal := v.shapeOrdinal(row, shape)
	streamRaw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			return dst, false, false
		}
		if at == hole {
			out, ok = stream.appendValue(dst, stream.shapeCoordinate(row, ordinal))
			return out, ok, ok
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	return dst, false, false
}

func (v *CompactPrimaryStripeView) ResolveHoles(
	dst []int,
	resolver *UnifiedHoleResolver,
) ([]int, bool) {
	if v == nil || resolver == nil {
		return dst, false
	}
	mark := len(dst)
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, ok := v.shapeEntry(shape)
		if !ok {
			return dst[:mark], false
		}
		dst = append(dst, resolver.resolveCompactTemplate(entry.template))
	}
	return dst, true
}
