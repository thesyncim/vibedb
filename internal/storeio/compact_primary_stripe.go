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
	compactPrimaryHeaderBytes   = 32
	compactPrimaryShapeHeader   = 16
	compactPrimaryMagic         = "VCS1"
	compactPrimaryHasOverflow   = 1 << 0
)

type compactPrimaryBuildScratch struct {
	payload    []byte
	shapeRows  [][]int
	shapeCodes []byte
	keys       [][]byte
	counts     []uint16
	columns    [][][]byte
	overflow   []byte
	stream     compactStreamScratch
}

// BuildCompactPrimaryStripePayload builds the replacement class-5 payload.
// It is exposed only inside storeio so the graph planner can measure a prefix
// before reserving its exact 4 KiB-rounded extent. Records are borrowed; the
// returned payload is borrowed from builder until its next compact build.
func BuildCompactPrimaryStripePayload(
	records []CommonPrimaryLeafRecord,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if builder == nil || len(records) > CompactPrimaryStripeMaxRows {
		return nil, fmt.Errorf("%w: compact stripe input", ErrInvalidWrite)
	}
	for row := range records {
		if len(records[row].Key) == 0 || len(records[row].Key) > CommonPrimaryLeafMaxKeyBytes ||
			(records[row].Value.IsOverflow() == (len(records[row].Value.Inline) != 0)) ||
			row != 0 && bytes.Compare(records[row-1].Key, records[row].Key) >= 0 {
			return nil, fmt.Errorf("%w: compact stripe record", ErrInvalidWrite)
		}
	}
	if err := builder.extract(records); err != nil {
		return nil, err
	}
	shapeCount := len(builder.shapes)
	if shapeCount > len(records) ||
		shapeCount > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: compact stripe shapes", ErrInvalidWrite)
	}
	overflowCount := 0
	for row := range records {
		if records[row].Value.IsOverflow() {
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
	shapeCodeBytes := (len(records)*shapeWidth + 7) / 8
	restarts := (len(records) + compactStreamRestart - 1) / compactStreamRestart
	rankBytes64 := uint64(restarts) * uint64(shapeCount) * 2
	if rankBytes64 > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: compact stripe rank checkpoints", ErrInvalidWrite)
	}
	rankBytes := int(rankBytes64)

	scratch := &builder.compact
	scratch.shapeRows = slices.Grow(scratch.shapeRows[:0], shapeCount)[:shapeCount]
	for shape := range scratch.shapeRows {
		scratch.shapeRows[shape] = scratch.shapeRows[shape][:0]
	}
	shapeRows := scratch.shapeRows
	scratch.shapeCodes = slices.Grow(scratch.shapeCodes[:0], shapeCodeBytes)[:shapeCodeBytes]
	clear(scratch.shapeCodes)
	shapeCodes := scratch.shapeCodes
	for row := range builder.rows {
		shape := int(builder.rows[row].shape)
		if shape < 0 {
			if !records[row].Value.IsOverflow() {
				return nil, fmt.Errorf("%w: compact stripe row shape", ErrInvalidWrite)
			}
			compactPutBits(shapeCodes, row*shapeWidth, shapeWidth, uint64(shapeCount))
			continue
		}
		if shape >= shapeCount {
			return nil, fmt.Errorf("%w: compact stripe row shape", ErrInvalidWrite)
		}
		compactPutBits(shapeCodes, row*shapeWidth, shapeWidth, uint64(shape))
		shapeRows[shape] = append(shapeRows[shape], row)
	}

	scratch.keys = slices.Grow(scratch.keys[:0], len(records))[:len(records)]
	keys := scratch.keys
	for row := range records {
		keys[row] = records[row].Key
	}

	headerBytes := compactPrimaryHeaderBytes + 4*shapeCount
	scratch.payload = slices.Grow(scratch.payload[:0], headerBytes)[:headerBytes]
	clear(scratch.payload)
	payload := scratch.payload
	copy(payload, compactPrimaryMagic)
	binary.LittleEndian.PutUint32(payload[4:], uint32(len(records)))
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
	if len(records) <= CommonPrimaryLeafWideSlots {
		anyNonZero := false
		for row := range records {
			anyNonZero = anyNonZero || records[row].Slot != 0
		}
		writeSlots := false
		if anyNonZero {
			for row := range records {
				if records[row].Slot != uint8(row) {
					writeSlots = true
					break
				}
			}
		}
		if writeSlots {
			var used [4]uint64
			for row := range records {
				slot := records[row].Slot
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
		bitmapBytes := (len(records) + 7) / 8
		scratch.overflow = slices.Grow(scratch.overflow[:0], bitmapBytes)[:bitmapBytes]
		clear(scratch.overflow)
		for row := range records {
			if records[row].Value.IsOverflow() {
				scratch.overflow[row>>3] |= byte(1) << uint(row&7)
			}
		}
		payload = append(payload, scratch.overflow...)
		for row := range records {
			if records[row].Value.IsOverflow() {
				start := len(payload)
				payload = append(payload, make([]byte, PageRefSize)...)
				encodePageRef(payload[start:], records[row].Value.Overflow)
			}
		}
	}
	payload = append(payload, shapeCodes...)

	scratch.counts = slices.Grow(scratch.counts[:0], shapeCount)[:shapeCount]
	clear(scratch.counts)
	counts := scratch.counts
	for block := 0; block < restarts; block++ {
		for shape := range shapeCount {
			var encoded [2]byte
			binary.LittleEndian.PutUint16(encoded[:], counts[shape])
			payload = append(payload, encoded[:]...)
		}
		first := block * compactStreamRestart
		last := min(first+compactStreamRestart, len(records))
		for row := first; row < last; row++ {
			shape := int(compactReadBits(shapeCodes, row*shapeWidth, shapeWidth))
			if shape < shapeCount {
				counts[shape]++
			}
		}
	}

	shapeDataStart := len(payload)
	for shape := range shapeCount {
		entryStart := len(payload)
		payload = append(payload, make([]byte, compactPrimaryShapeHeader)...)
		plan := &builder.shapes[shape]
		binary.LittleEndian.PutUint32(payload[entryStart:], uint32(len(shapeRows[shape])))
		binary.LittleEndian.PutUint16(payload[entryStart+4:], uint16(plan.holes))

		templateStart := len(payload)
		payload = appendCompactPrimaryTemplate(payload, builder, plan)
		templateBytes := len(payload) - templateStart
		if templateBytes != plan.entryBytes {
			return nil, fmt.Errorf("%w: compact stripe template drift", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint32(payload[entryStart+8:], uint32(templateBytes))

		streamsStart := len(payload)
		scratch.columns = slices.Grow(scratch.columns[:0], plan.holes)[:plan.holes]
		for hole := range scratch.columns {
			scratch.columns[hole] = scratch.columns[hole][:0]
		}
		columns := scratch.columns
		for _, rowIndex := range shapeRows[shape] {
			row := &builder.rows[rowIndex]
			canonical := builder.canonicalOf(rowIndex)
			for hole, span := range builder.spans[row.spanStart:row.spanEnd] {
				columns[hole] = append(columns[hole], canonical[span.Start:span.End])
			}
		}
		for hole := range columns {
			stream := scratch.stream.encode(columns[hole])
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
	binary.LittleEndian.PutUint32(payload[24:], uint32(len(payload)-shapeDataStart))
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
	header      CommonPrimaryLeafHeader
	payload     []byte
	rows        int
	shapeCount  int
	shapeWidth  int
	key         compactStreamView
	shapeCodes  []byte
	rankTable   []byte
	shapeDir    []byte
	slots       []byte
	overflow    []byte
	overflowRef []byte
	shapeData   []byte
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
	return openCompactPrimaryStripePayload(
		payload, pageHeader, storeID, bucket, bounds, true,
		expected.Offset < bounds.FileEnd,
	)
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
		uint64(shapeCodeBytes) + uint64(rankBytes) + uint64(shapeBytes)
	if shapeCodeBytes != wantShapeCodes || rankBytes != wantRanks ||
		(slotBytes != 0 && (slotBytes != rows || rows > CommonPrimaryLeafWideSlots)) ||
		fixed64 != uint64(len(payload)) {
		return corrupt("section lengths")
	}
	shapeDir := payload[compactPrimaryHeaderBytes : compactPrimaryHeaderBytes+4*shapeCount]
	cursor := compactPrimaryHeaderBytes + 4*shapeCount
	key, err := openCompactStream(payload[cursor : cursor+keyBytes])
	if err != nil || key.encoded != keyBytes || key.count != rows {
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
	shapeData := payload[cursor:]
	v := CompactPrimaryStripeView{
		header: CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		payload: payload, rows: rows, shapeCount: shapeCount, shapeWidth: shapeWidth,
		key: key, slots: slots, overflow: overflowBitmap, overflowRef: overflowRefs,
		shapeCodes: shapeCodes, rankTable: rankTable,
		shapeDir: shapeDir, shapeData: shapeData,
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
			if err != nil || stream.count != entry.rows {
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
	return int(compactReadBits(v.shapeCodes, row*v.shapeWidth, v.shapeWidth))
}

func (v *CompactPrimaryStripeView) shapeOrdinal(row, shape int) int {
	block := row / compactStreamRestart
	ordinal := int(binary.LittleEndian.Uint16(v.rankTable[(block*v.shapeCount+shape)*2:]))
	for at := block * compactStreamRestart; at < row; at++ {
		if v.rowShape(at) == shape {
			ordinal++
		}
	}
	return ordinal
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
	shape := v.rowShape(row)
	entry, ok := v.shapeEntry(shape)
	if !ok {
		return dst, false
	}
	ordinal := v.shapeOrdinal(row, shape)
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
		dst, ok = stream.appendValue(dst, ordinal)
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
			out, ok = stream.appendValue(dst, ordinal)
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
