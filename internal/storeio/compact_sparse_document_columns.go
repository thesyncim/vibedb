package storeio

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

// CompactSparseFloat64Row is one replacement row across every typed column in
// an SDP3 page. Present is a dense bitset over columns and Values contains one
// value per column. Values for missing columns are ignored. This shape lets a
// small document update rewrite its covering tuple without allocating a
// 64-row matrix.
type CompactSparseFloat64Row struct {
	Present []uint64
	Values  []float64
}

func compactSparseFloat64WriteLength(live uint64, columns DocumentFloat64Columns) (int, error) {
	if len(columns.Masks) == 0 {
		if len(columns.Values) != 0 {
			return 0, fmt.Errorf("%w: compact sparse float64 shape", ErrInvalidWrite)
		}
		return 0, nil
	}
	if len(columns.Masks) > math.MaxUint16 ||
		len(columns.Values) != len(columns.Masks)*SparseDocumentPageSlotCount {
		return 0, fmt.Errorf("%w: compact sparse float64 shape", ErrInvalidWrite)
	}
	length := 0
	for column, mask := range columns.Masks {
		if mask&^live != 0 {
			return 0, fmt.Errorf("%w: compact sparse float64 mask", ErrInvalidWrite)
		}
		length += 8 + bits.OnesCount64(mask)*8
		base := column * SparseDocumentPageSlotCount
		for slots := mask; slots != 0; slots &= slots - 1 {
			value := columns.Values[base+bits.TrailingZeros64(slots)]
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return 0, fmt.Errorf("%w: compact sparse non-finite float64", ErrInvalidWrite)
			}
		}
	}
	if length > math.MaxUint16 {
		return 0, fmt.Errorf("%w: compact sparse float64 length", ErrInvalidWrite)
	}
	return length, nil
}

func encodeCompactSparseFloat64Columns(dst []byte, columns DocumentFloat64Columns) int {
	cursor := 0
	for column, mask := range columns.Masks {
		binary.LittleEndian.PutUint64(dst[cursor:cursor+8], mask)
		cursor += 8
		base := column * SparseDocumentPageSlotCount
		for slots := mask; slots != 0; slots &= slots - 1 {
			slot := bits.TrailingZeros64(slots)
			binary.LittleEndian.PutUint64(
				dst[cursor:cursor+8],
				math.Float64bits(columns.Values[base+slot]),
			)
			cursor += 8
		}
	}
	return cursor
}

func openCompactSparseFloat64Columns(page []byte, live uint64, magic uint32) (int, uint16, error) {
	trailer := len(page) - PageTrailerSize
	if magic == compactSparseDocumentPageMagic {
		return trailer, 0, nil
	}
	if magic != compactSparseColumnsPageMagic ||
		trailer < CompactSparseDocumentPageHeapStart+compactSparseColumnsFooterSize {
		return 0, 0, fmt.Errorf("%w: compact float64 magic or geometry", ErrSparseDocumentPageCorrupt)
	}
	footer := trailer - compactSparseColumnsFooterSize
	length := int(binary.LittleEndian.Uint16(page[footer : footer+2]))
	count := binary.LittleEndian.Uint16(page[footer+2 : trailer])
	if count == 0 || length < int(count)*8 ||
		length > footer-CompactSparseDocumentPageHeapStart {
		return 0, 0, fmt.Errorf("%w: compact float64 footer", ErrSparseDocumentPageCorrupt)
	}
	start := footer - length
	cursor := start
	for range int(count) {
		if cursor+8 > footer {
			return 0, 0, fmt.Errorf("%w: compact truncated float64 mask", ErrSparseDocumentPageCorrupt)
		}
		mask := binary.LittleEndian.Uint64(page[cursor : cursor+8])
		cursor += 8
		if mask&^live != 0 {
			return 0, 0, fmt.Errorf("%w: compact float64 mask outside live rows", ErrSparseDocumentPageCorrupt)
		}
		valueBytes := bits.OnesCount64(mask) * 8
		if cursor+valueBytes > footer {
			return 0, 0, fmt.Errorf("%w: compact truncated float64 values", ErrSparseDocumentPageCorrupt)
		}
		for end := cursor + valueBytes; cursor < end; cursor += 8 {
			value := math.Float64frombits(binary.LittleEndian.Uint64(page[cursor : cursor+8]))
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return 0, 0, fmt.Errorf("%w: compact non-finite float64", ErrSparseDocumentPageCorrupt)
			}
		}
	}
	if cursor != footer {
		return 0, 0, fmt.Errorf("%w: compact unreferenced float64 bytes", ErrSparseDocumentPageCorrupt)
	}
	return start, count, nil
}

func admittedCompactSparseFloat64Columns(page []byte, magic uint32) (int, uint16) {
	trailer := len(page) - PageTrailerSize
	if magic != compactSparseColumnsPageMagic {
		return trailer, 0
	}
	footer := trailer - compactSparseColumnsFooterSize
	length := int(binary.LittleEndian.Uint16(page[footer : footer+2]))
	return footer - length, binary.LittleEndian.Uint16(page[footer+2 : trailer])
}

func rewriteCompactSparseFloat64Delete(dst []byte, view CompactSparseDocumentPageView, slot uint8) (int, error) {
	if view.float64Count == 0 {
		return 0, nil
	}
	bit := uint64(1) << slot
	newLength := 0
	float64Start := int(view.float64Start)
	cursor := float64Start
	for range int(view.float64Count) {
		mask := binary.LittleEndian.Uint64(view.page[cursor : cursor+8])
		cursor += 8 + bits.OnesCount64(mask)*8
		newLength += 8 + bits.OnesCount64(mask&^bit)*8
	}
	trailer := len(dst) - PageTrailerSize
	footer := trailer - compactSparseColumnsFooterSize
	newStart := footer - newLength
	if newStart < CompactSparseDocumentPageHeapStart {
		return 0, fmt.Errorf("%w: compact float64 delete geometry", ErrInvalidWrite)
	}
	clear(dst[float64Start:trailer])
	source := float64Start
	target := newStart
	for range int(view.float64Count) {
		mask := binary.LittleEndian.Uint64(view.page[source : source+8])
		source += 8
		values := view.page[source : source+bits.OnesCount64(mask)*8]
		source += len(values)
		newMask := mask &^ bit
		binary.LittleEndian.PutUint64(dst[target:target+8], newMask)
		target += 8
		if mask&bit == 0 {
			copy(dst[target:target+len(values)], values)
			target += len(values)
			continue
		}
		rank := bits.OnesCount64(mask & (bit - 1))
		prefix := rank * 8
		copy(dst[target:target+prefix], values[:prefix])
		target += prefix
		copy(dst[target:], values[prefix+8:])
		target += len(values) - prefix - 8
	}
	if target != footer {
		return 0, fmt.Errorf("%w: compact float64 delete length", ErrInvalidWrite)
	}
	binary.LittleEndian.PutUint16(dst[footer:footer+2], uint16(newLength))
	binary.LittleEndian.PutUint16(dst[footer+2:trailer], view.float64Count)
	return float64Start, nil
}

func validateCompactSparseFloat64Row(columns int, update CompactSparseFloat64Row) error {
	words := (columns + 63) / 64
	if columns == 0 || len(update.Present) != words || len(update.Values) != columns {
		return fmt.Errorf("%w: compact sparse float64 row shape", ErrInvalidWrite)
	}
	if trailing := columns & 63; trailing != 0 &&
		update.Present[words-1]&^(uint64(1)<<trailing-1) != 0 {
		return fmt.Errorf("%w: compact sparse float64 row trailing bits", ErrInvalidWrite)
	}
	for column, value := range update.Values {
		if update.Present[column>>6]&(uint64(1)<<uint(column&63)) != 0 &&
			(math.IsNaN(value) || math.IsInf(value, 0)) {
			return fmt.Errorf("%w: compact sparse non-finite float64 row", ErrInvalidWrite)
		}
	}
	return nil
}

// PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64 updates one document
// and its exact covering tuple in a single checksum-valid after-image. Other
// rows and their raw IEEE-754 bit patterns are copied exactly. The caller owns
// all buffers and the operation allocates nothing.
func PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(dst []byte, view CompactSparseDocumentPageView, row DocumentRecord, update CompactSparseFloat64Row, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	if err := validateCompactSparseFloat64Row(int(view.float64Count), update); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	plan, err := planAdmittedCompactSparseDocumentPageUpdateMode(
		dst, view, row, options, workspace, false,
	)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	if err := rewriteCompactSparseFloat64Row(plan.After, view, row.Slot, update); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	if _, err := SealPage(plan.After); err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return sparseDocumentChangedPlan(plan.After, view.page, options.SectorSize, workspace)
}

// PlanCompactSparseDocumentPageUpdateWithFloat64 is the checksum-verifying
// form of PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64.
func PlanCompactSparseDocumentPageUpdateWithFloat64(dst, src []byte, row DocumentRecord, update CompactSparseFloat64Row, options SparseDocumentMutationOptions, workspace *SparseDocumentWorkspace) (SparseDocumentMutationPlan, error) {
	view, err := openCompactSparseDocumentForMutation(src, options)
	if err != nil {
		return SparseDocumentMutationPlan{}, err
	}
	return PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
		dst, view, row, update, options, workspace,
	)
}

func rewriteCompactSparseFloat64Row(dst []byte, view CompactSparseDocumentPageView, slot uint8, update CompactSparseFloat64Row) error {
	bit := uint64(1) << slot
	newLength := 0
	float64Start := int(view.float64Start)
	source := float64Start
	for column := 0; column < int(view.float64Count); column++ {
		mask := binary.LittleEndian.Uint64(view.page[source : source+8])
		source += 8 + bits.OnesCount64(mask)*8
		present := update.Present[column>>6]&(uint64(1)<<uint(column&63)) != 0
		newMask := mask &^ bit
		if present {
			newMask |= bit
		}
		newLength += 8 + bits.OnesCount64(newMask)*8
	}
	trailer := len(dst) - PageTrailerSize
	footer := trailer - compactSparseColumnsFooterSize
	newStart := footer - newLength
	if newStart < CompactSparseDocumentPageHeapStart ||
		compactSparseDocumentMaxRecordEnd(dst, int(view.count)) > newStart {
		return ErrSparseDocumentPageNoSpace
	}
	clear(dst[min(float64Start, newStart):trailer])
	source = float64Start
	target := newStart
	for column := 0; column < int(view.float64Count); column++ {
		mask := binary.LittleEndian.Uint64(view.page[source : source+8])
		source += 8
		values := view.page[source : source+bits.OnesCount64(mask)*8]
		source += len(values)
		present := update.Present[column>>6]&(uint64(1)<<uint(column&63)) != 0
		newMask := mask &^ bit
		if present {
			newMask |= bit
		}
		binary.LittleEndian.PutUint64(dst[target:target+8], newMask)
		target += 8
		for slots := newMask; slots != 0; slots &= slots - 1 {
			current := uint8(bits.TrailingZeros64(slots))
			if current == slot {
				binary.LittleEndian.PutUint64(
					dst[target:target+8], math.Float64bits(update.Values[column]),
				)
			} else {
				rank := bits.OnesCount64(mask & (uint64(1)<<current - 1))
				copy(dst[target:target+8], values[rank*8:rank*8+8])
			}
			target += 8
		}
	}
	if target != footer {
		return fmt.Errorf("%w: compact sparse float64 row length", ErrInvalidWrite)
	}
	binary.LittleEndian.PutUint16(dst[footer:footer+2], uint16(newLength))
	binary.LittleEndian.PutUint16(dst[footer+2:trailer], view.float64Count)
	return nil
}

func compactSparseDocumentMaxRecordEnd(page []byte, count int) int {
	maxEnd := CompactSparseDocumentPageHeapStart
	for rank := range count {
		start, keyLength, valueLength := compactSparseDocumentDescriptor(page, rank)
		if valueLength == 0 {
			valueLength = DocumentOverflowDescriptorSize
		}
		maxEnd = max(maxEnd, start+keyLength+valueLength)
	}
	return maxEnd
}
