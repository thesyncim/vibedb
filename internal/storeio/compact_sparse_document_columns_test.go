package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"unsafe"
)

func compactSparseFloat64TestColumns(live uint64) DocumentFloat64Columns {
	masks := []uint64{
		live & 0x5555555555555555,
		live & ((uint64(1) << 0) | (uint64(1) << 26) | (uint64(1) << 63)),
		0,
	}
	values := make([]float64, len(masks)*SparseDocumentPageSlotCount)
	for slot := 0; slot < SparseDocumentPageSlotCount; slot++ {
		values[slot] = float64(slot) + 0.25
	}
	values[SparseDocumentPageSlotCount+0] = math.SmallestNonzeroFloat64
	values[SparseDocumentPageSlotCount+26] = math.Copysign(0, -1)
	values[SparseDocumentPageSlotCount+63] = math.MaxFloat64
	return DocumentFloat64Columns{Masks: masks, Values: values}
}

func encodeCompactSparseDocumentColumnsTestPage(tb testing.TB) ([]byte, DocumentPageHeader, [SparseDocumentPageSlotCount]DocumentRecord, DocumentFloat64Columns) {
	tb.Helper()
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))
	columns := compactSparseFloat64TestColumns(header.Live)
	page := make([]byte, header.PageSize)
	if _, err := EncodeCompactSparseDocumentPageWithColumns(
		page, header, rows[:], columns, testDocumentNextLogicalID,
		uint64(32*testSuperblockPageSize), testSuperblockPageSize,
	); err != nil {
		tb.Fatal(err)
	}
	return page, header, rows, columns
}

func TestCompactSparseDocumentPageFloat64ColumnsExactRoundTrip(t *testing.T) {
	page, header, rows, columns := encodeCompactSparseDocumentColumnsTestPage(t)
	if got := binary.LittleEndian.Uint32(page[PageHeaderSize : PageHeaderSize+4]); got != compactSparseColumnsPageMagic {
		t.Fatalf("magic = %08x, want SDP3", got)
	}
	view, err := OpenCompactSparseDocumentPage(
		page, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Float64ColumnCount() != len(columns.Masks) {
		t.Fatalf("columns = %d, want %d", view.Float64ColumnCount(), len(columns.Masks))
	}
	automatic, err := OpenSparseDocumentPageV2(
		page, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil || automatic.Format() != SparseDocumentDescriptorCompact4 ||
		automatic.Float64ColumnCount() != len(columns.Masks) {
		t.Fatalf("automatic view = (format=%d columns=%d err=%v)",
			automatic.Format(), automatic.Float64ColumnCount(), err)
	}
	if metadata := CompactSparseDocumentPageHeapStart - PageHeaderSize; metadata != 272 {
		t.Fatalf("document metadata = %d, want 272 (4.25 B/row)", metadata)
	}
	trailer := len(page) - PageTrailerSize
	footer := trailer - compactSparseColumnsFooterSize
	columnBytes := int(binary.LittleEndian.Uint16(page[footer : footer+2]))
	wantColumnBytes := 0
	for _, mask := range columns.Masks {
		wantColumnBytes += 8 + 8*bitsOnesCount64ForTest(mask)
	}
	if columnBytes != wantColumnBytes {
		t.Fatalf("column bytes = %d, want %d", columnBytes, wantColumnBytes)
	}
	if overhead := columnBytes + compactSparseColumnsFooterSize; overhead != 308 {
		t.Fatalf("fixture sidecar = %d bytes, want 308", overhead)
	}

	for column, wantMask := range columns.Masks {
		got, ok := view.Float64Column(column)
		if !ok || got.Mask() != wantMask {
			t.Fatalf("column %d = (mask=%016x,%v), want %016x", column, got.Mask(), ok, wantMask)
		}
		for slot := uint8(0); slot < SparseDocumentPageSlotCount; slot++ {
			value, present := got.Lookup(slot)
			wantPresent := wantMask&(uint64(1)<<slot) != 0
			if present != wantPresent {
				t.Fatalf("column %d slot %d present=%v, want %v", column, slot, present, wantPresent)
			}
			if present {
				want := columns.Values[column*SparseDocumentPageSlotCount+int(slot)]
				if math.Float64bits(value) != math.Float64bits(want) {
					t.Fatalf("column %d slot %d bits=%016x, want %016x",
						column, slot, math.Float64bits(value), math.Float64bits(want))
				}
			}
		}
	}
	if _, ok := view.Float64Column(-1); ok {
		t.Fatal("negative column succeeded")
	}
	if _, ok := view.Float64Column(len(columns.Masks)); ok {
		t.Fatal("past-end column succeeded")
	}
	for slot, want := range rows {
		got, ok := view.LookupJSON(uint8(slot))
		if !ok || !bytes.Equal(got, want.JSON) {
			t.Fatalf("JSON slot %d = (%q,%v), want %q", slot, got, ok, want.JSON)
		}
	}
}

func TestCompactSparseDocumentPageFloat64OverflowParity(t *testing.T) {
	const slot = 7
	header := testDocumentPageHeader(uint64(1) << slot)
	row := DocumentRecord{
		Slot: slot, Key: []byte("overflow"),
		Overflow: testOverflowRef(20, 20, header.Generation), JSONLength: 1 << 30,
	}
	columns := DocumentFloat64Columns{
		Masks:  []uint64{uint64(1) << slot},
		Values: make([]float64, SparseDocumentPageSlotCount),
	}
	columns.Values[slot] = math.Copysign(0, -1)
	page := make([]byte, header.PageSize)
	fileEnd := uint64(32 * testSuperblockPageSize)
	if _, err := EncodeCompactSparseDocumentPageWithColumns(
		page, header, []DocumentRecord{row}, columns,
		testDocumentNextLogicalID, fileEnd, testSuperblockPageSize,
	); err != nil {
		t.Fatal(err)
	}
	view, err := OpenCompactSparseDocumentPageWithOverflow(
		page, header.ChunkID+1, testDocumentNextLogicalID,
		fileEnd, testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := view.Lookup(slot)
	column, columnOK := view.Float64Column(0)
	value, valueOK := column.Lookup(slot)
	if !ok || got.Overflow != row.Overflow || got.JSONLength != row.JSONLength ||
		!columnOK || !valueOK || math.Float64bits(value) != math.Float64bits(columns.Values[slot]) {
		t.Fatalf("overflow/column parity = row(%+v,%v) column(%016x,%v,%v)",
			got, ok, math.Float64bits(value), columnOK, valueOK)
	}
	if _, err := OpenCompactSparseDocumentPage(
		page, header.ChunkID+1, testDocumentNextLogicalID,
	); !errors.Is(err, ErrSparseDocumentPageCorrupt) {
		t.Fatalf("non-overflow open = %v, want corrupt", err)
	}
}

func TestCompactSparseDocumentPageFloat64MutationParity(t *testing.T) {
	const slot = uint8(26)
	page, header, rows, columns := encodeCompactSparseDocumentColumnsTestPage(t)
	view := AdmittedCompactSparseDocumentPage(page)
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	var workspace SparseDocumentWorkspace
	updated := rows[slot]
	updated.JSON = bytes.Repeat([]byte{'u'}, len(updated.JSON))

	preserved, err := PlanAdmittedCompactSparseDocumentPageUpdate(
		make([]byte, len(page)), view, updated, options, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	preservedView, err := OpenCompactSparseDocumentPage(
		preserved.After, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	requireCompactSparseColumnsEqual(t, view, preservedView, columns.Masks, -1)

	replacement := CompactSparseFloat64Row{
		Present: []uint64{(uint64(1) << 0) | (uint64(1) << 2)},
		Values:  []float64{math.Copysign(0, -1), 0, math.MaxFloat64},
	}
	replaced, err := PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
		make([]byte, len(page)), view, updated, replacement, options, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	replacedView, err := OpenCompactSparseDocumentPage(
		replaced.After, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	for column := range columns.Masks {
		got, _ := replacedView.Float64Column(column)
		wantPresent := replacement.Present[0]&(uint64(1)<<uint(column)) != 0
		value, present := got.Lookup(slot)
		if present != wantPresent {
			t.Fatalf("updated column %d present=%v, want %v", column, present, wantPresent)
		}
		if present && math.Float64bits(value) != math.Float64bits(replacement.Values[column]) {
			t.Fatalf("updated column %d bits=%016x, want %016x",
				column, math.Float64bits(value), math.Float64bits(replacement.Values[column]))
		}
	}
	requireCompactSparseColumnsEqual(t, view, replacedView, columns.Masks, int(slot))

	deleted, err := PlanAdmittedCompactSparseDocumentPageDelete(
		make([]byte, len(page)), view, slot, options, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	deletedView, err := OpenCompactSparseDocumentPage(
		deleted.After, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	for column, mask := range columns.Masks {
		got, _ := deletedView.Float64Column(column)
		if got.Mask() != mask&^(uint64(1)<<slot) {
			t.Fatalf("deleted column %d mask=%016x, want %016x",
				column, got.Mask(), mask&^(uint64(1)<<slot))
		}
		if _, ok := got.Lookup(slot); ok {
			t.Fatalf("deleted column %d retained slot %d", column, slot)
		}
	}
	requireCompactSparseColumnsEqual(t, view, deletedView, columns.Masks, int(slot))
}

func requireCompactSparseColumnsEqual(tb testing.TB, before, after CompactSparseDocumentPageView, masks []uint64, ignoredSlot int) {
	tb.Helper()
	for column, mask := range masks {
		left, leftOK := before.Float64Column(column)
		right, rightOK := after.Float64Column(column)
		if !leftOK || !rightOK {
			tb.Fatalf("column %d missing: before=%v after=%v", column, leftOK, rightOK)
		}
		for slot := uint8(0); slot < SparseDocumentPageSlotCount; slot++ {
			if int(slot) == ignoredSlot || mask&(uint64(1)<<slot) == 0 {
				continue
			}
			leftValue, leftPresent := left.Lookup(slot)
			rightValue, rightPresent := right.Lookup(slot)
			if !leftPresent || !rightPresent ||
				math.Float64bits(leftValue) != math.Float64bits(rightValue) {
				tb.Fatalf("column %d slot %d changed: (%016x,%v) -> (%016x,%v)",
					column, slot, math.Float64bits(leftValue), leftPresent,
					math.Float64bits(rightValue), rightPresent)
			}
		}
	}
}

func TestCompactSparseDocumentPageFloat64RejectsInvalidAndResealedCorruption(t *testing.T) {
	page, header, rows, columns := encodeCompactSparseDocumentColumnsTestPage(t)
	invalid := columns
	invalid.Values = append([]float64(nil), columns.Values...)
	invalid.Values[0] = math.Inf(1)
	if _, err := EncodeCompactSparseDocumentPageWithColumns(
		make([]byte, header.PageSize), header, rows[:], invalid,
		testDocumentNextLogicalID, uint64(32*testSuperblockPageSize), testSuperblockPageSize,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("non-finite encode = %v, want invalid write", err)
	}
	view := AdmittedCompactSparseDocumentPage(page)
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	var workspace SparseDocumentWorkspace
	badRow := CompactSparseFloat64Row{
		Present: []uint64{1},
		Values:  []float64{math.Inf(-1), 0, 0},
	}
	if _, err := PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
		make([]byte, len(page)), view, rows[0], badRow, options, &workspace,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("non-finite row update = %v, want invalid write", err)
	}
	badRow = CompactSparseFloat64Row{
		Present: []uint64{uint64(1) << 63},
		Values:  []float64{1, 2, 3},
	}
	if _, err := PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
		make([]byte, len(page)), view, rows[0], badRow, options, &workspace,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("row trailing bits = %v, want invalid write", err)
	}

	trailer := len(page) - PageTrailerSize
	footer := trailer - compactSparseColumnsFooterSize
	start := footer - int(binary.LittleEndian.Uint16(page[footer:footer+2]))
	for name, mutate := range map[string]func([]byte){
		"zero count": func(corrupt []byte) {
			clear(corrupt[footer+2 : trailer])
		},
		"length beyond heap": func(corrupt []byte) {
			binary.LittleEndian.PutUint16(corrupt[footer:footer+2], math.MaxUint16)
		},
		"mask outside live": func(corrupt []byte) {
			binary.LittleEndian.PutUint64(corrupt[start:start+8], ^header.Live)
		},
		"non-finite value": func(corrupt []byte) {
			binary.LittleEndian.PutUint64(corrupt[start+8:start+16], math.Float64bits(math.NaN()))
		},
		"row overlaps sidecar": func(corrupt []byte) {
			_, keyLength, valueLength := compactSparseDocumentDescriptor(corrupt, 0)
			first := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
			binary.LittleEndian.PutUint32(
				corrupt[first:first+4],
				packCompactSparseDocumentDescriptor(start-keyLength-valueLength+1, keyLength, valueLength),
			)
		},
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := bytes.Clone(page)
			mutate(corrupt)
			if _, err := SealPage(corrupt); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenCompactSparseDocumentPage(
				corrupt, header.ChunkID+1, testDocumentNextLogicalID,
			); !errors.Is(err, ErrSparseDocumentPageCorrupt) {
				t.Fatalf("open = %v, want corrupt", err)
			}
		})
	}
}

func TestCompactSparseDocumentPageFloat64SteadyAllocationAndScratchBound(t *testing.T) {
	const slot = uint8(26)
	page, header, rows, _ := encodeCompactSparseDocumentColumnsTestPage(t)
	view := AdmittedCompactSparseDocumentPage(page)
	after := make([]byte, len(page))
	updated := rows[slot]
	updated.JSON = bytes.Repeat([]byte{'a'}, len(updated.JSON))
	replacement := CompactSparseFloat64Row{
		Present: []uint64{5},
		Values:  []float64{math.Copysign(0, -1), 0, math.MaxFloat64},
	}
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	var workspace SparseDocumentWorkspace
	if got := unsafe.Sizeof(workspace); got != 1408 {
		t.Fatalf("workspace = %d bytes, want bounded 1408", got)
	}
	type compactSparsePointReadLayout struct {
		header DocumentPageHeader
		page   []byte
		count  uint8
	}
	if got, withoutColumns := unsafe.Sizeof(view), unsafe.Sizeof(compactSparsePointReadLayout{}); got != withoutColumns {
		t.Fatalf("column metadata grew hot view from %d to %d bytes", withoutColumns, got)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		json, ok := view.LookupJSON(slot)
		column, columnOK := view.Float64Column(0)
		value, valueOK := column.Lookup(slot)
		if !ok || len(json) == 0 || !columnOK || !valueOK || value == 0 {
			panic("point lookup failed")
		}
		if _, err := PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
			after, view, updated, replacement, options, &workspace,
		); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("column point/update allocations = %g, want zero", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := PlanAdmittedCompactSparseDocumentPageDelete(
			after, view, slot, options, &workspace,
		); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("column delete allocations = %g, want zero", allocs)
	}
}

func bitsOnesCount64ForTest(value uint64) int {
	count := 0
	for value != 0 {
		value &= value - 1
		count++
	}
	return count
}
