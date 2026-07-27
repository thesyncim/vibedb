package storeio

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

const testDocumentNextLogicalID = uint64(1000)

func testDocumentPageHeader(live uint64) DocumentPageHeader {
	return DocumentPageHeader{
		StoreID:    testStoreID,
		Generation: 11,
		LogicalID:  2,
		PageSize:   testSuperblockPageSize,
		ChunkID:    7,
		Live:       live,
	}
}

func testDocumentRows() []DocumentRecord {
	return []DocumentRecord{
		{Slot: 0, Key: []byte(""), JSON: []byte(`null`)},
		{Slot: 5, Key: []byte("session:5"), JSON: []byte(`{"state":"open"}`)},
		{Slot: 63, Key: []byte("session:63"), JSON: []byte(`[1,true,"x"]`)},
	}
}

func encodeTestDocumentPage(t *testing.T, header DocumentPageHeader, rows []DocumentRecord) []byte {
	t.Helper()
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeDocumentPage(page, header, rows, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestDocumentPageSparseRoundTripAndLookup(t *testing.T) {
	const live = uint64(1)<<0 | uint64(1)<<5 | uint64(1)<<63
	header := testDocumentPageHeader(live)
	rows := testDocumentRows()
	page := encodeTestDocumentPage(t, header, rows)
	view, err := OpenDocumentPage(page, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.Header(); got != header {
		t.Fatalf("Header = %+v, want %+v", got, header)
	}
	if got := view.Len(); got != len(rows) {
		t.Fatalf("Len = %d, want %d", got, len(rows))
	}
	for rank, want := range rows {
		for _, byRank := range []bool{false, true} {
			var got DocumentRecord
			var ok bool
			if byRank {
				got, ok = view.RecordAt(rank)
			} else {
				got, ok = view.Lookup(want.Slot)
			}
			if !ok || got.Slot != want.Slot || string(got.Key) != string(want.Key) || string(got.JSON) != string(want.JSON) {
				t.Fatalf("row %d rank=%v = (%+v,%v), want %+v", rank, byRank, got, ok, want)
			}
			if cap(got.Key) != len(got.Key) || cap(got.JSON) != len(got.JSON) {
				t.Fatalf("row %d exposes spare capacity: key=%d/%d json=%d/%d", rank, len(got.Key), cap(got.Key), len(got.JSON), cap(got.JSON))
			}
		}
		json, ok := view.LookupJSON(want.Slot)
		if !ok || string(json) != string(want.JSON) || cap(json) != len(json) {
			t.Fatalf("LookupJSON(%d) = (%q,%v), want (%q,true)", want.Slot, json, ok, want.JSON)
		}
		json, ok = view.LookupKey(want.Slot, want.Key)
		if !ok || string(json) != string(want.JSON) {
			t.Fatalf("LookupKey(%d) = (%q,%v), want (%q,true)", want.Slot, json, ok, want.JSON)
		}
		json, ok = view.LookupString(want.Slot, string(want.Key))
		if !ok || string(json) != string(want.JSON) {
			t.Fatalf("LookupString(%d) = (%q,%v), want (%q,true)", want.Slot, json, ok, want.JSON)
		}
		wrong := append([]byte(nil), want.Key...)
		wrong = append(wrong, '!')
		if json, ok := view.LookupKey(want.Slot, wrong); ok {
			t.Fatalf("LookupKey(%d, wrong) = (%q,true), want miss", want.Slot, json)
		}
	}
	for _, slot := range []uint8{1, 6, 62, 64, 255} {
		if got, ok := view.Lookup(slot); ok {
			t.Fatalf("Lookup(%d) = (%+v,true), want miss", slot, got)
		}
		if json, ok := view.LookupJSON(slot); ok {
			t.Fatalf("LookupJSON(%d) = (%q,true), want miss", slot, json)
		}
		if json, ok := view.LookupString(slot, "missing"); ok {
			t.Fatalf("LookupString(%d) = (%q,true), want miss", slot, json)
		}
	}
	for _, rank := range []int{-1, len(rows)} {
		if got, ok := view.RecordAt(rank); ok {
			t.Fatalf("RecordAt(%d) = (%+v,true), want miss", rank, got)
		}
	}

	_, payload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	wantData := 0
	for _, row := range rows {
		wantData += len(row.Key) + len(row.JSON)
	}
	wantLength := DocumentPagePayloadHeaderSize + len(rows)*DocumentPageRecordSize + wantData
	if len(payload) != wantLength || cap(payload) != wantLength {
		t.Fatalf("payload = len %d cap %d, want %d", len(payload), cap(payload), wantLength)
	}
}

func TestDocumentPageAllStableSlots(t *testing.T) {
	header := testDocumentPageHeader(^uint64(0))
	rows := make([]DocumentRecord, 64)
	for slot := range rows {
		rows[slot] = DocumentRecord{Slot: uint8(slot), Key: []byte{byte(slot)}, JSON: []byte(`0`)}
	}
	page := encodeTestDocumentPage(t, header, rows)
	view, err := OpenDocumentPage(page, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	for slot := range rows {
		got, ok := view.Lookup(uint8(slot))
		if !ok || got.Slot != uint8(slot) || len(got.Key) != 1 || got.Key[0] != byte(slot) || string(got.JSON) != "0" {
			t.Fatalf("slot %d = (%+v,%v)", slot, got, ok)
		}
	}
}

func TestDocumentPageExactQuantumExtents(t *testing.T) {
	const live = uint64(1)
	rows := []DocumentRecord{{
		Slot: 0, Key: []byte("exact"), JSON: []byte(`{"v":1}`),
	}}
	fileEnd := uint64(64 * testSuperblockPageSize)
	for _, pages := range []uint32{3, 5, 7} {
		header := testDocumentPageHeader(live)
		header.PageSize = pages * testSuperblockPageSize
		page := make([]byte, header.PageSize)
		encoded, err := EncodeDocumentPageWithColumns(
			page, header, rows, DocumentFloat64Columns{},
			testDocumentNextLogicalID, fileEnd, testSuperblockPageSize,
		)
		if err != nil {
			t.Fatalf("%d-page EncodeDocumentPageWithColumns: %v", pages, err)
		}
		view, err := OpenDocumentPageWithOverflow(
			encoded, header.ChunkID+1, testDocumentNextLogicalID,
			fileEnd, testSuperblockPageSize,
		)
		if err != nil {
			t.Fatalf("%d-page OpenDocumentPageWithOverflow: %v", pages, err)
		}
		got, ok := view.LookupString(0, "exact")
		if !ok || string(got) != `{"v":1}` ||
			view.Header().PageSize != header.PageSize {
			t.Fatalf("%d-page lookup = (%q,%v), header %+v",
				pages, got, ok, view.Header())
		}
	}
}

func TestDocumentPageFloat64ColumnsSparseRoundTrip(t *testing.T) {
	const live = uint64(1)<<0 | uint64(1)<<5 | uint64(1)<<63
	header := testDocumentPageHeader(live)
	rows := testDocumentRows()
	masks := []uint64{
		uint64(1)<<0 | uint64(1)<<63,
		uint64(1) << 5,
	}
	values := make([]float64, len(masks)*64)
	values[0] = -12.5
	values[63] = 1.25e100
	values[64+5] = 42
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeDocumentPageWithColumns(
		page, header, rows, DocumentFloat64Columns{Masks: masks, Values: values},
		testDocumentNextLogicalID, uint64(32*testSuperblockPageSize), testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenDocumentPageWithOverflow(
		encoded, header.ChunkID+1, testDocumentNextLogicalID,
		uint64(32*testSuperblockPageSize), testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := OpenAdmittedDocumentPageWithOverflow(
		encoded, header.ChunkID+1, testDocumentNextLogicalID,
		uint64(32*testSuperblockPageSize), testSuperblockPageSize,
	)
	if err != nil || admitted.Float64ColumnCount() != len(masks) {
		t.Fatalf("OpenAdmittedDocumentPageWithOverflow = (columns %d,%v)", admitted.Float64ColumnCount(), err)
	}
	if got := view.Float64ColumnCount(); got != len(masks) {
		t.Fatalf("Float64ColumnCount = %d, want %d", got, len(masks))
	}
	first, ok := view.Float64Column(0)
	if !ok || first.Mask() != masks[0] {
		t.Fatalf("Float64Column(0) = (%+v,%v), want mask %#x", first, ok, masks[0])
	}
	for _, test := range []struct {
		slot uint8
		want float64
		ok   bool
	}{
		{0, -12.5, true},
		{5, 0, false},
		{63, 1.25e100, true},
		{64, 0, false},
	} {
		got, present := first.Lookup(test.slot)
		if present != test.ok || present && got != test.want {
			t.Fatalf("Lookup(%d) = (%g,%v), want (%g,%v)", test.slot, got, present, test.want, test.ok)
		}
	}
	iterator := first.Iterator()
	for _, want := range []struct {
		slot  uint8
		value float64
	}{{0, -12.5}, {63, 1.25e100}} {
		slot, value, present := iterator.Next()
		if !present || slot != want.slot || value != want.value {
			t.Fatalf("Next = (%d,%g,%v), want (%d,%g,true)", slot, value, present, want.slot, want.value)
		}
	}
	if _, _, present := iterator.Next(); present {
		t.Fatal("iterator returned a value after its mask was exhausted")
	}
	valuesIterator := first.Values()
	for _, want := range []float64{-12.5, 1.25e100} {
		value, present := valuesIterator.Next()
		if !present || value != want {
			t.Fatalf("dense Next = (%g,%v), want (%g,true)", value, present, want)
		}
	}
	if _, present := valuesIterator.Next(); present {
		t.Fatal("dense iterator returned a value after its values were exhausted")
	}
	second, ok := view.Float64Column(1)
	if !ok {
		t.Fatal("Float64Column(1) missed")
	}
	if got, present := second.Lookup(5); !present || got != 42 {
		t.Fatalf("second.Lookup(5) = (%g,%v), want (42,true)", got, present)
	}
	if _, ok := view.Float64Column(-1); ok {
		t.Fatal("Float64Column(-1) succeeded")
	}
	if _, ok := view.Float64Column(len(masks)); ok {
		t.Fatal("Float64Column(count) succeeded")
	}
	for _, row := range rows {
		json, ok := view.LookupJSON(row.Slot)
		if !ok || string(json) != string(row.JSON) || cap(json) != len(json) {
			t.Fatalf("LookupJSON(%d) = (%q,%v) cap=%d, want %q", row.Slot, json, ok, cap(json), row.JSON)
		}
	}
}

func TestDocumentPageFloat64ColumnsRejectInvalidWrite(t *testing.T) {
	header := testDocumentPageHeader(1)
	rows := []DocumentRecord{{Slot: 0, Key: []byte("k"), JSON: []byte(`{"n":1}`)}}
	validValues := make([]float64, 64)
	validValues[0] = 1
	for _, test := range []struct {
		name    string
		columns DocumentFloat64Columns
	}{
		{"values without masks", DocumentFloat64Columns{Values: validValues}},
		{"missing dense values", DocumentFloat64Columns{Masks: []uint64{1}}},
		{"mask outside live", DocumentFloat64Columns{Masks: []uint64{2}, Values: validValues}},
		{"nan", func() DocumentFloat64Columns {
			values := append([]float64(nil), validValues...)
			values[0] = math.NaN()
			return DocumentFloat64Columns{Masks: []uint64{1}, Values: values}
		}()},
		{"infinity", func() DocumentFloat64Columns {
			values := append([]float64(nil), validValues...)
			values[0] = math.Inf(1)
			return DocumentFloat64Columns{Masks: []uint64{1}, Values: values}
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			page := make([]byte, testSuperblockPageSize)
			if _, err := EncodeDocumentPageWithColumns(
				page, header, rows, test.columns, testDocumentNextLogicalID,
				uint64(32*testSuperblockPageSize), testSuperblockPageSize,
			); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeDocumentPageWithColumns = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}
}

func TestDocumentPageFloat64ColumnsRejectResealedCorruption(t *testing.T) {
	header := testDocumentPageHeader(1)
	rows := []DocumentRecord{{Slot: 0, Key: []byte("k"), JSON: []byte(`{"n":1}`)}}
	values := make([]float64, 64)
	values[0] = 1
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeDocumentPageWithColumns(
		page, header, rows, DocumentFloat64Columns{Masks: []uint64{1}, Values: values},
		testDocumentNextLogicalID, uint64(32*testSuperblockPageSize), testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	payload := PageHeaderSize
	dataStart := payload + DocumentPagePayloadHeaderSize + DocumentPageRecordSize
	dataLength := int(binary.LittleEndian.Uint32(encoded[payload+16 : payload+20]))
	columnStart := dataStart + dataLength
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"zero columns", func(p []byte) { clear(p[payload+25 : payload+27]) }},
		{"reserved", func(p []byte) { p[payload+27] = 1 }},
		{"short section", func(p []byte) {
			binary.LittleEndian.PutUint32(p[payload+28:payload+32], 8)
		}},
		{"mask outside live", func(p []byte) {
			binary.LittleEndian.PutUint64(p[columnStart:columnStart+8], 2)
		}},
		{"non-finite", func(p []byte) {
			binary.LittleEndian.PutUint64(p[columnStart+8:columnStart+16], math.Float64bits(math.Inf(-1)))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			test.mutate(corrupt)
			resealTestPage(corrupt)
			if _, err := OpenDocumentPageWithOverflow(
				corrupt, header.ChunkID+1, testDocumentNextLogicalID,
				uint64(32*testSuperblockPageSize), testSuperblockPageSize,
			); !errors.Is(err, ErrDocumentPageCorrupt) {
				t.Fatalf("OpenDocumentPageWithOverflow = %v, want %v", err, ErrDocumentPageCorrupt)
			}
			if _, err := OpenAdmittedDocumentPageWithOverflow(
				corrupt, header.ChunkID+1, testDocumentNextLogicalID,
				uint64(32*testSuperblockPageSize), testSuperblockPageSize,
			); !errors.Is(err, ErrDocumentPageCorrupt) {
				t.Fatalf("OpenAdmittedDocumentPageWithOverflow = %v, want %v", err, ErrDocumentPageCorrupt)
			}
		})
	}
}

func TestDocumentPageVariableExtent(t *testing.T) {
	header := testDocumentPageHeader(^uint64(0))
	header.PageSize = 8192
	rows := make([]DocumentRecord, 64)
	for slot := range rows {
		rows[slot] = DocumentRecord{
			Slot: uint8(slot),
			Key:  []byte("account:00000000"),
			JSON: []byte(`{"profile":{"tenant":"t07"},"state":"s3","payload":"shared"}`),
		}
	}
	page := make([]byte, header.PageSize)
	encoded, err := EncodeDocumentPage(page, header, rows, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenDocumentPage(encoded, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(rows) {
		t.Fatalf("variable extent = (%+v,%d), want (%+v,%d)", view.Header(), view.Len(), header, len(rows))
	}
	json, ok := view.LookupString(63, "account:00000000")
	if !ok || string(json) != string(rows[63].JSON) {
		t.Fatalf("LookupString = (%q,%v), want (%q,true)", json, ok, rows[63].JSON)
	}
}

func TestDocumentPageOverflowReferenceRoundTrip(t *testing.T) {
	const live = uint64(1)<<0 | uint64(1)<<5
	header := testDocumentPageHeader(live)
	fileEnd := uint64(32 * testSuperblockPageSize)
	overflow := testOverflowRef(10, 20, header.Generation)
	rows := []DocumentRecord{
		{Slot: 0, Key: []byte("inline"), JSON: []byte(`{"ok":true}`)},
		{Slot: 5, Key: []byte("large"), Overflow: overflow, JSONLength: 1 << 20},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeDocumentPageWithOverflow(page, header, rows, testDocumentNextLogicalID, fileEnd, testSuperblockPageSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDocumentPage(encoded, header.ChunkID+1, testDocumentNextLogicalID); !errors.Is(err, ErrDocumentPageCorrupt) {
		t.Fatalf("legacy OpenDocumentPage = %v, want %v", err, ErrDocumentPageCorrupt)
	}
	view, err := OpenDocumentPageWithOverflow(encoded, header.ChunkID+1, testDocumentNextLogicalID, fileEnd, testSuperblockPageSize)
	if err != nil {
		t.Fatal(err)
	}
	inline, ok := view.LookupValue(0)
	if !ok || string(inline.Inline) != string(rows[0].JSON) || inline.Length != uint64(len(rows[0].JSON)) || inline.Overflow != (PageRef{}) {
		t.Fatalf("inline value = (%+v,%v)", inline, ok)
	}
	large, ok := view.LookupValue(5)
	if !ok || large.Inline != nil || large.Overflow != overflow || large.Length != rows[1].JSONLength {
		t.Fatalf("overflow value = (%+v,%v)", large, ok)
	}
	record, ok := view.Lookup(5)
	if !ok || string(record.Key) != "large" || record.JSON != nil || record.Overflow != overflow || record.JSONLength != rows[1].JSONLength {
		t.Fatalf("overflow record = (%+v,%v)", record, ok)
	}
	if _, ok := view.LookupJSON(5); ok {
		t.Fatal("LookupJSON returned an overflow descriptor as JSON")
	}
	if _, ok := view.LookupKey(5, []byte("large")); ok {
		t.Fatal("LookupKey returned an overflow descriptor as JSON")
	}
	if got, ok := view.LookupStringValue(5, "large"); !ok || got.Inline != nil || got.Overflow != large.Overflow || got.Length != large.Length {
		t.Fatalf("LookupStringValue = (%+v,%v), want (%+v,true)", got, ok, large)
	}
	if got, ok := view.LookupKeyValue(5, []byte("large")); !ok || got.Inline != nil || got.Overflow != large.Overflow || got.Length != large.Length {
		t.Fatalf("LookupKeyValue = (%+v,%v), want (%+v,true)", got, ok, large)
	}
	if _, ok := view.LookupKeyValue(5, []byte("collision")); ok {
		t.Fatal("LookupKeyValue accepted wrong key")
	}
	if _, err := EncodeDocumentPage(page, header, rows, testDocumentNextLogicalID); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("EncodeDocumentPage with overflow = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestDocumentPageRejectsCorruptOverflowDescriptor(t *testing.T) {
	header := testDocumentPageHeader(1)
	fileEnd := uint64(32 * testSuperblockPageSize)
	rows := []DocumentRecord{{
		Slot: 0, Key: []byte("large"), Overflow: testOverflowRef(10, 20, header.Generation), JSONLength: 1024,
	}}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeDocumentPageWithOverflow(page, header, rows, testDocumentNextLogicalID, fileEnd, testSuperblockPageSize); err != nil {
		t.Fatal(err)
	}
	descriptor := PageHeaderSize + DocumentPagePayloadHeaderSize
	dataStart := PageHeaderSize + DocumentPagePayloadHeaderSize + DocumentPageRecordSize
	keyEnd := int(binary.LittleEndian.Uint32(page[descriptor : descriptor+4]))
	for _, mutate := range []func([]byte){
		func(p []byte) {
			binary.LittleEndian.PutUint32(p[descriptor+4:descriptor+8], uint32(keyEnd+DocumentOverflowDescriptorSize))
		},
		func(p []byte) { p[dataStart+keyEnd+29] = 1 },
		func(p []byte) {
			clear(p[dataStart+keyEnd+PageRefSize : dataStart+keyEnd+DocumentOverflowDescriptorSize])
		},
	} {
		corrupt := append([]byte(nil), page...)
		mutate(corrupt)
		resealTestPage(corrupt)
		if _, err := OpenDocumentPageWithOverflow(corrupt, header.ChunkID+1, testDocumentNextLogicalID, fileEnd, testSuperblockPageSize); !errors.Is(err, ErrDocumentPageCorrupt) {
			t.Fatalf("OpenDocumentPageWithOverflow = %v, want %v", err, ErrDocumentPageCorrupt)
		}
	}
}

func TestDocumentPageRejectsInvalidWrites(t *testing.T) {
	const live = uint64(1)<<0 | uint64(1)<<5 | uint64(1)<<63
	valid := testDocumentPageHeader(live)
	validRows := testDocumentRows()
	for _, test := range []struct {
		name   string
		mutate func(*DocumentPageHeader, *[]DocumentRecord, *uint64)
	}{
		{"store id", func(h *DocumentPageHeader, _ *[]DocumentRecord, _ *uint64) { h.StoreID = [16]byte{} }},
		{"generation", func(h *DocumentPageHeader, _ *[]DocumentRecord, _ *uint64) { h.Generation = 0 }},
		{"logical id", func(h *DocumentPageHeader, _ *[]DocumentRecord, _ *uint64) { h.LogicalID = StateRootLogicalID }},
		{"future logical id", func(h *DocumentPageHeader, _ *[]DocumentRecord, next *uint64) { h.LogicalID = *next }},
		{"page size", func(h *DocumentPageHeader, _ *[]DocumentRecord, _ *uint64) { h.PageSize = 5000 }},
		{"flags", func(h *DocumentPageHeader, _ *[]DocumentRecord, _ *uint64) { h.Flags = 1 }},
		{"empty live", func(h *DocumentPageHeader, rows *[]DocumentRecord, _ *uint64) { h.Live = 0; *rows = nil }},
		{"count", func(_ *DocumentPageHeader, rows *[]DocumentRecord, _ *uint64) { *rows = (*rows)[:2] }},
		{"wrong slot", func(_ *DocumentPageHeader, rows *[]DocumentRecord, _ *uint64) { (*rows)[1].Slot = 6 }},
		{"empty json", func(_ *DocumentPageHeader, rows *[]DocumentRecord, _ *uint64) { (*rows)[1].JSON = nil }},
		{"chunk overflow", func(h *DocumentPageHeader, _ *[]DocumentRecord, _ *uint64) { h.ChunkID = ^uint32(0) }},
		{"payload overflow", func(h *DocumentPageHeader, rows *[]DocumentRecord, _ *uint64) {
			h.PageSize = 4096
			(*rows)[1].JSON = make([]byte, 4096)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := valid
			rows := append([]DocumentRecord(nil), validRows...)
			nextLogicalID := testDocumentNextLogicalID
			test.mutate(&header, &rows, &nextLogicalID)
			page := make([]byte, testSuperblockPageSize)
			if _, err := EncodeDocumentPage(page, header, rows, nextLogicalID); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeDocumentPage = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}
}

func TestDocumentPageRejectsResealedSemanticCorruption(t *testing.T) {
	const live = uint64(1)<<0 | uint64(1)<<5 | uint64(1)<<63
	header := testDocumentPageHeader(live)
	page := encodeTestDocumentPage(t, header, testDocumentRows())
	firstDescriptor := PageHeaderSize + DocumentPagePayloadHeaderSize
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"version", func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize:PageHeaderSize+4], 2) }},
		{"chunk", func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize+4:PageHeaderSize+8], header.ChunkID+1) }},
		{"live", func(p []byte) { p[PageHeaderSize+8] ^= 2 }},
		{"data length", func(p []byte) { p[PageHeaderSize+16]++ }},
		{"count", func(p []byte) { p[PageHeaderSize+20]++ }},
		{"flags", func(p []byte) { p[PageHeaderSize+21] = 1 }},
		{"descriptor size", func(p []byte) { p[PageHeaderSize+22] = 16 }},
		{"reserved", func(p []byte) { p[PageHeaderSize+24] = 1 }},
		{"key before start", func(p []byte) {
			binary.LittleEndian.PutUint32(p[firstDescriptor+8:firstDescriptor+12], 0)
		}},
		{"empty json", func(p []byte) {
			copy(p[firstDescriptor+4:firstDescriptor+8], p[firstDescriptor:firstDescriptor+4])
		}},
		{"unreferenced data", func(p []byte) {
			last := firstDescriptor + 2*DocumentPageRecordSize
			end := binary.LittleEndian.Uint32(p[last+4 : last+8])
			binary.LittleEndian.PutUint32(p[last+4:last+8], end-1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), page...)
			test.mutate(corrupt)
			resealTestPage(corrupt)
			if _, err := OpenDocumentPage(corrupt, header.ChunkID+1, testDocumentNextLogicalID); !errors.Is(err, ErrDocumentPageCorrupt) {
				t.Fatalf("OpenDocumentPage = %v, want %v", err, ErrDocumentPageCorrupt)
			}
		})
	}

	for _, cut := range []int{0, PageHeaderSize - 1, len(page) - 1} {
		if _, err := OpenDocumentPage(page[:cut], header.ChunkID+1, testDocumentNextLogicalID); !errors.Is(err, ErrDocumentPageCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrDocumentPageCorrupt)
		}
	}
}

func TestDocumentPageOpenUsesStateBounds(t *testing.T) {
	header := testDocumentPageHeader(1)
	rows := []DocumentRecord{{Slot: 0, Key: []byte("k"), JSON: []byte(`1`)}}
	page := encodeTestDocumentPage(t, header, rows)
	for _, test := range []struct {
		name           string
		chunkHighWater uint32
		nextLogicalID  uint64
	}{
		{"chunk high water", header.ChunkID, testDocumentNextLogicalID},
		{"logical high water", header.ChunkID + 1, header.LogicalID},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := OpenDocumentPage(page, test.chunkHighWater, test.nextLogicalID); !errors.Is(err, ErrDocumentPageCorrupt) {
				t.Fatalf("OpenDocumentPage = %v, want %v", err, ErrDocumentPageCorrupt)
			}
		})
	}
}
