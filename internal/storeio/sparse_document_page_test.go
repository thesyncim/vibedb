package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"unsafe"
)

func sparseDocumentTestRows() [SparseDocumentPageSlotCount]DocumentRecord {
	var rows [SparseDocumentPageSlotCount]DocumentRecord
	for slot := range rows {
		rows[slot] = DocumentRecord{
			Slot: uint8(slot),
			Key:  []byte(fmt.Sprintf("key:%04d", slot)),
			JSON: []byte(fmt.Sprintf(`{"value":%022d}`, slot)),
		}
	}
	return rows
}

func encodeSparseDocumentTestPage(tb testing.TB, rows []DocumentRecord) ([]byte, DocumentPageHeader) {
	tb.Helper()
	live := uint64(0)
	for _, row := range rows {
		live |= uint64(1) << row.Slot
	}
	header := testDocumentPageHeader(live)
	page := make([]byte, header.PageSize)
	if _, err := EncodeSparseDocumentPage(page, header, rows, testDocumentNextLogicalID); err != nil {
		tb.Fatal(err)
	}
	return page, header
}

func sparseDocumentMutationTestOptions(generation uint64) SparseDocumentMutationOptions {
	return SparseDocumentMutationOptions{
		TargetGeneration: generation, ChunkHighWater: 8,
		NextLogicalID: testDocumentNextLogicalID, SectorSize: 512,
	}
}

func requireSparsePlanFitsJournal(tb testing.TB, before []byte, plan SparseDocumentMutationPlan, header DocumentPageHeader, targetGeneration uint64) {
	tb.Helper()
	journalHeader := MaterializationJournalHeader{
		StoreID: header.StoreID, Sequence: 1, TargetGeneration: targetGeneration,
		PageSize: header.PageSize, SectorSize: MaterializationJournalMinSectorSize,
	}
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		tb.Fatal(err)
	}
	ref := PageRef{
		Offset: layout.DataStart, LogicalID: header.LogicalID, Generation: header.Generation,
		Length: header.PageSize, Kind: PageDocument,
	}
	patches := make([]MaterializationPatch, len(plan.Changed))
	for index, changed := range plan.Changed {
		patches[index] = MaterializationPatch{
			Target: 0, Offset: changed.Offset,
			Data: before[changed.Offset : changed.Offset+changed.Length],
		}
	}
	target, err := BuildMaterializationTarget(journalHeader, ref, before, plan.After, patches, 0)
	if err != nil {
		tb.Fatal(err)
	}
	capsule := make([]byte, MaterializationJournalSize)
	if _, err := EncodeMaterializationJournal(capsule, journalHeader, []MaterializationTarget{target}, patches); err != nil {
		tb.Fatalf("real materialization capsule rejected %d bytes in %d runs: %v",
			plan.ChangedBytes, len(plan.Changed), err)
	}
}

func TestSparseDocumentPageDenseRoundTripAndMetadataSaving(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	view, err := OpenSparseDocumentPage(page, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(rows) {
		t.Fatalf("view=(%+v rows=%d), want (%+v rows=%d)", view.Header(), view.Len(), header, len(rows))
	}
	iterator := view.AllRows()
	for slot, want := range rows {
		got, ok := view.Lookup(uint8(slot))
		if !ok || got.Slot != want.Slot || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.JSON, want.JSON) {
			t.Fatalf("lookup slot %d = (%+v,%v), want %+v", slot, got, ok, want)
		}
		if cap(got.Key) != len(got.Key) || cap(got.JSON) != len(got.JSON) {
			t.Fatalf("slot %d exposes spare capacity", slot)
		}
		json, ok := view.LookupKey(uint8(slot), want.Key)
		if !ok || !bytes.Equal(json, want.JSON) {
			t.Fatalf("key lookup slot %d = (%q,%v)", slot, json, ok)
		}
		json, ok = view.LookupString(uint8(slot), string(want.Key))
		if !ok || !bytes.Equal(json, want.JSON) {
			t.Fatalf("string lookup slot %d = (%q,%v)", slot, json, ok)
		}
		scannedSlot, key, json, overflow, ok := iterator.Next()
		if !ok || overflow || scannedSlot != want.Slot || !bytes.Equal(key, want.Key) || !bytes.Equal(json, want.JSON) {
			t.Fatalf("scan slot %d = (slot=%d key=%q json=%q overflow=%v ok=%v)", slot, scannedSlot, key, json, overflow, ok)
		}
	}
	if _, _, _, _, ok := iterator.Next(); ok {
		t.Fatal("scan returned a 65th row")
	}

	currentMetadata := DocumentPagePayloadHeaderSize + len(rows)*DocumentPageRecordSize
	sparseMetadata := SparseDocumentPageHeapStart - PageHeaderSize
	if currentMetadata != 544 || sparseMetadata != 400 {
		t.Fatalf("metadata current=%d sparse=%d, want 544 and 400", currentMetadata, sparseMetadata)
	}
	if saving := currentMetadata - sparseMetadata; saving != 144 {
		t.Fatalf("dense metadata saving = %d, want 144 bytes/page (2.25 bytes/row)", saving)
	}
	firstDescriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	if got := binary.LittleEndian.Uint16(page[firstDescriptor : firstDescriptor+2]); got != SparseDocumentPageHeapStart {
		t.Fatalf("first heap offset = %d, want %d", got, SparseDocumentPageHeapStart)
	}
	if directoryEnd := PageHeaderSize + SparseDocumentPagePayloadHeaderSize +
		SparseDocumentPageSlotCount*SparseDocumentPageRecordSize; directoryEnd != SparseDocumentPageHeapStart {
		t.Fatalf("directory ends at %d, heap starts at %d", directoryEnd, SparseDocumentPageHeapStart)
	}

	masked := view.Rows(uint64(1)<<2 | uint64(1)<<31 | uint64(1)<<63)
	for _, want := range []uint8{2, 31, 63} {
		slot, key, json, overflow, ok := masked.Next()
		if !ok || overflow || slot != want || !bytes.Equal(key, rows[want].Key) || !bytes.Equal(json, rows[want].JSON) {
			t.Fatalf("masked slot %d = (slot=%d key=%q json=%q overflow=%v ok=%v)", want, slot, key, json, overflow, ok)
		}
	}
	if _, _, _, _, ok := masked.Next(); ok {
		t.Fatal("masked scan returned an extra row")
	}
}

func TestSparseDocumentPageWriterWorkspaceIsBounded(t *testing.T) {
	if got := unsafe.Sizeof(SparseDocumentWorkspace{}); got != 1408 {
		t.Fatalf("writer workspace = %d bytes, want 1408", got)
	}
}

func TestSparseDocumentPageExplicitOverflowMarker(t *testing.T) {
	const slot = 5
	header := testDocumentPageHeader(uint64(1) << slot)
	fileEnd := uint64(32 * testSuperblockPageSize)
	row := DocumentRecord{
		Slot: slot, Key: []byte("large"),
		Overflow: testOverflowRef(20, 20, header.Generation), JSONLength: 1 << 20,
	}
	page := make([]byte, header.PageSize)
	if _, err := EncodeSparseDocumentPageWithOverflow(
		page, header, []DocumentRecord{row}, testDocumentNextLogicalID,
		fileEnd, testSuperblockPageSize,
	); err != nil {
		t.Fatal(err)
	}
	descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	if marker := binary.LittleEndian.Uint16(page[descriptor+4 : descriptor+6]); marker != 0 {
		t.Fatalf("overflow value-length marker = %d, want zero", marker)
	}
	view, err := OpenSparseDocumentPageWithOverflow(
		page, header.ChunkID+1, testDocumentNextLogicalID,
		fileEnd, testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := view.Lookup(slot)
	if !ok || !bytes.Equal(got.Key, row.Key) || got.Overflow != row.Overflow || got.JSONLength != row.JSONLength {
		t.Fatalf("overflow lookup = (%+v,%v), want %+v", got, ok, row)
	}
	iterator := view.AllRows()
	scannedSlot, key, encoded, overflow, ok := iterator.Next()
	ref, length, decoded := iterator.OverflowDescriptor(encoded)
	if !ok || !overflow || !decoded || scannedSlot != slot || !bytes.Equal(key, row.Key) ||
		ref != row.Overflow || length != row.JSONLength {
		t.Fatalf("overflow scan = (slot=%d key=%q overflow=%v ref=%+v length=%d ok=%v decoded=%v)",
			scannedSlot, key, overflow, ref, length, ok, decoded)
	}
	if _, err := OpenSparseDocumentPage(page, header.ChunkID+1, testDocumentNextLogicalID); !errors.Is(err, ErrSparseDocumentPageCorrupt) {
		t.Fatalf("non-overflow open = %v, want corrupt", err)
	}
}

func TestSparseDocumentPageSameSizeUpdateFitsUndoCapsule(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	// Every row is 40 bytes. Slot 26 begins at 1504, deliberately crossing the
	// 1536-byte sector boundary. This is the maximum heap-sector cost of a
	// small same-size replacement.
	const slot = 26
	updated := rows[slot]
	updated.JSON = bytes.Repeat([]byte{'x'}, len(updated.JSON))
	after := make([]byte, len(page))
	var workspace SparseDocumentWorkspace
	plan, err := PlanSparseDocumentPageUpdate(
		after, page, updated, sparseDocumentMutationTestOptions(header.Generation+1), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ChangedBytes != 3*MaterializationJournalMinSectorSize ||
		plan.ChangedBytes > MaterializationJournalMaxData ||
		len(plan.Changed) > MaterializationJournalMaxPatches {
		t.Fatalf("same-size damage = %d bytes in %d runs; capsule max=%d bytes/%d runs",
			plan.ChangedBytes, len(plan.Changed), MaterializationJournalMaxData, MaterializationJournalMaxPatches)
	}
	requireSparsePlanFitsJournal(t, page, plan, header, header.Generation+1)
	opened, err := OpenSparseDocumentPage(plan.After, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := opened.Lookup(slot)
	if !ok || !bytes.Equal(got.JSON, updated.JSON) {
		t.Fatalf("updated row = (%+v,%v)", got, ok)
	}
	if opened.Header().Generation != header.Generation {
		t.Fatalf("canonical identity generation = %d, want unchanged %d", opened.Header().Generation, header.Generation)
	}
}

func TestSparseDocumentPageDeleteFitsUndoCapsuleAndLeavesNoTombstone(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	const slot = 26 // Same deliberate two-heap-sector worst case as update.
	after := make([]byte, len(page))
	var workspace SparseDocumentWorkspace
	plan, err := PlanSparseDocumentPageDelete(
		after, page, slot, sparseDocumentMutationTestOptions(header.Generation+1), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ChangedBytes != 4*MaterializationJournalMinSectorSize ||
		plan.ChangedBytes > MaterializationJournalMaxData ||
		len(plan.Changed) > MaterializationJournalMaxPatches {
		t.Fatalf("delete damage = %d bytes in %d runs; capsule max=%d bytes/%d runs",
			plan.ChangedBytes, len(plan.Changed), MaterializationJournalMaxData, MaterializationJournalMaxPatches)
	}
	requireSparsePlanFitsJournal(t, page, plan, header, header.Generation+1)
	opened, err := OpenSparseDocumentPage(plan.After, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Len() != len(rows)-1 || opened.Header().Live&(uint64(1)<<slot) != 0 {
		t.Fatalf("delete left occupancy: rows=%d live=%016x", opened.Len(), opened.Header().Live)
	}
	if opened.Header().Generation != header.Generation {
		t.Fatalf("canonical identity generation = %d, want unchanged %d", opened.Header().Generation, header.Generation)
	}
	if row, ok := opened.Lookup(slot); ok {
		t.Fatalf("delete left tombstone/row: %+v", row)
	}
	for next := slot + 1; next < SparseDocumentPageSlotCount; next++ {
		got, ok := opened.Lookup(uint8(next))
		if !ok || got.Slot != uint8(next) || !bytes.Equal(got.Key, rows[next].Key) {
			t.Fatalf("slot %d shifted identity: (%+v,%v)", next, got, ok)
		}
	}
}

func TestSparseDocumentPageGrowingUpdateUsesGapAndRebuildCompacts(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	var workspace SparseDocumentWorkspace
	deleted := make([]byte, len(page))
	deletePlan, err := PlanSparseDocumentPageDelete(
		deleted, page, 12, sparseDocumentMutationTestOptions(header.Generation+1), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	beforeGrowth, err := OpenSparseDocumentPage(deletePlan.After, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	grown := rows[13]
	grown.JSON = append(append([]byte(nil), grown.JSON...), bytes.Repeat([]byte{'g'}, 32)...)
	grownPage := make([]byte, len(page))
	growPlan, err := PlanSparseDocumentPageUpdate(
		grownPage, deletePlan.After, grown,
		sparseDocumentMutationTestOptions(header.Generation+2), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	afterGrowth, err := OpenSparseDocumentPage(growPlan.After, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := afterGrowth.Lookup(13)
	if !ok || !bytes.Equal(got.JSON, grown.JSON) || afterGrowth.Len() != beforeGrowth.Len() {
		t.Fatalf("grown row = (%+v,%v), rows=%d", got, ok, afterGrowth.Len())
	}

	rebuiltPage := make([]byte, len(page))
	rebuildPlan, err := RebuildSparseDocumentPage(
		rebuiltPage, growPlan.After, sparseDocumentMutationTestOptions(header.Generation+3), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := OpenSparseDocumentPage(rebuildPlan.After, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	cursor := SparseDocumentPageHeapStart
	for rank := 0; rank < rebuilt.Len(); rank++ {
		descriptor := PageHeaderSize + SparseDocumentPagePayloadHeaderSize + rank*SparseDocumentPageRecordSize
		start := int(binary.LittleEndian.Uint16(rebuildPlan.After[descriptor : descriptor+2]))
		keyLength := int(binary.LittleEndian.Uint16(rebuildPlan.After[descriptor+2 : descriptor+4]))
		valueLength := int(binary.LittleEndian.Uint16(rebuildPlan.After[descriptor+4 : descriptor+6]))
		if start != cursor {
			t.Fatalf("rank %d starts at %d, want compact cursor %d", rank, start, cursor)
		}
		cursor += keyLength + valueLength
	}
}

func TestSparseDocumentPageGrowingUpdateFallsBackWhenNoGapFits(t *testing.T) {
	rows := []DocumentRecord{
		{Slot: 0, Key: []byte("a"), JSON: bytes.Repeat([]byte{'a'}, 1700)},
		{Slot: 1, Key: []byte("b"), JSON: bytes.Repeat([]byte{'b'}, 1800)},
	}
	page, header := encodeSparseDocumentTestPage(t, rows)
	grown := rows[0]
	grown.JSON = bytes.Repeat([]byte{'g'}, 1800)
	var workspace SparseDocumentWorkspace
	if _, err := PlanSparseDocumentPageUpdate(
		make([]byte, len(page)), page, grown,
		sparseDocumentMutationTestOptions(header.Generation+1), &workspace,
	); !errors.Is(err, ErrSparseDocumentPageNoSpace) {
		t.Fatalf("oversized growth = %v, want copy-on-write fallback %v", err, ErrSparseDocumentPageNoSpace)
	}
}

func TestSparseDocumentPageRejectsResealedGapAndOverlap(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	for _, mutate := range []func([]byte){
		func(corrupt []byte) { corrupt[3500] = 1 },
		func(corrupt []byte) {
			first := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
			second := first + SparseDocumentPageRecordSize
			binary.LittleEndian.PutUint16(corrupt[second:second+2], binary.LittleEndian.Uint16(corrupt[first:first+2]))
		},
	} {
		corrupt := bytes.Clone(page)
		mutate(corrupt)
		if _, err := SealPage(corrupt); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenSparseDocumentPage(corrupt, header.ChunkID+1, testDocumentNextLogicalID); !errors.Is(err, ErrSparseDocumentPageCorrupt) {
			t.Fatalf("OpenSparseDocumentPage corruption = %v", err)
		}
	}
}

func TestSparseDocumentPageRejectsPartiallyOverlappingMutationBuffers(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	backing := make([]byte, len(page)+64)
	copy(backing[64:], page)
	src := backing[64:]
	dst := backing[:len(page)]
	updated := rows[12]
	updated.JSON = bytes.Repeat([]byte{'x'}, len(updated.JSON))
	var workspace SparseDocumentWorkspace
	if _, err := PlanSparseDocumentPageUpdate(
		dst, src, updated, sparseDocumentMutationTestOptions(header.Generation+1), &workspace,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("partially overlapping update = %v, want %v", err, ErrInvalidWrite)
	}
}

func TestSparseDocumentPageSteadyAllocation(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeSparseDocumentTestPage(t, rows[:])
	after := make([]byte, len(page))
	updated := rows[12]
	updated.JSON = bytes.Repeat([]byte{'x'}, len(updated.JSON))
	var workspace SparseDocumentWorkspace
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	if allocs := testing.AllocsPerRun(1000, func() {
		view, err := OpenSparseDocumentPage(page, header.ChunkID+1, testDocumentNextLogicalID)
		if err != nil {
			panic(err)
		}
		if _, ok := view.LookupString(63, string(rows[63].Key)); !ok {
			panic("lookup miss")
		}
		if _, err := PlanSparseDocumentPageUpdate(after, page, updated, options, &workspace); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("sparse codec/read/update allocations = %g, want 0", allocs)
	}
}
