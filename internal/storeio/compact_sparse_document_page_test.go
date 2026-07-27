package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func encodeCompactSparseDocumentTestPage(tb testing.TB, rows []DocumentRecord) ([]byte, DocumentPageHeader) {
	tb.Helper()
	live := uint64(0)
	for _, row := range rows {
		live |= uint64(1) << row.Slot
	}
	header := testDocumentPageHeader(live)
	page := make([]byte, header.PageSize)
	if _, err := EncodeCompactSparseDocumentPage(
		page, header, rows, testDocumentNextLogicalID,
	); err != nil {
		tb.Fatal(err)
	}
	return page, header
}

func TestCompactSparseDocumentPageRoundTripAndMetadataTarget(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeCompactSparseDocumentTestPage(t, rows[:])
	view, err := OpenCompactSparseDocumentPage(
		page, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(rows) {
		t.Fatalf("view=(%+v rows=%d), want (%+v rows=%d)",
			view.Header(), view.Len(), header, len(rows))
	}
	iterator := view.AllRows()
	for slot, want := range rows {
		got, ok := view.Lookup(uint8(slot))
		if !ok || got.Slot != want.Slot ||
			!bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.JSON, want.JSON) {
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
		if !ok || overflow || scannedSlot != want.Slot ||
			!bytes.Equal(key, want.Key) || !bytes.Equal(json, want.JSON) {
			t.Fatalf("scan slot %d = (slot=%d key=%q json=%q overflow=%v ok=%v)",
				slot, scannedSlot, key, json, overflow, ok)
		}
	}
	if _, _, _, _, ok := iterator.Next(); ok {
		t.Fatal("scan returned a 65th row")
	}

	metadata := CompactSparseDocumentPageHeapStart - PageHeaderSize
	if metadata != 272 {
		t.Fatalf("compact metadata = %d, want 272 bytes", metadata)
	}
	if perRow := float64(metadata) / SparseDocumentPageSlotCount; perRow != 4.25 {
		t.Fatalf("compact metadata = %.2f B/row, want 4.25", perRow)
	}
	if saving := SparseDocumentPageHeapStart - CompactSparseDocumentPageHeapStart; saving != 128 {
		t.Fatalf("saving over SDP1 = %d, want 128 bytes/page (2 B/row)", saving)
	}
	first := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	encoded := binary.LittleEndian.Uint32(page[first : first+4])
	start, keyLength, valueLength := compactSparseDocumentDescriptor(page, 0)
	if start != CompactSparseDocumentPageHeapStart ||
		keyLength != len(rows[0].Key) || valueLength != len(rows[0].JSON) {
		t.Fatalf("first descriptor %08x = (%d,%d,%d)", encoded, start, keyLength, valueLength)
	}
}

func TestSparseDocumentPageV2FallsBackExactlyToWideDescriptor(t *testing.T) {
	header := testDocumentPageHeader(1)
	rows := []DocumentRecord{{
		Slot: 0, Key: bytes.Repeat([]byte{'k'}, CompactSparseDocumentPageMaxKey+1),
		JSON: []byte(`{"v":1}`),
	}}
	page := make([]byte, header.PageSize)
	encoded, format, err := EncodeSparseDocumentPageV2(
		page, header, rows, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if format != SparseDocumentDescriptorWide6 {
		t.Fatalf("format = %d, want wide", format)
	}
	if got := binary.LittleEndian.Uint32(encoded[PageHeaderSize : PageHeaderSize+4]); got != sparseDocumentPageMagic {
		t.Fatalf("fallback magic = %08x, want SDP1", got)
	}
	view, err := OpenSparseDocumentPageV2(
		encoded, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Format() != SparseDocumentDescriptorWide6 {
		t.Fatalf("opened format = %d, want wide", view.Format())
	}
	json, ok := view.LookupJSON(0)
	if !ok || !bytes.Equal(json, rows[0].JSON) {
		t.Fatalf("fallback lookup = (%q,%v)", json, ok)
	}
	updated := rows[0]
	updated.JSON = []byte(`{"v":2}`)
	var workspace SparseDocumentWorkspace
	plan, err := PlanAdmittedSparseDocumentPageV2Update(
		make([]byte, len(encoded)), view, updated,
		sparseDocumentMutationTestOptions(header.Generation+1), &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(plan.After[PageHeaderSize : PageHeaderSize+4]); got != sparseDocumentPageMagic {
		t.Fatalf("wide mutation changed format: magic=%08x", got)
	}

	compactRows := sparseDocumentTestRows()
	compactPage := make([]byte, header.PageSize)
	_, format, err = EncodeSparseDocumentPageV2(
		compactPage, testDocumentPageHeader(^uint64(0)),
		compactRows[:], testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if format != SparseDocumentDescriptorCompact4 {
		t.Fatalf("compact format = %d", format)
	}
}

func TestCompactSparseDocumentPageExplicitOverflowMarker(t *testing.T) {
	const slot = 5
	header := testDocumentPageHeader(uint64(1) << slot)
	fileEnd := uint64(32 * testSuperblockPageSize)
	row := DocumentRecord{
		Slot: slot, Key: []byte("large"),
		Overflow: testOverflowRef(20, 20, header.Generation), JSONLength: 1 << 20,
	}
	page := make([]byte, header.PageSize)
	if _, err := EncodeCompactSparseDocumentPageWithOverflow(
		page, header, []DocumentRecord{row}, testDocumentNextLogicalID,
		fileEnd, testSuperblockPageSize,
	); err != nil {
		t.Fatal(err)
	}
	_, _, valueLength := compactSparseDocumentDescriptor(page, 0)
	if valueLength != 0 {
		t.Fatalf("overflow marker = %d, want zero", valueLength)
	}
	view, err := OpenCompactSparseDocumentPageWithOverflow(
		page, header.ChunkID+1, testDocumentNextLogicalID,
		fileEnd, testSuperblockPageSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := view.Lookup(slot)
	if !ok || !bytes.Equal(got.Key, row.Key) ||
		got.Overflow != row.Overflow || got.JSONLength != row.JSONLength {
		t.Fatalf("overflow lookup = (%+v,%v), want %+v", got, ok, row)
	}
	iterator := view.AllRows()
	scannedSlot, key, encoded, overflow, ok := iterator.Next()
	ref, length, decoded := iterator.OverflowDescriptor(encoded)
	if !ok || !overflow || !decoded || scannedSlot != slot ||
		!bytes.Equal(key, row.Key) || ref != row.Overflow || length != row.JSONLength {
		t.Fatalf("overflow scan = (slot=%d key=%q overflow=%v ref=%+v length=%d ok=%v decoded=%v)",
			scannedSlot, key, overflow, ref, length, ok, decoded)
	}
	if _, err := OpenCompactSparseDocumentPage(
		page, header.ChunkID+1, testDocumentNextLogicalID,
	); !errors.Is(err, ErrSparseDocumentPageCorrupt) {
		t.Fatalf("non-overflow open = %v, want corrupt", err)
	}
}

func TestCompactSparseDocumentPageUpdateDeleteAndDamageBounds(t *testing.T) {
	const slot = 42 // Its value crosses the 2048-byte sector boundary.
	rows := sparseDocumentTestRows()
	page, header := encodeCompactSparseDocumentTestPage(t, rows[:])
	admitted := AdmittedCompactSparseDocumentPage(page)
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	var workspace SparseDocumentWorkspace

	updated := rows[slot]
	updated.JSON = bytes.Repeat([]byte{'x'}, len(updated.JSON))
	after := make([]byte, len(page))
	update, err := PlanAdmittedCompactSparseDocumentPageUpdate(
		after, admitted, updated, options, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if update.ChangedBytes != 3*MaterializationJournalMinSectorSize ||
		update.ChangedBytes > MaterializationJournalMaxData ||
		len(update.Changed) > MaterializationJournalMaxPatches {
		t.Fatalf("update damage = %d bytes/%d runs", update.ChangedBytes, len(update.Changed))
	}
	opened, err := OpenCompactSparseDocumentPage(
		update.After, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := opened.Lookup(slot)
	if !ok || !bytes.Equal(got.JSON, updated.JSON) {
		t.Fatalf("updated row = (%+v,%v)", got, ok)
	}

	after = make([]byte, len(page))
	deleted, err := PlanAdmittedCompactSparseDocumentPageDelete(
		after, admitted, slot, options, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.ChangedBytes != 4*MaterializationJournalMinSectorSize ||
		deleted.ChangedBytes > MaterializationJournalMaxData ||
		len(deleted.Changed) > MaterializationJournalMaxPatches {
		t.Fatalf("delete damage = %d bytes/%d runs", deleted.ChangedBytes, len(deleted.Changed))
	}
	opened, err = OpenCompactSparseDocumentPage(
		deleted.After, header.ChunkID+1, testDocumentNextLogicalID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Len() != len(rows)-1 || opened.Header().Live&(uint64(1)<<slot) != 0 {
		t.Fatalf("delete occupancy = rows=%d live=%016x", opened.Len(), opened.Header().Live)
	}
	if _, ok := opened.Lookup(slot); ok {
		t.Fatal("delete left a row or tombstone")
	}
	for next := slot + 1; next < SparseDocumentPageSlotCount; next++ {
		got, ok := opened.Lookup(uint8(next))
		if !ok || got.Slot != uint8(next) || !bytes.Equal(got.Key, rows[next].Key) {
			t.Fatalf("slot %d shifted identity: (%+v,%v)", next, got, ok)
		}
	}
}

func TestCompactSparseDocumentPageRejectsResealedCorruption(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeCompactSparseDocumentTestPage(t, rows[:])
	first := PageHeaderSize + SparseDocumentPagePayloadHeaderSize
	for name, mutate := range map[string]func([]byte){
		"start before heap": func(corrupt []byte) {
			_, keyLength, valueLength := compactSparseDocumentDescriptor(corrupt, 0)
			binary.LittleEndian.PutUint32(
				corrupt[first:first+4],
				packCompactSparseDocumentDescriptor(
					CompactSparseDocumentPageHeapStart-1, keyLength, valueLength,
				),
			)
		},
		"overlap": func(corrupt []byte) {
			copy(corrupt[first+4:first+8], corrupt[first:first+4])
		},
		"record beyond trailer": func(corrupt []byte) {
			_, keyLength, _ := compactSparseDocumentDescriptor(corrupt, 0)
			binary.LittleEndian.PutUint32(
				corrupt[first:first+4],
				packCompactSparseDocumentDescriptor(
					len(corrupt)-PageTrailerSize-1, keyLength, 1,
				),
			)
		},
		"non-zero gap": func(corrupt []byte) {
			corrupt[3500] = 1
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

	t.Run("reserved descriptor", func(t *testing.T) {
		subset := rows[:3]
		subsetPage, subsetHeader := encodeCompactSparseDocumentTestPage(t, subset)
		reserved := PageHeaderSize + SparseDocumentPagePayloadHeaderSize +
			len(subset)*CompactSparseDocumentPageRecordSize
		subsetPage[reserved] = 1
		if _, err := SealPage(subsetPage); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenCompactSparseDocumentPage(
			subsetPage, subsetHeader.ChunkID+1, testDocumentNextLogicalID,
		); !errors.Is(err, ErrSparseDocumentPageCorrupt) {
			t.Fatalf("open = %v, want corrupt", err)
		}
	})
}

func TestCompactSparseDocumentPageSteadyAllocation(t *testing.T) {
	rows := sparseDocumentTestRows()
	page, header := encodeCompactSparseDocumentTestPage(t, rows[:])
	admitted := AdmittedCompactSparseDocumentPage(page)
	after := make([]byte, len(page))
	updated := rows[12]
	updated.JSON = bytes.Repeat([]byte{'x'}, len(updated.JSON))
	var workspace SparseDocumentWorkspace
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := admitted.LookupString(63, string(rows[63].Key)); !ok {
			panic("lookup miss")
		}
		if _, err := PlanAdmittedCompactSparseDocumentPageUpdate(
			after, admitted, updated, options, &workspace,
		); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("compact codec/read/update allocations = %g, want 0", allocs)
	}
}

func FuzzCompactSparseDocumentPageOpenNoPanic(f *testing.F) {
	rows := sparseDocumentTestRows()
	page, header := encodeCompactSparseDocumentTestPage(f, rows[:])
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{255, 254, 253, 252, 251, 250})
	f.Fuzz(func(t *testing.T, mutations []byte) {
		corrupt := bytes.Clone(page)
		for index := 0; index+1 < len(mutations); index += 2 {
			offset := int(mutations[index])<<4 | int(mutations[index+1]&15)
			offset %= len(corrupt) - PageTrailerSize
			corrupt[offset] ^= mutations[index+1] | 1
		}
		if _, err := SealPage(corrupt); err != nil {
			return
		}
		_, _ = OpenCompactSparseDocumentPage(
			corrupt, header.ChunkID+1, testDocumentNextLogicalID,
		)
	})
}
