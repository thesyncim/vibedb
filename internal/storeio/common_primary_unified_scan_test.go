//go:build legacy_primary_leaf

package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
)

type unifiedScanWant struct {
	key      []byte
	value    []byte
	overflow PageRef
}

type unifiedScanEdit struct {
	rank        int
	key         []byte
	value       []byte
	matchesBase bool
	delete      bool
}

func unifiedScanRecords(count int) ([]CommonPrimaryLeafRecord, [][]byte) {
	records := make([]CommonPrimaryLeafRecord, count)
	canonical := make([][]byte, count)
	for index := range count {
		key := fmt.Appendf(nil, "row:%03d", index)
		value := fmt.Appendf(nil,
			`{"group":"steady","id":%d,"name":"row-%03d"}`,
			index, index,
		)
		records[index] = CommonPrimaryLeafRecord{
			Key: key, Value: CommonPrimaryLeafValue{Inline: value},
		}
		canonical[index] = value
	}
	return records, canonical
}

func unifiedScanMixedFixture(
	t testing.TB,
) (CommonPrimaryUnifiedLeafView, []unifiedScanEdit, []unifiedScanWant) {
	t.Helper()
	records, canonical := unifiedScanRecords(64)
	layout, err := MutableStoreLayout(unifiedTestBounds().AllocationQuantum)
	if err != nil {
		t.Fatal(err)
	}
	overflowRank := 20
	overflow := PageRef{
		Offset: layout.DataStart + 100*4096, Length: 4096,
		LogicalID:  PrimaryFirstDynamicLogicalID + 500,
		Generation: 1, Kind: PageOverflow,
	}
	records[overflowRank].Value = CommonPrimaryLeafValue{Overflow: overflow}
	page, count := encodeUnifiedTestLeaf(t, records)
	if count != len(records) {
		t.Fatalf("fixture encoded %d/%d rows", count, len(records))
	}
	view := openUnifiedTestLeaf(t, page)

	edits := []unifiedScanEdit{
		{rank: 0, key: []byte("aaa"), value: []byte(`{"edit":"before"}`)},
		{rank: 0, key: records[0].Key, value: []byte(`{"edit":"first"}`), matchesBase: true},
		{rank: 17, key: append(append([]byte(nil), records[16].Key...), '~'), value: []byte(`{"edit":"middle"}`)},
		{rank: 17, key: records[17].Key, matchesBase: true, delete: true},
		{rank: 31, key: records[31].Key, value: []byte(`{"edit":"replace"}`), matchesBase: true},
		{rank: 48, key: records[48].Key, matchesBase: true, delete: true},
		{rank: count, key: []byte("zzz"), value: []byte(`{"edit":"after"}`)},
	}

	wantByKey := make(map[string]unifiedScanWant, count+3)
	for rank := range count {
		row := unifiedScanWant{key: records[rank].Key, value: canonical[rank]}
		if rank == overflowRank {
			row.value = nil
			row.overflow = overflow
		}
		wantByKey[string(row.key)] = row
	}
	for index := range edits {
		edit := &edits[index]
		if edit.delete {
			delete(wantByKey, string(edit.key))
			continue
		}
		wantByKey[string(edit.key)] = unifiedScanWant{
			key: edit.key, value: edit.value,
		}
	}
	keys := make([]string, 0, len(wantByKey))
	for key := range wantByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := make([]unifiedScanWant, len(keys))
	for index, key := range keys {
		want[index] = wantByKey[key]
	}
	return view, edits, want
}

func resetUnifiedScanCursor(
	cursor *PrimaryGraphCursor,
	view CommonPrimaryUnifiedLeafView,
	scratch []byte,
) {
	*cursor = PrimaryGraphCursor{
		unifiedLeaf: view, rows: view.AllRows(), spliceScratch: scratch,
	}
	cursor.unifiedRenderer.Reset(view)
}

func driveUnifiedScanEdits(
	cursor *PrimaryGraphCursor,
	view CommonPrimaryUnifiedLeafView,
	edits []unifiedScanEdit,
	visit func(key, value []byte) error,
	visitOverflow func(key []byte, ref PageRef) error,
) ([]byte, error) {
	editAt := 0
	var err error
	for {
		limit := view.Len()
		if editAt < len(edits) {
			limit = edits[editAt].rank
		}
		var key []byte
		var ref PageRef
		key, ref, err = cursor.visitCurrentLeafInlineUntil(limit, visit)
		if err != nil {
			return cursor.spliceScratch, err
		}
		if ref != (PageRef{}) {
			if visitOverflow == nil {
				return cursor.spliceScratch, ErrCommonPrimaryLeafCorrupt
			}
			if err = visitOverflow(key, ref); err != nil {
				return cursor.spliceScratch, err
			}
			continue
		}
		if editAt == len(edits) {
			return cursor.spliceScratch, nil
		}

		rank := edits[editAt].rank
		for editAt < len(edits) && edits[editAt].rank == rank &&
			!edits[editAt].matchesBase {
			edit := &edits[editAt]
			editAt++
			if edit.delete {
				continue
			}
			if err = visit(edit.key, edit.value); err != nil {
				return cursor.spliceScratch, err
			}
		}
		if editAt == len(edits) || edits[editAt].rank != rank {
			continue
		}
		edit := &edits[editAt]
		if !edit.matchesBase || cursor.ConsumeCurrentLeafBase(edit.key) != nil {
			return cursor.spliceScratch, ErrCommonPrimaryLeafCorrupt
		}
		editAt++
		if !edit.delete {
			if err = visit(edit.key, edit.value); err != nil {
				return cursor.spliceScratch, err
			}
		}
	}
}

func TestCommonPrimaryUnifiedRowScannerMixedEditsOverflowAndAllocations(t *testing.T) {
	view, edits, want := unifiedScanMixedFixture(t)
	var cursor PrimaryGraphCursor
	scratch := make([]byte, 0, 1024)
	check := func() error {
		resetUnifiedScanCursor(&cursor, view, scratch)
		at := 0
		visit := func(key, value []byte) error {
			if at >= len(want) || !bytes.Equal(key, want[at].key) ||
				want[at].overflow != (PageRef{}) ||
				!bytes.Equal(value, want[at].value) {
				return ErrCommonPrimaryLeafCorrupt
			}
			at++
			return nil
		}
		visitOverflow := func(key []byte, ref PageRef) error {
			if at >= len(want) || !bytes.Equal(key, want[at].key) ||
				ref != want[at].overflow {
				return ErrCommonPrimaryLeafCorrupt
			}
			at++
			return nil
		}
		var err error
		scratch, err = driveUnifiedScanEdits(
			&cursor, view, edits, visit, visitOverflow,
		)
		if err != nil {
			return err
		}
		if at != len(want) {
			return ErrCommonPrimaryLeafCorrupt
		}
		return nil
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
	var allocationErr error
	allocs := testing.AllocsPerRun(50, func() {
		allocationErr = check()
	})
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	if allocs != 0 {
		t.Fatalf("warmed mixed scan allocated %.2f times, want 0", allocs)
	}
}

func TestCommonPrimaryUnifiedScanSpanAndConsumeValidation(t *testing.T) {
	records, _ := unifiedScanRecords(32)
	page, count := encodeUnifiedTestLeaf(t, records)
	if count != len(records) {
		t.Fatalf("fixture encoded %d/%d rows", count, len(records))
	}
	view := openUnifiedTestLeaf(t, page)
	visit := func(_, _ []byte) error { return nil }
	var cursor PrimaryGraphCursor

	resetUnifiedScanCursor(&cursor, view, nil)
	if _, _, err := cursor.visitCurrentLeafInlineUntil(count+1, visit); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("past-end span error = %v", err)
	}
	resetUnifiedScanCursor(&cursor, view, nil)
	if err := cursor.ConsumeCurrentLeafBase([]byte("wrong")); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("wrong base certificate error = %v", err)
	}
	if err := cursor.ConsumeCurrentLeafBase(records[0].Key); err != nil {
		t.Fatalf("correct base certificate after rejection: %v", err)
	}
	if _, _, err := cursor.visitCurrentLeafInlineUntil(0, visit); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("backward span error = %v", err)
	}

	overflowView, _, _ := unifiedScanMixedFixture(t)
	resetUnifiedScanCursor(&cursor, overflowView, nil)
	if _, ref, err := cursor.visitCurrentLeafInlineUntil(20, visit); err != nil || ref != (PageRef{}) {
		t.Fatalf("span before overflow row = ref %+v, err %v", ref, err)
	}
	overflowKey, ok := overflowView.RowAt(20)
	if !ok {
		t.Fatal("overflow key")
	}
	if err := cursor.ConsumeCurrentLeafBase(overflowKey); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("overflow base certificate error = %v", err)
	}
}

var unifiedScanBenchmarkSink byte

func BenchmarkCommonPrimaryUnifiedRowScanner(b *testing.B) {
	records, canonical := unifiedScanRecords(96)
	page, count := encodeUnifiedTestLeaf(b, records)
	if count != len(records) {
		b.Fatalf("fixture encoded %d/%d rows", count, len(records))
	}
	view := openUnifiedTestLeaf(b, page)
	for _, editCount := range []int{0, 1, 8, 63} {
		edits := make([]unifiedScanEdit, editCount)
		for index := range edits {
			rank := index
			if editCount == 1 {
				rank = count / 2
			}
			edits[index] = unifiedScanEdit{
				rank: rank, key: records[rank].Key,
				// Keep replacement byte counts equal to the base document. The
				// edited cases measure handing canonical overlay bytes straight
				// through without winning by shrinking callback payloads.
				value: canonical[rank], matchesBase: true,
			}
		}
		b.Run(fmt.Sprintf("%02d-edits", editCount), func(b *testing.B) {
			var cursor PrimaryGraphCursor
			scratch := make([]byte, 0, 1024)
			var sink byte
			visit := func(key, value []byte) error {
				sink ^= key[0] ^ value[0]
				return nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				resetUnifiedScanCursor(&cursor, view, scratch)
				var err error
				scratch, err = driveUnifiedScanEdits(
					&cursor, view, edits, visit, nil,
				)
				if err != nil {
					b.Fatal("scan", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/row")
			unifiedScanBenchmarkSink = sink
		})
	}
}
