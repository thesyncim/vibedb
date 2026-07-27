package storeio

import (
	"errors"
	"testing"
)

func TestReplayInlineFreeLogUsesIndexWithoutExternalDelta(t *testing.T) {
	w := newFreeLogTestWriter(t)
	base := []FreeExtent{
		freeLogTestExtent(20, 1, 1),
		freeLogTestExtent(40, 2, 1),
	}
	segment := w.image(base)
	index := w.index([]FreeSegment{segmentOf(segment, base)}, PageRef{})
	inline := NewInlineFreeDelta(PageRef{}, index)
	if err := inline.Append([]FreeDelta{
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: base[0].Offset}},
		{Op: FreeOpSet, Extent: freeLogTestExtent(60, 3, 2)},
	}, testSuperblockPageSize, freeLogTestFileEnd); err != nil {
		t.Fatal(err)
	}

	got, pages, err := ReplayInlineFreeLog(
		w.cache, &inline, w.bounds(), nil, 16, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []FreeExtent{
		base[1],
		freeLogTestExtent(60, 3, 2),
	}
	requireFreeExtents(t, got, want)
	if len(pages.Delta) != 0 ||
		len(pages.Index) != 1 || pages.Index[0] != index ||
		len(pages.Segments) != 1 || !pages.Resident[0] {
		t.Fatalf("pages = %+v", pages)
	}
}

func TestReplayInlineFreeLogRecordsWinOverExternalChain(t *testing.T) {
	w := newFreeLogTestWriter(t)
	base := []FreeExtent{freeLogTestExtent(20, 1, 1)}
	segment := w.image(base)
	index := w.index([]FreeSegment{segmentOf(segment, base)}, PageRef{})
	external := w.delta([]FreeDelta{
		{Op: FreeOpSet, Extent: freeLogTestExtent(20, 2, 2)},
		{Op: FreeOpSet, Extent: freeLogTestExtent(40, 1, 2)},
	}, PageRef{}, index)
	inline := NewInlineFreeDelta(external, index)
	if err := inline.Append([]FreeDelta{
		{Op: FreeOpSet, Extent: freeLogTestExtent(20, 3, 3)},
		{Op: FreeOpDelete, Extent: FreeExtent{Offset: 40 * uint64(testSuperblockPageSize)}},
	}, testSuperblockPageSize, freeLogTestFileEnd); err != nil {
		t.Fatal(err)
	}

	got, pages, err := ReplayInlineFreeLog(
		w.cache, &inline, w.bounds(), nil, 16, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	requireFreeExtents(t, got, []FreeExtent{freeLogTestExtent(20, 3, 3)})
	if len(pages.Delta) != 1 || pages.Delta[0] != external {
		t.Fatalf("delta pages = %+v", pages.Delta)
	}
}

func TestReplayInlineFreeLogMakesTouchedSegmentResident(t *testing.T) {
	w := newFreeLogTestWriter(t)
	lower := []FreeExtent{freeLogTestExtent(20, 8, 1)}
	upper := []FreeExtent{freeLogTestExtent(200, 1, 1)}
	lowerPage := w.image(lower)
	upperPage := w.image(upper)
	index := w.index([]FreeSegment{
		segmentOf(lowerPage, lower),
		segmentOf(upperPage, upper),
	}, PageRef{})
	inline := NewInlineFreeDelta(PageRef{}, index)
	if err := inline.Append([]FreeDelta{{
		Op: FreeOpSet, Extent: freeLogTestExtent(200, 2, 2),
	}}, testSuperblockPageSize, freeLogTestFileEnd); err != nil {
		t.Fatal(err)
	}

	got, pages, err := ReplayInlineFreeLog(
		w.cache, &inline, w.bounds(), nil, 16, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	requireFreeExtents(t, got, []FreeExtent{freeLogTestExtent(200, 2, 2)})
	if len(pages.Resident) != 2 || pages.Resident[0] || !pages.Resident[1] {
		t.Fatalf("resident = %v, want [false true]", pages.Resident)
	}
}

func TestReplayInlineFreeLogRejectsExternalIndexMismatch(t *testing.T) {
	w := newFreeLogTestWriter(t)
	firstBase := []FreeExtent{freeLogTestExtent(20, 1, 1)}
	secondBase := []FreeExtent{freeLogTestExtent(200, 1, 1)}
	firstIndex := w.index([]FreeSegment{
		segmentOf(w.image(firstBase), firstBase),
	}, PageRef{})
	secondIndex := w.index([]FreeSegment{
		segmentOf(w.image(secondBase), secondBase),
	}, PageRef{})
	external := w.delta([]FreeDelta{{
		Op: FreeOpSet, Extent: freeLogTestExtent(40, 1, 2),
	}}, PageRef{}, firstIndex)
	inline := NewInlineFreeDelta(external, secondIndex)

	if _, _, err := ReplayInlineFreeLog(
		w.cache, &inline, w.bounds(), nil, 16, 1,
	); !errors.Is(err, ErrFreeLogCorrupt) {
		t.Fatalf("mismatched index replay = %v, want %v", err, ErrFreeLogCorrupt)
	}
}

func requireFreeExtents(t *testing.T, got, want []FreeExtent) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("extents = %+v, want %+v", got, want)
	}
	for rank := range want {
		if got[rank] != want[rank] {
			t.Fatalf("extent %d = %+v, want %+v", rank, got[rank], want[rank])
		}
	}
}
