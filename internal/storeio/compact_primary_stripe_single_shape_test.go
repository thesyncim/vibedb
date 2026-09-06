package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/thesyncim/vibejson"
)

func compactPrimarySingleShapePage(
	t testing.TB,
) ([]byte, CompactPrimaryStripeView, []CommonPrimaryLeafRecord) {
	t.Helper()
	const rows = 128
	records := make([]CommonPrimaryLeafRecord, rows)
	for row := range records {
		value := 9990 + 7*row
		records[row] = CommonPrimaryLeafRecord{
			Key: fmt.Appendf(nil, "row-%03d", row),
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(
				nil, `{"score":%d}`, value,
			)},
		}
	}
	storeID := unifiedTestStoreID()
	page, err := EncodeBestCompactPrimaryStripe(
		make([]byte, CommonPrimaryLeafMaxExtentBytes),
		CommonPrimaryLeafHeader{StoreID: storeID, Generation: 1, Bucket: 0},
		storeID, records, NewUnifiedPrimaryLeafBuilder(),
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	view, err := OpenCompactPrimaryStripe(
		page, storeID, 0,
		PageRef{
			Offset: 4096, Length: uint32(len(page)), LogicalID: logicalID,
			Generation: 1, Kind: PagePrimaryLeaf,
		},
		1, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return page, view, records
}

func compactPrimarySingleShapeRankPage(
	t testing.TB,
) (CompactPrimaryStripeView, []CommonPrimaryLeafRecord) {
	t.Helper()
	page, view, records := compactPrimarySingleShapePage(t)
	if view.shapeCount != 1 || len(view.overflow) != 0 {
		t.Fatalf("ordinary fixture shapeCount=%d overflow=%d", view.shapeCount, len(view.overflow))
	}
	entry, ok := view.shapeEntry(0)
	if !ok {
		t.Fatal("ordinary fixture shape entry")
	}
	stream, err := openCompactStream(entry.streamRaw)
	if err != nil {
		t.Fatal(err)
	}
	if stream.kind != compactStreamPrefixInt || stream.count != len(records) ||
		len(stream.data) != 18 || stream.data[0] != 2 {
		t.Fatalf("ordinary stream kind=%d count=%d data=%d mode=%d", stream.kind, stream.count, len(stream.data), stream.data[0])
	}
	// The one-shape writer correctly keeps the ordinary PrefixInt stream. Turn
	// that same 34-byte descriptor into an admitted physical-rank stream so
	// AppendValue is tested against a rank stream the writer does not emit here.
	templateBytes := int(binary.LittleEndian.Uint32(view.shapeData[8:]))
	streamOffset := PageHeaderSize + view.shapeStart + compactPrimaryShapeHeader + templateBytes
	if streamOffset < 0 || streamOffset >= len(page) {
		t.Fatalf("rank stream offset=%d page=%d", streamOffset, len(page))
	}
	page[streamOffset] = compactStreamRankAffine
	if _, err := sealPage(page, false); err != nil {
		t.Fatal(err)
	}
	storeID := unifiedTestStoreID()
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	rankView, err := OpenCompactPrimaryStripe(
		page, storeID, 0,
		PageRef{
			Offset: 4096, Length: uint32(len(page)), LogicalID: logicalID,
			Generation: 1, Kind: PagePrimaryLeaf,
		},
		1, unifiedTestBounds(),
	)
	if err != nil {
		t.Fatalf("admit one-shape rank page: %v", err)
	}
	return rankView, records
}

func assertCompactPrimarySingleShapeBounds(
	t testing.TB,
	view *CompactPrimaryStripeView,
) {
	t.Helper()
	sentinel := []byte("prefix")
	for _, row := range []int{-1, view.rows, view.rows + 1} {
		got, ok := view.AppendValue(sentinel, row)
		if ok || !bytes.Equal(got, sentinel) {
			t.Fatalf("row=%d got=%q ok=%v after invalid append", row, got, ok)
		}
	}
	var nilView *CompactPrimaryStripeView
	got, ok := nilView.AppendValue(sentinel, 0)
	if ok || !bytes.Equal(got, sentinel) {
		t.Fatalf("nil view got=%q ok=%v", got, ok)
	}
}

func TestCompactPrimaryStripeSingleShapeAppendValueOrdinaryParity(t *testing.T) {
	_, view, records := compactPrimarySingleShapePage(t)
	if view.shapeCount != 1 || len(view.overflow) != 0 {
		t.Fatalf("shapeCount=%d overflow=%d", view.shapeCount, len(view.overflow))
	}
	entry, ok := view.shapeEntry(0)
	if !ok {
		t.Fatal("shape entry")
	}
	stream, err := openCompactStream(entry.streamRaw)
	if err != nil {
		t.Fatal(err)
	}
	if stream.kind == compactStreamRankAffine {
		t.Fatal("one-shape ordinary writer unexpectedly emitted rank-affine")
	}
	for row := range records {
		want, wantOK := view.appendValueOrdinal(nil, row, 0, row)
		got, gotOK := view.AppendValue(nil, row)
		canonical, canonicalErr := vibejson.AppendCanonicalize(nil, records[row].Value.Inline)
		if canonicalErr != nil || !wantOK || !gotOK ||
			!bytes.Equal(want, canonical) || !bytes.Equal(got, want) {
			t.Fatalf("row=%d canonical=%q want=%q/%v got=%q/%v err=%v", row, canonical, want, wantOK, got, gotOK, canonicalErr)
		}
	}
	assertCompactPrimarySingleShapeBounds(t, &view)
}

func TestCompactPrimaryStripeSingleShapeAppendValueRankParity(t *testing.T) {
	view, records := compactPrimarySingleShapeRankPage(t)
	if view.shapeCount != 1 || len(view.overflow) != 0 {
		t.Fatalf("shapeCount=%d overflow=%d", view.shapeCount, len(view.overflow))
	}
	entry, ok := view.shapeEntry(0)
	if !ok {
		t.Fatal("rank shape entry")
	}
	stream, err := openCompactStream(entry.streamRaw)
	if err != nil {
		t.Fatal(err)
	}
	if stream.kind != compactStreamRankAffine || stream.count != view.rows {
		t.Fatalf("rank stream kind=%d count=%d rows=%d", stream.kind, stream.count, view.rows)
	}
	for row := range records {
		want, wantOK := view.appendValueOrdinal(nil, row, 0, row)
		got, gotOK := view.AppendValue(nil, row)
		canonical, canonicalErr := vibejson.AppendCanonicalize(nil, records[row].Value.Inline)
		if canonicalErr != nil || !wantOK || !gotOK ||
			!bytes.Equal(want, canonical) || !bytes.Equal(got, want) {
			t.Fatalf("row=%d canonical=%q want=%q/%v got=%q/%v err=%v", row, canonical, want, wantOK, got, gotOK, canonicalErr)
		}
	}
	assertCompactPrimarySingleShapeBounds(t, &view)
}
