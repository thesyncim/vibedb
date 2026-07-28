package storeio

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCompactPrimaryLeafSmoke(t *testing.T) {
	var storeID [16]byte
	for i := range storeID {
		storeID[i] = byte(i + 1)
	}
	bounds := CommonPrimaryLeafBounds{
		FileEnd:           1 << 20,
		NextLogicalID:     PrimaryFirstDynamicLogicalID + 1000,
		AllocationQuantum: 4096,
	}
	const n = 200
	records := make([]CommonPrimaryLeafRecord, n)
	want := make([][]byte, n)
	for i := range records {
		key := []byte(fmt.Sprintf("doc:%08d", i))
		doc := fmt.Appendf(nil,
			`{"id":%d,"name":"user-%d","country":"PT","score":%d,"active":%t,"profile":{"tier":"gold","region":"eu-west-1","joined":"2020-01-02"},"tags":["alpha","beta"],"note":"steady"}`,
			i, i, i%1000, i%3 == 0)
		records[i] = CommonPrimaryLeafRecord{Key: key, Value: CommonPrimaryLeafValue{Inline: doc}}
		want[i] = doc
	}
	builder := NewCompactPrimaryLeafBuilder()
	size, err := CompactPrimaryLeafImagePayloadBytes(records, builder)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	extent := 4096
	for extent-PageHeaderSize-PageTrailerSize < size {
		extent <<= 1
	}
	t.Logf("compact image payload=%d extent=%d rows=%d", size, extent, n)
	dst := make([]byte, extent)
	header := CommonPrimaryLeafHeader{
		StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
	}
	page, err := EncodeCommonPrimaryCompactLeaf(dst, header, records, bounds, builder)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if PrimaryLeafClass(page) != CommonPrimaryLeafCompact {
		t.Fatalf("class = %d", PrimaryLeafClass(page))
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(0)
	expected := PageRef{
		Offset: 4096, Length: uint32(extent), LogicalID: logicalID,
		Generation: 1, Kind: PagePrimaryLeaf,
	}
	view, err := OpenCommonPrimaryCompactLeaf(page, storeID, 0, expected, 1, bounds)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if view.Len() != n {
		t.Fatalf("len = %d want %d", view.Len(), n)
	}
	admitted, ok := AdmittedCommonPrimaryCompactLeaf(page, 0, bounds)
	if !ok {
		t.Fatal("admitted failed")
	}
	for i := range records {
		got, found := admitted.AppendRawByKey(nil, records[i].Key)
		if !found || !bytes.Equal(got, want[i]) {
			t.Fatalf("row %d: found=%v got=%q want=%q", i, found, got, want[i])
		}
		gotRank, _, okRank := admitted.RowAt(i)
		if !okRank || !bytes.Equal(gotRank, records[i].Key) {
			t.Fatalf("row %d rank key mismatch", i)
		}
	}
	// De-compact round trip.
	deRecords, _, err := admitted.DetemplateRecords(nil, nil)
	if err != nil {
		t.Fatalf("detemplate: %v", err)
	}
	if len(deRecords) != n {
		t.Fatalf("detemplate len = %d", len(deRecords))
	}
	for i := range deRecords {
		if !bytes.Equal(deRecords[i].Value.Inline, want[i]) {
			t.Fatalf("detemplate row %d mismatch: %q", i, deRecords[i].Value.Inline)
		}
	}
}
