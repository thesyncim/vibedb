package storeio

import (
	"os"
	"testing"
)

func TestPrimaryValueGraphStreamStagesMixedValues(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "primary-value-stream-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	r := UnrootedGenerationReservation{Offset: 64 << 10, Length: 8 << 20, FirstLogicalID: PrimaryFirstDynamicLogicalID, LogicalIDCount: 1 << 18}
	w, err := NewUnrootedGenerationWriter(file, r, testStoreID, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewUnrootedPrimaryGraphSink(w, testStoreID, 9, r.FirstLogicalID, r.Offset+r.Length, make([]byte, 512<<10))
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewPrimaryValueGraphStreamBuilder(sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]CommonPrimaryLeafRecord, 300)
	placements := make([]PrimaryGraphPlacement, len(records))
	for rank := range records {
		records[rank] = CommonPrimaryLeafRecord{Key: []byte{byte(rank >> 8), byte(rank), 1}, Value: CommonPrimaryLeafValue{Inline: []byte(`{"v":1}`)}}
	}
	records[17].Value = CommonPrimaryLeafValue{Overflow: PageRef{Offset: 32 << 20, LogicalID: 700, Generation: 9, Length: 4096, Kind: PageOverflow}}
	if err := builder.StageWindow(records, placements); err != nil {
		t.Fatal(err)
	}
	root, err := builder.Finish()
	if err != nil || root == (PageRef{}) {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	if placements[17].Bucket != placements[0].Bucket || placements[256].Bucket == placements[0].Bucket {
		t.Fatalf("placements=%+v/%+v/%+v", placements[0], placements[17], placements[256])
	}
}
