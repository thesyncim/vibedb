package storeio

import (
	"os"
	"testing"
)

func TestGenerationMigrationChainedSinkGrowsBoundedly(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "migration-chained-sink-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const pageSize = uint32(4096)
	var nextOffset, nextLogical uint64 = 64 << 10, 100
	var grows int
	sink, err := NewGenerationMigrationChainedSink(
		file, testStoreID, 8, pageSize, 16<<10, 4,
		make([]byte, 64<<10),
		func(bytes, logicalIDs uint64) (UnrootedGenerationReservation, GenerationMigrationManifest, error) {
			grows++
			r := UnrootedGenerationReservation{Offset: nextOffset, Length: bytes, FirstLogicalID: nextLogical, LogicalIDCount: logicalIDs}
			nextOffset += bytes
			nextLogical += logicalIDs
			return r, GenerationMigrationManifest{TargetFileEnd: nextOffset}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for rank := 0; rank < 9; rank++ {
		page, err := sink.AllocatePage(PagePrimaryLeaf, pageSize, 0)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := InitPage(page.Bytes(), PageHeader{StoreID: testStoreID, Generation: 8, LogicalID: page.Ref().LogicalID, PageSize: pageSize, PayloadLength: 1, Kind: PagePrimaryLeaf})
		if err != nil {
			t.Fatal(err)
		}
		payload[0] = byte(rank)
		if _, err := SealPage(page.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := page.Stage(); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Sync(); err != nil {
		t.Fatal(err)
	}
	if grows != 3 || sink.BuildFileEnd() != (64<<10)+3*(16<<10) {
		t.Fatalf("grows=%d fileEnd=%d", grows, sink.BuildFileEnd())
	}
}
