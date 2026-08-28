package durable

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestPrimaryExactIndexesEmitIntoUnrootedGenerationSink(t *testing.T) {
	normalized, err := (Options{
		Indexes: []store.IndexDefinition{
			{Name: "by_country", Paths: []string{"/country"}},
			{Name: "by_country_active", Paths: []string{"/country", "/active"}},
		},
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	const rows = 128
	records := make([]storeio.PrimaryGraphRecord, rows)
	placements := make([]storeio.PrimaryGraphPlacement, rows)
	for row := range rows {
		records[row] = storeio.PrimaryGraphRecord{
			Key: fmt.Sprintf("key-%04d", row),
			Value: fmt.Sprintf(
				`{"country":"c%02d","active":%t}`, row%13, row&1 == 0,
			),
		}
		placements[row] = storeio.PrimaryGraphPlacement{
			Bucket: 0, Slot: uint8(row),
		}
	}
	file, err := os.CreateTemp(t.TempDir(), "exact-unrooted-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reservation := storeio.UnrootedGenerationReservation{
		Offset: 64 << 10, Length: 16 << 20,
		FirstLogicalID: storeio.PrimaryFirstDynamicLogicalID,
		LogicalIDCount: 1 << 20,
	}
	writer, err := storeio.NewUnrootedGenerationWriter(
		file, reservation, [16]byte{1}, 19, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := storeio.NewUnrootedPrimaryGraphSink(
		writer, [16]byte{1}, 19, reservation.FirstLogicalID,
		reservation.Offset+reservation.Length, make([]byte, 512<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err := buildPrimaryExactIndexes(
		sink, records, placements, normalized.indexes, 4096, 64<<10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	image := make([]byte, root.Length)
	if _, err := file.ReadAt(image, int64(root.Offset)); err != nil {
		t.Fatal(err)
	}
	view, err := storeio.OpenPrimaryExactRootPage(
		image, root,
		storeio.PrimaryExactIndexBounds{
			StoreID: [16]byte{1}, Generation: 19,
			FileEnd:           reservation.Offset + reservation.Length,
			NextLogicalID:     sink.BuildNextLogicalID(),
			AllocationQuantum: 4096, MaxPageSize: 64 << 10,
			IndexCount: uint32(len(normalized.indexes)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != len(normalized.indexes) {
		t.Fatalf("exact root indexes=%d, want %d", view.Len(), len(normalized.indexes))
	}
}
