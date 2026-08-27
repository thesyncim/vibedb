package durable

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryExactExternalRunsEmitReopenableGraph(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "exact-external-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	storeID := [16]byte{1}
	reservation := storeio.UnrootedGenerationReservation{Offset: 64 << 10, Length: 64 << 20, FirstLogicalID: storeio.PrimaryFirstDynamicLogicalID, LogicalIDCount: 1 << 20}
	writer, err := storeio.NewUnrootedGenerationWriter(file, reservation, storeID, 19, 0)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := storeio.NewUnrootedPrimaryGraphSink(writer, storeID, 19, reservation.FirstLogicalID, reservation.Offset+reservation.Length, make([]byte, 512<<10))
	if err != nil {
		t.Fatal(err)
	}
	builder, err := storeio.NewGenerationMigrationExactRunBuilder(sink, 64<<10, 31, 16<<10)
	if err != nil {
		t.Fatal(err)
	}
	live := make(map[uint32]*[storeio.TermPostingTileChunks]uint64)
	for row := 0; row < 512; row++ {
		tile := uint32(row >> 6)
		mask := uint64(1) << uint(row&63)
		liveMask := live[tile]
		if liveMask == nil {
			liveMask = new([storeio.TermPostingTileChunks]uint64)
			live[tile] = liveMask
		}
		liveMask[0] |= mask
		for indexID := uint32(0); indexID < 2; indexID++ {
			canonical, ok := storeio.AppendIndexTermKey(nil, []storeio.IndexTermComponent{{Kind: storeio.IndexTermString, Direction: storeio.IndexTermAscending, JSON: []byte(fmt.Sprintf(`"i%d-%03d"`, indexID, row%97))}})
			if !ok {
				t.Fatal("canonical term")
			}
			if err := builder.Add(indexID, canonical, tile, mask); err != nil {
				t.Fatal(err)
			}
		}
	}
	region, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	read := func(ref storeio.PageRef, dst []byte) error {
		_, err := file.ReadAt(dst, int64(ref.Offset))
		return err
	}
	merged, err := storeio.MergeGenerationMigrationExactRuns(sink, read, region, 64<<10, 8)
	if err != nil {
		t.Fatal(err)
	}
	root, err := buildPrimaryExactIndexesFromMergedRun(sink, read, merged, 2, 4096, 64<<10, func(tile uint32) *[storeio.TermPostingTileChunks]uint64 { return live[tile] })
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	rootImage := make([]byte, root.Length)
	if _, err := file.ReadAt(rootImage, int64(root.Offset)); err != nil {
		t.Fatal(err)
	}
	bounds := storeio.PrimaryExactIndexBounds{StoreID: storeID, Generation: 19, FileEnd: reservation.Offset + reservation.Length, NextLogicalID: sink.BuildNextLogicalID(), AllocationQuantum: 4096, MaxPageSize: 64 << 10, IndexCount: 2}
	view, err := storeio.OpenPrimaryExactRootPage(rootImage, root, bounds)
	if err != nil {
		t.Fatal(err)
	}
	for indexID := uint32(0); indexID < 2; indexID++ {
		entry, ok := view.Entry(indexID)
		if !ok || entry.LeafCount == 0 || entry.Catalog == (storeio.PageRef{}) {
			t.Fatalf("index %d entry=%+v ok=%v", indexID, entry, ok)
		}
	}
}
