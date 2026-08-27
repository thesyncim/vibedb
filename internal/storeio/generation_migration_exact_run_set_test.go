package storeio

import (
	"fmt"
	"os"
	"testing"
)

func TestGenerationMigrationExactRunSetMergesDiscontinuousExtents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "exact-run-set-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var nextOffset, nextLogical uint64 = 64 << 10, 100
	sink, err := NewGenerationMigrationChainedSink(
		file, testStoreID, 12, 4096, 16<<10, 4, make([]byte, 64<<10),
		func(bytes, logicalIDs uint64) (UnrootedGenerationReservation, GenerationMigrationManifest, error) {
			r := UnrootedGenerationReservation{Offset: nextOffset, Length: bytes, FirstLogicalID: nextLogical, LogicalIDCount: logicalIDs}
			nextOffset += bytes + 4096 // authenticated chain metadata gap
			nextLogical += logicalIDs + 1
			return r, GenerationMigrationManifest{TargetFileEnd: nextOffset}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	read := func(ref PageRef, dst []byte) error { _, err := file.ReadAt(dst, int64(ref.Offset)); return err }
	set, err := NewGenerationMigrationExactRunSet(sink, read, 4096)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewStreamingGenerationMigrationExactRunBuilder(sink, 4096, 4, 4096, set.Add)
	if err != nil {
		t.Fatal(err)
	}
	for rank := 63; rank >= 0; rank-- {
		key := migrationExactRunKey(t, fmt.Sprintf("%q", fmt.Sprintf("k-%03d", rank)))
		if err := builder.Add(0, key, uint32(rank), 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	merged, err := set.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if merged.Runs != 1 || merged.Pages == 0 {
		t.Fatalf("merged=%+v", merged)
	}
	span, next, err := scanGenerationMigrationExactRunSpan(read, merged, 0, make([]byte, 4096), testStoreID, 12)
	if err != nil || next != merged.Pages || span.pages != merged.Pages {
		t.Fatalf("span=%+v next=%d err=%v", span, next, err)
	}
	cursor := generationMigrationExactRunCursor{read: read, span: span, page: make([]byte, 4096)}
	if err := cursor.start(testStoreID, 12); err != nil {
		t.Fatal(err)
	}
	count := 0
	for cursor.valid {
		count++
		if err := cursor.advance(testStoreID, 12); err != nil {
			t.Fatal(err)
		}
	}
	if count != 64 {
		t.Fatalf("merged records=%d want=64", count)
	}
}
