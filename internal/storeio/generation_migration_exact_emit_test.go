package storeio

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"
)

func TestStreamGenerationMigrationExactLeavesMatchesCanonicalCutter(t *testing.T) {
	sink := &migrationExactMemorySink{storeID: testStoreID, generation: 9, nextID: 100, nextAt: 64 << 10, pages: make(map[PageRef][]byte)}
	builder, err := NewGenerationMigrationExactRunBuilder(sink, 4096, 37, IndexTermMaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	live := make(map[uint32]*[TermPostingTileChunks]uint64)
	baseline := make([]IndexTermLeafTerm, 0, 80)
	for termAt := 0; termAt < 80; termAt++ {
		key := migrationExactRunKey(t, fmt.Sprintf(`"term-%04d"`, termAt))
		postingCount := 1 + termAt%5
		if termAt == 40 {
			postingCount = 300
		}
		postings := make([]IndexTermLeafPosting, 0, postingCount)
		for postingAt := 0; postingAt < postingCount; postingAt++ {
			tileID := uint32(postingAt)
			mask := uint64(1) << uint((termAt+postingAt)&63)
			liveMask := live[tileID]
			if liveMask == nil {
				liveMask = new([TermPostingTileChunks]uint64)
				live[tileID] = liveMask
			}
			liveMask[0] |= mask
			if err := builder.Add(0, key, tileID, mask); err != nil {
				t.Fatal(err)
			}
			postings = append(postings, IndexTermLeafPosting{Posting: TermPosting{TileID: tileID, Rows: uint16(bits.OnesCount64(mask))}, Live: liveMask, Chunk0Bits: mask, Chunk0Only: true})
		}
		record, ok := OpenIndexTermKeyRecord(testStoreID, key)
		if !ok {
			t.Fatal("term record")
		}
		baseline = append(baseline, IndexTermLeafTerm{Key: record, Postings: postings})
	}
	region, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	read := func(ref PageRef, dst []byte) error {
		image := sink.pages[ref]
		if len(image) != len(dst) {
			return ErrGenerationMigrationManifestCorrupt
		}
		copy(dst, image)
		return nil
	}
	merged, err := MergeGenerationMigrationExactRuns(sink, read, region, 4096, 4)
	if err != nil {
		t.Fatal(err)
	}
	budget := IndexTermLeafCutBudget(4096)
	var want [][]byte
	var wantPieces []bool
	if err := CutIndexTermLeaves(baseline, budget, func(leaf []IndexTermLeafTerm, piece bool) error {
		encoded, err := AppendIndexTermLeaf(nil, testStoreID, leaf)
		if err == nil {
			want = append(want, encoded)
			wantPieces = append(wantPieces, piece)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	var gotPieces []bool
	err = StreamGenerationMigrationExactLeaves(read, merged, testStoreID, 9, budget, func(tileID uint32) *[TermPostingTileChunks]uint64 { return live[tileID] }, func(indexID uint32, leaf []IndexTermLeafTerm, piece bool) error {
		if indexID != 0 {
			t.Fatalf("index id=%d", indexID)
		}
		encoded, err := AppendIndexTermLeaf(nil, testStoreID, leaf)
		if err == nil {
			got = append(got, encoded)
			gotPieces = append(gotPieces, piece)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || len(gotPieces) != len(wantPieces) {
		t.Fatalf("leaf count got=%d want=%d", len(got), len(want))
	}
	for index := range want {
		if gotPieces[index] != wantPieces[index] || !bytes.Equal(got[index], want[index]) {
			t.Fatalf("leaf %d differs: piece got=%v want=%v bytes=%v", index, gotPieces[index], wantPieces[index], bytes.Equal(got[index], want[index]))
		}
	}
}
