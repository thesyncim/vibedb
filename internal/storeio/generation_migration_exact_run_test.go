package storeio

import (
	"errors"
	"testing"
)

func migrationExactRunKey(t *testing.T, raw string) []byte {
	t.Helper()
	key, ok := AppendIndexTermKey(nil, []IndexTermComponent{{Kind: IndexTermString, Direction: IndexTermAscending, JSON: []byte(raw)}})
	if !ok {
		t.Fatal("canonical key")
	}
	return key
}

func TestGenerationMigrationExactRunPageCanonicalAndZeroAllocation(t *testing.T) {
	keys := [][]byte{migrationExactRunKey(t, `"a"`), migrationExactRunKey(t, `"b"`)}
	records := []GenerationMigrationExactRunRecord{
		{IndexID: 0, Key: keys[0], TileID: 1, Mask: 3},
		{IndexID: 0, Key: keys[0], TileID: 2, Mask: 4},
		{IndexID: 0, Key: keys[1], TileID: 0, Mask: 8},
		{IndexID: 1, Key: keys[0], TileID: 0, Mask: 16},
	}
	ref := PageRef{Offset: 64 << 10, LogicalID: 91, Generation: 7, Length: 4096, Kind: PageMigrationExactRun}
	page := make([]byte, ref.Length)
	if _, err := EncodeGenerationMigrationExactRunPage(page, testStoreID, 7, ref.LogicalID, 11, 3, true, records); err != nil {
		t.Fatal(err)
	}
	view, err := OpenGenerationMigrationExactRunPage(page, ref, testStoreID, 7)
	if err != nil {
		t.Fatal(err)
	}
	if view.RunID() != 11 || view.PageOrdinal() != 3 || !view.Last() || view.Len() != len(records) {
		t.Fatalf("view run=%d ordinal=%d last=%v len=%d", view.RunID(), view.PageOrdinal(), view.Last(), view.Len())
	}
	allocs := testing.AllocsPerRun(100, func() {
		it := view.Iterator()
		count := 0
		for {
			_, ok := it.Next()
			if !ok {
				break
			}
			count++
		}
		if count != len(records) {
			t.Fatalf("records=%d", count)
		}
	})
	if allocs != 0 {
		t.Fatalf("iterator allocations=%.2f want zero", allocs)
	}
	bad := append([]GenerationMigrationExactRunRecord(nil), records...)
	bad[1], bad[2] = bad[2], bad[1]
	if _, err := EncodeGenerationMigrationExactRunPage(page, testStoreID, 7, ref.LogicalID, 11, 3, true, bad); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("unordered encode err=%v", err)
	}
}
