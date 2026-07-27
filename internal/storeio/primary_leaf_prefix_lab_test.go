package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func primaryLeafPrefixLabRecords(
	t testing.TB, keys [][]byte,
) []CommonPrimaryLeafRecord {
	t.Helper()
	records := make([]CommonPrimaryLeafRecord, len(keys))
	for i := range keys {
		records[i] = CommonPrimaryLeafRecord{
			Key: keys[i],
			Value: CommonPrimaryLeafValue{
				Inline: []byte{byte(i), byte(i >> 8), 0xa5},
			},
		}
	}
	if err := PlaceCommonPrimaryLeafRecords(
		CommonPrimaryLeafNarrow, commonPrimaryLeafTestSeed, records,
	); err != nil {
		t.Fatal(err)
	}
	return records
}

func primaryLeafPrefixLabDenseKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("doc:%08d", i))
	}
	return keys
}

func TestPrimaryLeafPrefixLabRoundTrip(t *testing.T) {
	keys := primaryLeafPrefixLabDenseKeys(CommonPrimaryLeafNarrowLive)
	records := primaryLeafPrefixLabRecords(t, keys)
	for _, restart := range []int{4, 8, 16} {
		t.Run(fmt.Sprintf("R%d", restart), func(t *testing.T) {
			image, err := EncodePrimaryLeafPrefixLab(
				commonPrimaryLeafTestSeed, restart, records,
			)
			if err != nil {
				t.Fatal(err)
			}
			view, err := OpenPrimaryLeafPrefixLab(
				image, commonPrimaryLeafTestSeed,
			)
			if err != nil {
				t.Fatal(err)
			}
			if view.Len() != len(records) {
				t.Fatalf("Len=%d", view.Len())
			}
			for i := range records {
				slot, value, overflow, ok := view.Lookup(records[i].Key)
				if !ok || overflow || slot != records[i].Slot ||
					!bytes.Equal(value, records[i].Value.Inline) {
					t.Fatalf("lookup rank=%d slot=%d ok=%v", i, slot, ok)
				}
				if got := view.LowerBound(records[i].Key); got != i {
					t.Fatalf("lower bound rank=%d got=%d", i, got)
				}
			}
			if _, _, _, ok := view.Lookup([]byte("not-present")); ok {
				t.Fatal("unexpected miss")
			}
			it := view.AllRows()
			for i := range records {
				key, value, overflow, ok := it.NextRawBorrowed()
				if !ok || overflow ||
					!bytes.Equal(key, records[i].Key) ||
					!bytes.Equal(value, records[i].Value.Inline) {
					t.Fatalf("iteration rank=%d ok=%v", i, ok)
				}
			}
			if _, _, _, ok := it.NextRawBorrowed(); ok {
				t.Fatal("iterator ran past end")
			}
		})
	}
}

func TestPrimaryLeafPrefixLabEmpty(t *testing.T) {
	image, err := EncodePrimaryLeafPrefixLab(
		commonPrimaryLeafTestSeed, 8, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenPrimaryLeafPrefixLab(image, commonPrimaryLeafTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != 0 {
		t.Fatalf("Len=%d", view.Len())
	}
	if _, _, _, ok := view.Lookup([]byte("missing")); ok {
		t.Fatal("empty lookup hit")
	}
	it := view.AllRows()
	if _, _, _, ok := it.NextRawBorrowed(); ok {
		t.Fatal("empty iterator row")
	}
}

func TestPrimaryLeafPrefixLabLongKeysAndCorruption(t *testing.T) {
	keys := [][]byte{
		bytes.Repeat([]byte("a"), 126),
		append(bytes.Repeat([]byte("a"), 200), 'c'),
		append(bytes.Repeat([]byte("a"), 126), 'b'),
		bytes.Repeat([]byte("b"), 256),
	}
	records := primaryLeafPrefixLabRecords(t, keys)
	image, err := EncodePrimaryLeafPrefixLab(
		commonPrimaryLeafTestSeed, 4, records,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenPrimaryLeafPrefixLab(image, commonPrimaryLeafTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		if _, _, _, ok := view.Lookup(records[i].Key); !ok {
			t.Fatalf("long lookup %d", i)
		}
	}
	bad := append([]byte(nil), image...)
	bad[2] = 3
	if _, err := OpenPrimaryLeafPrefixLab(
		bad, commonPrimaryLeafTestSeed,
	); !errors.Is(err, ErrPrimaryLeafPrefixLabCorrupt) {
		t.Fatalf("bad restart err=%v", err)
	}
}

func TestPrimaryLeafPrefixLabZeroAllocHotPaths(t *testing.T) {
	records := primaryLeafPrefixLabRecords(
		t, primaryLeafPrefixLabDenseKeys(CommonPrimaryLeafNarrowLive),
	)
	image, err := EncodePrimaryLeafPrefixLab(
		commonPrimaryLeafTestSeed, 8, records,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenPrimaryLeafPrefixLab(image, commonPrimaryLeafTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	target := records[len(records)/2].Key
	if allocs := testing.AllocsPerRun(1000, func() {
		_, _, _, _ = view.Lookup(target)
	}); allocs != 0 {
		t.Fatalf("lookup allocs=%f", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		it := view.AllRows()
		for {
			if _, _, _, ok := it.NextRawBorrowed(); !ok {
				break
			}
		}
	}); allocs != 0 {
		t.Fatalf("iteration allocs=%f", allocs)
	}
}
