package storeio

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"
)

// buildRowsTestPage encodes one document page whose rows have deliberately
// unequal key and JSON lengths. Equal-length rows would hide every boundary
// mistake the iterator can make, because a wrong cumulative end would still
// land on a row boundary.
func buildRowsTestPage(t *testing.T, live uint64) DocumentPageView {
	t.Helper()
	rows := make([]DocumentRecord, 0, bits.OnesCount64(live))
	for remaining := live; remaining != 0; remaining &= remaining - 1 {
		slot := uint8(bits.TrailingZeros64(remaining))
		key := fmt.Appendf(nil, "k%s", bytes.Repeat([]byte{'a' + slot%23}, 1+int(slot)%7))
		json := fmt.Appendf(nil, `{"slot":%d,"pad":%q}`, slot, bytes.Repeat([]byte{'x'}, int(slot)%11))
		rows = append(rows, DocumentRecord{Key: key, JSON: json, Slot: slot})
	}
	page := make([]byte, 4096)
	encoded, err := EncodeDocumentPage(page, DocumentPageHeader{
		StoreID: [16]byte{1}, Generation: 1, LogicalID: 9, PageSize: 4096,
		ChunkID: 3, Live: live,
	}, rows, 64)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenDocumentPage(encoded, 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

// TestDocumentPageRowsMatchLookup pins DocumentPageRows to the random-access
// primitive it was introduced to replace. The iterator carries a row's start
// offset forward from the previous row instead of reloading it, which is only
// valid while consecutive packed ranks are consumed; a mask that skips live
// rows breaks that adjacency. Iterating every mask over a page whose rows have
// differing lengths is what makes a stale carried offset observable as a key
// or JSON slice that spans the skipped rows.
func TestDocumentPageRowsMatchLookup(t *testing.T) {
	for _, live := range []uint64{
		0x1, 0x3, 0xF, 0xFF, 0x5555, 0xAAAA, 0xFFFF,
		1<<0 | 1<<7 | 1<<8 | 1<<31 | 1<<63,
		^uint64(0) >> 40,
	} {
		view := buildRowsTestPage(t, live)
		for _, mask := range []uint64{
			^uint64(0), live, 0, 0x1, 0x9, 0x5555555555555555, 0xAAAAAAAAAAAAAAAA,
			1<<0 | 1<<63, live &^ 0x3, live &^ 0xF0,
		} {
			t.Run(fmt.Sprintf("live=%#x/mask=%#x", live, mask), func(t *testing.T) {
				rows := view.Rows(mask)
				want := live & mask
				for expected := want; expected != 0; expected &= expected - 1 {
					wantSlot := uint8(bits.TrailingZeros64(expected))
					slot, key, json, overflow, ok := rows.Next()
					if !ok {
						t.Fatalf("Rows ended early at slot %d (corrupt=%v)", wantSlot, rows.Err())
					}
					if slot != wantSlot || overflow {
						t.Fatalf("Rows = (slot %d, overflow %v), want slot %d inline", slot, overflow, wantSlot)
					}
					record, found := view.Lookup(wantSlot)
					if !found {
						t.Fatalf("Lookup(%d) missing", wantSlot)
					}
					if !bytes.Equal(key, record.Key) {
						t.Fatalf("slot %d key = %q, want %q", wantSlot, key, record.Key)
					}
					if !bytes.Equal(json, record.JSON) {
						t.Fatalf("slot %d json = %q, want %q", wantSlot, json, record.JSON)
					}
				}
				if _, _, _, _, ok := rows.Next(); ok {
					t.Fatal("Rows reported a row past the selected set")
				}
				if rows.Err() {
					t.Fatal("Rows reported corruption on a well-formed page")
				}
			})
		}
	}
}
