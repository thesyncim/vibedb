package storeio

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"
)

// buildGroupRowsPage encodes one logical chunk whose rows deliberately do not
// share a single template and whose values are a mix of dictionary-eligible
// repeats and unique literals.
//
// Both properties matter. A single-template chunk would never exercise the
// iterator's cached-template invalidation, and an all-repeats corpus would
// never take the inline-literal branch of the token loop, so a page built the
// obvious way would pass even with either path broken.
func buildGroupRowsPage(t *testing.T, live uint64) DocumentGroupChunkView {
	t.Helper()
	build := func(live uint64) []DocumentGroupRecord {
		rows := make([]DocumentGroupRecord, 0, bits.OnesCount64(live))
		for remaining := live; remaining != 0; remaining &= remaining - 1 {
			slot := uint8(bits.TrailingZeros64(remaining))
			var json []byte
			var spans []DocumentGroupSpan
			if slot%3 == 0 {
				// Shape A, repeated categorical value: dictionary-eligible.
				json = fmt.Appendf(nil, `{"kind":"same","n":%d}`, slot%10)
				spans = []DocumentGroupSpan{{Start: 8, End: 14}, {Start: 19, End: 20}}
			} else {
				// Shape B, unique value of varying length: inline literal.
				unique := string(bytes.Repeat([]byte{'a' + slot%23}, 1+int(slot)%9))
				json = fmt.Appendf(nil, `{"other":%q,"n":%d}`, unique, slot%10)
				spans = []DocumentGroupSpan{
					{Start: 9, End: uint32(11 + len(unique))},
					{Start: uint32(16 + len(unique)), End: uint32(17 + len(unique))},
				}
			}
			rows = append(rows, DocumentGroupRecord{
				Key:   fmt.Appendf(nil, "key-%03d", slot),
				JSON:  json,
				Slot:  slot,
				Spans: spans,
			})
		}
		return rows
	}
	// A group is at least two chunks by construction, so the fixture supplies a
	// neighbour whose rows differ; it also proves the iterator stays inside the
	// chunk it was asked for.
	chunks := []DocumentGroupChunk{
		{ChunkID: 7, Live: live, Rows: build(live)},
		{ChunkID: 8, Live: 0b11, Rows: build(0b11)},
	}
	var workspace DocumentGroupWorkspace
	size, ok := DocumentGroupSize(chunks, testSuperblockPageSize, &workspace)
	if !ok {
		t.Fatal("DocumentGroupSize rejected valid chunks")
	}
	page := make([]byte, size)
	encoded, err := EncodeDocumentGroup(page, DocumentGroupHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: 11, PageSize: size,
		FirstChunk: 7, ChunkCount: 2,
		RowCount: uint16(bits.OnesCount64(live) + 2),
	}, chunks, 20, &workspace)
	if err != nil {
		t.Fatalf("EncodeDocumentGroup: %v", err)
	}
	view, err := OpenDocumentGroup(encoded, 9, 20)
	if err != nil {
		t.Fatalf("OpenDocumentGroup: %v", err)
	}
	chunk, ok := view.Chunk(7)
	if !ok {
		t.Fatal("Chunk(7) missing")
	}
	return chunk
}

// TestDocumentGroupRowsMatchAppendJSON pins the sequential group decoder to the
// random-access primitives it replaced. DocumentGroupRows advances a packed
// rank instead of re-deriving it from a stable slot with a popcount, and caches
// the resolved template between rows; a mask that skips live rows and a chunk
// whose rows alternate templates are exactly the two inputs that make either
// shortcut observable as a wrong key or a document decoded against the previous
// row's template.
func TestDocumentGroupRowsMatchAppendJSON(t *testing.T) {
	for _, live := range []uint64{
		0x1, 0x7, 0xFF, 0x5555, 0xAAAA,
		1<<0 | 1<<5 | 1<<6 | 1<<31 | 1<<63,
		^uint64(0) >> 48,
	} {
		view := buildGroupRowsPage(t, live)
		for _, mask := range []uint64{
			^uint64(0), live, 0, 0x1, 0x21, 0x5555555555555555,
			1<<0 | 1<<63, live &^ 0x7, live &^ 0x30,
		} {
			t.Run(fmt.Sprintf("live=%#x/mask=%#x", live, mask), func(t *testing.T) {
				rows := view.Rows(mask)
				scratch := make([]byte, 0, 256)
				for expected := live & mask; expected != 0; expected &= expected - 1 {
					wantSlot := uint8(bits.TrailingZeros64(expected))
					slot, key, json, ok := rows.Next(scratch[:0])
					if !ok {
						t.Fatalf("Rows ended early at slot %d (corrupt=%v)", wantSlot, rows.Err())
					}
					if slot != wantSlot {
						t.Fatalf("Rows slot = %d, want %d", slot, wantSlot)
					}
					record, found := view.Lookup(wantSlot)
					if !found {
						t.Fatalf("Lookup(%d) missing", wantSlot)
					}
					if !bytes.Equal(key, record.Key) {
						t.Fatalf("slot %d key = %q, want %q", wantSlot, key, record.Key)
					}
					want, decoded := view.AppendJSON(nil, wantSlot)
					if !decoded {
						t.Fatalf("AppendJSON(%d) failed", wantSlot)
					}
					if !bytes.Equal(json, want) {
						t.Fatalf("slot %d json = %q, want %q", wantSlot, json, want)
					}
					if len(json) != int(record.JSONLength) {
						t.Fatalf("slot %d decoded %d bytes, want %d", wantSlot, len(json), record.JSONLength)
					}
				}
				if _, _, _, ok := rows.Next(scratch[:0]); ok {
					t.Fatal("Rows reported a row past the selected set")
				}
				if rows.Err() {
					t.Fatal("Rows reported corruption on a well-formed page")
				}
			})
		}
	}
}
