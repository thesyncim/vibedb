package storeio

import (
	"bytes"
	"fmt"
	"testing"
)

// The tree descents reconstruct directory pages with AdmittedKeyDirectoryPage
// and AdmittedChunkDirectoryPage instead of revalidating them, which is only
// sound if the reconstruction observes exactly what the validating opener
// would. These tests pin that equality. They are the safety net under the read
// path's fast route: a reconstruction that disagreed with the opener would not
// fail loudly, it would silently resolve keys to the wrong rows.

func TestAdmittedKeyDirectoryPageMatchesValidatedLeaf(t *testing.T) {
	header := testKeyDirectoryHeader(0, 9)
	entries := []KeyDirectoryEntry{
		{Key: []byte(""), Location: KeyLocation{Chunk: 0, Slot: 63}},
		{Key: []byte("alpha"), Location: KeyLocation{Chunk: 3, Slot: 5}},
		{Key: []byte("mid"), Location: KeyLocation{Chunk: 7, Slot: 11}},
		{Key: []byte("omega"), Location: KeyLocation{Chunk: 19, Slot: 0}},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeKeyDirectoryLeaf(page, header, entries, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := OpenKeyDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	admitted := AdmittedKeyDirectoryPage(encoded)
	if admitted.Header() != validated.Header() || admitted.Len() != validated.Len() {
		t.Fatalf("admitted leaf = (%+v,%d), want (%+v,%d)",
			admitted.Header(), admitted.Len(), validated.Header(), validated.Len())
	}
	for rank := range entries {
		wantEntry, _ := validated.EntryAt(rank)
		gotEntry, ok := admitted.EntryAt(rank)
		if !ok || !bytes.Equal(gotEntry.Key, wantEntry.Key) || gotEntry.Location != wantEntry.Location {
			t.Fatalf("EntryAt(%d) = (%+v,%v), want %+v", rank, gotEntry, ok, wantEntry)
		}
	}
	// Probe hits and misses, including keys that bracket every stored key, so a
	// wrong dataStart cannot pass by luck on the entries alone.
	for _, key := range [][]byte{nil, []byte(""), []byte("alpha"), []byte("alph"), []byte("alpha!"),
		[]byte("mid"), []byte("omega"), []byte("zeta")} {
		wantLocation, wantOK := validated.Lookup(key)
		gotLocation, gotOK := admitted.Lookup(key)
		if gotOK != wantOK || gotLocation != wantLocation {
			t.Fatalf("Lookup(%q) = (%+v,%v), want (%+v,%v)", key, gotLocation, gotOK, wantLocation, wantOK)
		}
	}
}

func TestAdmittedKeyDirectoryPageMatchesValidatedBranch(t *testing.T) {
	header := testKeyDirectoryHeader(1, 9)
	children := make([]KeyDirectoryChild, 0, 6)
	for i := range 6 {
		children = append(children, KeyDirectoryChild{
			Lower: fmt.Appendf(nil, "lower-%02d", i),
			Ref: PageRef{
				Offset: uint64(4+i) * uint64(testSuperblockPageSize), LogicalID: uint64(20 + i),
				Generation: 3, Length: testSuperblockPageSize, Kind: PageKeyDirectory,
			},
		})
	}
	children[0].Lower = nil
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeKeyDirectoryBranch(page, header, children, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := OpenKeyDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	admitted := AdmittedKeyDirectoryPage(encoded)
	if admitted.Header() != validated.Header() || admitted.Len() != validated.Len() {
		t.Fatalf("admitted branch = (%+v,%d), want (%+v,%d)",
			admitted.Header(), admitted.Len(), validated.Header(), validated.Len())
	}
	for rank := range children {
		wantChild, _ := validated.ChildAt(rank)
		gotChild, ok := admitted.ChildAt(rank)
		if !ok || !bytes.Equal(gotChild.Lower, wantChild.Lower) || gotChild.Ref != wantChild.Ref {
			t.Fatalf("ChildAt(%d) = (%+v,%v), want %+v", rank, gotChild, ok, wantChild)
		}
	}
	for _, key := range [][]byte{nil, []byte(""), []byte("lower-00"), []byte("lower-02"),
		[]byte("lower-029"), []byte("lower-05"), []byte("zzz")} {
		wantRef, wantOK := validated.Child(key)
		gotRef, gotOK := admitted.Child(key)
		if gotOK != wantOK || gotRef != wantRef {
			t.Fatalf("Child(%q) = (%+v,%v), want (%+v,%v)", key, gotRef, gotOK, wantRef, wantOK)
		}
	}
}
