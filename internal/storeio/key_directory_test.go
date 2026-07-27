package storeio

import (
	"encoding/binary"
	"errors"
	"testing"
)

const (
	testKeyDirectoryNextLogicalID = uint64(100)
	testKeyDirectoryFileEnd       = uint64(32 * testSuperblockPageSize)
)

func testKeyDirectoryHeader(level uint8, logicalID uint64) KeyDirectoryHeader {
	return KeyDirectoryHeader{
		StoreID: testStoreID, Generation: 11, LogicalID: logicalID,
		PageSize: testSuperblockPageSize, Level: level,
	}
}

func TestKeyDirectoryLeafRoundTripAndLookup(t *testing.T) {
	header := testKeyDirectoryHeader(0, 9)
	entries := []KeyDirectoryEntry{
		{Key: []byte(""), Location: KeyLocation{Chunk: 0, Slot: 63}},
		{Key: []byte("alpha"), Location: KeyLocation{Chunk: 3, Slot: 5}},
		{Key: []byte("omega"), Location: KeyLocation{Chunk: 19, Slot: 0}},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeKeyDirectoryLeaf(page, header, entries, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenKeyDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(entries) {
		t.Fatalf("leaf = (%+v,%d), want (%+v,%d)", view.Header(), view.Len(), header, len(entries))
	}
	for rank, want := range entries {
		got, ok := view.Lookup(want.Key)
		if !ok || got != want.Location {
			t.Fatalf("Lookup(%q) = (%+v,%v), want (%+v,true)", want.Key, got, ok, want.Location)
		}
		entry, ok := view.EntryAt(rank)
		if !ok || string(entry.Key) != string(want.Key) || entry.Location != want.Location {
			t.Fatalf("EntryAt(%d) = (%+v,%v), want %+v", rank, entry, ok, want)
		}
		if cap(entry.Key) != len(entry.Key) {
			t.Fatalf("EntryAt(%d) key exposes capacity %d/%d", rank, len(entry.Key), cap(entry.Key))
		}
	}
	for _, key := range [][]byte{[]byte("alp"), []byte("alpha!"), []byte("zeta")} {
		if got, ok := view.Lookup(key); ok {
			t.Fatalf("Lookup(%q) = (%+v,true), want miss", key, got)
		}
	}
	if _, ok := view.EntryAt(-1); ok {
		t.Fatal("EntryAt(-1) hit")
	}
	if _, ok := view.Child([]byte("alpha")); ok {
		t.Fatal("leaf Child hit")
	}
}

func TestKeyDirectoryBranchRoundTripAndSelection(t *testing.T) {
	header := testKeyDirectoryHeader(2, 12)
	children := []KeyDirectoryChild{
		{Lower: []byte(""), Ref: testKeyDirectoryRef(2, 4, 10)},
		{Lower: []byte("m"), Ref: testKeyDirectoryRef(3, 5, 11)},
		{Lower: []byte("z"), Ref: testKeyDirectoryRef(4, 6, 11)},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeKeyDirectoryBranch(page, header, children, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenKeyDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(children) {
		t.Fatalf("branch = (%+v,%d), want (%+v,%d)", view.Header(), view.Len(), header, len(children))
	}
	for rank, want := range children {
		got, ok := view.ChildAt(rank)
		if !ok || string(got.Lower) != string(want.Lower) || got.Ref != want.Ref {
			t.Fatalf("ChildAt(%d) = (%+v,%v), want %+v", rank, got, ok, want)
		}
	}
	for _, test := range []struct {
		key  string
		want PageRef
	}{
		{"", children[0].Ref}, {"a", children[0].Ref}, {"m", children[1].Ref},
		{"middle", children[1].Ref}, {"z", children[2].Ref}, {"zz", children[2].Ref},
	} {
		got, ok := view.Child([]byte(test.key))
		if !ok || got != test.want {
			t.Fatalf("Child(%q) = (%+v,%v), want (%+v,true)", test.key, got, ok, test.want)
		}
	}
	if _, ok := view.Lookup([]byte("m")); ok {
		t.Fatal("branch Lookup hit")
	}

	bounded := []KeyDirectoryChild{{Lower: []byte("b"), Ref: children[0].Ref}}
	if _, err := EncodeKeyDirectoryBranch(page, header, bounded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID); err != nil {
		t.Fatal(err)
	}
	view, err = OpenKeyDirectoryPage(page, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 20, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := view.Child([]byte("a")); ok {
		t.Fatal("key before first lower bound selected a child")
	}
}

func TestKeyDirectoryRejectsInvalidWrites(t *testing.T) {
	header := testKeyDirectoryHeader(0, 9)
	valid := []KeyDirectoryEntry{
		{Key: []byte("a"), Location: KeyLocation{Chunk: 1, Slot: 2}},
		{Key: []byte("b"), Location: KeyLocation{Chunk: 2, Slot: 3}},
	}
	for _, test := range []struct {
		name    string
		entries []KeyDirectoryEntry
		chunks  uint32
		docs    uint8
	}{
		{"empty", nil, 3, 64},
		{"duplicate", []KeyDirectoryEntry{valid[0], valid[0]}, 3, 64},
		{"unordered", []KeyDirectoryEntry{valid[1], valid[0]}, 3, 64},
		{"chunk", []KeyDirectoryEntry{{Key: []byte("a"), Location: KeyLocation{Chunk: 3}}}, 3, 64},
		{"slot", []KeyDirectoryEntry{{Key: []byte("a"), Location: KeyLocation{Slot: 64}}}, 3, 64},
		{"zero chunks", valid, 0, 64},
		{"zero docs", valid, 3, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			page := make([]byte, testSuperblockPageSize)
			if _, err := EncodeKeyDirectoryLeaf(page, header, test.entries, testKeyDirectoryNextLogicalID, test.chunks, test.docs); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("EncodeKeyDirectoryLeaf = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	branchHeader := testKeyDirectoryHeader(1, 9)
	ref := testKeyDirectoryRef(2, 4, 11)
	for _, children := range [][]KeyDirectoryChild{
		nil,
		{{Lower: []byte("a"), Ref: ref}, {Lower: []byte("a"), Ref: testKeyDirectoryRef(3, 5, 11)}},
		{{Lower: []byte("a"), Ref: ref}, {Lower: []byte("b"), Ref: ref}},
	} {
		page := make([]byte, testSuperblockPageSize)
		if _, err := EncodeKeyDirectoryBranch(page, branchHeader, children, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("EncodeKeyDirectoryBranch = %v, want %v", err, ErrInvalidWrite)
		}
	}
}

func TestKeyDirectoryRejectsResealedSemanticCorruption(t *testing.T) {
	header := testKeyDirectoryHeader(0, 9)
	entries := []KeyDirectoryEntry{
		{Key: []byte("a"), Location: KeyLocation{Chunk: 1, Slot: 2}},
		{Key: []byte("b"), Location: KeyLocation{Chunk: 2, Slot: 3}},
	}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeKeyDirectoryLeaf(page, header, entries, testKeyDirectoryNextLogicalID, 3, 64); err != nil {
		t.Fatal(err)
	}
	first := PageHeaderSize + KeyDirectoryPayloadHeaderSize
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"version", func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize:], 3) }},
		{"reserved header", func(p []byte) { p[PageHeaderSize+12] = 1 }},
		{"slot", func(p []byte) { p[first+8] = 64 }},
		{"reserved record", func(p []byte) { p[first+9] = 1 }},
		{"key order", func(p []byte) { binary.LittleEndian.PutUint32(p[first:first+4], 2) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), page...)
			test.mutate(corrupt)
			resealTestPage(corrupt)
			if _, err := OpenKeyDirectoryPage(corrupt, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 3, 64); !errors.Is(err, ErrKeyDirectoryCorrupt) {
				t.Fatalf("OpenKeyDirectoryPage = %v, want %v", err, ErrKeyDirectoryCorrupt)
			}
		})
	}
}

func testKeyDirectoryRef(logicalID uint64, page uint64, generation uint64) PageRef {
	return PageRef{
		Offset: page * uint64(testSuperblockPageSize), LogicalID: logicalID, Generation: generation,
		Length: testSuperblockPageSize, Kind: PageKeyDirectory,
	}
}
