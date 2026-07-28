package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func testIndexDirectoryHeader(level uint8, logicalID uint64) IndexDirectoryHeader {
	return IndexDirectoryHeader{
		StoreID: testStoreID, Generation: 11, LogicalID: logicalID,
		PageSize: testSuperblockPageSize, Level: level,
	}
}

// testIndexCertificateArena builds a certificate arena and the spans into it,
// mirroring how a writer accumulates representatives in one buffer and hands
// the encoder spans rather than slices.
func testIndexCertificateArena(certificates ...string) ([]byte, []CertSpan) {
	var arena []byte
	spans := make([]CertSpan, len(certificates))
	for i, certificate := range certificates {
		spans[i] = CertSpan{Offset: uint32(len(arena)), Length: uint16(len(certificate))}
		arena = append(arena, certificate...)
	}
	return arena, spans
}

func TestIndexDirectoryLeafRoundTripAndLookup(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	arena, spans := testIndexCertificateArena(`"cat"`, `"cat"`, `"dog"`)
	entries := []IndexDirectoryEntry{
		{Key: IndexDirectoryKey{IndexID: 0, TupleHash: 1}, Bits: 1 << 3, Cert: spans[0]},
		{Key: IndexDirectoryKey{IndexID: 0, TupleHash: 1, Chunk: 4}, Bits: ^uint64(0), Cert: spans[1]},
		{Key: IndexDirectoryKey{IndexID: 2, TupleHash: 0}, Bits: 7, Cert: spans[2], Flags: IndexEntryCollision},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 3)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != header || view.Len() != len(entries) {
		t.Fatalf("leaf = (%+v,%d), want (%+v,%d)", view.Header(), view.Len(), header, len(entries))
	}
	for rank, want := range entries {
		got, ok := view.Lookup(want.Key)
		if !ok || got.Key != want.Key || got.Bits != want.Bits || got.Flags != want.Flags ||
			!bytes.Equal(view.Certificate(got.Cert), arena[want.Cert.Offset:int(want.Cert.Offset)+int(want.Cert.Length)]) {
			t.Fatalf("Lookup(%+v) = (%+v,%v), want %+v", want.Key, got, ok, want)
		}
		entry, ok := view.EntryAt(rank)
		if !ok || entry != got {
			t.Fatalf("EntryAt(%d) = (%+v,%v), want %+v", rank, entry, ok, got)
		}
	}
	for _, key := range []IndexDirectoryKey{{}, {IndexID: 0, TupleHash: 2}, {IndexID: 1}, {IndexID: 2, TupleHash: 1}} {
		if got, ok := view.Lookup(key); ok {
			t.Fatalf("Lookup(%+v) = (%+v,true), want miss", key, got)
		}
	}
	if _, ok := view.Child(IndexDirectoryKey{}); ok {
		t.Fatal("leaf Child hit")
	}
}

func TestIndexDirectoryBranchRoundTrip(t *testing.T) {
	header := testIndexDirectoryHeader(2, 31)
	children := []IndexDirectoryChild{
		{Lower: IndexDirectoryKey{}, Ref: testIndexPageRef(PageIndexDirectory, 4, 4, 10)},
		{Lower: IndexDirectoryKey{IndexID: 1}, Ref: testIndexPageRef(PageIndexDirectory, 5, 5, 11)},
		{Lower: IndexDirectoryKey{IndexID: 2, TupleHash: 100}, Ref: testIndexPageRef(PageIndexDirectory, 6, 6, 11)},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeIndexDirectoryBranch(page, header, children, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 3)
	if err != nil {
		t.Fatal(err)
	}
	for rank, want := range children {
		got, ok := view.ChildAt(rank)
		if !ok || got != want {
			t.Fatalf("ChildAt(%d) = (%+v,%v), want (%+v,true)", rank, got, ok, want)
		}
	}
	for _, test := range []struct {
		key  IndexDirectoryKey
		want PageRef
	}{
		{IndexDirectoryKey{}, children[0].Ref},
		{IndexDirectoryKey{IndexID: 1, TupleHash: 99}, children[1].Ref},
		{IndexDirectoryKey{IndexID: 2, TupleHash: 200}, children[2].Ref},
	} {
		got, ok := view.Child(test.key)
		if !ok || got != test.want {
			t.Fatalf("Child(%+v) = (%+v,%v), want (%+v,true)", test.key, got, ok, test.want)
		}
	}
}

// TestIndexDirectoryLeafExactFitAndOneByteOver pins the byte budget from both
// sides. A leaf that exactly fills the payload must be admitted, and adding a
// single certificate byte to the same shape must be refused rather than
// silently truncated into the page trailer.
func TestIndexDirectoryLeafExactFitAndOneByteOver(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	budget := indexDirectoryLeafBudget(testSuperblockPageSize)
	const count = 30
	const length = 100
	heap := budget - count*IndexDirectoryLeafRecordSize
	var arena []byte
	entries := make([]IndexDirectoryEntry, count)
	for i := range entries {
		size := length
		if i == 0 {
			// Absorb the remainder into the first representative so records
			// plus heap land exactly on the payload budget.
			size = heap - (count-1)*length
		}
		entries[i] = IndexDirectoryEntry{
			Key:  IndexDirectoryKey{TupleHash: uint64(i)},
			Bits: 1,
			Cert: CertSpan{Offset: uint32(len(arena)), Length: uint16(size)},
		}
		arena = append(arena, bytes.Repeat([]byte{byte('a' + i)}, size)...)
	}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 1); err != nil {
		t.Fatalf("exact-fit leaf = %v, want admitted", err)
	}
	entries[0].Cert.Length++
	arena = append(arena, 'z')
	entries[0].Cert.Offset = uint32(len(arena)) - uint32(entries[0].Cert.Length)
	if _, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("one-byte-over leaf = %v, want %v", err, ErrInvalidWrite)
	}
}

// TestIndexDirectoryLeafRejectsEmptyMask covers the failure that motivates the
// check: a live routing key whose mask is zero answers "no rows" for every
// later probe, which is indistinguishable from a correct empty answer and so
// loses the rows the writer meant to record.
func TestIndexDirectoryLeafRejectsEmptyMask(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	entries := []IndexDirectoryEntry{{Key: IndexDirectoryKey{TupleHash: 1}}}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeIndexDirectoryLeaf(page, header, entries, nil, testKeyDirectoryNextLogicalID, 1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("zero-mask leaf = %v, want %v", err, ErrInvalidWrite)
	}
	entries[0].Bits = 1
	encoded, err := EncodeIndexDirectoryLeaf(page, header, entries, nil, testKeyDirectoryNextLogicalID, 1)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint64(corrupt[PageHeaderSize+IndexDirectoryPayloadHeaderSize+16:], 0)
	resealTestPage(corrupt)
	if _, err := OpenIndexDirectoryPage(corrupt, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 1); !errors.Is(err, ErrIndexDirectoryCorrupt) {
		t.Fatalf("decoded zero mask = %v, want %v", err, ErrIndexDirectoryCorrupt)
	}
}

func TestIndexDirectoryLeafRejectsInvalidWrites(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	arena, spans := testIndexCertificateArena(`"cat"`)
	valid := []IndexDirectoryEntry{
		{Key: IndexDirectoryKey{TupleHash: 1}, Bits: 1, Cert: spans[0]},
		{Key: IndexDirectoryKey{IndexID: 1}, Bits: 2},
	}
	page := make([]byte, testSuperblockPageSize)
	for name, entries := range map[string][]IndexDirectoryEntry{
		"empty":             nil,
		"duplicate key":     {valid[0], valid[0]},
		"descending keys":   {valid[1], valid[0]},
		"id beyond bounds":  {{Key: IndexDirectoryKey{IndexID: 2}, Bits: 1}},
		"unknown flag":      {{Key: IndexDirectoryKey{}, Bits: 1, Flags: 1 << 15, Cert: spans[0]}},
		"unknown kind":      {{Key: IndexDirectoryKey{}, Bits: 1, Kind: 1}},
		"collision no cert": {{Key: IndexDirectoryKey{}, Bits: 1, Flags: IndexEntryCollision}},
		"span past arena":   {{Key: IndexDirectoryKey{}, Bits: 1, Cert: CertSpan{Offset: 4, Length: 8}}},
	} {
		if _, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 2); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("EncodeIndexDirectoryLeaf(%s) = %v, want %v", name, err, ErrInvalidWrite)
		}
	}
}

// TestIndexDirectoryLeafRejectsCorruptHeap walks the ways a checksum-valid
// page can still describe a heap that no writer could have produced. Each of
// them would hand a reader certificate bytes belonging to another entry, or
// none at all, while looking structurally plausible.
func TestIndexDirectoryLeafRejectsCorruptHeap(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	arena, spans := testIndexCertificateArena(`"cat"`, `"dog"`, `"emu"`)
	entries := []IndexDirectoryEntry{
		{Key: IndexDirectoryKey{TupleHash: 1}, Bits: 1, Cert: spans[0]},
		{Key: IndexDirectoryKey{TupleHash: 2}, Bits: 2, Cert: spans[1]},
		{Key: IndexDirectoryKey{TupleHash: 3}, Bits: 4, Cert: spans[2]},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 1)
	if err != nil {
		t.Fatal(err)
	}
	first := PageHeaderSize + IndexDirectoryPayloadHeaderSize
	second := first + IndexDirectoryLeafRecordSize
	heapStart := PageHeaderSize + IndexDirectoryPayloadHeaderSize
	for name, mutate := range map[string]func([]byte){
		"version": func(p []byte) { binary.LittleEndian.PutUint32(p[PageHeaderSize:], 1) },
		"reserved payload byte": func(p []byte) {
			p[PageHeaderSize+12] = 1
		},
		"heapStart below records": func(p []byte) {
			binary.LittleEndian.PutUint32(p[PageHeaderSize+8:], uint32(IndexDirectoryPayloadHeaderSize))
		},
		"heapStart past payload": func(p []byte) {
			binary.LittleEndian.PutUint32(p[PageHeaderSize+8:], uint32(testSuperblockPageSize))
		},
		"descending keys": func(p []byte) {
			binary.LittleEndian.PutUint64(p[second+8:], 0)
		},
		"unknown kind": func(p []byte) { p[first+30] = 3 },
		"reserved pad": func(p []byte) { p[first+31] = 1 },
		"unknown flag": func(p []byte) { binary.LittleEndian.PutUint16(p[first+28:], 1<<9) },
		"empty span holds offset": func(p []byte) {
			binary.LittleEndian.PutUint16(p[first+26:], 0)
			binary.LittleEndian.PutUint16(p[second+26:], 0)
			binary.LittleEndian.PutUint16(p[second+28:], 0)
		},
		"span below heap": func(p []byte) {
			binary.LittleEndian.PutUint16(p[first+24:], uint16(IndexDirectoryPayloadHeaderSize))
		},
		"span past payload": func(p []byte) {
			binary.LittleEndian.PutUint16(p[first+26:], uint16(testSuperblockPageSize))
		},
		"overlapping spans": func(p []byte) {
			binary.LittleEndian.PutUint16(p[second+24:], uint16(heapStart-PageHeaderSize+1))
		},
		"gapped spans": func(p []byte) {
			binary.LittleEndian.PutUint16(p[second+24:], uint16(heapStart-PageHeaderSize+6))
			binary.LittleEndian.PutUint16(p[first+26:], 6)
		},
	} {
		corrupt := append([]byte(nil), encoded...)
		mutate(corrupt)
		resealTestPage(corrupt)
		if _, err := OpenIndexDirectoryPage(corrupt, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 1); !errors.Is(err, ErrIndexDirectoryCorrupt) {
			t.Fatalf("OpenIndexDirectoryPage(%s) = %v, want %v", name, err, ErrIndexDirectoryCorrupt)
		}
	}
}

// TestIndexDirectoryLeafRejectsRedundantCertificate keeps the encoding
// canonical: two neighbours carrying equal representatives must share one heap
// region. A second copy would still decode, but it would make one leaf's byte
// cost depend on which writer produced it, and the split arithmetic assumes it
// does not.
func TestIndexDirectoryLeafRejectsRedundantCertificate(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	arena, spans := testIndexCertificateArena(`"cat"`, `"dog"`)
	entries := []IndexDirectoryEntry{
		{Key: IndexDirectoryKey{TupleHash: 1}, Bits: 1, Cert: spans[0]},
		{Key: IndexDirectoryKey{TupleHash: 2}, Bits: 2, Cert: spans[1]},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the second heap region to equal the first without collapsing the
	// two spans, which is exactly what a writer that skipped the dedup emits.
	heap := PageHeaderSize + IndexDirectoryPayloadHeaderSize + 2*IndexDirectoryLeafRecordSize
	corrupt := append([]byte(nil), encoded...)
	copy(corrupt[heap+5:heap+10], corrupt[heap:heap+5])
	resealTestPage(corrupt)
	if _, err := OpenIndexDirectoryPage(corrupt, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 1); !errors.Is(err, ErrIndexDirectoryCorrupt) {
		t.Fatalf("redundant certificate = %v, want %v", err, ErrIndexDirectoryCorrupt)
	}
}

// TestIndexDirectoryLeafDeduplicatesHashStream is the reason the heap exists:
// one hash stream spanning many chunks is one representative on disk, not one
// per chunk. Re-encoding the decoded entries must reproduce the same page, so
// a leaf that survives a rewrite cycle never drifts toward the unshared form.
func TestIndexDirectoryLeafDeduplicatesHashStream(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	certificate := `"a-representative-long-enough-to-notice"`
	arena, spans := testIndexCertificateArena(certificate, certificate, certificate, `"other"`)
	entries := []IndexDirectoryEntry{
		{Key: IndexDirectoryKey{TupleHash: 1}, Bits: 1, Cert: spans[0]},
		{Key: IndexDirectoryKey{TupleHash: 1, Chunk: 1}, Bits: 2, Cert: spans[1]},
		{Key: IndexDirectoryKey{TupleHash: 1, Chunk: 2}, Bits: 4, Cert: spans[2]},
		{Key: IndexDirectoryKey{TupleHash: 2}, Bits: 8, Cert: spans[3]},
	}
	page := make([]byte, testSuperblockPageSize)
	encoded, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 1)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexDirectoryPage(encoded, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 1)
	if err != nil {
		t.Fatal(err)
	}
	wantHeap := len(certificate) + len(`"other"`)
	if got := len(view.payload) - int(view.heapStart); got != wantHeap {
		t.Fatalf("certificate heap = %d bytes, want %d", got, wantHeap)
	}
	decoded := make([]IndexDirectoryEntry, view.Len())
	var rewriteArena []byte
	shared, _ := view.EntryAt(0)
	for i := range decoded {
		entry, _ := view.EntryAt(i)
		if i < 3 && entry.Cert != shared.Cert {
			t.Fatalf("entry %d span = %+v, want the shared %+v", i, entry.Cert, shared.Cert)
		}
		rewriteArena, entry.Cert = appendIndexCertificate(rewriteArena, view.Certificate(entry.Cert))
		decoded[i] = entry
	}
	rewritten := make([]byte, testSuperblockPageSize)
	if _, err := EncodeIndexDirectoryLeaf(rewritten, header, decoded, rewriteArena, testKeyDirectoryNextLogicalID, 1); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rewritten, encoded) {
		t.Fatal("re-encoding the decoded leaf did not reproduce the page")
	}
}

// TestIndexDirectoryLeafSplitsByBytes checks that uneven representatives cut
// the sequence where the bytes run out rather than at the midpoint, and that
// both halves are admissible pages.
func TestIndexDirectoryLeafSplitsByBytes(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	maxCertificate := IndexDirectoryMaxCertificate(testSuperblockPageSize)
	var arena []byte
	var entries []IndexDirectoryEntry
	for i := range 26 {
		length := 1
		if i < 6 {
			length = maxCertificate
		}
		entries = append(entries, IndexDirectoryEntry{
			Key:  IndexDirectoryKey{TupleHash: uint64(i)},
			Bits: 1,
			Cert: CertSpan{Offset: uint32(len(arena)), Length: uint16(length)},
		})
		arena = append(arena, bytes.Repeat([]byte{byte('a' + i)}, length)...)
	}
	split, err := indexTreeLeafSplit(testSuperblockPageSize, entries, arena)
	if err != nil {
		t.Fatal(err)
	}
	// The heavy representatives are all at the front, so a count-driven cut at
	// the midpoint would put six maximum certificates in the left half and
	// overflow it. Only a byte-driven cut lands this early.
	if split < 1 || split >= len(entries)/2 {
		t.Fatalf("split = %d, want a byte-driven cut before the midpoint of %d", split, len(entries))
	}
	page := make([]byte, testSuperblockPageSize)
	for name, half := range map[string][]IndexDirectoryEntry{
		"left": entries[:split], "right": entries[split:],
	} {
		if _, err := EncodeIndexDirectoryLeaf(page, header, half, arena, testKeyDirectoryNextLogicalID, 1); err != nil {
			t.Fatalf("%s half of a byte split = %v, want admitted", name, err)
		}
	}
}

// TestIndexDirectoryMaximumCertificate confirms the advertised bound is
// simultaneously encodable and splittable: a leaf full of maximum-length
// representatives must still cut into two admissible halves, which is the
// property IndexDirectoryMaxCertificate is derived from.
func TestIndexDirectoryMaximumCertificate(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	maxCertificate := IndexDirectoryMaxCertificate(testSuperblockPageSize)
	var arena []byte
	var entries []IndexDirectoryEntry
	for i := range 5 {
		span := CertSpan{Offset: uint32(len(arena)), Length: uint16(maxCertificate)}
		arena = append(arena, bytes.Repeat([]byte{byte('a' + i)}, maxCertificate)...)
		entries = append(entries, IndexDirectoryEntry{
			Key: IndexDirectoryKey{TupleHash: uint64(i)}, Bits: 1, Cert: span,
		})
	}
	page := make([]byte, testSuperblockPageSize)
	if _, err := EncodeIndexDirectoryLeaf(page, header, entries[:1], arena, testKeyDirectoryNextLogicalID, 1); err != nil {
		t.Fatalf("maximum certificate = %v, want admitted", err)
	}
	oversize := append([]IndexDirectoryEntry(nil), entries[0])
	oversize[0].Cert.Length++
	arena = append(arena, 'z')
	if _, err := EncodeIndexDirectoryLeaf(page, header, oversize, arena, testKeyDirectoryNextLogicalID, 1); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("over-maximum certificate = %v, want %v", err, ErrInvalidWrite)
	}
	split, err := indexTreeLeafSplit(testSuperblockPageSize, entries, arena)
	if err != nil {
		t.Fatal(err)
	}
	for name, half := range map[string][]IndexDirectoryEntry{
		"left": entries[:split], "right": entries[split:],
	} {
		if _, err := EncodeIndexDirectoryLeaf(page, header, half, arena, testKeyDirectoryNextLogicalID, 1); err != nil {
			t.Fatalf("%s half of a maximum-certificate split = %v, want admitted", name, err)
		}
	}
}

func TestIndexDirectorySteadyAllocation(t *testing.T) {
	header := testIndexDirectoryHeader(0, 30)
	arena, spans := testIndexCertificateArena(`"cat"`, `"cat"`)
	entries := []IndexDirectoryEntry{
		{Key: IndexDirectoryKey{TupleHash: 1}, Bits: 1, Cert: spans[0]},
		{Key: IndexDirectoryKey{IndexID: 1}, Bits: 2, Cert: spans[1]},
	}
	page := make([]byte, testSuperblockPageSize)
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeIndexDirectoryLeaf(page, header, entries, arena, testKeyDirectoryNextLogicalID, 2); err != nil {
			panic(err)
		}
		view, err := OpenIndexDirectoryPage(page, testKeyDirectoryFileEnd, testKeyDirectoryNextLogicalID, 2)
		if err != nil {
			panic(err)
		}
		indexEntrySink, _ = view.Lookup(entries[1].Key)
	}); allocs != 0 {
		t.Fatalf("index-directory codec and lookup allocations = %g, want 0", allocs)
	}
}

var indexEntrySink IndexDirectoryEntry

func testIndexPageRef(kind PageKind, logicalID, page, generation uint64) PageRef {
	return PageRef{
		Offset: page * uint64(testSuperblockPageSize), LogicalID: logicalID, Generation: generation,
		Length: testSuperblockPageSize, Kind: kind,
	}
}
