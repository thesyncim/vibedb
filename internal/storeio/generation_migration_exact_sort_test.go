package storeio

import "testing"

type migrationExactMemorySink struct {
	storeID    [16]byte
	generation uint64
	nextID     uint64
	nextAt     uint64
	pages      map[PageRef][]byte
}

type migrationExactMemoryPage struct {
	sink  *migrationExactMemorySink
	ref   PageRef
	image []byte
}

func (p *migrationExactMemoryPage) Bytes() []byte { return p.image }
func (p *migrationExactMemoryPage) Ref() PageRef  { return p.ref }
func (p *migrationExactMemoryPage) Stage() error {
	p.sink.pages[p.ref] = append([]byte(nil), p.image...)
	return nil
}
func (s *migrationExactMemorySink) AllocatePage(kind PageKind, length uint32, logicalID uint64) (PrimaryGraphBuildPage, error) {
	if logicalID == 0 {
		logicalID = s.nextID
		s.nextID++
	}
	ref := PageRef{Offset: s.nextAt, LogicalID: logicalID, Generation: s.generation, Length: length, Kind: kind}
	s.nextAt += uint64(length)
	return &migrationExactMemoryPage{sink: s, ref: ref, image: make([]byte, length)}, nil
}
func (s *migrationExactMemorySink) StoreIdentity() [16]byte    { return s.storeID }
func (s *migrationExactMemorySink) BuildGeneration() uint64    { return s.generation }
func (s *migrationExactMemorySink) BuildFileEnd() uint64       { return 1 << 30 }
func (s *migrationExactMemorySink) BuildNextLogicalID() uint64 { return s.nextID }
func (s *migrationExactMemorySink) MaxBuildPageBytes() int     { return 4096 }

func TestGenerationMigrationExactRunBuilderBoundsAndCoalescesWindows(t *testing.T) {
	sink := &migrationExactMemorySink{storeID: testStoreID, generation: 9, nextID: 100, nextAt: 64 << 10, pages: make(map[PageRef][]byte)}
	builder, err := NewGenerationMigrationExactRunBuilder(sink, 4096, 3, IndexTermMaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	a := migrationExactRunKey(t, `"a"`)
	b := migrationExactRunKey(t, `"b"`)
	input := []GenerationMigrationExactRunRecord{
		{IndexID: 1, Key: b, TileID: 3, Mask: 1},
		{IndexID: 0, Key: a, TileID: 2, Mask: 2},
		{IndexID: 0, Key: a, TileID: 2, Mask: 4},
		{IndexID: 0, Key: b, TileID: 1, Mask: 8},
		{IndexID: 0, Key: a, TileID: 0, Mask: 16},
		{IndexID: 1, Key: a, TileID: 0, Mask: 32},
		{IndexID: 1, Key: b, TileID: 0, Mask: 64},
	}
	for _, record := range input {
		if err := builder.Add(record.IndexID, record.Key, record.TileID, record.Mask); err != nil {
			t.Fatal(err)
		}
	}
	region, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if region.Runs != 3 || region.Pages != 3 || len(sink.pages) != 3 {
		t.Fatalf("region=%+v pages=%d", region, len(sink.pages))
	}
	firstPage := sink.pages[region.First]
	view, err := OpenGenerationMigrationExactRunPage(firstPage, region.First, sink.storeID, sink.generation)
	if err != nil {
		t.Fatal(err)
	}
	it := view.Iterator()
	first, ok := it.Next()
	if !ok || first.IndexID != 0 || first.TileID != 2 || first.Mask != 6 {
		t.Fatalf("coalesced first = %+v ok=%v", first, ok)
	}
}
