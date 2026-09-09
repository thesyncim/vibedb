package durable

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

type recordingExactPackPage struct {
	buf []byte
	ref storeio.PageRef
}

func (p *recordingExactPackPage) Bytes() []byte        { return p.buf }
func (p *recordingExactPackPage) Ref() storeio.PageRef { return p.ref }
func (p *recordingExactPackPage) Stage() error         { return nil }

type recordingExactPackSink struct {
	storeID [16]byte
	gen     uint64
	offset  uint64
	nextID  uint64
	packs   int
}

func (s *recordingExactPackSink) AllocatePage(
	kind storeio.PageKind, length uint32, logicalID uint64,
) (storeio.PrimaryGraphBuildPage, error) {
	if logicalID == 0 {
		logicalID = s.nextID
		s.nextID++
	}
	if kind == storeio.PagePrimaryExactPack {
		s.packs++
	}
	ref := storeio.PageRef{
		Offset: s.offset, LogicalID: logicalID, Generation: s.gen,
		Length: length, Kind: kind,
	}
	s.offset += uint64(length)
	return &recordingExactPackPage{buf: make([]byte, length), ref: ref}, nil
}

func (s *recordingExactPackSink) StoreIdentity() [16]byte { return s.storeID }
func (s *recordingExactPackSink) BuildGeneration() uint64 { return s.gen }
func (s *recordingExactPackSink) BuildFileEnd() uint64    { return s.offset }
func (s *recordingExactPackSink) BuildNextLogicalID() uint64 {
	return s.nextID
}
func (s *recordingExactPackSink) MaxBuildPageBytes() int { return 64 << 10 }

func TestStagePackedExactLeavesPacksNonadjacentDirty(t *testing.T) {
	sink := &recordingExactPackSink{
		storeID: [16]byte{1, 2, 3},
		gen:     4,
		offset:  64 << 10,
		nextID:  storeio.PrimaryFirstDynamicLogicalID,
	}
	carried := storeio.PageRef{
		Offset: 8 << 20, LogicalID: 9, Generation: 3,
		Length: 4096, Kind: storeio.PagePrimaryExactPack,
	}
	leaves := []primaryExactLeaf{
		{encoded: bytes.Repeat([]byte("leaf-a/"), 40), firstKey: []byte("a")},
		{
			encoded:  bytes.Repeat([]byte("leaf-b/"), 40),
			firstKey: []byte("b"), ref: carried, member: 3,
		},
		{encoded: bytes.Repeat([]byte("leaf-c/"), 40), firstKey: []byte("c")},
	}
	staged, err := stagePackedExactLeaves(
		sink, 4096, 64<<10, 1, leaves, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sink.packs != 1 {
		t.Fatalf("pack pages = %d, want 1", sink.packs)
	}
	if len(staged) != 3 {
		t.Fatalf("staged = %d, want 3", len(staged))
	}
	if staged[1].ref != carried || staged[1].member != 3 {
		t.Fatalf("carried leaf = %+v", staged[1])
	}
	if staged[0].ref == (storeio.PageRef{}) || staged[0].ref == carried ||
		staged[0].ref != staged[2].ref {
		t.Fatalf("dirty leaves did not share a pack: %+v %+v", staged[0], staged[2])
	}
	if staged[0].member != 0 || staged[2].member != 1 {
		t.Fatalf("dirty members = %d, %d, want 0, 1", staged[0].member, staged[2].member)
	}
	if !bytes.Equal(staged[0].firstKey, []byte("a")) ||
		!bytes.Equal(staged[1].firstKey, []byte("b")) ||
		!bytes.Equal(staged[2].firstKey, []byte("c")) {
		t.Fatalf("catalog order lost: %q %q %q",
			staged[0].firstKey, staged[1].firstKey, staged[2].firstKey)
	}
}

func TestStagePackedExactLeavesAllCarriedWritesNothing(t *testing.T) {
	sink := &recordingExactPackSink{
		storeID: [16]byte{1, 2, 3},
		gen:     4,
		offset:  64 << 10,
		nextID:  storeio.PrimaryFirstDynamicLogicalID,
	}
	ref := storeio.PageRef{
		Offset: 8 << 20, LogicalID: 9, Generation: 3,
		Length: 4096, Kind: storeio.PagePrimaryExactPack,
	}
	leaves := []primaryExactLeaf{
		{encoded: []byte("a"), firstKey: []byte("a"), ref: ref, member: 0},
		{encoded: []byte("b"), firstKey: []byte("b"), ref: ref, member: 1},
	}
	staged, err := stagePackedExactLeaves(
		sink, 4096, 64<<10, 1, leaves, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sink.packs != 0 {
		t.Fatalf("pack pages = %d, want 0", sink.packs)
	}
	if len(staged) != 2 || staged[0].ref != ref || staged[1].member != 1 {
		t.Fatalf("carried catalog = %+v", staged)
	}
}
