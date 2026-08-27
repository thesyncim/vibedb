package storeio

import (
	"bytes"
	"fmt"

	vibejson "github.com/thesyncim/vibejson"
)

// PrimaryValueGraphStreamBuilder is the bounded mixed inline/overflow graph
// emitter used by online compaction. Each StageWindow consumes borrowed rows
// synchronously and optionally returns their exact posting-stable placements.
type PrimaryValueGraphStreamBuilder struct {
	sink              PrimaryGraphBuildSink
	planner           *PrimaryValueLeafWindowPlanner
	catalog           *PrimaryGraphCatalogFolder
	tabletID          uint32
	localID           uint16
	leaves            []primaryBuiltLeaf
	keyArena          []byte
	lastKey           [CommonPrimaryLeafMaxKeyBytes]byte
	lastKeyLen        int
	priorTabletMax    [CommonPrimaryLeafMaxKeyBytes]byte
	priorTabletMaxLen int
	records           uint64
	finished          bool
}

func NewPrimaryValueGraphStreamBuilder(
	sink PrimaryGraphBuildSink, summaries []vibejson.CompiledPointer,
) (*PrimaryValueGraphStreamBuilder, error) {
	planner, err := NewPrimaryValueLeafWindowPlanner(summaries)
	if err != nil {
		return nil, err
	}
	catalog, err := NewPrimaryGraphCatalogFolder(sink)
	if err != nil {
		return nil, err
	}
	return &PrimaryValueGraphStreamBuilder{
		sink: sink, planner: planner, catalog: catalog,
		leaves:   make([]primaryBuiltLeaf, 0, TabletLocalIdentityLocalCount),
		keyArena: make([]byte, 0, 2*TabletLocalIdentityLocalCount*CommonPrimaryLeafMaxKeyBytes),
	}, nil
}

func (b *PrimaryValueGraphStreamBuilder) StageWindow(
	records []CommonPrimaryLeafRecord, placements []PrimaryGraphPlacement,
) error {
	if b == nil || b.finished || len(records) == 0 ||
		placements != nil && len(placements) != len(records) {
		return fmt.Errorf("%w: primary value stream window", ErrInvalidWrite)
	}
	for consumed := 0; consumed < len(records); {
		end := min(consumed+CommonPrimaryLeafWideSlots, len(records))
		window := records[consumed:end]
		if b.lastKeyLen != 0 && bytes.Compare(b.lastKey[:b.lastKeyLen], window[0].Key) >= 0 {
			return fmt.Errorf("%w: primary value stream order", ErrInvalidWrite)
		}
		for rank := range window {
			window[rank].Slot = uint8(rank)
		}
		emission, err := b.planner.Stage(
			b.sink, b.tabletID, b.localID, window, CommonPrimaryLeafMaxExtentBytes,
		)
		if err != nil {
			return err
		}
		if placements != nil {
			for rank := 0; rank < emission.Count; rank++ {
				placements[consumed+rank] = PrimaryGraphPlacement{Bucket: emission.Bucket, Slot: uint8(rank)}
			}
		}
		first := b.copyTabletKey(emission.FirstKey)
		last := b.copyTabletKey(emission.LastKey)
		b.leaves = append(b.leaves, primaryBuiltLeaf{firstKey: first, lastKey: last, ref: emission.Ref})
		copy(b.lastKey[:], emission.LastKey)
		b.lastKeyLen = len(emission.LastKey)
		b.records += uint64(emission.Count)
		consumed += emission.Count
		b.localID++
		if int(b.localID) == TabletLocalIdentityLocalCount {
			if err := b.flushTablet(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *PrimaryValueGraphStreamBuilder) copyTabletKey(key []byte) []byte {
	start := len(b.keyArena)
	b.keyArena = append(b.keyArena, key...)
	return b.keyArena[start:len(b.keyArena):len(b.keyArena)]
}

func (b *PrimaryValueGraphStreamBuilder) flushTablet() error {
	if len(b.leaves) == 0 {
		return nil
	}
	child, err := stagePrimaryTabletWindow(
		b.sink, b.tabletID, b.leaves, b.priorTabletMax[:b.priorTabletMaxLen],
	)
	if err != nil {
		return err
	}
	if err := b.catalog.AddTablet(child); err != nil {
		return err
	}
	last := b.leaves[len(b.leaves)-1].lastKey
	copy(b.priorTabletMax[:], last)
	b.priorTabletMaxLen = len(last)
	b.tabletID++
	b.localID = 0
	b.leaves = b.leaves[:0]
	b.keyArena = b.keyArena[:0]
	return nil
}

func (b *PrimaryValueGraphStreamBuilder) Finish() (PageRef, error) {
	if b == nil || b.finished || b.records == 0 {
		return PageRef{}, ErrBatchState
	}
	b.finished = true
	if err := b.flushTablet(); err != nil {
		return PageRef{}, err
	}
	return b.catalog.Finish()
}
