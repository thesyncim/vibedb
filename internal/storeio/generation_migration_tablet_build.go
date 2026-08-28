package storeio

import (
	"bytes"
	"fmt"

	vibejson "github.com/thesyncim/vibejson"
)

type GenerationMigrationTabletEmission struct {
	TabletID          uint32
	Ref               PageRef
	FirstKey, LastKey []byte
	Records           uint64
}

// GenerationMigrationTabletBuilder emits one macro-tablet from borrowed leaf
// windows. It retains only the tablet's bounded leaf-route vector and key
// witnesses; document bytes are consumed before StageWindow returns.
type GenerationMigrationTabletBuilder struct {
	sink       PrimaryGraphBuildSink
	planner    *PrimaryGraphLeafWindowPlanner
	tabletID   uint32
	localID    uint16
	leaves     []primaryBuiltLeaf
	keyArena   []byte
	lastKey    [CommonPrimaryLeafMaxKeyBytes]byte
	lastKeyLen int
	records    uint64
	finished   bool
}

func NewGenerationMigrationTabletBuilder(sink PrimaryGraphBuildSink, tabletID uint32, placed bool, summaries []vibejson.CompiledPointer) (*GenerationMigrationTabletBuilder, error) {
	planner, err := NewPrimaryGraphLeafWindowPlanner(placed, summaries)
	if err != nil || sink == nil || tabletID >= TabletLocalIdentityTabletCount {
		return nil, fmt.Errorf("%w: migration tablet builder", ErrInvalidWrite)
	}
	return &GenerationMigrationTabletBuilder{sink: sink, planner: planner, tabletID: tabletID, leaves: make([]primaryBuiltLeaf, 0, TabletLocalIdentityLocalCount), keyArena: make([]byte, 0, 2*TabletLocalIdentityLocalCount*CommonPrimaryLeafMaxKeyBytes)}, nil
}

func (b *GenerationMigrationTabletBuilder) StageWindow(records []PrimaryGraphRecord, placements []PrimaryGraphPlacement) error {
	if b == nil || b.finished || len(records) == 0 || len(records) > CommonPrimaryLeafWideSlots || placements != nil && len(placements) != len(records) || int(b.localID) >= TabletLocalIdentityLocalCount {
		return fmt.Errorf("%w: migration tablet window", ErrInvalidWrite)
	}
	if b.lastKeyLen != 0 && bytes.Compare(b.lastKey[:b.lastKeyLen], records[0].keyBytes()) >= 0 {
		return fmt.Errorf("%w: migration tablet order", ErrInvalidWrite)
	}
	emission, err := b.planner.Stage(b.sink, b.tabletID, b.localID, records, CommonPrimaryLeafMaxExtentBytes, placements)
	if err != nil || emission.Count != len(records) {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: migration tablet partial window", ErrInvalidWrite)
	}
	firstAt := len(b.keyArena)
	b.keyArena = append(b.keyArena, emission.FirstKey...)
	lastAt := len(b.keyArena)
	b.keyArena = append(b.keyArena, emission.LastKey...)
	b.leaves = append(b.leaves, primaryBuiltLeaf{firstKey: b.keyArena[firstAt:lastAt:lastAt], lastKey: b.keyArena[lastAt:len(b.keyArena):len(b.keyArena)], ref: emission.Ref})
	copy(b.lastKey[:], emission.LastKey)
	b.lastKeyLen = len(emission.LastKey)
	b.records += uint64(emission.Count)
	b.localID++
	return nil
}

func (b *GenerationMigrationTabletBuilder) Full() bool {
	return b != nil && int(b.localID) == TabletLocalIdentityLocalCount
}

func (b *GenerationMigrationTabletBuilder) Finish(priorTabletMax []byte) (GenerationMigrationTabletEmission, error) {
	if b == nil || b.finished || len(b.leaves) == 0 || len(priorTabletMax) > CommonPrimaryLeafMaxKeyBytes {
		return GenerationMigrationTabletEmission{}, ErrInvalidWrite
	}
	b.finished = true
	firstKey := b.leaves[0].firstKey
	lastKey := b.leaves[len(b.leaves)-1].lastKey
	child, err := stagePrimaryTabletWindow(b.sink, b.tabletID, b.leaves, priorTabletMax)
	if err != nil {
		return GenerationMigrationTabletEmission{}, err
	}
	first := append([]byte(nil), firstKey...)
	last := append([]byte(nil), lastKey...)
	return GenerationMigrationTabletEmission{TabletID: b.tabletID, Ref: child.ref, FirstKey: first, LastKey: last, Records: b.records}, nil
}

// FoldGenerationMigrationTabletVector authenticates each final target tablet,
// derives only its boundary keys, and emits the replacement global catalog.
// Every ordinal must be populated: sparse vectors fail closed before a root is
// returned.
func FoldGenerationMigrationTabletVector(
	vector *GenerationMigrationTabletVector,
	cache *PageCache,
	bounds GlobalTabletCatalogBounds,
	folder *PrimaryGraphCatalogFolder,
) (PageRef, error) {
	if vector == nil || cache == nil || folder == nil {
		return PageRef{}, ErrInvalidWrite
	}
	expected := uint32(0)
	var firstScratch, lastScratch [CommonPrimaryLeafMaxKeyBytes]byte
	err := vector.Visit(func(tabletID uint32, entry GenerationMigrationTabletRef) error {
		if tabletID != expected || entry.Target == (PageRef{}) {
			return ErrGlobalTabletCatalogCorrupt
		}
		first, last, err := AppendPrimaryTabletKeyBounds(cache, entry.Target, bounds, firstScratch[:0], lastScratch[:0])
		if err != nil {
			return err
		}
		if err := folder.AddTabletRef(tabletID, first, last, entry.Target); err != nil {
			return err
		}
		expected++
		return nil
	})
	if err != nil {
		return PageRef{}, err
	}
	if expected != vector.tablets {
		return PageRef{}, ErrGlobalTabletCatalogCorrupt
	}
	return folder.Finish()
}
