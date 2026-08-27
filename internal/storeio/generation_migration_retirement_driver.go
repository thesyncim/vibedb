package storeio

import (
	"errors"
	"fmt"
)

var errGenerationMigrationRetirementBatch = errors.New("vibedb: retirement batch full")

// DurableGenerationRetirementSink must not return until the supplied extents
// are durably recorded behind the Store's snapshot and recovery floors.
type DurableGenerationRetirementSink func([]FreeExtent) error

// GenerationMigrationRetirementDriver advances at most BatchExtents per Step.
// Its durable manifest ordinal makes a crash repeat, but never skip, an extent;
// the retirement sink must therefore be idempotent for identical extents.
type GenerationMigrationRetirementDriver struct {
	Manifest      *GenerationMigrationManifestStore
	Cache         *PageCache
	PageSize      uint32
	MaxPageSize   uint32
	BatchExtents  int
	RetireDurably DurableGenerationRetirementSink
}

func (d *GenerationMigrationRetirementDriver) Step() (bool, error) {
	if d == nil || d.Manifest == nil || d.Cache == nil || d.PageSize == 0 ||
		d.MaxPageSize < d.PageSize || d.BatchExtents <= 0 || d.RetireDurably == nil {
		return false, fmt.Errorf("%w: migration retirement driver", ErrInvalidWrite)
	}
	m, err := d.Manifest.Load()
	if err != nil {
		return false, err
	}
	if m.Phase != GenerationMigrationPublished {
		return false, fmt.Errorf("%w: migration is not published", ErrInvalidWrite)
	}
	if m.RetirementPhase == GenerationMigrationRetireDone {
		return true, nil
	}
	if m.RetirementPhase == GenerationMigrationRetireNone {
		m.RetirementPhase = GenerationMigrationRetirePrimary
		m.RetirementOrdinal = 0
		return false, d.Manifest.Advance(m)
	}
	extents := make([]FreeExtent, 0, d.BatchExtents)
	ordinal := uint64(0)
	visit := func(ref PageRef) error {
		if ordinal < m.RetirementOrdinal {
			ordinal++
			return nil
		}
		if len(extents) == cap(extents) {
			return errGenerationMigrationRetirementBatch
		}
		extents = append(extents, FreeExtent{Offset: ref.Offset, Length: uint64(ref.Length), RetiredGeneration: m.SourceGeneration})
		ordinal++
		return nil
	}
	complete := false
	switch m.RetirementPhase {
	case GenerationMigrationRetirePrimary:
		err = VisitPrimaryGraphRefs(d.Cache, m.SourcePrimaryRoot, GlobalTabletCatalogBounds{
			StoreID: m.StoreID, SelectedRootGeneration: m.SourceGeneration,
			FileEnd: m.SourceFileEnd, NextLogicalID: m.FirstLogicalID,
		}, visit)
	case GenerationMigrationRetireExact:
		if m.SourceExactIndexRoot == (PageRef{}) {
			complete = true
			break
		}
		err = VisitPrimaryExactIndexRefs(d.Cache, m.SourceExactIndexRoot, PrimaryExactIndexBounds{
			StoreID: m.StoreID, Generation: m.SourceGeneration,
			FileEnd: m.SourceFileEnd, NextLogicalID: m.FirstLogicalID,
			AllocationQuantum: d.PageSize, MaxPageSize: d.MaxPageSize,
			IndexCount: m.SourceIndexCount,
		}, visit)
	case GenerationMigrationRetireCatalog:
		extent, ok, extentErr := GenerationMigrationSourceCatalogExtent(m, d.PageSize, m.SourceGeneration)
		if extentErr != nil {
			return false, extentErr
		}
		if ok && m.RetirementOrdinal == 0 {
			extents = append(extents, extent)
			ordinal = 1
		}
		complete = true
	case GenerationMigrationRetireScratch:
		if m.TargetScratchBytes != 0 && m.RetirementOrdinal == 0 {
			extents = append(extents, FreeExtent{Offset: m.TargetScratchOffset, Length: m.TargetScratchBytes, RetiredGeneration: m.TargetGeneration})
			ordinal = 1
		}
		complete = true
	default:
		return false, ErrGenerationMigrationManifestCorrupt
	}
	if err != nil && !errors.Is(err, errGenerationMigrationRetirementBatch) {
		return false, err
	}
	if err == nil {
		complete = true
	}
	if len(extents) != 0 {
		if err := d.RetireDurably(extents); err != nil {
			return false, err
		}
		m.RetirementOrdinal = ordinal
	}
	if complete {
		m.RetirementPhase++
		m.RetirementOrdinal = 0
	}
	if err := d.Manifest.Advance(m); err != nil {
		return false, err
	}
	return m.RetirementPhase == GenerationMigrationRetireDone, nil
}
