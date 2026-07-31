package durable

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// fileStorePageValidator carries monotonic publication bounds into PageCache's
// one-time admission check. Document pages written by this process were
// already validated by their encoder; pages read back from storage receive the
// same typed validation before any zero-copy admitted view can observe them.
type fileStorePageValidator struct {
	fileEnd        atomic.Uint64
	nextLogicalID  atomic.Uint64
	generation     atomic.Uint64
	pageSize       uint32
	indexHighWater uint32
	groupScratch   sync.Pool
}

func newFileStorePageValidator(pageSize, indexHighWater uint32) *fileStorePageValidator {
	v := &fileStorePageValidator{
		pageSize: pageSize, indexHighWater: indexHighWater,
	}
	v.groupScratch.New = func() any {
		scratch := make([]byte, 0, pageSize)
		return &scratch
	}
	return v
}

func (v *fileStorePageValidator) update(state *fileStoreState) {
	if v == nil || state == nil {
		return
	}
	// These bounds never decrease. Publish them before the corresponding state
	// pointer so a reader of that state cannot validate against older bounds.
	v.fileEnd.Store(state.fileEnd)
	v.nextLogicalID.Store(state.root.NextLogicalID)
	v.generation.Store(state.root.Generation)
}

func (v *fileStorePageValidator) validate(page []byte, ref storeio.PageRef) error {
	if v == nil {
		return nil
	}
	switch ref.Kind {
	case storeio.PageOverflow:
		_, err := storeio.OpenOverflowPage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(), v.pageSize,
		)
		return err
	case storeio.PageFreeImage:
		_, err := storeio.OpenFreeImagePage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(),
		)
		return err
	case storeio.PageFreeDelta:
		_, err := storeio.OpenFreeDeltaPage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(),
		)
		return err
	case storeio.PageFreeIndex:
		_, err := storeio.OpenFreeIndexPage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(),
		)
		return err
	case storeio.PagePrimaryCatalog:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		_, err = storeio.OpenGlobalTabletCatalogNode(
			page, ref, storeio.GlobalTabletCatalogBounds{
				StoreID: header.StoreID, SelectedRootGeneration: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
			},
		)
		return err
	case storeio.PagePrimaryLocator:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		_, err = storeio.OpenGlobalTabletCatalogLocator(
			page, ref, storeio.GlobalTabletCatalogBounds{
				StoreID: header.StoreID, SelectedRootGeneration: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
			},
		)
		return err
	case storeio.PageTabletRoute:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		_, err = storeio.OpenGlobalTabletCatalogTabletRoot(
			page, ref, storeio.GlobalTabletCatalogBounds{
				StoreID: header.StoreID, SelectedRootGeneration: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
			},
		)
		return err
	case storeio.PagePrimaryAnchor:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		return storeio.ValidateGlobalTabletCatalogAnchor(
			page, ref, storeio.GlobalTabletCatalogBounds{
				StoreID: header.StoreID, SelectedRootGeneration: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
			},
		)
	case storeio.PagePrimaryLeaf:
		if ref.LogicalID < storeio.PrimaryLeafLogicalIDBase ||
			ref.LogicalID >= storeio.PrimaryLeafLogicalIDLimit {
			return fmt.Errorf("%w: primary leaf identity",
				storeio.ErrPageCacheReference)
		}
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		bucket := storeio.BucketID(
			ref.LogicalID - storeio.PrimaryLeafLogicalIDBase,
		)
		leafBounds := storeio.CommonPrimaryLeafBounds{
			FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
			AllocationQuantum: v.pageSize,
		}
		if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
			return fmt.Errorf(
				"%w: non-unified primary leaf",
				storeio.ErrCommonPrimaryLeafCorrupt,
			)
		}
		_, err = storeio.OpenCommonPrimaryUnifiedLeaf(
			page, header.StoreID, bucket, ref, v.generation.Load(), leafBounds,
		)
		return err
	case storeio.PagePrimaryExactRoot:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		_, err = storeio.OpenPrimaryExactRootPage(
			page, ref, storeio.PrimaryExactIndexBounds{
				StoreID: header.StoreID, Generation: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
				AllocationQuantum: v.pageSize,
				MaxPageSize:       storeio.MaxPhysicalPageSize,
				IndexCount:        v.indexHighWater,
			},
		)
		return err
	case storeio.PagePrimaryExactLeaf:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		_, err = storeio.OpenPrimaryExactLeafPage(
			page, ref, storeio.PrimaryExactIndexBounds{
				StoreID: header.StoreID, Generation: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
				AllocationQuantum: v.pageSize,
				MaxPageSize:       storeio.MaxPhysicalPageSize,
				IndexCount:        v.indexHighWater,
			},
		)
		return err
	case storeio.PagePrimaryExactCatalog:
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			return err
		}
		_, err = storeio.OpenPrimaryExactCatalogPage(
			page, ref, storeio.PrimaryExactIndexBounds{
				StoreID: header.StoreID, Generation: v.generation.Load(),
				FileEnd: v.fileEnd.Load(), NextLogicalID: v.nextLogicalID.Load(),
				AllocationQuantum: v.pageSize,
				MaxPageSize:       storeio.MaxPhysicalPageSize,
				IndexCount:        v.indexHighWater,
			},
		)
		return err
	default:
		// validPageKind is intentionally private to storeio, so this default is
		// also the format-evolution tripwire: adding a durable kind cannot
		// silently inherit checksum-only admission.
		return fmt.Errorf("%w: page kind %d has no collection-cache validator",
			storeio.ErrPageCacheReference, ref.Kind)
	}
}
