package durable

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
)

// fileStorePageValidator carries monotonic publication bounds into PageCache's
// one-time admission check. Document pages written by this process were
// already validated by their encoder; pages read back from storage receive the
// same typed validation before any zero-copy admitted view can observe them.
type fileStorePageValidator struct {
	fileEnd        atomic.Uint64
	nextLogicalID  atomic.Uint64
	generation     atomic.Uint64
	chunkHighWater atomic.Uint32
	pageSize       uint32
	indexHighWater uint32
	chunkDocuments uint32
	groupScratch   sync.Pool
}

func newFileStorePageValidator(pageSize, indexHighWater, chunkDocuments uint32) *fileStorePageValidator {
	v := &fileStorePageValidator{
		pageSize: pageSize, indexHighWater: indexHighWater,
		chunkDocuments: chunkDocuments,
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
	v.fileEnd.Store(state.super.FileEnd)
	v.nextLogicalID.Store(state.root.NextLogicalID)
	v.generation.Store(state.root.Generation)
	v.chunkHighWater.Store(state.root.ChunkHighWater)
}

func (v *fileStorePageValidator) validate(page []byte, ref storeio.PageRef) error {
	if v == nil {
		return nil
	}
	switch ref.Kind {
	case storeio.PageStateRoot:
		// State roots are selected and decoded directly from the double
		// superblock. PageCache rejects their fixed logical ID before I/O, and
		// the validator repeats that admission boundary so a direct caller can
		// never accidentally turn this into a second root-selection path.
		return fmt.Errorf("%w: state roots are never collection-cache admitted",
			storeio.ErrPageCacheReference)
	case storeio.PageDocument:
		view, err := storeio.OpenAdmittedDocumentPageWithOverflow(
			page, v.chunkHighWater.Load(), v.nextLogicalID.Load(),
			v.fileEnd.Load(), v.pageSize,
		)
		if err != nil {
			return err
		}
		for live := view.Header().Live; live != 0; live &= live - 1 {
			value, ok := view.LookupValue(uint8(bits.TrailingZeros64(live)))
			if !ok {
				return storeio.ErrDocumentPageCorrupt
			}
			if value.Overflow == (storeio.PageRef{}) && !vibejson.Valid(value.Inline) {
				return fmt.Errorf("%w: invalid inline JSON", storeio.ErrDocumentPageCorrupt)
			}
		}
		return nil
	case storeio.PageOverflow:
		_, err := storeio.OpenOverflowPage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(), v.pageSize,
			v.chunkHighWater.Load(), uint8(v.chunkDocuments),
		)
		return err
	case storeio.PageChunkDirectory:
		// Same contract as PageKeyDirectory: LookupChunkTree and the scan walk
		// trust this one check for the life of the resident frame.
		_, err := storeio.OpenChunkDirectoryPage(page, v.fileEnd.Load(), v.nextLogicalID.Load())
		return err
	case storeio.PageKeyDirectory:
		// The tree descents reconstruct these pages without revalidating them,
		// so this is the only place their record ordering, location bounds, and
		// child references are ever proven. Dropping this case would not make
		// reads merely untrusted; it would let a malformed key offset index
		// outside the payload and panic.
		_, err := storeio.OpenKeyDirectoryPage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(),
			v.chunkHighWater.Load(), uint8(v.chunkDocuments),
		)
		return err
	case storeio.PageIndexDirectory:
		_, err := storeio.OpenIndexDirectoryPage(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(), v.indexHighWater,
		)
		return err
	case storeio.PageIndexPosting:
		_, err := storeio.OpenPostingPage(page, v.nextLogicalID.Load(), v.indexHighWater)
		return err
	case storeio.PageDocumentGroup:
		group, err := storeio.OpenAdmittedDocumentGroup(
			page, v.chunkHighWater.Load(), v.nextLogicalID.Load(),
		)
		if err != nil {
			return err
		}
		pooled := v.groupScratch.Get().(*[]byte)
		scratch := (*pooled)[:0]
		defer func() {
			clear(scratch)
			*pooled = scratch[:0]
			v.groupScratch.Put(pooled)
		}()
		header := group.Header()
		for ordinal := uint32(0); ordinal < uint32(header.ChunkCount); ordinal++ {
			chunk, ok := group.Chunk(header.FirstChunk + ordinal)
			if !ok {
				return storeio.ErrDocumentGroupCorrupt
			}
			for rank := 0; rank < chunk.Len(); rank++ {
				record, ok := chunk.RecordAt(rank)
				if !ok {
					return storeio.ErrDocumentGroupCorrupt
				}
				scratch = scratch[:0]
				scratch, ok = chunk.AppendJSON(scratch, record.Slot)
				if !ok || !vibejson.Valid(scratch) {
					return fmt.Errorf("%w: invalid decoded JSON", storeio.ErrDocumentGroupCorrupt)
				}
			}
		}
		return nil
	case storeio.PageFloat64Group:
		_, err := storeio.OpenAdmittedFloat64Group(
			page, v.chunkHighWater.Load(), v.nextLogicalID.Load(),
		)
		return err
	case storeio.PageFloat64Catalog:
		_, err := storeio.OpenAdmittedFloat64Directory(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(), v.pageSize,
		)
		return err
	case storeio.PageFloat64Stripe:
		_, err := storeio.OpenAdmittedFloat64Stripe(
			page, v.chunkHighWater.Load(), v.nextLogicalID.Load(),
		)
		return err
	case storeio.PageIndexGroupCatalog:
		catalog, err := storeio.OpenAdmittedIndexGroupCatalog(
			page, v.indexHighWater, v.chunkHighWater.Load(), v.chunkDocuments,
			v.fileEnd.Load(), v.nextLogicalID.Load(), v.pageSize,
		)
		if err != nil {
			return err
		}
		iterator := catalog.Iterator()
		for {
			entry, ok := iterator.Next()
			if !ok {
				break
			}
			if !fileIndexCertificateValid(entry.Value, 1) {
				return storeio.ErrIndexGroupCatalogCorrupt
			}
		}
		return nil
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
	case storeio.PageFingerprintDirectory:
		_, err := storeio.OpenPageFingerprintDirectory(
			page, v.fileEnd.Load(), v.nextLogicalID.Load(),
			v.chunkHighWater.Load(), v.chunkDocuments,
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
		// The class byte selects the decoder without probing. An unknown class
		// falls through to the raw decoder, which rejects it as corrupt.
		switch storeio.PrimaryLeafClass(page) {
		case storeio.CommonPrimaryLeafTemplate:
			_, err = storeio.OpenCommonPrimaryTemplateLeaf(
				page, header.StoreID, bucket, ref, v.generation.Load(),
				leafBounds,
			)
			return err
		case storeio.CommonPrimaryLeafCompact:
			_, err = storeio.OpenCommonPrimaryCompactLeaf(
				page, header.StoreID, bucket, ref, v.generation.Load(),
				leafBounds,
			)
			return err
		}
		_, err = storeio.OpenCommonPrimaryLeaf(
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
	case storeio.PageTabletDirectory:
		// Reserved by format 0, but not selected by the phase-4a graph.
		return fmt.Errorf("%w: tablet directory has no selected codec",
			storeio.ErrPageCacheReference)
	default:
		// validPageKind is intentionally private to storeio, so this default is
		// also the format-evolution tripwire: adding a durable kind cannot
		// silently inherit checksum-only admission.
		return fmt.Errorf("%w: page kind %d has no collection-cache validator",
			storeio.ErrPageCacheReference, ref.Kind)
	}
}
