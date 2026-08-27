package storeio

import (
	"fmt"
	"os"
)

// VisitGenerationMigrationScratch walks authenticated staging links and emits
// only external-sort and padding pages. Target graph/index/catalog pages share
// the extents but remain reachable after installation and are never emitted.
func VisitGenerationMigrationScratch(
	file *os.File, manifest GenerationMigrationManifest, scratch []byte,
	visit func(FreeExtent) error,
) error {
	if file == nil || manifest.StagingChainTail == (PageRef{}) ||
		len(scratch) < int(manifest.StagingChainTail.Length) || visit == nil {
		return fmt.Errorf("%w: migration scratch visitor", ErrInvalidWrite)
	}
	for chain := manifest.StagingChainTail; chain != (PageRef{}); {
		page := scratch[:chain.Length]
		if _, err := file.ReadAt(page, int64(chain.Offset)); err != nil {
			return err
		}
		view, err := OpenGenerationMigrationStagingChainPage(
			page, chain, manifest.StoreID, manifest.MigrationID, manifest.TargetGeneration,
		)
		if err != nil {
			return err
		}
		it := view.Iterator()
		for {
			extent, ok := it.Next()
			if !ok {
				break
			}
			offset := extent.Offset + uint64(chain.Length)
			end := offset + extent.DataBytes
			for offset < end {
				prefix := scratch[:PageHeaderSize]
				if _, err := file.ReadAt(prefix, int64(offset)); err != nil {
					return err
				}
				header, ok := decodePageHeader(prefix)
				if !ok || header.StoreID != manifest.StoreID ||
					header.Generation != manifest.TargetGeneration ||
					uint64(header.PageSize) > end-offset || int(header.PageSize) > len(scratch) {
					return ErrGenerationMigrationManifestCorrupt
				}
				image := scratch[:header.PageSize]
				if _, err := file.ReadAt(image, int64(offset)); err != nil {
					return err
				}
				opened, _, err := OpenPage(image)
				if err != nil || opened != header {
					return ErrGenerationMigrationManifestCorrupt
				}
				if header.Kind == PageMigrationExactRun || header.Kind == PageMigrationPadding {
					if err := visit(FreeExtent{
						Offset: offset, Length: uint64(header.PageSize),
						RetiredGeneration: manifest.TargetGeneration,
					}); err != nil {
						return err
					}
				}
				offset += uint64(header.PageSize)
			}
		}
		chain = view.Previous()
	}
	return nil
}
