package storeio

import "fmt"

// GenerationMigrationSourceCatalogExtent reconstructs the old schema/index
// catalog's contiguous physical run from the immutable source lineage. The
// result is suitable for the ordinary snapshot/recovery-fenced reclaimer only
// after the target state has been published.
func GenerationMigrationSourceCatalogExtent(
	manifest GenerationMigrationManifest,
	pageSize uint32,
	retiredGeneration uint64,
) (FreeExtent, bool, error) {
	if manifest.Phase != GenerationMigrationPublished || pageSize == 0 ||
		retiredGeneration != manifest.SourceGeneration {
		return FreeExtent{}, false, fmt.Errorf("%w: catalog retirement lineage", ErrInvalidWrite)
	}
	if manifest.SourceCatalogHead == (PageRef{}) {
		if manifest.SourceCatalogBytes != 0 {
			return FreeExtent{}, false, fmt.Errorf("%w: catalog retirement shape", ErrInvalidWrite)
		}
		return FreeExtent{}, false, nil
	}
	segments := pageCatalogSegmentCountFor(manifest.SourceCatalogBytes, pageSize)
	if segments == 0 || manifest.SourceCatalogHead.Length != pageSize {
		return FreeExtent{}, false, fmt.Errorf("%w: catalog retirement shape", ErrInvalidWrite)
	}
	length := uint64(segments) * uint64(pageSize)
	if manifest.SourceCatalogHead.Offset > ^uint64(0)-length {
		return FreeExtent{}, false, fmt.Errorf("%w: catalog retirement overflow", ErrInvalidWrite)
	}
	return FreeExtent{
		Offset: manifest.SourceCatalogHead.Offset, Length: length,
		RetiredGeneration: retiredGeneration,
	}, true, nil
}
