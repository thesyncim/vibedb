package storeio

import "fmt"

// PublishStagedStateConditional is the short writer-exclusive installation
// boundary for an already fsynced, unreachable generation. It preserves every
// logical/configuration field from current, permits only physical graph/catalog
// roots to change, and atomically publishes iff current.Generation is still the
// committer's published generation. descriptor must be the canonical topology
// publication descriptor for observer/inventory completeness.
func PublishStagedStateConditional(
	tx *WriteTransaction,
	current StateRoot,
	target StateRoot,
	free InlineFreeDelta,
	descriptor []byte,
) error {
	if tx == nil || !tx.active || len(descriptor) == 0 ||
		current.Generation == 0 || target.Generation != current.Generation+1 ||
		target.StoreID != current.StoreID || target.StoreID != tx.StoreID() ||
		target.Generation != tx.Generation() ||
		target.NextLogicalID != tx.NextLogicalID() ||
		target.PrimaryRoot == (PageRef{}) ||
		!stagedStateLogicalFieldsEqual(current, target) {
		return fmt.Errorf("%w: staged state install", ErrInvalidWrite)
	}
	view, err := OpenPublicationDescriptor(descriptor)
	if err != nil {
		return err
	}
	if _, ok, err := view.Next(); err != nil || ok {
		return fmt.Errorf("%w: staged install descriptor", ErrInvalidWrite)
	}
	if err := tx.SetPublicationDescriptor(descriptor); err != nil {
		return err
	}
	return tx.PublishInlineConditional(target, free, current.Generation)
}

func stagedStateLogicalFieldsEqual(a, b StateRoot) bool {
	return a.StoreID == b.StoreID && a.PageSize == b.PageSize &&
		a.Options == b.Options && a.DocumentCount == b.DocumentCount &&
		a.IndexCount == b.IndexCount && a.IndexMaxDepth == b.IndexMaxDepth &&
		a.IndexCatalogHash == b.IndexCatalogHash &&
		a.MaterializationDamageGranule == b.MaterializationDamageGranule &&
		a.MaxPageSize == b.MaxPageSize &&
		a.PageCatalogDigest == b.PageCatalogDigest &&
		a.PageCatalogBytes == b.PageCatalogBytes &&
		a.MaxKeyBytes == b.MaxKeyBytes &&
		a.InlineValueBytes == b.InlineValueBytes &&
		a.MaxDocumentBytes == b.MaxDocumentBytes &&
		a.JournalID == b.JournalID &&
		a.PhysicalCapacityBytes == b.PhysicalCapacityBytes
}
