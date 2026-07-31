package durable

import (
	"fmt"
	"math"
	"os"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// filePageCatalogPlan is the immutable, contiguous generation-one catalog
// extent. The run needs no retained reference vector: every physical and
// logical reference is derived from head and ordinal.
type filePageCatalogPlan struct {
	catalog  *storeio.CanonicalPageCatalog
	storeID  [16]byte
	head     storeio.PageRef
	bytes    uint32
	digest   [storeio.PageCatalogDigestSize]byte
	segments uint16
	fileEnd  uint64
	nextID   uint64
}

func planFilePageCatalog(
	catalog *storeio.CanonicalPageCatalog,
	storeID [16]byte,
	generation uint64,
	pageSize uint32,
	dataStart uint64,
	nextLogicalID uint64,
) (filePageCatalogPlan, error) {
	if catalog == nil || storeID == ([16]byte{}) || generation == 0 ||
		pageSize == 0 ||
		dataStart%uint64(pageSize) != 0 ||
		nextLogicalID == 0 {
		return filePageCatalogPlan{}, storeio.ErrInvalidWrite
	}
	size := catalog.CanonicalSize()
	definition := catalog.Definition()
	semanticEmpty := len(definition.Indexes) == 0 &&
		definition.Schema == nil
	if size == 0 {
		if !semanticEmpty {
			return filePageCatalogPlan{}, storeio.ErrInvalidWrite
		}
		return filePageCatalogPlan{
			catalog: catalog, fileEnd: dataStart, nextID: nextLogicalID,
		}, nil
	}
	if semanticEmpty || size > storeio.PageCatalogMaxCanonicalBytes {
		return filePageCatalogPlan{}, storeio.ErrInvalidWrite
	}
	count, ok := catalog.SegmentCountFor(pageSize)
	if !ok || count == 0 || count > int(^uint16(0)) ||
		uint64(count) > math.MaxUint64/uint64(pageSize) {
		return filePageCatalogPlan{}, storeio.ErrInvalidWrite
	}
	runBytes := uint64(count) * uint64(pageSize)
	if dataStart > math.MaxInt64-runBytes ||
		nextLogicalID > math.MaxUint64-uint64(count) {
		return filePageCatalogPlan{}, storeio.ErrInvalidWrite
	}
	return filePageCatalogPlan{
		catalog: catalog,
		storeID: storeID,
		head: storeio.PageRef{
			Offset: dataStart, LogicalID: nextLogicalID,
			Generation: generation, Length: pageSize,
			Kind: storeio.PageCatalogSegment,
		},
		bytes:    uint32(size),
		digest:   catalog.Digest(),
		segments: uint16(count),
		fileEnd:  dataStart + runBytes,
		nextID:   nextLogicalID + uint64(count),
	}, nil
}

func (p filePageCatalogPlan) apply(
	root *storeio.StateRoot,
	maxPageSize uint32,
) error {
	if root == nil || maxPageSize == 0 {
		return storeio.ErrInvalidWrite
	}
	root.MaxPageSize = maxPageSize
	if p.segments == 0 {
		if p.bytes != 0 || p.head != (storeio.PageRef{}) ||
			p.digest != ([storeio.PageCatalogDigestSize]byte{}) {
			return storeio.ErrInvalidWrite
		}
		return nil
	}
	root.PageCatalogHead = p.head
	root.PageCatalogDigest = p.digest
	root.PageCatalogBytes = p.bytes
	return nil
}

func (p filePageCatalogPlan) write(
	file *os.File,
	fileEnd uint64,
	nextLogicalID uint64,
	pageScratch []byte,
) error {
	if p.segments == 0 {
		return nil
	}
	if file == nil || p.catalog == nil ||
		len(pageScratch) < int(p.head.Length) ||
		fileEnd < p.fileEnd || nextLogicalID < p.nextID {
		return storeio.ErrInvalidWrite
	}
	bounds := storeio.PageCatalogBounds{
		StoreID: p.storeID, Generation: p.head.Generation,
		PageSize: p.head.Length, DataStart: p.head.Offset,
		FileEnd: fileEnd, NextLogicalID: nextLogicalID,
		TotalBytes:     p.bytes,
		ExpectedDigest: p.digest,
	}
	for ordinal := uint16(0); ordinal < p.segments; ordinal++ {
		ref := p.ref(ordinal)
		var next storeio.PageRef
		if ordinal+1 < p.segments {
			next = p.ref(ordinal + 1)
		}
		page, err := storeio.EncodePageCatalogSegment(
			pageScratch[:p.head.Length],
			storeio.PageCatalogSegmentHeader{
				StoreID: bounds.StoreID, Generation: ref.Generation,
				LogicalID: ref.LogicalID, Ordinal: ordinal, Next: next,
			},
			p.catalog,
			bounds,
		)
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, ref.Offset); err != nil {
			return err
		}
	}
	return nil
}

func (p filePageCatalogPlan) ref(ordinal uint16) storeio.PageRef {
	delta := uint64(ordinal)
	return storeio.PageRef{
		Offset:     p.head.Offset + delta*uint64(p.head.Length),
		LogicalID:  p.head.LogicalID + delta,
		Generation: p.head.Generation, Length: p.head.Length,
		Kind: storeio.PageCatalogSegment,
	}
}

// fileStoreCollectionOptionFlags returns durable collection semantics carried
// through every root generation.
func fileStoreCollectionOptionFlags(options store.Options) uint32 {
	if options.Schema != nil {
		return storeio.StateOptionSchema
	}
	return 0
}

// normalizeOpenedFileStoreOptions reconstructs every frozen collection option
// from the selected root and its exact catalog before any cache, committer, or
// schema resource is built. Non-zero/non-empty caller values are assertions;
// runtime-only tuning remains caller controlled.
func normalizeOpenedFileStoreOptions(
	supplied Options,
	root storeio.StateRoot,
	catalog *storeio.CanonicalPageCatalog,
) (normalizedFileStoreOptions, error) {
	if catalog == nil {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: missing canonical catalog", storeio.ErrPageCatalogCorrupt,
		)
	}
	definition := catalog.Definition()
	hasIndexes := len(definition.Indexes) != 0
	hasSchema := definition.Schema != nil
	catalogAsserted := supplied.Indexes != nil ||
		supplied.Collection.Schema != nil
	if root.IndexCount != uint32(catalog.PhysicalIndexCount()) ||
		(root.Options&storeio.StateOptionSchema != 0) != hasSchema ||
		(root.PageCatalogBytes != 0) !=
			(hasIndexes || hasSchema) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: catalog definition disagrees with state root",
			storeio.ErrPageCatalogCorrupt,
		)
	}
	if root.MaxKeyBytes == 0 ||
		root.InlineValueBytes == 0 ||
		root.MaxDocumentBytes == 0 ||
		uint64(root.MaxKeyBytes) > uint64(math.MaxInt) ||
		uint64(root.InlineValueBytes) > uint64(math.MaxInt) ||
		uint64(root.MaxDocumentBytes) > uint64(math.MaxInt) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: missing or unsupported admission geometry",
			storeio.ErrPageCatalogCorrupt,
		)
	}

	if root.PageSize != 4096 {
		return normalizedFileStoreOptions{}, ErrUnsupportedPageSize
	}
	if supplied.PageSize != 0 && supplied.PageSize != int(root.PageSize) ||
		supplied.MaxPageSize != 0 &&
			supplied.MaxPageSize != int(root.MaxPageSize) ||
		supplied.MaterializationDamageGranule !=
			int(root.MaterializationDamageGranule) ||
		supplied.MaxKeyBytes != 0 &&
			supplied.MaxKeyBytes != int(root.MaxKeyBytes) ||
		supplied.InlineValueBytes != 0 &&
			supplied.InlineValueBytes != int(root.InlineValueBytes) ||
		supplied.MaxDocumentBytes != 0 &&
			supplied.MaxDocumentBytes != int(root.MaxDocumentBytes) ||
		supplied.Collection.IndexOptions.MaxDepth > 0 &&
			supplied.Collection.IndexOptions.MaxDepth !=
				int(root.IndexMaxDepth) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibejson: collection persisted option mismatch",
		)
	}
	persistedFlags := root.Options & storeio.StateOptionSchema
	assertedFlags := fileStoreCollectionOptionFlags(supplied.Collection)
	if assertedFlags&^persistedFlags != 0 {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibejson: collection persisted representation mismatch",
		)
	}

	options := supplied
	options.PageSize = int(root.PageSize)
	options.MaxPageSize = int(root.MaxPageSize)
	options.MaxKeyBytes = int(root.MaxKeyBytes)
	options.InlineValueBytes = int(root.InlineValueBytes)
	options.MaxDocumentBytes = int(root.MaxDocumentBytes)
	options.Collection.IndexOptions.MaxDepth = int(root.IndexMaxDepth)
	if options.Indexes == nil {
		options.Indexes = make(
			[]store.IndexDefinition, len(definition.Indexes),
		)
		for i, index := range definition.Indexes {
			options.Indexes[i] = store.IndexDefinition{
				Name: index.Name, Paths: slices.Clone(index.Paths),
			}
		}
	}
	if options.Collection.Schema == nil && definition.Schema != nil {
		fields := make(
			[]store.SchemaField, len(definition.Schema.Fields),
		)
		for i, field := range definition.Schema.Fields {
			fields[i] = store.SchemaField{
				Path: field.Path, Types: store.SchemaType(field.Types),
				Required: field.Required,
			}
		}
		schema, err := store.CompileSchema(store.SchemaDefinition{
			Root:   store.SchemaType(definition.Schema.Root),
			Fields: fields,
		})
		if err != nil {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: persisted schema: %v",
				storeio.ErrPageCatalogCorrupt, err,
			)
		}
		options.Collection.Schema = schema
	}

	normalized, err := options.normalized()
	if err != nil {
		return normalizedFileStoreOptions{}, err
	}
	if !normalized.pageCatalog.Equal(catalog) {
		if catalogAsserted {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"vibejson: collection exact catalog assertion mismatch",
			)
		}
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: rehydrated catalog disagrees with canonical bytes",
			storeio.ErrPageCatalogCorrupt,
		)
	}
	if root.IndexCount != uint32(len(normalized.indexes)) ||
		root.IndexCatalogHash != normalized.indexCatalogHash ||
		root.IndexMaxDepth !=
			uint32(max(normalized.Collection.IndexOptions.MaxDepth, 0)) ||
		persistedFlags !=
			fileStoreCollectionOptionFlags(normalized.Collection) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: state summaries disagree with canonical catalog",
			storeio.ErrPageCatalogCorrupt,
		)
	}
	return normalized, nil
}
