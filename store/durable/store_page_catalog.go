package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// buildStorePageCatalog converts the standalone page store's complete frozen
// definition into the same canonical authority used by the mutable FileStore.
// StorePage currently rejects secondary and float indexes, so schema is the
// only optional catalog section.
func buildStorePageCatalog(
	schema *store.Schema,
) (*storeio.CanonicalPageCatalog, error) {
	var catalogSchema *storeio.PageCatalogSchema
	if schema != nil {
		if !schema.Valid() {
			return nil, fmt.Errorf(
				"%w: uninitialized compiled schema",
				store.ErrSchemaDefinition,
			)
		}
		definition := schema.Definition()
		catalogSchema = &storeio.PageCatalogSchema{
			Root:   uint16(definition.Root),
			Fields: make([]storeio.PageCatalogSchemaField, len(definition.Fields)),
		}
		for i, field := range definition.Fields {
			catalogSchema.Fields[i] = storeio.PageCatalogSchemaField{
				Path: field.Path, Types: uint16(field.Types),
				Required: field.Required,
			}
		}
	}
	catalog, err := storeio.BuildCanonicalPageCatalog(
		storeio.PageCatalogDefinition{Schema: catalogSchema},
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", store.ErrSchemaDefinition, err)
	}
	return catalog, nil
}

// storePageCatalogSchema cross-checks the exact canonical definition against
// every compact StateRoot summary and compiles the persisted schema used by
// later StorePageDB mutations. The canonical bytes are the authority; the
// 64-bit root hash is only a fast corruption rejection.
func storePageCatalogSchema(
	root storeio.StateRoot,
	catalog *storeio.CanonicalPageCatalog,
) (*store.Schema, error) {
	if catalog == nil {
		return nil, corruptStorePage(
			"canonical catalog", storeio.ErrPageCatalogCorrupt,
		)
	}
	definition := catalog.Definition()
	if len(definition.Indexes) != 0 ||
		len(definition.Float64Paths) != 0 ||
		root.IndexCount != 0 ||
		root.IndexDirectory != (storeio.PageRef{}) ||
		root.Options&storeio.StateOptionFloat64Columns != 0 {
		return nil, ErrStorePageUnsupported
	}
	hasSchema := definition.Schema != nil
	if hasSchema !=
		(root.Options&storeio.StateOptionSchema != 0) {
		return nil, corruptStorePage(
			"canonical schema presence", storeio.ErrPageCatalogCorrupt,
		)
	}
	if !hasSchema {
		if root.IndexCatalogHash != 0 {
			return nil, corruptStorePage(
				"empty canonical schema hash", storeio.ErrPageCatalogCorrupt,
			)
		}
		return nil, nil
	}
	schemaDefinition := store.SchemaDefinition{
		Root:   store.SchemaType(definition.Schema.Root),
		Fields: make([]store.SchemaField, len(definition.Schema.Fields)),
	}
	for i, field := range definition.Schema.Fields {
		schemaDefinition.Fields[i] = store.SchemaField{
			Path: field.Path, Types: store.SchemaType(field.Types),
			Required: field.Required,
		}
	}
	schema, err := store.CompileSchema(schemaDefinition)
	if err != nil {
		return nil, corruptStorePage("canonical schema", err)
	}
	if schema.Hash != root.IndexCatalogHash {
		return nil, corruptStorePage(
			"canonical schema hash", storeio.ErrPageCatalogCorrupt,
		)
	}
	return schema, nil
}

// resolveStorePageOpenOptions makes zero-option reopen self-describing. An
// explicit schema remains an exact assertion, while nil accepts and rehydrates
// the persisted schema. An explicit maximum is a safety ceiling; runtime frame
// geometry always uses the smaller exact persisted maximum.
func resolveStorePageOpenOptions(
	options StorePageOpenOptions,
	root storeio.StateRoot,
	catalog *storeio.CanonicalPageCatalog,
) (StorePageOpenOptions, error) {
	persistedSchema, err := storePageCatalogSchema(root, catalog)
	if err != nil {
		return StorePageOpenOptions{}, err
	}
	if options.Schema != nil {
		expected, buildErr := buildStorePageCatalog(options.Schema)
		if buildErr != nil {
			return StorePageOpenOptions{}, buildErr
		}
		if !expected.Equal(catalog) {
			return StorePageOpenOptions{}, ErrStorePageSchemaMismatch
		}
	}
	if options.MaxDocumentPageBytes != 0 &&
		options.MaxDocumentPageBytes < root.MaxPageSize {
		return StorePageOpenOptions{}, fmt.Errorf(
			"%w: file requires %d bytes, caller permits %d",
			ErrStoreDocumentPageTooLarge, root.MaxPageSize,
			options.MaxDocumentPageBytes,
		)
	}
	options.MaxDocumentPageBytes = root.MaxPageSize
	options.Schema = persistedSchema
	return options.normalized()
}
