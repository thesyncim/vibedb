package durable

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

func testFileCatalogOptions(t testing.TB) Options {
	t.Helper()
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
		Fields: []store.SchemaField{{
			Path: "/id", Types: store.SchemaInteger, Required: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Collection = store.Options{
		ChunkDocuments: 8,
		IndexOptions: document.IndexOptions{
			MaxDepth: 17, HashKeys: true,
		},
		ShapeTapes: true,
		Postings:   true,
		ValueDict:  true,
		Schema:     schema,
	}
	options.Indexes = []store.IndexDefinition{
		{Name: "id", Paths: []string{"/id"}},
		{Name: "id_alias", Paths: []string{"/id"}},
	}
	return options
}

func recoverFileCatalogRoot(
	t testing.TB,
	file *os.File,
	pageSize uint32,
) (storeio.InlineSuperblock, storeio.StateRoot) {
	t.Helper()
	// Recover exactly the way Open does: the deferred-canonical lanes settle the
	// authoritative root through the materialization capsules (and, for un-
	// checkpointed generations, the recovery journal), so a bare RecoverInlineState-
	// Root that only trusts the fixed superblock and its direct refs sees no valid
	// root. RecoverMutableInlineStateRoot resolves the capsules against each root
	// candidate the same way Open's Open-time recovery does; its scratch must hold
	// the largest top-level reference, i.e. one MaxPageSize extent.
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := storeio.RecoverMutableInlineStateRoot(
		file, bootstrap.PageSize, bootstrap.MaterializationDamageGranule,
		make([]byte, bootstrap.MaxPageSize),
	)
	if err != nil {
		t.Fatal(err)
	}
	return recovery.Root, recovery.State
}

func TestFileStoreExactCatalogZeroOptionReopenAndRootPreservation(
	t *testing.T,
) {
	file, err := os.CreateTemp(t.TempDir(), "file-catalog-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileCatalogOptions(t)
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	_, initial := recoverFileCatalogRoot(
		t, file, uint32(options.PageSize),
	)
	if initial.MaxPageSize != uint32(options.MaxPageSize) ||
		initial.MaxKeyBytes != uint32(options.MaxKeyBytes) ||
		initial.InlineValueBytes != uint32(options.InlineValueBytes) ||
		initial.MaxDocumentBytes != uint32(options.MaxDocumentBytes) ||
		initial.PageCatalogHead == (storeio.PageRef{}) ||
		initial.PageCatalogBytes < storeio.PageCatalogCanonicalHeaderSize ||
		initial.IndexCount != 1 {
		t.Fatalf("initial exact catalog root = %+v", initial)
	}
	if _, err := collection.Put(
		"key", []byte(`{"id":7,"score":1.5}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	_, mutated := recoverFileCatalogRoot(
		t, file, uint32(options.PageSize),
	)
	if mutated.Generation <= initial.Generation ||
		mutated.MaxPageSize != initial.MaxPageSize ||
		mutated.MaxKeyBytes != initial.MaxKeyBytes ||
		mutated.InlineValueBytes != initial.InlineValueBytes ||
		mutated.MaxDocumentBytes != initial.MaxDocumentBytes ||
		mutated.PageCatalogHead != initial.PageCatalogHead ||
		mutated.PageCatalogDigest != initial.PageCatalogDigest ||
		mutated.PageCatalogBytes != initial.PageCatalogBytes ||
		mutated.IndexMaxDepth != initial.IndexMaxDepth ||
		mutated.Options != initial.Options {
		t.Fatalf(
			"catalog/frozen root changed across mutation:\ninitial=%+v\nmutated=%+v",
			initial, mutated,
		)
	}

	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.options.Collection.ChunkDocuments !=
		options.Collection.ChunkDocuments ||
		reopened.options.MaxKeyBytes != options.MaxKeyBytes ||
		reopened.options.InlineValueBytes != options.InlineValueBytes ||
		reopened.options.MaxDocumentBytes != options.MaxDocumentBytes ||
		reopened.options.Collection.IndexOptions !=
			options.Collection.IndexOptions ||
		!reopened.options.Collection.ShapeTapes ||
		!reopened.options.Collection.Postings ||
		!reopened.options.Collection.ValueDict ||
		reopened.options.Collection.Schema == nil ||
		!reflect.DeepEqual(
			reopened.options.Collection.Schema.Definition(),
			options.Collection.Schema.Definition(),
		) ||
		!reflect.DeepEqual(reopened.options.Indexes, options.Indexes) ||
		len(reopened.options.indexes) != 1 ||
		reopened.options.indexNameIDs["id"] != 0 ||
		reopened.options.indexNameIDs["id_alias"] != 0 {
		t.Fatalf("zero-option reopen did not rehydrate catalog: %+v", reopened.options.Options)
	}
	raw, ok, err := reopened.AppendRaw(nil, "key")
	if err != nil || !ok || string(raw) !=
		`{"id":7,"score":1.5}` {
		t.Fatalf("zero-option reopened read = (%q,%v,%v)", raw, ok, err)
	}
	if _, err := reopened.Put(
		"invalid", []byte(`{"id":"wrong"}`),
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("rehydrated schema accepted invalid write: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	mismatch := Options{
		Indexes: []store.IndexDefinition{{
			Name: "other", Paths: []string{"/other"},
		}},
	}
	if _, err := Open(file, mismatch); err == nil {
		t.Fatal("Open accepted a caller catalog that differs from durable bytes")
	}
	explicitEmpty := Options{
		Indexes:        []store.IndexDefinition{},
		Float64Columns: []string{},
	}
	if _, err := Open(file, explicitEmpty); err == nil {
		t.Fatal("Open treated explicit empty catalogs as unspecified")
	}
	admissionMismatch := Options{
		MaxKeyBytes: options.MaxKeyBytes + 1,
	}
	if _, err := Open(file, admissionMismatch); err == nil {
		t.Fatal("Open accepted different immutable admission geometry")
	}
}

func TestFileStoreExactCatalogCorruptionFailsBeforeResources(
	t *testing.T,
) {
	file, err := os.CreateTemp(t.TempDir(), "file-catalog-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileCatalogOptions(t)
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	_, root := recoverFileCatalogRoot(
		t, file, uint32(options.PageSize),
	)
	offset := root.PageCatalogHead.Offset +
		uint64(storeio.PageHeaderSize+
			storeio.PageCatalogSegmentPayloadHeaderSize)
	var value [1]byte
	if _, err := file.ReadAt(value[:], int64(offset)); err != nil {
		t.Fatal(err)
	}
	value[0] ^= 0xff
	if _, err := file.WriteAt(value[:], int64(offset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file, Options{}); !errors.Is(
		err, storeio.ErrPageCatalogCorrupt,
	) && !errors.Is(err, storeio.ErrSuperblockNotFound) {
		t.Fatalf(
			"catalog corruption = %v, want catalog-corrupt root rejection",
			err,
		)
	}
}

func TestFileStoreMultiPageExactCatalogZeroOptionReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-catalog-multi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Indexes = make([]store.IndexDefinition, 256)
	for i := range options.Indexes {
		options.Indexes[i] = store.IndexDefinition{
			Name: fmt.Sprintf(
				"alias-%03d-xxxxxxxxxxxxxxxxxxxxxxxx", i,
			),
			Paths: []string{"/shared"},
		}
	}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	_, root := recoverFileCatalogRoot(
		t, file, uint32(options.PageSize),
	)
	if root.NextLogicalID <= root.PageCatalogHead.LogicalID+1 ||
		root.IndexCount != 1 {
		t.Fatalf("multi-page deduplicated catalog root = %+v", root)
	}
	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.options.Indexes) != len(options.Indexes) ||
		len(reopened.options.indexes) != 1 {
		t.Fatalf(
			"multi-page catalog aliases/physical = %d/%d",
			len(reopened.options.Indexes), len(reopened.options.indexes),
		)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}


func TestFileStoreMaterializationRequiresCurrentDeviceAssertion(
	t *testing.T,
) {
	file, err := os.CreateTemp(t.TempDir(), "file-catalog-materialize-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	// Canonical materialization requires a committer that owns the checkpoint. The
	// buffered-visible and sync-primary lanes drive it manually (ManualCheckpoint),
	// which the committer refuses to combine with a materialization granule, so the
	// device-assertion round trip is exercised on the async lane, whose committer
	// materializes each generation itself.
	options.Durability = DurabilityAsyncVisible
	options.MaterializationDamageGranule =
		storeio.MaterializationJournalMinSectorSize
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file, Options{}); err == nil {
		t.Fatal(
			"Open inherited a persisted storage-stack capability assertion",
		)
	}
	reopened, err := Open(file, Options{
		Durability:                   DurabilityAsyncVisible,
		MaterializationDamageGranule: options.MaterializationDamageGranule,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
