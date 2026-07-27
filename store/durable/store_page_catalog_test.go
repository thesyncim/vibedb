package durable

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func storePageCatalogTestSchema(t testing.TB) *store.Schema {
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
	return schema
}

func storePageCatalogTestFile(
	t testing.TB,
) (string, *store.Schema) {
	t.Helper()
	schema := storePageCatalogTestSchema(t)
	collection, err := store.New(store.Options{
		ChunkDocuments: 4, Schema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created, err := collection.Put(
		"one", []byte(`{"id":1,"name":"one"}`),
	); err != nil || !created {
		t.Fatalf("initial schema document = (%v, %v)", created, err)
	}
	path, _ := writeStorePageTestFile(
		t, collection,
		StorePageWriteOptions{MaxDocumentPageBytes: storePageQuantum},
	)
	return path, schema
}

func TestStorePageCatalogZeroOptionReopenRestoresSchemaAndGeometry(
	t *testing.T,
) {
	path, schema := storePageCatalogTestFile(t)

	recovery, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var scratch [storePageQuantum]byte
	super, root, _, err := storeio.RecoverStateRoot(
		recovery, storePageQuantum, scratch[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := openFilePageCatalog(
		recovery, root, super.FileEnd, scratch[:],
	)
	closeErr := recovery.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("open canonical catalog = (%v, %v)", err, closeErr)
	}
	if root.MaxPageSize != storePageQuantum ||
		root.PageCatalogBytes == 0 ||
		root.PageCatalogHead.Kind != storeio.PageCatalogSegment ||
		catalog.Definition().Schema == nil {
		t.Fatalf("self-describing root = %+v", root)
	}

	// ResidentBytes is deliberately too small for the former 64 KiB caller
	// default. Success proves Open used the persisted 4 KiB maximum first.
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * int64(storePageQuantum),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := reader.AppendRaw(nil, "one")
	closeErr = reader.Close()
	if err != nil || closeErr != nil || !ok ||
		string(raw) != `{"id":1,"name":"one"}` {
		t.Fatalf(
			"zero-option schema read = (%q, %v, %v, %v)",
			raw, ok, err, closeErr,
		)
	}

	other, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaArray,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStorePageReader(path, StorePageOpenOptions{
		Schema: other,
	}); !errors.Is(err, ErrStorePageSchemaMismatch) {
		t.Fatalf(
			"different exact schema = %v, want %v",
			err, ErrStorePageSchemaMismatch,
		)
	}

	db, err := OpenStorePageDB(path, StorePageDBOptions{
		Open: StorePageOpenOptions{
			ResidentBytes: 2 * int64(storePageQuantum),
		},
		CommitBackend: StorePageCommitPortable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if db.options.Open.MaxDocumentPageBytes != storePageQuantum ||
		db.options.Open.Schema == nil ||
		db.options.Open.Schema.Hash != schema.Hash {
		t.Fatalf("rehydrated mutable options = %+v", db.options.Open)
	}
	if _, err := db.Put("bad", []byte(`{}`)); !errors.Is(
		err, store.ErrSchemaViolation,
	) {
		t.Fatalf("rehydrated schema accepted invalid Put: %v", err)
	}
	if created, err := db.Put(
		"two", []byte(`{"id":2}`),
	); err != nil || !created {
		t.Fatalf("schema-valid Put = (%v, %v)", created, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStorePageDB(path, StorePageDBOptions{
		CommitBackend: StorePageCommitPortable,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	raw, ok, err = reopened.AppendRaw(nil, "two")
	if err != nil || !ok || string(raw) != `{"id":2}` ||
		reopened.root.PageCatalogHead != root.PageCatalogHead ||
		reopened.root.PageCatalogBytes != root.PageCatalogBytes ||
		reopened.root.PageCatalogDigest != root.PageCatalogDigest {
		t.Fatalf(
			"post-mutation catalog reopen = (%q, %v, %v, %+v)",
			raw, ok, err, reopened.root,
		)
	}
}

func TestStorePageCatalogCorruptionFailsClosed(t *testing.T) {
	path, _ := storePageCatalogTestFile(t)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var scratch [storePageQuantum]byte
	_, root, _, err := storeio.RecoverStateRoot(
		file, storePageQuantum, scratch[:],
	)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(root.PageCatalogHead.Offset) +
		int64(storeio.PageHeaderSize+
			storeio.PageCatalogSegmentPayloadHeaderSize)
	var one [1]byte
	if _, err := file.ReadAt(one[:], offset); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0x80
	if _, err := file.WriteAt(one[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStorePageReader(
		path, StorePageOpenOptions{},
	); !errors.Is(err, ErrStorePageCorrupt) ||
		!errors.Is(err, storeio.ErrPageCatalogCorrupt) {
		t.Fatalf(
			"corrupt canonical catalog = %v, want %v and %v",
			err, ErrStorePageCorrupt, storeio.ErrPageCatalogCorrupt,
		)
	}
}
