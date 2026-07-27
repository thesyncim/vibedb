package durable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func testDurableStoreSchema(t testing.TB) *store.Schema {
	t.Helper()
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
		Fields: []store.SchemaField{
			{
				Path: "/profile/name", Types: store.SchemaString,
				Required: true,
			},
			{Path: "/tags/0", Types: store.SchemaString},
			{
				Path:  "/profile/age",
				Types: store.SchemaInteger | store.SchemaNull,
			},
			{Path: "/id", Types: store.SchemaInteger, Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestStoreSchemaPersistsAndRevalidatesCheckpoint(t *testing.T) {
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
		Fields: []store.SchemaField{
			{Path: "/id", Types: store.SchemaInteger, Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	heapCollection, err := store.New(store.Options{
		ChunkDocuments: 2, ShapeTapes: true, Schema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	document := " \n{\"id\":7}\t"
	if _, err := heapCollection.Put("key", []byte(document)); err != nil {
		t.Fatal(err)
	}
	if _, err := heapCollection.Put("key2", []byte(`{"id":8}`)); err != nil {
		t.Fatal(err)
	}
	var image bytes.Buffer
	if _, err := heapCollection.WriteTo(&image); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(image.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Options.Schema == nil || !reflect.DeepEqual(
		reopened.Options.Schema.Definition(), schema.Definition(),
	) {
		t.Fatalf(
			"reopened schema = %+v",
			reopened.Options.Schema,
		)
	}
	generation := reopened.Generation()
	if _, err := reopened.Put(
		"key", []byte(`{"id":"wrong"}`),
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("reopened collection accepted invalid replacement: %v", err)
	}
	if reopened.Generation() != generation {
		t.Fatal("rejected reopened write advanced generation")
	}

	// A valid checksum is not enough: OpenStore revalidates every persisted
	// row against the restored contract before publishing the graph.
	corrupt := append([]byte(nil), image.Bytes()...)
	footer := corrupt[len(corrupt)-store.ImageFooterLen:]
	offset := binary.LittleEndian.Uint64(footer[8:16])
	length := binary.LittleEndian.Uint64(footer[16:24])
	manifest := corrupt[offset : offset+length]
	field := store.CheckpointManifestFixed
	binary.LittleEndian.PutUint16(
		manifest[field+4:field+6], uint16(store.SchemaString),
	)
	binary.LittleEndian.PutUint64(
		footer[24:32], store.ManifestChecksum(manifest),
	)
	if _, err := store.Open(corrupt); !errors.Is(
		err, store.ErrCheckpointCorrupt,
	) {
		t.Fatalf("schema/data mismatch = %v", err)
	}

	legacy, err := os.CreateTemp(t.TempDir(), "heapStore-page-schema-*")
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := legacy.Name()
	if _, err := WritePageFile(heapCollection,
		legacy, StorePageWriteOptions{},
	); err != nil {
		t.Fatalf("schema-bound page export = %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	zeroReader, err := OpenStorePageReader(
		legacyPath, StorePageOpenOptions{},
	)
	if err != nil {
		t.Fatalf("self-describing page reader = %v", err)
	}
	if raw, ok, err := zeroReader.AppendRaw(nil, "key"); err != nil ||
		!ok || string(raw) != document {
		t.Fatalf(
			"self-describing schema page read = (%q,%v,%v)",
			raw, ok, err,
		)
	}
	if err := zeroReader.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenStorePageReader(
		legacyPath, StorePageOpenOptions{Schema: schema},
	)
	if err != nil {
		t.Fatal(err)
	}
	if raw, ok, err := reader.AppendRaw(nil, "key"); err != nil ||
		!ok || string(raw) != document {
		t.Fatalf("schema page read = (%q,%v,%v)", raw, ok, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	pageDB, err := OpenStorePageDB(legacyPath, StorePageDBOptions{
		Open: StorePageOpenOptions{Schema: schema},
	})
	if err != nil {
		t.Fatal(err)
	}
	pageGeneration := pageDB.Generation()
	if _, err := pageDB.Put(
		"key", []byte(`{"id":"wrong"}`),
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("page DB schema rejection = %v", err)
	}
	if pageDB.Generation() != pageGeneration {
		t.Fatal("rejected page DB write advanced generation")
	}
	if _, err := pageDB.Put("key", []byte(`{"id":8}`)); err != nil {
		t.Fatal(err)
	}
	if err := pageDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreSchemaMutationRecoveryAndBulk(t *testing.T) {
	schema := testDurableStoreSchema(t)
	options := testFileStoreOptions()
	options.Collection.Schema = schema
	file, err := os.CreateTemp(t.TempDir(), "file-fs-schema-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":1,"profile":{"name":"Ada"}}`)
	if _, err := fs.Put("key", document); err != nil {
		t.Fatal(err)
	}
	generation := fs.Generation()
	sizeBefore, err := file.Seek(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put(
		"key", []byte(`{"id":1,"profile":{}}`),
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("collection invalid replacement = %v", err)
	}
	sizeAfter, err := file.Seek(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if fs.Generation() != generation || sizeAfter != sizeBefore {
		t.Fatalf(
			"rejected collection write changed generation/file = %d/%d, want %d/%d",
			fs.Generation(), sizeAfter, generation, sizeBefore,
		)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := reopened.AppendRaw(nil, "key"); err != nil ||
		!ok || !bytes.Equal(got, document) {
		t.Fatalf("reopened document = (%q,%v,%v)", got, ok, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	otherSchema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaArray,
	})
	if err != nil {
		t.Fatal(err)
	}
	wrong := options
	wrong.Collection.Schema = otherSchema
	if opened, err := Open(file, wrong); err == nil {
		_ = opened.Close()
		t.Fatal("Open accepted a mismatched schema")
	}

	source, err := store.New(store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put("bad", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	bulkFile, err := os.CreateTemp(t.TempDir(), "file-fs-schema-bulk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer bulkFile.Close()
	if _, err := CreateFrom(source,
		bulkFile, options,
	); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("bulk schema validation = %v", err)
	}
	if info, err := bulkFile.Stat(); err != nil || info.Size() != 0 {
		t.Fatalf("failed bulk image size = (%v,%v)", info, err)
	}

	validSource, err := store.New(store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validSource.Put(
		"good", []byte(`{"id":2,"profile":{"name":"bulk"}}`),
	); err != nil {
		t.Fatal(err)
	}
	validBulk, err := os.CreateTemp(
		t.TempDir(), "file-fs-schema-valid-bulk-*",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer validBulk.Close()
	if _, err := CreateFrom(validSource, validBulk, options); err != nil {
		t.Fatal(err)
	}
	bulkCollection, err := Open(validBulk, options)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := bulkCollection.AppendRaw(nil, "good"); err != nil ||
		!ok || string(got) != `{"id":2,"profile":{"name":"bulk"}}` {
		t.Fatalf("valid bulk document = (%q,%v,%v)", got, ok, err)
	}
	if err := bulkCollection.Close(); err != nil {
		t.Fatal(err)
	}
}
