package durable

import (
	"bytes"
	"errors"
	"os"
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

func TestFileStoreSchemaMutationAndRecovery(t *testing.T) {
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
}
