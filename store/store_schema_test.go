package store

import (
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibejson"
)

func testStoreSchema(t testing.TB) *Schema {
	t.Helper()
	schema, err := CompileSchema(SchemaDefinition{
		Root: SchemaObject,
		Fields: []SchemaField{
			{
				Path: "/profile/name", Types: SchemaString,
				Required: true,
			},
			{Path: "/tags/0", Types: SchemaString},
			{
				Path:  "/profile/age",
				Types: SchemaInteger | SchemaNull,
			},
			{Path: "/id", Types: SchemaInteger, Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestStoreSchemaCompileValidateNestedAndAllocateZero(t *testing.T) {
	schema := testStoreSchema(t)
	definition := schema.Definition()
	if got := []string{
		definition.Fields[0].Path,
		definition.Fields[1].Path,
		definition.Fields[2].Path,
		definition.Fields[3].Path,
	}; !slices.Equal(got, []string{
		"/id", "/profile/age", "/profile/name", "/tags/0",
	}) {
		t.Fatalf("canonical paths = %v", got)
	}
	slices.Reverse(definition.Fields)
	reordered, err := CompileSchema(definition)
	if err != nil {
		t.Fatal(err)
	}
	if reordered.Hash != schema.Hash {
		t.Fatalf(
			"declaration order changed identity: %#x != %#x",
			reordered.Hash, schema.Hash,
		)
	}
	redundantNumber, err := CompileSchema(SchemaDefinition{
		Root: SchemaNumber | SchemaInteger,
	})
	if err != nil {
		t.Fatal(err)
	}
	number, err := CompileSchema(SchemaDefinition{
		Root: SchemaNumber,
	})
	if err != nil {
		t.Fatal(err)
	}
	if redundantNumber.Hash != number.Hash ||
		redundantNumber.Definition().Root != SchemaNumber {
		t.Fatalf(
			"redundant integer subtype was not canonicalized: %#x/%s",
			redundantNumber.Hash,
			redundantNumber.Definition().Root,
		)
	}

	valid := []byte(
		`{"id":7,"profile":{"name":"Ada","age":null},"tags":["go"]}`,
	)
	index, err := vibejson.BuildIndex(valid, make([]vibejson.IndexEntry, 128))
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateIndex(index); err != nil {
		t.Fatalf("valid document rejected: %v", err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := schema.ValidateIndex(index); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("ValidateIndex allocated %.2f times, want 0", allocs)
	}

	for _, test := range []struct {
		name     string
		document string
		path     string
		missing  bool
	}{
		{
			name: "integer spelling", document: `{"id":7.0,"profile":{"name":"Ada"}}`,
			path: "/id",
		},
		{
			name: "nested type", document: `{"id":7,"profile":{"name":false}}`,
			path: "/profile/name",
		},
		{
			name: "required nested path", document: `{"id":7,"profile":{}}`,
			path: "/profile/name", missing: true,
		},
		{
			name: "root type", document: `[]`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			index, err := vibejson.BuildIndex(
				[]byte(test.document), make([]vibejson.IndexEntry, 128),
			)
			if err != nil {
				t.Fatal(err)
			}
			err = schema.ValidateIndex(index)
			if !errors.Is(err, ErrSchemaViolation) {
				t.Fatalf("ValidateIndex = %v", err)
			}
			var violation *SchemaViolationError
			if !errors.As(err, &violation) ||
				violation.Path != test.path ||
				violation.Missing != test.missing {
				t.Fatalf("violation = %+v", violation)
			}
		})
	}
}

func TestStoreSchemaRejectsInvalidDefinitions(t *testing.T) {
	for _, definition := range []SchemaDefinition{
		{Root: SchemaType(1 << 15)},
		{Fields: []SchemaField{{Path: "", Types: SchemaString}}},
		{Fields: []SchemaField{{
			Path: string([]byte{0xff}), Types: SchemaString,
		}}},
		{Fields: []SchemaField{{Path: "not-a-pointer", Types: SchemaString}}},
		{Fields: []SchemaField{{Path: "/x"}}},
		{Fields: []SchemaField{
			{Path: "/x", Types: SchemaString},
			{Path: "/x", Types: SchemaNumber},
		}},
	} {
		if _, err := CompileSchema(definition); !errors.Is(
			err, ErrSchemaDefinition,
		) {
			t.Fatalf("CompileStoreSchema(%+v) = %v", definition, err)
		}
	}

	collection := &Collection{Options: Options{Schema: &Schema{}}}
	if _, err := collection.Put("x", []byte(`{}`)); !errors.Is(
		err, ErrSchemaDefinition,
	) {
		t.Fatalf("uncompiled collection schema = %v", err)
	}
	if _, err := New(Options{
		Schema: &Schema{},
	}); !errors.Is(err, ErrSchemaDefinition) {
		t.Fatalf("uncompiled Collection schema = %v", err)
	}
}

func TestStoreSchemaMutationBuilderAndSnapshotAtomicity(t *testing.T) {
	schema := testStoreSchema(t)
	options := Options{
		ChunkDocuments: 4, ShapeTapes: true, Schema: schema,
	}
	collection := &Collection{Options: options}
	oldDocument := `{"id":1,"profile":{"name":"old","age":20}}`
	if created, err := collection.Put("key", []byte(oldDocument)); err != nil ||
		!created {
		t.Fatalf("initial Put = (%v,%v)", created, err)
	}
	old, _ := collection.Snapshot()
	generation := collection.Generation()
	if _, err := collection.Put(
		"key", []byte(`{"id":1,"profile":{"name":9}}`),
	); !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("invalid replacement = %v", err)
	}
	if _, err := collection.Put(
		"new", []byte(`{"id":2,"profile":{}}`),
	); !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("invalid insert = %v", err)
	}
	if collection.Generation() != generation || collection.Len() != 1 {
		t.Fatalf(
			"rejected writes changed generation/len = %d/%d",
			collection.Generation(), collection.Len(),
		)
	}
	if raw, ok := collection.GetRaw("key"); !ok ||
		string(raw.Bytes()) != oldDocument {
		t.Fatalf("current value after reject = (%q,%v)", raw.Bytes(), ok)
	}
	if raw, ok := old.GetRaw("key"); !ok ||
		string(raw.Bytes()) != oldDocument {
		t.Fatalf("snapshot after reject = (%q,%v)", raw.Bytes(), ok)
	}

	builder, err := NewBuilder(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(
		"bad", []byte(`{"id":1,"profile":{}}`),
	); !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("builder invalid Append = %v", err)
	}
	if builder.Len() != 0 {
		t.Fatalf("builder consumed rejected row: %d", builder.Len())
	}
	if err := builder.Append(
		"bad", []byte(`{"id":2,"profile":{"name":"ok"}}`),
	); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if built.Options.Schema != schema || built.Len() != 1 {
		t.Fatalf("built schema/len = %p/%d", built.Options.Schema, built.Len())
	}
}

func TestDatabaseCollectionsHaveIndependentHotPaths(t *testing.T) {
	schema := testStoreSchema(t)
	var database Database
	users, err := database.CreateCollection("users", Options{
		ChunkDocuments: 8, ShapeTapes: true, Schema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := database.CreateCollection(
		"events", Options{ChunkDocuments: 8},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(
		"same-key", []byte(`{"id":1,"profile":{"name":"Ada"}}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Put("same-key", []byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put("bad", []byte(`[]`)); !errors.Is(
		err, ErrSchemaViolation,
	) {
		t.Fatalf("users accepted invalid root: %v", err)
	}
	index, err := users.CreateIndex(IndexDefinition{
		Name:  "identity",
		Paths: []string{"/id", "/profile/name"},
	})
	if err != nil {
		t.Fatal(err)
	}
	index, err = users.BackfillIndex(index.Name, 0)
	if err != nil || index.State != IndexReady {
		t.Fatalf("collection index backfill = (%+v,%v)", index, err)
	}
	keys, err := users.AppendIndexRawKeys(
		nil, "identity", []byte(`1`), []byte(`"Ada"`),
	)
	if err != nil || !slices.Equal(keys, []string{"same-key"}) {
		t.Fatalf(
			"nested compound collection index = (%v,%v)", keys, err,
		)
	}
	if raw, ok := events.GetRaw("same-key"); !ok ||
		string(raw.Bytes()) != `[]` {
		t.Fatalf("independent event key = (%q,%v)", raw.Bytes(), ok)
	}
	if _, err := database.CreateCollection("users", Options{}); !errors.Is(
		err, ErrCollectionExists,
	) {
		t.Fatalf("duplicate collection = %v", err)
	}
	info := database.AppendCollections(nil)
	if len(info) != 2 || info[0].Name != "events" ||
		info[1].Name != "users" || !info[1].Schema {
		t.Fatalf("collection catalog = %+v", info)
	}
	if !database.DropCollection("users") {
		t.Fatal("DropCollection(users) missed")
	}
	if _, ok := database.Collection("users"); ok {
		t.Fatal("dropped collection remains cataloged")
	}
	// A catalog drop cannot revoke an already-held immutable graph or handle.
	if raw, ok := users.GetRaw("same-key"); !ok || len(raw.Bytes()) == 0 {
		t.Fatal("drop invalidated an existing collection handle")
	}
	replacement, err := database.CreateCollection(
		"users", Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if replacement == users || replacement.Len() != 0 {
		t.Fatal("recreated name aliased the dropped collection")
	}
}
