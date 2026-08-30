package query

import (
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestLowerTablePreservesSQLNullability(t *testing.T) {
	statement, err := PrepareDML(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			nickname STRING NULL,
			age INTEGER,
			active BOOL NOT NULL
		)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	definition, err := statement.LowerTable()
	if err != nil {
		t.Fatal(err)
	}
	if definition.Schema == nil {
		t.Fatal("LowerTable returned no schema")
	}
	fields := make(map[string]store.SchemaField)
	for _, field := range definition.Schema.Definition().Fields {
		fields[field.Path] = field
	}
	for _, check := range []struct {
		path     string
		types    store.SchemaType
		required bool
	}{
		{"/active", store.SchemaBool, true},
		{"/age", store.SchemaInteger | store.SchemaNull, false},
		{"/id", store.SchemaString, true},
		{"/nickname", store.SchemaString | store.SchemaNull, false},
	} {
		field, ok := fields[check.path]
		if !ok {
			t.Errorf("schema has no field %q", check.path)
			continue
		}
		if field.Types != check.types || field.Required != check.required {
			t.Errorf("%s = {%s required=%v}, want {%s required=%v}",
				check.path, field.Types, field.Required, check.types, check.required)
		}
	}
}

func TestLowerTablePrimaryOnlyIsSchemaFree(t *testing.T) {
	statement, err := PrepareDML(`CREATE TABLE docs (PRIMARY KEY (tenant.id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	definition, err := statement.LowerTable()
	if err != nil {
		t.Fatal(err)
	}
	if definition.Schema != nil {
		t.Fatalf("primary-only Schema = %+v, want nil schemaless profile", definition.Schema)
	}
	if got, want := definition.PrimaryKey, []string{"tenant.id"}; !slices.Equal(got, want) {
		t.Fatalf("primary-only PrimaryKey = %q, want %q", got, want)
	}
}

func TestLowerIndexDerivedNameIsDeterministicAndUnambiguous(t *testing.T) {
	lower := func(src string) IndexDefinition {
		t.Helper()
		statement, err := PrepareDML(src)
		if err != nil {
			t.Fatal(err)
		}
		defer statement.Release()
		definition, err := statement.LowerIndex()
		if err != nil {
			t.Fatal(err)
		}
		return definition
	}

	single := lower(`CREATE INDEX ON docs ("a+b")`)
	compound := lower(`CREATE INDEX ON docs (a, b)`)
	repeated := lower(`CREATE INDEX ON docs ("a+b")`)
	if single.Unique || compound.Unique || repeated.Unique {
		t.Fatal("ordinary CREATE INDEX lowered as unique")
	}
	if single.Definition.Name == compound.Definition.Name {
		t.Fatalf("single and compound index names collide: %q", single.Definition.Name)
	}
	if repeated.Definition.Name != single.Definition.Name {
		t.Fatalf("derived name changed: %q then %q",
			single.Definition.Name, repeated.Definition.Name)
	}
	if got, want := single.Definition.Paths, []string{"/a+b"}; !slices.Equal(got, want) {
		t.Fatalf("single paths = %q, want %q", got, want)
	}
	if got, want := compound.Definition.Paths, []string{"/a", "/b"}; !slices.Equal(got, want) {
		t.Fatalf("compound paths = %q, want %q", got, want)
	}
}

func TestLowerUniqueIndexPreservesDeclaration(t *testing.T) {
	statement, err := PrepareDML(
		`CREATE UNIQUE INDEX IF NOT EXISTS tenant_email ON docs (tenant, profile.email)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	definition, err := statement.LowerIndex()
	if err != nil {
		t.Fatal(err)
	}
	if !definition.Unique {
		t.Fatal("LowerIndex dropped UNIQUE")
	}
	if !definition.Definition.Unique {
		t.Fatal("LowerIndex dropped UNIQUE from the durable store definition")
	}
	if !definition.IfNotExists {
		t.Fatal("LowerIndex dropped IF NOT EXISTS")
	}
	if got, want := definition.Table, "docs"; got != want {
		t.Fatalf("table = %q, want %q", got, want)
	}
	if got, want := definition.Definition.Name, "tenant_email"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	if got, want := definition.Definition.Paths, []string{"/tenant", "/profile/email"}; !slices.Equal(got, want) {
		t.Fatalf("paths = %q, want %q", got, want)
	}
}

func TestLowerUnnamedUniqueIndexDerivesStableName(t *testing.T) {
	lower := func() IndexDefinition {
		t.Helper()
		statement, err := PrepareDML(`CREATE UNIQUE INDEX ON docs (email)`)
		if err != nil {
			t.Fatal(err)
		}
		defer statement.Release()
		definition, err := statement.LowerIndex()
		if err != nil {
			t.Fatal(err)
		}
		return definition
	}

	first, second := lower(), lower()
	if !first.Unique || !second.Unique ||
		!first.Definition.Unique || !second.Definition.Unique {
		t.Fatal("unnamed unique index lost UNIQUE")
	}
	if first.Definition.Name == "" || first.Definition.Name != second.Definition.Name {
		t.Fatalf("derived names = %q and %q, want one stable non-empty name",
			first.Definition.Name, second.Definition.Name)
	}
	ordinary, err := PrepareDML(`CREATE INDEX ON docs (email)`)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Release()
	ordinaryDefinition, err := ordinary.LowerIndex()
	if err != nil {
		t.Fatal(err)
	}
	if first.Definition.Name == ordinaryDefinition.Definition.Name {
		t.Fatalf(
			"unique and ordinary unnamed indexes share derived name %q",
			first.Definition.Name,
		)
	}
}
