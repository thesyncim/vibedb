package pgwire

import (
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestVirtualCatalogTablesQueryParsingAndRows(t *testing.T) {
	tables := []sqldriver.TableInfo{
		{
			Name:       "users",
			PrimaryKey: "/id",
			Columns: []sqldriver.ColumnInfo{
				{Path: "/id", Types: sqlast.TypeString, Required: true},
				{Path: "/email", Types: sqlast.TypeString | sqlast.TypeNull, Required: false},
			},
			Indexes: []sqldriver.IndexInfo{
				{Name: "idx_users_email", Paths: []string{"/email"}},
			},
		},
	}
	views := []sqldriver.ViewInfo{
		{Name: "active_users", Query: "SELECT id FROM users"},
	}

	// Verify parser recognizes all 6 virtual catalog tables.
	for _, q := range []string{
		`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`,
		`SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position`,
		`SELECT table_name, view_definition FROM information_schema.views`,
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`,
		`SELECT viewname FROM pg_catalog.pg_views`,
		`SELECT indexname FROM pg_indexes WHERE tablename = 'users'`,
	} {
		parsed, ok := parseVirtualCatalogQuery(q)
		if !ok {
			t.Fatalf("parseVirtualCatalogQuery(%q) = false, want true", q)
		}
		cols, rows := buildVirtualCatalogRows(parsed.table, "vibedb", "postgres", tables, views)
		if len(cols) == 0 || len(rows) == 0 {
			t.Fatalf("buildVirtualCatalogRows(%q) returned empty (%d cols, %d rows)", parsed.table, len(cols), len(rows))
		}
	}

	// Verify non-catalog query is NOT intercepted.
	if _, ok := parseVirtualCatalogQuery(`SELECT * FROM users`); ok {
		t.Fatal("ordinary table users was intercepted by virtual catalog shim")
	}
}
