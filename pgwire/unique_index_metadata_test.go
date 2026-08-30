package pgwire

import (
	"fmt"
	"strings"
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func uniqueIndexMetadataTable() sqldriver.TableInfo {
	return sqldriver.TableInfo{
		Name:       "docs",
		PrimaryKey: "/id",
		Indexes: []sqldriver.IndexInfo{
			{Name: "docs_by_city", Paths: []string{"/city"}},
			{Name: "docs_email_key", Paths: []string{"/email"}, Unique: true},
		},
	}
}

func fixedRowByValue(t *testing.T, result *fixedResult, column, value string) []*string {
	t.Helper()
	position := -1
	for i := range result.cols {
		if result.cols[i].name == column {
			position = i
			break
		}
	}
	if position < 0 {
		t.Fatalf("result has no %q column", column)
	}
	for _, row := range result.rows {
		if row[position] != nil && *row[position] == value {
			return row
		}
	}
	t.Fatalf("result has no row with %s=%q", column, value)
	return nil
}

func fixedValue(t *testing.T, result *fixedResult, row []*string, column string) *string {
	t.Helper()
	for i := range result.cols {
		if result.cols[i].name == column {
			return row[i]
		}
	}
	t.Fatalf("result has no %q column", column)
	return nil
}

func requireFixedValue(t *testing.T, result *fixedResult, row []*string, column, want string) {
	t.Helper()
	got := fixedValue(t, result, row, column)
	if got == nil || *got != want {
		t.Fatalf("%s = %v, want %q", column, got, want)
	}
}

func TestUniqueConstraintMapsToUniqueViolation(t *testing.T) {
	err := fmt.Errorf("insert docs: %w", sqldriver.ErrUniqueConstraint)
	got := asPGError(err)
	if got.code != sqlstateUniqueViolation {
		t.Fatalf("SQLSTATE = %q, want %q", got.code, sqlstateUniqueViolation)
	}
	if got.message != err.Error() {
		t.Fatalf("message = %q, want %q", got.message, err.Error())
	}
}

func TestPSQLUniqueSecondaryIndexMetadata(t *testing.T) {
	answer := catalogAnswer{
		tables: []sqldriver.TableInfo{uniqueIndexMetadataTable()},
		oids:   []catalogOIDEntry{{oid: firstCatalogTableOID, name: "docs"}},
	}
	result := respondTableIndexes(&answer, fmt.Sprint(firstCatalogTableOID))

	unique := fixedRowByValue(t, result, "relname", "docs_email_key")
	requireFixedValue(t, result, unique, "indisprimary", "f")
	requireFixedValue(t, result, unique, "indisunique", "t")
	requireFixedValue(t, result, unique, "pg_get_indexdef",
		"CREATE UNIQUE INDEX docs_email_key ON public.docs USING exact (email)")
	for _, column := range []string{"pg_get_constraintdef", "contype", "condeferrable", "condeferred"} {
		if got := fixedValue(t, result, unique, column); got != nil {
			t.Fatalf("unique index unexpectedly exposed %s=%q constraint metadata", column, *got)
		}
	}

	nonunique := fixedRowByValue(t, result, "relname", "docs_by_city")
	requireFixedValue(t, result, nonunique, "indisunique", "f")
	requireFixedValue(t, result, nonunique, "pg_get_indexdef",
		"CREATE INDEX docs_by_city ON public.docs USING exact (city)")
}

func TestGoLandUniqueSecondaryIndexMetadata(t *testing.T) {
	server := &Server{opts: Options{MaxResultRows: 32, MaxResultBytes: 1 << 20}}
	s := session{server: server}
	answer := catalogAnswer{
		database: "app",
		user:     "user",
		tables:   []sqldriver.TableInfo{uniqueIndexMetadataTable()},
		oidMap:   map[string]uint32{"docs": firstCatalogTableOID},
	}
	result, err := s.answerDiscovery(discoveryTestShape(t, "RetrieveIndices"), discoveryFilter{}, &answer)
	if err != nil {
		t.Fatal(err)
	}

	primary := fixedRowByValue(t, result, "index_name", "docs_pkey")
	requireFixedValue(t, result, primary, "is_primary", "t")
	requireFixedValue(t, result, primary, "is_unique", "t")
	unique := fixedRowByValue(t, result, "index_name", "docs_email_key")
	requireFixedValue(t, result, unique, "is_primary", "f")
	requireFixedValue(t, result, unique, "is_unique", "t")
	nonunique := fixedRowByValue(t, result, "index_name", "docs_by_city")
	requireFixedValue(t, result, nonunique, "is_primary", "f")
	requireFixedValue(t, result, nonunique, "is_unique", "f")
}

func TestJDBCUniqueSecondaryIndexMetadata(t *testing.T) {
	server := &Server{opts: Options{MaxResultRows: 32, MaxResultBytes: 1 << 20}}
	s := session{server: server}
	answer := catalogAnswer{
		database: "app",
		user:     "user",
		tables:   []sqldriver.TableInfo{uniqueIndexMetadataTable()},
	}

	seenAll, seenUniqueOnly := false, false
	for i := range discoveryShapes {
		shape := &discoveryShapes[i]
		if shape.Name != "JDBC indexes" {
			continue
		}
		result, err := s.answerJDBCDiscovery(shape, discoveryFilter{}, &answer, nil)
		if err != nil {
			t.Fatal(err)
		}
		uniqueOnly := strings.Contains(strings.Join(shape.tokens, " "), "and i . indisunique")
		unique := fixedRowByValue(t, result, "index_name", "docs_email_key")
		requireFixedValue(t, result, unique, "non_unique", "f")
		primary := fixedRowByValue(t, result, "index_name", "docs_pkey")
		requireFixedValue(t, result, primary, "non_unique", "f")
		if uniqueOnly {
			seenUniqueOnly = true
			if len(result.rows) != 2 {
				t.Fatalf("unique-only JDBC rows = %d, want primary plus secondary unique", len(result.rows))
			}
			continue
		}
		seenAll = true
		nonunique := fixedRowByValue(t, result, "index_name", "docs_by_city")
		requireFixedValue(t, result, nonunique, "non_unique", "t")
	}
	if !seenAll || !seenUniqueOnly {
		t.Fatalf("JDBC index shapes: all=%t unique-only=%t", seenAll, seenUniqueOnly)
	}
}
