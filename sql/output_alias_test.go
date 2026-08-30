package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestPostgreSQLBareOutputAliasMatchesASAST(t *testing.T) {
	for _, test := range []struct {
		name     string
		bare     string
		explicit string
	}{
		{"path", `SELECT id value FROM docs`, `SELECT id AS value FROM docs`},
		{"scalar", `SELECT id + 1 value FROM docs`, `SELECT id + 1 AS value FROM docs`},
		{"cast", `SELECT id::text value FROM docs`, `SELECT id::text AS value FROM docs`},
		{"aggregate", `SELECT COUNT(*) value FROM docs`, `SELECT COUNT(*) AS value FROM docs`},
		{"window", `SELECT ROW_NUMBER() OVER () value FROM docs`, `SELECT ROW_NUMBER() OVER () AS value FROM docs`},
		{"qualified star", `SELECT d.* value FROM docs d`, `SELECT d.* AS value FROM docs d`},
		{
			"CTE output",
			`WITH q AS (SELECT id value FROM docs) SELECT value FROM q`,
			`WITH q AS (SELECT id AS value FROM docs) SELECT value FROM q`,
		},
		{
			"set operands",
			`SELECT id value    FROM a UNION ALL SELECT id value    FROM b`,
			`SELECT id AS value FROM a UNION ALL SELECT id AS value FROM b`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bare, err := Parse(test.bare)
			if err != nil {
				t.Fatal(err)
			}
			explicit, err := Parse(test.explicit)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := dumpStmt(bare), dumpStmt(explicit); got != want {
				t.Fatalf("bare alias AST differs from AS:\n got %s\nwant %s", got, want)
			}
		})
	}

	bare, err := ParseStatement(`DELETE FROM docs RETURNING id value`)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := ParseStatement(`DELETE FROM docs RETURNING id AS value`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dumpAny(bare), dumpAny(explicit); got != want {
		t.Fatalf("bare RETURNING alias AST differs from AS:\n got %s\nwant %s", got, want)
	}
}

func TestPostgreSQLBareOutputAliasesAcrossExpressionKinds(t *testing.T) {
	statement, err := Parse(`SELECT id plain, amount "Quoted",
		ROW_NUMBER() OVER () rn, 1 select FROM docs ORDER BY plain`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"plain", "Quoted", "rn", "select"}
	if len(statement.Columns) != len(want) {
		t.Fatalf("columns = %d, want %d", len(statement.Columns), len(want))
	}
	for i := range want {
		if statement.Columns[i].Alias != want[i] {
			t.Fatalf("column %d alias = %q, want %q", i, statement.Columns[i].Alias, want[i])
		}
	}
	if len(statement.OrderBy) != 1 || statement.OrderBy[0].Path != statement.Columns[0].Path {
		t.Fatalf("ORDER BY bare alias did not resolve to its projection: %+v", statement.OrderBy)
	}
	aggregate, err := Parse(`SELECT COUNT(*) count FROM docs`)
	if err != nil || aggregate.Columns[0].Alias != "count" {
		t.Fatalf("aggregate bare alias = %q, error %v", aggregate.Columns[0].Alias, err)
	}
}

func TestPostgreSQLBareOutputAliasKeywordBoundary(t *testing.T) {
	// This is the complete AS_LABEL set from PostgreSQL 18.6 kwlist.h. These
	// unquoted spellings require AS even when VibeDB does not otherwise tokenize
	// them as keywords.
	const asLabels = `ARRAY AS CHAR CHARACTER CREATE DAY EXCEPT FETCH FILTER FOR FROM GRANT GROUP HAVING HOUR INTERSECT INTO ISNULL LIMIT MINUTE MONTH NOTNULL OFFSET ON ORDER OVER OVERLAPS PRECISION RETURNING SECOND TO UNION VARYING WHERE WINDOW WITH WITHIN WITHOUT YEAR`
	for _, label := range strings.Fields(asLabels) {
		if !postgresAliasRequiresAS(strings.ToLower(label)) ||
			!postgresAliasRequiresAS(label) {
			t.Errorf("%s was not classified as PostgreSQL AS_LABEL", label)
		}
	}
	for _, label := range []string{"plain", "select", "and", "null", "count", "missing", "row_number", "café"} {
		if postgresAliasRequiresAS(label) {
			t.Errorf("%s was not classified as PostgreSQL BareColLabel", label)
		}
	}
	for _, label := range []string{"and", "null", "select"} {
		statement, err := Parse(`SELECT 1 ` + label)
		if err != nil {
			t.Errorf("bare PostgreSQL keyword %q: %v", label, err)
			continue
		}
		if statement.Columns[0].Alias != label {
			t.Errorf("bare PostgreSQL keyword %q = alias %q", label, statement.Columns[0].Alias)
		}
	}

	for _, label := range []string{"day", "filter", "within"} {
		if _, err := Parse(`SELECT 1 ` + label); err == nil {
			t.Errorf("unquoted PostgreSQL AS_LABEL %q was accepted as a bare alias", label)
		}
	}
	statement, err := Parse(`SELECT 1 AS day`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Columns[0].Alias != "day" {
		t.Fatalf("explicit AS label = alias %q", statement.Columns[0].Alias)
	}
	quoted, err := Parse(`SELECT 1 "from"`)
	if err != nil {
		t.Fatal(err)
	}
	if quoted.Columns[0].Alias != "from" {
		t.Fatalf("quoted bare label = alias %q", quoted.Columns[0].Alias)
	}
}

func TestPostgreSQLBareOutputAliasDuplicateRemainsAmbiguous(t *testing.T) {
	const source = `SELECT a score, b score FROM docs ORDER BY score`
	_, err := Parse(source)
	var ambiguous *AmbiguousOutputError
	if !errors.As(err, &ambiguous) || ambiguous.Name != "score" ||
		ambiguous.Pos != strings.LastIndex(source, "score") {
		t.Fatalf("duplicate bare alias error = %T %+v", err, err)
	}
}

func TestPostgreSQLStandaloneStarCannotCarryOutputAlias(t *testing.T) {
	for _, source := range []string{
		`SELECT * alias FROM docs`,
		`SELECT * AS alias FROM docs`,
		`SELECT * "alias" FROM docs`,
	} {
		if _, err := Parse(source); err == nil ||
			!strings.Contains(err.Error(), "standalone '*'") {
			t.Fatalf("Parse(%q) = %v, want a standalone-star alias rejection", source, err)
		}
	}
	statement, err := Parse(`SELECT d.* payload FROM docs d`)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Columns[0].Alias; got != "payload" {
		t.Fatalf("qualified-star bare alias = %q, want payload", got)
	}
}

func TestPostgreSQLBareOutputAliasStillDetectsMissingComma(t *testing.T) {
	const source = `SELECT 1 first 2`
	_, err := Parse(source)
	if err == nil {
		t.Fatal("a second path after a bare alias was accepted without a comma")
	}
	var positioned *ParseError
	if !errors.As(err, &positioned) {
		t.Fatalf("error = %T %v, want ParseError", err, err)
	}
	if positioned.Pos != strings.LastIndex(source, "2") {
		t.Fatalf("error position = %d, want %d", positioned.Pos, strings.LastIndex(source, "2"))
	}
}
