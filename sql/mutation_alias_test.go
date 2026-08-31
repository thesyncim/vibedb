package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestUpdateTargetAliasesBindEveryMutationProjection(t *testing.T) {
	const source = `UPDATE docs AS d SET total = d.base + 1 WHERE d.id = ? ORDER BY d.id LIMIT 1 RETURNING d.total`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	update := statement.Update
	if update == nil || update.Table != "docs" || update.Alias != "d" ||
		update.AliasPos != strings.Index(source, "d SET") {
		t.Fatalf("UPDATE target = %+v", update)
	}
	checkMutationAliasRef(t, update.Filter, "docs", "d")
	checkMutationAliasRef(t, update.Returning, "docs", "d")
	assignment := update.Assignments[0].Expr
	if assignment == nil || assignment.Left == nil || assignment.Left.Path == nil ||
		assignment.Left.Path.Source != 0 || assignment.Left.Path.Spec() != "base" {
		t.Fatalf("qualified assignment = %+v", assignment)
	}
	if update.Filter.Where == nil || update.Filter.Where.Path == nil ||
		update.Filter.Where.Path.Source != 0 || update.Filter.Where.Path.Spec() != "id" {
		t.Fatalf("qualified WHERE = %+v", update.Filter.Where)
	}
	if len(update.OrderBy) != 1 || update.OrderBy[0].Path == nil ||
		update.OrderBy[0].Path.Source != 0 || update.OrderBy[0].Path.Spec() != "id" {
		t.Fatalf("qualified ORDER BY = %+v", update.OrderBy)
	}
	if len(update.Returning.Columns) != 1 ||
		update.Returning.Columns[0].Path == nil ||
		update.Returning.Columns[0].Path.Source != 0 ||
		update.Returning.Columns[0].Path.Spec() != "total" {
		t.Fatalf("qualified RETURNING = %+v", update.Returning.Columns)
	}
}

func TestUpdateBareAndSameNameAliases(t *testing.T) {
	for _, source := range []string{
		`UPDATE docs d SET total = d.total + 1`,
		`UPDATE docs AS docs SET total = docs.total + 1`,
		`UPDATE "docs" AS "docs" SET total = "docs".total + 1`,
	} {
		statement, err := ParseStatement(source)
		if err != nil {
			t.Fatalf("ParseStatement(%q): %v", source, err)
		}
		update := statement.Update
		if update.Alias == "" || update.Filter.From[0].Alias != update.Alias ||
			!update.Filter.From[0].HasAlias {
			t.Fatalf("UPDATE alias for %q = %+v, filter=%+v",
				source, update, update.Filter.From)
		}
		path := update.Assignments[0].Expr.Left.Path
		if path == nil || path.Source != 0 || path.Spec() != "total" {
			t.Fatalf("UPDATE alias path for %q = %+v", source, path)
		}
	}
}

func TestInsertTargetAliasSeparatesExcludedTableNamespaces(t *testing.T) {
	const source = `INSERT INTO excluded AS existing (id, a) VALUES (?, ?)
		ON CONFLICT DO UPDATE SET a = existing.a + EXCLUDED.a
		RETURNING existing.a`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	insert := statement.Insert
	if insert == nil || insert.Table != "excluded" || insert.Alias != "existing" ||
		insert.AliasPos != strings.Index(source, "existing") {
		t.Fatalf("INSERT target = %+v", insert)
	}
	expression := insert.OnConflictUpdate.Assignments[0].Expr
	if expression == nil || expression.Left == nil || expression.Left.Path == nil ||
		expression.Left.Path.Source != 0 || expression.Left.Path.Spec() != "a" ||
		expression.Right == nil || expression.Right.Path == nil ||
		expression.Right.Path.Source != 1 || expression.Right.Path.Spec() != "a" {
		t.Fatalf("conflict namespaces = %+v", expression)
	}
	checkMutationAliasRef(t, insert.Returning, "excluded", "existing")
	if insert.Returning.Columns[0].Path.Source != 0 ||
		insert.Returning.Columns[0].Path.Spec() != "a" {
		t.Fatalf("INSERT RETURNING path = %+v", insert.Returning.Columns[0].Path)
	}
}

func TestQuotedUpperExcludedAliasRemainsDistinctFromCandidate(t *testing.T) {
	const source = `INSERT INTO excluded AS "EXCLUDED" (id, a) VALUES (?, ?)
		ON CONFLICT DO UPDATE SET a = "EXCLUDED".a + excluded.a`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	expression := statement.Insert.OnConflictUpdate.Assignments[0].Expr
	if expression.Left.Path.Source != 0 || expression.Right.Path.Source != 1 {
		t.Fatalf("quoted EXCLUDED namespaces = %+v", expression)
	}
}

func TestUnquotedMixedCaseExcludedAliasCollidesWithCandidate(t *testing.T) {
	const source = `INSERT INTO docs AS ExClUdEd VALUES (?)
		ON CONFLICT DO UPDATE SET a = EXCLUDED.a`
	_, err := ParseStatement(source)
	var ambiguous *AmbiguousAliasError
	if !errors.As(err, &ambiguous) ||
		ambiguous.Pos != strings.LastIndex(source, "EXCLUDED") {
		t.Fatalf("mixed-case EXCLUDED collision = %T %+v", err, ambiguous)
	}
}

func TestMutationAliasHidesPhysicalTargetName(t *testing.T) {
	tests := []struct {
		name   string
		source string
		last   bool
	}{
		{"update assignment", `UPDATE docs AS d SET a = docs.a`, false},
		{"update where", `UPDATE docs AS d SET a = 1 WHERE docs.id = 1`, false},
		{"update order", `UPDATE docs AS d SET a = 1 ORDER BY docs.id LIMIT 1`, false},
		{"update returning", `UPDATE docs AS d SET a = 1 RETURNING docs.a`, true},
		{"insert conflict", `INSERT INTO docs AS d VALUES (?) ON CONFLICT DO UPDATE SET a = docs.a`, true},
		{"insert returning", `INSERT INTO docs AS d VALUES (?) RETURNING docs.a`, true},
		{"quoted update target", `UPDATE "Docs" AS d SET a = "Docs".a`, false},
		{"quoted insert target", `INSERT INTO "Docs" AS d VALUES (?) RETURNING "Docs".a`, true},
		{"document-named update", `UPDATE "$doc" AS d SET a = "$doc".a`, false},
		{"document-named conflict", `INSERT INTO "$doc" AS d VALUES (?) ON CONFLICT DO UPDATE SET a = "$doc".a`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseStatement(test.source)
			var invalid *InvalidTableReferenceError
			if !errors.As(err, &invalid) {
				t.Fatalf("error = %T %v, want InvalidTableReferenceError", err, err)
			}
			marker := "docs."
			table := "docs"
			if strings.Contains(test.source, `"Docs"`) {
				marker, table = `"Docs".`, "Docs"
			} else if strings.Contains(test.source, `"$doc"`) {
				marker, table = `"$doc".`, "$doc"
			}
			want := strings.Index(test.source, marker)
			if test.last {
				want = strings.LastIndex(test.source, marker)
			}
			if invalid.Pos != want || invalid.Table != table ||
				invalid.Alias != "d" || invalid.SQLHint() == "" {
				t.Fatalf("invalid reference = %+v, want byte %d", invalid, want)
			}
		})
	}
}

func TestMutationAliasHidesPhysicalTargetInsidePredicateSubqueries(t *testing.T) {
	tests := []string{
		`UPDATE docs AS d SET a = 1 WHERE EXISTS (` +
			`SELECT 1 FROM other AS o WHERE docs.id = o.id)`,
		`UPDATE docs AS d SET a = 1 WHERE d.id IN (` +
			`SELECT o.id FROM other AS o WHERE docs.id = o.id)`,
		`UPDATE docs AS d SET a = 1 WHERE d.id = (` +
			`SELECT docs.id FROM other AS o LIMIT 1)`,
	}
	for _, source := range tests {
		_, err := ParseStatement(source)
		var invalid *InvalidTableReferenceError
		if !errors.As(err, &invalid) ||
			invalid.Pos != strings.LastIndex(source, "docs.id") ||
			invalid.Table != "docs" || invalid.Alias != "d" {
			t.Fatalf("hidden target in %q = %T %+v", source, err, invalid)
		}
	}

	const valid = `UPDATE docs AS d SET a = 1 WHERE EXISTS (` +
		`SELECT 1 FROM other AS o WHERE d.id = o.id)`
	if _, err := ParseStatement(valid); err != nil {
		t.Fatalf("aliased correlated reference rejected: %v", err)
	}
	for _, source := range []string{
		`UPDATE docs AS d SET a = 1 WHERE EXISTS (` +
			`SELECT 1 FROM other AS o WHERE o.docs.id = 1)`,
		`UPDATE docs AS d SET a = 1 WHERE EXISTS (` +
			`SELECT 1 FROM docs WHERE docs.docs.id = 1)`,
	} {
		if _, err := ParseStatement(source); err != nil {
			t.Fatalf("local subquery qualifier in %q rejected: %v", source, err)
		}
	}
}

func TestInsertRequiresASForTargetAliasAndQualifiedSetTargetStaysInvalid(t *testing.T) {
	_, err := ParseStatement(`INSERT INTO docs d VALUES (?)`)
	var parse *ParseError
	if !errors.As(err, &parse) || !strings.Contains(parse.Msg, "VALUES or SELECT") {
		t.Fatalf("bare INSERT alias error = %T %v", err, err)
	}

	for _, test := range []struct {
		source    string
		marker    string
		table     string
		qualifier string
	}{
		{`UPDATE docs AS d SET d.a = 1`, "d.a", "docs", "d"},
		{`UPDATE docs AS d SET docs.a = 1`, "docs.a", "docs", "docs"},
		{`UPDATE docs SET docs.a = 1`, "docs.a", "docs", "docs"},
		{`UPDATE docs AS d SET d."$doc" = 1`, `d."$doc"`, "docs", "d"},
		{`INSERT INTO docs AS d VALUES (?) ON CONFLICT DO UPDATE SET d.a = 1`, "d.a", "docs", "d"},
		{`INSERT INTO docs AS d VALUES (?) ON CONFLICT DO UPDATE SET EXCLUDED.a = 1`, "EXCLUDED.a", "docs", "EXCLUDED"},
	} {
		_, err = ParseStatement(test.source)
		var qualified *QualifiedAssignmentTargetError
		if !errors.As(err, &qualified) ||
			qualified.Pos != strings.Index(test.source, test.marker) ||
			qualified.Table != test.table ||
			qualified.Qualifier != test.qualifier || qualified.SQLHint() == "" {
			t.Fatalf("qualified SET target in %q = %T %+v", test.source, err, qualified)
		}
	}

	_, err = ParseStatement(`UPDATE docs SET "$doc".a = 1`)
	var qualified *QualifiedAssignmentTargetError
	if !errors.As(err, &parse) || errors.As(err, &qualified) ||
		!strings.Contains(parse.Msg, "expected '='") {
		t.Fatalf("document-root SET target error = %T %v", err, err)
	}

	_, err = ParseStatement(`UPDATE docs AS "$doc" SET a = 1`)
	var unsupported *FeatureNotSupportedError
	if !errors.As(err, &unsupported) ||
		!strings.Contains(unsupported.Msg, "mutation target alias") {
		t.Fatalf("reserved mutation alias error = %T %v", err, err)
	}
}

func TestInsertTargetAliasDoesNotLeakIntoIndependentSource(t *testing.T) {
	statement, err := ParseStatement(
		`INSERT INTO docs AS d SELECT docs.id FROM other RETURNING d.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	path := statement.Insert.Source.Columns[0].Path
	if path == nil || path.Source != 0 || path.Spec() != "docs.id" ||
		statement.Insert.Source.From[0].Name != "other" ||
		statement.Insert.Returning.Columns[0].Path.Spec() != "id" {
		t.Fatalf("independent INSERT source = %+v / %+v",
			path, statement.Insert.Source.From)
	}
}

func TestWholeDocumentConflictAliasDisambiguatesExcludedTarget(t *testing.T) {
	const accepted = `INSERT INTO excluded AS target VALUES (?) ` +
		`ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc"`
	statement, err := ParseStatement(accepted)
	if err != nil || statement.Insert.OnConflictUpdate == nil ||
		!statement.Insert.OnConflictUpdate.WholeDocument() {
		t.Fatalf("aliased whole-document conflict = %+v / %v", statement, err)
	}

	const ambiguous = `INSERT INTO excluded VALUES (?) ` +
		`ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc"`
	_, err = ParseStatement(ambiguous)
	var alias *AmbiguousAliasError
	if !errors.As(err, &alias) ||
		alias.Pos != strings.LastIndex(ambiguous, "EXCLUDED") {
		t.Fatalf("unaliased whole-document collision = %T %+v", err, alias)
	}
}

func TestMutationAliasesResetAcrossParserReuse(t *testing.T) {
	var parser Parser
	var statement Statement
	for _, source := range []string{
		`UPDATE docs AS d SET a = d.a`,
		`UPDATE docs SET a = a`,
		`INSERT INTO docs AS d VALUES (?) RETURNING d.a`,
		`INSERT INTO docs VALUES (?) RETURNING a`,
	} {
		if err := parser.ParseStatement(&statement, source); err != nil {
			t.Fatalf("ParseStatement(%q): %v", source, err)
		}
	}
	if statement.Insert.Alias != "" || statement.Insert.AliasPos != 0 ||
		statement.Insert.Returning.From[0].Alias != "docs" ||
		statement.Insert.Returning.From[0].HasAlias {
		t.Fatalf("reused INSERT retained alias state: %+v, from=%+v",
			statement.Insert, statement.Insert.Returning.From)
	}
}

func checkMutationAliasRef(t *testing.T, statement *SelectStmt, table, alias string) {
	t.Helper()
	if statement == nil || len(statement.From) != 1 {
		t.Fatalf("mutation projection = %+v", statement)
	}
	ref := statement.From[0]
	if ref.Name != table || ref.Alias != alias || !ref.HasAlias {
		t.Fatalf("mutation target ref = %+v, want %s AS %s", ref, table, alias)
	}
}
