package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestCTEASTMultipleDefinitionsHintsAliasesAndPlaceholders(t *testing.T) {
	const src = `WITH active(id, name) AS MATERIALIZED (
		SELECT user_id AS id, display AS name FROM users WHERE enabled = ?
	), selected AS NOT MATERIALIZED (
		SELECT id FROM active WHERE name = ?
	)
	SELECT id FROM selected WHERE id = ?`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.With == nil || len(stmt.With.CTEs) != 2 {
		t.Fatalf("WITH = %+v, want two definitions", stmt.With)
	}
	active := &stmt.With.CTEs[0]
	selected := &stmt.With.CTEs[1]
	if active.Name != "active" || active.Materialization != CTEMaterialized ||
		active.HintPos != strings.Index(src, "MATERIALIZED") {
		t.Fatalf("active definition = %+v", active)
	}
	if got, want := strings.Join(active.Columns, ","), "id,name"; got != want {
		t.Fatalf("active aliases = %q, want %q", got, want)
	}
	if len(active.ColumnPos) != 2 ||
		active.ColumnPos[0] != strings.Index(src, "id, name") ||
		active.ColumnPos[1] != strings.Index(src, "name) AS") {
		t.Fatalf("active alias positions = %v", active.ColumnPos)
	}
	if selected.Materialization != CTENotMaterialized ||
		selected.HintPos != strings.Index(src, "NOT MATERIALIZED") {
		t.Fatalf("selected definition = %+v", selected)
	}
	if active.Query.ParamBase != 0 || active.Query.Params != 1 ||
		selected.Query.ParamBase != 1 || selected.Query.Params != 1 || stmt.Params != 3 {
		t.Fatalf("placeholder ranges = active %d/%d selected %d/%d total %d",
			active.Query.ParamBase, active.Query.Params,
			selected.Query.ParamBase, selected.Query.Params, stmt.Params)
	}
	if ref := &selected.Query.From[0]; ref.Kind != RelationCTE || ref.Name != "active" || ref.Query != active.Query {
		t.Fatalf("earlier sibling reference = %+v, want active definition identity", ref)
	}
	if ref := &stmt.From[0]; ref.Kind != RelationCTE || ref.Name != "selected" || ref.Query != selected.Query {
		t.Fatalf("primary reference = %+v, want selected definition identity", ref)
	}
	marks := positionsOf(src, '?')
	if active.Query.Where.Value.Ordinal != 0 || active.Query.Where.Value.Pos != marks[0] ||
		selected.Query.Where.Value.Ordinal != 0 || selected.Query.Where.Value.Pos != marks[1] ||
		stmt.Where.Value.Ordinal != 2 || stmt.Where.Value.Pos != marks[2] {
		t.Fatalf("placeholder ordinals or positions were not stable")
	}
}

func TestCTELexicalVisibilityAndNestedShadowing(t *testing.T) {
	const src = `WITH outer_cte AS (SELECT id FROM physical),
		chain AS (SELECT id FROM outer_cte),
		wrapper AS (
			WITH outer_cte AS (SELECT local_id AS id FROM local_docs)
			SELECT id FROM outer_cte
		)
	SELECT id FROM outer_cte`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	defs := stmt.With.CTEs
	if got := defs[1].Query.From[0]; got.Kind != RelationCTE || got.Query != defs[0].Query {
		t.Fatalf("later sibling did not bind earlier sibling: %+v", got)
	}
	local := defs[2].Query.With.CTEs[0].Query
	if got := defs[2].Query.From[0]; got.Kind != RelationCTE || got.Query != local || got.Query == defs[0].Query {
		t.Fatalf("inner WITH did not shadow outer definition: %+v", got)
	}
	if got := stmt.From[0]; got.Kind != RelationCTE || got.Query != defs[0].Query {
		t.Fatalf("main query did not bind outer definition: %+v", got)
	}
}

func TestCTEScopeFlowsIntoPredicateSubqueries(t *testing.T) {
	const src = `WITH visible AS (SELECT id FROM allowed) ` +
		`SELECT id FROM docs WHERE id IN (` +
		`WITH local_filter AS (SELECT id FROM visible) SELECT id FROM local_filter)`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	visible := stmt.With.CTEs[0].Query
	subquery := stmt.Where.Subquery
	if subquery == nil || subquery.With == nil {
		t.Fatalf("predicate subquery lost WITH: %+v", subquery)
	}
	if got := subquery.With.CTEs[0].Query.From[0]; got.Kind != RelationCTE || got.Query != visible {
		t.Fatalf("nested predicate did not inherit outer CTE scope: %+v", got)
	}
	local := subquery.With.CTEs[0].Query
	if got := subquery.From[0]; got.Kind != RelationCTE || got.Query != local {
		t.Fatalf("predicate primary query did not bind local CTE: %+v", got)
	}
}

func TestCTESelfAndForwardCandidatesPreservePhysicalResolution(t *testing.T) {
	const src = `WITH users AS (SELECT id FROM users),
		first AS (SELECT id FROM later),
		later AS (SELECT id FROM docs)
	SELECT id FROM first`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	defs := stmt.With.CTEs
	self := defs[0].Query.From[0]
	if self.Kind != RelationCollection || self.Name != "users" ||
		self.UnresolvedCTE.Kind != CTEReferenceSelf ||
		self.UnresolvedCTE.DefinitionPos != defs[0].Pos {
		t.Fatalf("physical/self candidate = %+v", self)
	}
	forward := defs[1].Query.From[0]
	if forward.Kind != RelationCollection || forward.Name != "later" ||
		forward.UnresolvedCTE.Kind != CTEReferenceForward ||
		forward.UnresolvedCTE.DefinitionPos != defs[2].Pos {
		t.Fatalf("physical/forward candidate = %+v", forward)
	}
	if got := defs[2].Query.From[0].UnresolvedCTE.Kind; got != CTEReferenceNone {
		t.Fatalf("ordinary physical collection carries CTE metadata %d", got)
	}
}

func TestNestedWITHDeferredMetadataKeepsNearestScope(t *testing.T) {
	const src = `WITH holder AS (
		WITH future_inner AS (SELECT id FROM future_inner),
			before AS (SELECT id FROM after),
			after AS (SELECT id FROM local_docs)
		SELECT id FROM before
	), future_inner AS (SELECT id FROM outer_inner),
	   after AS (SELECT id FROM outer_after)
	SELECT id FROM holder`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	local := stmt.With.CTEs[0].Query.With.CTEs
	self := local[0].Query.From[0].UnresolvedCTE
	if self.Kind != CTEReferenceSelf || self.DefinitionPos != local[0].Pos {
		t.Fatalf("inner self metadata was overwritten by outer scope: %+v", self)
	}
	forward := local[1].Query.From[0].UnresolvedCTE
	if forward.Kind != CTEReferenceForward || forward.DefinitionPos != local[2].Pos {
		t.Fatalf("inner forward metadata was overwritten by outer scope: %+v", forward)
	}
}

func TestCTEColumnAliasArityKnownAndDeferred(t *testing.T) {
	const excessive = `WITH c(a, b, extra) AS (SELECT x, y FROM docs) SELECT a FROM c`
	_, err := Parse(excessive)
	var arity *CTEColumnAliasArityError
	if !errors.As(err, &arity) {
		t.Fatalf("excess alias error = %T %v, want *CTEColumnAliasArityError", err, err)
	}
	if arity.Name != "c" || arity.Aliases != 3 || arity.Outputs != 2 ||
		arity.Pos != strings.Index(excessive, "extra") {
		t.Fatalf("arity error = %+v", arity)
	}

	const wildcard = `WITH c(a, b, possible_runtime_column) AS (SELECT * FROM docs) SELECT * FROM c`
	stmt, err := Parse(wildcard)
	if err != nil {
		t.Fatal(err)
	}
	cte := &stmt.With.CTEs[0]
	if !cte.ColumnArityDeferred || len(cte.Columns) != 3 || len(cte.ColumnPos) != 3 {
		t.Fatalf("wildcard alias arity was not deferred safely: %+v", cte)
	}

	const shorter = `WITH c(a) AS (SELECT x, y FROM docs) SELECT a FROM c`
	if _, err := Parse(shorter); err != nil {
		t.Fatalf("shorter alias list is valid: %v", err)
	}
}

func TestCTENestedUTF8PositionsAndDefinitionIdentityShiftOnce(t *testing.T) {
	const src = `SELECT d.id FROM (/* préfix */ WITH "名" AS MATERIALIZED (
		SELECT b.id FROM base AS b
	) SELECT "名".id FROM "名") AS d`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	nested := stmt.From[0].Query
	cte := &nested.With.CTEs[0]
	ref := &nested.From[0]
	if cte.Pos != strings.Index(src, `"名"`) ||
		cte.HintPos != strings.Index(src, "MATERIALIZED") ||
		cte.Query.Columns[0].Pos != strings.Index(src, "b.id") ||
		cte.Query.From[0].Pos != strings.Index(src, "base AS b") {
		t.Fatalf("nested absolute positions were shifted incorrectly: cte=%+v query=%+v", cte, cte.Query)
	}
	if ref.Kind != RelationCTE || ref.Query != cte.Query {
		t.Fatalf("nested CTE reference lost stable definition identity: %+v", ref)
	}
	if ref.Pos != strings.LastIndex(src, `"名"`) {
		t.Fatalf("nested CTE reference Pos = %d, want %d", ref.Pos, strings.LastIndex(src, `"名"`))
	}
}

func TestCTEParserReuseDoesNotLeakOuterScope(t *testing.T) {
	var parser Parser
	var stmt SelectStmt
	const withScope = `WITH visible AS (SELECT id FROM docs) ` +
		`SELECT d.id FROM (SELECT id FROM visible) AS d`
	if err := parser.Parse(&stmt, withScope); err != nil {
		t.Fatal(err)
	}
	if got := stmt.From[0].Query.From[0].Kind; got != RelationCTE {
		t.Fatalf("nested relation kind = %d, want CTE", got)
	}
	const withoutScope = `SELECT d.id FROM (SELECT id FROM visible) AS d`
	if err := parser.Parse(&stmt, withoutScope); err != nil {
		t.Fatal(err)
	}
	ref := stmt.From[0].Query.From[0]
	if stmt.With != nil || ref.Kind != RelationCollection ||
		ref.UnresolvedCTE.Kind != CTEReferenceNone || ref.Query != nil {
		t.Fatalf("reused child parser leaked outer CTE scope: stmt=%+v ref=%+v", stmt.With, ref)
	}
}

func TestCTEUnsupportedBoundariesStayTypedAndRebased(t *testing.T) {
	for _, tc := range []struct {
		name   string
		src    string
		marker string
	}{
		{"recursive", `WITH RECURSIVE c AS (SELECT id FROM docs) SELECT id FROM c`, "RECURSIVE"},
		{"insert body", `WITH c AS (INSERT INTO docs VALUES (?)) SELECT id FROM c`, "INSERT"},
		{"update body", `WITH c AS (UPDATE docs SET "$doc" = ?) SELECT id FROM c`, "UPDATE"},
		{"delete primary", `WITH c AS (SELECT id FROM docs) DELETE FROM docs`, "DELETE"},
		{"nested recursive UTF8", `SELECT d.id FROM (/* é */ WITH RECURSIVE c AS (SELECT id FROM docs) SELECT id FROM c) d`, "RECURSIVE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.src)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			if unsupported.Pos != strings.Index(tc.src, tc.marker) {
				t.Fatalf("error Pos = %d, want %d", unsupported.Pos, strings.Index(tc.src, tc.marker))
			}
		})
	}
}

func TestCTEJoinOperandsPreserveDefinitionIdentity(t *testing.T) {
	for _, src := range []string{
		`WITH c AS (SELECT id FROM docs) SELECT c.id FROM c JOIN other o ON c.id = o.id`,
		`WITH c AS (SELECT id FROM docs) SELECT c.id FROM other o JOIN c ON o.id = c.id`,
	} {
		stmt, err := Parse(src)
		if err != nil {
			t.Fatalf("Parse(%q): %v", src, err)
		}
		definition := stmt.With.CTEs[0].Query
		found := false
		for i := range stmt.From {
			if stmt.From[i].Kind == RelationCTE {
				found = true
				if stmt.From[i].Query != definition {
					t.Fatalf("Parse(%q) CTE identity = %p, want definition %p", src, stmt.From[i].Query, definition)
				}
			}
		}
		if !found {
			t.Fatalf("Parse(%q) did not retain a CTE operand", src)
		}
	}
}

func TestCTESyntaxErrorsArePrecise(t *testing.T) {
	runRejections(t, []rejection{
		{"missing definition", `WITH`, 4, "expected a common table expression name"},
		{"missing AS", `WITH c (id) (SELECT id FROM docs) SELECT id FROM c`, 12, "expected AS"},
		{"missing body parenthesis", `WITH c AS SELECT id FROM docs SELECT id FROM c`, 10, "expected '('"},
		{"empty body", `WITH c AS () SELECT id FROM c`, 11, "expected SELECT"},
		{"empty alias list", `WITH c() AS (SELECT id FROM docs) SELECT id FROM c`, 7, "expected a column name"},
		{"trailing alias comma", `WITH c(id,) AS (SELECT id FROM docs) SELECT id FROM c`, 10, "expected a column name"},
		{"missing alias comma", `WITH c(id name) AS (SELECT id FROM docs) SELECT id FROM c`, 10, "expected ',' or ')'"},
		{"NOT without MATERIALIZED", `WITH c AS NOT (SELECT id FROM docs) SELECT id FROM c`, 14, "expected MATERIALIZED"},
		{"trailing CTE comma", `WITH c AS (SELECT id FROM docs), SELECT id FROM c`, 33, "expected a common table expression name"},
	})
}

func TestCTEDuplicateNameTypeSurvivesNestedRebase(t *testing.T) {
	const src = `SELECT d.id FROM (/* é */ WITH c AS (SELECT id FROM docs), c AS (SELECT id FROM other) SELECT id FROM c) d`
	_, err := Parse(src)
	var duplicate *DuplicateCTEError
	if !errors.As(err, &duplicate) {
		t.Fatalf("error = %T %v, want *DuplicateCTEError", err, err)
	}
	first := strings.Index(src, "c AS")
	second := first + len("c AS") + strings.Index(src[first+len("c AS"):], "c AS")
	if duplicate.Name != "c" || duplicate.Pos != second || duplicate.FirstPos != first {
		t.Fatalf("duplicate error = %+v", duplicate)
	}
}

func TestCTETypedErrorNamesOutliveParserReuse(t *testing.T) {
	var parser Parser
	var stmt SelectStmt
	err := parser.Parse(&stmt, `WITH stable_name AS (SELECT id FROM docs), stable_name AS (SELECT id FROM other) SELECT id FROM stable_name`)
	var duplicate *DuplicateCTEError
	if !errors.As(err, &duplicate) {
		t.Fatalf("error = %T %v, want *DuplicateCTEError", err, err)
	}
	if err := parser.Parse(&stmt, `SELECT a_very_different_identifier FROM another_collection`); err != nil {
		t.Fatal(err)
	}
	if duplicate.Name != "stable_name" {
		t.Fatalf("retained typed error name = %q after parser reuse", duplicate.Name)
	}
}

func TestCTEAbsentLeavesNoFrontendState(t *testing.T) {
	var parser Parser
	var stmt SelectStmt
	if err := parser.Parse(&stmt, `SELECT id FROM docs`); err != nil {
		t.Fatal(err)
	}
	if stmt.With != nil || parser.with.CTEs != nil || parser.activeCTEs.defs != nil ||
		len(parser.ctes.chunks) != 0 || len(parser.names.chunks) != 0 || len(parser.ints.chunks) != 0 ||
		parser.nested != nil {
		t.Fatalf("plain SELECT initialized CTE state: with=%+v scope=%+v arenas=%d/%d/%d nested=%v",
			stmt.With, parser.activeCTEs, len(parser.ctes.chunks), len(parser.names.chunks), len(parser.ints.chunks), parser.nested != nil)
	}
}

func TestParseStatementAndExplainAcceptCTESelect(t *testing.T) {
	for _, src := range []string{
		`WITH c AS (SELECT id FROM docs) SELECT id FROM c`,
		`EXPLAIN WITH c AS (SELECT id FROM docs) SELECT id FROM c`,
		`EXPLAIN ANALYZE WITH c AS (SELECT id FROM docs) SELECT id FROM c`,
	} {
		stmt, err := ParseStatement(src)
		if err != nil {
			t.Fatalf("ParseStatement(%q): %v", src, err)
		}
		if stmt.Kind != KindSelect || stmt.Select == nil || stmt.Select.With == nil {
			t.Fatalf("ParseStatement(%q) = %+v", src, stmt)
		}
	}
}
