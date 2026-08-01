package sql

import (
	"errors"
	"strings"
	"testing"
	"unsafe"
)

const lateralAllocSQL = `SELECT a.id, d.id FROM accounts a LEFT JOIN LATERAL (` +
	`SELECT i.id FROM items i WHERE i.owner = a.id AND a.region = ?` +
	`) d ON TRUE WHERE d.id = ?`

func TestLateralCrossBindsStablePrecedingSourcesAndPlaceholders(t *testing.T) {
	const src = `SELECT a.id, r.id, d.item_id
		FROM accounts a
		INNER JOIN regions r ON a.region = r.id
		CROSS JOIN LATERAL (
			SELECT i.id AS item_id, a.id AS account_id, r.id AS region_id
			FROM items i
			WHERE i.account_id = a.id AND i.region_id = r.id AND a.id = ?
		) d
		WHERE d.item_id = ?`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Params != 2 || len(stmt.From) != 3 {
		t.Fatalf("statement shape = params %d, FROM %d", stmt.Params, len(stmt.From))
	}
	ref := &stmt.From[2]
	if ref.Kind != RelationDerived || ref.Join != JoinCross || ref.Lateral == nil ||
		ref.Lateral.Decorrelated || ref.On != nil || ref.Query == nil {
		t.Fatalf("CROSS JOIN LATERAL relation = %+v", ref)
	}
	if ref.Lateral.Pos != strings.Index(src, "LATERAL") || ref.Pos != strings.Index(src, "(\n") {
		t.Fatalf("LATERAL positions = keyword %d, relation %d", ref.Lateral.Pos, ref.Pos)
	}
	if got := ref.Lateral.Bindings; len(got) != 2 || cap(got) != len(got) ||
		got[0].Depth != 1 || got[0].Source != 0 || lateralBindingSpec(&got[0]) != "id" ||
		got[1].Depth != 1 || got[1].Source != 1 || lateralBindingSpec(&got[1]) != "id" {
		t.Fatalf("LATERAL bindings = %+v", got)
	}
	if got := len(ref.Lateral.References); got != 5 {
		t.Fatalf("LATERAL references = %d, want every one of 5 correlated occurrences", got)
	}
	query := ref.Query
	if query.ParamBase != 0 || query.Params != 1 ||
		stmt.Where.Value.Ordinal != 1 {
		t.Fatalf("placeholder ranges = child %d+%d, outer ordinal %d",
			query.ParamBase, query.Params, stmt.Where.Value.Ordinal)
	}
	if _, ok := lateralReferenceBinding(ref.Lateral, query.Columns[0].Path); ok ||
		query.Columns[0].Path.Source != 0 ||
		!hasLateralReference(ref.Lateral, query.Columns[1].Path, 0) ||
		!hasLateralReference(ref.Lateral, query.Columns[2].Path, 1) {
		t.Fatalf("projection bindings = %+v", query.Columns)
	}
	where := query.Where
	if where == nil || where.Kind != ExprAnd || len(where.Kids) != 3 ||
		where.Kids[0].RightPath == nil ||
		!hasLateralReference(ref.Lateral, where.Kids[0].RightPath, 0) ||
		where.Kids[1].RightPath == nil ||
		!hasLateralReference(ref.Lateral, where.Kids[1].RightPath, 1) ||
		!hasLateralReference(ref.Lateral, where.Kids[2].Path, 0) ||
		where.Kids[2].Value.Kind != OperandParam {
		t.Fatalf("correlated WHERE = %+v", where)
	}
	dump := dumpStmt(stmt)
	for _, fragment := range []string{
		"lateral[1=depth1/source0:id,2=depth1/source1:id;refs=1@0:id,2@1:id",
		"path(0:id) as account_id",
		"(cmp = 0:account_id 0:id)",
	} {
		if !strings.Contains(dump, fragment) {
			t.Fatalf("dump %q lacks %q", dump, fragment)
		}
	}
}

func TestLateralInnerLeftAndDecorrelatedJoinMetadata(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		join JoinKind
	}{
		{
			name: "inner",
			sql: `SELECT a.id, d.id FROM accounts a JOIN LATERAL (` +
				`SELECT i.id FROM items i WHERE i.owner = a.id) d ON d.id = a.id`,
			join: JoinInner,
		},
		{
			name: "left",
			sql: `SELECT a.id, d.id FROM accounts a LEFT OUTER JOIN LATERAL (` +
				`SELECT i.id FROM items i WHERE i.owner = a.id) d ON TRUE`,
			join: JoinLeft,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := Parse(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			ref := &stmt.From[1]
			if ref.Join != test.join || ref.Lateral == nil || ref.Lateral.Decorrelated ||
				len(ref.Lateral.Bindings) != 1 || ref.On == nil {
				t.Fatalf("%s JOIN LATERAL = %+v", test.name, ref)
			}
		})
	}

	for _, join := range []string{"CROSS", "RIGHT", "FULL"} {
		suffix := ""
		if join != "CROSS" {
			suffix = " ON a.id = d.id"
		}
		src := `SELECT a.id, d.id FROM accounts a ` + join +
			` JOIN LATERAL (SELECT i.id FROM items i) d` + suffix
		stmt, err := Parse(src)
		if err != nil {
			t.Fatalf("decorrelated %s: %v", join, err)
		}
		ref := &stmt.From[1]
		if ref.Lateral == nil || !ref.Lateral.Decorrelated ||
			len(ref.Lateral.Bindings) != 0 || len(ref.Lateral.References) != 0 {
			t.Fatalf("decorrelated %s metadata = %+v", join, ref)
		}
	}
}

func TestLateralNestedPositionsAndCTEShadowing(t *testing.T) {
	const src = `SELECT q.item_id FROM (
		SELECT d.item_id FROM "é" AS a
		LEFT JOIN LATERAL (
			SELECT a.id AS item_id FROM items i WHERE i.owner = a.id AND i.tag = ?
		) d ON TRUE
	) q WHERE q.item_id = ?`
	stmt, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	nested := stmt.From[0].Query
	ref := &nested.From[1]
	firstOuter := strings.Index(src, "a.id AS item_id")
	if ref.Lateral == nil || ref.Lateral.Pos != strings.Index(src, "LATERAL") ||
		len(ref.Lateral.Bindings) != 1 || ref.Lateral.Bindings[0].Pos != firstOuter ||
		ref.Query.Columns[0].Path.Pos != firstOuter {
		t.Fatalf("nested LATERAL positions/binding = %+v, column=%+v",
			ref, ref.Query.Columns[0].Path)
	}
	if stmt.Params != 2 || nested.Params != 1 || nested.ParamBase != 0 ||
		ref.Query.Params != 1 || ref.Query.ParamBase != 0 || stmt.Where.Value.Ordinal != 1 {
		t.Fatalf("nested placeholder ranges = outer %d, nested %d+%d, lateral %d+%d, final %d",
			stmt.Params, nested.ParamBase, nested.Params,
			ref.Query.ParamBase, ref.Query.Params, stmt.Where.Value.Ordinal)
	}

	shadowed, err := Parse(`
		SELECT a.id, d.id FROM accounts a CROSS JOIN LATERAL (
			WITH a AS (SELECT id FROM shadow)
			SELECT a.id FROM a WHERE a.id = ?
		) d`)
	if err != nil {
		t.Fatal(err)
	}
	shadowRef := &shadowed.From[1]
	if shadowRef.Lateral == nil || !shadowRef.Lateral.Decorrelated ||
		len(shadowRef.Lateral.Bindings) != 0 || len(shadowRef.Lateral.References) != 0 ||
		shadowRef.Query.From[0].Kind != RelationCTE ||
		hasAnyLateralReference(shadowRef.Lateral, shadowRef.Query.Columns[0].Path) {
		t.Fatalf("local CTE did not shadow outer range variable: %+v", shadowRef)
	}

	localAlias, err := Parse(`
		SELECT a.id, d.id FROM accounts a CROSS JOIN LATERAL (
			SELECT a.id FROM archived_accounts a WHERE a.active = TRUE
		) d`)
	if err != nil {
		t.Fatal(err)
	}
	aliasRef := &localAlias.From[1]
	if aliasRef.Lateral == nil || !aliasRef.Lateral.Decorrelated ||
		len(aliasRef.Lateral.Bindings) != 0 || len(aliasRef.Lateral.References) != 0 ||
		aliasRef.Query.Columns[0].Path.Source != 0 {
		t.Fatalf("local range alias did not shadow outer range variable: %+v", aliasRef)
	}
}

func TestNestedLateralScopeDepthPropagatesTransitiveDependency(t *testing.T) {
	stmt, err := Parse(`
		SELECT a.id, q.id FROM accounts a CROSS JOIN LATERAL (
			SELECT d.id FROM local_rows l CROSS JOIN LATERAL (
				SELECT i.id FROM items i
				WHERE i.account = a.id AND i.local_id = l.id
			) d
		) q`)
	if err != nil {
		t.Fatal(err)
	}
	outer := &stmt.From[1]
	if outer.Lateral == nil || outer.Lateral.Decorrelated || len(outer.Lateral.Bindings) != 1 ||
		outer.Lateral.Bindings[0].Depth != 1 || outer.Lateral.Bindings[0].Source != 0 ||
		lateralBindingSpec(&outer.Lateral.Bindings[0]) != "id" ||
		len(outer.Lateral.References) != 0 {
		t.Fatalf("outer transitive dependency = %+v", outer)
	}
	inner := &outer.Query.From[1]
	if inner.Lateral == nil || inner.Lateral.Decorrelated || len(inner.Lateral.Bindings) != 2 ||
		inner.Lateral.Bindings[0].Depth != 2 || inner.Lateral.Bindings[0].Source != 0 ||
		inner.Lateral.Bindings[1].Depth != 1 || inner.Lateral.Bindings[1].Source != 0 {
		t.Fatalf("nested lexical bindings = %+v", inner)
	}
	where := inner.Query.Where
	if where == nil || len(where.Kids) != 2 ||
		!hasLateralReference(inner.Lateral, where.Kids[0].RightPath, 0) ||
		!hasLateralReference(inner.Lateral, where.Kids[1].RightPath, 1) {
		t.Fatalf("nested correlated predicate = %+v", where)
	}
}

func TestNestedLateralBindingsRemainInLexicalFirstReferenceOrder(t *testing.T) {
	stmt, err := Parse(`
		SELECT a.id, q.z FROM accounts a CROSS JOIN LATERAL (
			SELECT a.z AS z, d.id FROM local_rows l CROSS JOIN LATERAL (
				SELECT i.id FROM items i WHERE i.account = a.id
			) d
		) q`)
	if err != nil {
		t.Fatal(err)
	}
	bindings := stmt.From[1].Lateral.Bindings
	if len(bindings) != 2 || lateralBindingSpec(&bindings[0]) != "z" ||
		lateralBindingSpec(&bindings[1]) != "id" || bindings[0].Pos >= bindings[1].Pos {
		t.Fatalf("lexical outer binding order = %+v", bindings)
	}
	inner := stmt.From[1].Query.From[1].Lateral
	wherePath := stmt.From[1].Query.From[1].Query.Where.RightPath
	if inner == nil || !hasLateralReference(inner, wherePath, 0) ||
		lateralBindingSpec(&inner.Bindings[0]) != "id" {
		t.Fatalf("inner reference remap = spec %+v, path %+v", inner, wherePath)
	}
}

func TestLateralIllegalDirectionsAndMalformedFormsArePositioned(t *testing.T) {
	tests := []struct {
		name        string
		src         string
		marker      string
		message     string
		unsupported bool
	}{
		{
			name: "forward reference",
			src: `SELECT a.id FROM accounts a CROSS JOIN LATERAL (` +
				`SELECT i.id FROM items i WHERE i.owner = c.id) d ` +
				`JOIN countries c ON TRUE`,
			marker: "c.id", message: "joined later",
		},
		{
			name: "self reference",
			src: `SELECT a.id FROM accounts a CROSS JOIN LATERAL (` +
				`SELECT i.id FROM items i WHERE i.owner = d.id) d`,
			marker: "d.id", message: "own output",
		},
		{
			name: "right correlation",
			src: `SELECT a.id FROM accounts a RIGHT JOIN LATERAL (` +
				`SELECT i.id FROM items i WHERE i.owner = a.id) d ON TRUE`,
			marker: "a.id)", message: "RIGHT JOIN LATERAL cannot correlate",
		},
		{
			name: "full correlation",
			src: `SELECT a.id FROM accounts a FULL JOIN LATERAL (` +
				`SELECT i.id FROM items i WHERE i.owner = a.id) d ON TRUE`,
			marker: "a.id)", message: "FULL JOIN LATERAL cannot correlate",
		},
		{
			name:   "physical lateral",
			src:    `SELECT a.id FROM accounts a CROSS JOIN LATERAL items d`,
			marker: "items", message: "parenthesized SELECT",
		},
		{
			name: "lateral using",
			src: `SELECT a.id FROM accounts a JOIN LATERAL (` +
				`SELECT i.id FROM items i) d USING (id)`,
			marker: "USING", message: "write the equivalent ON", unsupported: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.src)
			if err == nil {
				t.Fatal("Parse succeeded")
			}
			var positioned *ParseError
			if !errors.As(err, &positioned) {
				t.Fatalf("error = %T %v, want ParseError", err, err)
			}
			wantPos := strings.LastIndex(test.src, test.marker)
			if positioned.Pos != wantPos || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error = %v at %d, want %d containing %q",
					err, positioned.Pos, wantPos, test.message)
			}
			var unsupported *FeatureNotSupportedError
			if errors.As(err, &unsupported) != test.unsupported {
				t.Fatalf("FeatureNotSupported = %t, want %t: %v",
					errors.As(err, &unsupported), test.unsupported, err)
			}
		})
	}
}

func TestLateralParserReuseIsDeterministicAndArenaOwned(t *testing.T) {
	const src = `SELECT a.id, d.id FROM accounts a LEFT JOIN LATERAL (` +
		`SELECT i.id FROM items i WHERE i.owner = a.id AND a.region = ?` +
		`) d ON TRUE`
	var parser Parser
	var statement SelectStmt
	var first string
	for attempt := 0; attempt < 3; attempt++ {
		if err := parser.Parse(&statement, src); err != nil {
			t.Fatal(err)
		}
		ref := &statement.From[1]
		if ref.Lateral == nil || len(ref.Lateral.Bindings) != 2 || cap(ref.Lateral.Bindings) != 2 ||
			len(ref.Lateral.References) != 2 || cap(ref.Lateral.References) != 2 {
			t.Fatalf("LATERAL arena runs = bindings %d/%d, references %d/%d",
				len(ref.Lateral.Bindings), cap(ref.Lateral.Bindings),
				len(ref.Lateral.References), cap(ref.Lateral.References))
		}
		for i := range ref.Lateral.Bindings {
			segments := ref.Lateral.Bindings[i].Segments
			if cap(segments) != len(segments) {
				t.Fatalf("binding %d segment run = len/cap %d/%d", i, len(segments), cap(segments))
			}
		}
		dump := dumpStmt(&statement)
		if attempt == 0 {
			first = dump
		} else if dump != first {
			t.Fatalf("reparse %d differs:\nfirst %s\nnext  %s", attempt, first, dump)
		}
	}
}

func TestLateralAbsentLeavesNoFrontendState(t *testing.T) {
	// Correlation lives entirely in LateralSpec. Keep the ordinary path node at
	// exactly its pre-LATERAL three-int-plus-slice footprint on every target.
	wantPathSize := unsafe.Sizeof(int(0))*3 + unsafe.Sizeof([]Segment(nil))
	if got := unsafe.Sizeof(PathExpr{}); got != wantPathSize {
		t.Fatalf("PathExpr size = %d, want unchanged footprint %d", got, wantPathSize)
	}
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, `SELECT id FROM docs WHERE id = ?`); err != nil {
		t.Fatal(err)
	}
	if parser.lateral != nil {
		t.Fatalf("ordinary SELECT initialized LATERAL state: %+v", parser.lateral)
	}
	for i := range statement.From {
		ref := &statement.From[i]
		if ref.Lateral != nil {
			t.Fatalf("ordinary relation carries LATERAL metadata: %+v", ref)
		}
	}
}

func BenchmarkLateralParse(b *testing.B) {
	var parser Parser
	var statement SelectStmt
	if err := parser.Parse(&statement, lateralAllocSQL); err != nil {
		b.Fatal(err)
	}
	if err := parser.Parse(&statement, lateralAllocSQL); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := parser.Parse(&statement, lateralAllocSQL); err != nil {
			b.Fatal(err)
		}
	}
}

func lateralBindingSpec(binding *LateralBinding) string {
	if binding == nil {
		return ""
	}
	return (&PathExpr{Segments: binding.Segments}).Spec()
}

func lateralReferenceBinding(spec *LateralSpec, path *PathExpr) (int, bool) {
	if spec == nil || path == nil {
		return 0, false
	}
	for i := range spec.References {
		if spec.References[i].Path == path {
			return spec.References[i].Binding, true
		}
	}
	return 0, false
}

func hasLateralReference(spec *LateralSpec, path *PathExpr, binding int) bool {
	got, ok := lateralReferenceBinding(spec, path)
	return ok && got == binding
}

func hasAnyLateralReference(spec *LateralSpec, path *PathExpr) bool {
	_, ok := lateralReferenceBinding(spec, path)
	return ok
}
