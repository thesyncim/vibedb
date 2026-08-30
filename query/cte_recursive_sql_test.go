package query

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const recursiveSQLGraphStatement = `WITH RECURSIVE reachable(node) AS MATERIALIZED (
SELECT node FROM seeds WHERE node = ?
UNION
SELECT e.dst AS node
FROM reachable r JOIN edges e ON r.node = e.src
WHERE e.enabled = ?
)
SELECT l.node AS left_node, r.node AS right_node
FROM reachable l JOIN reachable r ON l.node = r.node
ORDER BY l.node`

func prepareRecursiveSQLGraph(tb testing.TB) *Statement {
	tb.Helper()
	statement, err := PrepareRecursiveSQLStatement(
		recursiveSQLGraphStatement,
		RecursiveSQLStatementOptions{Limits: RecursiveCTELimits{
			MaxIterations: 32, MaxRows: 256, MaxBytes: -1,
		}},
	)
	if err != nil {
		tb.Fatal(err)
	}
	return statement
}

func runRecursiveSQLGraph(
	tb testing.TB,
	statement *Statement,
	exec *Exec,
	snapshot Source,
	start int,
) []int {
	tb.Helper()
	cursor, err := statement.RunInto(
		exec, snapshot, []any{int64(start), true},
	)
	if err != nil {
		tb.Fatal(err)
	}
	rows := make([]int, 0, exec.Result.RowCount)
	for cursor.Next() {
		left, leftOK := cursor.Cell(0).Int64()
		right, rightOK := cursor.Cell(1).Int64()
		if !leftOK || !rightOK || left != right {
			tb.Fatalf("recursive SQL row = %d/%v %d/%v", left, leftOK, right, rightOK)
		}
		rows = append(rows, int(left))
	}
	return rows
}

func TestRecursiveSQLBridgeGraphSnapshotIdentityAndOwnership(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	liveEdges, snapshot := recursiveStatementDatabase(t, edges)
	if _, err := liveEdges.Put(
		"late", []byte(`{"src":5,"dst":7,"enabled":true}`),
	); err != nil {
		t.Fatal(err)
	}
	statement := prepareRecursiveSQLGraph(t)
	defer statement.Release()
	if statement.NumParams() != 2 {
		t.Fatalf("NumParams = %d, want 2", statement.NumParams())
	}
	catalog := statement.cteCatalog()
	if catalog == nil || len(catalog.defs) != 1 {
		t.Fatalf("owner catalog = %+v", catalog)
	}
	definition := catalog.defs[0]
	prepared := definition.recursiveDefinition
	if prepared == nil || prepared.anchor.paramBase != 0 ||
		prepared.recursive.paramBase != 0 ||
		prepared.anchorStmt.NumParams() != statement.NumParams() ||
		prepared.recursiveStmt.NumParams() != statement.NumParams() ||
		definition.references != 2 {
		t.Fatalf("recursive installation = %+v refs=%d", prepared, definition.references)
	}
	if prepared.recursive.target == nil ||
		prepared.recursive.target == definition ||
		prepared.recursive.target.definition == definition.definition ||
		prepared.recursive.target.tree == definition.tree ||
		prepared.recursiveStmt.cteCatalog() == catalog {
		t.Fatal("recursive delta identity routed through the owning CTE catalog")
	}
	if prepared.recursive.target.definition.Query != prepared.recursive.target.tree ||
		prepared.recursive.target.recursiveOwner != prepared.recursive {
		t.Fatal("synthetic delta identity is not owned by the recursive Statement adapter")
	}
	// The caller AST is restored after isolated preparation. The compiled owner
	// retains the anchor identity internally, while authored metadata stays the
	// lossless recursive UNION for later set/explain lowering.
	authored := statement.tree.With.CTEs[0].Query
	if authored.Set == nil || statement.tree.From[0].Query != authored ||
		definition.tree != statement.tree.With.CTEs[0].Recursive.Anchor {
		t.Fatal("recursive SQL prepare did not restore authored AST identity")
	}

	var exec Exec
	got := runRecursiveSQLGraph(
		t, statement, &exec,
		FromDatabase(snapshot, statement.Collection()), 0,
	)
	want := recursiveStatementGraphOracle(0, edges)
	sort.Ints(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("recursive SQL rows = %v, want snapshot rows %v", got, want)
	}
	if definition.runEvaluations != 1 || prepared.runtime.Evaluations() != 1 ||
		prepared.runtime.frame != nil ||
		prepared.recursive.target.recursiveBinding != nil {
		t.Fatalf("publication lifecycle evaluations=%d/%d frame=%p binding=%p",
			definition.runEvaluations, prepared.runtime.Evaluations(),
			prepared.runtime.frame, prepared.recursive.target.recursiveBinding)
	}
	exec.Release()
}

func TestRecursiveSQLBridgeUnionAndMaterializationMapping(t *testing.T) {
	for _, test := range []struct {
		name         string
		union        string
		hint         string
		wantUnion    RecursiveUnionMode
		wantMaterial RecursiveCTEMaterialization
	}{
		{"distinct default shared", "UNION", "", RecursiveUnionDistinct, RecursiveCTEShared},
		{"all materialized", "UNION ALL", "MATERIALIZED", RecursiveUnionAll, RecursiveCTEShared},
		{"all reference local", "UNION ALL", "NOT MATERIALIZED", RecursiveUnionAll, RecursiveCTEReferenceLocal},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := fmt.Sprintf(`WITH RECURSIVE r(n) AS %s (
				SELECT node FROM seeds WHERE node = ?
				%s
				SELECT e.dst AS n FROM r JOIN edges e ON r.n = e.src
			) SELECT n FROM r`, test.hint, test.union)
			statement, err := PrepareRecursiveSQLStatement(
				source, RecursiveSQLStatementOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			prepared := statement.cteCatalog().defs[0].recursiveDefinition
			if prepared.descriptor.union != test.wantUnion ||
				prepared.descriptor.materialize != test.wantMaterial {
				t.Fatalf("mapping = %d/%d, want %d/%d",
					prepared.descriptor.union, prepared.descriptor.materialize,
					test.wantUnion, test.wantMaterial)
			}
			statement.Release()
		})
	}
}

func TestRecursiveSQLBridgeMergesTermOnlyParameterMetadata(t *testing.T) {
	const source = `WITH RECURSIVE walk(v) AS (
		SELECT flag AS v FROM seeds
		UNION ALL
		SELECT CASE BOOL 't' WHEN ? THEN v ELSE v END AS v FROM walk
	) SELECT v FROM walk`
	statement, err := PrepareRecursiveSQLStatement(
		source, RecursiveSQLStatementOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	position := strings.Index(source, "?")
	if got := statement.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("owner parameter type = %s, want boolean", got)
	}
	if got := statement.ParameterTypePosition(0); got != position {
		t.Fatalf("owner parameter position = %d, want %d", got, position)
	}
	prepared := statement.cteCatalog().defs[0].recursiveDefinition
	if prepared == nil ||
		prepared.recursiveStmt.ParameterType(0) != ParameterTypeBool ||
		prepared.recursiveStmt.ParameterTypePosition(0) != position {
		t.Fatalf("recursive term metadata was not installed: %+v", prepared)
	}
}

func TestRecursiveSQLBridgeCommonTypeParity(t *testing.T) {
	const mismatch = `WITH RECURSIVE walk(v) AS (
		SELECT BOOL 't' AS v FROM seeds
		UNION ALL
		SELECT TEXT 'x' AS v FROM walk
	) SELECT v FROM walk`
	_, err := PrepareRecursiveSQLStatement(
		mismatch, RecursiveSQLStatementOptions{},
	)
	var typeErr *ScalarTypeError
	if !errors.As(err, &typeErr) || !errors.Is(err, ErrScalarType) ||
		typeErr.Position() != strings.LastIndex(mismatch, "TEXT") {
		t.Fatalf("recursive BOOL/TEXT mismatch = %T %v, want 42804 class at TEXT", err, err)
	}

	const compatible = `WITH RECURSIVE walk(v) AS (
		SELECT ? AS v FROM seeds
		UNION ALL
		SELECT ? AS v FROM walk
	) SELECT v FROM walk`
	for _, test := range []struct {
		name       string
		types      []ParameterType
		want       OutputRepresentation
		wantErrPos int
	}{
		{
			name:  "varchar anchor keeps exact identity",
			types: []ParameterType{ParameterTypeVarchar, ParameterTypeText},
			want:  OutputSQLVarchar, wantErrPos: -1,
		},
		{
			name:  "name anchor accepts bpchar",
			types: []ParameterType{ParameterTypeName, ParameterTypeBPChar},
			want:  OutputSQLName, wantErrPos: -1,
		},
		{
			name:       "name candidate rejects varchar anchor",
			types:      []ParameterType{ParameterTypeVarchar, ParameterTypeName},
			wantErrPos: strings.Index(compatible, "?"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, prepareErr := PrepareRecursiveSQLStatement(
				compatible, RecursiveSQLStatementOptions{parameterTypes: test.types},
			)
			if test.wantErrPos >= 0 {
				var mismatch *ScalarTypeError
				if !errors.As(prepareErr, &mismatch) ||
					mismatch.Position() != test.wantErrPos {
					t.Fatalf("prepare error = %T %v, want positioned recursive mismatch", prepareErr, prepareErr)
				}
				return
			}
			if prepareErr != nil {
				t.Fatal(prepareErr)
			}
			defer statement.Release()
			schema := statement.AppendSchema(nil)
			if len(schema) != 1 || schema[0].Representation != test.want {
				t.Fatalf("recursive schema = %+v, want representation %d", schema, test.want)
			}
		})
	}
}

func TestRecursiveSQLBridgeUntypedParameterMetadataStaysNil(t *testing.T) {
	statement := prepareRecursiveSQLGraph(t)
	defer statement.Release()
	if statement.paramTypes != nil || statement.paramTypePositions != nil {
		t.Fatalf("untyped recursive owner retained parameter sidecars: %v/%v",
			statement.paramTypes, statement.paramTypePositions)
	}
	prepared := statement.cteCatalog().defs[0].recursiveDefinition
	if prepared.anchorStmt.paramTypes != nil ||
		prepared.recursiveStmt.paramTypes != nil {
		t.Fatalf("untyped recursive terms retained parameter sidecars: %v/%v",
			prepared.anchorStmt.paramTypes, prepared.recursiveStmt.paramTypes)
	}
}

func TestRecursiveSQLBridgeUnknownTermCoercion(t *testing.T) {
	const source = `WITH RECURSIVE walk(v) AS (
		SELECT BOOL 't' AS v FROM seeds
		UNION
		SELECT ? AS v FROM walk
	) SELECT v FROM walk ORDER BY v`
	statement, err := PrepareRecursiveSQLStatement(
		source, RecursiveSQLStatementOptions{Limits: RecursiveCTELimits{
			MaxIterations: 8, MaxRows: 16, MaxBytes: -1,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if got := statement.ParameterType(0); got != ParameterTypeBool {
		t.Fatalf("recursive unknown parameter type = %s, want boolean", got)
	}
	_, snapshot := recursiveStatementDatabase(t, nil)
	var execution Exec
	value := "f"
	cursor, err := statement.RunInto(
		&execution, FromDatabase(snapshot, statement.Collection()),
		[]any{&value},
	)
	if err != nil {
		t.Fatal(err)
	}
	var values []bool
	for cursor.Next() {
		value, ok := cursor.Cell(0).Bool()
		if !ok {
			t.Fatalf("recursive value is %v, want boolean", cursor.Cell(0).Kind())
		}
		values = append(values, value)
	}
	if want := []bool{false, true}; !reflect.DeepEqual(values, want) {
		t.Fatalf("recursive values = %v, want %v", values, want)
	}
	execution.Release()
}

func TestRecursiveSQLBridgeCancellationBudgetAtomicityAndReuse(t *testing.T) {
	edges := [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	statement := prepareRecursiveSQLGraph(t)
	defer statement.Release()
	source := FromDatabase(snapshot, statement.Collection())
	definition := statement.cteCatalog().defs[0]
	prepared := definition.recursiveDefinition
	var exec Exec
	if got := runRecursiveSQLGraph(t, statement, &exec, source, 0); len(got) != 5 {
		t.Fatalf("warm rows = %d, want 5", len(got))
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options = ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	_, err := statement.RunInto(&exec, source, []any{int64(0), true})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancel error = %v", err)
	}
	assertRecursiveSQLFailureAtomic(t, definition, prepared, &exec)

	cancel.Reset()
	exec.Options = ExecOptions{IntermediateBytes: 1, Cancel: &cancel}
	_, err = statement.RunInto(&exec, source, []any{int64(0), true})
	if !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("budget error = %v", err)
	}
	assertRecursiveSQLFailureAtomic(t, definition, prepared, &exec)

	exec.Options = ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	if got := runRecursiveSQLGraph(t, statement, &exec, source, 0); len(got) != 5 {
		t.Fatalf("reuse rows = %d, want 5", len(got))
	}
	exec.Release()
}

func assertRecursiveSQLFailureAtomic(
	tb testing.TB,
	definition *statementCTE,
	prepared *statementRecursiveDefinition,
	exec *Exec,
) {
	tb.Helper()
	if exec.Result.RowCount != 0 || definition.state != cteIdle ||
		definition.activeBytes != 0 || definition.activeFrame != nil ||
		definition.spool.rows != 0 ||
		prepared.runtime.frame != nil || prepared.execution.intermediate.used != 0 ||
		prepared.recursive.target.recursiveBinding != nil {
		tb.Fatalf("recursive SQL failure retained publication state")
	}
}

func TestRecursiveSQLBridgeOrdinaryDispatchAndASTRestorationOnFailure(t *testing.T) {
	dispatched, err := PrepareStatement(recursiveSQLGraphStatement)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched.cteCatalog() == nil || len(dispatched.cteCatalog().defs) != 1 ||
		dispatched.cteCatalog().defs[0].recursiveDefinition == nil {
		dispatched.Release()
		t.Fatal("ordinary PrepareStatement did not install recursive publication")
	}
	dispatched.Release()

	var parser sqlast.Parser
	var tree sqlast.SelectStmt
	if err := parser.Parse(&tree, recursiveSQLGraphStatement); err != nil {
		t.Fatal(err)
	}
	if required, pos := RecursiveSQLStatementRequired(&tree); !required ||
		pos != tree.With.CTEs[0].Pos {
		t.Fatalf("recursive dispatch detector = %v/%d", required, pos)
	}
	definition := &tree.With.CTEs[0]
	authored := definition.Query
	anchor := definition.Recursive.Anchor
	selfQuery := definition.Recursive.Term.From[0].Query
	anchorBase := anchor.ParamBase
	_, err = PrepareParsedRecursiveSQLStatement(
		recursiveSQLGraphStatement, &tree,
		RecursiveSQLStatementOptions{Limits: RecursiveCTELimits{MaxRows: -2}},
	)
	if err == nil {
		t.Fatal("invalid recursive limits prepared")
	}
	if definition.Query != authored || anchor.ParamBase != anchorBase ||
		definition.Recursive.Term.From[0].Query != selfQuery ||
		tree.From[0].Query != authored {
		t.Fatal("failed recursive prepare mutated caller AST")
	}
	reused, reuseErr := PrepareParsedRecursiveSQLStatement(
		recursiveSQLGraphStatement, &tree,
		RecursiveSQLStatementOptions{Limits: RecursiveCTELimits{
			MaxIterations: 32, MaxRows: 256, MaxBytes: -1,
		}},
	)
	if reuseErr != nil {
		t.Fatalf("AST reuse after failed ownership transfer: %v", reuseErr)
	}
	reused.Release()
}

func TestRecursiveSQLBridgeDeferredArityAndNestedCompoundRefusals(t *testing.T) {
	_, err := PrepareStatement(`WITH RECURSIVE c(a, b) AS (
		SELECT * FROM seeds UNION ALL SELECT * FROM c
	) SELECT a FROM c`)
	var aliases *CTEColumnAliasArityError
	if !errors.As(err, &aliases) || aliases.Aliases != 2 || aliases.Outputs != 1 {
		t.Fatalf("deferred wildcard arity error = %T %v", err, err)
	}

	for _, test := range []struct {
		name   string
		source string
		marker string
	}{
		{
			name: "nested derived recursive",
			source: `SELECT d.n FROM (WITH RECURSIVE r(n) AS (
				SELECT node FROM seeds UNION ALL SELECT e.dst AS n FROM r JOIN edges e ON r.n = e.src
			) SELECT n FROM r) d`,
			marker: "r(n)",
		},
		{
			name: "top level compound recursive",
			source: `(WITH RECURSIVE r(n) AS (
				SELECT node FROM seeds UNION ALL SELECT e.dst AS n FROM r JOIN edges e ON r.n = e.src
			) SELECT n FROM r) UNION ALL SELECT node FROM seeds`,
			marker: "r(n)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := PrepareStatement(test.source)
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want positioned FeatureNotSupported", err, err)
			}
			want := len(test.source[:strings.Index(test.source, test.marker)])
			if unsupported.Pos < want || unsupported.Pos >= want+len(test.marker) {
				t.Fatalf("position = %d, want inside %q at %d: %v",
					unsupported.Pos, test.marker, want, err)
			}
		})
	}
}

func TestRecursiveSQLBridgeOwningStatementReleaseIsExactAndIdempotent(t *testing.T) {
	statement := prepareRecursiveSQLGraph(t)
	definition := statement.cteCatalog().defs[0]
	prepared := definition.recursiveDefinition
	anchor, recursive := prepared.anchor, prepared.recursive
	anchorStmt, recursiveStmt := prepared.anchorStmt, prepared.recursiveStmt
	deltaTarget := recursive.target
	statement.Release()
	statement.Release()
	if prepared.descriptor != nil || prepared.anchor != nil || prepared.recursive != nil ||
		anchor.statement != nil || recursive.statement != nil ||
		anchorStmt.tree != nil || recursiveStmt.tree != nil ||
		deltaTarget.recursiveOwner != nil {
		t.Fatal("recursive SQL owning Statement retained transferred term lifecycle state")
	}
}

func TestRecursiveSQLBridgeIndependentStatementsRaceSafe(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	const workers = 6
	statements := make([]*Statement, workers)
	for i := range statements {
		statements[i] = prepareRecursiveSQLGraph(t)
		defer statements[i].Release()
	}
	want := recursiveStatementGraphOracle(0, edges)
	sort.Ints(want)
	errorsOut := make(chan error, workers)
	var wait sync.WaitGroup
	for i := range statements {
		wait.Add(1)
		go func(statement *Statement) {
			defer wait.Done()
			var exec Exec
			cursor, err := statement.RunInto(
				&exec, FromDatabase(snapshot, statement.Collection()),
				[]any{int64(0), true},
			)
			if err != nil {
				errorsOut <- err
				return
			}
			got := make([]int, 0, exec.Result.RowCount)
			for cursor.Next() {
				value, ok := cursor.Cell(0).Int64()
				if !ok {
					errorsOut <- fmt.Errorf("non-integer recursive SQL row")
					return
				}
				got = append(got, int(value))
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				errorsOut <- fmt.Errorf("rows %v, want %v", got, want)
			}
		}(statements[i])
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Error(err)
	}
}
