package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestSQLCTEChainsMainAndPredicateReferences(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH pro(uid, tier) AS MATERIALIZED (` +
			`SELECT id, tier FROM customers WHERE tier = ?` +
			`), ids AS NOT MATERIALIZED (` +
			`SELECT uid FROM pro` +
			`) ` +
			`SELECT uid FROM ids WHERE uid IN (SELECT uid FROM pro) ORDER BY uid`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	got := runStatement(
		t, stmt, FromDatabase(catalog, stmt.Collection()), "pro",
	)
	if rows := strings.TrimSpace(strings.SplitN(got, "\n", 2)[1]); rows != "4:\"c1\"|\n4:\"c3\"|" {
		t.Fatalf("rows = %q", rows)
	}
	defs := stmt.cteCatalog().defs
	if len(defs) != 2 || defs[0].runEvaluations != 1 || defs[1].runEvaluations != 1 {
		t.Fatalf("CTE evaluations = %d/%d, want 1/1",
			defs[0].runEvaluations, defs[1].runEvaluations)
	}
}

func TestSQLCTEMaterializationPoliciesAreTruthful(t *testing.T) {
	catalog := subqueryDatabase(t)
	for _, test := range []struct {
		name string
		hint string
		want cteExecutionMode
		eval uint64
	}{
		{name: "forced materialized", hint: "MATERIALIZED", want: cteSharedMaterialized, eval: 1},
		{name: "forced independent", hint: "NOT MATERIALIZED", want: cteIndependent, eval: 3},
		{name: "default repeated", hint: "", want: cteSharedMaterialized, eval: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := PrepareStatement(
				`WITH c AS ` + test.hint + ` (SELECT id FROM customers) ` +
					`SELECT id FROM c WHERE id IN (SELECT id FROM c) ` +
					`AND id IN (SELECT id FROM c) ORDER BY id`,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()
			if got := stmt.cteReference().mode(); got != test.want {
				t.Fatalf("mode = %s, want %s", got, test.want)
			}
			_ = runStatement(t, stmt, FromDatabase(catalog, stmt.Collection()))
			if got := stmt.cteCatalog().defs[0].runEvaluations; got != test.eval {
				t.Fatalf("evaluations = %d, want %d", got, test.eval)
			}
		})
	}
}

func TestSQLCTEDefaultSafePassthroughFuses(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH c AS (SELECT id FROM customers WHERE tier = 'pro') ` +
			`SELECT * FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	if got := stmt.cteReference().mode(); got != cteFused {
		t.Fatalf("mode = %s, want fused", got)
	}
	plan, err := stmt.Explain()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"collection":"customers"`) ||
		!strings.Contains(plan, `"path":"tier","operator":"="`) ||
		strings.Contains(plan, `"path":"/0"`) {
		t.Fatalf("fused EXPLAIN did not render the physical child plan: %s", plan)
	}
	var exec Exec
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 2 || stmt.cteReference().activeBytes != 0 ||
		len(stmt.cteReference().spool.columns) != 0 {
		t.Fatalf("fused rows/spool = %d/%d/%d", rows,
			stmt.cteReference().activeBytes, len(stmt.cteReference().spool.columns))
	}
	if len(exec.Result.Columns) != 1 || exec.Result.Columns[0].Header != "id" ||
		stmt.Columns()[0] != exec.Result.Columns[0].Header {
		t.Fatalf("fused public schema = statement %v result %v",
			stmt.Columns(), exec.Result.Columns)
	}
}

func TestSQLCTEAliasesWildcardsAndDuplicateResolution(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH c(uid) AS MATERIALIZED (` +
			`SELECT id, tier FROM customers WHERE id = 'c1'` +
			`) SELECT * FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	if got := stmt.Columns(); len(got) != 2 || got[0] != "uid" || got[1] != "tier" {
		t.Fatalf("columns = %v", got)
	}
	var exec Exec
	cursor, err := stmt.RunInto(&exec, FromDatabase(catalog, stmt.Collection()), nil)
	if err != nil || !cursor.Next() || cursor.Cell(0).String() != `"c1"` ||
		cursor.Cell(1).String() != `"pro"` || cursor.Next() {
		t.Fatalf("alias wildcard execution failed: err=%v", err)
	}
	if len(exec.Result.Columns) != 2 || exec.Result.Columns[0].Header != "uid" ||
		exec.Result.Columns[1].Header != "tier" {
		t.Fatalf("aliased Result schema = %v", exec.Result.Columns)
	}

	duplicate, err := PrepareStatement(
		`WITH c(x, x) AS MATERIALIZED (` +
			`SELECT id, tier FROM customers WHERE id = 'c1'` +
			`) SELECT * FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Release()
	if got := duplicate.Columns(); len(got) != 2 || got[0] != "x" || got[1] != "x" {
		t.Fatalf("duplicate columns = %v", got)
	}
	_, err = PrepareStatement(
		`WITH c(x, x) AS (SELECT id, tier FROM customers) SELECT x FROM c`,
	)
	if !errors.Is(err, ErrAmbiguousColumn) {
		t.Fatalf("named duplicate error = %v, want ambiguity", err)
	}
}

func TestSQLCTERuntimeAliasArityUsesFirstExcessPosition(t *testing.T) {
	const text = `WITH c(first, excess) AS (SELECT * FROM customers) SELECT * FROM c`
	_, err := PrepareStatement(text)
	var arity *CTEColumnAliasArityError
	if !errors.As(err, &arity) {
		t.Fatalf("error = %T %v, want runtime CTE alias arity", err, err)
	}
	if want := strings.Index(text, "excess"); arity.Position() != want {
		t.Fatalf("position = %d, want first excess alias at %d", arity.Position(), want)
	}
}

func TestSQLCTENestedDerivedPlaceholderBases(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH base AS MATERIALIZED (` +
			`SELECT d.id FROM (` +
			`SELECT id, tier FROM customers WHERE tier = ?` +
			`) d WHERE d.id <> ?` +
			`), picked AS NOT MATERIALIZED (` +
			`SELECT id FROM base WHERE id <> ?` +
			`) SELECT id FROM picked WHERE id <> ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	got := runStatement(
		t, stmt, FromDatabase(catalog, stmt.Collection()),
		"pro", "missing", "missing", "c3",
	)
	if rows := strings.TrimSpace(strings.SplitN(got, "\n", 2)[1]); rows != `4:"c1"|` {
		t.Fatalf("rows = %q", rows)
	}
}

func TestSQLCTEPreservesExactValues(t *testing.T) {
	var database store.Database
	docs, err := database.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Put("a", []byte(
		`{"n":12345678901234567890.0100,"v":{"x":[1,2]},"z":null}`,
	)); err != nil {
		t.Fatal(err)
	}
	stmt, err := PrepareStatement(
		`WITH c AS MATERIALIZED (SELECT n, v, z FROM docs) ` +
			`SELECT n, v.x[1], z FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(database.Snapshot(), stmt.Collection()), nil,
	)
	if err != nil || !cursor.Next() {
		t.Fatalf("RunInto: %v", err)
	}
	want := []string{"12345678901234567890.0100", "2", "null"}
	for i := range want {
		if got := cursor.Cell(i).String(); got != want[i] {
			t.Fatalf("cell %d = %q, want %q", i, got, want[i])
		}
	}
}

func TestSQLCTESharedIntermediateBudgetCancellationAndReuse(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH a AS MATERIALIZED (SELECT id FROM customers), ` +
			`b AS MATERIALIZED (SELECT id FROM a) ` +
			`SELECT id FROM b`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	exec.Options.IntermediateBytes = 1
	_, err = stmt.RunInto(&exec, FromDatabase(catalog, stmt.Collection()), nil)
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
		t.Fatalf("budget error/result = %v/%d", err, exec.Result.RowCount)
	}
	for _, def := range stmt.cteCatalog().defs {
		if def.state != cteIdle || def.activeBytes != 0 || def.spool.rows != 0 {
			t.Fatalf("failed CTE retained state=%d bytes=%d rows=%d",
				def.state, def.activeBytes, def.spool.rows)
		}
	}
	exec.Options.IntermediateBytes = -1
	var cancel CancelFlag
	exec.Options.Cancel = &cancel
	cancel.Cancel()
	if _, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	cancel.Reset()
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	next := cursor.Next()
	if err != nil || !next {
		t.Fatalf("post-cancel reuse: row=%t err=%v", next, err)
	}
}

func TestSQLCTEExecutionErrorClearsStateAndPreparedStatementRecovers(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH c AS MATERIALIZED (` +
			`SELECT id FROM customers WHERE tier = ?` +
			`) SELECT id FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	source := FromDatabase(catalog, stmt.Collection())
	if _, err := stmt.RunInto(&exec, source, []any{"pro"}); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.RunInto(&exec, source, []any{struct{}{}}); err == nil {
		t.Fatalf("bad binding error = %v", err)
	}
	def := stmt.cteCatalog().defs[0]
	if def.state != cteIdle || def.activeBytes != 0 || def.spool.rows != 0 ||
		exec.Result.RowCount != 0 {
		t.Fatalf("failed bind retained state=%d bytes=%d rows=%d result=%d",
			def.state, def.activeBytes, def.spool.rows, exec.Result.RowCount)
	}
	cursor, err := stmt.RunInto(&exec, source, []any{"free"})
	if err != nil || !cursor.Next() || cursor.Cell(0).String() != `"c2"` || cursor.Next() {
		t.Fatalf("post-error reuse failed: err=%v", err)
	}
}

func TestExplainReportsCTEModeReasonAndEvaluationCount(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`WITH shared AS (SELECT id FROM customers), ` +
			`independent AS NOT MATERIALIZED (SELECT id FROM shared) ` +
			`SELECT id FROM independent WHERE id IN (SELECT id FROM shared)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	before, err := stmt.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"name":"shared","mode":"materialized","reason":"default policy shares a multiply referenced definition","references":2`,
		`"name":"independent","mode":"not-materialized","reason":"NOT MATERIALIZED evaluates each syntactic reference independently","references":1`,
	} {
		if !strings.Contains(before, want) {
			t.Fatalf("EXPLAIN missing %s: %s", want, before)
		}
	}
	_ = runStatement(t, stmt, FromDatabase(catalog, stmt.Collection()))
	after, err := stmt.Explain()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(after, `"evaluations":1`); got != 2 {
		t.Fatalf("EXPLAIN evaluation counters = %d, want 2: %s", got, after)
	}
}

func TestSQLCTEWarmPoliciesAreAllocationFreeAndAbsentPathStaysEmpty(t *testing.T) {
	catalog := subqueryDatabase(t)
	for _, text := range []string{
		`WITH c AS MATERIALIZED (SELECT id FROM customers) SELECT id FROM c`,
		`WITH c AS NOT MATERIALIZED (SELECT id FROM customers) SELECT id FROM c`,
	} {
		stmt, err := PrepareStatement(text)
		if err != nil {
			t.Fatal(err)
		}
		var exec Exec
		run := func() {
			cursor, err := stmt.RunInto(
				&exec, FromDatabase(catalog, stmt.Collection()), nil,
			)
			if err != nil {
				panic(err)
			}
			for cursor.Next() {
				sqlSink += len(cursor.Cell(0).Payload())
			}
		}
		run()
		run()
		if got := testing.AllocsPerRun(50, run); got != 0 {
			t.Fatalf("%q allocated %.1f times", text, got)
		}
		exec.Release()
		stmt.Release()
	}

	plain, err := PrepareStatement(`SELECT id FROM customers WHERE tier = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Release()
	if plain.nested != nil || plain.cteCatalog() != nil || plain.cteReference() != nil {
		t.Fatal("plain SELECT initialized CTE state")
	}
}

func TestSQLCTESamePhysicalSourceDoesNotRequireCatalog(t *testing.T) {
	stmt, err := PrepareStatement(
		`WITH c AS MATERIALIZED (SELECT id FROM customers) SELECT id FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	if stmt.RequiresCatalog() {
		t.Fatal("single-physical-source CTE requires a catalog")
	}
	multi, err := PrepareStatement(
		`WITH c AS MATERIALIZED (SELECT id FROM customers) ` +
			`SELECT id FROM orders WHERE id IN (SELECT id FROM c)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Release()
	if !multi.RequiresCatalog() {
		t.Fatal("multi-physical-source CTE did not require a coherent catalog")
	}
}
