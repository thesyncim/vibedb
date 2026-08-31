package query

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

func legacySQLJoinDatabase(
	t testing.TB,
	leftDocs, rightDocs []string,
	indexLeftKey bool,
) *store.Database {
	t.Helper()
	db := new(store.Database)
	left, err := db.CreateCollection("left_docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range leftDocs {
		if _, err := left.Put(fmt.Sprintf("l%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if indexLeftKey {
		if _, err := left.CreateIndex(store.IndexDefinition{
			Name: "left-k", Paths: []string{"/k"},
		}); err != nil {
			t.Fatal(err)
		}
		if info, err := left.BackfillIndex("left-k", 0); err != nil || info.State != store.IndexReady {
			t.Fatalf("BackfillIndex(left-k) = (%+v, %v)", info, err)
		}
	}
	right, err := db.CreateCollection("right_docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range rightDocs {
		if _, err := right.Put(fmt.Sprintf("r%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func runLegacySQLJoin(
	t testing.TB,
	db *store.Database,
	source string,
) (*Statement, *Exec, error) {
	t.Helper()
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	if statement.UsesGeneralizedRelationJoin() {
		statement.Release()
		t.Fatalf("statement unexpectedly selected generalized relation JOIN: %s", source)
	}
	plan, err := statement.q.compiled()
	if err != nil {
		statement.Release()
		t.Fatal(err)
	}
	if len(plan.joins) != 1 || plan.joins[0].origin != joinOriginSQL {
		statement.Release()
		t.Fatalf("legacy JOIN provenance = %+v", plan.joins)
	}
	exec := new(Exec)
	_, err = statement.RunInto(exec, FromDatabase(db.Snapshot(), "left_docs"), nil)
	return statement, exec, err
}

func TestLegacySQLJoinRejectsUndefinedPathOperators(t *testing.T) {
	tests := []struct {
		name       string
		left       string
		right      string
		source     string
		wantLeft   string
		wantRight  string
		positioned bool
	}{
		{
			name: "inner normalized", left: `{"id":"l","k":1}`, right: `{"id":"r","k":"1"}`,
			source:   `SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k`,
			wantLeft: "numeric", wantRight: "text", positioned: true,
		},
		{
			name: "inner authored reverse", left: `{"id":"l","k":1}`, right: `{"id":"r","k":"1"}`,
			source:   `SELECT l.id FROM left_docs l JOIN right_docs r ON r.k = l.k`,
			wantLeft: "text", wantRight: "numeric", positioned: true,
		},
		{
			name: "left", left: `{"id":"l","k":1}`, right: `{"id":"r","k":"1"}`,
			source:   `SELECT l.id FROM left_docs l LEFT JOIN right_docs r ON l.k = r.k`,
			wantLeft: "numeric", wantRight: "text", positioned: true,
		},
		{
			name: "json inner", left: `{"id":"l","k":{"x":1}}`, right: `{"id":"r","k":{"x":1}}`,
			source:   `SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k`,
			wantLeft: "json", wantRight: "json", positioned: true,
		},
		{
			name: "json left", left: `{"id":"l","k":{"x":1}}`, right: `{"id":"r","k":{"x":1}}`,
			source:   `SELECT l.id FROM left_docs l LEFT JOIN right_docs r ON l.k = r.k`,
			wantLeft: "json", wantRight: "json", positioned: true,
		},
		{
			name: "using generated equality", left: `{"id":"l","k":1}`, right: `{"id":"r","k":"1"}`,
			source:   `SELECT l.id FROM left_docs l JOIN right_docs r USING (k)`,
			wantLeft: "numeric", wantRight: "text", positioned: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := legacySQLJoinDatabase(t, []string{test.left}, []string{test.right}, false)
			statement, _, err := runLegacySQLJoin(t, db, test.source)
			defer statement.Release()
			var undefined *sqlast.UndefinedOperatorError
			if !errors.As(err, &undefined) {
				t.Fatalf("error = %T %v", err, err)
			}
			if undefined.Left != test.wantLeft || undefined.Operator != "=" ||
				undefined.Right != test.wantRight || undefined.Unpositioned == test.positioned {
				t.Fatalf("undefined operator = %+v", undefined)
			}
			if test.positioned && undefined.Pos != strings.Index(test.source, "=") {
				t.Fatalf("operator position = %d, want %d", undefined.Pos, strings.Index(test.source, "="))
			}
			if !test.positioned {
				var positioned *sqlast.ParseError
				if errors.As(err, &positioned) {
					t.Fatalf("generated USING equality exposed position %+v", positioned)
				}
			}
		})
	}
}

func TestLegacySQLJoinNullAndMissingRemainUnknown(t *testing.T) {
	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"null","k":null}`, `{"id":"missing"}`},
		[]string{`{"id":"right","k":1}`}, false,
	)
	inner, innerExec, err := runLegacySQLJoin(t, db,
		`SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k`)
	defer inner.Release()
	if err != nil || innerExec.Result.RowCount != 0 {
		t.Fatalf("INNER null/missing = (%d rows, %v)", innerExec.Result.RowCount, err)
	}
	left, leftExec, err := runLegacySQLJoin(t, db,
		`SELECT l.id FROM left_docs l LEFT JOIN right_docs r ON l.k = r.k`)
	defer left.Release()
	if err != nil || leftExec.Result.RowCount != 2 {
		t.Fatalf("LEFT null/missing = (%d rows, %v), want two null-extended rows",
			leftExec.Result.RowCount, err)
	}
}

func TestLegacySQLJoinForcesFullScanBeforeRuntimeDomainCheck(t *testing.T) {
	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"bad","k":1}`},
		[]string{`{"id":"right","k":"1"}`}, true,
	)
	source := `SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k WHERE l.k = 1`
	statement, exec, err := runLegacySQLJoin(t, db, source)
	defer statement.Release()
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) {
		t.Fatalf("error = %T %v", err, err)
	}
	if exec.Workspace.storeIndexProbes != 0 || exec.Stats.IndexBounded {
		t.Fatalf("runtime-typed JOIN used candidate pruning: probes=%d stats=%+v",
			exec.Workspace.storeIndexProbes, exec.Stats)
	}
}

func TestLegacySQLJoinResolvesONBeforeOuterWhere(t *testing.T) {
	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"bad","k":1,"keep":false}`},
		[]string{`{"id":"right","k":"1"}`}, false,
	)
	for _, source := range []string{
		`SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k WHERE l.keep = TRUE`,
		`SELECT l.id FROM left_docs l LEFT JOIN right_docs r ON l.k = r.k WHERE l.keep = TRUE`,
	} {
		statement, exec, err := runLegacySQLJoin(t, db, source)
		statement.Release()
		var undefined *sqlast.UndefinedOperatorError
		if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
			undefined.Operator != "=" || undefined.Right != "text" ||
			undefined.Unpositioned || undefined.Pos != strings.Index(source, "=") {
			t.Fatalf("%s: error = %T %+v", source, err, undefined)
		}
		if exec.Result.RowCount != 0 {
			t.Fatalf("%s: partial result has %d rows", source, exec.Result.RowCount)
		}
	}
}

func TestSQLJoinResolvesONBeforeJoinedSideWhere(t *testing.T) {
	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"bad","k":1}`},
		[]string{`{"id":"right","k":"1","active":false}`}, false,
	)
	source := `SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k WHERE r.active = TRUE`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.UsesGeneralizedRelationJoin() {
		t.Fatal("single-key INNER joined-side WHERE left the legacy physical pipeline")
	}
	var exec Exec
	_, err = statement.RunInto(&exec, FromDatabase(db.Snapshot(), "left_docs"), nil)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
		undefined.Operator != "=" || undefined.Right != "text" ||
		undefined.Unpositioned || undefined.Pos != strings.Index(source, "=") {
		t.Fatalf("error = %T %+v", err, undefined)
	}
}

func TestGeneralizedSQLJoinResolvesResidualOperatorBeforeShortCircuit(t *testing.T) {
	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"left","k":1,"keep":false}`},
		[]string{`{"id":"right","owner":"left","k":"1"}`}, false,
	)
	source := `SELECT l.id FROM left_docs l JOIN right_docs r ` +
		`ON l.id = r.owner AND l.keep = TRUE AND l.k < r.k`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if !statement.UsesGeneralizedRelationJoin() {
		t.Fatal("residual ON did not select generalized relation JOIN")
	}
	var exec Exec
	_, err = statement.RunInto(&exec, FromDatabase(db.Snapshot(), "left_docs"), nil)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
		undefined.Operator != "<" || undefined.Right != "text" ||
		undefined.Unpositioned || undefined.Pos != strings.Index(source, "<") {
		t.Fatalf("error = %T %+v", err, undefined)
	}
}

func TestLegacySQLJoinResolvesONBeforeLimitZero(t *testing.T) {
	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"bad","k":1}`},
		[]string{`{"id":"right","k":"1"}`}, false,
	)
	for _, source := range []string{
		`SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k LIMIT 0`,
		`SELECT l.id FROM left_docs l LEFT JOIN right_docs r ON l.k = r.k LIMIT 0`,
	} {
		statement, _, err := runLegacySQLJoin(t, db, source)
		statement.Release()
		var undefined *sqlast.UndefinedOperatorError
		if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
			undefined.Operator != "=" || undefined.Right != "text" ||
			undefined.Unpositioned || undefined.Pos != strings.Index(source, "=") {
			t.Fatalf("%s: error = %T %+v", source, err, undefined)
		}
	}
}

func TestLegacySQLJoinLayoutAndWarmAllocationGates(t *testing.T) {
	switch unsafe.Sizeof(uintptr(0)) {
	case 8:
		if got := unsafe.Sizeof(Join{}); got != 208 {
			t.Fatalf("Join size = %d, want unchanged 208", got)
		}
		if got := unsafe.Sizeof(planJoin{}); got != 104 {
			t.Fatalf("planJoin size = %d, want unchanged 104", got)
		}
		if got := unsafe.Sizeof(joinBinding{}); got != 3760 {
			t.Fatalf("joinBinding size = %d, want unchanged 3760", got)
		}
		if got := unsafe.Offsetof(joinBinding{}.lits); got != 8 {
			t.Fatalf("joinBinding.lits offset = %d, want unchanged 8", got)
		}
		if got := unsafe.Sizeof(Predicate{}); got != 136 {
			t.Fatalf("Predicate size = %d, want unchanged 136", got)
		}
		if got := unsafe.Sizeof(compiledPredicate{}); got != 360 {
			t.Fatalf("compiledPredicate size = %d, want unchanged 360", got)
		}
		if got := unsafe.Sizeof(scalar{}); got != 88 {
			t.Fatalf("scalar size = %d, want unchanged 88", got)
		}
	case 4:
		if got := unsafe.Sizeof(Join{}); got != 108 {
			t.Fatalf("Join size = %d, want unchanged 108", got)
		}
		if got := unsafe.Sizeof(planJoin{}); got != 56 {
			t.Fatalf("planJoin size = %d, want unchanged 56", got)
		}
		if got := unsafe.Sizeof(joinBinding{}); got != 2064 {
			t.Fatalf("joinBinding size = %d, want unchanged 2064", got)
		}
		if got := unsafe.Offsetof(joinBinding{}.lits); got != 4 {
			t.Fatalf("joinBinding.lits offset = %d, want unchanged 4", got)
		}
		if got := unsafe.Sizeof(Predicate{}); got != 72 {
			t.Fatalf("Predicate size = %d, want unchanged 72", got)
		}
		if got := unsafe.Sizeof(compiledPredicate{}); got != 184 {
			t.Fatalf("compiledPredicate size = %d, want unchanged 184", got)
		}
		if got := unsafe.Sizeof(scalar{}); got != 48 {
			t.Fatalf("scalar size = %d, want unchanged 48", got)
		}
	}

	db := legacySQLJoinDatabase(t,
		[]string{`{"id":"left","k":1}`},
		[]string{`{"id":"right","k":1}`}, false,
	)
	statement, err := PrepareStatement(
		`SELECT l.id FROM left_docs l JOIN right_docs r ON l.k = r.k`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), "left_docs")
	exec := Exec{Options: ExecOptions{Workers: 1, IntermediateBytes: -1, JoinPairBytes: -1}}
	run := func() {
		if _, err := statement.RunInto(&exec, source, nil); err != nil {
			panic(err)
		}
		if exec.Result.RowCount != 1 {
			panic("legacy SQL JOIN returned the wrong row count")
		}
	}
	run()
	run()
	if allocs := testing.AllocsPerRun(25, run); allocs != 0 {
		t.Fatalf("warmed legacy SQL JOIN allocated %.2f/run", allocs)
	}
}
