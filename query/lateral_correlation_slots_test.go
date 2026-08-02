package query

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// This file is the query-level contract for compile-once LATERAL correlation
// slots. Most assertions are black-box SQL semantics. The two white-box checks
// use state that already exists: Statement.cached proves a correlation-only
// child is reusable, and statementLateral.evaluations proves duplicate tuples
// are still executed once per left row rather than silently memoized.

var lateralCorrelationSlotsSink int

const lateralCorrelationSlotsSQL = `
	SELECT o.id, d.id
	FROM slot_outer o
	LEFT JOIN LATERAL (
		SELECT i.id FROM slot_inner i WHERE i.k = o.k
	) d ON TRUE`

func TestSQLLateralCorrelationSlotsCompileOnceContract(t *testing.T) {
	db := lateralCorrelationSlotsScaleHeap(t, 64, true)
	statement, err := PrepareStatement(lateralCorrelationSlotsSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	join := statement.relationJoin()
	if join == nil || len(join.operands) != 2 || join.operands[1].lateral == nil ||
		join.operands[1].stmt == nil {
		t.Fatal("correlated LATERAL child was not prepared")
	}
	child := join.operands[1].stmt
	if !child.cached {
		t.Fatal("correlation-only LATERAL child is not cacheable: execution would re-lower it per outer row instead of reading runtime slots")
	}

	var exec Exec
	defer exec.Release()
	rows := lateralCorrelationSlotsRun(
		t, statement, &exec,
		FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	if len(rows) != 64 {
		t.Fatalf("rows = %d, want 64", len(rows))
	}
	if got := join.operands[1].lateral.evaluations; got != 64 {
		t.Fatalf("child evaluations = %d, want 64", got)
	}
	if !child.cached {
		t.Fatal("successful execution invalidated the compile-once child plan")
	}
}

func TestSQLLateralCorrelationSlotsOuterRowScaling(t *testing.T) {
	for _, rows := range []int{1, 8, 64, 257} {
		rows := rows
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			db := lateralCorrelationSlotsScaleHeap(t, rows, true)
			statement, err := PrepareStatement(lateralCorrelationSlotsSQL)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			got := lateralCorrelationSlotsRun(
				t, statement, &exec,
				FromDatabase(db.Snapshot(), statement.Collection()), nil,
			)
			if len(got) != rows {
				t.Fatalf("result rows = %d, want %d", len(got), rows)
			}
			lateral := statement.relationJoin().operands[1].lateral
			if lateral.evaluations != uint64(rows) {
				t.Fatalf("child evaluations = %d, want %d", lateral.evaluations, rows)
			}
			want := make([]string, rows)
			for i := range want {
				want[i] = fmt.Sprintf("%d,%d", i, i)
			}
			lateralCorrelationSlotsAssertRows(t, got, want)
		})
	}
}

func TestSQLLateralCorrelationSlotsComparisonTruthTables(t *testing.T) {
	db := &store.Database{}
	outer := lateralCorrelationSlotsHeapCollection(t, db, "slot_cmp_outer")
	for i, doc := range []string{
		`{"id":"known","k":2}`,
		`{"id":"null","k":null}`,
		`{"id":"missing"}`,
	} {
		if _, err := outer.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner := lateralCorrelationSlotsHeapCollection(t, db, "slot_cmp_inner")
	for i, doc := range []string{
		`{"id":"one","k":1}`,
		`{"id":"two","k":2}`,
		`{"id":"three","k":3}`,
		`{"id":"null","k":null}`,
		`{"id":"missing"}`,
	} {
		if _, err := inner.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		predicate string
		known     []string
	}{
		{name: "equal", predicate: "i.k = o.k", known: []string{"two"}},
		{name: "not-equal", predicate: "i.k <> o.k", known: []string{"one", "three"}},
		{name: "not-equal-expression", predicate: "NOT (i.k = o.k)", known: []string{"one", "three"}},
		{name: "less", predicate: "i.k < o.k", known: []string{"one"}},
		{name: "less-equal", predicate: "i.k <= o.k", known: []string{"one", "two"}},
		{name: "greater", predicate: "i.k > o.k", known: []string{"three"}},
		{name: "greater-equal", predicate: "i.k >= o.k", known: []string{"two", "three"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sql := `SELECT o.id, d.id FROM slot_cmp_outer o LEFT JOIN LATERAL (` +
				`SELECT i.id FROM slot_cmp_inner i WHERE ` + test.predicate +
				`) d ON TRUE`
			statement, err := PrepareStatement(sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			got := lateralCorrelationSlotsRun(
				t, statement, &exec,
				FromDatabase(db.Snapshot(), statement.Collection()), nil,
			)
			want := make([]string, 0, len(test.known)+2)
			for _, id := range test.known {
				want = append(want, fmt.Sprintf(`"known","%s"`, id))
			}
			// A NULL or missing correlation value makes every ordinary comparison
			// UNKNOWN. LEFT LATERAL therefore emits exactly one null extension.
			want = append(want, `"null",null`, `"missing",null`)
			lateralCorrelationSlotsAssertRows(t, got, want)
		})
	}
}

func TestSQLLateralCorrelationSlotsNestedDepthTwoAndThree(t *testing.T) {
	db := lateralStatementDatabase(t)
	tests := []struct {
		name string
		sql  string
	}{
		{name: "depth-two", sql: nestedInheritedLateralSQL},
		{name: "depth-three", sql: `
			SELECT a.id, q.id FROM accounts a CROSS JOIN LATERAL (
				SELECT d.id AS id
				FROM items i CROSS JOIN LATERAL (
					SELECT e.id AS id
					FROM items x CROSS JOIN LATERAL (
						SELECT y.id FROM items y
						WHERE y.owner = a.id AND y.active = x.active
					) e
					WHERE x.owner = a.id AND x.active = i.active
				) d
				WHERE i.owner = a.id
			) q`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			got := lateralCorrelationSlotsRun(
				t, statement, &exec,
				FromDatabase(db.Snapshot(), statement.Collection()), nil,
			)
			want := []string{`1,"a"`, `1,"b"`, `2,"c"`}
			lateralCorrelationSlotsAssertRows(t, got, want)
			lateralCorrelationSlotsAssertInactive(t, statement)
		})
	}
}

func TestSQLLateralCorrelationSlotsAuthoredPlaceholderRebinding(t *testing.T) {
	db := &store.Database{}
	outer := lateralCorrelationSlotsHeapCollection(t, db, "slot_rebind_outer")
	for i, doc := range []string{`{"id":"o1","k":1}`, `{"id":"o2","k":2}`} {
		if _, err := outer.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner := lateralCorrelationSlotsHeapCollection(t, db, "slot_rebind_inner")
	for i, doc := range []string{
		`{"id":"a","k":1,"score":10}`,
		`{"id":"b","k":1,"score":20}`,
		`{"id":"c","k":2,"score":15}`,
	} {
		if _, err := inner.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	statement, err := PrepareStatement(`
		SELECT o.id, d.id FROM slot_rebind_outer o LEFT JOIN LATERAL (
			SELECT i.id FROM slot_rebind_inner i
			WHERE i.k = o.k AND i.score >= ?
		) d ON TRUE`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	defer exec.Release()

	tests := []struct {
		name string
		arg  any
		want []string
	}{
		{name: "all", arg: int64(0), want: []string{`"o1","a"`, `"o1","b"`, `"o2","c"`}},
		{name: "selective", arg: int64(16), want: []string{`"o1","b"`, `"o2",null`}},
		{name: "null", arg: nil, want: []string{`"o1",null`, `"o2",null`}},
		{name: "rebound", arg: int64(15), want: []string{`"o1","b"`, `"o2","c"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := lateralCorrelationSlotsRun(t, statement, &exec, source, []any{test.arg})
			lateralCorrelationSlotsAssertRows(t, got, test.want)
		})
	}
}

func TestSQLLateralCorrelationSlotsDuplicateTuplesExecutePerRow(t *testing.T) {
	db := &store.Database{}
	outer := lateralCorrelationSlotsHeapCollection(t, db, "slot_dup_outer")
	for i := 0; i < 17; i++ {
		doc := fmt.Sprintf(`{"id":%d,"k":1}`, i)
		if _, err := outer.Put(fmt.Sprint(i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner := lateralCorrelationSlotsHeapCollection(t, db, "slot_dup_inner")
	if _, err := inner.Put("match", []byte(`{"id":"match","k":1}`)); err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareStatement(`
		SELECT o.id, d.id FROM slot_dup_outer o CROSS JOIN LATERAL (
			SELECT i.id FROM slot_dup_inner i WHERE i.k = o.k
		) d`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	got := lateralCorrelationSlotsRun(
		t, statement, &exec,
		FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	if len(got) != 17 {
		t.Fatalf("rows = %d, want 17", len(got))
	}
	want := make([]string, 17)
	for row := range want {
		want[row] = fmt.Sprintf(`%d,"match"`, row)
	}
	lateralCorrelationSlotsAssertRows(t, got, want)
	if evaluations := statement.relationJoin().operands[1].lateral.evaluations; evaluations != 17 {
		t.Fatalf("duplicate tuple evaluations = %d, want once per outer row (17)", evaluations)
	}
}

func TestSQLLateralCorrelationSlotsHeapDurableIndexDifferential(t *testing.T) {
	shapes := []struct {
		name  string
		paths []string
		sql   string
		want  []string
	}{
		{
			name: "single", paths: []string{"/k"},
			sql: `SELECT o.id, d.id FROM slot_idx_outer o LEFT JOIN LATERAL (` +
				`SELECT i.id FROM slot_idx_inner i WHERE i.k = o.k) d ON TRUE`,
			want: []string{
				`"escaped","escaped"`, `"null",null`, `"missing",null`,
				`"oa1","a1-first"`, `"oa1","a1-second"`, `"oa1","b1"`,
				`"oa2","a2"`, `"oa2","b2"`,
				`"ob1","a1-first"`, `"ob1","a1-second"`, `"ob1","b1"`,
				`"ob2","a2"`, `"ob2","b2"`,
			},
		},
		{
			name: "compound", paths: []string{"/tenant", "/k"},
			sql: `SELECT o.id, d.id FROM slot_idx_outer o LEFT JOIN LATERAL (` +
				`SELECT i.id FROM slot_idx_inner i ` +
				`WHERE i.tenant = o.tenant AND i.k = o.k) d ON TRUE`,
			want: []string{
				`"escaped","escaped"`, `"null",null`, `"missing",null`,
				`"oa1","a1-first"`, `"oa1","a1-second"`,
				`"oa2","a2"`, `"ob1","b1"`, `"ob2","b2"`,
			},
		},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			var baseline []string
			for _, backend := range []string{"heap", "durable"} {
				for _, indexed := range []bool{false, true} {
					name := fmt.Sprintf("%s/indexed=%v", backend, indexed)
					t.Run(name, func(t *testing.T) {
						definition := store.IndexDefinition{}
						if indexed {
							definition = store.IndexDefinition{Name: "slot_idx", Paths: shape.paths}
						}
						source := lateralCorrelationSlotsIndexSource(t, backend, definition)
						statement, err := PrepareStatement(shape.sql)
						if err != nil {
							t.Fatal(err)
						}
						defer statement.Release()
						var exec Exec
						defer exec.Release()
						got := lateralCorrelationSlotsRun(t, statement, &exec, source, nil)
						lateralCorrelationSlotsAssertRows(t, got, shape.want)
						if baseline == nil {
							baseline = append(baseline, got...)
						} else {
							lateralCorrelationSlotsAssertRows(t, got, baseline)
						}

						childExec := &statement.relationJoin().operands[1].exec
						bounded := childExec.Workspace.storeIndexProbes != 0
						if backend == "durable" {
							bounded = childExec.Stats.IndexBounded && childExec.Stats.IndexLookups != 0
						}
						if indexed != bounded {
							t.Fatalf("dynamic correlation index bounded = %v, want %v (heap probes=%d durable stats=%+v)",
								bounded, indexed, childExec.Workspace.storeIndexProbes, childExec.Stats)
						}
					})
				}
			}
		})
	}
}

func TestSQLLateralCorrelationSlotsBudgetCancellationRecovery(t *testing.T) {
	const rows = 32
	db := lateralCorrelationSlotsScaleHeap(t, rows, true)
	statement, err := PrepareStatement(lateralCorrelationSlotsSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	if got := len(lateralCorrelationSlotsRun(t, statement, &exec, source, nil)); got != rows {
		t.Fatalf("warm rows = %d, want %d", got, rows)
	}
	required := lateralCorrelationSlotsMinimumBudget(t, statement, &exec, source, rows)
	if required <= 1 {
		t.Fatalf("minimum intermediate budget = %d, want > 1", required)
	}

	exec.Options.IntermediateBytes = required - 1
	_, err = statement.RunInto(&exec, source, nil)
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || exec.Result.RowCount != 0 {
		t.Fatalf("one-byte-short execution = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}
	lateralCorrelationSlotsAssertInactive(t, statement)

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.IntermediateBytes = required
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(&exec, source, nil)
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("canceled execution = %T %v, rows=%d", err, err, exec.Result.RowCount)
	}
	lateralCorrelationSlotsAssertInactive(t, statement)

	cancel.Reset()
	if got := len(lateralCorrelationSlotsRun(t, statement, &exec, source, nil)); got != rows {
		t.Fatalf("recovery rows = %d, want %d", got, rows)
	}
	lateralCorrelationSlotsAssertInactive(t, statement)
}

func TestSQLLateralCorrelationSlotsIndependentStatementsRace(t *testing.T) {
	const rows = 64
	db := lateralCorrelationSlotsScaleHeap(t, rows, true)
	snapshot := db.Snapshot()
	const workers = 6
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statement, err := PrepareStatement(lateralCorrelationSlotsSQL)
			if err != nil {
				errs <- err
				return
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			source := FromDatabase(snapshot, statement.Collection())
			for run := 0; run < 12; run++ {
				cursor, err := statement.RunInto(&exec, source, nil)
				if err != nil {
					errs <- err
					return
				}
				count := 0
				for cursor.Next() {
					count++
				}
				if count != rows {
					errs <- fmt.Errorf("run %d rows = %d, want %d", run, count, rows)
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestSQLLateralCorrelationSlotsWarmExecutionIsAllocationFree(t *testing.T) {
	const rows = 64
	db := lateralCorrelationSlotsScaleHeap(t, rows, true)
	statement, err := PrepareStatement(lateralCorrelationSlotsSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	run := func() {
		cursor, err := statement.RunInto(&exec, source, nil)
		if err != nil {
			panic(err)
		}
		rowsSeen := 0
		for cursor.Next() {
			lateralCorrelationSlotsSink += len(cursor.Cell(1).Payload())
			rowsSeen++
		}
		if rowsSeen != rows {
			panic(fmt.Sprintf("rows = %d, want %d", rowsSeen, rows))
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warm compile-once LATERAL allocations = %.2f, want 0", got)
	}
}

func BenchmarkSQLLateralCorrelationSlotsScaling(b *testing.B) {
	shapes := []struct {
		name    string
		indexed bool
		rows    []int
	}{
		{name: "indexed", indexed: true, rows: []int{1, 8, 64, 512}},
		{name: "scan", indexed: false, rows: []int{1, 8, 64}},
	}
	for _, shape := range shapes {
		for _, rows := range shape.rows {
			rows := rows
			b.Run(fmt.Sprintf("%s/rows=%d", shape.name, rows), func(b *testing.B) {
				db := lateralCorrelationSlotsScaleHeap(b, rows, shape.indexed)
				statement, err := PrepareStatement(lateralCorrelationSlotsSQL)
				if err != nil {
					b.Fatal(err)
				}
				defer statement.Release()
				source := FromDatabase(db.Snapshot(), statement.Collection())
				exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
				defer exec.Release()
				if _, err := statement.RunInto(&exec, source, nil); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ReportMetric(float64(rows), "outer-rows/op")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cursor, err := statement.RunInto(&exec, source, nil)
					if err != nil {
						b.Fatal(err)
					}
					for cursor.Next() {
						lateralCorrelationSlotsSink += len(cursor.Cell(1).Payload())
					}
				}
				b.StopTimer()
				b.ReportMetric(
					float64(b.Elapsed().Nanoseconds())/float64(b.N*rows),
					"ns/outer-row",
				)
			})
		}
	}
}

func lateralCorrelationSlotsScaleHeap(
	t testing.TB,
	rows int,
	indexed bool,
) *store.Database {
	t.Helper()
	db := &store.Database{}
	outer := lateralCorrelationSlotsHeapCollection(t, db, "slot_outer")
	inner := lateralCorrelationSlotsHeapCollection(t, db, "slot_inner")
	if indexed {
		if _, err := inner.CreateIndex(store.IndexDefinition{
			Name: "slot_k", Paths: []string{"/k"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < rows; i++ {
		doc := []byte(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))
		if _, err := outer.Put(fmt.Sprintf("o%d", i), doc); err != nil {
			t.Fatal(err)
		}
		if _, err := inner.Put(fmt.Sprintf("i%d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	if indexed {
		if _, err := inner.BackfillIndex("slot_k", 0); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func lateralCorrelationSlotsHeapCollection(
	t testing.TB,
	db *store.Database,
	name string,
) *store.Collection {
	t.Helper()
	collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func lateralCorrelationSlotsRun(
	t testing.TB,
	statement *Statement,
	exec *Exec,
	source Source,
	args []any,
) []string {
	t.Helper()
	cursor, err := statement.RunInto(exec, source, args)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		cells := make([]string, len(statement.Columns()))
		for column := range cells {
			cells[column] = string(cursor.Cell(column).JSON())
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	return rows
}

// SQL does not promise row order without ORDER BY. Compare multisets so the
// index and scan paths remain free to choose their natural traversal order.
func lateralCorrelationSlotsAssertRows(t testing.TB, got, want []string) {
	t.Helper()
	got = slices.Clone(got)
	want = slices.Clone(want)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("rows = %q, want %q", got, want)
	}
}

// lateralCorrelationSlotsAssertInactive walks every prepared relation child.
// Unlike the older depth-two fixture helper, it is valid for both a single
// LATERAL edge and an arbitrarily nested tree. Besides the public execution
// gate, it verifies that no borrowed correlation tuple remains published in a
// child evaluator after success, cancellation, or a budget failure.
func lateralCorrelationSlotsAssertInactive(t testing.TB, statement *Statement) {
	t.Helper()
	seen := make(map[*Statement]struct{})
	var walk func(*Statement, string)
	walk = func(current *Statement, path string) {
		if current == nil {
			return
		}
		if _, ok := seen[current]; ok {
			return
		}
		seen[current] = struct{}{}
		join := current.relationJoin()
		if join == nil {
			return
		}
		for i := range join.operands {
			op := &join.operands[i]
			next := fmt.Sprintf("%s/operand[%d]", path, i)
			if lateral := op.lateral; lateral != nil {
				if lateral.bindingReady || lateral.bindingActive != 0 {
					t.Fatalf("%s retained active LATERAL binding: ready=%t bytes=%d",
						next, lateral.bindingReady, lateral.bindingActive)
				}
				for slot := range lateral.values {
					if !lateralCorrelationSlotsScalarCleared(lateral.values[slot]) {
						t.Fatalf("%s retained correlation value[%d]", next, slot)
					}
				}
				for slot := range lateral.slots {
					if !lateralCorrelationSlotsScalarCleared(lateral.slots[slot].value) {
						t.Fatalf("%s retained binding slot[%d]", next, slot)
					}
				}
				workspace := &op.exec.Workspace
				if len(workspace.correlations) != 0 ||
					len(workspace.eval.correlations) != 0 {
					t.Fatalf("%s retained child correlations: workspace=%d evaluator=%d",
						next, len(workspace.correlations), len(workspace.eval.correlations))
				}
				if workspace.pool != nil {
					for worker := range workspace.pool.workers {
						if got := len(workspace.pool.workers[worker].eval.correlations); got != 0 {
							t.Fatalf("%s retained worker[%d] correlations: %d", next, worker, got)
						}
					}
				}
			}
			walk(op.stmt, next)
		}
	}
	walk(statement, "statement")
}

func lateralCorrelationSlotsScalarCleared(value scalar) bool {
	return value.kind == kindNull && !value.bval && len(value.num) == 0 &&
		!value.isInt && value.ival == 0 && value.sval == "" && len(value.raw) == 0
}

func lateralCorrelationSlotsIndexSource(
	t testing.TB,
	backend string,
	definition store.IndexDefinition,
) Source {
	t.Helper()
	outerDocs := []string{
		`{"id":"oa1","tenant":"a","k":1}`,
		`{"id":"oa2","tenant":"a","k":2}`,
		`{"id":"ob1","tenant":"b","k":1}`,
		`{"id":"ob2","tenant":"b","k":2}`,
		`{"id":"null","tenant":"a","k":null}`,
		`{"id":"missing","tenant":"a"}`,
		`{"id":"escaped","tenant":"a\"z","k":7}`,
	}
	innerDocs := []string{
		`{"id":"a1-first","tenant":"a","k":1}`,
		`{"id":"a1-second","tenant":"a","k":1}`,
		`{"id":"a2","tenant":"a","k":2}`,
		`{"id":"b1","tenant":"b","k":1}`,
		`{"id":"b2","tenant":"b","k":2}`,
		`{"id":"null","tenant":"a","k":null}`,
		`{"id":"missing","tenant":"a"}`,
		`{"id":"escaped","tenant":"a\"z","k":7}`,
	}
	indexed := definition.Name != ""
	if backend == "heap" {
		db := &store.Database{}
		outer := lateralCorrelationSlotsHeapCollection(t, db, "slot_idx_outer")
		inner := lateralCorrelationSlotsHeapCollection(t, db, "slot_idx_inner")
		if indexed {
			if _, err := inner.CreateIndex(definition); err != nil {
				t.Fatal(err)
			}
		}
		for i, doc := range outerDocs {
			if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
				t.Fatal(err)
			}
		}
		for i, doc := range innerDocs {
			if _, err := inner.Put(fmt.Sprintf("i%d", i), []byte(doc)); err != nil {
				t.Fatal(err)
			}
		}
		if indexed {
			if _, err := inner.BackfillIndex(definition.Name, 0); err != nil {
				t.Fatal(err)
			}
		}
		return FromDatabase(db.Snapshot(), "slot_idx_outer")
	}

	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	outer, err := db.CreateCollection("slot_idx_outer", durableJoinOptions())
	if err != nil {
		t.Fatal(err)
	}
	innerOptions := durableJoinOptions()
	if indexed {
		innerOptions = durableJoinOptions(definition)
	}
	inner, err := db.CreateCollection("slot_idx_inner", innerOptions)
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put([]byte(fmt.Sprintf("o%d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	for i, doc := range innerDocs {
		if _, err := inner.Put([]byte(fmt.Sprintf("i%d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return FromFileDatabase(snapshot, "slot_idx_outer")
}

func lateralCorrelationSlotsMinimumBudget(
	t testing.TB,
	statement *Statement,
	exec *Exec,
	source Source,
	wantRows int,
) int64 {
	t.Helper()
	high := statement.nested.frame.intermediate.used
	if high < 2 {
		high = 2
	}
	low := int64(0)
	for {
		exec.Options.IntermediateBytes = high
		cursor, err := statement.RunInto(exec, source, nil)
		if err == nil {
			rows := 0
			for cursor.Next() {
				rows++
			}
			if rows != wantRows {
				t.Fatalf("budget probe rows = %d, want %d", rows, wantRows)
			}
			break
		}
		var budget *IntermediateBudgetError
		if !errors.As(err, &budget) {
			t.Fatalf("budget discovery at %d: %T %v", high, err, err)
		}
		low = high
		if high > int64(^uint64(0)>>2) {
			t.Fatal("intermediate budget search overflow")
		}
		high *= 2
	}
	for low+1 < high {
		middle := low + (high-low)/2
		exec.Options.IntermediateBytes = middle
		cursor, err := statement.RunInto(exec, source, nil)
		if err == nil {
			for cursor.Next() {
			}
			high = middle
			continue
		}
		var budget *IntermediateBudgetError
		if !errors.As(err, &budget) {
			t.Fatalf("budget binary search at %d: %T %v", middle, err, err)
		}
		low = middle
	}
	return high
}
