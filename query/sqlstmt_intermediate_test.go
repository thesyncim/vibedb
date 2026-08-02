package query

import (
	"errors"
	"testing"
)

func TestStatementRunIntermediateIntoExactSharedBoundary(t *testing.T) {
	catalog := subqueryDatabase(t)
	const source = `SELECT d.id, d.id, d.id FROM (` +
		`SELECT id FROM customers ORDER BY id` +
		`) d ORDER BY d.id`

	probe, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	var probeExec Exec
	probeExec.Options.ResultRows = -1
	probeExec.Options.ResultBytes = -1
	probeExec.Options.IntermediateBytes = -1
	probeCursor, retained, err := probe.RunIntermediateInto(
		&probeExec, FromDatabase(catalog, probe.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows := consumeIntermediateCursor(&probeCursor); rows != 3 {
		t.Fatalf("probe rows = %d, want 3", rows)
	}
	spools := probe.nested.frame.intermediate.used
	result := probeExec.Result.RetainedBytes()
	if spools <= 0 || result <= 0 || retained != spools+result {
		t.Fatalf(
			"retained = %d, spools/result = %d/%d",
			retained, spools, result,
		)
	}
	if probeExec.Result.rootIntermediateActive ||
		probeExec.Result.rootIntermediate != nil {
		t.Fatal("successful execution retained its borrowed root budget")
	}
	probe.Release()
	probeExec.Release()

	rejected, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer rejected.Release()
	var rejectedExec Exec
	defer rejectedExec.Release()
	rejectedExec.Options.ResultRows = -1
	rejectedExec.Options.ResultBytes = -1
	rejectedExec.Options.IntermediateBytes = retained - 1
	_, charged, err := rejected.RunIntermediateInto(
		&rejectedExec, FromDatabase(catalog, rejected.Collection()), nil,
	)
	var intermediate *IntermediateBudgetError
	if charged != 0 || !errors.As(err, &intermediate) ||
		!errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("rejected run = charge %d, error %#v", charged, err)
	}
	if intermediate.Resource != statementRootIntermediateResource ||
		intermediate.Bytes != retained || intermediate.Limit != retained-1 {
		t.Fatalf("intermediate error = %+v, want bytes/limit %d/%d",
			intermediate, retained, retained-1)
	}
	if rejectedExec.Result.RowCount != 0 ||
		rejectedExec.Result.RetainedBytes() != 0 {
		t.Fatalf(
			"rejected root published rows=%d bytes=%d",
			rejectedExec.Result.RowCount,
			rejectedExec.Result.RetainedBytes(),
		)
	}
	if rejected.nested.frame.intermediate.used != 0 ||
		rejectedExec.Result.rootIntermediateActive ||
		rejectedExec.Result.rootIntermediate != nil {
		t.Fatalf(
			"failure retained frame/result budget = %d/%t/%p",
			rejected.nested.frame.intermediate.used,
			rejectedExec.Result.rootIntermediateActive,
			rejectedExec.Result.rootIntermediate,
		)
	}

	rejectedExec.Options.IntermediateBytes = retained
	cursor, charged, err := rejected.RunIntermediateInto(
		&rejectedExec, FromDatabase(catalog, rejected.Collection()), nil,
	)
	if err != nil || charged != retained || consumeIntermediateCursor(&cursor) != 3 {
		t.Fatalf("exact boundary = charge %d, rows %d, error %v",
			charged, cursor.Row()+1, err)
	}
}

func TestStatementRunIntermediateIntoPreservesStricterResultLimits(t *testing.T) {
	catalog := subqueryDatabase(t)
	shapes := []struct {
		name string
		sql  string
	}{
		{name: "direct", sql: `SELECT id FROM customers ORDER BY id`},
		{
			name: "nested",
			sql: `SELECT d.id, d.id, d.id FROM (` +
				`SELECT id FROM customers ORDER BY id` +
				`) d ORDER BY d.id`,
		},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			probe, err := PrepareStatement(shape.sql)
			if err != nil {
				t.Fatal(err)
			}
			var probeExec Exec
			probeExec.Options.ResultRows = -1
			probeExec.Options.ResultBytes = -1
			probeExec.Options.IntermediateBytes = -1
			_, retainedRequired, err := probe.RunIntermediateInto(
				&probeExec, FromDatabase(catalog, probe.Collection()), nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			resultRequired := probeExec.Result.RetainedBytes()
			frameRequired := retainedRequired - resultRequired
			probe.Release()
			probeExec.Release()
			if resultRequired <= 1 || frameRequired < 0 {
				t.Fatalf("required result/frame = %d/%d",
					resultRequired, frameRequired)
			}
			exact, err := PrepareStatement(shape.sql)
			if err != nil {
				t.Fatal(err)
			}
			var exactExec Exec
			exactExec.Options.ResultRows = 3
			exactExec.Options.ResultBytes = resultRequired
			exactExec.Options.IntermediateBytes = retainedRequired
			exactCursor, exactRetained, err := exact.RunIntermediateInto(
				&exactExec, FromDatabase(catalog, exact.Collection()), nil,
			)
			if err != nil || exactRetained != retainedRequired ||
				consumeIntermediateCursor(&exactCursor) != 3 {
				t.Fatalf("exact result/intermediate boundary = %d/%d, error %v",
					exactRetained, retainedRequired, err)
			}
			exact.Release()
			exactExec.Release()

			tests := []struct {
				name         string
				resultBytes  int64
				intermediate int64
				wantResult   bool
			}{
				{
					name:         "result stricter",
					resultBytes:  resultRequired - 1,
					intermediate: retainedRequired + 1,
					wantResult:   true,
				},
				{
					name:         "intermediate stricter",
					resultBytes:  resultRequired + 1,
					intermediate: retainedRequired - 1,
				},
				{
					name:         "result wins equal remainder tie",
					resultBytes:  resultRequired - 1,
					intermediate: frameRequired + resultRequired - 1,
					wantResult:   true,
				},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					statement, err := PrepareStatement(shape.sql)
					if err != nil {
						t.Fatal(err)
					}
					defer statement.Release()
					var exec Exec
					defer exec.Release()
					exec.Options.ResultRows = -1
					exec.Options.ResultBytes = test.resultBytes
					exec.Options.IntermediateBytes = test.intermediate
					_, retained, err := statement.RunIntermediateInto(
						&exec, FromDatabase(catalog, statement.Collection()), nil,
					)
					if retained != 0 || exec.Result.RowCount != 0 ||
						exec.Result.RetainedBytes() != 0 {
						t.Fatalf("failure published retained/result = %d/%d/%d",
							retained, exec.Result.RowCount, exec.Result.RetainedBytes())
					}
					var resultErr *ResultBudgetError
					var intermediateErr *IntermediateBudgetError
					if test.wantResult {
						if !errors.As(err, &resultErr) ||
							resultErr.Bytes != resultRequired ||
							resultErr.ByteLimit != test.resultBytes {
							t.Fatalf("error = %#v, want result %d/%d",
								err, resultRequired, test.resultBytes)
						}
					} else if !errors.As(err, &intermediateErr) ||
						intermediateErr.Bytes != retainedRequired ||
						intermediateErr.Limit != test.intermediate {
						t.Fatalf("error = %#v, want intermediate %d/%d",
							err, retainedRequired, test.intermediate)
					}
				})
			}

			statement, err := PrepareStatement(shape.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			var exec Exec
			exec.Options.ResultRows = 2
			exec.Options.ResultBytes = -1
			exec.Options.IntermediateBytes = -1
			_, retained, err := statement.RunIntermediateInto(
				&exec, FromDatabase(catalog, statement.Collection()), nil,
			)
			var rowLimit *ResultBudgetError
			if retained != 0 || !errors.As(err, &rowLimit) ||
				rowLimit.Rows != 3 || rowLimit.RowLimit != 2 {
				t.Fatalf("row limit = retained %d, error %#v", retained, err)
			}
		})
	}
}

func TestStatementRunIntermediateIntoRetainsEveryRootPipeline(t *testing.T) {
	catalog := subqueryDatabase(t)
	joinDatabase := relationJoinDatabase(t)
	windowDatabase := windowStatementDatabase(t)
	tests := []struct {
		name      string
		sql       string
		src       func(*Statement) Source
		wantSpool bool
	}{
		{
			name: "materialized CTE",
			sql: `WITH c AS MATERIALIZED (` +
				`SELECT id FROM customers ORDER BY id` +
				`) SELECT id, id, id FROM c ORDER BY id`,
			src: func(statement *Statement) Source {
				return FromDatabase(catalog, statement.Collection())
			},
			wantSpool: true,
		},
		{
			name: "join chain",
			sql: `SELECT a.k, a.label, b.label, a.label, b.label ` +
				`FROM a FULL JOIN b USING (k) ORDER BY a.k, a.label, b.label`,
			src: func(statement *Statement) Source {
				return FromDatabase(joinDatabase.Snapshot(), statement.Collection())
			},
			wantSpool: true,
		},
		{
			name: "window",
			sql: `SELECT id, id, id, ` +
				`ROW_NUMBER() OVER (PARTITION BY team ORDER BY score) AS n ` +
				`FROM events ORDER BY id`,
			src: func(statement *Statement) Source {
				return FromDatabase(windowDatabase.Snapshot(), statement.Collection())
			},
			wantSpool: true,
		},
		{
			name: "set tree",
			sql: `(SELECT id FROM customers) UNION ALL ` +
				`(SELECT id FROM customers) ORDER BY id`,
			src: func(statement *Statement) Source {
				return FromDatabase(catalog, statement.Collection())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if statement.nested == nil {
				t.Fatal("pipeline unexpectedly has no shared statement frame")
			}
			var exec Exec
			defer exec.Release()
			exec.Options.ResultRows = -1
			exec.Options.ResultBytes = -1
			exec.Options.IntermediateBytes = -1
			cursor, retained, err := statement.RunIntermediateInto(
				&exec, test.src(statement), nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if rows := consumeIntermediateCursor(&cursor); rows == 0 {
				t.Fatal("pipeline returned no rows")
			}
			spools := statement.nested.frame.intermediate.used
			result := exec.Result.RetainedBytes()
			if result <= 0 || retained != spools+result ||
				test.wantSpool && spools <= 0 {
				t.Fatalf("retained = %d, spools/result = %d/%d",
					retained, spools, result)
			}
		})
	}
}

func TestStatementRunIntermediateIntoCancellationIsAtomicAndReusable(t *testing.T) {
	catalog := subqueryDatabase(t)
	statement, err := PrepareStatement(
		`SELECT d.id FROM (SELECT id FROM customers ORDER BY id) d ORDER BY d.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	exec.Options.ResultRows = -1
	exec.Options.ResultBytes = -1
	exec.Options.IntermediateBytes = -1

	stale, wantRetained, err := statement.RunIntermediateInto(
		&exec, FromDatabase(catalog, statement.Collection()), nil,
	)
	if err != nil || !stale.Next() || wantRetained <= 0 {
		t.Fatalf("warm run = retained %d, row %d, error %v",
			wantRetained, stale.Row(), err)
	}
	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	_, retained, err := statement.RunIntermediateInto(
		&exec, FromDatabase(catalog, statement.Collection()), nil,
	)
	if retained != 0 || !errors.Is(err, ErrCanceled) || stale.Next() {
		t.Fatalf("canceled run = retained %d, stale %t, error %v",
			retained, stale.Row() >= 0, err)
	}
	if exec.Result.RowCount != 0 || exec.Result.RetainedBytes() != 0 ||
		statement.nested.frame.intermediate.used != 0 ||
		exec.Result.rootIntermediateActive || exec.Result.rootIntermediate != nil {
		t.Fatalf("canceled state = rows/bytes/frame/active/pointer %d/%d/%d/%t/%p",
			exec.Result.RowCount, exec.Result.RetainedBytes(),
			statement.nested.frame.intermediate.used,
			exec.Result.rootIntermediateActive, exec.Result.rootIntermediate)
	}

	cancel.Reset()
	cursor, retained, err := statement.RunIntermediateInto(
		&exec, FromDatabase(catalog, statement.Collection()), nil,
	)
	if err != nil || retained != wantRetained ||
		consumeIntermediateCursor(&cursor) != 3 {
		t.Fatalf("recovery = retained %d/%d, row %d, error %v",
			retained, wantRetained, cursor.Row(), err)
	}
}

func TestStatementRunIntermediateIntoWarmPathsAllocateZero(t *testing.T) {
	catalog := subqueryDatabase(t)
	tests := []struct {
		name string
		sql  string
	}{
		{name: "direct", sql: `SELECT id FROM customers ORDER BY id`},
		{
			name: "nested shared frame",
			sql: `SELECT d.id FROM (` +
				`SELECT id FROM customers ORDER BY id` +
				`) d ORDER BY d.id`,
		},
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
			exec.Options.ResultRows = -1
			exec.Options.ResultBytes = -1
			exec.Options.IntermediateBytes = -1
			source := FromDatabase(catalog, statement.Collection())
			ordinary := func() {
				cursor, runErr := statement.RunInto(&exec, source, nil)
				if runErr != nil {
					panic(runErr)
				}
				for cursor.Next() {
					sqlSink += len(cursor.Cell(0).Payload())
				}
			}
			ordinary()
			ordinary()
			if allocs := testing.AllocsPerRun(100, ordinary); allocs != 0 {
				t.Fatalf("warm ordinary RunInto allocations = %.2f, want 0", allocs)
			}

			intermediate := func() {
				cursor, retained, runErr := statement.RunIntermediateInto(
					&exec, source, nil,
				)
				if runErr != nil {
					panic(runErr)
				}
				for cursor.Next() {
					sqlSink += len(cursor.Cell(0).Payload())
				}
				sqlSink += int(retained & 1)
			}
			intermediate()
			intermediate()
			if allocs := testing.AllocsPerRun(100, intermediate); allocs != 0 {
				t.Fatalf("warm RunIntermediateInto allocations = %.2f, want 0", allocs)
			}
		})
	}
}

func BenchmarkStatementRootIntermediateAdmission(b *testing.B) {
	catalog := subqueryDatabase(b)
	statement, err := PrepareStatement(`SELECT id FROM customers ORDER BY id`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	exec.Options.ResultRows = -1
	exec.Options.ResultBytes = -1
	exec.Options.IntermediateBytes = -1
	source := FromDatabase(catalog, statement.Collection())

	b.Run("ordinary_RunInto", func(b *testing.B) {
		cursor, err := statement.RunInto(&exec, source, nil)
		if err != nil {
			b.Fatal(err)
		}
		consumeIntermediateCursor(&cursor)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			cursor, err = statement.RunInto(&exec, source, nil)
			if err != nil {
				b.Fatal(err)
			}
			sqlSink += consumeIntermediateCursor(&cursor)
		}
	})

	b.Run("shared_root_RunIntermediateInto", func(b *testing.B) {
		cursor, retained, err := statement.RunIntermediateInto(&exec, source, nil)
		if err != nil {
			b.Fatal(err)
		}
		consumeIntermediateCursor(&cursor)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			cursor, retained, err = statement.RunIntermediateInto(&exec, source, nil)
			if err != nil {
				b.Fatal(err)
			}
			sqlSink += consumeIntermediateCursor(&cursor) + int(retained&1)
		}
	})
}

func consumeIntermediateCursor(cursor *Cursor) int {
	rows := 0
	for cursor.Next() {
		rows++
	}
	return rows
}
