package query

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestSQLScalarStatementExactArithmeticNullAndPredicate(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","n":9007199254740993,"m":2,"s":"x"}`,
		`{"id":"b","n":null,"m":4,"s":"y"}`,
		`{"id":"c","m":3,"s":"z"}`,
	)
	statement, err := PrepareStatement(`
		SELECT id, n + ? AS total, -m AS neg, s || '!' AS label
		FROM docs
		WHERE m * 2 >= ? AND id <> 'z'
		ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Columns(); len(got) != 4 || got[0] != "id" ||
		got[1] != "total" || got[2] != "neg" || got[3] != "label" {
		t.Fatalf("columns = %v", got)
	}
	schema := statement.AppendSchema(nil)
	if len(schema) != 4 || schema[0].Type != TypeAny || schema[1].Type != TypeNumber ||
		schema[2].Type != TypeNumber || schema[3].Type != TypeString ||
		schema[0].Representation != OutputJSON ||
		schema[1].Representation != OutputSQLNumber ||
		schema[2].Representation != OutputSQLNumber ||
		schema[3].Representation != OutputSQLText {
		t.Fatalf("scalar schema = %+v", schema)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), []any{int64(1), int64(4)})
	if err != nil {
		t.Fatal(err)
	}
	want := [][4]string{
		{`"a"`, `9.007199254740994e15`, `-2`, `"x!"`},
		{`"b"`, `null`, `-4`, `"y!"`},
	}
	for row := range want {
		if !cursor.Next() {
			t.Fatalf("missing row %d", row)
		}
		for column := range want[row] {
			if got := string(cursor.Cell(column).JSON()); got != want[row][column] {
				t.Fatalf("row %d column %d = %s, want %s", row, column, got, want[row][column])
			}
		}
	}
	if cursor.Next() {
		t.Fatal("unexpected third row")
	}
}

func TestSQLScalarStatementTypeErrorPositionNoPartialAndRecovery(t *testing.T) {
	segment := mustSegment(t, `{"v":"ok"}`, `{"v":7}`)
	statement, err := PrepareStatement(`SELECT v || '!' AS x FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	_, err = statement.RunInto(&exec, FromSegment(segment), nil)
	var typed *ScalarTypeError
	if !errors.As(err, &typed) || !errors.Is(err, ErrScalarType) || typed.Position() != 9 {
		t.Fatalf("type error = %T %v", err, err)
	}
	if got, want := err.Error(), "query: scalar concatenation at byte 9 does not accept number and string: query: scalar expression type mismatch"; got != want {
		t.Fatalf("type error text = %q, want %q", got, want)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed execution published %d rows", exec.Result.RowCount)
	}

	valid := mustSegment(t, `{"v":"again"}`)
	cursor, err := statement.RunInto(&exec, FromSegment(valid), nil)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != `"again!"` || cursor.Next() {
		t.Fatal("prepared statement did not recover after a row type failure")
	}
}

func TestSQLScalarStatementDivisionByZeroPositionAndWarmZeroAlloc(t *testing.T) {
	segment := mustSegment(t, `{"n":9}`)
	statement, err := PrepareStatement(`SELECT n / ? AS q FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	_, err = statement.RunInto(&exec, FromSegment(segment), []any{int64(0)})
	var zero *ScalarDivisionByZeroError
	if !errors.As(err, &zero) || zero.Position() != 9 {
		t.Fatalf("division error = %T %v", err, err)
	}
	one := int64(3)
	args := []any{&one}
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), args)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if !cursor.Next() || string(cursor.Cell(0).Payload()) != "3" || cursor.Next() {
			t.Fatal("unexpected warmed scalar result")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed scalar statement allocated %.2f/run", allocs)
	}
}

func TestSQLScalarStatementConstantPredicateAndParameterProjection(t *testing.T) {
	segment := mustSegment(t, `{"a":1}`, `{"a":2}`)
	statement, err := PrepareStatement(`SELECT ? AS value FROM docs WHERE 1 = 1`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), []any{int64(7)})
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 2; row++ {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != "7" {
			t.Fatalf("parameter projection row %d is missing or wrong", row)
		}
	}
	if cursor.Next() {
		t.Fatal("constant predicate returned too many rows")
	}

	falseStatement, err := PrepareStatement(`SELECT a FROM docs WHERE 1 = 0`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = falseStatement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next() {
		t.Fatal("constant false predicate retained a row")
	}
}

func TestSQLScalarStatementMidMaterializationBudgetIsAtomicAndReusable(t *testing.T) {
	segment := mustSegment(t, `{"s":"aa"}`, `{"s":"bb"}`)
	statement, err := PrepareStatement(`SELECT s || '!' AS value FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	shape := resultColumnBytes + 2*resultCellBytes
	// Each output holds three decoded bytes and five JSON bytes. This admits
	// the first cell, then rejects the second after materialization has begun.
	var exec Exec
	exec.Options.ResultBytes = shape + 8
	_, err = statement.RunInto(&exec, FromSegment(segment), nil)
	if !errors.Is(err, ErrResultBudget) {
		t.Fatalf("budget error = %T %v", err, err)
	}
	if exec.Result.RowCount != 0 || exec.Result.resultBytesUsed != 0 {
		t.Fatalf("partial result survived: rows=%d bytes=%d",
			exec.Result.RowCount, exec.Result.resultBytesUsed)
	}
	for i := range exec.Result.Columns {
		if len(exec.Result.Columns[i].Cells) != 0 {
			t.Fatalf("partial column %d retained %d cells", i, len(exec.Result.Columns[i].Cells))
		}
	}

	exec.Options.ResultBytes = 0
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	for _, want := range []string{`"aa!"`, `"bb!"`} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want {
			t.Fatalf("recovery row != %s", want)
		}
	}
	if cursor.Next() {
		t.Fatal("recovery returned extra row")
	}
}

func BenchmarkSQLScalarStatementWarm(b *testing.B) {
	segment := mustSegment(b, `{"n":9}`)
	statement, err := PrepareStatement(`SELECT n * ? + 1 AS q FROM docs`)
	if err != nil {
		b.Fatal(err)
	}
	var exec Exec
	three := int64(3)
	args := []any{&three}
	for range 2 {
		if _, err := statement.RunInto(&exec, FromSegment(segment), args); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := statement.RunInto(&exec, FromSegment(segment), args); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSQLScalarStatementMultipleConstantOutputsReuse(t *testing.T) {
	segment := mustSegment(t, `{"id":1}`)
	statement, err := PrepareStatement(`SELECT ? AS a, ? + 1 AS b, 'x' || ? AS c FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	args := []any{int64(2), int64(3), "y"}
	for run := 0; run < 3; run++ {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), args)
		if runErr != nil {
			t.Fatalf("run %d: %v", run, runErr)
		}
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != "2" ||
			string(cursor.Cell(1).JSON()) != "4" || string(cursor.Cell(2).JSON()) != `"xy"` ||
			cursor.Next() {
			t.Fatalf("run %d returned wrong constant tuple", run)
		}
	}
}

func TestSQLScalarMalformedDependencyResultReturnsErrorWithoutPanic(t *testing.T) {
	runtime := &statementScalar{deps: make([]statementScalarDependencySpec, 1)}
	result := &Result{
		RowCount: 4,
		Columns:  []ResultColumn{{Cells: make([]Cell, 3)}},
	}
	err := runtime.validateResult(result)
	var shape *ScalarResultShapeError
	if !errors.As(err, &shape) || !errors.Is(err, ErrScalarResultShape) ||
		shape.Dependency != 0 || shape.Rows != 4 || shape.Cells != 3 {
		t.Fatalf("shape error = %T %v", err, err)
	}
}

func TestSQLScalarFilteredOrderedTailDependencyCardinalityReuse(t *testing.T) {
	segment := mustSegment(t,
		`{"id":1,"age":30,"name":"a"}`,
		`{"id":2,"age":21,"name":"b"}`,
		`{"id":3,"age":null,"name":"c"}`,
		`{"id":4,"age":17,"name":"d"}`,
		`{"id":5,"age":45,"name":"e"}`,
		`{"id":6,"age":30}`,
		`{"id":7,"age":21,"name":"g"}`,
	)
	statement, err := PrepareStatement(`
		SELECT id + 1 AS next, name || '!' AS label
		FROM docs WHERE age * 2 >= 60 ORDER BY id LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 20; run++ {
		var exec Exec
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatalf("run %d: %v", run, runErr)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 3 {
			t.Fatalf("run %d rows = %d, want 3", run, rows)
		}
	}
}

func TestSQLScalarFilteredOrderedConcurrentStatements(t *testing.T) {
	segment := mustSegment(t,
		`{"id":1,"age":30}`, `{"id":2,"age":20}`,
		`{"id":3,"age":40}`, `{"id":4,"age":10}`,
	)
	const source = `SELECT id + 1 FROM docs WHERE age * 2 >= 60 ORDER BY id`
	var wait sync.WaitGroup
	errorsOut := make(chan error, 4)
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			statement, err := PrepareStatement(source)
			if err != nil {
				errorsOut <- err
				return
			}
			defer statement.Release()
			var exec Exec
			defer exec.Release()
			for range 20 {
				cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
				if runErr != nil {
					errorsOut <- runErr
					return
				}
				rows := 0
				for cursor.Next() {
					rows++
				}
				if rows != 2 {
					errorsOut <- fmt.Errorf("scalar concurrent rows = %d, want 2", rows)
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
}
