package query

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func TestRelationSpoolPublicationExactlyMatchesSizingPass(t *testing.T) {
	large := []byte(`{"blob":"` + strings.Repeat("x", 128<<10) + `"}`)
	var decoded []byte
	largeCell := cellFromScalar(classifyRawInto(
		vibejson.RawValue{Src: large}, &decoded,
	))
	decoded = decoded[:0]
	escapedRaw := []byte(`"raw\nstring"`)
	escapedCell := cellFromScalar(classifyRawInto(
		vibejson.RawValue{Src: escapedRaw}, &decoded,
	))
	cells := []Cell{
		{kind: TypeString, text: "computed α"},
		{kind: TypeString, text: "line\n\"slash\\"},
		{kind: TypeNumber, flag: cellInteger, word: uint64(42)},
		{kind: TypeNumber, word: math.Float64bits(3.25)},
		largeCell,
		cellFromScalar(scalar{kind: kindNull}),
		nullCell(),
		escapedCell,
	}
	result := Result{Columns: make([]ResultColumn, len(cells)), RowCount: 1}
	for column := range cells {
		result.Columns[column].Cells = []Cell{cells[column]}
	}
	statement := Statement{outputs: len(cells)}
	cursor := statement.cursor(&result)
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var spool relationSpool
	charge, err := spool.materialize(
		cursor, len(cells), &frame, nil, "test relation spool",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.intermediate.release(charge)
	if len(spool.data) != spool.plannedData {
		t.Fatalf("published data = %d bytes, sizing pass planned %d",
			len(spool.data), spool.plannedData)
	}
	wantRaw := []string{
		`"computed α"`, `"line\n\"slash\\"`, "42", "3.25",
		string(large), "", "null", string(escapedRaw),
	}
	for column := range wantRaw {
		if got := string(spool.columns[column][0].raw); got != wantRaw[column] {
			t.Fatalf("column %d raw = %q, want %q", column, got, wantRaw[column])
		}
	}
	if got := spool.columns[0][0].sval; got != "computed α" {
		t.Fatalf("computed string text = %q", got)
	}
	if got := spool.columns[1][0].sval; got != "line\n\"slash\\" {
		t.Fatalf("escaped computed string text = %q", got)
	}
	if got := spool.columns[7][0].sval; got != "raw\nstring" {
		t.Fatalf("escaped raw string text = %q", got)
	}
	if spool.columns[5][0].raw != nil {
		t.Fatalf("missing raw = %q, want nil", spool.columns[5][0].raw)
	}
}

func TestRelationSpoolPublicationRefusesUnmeasuredGrowth(t *testing.T) {
	var spool relationSpool
	if err := spool.begin(1, 1, 1); err != nil {
		t.Fatal(err)
	}
	beforeCap := cap(spool.data)
	_, err := spool.ownCell(
		Cell{kind: TypeString, text: strings.Repeat("x", 4096)}, nil,
	)
	if !errors.Is(err, errRelationSpoolSizing) {
		t.Fatalf("undersized publication error = %v, want sizing invariant", err)
	}
	if len(spool.data) != 0 || cap(spool.data) != beforeCap {
		t.Fatalf("rejected publication changed data len/cap to %d/%d, want 0/%d",
			len(spool.data), cap(spool.data), beforeCap)
	}
}

func TestRelationSpoolMixedCellWarmMaterializationIsAllocationFree(t *testing.T) {
	large := []byte(`{"blob":"` + strings.Repeat("z", 64<<10) + `"}`)
	var decoded []byte
	cells := []Cell{
		{kind: TypeString, text: "computed\nstring"},
		{kind: TypeNumber, flag: cellInteger, word: uint64(99)},
		{kind: TypeNumber, word: math.Float64bits(1.25)},
		cellFromScalar(classifyRawInto(vibejson.RawValue{Src: large}, &decoded)),
		cellFromScalar(scalar{kind: kindNull}),
		nullCell(),
	}
	result := Result{Columns: make([]ResultColumn, len(cells)), RowCount: 1}
	for column := range cells {
		result.Columns[column].Cells = []Cell{cells[column]}
	}
	statement := Statement{outputs: len(cells)}
	var spool relationSpool
	run := func() {
		var frame statementFrame
		if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
			panic(err)
		}
		charge, err := spool.materialize(
			statement.cursor(&result), len(cells), &frame, nil,
			"allocation relation spool",
		)
		if err != nil {
			panic(err)
		}
		frame.intermediate.release(charge)
		sqlSink += len(spool.data)
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed mixed relation materialization allocated %.1f times, want 0", got)
	}
}

func TestRelationSpoolExecutionMatchesSegmentOracle(t *testing.T) {
	rows := [][]string{
		{`1`, `{"x":"one"}`},
		{`3.00`, `{"x":"three"}`},
		{`2e0`, `{"x":"two"}`},
		{`null`, `{"x":"nil"}`},
	}
	spool := buildRelationSpoolForTest(t, rows)
	segment := mustSegment(t,
		`{"0":1,"1":{"x":"one"}}`,
		`{"0":3.00,"1":{"x":"three"}}`,
		`{"0":2e0,"1":{"x":"two"}}`,
		`{"0":null,"1":{"x":"nil"}}`,
	)
	plan := Select(Path("/0"), Path("/1/x")).
		Where(Cmp("/0", Gt, 1)).
		OrderBy("/0", Desc)
	var relationExec, segmentExec Exec
	if err := plan.RunInto(&relationExec, fromRelationSpool(&spool)); err != nil {
		t.Fatalf("relation execution: %v", err)
	}
	if err := plan.RunInto(&segmentExec, FromSegment(segment)); err != nil {
		t.Fatalf("segment oracle: %v", err)
	}
	assertResultCellsEqual(t, relationExec.Result, segmentExec.Result)
}

func TestRelationSpoolExecutionErrorsClearBorrowsAndExecReuses(t *testing.T) {
	spool := buildRelationSpoolForTest(t, [][]string{
		{`1`, `{"x":"one"}`},
		{`2`, `{"x":"two"}`},
		{`3`, `{"x":"one"}`},
	})
	tests := []struct {
		name string
		plan *Query
	}{
		{
			name: "ordered projection result budget",
			plan: Select(Path("/1/x")).OrderBy("/1/x", Asc),
		},
		{
			name: "grouped aggregate result budget",
			plan: Select(Path("/1/x"), Count()).
				GroupBy("/1/x").OrderBy("/1/x", Asc),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var exec Exec
			exec.Options.ResultBytes = 1
			err := test.plan.RunInto(&exec, fromRelationSpool(&spool))
			var budgetErr *ResultBudgetError
			if !errors.As(err, &budgetErr) {
				t.Fatalf("RunInto error = %v, want result budget", err)
			}
			assertWorkspaceBorrowedViewsCleared(t, &exec.Workspace)
			if exec.Result.RowCount != 0 {
				t.Fatalf("failed result retained %d rows", exec.Result.RowCount)
			}

			exec.Options.ResultBytes = -1
			if err := test.plan.RunInto(&exec, fromRelationSpool(&spool)); err != nil {
				t.Fatalf("Exec reuse: %v", err)
			}
			if exec.Result.RowCount == 0 {
				t.Fatal("Exec reuse returned no rows")
			}
		})
	}
}

func TestSQLRelationSpoolPreservesExactOwnedValuesAndMissing(t *testing.T) {
	var database store.Database
	docs, err := database.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Put("one", []byte(
		`{"explicit":null,"decimal":12345678901234567890.0100,`+
			`"nested":{"a":[true,{"x":2}]},"escaped":"line\nend"}`,
	)); err != nil {
		t.Fatal(err)
	}
	stmt, err := PrepareStatement(
		`SELECT d.missing, d.explicit, d.decimal, d.nested.a[1].x, d.escaped ` +
			`FROM (SELECT missing, explicit, decimal, nested, escaped FROM docs) d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(database.Snapshot(), stmt.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("relation returned no row")
	}
	want := []string{
		"null", "null", "12345678901234567890.0100", "2", `"line\nend"`,
	}
	for i := range want {
		if got := cursor.Cell(i).String(); got != want[i] {
			t.Fatalf("cell %d = %q, want %q", i, got, want[i])
		}
	}
	d := stmt.derived()
	if d.spool.columns[0][0].raw != nil {
		t.Fatalf("missing output raw = %q, want nil missing marker", d.spool.columns[0][0].raw)
	}
	if got := string(d.spool.columns[1][0].raw); got != "null" {
		t.Fatalf("explicit null raw = %q, want null", got)
	}
	text, ok := cursor.Cell(4).Text()
	if !ok || text != "line\nend" {
		t.Fatalf("escaped text = (%q,%t), want decoded newline", text, ok)
	}
}

func TestSQLRelationSpoolBudgetRejectsBeforeGrowth(t *testing.T) {
	catalog := subqueryDatabase(t)
	const text = `SELECT d.id, d.tier FROM (` +
		`SELECT id, tier FROM customers ORDER BY id` +
		`) d`

	probe, err := PrepareStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	child := probe.derived().stmt
	var childExec Exec
	if _, err := child.RunInto(
		&childExec, FromDatabase(catalog, child.Collection()), nil,
	); err != nil {
		t.Fatal(err)
	}
	resultBytes := childExec.Result.resultBytesUsed
	var probeExec Exec
	if _, err := probe.RunInto(
		&probeExec, FromDatabase(catalog, probe.Collection()), nil,
	); err != nil {
		t.Fatal(err)
	}
	spoolBytes := probe.derived().activeBytes
	probe.Release()
	if resultBytes <= 0 || spoolBytes <= 0 {
		t.Fatalf("charges = result %d spool %d, want positive", resultBytes, spoolBytes)
	}

	stmt, err := PrepareStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	exec.Options.IntermediateBytes = resultBytes + spoolBytes - 1
	_, err = stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	var budgetErr *IntermediateBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Resource != "derived relation spool" {
		t.Fatalf("RunInto error = %#v, want relation spool budget", err)
	}
	spool := &stmt.derived().spool
	if cap(spool.columns) != 0 || cap(spool.data) != 0 || spool.rows != 0 {
		t.Fatalf("rejected spool grew columns=%d data=%d rows=%d",
			cap(spool.columns), cap(spool.data), spool.rows)
	}
}

func TestSQLRelationSpoolStaleViewsInvalidateAndRecover(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT d.id FROM (SELECT id FROM customers ORDER BY id) d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	stale, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !stale.Next() {
		t.Fatal("first execution returned no row")
	}
	exec.Options.IntermediateBytes = -2
	if _, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	); err == nil {
		t.Fatal("invalid execution option succeeded")
	}
	spool := &stmt.derived().spool
	if spool.rows != 0 || len(spool.data) != 0 || exec.Result.RowCount != 0 || stale.Next() {
		t.Fatalf("failed attempt retained stale state: spool rows=%d data=%d result=%d",
			spool.rows, len(spool.data), exec.Result.RowCount)
	}
	for column := range spool.columns {
		if len(spool.columns[column]) != 0 {
			t.Fatalf("stale spool column %d retained %d rows", column, len(spool.columns[column]))
		}
	}

	exec.Options.IntermediateBytes = 0
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	if err != nil || !cursor.Next() || cursor.Cell(0).String() != `"c1"` {
		t.Fatalf("recovery = cursor(%v) err %v", cursor.Row(), err)
	}
}

func TestSQLRelationSpoolCancellationDoesNotPublishAndRecovers(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT d.id FROM (SELECT id FROM customers ORDER BY id) d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var cancel CancelFlag
	cancel.Cancel()
	var exec Exec
	exec.Options.Cancel = &cancel
	if _, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled RunInto error = %v, want ErrCanceled", err)
	}
	spool := &stmt.derived().spool
	if spool.rows != 0 || len(spool.data) != 0 || exec.Result.RowCount != 0 {
		t.Fatalf("canceled execution published rows=%d data=%d result=%d",
			spool.rows, len(spool.data), exec.Result.RowCount)
	}
	cancel.Reset()
	cursor, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	)
	if err != nil || !cursor.Next() {
		t.Fatalf("post-cancel recovery failed: row=%d err=%v", cursor.Row(), err)
	}
	cancel.Cancel()
	if _, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancel after success error = %v, want ErrCanceled", err)
	}
	if spool.rows != 0 || len(spool.data) != 0 || exec.Result.RowCount != 0 {
		t.Fatalf("cancel after success retained rows=%d data=%d result=%d",
			spool.rows, len(spool.data), exec.Result.RowCount)
	}
	assertWorkspaceBorrowedViewsCleared(t, &exec.Workspace)
	cancel.Reset()
	if _, err := stmt.RunInto(
		&exec, FromDatabase(catalog, stmt.Collection()), nil,
	); err != nil {
		t.Fatalf("second post-cancel reuse: %v", err)
	}
}

func TestSQLRelationSpoolWarmExecutionIsAllocationFree(t *testing.T) {
	catalog := subqueryDatabase(t)
	stmt, err := PrepareStatement(
		`SELECT d.id, d.score FROM (` +
			`SELECT id, score FROM customers WHERE tier = ?` +
			`) d WHERE d.id <> ? ORDER BY d.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	defer exec.Release()
	args := []any{"pro", "missing"}
	run := func() {
		cursor, err := stmt.RunInto(
			&exec, FromDatabase(catalog, stmt.Collection()), args,
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
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed relation spool execution allocated %.1f times, want 0", got)
	}
}

func buildRelationSpoolForTest(t testing.TB, rows [][]string) relationSpool {
	t.Helper()
	columns := 0
	if len(rows) != 0 {
		columns = len(rows[0])
	}
	cells := make([][]Cell, len(rows))
	payload := int64(0)
	for row := range rows {
		if len(rows[row]) != columns {
			t.Fatalf("row %d has %d columns, want %d", row, len(rows[row]), columns)
		}
		cells[row] = make([]Cell, columns)
		for column, src := range rows[row] {
			var decoded []byte
			value := classifyRawInto(vibejson.RawValue{Src: []byte(src)}, &decoded)
			cells[row][column] = cellFromScalar(value)
			payload += int64(relationCellOwnedBytes(cells[row][column]))
		}
	}
	var spool relationSpool
	if err := spool.begin(len(rows), columns, payload); err != nil {
		t.Fatal(err)
	}
	for row := range cells {
		for column := range cells[row] {
			value, err := spool.ownCell(cells[row][column], nil)
			if err != nil {
				t.Fatal(err)
			}
			spool.columns[column][row] = value
		}
	}
	return spool
}

func assertResultCellsEqual(t testing.TB, got, want Result) {
	t.Helper()
	if got.RowCount != want.RowCount || len(got.Columns) != len(want.Columns) {
		t.Fatalf("result shape = %dx%d, want %dx%d",
			got.RowCount, len(got.Columns), want.RowCount, len(want.Columns))
	}
	for column := range got.Columns {
		if got.Columns[column].Header != want.Columns[column].Header {
			t.Fatalf("header %d = %q, want %q", column,
				got.Columns[column].Header, want.Columns[column].Header)
		}
		for row := 0; row < got.RowCount; row++ {
			left, right := got.Columns[column].Cells[row], want.Columns[column].Cells[row]
			if left.Kind() != right.Kind() || left.String() != right.String() {
				t.Fatalf("cell [%d,%d] = (%v,%q), want (%v,%q)",
					row, column, left.Kind(), left.String(), right.Kind(), right.String())
			}
		}
	}
}
