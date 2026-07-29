package query

import (
	"fmt"
	"strings"
	"testing"
)

func TestSQLStatementRecoversAfterRejectedBinding(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":2}`, `{"a":3}`)
	tests := []struct {
		name string
		sql  string
		bad  any
		good any
		want string
	}{
		{
			name: "predicate type",
			sql:  `SELECT a FROM docs WHERE a >= ? ORDER BY a`,
			bad:  struct{}{},
			good: int64(2),
			want: "2,3",
		},
		{
			name: "number grammar",
			sql:  `SELECT a FROM docs WHERE a >= ? ORDER BY a`,
			bad:  Number("NaN"),
			good: Number("2"),
			want: "2,3",
		},
		{
			name: "negative limit",
			sql:  `SELECT a FROM docs ORDER BY a LIMIT ?`,
			bad:  int64(-1),
			good: int64(2),
			want: "1,2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()
			var exec Exec
			if _, err := stmt.RunInto(&exec, FromSegment(set), []any{tc.bad}); err == nil {
				t.Fatal("invalid binding succeeded")
			}
			cursor, err := stmt.RunInto(&exec, FromSegment(set), []any{tc.good})
			if err != nil {
				t.Fatalf("valid binding after rejection: %v", err)
			}
			var got []string
			for cursor.Next() {
				got = append(got, string(cursor.Cell(0).JSON()))
			}
			if joined := strings.Join(got, ","); joined != tc.want {
				t.Fatalf("valid binding returned %q, want %q", joined, tc.want)
			}
		})
	}
}

func TestSQLStatementAcceptsPointerShapedZeroCopyBindings(t *testing.T) {
	set := mustSegment(t,
		`{"s":"x","n":0.1,"b":true,"i":2,"f":1.5}`,
		`{"s":"y","n":0.2,"b":false,"i":3,"f":2.5}`,
	)
	text := "x"
	number := Number("1e-1")
	boolean := true
	integer := int64(2)
	floating := 1.5
	for _, tc := range []struct {
		name string
		sql  string
		arg  any
	}{
		{"string", `SELECT s FROM docs WHERE s = ?`, &text},
		{"exact number", `SELECT s FROM docs WHERE n = ?`, &number},
		{"boolean", `SELECT s FROM docs WHERE b = ?`, &boolean},
		{"integer", `SELECT s FROM docs WHERE i = ?`, &integer},
		{"float", `SELECT s FROM docs WHERE f = ?`, &floating},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()
			var exec Exec
			cursor, err := stmt.RunInto(&exec, FromSegment(set), []any{tc.arg})
			if err != nil {
				t.Fatal(err)
			}
			if got := cursorJSON(cursor, 0); got != `"x"` {
				t.Fatalf("pointer binding returned %q, want %q", got, `"x"`)
			}
		})
	}

	t.Run("limit and steady state", func(t *testing.T) {
		stmt, err := PrepareStatement(`SELECT s FROM docs ORDER BY s LIMIT ?`)
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Release()
		args := []any{&integer}
		var exec Exec
		run := func() {
			cursor, err := stmt.RunInto(&exec, FromSegment(set), args)
			if err != nil {
				panic(err)
			}
			for cursor.Next() {
			}
		}
		run()
		if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
			t.Fatalf("warm pointer-shaped binding allocated %.2f times, want zero", allocs)
		}
	})
}

func TestSQLStatementsHaveIndependentCompilerLifetimes(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":2}`, `{"a":3}`)
	first, err := PrepareStatement(`SELECT a FROM docs WHERE a >= ? ORDER BY a`)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := PrepareStatement(`SELECT a FROM docs WHERE a <= ? ORDER BY a DESC`)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()

	var firstExec, secondExec Exec
	for pass := range 3 {
		firstCursor, err := first.RunInto(&firstExec, FromSegment(set), []any{int64(2)})
		if err != nil {
			t.Fatalf("pass %d first: %v", pass, err)
		}
		secondCursor, err := second.RunInto(&secondExec, FromSegment(set), []any{int64(2)})
		if err != nil {
			t.Fatalf("pass %d second: %v", pass, err)
		}
		if got := cursorJSON(firstCursor, 0); got != "2,3" {
			t.Fatalf("pass %d first = %q, want 2,3", pass, got)
		}
		if got := cursorJSON(secondCursor, 0); got != "2,1" {
			t.Fatalf("pass %d second = %q, want 2,1", pass, got)
		}
	}
}

func TestSQLStatementReleaseThenPrepareAndRun(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":2}`)
	for range 3 {
		stmt, err := PrepareStatement(`SELECT a FROM docs WHERE a = ?`)
		if err != nil {
			t.Fatal(err)
		}
		var exec Exec
		cursor, err := stmt.RunInto(&exec, FromSegment(set), []any{int64(2)})
		if err != nil {
			t.Fatal(err)
		}
		if got := cursorJSON(cursor, 0); got != "2" {
			t.Fatalf("result = %q, want 2", got)
		}
		stmt.Release()
		if _, err := stmt.RunInto(&exec, FromSegment(set), nil); err == nil {
			t.Fatal("released Statement remained executable")
		}
	}
}

func TestSQLStatementPathMemoIsBounded(t *testing.T) {
	var compiler compiler
	for i := range pathCacheMax * 3 {
		if _, err := compiler.compilePath(fmt.Sprintf("field%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if len(compiler.paths.entries) > pathCacheMax {
		t.Fatalf("path memo holds %d entries, want at most %d",
			len(compiler.paths.entries), pathCacheMax)
	}
}

func cursorJSON(cursor Cursor, column int) string {
	var values []string
	for cursor.Next() {
		values = append(values, string(cursor.Cell(column).JSON()))
	}
	return strings.Join(values, ",")
}
