package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func directTestConn(tb testing.TB) sqldriver.Conn {
	tb.Helper()
	connection, err := (Driver{}).Open(filepath.Join(tb.TempDir(), "catalog.vdb"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := connection.Close(); err != nil {
			tb.Error(err)
		}
	})
	return connection
}

func directExec(
	tb testing.TB,
	connection sqldriver.Conn,
	statement string,
	args []sqldriver.NamedValue,
) {
	tb.Helper()
	if _, err := connection.(sqldriver.ExecerContext).ExecContext(
		context.Background(), statement, args,
	); err == nil {
		return
	} else if !errors.Is(err, sqldriver.ErrSkip) {
		tb.Fatal(err)
	}
	prepared, err := connection.Prepare(statement)
	if err != nil {
		tb.Fatal(err)
	}
	defer prepared.Close()
	values := make([]sqldriver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	if _, err := prepared.Exec(values); err != nil {
		tb.Fatal(err)
	}
}

func directPointFixture(tb testing.TB) (sqldriver.Stmt, []sqldriver.Value) {
	tb.Helper()
	connection := directTestConn(tb)
	directExec(tb, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`, nil)
	directExec(tb, connection, `INSERT INTO docs VALUES (?)`,
		[]sqldriver.NamedValue{{
			Ordinal: 1, Value: `{"id":"a","n":1}`,
		}})
	prepared, err := connection.Prepare(`SELECT n FROM docs WHERE id = ?`)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			tb.Error(err)
		}
	})
	return prepared, []sqldriver.Value{"a"}
}

func runDirectQuery(
	statement sqldriver.Stmt,
	args []sqldriver.Value,
	dest []sqldriver.Value,
) error {
	rows, err := statement.Query(args)
	if err != nil {
		return err
	}
	if err := rows.Next(dest); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

func TestPreparedPointQueryWarmAllocations(t *testing.T) {
	statement, args := directPointFixture(t)
	dest := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, args, dest); err != nil {
		t.Fatal(err)
	}
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		runErr = runDirectQuery(statement, args, dest)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf(
			"warmed direct-driver primary point query allocated %.2f times, want zero",
			allocs)
	}
}

func TestPreparedPointQueryByteKeyWarmAllocations(t *testing.T) {
	statement, _ := directPointFixture(t)
	args := []sqldriver.Value{[]byte("a")}
	dest := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, args, dest); err != nil {
		t.Fatal(err)
	}
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		runErr = runDirectQuery(statement, args, dest)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf(
			"warmed []byte primary point query allocated %.2f times, want zero",
			allocs)
	}
}

func TestDocumentPrimaryKeyBoundedBeforeEncoding(t *testing.T) {
	primary, err := vibejson.CompilePointer("/id")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"number": []byte(
			`{"id":1e` + strings.Repeat("9", 64<<10) + `}`,
		),
		"escaped string": []byte(
			`{"id":"` + strings.Repeat(`\u0061`, 8<<10) + `"}`,
		),
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := documentKey(
				document, "/id", primary, 256,
			); !errors.Is(err, durable.ErrKeyTooLarge) {
				t.Fatalf("oversize primary key = %v, want ErrKeyTooLarge", err)
			}
			var runErr error
			allocations := testing.AllocsPerRun(20, func() {
				_, runErr = documentKey(document, "/id", primary, 256)
			})
			if !errors.Is(runErr, durable.ErrKeyTooLarge) {
				t.Fatalf(
					"warm oversize primary key = %v, want ErrKeyTooLarge",
					runErr,
				)
			}
			if allocations != 0 {
				t.Fatalf(
					"bounded document-key rejection allocated %.2f times, want zero",
					allocations,
				)
			}
		})
	}
}

func directTransactionOverlayFixture(tb testing.TB) sqldriver.Stmt {
	tb.Helper()
	connection := directTestConn(tb)
	directExec(tb, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`, nil)
	directExec(tb, connection, `INSERT INTO docs VALUES (?)`,
		[]sqldriver.NamedValue{{
			Ordinal: 1, Value: `{"id":"a","n":1}`,
		}})
	transaction, err := connection.(sqldriver.ConnBeginTx).BeginTx(
		context.Background(), sqldriver.TxOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := transaction.Rollback(); err != nil {
			tb.Error(err)
		}
	})
	directExec(tb, connection,
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		[]sqldriver.NamedValue{
			{Ordinal: 1, Value: `{"id":"a","n":2}`},
			{Ordinal: 2, Value: "a"},
		})
	prepared, err := connection.Prepare(
		`SELECT COUNT(*) FROM docs WHERE n = 2`)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			tb.Error(err)
		}
	})
	return prepared
}

func TestTransactionOverlayQueryWarmAllocations(t *testing.T) {
	statement := directTransactionOverlayFixture(t)
	dest := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, nil, dest); err != nil {
		t.Fatal(err)
	}
	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		runErr = runDirectQuery(statement, nil, dest)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf(
			"warmed direct-driver transaction overlay query allocated %.2f times, want zero",
			allocs)
	}
}

func BenchmarkDriverPreparedPointQuery(b *testing.B) {
	statement, args := directPointFixture(b)
	dest := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, args, dest); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := runDirectQuery(statement, args, dest); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDriverPreparePointQuery(b *testing.B) {
	connection := directTestConn(b)
	directExec(b, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, n NUMBER NOT NULL)`, nil)
	const source = `SELECT n FROM docs WHERE id = ?`
	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for b.Loop() {
		statement, err := connection.Prepare(source)
		if err != nil {
			b.Fatal(err)
		}
		if err := statement.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDriverTransactionOverlayQuery(b *testing.B) {
	statement := directTransactionOverlayFixture(b)
	dest := make([]sqldriver.Value, 1)
	if err := runDirectQuery(statement, nil, dest); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := runDirectQuery(statement, nil, dest); err != nil {
			b.Fatal(err)
		}
	}
}
