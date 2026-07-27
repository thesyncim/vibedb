package vibesql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
)

// A stmt is one prepared statement bound to one connection.
//
// It holds one of two prepared forms and never both. A SELECT is a
// [query.Statement], which owns its own compiler — what makes re-binding a
// placeholder free of allocation after the first execution and what keeps two
// prepared statements on one connection from invalidating each other's plan. A
// mutation or a definition is a [query.DMLStatement], which holds the same
// thing for the condition it filters by, and the parsed statement the write
// path reads.
//
// Which one it is decided at Prepare, from the statement's leading keyword,
// because database/sql has already decided by then: it routes a statement to
// Query or to Exec and expects the driver to have an opinion about which is
// right. Answering "this is a mutation, use Exec" from Query is more useful
// than executing a mutation and reporting no rows.
type stmt struct {
	conn      *conn
	statement *query.Statement
	mutation  *query.DMLStatement
	// adhoc marks a statement QueryContext or ExecContext created for one
	// execution. Its rows own it, because nothing else will ever close it.
	adhoc  bool
	closed bool
}

var (
	_ driver.Stmt             = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
	_ driver.StmtExecContext  = (*stmt)(nil)
)

// NumInput reports the placeholder count, which database/sql checks the
// argument count against before it calls Query. Reporting it accurately is what
// turns a wrong argument count into an error naming both numbers instead of a
// failure inside the bind.
func (s *stmt) NumInput() int {
	if s.mutation != nil {
		return s.mutation.NumParams()
	}
	return s.statement.NumParams()
}

// Close releases the statement's plan and compiler.
func (s *stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.statement.Release()
	s.mutation.Release()
	return nil
}

// Query executes the statement with positional arguments.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	named, err := named(args)
	if err != nil {
		return nil, err
	}
	return s.QueryContext(context.Background(), named)
}

// QueryContext executes the statement and returns its rows.
//
// The rows read the connection's [query.Exec] directly, without copying, which
// is what makes the per-row cost the executor's rather than the driver's.
// database/sql holds the connection until the rows are closed, so nothing else
// can execute into that Exec while they are being read — the invariant that
// makes borrowing safe rather than merely fast.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	c := s.conn
	if s.closed {
		return nil, errors.New("vibesql: the statement is closed")
	}
	if s.mutation != nil {
		return nil, fmt.Errorf(
			"vibesql: %s returns no rows; use Exec rather than Query", s.mutation.Kind())
	}
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	if c.open {
		return nil, errors.New(
			"vibesql: this connection already has open rows; close them before executing again")
	}
	values, err := c.values(args)
	if err != nil {
		return nil, err
	}
	var handle handle
	if c.tx == nil {
		handle, err = c.src.resolve(s.statement.Collection(), s.statement.NumJoins() != 0)
	} else {
		handle, err = c.tx.resolve(s.statement.Collection(), s.statement.NumJoins() != 0)
	}
	if err != nil {
		return nil, err
	}
	cursor, err := s.statement.RunInto(&c.exec, handle.src, values)
	if err != nil {
		handle.close()
		return nil, err
	}
	c.open = true
	return &rows{stmt: s, cursor: cursor, handle: handle}, nil
}

// ExecContext runs a mutation or a definition and reports what it wrote.
//
// A SELECT reaching here is refused rather than executed and reported as zero
// rows affected. Exec of a SELECT is a mistake with a silent failure mode —
// the query runs, its rows are discarded, and the caller is told nothing
// happened — and the two spellings are one character apart.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	c := s.conn
	if s.closed {
		return nil, errors.New("vibesql: the statement is closed")
	}
	if s.mutation == nil {
		return nil, errQueryExec
	}
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	if c.open {
		return nil, errors.New(
			"vibesql: this connection already has open rows; close them before executing again")
	}
	values, err := c.values(args)
	if err != nil {
		return nil, err
	}
	switch s.mutation.Kind() {
	case query.DDLCreateTable, query.DDLCreateIndex:
		return c.execDefine(s.mutation)
	}
	return c.execWrite(s.mutation, values)
}

// Exec runs a mutation with positional arguments.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	named, err := named(args)
	if err != nil {
		return nil, err
	}
	return s.ExecContext(context.Background(), named)
}

// named lifts positional arguments into the named form the context entry points
// take. It is the legacy driver.Stmt shape adapted to the modern one, rather
// than two implementations of every statement.
func named(args []driver.Value) ([]driver.NamedValue, error) {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out, nil
}

var errQueryExec = errors.New(
	"vibesql: a SELECT returns rows and Exec discards them; use Query, or QueryRow if you want one row")
