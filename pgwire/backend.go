package pgwire

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// SessionIdentity is the authenticated PostgreSQL principal. A backend must
// bind it to its own execution authority; it is not an RF3 service credential.
type SessionIdentity struct {
	User     string
	Database string
}

// Backend opens independent, single-consumer execution sessions. It is borrowed
// by Server, like the embedded Database, and must outlive the server. Opening a
// session happens only after authentication and connection admission succeed.
type Backend interface {
	NewSession(context.Context, SessionIdentity) (BackendSession, error)
}

// BackendSession is the execution boundary beneath the PostgreSQL protocol.
// Implementations must provide the transaction semantics they report. An
// unsupported transaction mode must be rejected before executing mutations.
// Close releases all statements, cursors, and execution authority it owns.
type BackendSession interface {
	State() sqldriver.SessionState
	MarkFailed()
	SetCancelFlag(*query.CancelFlag) error
	SetResultLimits(int, int64) error
	SetIntermediateLimit(int64) error
	Prepare(context.Context, string) (BackendStatement, error)
	Begin(context.Context, sqldriver.TxOptions) error
	Commit(context.Context) error
	Rollback(context.Context) error
	Savepoint(context.Context, string) error
	ReleaseSavepoint(context.Context, string) error
	RollbackTo(context.Context, string) error
	Tables(context.Context) ([]sqldriver.TableInfo, error)
	Close() error
}

// BackendStatement retains one parsed statement and its parameter/output
// contract. QueryInto borrows caller-owned cursor storage to avoid allocating a
// handle per execution. Rows and cells remain borrowed until that cursor closes.
type BackendStatement interface {
	Kind() sqlast.Kind
	ReturnsRows() bool
	NumParams() int
	ParamKind(int) sqldriver.ParamKind
	ParamPosition(int) int
	Columns() []string
	AppendSchema([]query.OutputColumn) []query.OutputColumn
	Exec(context.Context, []any) (sqldriver.Result, error)
	QueryInto(context.Context, []any, *BackendRows) error
	Close() error
}

// BackendRows owns one live result. Embedded sessions retain their inline
// runtime cursor; distributed sessions install a materialized query cursor and
// a release callback. Neither path adds interface dispatch or allocation per
// row. A value must not be copied while open.
type BackendRows struct {
	local        sqldriver.Cursor
	materialized query.Cursor
	release      func() error
	open         bool
	remote       bool
}

// SetMaterialized transfers a result's lifetime into this previously closed
// destination. release is invoked at most once, including protocol-error and
// disconnect cleanup. Snapshot borrows that same lifetime for row preflight.
func (rows *BackendRows) SetMaterialized(cursor query.Cursor, release func() error) error {
	if rows == nil {
		return errors.New("pgwire: nil result destination")
	}
	if rows.open {
		return sqldriver.ErrCursorOpen
	}
	rows.materialized, rows.release = cursor, release
	rows.open, rows.remote = true, true
	return nil
}

func (rows *BackendRows) Next() bool {
	if rows == nil || !rows.open {
		return false
	}
	if rows.remote {
		return rows.materialized.Next()
	}
	return rows.local.Next()
}

func (rows *BackendRows) Cell(column int) query.Cell {
	if rows == nil || !rows.open {
		return query.Cell{}
	}
	if rows.remote {
		return rows.materialized.Cell(column)
	}
	return rows.local.Cell(column)
}

func (rows *BackendRows) Snapshot() query.Cursor {
	if rows == nil || !rows.open {
		return query.Cursor{}
	}
	if rows.remote {
		return rows.materialized
	}
	return rows.local.Snapshot()
}

func (rows *BackendRows) Close() error {
	if rows == nil || !rows.open {
		return nil
	}
	rows.open = false
	if !rows.remote {
		return rows.local.Close()
	}
	release := rows.release
	rows.release, rows.materialized = nil, query.Cursor{}
	if release != nil {
		return release()
	}
	return nil
}

type embeddedBackend struct{ database *sqldriver.Database }

func (backend embeddedBackend) NewSession(ctx context.Context, _ SessionIdentity) (BackendSession, error) {
	session, err := backend.database.NewSession(ctx)
	if err != nil {
		return nil, err
	}
	return &embeddedSession{session}, nil
}

type embeddedSession struct{ *sqldriver.Session }

func (session *embeddedSession) Prepare(ctx context.Context, sql string) (BackendStatement, error) {
	statement, err := session.Session.Prepare(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &embeddedStatement{statement}, nil
}

type embeddedStatement struct{ *sqldriver.Prepared }

func (statement *embeddedStatement) QueryInto(ctx context.Context, args []any, rows *BackendRows) error {
	if rows == nil {
		return errors.New("pgwire: nil result destination")
	}
	if rows.open {
		return sqldriver.ErrCursorOpen
	}
	if err := statement.Prepared.QueryInto(ctx, args, &rows.local); err != nil {
		return err
	}
	rows.open, rows.remote = true, false
	return nil
}
