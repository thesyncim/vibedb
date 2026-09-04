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

// BackendAutocommitWrites marks an adapter whose mutations are indivisible
// durable requests, not writes staged in BackendSession's read transaction.
// Such writes require a standalone Query or Execute/Sync batch. They cannot
// participate in explicit transactions, and cancellation after success cannot
// undo them. Embedded backends retain their normal transactional behavior.
type BackendAutocommitWrites interface {
	AutocommitWrites() bool
}

func autocommitWrites(s BackendSession) bool {
	b, ok := s.(BackendAutocommitWrites)
	return ok && b.AutocommitWrites()
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

// BackendSessionParameterPreparer is the optional Parse-time parameter type
// channel. PostgreSQL clients may declare OIDs before analysis; backends that
// implement this interface can let supported boolean/string declarations and
// concrete declarations outside that bounded model
// participate in common-type resolution. Backends without it retain the
// post-analysis compatibility check.
type BackendSessionParameterPreparer interface {
	PrepareWithParameterTypes(
		context.Context,
		string,
		[]sqldriver.ParamType,
	) (BackendStatement, error)
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

// BackendStatementParseReuser is the optional contract for reusing a live
// backend statement when PostgreSQL replaces the unnamed statement with the
// exact same SQL and declared parameter types. Implementations must return
// false when external state that participates in Prepare, such as a catalog
// generation, has changed. Backends that do not implement this interface keep
// the ordinary close-and-prepare behavior.
type BackendStatementParseReuser interface {
	ReusableForParse() bool
}

// BackendStatementParamTyper is the optional analyzed SQL-input contract of a
// prepared statement. Keeping it separate from BackendStatement preserves
// compatibility for external backends whose schemaless parameters are all
// unspecified, while typed runtimes can let pgwire advertise and decode an
// exact inferred domain.
type BackendStatementParamTyper interface {
	ParamType(int) sqldriver.ParamType
}

// BackendStatementParamTypePositioner optionally reports the authored byte
// position at which one occurrence acquired its analyzed SQL type. It is used
// only on Parse-time errors, never on execution's hot path.
type BackendStatementParamTypePositioner interface {
	ParamTypePosition(int) int
}

// BackendStatementParamTypeTargetDefaulter distinguishes PostgreSQL's final
// unresolved SELECT-target coercion from a contextual type constraint. Numbered
// parameters can map several authored occurrences onto one wire parameter; a
// contextual occurrence must be considered before this last-resort text
// default, regardless of source order.
type BackendStatementParamTypeTargetDefaulter interface {
	ParamTypeTargetDefault(int) bool
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

func (session *embeddedSession) PrepareWithParameterTypes(
	ctx context.Context,
	sql string,
	parameterTypes []sqldriver.ParamType,
) (BackendStatement, error) {
	statement, err := session.Session.PrepareWithParameterTypes(
		ctx, sql, parameterTypes,
	)
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
