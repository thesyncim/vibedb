package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// Database owns one SQL catalog and its exclusive durable writer lease.
//
// Close prevents new sessions immediately. Existing sessions remain usable and
// keep the catalog alive until their own Close calls; the last owner performs
// the durability fences and releases every table handle. This is the same
// reference-counted ownership primitive database/sql uses through Connector.
type Database struct {
	connector *dbConnector
}

// Open opens the durable SQL catalog at path.
func Open(path string) (*Database, error) {
	connector, err := openConnector(path)
	if err != nil {
		return nil, err
	}
	return &Database{connector: connector}, nil
}

// NewSession creates one single-consumer SQL session.
//
// A Session owns one query executor, at most one live Cursor, and at most one
// transaction. It is intentionally not safe for concurrent use: protocol and
// database adapters obtain concurrency by opening independent sessions.
func (d *Database) NewSession(ctx context.Context) (*Session, error) {
	if d == nil || d.connector == nil {
		return nil, ErrDatabaseClosed
	}
	connection, err := d.connector.Connect(ctx)
	if err != nil {
		if errors.Is(err, sqldriver.ErrBadConn) {
			return nil, ErrDatabaseClosed
		}
		return nil, err
	}
	return &Session{
		conn:  connection.(*conn),
		state: SessionIdle,
	}, nil
}

// Close retires the database owner. It is idempotent.
//
// Sessions already handed out keep the catalog alive; their Close methods are
// therefore part of Database's explicit ownership boundary.
func (d *Database) Close() error {
	if d == nil || d.connector == nil {
		return nil
	}
	return d.connector.Close()
}

// SessionState is the transaction/lifecycle state a protocol adapter reports
// to its client.
type SessionState uint8

const (
	// SessionIdle has no active transaction.
	SessionIdle SessionState = iota
	// SessionInTransaction has an active, usable snapshot transaction.
	SessionInTransaction
	// SessionFailedTransaction has an active transaction that rejected a
	// prepare or execution. Only Rollback is accepted. Commit first rolls back
	// deterministically and then returns ErrTransactionFailed, allowing a wire
	// adapter to map that outcome to its protocol's failed-COMMIT behavior.
	SessionFailedTransaction
	// SessionClosed is terminal.
	SessionClosed
)

func (s SessionState) String() string {
	switch s {
	case SessionIdle:
		return "idle"
	case SessionInTransaction:
		return "in transaction"
	case SessionFailedTransaction:
		return "failed transaction"
	case SessionClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// TxOptions is the transaction policy the current SQL runtime supports.
//
// Transactions always provide snapshot isolation. ReadOnly prevents every DML
// and DDL execution while retaining the same multi-table snapshot semantics.
type TxOptions struct {
	ReadOnly bool
}

// Session is one typed SQL execution session.
type Session struct {
	conn     *conn
	state    SessionState
	prepared map[*Prepared]struct{}
	current  *Cursor
}

// State reports the session's transaction/lifecycle state.
func (s *Session) State() SessionState {
	if s == nil || s.conn == nil || s.state == SessionClosed {
		return SessionClosed
	}
	return s.state
}

// MarkFailed moves an active transaction into SessionFailedTransaction.
//
// Protocol adapters call this for errors that occur outside runtime execution,
// such as malformed Bind messages. It is a no-op while idle, already failed, or
// closed.
func (s *Session) MarkFailed() {
	if s != nil && s.state == SessionInTransaction {
		s.state = SessionFailedTransaction
	}
}

// SetCancelFlag installs the reusable cooperative cancellation signal used by
// this session's query executor. Install it before an execution and do not
// replace it while another goroutine is using the Session. A live Cursor must
// be closed first because its result still borrows the same executor.
func (s *Session) SetCancelFlag(flag *query.CancelFlag) error {
	if err := s.live(); err != nil {
		return err
	}
	if s.current != nil {
		return ErrCursorOpen
	}
	if s.state == SessionFailedTransaction {
		return ErrTransactionFailed
	}
	if s.state != SessionIdle {
		return ErrTransactionActive
	}
	s.conn.exec.Options.Cancel = flag
	return nil
}

// SetResultLimits configures the maximum materialized rows and bytes for one
// query result. Zero selects the query package default and -1 disables the
// corresponding limit. Configure a session before preparing or executing
// statements.
func (s *Session) SetResultLimits(rows int, bytes int64) error {
	if err := s.live(); err != nil {
		return err
	}
	if rows < -1 || bytes < -1 {
		return errors.New("vibedb: result limits must be -1, zero, or positive")
	}
	if s.current != nil {
		return ErrCursorOpen
	}
	if s.state != SessionIdle {
		return ErrTransactionActive
	}
	s.conn.exec.Options.ResultRows = rows
	s.conn.exec.Options.ResultBytes = bytes
	return nil
}

// Prepare parses and lowers one statement exactly once.
//
// An error while a transaction is active moves the session to
// SessionFailedTransaction, matching PostgreSQL's failed-transaction rule.
func (s *Session) Prepare(ctx context.Context, text string) (*Prepared, error) {
	if err := s.ready(ctx); err != nil {
		return nil, s.fail(err)
	}
	statement, err := s.conn.prepareContext(ctx, text)
	if err != nil {
		return nil, s.fail(err)
	}
	prepared := &Prepared{session: s, statement: statement}
	if s.prepared == nil {
		s.prepared = make(map[*Prepared]struct{})
	}
	s.prepared[prepared] = struct{}{}
	return prepared, nil
}

// Begin captures one coherent, generation-leased snapshot of every cataloged
// table. Nested transactions are rejected.
func (s *Session) Begin(ctx context.Context, options TxOptions) error {
	if err := s.ready(ctx); err != nil {
		return s.fail(err)
	}
	if s.state != SessionIdle || s.conn.tx != nil {
		return ErrTransactionActive
	}
	if _, err := s.conn.BeginTx(ctx, sqldriver.TxOptions{ReadOnly: options.ReadOnly}); err != nil {
		return err
	}
	s.state = SessionInTransaction
	return nil
}

// Commit publishes the current transaction and returns to SessionIdle.
//
// Commit on SessionFailedTransaction always rolls back first, then returns
// ErrTransactionFailed. A PostgreSQL adapter can therefore emit the protocol's
// ROLLBACK outcome without guessing whether any staged work survived.
func (s *Session) Commit(ctx context.Context) error {
	if err := s.live(); err != nil {
		return err
	}
	if s.current != nil {
		return ErrCursorOpen
	}
	if s.state == SessionFailedTransaction {
		var rollbackErr error
		if s.conn.tx != nil {
			rollbackErr = s.conn.tx.Rollback()
		}
		s.state = SessionIdle
		if rollbackErr != nil {
			return errors.Join(ErrTransactionFailed, rollbackErr)
		}
		return ErrTransactionFailed
	}
	if s.state != SessionInTransaction || s.conn.tx == nil {
		return ErrNoTransaction
	}
	if err := contextCheckpoint(ctx); err != nil {
		s.state = SessionFailedTransaction
		return err
	}
	err := s.conn.tx.Commit()
	s.state = SessionIdle
	return err
}

// Rollback discards the current transaction and returns to SessionIdle.
//
// Cleanup is unconditional even when ctx is already canceled: Rollback is the
// one operation a failed transaction must always retain, and leaving snapshot
// leases behind would turn cancellation into an ownership leak.
func (s *Session) Rollback(ctx context.Context) error {
	_ = ctx
	if err := s.live(); err != nil {
		return err
	}
	var cursorErr error
	if s.current != nil {
		cursorErr = s.current.Close()
	}
	if s.state != SessionInTransaction && s.state != SessionFailedTransaction {
		return errors.Join(ErrNoTransaction, cursorErr)
	}
	if s.conn.tx == nil {
		s.state = SessionIdle
		return errors.Join(ErrNoTransaction, cursorErr)
	}
	rollbackErr := s.conn.tx.Rollback()
	s.state = SessionIdle
	return errors.Join(cursorErr, rollbackErr)
}

// Close releases every Prepared statement, the live Cursor, a transaction
// snapshot, the query executor's retained storage, and this session's database
// ownership reference. It is idempotent.
func (s *Session) Close() error {
	if s == nil || s.state == SessionClosed {
		return nil
	}
	var errs []error
	if s.current != nil {
		if err := s.current.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for prepared := range s.prepared {
		if err := prepared.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.conn = nil
	s.prepared = nil
	s.current = nil
	s.state = SessionClosed
	return errors.Join(errs...)
}

func (s *Session) live() error {
	if s == nil || s.state == SessionClosed || s.conn == nil {
		return ErrSessionClosed
	}
	return nil
}

func (s *Session) ready(ctx context.Context) error {
	if err := s.live(); err != nil {
		return err
	}
	if s.state == SessionFailedTransaction {
		return ErrTransactionFailed
	}
	if s.current != nil {
		return ErrCursorOpen
	}
	return contextCheckpoint(ctx)
}

func (s *Session) fail(err error) error {
	if err != nil && s != nil && s.state == SessionInTransaction {
		s.state = SessionFailedTransaction
	}
	return err
}

// ParamKind identifies how one placeholder is decoded before binding.
//
// Scalar parameters take JSON scalar/SQL literal values. Document parameters
// are the right side of whole-document INSERT or UPDATE and take string,
// []byte, *string, or *[]byte containing complete JSON, including arrays and
// objects. Pointer forms let protocol adapters bind stable slots without
// boxing variable-width values.
type ParamKind uint8

const (
	// ParamInvalid reports an out-of-range parameter index.
	ParamInvalid ParamKind = iota
	// ParamScalar is a predicate, flat INSERT field, LIMIT, or OFFSET value.
	ParamScalar
	// ParamDocument is a complete stored JSON document.
	ParamDocument
)

func (k ParamKind) String() string {
	switch k {
	case ParamScalar:
		return "scalar"
	case ParamDocument:
		return "document"
	default:
		return "invalid"
	}
}

// Prepared owns one parsed AST and its reusable lowered statement.
type Prepared struct {
	session   *Session
	statement *stmt
	closed    bool
}

// Kind reports the parsed statement kind. Call it before Close; sql.Kind has no
// invalid value, so a closed Prepared cannot represent meaningful metadata.
func (p *Prepared) Kind() sqlast.Kind {
	if p == nil || p.statement == nil || p.statement.tree == nil {
		return sqlast.KindSelect
	}
	return p.statement.tree.Kind
}

// ReturnsRows reports whether this statement must be executed through Query.
// It is true for SELECT and for a mutation with a RETURNING projection.
func (p *Prepared) ReturnsRows() bool {
	return p != nil && p.statement != nil && p.statement.query != nil
}

// NumParams reports the statement's placeholder count.
func (p *Prepared) NumParams() int {
	if p == nil || p.statement == nil {
		return 0
	}
	return p.statement.params
}

// ParamKind reports the role of zero-based placeholder index.
//
// It performs one bounded slice lookup and allocates nothing.
func (p *Prepared) ParamKind(index int) ParamKind {
	if p == nil || p.statement == nil ||
		index < 0 || index >= len(p.statement.paramKinds) {
		return ParamInvalid
	}
	return p.statement.paramKinds[index]
}

// Columns returns immutable borrowed output names for a SELECT. It returns nil
// for statements that do not return rows and after Close.
func (p *Prepared) Columns() []string {
	if p == nil || p.statement == nil || p.statement.query == nil {
		return nil
	}
	return p.statement.query.Columns()
}

// AppendSchema appends the typed SELECT output schema to dst.
func (p *Prepared) AppendSchema(dst []query.OutputColumn) []query.OutputColumn {
	if p == nil || p.statement == nil || p.statement.query == nil {
		return dst
	}
	return p.statement.query.AppendSchema(dst)
}

// Query executes a SELECT and returns a typed cursor over query.Cell values.
//
// Values and variable-width payloads borrow the session executor and current
// snapshot. Their lifetime ends at Cursor.Close, even if an implementation's
// retained high-water storage has not yet been overwritten.
func (p *Prepared) Query(ctx context.Context, values []any) (*Cursor, error) {
	cursor := new(Cursor)
	if err := p.QueryInto(ctx, values, cursor); err != nil {
		return nil, err
	}
	return cursor, nil
}

// QueryInto executes a SELECT into caller-owned cursor storage.
//
// The destination must be non-nil and either its zero value or a cursor whose
// Close method has completed. After statement and executor warmup this avoids
// the one handle allocation made by Query, which is the intended entry point
// for a protocol portal that embeds its cursor. QueryInto never changes dst on
// failure.
func (p *Prepared) QueryInto(
	ctx context.Context,
	values []any,
	dst *Cursor,
) error {
	if err := p.usable(); err != nil {
		return p.fail(err)
	}
	if !p.ReturnsRows() {
		return p.fail(fmt.Errorf(
			"vibedb: %s returns no rows; use Exec", p.Kind()))
	}
	if dst == nil {
		return p.fail(errors.New(
			"vibedb: QueryInto requires a non-nil cursor destination"))
	}
	if !dst.closed && dst.rows != nil {
		return p.fail(ErrCursorOpen)
	}
	if err := p.session.ready(ctx); err != nil {
		return p.fail(err)
	}
	if err := p.statement.checkArgumentCount(len(values)); err != nil {
		return p.fail(err)
	}
	args, err := p.session.conn.runtimeValues(p.statement.paramKinds, values)
	if err != nil {
		return p.fail(err)
	}
	rowset, err := p.statement.queryRows(ctx, args)
	if err != nil {
		return p.fail(err)
	}
	*dst = Cursor{session: p.session, prepared: p, rows: rowset}
	p.session.current = dst
	return nil
}

// Exec executes DDL or DML and reports its affected-row count.
func (p *Prepared) Exec(ctx context.Context, values []any) (Result, error) {
	if err := p.usable(); err != nil {
		return Result{}, p.fail(err)
	}
	if p.ReturnsRows() {
		return Result{}, p.fail(fmt.Errorf(
			"vibedb: %s returns rows; use Query", p.Kind()))
	}
	if err := p.session.ready(ctx); err != nil {
		return Result{}, p.fail(err)
	}
	if err := p.statement.checkArgumentCount(len(values)); err != nil {
		return Result{}, p.fail(err)
	}
	args, err := p.session.conn.runtimeValues(p.statement.paramKinds, values)
	if err != nil {
		return Result{}, p.fail(err)
	}
	driverResult, err := p.statement.exec(ctx, args)
	if err != nil {
		return Result{}, p.fail(err)
	}
	affected, err := driverResult.RowsAffected()
	if err != nil {
		return Result{}, p.fail(err)
	}
	return Result{RowsAffected: affected}, nil
}

// Close releases the parsed AST and compiler arenas. If this statement owns the
// session's live Cursor, that cursor is closed first. Close is idempotent.
func (p *Prepared) Close() error {
	if p == nil || p.closed {
		return nil
	}
	var err error
	if p.session != nil && p.session.current != nil &&
		p.session.current.prepared == p {
		err = p.session.current.Close()
	}
	if p.statement != nil {
		if closeErr := p.statement.Close(); err == nil {
			err = closeErr
		}
	}
	if p.session != nil && p.session.prepared != nil {
		delete(p.session.prepared, p)
	}
	p.closed = true
	p.statement = nil
	p.session = nil
	return err
}

func (p *Prepared) usable() error {
	if p == nil || p.closed || p.statement == nil {
		return ErrPreparedClosed
	}
	return p.session.live()
}

func (p *Prepared) fail(err error) error {
	if p != nil && p.session != nil {
		return p.session.fail(err)
	}
	return err
}

// Result is the outcome of one DDL or DML execution.
type Result struct {
	RowsAffected int64
}

// Cursor is a typed iterator over one materialized SELECT result.
//
// Cell values borrow runtime-owned storage. Close is their documented lifetime
// boundary: callers must copy payloads that need to survive it.
type Cursor struct {
	session  *Session
	prepared *Prepared
	rows     *rows
	closed   bool
}

// Next advances directly through the underlying query.Cursor.
func (c *Cursor) Next() bool {
	if c == nil || c.closed || c.rows == nil {
		return false
	}
	return c.rows.cursor.Next()
}

// Cell returns the typed value at column in the current row.
func (c *Cursor) Cell(column int) query.Cell {
	if c == nil || c.closed || c.rows == nil {
		return query.Cell{}
	}
	return c.rows.cursor.Cell(column)
}

// Row reports the underlying materialized result row, or -1.
func (c *Cursor) Row() int {
	if c == nil || c.closed || c.rows == nil {
		return -1
	}
	return c.rows.cursor.Row()
}

// Snapshot returns an allocation-free value copy of the underlying materialized
// query cursor. Advancing the copy neither advances nor closes c, which lets a
// protocol adapter preflight every encoded row before it sends the first one.
//
// The copy borrows the same result and prepared statement as c and is valid only
// until c is closed.
func (c *Cursor) Snapshot() query.Cursor {
	if c == nil || c.closed || c.rows == nil {
		return query.Cursor{}
	}
	return c.rows.cursor
}

// Columns returns immutable borrowed output names.
func (c *Cursor) Columns() []string {
	if c == nil || c.closed || c.prepared == nil {
		return nil
	}
	return c.prepared.Columns()
}

// AppendSchema appends the typed output schema to dst.
func (c *Cursor) AppendSchema(dst []query.OutputColumn) []query.OutputColumn {
	if c == nil || c.closed || c.prepared == nil {
		return dst
	}
	return c.prepared.AppendSchema(dst)
}

// Close releases the snapshot lease and invalidates this cursor and all cells
// borrowed through it. It is idempotent.
func (c *Cursor) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	var err error
	if c.rows != nil {
		err = c.rows.Close()
	}
	if c.session != nil && c.session.current == c {
		c.session.current = nil
		if err != nil {
			c.session.fail(err)
		}
	}
	c.rows = nil
	c.prepared = nil
	c.session = nil
	return err
}

var (
	// ErrDatabaseClosed reports use of a retired Database owner.
	ErrDatabaseClosed = errors.New("vibedb: SQL database is closed")
	// ErrSessionClosed reports use of a closed Session.
	ErrSessionClosed = errors.New("vibedb: SQL session is closed")
	// ErrPreparedClosed reports use of a closed Prepared statement.
	ErrPreparedClosed = errors.New("vibedb: SQL prepared statement is closed")
	// ErrNoTransaction reports COMMIT or ROLLBACK without BEGIN.
	ErrNoTransaction = errors.New("vibedb: SQL session has no transaction")
	// ErrTransactionActive reports BEGIN or an idle-only session operation while
	// a transaction is already active.
	ErrTransactionActive = errors.New("vibedb: SQL session already has an active transaction")
	// ErrTransactionFailed reports a failed transaction in which only ROLLBACK
	// remains valid.
	ErrTransactionFailed = errors.New("vibedb: SQL transaction is failed; roll it back")
	// ErrCursorOpen reports an operation that cannot replace the session's
	// current typed result and snapshot lease.
	ErrCursorOpen = errors.New("vibedb: SQL session has an open cursor")
)

func statementParamKinds(statement *sqlast.Statement) []ParamKind {
	if statement == nil || statement.Params() == 0 {
		return nil
	}
	kinds := make([]ParamKind, statement.Params())
	for i := range kinds {
		kinds[i] = ParamScalar
	}
	switch statement.Kind {
	case sqlast.KindInsert:
		if len(statement.Insert.Columns) == 0 {
			for i := range statement.Insert.Rows {
				if len(statement.Insert.Rows[i].Values) != 0 {
					markDocumentParam(kinds, statement.Insert.Rows[i].Values[0])
				}
			}
		}
	case sqlast.KindUpdate:
		markDocumentParam(kinds, statement.Update.Doc)
	}
	return kinds
}

func markDocumentParam(kinds []ParamKind, operand sqlast.Operand) {
	if operand.Kind == sqlast.OperandParam &&
		operand.Ordinal >= 0 && operand.Ordinal < len(kinds) {
		kinds[operand.Ordinal] = ParamDocument
	}
}

// runtimeValues validates and copies caller-owned interface headers into the
// connection arena. queryRows and stmt.exec may clear the returned slice; they
// can therefore never mutate the caller's args slice.
func (c *conn) runtimeValues(kinds []ParamKind, values []any) ([]any, error) {
	if cap(c.args) < len(values) {
		c.args = make([]any, len(values))
	} else if len(c.args) > len(values) {
		clear(c.args[len(values):])
	}
	c.args = c.args[:len(values)]
	clear(c.args)

	total := 0
	for i, value := range values {
		normalized, err := normalizeRuntimeValue(value)
		if err != nil {
			clear(c.args)
			return nil, err
		}
		if err := addRuntimeArgumentBytes(&total, normalized); err != nil {
			clear(c.args)
			return nil, err
		}
		if i < len(kinds) && kinds[i] == ParamDocument {
			switch value := normalized.(type) {
			case string, []byte:
			case *string:
				if value == nil {
					clear(c.args)
					return nil, fmt.Errorf(
						"vibedb: document parameter %d cannot be a nil *string",
						i+1,
					)
				}
			case *[]byte:
				if value == nil {
					clear(c.args)
					return nil, fmt.Errorf(
						"vibedb: document parameter %d cannot be a nil *[]byte",
						i+1,
					)
				}
			default:
				clear(c.args)
				return nil, fmt.Errorf(
					"vibedb: document parameter %d must be string, []byte, *string, or *[]byte containing JSON, got %T",
					i+1, normalized,
				)
			}
		}
		c.args[i] = normalized
	}
	return c.args, nil
}

func normalizeRuntimeValue(argument any) (any, error) {
	switch value := argument.(type) {
	case nil, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return argument, nil
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("vibedb: numeric parameters must be finite JSON numbers")
		}
		return argument, nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("vibedb: numeric parameters must be finite JSON numbers")
		}
		return argument, nil
	case string:
		if !utf8.ValidString(value) {
			return nil, errors.New("vibedb: string parameters must be valid UTF-8")
		}
		return argument, nil
	case []byte:
		if !utf8.Valid(value) {
			return nil, errors.New("vibedb: []byte parameters must be valid UTF-8")
		}
		return argument, nil
	case json.Number:
		return query.Number(value.String()), nil
	case query.Number:
		return argument, nil
	case *bool, *int64:
		return argument, nil
	case *float64:
		if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return nil, errors.New("vibedb: numeric parameters must be finite JSON numbers")
		}
		return argument, nil
	case *string:
		if value != nil && !utf8.ValidString(*value) {
			return nil, errors.New("vibedb: string parameters must be valid UTF-8")
		}
		return argument, nil
	case *[]byte:
		if value != nil && !utf8.Valid(*value) {
			return nil, errors.New("vibedb: []byte parameters must be valid UTF-8")
		}
		return argument, nil
	case *query.Number:
		return argument, nil
	default:
		return nil, fmt.Errorf(
			"vibedb: cannot bind %T as a SQL parameter", argument)
	}
}

func addRuntimeArgumentBytes(total *int, value any) error {
	size := runtimeArgumentBytes(value)
	if err := checkSQLParameterBytes(size); err != nil {
		return err
	}
	if size > maxSQLArgumentsBytes-*total {
		return fmt.Errorf(
			"%w: payload exceeds %d bytes",
			ErrArgumentsTooLarge, maxSQLArgumentsBytes,
		)
	}
	*total += size
	return nil
}

func runtimeArgumentBytes(value any) int {
	switch value := value.(type) {
	case string:
		return len(value)
	case []byte:
		return len(value)
	case query.Number:
		return len(value)
	case *string:
		if value == nil {
			return 4
		}
		return len(*value)
	case *query.Number:
		if value == nil {
			return 4
		}
		return len(*value)
	case *[]byte:
		if value == nil {
			return 4
		}
		return len(*value)
	case nil:
		return 4
	default:
		return 8
	}
}
