package vibesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// The database/sql surface: driver, connector, and connection.
//
// # Why one Exec per connection, and why that is the whole concurrency story
//
// database/sql hands a driver.Conn to one goroutine at a time and never lends
// the same one to two, but it will happily run many connections at once. A
// [query.Exec] is single-consumer for the same reason a Result is: it holds the
// column, posting, group, and result buffers whose retained high-water capacity
// is what makes a warmed execution allocation-free, and two goroutines sharing
// one would interleave writes into a single set of them. Those two facts fit
// exactly: one Exec per connection is both safe and the only arrangement that
// keeps the buffers warm, because a per-execution Exec would start cold every
// time.
//
// Nothing else here is shared. A prepared statement's plan lives in a
// [query.Statement] that belongs to one connection, so two goroutines preparing
// the same SQL through one *sql.Stmt get two connections, two statements, two
// compilers, and two plans. The source — the heap database or the durable
// collection — is shared, and is safe for concurrent readers by its own
// contract.

func init() {
	sql.Register("vibejson", Driver{})
}

// Driver is the database/sql driver, registered under the name "vibejson".
// Its zero value is ready to use; the package's init registers one, so
// importing this package for its side effect is enough.
type Driver struct{}

var (
	_ driver.Driver        = Driver{}
	_ driver.DriverContext = Driver{}
)

// Open resolves the DSN and returns a connection. See the package
// documentation for what a DSN may be.
func (d Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector resolves the DSN once instead of once per connection.
//
// It is what makes a file DSN work at all: a durable collection takes an
// exclusive writer lock on its file, so the handle has to be opened once and
// shared, and the connector is the object database/sql closes when the DB is
// closed — which is the only signal this package gets that the file may be
// released.
func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	src, err := acquire(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{src: src}, nil
}

// A connector holds one resolved source and hands out connections onto it.
type connector struct {
	src *source
}

var (
	_ driver.Connector = (*connector)(nil)
	_ io.Closer        = (*connector)(nil)
)

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &conn{src: c.src}, nil
}

func (c *connector) Driver() driver.Driver { return Driver{} }

// Close releases this connector's reference to its source, closing a durable
// collection this package opened once no connector holds it. database/sql calls
// it from DB.Close.
func (c *connector) Close() error { return c.src.release() }

// A conn is one database/sql connection: a source, and the execution storage
// every statement on it reuses.
type conn struct {
	src *source
	// exec is this connection's execution storage. It is reused across every
	// statement and every execution, which is where the steady-state
	// allocation contract comes from.
	exec query.Exec
	// args is the conversion buffer driver arguments are copied into.
	// []driver.Value and []any hold the same thing and are not convertible to
	// one another, so the copy is unavoidable; retaining the destination makes
	// it free after the first bind.
	args []any
	// open is the rows currently reading exec, if any. database/sql holds a
	// connection until its rows are closed, so this is a diagnostic against
	// misuse rather than a lock.
	open   bool
	closed bool

	// tx is the open transaction, or nil. database/sql runs one transaction per
	// connection at a time and routes every statement inside it to this same
	// connection, so a single field is the whole of the bookkeeping.
	tx *tx

	// The write path's retained scratch. A mutation is decided before it is
	// applied — see write.go — and these hold that decision: the mutation list,
	// the keys a primary-key condition resolved, the buffer an existence check
	// reads a document into, and the two scan buffers that stop a filtered
	// UPDATE or DELETE from allocating once per document it examines.
	mutations   []mutation
	keys        []string
	docBuf      []byte
	scanScratch []byte
	keyScratch  []byte
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
)

// Prepare parses and lowers one statement, compiling its plan now so a
// malformed statement fails where it was written.
func (c *conn) Prepare(src string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), src)
}

// PrepareContext is Prepare with a context. Preparation is CPU-bound and
// bounded by the statement's length, so the context is consulted once, at the
// start, rather than pretended to be checked throughout.
func (c *conn) PrepareContext(ctx context.Context, src string) (driver.Stmt, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	return c.prepare(src, false)
}

// prepare routes one statement to the front end that takes it.
//
// The routing decision is the statement's leading keyword, read by
// [sqlast.KindOf], which lexes one token and allocates nothing. Routing before
// parsing rather than parsing and then switching on the result keeps the two
// prepared forms genuinely separate: a SELECT never carries a mutation's
// machinery and a mutation never carries a result schema, so neither has a
// field the other has to leave nil and check.
func (c *conn) prepare(src string, adhoc bool) (*stmt, error) {
	if sqlast.KindOf(src).IsQuery() {
		statement, err := query.PrepareStatement(src)
		if err != nil {
			return nil, err
		}
		return &stmt{conn: c, statement: statement, adhoc: adhoc}, nil
	}
	mutation, err := query.PrepareDML(src)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, mutation: mutation, adhoc: adhoc}, nil
}

// QueryContext runs a statement that was not prepared.
//
// It exists so the unprepared path is one round trip rather than the
// prepare-query-close database/sql would otherwise perform, and so that its
// cost is honestly attributable: this path parses and compiles the statement on
// every call and throws the plan away. In a loop that is the wrong shape — hold
// a *sql.Stmt — and the package benchmarks measure the difference rather than
// asserting it is small.
func (c *conn) QueryContext(
	ctx context.Context, src string, args []driver.NamedValue,
) (driver.Rows, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	s, err := c.prepare(src, true)
	if err != nil {
		return nil, err
	}
	rows, err := s.QueryContext(ctx, args)
	if err != nil {
		// Nothing else will ever close an ad-hoc statement: on success its rows
		// own it, and on failure there are no rows. Leaking one leaks a
		// compiler's arenas per failed call, which a retry loop turns into a
		// leak proportional to the retries.
		_ = s.Close()
		return nil, err
	}
	return rows, nil
}

// ExecContext runs a mutation or a definition that was not prepared. It is
// QueryContext's counterpart and carries the same cost note: this path parses
// and compiles on every call.
func (c *conn) ExecContext(
	ctx context.Context, src string, args []driver.NamedValue,
) (driver.Result, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	s, err := c.prepare(src, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	return s.ExecContext(ctx, args)
}

// Close releases the connection's execution storage. The source outlives it and
// is released by the connector.
func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	// A connection closed with a transaction open discards it. database/sql
	// rolls back before closing, so reaching here with one open means the
	// application dropped the DB with work in flight; discarding is what a
	// transaction that never committed means.
	if c.tx != nil {
		_ = c.tx.Rollback()
	}
	c.closed = true
	c.exec.Release()
	return nil
}

// Ping reports whether the connection can still serve statements.
func (c *conn) Ping(ctx context.Context) error { return c.usable(ctx) }

// IsValid tells database/sql's pool whether this connection may be reused.
func (c *conn) IsValid() bool { return !c.closed }

// ResetSession is called before a pooled connection is handed out again. The
// only per-session state is an open transaction, which database/sql should
// already have ended; see the body for why it is discarded rather than trusted.
func (c *conn) ResetSession(ctx context.Context) error {
	if c.tx != nil {
		// A pooled connection handed back with a transaction still open is a
		// bug somewhere above, and inheriting it would give the next borrower
		// somebody else's staged writes. Discarding it is the only safe answer.
		_ = c.tx.Rollback()
	}
	return c.usable(ctx)
}

// usable reports whether the connection can start work now.
//
// The context is checked here and nowhere else, and that is a limitation worth
// naming rather than burying: the executor has no cancellation hook, so a query
// already scanning cannot be interrupted. Cancelling before it starts is what
// this driver can honestly offer, and claiming more by checking the context in
// a loop that does not own the scan would be theatre.
func (c *conn) usable(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	return ctx.Err()
}

// values copies driver arguments into the connection's retained []any, checking
// their ordinals.
//
// A driver.NamedValue carries a 1-based ordinal, and database/sql fills them in
// order for '?' placeholders, but the check is cheap and a mis-ordered bind
// would otherwise silently compare against the wrong literal.
func (c *conn) values(args []driver.NamedValue) ([]any, error) {
	if cap(c.args) < len(args) {
		c.args = make([]any, len(args))
	}
	c.args = c.args[:len(args)]
	for i := range c.args {
		c.args[i] = nil
	}
	for _, arg := range args {
		if arg.Name != "" {
			return nil, errors.New(
				"vibesql: named parameters are not supported; the dialect has '?' placeholders")
		}
		if arg.Ordinal < 1 || arg.Ordinal > len(args) {
			return nil, errors.New("vibesql: argument ordinal out of range")
		}
		c.args[arg.Ordinal-1] = arg.Value
	}
	return c.args, nil
}
