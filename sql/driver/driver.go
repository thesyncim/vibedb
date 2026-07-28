package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"io"
	"sync"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func init() {
	stdsql.Register("vibedb", Driver{})
}

// Driver is the database/sql driver registered as "vibedb".
type Driver struct{}

var (
	_ sqldriver.Driver        = Driver{}
	_ sqldriver.DriverContext = Driver{}
)

// Open opens one connection. database/sql normally uses OpenConnector so all
// pooled connections share the one writer-locked durable handle.
func (d Driver) Open(dsn string) (sqldriver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	c, err := connector.Connect(context.Background())
	if err != nil {
		_ = connector.(*dbConnector).Close()
		return nil, err
	}
	c.(*conn).standalone = connector.(*dbConnector)
	return c, nil
}

// OpenConnector opens the catalog once and returns a connector that shares it
// across database/sql's connection pool.
func (Driver) OpenConnector(dsn string) (sqldriver.Connector, error) {
	db, err := openDatabase(dsn)
	if err != nil {
		return nil, err
	}
	return &dbConnector{db: db}, nil
}

type dbConnector struct {
	mu     sync.Mutex
	db     *database
	closed bool
}

var (
	_ sqldriver.Connector = (*dbConnector)(nil)
	_ io.Closer           = (*dbConnector)(nil)
)

func (c *dbConnector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, sqldriver.ErrBadConn
	}
	return &conn{db: c.db}, nil
}

func (c *dbConnector) Driver() sqldriver.Driver { return Driver{} }

func (c *dbConnector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.db.close()
}

type conn struct {
	db         *database
	exec       query.Exec
	args       []any
	open       bool
	closed     bool
	standalone *dbConnector
}

var (
	_ sqldriver.Conn               = (*conn)(nil)
	_ sqldriver.ConnPrepareContext = (*conn)(nil)
	_ sqldriver.ConnBeginTx        = (*conn)(nil)
	_ sqldriver.QueryerContext     = (*conn)(nil)
	_ sqldriver.ExecerContext      = (*conn)(nil)
	_ sqldriver.Pinger             = (*conn)(nil)
	_ sqldriver.Validator          = (*conn)(nil)
	_ sqldriver.SessionResetter    = (*conn)(nil)
)

func (c *conn) Prepare(src string) (sqldriver.Stmt, error) {
	return c.PrepareContext(context.Background(), src)
}

func (c *conn) PrepareContext(ctx context.Context, src string) (sqldriver.Stmt, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	tree, err := sqlast.ParseStatement(src)
	if err != nil {
		return nil, err
	}
	if err := c.validateSurface(tree); err != nil {
		return nil, err
	}
	s := &stmt{conn: c, tree: tree}
	if tree.Kind.IsQuery() {
		s.query, err = query.PrepareStatement(src)
	} else {
		s.mutation, err = query.PrepareDML(src)
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *conn) QueryContext(ctx context.Context, src string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	s, err := c.PrepareContext(ctx, src)
	if err != nil {
		return nil, err
	}
	prepared := s.(*stmt)
	prepared.adhoc = true
	rows, err := prepared.QueryContext(ctx, args)
	if err != nil {
		_ = prepared.Close()
	}
	return rows, err
}

func (c *conn) ExecContext(ctx context.Context, src string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	s, err := c.PrepareContext(ctx, src)
	if err != nil {
		return nil, err
	}
	prepared := s.(*stmt)
	defer prepared.Close()
	return prepared.ExecContext(ctx, args)
}

func (c *conn) Begin() (sqldriver.Tx, error) {
	return nil, ErrAutocommitOnly
}

func (c *conn) BeginTx(ctx context.Context, _ sqldriver.TxOptions) (sqldriver.Tx, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	return nil, ErrAutocommitOnly
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.exec.Release()
	if c.standalone != nil {
		return c.standalone.Close()
	}
	return nil
}

func (c *conn) Ping(ctx context.Context) error { return c.usable(ctx) }
func (c *conn) IsValid() bool                  { return !c.closed }
func (c *conn) ResetSession(ctx context.Context) error {
	return c.usable(ctx)
}

func (c *conn) usable(ctx context.Context) error {
	if c.closed {
		return sqldriver.ErrBadConn
	}
	return ctx.Err()
}

func (c *conn) values(args []sqldriver.NamedValue) ([]any, error) {
	if cap(c.args) < len(args) {
		c.args = make([]any, len(args))
	}
	c.args = c.args[:len(args)]
	clear(c.args)
	for _, arg := range args {
		if arg.Name != "" {
			return nil, errors.New("vibedb: named parameters are not supported; use '?'")
		}
		if arg.Ordinal < 1 || arg.Ordinal > len(args) {
			return nil, errors.New("vibedb: argument ordinal out of range")
		}
		c.args[arg.Ordinal-1] = arg.Value
	}
	return c.args, nil
}
