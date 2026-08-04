package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func init() {
	stdsql.Register("vibedb", Driver{})
}

// Driver is the database/sql driver registered as "vibedb".
type Driver struct{}

// SQL tables currently freeze the durable default as their maximum document
// size. No string-shaped scalar larger than one document can match a row or be
// stored in one, so reject it before the query compiler interns a copy.
const maxSQLParameterBytes = 4 << 20

// maxSQLArgumentsBytes bounds one execution's aggregate borrowed payload.
// Parameter count bounds cap interface/header storage, while this prevents a
// legal wide VALUES or IN list from retaining thousands of individually legal
// multi-megabyte values on an otherwise idle pooled connection.
const maxSQLArgumentsBytes = 16 << 20

// driverQueryWorkers keeps one database/sql connection to one executor worker.
// database/sql already obtains concurrency by opening several connections; if
// every pooled connection inherited query's zero-value GOMAXPROCS policy, each
// one could retain a scanner and a full worker pool after its first durable
// scan. Multiplying that by MaxOpenConns oversubscribes the process and makes
// idle pooled connections retain worker arenas they do not need.
const driverQueryWorkers = 1

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
	// Driver.Open hands out exactly one connection. Retire its private
	// connector now; the connector's live-connection reference keeps the
	// database open until that connection closes.
	if err := connector.(*dbConnector).Close(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// OpenConnector opens the catalog once and returns a connector that shares it
// across database/sql's connection pool.
func (Driver) OpenConnector(dsn string) (sqldriver.Connector, error) {
	return openConnector(dsn)
}

// openConnector is the one catalog-ownership constructor shared by
// database/sql and the typed runtime. Keeping the writer lease behind one
// reference-counted owner means neither adapter can accidentally invent a
// second close policy.
func openConnector(dsn string) (*dbConnector, error) {
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
	refs   int
}

const (
	terminalRetirementInitialBackoff = 10 * time.Millisecond
	terminalRetirementMaximumBackoff = time.Second
)

// terminalRetirements is the process ownership boundary for a database whose
// final Collection.Close stopped at a retryable phase. database/sql does not
// promise another Connector.Close call, and a failed open has no caller at all;
// keeping only the connector or stack frame would therefore strand writer
// locks when that object becomes unreachable. One bounded worker owns every
// such database and retries their monotonic close state machines with backoff.
// It exits when the registry empties, so the ordinary successful-close path
// retains no goroutine or timer.
var terminalRetirements struct {
	sync.Mutex
	pending map[*database]struct{}
	running bool
}

func retainTerminalDatabase(d *database) {
	if d == nil || d.closeCompleted() {
		return
	}
	terminalRetirements.Lock()
	if terminalRetirements.pending == nil {
		terminalRetirements.pending = make(map[*database]struct{})
	}
	terminalRetirements.pending[d] = struct{}{}
	if !terminalRetirements.running {
		terminalRetirements.running = true
		go drainTerminalDatabases()
	}
	terminalRetirements.Unlock()
}

func drainTerminalDatabases() {
	backoff := terminalRetirementInitialBackoff
	var databases []*database
	for {
		terminalRetirements.Lock()
		if len(terminalRetirements.pending) == 0 {
			terminalRetirements.running = false
			terminalRetirements.Unlock()
			return
		}
		clear(databases)
		databases = databases[:0]
		for d := range terminalRetirements.pending {
			databases = append(databases, d)
		}
		terminalRetirements.Unlock()

		completedAny := false
		for _, d := range databases {
			_ = d.closeTerminal()
			if !d.closeCompleted() {
				continue
			}
			terminalRetirements.Lock()
			delete(terminalRetirements.pending, d)
			terminalRetirements.Unlock()
			completedAny = true
		}
		if completedAny {
			backoff = terminalRetirementInitialBackoff
		} else if backoff < terminalRetirementMaximumBackoff {
			backoff *= 2
			if backoff > terminalRetirementMaximumBackoff {
				backoff = terminalRetirementMaximumBackoff
			}
		}
		time.Sleep(backoff)
	}
}

var (
	_ sqldriver.Connector = (*dbConnector)(nil)
	_ io.Closer           = (*dbConnector)(nil)
)

func (c *dbConnector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := mutexLockContext(ctx, &c.mu); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if c.closed {
		return nil, sqldriver.ErrBadConn
	}
	c.refs++
	return &conn{
		db: c.db, owner: c,
		exec: query.Exec{Options: query.ExecOptions{
			Workers: driverQueryWorkers,
		}},
	}, nil
}

func (c *dbConnector) Driver() sqldriver.Driver { return Driver{} }

func (c *dbConnector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.refs != 0 || c.db == nil {
		return nil
	}
	// database/sql does not retry Connector.Close. Once there are no live
	// connections, this connector is the database's sole owner. Completed
	// teardown is detached even when it carries a terminal persistence error;
	// retryable engine teardown remains owned here rather than invalidating its
	// descriptors or writer locks.
	err := c.db.closeTerminal()
	if c.db.closeCompleted() {
		c.db = nil
	} else {
		retainTerminalDatabase(c.db)
	}
	return err
}

func (c *dbConnector) release() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refs == 0 {
		return errors.New("vibedb: connector connection reference underflow")
	}
	c.refs--
	if c.refs != 0 || !c.closed || c.db == nil {
		return nil
	}
	// A connector can be retired while database/sql is still draining its last
	// connection. release is then its terminal-close attempt; incomplete engine
	// teardown remains owned by the retired connector.
	err := c.db.closeTerminal()
	if c.db.closeCompleted() {
		c.db = nil
	} else {
		retainTerminalDatabase(c.db)
	}
	return err
}

type conn struct {
	db             *database
	exec           query.Exec
	args           []any
	pointDocs      store.Segment
	pointRaw       []byte
	pointKeyRaw    []byte
	pointKeyEnds   []int
	pointKeys      []string
	matchKeys      []string
	insertSeeds    []seedDocument
	insertKeyRaw   []byte
	insertSeen     map[string]struct{}
	insertTape     []vibejson.IndexEntry
	joinCatalog    []durable.NamedCollection
	joinSnapshot   durable.DatabaseSnapshot
	insertSnapshot durable.Snapshot
	rowset         rows
	open           bool
	tx             *tx
	closed         bool
	owner          *dbConnector
	// routing is the per-connection Router used by the cluster facade's write
	// preflight. It is created on first placed write and stays nil for the
	// default single-store path.
	routing *distribution.Router
}

var (
	_ sqldriver.Conn               = (*conn)(nil)
	_ sqldriver.ConnPrepareContext = (*conn)(nil)
	_ sqldriver.ConnBeginTx        = (*conn)(nil)
	_ sqldriver.ExecerContext      = (*conn)(nil)
	_ sqldriver.NamedValueChecker  = (*conn)(nil)
	_ sqldriver.Pinger             = (*conn)(nil)
	_ sqldriver.Validator          = (*conn)(nil)
	_ sqldriver.SessionResetter    = (*conn)(nil)
)

// CheckNamedValue preserves exact decimal parameters. database/sql's default
// converter treats json.Number as a string-shaped named type; converting it to
// query.Number here keeps it numeric. Already-native driver values return
// directly, and the remaining ordinary Go numeric/Valuer forms run through the
// standard converter here, avoiding an ErrSkip round trip for every parameter.
func (c *conn) CheckNamedValue(value *sqldriver.NamedValue) error {
	if value.Name != "" {
		return errors.New("vibedb: named parameters are not supported; use '?'")
	}
	switch number := value.Value.(type) {
	case json.Number:
		if err := checkSQLParameterBytes(len(number)); err != nil {
			return err
		}
		value.Value = query.Number(number.String())
		return nil
	case query.Number:
		return checkSQLParameterBytes(len(number))
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return errors.New("vibedb: numeric parameters must be finite JSON numbers")
		}
		return nil
	case string:
		return checkSQLParameterBytes(len(number))
	case []byte:
		return checkSQLParameterBytes(len(number))
	case nil, bool, int64:
		return nil
	default:
		converted, err := sqldriver.DefaultParameterConverter.ConvertValue(value.Value)
		if err != nil {
			return err
		}
		if number, ok := converted.(float64); ok &&
			(math.IsNaN(number) || math.IsInf(number, 0)) {
			return errors.New("vibedb: numeric parameters must be finite JSON numbers")
		}
		switch converted := converted.(type) {
		case string:
			if err := checkSQLParameterBytes(len(converted)); err != nil {
				return err
			}
		case []byte:
			if err := checkSQLParameterBytes(len(converted)); err != nil {
				return err
			}
		}
		value.Value = converted
		return nil
	}
}

func checkSQLParameterBytes(size int) error {
	if size <= maxSQLParameterBytes {
		return nil
	}
	return fmt.Errorf(
		"vibedb: SQL parameter is %d bytes, maximum is %d: %w",
		size, maxSQLParameterBytes, durable.ErrDocumentTooLarge,
	)
}

func sqlArgumentBytes(value any) int {
	switch value := value.(type) {
	case string:
		return len(value)
	case []byte:
		return len(value)
	case json.Number:
		return len(value)
	case query.Number:
		return len(value)
	case nil:
		return 4
	default:
		return 8
	}
}

func addSQLArgumentBytes(total *int, value any) error {
	size := sqlArgumentBytes(value)
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

func checkSQLValues(values []sqldriver.Value) error {
	total := 0
	for _, value := range values {
		if err := addSQLArgumentBytes(&total, value); err != nil {
			return err
		}
	}
	return nil
}

func checkSQLNamedValues(values []sqldriver.NamedValue) error {
	total := 0
	for i := range values {
		if err := addSQLArgumentBytes(&total, values[i].Value); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) Prepare(src string) (sqldriver.Stmt, error) {
	return c.PrepareContext(context.Background(), src)
}

func (c *conn) PrepareContext(ctx context.Context, src string) (sqldriver.Stmt, error) {
	statement, err := c.prepareContext(ctx, src)
	if err != nil {
		return nil, err
	}
	return statement, nil
}

// prepareContext is the parse-once preparation primitive shared by
// database/sql and the typed runtime. The returned statement owns the parsed
// tree and both possible lowerings; adapters must not parse src independently.
func (c *conn) prepareContext(ctx context.Context, src string) (*stmt, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	var tree *sqlast.Statement
	var err error
	cancel := c.exec.Options.Cancel
	if ctx.Done() == nil && cancel == nil {
		tree, err = sqlast.ParseStatement(src)
	} else {
		var parser sqlast.Parser
		parser.SetCancellationCheck(func() error {
			if contextErr := contextCheckpoint(ctx); contextErr != nil {
				return contextErr
			}
			if cancel != nil && cancel.Canceled() {
				return query.ErrCanceled
			}
			return nil
		})
		tree = new(sqlast.Statement)
		err = parser.ParseStatement(tree, src)
	}
	if err != nil {
		var parseErr *sqlast.ParseError
		if errors.As(err, &parseErr) &&
			parseErr.Msg == "statement is not valid UTF-8" {
			return nil, errors.New("vibedb: SQL text must be valid UTF-8")
		}
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	s := &stmt{
		conn:       c,
		tree:       tree,
		params:     tree.Params(),
		paramKinds: statementParamKinds(tree),
	}
	s.paramPositions = statementDocumentParamPositions(tree, s.paramKinds)
	if tree.Kind == sqlast.KindCreateView || tree.Kind == sqlast.KindDropView {
		var ddl *preparedViewDDL
		ddl, err = c.prepareViewDDL(ctx, src, tree)
		if err != nil {
			return nil, err
		}
		s.views = &preparedViewState{ddl: ddl}
		return s, nil
	}
	if isSavepointKind(tree.Kind) {
		return s, nil
	}
	if err := c.validateViewStatementContext(ctx, src, tree); err != nil {
		return nil, err
	}
	viewQuery := tree.Select
	if tree.Kind == sqlast.KindInsert && tree.Insert.Source != nil {
		viewQuery = tree.Insert.Source
	}
	if viewQuery != nil {
		var dependencies []viewDependency
		dependencies, err = c.expandPreparedViews(ctx, src, viewQuery)
		if err != nil {
			return nil, err
		}
		if len(dependencies) != 0 {
			s.views = &preparedViewState{dependencies: dependencies}
		}
	}
	if err := c.validateSurfaceContext(ctx, tree); err != nil {
		return nil, err
	}
	if tree.Kind.IsQuery() {
		s.query, err = query.PrepareParsedStatement(src, tree.Select)
		if err == nil {
			s.explain = tree.Explain
			s.analyze = tree.Explain && tree.Analyze
			// Source requirements are properties of the prepared SELECT, not
			// of the wrapper that will consume it. EXPLAIN ANALYZE re-enters the
			// ordinary execution path, so it must retain the same join catalog or
			// primary-point fact as SELECT.
			if s.query.RequiresCatalog() || tree.Select.With != nil {
				s.dependencies = selectPhysicalDependencies(tree.Select)
				s.catalogJoin = selectContainsJoin(tree.Select)
			}
			if !s.query.RequiresCatalog() {
				if lockErr := rlockContext(ctx, &c.db.mu); lockErr != nil {
					s.query.Release()
					return nil, lockErr
				}
				t := c.db.tables[s.query.Collection()]
				if t != nil {
					s.pointPredicate = s.query.DrivingPredicate()
					s.primaryPoint = isPrimaryPredicate(
						s.pointPredicate, t.meta.PrimaryKey)
					if !s.primaryPoint {
						s.pointPredicate = nil
					}
				}
				c.db.mu.RUnlock()
			}
		}
	} else {
		s.mutation, err = query.PrepareParsedDML(src, tree)
		if err == nil && tree.Kind == sqlast.KindInsert &&
			tree.Insert.Source != nil {
			s.applyInsertSourceDocumentParams()
		}
		if err == nil && tree.Kind == sqlast.KindInsert &&
			tree.Insert.Source != nil {
			s.insertSource = &insertSelectPlan{
				statement:    s.mutation.InsertSource(),
				tree:         tree.Insert.Source,
				dependencies: selectPhysicalDependencies(tree.Insert.Source),
				catalogJoin:  selectContainsJoin(tree.Insert.Source),
			}
		}
		if err == nil && tree.Kind == sqlast.KindInsert &&
			tree.Insert.Returning != nil {
			s.query, err = query.PrepareParsedStatement(
				src, tree.Insert.Returning,
			)
		} else if err == nil && tree.Kind == sqlast.KindUpdate &&
			tree.Update.Returning != nil {
			s.query, err = query.PrepareParsedStatement(
				src, tree.Update.Returning,
			)
		} else if err == nil && tree.Kind == sqlast.KindDelete &&
			tree.Delete.Returning != nil {
			s.query, err = query.PrepareParsedStatement(
				src, tree.Delete.Returning,
			)
		}
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.validatePreparedViewDependencies(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
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
	return c.BeginTx(context.Background(), sqldriver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, options sqldriver.TxOptions) (sqldriver.Tx, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	if c.tx != nil {
		return nil, errors.New("vibedb: this connection already has an open transaction")
	}
	transaction, err := c.beginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.tx = transaction
	return transaction, nil
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	if c.open {
		return errors.New("vibedb: close the current rows before closing the connection")
	}
	c.closed = true
	if c.tx != nil {
		_ = c.tx.Rollback()
	}
	c.exec.Release()
	snapshotErr := c.joinSnapshot.Close()
	snapshotErr = errors.Join(snapshotErr, c.insertSnapshot.Close())
	owner := c.owner
	err := snapshotErr
	if owner != nil {
		// release may perform the database's terminal close, so it must run
		// while c still holds the ownership graph needed by that operation.
		err = errors.Join(err, owner.release())
	}
	// A closed direct driver.Conn is never reusable. Sever every retained
	// ownership and high-water edge after release so a caller holding the
	// invalid value cannot pin the catalog, collections, point materialization
	// arenas, argument boxes, or rows metadata indefinitely.
	c.db = nil
	c.owner = nil
	c.exec = query.Exec{}
	c.args = nil
	c.pointDocs = store.Segment{}
	c.pointRaw = nil
	c.pointKeyRaw = nil
	c.pointKeyEnds = nil
	c.pointKeys = nil
	c.matchKeys = nil
	c.insertSeeds = nil
	c.insertKeyRaw = nil
	c.insertSeen = nil
	c.insertTape = nil
	c.joinCatalog = nil
	c.joinSnapshot = durable.DatabaseSnapshot{}
	c.insertSnapshot = durable.Snapshot{}
	c.rowset = rows{closed: true}
	c.tx = nil
	c.routing = nil
	return err
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
	} else if len(c.args) > len(args) {
		clear(c.args[len(args):])
	}
	c.args = c.args[:len(args)]
	clear(c.args)
	for i, arg := range args {
		if arg.Name != "" {
			clear(c.args)
			return nil, errors.New("vibedb: named parameters are not supported; use '?'")
		}
		if arg.Ordinal != i+1 {
			clear(c.args)
			return nil, errors.New(
				"vibedb: argument ordinals must be contiguous and ordered from one")
		}
		c.args[arg.Ordinal-1] = arg.Value
	}
	return c.args, nil
}

func (c *conn) positionalValues(args []sqldriver.Value) []any {
	if cap(c.args) < len(args) {
		c.args = make([]any, len(args))
	} else if len(c.args) > len(args) {
		clear(c.args[len(args):])
	}
	c.args = c.args[:len(args)]
	for i := range args {
		c.args[i] = args[i]
	}
	return c.args
}
