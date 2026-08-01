package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

type stmt struct {
	conn         *conn
	tree         *sqlast.Statement
	query        *query.Statement
	mutation     *query.DMLStatement
	primaryPoint bool
	params       int
	paramKinds   []ParamKind
	joinNames    []string
	explain      bool
	analyze      bool
	closed       bool
}

var (
	_ sqldriver.Stmt             = (*stmt)(nil)
	_ sqldriver.StmtExecContext  = (*stmt)(nil)
	_ sqldriver.StmtQueryContext = (*stmt)(nil)
)

func (s *stmt) NumInput() int { return s.params }

func (s *stmt) checkArgumentCount(got int) error {
	want := s.NumInput()
	if got == want {
		return nil
	}
	return fmt.Errorf(
		"vibedb: statement expects %d arguments, got %d",
		want, got,
	)
}

func (s *stmt) preflightExec(got int) error {
	if s.closed {
		return errors.New("vibedb: statement is closed")
	}
	if s.mutation == nil {
		return errors.New("vibedb: SELECT returns rows; use Query")
	}
	if s.query != nil {
		return fmt.Errorf("vibedb: %s RETURNING returns rows; use Query", s.tree.Kind)
	}
	return s.checkArgumentCount(got)
}

func (s *stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.query != nil {
		s.query.Release()
	}
	if s.mutation != nil {
		s.mutation.Release()
	}
	// Both prepared forms borrow the parsed tree, whose arenas can be as large
	// as the bounded SQL input. Keep only the scalar parameter count needed by
	// NumInput after Close; a caller retaining a closed Stmt must not retain the
	// parser and compiler high-water storage with it.
	s.tree = nil
	s.query = nil
	s.mutation = nil
	s.primaryPoint = false
	s.paramKinds = nil
	s.joinNames = nil
	s.explain = false
	s.analyze = false
	s.conn = nil
	return nil
}

func (s *stmt) Query(values []sqldriver.Value) (sqldriver.Rows, error) {
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.query == nil {
		return nil, fmt.Errorf("vibedb: %s returns no rows; use Exec", s.tree.Kind)
	}
	if err := s.checkArgumentCount(len(values)); err != nil {
		return nil, err
	}
	if err := checkSQLValues(values); err != nil {
		return nil, err
	}
	args := s.conn.positionalValues(values)
	return s.queryRows(backgroundContext, args)
}

// QueryContext bridges ctx into the executor only for the duration of this
// materializing query. The watcher exists only for a cancellable context; the
// background path retains the same nil CancelFlag used by Query.
func (s *stmt) QueryContext(
	ctx context.Context,
	values []sqldriver.NamedValue,
) (rowset sqldriver.Rows, err error) {
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.query == nil {
		return nil, fmt.Errorf("vibedb: %s returns no rows; use Exec", s.tree.Kind)
	}
	if err := s.checkArgumentCount(len(values)); err != nil {
		return nil, err
	}
	if err := checkSQLNamedValues(values); err != nil {
		return nil, err
	}
	args, err := s.conn.values(values)
	if err != nil {
		return nil, err
	}
	scope, err := s.conn.beginContextCancellation(ctx)
	if err != nil {
		clear(args)
		return nil, err
	}
	if scope != nil {
		defer func() { err = scope.finish(err) }()
	}
	return s.queryRows(ctx, args)
}

// queryRows is the typed query primitive shared by database/sql and the public
// runtime. args must be connection-owned storage: it is cleared on return so an
// idle session never pins caller values. Runtime callers copy interface headers
// into conn.args before arriving here.
func (s *stmt) queryRows(ctx context.Context, args []any) (*rows, error) {
	defer clear(args)
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.query == nil {
		return nil, fmt.Errorf("vibedb: %s returns no rows; use Exec", s.tree.Kind)
	}
	if err := s.checkArgumentCount(len(args)); err != nil {
		return nil, err
	}
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	if s.conn.open {
		return nil, errors.New("vibedb: close the current rows before querying again on this connection")
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if s.explain {
		if s.analyze {
			return s.analyzeRows(ctx, args)
		}
		plan, err := s.explainPlan(ctx, args)
		if err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(
			s, query.NewTextCursor("QUERY PLAN", plan), nil,
		), nil
	}
	var err error
	if s.mutation != nil {
		var cursor query.Cursor
		if s.conn.tx != nil {
			cursor, err = s.conn.tx.execMutationReturningContext(
				ctx, s.mutation, args, s.query,
			)
		} else {
			cursor, err = s.conn.mutationReturningContext(
				ctx, s.mutation, args, s.query,
			)
		}
		if err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	if s.conn.tx != nil {
		if s.query.RequiresCatalog() {
			source, err := s.conn.materializeTransactionJoinSource(
				ctx, s.conn.tx, s.tree.Select, s.joinNames)
			if err != nil {
				return nil, err
			}
			cursor, err := s.query.RunInto(&s.conn.exec, source, args)
			if err != nil {
				return nil, err
			}
			if err := contextCheckpoint(ctx); err != nil {
				return nil, err
			}
			s.conn.open = true
			return s.conn.resetRows(s, cursor, nil), nil
		}
		state, ok := s.conn.tx.tables[s.query.Collection()]
		if !ok {
			return nil, fmt.Errorf(
				"%w: %q was not present when the transaction began",
				ErrTableNotFound, s.query.Collection())
		}
		var source query.Source
		if s.primaryPoint {
			keys, keyErr := s.conn.bindPointPredicateKeys(
				s.tree.Select.Where, args, state.limits.MaxKeyBytes)
			if keyErr != nil {
				return nil, keyErr
			}
			source, err = s.conn.pointTransactionSource(ctx, state, keys)
			if errors.Is(err, errPointMaterializationTooLarge) {
				source, err = s.conn.tx.querySource(s.query.Collection())
			}
		} else {
			source, err = s.conn.tx.querySource(s.query.Collection())
		}
		if err != nil {
			return nil, err
		}
		cursor, err := s.query.RunInto(&s.conn.exec, source, args)
		if err != nil {
			return nil, err
		}
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	if err := rlockContext(ctx, &s.conn.db.mu); err != nil {
		return nil, err
	}
	if s.query.RequiresCatalog() {
		clear(s.conn.joinCatalog)
		collections := s.conn.joinCatalog[:0]
		for _, name := range s.joinNames {
			t, ok := s.conn.db.tables[name]
			if !ok {
				s.conn.releaseJoinCatalog(collections)
				s.conn.db.mu.RUnlock()
				return nil, fmt.Errorf("%w: %q", ErrTableNotFound, name)
			}
			collections = append(collections, durable.NamedCollection{
				Name: name, Collection: t.collection,
			})
		}
		if err := contextCheckpoint(ctx); err != nil {
			s.conn.releaseJoinCatalog(collections)
			s.conn.db.mu.RUnlock()
			return nil, err
		}
		catalog, snapshotErr := durable.SnapshotCollections(collections)
		s.conn.releaseJoinCatalog(collections)
		s.conn.db.mu.RUnlock()
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		source, materializeErr := s.conn.materializeDurableJoinSource(
			ctx, catalog, s.tree.Select, s.joinNames)
		closeErr := catalog.Close()
		if materializeErr != nil {
			return nil, materializeErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		cursor, runErr := s.query.RunInto(&s.conn.exec, source, args)
		if runErr != nil {
			return nil, runErr
		}
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	t, ok := s.conn.db.tables[s.query.Collection()]
	if !ok {
		s.conn.db.mu.RUnlock()
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, s.query.Collection())
	}
	var (
		source   query.Source
		snapshot *durable.Snapshot
	)
	if s.primaryPoint {
		limits, limitErr := tableMutationLimits(t)
		if limitErr != nil {
			err = limitErr
		} else {
			keys, keyErr := s.conn.bindPointPredicateKeys(
				s.tree.Select.Where, args, limits.MaxKeyBytes)
			if keyErr != nil {
				err = keyErr
			} else if t.collection == nil {
				source = query.FromSnapshot(store.Snapshot{})
			} else {
				source, err = s.conn.pointCollectionSource(ctx, t.collection, keys)
				if errors.Is(err, errPointMaterializationTooLarge) {
					snapshot, err = t.collection.Snapshot()
					if err == nil {
						source = query.FromFile(snapshot)
					}
				}
			}
		}
	} else if t.collection == nil {
		source = query.FromSnapshot(store.Snapshot{})
	} else {
		snapshot, err = t.collection.Snapshot()
		if err == nil {
			source = query.FromFile(snapshot)
		}
	}
	s.conn.db.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	cursor, err := s.query.RunInto(&s.conn.exec, source, args)
	if err != nil {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return nil, err
	}
	s.conn.open = true
	return s.conn.resetRows(s, cursor, snapshot), nil
}

// analyzeRows deliberately re-enters the existing normal query path. This
// keeps EXPLAIN ANALYZE honest: it measures the same source selection, bind,
// index admission, joins, scans, and result pipeline that SELECT uses. The
// plain SELECT path never reaches this method and pays no timer/counter cost.
func (s *stmt) analyzeRows(ctx context.Context, args []any) (*rows, error) {
	started := time.Now()
	s.explain = false
	resultRows, err := s.queryRows(ctx, args)
	s.explain = true
	if err != nil {
		return nil, err
	}
	returned := 0
	for resultRows.cursor.Next() {
		returned++
	}

	options := query.ExplainOptions{PrimaryPoint: s.primaryPoint}
	if resultRows.snapshot != nil {
		options.IndexCatalogKnown = true
		options.Indexes = resultRows.snapshot.AppendIndexes(nil)
	} else {
		options, err = s.explainOptions(ctx)
		if err != nil {
			_ = resultRows.Close()
			return nil, err
		}
	}
	pointSource := resultRows.snapshot == nil
	if err := resultRows.Close(); err != nil {
		return nil, err
	}
	stats := s.conn.exec.Stats
	plan, err := s.query.ExplainAnalyze(options, query.ExplainAnalysis{
		ElapsedNanoseconds: time.Since(started).Nanoseconds(),
		Rows:               returned,
		ActualAccessPath:   analyzeAccessPath(s, pointSource, stats),
		Stats:              stats,
	})
	if err != nil {
		return nil, err
	}
	s.conn.open = true
	return s.conn.resetRows(
		s, query.NewTextCursor("QUERY PLAN", plan), nil,
	), nil
}

func analyzeAccessPath(s *stmt, pointSource bool, stats query.ExecStats) string {
	if s.query.RequiresCatalog() {
		switch {
		case stats.JoinBuilds != 0:
			return "hash-build-and-probe"
		case stats.JoinMemberships != 0:
			return "join-membership"
		case stats.JoinLookups != 0:
			return "join-key-lookup"
		default:
			return "join-no-matches"
		}
	}
	if s.primaryPoint && pointSource {
		return "primary-key-point"
	}
	if stats.IndexBounded {
		return "exact-index"
	}
	return "full-scan"
}

// explainPlan is cold-path work: it binds only the EXPLAIN statement, captures
// immutable index metadata, and never opens a row source. Keeping it here
// leaves normal query execution on the existing source-selection and RunInto
// path with no explain-specific planner work.
func (s *stmt) explainPlan(ctx context.Context, args []any) (string, error) {
	if err := contextCheckpoint(ctx); err != nil {
		return "", err
	}
	options, err := s.explainOptions(ctx)
	if err != nil {
		return "", err
	}
	return s.query.ExplainBoundWith(args, options)
}

func (s *stmt) explainOptions(ctx context.Context) (query.ExplainOptions, error) {
	options := query.ExplainOptions{
		PrimaryPoint: s.primaryPoint,
	}
	// The driver materializes joins into the heap executor, whose source has
	// no durable catalog. Keep the plan logical rather than attributing the
	// original table's indexes to the materialized execution.
	if s.query.RequiresCatalog() {
		return options, contextCheckpoint(ctx)
	}
	options.IndexCatalogKnown = true
	collection := s.query.Collection()
	if s.conn.tx != nil {
		state, ok := s.conn.tx.tables[collection]
		if !ok {
			return query.ExplainOptions{}, fmt.Errorf(
				"%w: %q was not present when the transaction began",
				ErrTableNotFound, collection)
		}
		if state.snapshot != nil {
			options.Indexes = state.snapshot.AppendIndexes(options.Indexes)
		}
		return options, contextCheckpoint(ctx)
	}
	if err := rlockContext(ctx, &s.conn.db.mu); err != nil {
		return query.ExplainOptions{}, err
	}
	t, ok := s.conn.db.tables[collection]
	if !ok {
		s.conn.db.mu.RUnlock()
		return query.ExplainOptions{}, fmt.Errorf("%w: %q", ErrTableNotFound, collection)
	}
	if t.collection == nil {
		s.conn.db.mu.RUnlock()
		return options, contextCheckpoint(ctx)
	}
	snapshot, err := t.collection.Snapshot()
	s.conn.db.mu.RUnlock()
	if err != nil {
		return query.ExplainOptions{}, err
	}
	options.Indexes = snapshot.AppendIndexes(options.Indexes)
	closeErr := snapshot.Close()
	if closeErr != nil {
		return query.ExplainOptions{}, closeErr
	}
	return options, contextCheckpoint(ctx)
}

func (c *conn) releaseJoinCatalog(collections []durable.NamedCollection) {
	c.joinCatalog = collections
	clear(c.joinCatalog)
	c.joinCatalog = c.joinCatalog[:0]
}

// pointCollectionSource copies only the primary-key candidates into a
// connection-owned scratch Segment. Segment.Reset retains its arenas, so after
// one warm execution this preserves the O(candidate keys) point-read path
// without constructing a heap Collection for every query.
//
// Result cells from a Segment borrow it. The connection's one-open-rows rule
// therefore is also the lifetime fence: another query cannot Reset pointDocs
// until the current rows have closed.
func (c *conn) pointCollectionSource(
	ctx context.Context,
	collection *durable.Collection,
	keys []string,
) (query.Source, error) {
	c.pointDocs.Reset()
	limit, err := driverQueryMemory(c.exec.Options)
	if err != nil {
		return query.Source{}, err
	}
	budget := pointMaterializationBudget{limit: limit}
	document := c.pointRaw[:0]
	for _, key := range keys {
		if err := contextCheckpoint(ctx); err != nil {
			c.pointRaw = document
			c.pointDocs.Reset()
			return query.Source{}, err
		}
		var (
			found bool
			err   error
		)
		document, found, err = collection.AppendRaw(document[:0], []byte(key))
		if err != nil {
			c.pointRaw = document
			return query.Source{}, err
		}
		if found {
			if err := budget.add(key, document); err != nil {
				c.pointRaw = document
				c.pointDocs.Reset()
				return query.Source{}, err
			}
			if _, err := c.pointDocs.Append(document); err != nil {
				c.pointRaw = document
				return query.Source{}, err
			}
		}
	}
	c.pointRaw = document
	return query.FromSegment(&c.pointDocs), nil
}

func (c *conn) pointTransactionSource(
	ctx context.Context,
	state *txTable,
	keys []string,
) (query.Source, error) {
	c.pointDocs.Reset()
	limit, err := driverQueryMemory(c.exec.Options)
	if err != nil {
		return query.Source{}, err
	}
	budget := pointMaterializationBudget{limit: limit}
	document := c.pointRaw[:0]
	for _, key := range keys {
		if err := contextCheckpoint(ctx); err != nil {
			c.pointRaw = document
			c.pointDocs.Reset()
			return query.Source{}, err
		}
		var (
			found bool
			err   error
		)
		document, found, err = state.appendRaw(document[:0], key)
		if err != nil {
			c.pointRaw = document
			return query.Source{}, err
		}
		if found {
			if err := budget.add(key, document); err != nil {
				c.pointRaw = document
				c.pointDocs.Reset()
				return query.Source{}, err
			}
			if _, err := c.pointDocs.Append(document); err != nil {
				c.pointRaw = document
				return query.Source{}, err
			}
		}
	}
	c.pointRaw = document
	return query.FromSegment(&c.pointDocs), nil
}

var errPointMaterializationTooLarge = errors.New(
	"vibedb: primary point-source materialization exceeds the query memory bound")

type pointMaterializationBudget struct {
	limit int64
	used  int64
}

func (b *pointMaterializationBudget) add(key string, document []byte) error {
	raw := int64(len(key)) + int64(len(document))
	remaining := b.limit - b.used
	if remaining < joinMaterializationRowBytes ||
		raw > (remaining-joinMaterializationRowBytes)/
			joinMaterializationExpansion {
		return errPointMaterializationTooLarge
	}
	b.used += raw*joinMaterializationExpansion + joinMaterializationRowBytes
	return nil
}

func (s *stmt) Exec(values []sqldriver.Value) (sqldriver.Result, error) {
	if err := s.preflightExec(len(values)); err != nil {
		return nil, err
	}
	if err := checkSQLValues(values); err != nil {
		return nil, err
	}
	return s.exec(backgroundContext, s.conn.positionalValues(values))
}

// ExecContext is the context-aware mutation boundary required by database/sql.
// Scan-shaped UPDATE and DELETE operations observe ctx through the same query
// CancelFlag as SELECT. Cancellation is joined before return, and publication
// still begins only after the final contextCheckpoint in the mutation path.
func (s *stmt) ExecContext(
	ctx context.Context,
	arguments []sqldriver.NamedValue,
) (result sqldriver.Result, err error) {
	if err := s.preflightExec(len(arguments)); err != nil {
		return nil, err
	}
	if err := checkSQLNamedValues(arguments); err != nil {
		return nil, err
	}
	args, err := s.conn.values(arguments)
	if err != nil {
		return nil, err
	}
	scope, err := s.conn.beginContextCancellation(ctx)
	if err != nil {
		clear(args)
		return nil, err
	}
	if scope != nil {
		defer func() { err = scope.finish(err) }()
	}
	return s.exec(ctx, args)
}

func (s *stmt) exec(ctx context.Context, args []any) (sqldriver.Result, error) {
	defer clear(args)
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.mutation == nil {
		return nil, errors.New("vibedb: SELECT returns rows; use Query")
	}
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	if s.conn.open {
		return nil, errors.New("vibedb: close the current rows before executing another statement on this connection")
	}
	if s.conn.tx != nil {
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		return s.conn.tx.execMutationContext(ctx, s.mutation, args)
	}
	return s.conn.execMutationContext(ctx, s.mutation, args)
}
