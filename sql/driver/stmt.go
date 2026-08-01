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
	conn           *conn
	tree           *sqlast.Statement
	query          *query.Statement
	mutation       *query.DMLStatement
	pointPredicate *sqlast.Expr
	primaryPoint   bool
	catalogJoin    bool
	params         int
	paramKinds     []ParamKind
	dependencies   []physicalDependency
	explain        bool
	analyze        bool
	closed         bool
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
	s.pointPredicate = nil
	s.primaryPoint = false
	s.catalogJoin = false
	s.paramKinds = nil
	s.dependencies = nil
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
		if s.transactionRequiresCatalogSource() {
			source, err := s.conn.materializeTransactionJoinSource(
				ctx, s.conn.tx, s.query.Collection(), s.dependencies)
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
			return nil, s.missingDependency(s.query.Collection(), true)
		}
		var source query.Source
		if s.primaryPoint {
			keys, keyErr := s.conn.bindPointPredicateKeys(
				s.pointPredicate, args, state.limits.MaxKeyBytes)
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
	if s.requiresCatalogSource() {
		catalog, snapshotErr := s.snapshotCatalogDependenciesLocked(ctx)
		s.conn.db.mu.RUnlock()
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		if s.usesDirectDurableCatalog() {
			// Generalized SQL joins materialize their physical, derived, and CTE
			// operands inside the query runtime. Give that runtime the coherent
			// durable catalog directly so every physical operand can retain its
			// durable scan/index path. The catalog is needed only until RunInto
			// returns: durable cells are copied into result- or relation-owned
			// storage before then.
			cursor, runErr := s.query.RunInto(
				&s.conn.exec,
				query.FromFileDatabase(*catalog, s.query.Collection()),
				args,
			)
			closeErr := catalog.Close()
			if runErr != nil {
				if closeErr != nil {
					return nil, errors.Join(runErr, closeErr)
				}
				return nil, runErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if err := contextCheckpoint(ctx); err != nil {
				return nil, err
			}
			s.conn.open = true
			return s.conn.resetRows(s, cursor, nil), nil
		}
		source, materializeErr := s.conn.materializeDurableJoinSource(
			ctx, catalog, s.query.Collection(), s.dependencies)
		closeErr := catalog.Close()
		if err := errors.Join(materializeErr, closeErr); err != nil {
			return nil, err
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
		return nil, s.missingDependency(s.query.Collection(), false)
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
				s.pointPredicate, args, limits.MaxKeyBytes)
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
	if s.requiresCatalogSource() {
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
	// A catalog-dependent relation plan may read several physical sources, while
	// ExplainOptions carries one index catalog. Keep that part of the plan
	// logical rather than attributing one operand's indexes to every source;
	// EXPLAIN ANALYZE still reports the generalized stages' actual algorithms
	// and counters. Dependency validation uses the same coherent catalog cut.
	if s.requiresCatalogSource() {
		return options, s.validateCatalogDependenciesForExplain(ctx)
	}
	options.IndexCatalogKnown = true
	collection := s.query.Collection()
	if s.conn.tx != nil {
		state, ok := s.conn.tx.tables[collection]
		if !ok {
			return query.ExplainOptions{}, s.missingDependency(collection, true)
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
		return query.ExplainOptions{}, s.missingDependency(collection, false)
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

// requiresCatalogSource distinguishes coherent multi-collection execution
// from a nested plan whose every physical read resolves to one collection.
// The latter can keep the ordinary durable Source and its exact-index path;
// self-joins still require a catalog even though their distinct name count is
// one because the join executor intentionally rejects FromFile.
func (s *stmt) requiresCatalogSource() bool {
	if !s.query.RequiresCatalog() {
		return false
	}
	return s.catalogJoin || len(s.dependencies) != 1
}

// usesDirectDurableCatalog is the cross-layer source contract for SQL JOINs.
// The generalized relation pipeline owns operand materialization and can
// consume every physical source from one FromFileDatabase cut. The legacy
// physical equi-join pipeline retains the driver's existing bounded heap path
// until the durable executor can publish projected fan-out rows itself.
// Keeping the decision on the prepared statement makes the ordinary no-JOIN
// path one nil-cheap pointer test with no feature state or allocation.
func (s *stmt) usesDirectDurableCatalog() bool {
	return s != nil && s.query != nil && s.query.UsesGeneralizedRelationJoin()
}

func (s *stmt) transactionRequiresCatalogSource() bool {
	if s.requiresCatalogSource() {
		return true
	}
	if !s.query.RequiresCatalog() {
		return false
	}
	if len(s.dependencies) != 1 {
		// Keep the index below structurally guarded even if the catalog-source
		// classifier changes independently in the future.
		return true
	}
	// FromFileOverlay cannot be recursively rebound yet. Retain the coherent
	// transaction-catalog fallback only while staged writes coexist with a
	// durable BEGIN snapshot; no-pending and initially-empty views use the
	// direct file/segment source without sacrificing read-your-writes.
	state := s.conn.tx.tables[s.dependencies[0].name]
	return state != nil && state.snapshot != nil && len(state.pending) != 0
}

func (s *stmt) missingDependency(name string, transaction bool) error {
	for i := range s.dependencies {
		if s.dependencies[i].name == name {
			return missingTableDependency(
				name, s.dependencies[i].pos, transaction,
			)
		}
	}
	if s.tree != nil && s.tree.Select != nil && len(s.tree.Select.From) != 0 {
		return missingTableDependency(
			name, s.tree.Select.From[0].Pos, transaction,
		)
	}
	return missingTableDependency(name, 0, transaction)
}

// snapshotCatalogDependenciesLocked captures every physical relation the
// prepared statement may read at one durable instant. The caller holds db.mu
// for reading; retaining that lock through SnapshotCollections keeps DROP and
// replacement publication from changing the catalog between name resolution
// and the generation cut. The connection-owned destination retains only its
// high-water scratch after Close, so a stable catalog capture is allocation
// free once warm and cannot leak leases across executions.
func (s *stmt) snapshotCatalogDependenciesLocked(
	ctx context.Context,
) (*durable.DatabaseSnapshot, error) {
	clear(s.conn.joinCatalog)
	collections := s.conn.joinCatalog[:0]
	for _, dependency := range s.dependencies {
		t, ok := s.conn.db.tables[dependency.name]
		if !ok {
			s.conn.releaseJoinCatalog(collections)
			return nil,
				missingTableDependency(
					dependency.name, dependency.pos, false,
				)
		}
		collections = append(collections, durable.NamedCollection{
			Name: dependency.name, Collection: t.collection,
		})
	}
	if err := contextCheckpoint(ctx); err != nil {
		s.conn.releaseJoinCatalog(collections)
		return nil, err
	}
	err := durable.SnapshotCollectionsIntoContext(
		ctx, &s.conn.joinSnapshot, collections,
	)
	s.conn.releaseJoinCatalog(collections)
	if err != nil {
		return nil, err
	}
	return &s.conn.joinSnapshot, nil
}

// validateCatalogDependenciesForExplain performs the source-acquisition part
// of ordinary execution without materializing or scanning rows. A logical
// EXPLAIN may omit physical access details for catalog plans, but it must not
// claim an executable plan after one of its recursively discovered physical
// dependencies has disappeared or can no longer be snapshotted.
func (s *stmt) validateCatalogDependenciesForExplain(ctx context.Context) error {
	if s.conn.tx != nil {
		if s.conn.tx.done {
			return errors.New("vibedb: transaction is finished")
		}
		for _, dependency := range s.dependencies {
			if err := contextCheckpoint(ctx); err != nil {
				return err
			}
			if _, ok := s.conn.tx.tables[dependency.name]; !ok {
				return missingTableDependency(
					dependency.name, dependency.pos, true,
				)
			}
		}
		return contextCheckpoint(ctx)
	}
	if err := rlockContext(ctx, &s.conn.db.mu); err != nil {
		return err
	}
	catalog, err := s.snapshotCatalogDependenciesLocked(ctx)
	s.conn.db.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := catalog.Close(); err != nil {
		return err
	}
	return contextCheckpoint(ctx)
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
