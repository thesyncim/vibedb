package driver

import (
	"bytes"
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson/x/byteview"
)

type stmt struct {
	conn               *conn
	tree               *sqlast.Statement
	query              *query.Statement
	mutation           *query.DMLStatement
	insertSource       *insertSelectPlan
	views              *preparedViewState
	pointPredicate     *sqlast.Expr
	pointPath          string
	pointCandidate     bool
	primaryRange       *primaryRangeProgram
	primaryPoint       bool
	serialPointSafe    bool
	serialMutationSafe bool
	catalogJoin        bool
	params             int
	paramKinds         []ParamKind
	// paramTypes is absent for the ordinary schemaless path. It is allocated
	// only when statement analysis constrains at least one scalar parameter to
	// an exact SQL input domain.
	paramTypes []ParamType
	// paramTypePositions stores the first authored byte position plus one for
	// inferred typed parameters. It is absent with paramTypes and is consumed by
	// protocol adapters only when reporting a Parse-time type conflict.
	paramTypePositions []int
	// paramTypeTargetDefaults is absent unless PostgreSQL's unresolved target-list
	// rule supplied a parameter's text type. It is cold metadata only; execution
	// never reads it.
	paramTypeTargetDefaults []bool
	// paramPositions stores authored byte positions plus one for document
	// placeholders only. It remains nil for the scalar-only path.
	paramPositions []int
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

type preparedParameterTyper interface {
	NumParams() int
	ParameterType(int) query.ParameterType
}

type preparedParameterTypePositioner interface {
	ParameterTypePosition(int) int
}

type preparedParameterTypeTargetDefaulter interface {
	ParameterTypeTargetDefault(int) bool
}

func (s *stmt) applyPreparedParamTypes(statement preparedParameterTyper, base int) error {
	if s == nil || statement == nil || statement.NumParams() == 0 {
		return nil
	}
	if base < 0 || base > s.params || statement.NumParams() > s.params-base {
		return fmt.Errorf(
			"vibedb: query parameter range [%d,%d) exceeds statement parameter count %d",
			base, base+statement.NumParams(), s.params,
		)
	}
	positioner, hasPositions := statement.(preparedParameterTypePositioner)
	defaulter, hasTargetDefaults := statement.(preparedParameterTypeTargetDefaulter)
	for local := 0; local < statement.NumParams(); local++ {
		var inferred ParamType
		switch statement.ParameterType(local) {
		case query.ParameterTypeUnspecified:
			continue
		case query.ParameterTypeBool:
			inferred = ParamTypeBool
		case query.ParameterTypeText:
			inferred = ParamTypeText
		case query.ParameterTypeVarchar:
			inferred = ParamTypeVarchar
		case query.ParameterTypeName:
			inferred = ParamTypeName
		case query.ParameterTypeBPChar:
			inferred = ParamTypeBPChar
		case query.ParameterTypeOther:
			inferred = ParamTypeOther
		default:
			return fmt.Errorf(
				"vibedb: query returned invalid type metadata for parameter %d", local+1,
			)
		}
		ordinal := base + local
		if ordinal >= len(s.paramKinds) || s.paramKinds[ordinal] != ParamScalar {
			return fmt.Errorf(
				"vibedb: parameter %d cannot be both a JSON document and %s",
				ordinal+1, inferred,
			)
		}
		if s.paramTypes == nil {
			s.paramTypes = make([]ParamType, s.params)
		}
		if existing := s.paramTypes[ordinal]; existing != ParamTypeUnspecified && existing != inferred {
			return fmt.Errorf(
				"vibedb: parameter %d cannot be both %s and %s",
				ordinal+1, existing, inferred,
			)
		}
		s.paramTypes[ordinal] = inferred
		if hasTargetDefaults && defaulter.ParameterTypeTargetDefault(local) {
			if s.paramTypeTargetDefaults == nil {
				s.paramTypeTargetDefaults = make([]bool, s.params)
			}
			s.paramTypeTargetDefaults[ordinal] = true
		} else if ordinal < len(s.paramTypeTargetDefaults) {
			s.paramTypeTargetDefaults[ordinal] = false
		}
		if hasPositions {
			position := positioner.ParameterTypePosition(local)
			if position >= 0 {
				if s.paramTypePositions == nil {
					s.paramTypePositions = make([]int, s.params)
				}
				encoded := position + 1
				if s.paramTypePositions[ordinal] == 0 ||
					encoded < s.paramTypePositions[ordinal] {
					s.paramTypePositions[ordinal] = encoded
				}
			}
		}
	}
	return nil
}

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
	if s.tree != nil && isSavepointKind(s.tree.Kind) {
		return s.checkArgumentCount(got)
	}
	if s.mutation == nil && (s.views == nil || s.views.ddl == nil) {
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
	s.insertSource = nil
	s.views = nil
	s.pointPredicate = nil
	s.pointPath = ""
	s.pointCandidate = false
	s.primaryRange = nil
	s.primaryPoint = false
	s.serialPointSafe = false
	s.serialMutationSafe = false
	s.catalogJoin = false
	s.paramKinds = nil
	s.paramTypes = nil
	s.paramTypePositions = nil
	s.paramTypeTargetDefaults = nil
	s.paramPositions = nil
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
	return s.queryRowsCandidates(ctx, args, nil, nil)
}

func (s *stmt) queryRowsCandidates(
	ctx context.Context,
	args []any,
	primaryPath []byte,
	candidateKeys [][]byte,
) (*rows, error) {
	defer clear(args)
	candidateRead := candidateKeys != nil
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
		if candidateRead {
			return nil, errors.New("vibedb: candidate keys cannot execute EXPLAIN")
		}
		if s.analyze {
			return s.analyzeRows(ctx, args)
		}
		if s.conn.tx != nil {
			var views []viewDependency
			if s.views != nil {
				views = s.views.dependencies
			}
			if err := s.conn.tx.refreshStatementCut(
				ctx, s.query.Collection(), s.dependencies, views,
			); err != nil {
				return nil, err
			}
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
		if candidateRead {
			return nil, errors.New("vibedb: candidate keys require a read-only SELECT")
		}
		if err := s.conn.requireDirectWriteAllowed(); err != nil {
			return nil, err
		}
		var cursor query.Cursor
		if s.conn.tx != nil {
			cursor, err = s.conn.tx.execPreparedMutationReturningContext(
				ctx, s, args,
			)
		} else {
			cursor, err = s.conn.preparedMutationReturningContext(
				ctx, s, args,
			)
		}
		if err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	// A source-independent SELECT or compound (including bare/all-VALUES) uses the
	// complete prepared set runtime and its ordinary result/intermediate
	// accounts. The parser and dependency walk prove that a statement reaching
	// this branch has no physical relation, so it cannot divert a stored-row
	// query or its transaction snapshot.
	if sourceIndependentStatement(s.query) {
		if candidateRead {
			return nil, errors.New("vibedb: candidate keys require one physical table")
		}
		if s.conn.tx != nil && s.views != nil && len(s.views.dependencies) != 0 {
			if err := s.conn.tx.refreshStatementCut(
				ctx, "", nil, s.views.dependencies,
			); err != nil {
				return nil, err
			}
		}
		if err := s.validatePreparedViewDependencies(ctx); err != nil {
			return nil, err
		}
		cursor, runErr := s.query.RunInto(&s.conn.exec, query.Source{}, args)
		if runErr != nil {
			return nil, runErr
		}
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	if s.conn.tx != nil {
		if err := s.conn.tx.beginQueryStatement(ctx, s); err != nil {
			return nil, err
		}
		if err := s.validateTransactionViewDependencies(); err != nil {
			return nil, err
		}
		if s.transactionRequiresCatalogSource() {
			if candidateRead {
				return nil, errors.New("vibedb: candidate keys do not support catalog joins")
			}
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
		if candidateRead && !bytes.Equal(primaryPath, byteview.Bytes(state.primaryKey)) {
			return nil, errors.New("vibedb: candidate primary path does not match the live table")
		}
		var source query.Source
		pointPredicate := s.pointPredicate
		pointRead := candidateRead || s.pointCandidate && s.pointPath == state.primaryKey
		if pointRead && s.conn.tx.isolation == IsolationSerializable {
			if !candidateRead {
				_, pointRead = s.conn.tx.serializablePointQuery(s)
			}
		}
		if pointRead {
			var keys []string
			var keyErr error
			if candidateRead {
				keys, keyErr = s.conn.borrowPointCandidateKeys(
					candidateKeys, state.limits.MaxKeyBytes,
				)
			} else {
				keys, keyErr = s.conn.bindPointPredicateKeys(
					pointPredicate, args, state.limits.MaxKeyBytes)
			}
			if keyErr != nil {
				return nil, keyErr
			}
			s.conn.tx.trackSerializablePointReads(state, keys)
			source, err = s.conn.pointTransactionSource(ctx, state, keys)
			if !candidateRead && errors.Is(err, errPointMaterializationTooLarge) {
				source, err = s.conn.tx.querySource(s.query.Collection())
			}
		} else if s.transactionPrimaryRangeEligible(state) {
			bounds, eligible, empty, bindErr := s.conn.bindPrimaryRangeProgram(s.primaryRange, args, state.limits.MaxKeyBytes)
			switch {
			case bindErr != nil:
				err = bindErr
			case empty:
				source = query.FromSnapshot(store.Snapshot{})
			case !eligible:
				source, err = s.conn.tx.querySource(s.query.Collection())
			default:
				s.conn.fileRange.Bind(bounds.lower, bounds.upper, bounds.lowerExclusive)
				s.conn.fileRange.BindPrimaryOrder(s.primaryRange.path)
				source = query.FromFileRange(state.snapshot, &s.conn.fileRange)
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
	if err := s.validateViewDependenciesLocked(); err != nil {
		s.conn.db.mu.RUnlock()
		return nil, err
	}
	if s.requiresCatalogSource() {
		if candidateRead {
			s.conn.db.mu.RUnlock()
			return nil, errors.New("vibedb: candidate keys do not support catalog joins")
		}
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
	if candidateRead && !bytes.Equal(primaryPath, byteview.Bytes(t.meta.PrimaryKey)) {
		s.conn.db.mu.RUnlock()
		return nil, errors.New("vibedb: candidate primary path does not match the live table")
	}
	var (
		source   query.Source
		snapshot *durable.Snapshot
	)
	pointPredicate := s.pointPredicate
	if candidateRead || s.pointCandidate && s.pointPath == t.meta.PrimaryKey {
		limits, limitErr := tableMutationLimits(t)
		if limitErr != nil {
			err = limitErr
		} else {
			var keys []string
			var keyErr error
			if candidateRead {
				keys, keyErr = s.conn.borrowPointCandidateKeys(
					candidateKeys, limits.MaxKeyBytes,
				)
			} else {
				keys, keyErr = s.conn.bindPointPredicateKeys(
					pointPredicate, args, limits.MaxKeyBytes)
			}
			if keyErr != nil {
				err = keyErr
			} else if t.collection == nil {
				source = query.FromSnapshot(store.Snapshot{})
			} else {
				source, err = s.conn.pointCollectionSource(
					ctx, t.collection, keys, s.views == nil && !s.query.RequiresCatalog(),
				)
				if !candidateRead && errors.Is(err, errPointMaterializationTooLarge) {
					snapshot, err = t.collection.Snapshot()
					if err == nil {
						source = query.FromFile(snapshot)
					}
				}
			}
		}
	} else if s.primaryRange != nil && s.primaryRange.path == t.meta.PrimaryKey {
		limits, limitErr := tableMutationLimits(t)
		if limitErr != nil {
			err = limitErr
		} else {
			bounds, eligible, empty, bindErr := s.conn.bindPrimaryRangeProgram(
				s.primaryRange, args, limits.MaxKeyBytes,
			)
			switch {
			case bindErr != nil:
				err = bindErr
			case empty || t.collection == nil:
				source = query.FromSnapshot(store.Snapshot{})
			case !eligible:
				snapshot, err = t.collection.Snapshot()
				if err == nil {
					source = query.FromFile(snapshot)
				}
			default:
				snapshot, err = t.collection.Snapshot()
				if err == nil {
					s.conn.fileRange.Bind(
						bounds.lower, bounds.upper, bounds.lowerExclusive,
					)
					s.conn.fileRange.BindPrimaryOrder(s.primaryRange.path)
					source = query.FromFileRange(snapshot, &s.conn.fileRange)
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

func (c *conn) borrowPointCandidateKeys(
	encoded [][]byte,
	maxKeyBytes int,
) ([]string, error) {
	clear(c.pointKeys)
	c.pointKeys = c.pointKeys[:0]
	for i := range encoded {
		key := encoded[i]
		if len(key) == 0 || len(key) > maxKeyBytes ||
			(i != 0 && bytes.Compare(encoded[i-1], key) >= 0) {
			clear(c.pointKeys)
			c.pointKeys = c.pointKeys[:0]
			return nil, fmt.Errorf("vibedb: candidate primary key %d is not canonical", i)
		}
		c.pointKeys = append(c.pointKeys, byteview.String(key))
	}
	return c.pointKeys, nil
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

	options := query.ExplainOptions{}
	if resultRows.snapshot != nil {
		options.IndexCatalogKnown = true
		options.IndexRanges = true
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
		ActualAccessPath:   analyzeAccessPath(s, options.PrimaryPoint, pointSource, stats),
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

func analyzeAccessPath(
	s *stmt,
	primaryPoint, pointSource bool,
	stats query.ExecStats,
) string {
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
	if primaryPoint && pointSource {
		return "primary-key-point"
	}
	if stats.PrimaryRangeBounded {
		return "primary-key-range"
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
	if err := s.validatePreparedViewDependencies(ctx); err != nil {
		return query.ExplainOptions{}, err
	}
	options := query.ExplainOptions{}
	// A catalog-dependent relation plan may read several physical sources, while
	// ExplainOptions carries one index catalog. Keep that part of the plan
	// logical rather than attributing one operand's indexes to every source;
	// EXPLAIN ANALYZE still reports the generalized stages' actual algorithms
	// and counters. Dependency validation uses the same coherent catalog cut.
	if s.requiresCatalogSource() {
		return options, s.validateCatalogDependenciesForExplain(ctx)
	}
	collection := s.query.Collection()
	if sourceIndependentStatement(s.query) {
		return options, contextCheckpoint(ctx)
	}
	options.IndexCatalogKnown = true
	if s.conn.tx != nil {
		state, ok := s.conn.tx.tables[collection]
		if !ok {
			return query.ExplainOptions{}, s.missingDependency(collection, true)
		}
		if state.snapshot != nil {
			options.IndexRanges = true
			options.Indexes = state.snapshot.AppendIndexes(options.Indexes)
		}
		options.PrimaryPoint = s.pointCandidate && s.pointPath == state.primaryKey
		options.PrimaryRange = s.transactionPrimaryRangeEligible(state)
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
	options.PrimaryPoint = s.pointCandidate && s.pointPath == t.meta.PrimaryKey
	options.PrimaryRange = s.primaryRange != nil && s.primaryRange.path == t.meta.PrimaryKey
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
	options.IndexRanges = true
	closeErr := snapshot.Close()
	if closeErr != nil {
		return query.ExplainOptions{}, closeErr
	}
	return options, contextCheckpoint(ctx)
}

// requiresCatalogSource honors the prepared runtime's source contract. A
// catalog can be required either for multiple physical relations or because a
// source-free intermediate must rebind a deeper physical read; distinct-name
// counts alone cannot distinguish those cases.
func (s *stmt) requiresCatalogSource() bool {
	return s != nil && s.query != nil && s.query.RequiresCatalog()
}

// sourceIndependentStatement is the single adapter boundary for a prepared
// query whose root reads no relation. The query runtime is authoritative here:
// an unreferenced SELECT CTE is parsed and prepared for semantic validation but
// contributes neither execution nor dependency/catalog classification. The
// RequiresCatalog guard remains necessary because a root without FROM can
// still execute a physical predicate subquery.
func sourceIndependentStatement(statement *query.Statement) bool {
	return statement != nil && statement.Collection() == "" &&
		!statement.RequiresCatalog()
}

// usesDirectDurableCatalog is the cross-layer source contract for SQL JOINs.
// The generalized relation pipeline owns operand materialization and can
// consume every physical source from one FromFileDatabase cut. The legacy
// physical equi-join pipeline retains the driver's existing bounded heap path
// until the durable executor can publish projected fan-out rows itself.
// Keeping the decision on the prepared statement makes the ordinary no-JOIN
// path one nil-cheap pointer test with no feature state or allocation.
func (s *stmt) usesDirectDurableCatalog() bool {
	return s != nil && s.query != nil && s.query.UsesDirectCatalogExecution()
}

func (s *stmt) transactionRequiresCatalogSource() bool {
	return s.requiresCatalogSource()
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
	validatedRaw bool,
) (query.Source, error) {
	c.pointDocs.Reset()
	limit, err := driverQueryMemory(c.exec.Options)
	if err != nil {
		return query.Source{}, err
	}
	budget := pointMaterializationBudget{limit: limit}
	document := c.pointRaw[:0]
	if validatedRaw && len(keys) == 1 {
		if err := contextCheckpoint(ctx); err != nil {
			return query.Source{}, err
		}
		var found bool
		document, found, err = collection.AppendRaw(document, byteview.Bytes(keys[0]))
		c.pointRaw = document
		if err != nil {
			return query.Source{}, err
		}
		if !found {
			return query.FromSnapshot(store.Snapshot{}), nil
		}
		if err := budget.add(keys[0], document); err != nil {
			return query.Source{}, err
		}
		c.pointSource.Bind(document)
		return query.FromValidatedRaw(&c.pointSource), nil
	}
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
			// Reserve only on cold materialization. A flat point document
			// usually needs fewer than 16 entries; wider shapes retain the
			// ordinary spill/growth path after this capacity hint.
			if c.pointDocs.Len() == 0 {
				c.pointDocs.Reserve(1, len(document), 16)
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
			// Reserve only on cold materialization. A flat point document
			// usually needs fewer than 16 entries; wider shapes retain the
			// ordinary spill/growth path after this capacity hint.
			if c.pointDocs.Len() == 0 {
				c.pointDocs.Reserve(1, len(document), 16)
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
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	if s.conn.open {
		return nil, errors.New("vibedb: close the current rows before executing another statement on this connection")
	}
	if s.tree != nil && isSavepointKind(s.tree.Kind) {
		if s.conn.tx == nil {
			return nil, ErrNoTransaction
		}
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		if err := s.conn.tx.execSavepointStatement(s.tree); err != nil {
			return nil, err
		}
		return result{}, nil
	}
	if s.mutation != nil || (s.views != nil && s.views.ddl != nil) {
		if err := s.conn.requireDirectWriteAllowed(); err != nil {
			return nil, err
		}
	}
	if s.mutation == nil && (s.views == nil || s.views.ddl == nil) {
		return nil, errors.New("vibedb: SELECT returns rows; use Query")
	}
	if s.views != nil && s.views.ddl != nil {
		return s.conn.execViewDDL(ctx, s.views.ddl)
	}
	if s.conn.tx != nil {
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		return s.conn.tx.execPreparedMutationContext(ctx, s, args)
	}
	return s.conn.execPreparedMutationContext(ctx, s, args)
}

func isSavepointKind(kind sqlast.Kind) bool {
	switch kind {
	case sqlast.KindSavepoint, sqlast.KindReleaseSavepoint, sqlast.KindRollbackToSavepoint:
		return true
	default:
		return false
	}
}

func (t *tx) execSavepointStatement(tree *sqlast.Statement) error {
	if t == nil || tree == nil {
		return errors.New("vibedb: internal savepoint statement is nil")
	}
	switch tree.Kind {
	case sqlast.KindSavepoint:
		if tree.Savepoint == nil {
			return errors.New("vibedb: internal SAVEPOINT body is nil")
		}
		return t.savepoint(tree.Savepoint.Name)
	case sqlast.KindReleaseSavepoint:
		if tree.ReleaseSavepoint == nil {
			return errors.New("vibedb: internal RELEASE SAVEPOINT body is nil")
		}
		return t.releaseSavepoint(tree.ReleaseSavepoint.Name)
	case sqlast.KindRollbackToSavepoint:
		if tree.RollbackToSavepoint == nil {
			return errors.New("vibedb: internal ROLLBACK TO SAVEPOINT body is nil")
		}
		return t.rollbackToSavepoint(tree.RollbackToSavepoint.Name)
	default:
		return fmt.Errorf("vibedb: unsupported savepoint statement %s", tree.Kind)
	}
}

// A native range may borrow the transaction snapshot only when no staged
// writes need overlay visibility and no split ownership filter is required.
// Serializable admission remains unchanged; this is only a physical source
// choice after the existing transaction checks.
func (s *stmt) transactionPrimaryRangeEligible(state *txTable) bool {
	return state != nil && state.snapshot != nil && len(state.pending) == 0 &&
		(state.readCut == nil || state.readCut.FullOwnership()) &&
		s.primaryRange != nil && s.primaryRange.path == state.primaryKey
}
