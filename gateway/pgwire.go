package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	driver "github.com/thesyncim/vibedb/sql/driver"
)

const (
	maxPostgresReadCacheSQLBytes   = 4 << 10
	maxPostgresReadCacheParameters = 256
)

// PostgreSQLBackend exposes distributed reads and optional durable autocommit
// writes, never an embedded database or a replica-local mutation path.
type PostgreSQLBackend struct {
	Executor  *Executor
	Authorize func(pgwire.SessionIdentity) (serviceauthz.Authority, error)
	Write     func(context.Context, serviceauthz.Authority, Query) (*Result, error)
	// DDL is a cold, coordinated schema operation. It must return success only
	// after the serving catalog is durable; it is never a replica-local Exec.
	DDL func(context.Context, serviceauthz.Authority, string) error
}

func (b *PostgreSQLBackend) NewSession(ctx context.Context, identity pgwire.SessionIdentity) (pgwire.BackendSession, error) {
	if b == nil || ctx == nil || b.Executor == nil || b.Executor.catalog == nil || b.Executor.catalog.Current() == nil || b.Authorize == nil {
		return nil, ErrReplicatedUnauthorized
	}
	authority, err := b.Authorize(identity)
	if err != nil {
		return nil, err
	}
	if _, err := serviceauthz.WithAuthority(ctx, authority); err != nil {
		return nil, err
	}
	session := &postgresSession{backend: b, authority: authority, state: driver.SessionIdle, statements: make(map[pgwire.BackendStatement]struct{}), rows: 100000, bytes: shardservice.MaxReplicatedSQLResultBytes}
	session.materializedRelease = session.releaseMaterialized
	return session, nil
}

type postgresSession struct {
	backend             *PostgreSQLBackend
	authority           serviceauthz.Authority
	state               driver.SessionState
	flag                *query.CancelFlag
	rows                int
	bytes               int64
	intermediate        int64
	statements          map[pgwire.BackendStatement]struct{}
	readCache           postgresReadCache
	params              []shardservice.Param
	materialized        query.Result
	materializedRelease func() error
	cancelMu            sync.Mutex
	cancel              context.CancelFunc
}

func (s *postgresSession) releaseMaterialized() error {
	for i := range s.materialized.Columns {
		clear(s.materialized.Columns[i].Cells)
		s.materialized.Columns[i].Cells = s.materialized.Columns[i].Cells[:0]
	}
	s.materialized.RowCount = 0
	return nil
}

// postgresReadCache owns one recently closed distributed SELECT. PostgreSQL's
// extended unnamed protocol closes that backend statement before parsing the
// next one, so a single exact entry covers the steady state without retaining
// an unbounded SQL-keyed map per connection.
type postgresReadCache struct {
	text              string
	parameterTypes    []driver.ParamType
	compiled          *query.Statement
	resultParamTypes  []driver.ParamType
	catalogGeneration uint64
	execution         preparedQueryExecution
}

func (s *postgresSession) State() driver.SessionState { return s.state }
func (s *postgresSession) AutocommitWrites() bool {
	return s.backend.Write != nil || s.backend.DDL != nil
}
func (s *postgresSession) MarkFailed() {
	if s.state == driver.SessionInTransaction {
		s.state = driver.SessionFailedTransaction
	}
}
func (s *postgresSession) SetCancelFlag(flag *query.CancelFlag) error { s.flag = flag; return nil }
func (s *postgresSession) SetResultLimits(rows int, bytes int64) error {
	if rows <= 0 || bytes <= 0 {
		return ErrResultLimit
	}
	s.rows, s.bytes = rows, bytes
	return nil
}
func (s *postgresSession) SetIntermediateLimit(bytes int64) error {
	if bytes <= 0 {
		return ErrResultLimit
	}
	s.intermediate = bytes
	return nil
}
func (s *postgresSession) Cancel() {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
}
func (s *postgresSession) Begin(_ context.Context, options driver.TxOptions) error {
	if s.state != driver.SessionIdle {
		return errors.New("gateway: transaction already active")
	}
	if options.Isolation != driver.IsolationDefault && options.Isolation != driver.IsolationReadCommitted {
		return errors.New("gateway: RF3 PostgreSQL supports per-statement quorum reads, not repeatable-read or serializable snapshots")
	}
	s.state = driver.SessionInTransaction
	return nil
}
func (s *postgresSession) Commit(context.Context) error {
	if s.state == driver.SessionClosed {
		return driver.ErrSessionClosed
	}
	failed := s.state == driver.SessionFailedTransaction
	s.state = driver.SessionIdle
	if failed {
		return driver.ErrTransactionFailed
	}
	return nil
}
func (s *postgresSession) Rollback(context.Context) error {
	if s.state == driver.SessionClosed {
		return driver.ErrSessionClosed
	}
	s.state = driver.SessionIdle
	return nil
}
func (s *postgresSession) Savepoint(context.Context, string) error {
	return errors.New("gateway: savepoints are not supported on the RF3 read endpoint")
}
func (s *postgresSession) ReleaseSavepoint(ctx context.Context, name string) error {
	return s.Savepoint(ctx, name)
}
func (s *postgresSession) RollbackTo(ctx context.Context, name string) error {
	return s.Savepoint(ctx, name)
}
func (s *postgresSession) Close() error {
	s.Cancel()
	s.state = driver.SessionClosed
	for statement := range s.statements {
		_ = statement.Close()
	}
	s.releaseReadCache()
	s.params = nil
	s.materialized.Release()
	return nil
}

func (s *postgresSession) releaseReadCache() {
	if s.readCache.compiled != nil {
		s.readCache.compiled.Release()
	}
	s.readCache = postgresReadCache{}
}

func (s *postgresSession) takeCachedRead(
	text string,
	parameterTypes []driver.ParamType,
) *postgresStatement {
	cache := &s.readCache
	if cache.compiled == nil || cache.text != text ||
		!slices.Equal(cache.parameterTypes, parameterTypes) {
		return nil
	}
	snapshot := s.backend.Executor.catalog.Current()
	if snapshot == nil || snapshot.Generation() != cache.catalogGeneration {
		return nil
	}
	statement := &postgresStatement{
		session: s, compiled: cache.compiled,
		paramTypes:          cache.resultParamTypes,
		cacheParameterTypes: cache.parameterTypes,
		catalogGeneration:   cache.catalogGeneration,
		execution:           cache.execution,
	}
	*cache = postgresReadCache{}
	s.statements[statement] = struct{}{}
	return statement
}

func (s *postgresSession) retainRead(statement *postgresStatement) bool {
	if s == nil || s.state == driver.SessionClosed || statement == nil ||
		statement.compiled == nil || statement.local ||
		len(statement.compiled.SQL()) > maxPostgresReadCacheSQLBytes ||
		statement.compiled.NumParams() > maxPostgresReadCacheParameters {
		return false
	}
	s.releaseReadCache()
	s.readCache = postgresReadCache{
		text:              statement.compiled.SQL(),
		parameterTypes:    statement.cacheParameterTypes,
		compiled:          statement.compiled,
		resultParamTypes:  statement.paramTypes,
		catalogGeneration: statement.catalogGeneration,
		execution:         statement.execution,
	}
	statement.compiled = nil
	statement.paramTypes = nil
	statement.cacheParameterTypes = nil
	return true
}
func (s *postgresSession) Tables(ctx context.Context) ([]driver.TableInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot := s.backend.Executor.catalog.Current()
	if snapshot == nil {
		return nil, ErrNoCatalog
	}
	profiles := snapshot.ReplicatedTableProfiles()
	return replicatedTableInfos(snapshot, profiles), nil
}

func replicatedTableInfos(snapshot *Snapshot, profiles []ReplicatedTableProfile) []driver.TableInfo {
	tables := make([]driver.TableInfo, len(profiles))
	for i, p := range profiles {
		tables[i] = driver.TableInfo{Name: p.Table, PrimaryKey: p.PrimaryKey}
		if declared, ok := snapshot.declaredTableInfo(p.Table); ok {
			tables[i] = declared
		}
		indexes := snapshot.Indexes(p.Table)
		for ordinal := 0; ordinal < indexes.Len(); ordinal++ {
			index, ok := indexes.At(ordinal)
			if !ok || !index.Ready() {
				continue
			}
			paths := make([]string, int(index.PathCount))
			copy(paths, index.Paths[:index.PathCount])
			tables[i].Indexes = append(tables[i].Indexes, driver.IndexInfo{
				Name: index.Name, Unique: index.Flags&IndexUnique != 0, Paths: paths,
			})
		}
	}
	return tables
}
func (s *postgresSession) Prepare(ctx context.Context, text string) (pgwire.BackendStatement, error) {
	return s.prepare(ctx, text, nil)
}

func (s *postgresSession) PrepareWithParameterTypes(
	ctx context.Context,
	text string,
	parameterTypes []driver.ParamType,
) (pgwire.BackendStatement, error) {
	return s.prepare(ctx, text, parameterTypes)
}

func (s *postgresSession) prepare(
	ctx context.Context,
	text string,
	parameterTypes []driver.ParamType,
) (pgwire.BackendStatement, error) {
	if s.state == driver.SessionClosed {
		return nil, driver.ErrSessionClosed
	}
	if s.state == driver.SessionFailedTransaction {
		return nil, driver.ErrTransactionFailed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cached := s.takeCachedRead(text, parameterTypes); cached != nil {
		return cached, nil
	}
	var parser sqlast.Parser
	var parsed sqlast.Statement
	err := parser.ParseStatement(&parsed, text)
	if err != nil {
		return nil, err
	}
	queryParameterTypes, err := postgresQueryParameterTypes(parameterTypes, parsed.Params())
	if err != nil {
		return nil, err
	}
	if parsed.Kind != sqlast.KindSelect {
		return s.prepareWrite(ctx, text, &parsed, queryParameterTypes)
	}
	tree := parsed.Select
	var compiled *query.Statement
	if len(queryParameterTypes) == 0 {
		compiled, err = query.PrepareParsedStatement(text, tree)
	} else {
		compiled, err = query.PrepareParsedStatementWithParameterTypes(
			text, tree, queryParameterTypes,
		)
	}
	if err != nil {
		return nil, err
	}
	local := !coordinatorHasPhysicalReferences(tree)
	var catalogGeneration uint64
	var execution preparedQueryExecution
	if !local {
		// Snapshot.Prepare is the catalog-pinned physical routing compiler. It
		// parses placement, constraints, ordering, and aggregate shape but never
		// performs scalar/common-type analysis; compiled above is therefore the
		// sole semantic prepare and already consumed the declared type hints.
		generation, routing, prepareErr := s.backend.Executor.prepareCatalogWithRefresh(ctx, text)
		if prepareErr != nil {
			compiled.Release()
			return nil, prepareErr
		}
		catalogGeneration = generation
		execution = preparedQueryExecution{
			generation: catalogGeneration, prepared: routing,
		}
	}
	statement := &postgresStatement{
		session: s, compiled: compiled, local: local,
		paramTypes:        postgresSelectParameterTypes(compiled),
		catalogGeneration: catalogGeneration,
		execution:         execution,
	}
	if !local && len(text) <= maxPostgresReadCacheSQLBytes &&
		len(parameterTypes) <= maxPostgresReadCacheParameters {
		statement.cacheParameterTypes = slices.Clone(parameterTypes)
	}
	s.statements[statement] = struct{}{}
	return statement, nil
}

func postgresQueryParameterTypes(
	parameterTypes []driver.ParamType,
	params int,
) ([]query.ParameterType, error) {
	if len(parameterTypes) == 0 {
		return nil, nil
	}
	if len(parameterTypes) > params {
		return nil, fmt.Errorf(
			"gateway: %d parameter type hints exceed %d placeholders",
			len(parameterTypes), params,
		)
	}
	hasType := false
	for _, parameterType := range parameterTypes {
		if parameterType >= driver.ParamTypeInvalid {
			return nil, fmt.Errorf(
				"gateway: invalid parameter type hint %d", parameterType,
			)
		}
		hasType = hasType || parameterType != driver.ParamTypeUnspecified
	}
	if !hasType {
		return nil, nil
	}
	resolved := make([]query.ParameterType, len(parameterTypes))
	for index, parameterType := range parameterTypes {
		switch parameterType {
		case driver.ParamTypeBool:
			resolved[index] = query.ParameterTypeBool
		case driver.ParamTypeText:
			resolved[index] = query.ParameterTypeText
		case driver.ParamTypeVarchar:
			resolved[index] = query.ParameterTypeVarchar
		case driver.ParamTypeName:
			resolved[index] = query.ParameterTypeName
		case driver.ParamTypeBPChar:
			resolved[index] = query.ParameterTypeBPChar
		case driver.ParamTypeOther:
			resolved[index] = query.ParameterTypeOther
		}
	}
	return resolved, nil
}

func postgresParameterType(parameterType query.ParameterType) driver.ParamType {
	switch parameterType {
	case query.ParameterTypeUnspecified:
		return driver.ParamTypeUnspecified
	case query.ParameterTypeBool:
		return driver.ParamTypeBool
	case query.ParameterTypeText:
		return driver.ParamTypeText
	case query.ParameterTypeVarchar:
		return driver.ParamTypeVarchar
	case query.ParameterTypeName:
		return driver.ParamTypeName
	case query.ParameterTypeBPChar:
		return driver.ParamTypeBPChar
	case query.ParameterTypeOther:
		return driver.ParamTypeOther
	default:
		return driver.ParamTypeInvalid
	}
}

func postgresSelectParameterTypes(statement *query.Statement) []driver.ParamType {
	if statement == nil {
		return nil
	}
	var parameterTypes []driver.ParamType
	for index := 0; index < statement.NumParams(); index++ {
		parameterType := postgresParameterType(statement.ParameterType(index))
		if parameterType == driver.ParamTypeUnspecified {
			continue
		}
		if parameterTypes == nil {
			parameterTypes = make([]driver.ParamType, statement.NumParams())
		}
		parameterTypes[index] = parameterType
	}
	return parameterTypes
}

type postgresStatement struct {
	session  *postgresSession
	compiled *query.Statement
	local    bool
	// paramTypes is absent on the ordinary schemaless path. Distributed
	// execution forwards a present vector so the shard's independent prepare
	// observes the same analyzed input domains as this gateway prepare.
	paramTypes []driver.ParamType
	// cacheParameterTypes is the exact input-hint vector for an eligible
	// distributed SELECT. It moves with compiled into the session's one-entry
	// cache and is reused without allocating on an exact hit.
	cacheParameterTypes []driver.ParamType
	catalogGeneration   uint64
	execution           preparedQueryExecution
}

func (p *postgresStatement) Kind() sqlast.Kind { return sqlast.KindSelect }
func (p *postgresStatement) ReturnsRows() bool { return true }
func (p *postgresStatement) NumParams() int    { return p.compiled.NumParams() }
func (p *postgresStatement) ParamKind(i int) driver.ParamKind {
	if i < 0 || i >= p.NumParams() {
		return driver.ParamInvalid
	}
	return driver.ParamScalar
}
func (p *postgresStatement) ParamPosition(int) int { return 0 }
func (p *postgresStatement) ParamType(i int) driver.ParamType {
	if p == nil || p.compiled == nil || i < 0 || i >= p.NumParams() {
		return driver.ParamTypeInvalid
	}
	if i >= len(p.paramTypes) {
		return driver.ParamTypeUnspecified
	}
	return p.paramTypes[i]
}
func (p *postgresStatement) ParamTypePosition(i int) int {
	if p == nil || p.compiled == nil {
		return -1
	}
	return p.compiled.ParameterTypePosition(i)
}
func (p *postgresStatement) ParamTypeTargetDefault(i int) bool {
	return p != nil && p.compiled != nil &&
		p.compiled.ParameterTypeTargetDefault(i)
}
func (p *postgresStatement) ReusableForParse() bool {
	if p == nil || p.session == nil || p.compiled == nil || p.local ||
		p.session.state == driver.SessionClosed ||
		p.session.state == driver.SessionFailedTransaction {
		return false
	}
	snapshot := p.session.backend.Executor.catalog.Current()
	return snapshot != nil && snapshot.Generation() == p.catalogGeneration
}
func (p *postgresStatement) Columns() []string { return p.compiled.Columns() }
func (p *postgresStatement) AppendSchema(dst []query.OutputColumn) []query.OutputColumn {
	return p.compiled.AppendSchema(dst)
}
func (p *postgresStatement) Close() error {
	if p.compiled != nil && !p.session.retainRead(p) {
		p.compiled.Release()
		p.compiled = nil
	}
	p.paramTypes = nil
	p.cacheParameterTypes = nil
	delete(p.session.statements, p)
	return nil
}
func (p *postgresStatement) QueryInto(ctx context.Context, args []any, rows *pgwire.BackendRows) error {
	s := p.session
	if s.state == driver.SessionClosed {
		return driver.ErrSessionClosed
	}
	if s.state == driver.SessionFailedTransaction {
		return driver.ErrTransactionFailed
	}
	if s.flag.Canceled() {
		return query.ErrCanceled
	}
	ctx, cancel := context.WithCancel(ctx)
	s.cancelMu.Lock()
	s.cancel = cancel
	if s.flag.Canceled() {
		cancel()
	}
	s.cancelMu.Unlock()
	defer func() { s.cancelMu.Lock(); s.cancel = nil; s.cancelMu.Unlock(); cancel() }()
	ctx, err := serviceauthz.WithAuthority(ctx, s.authority)
	if err != nil {
		return err
	}
	if p.local {
		exec := &query.Exec{Options: query.ExecOptions{Cancel: s.flag, ResultRows: s.rows, ResultBytes: s.bytes, IntermediateBytes: s.intermediate}}
		cursor, err := p.compiled.RunInto(exec, query.Source{}, args)
		if err != nil {
			exec.Release()
			return err
		}
		if err := rows.SetMaterialized(cursor, func() error { exec.Release(); return nil }); err != nil {
			exec.Release()
			return err
		}
		return nil
	}
	params := slices.Grow(s.params[:0], len(args))[:len(args)]
	clear(params)
	s.params = params
	defer clear(s.params)
	for i, value := range args {
		switch v := value.(type) {
		case *string:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *[]byte:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *int64:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *float64:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *bool:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *query.Number:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		}
		switch v := value.(type) {
		case nil:
			params[i] = shardservice.NullParam()
		case bool:
			params[i] = shardservice.BoolParam(v)
		case string:
			params[i] = shardservice.StringParam(v)
		case []byte:
			params[i] = shardservice.StringBytesParam(v)
		case int64:
			params[i] = shardservice.NumberParam(strconv.FormatInt(v, 10))
		case float64:
			params[i] = shardservice.NumberParam(strconv.FormatFloat(v, 'g', -1, 64))
		case query.Number:
			params[i] = shardservice.NumberParam(string(v))
		default:
			return fmt.Errorf("gateway: unsupported PostgreSQL parameter %T", v)
		}
	}
	profile := s.backend.Executor.profileFor(ClassBatch)
	profile.MaxConcurrency = min(profile.MaxConcurrency, 4)
	profile.MaxAggregateRows = min(profile.MaxAggregateRows, uint64(s.rows))
	profile.PerShardRows = min(profile.PerShardRows, profile.MaxAggregateRows)
	profile.MaxAggregateBytes = min(profile.MaxAggregateBytes, uint64(s.bytes))
	profile.PerShardBytes = min(profile.PerShardBytes, profile.MaxAggregateBytes)
	result, err := s.backend.Executor.queryPreparedWithProfile(ctx, Query{
		SQL: p.compiled.SQL(), Params: params, ParamTypes: p.paramTypes, Class: ClassBatch,
	}, profile, p.compiled.NumParams(), &p.execution)
	if err != nil {
		return err
	}
	if len(result.Rows) > s.rows {
		return ErrResultLimit
	}
	retained := int64(len(result.Rows)) * int64(len(result.Columns)) * 64
	for _, row := range result.Rows {
		for _, cell := range row {
			retained += int64(2 * len(cell.Bytes))
		}
	}
	if retained > s.bytes {
		return ErrResultLimit
	}
	materialized := &s.materialized
	installed := false
	defer func() {
		if !installed {
			_ = s.releaseMaterialized()
		}
	}()
	previousColumns := materialized.Columns
	if cap(previousColumns) < len(result.Columns) {
		for i := range previousColumns {
			clear(previousColumns[i].Cells)
		}
		materialized.Columns = make([]query.ResultColumn, len(result.Columns))
	} else {
		for i := len(result.Columns); i < len(previousColumns); i++ {
			clear(previousColumns[i].Cells)
			previousColumns[i] = query.ResultColumn{}
		}
		materialized.Columns = previousColumns[:len(result.Columns)]
	}
	materialized.RowCount = len(result.Rows)
	for i, column := range result.Columns {
		cells := materialized.Columns[i].Cells
		if cap(cells) < len(result.Rows) {
			clear(cells)
			cells = make([]query.Cell, len(result.Rows))
		} else {
			cells = cells[:len(result.Rows)]
			clear(cells)
		}
		materialized.Columns[i] = query.ResultColumn{Header: column.Name, Cells: cells}
	}
	for r, row := range result.Rows {
		if len(row) != len(result.Columns) {
			return ErrMergeSchema
		}
		for c, cell := range row {
			if cell.Null {
				materialized.Columns[c].Cells[r] = query.NullCell()
			} else {
				value, err := query.ParseJSONCell(cell.Bytes)
				if err != nil {
					return err
				}
				materialized.Columns[c].Cells[r] = value
			}
		}
	}
	cursor, err := query.NewResultCursor(materialized)
	if err != nil {
		return err
	}
	if err := rows.SetMaterialized(cursor, s.materializedRelease); err != nil {
		return err
	}
	installed = true
	return nil
}
