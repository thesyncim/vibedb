package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	driver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func (p *postgresStatement) Exec(context.Context, []any) (driver.Result, error) {
	return driver.Result{}, ErrExecRequiresMutation
}

type postgresWriteStatement struct {
	session    *postgresSession
	text       string
	kind       sqlast.Kind
	params     int
	documents  map[int]int
	compiled   *query.DMLStatement
	paramTypes []driver.ParamType
	closed     bool
}

func (s *postgresSession) prepareWrite(
	ctx context.Context,
	text string,
	parsed *sqlast.Statement,
	parameterTypes []query.ParameterType,
) (pgwire.BackendStatement, error) {
	if postgresDDLKind(parsed.Kind) {
		return s.prepareDDL(text, parsed)
	}
	if s.backend.Write == nil {
		return nil, driver.ErrReadOnlyTransaction
	}
	if parsed.Kind != sqlast.KindInsert && parsed.Kind != sqlast.KindUpdate && parsed.Kind != sqlast.KindDelete || parsed.ReturnsRows() {
		return nil, sqlast.NewFeatureNotSupportedError(text, 0, "RF3 PostgreSQL supports INSERT, UPDATE and DELETE without RETURNING")
	}
	if parsed.Kind == sqlast.KindInsert && parsed.Insert.Source != nil {
		return nil, sqlast.NewFeatureNotSupportedError(text, 0, "RF3 PostgreSQL INSERT requires VALUES")
	}
	if parsed.Kind == sqlast.KindInsert && parsed.Insert.OnConflictUpdate != nil {
		return nil, sqlast.NewFeatureNotSupportedError(
			text, replicatedSQLConflictActionPosition(parsed.Insert),
			"RF3 PostgreSQL ON CONFLICT requires branch-aware replicated writes",
		)
	}
	documents := postgresWriteDocumentParameters(parsed)
	parameterTypes = postgresScalarDMLParameterTypes(parameterTypes, documents)
	var compiled *query.DMLStatement
	var resolvedParameterTypes []driver.ParamType
	if len(parameterTypes) != 0 || postgresDMLNeedsParameterTypeAnalysis(parsed) ||
		hasComputedUpdateAssignments(parsed) {
		var err error
		compiled, err = query.PrepareParsedDMLWithParameterTypes(
			text, parsed, parameterTypes,
		)
		if err != nil {
			return nil, err
		}
		resolvedParameterTypes = postgresDMLParameterTypes(compiled, documents)
		if len(resolvedParameterTypes) == 0 {
			compiled.Release()
			compiled = nil
		}
	}
	// The catalog prepare below compiles routing only and never analyzes scalar
	// input types. The typed DML compile above is the sole semantic authority.
	if _, err := s.backend.Executor.validateCatalogPrepare(ctx, text); err != nil {
		if compiled != nil {
			compiled.Release()
		}
		return nil, err
	}
	p := &postgresWriteStatement{
		session: s, text: strings.Clone(text), kind: parsed.Kind,
		params: parsed.Params(), compiled: compiled,
		documents:  documents,
		paramTypes: resolvedParameterTypes,
	}
	s.statements[p] = struct{}{}
	return p, nil
}

func postgresWriteDocumentParameters(parsed *sqlast.Statement) map[int]int {
	var documents map[int]int
	mark := func(op sqlast.Operand) {
		if op.Kind == sqlast.OperandParam {
			if documents == nil {
				documents = make(map[int]int)
			}
			documents[op.Ordinal] = op.Pos + 1
		}
	}
	if parsed.Kind == sqlast.KindInsert && len(parsed.Insert.Columns) == 0 {
		for _, row := range parsed.Insert.Rows {
			for _, op := range row.Values {
				mark(op)
			}
		}
	}
	if parsed.Kind == sqlast.KindUpdate {
		if len(parsed.Update.Assignments) == 0 {
			mark(parsed.Update.Doc)
		}
	}
	return documents
}

func postgresScalarDMLParameterTypes(
	parameterTypes []query.ParameterType,
	documents map[int]int,
) []query.ParameterType {
	if len(parameterTypes) == 0 || len(documents) == 0 {
		return parameterTypes
	}
	filtered := append([]query.ParameterType(nil), parameterTypes...)
	hasType := false
	for index := range filtered {
		if _, document := documents[index]; document {
			filtered[index] = query.ParameterTypeUnspecified
		}
		hasType = hasType || filtered[index] != query.ParameterTypeUnspecified
	}
	if !hasType {
		return nil
	}
	return filtered
}

// postgresDMLNeedsParameterTypeAnalysis reports whether a mutation-owned
// SELECT subtree contains a parameter in a PostgreSQL type-resolving construct.
// It walks the parser's AST directly: the ordinary path predicate id = $1 has
// no such node and retains the established nil compiled/type sidecars, while a
// set expression, scalar expression, derived relation, or CTE can infer a
// concrete input domain without a client-supplied Parse OID.
func postgresDMLNeedsParameterTypeAnalysis(parsed *sqlast.Statement) bool {
	if parsed == nil || parsed.Params() == 0 {
		return false
	}
	switch parsed.Kind {
	case sqlast.KindInsert:
		return parsed.Insert != nil &&
			postgresSelectNeedsParameterTypeAnalysis(parsed.Insert.Source)
	case sqlast.KindUpdate:
		if parsed.Update == nil {
			return false
		}
		for index := range parsed.Update.Assignments {
			if postgresScalarHasParameter(parsed.Update.Assignments[index].Expr) {
				return true
			}
		}
		return postgresSelectNeedsParameterTypeAnalysis(parsed.Update.Filter)
	case sqlast.KindDelete:
		return parsed.Delete != nil &&
			postgresSelectNeedsParameterTypeAnalysis(parsed.Delete.Filter)
	default:
		return false
	}
}

func postgresSelectNeedsParameterTypeAnalysis(statement *sqlast.SelectStmt) bool {
	if statement == nil {
		return false
	}
	// Set.Params is the exact parameter range owned by the complete expression.
	// PostgreSQL resolves every set output to a common type, including an
	// all-unknown output becoming text, so any parameter in this sidecar needs
	// the semantic compiler even when Parse supplied no OIDs.
	if statement.Set != nil && statement.Set.Params != 0 {
		return true
	}
	if statement.With != nil {
		for index := range statement.With.CTEs {
			if postgresSelectNeedsParameterTypeAnalysis(
				statement.With.CTEs[index].Query,
			) {
				return true
			}
		}
	}
	for index := range statement.Columns {
		if postgresScalarHasParameter(statement.Columns[index].Scalar) {
			return true
		}
	}
	for index := range statement.From {
		relation := &statement.From[index]
		// CTE references point back to definitions already visited through With;
		// following them here would form a cycle for a recursive CTE.
		if relation.Kind == sqlast.RelationDerived &&
			postgresSelectNeedsParameterTypeAnalysis(relation.Query) {
			return true
		}
		if relation.On != nil &&
			postgresExprNeedsParameterTypeAnalysis(relation.On.Expr) {
			return true
		}
	}
	if postgresExprNeedsParameterTypeAnalysis(statement.Where) ||
		postgresExprNeedsParameterTypeAnalysis(statement.Having) {
		return true
	}
	for index := range statement.OrderBy {
		if postgresScalarHasParameter(statement.OrderBy[index].Scalar) {
			return true
		}
	}
	return false
}

func postgresExprNeedsParameterTypeAnalysis(expr *sqlast.Expr) bool {
	if expr == nil {
		return false
	}
	if postgresScalarHasParameter(expr.ScalarLeft) ||
		postgresScalarHasParameter(expr.ScalarRight) ||
		postgresSelectNeedsParameterTypeAnalysis(expr.Subquery) {
		return true
	}
	for index := range expr.Kids {
		if postgresExprNeedsParameterTypeAnalysis(expr.Kids[index]) {
			return true
		}
	}
	return false
}

func postgresScalarHasParameter(expr *sqlast.ScalarExpr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == sqlast.ScalarLiteral &&
		expr.Value.Kind == sqlast.OperandParam {
		return true
	}
	if postgresScalarHasParameter(expr.Left) ||
		postgresScalarHasParameter(expr.Right) ||
		postgresScalarHasParameter(expr.Else) {
		return true
	}
	for index := range expr.Whens {
		when := &expr.Whens[index]
		if postgresExprNeedsParameterTypeAnalysis(when.Predicate) ||
			postgresScalarHasParameter(when.Match) ||
			postgresScalarHasParameter(when.Result) {
			return true
		}
	}
	return false
}

func (p *postgresWriteStatement) Kind() sqlast.Kind { return p.kind }
func (p *postgresWriteStatement) ReturnsRows() bool { return false }
func (p *postgresWriteStatement) NumParams() int    { return p.params }
func (p *postgresWriteStatement) ParamKind(i int) driver.ParamKind {
	if i < 0 || i >= p.params {
		return driver.ParamInvalid
	}
	if _, ok := p.documents[i]; ok {
		return driver.ParamDocument
	}
	return driver.ParamScalar
}
func (p *postgresWriteStatement) ParamPosition(i int) int { return p.documents[i] }
func (p *postgresWriteStatement) ParamType(i int) driver.ParamType {
	if p == nil || i < 0 || i >= p.params {
		return driver.ParamTypeInvalid
	}
	if p.compiled == nil {
		return driver.ParamTypeUnspecified
	}
	if i >= len(p.paramTypes) {
		return driver.ParamTypeUnspecified
	}
	return p.paramTypes[i]
}
func (p *postgresWriteStatement) ParamTypePosition(i int) int {
	if p == nil || p.compiled == nil {
		return -1
	}
	return p.compiled.ParameterTypePosition(i)
}
func (p *postgresWriteStatement) ParamTypeTargetDefault(i int) bool {
	return p != nil && p.compiled != nil &&
		p.compiled.ParameterTypeTargetDefault(i)
}
func (p *postgresWriteStatement) Columns() []string { return nil }
func (p *postgresWriteStatement) AppendSchema(dst []query.OutputColumn) []query.OutputColumn {
	return dst
}
func (p *postgresWriteStatement) Close() error {
	p.closed = true
	p.text = ""
	p.documents = nil
	p.paramTypes = nil
	if p.compiled != nil {
		p.compiled.Release()
		p.compiled = nil
	}
	delete(p.session.statements, p)
	return nil
}
func (p *postgresWriteStatement) QueryInto(context.Context, []any, *pgwire.BackendRows) error {
	return fmt.Errorf("gateway: mutation returns no rows")
}

func (p *postgresWriteStatement) Exec(ctx context.Context, args []any) (driver.Result, error) {
	s := p.session
	if p.closed || s.state == driver.SessionClosed {
		return driver.Result{}, driver.ErrSessionClosed
	}
	if s.state == driver.SessionFailedTransaction {
		return driver.Result{}, driver.ErrTransactionFailed
	}
	if s.state != driver.SessionIdle {
		return driver.Result{}, sqlast.NewFeatureNotSupportedError(p.text, 0, "distributed writes require auto-commit mode")
	}
	if len(args) != p.params {
		return driver.Result{}, ErrPlanParameters
	}
	if s.flag.Canceled() {
		return driver.Result{}, query.ErrCanceled
	}
	params := make([]shardservice.Param, len(args))
	for i, value := range args {
		var err error
		params[i], err = postgresWriteParam(value, p.ParamKind(i) == driver.ParamDocument)
		if err != nil {
			return driver.Result{}, err
		}
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
		return driver.Result{}, err
	}
	result, err := s.backend.Write(ctx, s.authority, Query{
		SQL: p.text, Params: params, ParamTypes: p.paramTypes, Class: ClassBatch,
	})
	if errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		return driver.Result{}, err
	}
	if errors.Is(err, ErrDurableSQLAborted) {
		return driver.Result{}, errors.Join(driver.ErrTransactionConflict, err)
	}
	if errors.Is(err, ErrReplicatedSQLTransactionUnsupported) {
		return driver.Result{}, sqlast.NewFeatureNotSupportedError(p.text, 0, "RF3 writes require INSERT VALUES, primary-key UPDATE, or primary-key equality/IN DELETE without RETURNING or ON CONFLICT")
	}
	if err != nil {
		return driver.Result{}, err
	}
	if result == nil || result.Kind != shardservice.ResponseCompletion {
		return driver.Result{}, ErrDurableSQLRequest
	}
	return driver.Result{RowsAffected: result.RowsAffected}, nil
}

func postgresDMLParameterTypes(
	statement *query.DMLStatement,
	documents map[int]int,
) []driver.ParamType {
	if statement == nil {
		return nil
	}
	var parameterTypes []driver.ParamType
	for index := 0; index < statement.NumParams(); index++ {
		if _, document := documents[index]; document {
			continue
		}
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

func postgresWriteParam(value any, document bool) (shardservice.Param, error) {
	switch v := value.(type) {
	case *string:
		value = postgresParameterValue(v)
	case *[]byte:
		value = postgresParameterValue(v)
	case *bool:
		value = postgresParameterValue(v)
	case *int64:
		value = postgresParameterValue(v)
	case *float64:
		value = postgresParameterValue(v)
	case *query.Number:
		value = postgresParameterValue(v)
	}
	var param shardservice.Param
	switch v := value.(type) {
	case nil:
		param = shardservice.NullParam()
	case bool:
		param = shardservice.BoolParam(v)
	case string:
		param = shardservice.StringParam(v)
	case []byte:
		param = shardservice.StringBytesParam(v)
	case int64:
		param = shardservice.NumberParam(strconv.FormatInt(v, 10))
	case float64:
		param = shardservice.NumberParam(strconv.FormatFloat(v, 'g', -1, 64))
	case query.Number:
		param = shardservice.NumberParam(string(v))
	default:
		return param, fmt.Errorf("gateway: unsupported PostgreSQL parameter %T", value)
	}
	if document {
		if param.Kind != shardservice.ParamString {
			return param, ErrPlanParameters
		}
		param.Kind = shardservice.ParamDocument
	}
	if !param.Valid() {
		return param, ErrPlanParameters
	}
	return param, nil
}

func postgresParameterValue[T any](pointer *T) any {
	if pointer == nil {
		return nil
	}
	return *pointer
}
