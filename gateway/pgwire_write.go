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
	session   *postgresSession
	text      string
	kind      sqlast.Kind
	params    int
	documents map[int]int
	closed    bool
}

func (s *postgresSession) prepareWrite(ctx context.Context, text string, parsed *sqlast.Statement) (pgwire.BackendStatement, error) {
	if s.backend.Write == nil {
		return nil, driver.ErrReadOnlyTransaction
	}
	if parsed.Kind != sqlast.KindInsert && parsed.Kind != sqlast.KindUpdate && parsed.Kind != sqlast.KindDelete || parsed.ReturnsRows() {
		return nil, sqlast.NewFeatureNotSupportedError(text, 0, "RF3 PostgreSQL supports INSERT, whole-document UPDATE and DELETE without RETURNING")
	}
	if parsed.Kind == sqlast.KindInsert && parsed.Insert.Source != nil {
		return nil, sqlast.NewFeatureNotSupportedError(text, 0, "RF3 PostgreSQL INSERT requires VALUES")
	}
	if _, err := s.backend.Executor.catalog.Current().Prepare(ctx, text); err != nil {
		return nil, err
	}
	p := &postgresWriteStatement{session: s, text: strings.Clone(text), kind: parsed.Kind, params: parsed.Params()}
	mark := func(op sqlast.Operand) {
		if op.Kind == sqlast.OperandParam {
			if p.documents == nil {
				p.documents = make(map[int]int)
			}
			p.documents[op.Ordinal] = op.Pos + 1
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
		mark(parsed.Update.Doc)
	}
	s.statements[p] = struct{}{}
	return p, nil
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
func (p *postgresWriteStatement) Columns() []string       { return nil }
func (p *postgresWriteStatement) AppendSchema(dst []query.OutputColumn) []query.OutputColumn {
	return dst
}
func (p *postgresWriteStatement) Close() error {
	p.closed = true
	p.text = ""
	p.documents = nil
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
	result, err := s.backend.Write(ctx, s.authority, Query{SQL: p.text, Params: params, Class: ClassBatch})
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
