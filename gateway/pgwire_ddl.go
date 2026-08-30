package gateway

import (
	"context"
	"strings"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	driver "github.com/thesyncim/vibedb/sql/driver"
)

func postgresDDLKind(kind sqlast.Kind) bool {
	switch kind {
	case sqlast.KindCreateTable, sqlast.KindDropTable, sqlast.KindCreateIndex, sqlast.KindAlterTable,
		sqlast.KindDropIndex, sqlast.KindTruncate:
		return true
	}
	return false
}

type postgresDDLStatement struct{ postgresWriteStatement }

func (s *postgresSession) prepareDDL(text string, parsed *sqlast.Statement) (pgwire.BackendStatement, error) {
	if s.backend.DDL == nil {
		return nil, sqlast.NewFeatureNotSupportedError(text, 0, "coordinated SQL DDL is not configured for this endpoint")
	}
	p := &postgresDDLStatement{postgresWriteStatement: postgresWriteStatement{
		session: s, text: strings.Clone(text), kind: parsed.Kind,
	}}
	s.statements[p] = struct{}{}
	return p, nil
}

func (p *postgresDDLStatement) Close() error {
	delete(p.session.statements, p)
	return p.postgresWriteStatement.Close()
}

func (p *postgresDDLStatement) Exec(ctx context.Context, args []any) (driver.Result, error) {
	s := p.session
	if p.closed || s.state == driver.SessionClosed {
		return driver.Result{}, driver.ErrSessionClosed
	}
	if s.state == driver.SessionFailedTransaction {
		return driver.Result{}, driver.ErrTransactionFailed
	}
	if s.state != driver.SessionIdle {
		return driver.Result{}, sqlast.NewFeatureNotSupportedError(p.text, 0, "distributed schema changes require auto-commit mode")
	}
	if len(args) != 0 {
		return driver.Result{}, ErrPlanParameters
	}
	if err := ctx.Err(); err != nil {
		return driver.Result{}, err
	}
	if s.flag.Canceled() {
		return driver.Result{}, query.ErrCanceled
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
	return driver.Result{}, s.backend.DDL(ctx, s.authority, p.text)
}
