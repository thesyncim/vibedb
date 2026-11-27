package gateway

import (
	"context"
	"strings"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	driver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func postgresDDLKind(kind sqlast.Kind) bool {
	switch kind {
	case sqlast.KindCreateTable, sqlast.KindDropTable, sqlast.KindCreateIndex, sqlast.KindAlterTable,
		sqlast.KindDropIndex, sqlast.KindTruncate, sqlast.KindCreateView, sqlast.KindDropView:
		return true
	}
	return false
}

type postgresDDLStatement struct{ postgresWriteStatement }

func (s *postgresSession) prepareDDL(text string, parsed *sqlast.Statement) (pgwire.BackendStatement, error) {
	if err := s.requireCapability(serviceauthz.CapabilitySchema); err != nil {
		return nil, err
	}
	if err := rejectReplicatedUniqueIndex(text, parsed); err != nil {
		return nil, err
	}
	isViewDDL := parsed.Kind == sqlast.KindCreateView || parsed.Kind == sqlast.KindDropView
	if !isViewDDL && s.backend.DDL == nil {
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
	if p.kind == sqlast.KindCreateView || p.kind == sqlast.KindDropView {
		var parser sqlast.Parser
		var parsed sqlast.Statement
		if err := parser.ParseStatement(&parsed, p.text); err != nil {
			return driver.Result{}, err
		}
		hasTable := func(name string) bool {
			if s.backend.Executor == nil || s.backend.Executor.catalog == nil {
				return false
			}
			snap := s.backend.Executor.catalog.Current()
			if snap == nil {
				return false
			}
			for _, prof := range snap.ReplicatedTableProfiles() {
				if prof.Table == name {
					return true
				}
			}
			return false
		}
		if p.kind == sqlast.KindCreateView {
			if err := s.backend.viewRegistry().createView(parsed.CreateView, p.text, hasTable); err != nil {
				return driver.Result{}, err
			}
		} else {
			if err := s.backend.viewRegistry().dropView(parsed.DropView, hasTable); err != nil {
				return driver.Result{}, err
			}
		}
		return driver.Result{}, nil
	}
	if p.kind == sqlast.KindCreateTable {
		var parser sqlast.Parser
		var parsed sqlast.Statement
		if err := parser.ParseStatement(&parsed, p.text); err == nil &&
			parsed.CreateTable != nil && parsed.CreateTable.AsQuery != nil {
			return p.execCreateTableAsSelect(ctx, parsed.CreateTable)
		}
	}
	return driver.Result{}, s.backend.DDL(ctx, s.authority, p.text)
}

func (p *postgresDDLStatement) execCreateTableAsSelect(
	ctx context.Context,
	ct *sqlast.CreateTableStmt,
) (driver.Result, error) {
	s := p.session
	if s.backend.Executor == nil || s.backend.DDL == nil || s.backend.Write == nil {
		return driver.Result{}, sqlast.NewFeatureNotSupportedError(p.text, 0, "distributed CREATE TABLE AS SELECT requires executor, DDL, and write backends")
	}
	sourceRes, err := s.backend.Executor.Query(ctx, Query{
		SQL: ct.AsQuerySQL, Class: ClassBatch,
	})
	if err != nil {
		return driver.Result{}, err
	}
	if len(sourceRes.Columns) == 0 {
		return driver.Result{}, ErrMalformedRequest
	}
	pkIdx := 0
	for i, col := range sourceRes.Columns {
		if strings.EqualFold(col.Name, "id") {
			pkIdx = i
			break
		}
	}
	pkType := "TEXT"
	for _, row := range sourceRes.Rows {
		if pkIdx < len(row) && !row[pkIdx].Null && len(row[pkIdx].Bytes) > 0 {
			b := row[pkIdx].Bytes
			if b[0] == '"' {
				pkType = "TEXT"
			} else if string(b) == "true" || string(b) == "false" {
				pkType = "BOOLEAN"
			} else {
				pkType = "DOUBLE PRECISION"
				if !strings.ContainsAny(string(b), ".eE") {
					pkType = "INTEGER"
				}
			}
			break
		}
	}
	var createSQL strings.Builder
	createSQL.WriteString("CREATE TABLE ")
	if ct.IfNotExists {
		createSQL.WriteString("IF NOT EXISTS ")
	}
	createSQL.WriteString(quoteSQLIdentifier(ct.Table))
	createSQL.WriteString(" (")
	for i, col := range sourceRes.Columns {
		if i > 0 {
			createSQL.WriteString(", ")
		}
		createSQL.WriteString(quoteSQLIdentifier(col.Name))
		if i == pkIdx {
			createSQL.WriteString(" ")
			createSQL.WriteString(pkType)
			createSQL.WriteString(" PRIMARY KEY")
		} else {
			createSQL.WriteString(" ANY")
		}
	}
	createSQL.WriteByte(')')
	if err := s.backend.DDL(ctx, s.authority, createSQL.String()); err != nil {
		return driver.Result{}, err
	}
	if len(sourceRes.Rows) == 0 {
		return driver.Result{}, nil
	}
	var insSQL strings.Builder
	insSQL.WriteString("INSERT INTO ")
	insSQL.WriteString(quoteSQLIdentifier(ct.Table))
	insSQL.WriteString(" VALUES ")
	docParams := make([]shardservice.Param, 0, len(sourceRes.Rows))
	for i, row := range sourceRes.Rows {
		if i > 0 {
			insSQL.WriteString(", ")
		}
		insSQL.WriteString("(?)")
		var docBuf []byte
		docBuf = append(docBuf, '{')
		for ci, col := range sourceRes.Columns {
			if ci > 0 {
				docBuf = append(docBuf, ',')
			}
			colName := col.Name
			keyBytes, err := vibejson.Marshal(&colName)
			if err != nil {
				return driver.Result{}, err
			}
			docBuf = append(docBuf, keyBytes...)
			docBuf = append(docBuf, ':')
			if ci >= len(row) || row[ci].Null {
				docBuf = append(docBuf, "null"...)
			} else {
				docBuf = append(docBuf, row[ci].Bytes...)
			}
		}
		docBuf = append(docBuf, '}')
		docParams = append(docParams, shardservice.DocumentParam(string(docBuf)))
	}
	res, err := s.backend.Write(ctx, s.authority, Query{
		SQL: insSQL.String(), Params: docParams, Class: ClassBatch,
	})
	if err != nil {
		return driver.Result{}, err
	}
	var affected int64
	if res != nil {
		affected = res.RowsAffected
	}
	return driver.Result{RowsAffected: affected}, nil
}
