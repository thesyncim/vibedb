package serviceauthz

import (
	"slices"
	"sync"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const maxRetainedAuthorizationSQLBytes = 64 << 10

type authorizationSQLParser struct {
	parser    sqlast.Parser
	statement sqlast.Statement
	visited   []*sqlast.SelectStmt
}

var authorizationSQLParsers = sync.Pool{
	New: func() any { return new(authorizationSQLParser) },
}

// SQLCapability parses the complete canonical statement and derives authority
// from its sealed AST kind. A prefix is never sufficient evidence: malformed,
// unsupported, mixed, or trailing syntax requires every SQL capability and
// therefore fails closed before execution. Warm parsers are pooled so the
// successful authorization path remains allocation-free.
func SQLCapability(sql string) Capability {
	workspace := authorizationSQLParsers.Get().(*authorizationSQLParser)
	err := workspace.parser.ParseStatement(&workspace.statement, sql)
	kind := workspace.statement.Kind
	unproven := false
	if err == nil {
		switch kind {
		case sqlast.KindSelect:
			unproven = workspace.selectHasMutationCTE(workspace.statement.Select)
		case sqlast.KindCreateView:
			if workspace.statement.CreateView != nil {
				unproven = workspace.selectHasMutationCTE(workspace.statement.CreateView.Query)
			}
		}
	}
	clear(workspace.visited)
	workspace.visited = workspace.visited[:0]
	workspace.statement = sqlast.Statement{}
	if len(sql) > maxRetainedAuthorizationSQLBytes {
		workspace.parser.Release()
	}
	authorizationSQLParsers.Put(workspace)
	if err != nil || unproven {
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
	switch kind {
	case sqlast.KindCreateTable, sqlast.KindCreateIndex, sqlast.KindAlterTable, sqlast.KindCreateView,
		sqlast.KindDropTable, sqlast.KindDropIndex, sqlast.KindDropView, sqlast.KindTruncate:
		return CapabilitySchema
	case sqlast.KindInsert, sqlast.KindUpdate, sqlast.KindDelete,
		sqlast.KindSavepoint, sqlast.KindReleaseSavepoint, sqlast.KindRollbackToSavepoint:
		return CapabilityDataWrite
	case sqlast.KindSelect:
		return CapabilityDataRead
	default:
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
}

// selectHasMutationCTE reports whether any WITH clause reachable from root
// embeds a data-modifying CTE (INSERT/UPDATE/DELETE). Such statements mix
// mutation and query semantics and are not proven for read-only authority, so
// SQLCapability fails closed rather than classifying them as CapabilityDataRead.
func (w *authorizationSQLParser) selectHasMutationCTE(s *sqlast.SelectStmt) bool {
	if s == nil || slices.Contains(w.visited, s) {
		return false
	}
	w.visited = append(w.visited, s)
	if s.With != nil {
		for index := range s.With.CTEs {
			cte := &s.With.CTEs[index]
			if cte.MutationSQL != "" {
				return true
			}
			if w.selectHasMutationCTE(cte.Query) ||
				w.selectHasMutationCTE(cte.Recursive.Anchor) ||
				w.selectHasMutationCTE(cte.Recursive.Term) {
				return true
			}
		}
	}
	if s.Set != nil && w.setExprHasMutationCTE(s.Set.Root) {
		return true
	}
	for index := range s.From {
		ref := &s.From[index]
		if w.selectHasMutationCTE(ref.Query) {
			return true
		}
		if ref.On != nil && w.exprHasMutationCTE(ref.On.Expr) {
			return true
		}
	}
	if w.exprHasMutationCTE(s.Where) || w.exprHasMutationCTE(s.Having) {
		return true
	}
	for index := range s.Columns {
		col := &s.Columns[index]
		if w.exprHasMutationCTE(col.AggFilter) || w.scalarHasMutationCTE(col.Scalar) {
			return true
		}
	}
	for index := range s.OrderBy {
		if w.scalarHasMutationCTE(s.OrderBy[index].Scalar) {
			return true
		}
	}
	for _, groupScalar := range s.GroupByScalarExprs() {
		if w.scalarHasMutationCTE(groupScalar) {
			return true
		}
	}
	return false
}

func (w *authorizationSQLParser) setExprHasMutationCTE(e *sqlast.SetExpr) bool {
	if e == nil {
		return false
	}
	if w.selectHasMutationCTE(e.Select) || w.selectHasMutationCTE(e.First) {
		return true
	}
	if e.Table != nil && w.selectHasMutationCTE(e.Table.Ref.Query) {
		return true
	}
	return w.setExprHasMutationCTE(e.Left) ||
		w.setExprHasMutationCTE(e.Right) ||
		w.setExprHasMutationCTE(e.Child)
}

func (w *authorizationSQLParser) exprHasMutationCTE(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if w.selectHasMutationCTE(e.Subquery) ||
		w.scalarHasMutationCTE(e.ScalarLeft) ||
		w.scalarHasMutationCTE(e.ScalarRight) {
		return true
	}
	for _, child := range e.Kids {
		if w.exprHasMutationCTE(child) {
			return true
		}
	}
	return false
}

func (w *authorizationSQLParser) scalarHasMutationCTE(e *sqlast.ScalarExpr) bool {
	if e == nil {
		return false
	}
	if w.scalarHasMutationCTE(e.Left) ||
		w.scalarHasMutationCTE(e.Right) ||
		w.scalarHasMutationCTE(e.Else) {
		return true
	}
	for index := range e.Whens {
		arm := &e.Whens[index]
		if w.exprHasMutationCTE(arm.Predicate) ||
			w.scalarHasMutationCTE(arm.Match) ||
			w.scalarHasMutationCTE(arm.Result) {
			return true
		}
	}
	return false
}
