package gateway

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	driver "github.com/thesyncim/vibedb/sql/driver"
)

type distributedViewRegistry struct {
	mu    sync.RWMutex
	views map[string]driver.ViewInfo
}

func (b *PostgreSQLBackend) viewRegistry() *distributedViewRegistry {
	b.viewsOnce.Do(func() {
		b.views = &distributedViewRegistry{
			views: make(map[string]driver.ViewInfo),
		}
	})
	return b.views
}

func (r *distributedViewRegistry) list() []driver.ViewInfo {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.views) == 0 {
		return nil
	}
	out := make([]driver.ViewInfo, 0, len(r.views))
	for _, info := range r.views {
		out = append(out, driver.ViewInfo{
			Name:    info.Name,
			Query:   info.Query,
			Columns: slices.Clone(info.Columns),
			Outputs: slices.Clone(info.Outputs),
		})
	}
	slices.SortFunc(out, func(a, b driver.ViewInfo) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func (r *distributedViewRegistry) ResolveSQLView(name string) (query.SQLViewDefinition, bool, error) {
	if r == nil {
		return query.SQLViewDefinition{}, false, nil
	}
	r.mu.RLock()
	info, ok := r.views[name]
	r.mu.RUnlock()
	if !ok {
		return query.SQLViewDefinition{}, false, nil
	}
	return query.SQLViewDefinition{
		Name:    info.Name,
		Query:   info.Query,
		Columns: slices.Clone(info.Columns),
	}, true, nil
}

func (r *distributedViewRegistry) createView(stmt *sqlast.CreateViewStmt, text string, hasTable func(string) bool) error {
	if stmt == nil {
		return ErrMalformedRequest
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if hasTable != nil && hasTable(stmt.Name) {
		return fmt.Errorf("%w: %q", driver.ErrTableExists, stmt.Name)
	}
	if _, exists := r.views[stmt.Name]; exists {
		return fmt.Errorf("%w: %q", driver.ErrViewExists, stmt.Name)
	}
	querySQL := extractCreateViewQuery(text, stmt)
	if querySQL == "" {
		return ErrMalformedRequest
	}
	r.views[stmt.Name] = driver.ViewInfo{
		Name:    stmt.Name,
		Query:   querySQL,
		Columns: slices.Clone(stmt.Columns),
	}
	return nil
}

func (r *distributedViewRegistry) dropView(stmt *sqlast.DropViewStmt, hasTable func(string) bool) error {
	if stmt == nil {
		return ErrMalformedRequest
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.views[stmt.Name]; !exists {
		if hasTable != nil && hasTable(stmt.Name) {
			return fmt.Errorf("%w: %q is a table, not a view", driver.ErrWrongObjectType, stmt.Name)
		}
		if stmt.IfExists {
			return nil
		}
		return fmt.Errorf("%w: %q", driver.ErrViewNotFound, stmt.Name)
	}
	delete(r.views, stmt.Name)
	return nil
}

func (r *distributedViewRegistry) hasAny() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	n := len(r.views)
	r.mu.RUnlock()
	return n > 0
}

// expandViewSQL rewrites any FROM/JOIN references to registered distributed
// views as CTEs prefixed onto the query so both local semantic prepare and
// distributed catalog routing resolve only physical tables.
func (r *distributedViewRegistry) expandViewSQL(text string) (string, bool) {
	if !r.hasAny() {
		return text, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var referenced []driver.ViewInfo
	for _, info := range r.views {
		if strings.Contains(text, info.Name) {
			referenced = append(referenced, info)
		}
	}
	if len(referenced) == 0 {
		return text, false
	}
	slices.SortFunc(referenced, func(a, b driver.ViewInfo) int {
		return strings.Compare(a.Name, b.Name)
	})
	var b strings.Builder
	trimmed := strings.TrimSpace(text)
	hasExplain := false
	hasAnalyze := false
	if len(trimmed) >= 7 && strings.EqualFold(trimmed[:7], "EXPLAIN") {
		hasExplain = true
		trimmed = strings.TrimSpace(trimmed[7:])
		if len(trimmed) >= 7 && strings.EqualFold(trimmed[:7], "ANALYZE") {
			hasAnalyze = true
			trimmed = strings.TrimSpace(trimmed[7:])
		}
	}
	if hasExplain {
		b.WriteString("EXPLAIN ")
		if hasAnalyze {
			b.WriteString("ANALYZE ")
		}
	}
	b.WriteString("WITH ")
	for i, v := range referenced {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteSQLIdentifier(v.Name))
		if len(v.Columns) > 0 {
			b.WriteByte('(')
			for ci, col := range v.Columns {
				if ci > 0 {
					b.WriteString(", ")
				}
				b.WriteString(quoteSQLIdentifier(col))
			}
			b.WriteByte(')')
		}
		b.WriteString(" AS (")
		b.WriteString(v.Query)
		b.WriteByte(')')
	}
	if len(trimmed) >= 4 && strings.EqualFold(trimmed[:4], "WITH") && (len(trimmed) == 4 || !isIdentByte(trimmed[4])) {
		b.WriteString(", ")
		b.WriteString(strings.TrimSpace(trimmed[4:]))
	} else {
		b.WriteByte(' ')
		b.WriteString(trimmed)
	}
	return b.String(), true
}

func extractCreateViewQuery(text string, stmt *sqlast.CreateViewStmt) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text), ";"))
	upper := strings.ToUpper(trimmed)
	asIdx := findTopLevelKeyword(upper, "AS")
	if asIdx < 0 {
		return ""
	}
	return strings.TrimSpace(trimmed[asIdx+2:])
}

func (s *postgresSession) Views(ctx context.Context) ([]driver.ViewInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.backend == nil {
		return nil, nil
	}
	return s.backend.viewRegistry().list(), nil
}
