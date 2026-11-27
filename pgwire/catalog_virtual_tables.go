package pgwire

import (
	"context"
	"sort"
	"strconv"
	"strings"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// virtualCatalogShim answers ad-hoc SELECT queries against standard PostgreSQL
// catalog views (information_schema.tables, information_schema.columns,
// information_schema.views, pg_tables, pg_views, pg_indexes) on the cold
// parse-failure path. It never runs on successful SQL queries.
func (s *session) virtualCatalogShim(
	text string,
	check func() error,
) (*fixedResult, bool, error) {
	if s.sql == nil || s.sql.State() != sqldriver.SessionIdle {
		return nil, false, nil
	}
	q, ok := parseVirtualCatalogQuery(text)
	if !ok {
		return nil, false, nil
	}
	if check != nil {
		if err := check(); err != nil {
			return nil, false, err
		}
	}
	var allCols []column
	var allRows [][]*string
	if q.table == "pg_catalog.pg_stat_activity" || q.table == "pg_stat_activity" {
		allCols, allRows = buildStatActivityRows(s.server)
	} else {
		tables, err := s.sql.Tables(s.operationContext())
		if err != nil {
			return nil, false, nil
		}
		var views []sqldriver.ViewInfo
		if lister, ok := s.sql.(interface {
			Views(context.Context) ([]sqldriver.ViewInfo, error)
		}); ok {
			if v, vErr := lister.Views(s.operationContext()); vErr == nil {
				views = v
			}
		}
		allCols, allRows = buildVirtualCatalogRows(q.table, s.database, s.user, tables, views)
	}
	if len(allCols) == 0 {
		return nil, false, nil
	}
	colIndex := make(map[string]int, len(allCols))
	for i, c := range allCols {
		colIndex[strings.ToLower(c.name)] = i
	}

	// Filter rows by WHERE predicates.
	filtered := make([][]*string, 0, len(allRows))
	for _, row := range allRows {
		match := true
		for _, pred := range q.preds {
			idx, exists := colIndex[pred.col]
			if !exists {
				return nil, false, nil
			}
			var val string
			if row[idx] != nil {
				val = *row[idx]
			}
			if pred.negated {
				if val == pred.val {
					match = false
					break
				}
			} else {
				if val != pred.val {
					match = false
					break
				}
			}
		}
		if match {
			filtered = append(filtered, row)
		}
	}

	// Sort if ORDER BY was specified.
	if q.orderCol != "" {
		if idx, exists := colIndex[q.orderCol]; exists {
			sort.SliceStable(filtered, func(i, j int) bool {
				var a, b string
				if filtered[i][idx] != nil {
					a = *filtered[i][idx]
				}
				if filtered[j][idx] != nil {
					b = *filtered[j][idx]
				}
				if q.orderDesc {
					return a > b
				}
				return a < b
			})
		}
	}

	// Apply LIMIT.
	if q.limit >= 0 && q.limit < len(filtered) {
		filtered = filtered[:q.limit]
	}

	// Project columns.
	if q.star {
		return catalogResult(allCols, filtered), true, nil
	}
	projCols := make([]column, len(q.proj))
	projIdxs := make([]int, len(q.proj))
	for i, p := range q.proj {
		idx, exists := colIndex[p.col]
		if !exists {
			return nil, false, nil
		}
		projIdxs[i] = idx
		col := allCols[idx]
		if p.alias != "" {
			col.name = p.alias
		}
		projCols[i] = col
	}
	projRows := make([][]*string, len(filtered))
	for r, row := range filtered {
		out := make([]*string, len(projIdxs))
		for i, idx := range projIdxs {
			out[i] = row[idx]
		}
		projRows[r] = out
	}
	return catalogResult(projCols, projRows), true, nil
}

type virtualCatalogProj struct {
	col   string
	alias string
}

type virtualCatalogPred struct {
	col     string
	val     string
	negated bool
}

type virtualCatalogQuery struct {
	table     string
	star      bool
	proj      []virtualCatalogProj
	preds     []virtualCatalogPred
	orderCol  string
	orderDesc bool
	limit     int
}

func parseVirtualCatalogQuery(raw string) (virtualCatalogQuery, bool) {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw), ";"))
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, "SELECT ") && !strings.HasPrefix(upper, "SELECT\t") && !strings.HasPrefix(upper, "SELECT\n") {
		return virtualCatalogQuery{}, false
	}
	fromIdx := findWordBoundary(upper, "FROM")
	if fromIdx < 0 {
		return virtualCatalogQuery{}, false
	}
	selectPart := strings.TrimSpace(s[len("SELECT"):fromIdx])
	rest := strings.TrimSpace(s[fromIdx+len("FROM"):])
	if selectPart == "" || rest == "" {
		return virtualCatalogQuery{}, false
	}

	// Extract table name (first token after FROM).
	tableEnd := strings.IndexAny(rest, " \t\n\r")
	var tableTok, tail string
	if tableEnd < 0 {
		tableTok = rest
	} else {
		tableTok = rest[:tableEnd]
		tail = strings.TrimSpace(rest[tableEnd:])
	}
	tableNorm := strings.ToLower(strings.ReplaceAll(tableTok, `"`, ""))
	switch tableNorm {
	case "information_schema.tables",
		"information_schema.columns",
		"information_schema.views",
		"pg_catalog.pg_tables", "pg_tables",
		"pg_catalog.pg_views", "pg_views",
		"pg_catalog.pg_indexes", "pg_indexes",
		"pg_catalog.pg_stat_activity", "pg_stat_activity":
	default:
		return virtualCatalogQuery{}, false
	}

	// Strip optional table alias before WHERE / ORDER BY / LIMIT.
	tailUpper := strings.ToUpper(tail)
	if tail != "" && !strings.HasPrefix(tailUpper, "WHERE ") &&
		!strings.HasPrefix(tailUpper, "ORDER ") && !strings.HasPrefix(tailUpper, "LIMIT ") {
		if strings.HasPrefix(tailUpper, "AS ") {
			tail = strings.TrimSpace(tail[3:])
		}
		if sp := strings.IndexAny(tail, " \t\n\r"); sp < 0 {
			tail = ""
		} else {
			tail = strings.TrimSpace(tail[sp:])
		}
	}

	q := virtualCatalogQuery{table: tableNorm, limit: -1}
	if selectPart == "*" || strings.HasSuffix(selectPart, ".*") {
		q.star = true
	} else {
		for _, item := range strings.Split(selectPart, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				return virtualCatalogQuery{}, false
			}
			var colExpr, alias string
			itemUpper := strings.ToUpper(item)
			if asIdx := findWordBoundary(itemUpper, "AS"); asIdx >= 0 {
				colExpr = strings.TrimSpace(item[:asIdx])
				alias = strings.Trim(strings.TrimSpace(item[asIdx+2:]), `"`)
			} else if parts := strings.Fields(item); len(parts) == 2 {
				colExpr = parts[0]
				alias = strings.Trim(parts[1], `"`)
			} else {
				colExpr = item
			}
			if dot := strings.LastIndexByte(colExpr, '.'); dot >= 0 {
				colExpr = colExpr[dot+1:]
			}
			colExpr = strings.ToLower(strings.Trim(colExpr, `"`))
			q.proj = append(q.proj, virtualCatalogProj{col: colExpr, alias: alias})
		}
	}

	// Parse optional WHERE, ORDER BY, LIMIT clauses.
	tailUpper = strings.ToUpper(tail)
	var wherePart, orderPart, limitPart string
	orderIdx := findWordBoundary(tailUpper, "ORDER")
	limitIdx := findWordBoundary(tailUpper, "LIMIT")
	if strings.HasPrefix(tailUpper, "WHERE ") || strings.HasPrefix(tailUpper, "WHERE\t") || strings.HasPrefix(tailUpper, "WHERE\n") {
		end := len(tail)
		if orderIdx >= 0 && orderIdx < end {
			end = orderIdx
		}
		if limitIdx >= 0 && limitIdx < end {
			end = limitIdx
		}
		wherePart = strings.TrimSpace(tail[5:end])
	}
	if orderIdx >= 0 {
		end := len(tail)
		if limitIdx >= 0 && limitIdx > orderIdx {
			end = limitIdx
		}
		orderPart = strings.TrimSpace(tail[orderIdx+5 : end])
		if strings.HasPrefix(strings.ToUpper(orderPart), "BY ") {
			orderPart = strings.TrimSpace(orderPart[3:])
		}
	}
	if limitIdx >= 0 {
		limitPart = strings.TrimSpace(tail[limitIdx+5:])
	}

	if wherePart != "" {
		// Split simple AND clauses.
		for _, clause := range splitTopLevelAND(wherePart) {
			clause = strings.TrimSpace(clause)
			if clause == "" {
				continue
			}
			var col, val string
			var negated bool
			if idx := strings.Index(clause, "<>"); idx >= 0 {
				col, val, negated = strings.TrimSpace(clause[:idx]), strings.TrimSpace(clause[idx+2:]), true
			} else if idx := strings.Index(clause, "!="); idx >= 0 {
				col, val, negated = strings.TrimSpace(clause[:idx]), strings.TrimSpace(clause[idx+2:]), true
			} else if idx := strings.IndexByte(clause, '='); idx >= 0 {
				col, val = strings.TrimSpace(clause[:idx]), strings.TrimSpace(clause[idx+1:])
			} else {
				return virtualCatalogQuery{}, false
			}
			if len(val) < 2 || val[0] != '\'' || val[len(val)-1] != '\'' {
				return virtualCatalogQuery{}, false
			}
			if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
				col = col[dot+1:]
			}
			col = strings.ToLower(strings.Trim(col, `"`))
			val = strings.ReplaceAll(val[1:len(val)-1], "''", "'")
			q.preds = append(q.preds, virtualCatalogPred{col: col, val: val, negated: negated})
		}
	}
	if orderPart != "" {
		first := strings.Split(orderPart, ",")[0]
		fields := strings.Fields(first)
		if len(fields) >= 1 {
			col := fields[0]
			if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
				col = col[dot+1:]
			}
			q.orderCol = strings.ToLower(strings.Trim(col, `"`))
			if len(fields) >= 2 && strings.EqualFold(fields[1], "DESC") {
				q.orderDesc = true
			}
		}
	}
	if limitPart != "" {
		n, err := strconv.Atoi(strings.Fields(limitPart)[0])
		if err != nil || n < 0 {
			return virtualCatalogQuery{}, false
		}
		q.limit = n
	}
	return q, true
}

func findWordBoundary(upper, word string) int {
	n := len(word)
	for i := 0; i+n <= len(upper); i++ {
		if upper[i:i+n] != word {
			continue
		}
		leftOK := i == 0 || isSQLSpace(upper[i-1])
		rightOK := i+n == len(upper) || isSQLSpace(upper[i+n])
		if leftOK && rightOK {
			return i
		}
	}
	return -1
}

func isSQLSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func splitTopLevelAND(s string) []string {
	var out []string
	upper := strings.ToUpper(s)
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			inQuote = !inQuote
			continue
		}
		if !inQuote && i+5 <= len(s) && isSQLSpace(s[i]) && upper[i+1:i+4] == "AND" && isSQLSpace(s[i+4]) {
			out = append(out, s[start:i])
			start = i + 5
			i += 4
		}
	}
	out = append(out, s[start:])
	return out
}

func buildStatActivityRows(srv *Server) ([]column, [][]*string) {
	cols := textCols("pid", "datname", "usename", "application_name", "client_addr", "backend_start", "state", "query")
	if srv == nil {
		return cols, nil
	}
	snaps := srv.activitySnapshot()
	rows := make([][]*string, 0, len(snaps))
	for _, a := range snaps {
		rows = append(rows, []*string{
			strPtr(strconv.FormatInt(int64(a.pid), 10)),
			strPtr(a.datname),
			strPtr(a.usename),
			strPtr(a.appName),
			strPtr(a.clientAddr),
			strPtr(a.backendStart),
			strPtr(a.state),
			strPtr(a.query),
		})
	}
	return cols, rows
}

func buildVirtualCatalogRows(
	tableName, database, user string,
	tables []sqldriver.TableInfo,
	views []sqldriver.ViewInfo,
) ([]column, [][]*string) {
	switch tableName {
	case "information_schema.tables":
		cols := textCols("table_catalog", "table_schema", "table_name", "table_type")
		rows := make([][]*string, 0, len(tables)+len(views))
		for _, t := range tables {
			rows = append(rows, []*string{strPtr(database), strPtr("public"), strPtr(t.Name), strPtr("BASE TABLE")})
		}
		for _, v := range views {
			rows = append(rows, []*string{strPtr(database), strPtr("public"), strPtr(v.Name), strPtr("VIEW")})
		}
		return cols, rows
	case "information_schema.columns":
		cols := textCols("table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "is_nullable", "data_type")
		var rows [][]*string
		for _, t := range tables {
			for i, c := range t.Columns {
				nullable := "YES"
				if c.Required {
					nullable = "NO"
				}
				colName := strings.TrimPrefix(c.Path, "/")
				rows = append(rows, []*string{
					strPtr(database),
					strPtr("public"),
					strPtr(t.Name),
					strPtr(colName),
					strPtr(strconv.Itoa(i + 1)),
					strPtr(nullable),
					strPtr(strings.ToLower(c.Types.String())),
				})
			}
		}
		return cols, rows
	case "information_schema.views":
		cols := textCols("table_catalog", "table_schema", "table_name", "view_definition")
		rows := make([][]*string, 0, len(views))
		for _, v := range views {
			rows = append(rows, []*string{strPtr(database), strPtr("public"), strPtr(v.Name), strPtr(v.Query)})
		}
		return cols, rows
	case "pg_catalog.pg_tables", "pg_tables":
		cols := textCols("schemaname", "tablename", "tableowner", "hasindexes")
		rows := make([][]*string, 0, len(tables))
		for _, t := range tables {
			hasIdx := "false"
			if len(t.Indexes) > 0 || t.PrimaryKey != "" {
				hasIdx = "true"
			}
			rows = append(rows, []*string{strPtr("public"), strPtr(t.Name), strPtr(user), strPtr(hasIdx)})
		}
		return cols, rows
	case "pg_catalog.pg_views", "pg_views":
		cols := textCols("schemaname", "viewname", "viewowner", "definition")
		rows := make([][]*string, 0, len(views))
		for _, v := range views {
			rows = append(rows, []*string{strPtr("public"), strPtr(v.Name), strPtr(user), strPtr(v.Query)})
		}
		return cols, rows
	case "pg_catalog.pg_indexes", "pg_indexes":
		cols := textCols("schemaname", "tablename", "indexname", "indexdef")
		var rows [][]*string
		for _, t := range tables {
			for _, idx := range t.Indexes {
				paths := make([]string, len(idx.Paths))
				for i, p := range idx.Paths {
					paths[i] = strings.TrimPrefix(p, "/")
				}
				def := "CREATE INDEX " + idx.Name + " ON public." + t.Name + " (" + strings.Join(paths, ", ") + ")"
				rows = append(rows, []*string{strPtr("public"), strPtr(t.Name), strPtr(idx.Name), strPtr(def)})
			}
		}
		return cols, rows
	default:
		return nil, nil
	}
}
