package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	vibejson "github.com/thesyncim/vibejson"
	vibejsonsimd "github.com/thesyncim/vibejson/simd"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func hasMutationCTE(sel *sqlast.SelectStmt) bool {
	if sel == nil || sel.With == nil {
		return false
	}
	for i := range sel.With.CTEs {
		if sel.With.CTEs[i].MutationSQL != "" {
			return true
		}
	}
	return false
}

func (c *conn) queryMutationCTE(
	ctx context.Context,
	originalSQL string,
	sel *sqlast.SelectStmt,
	values []sqldriver.NamedValue,
) (sqldriver.Rows, error) {
	if sel == nil || sel.With == nil {
		return nil, fmt.Errorf("vibedb: missing WITH clause for mutation CTE")
	}
	type replacement struct {
		start int
		end   int
		sql   string
	}
	var reps []replacement
	for i := range sel.With.CTEs {
		cte := &sel.With.CTEs[i]
		if cte.MutationSQL == "" {
			continue
		}
		subStmt, err := c.prepareContext(ctx, cte.MutationSQL)
		if err != nil {
			return nil, err
		}
		var cols []string
		var matRows [][]sqldriver.Value
		if subStmt.tree.ReturnsRows() {
			r, qErr := subStmt.QueryContext(ctx, nil)
			if qErr != nil {
				_ = subStmt.Close()
				return nil, qErr
			}
			cols = append([]string(nil), r.Columns()...)
			for {
				row := make([]sqldriver.Value, len(cols))
				nErr := r.Next(row)
				if nErr == io.EOF {
					break
				}
				if nErr != nil {
					_ = r.Close()
					_ = subStmt.Close()
					return nil, nErr
				}
				matRows = append(matRows, row)
			}
			_ = r.Close()
		} else {
			if _, eErr := subStmt.ExecContext(ctx, nil); eErr != nil {
				_ = subStmt.Close()
				return nil, eErr
			}
		}
		_ = subStmt.Close()

		// Build a pure SELECT replacement for this CTE body.
		var b strings.Builder
		if len(cols) == 0 {
			b.WriteString("SELECT 1 WHERE FALSE")
		} else if len(matRows) == 0 {
			b.WriteString("SELECT ")
			for ci, col := range cols {
				if ci > 0 {
					b.WriteString(", ")
				}
				b.WriteString("NULL AS ")
				b.WriteString(quoteIdentForCTE(col))
			}
			b.WriteString(" WHERE FALSE")
		} else {
			for ri, row := range matRows {
				if ri > 0 {
					b.WriteString(" UNION ALL ")
				}
				b.WriteString("SELECT ")
				for ci, v := range row {
					if ci > 0 {
						b.WriteString(", ")
					}
					b.WriteString(sqlLiteralForCTEValue(v))
					if ri == 0 {
						b.WriteString(" AS ")
						b.WriteString(quoteIdentForCTE(cols[ci]))
					}
				}
			}
		}
		reps = append(reps, replacement{start: cte.MutationStart, end: cte.MutationEnd, sql: b.String()})
	}

	// Substitute replacements from right to left so earlier offsets remain valid.
	rewritten := originalSQL
	for i := len(reps) - 1; i >= 0; i-- {
		r := reps[i]
		rewritten = rewritten[:r.start] + r.sql + rewritten[r.end:]
	}
	finalStmt, err := c.prepareContext(ctx, rewritten)
	if err != nil {
		return nil, err
	}
	return finalStmt.QueryContext(ctx, values)
}

func quoteIdentForCTE(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func sqlLiteralForCTEValue(v sqldriver.Value) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	case time.Time:
		var buf [64]byte
		if raw, err := vibejsonsimd.AppendTime(buf[:0], x); err == nil {
			return "'" + strings.ReplaceAll(string(raw), "'", "''") + "'"
		}
		return "'" + x.Format(time.RFC3339Nano) + "'"
	default:
		raw, _ := vibejson.Marshal(&x)
		return "'" + strings.ReplaceAll(string(raw), "'", "''") + "'"
	}
}
