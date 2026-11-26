package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func (c *conn) execCreateTableAsSelect(
	ctx context.Context,
	ct *sqlast.CreateTableStmt,
	args []any,
) (sqldriver.Result, error) {
	if ct == nil || ct.AsQuerySQL == "" {
		return nil, fmt.Errorf("vibedb: CREATE TABLE AS requires a SELECT query")
	}
	// Check IF NOT EXISTS before executing the source query.
	if ct.IfNotExists {
		if err := rlockContext(ctx, &c.db.mu); err != nil {
			return nil, err
		}
		_, exists := c.db.tables[ct.Table]
		c.db.mu.RUnlock()
		if exists {
			return result{}, nil
		}
	}

	// Execute the source query and collect columns and rows.
	rawStmt, err := c.PrepareContext(ctx, ct.AsQuerySQL)
	if err != nil {
		return nil, err
	}
	qStmt, ok := rawStmt.(*stmt)
	if !ok {
		_ = rawStmt.Close()
		return nil, fmt.Errorf("vibedb: unexpected statement type in CREATE TABLE AS")
	}
	namedArgs := make([]sqldriver.NamedValue, len(args))
	for i, a := range args {
		namedArgs[i] = sqldriver.NamedValue{Ordinal: i + 1, Value: a}
	}
	rows, err := qStmt.QueryContext(ctx, namedArgs)
	if err != nil {
		_ = qStmt.Close()
		return nil, err
	}
	cols := rows.Columns()
	if len(cols) == 0 {
		_ = rows.Close()
		_ = qStmt.Close()
		return nil, fmt.Errorf("vibedb: CREATE TABLE AS SELECT produced no columns")
	}
	var collected [][]sqldriver.Value
	for {
		vals := make([]sqldriver.Value, len(cols))
		nextErr := rows.Next(vals)
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			_ = rows.Close()
			_ = qStmt.Close()
			return nil, nextErr
		}
		collected = append(collected, vals)
	}
	_ = rows.Close()
	_ = qStmt.Close()

	// Pick PRIMARY KEY column: prefer "id" if present, otherwise first column.
	pkIdx := 0
	for i, col := range cols {
		if strings.EqualFold(col, "id") {
			pkIdx = i
			break
		}
	}

	// Infer scalar type for the primary key column from collected rows (default TEXT).
	pkType := "TEXT"
	for _, r := range collected {
		if pkIdx < len(r) && r[pkIdx] != nil {
			switch r[pkIdx].(type) {
			case int64:
				pkType = "INTEGER"
			case float64:
				pkType = "DOUBLE PRECISION"
			case bool:
				pkType = "BOOLEAN"
			default:
				pkType = "TEXT"
			}
			break
		}
	}

	// Build CREATE TABLE statement.
	var createSQL strings.Builder
	createSQL.WriteString("CREATE TABLE ")
	if ct.IfNotExists {
		createSQL.WriteString("IF NOT EXISTS ")
	}
	createSQL.WriteString(quoteSQLIdent(ct.Table))
	createSQL.WriteString(" (")
	for i, col := range cols {
		if i > 0 {
			createSQL.WriteString(", ")
		}
		createSQL.WriteString(quoteSQLIdent(col))
		if i == pkIdx {
			createSQL.WriteString(" ")
			createSQL.WriteString(pkType)
			createSQL.WriteString(" PRIMARY KEY")
		} else {
			createSQL.WriteString(" ANY")
		}
	}
	createSQL.WriteByte(')')

	createStmt, err := c.PrepareContext(ctx, createSQL.String())
	if err != nil {
		return nil, err
	}
	if cs, ok := createStmt.(*stmt); ok {
		_, err = cs.ExecContext(ctx, nil)
	} else {
		_, err = createStmt.Exec(nil)
	}
	_ = createStmt.Close()
	if err != nil {
		return nil, err
	}

	if len(collected) == 0 {
		return result{}, nil
	}

	// Build INSERT INTO <table> (cols...) VALUES (?, ?, ...)
	var insSQL strings.Builder
	insSQL.WriteString("INSERT INTO ")
	insSQL.WriteString(quoteSQLIdent(ct.Table))
	insSQL.WriteString(" (")
	for i, col := range cols {
		if i > 0 {
			insSQL.WriteString(", ")
		}
		insSQL.WriteString(quoteSQLIdent(col))
	}
	insSQL.WriteString(") VALUES (")
	for i := range cols {
		if i > 0 {
			insSQL.WriteString(", ")
		}
		insSQL.WriteByte('?')
	}
	insSQL.WriteByte(')')

	insRaw, err := c.PrepareContext(ctx, insSQL.String())
	if err != nil {
		return nil, err
	}
	insStmt := insRaw.(*stmt)
	defer insStmt.Close()

	rowArgs := make([]sqldriver.NamedValue, len(cols))
	for _, r := range collected {
		for i, v := range r {
			rowArgs[i] = sqldriver.NamedValue{Ordinal: i + 1, Value: v}
		}
		if _, err := insStmt.ExecContext(ctx, rowArgs); err != nil {
			return nil, err
		}
	}
	return result{affected: int64(len(collected))}, nil
}

func quoteSQLIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
