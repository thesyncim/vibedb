package driver

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func (c *conn) validateSurface(statement *sqlast.Statement) error {
	switch statement.Kind {
	case sqlast.KindCreateTable:
		if len(statement.CreateTable.PrimaryKey) != 1 || len(statement.CreateTable.Columns) != 0 {
			return fmt.Errorf("vibedb: CREATE TABLE supports exactly PRIMARY KEY (path), without SQL column declarations")
		}
		return nil
	case sqlast.KindCreateIndex:
		if len(statement.CreateIndex.Paths) != 1 {
			return fmt.Errorf("vibedb: CREATE INDEX supports one exact JSON path")
		}
	case sqlast.KindInsert:
		if !statement.Insert.DerivedKey {
			return fmt.Errorf("vibedb: INSERT derives the declared primary key; use VALUES (?) or a flat field list")
		}
	}
	c.db.mu.RLock()
	t, exists := c.db.tables[statement.Table()]
	c.db.mu.RUnlock()
	if !exists {
		return fmt.Errorf("vibedb: table %q does not exist", statement.Table())
	}
	switch statement.Kind {
	case sqlast.KindCreateIndex, sqlast.KindInsert:
		return nil
	case sqlast.KindSelect:
		return validateSelectSurface(statement.Select, t.meta)
	case sqlast.KindUpdate:
		if statement.Update.Filter != nil {
			return validatePredicateSurface(statement.Update.Filter.Where, t.meta)
		}
	case sqlast.KindDelete:
		if statement.Delete.Filter != nil {
			return validatePredicateSurface(statement.Delete.Filter.Where, t.meta)
		}
	}
	return nil
}

func validateSelectSurface(statement *sqlast.SelectStmt, meta *tableMeta) error {
	if len(statement.From) != 1 {
		return fmt.Errorf("vibedb: joins wait for a durable multi-collection SQL snapshot")
	}
	if len(statement.GroupBy) != 0 || statement.Having != nil || statement.Offset != nil {
		return fmt.Errorf("vibedb: GROUP BY, HAVING, and OFFSET are outside the compact driver surface")
	}
	for _, column := range statement.Columns {
		if column.Agg != sqlast.AggNone && column.Agg != sqlast.AggCount {
			return fmt.Errorf("vibedb: only projection and COUNT(*) are supported")
		}
		if column.Agg == sqlast.AggCount && column.Path != nil {
			return fmt.Errorf("vibedb: only COUNT(*) is supported")
		}
	}
	for _, order := range statement.OrderBy {
		if string(order.Path.AppendPointer(nil)) != meta.PrimaryKey {
			return fmt.Errorf("vibedb: ORDER BY is supported only on the declared primary key")
		}
	}
	return validatePredicateSurface(statement.Where, meta)
}

func validatePredicateSurface(where *sqlast.Expr, meta *tableMeta) error {
	if where == nil {
		return nil
	}
	path := ""
	if where.Path != nil {
		path = string(where.Path.AppendPointer(nil))
	}
	indexed := path == meta.PrimaryKey
	for _, index := range meta.Indexes {
		if len(index.Paths) == 1 && index.Paths[0] == path {
			indexed = true
			break
		}
	}
	switch where.Kind {
	case sqlast.ExprCompare:
		if where.Op == sqlast.OpEq && indexed {
			return nil
		}
	case sqlast.ExprIn:
		if !where.Negated && path == meta.PrimaryKey {
			return nil
		}
	}
	return fmt.Errorf("vibedb: WHERE supports primary-key equality/IN or equality on one declared exact index")
}
