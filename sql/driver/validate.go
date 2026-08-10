package driver

import (
	"context"
	"errors"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func (c *conn) validateSurfaceContext(
	ctx context.Context,
	statement *sqlast.Statement,
) error {
	switch statement.Kind {
	case sqlast.KindCreateTable:
		if len(statement.CreateTable.PrimaryKey) != 1 {
			return fmt.Errorf("vibedb: CREATE TABLE requires exactly one PRIMARY KEY JSON path")
		}
		for i := range statement.CreateTable.Columns {
			if pseudoDocumentPath(statement.CreateTable.Columns[i].Path) {
				return reservedDocumentPathError(
					statement.CreateTable.Columns[i].Path)
			}
		}
		for i := range statement.CreateTable.PrimaryKey {
			if pseudoDocumentPath(statement.CreateTable.PrimaryKey[i]) {
				return reservedDocumentPathError(
					statement.CreateTable.PrimaryKey[i])
			}
		}
		return nil
	case sqlast.KindInsert:
		for i := range statement.Insert.Columns {
			if pseudoDocumentPath(statement.Insert.Columns[i]) {
				return reservedDocumentPathError(statement.Insert.Columns[i])
			}
		}
		if statement.Insert.Source != nil {
			if err := rlockContext(ctx, &c.db.mu); err != nil {
				return err
			}
			err := c.validateSelectTables(statement.Insert.Source)
			c.db.mu.RUnlock()
			if err != nil {
				return err
			}
		}
	case sqlast.KindCreateIndex:
		for i := range statement.CreateIndex.Paths {
			if pseudoDocumentPath(statement.CreateIndex.Paths[i]) {
				return reservedDocumentPathError(
					statement.CreateIndex.Paths[i])
			}
		}
	}
	if statement.Kind == sqlast.KindDropTable && statement.DropTable.IfExists {
		// DROP TABLE IF EXISTS is intentionally preparable before the table is
		// present. Execution rechecks the catalog under its exclusive lock.
		return nil
	}
	if statement.Kind == sqlast.KindDropIndex {
		if err := rlockContext(ctx, &c.db.mu); err != nil {
			return err
		}
		defer c.db.mu.RUnlock()
		_, _, err := c.db.resolveDropIndexLocked(statement.DropIndex)
		return err
	}
	if err := rlockContext(ctx, &c.db.mu); err != nil {
		return err
	}
	defer c.db.mu.RUnlock()
	if statement.Kind == sqlast.KindSelect {
		return c.validateSelectTables(statement.Select)
	}
	if _, exists := c.db.tables[statement.Table()]; !exists {
		return fmt.Errorf("%w: %q", ErrTableNotFound, statement.Table())
	}
	return nil
}

func (c *conn) validateSelectTables(selectStmt *sqlast.SelectStmt) error {
	walk := selectTableValidation{conn: c}
	return walk.selectStmt(selectStmt)
}

type selectTableValidation struct {
	conn    *conn
	visited []*sqlast.SelectStmt
}

func (w *selectTableValidation) selectStmt(selectStmt *sqlast.SelectStmt) error {
	if selectStmt == nil {
		return errors.New("vibedb: SELECT has no query")
	}
	for i := range w.visited {
		if w.visited[i] == selectStmt {
			return nil
		}
	}
	w.visited = append(w.visited, selectStmt)
	// PostgreSQL resolves every lexical WITH body even when the optimizer can
	// prove that the definition is dormant. Preserve that prepare-time contract
	// independently from executable dependency/source routing.
	if selectStmt.With != nil {
		for i := range selectStmt.With.CTEs {
			definition := &selectStmt.With.CTEs[i]
			if definition.Query == nil {
				return errors.New("vibedb: CTE definition has no query")
			}
			if err := w.selectStmt(definition.Query); err != nil {
				return err
			}
		}
	}
	if selectStmt.Set != nil {
		return w.setExpr(selectStmt.Set.Root)
	}
	for i := range selectStmt.From {
		relation := &selectStmt.From[i]
		switch relation.Kind {
		case sqlast.RelationCollection:
			if !w.conn.selectTableExists(relation.Name) {
				return missingTableDependency(
					relation.Name, relation.Pos, false,
				)
			}
		case sqlast.RelationDerived, sqlast.RelationCTE:
			// Derived and CTE relations have no physical Name. The visited set
			// makes a lexical CTE definition cheap when its reference reaches it
			// again and also closes recursive self edges.
			if relation.Query == nil {
				return errors.New("vibedb: derived or CTE relation has no query")
			}
			if err := w.selectStmt(relation.Query); err != nil {
				return err
			}
		default:
			return fmt.Errorf("vibedb: unsupported relation kind %d", relation.Kind)
		}
		if relation.On != nil {
			if err := validateExprSubqueries(relation.On.Expr, w.selectStmt); err != nil {
				return err
			}
		}
	}
	validate := func(query *sqlast.SelectStmt) error {
		return w.selectStmt(query)
	}
	if err := validateExprSubqueries(selectStmt.Where, validate); err != nil {
		return err
	}
	return validateExprSubqueries(selectStmt.Having, validate)
}

func (w *selectTableValidation) setExpr(expression *sqlast.SetExpr) error {
	if expression == nil {
		return errors.New("vibedb: set expression has no root")
	}
	switch expression.Kind {
	case sqlast.SetSelectExpr, sqlast.SetTableExpr:
		return w.selectStmt(expression.Select)
	case sqlast.SetValuesExpr:
		return nil
	case sqlast.SetBinaryExpr:
		if err := w.setExpr(expression.Left); err != nil {
			return err
		}
		return w.setExpr(expression.Right)
	case sqlast.SetGroupExpr:
		return w.setExpr(expression.Child)
	default:
		return fmt.Errorf("vibedb: unsupported set expression kind %d", expression.Kind)
	}
}

func (c *conn) selectTableExists(name string) bool {
	if c.tx != nil {
		if _, exists := c.tx.tables[name]; exists {
			return true
		}
	}
	_, exists := c.db.tables[name]
	return exists
}

func validateExprSubqueries(
	e *sqlast.Expr,
	validate func(*sqlast.SelectStmt) error,
) error {
	if e == nil {
		return nil
	}
	if e.Subquery != nil {
		if err := validate(e.Subquery); err != nil {
			return err
		}
	}
	for _, kid := range e.Kids {
		if err := validateExprSubqueries(kid, validate); err != nil {
			return err
		}
	}
	return nil
}

func pseudoDocumentPath(path *sqlast.PathExpr) bool {
	if path == nil || len(path.Segments) != 1 || path.Segments[0].IsIndex {
		return false
	}
	return path.Segments[0].Key == sqlast.DocumentColumn
}

func reservedDocumentPathError(path *sqlast.PathExpr) error {
	return fmt.Errorf(
		"vibedb: JSON field %q is reserved by the SQL adapter; "+
			"use an ordinary document field name",
		path.Spec(),
	)
}
