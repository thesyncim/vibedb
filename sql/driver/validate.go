package driver

import (
	"context"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func (c *conn) validateSurface(statement *sqlast.Statement) error {
	return c.validateSurfaceContext(backgroundContext, statement)
}

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
	case sqlast.KindCreateIndex:
		for i := range statement.CreateIndex.Paths {
			if pseudoDocumentPath(statement.CreateIndex.Paths[i]) {
				return reservedDocumentPathError(
					statement.CreateIndex.Paths[i])
			}
		}
	}
	if err := rlockContext(ctx, &c.db.mu); err != nil {
		return err
	}
	defer c.db.mu.RUnlock()
	if statement.Kind == sqlast.KindSelect {
		for i := range statement.Select.From {
			ref := &statement.Select.From[i]
			name := ref.Name
			if _, exists := c.db.tables[name]; !exists {
				return fmt.Errorf("%w: %q", ErrTableNotFound, name)
			}
		}
		return nil
	}
	if _, exists := c.db.tables[statement.Table()]; !exists {
		return fmt.Errorf("%w: %q", ErrTableNotFound, statement.Table())
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
