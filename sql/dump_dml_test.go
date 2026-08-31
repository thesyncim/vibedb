package sql

import (
	"fmt"
	"strings"
)

// The AST renderer for the statement kinds that are not SELECT.
//
// It follows dump_test.go's rule exactly: lossless for every field a lowering
// pass reads, so a test case about one clause still fails if another regresses,
// and so the fuzz target's reparse comparison actually compares the whole tree
// rather than the part somebody remembered to render.

func dumpAny(s *Statement) string {
	switch s.Kind {
	case KindInsert:
		return dumpInsert(s.Insert)
	case KindUpdate:
		return dumpUpdate(s.Update)
	case KindDelete:
		return dumpDelete(s.Delete)
	case KindCreateTable:
		return dumpCreateTable(s.CreateTable)
	case KindCreateIndex:
		return dumpCreateIndex(s.CreateIndex)
	case KindAlterTable:
		var b strings.Builder
		b.WriteString("alter table ")
		b.WriteString(s.AlterTable.Table)
		b.WriteString(" add column")
		if s.AlterTable.IfNotExists {
			b.WriteString(" if not exists")
		}
		b.WriteByte(' ')
		dumpPath(&b, s.AlterTable.Column.Path)
		b.WriteByte(' ')
		b.WriteString(s.AlterTable.Column.Type.String())
		if s.AlterTable.Column.Required {
			b.WriteString(" not null")
		}
		return b.String()
	case KindDropTable:
		if s.DropTable.IfExists {
			return "drop table if exists " + s.DropTable.Table
		}
		return "drop table " + s.DropTable.Table
	case KindTruncate:
		return "truncate " + s.Truncate.Table
	case KindDropIndex:
		var b strings.Builder
		b.WriteString("drop index")
		if s.DropIndex.IfExists {
			b.WriteString(" if exists")
		}
		b.WriteByte(' ')
		b.WriteString(s.DropIndex.Name)
		if s.DropIndex.HasTable {
			b.WriteString(" on ")
			b.WriteString(s.DropIndex.Table)
		}
		return b.String()
	}
	return dumpStmt(s.Select)
}

func dumpInsert(s *InsertStmt) string {
	var b strings.Builder
	b.WriteString("insert into ")
	b.WriteString(s.Table)
	if s.Alias != "" {
		b.WriteString(" as ")
		b.WriteString(s.Alias)
	}
	if len(s.Columns) != 0 {
		b.WriteString(" fields")
		for _, column := range s.Columns {
			b.WriteByte(' ')
			dumpPath(&b, column)
		}
	}
	if s.Source != nil {
		b.WriteString(" source ")
		b.WriteString(dumpStmt(s.Source))
	}
	for i := range s.Rows {
		b.WriteString(" (")
		for j, value := range s.Rows[i].Values {
			if j != 0 {
				b.WriteString(", ")
			}
			dumpOperand(&b, value)
		}
		b.WriteByte(')')
	}
	if s.OnConflictDoNothing {
		b.WriteString(" on conflict do nothing")
	}
	if s.OnConflictUpdate != nil {
		b.WriteString(" on conflict do update set ")
		if s.OnConflictUpdate.WholeDocument() {
			b.WriteString("\"$doc\"=")
			dumpOperand(&b, s.OnConflictUpdate.Doc)
		} else {
			dumpAssignments(&b, s.OnConflictUpdate.Assignments)
		}
	}
	if s.Returning != nil {
		b.WriteString(" returning ")
		for i := range s.Returning.Columns {
			if i != 0 {
				b.WriteString(", ")
			}
			dumpColumn(&b, &s.Returning.Columns[i])
		}
	}
	fmt.Fprintf(&b, " params=%d", s.Params)
	return b.String()
}

func dumpUpdate(s *UpdateStmt) string {
	var b strings.Builder
	b.WriteString("update ")
	b.WriteString(s.Table)
	if s.Alias != "" {
		b.WriteString(" as ")
		b.WriteString(s.Alias)
	}
	b.WriteString(" set ")
	if len(s.Assignments) != 0 {
		dumpAssignments(&b, s.Assignments)
	} else {
		dumpOperand(&b, s.Doc)
	}
	dumpTargets(&b, s.Filter, false)
	dumpMutationWindow(&b, s.OrderBy, s.Limit)
	if s.Returning != nil {
		b.WriteString(" returning ")
		for i := range s.Returning.Columns {
			if i != 0 {
				b.WriteString(", ")
			}
			dumpColumn(&b, &s.Returning.Columns[i])
		}
	}
	fmt.Fprintf(&b, " params=%d", s.Params)
	return b.String()
}

func dumpAssignments(b *strings.Builder, assignments []UpdateAssignment) {
	for i := range assignments {
		if i != 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "%q=", assignments[i].Column)
		if assignments[i].Expr != nil {
			dumpScalar(b, assignments[i].Expr)
		} else {
			dumpOperand(b, assignments[i].Value)
		}
	}
}

func dumpDelete(s *DeleteStmt) string {
	var b strings.Builder
	b.WriteString("delete from ")
	b.WriteString(s.Table)
	dumpTargets(&b, s.Filter, s.All)
	dumpMutationWindow(&b, s.OrderBy, s.Limit)
	if s.Returning != nil {
		b.WriteString(" returning ")
		for i := range s.Returning.Columns {
			if i != 0 {
				b.WriteString(", ")
			}
			dumpColumn(&b, &s.Returning.Columns[i])
		}
	}
	fmt.Fprintf(&b, " params=%d", s.Params)
	return b.String()
}

// dumpTargets renders which documents a statement acts on.
func dumpTargets(b *strings.Builder, filter *SelectStmt, all bool) {
	switch {
	case all:
		b.WriteString(" all")
	case filter != nil && filter.Where != nil:
		b.WriteString(" where ")
		dumpExpr(b, filter.Where)
	default:
		b.WriteString(" <no target>")
	}
}

func dumpMutationWindow(b *strings.Builder, order []OrderTerm, limit *Operand) {
	if len(order) != 0 {
		b.WriteString(" order")
		for i := range order {
			b.WriteByte(' ')
			dumpPath(b, order[i].Path)
			if order[i].Desc {
				b.WriteString(":desc")
			} else {
				b.WriteString(":asc")
			}
		}
	}
	if limit != nil {
		b.WriteString(" limit ")
		dumpOperand(b, *limit)
	}
}

func dumpCreateTable(s *CreateTableStmt) string {
	var b strings.Builder
	b.WriteString("create table ")
	b.WriteString(s.Table)
	if s.IfNotExists {
		b.WriteString(" ifnotexists")
	}
	for i := range s.Columns {
		column := &s.Columns[i]
		b.WriteByte(' ')
		dumpPath(&b, column.Path)
		b.WriteByte(':')
		b.WriteString(column.Type.String())
		if column.Required {
			b.WriteString(":required")
		}
		if column.PrimaryKey {
			b.WriteString(":pk")
		}
	}
	if len(s.PrimaryKey) != 0 {
		b.WriteString(" primary")
		for _, key := range s.PrimaryKey {
			b.WriteByte(' ')
			dumpPath(&b, key)
		}
	}
	return b.String()
}

func dumpCreateIndex(s *CreateIndexStmt) string {
	var b strings.Builder
	b.WriteString("create ")
	if s.Unique {
		b.WriteString("unique ")
	}
	b.WriteString("index")
	if s.HasName {
		b.WriteByte(' ')
		b.WriteString(s.Name)
	}
	if s.IfNotExists {
		b.WriteString(" ifnotexists")
	}
	b.WriteString(" on ")
	b.WriteString(s.Table)
	for _, path := range s.Paths {
		b.WriteByte(' ')
		dumpPath(&b, path)
		b.WriteString(string(path.AppendPointer(nil)))
	}
	return b.String()
}
