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
	}
	return dumpStmt(s.Select)
}

func dumpInsert(s *InsertStmt) string {
	var b strings.Builder
	b.WriteString("insert into ")
	b.WriteString(s.Table)
	if s.Explicit {
		b.WriteString(" explicit")
	}
	for i := range s.Rows {
		b.WriteString(" (")
		dumpOperand(&b, s.Rows[i].Key)
		b.WriteString(", ")
		dumpOperand(&b, s.Rows[i].Doc)
		b.WriteByte(')')
	}
	fmt.Fprintf(&b, " params=%d", s.Params)
	return b.String()
}

func dumpUpdate(s *UpdateStmt) string {
	var b strings.Builder
	b.WriteString("update ")
	b.WriteString(s.Table)
	b.WriteString(" set ")
	dumpOperand(&b, s.Doc)
	dumpTargets(&b, s.Keys, s.Filter, false)
	fmt.Fprintf(&b, " params=%d", s.Params)
	return b.String()
}

func dumpDelete(s *DeleteStmt) string {
	var b strings.Builder
	b.WriteString("delete from ")
	b.WriteString(s.Table)
	dumpTargets(&b, s.Keys, s.Filter, s.All)
	fmt.Fprintf(&b, " params=%d", s.Params)
	return b.String()
}

// dumpTargets renders which documents a statement acts on, in whichever of the
// three forms it has.
func dumpTargets(b *strings.Builder, keys []Operand, filter *SelectStmt, all bool) {
	switch {
	case keys != nil:
		b.WriteString(" keys")
		for _, key := range keys {
			b.WriteByte(' ')
			dumpOperand(b, key)
		}
	case all:
		b.WriteString(" all")
	case filter != nil && filter.Where != nil:
		b.WriteString(" where ")
		dumpExpr(b, filter.Where)
	default:
		b.WriteString(" <no target>")
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
	b.WriteString("create index")
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
