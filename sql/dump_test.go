package sql

import (
	"fmt"
	"strings"
)

// The AST renderer the acceptance tests assert against.
//
// A table-driven test that asserted field by field would either check a handful
// of fields per case — leaving the rest silently unverified — or need a
// hand-written expectation per case that is longer than the SQL it describes.
// One total rendering means every case asserts the whole tree, including the
// fields it was not written to exercise: a case about operator precedence still
// fails if path resolution regresses.
//
// The rendering is deliberately lossless for every field a lowering pass reads.
// A field that does not appear here is a field the tests cannot see.

func dumpStmt(s *SelectStmt) string {
	var b strings.Builder
	if s.With != nil {
		b.WriteString("with")
		for i := range s.With.CTEs {
			cte := &s.With.CTEs[i]
			fmt.Fprintf(&b, " %s", cte.Name)
			if len(cte.Columns) != 0 {
				b.WriteByte('(')
				for j, name := range cte.Columns {
					if j != 0 {
						b.WriteByte(',')
					}
					b.WriteString(name)
				}
				b.WriteByte(')')
			}
			switch cte.Materialization {
			case CTEMaterialized:
				b.WriteString(" materialized")
			case CTENotMaterialized:
				b.WriteString(" not-materialized")
			}
			if cte.ColumnArityDeferred {
				b.WriteString(" arity-deferred")
			}
			b.WriteString("=(")
			b.WriteString(dumpStmt(cte.Query))
			b.WriteByte(')')
		}
		b.WriteByte(' ')
	}
	b.WriteString("select")
	if s.Distinct {
		b.WriteString(" distinct")
	}
	for i := range s.Columns {
		b.WriteByte(' ')
		dumpColumn(&b, &s.Columns[i])
	}
	b.WriteString(" from")
	for i := range s.From {
		b.WriteByte(' ')
		dumpTable(&b, &s.From[i])
	}
	if s.Where != nil {
		b.WriteString(" where ")
		dumpExpr(&b, s.Where)
	}
	if len(s.GroupBy) > 0 {
		b.WriteString(" group")
		for _, key := range s.GroupBy {
			b.WriteByte(' ')
			dumpPath(&b, key)
		}
	}
	if s.Having != nil {
		b.WriteString(" having ")
		dumpExpr(&b, s.Having)
	}
	if len(s.OrderBy) > 0 {
		b.WriteString(" order")
		for i := range s.OrderBy {
			b.WriteByte(' ')
			if s.OrderBy[i].Output != 0 {
				fmt.Fprintf(&b, "output(%d)", s.OrderBy[i].Output-1)
			} else {
				dumpPath(&b, s.OrderBy[i].Path)
			}
			if s.OrderBy[i].Desc {
				b.WriteString(":desc")
			} else {
				b.WriteString(":asc")
			}
		}
	}
	if s.Limit != nil {
		b.WriteString(" limit ")
		dumpOperand(&b, *s.Limit)
	}
	if s.Offset != nil {
		b.WriteString(" offset ")
		dumpOperand(&b, *s.Offset)
	}
	if s.Params > 0 {
		fmt.Fprintf(&b, " params=%d", s.Params)
	}
	return b.String()
}

func dumpColumn(b *strings.Builder, c *ResultColumn) {
	switch {
	case c.Window != nil:
		dumpWindow(b, c.Window)
	case c.Agg == AggNone:
		b.WriteString("path(")
		dumpPath(b, c.Path)
		b.WriteByte(')')
	case c.Path == nil:
		b.WriteString("count(*)")
	default:
		b.WriteString(strings.ToLower(c.Agg.String()))
		b.WriteByte('(')
		dumpPath(b, c.Path)
		b.WriteByte(')')
	}
	if c.Alias != "" {
		fmt.Fprintf(b, " as %s", c.Alias)
	}
}

func dumpWindow(b *strings.Builder, w *WindowExpr) {
	b.WriteString(strings.ToLower(w.Kind.String()))
	b.WriteByte('(')
	if w.Argument != nil {
		dumpPath(b, w.Argument)
	} else if w.Kind == WindowCount {
		b.WriteByte('*')
	}
	if w.Kind == WindowLag || w.Kind == WindowLead {
		if w.HasOffset {
			b.WriteByte(',')
			dumpOperand(b, w.Offset)
		}
		if w.HasDefault {
			b.WriteByte(',')
			if w.DefaultNull {
				b.WriteString("null")
			} else {
				dumpOperand(b, w.Default)
			}
		}
	}
	if w.HasBuckets {
		dumpOperand(b, w.Buckets)
	}
	if w.HasNth {
		b.WriteByte(',')
		dumpOperand(b, w.Nth)
	}
	b.WriteString(") over(")
	if len(w.Spec.PartitionBy) != 0 {
		b.WriteString("partition")
		for _, path := range w.Spec.PartitionBy {
			b.WriteByte(' ')
			dumpPath(b, path)
		}
	}
	if len(w.Spec.OrderBy) != 0 {
		if len(w.Spec.PartitionBy) != 0 {
			b.WriteByte(' ')
		}
		b.WriteString("order")
		for i := range w.Spec.OrderBy {
			term := &w.Spec.OrderBy[i]
			b.WriteByte(' ')
			dumpPath(b, term.Path)
			if term.Desc {
				b.WriteString(":desc")
			} else {
				b.WriteString(":asc")
			}
			switch term.Nulls {
			case WindowNullsFirst:
				b.WriteString(":nulls-first")
			case WindowNullsLast:
				b.WriteString(":nulls-last")
			}
		}
	}
	if w.Spec.Frame.Explicit {
		if len(w.Spec.PartitionBy) != 0 || len(w.Spec.OrderBy) != 0 {
			b.WriteByte(' ')
		}
		if w.Spec.Frame.Unit == WindowFrameGroups {
			b.WriteString("groups ")
		} else {
			b.WriteString("rows ")
		}
		dumpWindowBound(b, w.Spec.Frame.Start)
		b.WriteString(" to ")
		dumpWindowBound(b, w.Spec.Frame.End)
	}
	b.WriteByte(')')
}

func dumpWindowBound(b *strings.Builder, bound WindowFrameBound) {
	switch bound.Kind {
	case WindowUnboundedPreceding:
		b.WriteString("unbounded-preceding")
	case WindowPreceding:
		dumpOperand(b, bound.Offset)
		b.WriteString("-preceding")
	case WindowCurrentRow:
		b.WriteString("current-row")
	case WindowFollowing:
		dumpOperand(b, bound.Offset)
		b.WriteString("-following")
	case WindowUnboundedFollowing:
		b.WriteString("unbounded-following")
	}
}

func dumpTable(b *strings.Builder, t *TableRef) {
	switch t.Kind {
	case RelationCollection:
		b.WriteString(t.Name)
	case RelationDerived:
		b.WriteString("derived(")
		b.WriteString(dumpStmt(t.Query))
		b.WriteByte(')')
	case RelationCTE:
		b.WriteString("cte(")
		b.WriteString(t.Name)
		b.WriteByte(')')
	default:
		fmt.Fprintf(b, "relation-kind-%d", t.Kind)
	}
	if t.UnresolvedCTE.Kind != CTEReferenceNone {
		fmt.Fprintf(b, " unresolved-cte-%d", t.UnresolvedCTE.Kind)
	}
	if t.HasAlias {
		fmt.Fprintf(b, "/%s", t.Alias)
	}
	if t.On != nil {
		if t.Join == JoinLeft {
			b.WriteString(" left-join(")
		} else {
			b.WriteString(" join(")
		}
		dumpPath(b, t.On.Left)
		b.WriteByte('=')
		dumpPath(b, t.On.Right)
		b.WriteByte(')')
	}
}

// dumpPath renders a path as "<source index>:<engine path spec>", so a case
// asserts both which range variable a path bound to and the exact spelling the
// engine's path compiler will receive.
func dumpPath(b *strings.Builder, p *PathExpr) {
	if p == nil {
		b.WriteString("<nil>")
		return
	}
	fmt.Fprintf(b, "%d:%s", p.Source, p.Spec())
}

func dumpExpr(b *strings.Builder, e *Expr) {
	switch e.Kind {
	case ExprAnd, ExprOr, ExprNot:
		b.WriteByte('(')
		switch e.Kind {
		case ExprAnd:
			b.WriteString("and")
		case ExprOr:
			b.WriteString("or")
		default:
			b.WriteString("not")
		}
		for _, kid := range e.Kids {
			b.WriteByte(' ')
			dumpExpr(b, kid)
		}
		b.WriteByte(')')
		return
	}
	b.WriteByte('(')
	switch e.Kind {
	case ExprCompare:
		fmt.Fprintf(b, "cmp %s ", e.Op)
		dumpLeaf(b, e)
		if e.Subquery != nil {
			b.WriteString(" (")
			b.WriteString(dumpStmt(e.Subquery))
			b.WriteByte(')')
		} else {
			b.WriteByte(' ')
			dumpOperand(b, e.Value)
		}
	case ExprIn:
		b.WriteString(negated(e, "in", "notin"))
		b.WriteByte(' ')
		dumpLeaf(b, e)
		if e.Subquery != nil {
			b.WriteString(" (")
			b.WriteString(dumpStmt(e.Subquery))
			b.WriteByte(')')
		}
		for _, value := range e.List {
			b.WriteByte(' ')
			dumpOperand(b, value)
		}
	case ExprExists:
		b.WriteString("exists (")
		b.WriteString(dumpStmt(e.Subquery))
		b.WriteByte(')')
	case ExprConstant:
		b.WriteString("constant ")
		dumpOperand(b, e.Value)
	case ExprBetween:
		b.WriteString(negated(e, "between", "notbetween"))
		b.WriteByte(' ')
		dumpLeaf(b, e)
		for _, value := range e.List {
			b.WriteByte(' ')
			dumpOperand(b, value)
		}
	case ExprIsNull:
		b.WriteString(negated(e, "isnull", "isnotnull"))
		b.WriteByte(' ')
		dumpLeaf(b, e)
	case ExprIsMissing:
		b.WriteString(negated(e, "ismissing", "isnotmissing"))
		b.WriteByte(' ')
		dumpLeaf(b, e)
	case ExprContains:
		b.WriteString("contains ")
		dumpLeaf(b, e)
		b.WriteByte(' ')
		dumpOperand(b, e.Value)
	case ExprLike:
		if e.Insensitive {
			b.WriteString(negated(e, "ilike", "notilike"))
		} else {
			b.WriteString(negated(e, "like", "notlike"))
		}
		b.WriteByte(' ')
		dumpLeaf(b, e)
		b.WriteByte(' ')
		dumpOperand(b, e.Value)
	}
	b.WriteByte(')')
}

func negated(e *Expr, plain, not string) string {
	if e.Negated {
		return not
	}
	return plain
}

// dumpLeaf renders a leaf's left operand, including the HAVING binding when
// there is one: "sum(0:score)@1" is SUM over score, bound to output column 1.
func dumpLeaf(b *strings.Builder, e *Expr) {
	if e.Agg == AggNone {
		dumpPath(b, e.Path)
		if e.Column >= 0 {
			fmt.Fprintf(b, "@%d", e.Column)
		}
		return
	}
	b.WriteString(strings.ToLower(e.Agg.String()))
	b.WriteByte('(')
	if e.Path == nil {
		b.WriteByte('*')
	} else {
		dumpPath(b, e.Path)
	}
	fmt.Fprintf(b, ")@%d", e.Column)
}

func dumpOperand(b *strings.Builder, o Operand) {
	switch o.Kind {
	case OperandString:
		fmt.Fprintf(b, "s%q", o.Text)
	case OperandNumber:
		fmt.Fprintf(b, "n%s", o.Text)
	case OperandBool:
		if o.Bool {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case OperandJSON:
		fmt.Fprintf(b, "j%s", o.Text)
	case OperandParam:
		fmt.Fprintf(b, "?%d", o.Ordinal)
	}
}
