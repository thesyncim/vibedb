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
	if s.Set != nil {
		return dumpSetStmt(s)
	}
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
	if len(s.Windows) != 0 {
		b.WriteString(" window")
		for i := range s.Windows {
			b.WriteByte(' ')
			b.WriteString(s.Windows[i].Name)
			b.WriteString("=(")
			dumpWindowSpec(&b, &s.Windows[i].Spec)
			b.WriteByte(')')
		}
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
	case c.Scalar != nil:
		dumpScalar(b, c.Scalar)
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
	dumpWindowSpec(b, &w.Spec)
	b.WriteByte(')')
}

func dumpWindowSpec(b *strings.Builder, spec *WindowSpec) {
	wrote := false
	if spec.Name != "" {
		b.WriteString("name=")
		b.WriteString(spec.Name)
		wrote = true
	}
	if len(spec.PartitionBy) != 0 {
		if wrote {
			b.WriteByte(' ')
		}
		b.WriteString("partition")
		for _, path := range spec.PartitionBy {
			b.WriteByte(' ')
			dumpPath(b, path)
		}
		wrote = true
	}
	if len(spec.OrderBy) != 0 {
		if wrote {
			b.WriteByte(' ')
		}
		b.WriteString("order")
		for i := range spec.OrderBy {
			term := &spec.OrderBy[i]
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
		wrote = true
	}
	if spec.Frame.Explicit {
		if wrote {
			b.WriteByte(' ')
		}
		switch spec.Frame.Unit {
		case WindowFrameGroups:
			b.WriteString("groups ")
		case WindowFrameRange:
			b.WriteString("range ")
		default:
			b.WriteString("rows ")
		}
		dumpWindowBound(b, spec.Frame.Start)
		b.WriteString(" to ")
		dumpWindowBound(b, spec.Frame.End)
		if spec.Frame.ExclusionExplicit {
			b.WriteString(" exclude ")
			switch spec.Frame.Exclusion {
			case WindowExcludeCurrentRow:
				b.WriteString("current-row")
			case WindowExcludeGroup:
				b.WriteString("group")
			case WindowExcludeTies:
				b.WriteString("ties")
			default:
				b.WriteString("no-others")
			}
		}
	}
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
	if t.Lateral != nil {
		b.WriteString("lateral")
		if t.Lateral.Decorrelated {
			b.WriteString("-decorrelated")
		}
		b.WriteByte('[')
		for i := range t.Lateral.Bindings {
			if i != 0 {
				b.WriteByte(',')
			}
			binding := &t.Lateral.Bindings[i]
			fmt.Fprintf(b, "%d=depth%d/source%d:", i+1, binding.Depth, binding.Source)
			path := PathExpr{Segments: binding.Segments}
			b.WriteString(path.Spec())
		}
		if len(t.Lateral.References) != 0 {
			b.WriteString(";refs=")
			for i := range t.Lateral.References {
				if i != 0 {
					b.WriteByte(',')
				}
				reference := &t.Lateral.References[i]
				fmt.Fprintf(b, "%d@", reference.Binding+1)
				dumpPath(b, reference.Path)
			}
		}
		b.WriteString("]-")
	}
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

// dumpPath renders a path as "<source index>:<engine path spec>". Correlated
// occurrences remain ordinary-sized PathExpr nodes; dumpTable renders their
// exact occurrence-to-slot mapping from the relation's LateralSpec sidecar.
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
		} else if e.RightPath != nil {
			b.WriteByte(' ')
			dumpPath(b, e.RightPath)
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
	case ExprScalarCompare:
		fmt.Fprintf(b, "scalarcmp %s ", e.Op)
		dumpScalar(b, e.ScalarLeft)
		b.WriteByte(' ')
		dumpScalar(b, e.ScalarRight)
	case ExprScalarIsNull:
		b.WriteString(negated(e, "scalar-isnull ", "scalar-isnotnull "))
		dumpScalar(b, e.ScalarLeft)
	case ExprScalarTruth:
		b.WriteString("scalar-truth ")
		dumpScalar(b, e.ScalarLeft)
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

func dumpScalar(b *strings.Builder, e *ScalarExpr) {
	if e == nil {
		b.WriteString("<nil-scalar>")
		return
	}
	switch e.Kind {
	case ScalarPath:
		b.WriteString("path(")
		dumpPath(b, e.Path)
		b.WriteByte(')')
	case ScalarAggregate:
		b.WriteString(strings.ToLower(e.Agg.String()))
		b.WriteByte('(')
		if e.Path == nil {
			b.WriteByte('*')
		} else {
			dumpPath(b, e.Path)
		}
		b.WriteByte(')')
	case ScalarLiteral:
		dumpOperand(b, e.Value)
	case ScalarNull:
		b.WriteString("null")
	case ScalarUnary:
		b.WriteByte('(')
		b.WriteString(dumpScalarOp(e.Op))
		b.WriteByte(' ')
		dumpScalar(b, e.Left)
		b.WriteByte(')')
	case ScalarBinary:
		b.WriteByte('(')
		b.WriteString(dumpScalarOp(e.Op))
		b.WriteByte(' ')
		dumpScalar(b, e.Left)
		b.WriteByte(' ')
		dumpScalar(b, e.Right)
		b.WriteByte(')')
	case ScalarCast:
		b.WriteString("cast(")
		b.WriteString(dumpScalarCastTarget(e.Cast))
		b.WriteByte(' ')
		dumpScalar(b, e.Left)
		b.WriteByte(')')
	case ScalarCase:
		b.WriteString("case")
		if e.Left != nil {
			b.WriteByte(' ')
			dumpScalar(b, e.Left)
		}
		for i := range e.Whens {
			b.WriteString(" when ")
			if e.Whens[i].Predicate != nil {
				dumpExpr(b, e.Whens[i].Predicate)
			} else {
				dumpScalar(b, e.Whens[i].Match)
			}
			b.WriteString(" then ")
			dumpScalar(b, e.Whens[i].Result)
		}
		if e.Else != nil {
			b.WriteString(" else ")
			dumpScalar(b, e.Else)
		}
		b.WriteString(" end")
	default:
		fmt.Fprintf(b, "scalar-kind(%d)", e.Kind)
	}
}

func dumpScalarOp(op ScalarOp) string {
	switch op {
	case ScalarAdd:
		return "+"
	case ScalarSubtract:
		return "-"
	case ScalarMultiply:
		return "*"
	case ScalarDivide:
		return "/"
	case ScalarModulo:
		return "%"
	case ScalarConcat:
		return "||"
	case ScalarPositive:
		return "u+"
	case ScalarNegative:
		return "u-"
	default:
		return fmt.Sprintf("op(%d)", op)
	}
}

func dumpScalarCastTarget(target ScalarCastTarget) string {
	switch target {
	case ScalarCastText:
		return "text"
	case ScalarCastBoolean:
		return "boolean"
	case ScalarCastNumeric:
		return "numeric"
	case ScalarCastJSON:
		return "json"
	default:
		return fmt.Sprintf("target(%d)", target)
	}
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
