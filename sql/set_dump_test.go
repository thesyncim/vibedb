package sql

import (
	"fmt"
	"strings"
)

func dumpSetStmt(statement *SelectStmt) string {
	var b strings.Builder
	expression := statement.Set
	b.WriteString("set{")
	dumpSetExpr(&b, expression.Root)
	b.WriteString(" outputs=[")
	for i := range expression.Outputs {
		if i != 0 {
			b.WriteByte(',')
		}
		output := &expression.Outputs[i]
		fmt.Fprintf(&b, "%q@%d", output.Name, output.Pos)
		if output.Deferred {
			b.WriteByte('?')
		}
	}
	b.WriteByte(']')
	dumpSetTail(&b, expression.Tail)
	fmt.Fprintf(&b, " params=%d pos=%d", expression.Params, expression.Pos)
	if expression.ArityDeferred {
		b.WriteString(" arity-deferred")
	}
	b.WriteByte('}')
	return b.String()
}

func dumpSetExpr(b *strings.Builder, expression *SetExpr) {
	if expression == nil {
		b.WriteString("<nil>")
		return
	}
	switch expression.Kind {
	case SetSelectExpr:
		b.WriteString("leaf[")
		b.WriteString(dumpStmt(expression.Select))
		b.WriteByte(']')
	case SetBinaryExpr:
		b.WriteByte('(')
		dumpSetExpr(b, expression.Left)
		b.WriteByte(' ')
		b.WriteString(dumpSetOperation(expression.Operation))
		b.WriteByte(' ')
		dumpSetExpr(b, expression.Right)
		b.WriteByte(')')
	case SetGroupExpr:
		b.WriteString("group(")
		dumpSetExpr(b, expression.Child)
		dumpSetTail(b, expression.Tail)
		b.WriteByte(')')
	default:
		fmt.Fprintf(b, "kind(%d)", expression.Kind)
	}
	fmt.Fprintf(b, "<c=%d,p=%d+%d,@%d", expression.Columns,
		expression.ParamBase, expression.Params, expression.Pos)
	if expression.ArityDeferred {
		b.WriteString(",deferred")
	}
	if expression.End >= 0 {
		fmt.Fprintf(b, ",end=%d", expression.End)
	}
	b.WriteByte('>')
}

func dumpSetOperation(operation SetOperation) string {
	switch operation {
	case SetUnionAll:
		return "union-all"
	case SetUnionDistinct:
		return "union-distinct"
	case SetIntersectAll:
		return "intersect-all"
	case SetIntersectDistinct:
		return "intersect-distinct"
	case SetExceptAll:
		return "except-all"
	case SetExceptDistinct:
		return "except-distinct"
	default:
		return fmt.Sprintf("operation(%d)", operation)
	}
}

func dumpSetTail(b *strings.Builder, tail *SetTail) {
	if tail == nil {
		return
	}
	b.WriteString(" tail{")
	if len(tail.OrderBy) != 0 {
		b.WriteString("order")
		for i := range tail.OrderBy {
			term := &tail.OrderBy[i]
			fmt.Fprintf(b, " %q#%d", term.Name, term.Output)
			if term.Desc {
				b.WriteString(":desc")
			} else {
				b.WriteString(":asc")
			}
		}
	}
	if tail.Limit != nil {
		b.WriteString(" limit ")
		dumpOperand(b, *tail.Limit)
	}
	if tail.Offset != nil {
		b.WriteString(" offset ")
		dumpOperand(b, *tail.Offset)
	}
	fmt.Fprintf(b, " p=%d+%d@%d}", tail.ParamBase, tail.Params, tail.Pos)
}
