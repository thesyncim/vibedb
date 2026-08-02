package query

import (
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// lateralGroupPlan is cold lowering state. Correlated grouping keys are
// invocation constants: removing them from the child grouping tuple is exact,
// but only if the adapter preserves the original grouped-empty cardinality and
// synthesizes every reduction that reads a captured value.
type lateralGroupPlan struct {
	program     lateralGroupProgram
	synthetic   bool
	grouped     bool
	localGroups int
}

type lateralHavingSource uint8

const (
	lateralHavingAuthored lateralHavingSource = iota
	lateralHavingChild
	lateralHavingBinding
)

type lateralHavingNode struct {
	kind    sqlast.ExprKind
	op      sqlast.CmpOp
	negated bool
	source  lateralHavingSource
	ordinal int
	value   sqlast.Operand
	list    []sqlast.Operand
	kids    []lateralHavingNode
}

// lateralGroupProgram is the warm APPLY-side reduction adapter. It exists only
// on a correlated aggregate/group shape; ordinary statements and ordinary
// LATERAL projections never branch through it.
type lateralGroupProgram struct {
	active       bool
	suppressZero bool
	countColumn  int

	having *lateralHavingNode

	// columnOutput maps an authored SELECT ordinal to the first physical APPLY
	// output after relation-wildcard expansion. HAVING requires a scalar and
	// therefore rejects an expansion wider than one.
	columnOutput []int
	columnWidth  []int

	// exact is allocated at prepare only when SUM/AVG over a captured numeric
	// value is present. One accumulator is enough: generated cells are consumed
	// or copied before the next output is computed.
	exact *Workspace
	acc   aggAcc

	// scratch is the reusable exact JSON spelling for a computed HAVING value.
	scratch []byte
}

func lateralProjectionName(column *sqlast.ResultColumn) string {
	if column.Alias != "" {
		return column.Alias
	}
	spec := column.Path.Spec()
	if column.Agg == sqlast.AggNone {
		if spec == "" {
			return "*"
		}
		return spec
	}
	return aggName(column.Agg) + "(" + spec + ")"
}

func (c *lateralClone) cloneGroupBy(
	groupBy []*sqlast.PathExpr,
) ([]*sqlast.PathExpr, error) {
	c.group.grouped = len(groupBy) != 0
	if len(groupBy) == 0 {
		return nil, nil
	}
	clone := make([]*sqlast.PathExpr, 0, len(groupBy))
	for _, path := range groupBy {
		reference, binding, outer := c.reference(path)
		if outer {
			c.mark(reference, binding, false)
			c.group.program.active = true
			continue
		}
		clone = append(clone, path)
	}
	c.group.localGroups = len(clone)
	return clone, nil
}

func (c *lateralClone) cloneHaving(
	expr *sqlast.Expr,
	columns *[]sqlast.ResultColumn,
) (*sqlast.Expr, error) {
	if expr == nil || (!c.group.program.active && !c.group.synthetic) {
		return expr, nil
	}
	node, err := c.compileHaving(expr, columns)
	if err != nil {
		return nil, err
	}
	c.group.program.active = true
	c.group.program.having = node
	return nil, nil
}

func (c *lateralClone) validateGroupTail(query *sqlast.SelectStmt) error {
	if !c.group.program.active && !c.group.synthetic {
		return nil
	}
	if query.Distinct {
		return sqlast.NewFeatureNotSupportedError(
			c.text, query.Columns[0].Pos,
			"DISTINCT after correlated LATERAL reduction needs a post-reduction distinct operator",
		)
	}
	for i := range query.Columns {
		if query.Columns[i].Window != nil {
			return sqlast.NewFeatureNotSupportedError(
				c.text, query.Columns[i].Window.Pos,
				"window evaluation after correlated LATERAL reduction needs a post-HAVING window plan",
			)
		}
	}
	if c.group.program.having == nil {
		return nil
	}
	if query.Offset != nil {
		return sqlast.NewFeatureNotSupportedError(
			c.text, query.Offset.Pos,
			"OFFSET after correlated LATERAL HAVING needs post-reduction tail execution",
		)
	}
	if query.Limit != nil {
		return sqlast.NewFeatureNotSupportedError(
			c.text, query.Limit.Pos,
			"LIMIT after correlated LATERAL HAVING needs post-reduction tail execution",
		)
	}
	return nil
}

func (c *lateralClone) compileHaving(
	expr *sqlast.Expr,
	columns *[]sqlast.ResultColumn,
) (*lateralHavingNode, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.Subquery != nil || expr.Kind == sqlast.ExprExists {
		return nil, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Pos,
			"correlated LATERAL HAVING subqueries need an inherited correlation frame",
		)
	}
	node := &lateralHavingNode{
		kind: expr.Kind, op: expr.Op, negated: expr.Negated,
		ordinal: -1, value: expr.Value, list: expr.List,
	}
	switch expr.Kind {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		node.kids = make([]lateralHavingNode, len(expr.Kids))
		for i := range expr.Kids {
			kid, err := c.compileHaving(expr.Kids[i], columns)
			if err != nil {
				return nil, err
			}
			node.kids[i] = *kid
		}
		return node, nil
	case sqlast.ExprCompare, sqlast.ExprIn, sqlast.ExprBetween, sqlast.ExprIsNull:
	case sqlast.ExprIsMissing, sqlast.ExprContains, sqlast.ExprLike:
		return nil, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Pos,
			"this HAVING operator cannot be evaluated after correlated LATERAL reduction without changing its semantics",
		)
	default:
		return nil, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Pos,
			"correlated LATERAL HAVING expression kind is not executable",
		)
	}
	if expr.RightPath != nil {
		return nil, sqlast.NewFeatureNotSupportedError(
			c.text, expr.RightPath.Pos,
			"correlated LATERAL HAVING path-to-path comparison needs an expression-valued reduction plan",
		)
	}
	if expr.Path != nil {
		reference, binding, outer := c.reference(expr.Path)
		if outer {
			c.mark(reference, binding, false)
		}
	}
	if expr.Column >= 0 {
		if expr.Column >= len(c.projection) {
			return nil, fmt.Errorf("query: invalid correlated LATERAL HAVING column")
		}
		node.source = lateralHavingAuthored
		node.ordinal = expr.Column
		return node, nil
	}
	if expr.Path == nil {
		return nil, fmt.Errorf("query: correlated LATERAL HAVING leaf has no value")
	}
	if reference, binding, outer := c.reference(expr.Path); outer {
		c.mark(reference, binding, false)
		node.source = lateralHavingBinding
		node.ordinal = binding
		return node, nil
	}
	// An unprojected local GROUP key remains legal SQL. Append it as a private
	// child output; it is grouped already and has scalar width one.
	node.source = lateralHavingChild
	node.ordinal = len(*columns)
	*columns = append(*columns, sqlast.ResultColumn{
		Path: expr.Path, Pos: expr.Path.Pos,
	})
	c.hidden++
	return node, nil
}

func (c *lateralClone) finishColumns(
	query *sqlast.SelectStmt,
	columns *[]sqlast.ResultColumn,
) error {
	needCount := c.group.synthetic ||
		(c.group.grouped && (c.group.localGroups == 0 || len(*columns) == 0))
	if needCount {
		count := -1
		for i := range *columns {
			column := &(*columns)[i]
			if column.Agg == sqlast.AggCount && column.Path == nil {
				count = i
				break
			}
		}
		if count < 0 {
			pos := 0
			if len(query.Columns) != 0 {
				pos = query.Columns[0].Pos
			}
			count = len(*columns)
			*columns = append(*columns, sqlast.ResultColumn{
				Agg: sqlast.AggCount, Pos: pos,
			})
			c.hidden++
		}
		c.group.program.active = true
		c.group.program.countColumn = count
		c.group.program.suppressZero = c.group.grouped && c.group.localGroups == 0
	}
	if len(*columns) == 0 {
		if query.Distinct {
			return sqlast.NewFeatureNotSupportedError(
				c.text, query.Columns[0].Pos,
				"DISTINCT over only correlated LATERAL projections needs a cardinality-only distinct operator",
			)
		}
		path := c.cardinalityPath(query.Where)
		if path == nil {
			path = &sqlast.PathExpr{Source: 0, Pos: query.Columns[0].Pos}
		}
		*columns = append(*columns, sqlast.ResultColumn{
			Path: path, Pos: query.Columns[0].Pos,
		})
		c.hidden++
	}
	if c.group.synthetic {
		for i := range c.projection {
			switch c.projection[i].agg {
			case sqlast.AggSum, sqlast.AggAvg:
				c.group.program.exact = new(Workspace)
				return nil
			}
		}
	}
	return nil
}

func (g *lateralGroupProgram) resolve(starts, widths []int) error {
	if !g.active {
		return nil
	}
	if g.countColumn >= 0 {
		if g.countColumn >= len(starts) || widths[g.countColumn] != 1 {
			return fmt.Errorf("query: correlated LATERAL cardinality column is unstable")
		}
		g.countColumn = starts[g.countColumn]
	}
	var resolveNode func(*lateralHavingNode) error
	resolveNode = func(node *lateralHavingNode) error {
		if node == nil {
			return nil
		}
		if node.source == lateralHavingChild {
			if node.ordinal < 0 || node.ordinal >= len(starts) || widths[node.ordinal] != 1 {
				return fmt.Errorf("query: correlated LATERAL HAVING child column is unstable")
			}
			node.ordinal = starts[node.ordinal]
		}
		for i := range node.kids {
			if err := resolveNode(&node.kids[i]); err != nil {
				return err
			}
		}
		return nil
	}
	return resolveNode(g.having)
}

func (g *lateralGroupProgram) begin(options ExecOptions) error {
	if g.exact == nil {
		return nil
	}
	limit, err := normalizeAggregateBytes(options)
	if err != nil {
		return err
	}
	g.exact.aggregateBudget.begin(limit)
	g.exact.aggregateOut = g.exact.aggregateOut[:0]
	return nil
}

func (g *lateralGroupProgram) count(cursor *Cursor) (int, error) {
	if g.countColumn < 0 {
		return 0, fmt.Errorf("query: correlated LATERAL reduction has no cardinality column")
	}
	count, ok := cursor.Cell(g.countColumn).Int64()
	if !ok || count < 0 || uint64(count) > uint64(math.MaxInt) {
		return 0, fmt.Errorf("query: invalid correlated LATERAL group cardinality")
	}
	return int(count), nil
}

func (l *statementLateral) keepRightRow(
	join *statementRelationJoin,
	cursor *Cursor,
	cancel *CancelFlag,
) (bool, error) {
	g := &l.group
	if !g.active {
		return true, nil
	}
	if g.suppressZero {
		count, err := g.count(cursor)
		if err != nil {
			return false, err
		}
		if count == 0 {
			return false, nil
		}
	}
	if g.having == nil {
		return true, cancellationError(cancel)
	}
	value, err := l.evalHaving(join, cursor, g.having, cancel)
	return value == triTrue, err
}

func (l *statementLateral) evalHaving(
	join *statementRelationJoin,
	cursor *Cursor,
	node *lateralHavingNode,
	cancel *CancelFlag,
) (tri, error) {
	switch node.kind {
	case sqlast.ExprAnd:
		out := triTrue
		for i := range node.kids {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			value, err := l.evalHaving(join, cursor, &node.kids[i], cancel)
			if err != nil || value == triFalse {
				return value, err
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprOr:
		out := triFalse
		for i := range node.kids {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			value, err := l.evalHaving(join, cursor, &node.kids[i], cancel)
			if err != nil || value == triTrue {
				return value, err
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprNot:
		if len(node.kids) != 1 {
			return triFalse, fmt.Errorf("query: invalid correlated LATERAL HAVING NOT")
		}
		value, err := l.evalHaving(join, cursor, &node.kids[0], cancel)
		return notTri(value), err
	}
	value, err := l.havingScalar(cursor, node, cancel)
	if err != nil {
		return triFalse, err
	}
	var out tri
	switch node.kind {
	case sqlast.ExprIsNull:
		out = boolTri(value.kind == kindNull)
	case sqlast.ExprCompare:
		literal, known, err := join.joinOperand(node.value, l.args)
		if err != nil {
			return triFalse, err
		}
		out = compareTri(value, havingLit{value: literal, known: known}, Op(node.op))
	case sqlast.ExprBetween:
		if len(node.list) != 2 {
			return triFalse, fmt.Errorf("query: invalid correlated LATERAL HAVING BETWEEN")
		}
		lower, known, err := join.joinOperand(node.list[0], l.args)
		if err != nil {
			return triFalse, err
		}
		lo := compareTri(value, havingLit{value: lower, known: known}, Ge)
		upper, known, err := join.joinOperand(node.list[1], l.args)
		if err != nil {
			return triFalse, err
		}
		out = andTri(lo, compareTri(value, havingLit{value: upper, known: known}, Le))
	case sqlast.ExprIn:
		if value.kind == kindNull {
			out = triUnknown
			break
		}
		out = triFalse
		for i := range node.list {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			literal, known, err := join.joinOperand(node.list[i], l.args)
			if err != nil {
				return triFalse, err
			}
			if !known {
				if out == triFalse {
					out = triUnknown
				}
				continue
			}
			if compareScalar(value, literal) == 0 {
				out = triTrue
				break
			}
		}
	default:
		return triFalse, fmt.Errorf("query: unsupported correlated LATERAL HAVING kind %d", node.kind)
	}
	if node.negated {
		out = notTri(out)
	}
	return out, cancellationError(cancel)
}

func (l *statementLateral) havingScalar(
	cursor *Cursor,
	node *lateralHavingNode,
	cancel *CancelFlag,
) (scalar, error) {
	switch node.source {
	case lateralHavingBinding:
		if node.ordinal < 0 || node.ordinal >= len(l.slots) {
			return scalar{}, fmt.Errorf("query: correlated LATERAL HAVING binding is out of range")
		}
		return l.slots[node.ordinal].value, nil
	case lateralHavingChild:
		if node.ordinal < 0 || node.ordinal >= l.localOutputs {
			return scalar{}, fmt.Errorf("query: correlated LATERAL HAVING child output is out of range")
		}
		return l.group.cellScalar(cursor.Cell(node.ordinal)), nil
	case lateralHavingAuthored:
		if node.ordinal < 0 || node.ordinal >= len(l.group.columnOutput) ||
			l.group.columnWidth[node.ordinal] != 1 {
			return scalar{}, fmt.Errorf("query: correlated LATERAL HAVING requires one scalar output")
		}
		cell, err := l.outputCell(cursor, l.group.columnOutput[node.ordinal], cancel)
		if err != nil {
			return scalar{}, err
		}
		return l.group.cellScalar(cell), nil
	default:
		return scalar{}, fmt.Errorf("query: invalid correlated LATERAL HAVING source")
	}
}

func (g *lateralGroupProgram) cellScalar(cell Cell) scalar {
	// Reuse the established HAVING conversion so exact computed decimals and
	// container/null distinctions flow through the same comparator.
	program := havingProgram{scratch: g.scratch}
	value := program.cellScalar(cell)
	g.scratch = program.scratch
	return value
}

func (l *statementLateral) aggregateOutputCell(
	cursor *Cursor,
	output *lateralOutput,
	cancel *CancelFlag,
) (Cell, error) {
	count, err := l.group.count(cursor)
	if err != nil {
		return Cell{}, err
	}
	if output.binding < 0 || output.binding >= len(l.slots) {
		return Cell{}, fmt.Errorf("query: correlated LATERAL aggregate binding is out of range")
	}
	value := l.slots[output.binding].value
	switch output.agg {
	case sqlast.AggCount:
		if !present(value) {
			count = 0
		}
		return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(count)}, nil
	case sqlast.AggMin, sqlast.AggMax:
		if count == 0 || value.kind != kindNumber {
			return nullCell(), nil
		}
		return cellFromScalar(value), cancellationError(cancel)
	case sqlast.AggSum, sqlast.AggAvg:
		if count == 0 || value.kind != kindNumber {
			return nullCell(), nil
		}
		if l.group.exact == nil {
			return Cell{}, fmt.Errorf("query: correlated LATERAL exact aggregate workspace is absent")
		}
		return l.group.repeatNumber(value, count, output.agg, cancel)
	default:
		return Cell{}, fmt.Errorf("query: invalid correlated LATERAL aggregate kind %d", output.agg)
	}
}

func (g *lateralGroupProgram) repeatNumber(
	value scalar,
	count int,
	kind sqlast.AggKind,
	cancel *CancelFlag,
) (Cell, error) {
	if err := cancellationError(cancel); err != nil {
		return Cell{}, err
	}
	if g.acc.num != nil {
		g.acc.num.reset()
	}
	g.acc.count = 0
	number, err := g.acc.number(&g.exact.aggregateBudget)
	if err != nil {
		return Cell{}, err
	}
	if err := number.sum.add(value, &g.acc.lease, &g.exact.aggregateBudget); err != nil {
		return Cell{}, err
	}
	number.n = count
	if count > 1 {
		if err := lateralMultiplyDecimal(
			&number.sum, count, &g.acc.lease, &g.exact.aggregateBudget,
		); err != nil {
			return Cell{}, err
		}
	}
	if err := cancellationError(cancel); err != nil {
		return Cell{}, err
	}
	g.exact.aggregateOut = g.exact.aggregateOut[:0]
	if kind == sqlast.AggAvg {
		return g.exact.exactAverageCell(&g.acc)
	}
	return g.exact.exactSumCell(&g.acc)
}

func lateralMultiplyDecimal(
	sum *decimalSum,
	count int,
	lease *aggregateLease,
	budget *aggregateBudget,
) error {
	if count <= 1 || !sum.set {
		return nil
	}
	if !sum.big {
		if product, ok := checkedMulInt64(sum.smallCoeff, int64(count)); ok {
			sum.smallCoeff = product
			sum.digits = intDigits64(product)
			return nil
		}
	}
	digits := saturatedBytes(int64(max(sum.digits, 1)), 32)
	digits = saturatedBytes(digits, 96)
	need := saturatedBytes(aggregateAccBaseBytes, saturatedProduct(8, digits))
	if err := lease.reserve(budget, need); err != nil {
		return err
	}
	if !sum.big {
		sum.promote()
	}
	sum.tmp.SetInt64(int64(count))
	sum.coeff.Mul(&sum.coeff, &sum.tmp)
	sum.normalizeBig()
	return nil
}

func (g *lateralGroupProgram) release() {
	if g == nil {
		return
	}
	if g.exact != nil {
		g.exact.Release()
	}
	*g = lateralGroupProgram{}
}
