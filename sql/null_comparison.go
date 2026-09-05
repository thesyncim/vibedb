package sql

func (p *Parser) parseDistinctTail(ctx exprContext, left *ScalarExpr, negated bool, pos int) (*Expr, error) {
	if err := p.expectKeyword(kwFrom, "FROM after IS [NOT] DISTINCT"); err != nil {
		return nil, err
	}
	right, err := p.parseScalarExpression(scalarContextForPredicate(ctx))
	if err != nil {
		return nil, err
	}
	if err := p.coerceTypedCaseUnknownResults([]ScalarWhen{{Result: left}, {Result: right}}, nil); err != nil {
		return nil, err
	}
	comparison := p.newScalar(ScalarBinary, pos)
	comparison.Op, comparison.Left, comparison.Right = ScalarDistinct, left, right
	if negated {
		comparison.Op = ScalarNotDistinct
	}
	node := p.exprs.one()
	*node = Expr{Kind: ExprScalarTruth, Column: -1, ScalarLeft: comparison, Pos: pos}
	return node, nil
}

// NullSafeEqualityPathOperand identifies a necessary equality domain for a
// non-null physical placement key. It never rewrites the executed predicate:
// the caller may use this proof only to select owning shards.
func NullSafeEqualityPathOperand(expr *Expr) (*PathExpr, Operand, bool) {
	if expr == nil || expr.Kind != ExprScalarTruth || expr.Negated || expr.ScalarLeft == nil {
		return nil, Operand{}, false
	}
	value := expr.ScalarLeft
	if value.Kind != ScalarBinary || value.Op != ScalarNotDistinct || value.Left == nil || value.Right == nil {
		return nil, Operand{}, false
	}
	for _, pair := range [][2]*ScalarExpr{{value.Left, value.Right}, {value.Right, value.Left}} {
		if pair[0].Kind != ScalarPath || pair[0].Path == nil {
			continue
		}
		if pair[1].Kind == ScalarNull {
			return pair[0].Path, Operand{Kind: OperandNull}, true
		}
		if pair[1].Kind == ScalarLiteral {
			return pair[0].Path, pair[1].Value, true
		}
	}
	return nil, Operand{}, false
}

// NullSafePathComparison exposes the two physical paths to existing schema
// operator analysis and runtime comparison barriers. The authored expression
// is retained for evaluation; this small view is only a comparison proof.
func NullSafePathComparison(expr *ScalarExpr) *Expr {
	if expr == nil || expr.Kind != ScalarBinary || !expr.Op.NullSafeComparison() ||
		expr.Left == nil || expr.Right == nil || expr.Left.Kind != ScalarPath || expr.Right.Kind != ScalarPath {
		return nil
	}
	return &Expr{Kind: ExprCompare, Op: OpEq, Path: expr.Left.Path, RightPath: expr.Right.Path, Pos: expr.Pos, Column: -1}
}
