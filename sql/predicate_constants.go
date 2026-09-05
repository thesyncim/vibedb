package sql

// BooleanPredicateConstant recognizes an authored Boolean literal beneath NOT.
// It does not evaluate scalar expressions or simplify branches containing row
// references: those may require domain validation before any source is pruned.
func BooleanPredicateConstant(expression *Expr) (value bool, known bool) {
	negated := false
	for expression != nil {
		negated = negated != expression.Negated
		switch expression.Kind {
		case ExprConstant:
			if expression.Value.Kind != OperandBool {
				return false, false
			}
			return expression.Value.Bool != negated, true
		case ExprNot:
			if len(expression.Kids) != 1 {
				return false, false
			}
			negated = !negated
			expression = expression.Kids[0]
		default:
			return false, false
		}
	}
	return false, false
}
