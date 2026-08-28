package sql

import "strings"

// Text equality can use the native indexed/routed path predicate when no
// non-string JSON value can stringify to the literal. This is a sufficient
// condition from the JSON grammar, never an assumption about stored types.
// Values such as "92", "true", or "{}" retain the actual text conversion.
// Applying this on the cold parser path also leaves distributed planners the
// same path predicate they receive for city = 'Lisbon'; no row-time wrapper,
// document decode, or extra dependency is needed.
func lowerTextPathEquality(node *Expr) {
	if node.Op != OpEq {
		return
	}
	left, right := node.ScalarLeft, node.ScalarRight
	if left.Kind == ScalarLiteral {
		left, right = right, left
	}
	if left.Kind != ScalarCast || left.Cast != ScalarCastText || left.Left == nil ||
		left.Left.Kind != ScalarPath || right.Kind != ScalarLiteral || right.Value.Kind != OperandString {
		return
	}
	text := strings.TrimSpace(right.Value.Text)
	if text == "true" || text == "false" {
		return
	}
	if len(text) != 0 {
		c := text[0]
		if c == '{' || c == '[' || c == '-' || c >= '0' && c <= '9' {
			return
		}
	}
	*node = Expr{Kind: ExprCompare, Op: OpEq, Column: -1,
		Path: left.Left.Path, Value: right.Value, Pos: node.Pos}
}
