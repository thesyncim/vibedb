package sql

// ScalarExprKind identifies one node in a computed SQL scalar expression.
// The zero value is deliberately a path: existing AST construction that opts
// into ScalarExpr must still provide a path, while ordinary statements keep a
// nil scalar pointer and pay no scalar-specific work.
type ScalarExprKind uint8

const (
	ScalarPath ScalarExprKind = iota
	ScalarLiteral
	ScalarNull
	ScalarUnary
	ScalarBinary
	ScalarAggregate
)

// ScalarOp is an arithmetic, sign, or concatenation operation.
type ScalarOp uint8

const (
	ScalarAdd ScalarOp = iota
	ScalarSubtract
	ScalarMultiply
	ScalarDivide
	ScalarModulo
	ScalarConcat
	ScalarPositive
	ScalarNegative
)

// ScalarExpr is a lossless, parser-owned scalar expression tree. Path and
// Aggregate nodes identify values the prepared query must materialize;
// Literal and Null are source-independent; Unary and Binary retain authored
// grouping through their exact child shape.
type ScalarExpr struct {
	Kind  ScalarExprKind
	Op    ScalarOp
	Path  *PathExpr
	Value Operand
	Agg   AggKind
	Left  *ScalarExpr
	Right *ScalarExpr
	Pos   int
}
