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
	ScalarCast
	ScalarCase
)

// ScalarOp is an arithmetic, sign, concatenation, or conditional operation.
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
	ScalarCoalesce
	ScalarGreatest
	ScalarLeast
	ScalarNullIf
	ScalarDistinct
	ScalarNotDistinct
)

// Conditional reports whether op selects among its operands. Variadic SQL
// conditional expressions are represented by balanced binary trees, retaining
// argument order and evaluating each argument at most once.
func (op ScalarOp) Conditional() bool { return op >= ScalarCoalesce && op <= ScalarNotDistinct }

// NullSafeComparison reports a total equality predicate: NULL compares equal
// to NULL and different from a live scalar value.
func (op ScalarOp) NullSafeComparison() bool { return op == ScalarDistinct || op == ScalarNotDistinct }

// ScalarCastTarget is one closed, executable SQL conversion domain. JSON is
// deliberately the textual json type, not jsonb: the engine preserves exact
// number spellings, string escapes, object order, and duplicate keys and must
// not advertise jsonb normalization it did not perform.
type ScalarCastTarget uint8

const (
	ScalarCastText ScalarCastTarget = iota
	ScalarCastBoolean
	ScalarCastNumeric
	ScalarCastJSON
)

// ScalarExpr is a lossless, parser-owned scalar expression tree. Path and
// Aggregate nodes identify values the prepared query must materialize;
// Literal and Null are source-independent; Unary and Binary retain authored
// grouping through their exact child shape; Case retains ordered lazy arms.
type ScalarExpr struct {
	Kind  ScalarExprKind
	Op    ScalarOp
	Path  *PathExpr
	Value Operand
	Agg   AggKind
	Cast  ScalarCastTarget
	// TypedConstant marks PostgreSQL's type 'string' spelling. It is set only
	// on the ScalarCast introduced by that grammar production, allowing the
	// compiler to perform typinput once at prepare and the output-name resolver
	// to mirror PostgreSQL without confusing an ordinary CAST over a path.
	TypedConstant bool
	Left          *ScalarExpr
	Right         *ScalarExpr
	Pos           int
	// TargetPos is the source byte position of a CAST target. Keeping it
	// distinct from Pos lets unsupported target diagnostics point at the type
	// while runtime conversion failures point at the authored CAST.
	TargetPos int
	// Whens is populated only for ScalarCase. Left is the simple-CASE selector
	// and is nil for searched CASE; Else is nil when ELSE was omitted. A WHEN
	// owns exactly one of Predicate (searched) and Match (simple).
	Whens []ScalarWhen
	Else  *ScalarExpr
}

// ScalarWhen is one ordered CASE arm. Predicate retains SQL three-valued
// boolean structure for searched CASE; Match is a scalar compared with the
// selector for simple CASE. Result is evaluated only when that arm is chosen.
type ScalarWhen struct {
	Predicate *Expr
	Match     *ScalarExpr
	Result    *ScalarExpr
	Pos       int
}
