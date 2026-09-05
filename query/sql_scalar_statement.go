package query

import (
	"errors"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/pginput"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

var ErrScalarType = errors.New("query: scalar expression type mismatch")

var ErrScalarResultShape = errors.New("query: malformed scalar dependency result")

// ScalarResultShapeError protects the scalar boundary from a buggy internal
// runner publishing RowCount independently of its dense dependency columns.
// It is an internal-contract error, not a license to truncate rows.
type ScalarResultShapeError struct {
	Dependency int
	Rows       int
	Cells      int
}

func (e *ScalarResultShapeError) Error() string {
	return fmt.Sprintf(
		"query: scalar dependency %d has %d cells for RowCount %d: %v",
		e.Dependency, e.Cells, e.Rows, ErrScalarResultShape,
	)
}
func (e *ScalarResultShapeError) Unwrap() error { return ErrScalarResultShape }

// ScalarTypeError is a positioned runtime type failure. JSON containers never
// participate in SQL arithmetic implicitly, and concatenation accepts decoded
// strings only; callers can therefore distinguish bad data from bad syntax.
type ScalarTypeError struct {
	Pos       int
	Operation string
	Left      ValueType
	Right     ValueType
}

func (e *ScalarTypeError) Error() string {
	if e.Right == TypeAny {
		return fmt.Sprintf("query: scalar %s at byte %d does not accept %s: %v",
			e.Operation, e.Pos, scalarValueTypeName(e.Left), ErrScalarType)
	}
	return fmt.Sprintf("query: scalar %s at byte %d does not accept %s and %s: %v",
		e.Operation, e.Pos, scalarValueTypeName(e.Left), scalarValueTypeName(e.Right), ErrScalarType)
}
func (e *ScalarTypeError) Unwrap() error { return ErrScalarType }
func (e *ScalarTypeError) Position() int { return e.Pos }

type statementScalarNodeKind uint8

const (
	statementScalarDependency statementScalarNodeKind = iota
	statementScalarLiteral
	statementScalarNull
	statementScalarUnary
	statementScalarBinary
	statementScalarCast
	statementScalarCaseNode
	statementScalarConditionalNode
	statementScalarBooleanNode
)

type statementScalarNode struct {
	kind             statementScalarNodeKind
	op               sqlast.ScalarOp
	cast             sqlast.ScalarCastTarget
	left             int32
	right            int32
	dependency       int32
	caseIndex        int32
	conditionalIndex int32
	skip             int32
	operand          sqlast.Operand
	pos              int
	bound            scalar
	known            bool
	representation   OutputRepresentation
}

type statementScalarDependencySpec struct {
	path *sqlast.PathExpr
	agg  sqlast.AggKind
	spec string
}

type statementScalarPredicate struct {
	kind        sqlast.ExprKind
	op          sqlast.CmpOp
	left        int32
	right       int32
	kids        []int32
	negated     bool
	pathCompare bool
	pos         int
	start       int32
	end         int32
	leftDom     scalarCaseDomain
	rightDom    scalarCaseDomain
}

type statementScalarValue struct {
	value  scalar
	cell   Cell
	direct bool
	// exact retains a newly parsed JSON cast's authored representation. Unlike
	// direct, its cell borrows evalArena and must be copied at publication.
	exact bool
}

type statementScalarOrder struct {
	start int32
	end   int32
	root  int32
	desc  bool
	nulls sqlast.WindowNullOrder
}

type statementScalarOrderRow struct {
	input   int
	keyBase int
}

type statementScalarOrdered struct {
	projectionEnd int
	havingEnd     int
	order         []statementScalarOrder
	having        *havingProgram
	rows          []statementScalarOrderRow
	scratch       []statementScalarOrderRow
	values        []scalar
	arena         []byte
	cells         []Cell
}

// statementScalar is the cold prepared sidecar. It owns both the postorder
// program and its single-consumer warmed storage; ordinary Statements retain
// only the pre-existing nil nested pointer and never execute this code.
type statementScalar struct {
	deps           []statementScalarDependencySpec
	semanticDeps   []statementScalarDependencySpec
	nodes          []statementScalarNode
	predicates     []statementScalarPredicate
	predRoots      []int32
	cases          []statementScalarCase
	caseArms       []statementScalarCaseArm
	conditionals   []statementScalarConditional
	predicateNodes int
	outputs        []int32
	types          []ValueType
	ordered        *statementScalarOrdered

	values             []statementScalarValue
	outputValues       []statementScalarValue
	evalArena          []byte
	resultArena        []byte
	decimal            sqlScalarDecimal
	cardinality        bool
	groupedCardinality bool
	hasAggregate       bool
}

func (s *Statement) scalarStatement() *statementScalar {
	if s == nil || s.nested == nil {
		return nil
	}
	return s.nested.scalar
}

func selectHasScalar(tree *sqlast.SelectStmt) bool {
	if tree == nil {
		return false
	}
	for i := range tree.Columns {
		if tree.Columns[i].Scalar != nil {
			return true
		}
	}
	return exprHasScalar(tree.Where) || exprHasScalar(tree.Having)
}

func selectNeedsPostScalarOrder(tree *sqlast.SelectStmt) bool {
	if tree == nil || selectHasWindows(tree) {
		return false
	}
	if exprHasScalar(tree.Having) {
		return true
	}
	for i := range tree.OrderBy {
		if tree.OrderBy[i].Scalar != nil {
			return true
		}
		output := tree.OrderBy[i].Output - 1
		if output < 0 || output >= len(tree.Columns) {
			continue
		}
		column := &tree.Columns[output]
		if column.Scalar != nil || column.Agg != sqlast.AggNone {
			return true
		}
	}
	return false
}

func exprHasScalar(expr *sqlast.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == sqlast.ExprScalarCompare || expr.Kind == sqlast.ExprScalarIsNull ||
		expr.Kind == sqlast.ExprScalarTruth {
		return true
	}
	for _, kid := range expr.Kids {
		if exprHasScalar(kid) {
			return true
		}
	}
	return false
}

func (s *Statement) prepareScalar(preserveUnknownOutput bool) error {
	if s.window() != nil {
		// The window input already evaluates the authored scalar projections,
		// predicates and hidden sort keys. The final window stage addresses
		// those materialized columns by ordinal.
		return nil
	}
	if !selectHasScalar(s.tree) && !selectNeedsPostScalarOrder(s.tree) {
		return nil
	}
	if !preserveUnknownOutput {
		if err := s.finalizeScalarOutputParameterTypes(); err != nil {
			return err
		}
	}
	postOrder := selectNeedsPostScalarOrder(s.tree)
	hasScalar := selectHasScalar(s.tree)
	if s.tree.Distinct && hasScalar {
		return sqlast.NewFeatureNotSupportedError(s.text, firstScalarStatementPos(s.tree),
			"SELECT DISTINCT over computed scalar expressions requires distinctness after scalar evaluation")
	}
	if s.tree.Having != nil && !postOrder {
		pos := firstScalarStatementPos(s.tree)
		if !hasScalar {
			for i := range s.tree.OrderBy {
				if s.tree.OrderBy[i].Output != 0 {
					pos = s.tree.OrderBy[i].Pos
					break
				}
			}
		}
		return sqlast.NewFeatureNotSupportedError(s.text, pos,
			"HAVING must filter every group before a post-output stable ORDER BY stage can apply LIMIT/OFFSET")
	}
	if exprHasScalar(s.tree.Where) && (len(s.tree.GroupBy) != 0 || selectHasAggregate(s.tree)) {
		return sqlast.NewFeatureNotSupportedError(s.text, firstScalarExprPos(s.tree.Where),
			"computed scalar WHERE expressions must run before grouping and cannot yet share a grouped statement")
	}
	for i := range s.tree.Columns {
		column := &s.tree.Columns[i]
		if column.Scalar == nil && column.Path != nil && len(column.Path.Segments) == 0 &&
			(s.hasRelationBinding() || s.relationJoin() != nil) {
			return sqlast.NewFeatureNotSupportedError(s.text, column.Pos,
				"a relation wildcard cannot be mixed with the cold scalar output stage yet; name its columns explicitly")
		}
	}

	runtime := new(statementScalar)
	if err := runtime.compileWhere(s, s.tree.Where); err != nil {
		return err
	}
	runtime.predicateNodes = len(runtime.nodes)
	var outputStarts []int32
	if postOrder {
		runtime.ordered = new(statementScalarOrdered)
		if s.tree.Having != nil && !exprHasScalar(s.tree.Having) {
			runtime.ordered.having = new(havingProgram)
		}
		outputStarts = make([]int32, 0, len(s.tree.Columns))
	}
	for i := range s.tree.Columns {
		column := &s.tree.Columns[i]
		if postOrder {
			outputStarts = append(outputStarts, int32(len(runtime.nodes)))
		}
		var root int32
		var err error
		if column.Scalar != nil {
			root, err = runtime.compileExpr(s, column.Scalar)
		} else {
			root, err = runtime.compileDependency(s, column.Path, column.Agg, column.Pos)
		}
		if err != nil {
			return err
		}
		if !preserveUnknownOutput {
			runtime.finalizeUnknownScalarOutput(root, column.Scalar, s)
		}
		runtime.outputs = append(runtime.outputs, root)
		runtime.types = append(runtime.types, runtime.nodeType(root))
	}
	if postOrder {
		runtime.ordered.projectionEnd = len(runtime.nodes)
		runtime.ordered.havingEnd = runtime.ordered.projectionEnd
		if exprHasScalar(s.tree.Having) {
			if !exprEntirelyScalar(s.tree.Having) {
				return sqlast.NewFeatureNotSupportedError(
					s.text, firstScalarExprPos(s.tree.Having),
					"a computed HAVING predicate may combine only computed scalar boolean terms",
				)
			}
			runtime.predRoots = append(runtime.predRoots, -1)
			root, err := runtime.compilePredicate(s, s.tree.Having)
			if err != nil {
				return err
			}
			runtime.predRoots = append(runtime.predRoots, root)
			runtime.ordered.havingEnd = len(runtime.nodes)
		}
		for i := range s.tree.OrderBy {
			term := &s.tree.OrderBy[i]
			var start, end, root int32
			if term.Scalar != nil {
				start = int32(len(runtime.nodes))
				var err error
				root, err = runtime.compileExpr(s, term.Scalar)
				if err != nil {
					return err
				}
				end = int32(len(runtime.nodes))
			} else if term.Output != 0 {
				output := term.Output - 1
				if output < 0 || output >= len(runtime.outputs) {
					return fmt.Errorf("query: malformed ORDER BY output %d", term.Output)
				}
				start, root = outputStarts[output], runtime.outputs[output]
				end = int32(runtime.ordered.projectionEnd)
				if output+1 < len(outputStarts) {
					end = outputStarts[output+1]
				}
			} else {
				if term.Path == nil {
					return fmt.Errorf("query: malformed scalar ORDER BY path")
				}
				start = int32(len(runtime.nodes))
				var err error
				root, err = runtime.compileDependency(s, term.Path, sqlast.AggNone, term.Pos)
				if err != nil {
					return err
				}
				end = root + 1
			}
			runtime.ordered.order = append(runtime.ordered.order, statementScalarOrder{
				start: start, end: end, root: root, desc: term.Desc, nulls: term.Nulls,
			})
		}
	}
	if len(runtime.deps) == 0 && !runtime.hasSemanticAggregate() {
		// One hidden payload-free projection requests source cardinality. The
		// query planner materializes it for an ungrouped scan and treats it as a
		// metadata-only marker under GROUP BY, whose RowCount already is exact.
		runtime.deps = append(runtime.deps, statementScalarDependencySpec{})
		runtime.cardinality = true
	}
	runtime.groupedCardinality = runtime.cardinality && len(s.tree.GroupBy) != 0
	s.ensureNested().scalar = runtime
	return nil
}

// finalizeScalarOutputParameterTypes applies PostgreSQL's query-boundary rule
// for a bare unknown output. Set operands opt out until their enclosing common-
// type pass runs; an ordinary SELECT resolves the parameter to text here so a
// later occurrence cannot silently acquire an incompatible domain.
func (s *Statement) finalizeScalarOutputParameterTypes() error {
	if s == nil || s.tree == nil {
		return nil
	}
	for i := range s.tree.Columns {
		expr := s.tree.Columns[i].Scalar
		if expr == nil || expr.Kind != sqlast.ScalarLiteral ||
			expr.Value.Kind != sqlast.OperandParam ||
			s.ParameterType(expr.Value.Ordinal) != ParameterTypeUnspecified {
			continue
		}
		if err := s.mergeParameterType(
			s.paramBase+expr.Value.Ordinal, ParameterTypeText, expr.Value.Pos,
		); err != nil {
			return err
		}
		s.markParameterTypeTargetDefault(s.paramBase + expr.Value.Ordinal)
	}
	return nil
}

func (r *statementScalar) finalizeUnknownScalarOutput(
	root int32,
	expr *sqlast.ScalarExpr,
	s *Statement,
) {
	if r == nil || expr == nil || root < 0 || int(root) >= len(r.nodes) {
		return
	}
	node := &r.nodes[root]
	switch expr.Kind {
	case sqlast.ScalarNull:
		node.representation = OutputSQLText
	case sqlast.ScalarLiteral:
		switch expr.Value.Kind {
		case sqlast.OperandString:
			node.representation = OutputSQLText
		case sqlast.OperandParam:
			if s != nil {
				node.representation = parameterTypeOutputRepresentation(
					s.ParameterType(expr.Value.Ordinal),
				)
			}
		}
	}
}

func parameterTypeOutputRepresentation(paramType ParameterType) OutputRepresentation {
	switch paramType {
	case ParameterTypeBool:
		return OutputSQLBool
	case ParameterTypeText:
		return OutputSQLText
	case ParameterTypeVarchar:
		return OutputSQLVarchar
	case ParameterTypeName:
		return OutputSQLName
	case ParameterTypeBPChar:
		return OutputSQLBPChar
	default:
		return OutputJSON
	}
}

func outputRepresentationValueType(
	representation OutputRepresentation,
) (ValueType, bool) {
	switch representation {
	case OutputSQLBool:
		return TypeBool, true
	case OutputSQLText, OutputSQLVarchar, OutputSQLName, OutputSQLBPChar:
		return TypeString, true
	case OutputSQLNumber:
		return TypeNumber, true
	default:
		return TypeAny, false
	}
}

func selectHasAggregate(tree *sqlast.SelectStmt) bool {
	for i := range tree.Columns {
		if tree.Columns[i].Agg != sqlast.AggNone || scalarHasAggregate(tree.Columns[i].Scalar) {
			return true
		}
	}
	for i := range tree.OrderBy {
		if scalarHasAggregate(tree.OrderBy[i].Scalar) {
			return true
		}
	}
	return false
}

func scalarHasAggregate(expr *sqlast.ScalarExpr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == sqlast.ScalarAggregate || scalarHasAggregate(expr.Left) ||
		scalarHasAggregate(expr.Right) || scalarHasAggregate(expr.Else) {
		return true
	}
	for i := range expr.Whens {
		if scalarHasAggregate(expr.Whens[i].Match) ||
			scalarHasAggregate(expr.Whens[i].Result) ||
			exprPredicateHasAggregate(expr.Whens[i].Predicate) {
			return true
		}
	}
	return false
}

func exprPredicateHasAggregate(expr *sqlast.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Agg != sqlast.AggNone || scalarHasAggregate(expr.ScalarLeft) ||
		scalarHasAggregate(expr.ScalarRight) {
		return true
	}
	for _, kid := range expr.Kids {
		if exprPredicateHasAggregate(kid) {
			return true
		}
	}
	return false
}

func firstScalarStatementPos(tree *sqlast.SelectStmt) int {
	for i := range tree.Columns {
		if tree.Columns[i].Scalar != nil {
			return tree.Columns[i].Scalar.Pos
		}
	}
	if exprHasScalar(tree.Where) {
		return firstScalarExprPos(tree.Where)
	}
	for _, term := range tree.OrderBy {
		if term.Scalar != nil {
			return term.Pos
		}
	}
	return firstScalarExprPos(tree.Having)
}

func firstScalarExprPos(expr *sqlast.Expr) int {
	if expr == nil {
		return 0
	}
	if expr.Kind == sqlast.ExprScalarCompare || expr.Kind == sqlast.ExprScalarIsNull {
		return expr.Pos
	}
	for _, kid := range expr.Kids {
		if exprHasScalar(kid) {
			return firstScalarExprPos(kid)
		}
	}
	return expr.Pos
}

func (r *statementScalar) compileExpr(s *Statement, expr *sqlast.ScalarExpr) (int32, error) {
	if expr == nil {
		return 0, fmt.Errorf("query: nil scalar expression")
	}
	switch expr.Kind {
	case sqlast.ScalarPath:
		return r.compileDependency(s, expr.Path, sqlast.AggNone, expr.Pos)
	case sqlast.ScalarAggregate:
		return r.compileDependency(s, expr.Path, expr.Agg, expr.Pos)
	case sqlast.ScalarLiteral:
		representation := OutputJSON
		if expr.Value.Kind == sqlast.OperandParam && s != nil {
			representation = parameterTypeOutputRepresentation(
				s.ParameterType(expr.Value.Ordinal),
			)
		}
		r.nodes = append(r.nodes, statementScalarNode{
			kind: statementScalarLiteral, operand: expr.Value, pos: expr.Pos,
			representation: representation,
		})
	case sqlast.ScalarNull:
		r.nodes = append(r.nodes, statementScalarNode{kind: statementScalarNull, pos: expr.Pos})
	case sqlast.ScalarUnary:
		if expr.Op != sqlast.ScalarPositive && expr.Op != sqlast.ScalarNegative {
			return 0, fmt.Errorf("query: invalid scalar unary operation %d", expr.Op)
		}
		left, err := r.compileExpr(s, expr.Left)
		if err != nil {
			return 0, err
		}
		r.nodes = append(r.nodes, statementScalarNode{
			kind: statementScalarUnary, op: expr.Op, left: left, right: -1, pos: expr.Pos,
		})
	case sqlast.ScalarBinary:
		if expr.Op.Conditional() {
			return r.compileConditional(s, expr)
		}
		if expr.Op > sqlast.ScalarConcat {
			return 0, fmt.Errorf("query: invalid scalar binary operation %d", expr.Op)
		}
		left, err := r.compileExpr(s, expr.Left)
		if err != nil {
			return 0, err
		}
		right, err := r.compileExpr(s, expr.Right)
		if err != nil {
			return 0, err
		}
		r.nodes = append(r.nodes, statementScalarNode{
			kind: statementScalarBinary, op: expr.Op, left: left, right: right, pos: expr.Pos,
		})
	case sqlast.ScalarCast:
		if expr.Cast > sqlast.ScalarCastJSON {
			return 0, fmt.Errorf("query: invalid scalar CAST target %d", expr.Cast)
		}
		if operand, typed, ok, err := foldTextBooleanConstant(expr); err != nil {
			return 0, err
		} else if typed && ok {
			representation := OutputSQLText
			if expr.Cast == sqlast.ScalarCastBoolean {
				representation = OutputSQLBool
			}
			r.nodes = append(r.nodes, statementScalarNode{
				kind: statementScalarLiteral, cast: expr.Cast,
				operand: operand, pos: expr.Pos, representation: representation,
			})
			break
		}
		left, err := r.compileExpr(s, expr.Left)
		if err != nil {
			return 0, err
		}
		r.nodes = append(r.nodes, statementScalarNode{
			kind: statementScalarCast, cast: expr.Cast, left: left, right: -1, pos: expr.Pos,
		})
	case sqlast.ScalarCase:
		return r.compileCase(s, expr)
	default:
		return 0, fmt.Errorf("query: invalid scalar expression kind %d", expr.Kind)
	}
	return int32(len(r.nodes) - 1), nil
}

// foldTextBooleanConstant performs PostgreSQL's analysis-time typinput for a
// source-independent BOOL/TEXT cast chain. The resulting program contains one
// literal node and no row-time cast. Parameters and data dependencies remain
// ordinary cast programs because their values are not known at prepare.
func foldTextBooleanConstant(expr *sqlast.ScalarExpr) (sqlast.Operand, bool, bool, error) {
	if expr == nil {
		return sqlast.Operand{}, false, false, nil
	}
	if expr.Kind == sqlast.ScalarLiteral {
		if expr.Value.Kind == sqlast.OperandParam {
			return sqlast.Operand{}, false, false, nil
		}
		return expr.Value, false, true, nil
	}
	if expr.Kind != sqlast.ScalarCast ||
		(expr.Cast != sqlast.ScalarCastText && expr.Cast != sqlast.ScalarCastBoolean) {
		return sqlast.Operand{}, false, false, nil
	}
	operand, typed, ok, err := foldTextBooleanConstant(expr.Left)
	if err != nil || !ok {
		return sqlast.Operand{}, typed, ok, err
	}
	typed = typed || expr.TypedConstant
	// Ordinary CAST remains lazy: a dead CASE arm or a projection eliminated
	// by WHERE/OFFSET must not start failing at prepare. PostgreSQL typed-string
	// constants are the explicit analysis-time exception represented here.
	if !typed {
		return operand, false, true, nil
	}
	if expr.Cast == sqlast.ScalarCastText {
		switch operand.Kind {
		case sqlast.OperandString:
			return operand, true, true, nil
		case sqlast.OperandBool:
			text := "false"
			if operand.Bool {
				text = "true"
			}
			return sqlast.Operand{Kind: sqlast.OperandString, Text: text, Pos: expr.Pos}, true, true, nil
		case sqlast.OperandNumber:
			return sqlast.Operand{Kind: sqlast.OperandString, Text: operand.Text, Pos: expr.Pos}, true, true, nil
		default:
			return sqlast.Operand{}, true, false, nil
		}
	}
	switch operand.Kind {
	case sqlast.OperandBool:
		return operand, true, true, nil
	case sqlast.OperandString:
		value, valid := pginput.Boolean(operand.Text)
		if !valid {
			// This is an ordinary outer cast over a valid typed TEXT constant,
			// not BOOL 'value' typinput. Keep it in the lazy runtime program so
			// an unreachable CASE arm or eliminated projection cannot fail at
			// prepare. A direct invalid BOOL typed constant was already rejected
			// by the parser before compilation.
			return sqlast.Operand{}, true, false, nil
		}
		return sqlast.Operand{Kind: sqlast.OperandBool, Bool: value, Pos: expr.Pos}, true, true, nil
	default:
		return sqlast.Operand{}, true, false, nil
	}
}

func (r *statementScalar) compileDependency(
	s *Statement,
	path *sqlast.PathExpr,
	agg sqlast.AggKind,
	pos int,
) (int32, error) {
	spec := s.spec(path)
	for i := range r.deps {
		dep := &r.deps[i]
		if dep.agg == agg && dep.spec == spec {
			r.nodes = append(r.nodes, statementScalarNode{
				kind: statementScalarDependency, dependency: int32(i), right: -1, pos: pos,
			})
			return int32(len(r.nodes) - 1), nil
		}
	}
	r.deps = append(r.deps, statementScalarDependencySpec{path: path, agg: agg, spec: spec})
	if agg != sqlast.AggNone {
		r.hasAggregate = true
	}
	r.nodes = append(r.nodes, statementScalarNode{
		kind: statementScalarDependency, dependency: int32(len(r.deps) - 1), right: -1, pos: pos,
	})
	return int32(len(r.nodes) - 1), nil
}

func (r *statementScalar) compileWhere(s *Statement, expr *sqlast.Expr) error {
	if expr == nil || !exprHasScalar(expr) {
		return nil
	}
	terms := []*sqlast.Expr{expr}
	if expr.Kind == sqlast.ExprAnd {
		terms = expr.Kids
	}
	for _, term := range terms {
		if !exprHasScalar(term) {
			continue
		}
		var root int32
		var err error
		if exprEntirelyScalar(term) {
			root, err = r.compilePredicate(s, term)
		} else {
			// WHERE retains exactly TRUE. The shared searched-CASE evaluator
			// preserves NULL, short-circuiting, and runtime operand checks across
			// mixed path/scalar boolean trees before this final truth selection.
			value := &sqlast.ScalarExpr{Kind: sqlast.ScalarCase, Pos: term.Pos,
				Whens: []sqlast.ScalarWhen{{Predicate: term, Result: &sqlast.ScalarExpr{Kind: sqlast.ScalarLiteral, Value: sqlast.Operand{Kind: sqlast.OperandBool, Bool: true}}}},
				Else:  &sqlast.ScalarExpr{Kind: sqlast.ScalarLiteral, Value: sqlast.Operand{Kind: sqlast.OperandBool}},
			}
			root, err = r.compilePredicate(s, &sqlast.Expr{Kind: sqlast.ExprScalarTruth, ScalarLeft: value, Pos: term.Pos})
		}
		if err != nil {
			return err
		}
		r.predRoots = append(r.predRoots, root)
	}
	return nil
}

func exprEntirelyScalar(expr *sqlast.Expr) bool {
	if expr == nil {
		return false
	}
	switch expr.Kind {
	case sqlast.ExprScalarCompare, sqlast.ExprScalarIsNull, sqlast.ExprScalarTruth:
		return true
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		for _, kid := range expr.Kids {
			if !exprEntirelyScalar(kid) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (r *statementScalar) compilePredicate(s *Statement, expr *sqlast.Expr) (int32, error) {
	node := statementScalarPredicate{kind: expr.Kind, op: expr.Op, left: -1, right: -1, negated: expr.Negated}
	switch expr.Kind {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		for _, kid := range expr.Kids {
			index, err := r.compilePredicate(s, kid)
			if err != nil {
				return 0, err
			}
			node.kids = append(node.kids, index)
		}
	case sqlast.ExprScalarCompare:
		left, err := r.compileExpr(s, expr.ScalarLeft)
		if err != nil {
			return 0, err
		}
		right, err := r.compileExpr(s, expr.ScalarRight)
		if err != nil {
			return 0, err
		}
		node.left, node.right = left, right
	case sqlast.ExprScalarIsNull:
		left, err := r.compileExpr(s, expr.ScalarLeft)
		if err != nil {
			return 0, err
		}
		node.left = left
	case sqlast.ExprScalarTruth:
		left, err := r.compileExpr(s, expr.ScalarLeft)
		if err != nil {
			return 0, err
		}
		domain := r.nodeDomain(left)
		if domain != caseDomainDynamic && domain != caseDomainNull && domain != caseDomainBoolean {
			return 0, &ScalarTypeError{Pos: expr.Pos, Operation: "Boolean predicate", Left: r.nodeType(left), Right: TypeBool}
		}
		r.nodes = append(r.nodes, statementScalarNode{kind: statementScalarBooleanNode, left: left, pos: expr.Pos})
		node.left = int32(len(r.nodes) - 1)
	default:
		return 0, fmt.Errorf("query: invalid scalar predicate kind %d", expr.Kind)
	}
	r.predicates = append(r.predicates, node)
	return int32(len(r.predicates) - 1), nil
}

func (r *statementScalar) ownsWhere(expr *sqlast.Expr) bool {
	return exprHasScalar(expr)
}

func (r *statementScalar) buildColumns(s *Statement, args []any) error {
	for i := range r.nodes {
		node := &r.nodes[i]
		if node.kind != statementScalarLiteral {
			continue
		}
		value, known, err := s.operand(node.operand, args)
		if err != nil {
			return err
		}
		node.known = known
		if !known {
			node.bound = scalar{kind: kindNull}
			continue
		}
		literal, err := s.c.makeLiteral(value)
		if err != nil {
			return err
		}
		node.bound = classifyLiteral(literal)
	}
	for i := range r.deps {
		dep := &r.deps[i]
		if r.cardinality && i == 0 {
			s.q.columns = append(s.q.columns, Column{
				header: "__scalar_cardinality", cardinalityOnly: true,
			})
			continue
		}
		header := dep.spec
		if dep.agg != sqlast.AggNone {
			header = s.header(aggName(dep.agg), dep.spec)
		}
		s.q.columns = append(s.q.columns, Column{
			agg: aggKind(dep.agg), spec: dep.spec, header: header,
		})
	}
	semanticAggregate := false
	for i := range r.semanticDeps {
		dep := &r.semanticDeps[i]
		if dep.agg != sqlast.AggNone {
			semanticAggregate = true
			continue
		}
		if r.hasLiveDependency(dep) {
			continue
		}
		s.q.columns = append(s.q.columns, Column{
			spec: dep.spec, header: dep.spec, semanticOnly: true,
		})
	}
	if semanticAggregate && !r.hasAggregate {
		// COUNT(*) supplies aggregate/group cardinality without reading or
		// reducing a statically unreachable aggregate argument.
		s.q.columns = append(s.q.columns, Column{
			agg: aggCount, header: "__scalar_aggregate", semanticOnly: true,
		})
	}
	return nil
}

func (r *statementScalar) hasSemanticAggregate() bool {
	for i := range r.semanticDeps {
		if r.semanticDeps[i].agg != sqlast.AggNone {
			return true
		}
	}
	return false
}

func (r *statementScalar) hasLiveDependency(candidate *statementScalarDependencySpec) bool {
	for i := range r.deps {
		dep := &r.deps[i]
		if dep.agg == candidate.agg && dep.spec == candidate.spec {
			return true
		}
	}
	return false
}

func (r *statementScalar) nodeType(root int32) ValueType {
	if root < 0 || int(root) >= len(r.nodes) {
		return TypeAny
	}
	node := &r.nodes[root]
	switch node.kind {
	case statementScalarDependency:
		if r.deps[node.dependency].agg != sqlast.AggNone {
			return TypeNumber
		}
		return TypeAny
	case statementScalarLiteral:
		if valueType, ok := outputRepresentationValueType(node.representation); ok {
			return valueType
		}
		switch node.operand.Kind {
		case sqlast.OperandString:
			return TypeString
		case sqlast.OperandNumber:
			return TypeNumber
		case sqlast.OperandBool:
			return TypeBool
		default:
			return TypeAny
		}
	case statementScalarNull:
		return TypeNull
	case statementScalarBooleanNode:
		return TypeBool
	case statementScalarUnary:
		return TypeNumber
	case statementScalarBinary:
		if node.op == sqlast.ScalarConcat {
			return TypeString
		}
		return TypeNumber
	case statementScalarCast:
		switch node.cast {
		case sqlast.ScalarCastText:
			return TypeString
		case sqlast.ScalarCastBoolean:
			return TypeBool
		case sqlast.ScalarCastNumeric:
			return TypeNumber
		default:
			return TypeAny
		}
	case statementScalarCaseNode:
		return r.cases[node.caseIndex].domain.schemaType()
	case statementScalarConditionalNode:
		if node.op.NullSafeComparison() {
			return TypeBool
		}
		return r.conditionals[node.conditionalIndex].domain.schemaType()
	default:
		return TypeAny
	}
}

func (r *statementScalar) appendSchema(dst []OutputColumn, names []string) []OutputColumn {
	for i := range r.outputs {
		dst = append(dst, OutputColumn{
			Header: names[i], Ordinal: uint32(i), Type: r.types[i],
			Representation: r.nodeRepresentation(r.outputs[i]),
		})
	}
	return dst
}

func (r *statementScalar) nodeRepresentation(root int32) OutputRepresentation {
	if root < 0 || int(root) >= len(r.nodes) {
		return OutputJSON
	}
	node := &r.nodes[root]
	switch node.kind {
	case statementScalarLiteral:
		return node.representation
	case statementScalarUnary:
		return OutputSQLNumber
	case statementScalarBinary:
		if node.op == sqlast.ScalarConcat {
			return OutputSQLText
		}
		return OutputSQLNumber
	case statementScalarCast:
		switch node.cast {
		case sqlast.ScalarCastText:
			return OutputSQLText
		case sqlast.ScalarCastBoolean:
			return OutputSQLBool
		case sqlast.ScalarCastNumeric:
			return OutputSQLNumber
		default:
			return OutputJSON
		}
	case statementScalarCaseNode:
		return r.cases[node.caseIndex].domain.representation()
	case statementScalarConditionalNode:
		if node.op.NullSafeComparison() {
			return OutputSQLBool
		}
		return r.conditionals[node.conditionalIndex].domain.representation()
	default:
		return OutputJSON
	}
}

func (r *statementScalar) evalNodes(
	result *Result,
	row, start, end int,
	arena *[]byte,
	budget *aggregateBudget,
	intermediate *intermediateBudget,
	intermediateCharge *int64,
	cancel *CancelFlag,
) error {
	r.values = resize(r.values, len(r.nodes))
	for i := start; i < end; i++ {
		// One source row may carry the parser's full bounded scalar program
		// (many output expressions, each with nested arithmetic). Row-level
		// checkpoints alone would make cancellation wait for that whole program.
		// The nil flag remains one predictable branch and the armed atomic load is
		// amortized across cancellationCheckMask+1 nodes.
		if err := cancellationCheckpoint(cancel, i-start); err != nil {
			return err
		}
		write := i
		node := &r.nodes[i]
		var value statementScalarValue
		switch node.kind {
		case statementScalarDependency:
			cell := result.Columns[node.dependency].Cells[row]
			value = statementScalarValue{cell: cell, direct: true}
			value.value = scalarFromResultCell(cell, arena)
		case statementScalarLiteral:
			value.value = node.bound
		case statementScalarNull:
			value.value = scalar{kind: kindNull}
		case statementScalarUnary:
			left := r.values[node.left]
			if left.value.kind == kindNull {
				value.value = scalar{kind: kindNull}
				break
			}
			if left.value.kind != kindNumber {
				return &ScalarTypeError{Pos: node.pos, Operation: scalarOperationName(node.op), Left: valueTypeOfScalar(left.value), Right: TypeAny}
			}
			if node.op == sqlast.ScalarPositive {
				value = left
				break
			}
			start := len(*arena)
			var err error
			*arena, _, err = r.decimal.binary(sqlScalarSubtract, zeroNumberBytes, left.value.num, node.pos, *arena, budget)
			if err != nil {
				return err
			}
			value.value = classifyComputedNumber((*arena)[start:])
		case statementScalarBinary:
			left, right := r.values[node.left], r.values[node.right]
			if left.value.kind == kindNull || right.value.kind == kindNull {
				value.value = scalar{kind: kindNull}
				break
			}
			if node.op == sqlast.ScalarConcat {
				if left.value.kind != kindString || right.value.kind != kindString {
					return &ScalarTypeError{Pos: node.pos, Operation: "concatenation", Left: valueTypeOfScalar(left.value), Right: valueTypeOfScalar(right.value)}
				}
				start := len(*arena)
				*arena = append(*arena, left.value.sval...)
				*arena = append(*arena, right.value.sval...)
				value.value = scalar{kind: kindString, sval: byteview.String((*arena)[start:])}
				break
			}
			if left.value.kind != kindNumber || right.value.kind != kindNumber {
				return &ScalarTypeError{Pos: node.pos, Operation: scalarOperationName(node.op), Left: valueTypeOfScalar(left.value), Right: valueTypeOfScalar(right.value)}
			}
			op, ok := decimalOperation(node.op)
			if !ok {
				return fmt.Errorf("query: invalid scalar operation %d", node.op)
			}
			start := len(*arena)
			var err error
			*arena, _, err = r.decimal.binary(op, left.value.num, right.value.num, node.pos, *arena, budget)
			if err != nil {
				return err
			}
			value.value = classifyComputedNumber((*arena)[start:])
		case statementScalarCast:
			var err error
			value, err = r.evalCast(
				node, r.values[node.left], arena, budget,
				intermediate, intermediateCharge,
			)
			if err != nil {
				return err
			}
		case statementScalarBooleanNode:
			value = r.values[node.left]
			if value.value.kind != kindNull && value.value.kind != kindBool {
				return &ScalarTypeError{Pos: node.pos, Operation: "Boolean predicate", Left: valueTypeOfScalar(value.value), Right: TypeBool}
			}
		case statementScalarConditionalNode:
			var err error
			value, err = r.evalConditional(result, row, node, arena, budget,
				intermediate, intermediateCharge, cancel)
			if err != nil {
				return err
			}
			if node.skip <= int32(i) || int(node.skip) > end {
				return fmt.Errorf("query: malformed scalar conditional program range")
			}
			i = int(node.skip) - 1
		case statementScalarCaseNode:
			var err error
			value, err = r.evalCase(
				result, row, node, arena, budget,
				intermediate, intermediateCharge, cancel,
			)
			if err != nil {
				return err
			}
			if node.skip <= int32(i) || int(node.skip) > end {
				return fmt.Errorf("query: malformed scalar CASE program range")
			}
			i = int(node.skip) - 1
		}
		r.values[write] = value
	}
	return nil
}

func (r *statementScalar) validateResult(result *Result) error {
	if result == nil {
		return &ScalarResultShapeError{Dependency: 0, Rows: 0, Cells: -1}
	}
	for dependency := 0; dependency < r.resultDependencyColumns(); dependency++ {
		if dependency >= len(result.Columns) {
			return &ScalarResultShapeError{
				Dependency: dependency, Rows: result.RowCount, Cells: -1,
			}
		}
		if cells := len(result.Columns[dependency].Cells); cells != result.RowCount {
			return &ScalarResultShapeError{
				Dependency: dependency, Rows: result.RowCount, Cells: cells,
			}
		}
	}
	return nil
}

func (r *statementScalar) resultDependencyColumns() int {
	if r.groupedCardinality {
		return 0
	}
	return len(r.deps)
}

// clearValues severs every borrowed source/result view retained by the lazy
// scalar program while preserving the warmed slices themselves. CASE leaves
// unselected nodes untouched, so clearing only the nodes evaluated by the last
// row would let an earlier branch pin a Segment, snapshot page, or arena.
func (r *statementScalar) clearValues() {
	clear(r.values)
	clear(r.outputValues)
	if r.ordered != nil {
		clear(r.ordered.values)
		clear(r.ordered.cells)
	}
}

var zeroNumberBytes = []byte("0")

func decimalOperation(op sqlast.ScalarOp) (sqlScalarArithmeticOp, bool) {
	switch op {
	case sqlast.ScalarAdd:
		return sqlScalarAdd, true
	case sqlast.ScalarSubtract:
		return sqlScalarSubtract, true
	case sqlast.ScalarMultiply:
		return sqlScalarMultiply, true
	case sqlast.ScalarDivide:
		return sqlScalarDivide, true
	case sqlast.ScalarModulo:
		return sqlScalarModulo, true
	default:
		return 0, false
	}
}

func scalarOperationName(op sqlast.ScalarOp) string {
	switch op {
	case sqlast.ScalarAdd:
		return "addition"
	case sqlast.ScalarSubtract:
		return "subtraction"
	case sqlast.ScalarMultiply:
		return "multiplication"
	case sqlast.ScalarDivide:
		return "division"
	case sqlast.ScalarModulo:
		return "modulo"
	case sqlast.ScalarConcat:
		return "concatenation"
	case sqlast.ScalarPositive:
		return "unary plus"
	default:
		return "unary minus"
	}
}

func scalarFromResultCell(cell Cell, scratch *[]byte) scalar {
	switch cell.kind {
	case TypeNull:
		return scalar{kind: kindNull}
	case TypeBool:
		return scalar{kind: kindBool, bval: cell.flag&cellTrue != 0, raw: cell.raw}
	case TypeNumber:
		value := scalar{kind: kindNumber, num: cell.raw, raw: cell.raw}
		if cell.flag&cellInteger != 0 {
			value.isInt, value.ival = true, int64(cell.word)
		}
		if value.num == nil {
			start := len(*scratch)
			*scratch = cell.AppendJSON(*scratch)
			value.num = (*scratch)[start:len(*scratch):len(*scratch)]
			value.raw = value.num
		}
		return value
	case TypeString:
		return scalar{kind: kindString, sval: cell.text, raw: cell.raw}
	default:
		return scalar{kind: kindContainer, raw: cell.raw}
	}
}

func classifyComputedNumber(raw []byte) scalar {
	value := scalar{kind: kindNumber, num: raw, raw: raw}
	plain := len(raw) != 0
	for index, digit := range raw {
		if digit == '-' && index == 0 {
			continue
		}
		if digit < '0' || digit > '9' {
			plain = false
			break
		}
	}
	if plain {
		integer, err := strconv.ParseInt(byteview.String(raw), 10, 64)
		if err != nil {
			return value
		}
		value.isInt, value.ival = true, integer
	}
	return value
}

func valueTypeOfScalar(value scalar) ValueType {
	switch value.kind {
	case kindNull:
		return TypeNull
	case kindBool:
		return TypeBool
	case kindNumber:
		return TypeNumber
	case kindString:
		return TypeString
	default:
		return TypeJSON
	}
}

func scalarValueTypeName(value ValueType) string {
	switch value {
	case TypeNull:
		return "NULL"
	case TypeBool:
		return "boolean"
	case TypeNumber:
		return "number"
	case TypeString:
		return "string"
	case TypeJSON:
		return "JSON container"
	default:
		return "dynamic value"
	}
}

func (r *statementScalar) evalPredicate(index int32) tri {
	node := &r.predicates[index]
	var out tri
	switch node.kind {
	case sqlast.ExprAnd:
		out = triTrue
		for _, kid := range node.kids {
			out = andTri(out, r.evalPredicate(kid))
			if out == triFalse {
				break
			}
		}
	case sqlast.ExprOr:
		out = triFalse
		for _, kid := range node.kids {
			value := r.evalPredicate(kid)
			if value == triTrue {
				out = triTrue
				break
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
	case sqlast.ExprNot:
		out = notTri(r.evalPredicate(node.kids[0]))
	case sqlast.ExprScalarIsNull:
		out = boolTri(r.values[node.left].value.kind == kindNull)
	case sqlast.ExprScalarTruth:
		value := r.values[node.left].value
		if value.kind == kindNull {
			out = triUnknown
		} else {
			out = boolTri(value.kind == kindBool && value.bval)
		}
	default:
		left, right := r.values[node.left].value, r.values[node.right].value
		if left.kind == kindNull || right.kind == kindNull {
			out = triUnknown
		} else {
			out = boolTri(acceptSign(compareScalar(left, right), Op(node.op)))
		}
	}
	if node.negated {
		return notTri(out)
	}
	return out
}

func (r *statementScalar) keep() bool {
	for _, root := range r.predRoots {
		if root < 0 {
			break
		}
		if r.evalPredicate(root) != triTrue {
			return false
		}
	}
	return true
}

func (r *statementScalar) keepHaving() bool {
	having := false
	for _, root := range r.predRoots {
		if root < 0 {
			having = true
			continue
		}
		if having && r.evalPredicate(root) != triTrue {
			return false
		}
	}
	return true
}

func (r *statementScalar) resultCell(value statementScalarValue, arena *[]byte) Cell {
	if value.direct {
		return value.cell
	}
	if value.exact {
		cell := value.cell
		if len(cell.text) != 0 {
			start := len(*arena)
			*arena = append(*arena, cell.text...)
			cell.text = byteview.String((*arena)[start:len(*arena):len(*arena)])
		}
		if len(cell.raw) != 0 && cell.kind != TypeNull && cell.kind != TypeBool {
			start := len(*arena)
			*arena = append(*arena, cell.raw...)
			cell.raw = (*arena)[start:len(*arena):len(*arena)]
		}
		return cell
	}
	switch value.value.kind {
	case kindNull:
		return nullCell()
	case kindBool:
		if value.value.bval {
			return Cell{kind: TypeBool, flag: cellTrue, raw: trueBytes}
		}
		return Cell{kind: TypeBool, raw: falseBytes}
	case kindNumber:
		start := len(*arena)
		*arena = append(*arena, value.value.num...)
		return Cell{kind: TypeNumber, flag: cellNumberRaw, raw: (*arena)[start:len(*arena):len(*arena)]}
	case kindString:
		textStart := len(*arena)
		*arena = append(*arena, value.value.sval...)
		rawStart := len(*arena)
		*arena = appendJSONString(*arena, value.value.sval)
		return Cell{
			kind: TypeString,
			text: byteview.String((*arena)[textStart:rawStart]),
			raw:  (*arena)[rawStart:len(*arena):len(*arena)],
		}
	default:
		return Cell{kind: TypeJSON, raw: value.value.raw}
	}
}

func (r *statementScalar) execute(
	s *Statement,
	exec *Exec,
	frame *statementFrame,
	options ExecOptions,
	intermediateResource string,
) (cursor Cursor, err error) {
	result := &exec.Result
	defer r.clearValues()
	defer func() {
		if err != nil {
			result.abortResult()
		}
	}()
	if !r.hasAggregate {
		limit, limitErr := normalizeAggregateBytes(options)
		if limitErr != nil {
			result.abortResult()
			return Cursor{}, limitErr
		}
		exec.Workspace.aggregateBudget.begin(limit)
	}
	inputRows := result.RowCount
	if shapeErr := r.validateResult(result); shapeErr != nil {
		return Cursor{}, shapeErr
	}
	inputBytes := result.resultBytesUsed
	scratchBytes := scalarExecutionScratchBytes(len(r.nodes), len(r.outputs))
	if err := frame.intermediate.reserve("scalar dependency result", inputBytes); err != nil {
		result.abortResult()
		return Cursor{}, err
	}
	defer frame.intermediate.release(inputBytes)
	if err := frame.intermediate.reserve("scalar expression workspace", scratchBytes); err != nil {
		result.abortResult()
		return Cursor{}, err
	}
	defer frame.intermediate.release(scratchBytes)
	if r.ordered != nil {
		return r.executeOrdered(s, exec, frame, options, intermediateResource, inputRows)
	}

	// The first pass evaluates only the predicate prefix and determines the
	// exact caller-visible cardinality before any output slice grows.
	rows, skipped := 0, 0
	for row := 0; row < inputRows; row++ {
		// LIMIT is a semantic execution bound, not merely an output trim. Once
		// enough admitted rows exist, neither later predicates nor projections
		// may be evaluated (and their errors must remain unobserved).
		if s.hasLimit && rows >= s.limit {
			break
		}
		if err := cancellationCheckpoint(options.Cancel, row); err != nil {
			result.abortResult()
			return Cursor{}, err
		}
		r.evalArena = r.evalArena[:0]
		dynamicCharge := int64(0)
		if err := r.evalNodes(
			result, row, 0, r.predicateNodes, &r.evalArena,
			&exec.Workspace.aggregateBudget, &frame.intermediate, &dynamicCharge,
			options.Cancel,
		); err != nil {
			frame.intermediate.release(dynamicCharge)
			result.abortResult()
			return Cursor{}, err
		}
		keep := r.keep()
		frame.intermediate.release(dynamicCharge)
		if !keep {
			continue
		}
		if skipped < s.offset {
			skipped++
			continue
		}
		rows++
	}

	rowLimit, byteLimit, err := normalizeResultBudget(options)
	if err != nil {
		result.abortResult()
		return Cursor{}, err
	}
	if intermediateResource != "" {
		rowLimit = -1
		byteLimit = frame.intermediate.remaining()
	}
	result.resultRowsLimit = rowLimit
	result.resultBytesLimit = byteLimit
	result.resultBytesUsed = 0
	required, err := result.checkResultBudget(len(r.outputs), rows, 0)
	if err != nil {
		result.abortResult()
		return Cursor{}, err
	}
	result.resultBytesUsed = required

	columns := max(len(r.outputs), len(result.Columns))
	if cap(result.Columns) < columns {
		result.Columns = append(result.Columns, make([]ResultColumn, columns-len(result.Columns))...)
	} else {
		result.Columns = result.Columns[:columns]
	}
	// Dependency columns remain at input cardinality until every input row has
	// been evaluated. Shrinking an overlapping output column here would make a
	// filtered/tail result overwrite the very dependency rows it still needs.
	for column := r.resultDependencyColumns(); column < len(r.outputs); column++ {
		cells := result.Columns[column].Cells
		if cap(cells) < rows {
			cells = append(cells, make([]Cell, rows-len(cells))...)
		} else {
			cells = cells[:rows]
		}
		result.Columns[column].Cells = cells
	}

	r.resultArena = r.resultArena[:0]
	outRow, skipped := 0, 0
	for row := 0; row < inputRows && outRow < rows; row++ {
		if err := cancellationCheckpoint(options.Cancel, row); err != nil {
			result.abortResult()
			return Cursor{}, err
		}
		r.evalArena = r.evalArena[:0]
		predicateCharge := int64(0)
		if err := r.evalNodes(
			result, row, 0, r.predicateNodes, &r.evalArena,
			&exec.Workspace.aggregateBudget, &frame.intermediate, &predicateCharge,
			options.Cancel,
		); err != nil {
			frame.intermediate.release(predicateCharge)
			result.abortResult()
			return Cursor{}, err
		}
		keep := r.keep()
		frame.intermediate.release(predicateCharge)
		if !keep {
			continue
		}
		if skipped < s.offset {
			skipped++
			continue
		}

		// Every output expression owns a postorder node interval compiled after
		// predicateNodes. Resetting the arena after WHERE/OFFSET therefore drops
		// predicate temporaries without invalidating any output dependency, and
		// guarantees filtered/skipped rows never evaluate projection errors.
		r.evalArena = r.evalArena[:0]
		outputCharge := int64(0)
		if err := r.evalNodes(
			result, row, r.predicateNodes, len(r.nodes), &r.evalArena,
			&exec.Workspace.aggregateBudget, &frame.intermediate, &outputCharge,
			options.Cancel,
		); err != nil {
			frame.intermediate.release(outputCharge)
			result.abortResult()
			return Cursor{}, err
		}
		r.outputValues = resize(r.outputValues, len(r.outputs))
		for i, root := range r.outputs {
			r.outputValues[i] = r.values[root]
		}
		for column := range r.outputs {
			cell := r.resultCell(r.outputValues[column], &r.resultArena)
			if err := result.admitResultCell(cell); err != nil {
				frame.intermediate.release(outputCharge)
				return Cursor{}, err
			}
			result.Columns[column].Cells[outRow] = cell
		}
		frame.intermediate.release(outputCharge)
		outRow++
	}
	for column := range r.outputs {
		cells := result.Columns[column].Cells
		if rows < len(cells) {
			clear(cells[rows:])
		}
		result.Columns[column].Cells = cells[:rows]
		result.Columns[column].Header = s.names[column]
	}
	for column := len(r.outputs); column < len(result.Columns); column++ {
		// Keep hidden dependency-column capacity behind the truncated public
		// result. The next execution's base projection expands Columns and reuses
		// these exact slices; clearing cells and metadata drops every borrowed
		// document reference without turning a CASE that reads more dependencies
		// than it returns into one allocation per hidden column per execution.
		cells := result.Columns[column].Cells
		clear(cells)
		result.Columns[column] = ResultColumn{Cells: cells[:0]}
	}
	result.Columns = result.Columns[:len(r.outputs)]
	result.RowCount = rows
	return Cursor{st: s, res: result, cur: -1, left: -1}, nil
}

func (r *statementScalar) executeOrdered(
	s *Statement,
	exec *Exec,
	frame *statementFrame,
	options ExecOptions,
	intermediateResource string,
	inputRows int,
) (Cursor, error) {
	result := &exec.Result
	ordered := r.ordered
	if ordered == nil {
		return Cursor{}, fmt.Errorf("query: missing scalar ORDER BY runtime")
	}
	if s.hasLimit && s.limit == 0 {
		return r.publishEmptyOrdered(s, result, options, intermediateResource, frame)
	}
	ordered.rows = ordered.rows[:0]
	ordered.values = ordered.values[:0]
	ordered.arena = ordered.arena[:0]
	orderCharge := int64(0)
	defer func() { frame.intermediate.release(orderCharge) }()

	perRow := saturatedBytes(
		int64(unsafe.Sizeof(statementScalarOrderRow{})),
		saturatedProduct(int64(len(ordered.order)), int64(unsafe.Sizeof(scalar{}))),
	)
	for row := 0; row < inputRows; row++ {
		if err := cancellationCheckpoint(options.Cancel, row); err != nil {
			return Cursor{}, err
		}
		r.evalArena = r.evalArena[:0]
		predicateCharge := int64(0)
		if err := r.evalNodes(
			result, row, 0, r.predicateNodes, &r.evalArena,
			&exec.Workspace.aggregateBudget, &frame.intermediate, &predicateCharge,
			options.Cancel,
		); err != nil {
			frame.intermediate.release(predicateCharge)
			return Cursor{}, err
		}
		keep := r.keep()
		frame.intermediate.release(predicateCharge)
		if !keep {
			continue
		}
		if ordered.havingEnd > ordered.projectionEnd {
			r.evalArena = r.evalArena[:0]
			havingCharge := int64(0)
			if err := r.evalNodes(
				result, row, ordered.projectionEnd, ordered.havingEnd,
				&r.evalArena, &exec.Workspace.aggregateBudget, &frame.intermediate,
				&havingCharge, options.Cancel,
			); err != nil {
				frame.intermediate.release(havingCharge)
				return Cursor{}, err
			}
			keep = r.keepHaving()
			frame.intermediate.release(havingCharge)
			if !keep {
				continue
			}
		}
		// HAVING reads the already-reduced dependency result. It must reject
		// groups before any deferred sort key is evaluated and before
		// OFFSET/LIMIT select a tail; applying it from Cursor afterwards can
		// otherwise discard the chosen row instead of admitting the next group.
		if ordered.having != nil && !ordered.having.keep(result, row) {
			continue
		}
		if err := frame.intermediate.reserve("scalar ORDER BY rows", perRow); err != nil {
			return Cursor{}, err
		}
		orderCharge = saturatedBytes(orderCharge, perRow)
		keyBase := len(ordered.values)
		for key := range ordered.order {
			term := &ordered.order[key]
			r.evalArena = r.evalArena[:0]
			temporaryCharge := int64(0)
			if err := r.evalNodes(
				result, row, int(term.start), int(term.end), &r.evalArena,
				&exec.Workspace.aggregateBudget, &frame.intermediate, &temporaryCharge,
				options.Cancel,
			); err != nil {
				frame.intermediate.release(temporaryCharge)
				return Cursor{}, err
			}
			value := r.values[term.root].value
			ownedBytes := scalarOwnedBytes(value)
			if err := frame.intermediate.reserve("scalar ORDER BY values", ownedBytes); err != nil {
				frame.intermediate.release(temporaryCharge)
				return Cursor{}, err
			}
			orderCharge = saturatedBytes(orderCharge, ownedBytes)
			ordered.values = append(ordered.values, ownScalar(value, &ordered.arena))
			frame.intermediate.release(temporaryCharge)
		}
		ordered.rows = append(ordered.rows, statementScalarOrderRow{
			input: row, keyBase: keyBase,
		})
	}

	if err := cancellationCheckpoint(options.Cancel, len(ordered.rows)); err != nil {
		return Cursor{}, err
	}
	sortScratchBytes := saturatedProduct(
		int64(len(ordered.rows)), int64(unsafe.Sizeof(statementScalarOrderRow{})),
	)
	if err := frame.intermediate.reserve("scalar ORDER BY sort workspace", sortScratchBytes); err != nil {
		return Cursor{}, err
	}
	if err := ordered.sort(options.Cancel); err != nil {
		frame.intermediate.release(sortScratchBytes)
		return Cursor{}, err
	}
	frame.intermediate.release(sortScratchBytes)
	if err := cancellationCheckpoint(options.Cancel, len(ordered.rows)+1); err != nil {
		return Cursor{}, err
	}

	first := min(s.offset, len(ordered.rows))
	last := len(ordered.rows)
	if s.hasLimit {
		last = first + min(s.limit, len(ordered.rows)-first)
	}
	selected := ordered.rows[first:last]
	rows := len(selected)
	if len(r.outputs) != 0 && rows > int(^uint(0)>>1)/len(r.outputs) {
		return Cursor{}, &IntermediateBudgetError{
			Resource: "scalar ordered output staging",
			Bytes:    int64(^uint64(0) >> 1),
			Limit:    frame.intermediate.limit,
		}
	}
	cellCount := rows * len(r.outputs)
	cellBytes := saturatedProduct(int64(cellCount), int64(unsafe.Sizeof(Cell{})))
	if err := frame.intermediate.reserve("scalar ordered output staging", cellBytes); err != nil {
		return Cursor{}, err
	}
	orderCharge = saturatedBytes(orderCharge, cellBytes)
	ordered.cells = resize(ordered.cells, cellCount)

	rowLimit, byteLimit, err := normalizeResultBudget(options)
	if err != nil {
		return Cursor{}, err
	}
	if intermediateResource != "" {
		rowLimit = -1
		byteLimit = frame.intermediate.remaining()
	}
	result.resultRowsLimit = rowLimit
	result.resultBytesLimit = byteLimit
	result.resultBytesUsed = 0
	required, err := result.checkResultBudget(len(r.outputs), rows, 0)
	if err != nil {
		return Cursor{}, err
	}
	result.resultBytesUsed = required

	r.resultArena = r.resultArena[:0]
	for outRow := range selected {
		if err := cancellationCheckpoint(options.Cancel, outRow); err != nil {
			return Cursor{}, err
		}
		r.evalArena = r.evalArena[:0]
		outputCharge := int64(0)
		if err := r.evalNodes(
			result, selected[outRow].input, r.predicateNodes, ordered.projectionEnd,
			&r.evalArena, &exec.Workspace.aggregateBudget, &frame.intermediate,
			&outputCharge, options.Cancel,
		); err != nil {
			frame.intermediate.release(outputCharge)
			return Cursor{}, err
		}
		r.outputValues = resize(r.outputValues, len(r.outputs))
		for output, root := range r.outputs {
			r.outputValues[output] = r.values[root]
		}
		base := outRow * len(r.outputs)
		for column := range r.outputs {
			cell := r.resultCell(r.outputValues[column], &r.resultArena)
			if err := result.admitResultCell(cell); err != nil {
				frame.intermediate.release(outputCharge)
				return Cursor{}, err
			}
			ordered.cells[base+column] = cell
		}
		frame.intermediate.release(outputCharge)
	}

	columns := max(len(r.outputs), len(result.Columns))
	if cap(result.Columns) < columns {
		result.Columns = append(result.Columns, make([]ResultColumn, columns-len(result.Columns))...)
	} else {
		result.Columns = result.Columns[:columns]
	}
	operations := 0
	for column := range r.outputs {
		cells := resize(result.Columns[column].Cells, rows)
		for row := 0; row < rows; row++ {
			if err := cancellationCheckpoint(options.Cancel, operations); err != nil {
				return Cursor{}, err
			}
			operations++
			cells[row] = ordered.cells[row*len(r.outputs)+column]
		}
		result.Columns[column].Cells = cells
		result.Columns[column].Header = s.names[column]
	}
	if err := cancellationError(options.Cancel); err != nil {
		return Cursor{}, err
	}
	for column := len(r.outputs); column < len(result.Columns); column++ {
		cells := result.Columns[column].Cells
		for row := range cells {
			if err := cancellationCheckpoint(options.Cancel, operations); err != nil {
				return Cursor{}, err
			}
			operations++
			cells[row] = Cell{}
		}
		result.Columns[column] = ResultColumn{Cells: cells[:0]}
	}
	if err := cancellationError(options.Cancel); err != nil {
		return Cursor{}, err
	}
	result.Columns = result.Columns[:len(r.outputs)]
	result.RowCount = rows
	return Cursor{st: s, res: result, cur: -1, left: -1}, nil
}

func (r *statementScalar) publishEmptyOrdered(
	s *Statement,
	result *Result,
	options ExecOptions,
	intermediateResource string,
	frame *statementFrame,
) (Cursor, error) {
	if err := cancellationError(options.Cancel); err != nil {
		return Cursor{}, err
	}
	rowLimit, byteLimit, err := normalizeResultBudget(options)
	if err != nil {
		return Cursor{}, err
	}
	if intermediateResource != "" {
		rowLimit = -1
		byteLimit = frame.intermediate.remaining()
	}
	result.resultRowsLimit = rowLimit
	result.resultBytesLimit = byteLimit
	result.resultBytesUsed = 0
	required, err := result.checkResultBudget(len(r.outputs), 0, 0)
	if err != nil {
		return Cursor{}, err
	}
	result.resultBytesUsed = required
	columns := max(len(r.outputs), len(result.Columns))
	if cap(result.Columns) < columns {
		result.Columns = append(result.Columns, make([]ResultColumn, columns-len(result.Columns))...)
	} else {
		result.Columns = result.Columns[:columns]
	}
	operations := 0
	for column := range result.Columns {
		cells := result.Columns[column].Cells
		for row := range cells {
			if err := cancellationCheckpoint(options.Cancel, operations); err != nil {
				return Cursor{}, err
			}
			operations++
			cells[row] = Cell{}
		}
		result.Columns[column] = ResultColumn{Cells: cells[:0]}
		if column < len(r.outputs) {
			result.Columns[column].Header = s.names[column]
		}
	}
	if err := cancellationError(options.Cancel); err != nil {
		return Cursor{}, err
	}
	result.Columns = result.Columns[:len(r.outputs)]
	result.RowCount = 0
	return Cursor{st: s, res: result, cur: -1, left: -1}, nil
}

func (o *statementScalarOrdered) compare(left, right statementScalarOrderRow) int {
	for key := range o.order {
		cmp := compareOrderedScalar(
			o.values[left.keyBase+key],
			o.values[right.keyBase+key],
			sqlOrderDirection(o.order[key].desc, o.order[key].nulls),
		)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// sortOrderRows is a stable bottom-up merge sort with cancellation checks in
// both the comparison and copy portions of every pass. The standard-library
// stable sorter has no error/cancellation channel, which could otherwise make
// an armed request wait through O(n log n) scalar comparisons.
func (o *statementScalarOrdered) sort(cancel *CancelFlag) error {
	rows := len(o.rows)
	if rows < 2 {
		return cancellationCheckpoint(cancel, 0)
	}
	o.scratch = resize(o.scratch, rows)
	source, target := o.rows, o.scratch
	operations := 0
	for width := 1; width < rows; {
		for low := 0; low < rows; low += 2 * width {
			middle := min(low+width, rows)
			high := min(low+2*width, rows)
			left, right, write := low, middle, low
			for left < middle && right < high {
				if err := cancellationCheckpoint(cancel, operations); err != nil {
					return err
				}
				operations++
				if o.compare(source[left], source[right]) <= 0 {
					target[write] = source[left]
					left++
				} else {
					target[write] = source[right]
					right++
				}
				write++
			}
			for left < middle {
				if err := cancellationCheckpoint(cancel, operations); err != nil {
					return err
				}
				operations++
				target[write] = source[left]
				left, write = left+1, write+1
			}
			for right < high {
				if err := cancellationCheckpoint(cancel, operations); err != nil {
					return err
				}
				operations++
				target[write] = source[right]
				right, write = right+1, write+1
			}
		}
		source, target = target, source
		if width > rows/2 {
			width = rows
		} else {
			width *= 2
		}
	}
	if len(source) != 0 && &source[0] != &o.rows[0] {
		for i := range source {
			if err := cancellationCheckpoint(cancel, operations+i); err != nil {
				return err
			}
			o.rows[i] = source[i]
		}
	}
	return nil
}

func scalarOwnedBytes(value scalar) int64 {
	switch value.kind {
	case kindNumber:
		return int64(len(value.num))
	case kindString:
		return int64(len(value.sval))
	case kindContainer:
		return int64(len(value.raw))
	default:
		return 0
	}
}

func ownScalar(value scalar, arena *[]byte) scalar {
	switch value.kind {
	case kindNumber:
		start := len(*arena)
		*arena = append(*arena, value.num...)
		value.num = (*arena)[start:len(*arena):len(*arena)]
		value.raw = value.num
	case kindString:
		start := len(*arena)
		*arena = append(*arena, value.sval...)
		value.sval = byteview.String((*arena)[start:len(*arena):len(*arena)])
		value.raw = nil
	case kindContainer:
		start := len(*arena)
		*arena = append(*arena, value.raw...)
		value.raw = (*arena)[start:len(*arena):len(*arena)]
	}
	return value
}

func scalarExecutionScratchBytes(nodes, outputs int) int64 {
	return saturatedBytes(
		saturatedProduct(int64(nodes), int64(unsafe.Sizeof(statementScalarValue{}))),
		saturatedProduct(int64(outputs), int64(unsafe.Sizeof(statementScalarValue{}))),
	)
}
