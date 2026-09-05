package query

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

type statementScalarConditional struct {
	left, right statementScalarRange
	domain      scalarCaseDomain
}

func (r *statementScalar) compileConditional(s *Statement, expr *sqlast.ScalarExpr) (int32, error) {
	root := int32(len(r.nodes))
	index := int32(len(r.conditionals))
	r.conditionals = append(r.conditionals, statementScalarConditional{})
	r.nodes = append(r.nodes, statementScalarNode{
		kind: statementScalarConditionalNode, op: expr.Op, pos: expr.Pos,
		conditionalIndex: index,
	})
	left, err := r.compileCaseRange(s, expr.Left)
	if err != nil {
		return 0, err
	}
	right, err := r.compileCaseRange(s, expr.Right)
	if err != nil {
		return 0, err
	}
	domain, ok := unifyCaseDomain(r.nodeDomain(left.root), r.nodeDomain(right.root))
	if !ok {
		return 0, sqlast.NewFeatureNotSupportedError(s.text, expr.Pos, "conditional expression arguments must have a common type")
	}
	if domain == caseDomainJSON && expr.Op != sqlast.ScalarCoalesce {
		return 0, sqlast.NewFeatureNotSupportedError(s.text, expr.Pos, "conditional comparison requires scalar boolean, numeric, or text arguments")
	}
	r.conditionals[index] = statementScalarConditional{left: left, right: right, domain: domain}
	r.nodes[root].skip = int32(len(r.nodes))
	return root, nil
}

func (r *statementScalar) evalConditional(
	result *Result, row int, node *statementScalarNode, arena *[]byte,
	budget *aggregateBudget, intermediate *intermediateBudget,
	intermediateCharge *int64, cancel *CancelFlag,
) (statementScalarValue, error) {
	program := &r.conditionals[node.conditionalIndex]
	if err := r.evalNodes(result, row, int(program.left.start), int(program.left.end), arena,
		budget, intermediate, intermediateCharge, cancel); err != nil {
		return statementScalarValue{}, err
	}
	left := r.values[program.left.root]
	validate := func(value statementScalarValue, root int32) (statementScalarValue, error) {
		actual := r.nodeDomain(root)
		if actual == caseDomainDynamic {
			actual = scalarValueDomain(value.value)
		}
		if node.op != sqlast.ScalarCoalesce && value.value.kind != kindNull &&
			(value.value.kind != kindBool && value.value.kind != kindNumber && value.value.kind != kindString) {
			return statementScalarValue{}, &ScalarTypeError{Pos: node.pos, Operation: conditionalOperationName(node.op), Left: valueTypeOfScalar(value.value), Right: TypeAny}
		}
		if value.value.kind != kindNull && program.domain != caseDomainDynamic &&
			program.domain != caseDomainNull && actual != program.domain {
			return statementScalarValue{}, &ScalarTypeError{Pos: node.pos,
				Operation: conditionalOperationName(node.op), Left: valueTypeOfScalar(value.value), Right: program.domain.valueType()}
		}
		return value, nil
	}
	if node.op == sqlast.ScalarCoalesce && (left.value.kind != kindNull || left.exact) {
		return validate(left, program.left.root)
	}
	if err := r.evalNodes(result, row, int(program.right.start), int(program.right.end), arena,
		budget, intermediate, intermediateCharge, cancel); err != nil {
		return statementScalarValue{}, err
	}
	right := r.values[program.right.root]
	if _, err := validate(left, program.left.root); err != nil {
		return statementScalarValue{}, err
	}
	if _, err := validate(right, program.right.root); err != nil {
		return statementScalarValue{}, err
	}
	if node.op == sqlast.ScalarCoalesce {
		return right, nil
	}
	if left.value.kind == kindNull {
		if node.op == sqlast.ScalarNullIf {
			return left, nil
		}
		return right, nil
	}
	if right.value.kind == kindNull {
		return left, nil
	}
	if left.value.kind != right.value.kind ||
		(left.value.kind != kindNumber && left.value.kind != kindString && left.value.kind != kindBool) {
		return statementScalarValue{}, &ScalarTypeError{Pos: node.pos,
			Operation: conditionalOperationName(node.op), Left: valueTypeOfScalar(left.value), Right: valueTypeOfScalar(right.value)}
	}
	cmp := compareScalar(left.value, right.value)
	switch node.op {
	case sqlast.ScalarGreatest:
		if cmp < 0 {
			return right, nil
		}
	case sqlast.ScalarLeast:
		if cmp > 0 {
			return right, nil
		}
	case sqlast.ScalarNullIf:
		if cmp == 0 {
			return statementScalarValue{value: scalar{kind: kindNull}}, nil
		}
	default:
		return statementScalarValue{}, fmt.Errorf("query: invalid conditional operation %d", node.op)
	}
	return left, nil
}

func conditionalOperationName(op sqlast.ScalarOp) string {
	switch op {
	case sqlast.ScalarCoalesce:
		return "COALESCE"
	case sqlast.ScalarGreatest:
		return "GREATEST"
	case sqlast.ScalarLeast:
		return "LEAST"
	default:
		return "NULLIF"
	}
}
