package query

import (
	"strconv"
)

// nativePreparedQuery is the internal bridge between strict wire syntax and
// the shared immutable Query plan. It is deliberately not the public native
// statement API: exposing that API also requires database-source enforcement,
// binding storage, execution budgets, and heap/durable parity.
type nativePreparedQuery struct {
	from  string
	query *Query
}

func prepareNativeQuery(src []byte) (*nativePreparedQuery, error) {
	syntax, err := parseNativeQuerySyntax(src)
	if err != nil {
		return nil, err
	}
	return lowerNativeQuery(syntax)
}

func lowerNativeQuery(syntax nativeQuerySyntax) (*nativePreparedQuery, error) {
	if len(syntax.exists) != 0 {
		return nil, nativeSyntaxErr(
			"feature_not_ready", "/exists",
			"native existence joins require shared execution budgets and selected snapshots",
		)
	}
	if syntax.join != nil {
		return nil, nativeSyntaxErr(
			"feature_not_ready", "/join",
			"native fan-out joins require backend-neutral bounded execution",
		)
	}
	if len(syntax.orderBy) != 0 {
		return nil, nativeSyntaxErr(
			"feature_not_ready", "/orderBy",
			"native ordering requires scalar-domain validation",
		)
	}

	columns := make([]Column, 0, len(syntax.selects))
	if len(syntax.selects) == 0 {
		columns = append(columns, wholeDocument)
	} else {
		for _, projection := range syntax.selects {
			column := Path(projection.path.spec)
			column.header = projection.name
			columns = append(columns, column)
		}
	}
	query := Select(columns...)
	if syntax.where != nil {
		predicate, err := lowerNativePredicate(*syntax.where)
		if err != nil {
			return nil, err
		}
		query.Where(predicate)
	}
	if syntax.hasLimit {
		if syntax.limit.kind == nativeOperandParameter {
			return nil, nativeSyntaxErr(
				"feature_not_ready", "/limit",
				"native parameter binding is not implemented yet",
			)
		}
		limit, err := strconv.Atoi(syntax.limit.scalar.text)
		if err != nil {
			return nil, nativeSyntaxErr(
				"invalid_limit", "/limit", "cannot lower limit %q", syntax.limit.scalar.text,
			)
		}
		query.Limit(limit)
	}
	if err := query.Prepare(); err != nil {
		return nil, err
	}
	return &nativePreparedQuery{from: syntax.from, query: query}, nil
}

func lowerNativePredicate(syntax nativePredicateSyntax) (Predicate, error) {
	switch syntax.kind {
	case nativePredicateAnd, nativePredicateOr:
		children := make([]Predicate, 0, len(syntax.children))
		for _, childSyntax := range syntax.children {
			child, err := lowerNativePredicate(childSyntax)
			if err != nil {
				return Predicate{}, err
			}
			children = append(children, child)
		}
		if syntax.kind == nativePredicateAnd {
			return And(children...), nil
		}
		return Or(children...), nil
	case nativePredicateNot:
		if len(syntax.children) != 1 {
			return Predicate{}, nativeSyntaxErr(
				"invalid_predicate", "/where", "$not must have exactly one child",
			)
		}
		child, err := lowerNativePredicate(syntax.children[0])
		if err != nil {
			return Predicate{}, err
		}
		return Not(child), nil
	case nativePredicateField:
		return lowerNativeFieldPredicate(syntax)
	default:
		return Predicate{}, nativeSyntaxErr(
			"invalid_predicate", "/where", "cannot lower invalid predicate node",
		)
	}
}

func lowerNativeFieldPredicate(syntax nativePredicateSyntax) (Predicate, error) {
	switch syntax.operator {
	case nativeFieldEq:
		return lowerNativeEquality(syntax.path, syntax.operand)
	case nativeFieldNe:
		equal, err := lowerNativeEquality(syntax.path, syntax.operand)
		if err != nil {
			return Predicate{}, err
		}
		return Not(equal), nil
	case nativeFieldIn, nativeFieldNotIn:
		membership, err := lowerNativeMembership(syntax.path, syntax.operand)
		if err != nil {
			return Predicate{}, err
		}
		if syntax.operator == nativeFieldNotIn {
			return Not(membership), nil
		}
		return membership, nil
	case nativeFieldExists:
		return Exists(syntax.path), nil
	case nativeFieldNull:
		return nativeExactNull(syntax.path), nil
	case nativeFieldMissing:
		return Not(Exists(syntax.path)), nil
	case nativeFieldNullish:
		return IsNull(syntax.path), nil
	case nativeFieldLt, nativeFieldLe, nativeFieldGt, nativeFieldGe:
		return Predicate{}, nativeSyntaxErr(
			"feature_not_ready", "/where",
			"native ordered comparisons require same-kind scalar comparison",
		)
	default:
		return Predicate{}, nativeSyntaxErr(
			"invalid_operator", "/where", "cannot lower invalid field operator",
		)
	}
}

func lowerNativeEquality(path string, operand nativeOperandSyntax) (Predicate, error) {
	if operand.kind == nativeOperandParameter {
		return Predicate{}, nativeSyntaxErr(
			"feature_not_ready", "/where",
			"native parameter binding is not implemented yet",
		)
	}
	if operand.kind != nativeOperandScalar {
		return Predicate{}, nativeSyntaxErr(
			"invalid_operand", "/where", "equality requires one scalar operand",
		)
	}
	if operand.scalar.kind == qNull {
		return nativeExactNull(path), nil
	}
	value, err := nativeScalarValue(operand.scalar)
	if err != nil {
		return Predicate{}, err
	}
	return Cmp(path, Eq, value), nil
}

func lowerNativeMembership(path string, operand nativeOperandSyntax) (Predicate, error) {
	if operand.kind == nativeOperandParameter {
		return Predicate{}, nativeSyntaxErr(
			"feature_not_ready", "/where",
			"native parameter binding is not implemented yet",
		)
	}
	if operand.kind != nativeOperandList {
		return Predicate{}, nativeSyntaxErr(
			"invalid_operand", "/where", "membership requires a scalar list",
		)
	}
	values := make([]any, 0, len(operand.list))
	hasNull := false
	for _, scalar := range operand.list {
		if scalar.kind == qNull {
			hasNull = true
			continue
		}
		value, err := nativeScalarValue(scalar)
		if err != nil {
			return Predicate{}, err
		}
		values = append(values, value)
	}
	membership := In(path, values...)
	if hasNull {
		membership = Or(membership, nativeExactNull(path))
	}
	return membership, nil
}

func nativeScalarValue(scalar nativeScalarSyntax) (any, error) {
	switch scalar.kind {
	case qBool:
		return scalar.boolean, nil
	case qNumber:
		return Number(scalar.text), nil
	case qString:
		return scalar.text, nil
	default:
		return nil, nativeSyntaxErr(
			"invalid_operand", "/where", "cannot lower non-scalar operand",
		)
	}
}

func nativeExactNull(path string) Predicate {
	return And(Exists(path), IsNull(path))
}
