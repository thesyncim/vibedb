package driver

import (
	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
)

type booleanConstraintProgram struct {
	union    bool
	children []*ConstraintProgram
}

func containsRoutingDisjunction(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if e.Kind == sqlast.ExprOr {
		return true
	}
	if e.Kind != sqlast.ExprAnd {
		return false
	}
	for _, kid := range e.Kids {
		if containsRoutingDisjunction(kid) {
			return true
		}
	}
	return false
}
func compileBooleanConstraints(columns []string, e *sqlast.Expr) *booleanConstraintProgram {
	if containsRuntimeSQLPathComparison(e) || !containsRoutingDisjunction(e) {
		return nil
	}
	node := &booleanConstraintProgram{union: e.Kind == sqlast.ExprOr}
	for _, kid := range e.Kids {
		node.children = append(node.children, CompileConstraintProgram(columns, kid))
	}
	return node
}
func (p *booleanConstraintProgram) bind(args []any) (distribution.BoundConstraints, error) {
	var result distribution.BoundConstraints
	for _, kid := range p.children {
		child, err := kid.Bind(args)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = child
			continue
		}
		for i := range result {
			if p.union {
				result[i], err = distribution.UnionDomains(result[i], child[i])
			} else {
				result[i], err = distribution.IntersectDomains(result[i], child[i])
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}
