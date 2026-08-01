package query

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

type explainSQLSetDocument struct {
	Version uint8             `json:"version"`
	Plan    explainSQLSetPlan `json:"plan"`
}

type explainSQLSetPlan struct {
	Node       string             `json:"node"`
	AccessPath string             `json:"access_path"`
	Scope      string             `json:"scope"`
	Output     []string           `json:"output"`
	Expression *explainSQLSetNode `json:"expression"`
	Tail       *explainSQLSetTail `json:"tail,omitempty"`
	CTEs       []explainCTE       `json:"ctes,omitempty"`
	Analyze    *explainAnalyze    `json:"analyze,omitempty"`
}

type explainSQLSetNode struct {
	Kind       string             `json:"kind"`
	Operation  string             `json:"operation,omitempty"`
	Collection string             `json:"collection,omitempty"`
	AccessPath string             `json:"access_path,omitempty"`
	Output     []string           `json:"output,omitempty"`
	Left       *explainSQLSetNode `json:"left,omitempty"`
	Right      *explainSQLSetNode `json:"right,omitempty"`
	Child      *explainSQLSetNode `json:"child,omitempty"`
	Tail       *explainSQLSetTail `json:"tail,omitempty"`
}

type explainSQLSetTail struct {
	OrderBy []string `json:"order_by,omitempty"`
	Limit   *int     `json:"limit,omitempty"`
	Offset  *int     `json:"offset,omitempty"`
}

func (r *statementSetSQL) explain(
	options ExplainOptions,
	analysis *ExplainAnalysis,
) (string, error) {
	if r == nil || r.descriptor == nil {
		return "", queryExplainError("query: cannot explain a released set statement")
	}
	expression, err := r.explainExpr(r.expression, options)
	if err != nil {
		return "", err
	}
	scope := "logical"
	if options.IndexCatalogKnown {
		scope = "source-aware"
	}
	document := explainSQLSetDocument{Version: 1, Plan: explainSQLSetPlan{
		Node: "set", AccessPath: "bounded-set-tree", Scope: scope,
		Output: r.Columns(), Expression: expression, Tail: r.explainTail(),
		CTEs: r.explainCTEs(), Analyze: newExplainAnalyze(analysis),
	}}
	encoded, err := vibejson.Marshal(&document)
	if err != nil {
		return "", err
	}
	return byteview.String(encoded), nil
}

func (r *statementSetSQL) explainExpr(
	expr *sqlast.SetExpr,
	options ExplainOptions,
) (*explainSQLSetNode, error) {
	if expr == nil {
		return nil, fmt.Errorf("query: cannot explain a nil set node: %w", ErrSetTreePlan)
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		leaf := r.findLeaf(expr.Select)
		if leaf == nil {
			return nil, fmt.Errorf("query: set explain lost a prepared leaf: %w", ErrSetTreePlan)
		}
		plan, err := leaf.explainSourcePlan()
		if err != nil {
			return nil, err
		}
		return &explainSQLSetNode{
			Kind: "scan", Collection: leaf.Collection(), Output: leaf.Columns(),
			AccessPath: explainAccessPath(
				plan.where, plan.valuePaths, len(plan.joins), options,
			),
		}, nil

	case sqlast.SetBinaryExpr:
		left, err := r.explainExpr(expr.Left, options)
		if err != nil {
			return nil, err
		}
		right, err := r.explainExpr(expr.Right, options)
		if err != nil {
			return nil, err
		}
		operation, err := explainSetSQLOperation(expr.Operation)
		if err != nil {
			return nil, err
		}
		return &explainSQLSetNode{
			Kind: "operation", Operation: operation, Left: left, Right: right,
		}, nil

	case sqlast.SetGroupExpr:
		if expr.Tail == nil {
			child, err := r.explainExpr(expr.Child, options)
			if err != nil {
				return nil, err
			}
			return &explainSQLSetNode{Kind: "group", Child: child}, nil
		}
		group := r.findGroup(expr)
		if group == nil {
			return nil, fmt.Errorf("query: set explain lost a prepared group: %w", ErrSetTreePlan)
		}
		child, err := group.explainExpr(group.expression, options)
		if err != nil {
			return nil, err
		}
		return &explainSQLSetNode{
			Kind: "group", Child: child, Tail: group.explainTail(),
		}, nil

	default:
		return nil, fmt.Errorf("query: cannot explain set node kind %d: %w", expr.Kind, ErrSetTreePlan)
	}
}

func explainSetSQLOperation(operation sqlast.SetOperation) (string, error) {
	switch operation {
	case sqlast.SetUnionAll:
		return "union all", nil
	case sqlast.SetUnionDistinct:
		return "union distinct", nil
	case sqlast.SetIntersectAll:
		return "intersect all", nil
	case sqlast.SetIntersectDistinct:
		return "intersect distinct", nil
	case sqlast.SetExceptAll:
		return "except all", nil
	case sqlast.SetExceptDistinct:
		return "except distinct", nil
	default:
		return "", fmt.Errorf("query: cannot explain set operation %d: %w", operation, ErrSetTreePlan)
	}
}

func (r *statementSetSQL) explainTail() *explainSQLSetTail {
	if r == nil || r.tail == nil {
		return nil
	}
	tail := &explainSQLSetTail{}
	for i := range r.tail.OrderBy {
		term := &r.tail.OrderBy[i]
		direction := " ASC"
		if term.Desc {
			direction = " DESC"
		}
		tail.OrderBy = append(tail.OrderBy, term.Name+direction)
	}
	if r.hasLimit {
		limit := r.limit
		tail.Limit = &limit
	}
	if r.offset != 0 || r.tail.Offset != nil {
		offset := r.offset
		tail.Offset = &offset
	}
	return tail
}

func (r *statementSetSQL) findLeaf(tree *sqlast.SelectStmt) *Statement {
	for i := range r.leaves {
		if r.leaves[i].tree == tree {
			return r.leaves[i].stmt
		}
	}
	return nil
}

func (r *statementSetSQL) findGroup(expr *sqlast.SetExpr) *statementSetSQL {
	for i := range r.groups {
		if r.groups[i].expr == expr {
			return r.groups[i].runner
		}
	}
	return nil
}

func (r *statementSetSQL) explainCTEs() []explainCTE {
	for i := range r.leaves {
		if ctes := r.leaves[i].stmt.explainCTEs(); len(ctes) != 0 {
			return ctes
		}
	}
	for i := range r.groups {
		if ctes := r.groups[i].runner.explainCTEs(); len(ctes) != 0 {
			return ctes
		}
	}
	return nil
}
