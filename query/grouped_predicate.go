package query

import (
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// stageGroupedScalarPredicate gives computed WHERE its own row stage before
// aggregation. The existing derived relation runner owns the bounded columnar
// spool, cancellation, and warm reuse. No JSON document reconstruction or new
// ordinary-Statement state is needed. The caller's resolved tree is immutable.
func stageGroupedScalarPredicate(text string, tree *sqlast.SelectStmt, correlation *lateralPrepareFrame) (*sqlast.SelectStmt, error) {
	if tree == nil || tree.Set != nil || selectHasWindows(tree) || !exprHasScalar(tree.Where) ||
		len(tree.GroupBy) == 0 && !selectHasAggregate(tree) {
		return tree, nil
	}
	if tree.Correlation != nil || correlation != nil {
		return nil, sqlast.NewFeatureNotSupportedError(text, firstScalarExprPos(tree.Where),
			"correlated scalar WHERE before grouping requires a staged correlation frame")
	}
	input := *tree
	input.With, input.Columns, input.GroupBy, input.Having = nil, nil, nil, nil
	input.Windows, input.OrderBy, input.Limit, input.Offset = nil, nil, nil, nil
	input.Distinct, input.ParamBase = false, 0
	outer := *tree
	outer.Where = nil
	outer.From = []sqlast.TableRef{{Kind: sqlast.RelationDerived, Query: &input, Alias: "__filtered", HasAlias: true}}
	outer.Columns = append([]sqlast.ResultColumn(nil), tree.Columns...)
	outer.GroupBy = append([]*sqlast.PathExpr(nil), tree.GroupBy...)
	outer.OrderBy = append([]sqlast.OrderTerm(nil), tree.OrderBy...)
	var paths, mapped []*sqlast.PathExpr
	mapPath := func(path *sqlast.PathExpr) *sqlast.PathExpr {
		if path == nil {
			return nil
		}
		for i, prior := range paths {
			if sameWindowPath(path, prior) {
				return mapped[i]
			}
		}
		name := "__value" + strconv.Itoa(len(paths))
		input.Columns = append(input.Columns, sqlast.ResultColumn{Path: path, Alias: name, Pos: path.Pos})
		result := &sqlast.PathExpr{Source: 0, Segments: []sqlast.Segment{{Key: name}}, Pos: path.Pos}
		paths, mapped = append(paths, path), append(mapped, result)
		return result
	}
	var scalar func(*sqlast.ScalarExpr) (*sqlast.ScalarExpr, error)
	var predicate func(*sqlast.Expr) (*sqlast.Expr, error)
	predicate = func(expr *sqlast.Expr) (*sqlast.Expr, error) {
		if expr == nil {
			return nil, nil
		}
		if expr.Subquery != nil {
			var correlated bool
			_ = sqlast.WalkSelectStatements(expr.Subquery, func(s *sqlast.SelectStmt) error { correlated = correlated || s.Correlation != nil; return nil })
			if correlated {
				return nil, sqlast.NewFeatureNotSupportedError(text, expr.Pos,
					"a correlated post-group subquery requires staged outer reference remapping")
			}
		}
		out := *expr
		out.Path, out.RightPath = mapPath(expr.Path), mapPath(expr.RightPath)
		var err error
		if out.ScalarLeft, err = scalar(expr.ScalarLeft); err != nil {
			return nil, err
		}
		if out.ScalarRight, err = scalar(expr.ScalarRight); err != nil {
			return nil, err
		}
		out.Kids = make([]*sqlast.Expr, len(expr.Kids))
		for i, kid := range expr.Kids {
			if out.Kids[i], err = predicate(kid); err != nil {
				return nil, err
			}
		}
		return &out, nil
	}
	scalar = func(expr *sqlast.ScalarExpr) (*sqlast.ScalarExpr, error) {
		if expr == nil {
			return nil, nil
		}
		out := *expr
		out.Path = mapPath(expr.Path)
		var err error
		if out.Left, err = scalar(expr.Left); err != nil {
			return nil, err
		}
		if out.Right, err = scalar(expr.Right); err != nil {
			return nil, err
		}
		if out.Else, err = scalar(expr.Else); err != nil {
			return nil, err
		}
		out.Whens = append([]sqlast.ScalarWhen(nil), expr.Whens...)
		for i := range out.Whens {
			arm := &out.Whens[i]
			if arm.Predicate, err = predicate(arm.Predicate); err != nil {
				return nil, err
			}
			if arm.Match, err = scalar(arm.Match); err != nil {
				return nil, err
			}
			if arm.Result, err = scalar(arm.Result); err != nil {
				return nil, err
			}
		}
		return &out, nil
	}
	// Preserve public headers before internal ordinal aliases replace paths.
	namer := Statement{tree: tree}
	for i := range outer.Columns {
		column := &outer.Columns[i]
		column.Alias = strings.Clone(namer.columnName(&tree.Columns[i]))
		column.Path = mapPath(column.Path)
		var err error
		if column.Scalar, err = scalar(column.Scalar); err != nil {
			return nil, err
		}
	}
	for i, path := range outer.GroupBy {
		outer.GroupBy[i] = mapPath(path)
	}
	var err error
	if outer.Having, err = predicate(outer.Having); err != nil {
		return nil, err
	}
	for i := range outer.OrderBy {
		order := &outer.OrderBy[i]
		order.Path = mapPath(order.Path)
		if order.Scalar, err = scalar(order.Scalar); err != nil {
			return nil, err
		}
	}
	if len(input.Columns) == 0 {
		input.Columns = []sqlast.ResultColumn{{Alias: "__row", Scalar: &sqlast.ScalarExpr{Kind: sqlast.ScalarLiteral, Value: sqlast.Operand{Kind: sqlast.OperandNumber, Text: "1"}}}}
	}
	return &outer, nil
}
