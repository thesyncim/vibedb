package gateway

import (
	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type coordinatorPredicate struct {
	expr *sqlast.Expr
	base int
}

// coordinatorConstraints tracks each physical occurrence independently. Reused
// relations require the union of their consumers' domains; an unconstrained
// consumer therefore prevents pruning that table. Inherited predicates cross
// only plain path projections, never LIMIT, window, aggregate, or recursive
// boundaries that would change which rows the inner statement selects.
func coordinatorConstraints(snap *Snapshot, root *sqlast.SelectStmt, args []any) (map[string]distribution.BoundConstraints, error) {
	result := make(map[string]distribution.BoundConstraints)
	bases := make(map[*sqlast.SelectStmt]int)
	aliases := make(map[*sqlast.SelectStmt][]string)
	var index func(*sqlast.SelectStmt, int)
	index = func(s *sqlast.SelectStmt, base int) {
		if s == nil {
			return
		}
		if _, ok := bases[s]; ok {
			return
		}
		bases[s] = base
		if s.With != nil {
			for _, cte := range s.With.CTEs {
				aliases[cte.Query] = cte.Columns
				index(cte.Query, base+cte.Query.ParamBase)
			}
		}
		for _, ref := range s.From {
			if ref.Query != nil {
				index(ref.Query, base+ref.Query.ParamBase)
			}
		}
	}
	index(root, 0)
	active := make(map[*sqlast.SelectStmt]bool)
	var visit func(*sqlast.SelectStmt, []coordinatorPredicate) error
	add := func(table string, predicates []coordinatorPredicate) error {
		placement, _, _, ok := snap.plannerTableFor(table)
		if !ok {
			return ErrTableNotPlaced
		}
		var expression *sqlast.Expr
		for _, p := range predicates {
			leaf := coordinatorMapPredicate(p.expr, p.base, func(path *sqlast.PathExpr) *sqlast.PathExpr { return path })
			expression = coordinatorAnd(expression, leaf)
		}
		constraints, err := coordinatorBindConstraints(placement.Columns, expression, args)
		if err != nil {
			return err
		}
		prior, seen := result[table]
		if !seen {
			result[table] = constraints
			return nil
		}
		for i := range prior {
			prior[i], err = coordinatorCombineDomain(prior[i], constraints[i], true)
			if err != nil {
				return err
			}
		}
		return nil
	}
	visit = func(s *sqlast.SelectStmt, inherited []coordinatorPredicate) error {
		if s == nil {
			return nil
		}
		if active[s] {
			// Set leaves and recursive iterations can require different source sets.
			// Keep their union complete until a leaf-specific lineage proof is present.
			for _, table := range coordinatorPhysicalTables(s) {
				if err := add(table, nil); err != nil {
					return err
				}
			}
			return nil
		}
		if s.Set != nil {
			active[s] = true
			defer delete(active, s)
			var walk func(*sqlast.SetExpr) error
			walk = func(e *sqlast.SetExpr) error {
				if e == nil {
					return nil
				}
				if e.Select != nil {
					index(e.Select, bases[s]+e.Select.ParamBase)
					if err := visit(e.Select, nil); err != nil {
						return err
					}
				}
				for _, child := range []*sqlast.SetExpr{e.Left, e.Right, e.Child} {
					if err := walk(child); err != nil {
						return err
					}
				}
				return nil
			}
			return walk(s.Set.Root)
		}
		active[s] = true
		defer delete(active, s)
		predicates := append([]coordinatorPredicate(nil), inherited...)
		predicates = append(predicates, coordinatorPredicate{s.Where, bases[s]})
		if s.Correlation != nil || coordinatorHasPathComparison(s.Where) {
			predicates = nil
		}
		for source, ref := range s.From {
			var mapped []coordinatorPredicate
			for _, p := range predicates {
				translated := coordinatorMapPredicate(p.expr, p.base, func(path *sqlast.PathExpr) *sqlast.PathExpr {
					path = coordinatorEquivalentPath(s, source, path)
					if path == nil || path.MergedUsing != 0 {
						return nil
					}
					clone := *path
					clone.Source = 0
					return &clone
				})
				if translated != nil {
					mapped = append(mapped, coordinatorPredicate{translated, 0})
				}
			}
			if ref.Kind == sqlast.RelationCollection {
				if err := add(ref.Name, mapped); err != nil {
					return err
				}
				continue
			}
			var pushed []coordinatorPredicate
			if coordinatorTransparentProjection(ref.Query) {
				for _, p := range mapped {
					translated := coordinatorMapPredicate(p.expr, 0, func(path *sqlast.PathExpr) *sqlast.PathExpr {
						return coordinatorProjectionPath(ref.Query, aliases[ref.Query], path)
					})
					if translated != nil {
						pushed = append(pushed, coordinatorPredicate{translated, 0})
					}
				}
			}
			if err := visit(ref.Query, pushed); err != nil {
				return err
			}
		}
		// Predicate subqueries may read physical tables not present in FROM. They
		// retain their own WHERE predicates; outer correlations remain unbound.
		var subqueries func(*sqlast.Expr) error
		var scalarSubqueries func(*sqlast.ScalarExpr) error
		scalarSubqueries = func(e *sqlast.ScalarExpr) error {
			if e == nil {
				return nil
			}
			for _, child := range []*sqlast.ScalarExpr{e.Left, e.Right, e.Else} {
				if err := scalarSubqueries(child); err != nil {
					return err
				}
			}
			for _, arm := range e.Whens {
				if err := subqueries(arm.Predicate); err != nil {
					return err
				}
				if err := scalarSubqueries(arm.Match); err != nil {
					return err
				}
				if err := scalarSubqueries(arm.Result); err != nil {
					return err
				}
			}
			return nil
		}
		subqueries = func(e *sqlast.Expr) error {
			if e == nil {
				return nil
			}
			if e.Subquery != nil {
				index(e.Subquery, bases[s]+e.Subquery.ParamBase)
				if err := visit(e.Subquery, nil); err != nil {
					return err
				}
			}
			if err := scalarSubqueries(e.ScalarLeft); err != nil {
				return err
			}
			if err := scalarSubqueries(e.ScalarRight); err != nil {
				return err
			}
			for _, kid := range e.Kids {
				if err := subqueries(kid); err != nil {
					return err
				}
			}
			return nil
		}
		if err := subqueries(s.Where); err != nil {
			return err
		}
		if err := subqueries(s.Having); err != nil {
			return err
		}
		for _, c := range s.Columns {
			if err := scalarSubqueries(c.Scalar); err != nil {
				return err
			}
		}
		for _, ref := range s.From {
			if ref.On != nil {
				if err := subqueries(ref.On.Expr); err != nil {
					return err
				}
			}
		}
		for _, order := range s.OrderBy {
			if err := scalarSubqueries(order.Scalar); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root, nil); err != nil {
		return nil, err
	}
	// Physical references hidden in scalar CASE conditions are deliberately
	// widened unless visited above; missing proof must never become an empty set.
	for _, table := range coordinatorPhysicalTables(root) {
		if _, ok := result[table]; !ok {
			if err := add(table, nil); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func coordinatorTransparentProjection(s *sqlast.SelectStmt) bool {
	if s == nil || s.Set != nil || s.Limit != nil || s.Offset != nil || len(s.GroupBy) != 0 || s.Having != nil || len(s.Windows) != 0 || s.Correlation != nil {
		return false
	}
	if coordinatorExpressionBarrier(s.Where) {
		return false
	}
	for _, order := range s.OrderBy {
		if order.Scalar != nil {
			return false
		}
	}
	for _, c := range s.Columns {
		if c.Agg != sqlast.AggNone || c.Scalar != nil || c.Window != nil {
			return false
		}
	}
	return true
}
func coordinatorProjectionPath(s *sqlast.SelectStmt, aliases []string, path *sqlast.PathExpr) *sqlast.PathExpr {
	if path == nil || len(path.Segments) == 0 {
		return nil
	}
	for ordinal, c := range s.Columns {
		if c.Path == nil {
			continue
		}
		if len(c.Path.Segments) == 0 {
			if len(aliases) != 0 {
				return nil
			}
			clone := *path
			clone.Source = c.Path.Source
			return &clone
		}
		name := c.Alias
		if ordinal < len(aliases) {
			name = aliases[ordinal]
		}
		if name == "" && len(c.Path.Segments) == 1 {
			name = c.Path.Segments[0].Key
		}
		if path.Segments[0].Key != name {
			continue
		}
		clone := *c.Path
		clone.Segments = append(append([]sqlast.Segment(nil), c.Path.Segments...), path.Segments[1:]...)
		return &clone
	}
	return nil
}
func coordinatorAnd(a, b *sqlast.Expr) *sqlast.Expr {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &sqlast.Expr{Kind: sqlast.ExprAnd, Kids: []*sqlast.Expr{a, b}}
}
func coordinatorMapPredicate(e *sqlast.Expr, base int, path func(*sqlast.PathExpr) *sqlast.PathExpr) *sqlast.Expr {
	if e == nil {
		return nil
	}
	if e.Kind == sqlast.ExprAnd || e.Kind == sqlast.ExprOr {
		clone := *e
		clone.Kids = nil
		for _, kid := range e.Kids {
			mapped := coordinatorMapPredicate(kid, base, path)
			if mapped == nil {
				if e.Kind == sqlast.ExprOr {
					return nil
				}
				continue
			}
			clone.Kids = append(clone.Kids, mapped)
		}
		if len(clone.Kids) == 0 {
			return nil
		}
		return &clone
	}
	if e.Kind != sqlast.ExprCompare && e.Kind != sqlast.ExprIn || e.Subquery != nil || e.RightPath != nil || e.Negated || e.Kind == sqlast.ExprCompare && e.Op != sqlast.OpEq {
		return nil
	}
	mapped := path(e.Path)
	if mapped == nil {
		return nil
	}
	clone := *e
	clone.Path = mapped
	if clone.Value.Kind == sqlast.OperandParam {
		clone.Value.Ordinal += base
	}
	if e.List != nil {
		clone.List = append([]sqlast.Operand(nil), e.List...)
		for i := range clone.List {
			if clone.List[i].Kind == sqlast.OperandParam {
				clone.List[i].Ordinal += base
			}
		}
	}
	return &clone
}
func coordinatorBindConstraints(columns []string, e *sqlast.Expr, args []any) (distribution.BoundConstraints, error) {
	return sqldriver.CompileConstraintProgram(columns, e).Bind(args)
}
func coordinatorCombineDomain(a, b distribution.ValueDomain, union bool) (distribution.ValueDomain, error) {
	if union {
		return distribution.UnionDomains(a, b)
	}
	return distribution.IntersectDomains(a, b)
}

func coordinatorHasPathComparison(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if e.RightPath != nil {
		return true
	}
	if e.Subquery != nil {
		found := false
		_ = sqlast.WalkSelectStatements(e.Subquery, func(s *sqlast.SelectStmt) error {
			// The graph walker owns recursion through child statements.
			for _, expression := range []*sqlast.Expr{s.Where, s.Having} {
				found = found || coordinatorLocalPathComparison(expression)
			}
			for _, ref := range s.From {
				if ref.On != nil {
					found = found || coordinatorLocalPathComparison(ref.On.Expr)
				}
			}
			return nil
		})
		if found {
			return true
		}
	}
	for _, child := range e.Kids {
		if coordinatorHasPathComparison(child) {
			return true
		}
	}
	return false
}

// Join equalities transfer finite key domains along legal null-preserving
// directions. LEFT JOIN can narrow its right input from the preserved left;
// the reverse would discard unmatched left rows. FULL JOIN transfers neither.
func coordinatorEquivalentPath(s *sqlast.SelectStmt, source int, path *sqlast.PathExpr) *sqlast.PathExpr {
	if path == nil || path.MergedUsing != 0 {
		return nil
	}
	if path.Source == source {
		return path
	}
	paths := []*sqlast.PathExpr{path}
	for next := 0; next < len(paths); next++ {
		for _, ref := range s.From {
			if ref.On == nil {
				continue
			}
			for _, key := range ref.On.Keys {
				var candidates []*sqlast.PathExpr
				if (ref.Join == sqlast.JoinInner || ref.Join == sqlast.JoinLeft) && samePlanPath(paths[next], key.Left) {
					candidates = append(candidates, key.Right)
				}
				if (ref.Join == sqlast.JoinInner || ref.Join == sqlast.JoinRight) && samePlanPath(paths[next], key.Right) {
					candidates = append(candidates, key.Left)
				}
				for _, candidate := range candidates {
					if candidate == nil {
						continue
					}
					if candidate.Source == source {
						return candidate
					}
					seen := false
					for _, prior := range paths {
						seen = seen || samePlanPath(prior, candidate)
					}
					if !seen {
						paths = append(paths, candidate)
					}
				}
			}
		}
	}
	return nil
}

// Scalar predicates and subqueries can fail or materialize before an outer
// filter runs. Their relation boundary must retain all of its own input rows.
func coordinatorExpressionBarrier(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if e.ScalarLeft != nil || e.ScalarRight != nil || e.RightPath != nil || e.Subquery != nil {
		return true
	}
	for _, kid := range e.Kids {
		if coordinatorExpressionBarrier(kid) {
			return true
		}
	}
	return false
}

func coordinatorLocalPathComparison(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if e.RightPath != nil {
		return true
	}
	for _, kid := range e.Kids {
		if coordinatorLocalPathComparison(kid) {
			return true
		}
	}
	return false
}
