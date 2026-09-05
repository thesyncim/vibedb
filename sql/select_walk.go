package sql

// WalkSelectStatements visits every statement in a SELECT graph once, including
// CTE definitions, derived relations, set leaves, and predicate subqueries in
// scalar CASE expressions. Recursive CTE references cannot recurse indefinitely.
// The visitor sees parser-owned statements and must not mutate them.
func WalkSelectStatements(root *SelectStmt, visit func(*SelectStmt) error) error {
	seen := make(map[*SelectStmt]bool)
	var statement func(*SelectStmt) error
	var set func(*SetExpr) error
	var predicate func(*Expr) error
	var scalar func(*ScalarExpr) error
	scalar = func(e *ScalarExpr) error {
		if e == nil {
			return nil
		}
		for _, child := range []*ScalarExpr{e.Left, e.Right, e.Else} {
			if err := scalar(child); err != nil {
				return err
			}
		}
		for _, arm := range e.Whens {
			if err := predicate(arm.Predicate); err != nil {
				return err
			}
			if err := scalar(arm.Match); err != nil {
				return err
			}
			if err := scalar(arm.Result); err != nil {
				return err
			}
		}
		return nil
	}
	predicate = func(e *Expr) error {
		if e == nil {
			return nil
		}
		if err := statement(e.Subquery); err != nil {
			return err
		}
		if err := scalar(e.ScalarLeft); err != nil {
			return err
		}
		if err := scalar(e.ScalarRight); err != nil {
			return err
		}
		for _, child := range e.Kids {
			if err := predicate(child); err != nil {
				return err
			}
		}
		return nil
	}
	set = func(e *SetExpr) error {
		if e == nil {
			return nil
		}
		if err := statement(e.Select); err != nil {
			return err
		}
		if err := statement(e.First); err != nil {
			return err
		}
		if e.Table != nil {
			if err := statement(e.Table.Ref.Query); err != nil {
				return err
			}
		}
		for _, child := range []*SetExpr{e.Left, e.Right, e.Child} {
			if err := set(child); err != nil {
				return err
			}
		}
		return nil
	}
	statement = func(s *SelectStmt) error {
		if s == nil || seen[s] {
			return nil
		}
		seen[s] = true
		if err := visit(s); err != nil {
			return err
		}
		if s.With != nil {
			for _, cte := range s.With.CTEs {
				if err := statement(cte.Query); err != nil {
					return err
				}
			}
		}
		if s.Set != nil {
			return set(s.Set.Root)
		}
		for _, ref := range s.From {
			if err := statement(ref.Query); err != nil {
				return err
			}
			if ref.On != nil {
				if err := predicate(ref.On.Expr); err != nil {
					return err
				}
			}
		}
		if err := predicate(s.Where); err != nil {
			return err
		}
		if err := predicate(s.Having); err != nil {
			return err
		}
		for _, column := range s.Columns {
			if err := scalar(column.Scalar); err != nil {
				return err
			}
		}
		for _, order := range s.OrderBy {
			if err := scalar(order.Scalar); err != nil {
				return err
			}
		}
		return nil
	}
	if visit == nil {
		return nil
	}
	return statement(root)
}
