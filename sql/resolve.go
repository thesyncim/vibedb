package sql

// Path resolution and the semantic checks that keep an accepted statement
// executable.
//
// Everything here runs after the whole statement has been read, for one
// structural reason: the SELECT list is written before FROM, and a JOIN may
// declare a range variable later still, so no path can be bound to a source at
// the point it is parsed. Deferring all of them to one pass means a path in
// SELECT is resolved by exactly the same rule as a path in HAVING, rather than
// by whatever happened to be known when it was read.

// resolvePaths binds every parsed path to a range variable.
//
// The rule is stated once here and in the package documentation, because it is
// the one place this dialect makes a decision SQL does not have to: SQL knows
// its tables' columns, so `u.address` is unambiguous, while a schemaless
// document may itself contain a field called `u`. The rule is purely syntactic
// and needs no schema:
//
//   - A leading identifier immediately followed by '.' is a range variable if
//     the statement declares one by that name; the rest of the chain is then
//     the path into that source's documents.
//   - Otherwise the whole chain, leading identifier included, is a path into
//     the statement's only source. A statement with more than one source has
//     no "only source", so an unqualified path there is rejected rather than
//     guessed at.
//
// The rule gives range variables priority over top-level fields of the same
// name, which is the only choice that keeps `u.city` meaning what a join author
// expects. The field a range variable shadows is still addressable, and by the
// same rule: qualify it. In a source aliased `u`, the field `u` is `u.u`, and
// its member `city` is `u.u.city`. Nothing becomes unreachable, so the priority
// costs a longer spelling in a rare case rather than expressiveness.
func (p *Parser) resolvePaths() error {
	for i := range p.pending {
		entry := &p.pending[i]
		path := entry.path
		if entry.eligible {
			if source := p.rangeVar(path.Segments[0].Key); source >= 0 {
				path.Source = source
				path.Segments = path.Segments[1:]
				continue
			}
			if entry.star {
				return p.errfAt(path.Pos,
					"%q is not a collection or alias declared in FROM or JOIN, so %q projects nothing",
					path.Segments[0].Key, path.Segments[0].Key+".*")
			}
		}
		if len(p.out.From) != 1 {
			if entry.star {
				return p.errAt(path.Pos,
					"'*' is ambiguous when the statement joins more than one collection; write alias.* to name one")
			}
			return p.errfAt(path.Pos,
				"path %q is unqualified and the statement declares %d collections; qualify it with a range variable, as in %s.%s",
				path.Spec(), len(p.out.From), p.out.From[0].Alias, path.Spec())
		}
		path.Source = 0
	}
	return nil
}

// normalizeRightJoins rewrites RIGHT JOIN into the LEFT JOIN orientation used
// by the shared executor. It runs after path resolution, so swapping the two
// input relations only requires remapping source ordinals; aliases and JSON
// paths retain their original meaning.
//
// The current SQL executor supports one joined relation directly against the
// driving FROM relation. Rejecting a RIGHT JOIN later in a chained statement
// would produce a less useful lowering error after this rewrite, so leave the
// join shape untouched unless it is the first joined relation.
func (p *Parser) normalizeRightJoins() error {
	for i := 1; i < len(p.out.From); i++ {
		if p.out.From[i].Join != JoinRight {
			continue
		}
		if i != 1 || len(p.out.From) != 2 {
			return p.errAt(p.out.From[i].Pos,
				"RIGHT JOIN is supported only for one joined relation; use LEFT JOIN for a chained query")
		}
		condition := p.out.From[i].On
		p.swapSources(0, 1)
		p.out.From[0].Join = JoinNone
		p.out.From[0].On = nil
		p.out.From[1].Join = JoinLeft
		p.out.From[1].On = condition
		return nil
	}
	return nil
}

func (p *Parser) swapSources(a, b int) {
	for i := range p.out.Columns {
		if path := p.out.Columns[i].Path; path != nil {
			path.Source = swappedSource(path.Source, a, b)
		}
	}
	for i := range p.out.GroupBy {
		p.out.GroupBy[i].Source = swappedSource(p.out.GroupBy[i].Source, a, b)
	}
	for i := range p.out.OrderBy {
		p.out.OrderBy[i].Path.Source = swappedSource(p.out.OrderBy[i].Path.Source, a, b)
	}
	remapExprSources(p.out.Where, a, b)
	remapExprSources(p.out.Having, a, b)
	for i := range p.out.From {
		if cond := p.out.From[i].On; cond != nil {
			cond.Left.Source = swappedSource(cond.Left.Source, a, b)
			cond.Right.Source = swappedSource(cond.Right.Source, a, b)
		}
	}
	p.out.From[a], p.out.From[b] = p.out.From[b], p.out.From[a]
}

func swappedSource(source, a, b int) int {
	switch source {
	case a:
		return b
	case b:
		return a
	default:
		return source
	}
}

func remapExprSources(e *Expr, a, b int) {
	if e == nil {
		return
	}
	if e.Path != nil {
		e.Path.Source = swappedSource(e.Path.Source, a, b)
	}
	if e.Subquery != nil {
		// A nested SELECT has its own source namespace.
		return
	}
	for _, kid := range e.Kids {
		remapExprSources(kid, a, b)
	}
}

// rangeVar answers the index of the range variable named name, or -1.
//
// The comparison is case-sensitive, unlike keyword matching. Identifiers here
// are overwhelmingly JSON object keys, and JSON keys are case-sensitive, so
// folding them would make `SELECT Name` and `SELECT name` read the same field
// and one of them silently return nothing. Applying one rule to every
// identifier — range variables included — is what stops the same spelling from
// meaning different things in different clauses.
func (p *Parser) rangeVar(name string) int {
	for i := range p.out.From {
		if p.out.From[i].Alias == name {
			return i
		}
	}
	return -1
}

// validate enforces the plan rules the engine already has, at parse time and
// with a position.
//
// Duplicating them here is deliberate. query enforces the same rules when it
// compiles, but by then the statement text is gone and the error can only name
// a path spec; a driver's user gets "projected path is not a GROUP BY key" with
// a byte offset instead. The rules restated are the ones documented in query's
// own package comment, so they are a contract rather than an implementation
// detail that could drift.
func (p *Parser) validate() error {
	if err := p.validateJoins(); err != nil {
		return err
	}
	grouped := len(p.out.GroupBy) > 0
	aggregates, projections := 0, 0
	firstProjection := -1
	for i := range p.out.Columns {
		if p.out.Columns[i].Agg == AggNone {
			projections++
			if firstProjection < 0 {
				firstProjection = i
			}
			continue
		}
		aggregates++
	}
	if grouped {
		for i := range p.out.Columns {
			column := &p.out.Columns[i]
			if column.Agg != AggNone {
				continue
			}
			if !p.isGroupKey(column.Path) {
				return p.errfAt(column.Path.Pos,
					"projected path %q is not a GROUP BY key: with GROUP BY, one row stands for many, so every projection must be a key or an aggregate",
					column.Path.Spec())
			}
		}
	} else if aggregates > 0 && projections > 0 {
		return p.errAt(p.out.Columns[firstProjection].Pos,
			"a plain path cannot be selected alongside an aggregate without GROUP BY: the aggregate collapses every row into one, and the path has no single value there")
	}
	if err := p.validateOrder(grouped, aggregates > 0); err != nil {
		return err
	}
	return p.validateHaving(grouped, aggregates > 0)
}

func (p *Parser) validateJoins() error {
	for i := 1; i < len(p.out.From); i++ {
		condition := p.out.From[i].On
		left, right := condition.Left, condition.Right
		if len(left.Segments) == 0 || len(right.Segments) == 0 {
			return p.errAt(condition.Pos, "a join key must be a field path, not a whole document")
		}
		if left.Source == right.Source {
			return p.errfAt(condition.Pos,
				"both sides of ON name %q: an equi-join matches a key of one collection against a key of the other",
				p.out.From[left.Source].Alias)
		}
		if left.Source != i && right.Source != i {
			return p.errfAt(condition.Pos,
				"ON does not name %q, the collection this JOIN adds", p.out.From[i].Alias)
		}
		if left.Source == i {
			// Normalizing so Right is always the newly joined side means a
			// lowering pass reads the probe and build sides off the structure
			// instead of re-deriving them from the source indices.
			condition.Left, condition.Right = right, left
			left = condition.Left
		}
		if left.Source > i {
			return p.errfAt(condition.Pos,
				"ON references %q, which this statement joins later; a join condition may only name collections already in scope",
				p.out.From[left.Source].Alias)
		}
	}
	return nil
}

func (p *Parser) validateOrder(grouped, hasAggregate bool) error {
	if len(p.out.OrderBy) == 0 {
		return nil
	}
	if grouped {
		for i := range p.out.OrderBy {
			term := &p.out.OrderBy[i]
			if !p.isGroupKey(term.Path) {
				return p.errfAt(term.Pos,
					"ORDER BY %q is not a GROUP BY key: grouped rows are ordered by their key",
					term.Path.Spec())
			}
		}
		return nil
	}
	if hasAggregate {
		// query's compiler silently drops ORDER BY for a single-row aggregate
		// result. Silently dropping a clause is worse than refusing it: the
		// author wrote an ordering because they believed there were rows to
		// order, and the belief, not the clause, is the mistake.
		return p.errAt(p.out.OrderBy[0].Pos,
			"ORDER BY has no effect on an aggregate without GROUP BY, which returns exactly one row")
	}
	return nil
}

func (p *Parser) validateHaving(grouped, hasAggregate bool) error {
	if p.out.Having == nil {
		return nil
	}
	if !grouped && !hasAggregate {
		return p.errAt(p.out.Having.Pos,
			"HAVING requires GROUP BY or an aggregate: without one there are no groups to filter, and a per-row condition belongs in WHERE")
	}
	return p.bindHaving(p.out.Having)
}

// bindHaving resolves every HAVING leaf to a value the reduction already
// produces, recording which output column that is.
//
// This is what makes a parsed HAVING executable in principle rather than in
// hope. A leaf that tests an aggregate must test one the SELECT list computes,
// because the plan's reductions are exactly its output columns and there is no
// hidden column to add one to; a leaf that tests a plain path must test a
// GROUP BY key, because that is the only per-row value that survives grouping.
// Anything else would need a second aggregation pass, and rejecting it here is
// the difference between an error with a position and a failure at lowering.
func (p *Parser) bindHaving(e *Expr) error {
	if e.Subquery != nil || e.Kind == ExprExists {
		return p.errAt(e.Pos, "subqueries are not supported in HAVING; put the uncorrelated condition in WHERE")
	}
	switch e.Kind {
	case ExprAnd, ExprOr, ExprNot:
		for _, kid := range e.Kids {
			if err := p.bindHaving(kid); err != nil {
				return err
			}
		}
		return nil
	}
	if e.Agg != AggNone {
		column := p.aggregateColumn(e.Agg, e.Path)
		if column < 0 {
			// A nil path is COUNT(*), which has no spec to quote; spelling it
			// back as the author wrote it is what makes the message a fix
			// rather than a puzzle.
			argument := "*"
			if e.Path != nil {
				argument = e.Path.Spec()
			}
			return p.errfAt(e.Pos,
				"HAVING tests %s(%s), which the SELECT list does not compute: add it to the SELECT list",
				e.Agg, argument)
		}
		e.Column = column
		return nil
	}
	if !p.isGroupKey(e.Path) {
		return p.errfAt(e.Pos,
			"HAVING tests %q, which is neither an aggregate nor a GROUP BY key: a condition on a single row's value belongs in WHERE",
			e.Path.Spec())
	}
	e.Column = p.projectionColumn(e.Path)
	return nil
}

// isGroupKey reports whether path is one of the GROUP BY keys.
func (p *Parser) isGroupKey(path *PathExpr) bool {
	for _, key := range p.out.GroupBy {
		if sameSpec(key, path) {
			return true
		}
	}
	return false
}

// aggregateColumn answers the output column computing agg over path, or -1.
func (p *Parser) aggregateColumn(agg AggKind, path *PathExpr) int {
	for i := range p.out.Columns {
		column := &p.out.Columns[i]
		if column.Agg != agg {
			continue
		}
		if column.Path == nil || path == nil {
			if column.Path == path {
				return i // COUNT(*) on both sides
			}
			continue
		}
		if sameSpec(column.Path, path) {
			return i
		}
	}
	return -1
}

// projectionColumn answers the output column projecting path, or -1 when the
// statement groups by a key it does not project.
func (p *Parser) projectionColumn(path *PathExpr) int {
	for i := range p.out.Columns {
		if p.out.Columns[i].Agg == AggNone && sameSpec(p.out.Columns[i].Path, path) {
			return i
		}
	}
	return -1
}
