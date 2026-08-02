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
			if source, depth, ok := p.outerRangeVar(path.Segments[0].Key); ok {
				path.Source = source
				path.Segments = path.Segments[1:]
				p.bindLateralPath(path, depth, source)
				continue
			}
			p.noteLateralForwardCandidate(path, path.Segments[0].Key)
			if entry.star {
				return p.errfAt(path.Pos,
					"%q is not a collection or alias declared in FROM or JOIN, so %q projects nothing",
					path.Segments[0].Key, path.Segments[0].Key+".*")
			}
		}
		if source, merged, ok := p.usingColumnSource(path); ok {
			path.Source = source
			path.MergedUsing = merged
			continue
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

func (p *Parser) outerRangeVar(name string) (source, depth int, ok bool) {
	if p.lateral == nil {
		return 0, 0, false
	}
	depth = 1
	for scope := p.lateral.outerRanges; scope != nil; scope = scope.outer {
		if scope.parser != nil {
			limit := min(scope.limit, len(scope.parser.from))
			for i := 0; i < limit; i++ {
				if scope.parser.from[i].Alias == name {
					return i, depth, true
				}
			}
		}
		depth++
	}
	return 0, 0, false
}

func (p *Parser) bindLateralPath(path *PathExpr, depth, source int) {
	if p.lateral == nil {
		return
	}
	capture := p.lateral.capture
	if capture == nil || capture.owner == nil {
		return
	}
	bindLateralCapture(capture, path, depth, source, true)
	// A nested LATERAL may reach through its parent query to an ancestor of
	// that parent. Propagate the dependency through every owning LATERAL so no
	// intermediate relation is incorrectly marked decorrelated.
	for depth > 1 {
		if capture.owner.lateral == nil {
			break
		}
		capture = capture.owner.lateral.capture
		if capture == nil || capture.owner == nil {
			break
		}
		depth--
		bindLateralCapture(capture, path, depth, source, false)
	}
}

func bindLateralCapture(
	capture *lateralCapture,
	path *PathExpr,
	depth, source int,
	reference bool,
) int {
	state := capture.owner.lateralState()
	bindings := state.bindingScratch[capture.bindingBase:]
	bindingIndex := -1
	for i := range bindings {
		binding := &bindings[i].binding
		if binding.Depth == depth && binding.Source == source &&
			sameSegments(binding.Segments, path.Segments) {
			bindingIndex = i
			break
		}
	}
	if bindingIndex < 0 {
		state.bindingScratch = append(state.bindingScratch, lateralScratchBinding{
			binding: LateralBinding{
				Depth: depth, Source: source, Segments: path.Segments, Pos: path.Pos,
			},
			path: path,
		})
		bindingIndex = len(state.bindingScratch) - capture.bindingBase - 1
	}
	if reference {
		state.referenceScratch = append(state.referenceScratch, lateralScratchReference{
			path: path, binding: bindingIndex,
		})
	}
	return bindingIndex
}

func sameSegments(left, right []Segment) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (p *Parser) isLateralReference(path *PathExpr) bool {
	_, ok := p.lateralReferenceBinding(path)
	return ok
}

func (p *Parser) lateralReferenceBinding(path *PathExpr) (int, bool) {
	if path == nil || p.lateral == nil {
		return 0, false
	}
	capture := p.lateral.capture
	if capture == nil || capture.owner == nil ||
		capture.owner.lateral == nil {
		return 0, false
	}
	references := capture.owner.lateral.referenceScratch[capture.referenceBase:]
	for i := range references {
		if references[i].path == path {
			return references[i].binding, true
		}
	}
	return 0, false
}

func (p *Parser) noteLateralForwardCandidate(path *PathExpr, alias string) {
	if p.lateral == nil {
		return
	}
	capture := p.lateral.capture
	if capture == nil || capture.owner == nil {
		return
	}
	state := capture.owner.lateralState()
	state.forward = append(state.forward, lateralForwardCandidate{
		path: path, alias: alias, source: capture.source,
	})
}

// usingColumnSource resolves the one unqualified column JOIN ... USING adds to
// the joined row.
//
// SQL defines that column as COALESCE(left.key, right.key). MergedUsing records
// the exact join-stage output carrying that value; Source remains the
// accumulated-left binding for consumers that only need name resolution.
// RIGHT and FULL therefore need no AST source rewrite, and repeated USING in a
// chain can feed the prior synthetic merge into the next one.
//
// Only each exact, simple name in the USING list is merged. Qualified spellings
// continue to address their relation's own key.
func (p *Parser) usingColumnSource(path *PathExpr) (source, merged int, ok bool) {
	if len(path.Segments) != 1 || path.Segments[0].IsIndex {
		return 0, 0, false
	}
	// The latest merge wins for a repeated USING name in a chain: it is the
	// output of the accumulated left relation joined with source i.
	for i := len(p.out.From) - 1; i >= 1; i-- {
		ref := &p.out.From[i]
		cond := ref.On
		if cond == nil || !cond.Using {
			continue
		}
		for _, name := range cond.UsingColumns {
			if name == path.Segments[0].Key {
				return cond.Left.Source, i, true
			}
		}
	}
	return 0, 0, false
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
		if p.out.Columns[i].Window != nil {
			continue
		}
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
			if column.Window != nil || column.Agg != AggNone {
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
	if err := p.validateWindows(grouped, aggregates > 0); err != nil {
		return err
	}
	if err := p.validateOrder(grouped, aggregates > 0); err != nil {
		return err
	}
	return p.validateHaving(grouped, aggregates > 0)
}

func (p *Parser) rejectLateralForwardAlias(alias string, source int) error {
	for i := range p.lateral.forward {
		candidate := &p.lateral.forward[i]
		if source < candidate.source || alias != candidate.alias {
			continue
		}
		if source == candidate.source {
			return p.errfAt(candidate.path.Pos,
				"LATERAL derived table %q cannot reference its own output while it is being computed",
				candidate.alias,
			)
		}
		return p.errfAt(candidate.path.Pos,
			"LATERAL reference %q is joined later; only preceding FROM sources are visible",
			candidate.alias,
		)
	}
	return nil
}

func (p *Parser) validateWindows(grouped, hasAggregate bool) error {
	for i := range p.out.Windows {
		if err := p.validateWindowSpecPaths(
			&p.out.Windows[i].Spec, grouped, hasAggregate,
		); err != nil {
			return err
		}
	}
	for i := range p.out.Columns {
		window := p.out.Columns[i].Window
		if window == nil {
			continue
		}
		if err := p.validateWindowPath(
			window.Argument, "window function argument", grouped, hasAggregate,
		); err != nil {
			return err
		}
		if err := p.validateWindowSpecPaths(&window.Spec, grouped, hasAggregate); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) validateWindowSpecPaths(
	spec *WindowSpec,
	grouped, hasAggregate bool,
) error {
	for _, path := range spec.PartitionBy {
		if err := p.validateWindowPath(path, "PARTITION BY", grouped, hasAggregate); err != nil {
			return err
		}
	}
	for i := range spec.OrderBy {
		if err := p.validateWindowPath(
			spec.OrderBy[i].Path, "window ORDER BY", grouped, hasAggregate,
		); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) validateWindowPath(
	path *PathExpr,
	clause string,
	grouped, hasAggregate bool,
) error {
	if path == nil || !grouped && !hasAggregate {
		return nil
	}
	if grouped && p.isGroupKey(path) {
		return nil
	}
	if hasAggregate && !grouped {
		return p.errfAt(path.Pos,
			"%s path %q is unavailable after an aggregate without GROUP BY",
			clause, path.Spec())
	}
	return p.errfAt(path.Pos,
		"%s path %q is not a GROUP BY key and is unavailable to the window stage",
		clause, path.Spec())
}

func (p *Parser) validateJoins() error {
	for i := 1; i < len(p.out.From); i++ {
		ref := &p.out.From[i]
		condition := ref.On
		if ref.Join == JoinCross {
			if condition != nil {
				return p.errAt(ref.Pos, "CROSS JOIN must not carry a condition")
			}
			continue
		}
		if condition == nil {
			return p.errAt(ref.Pos, "JOIN requires ON or USING")
		}
		if condition.Using {
			for _, key := range condition.Keys {
				if len(key.Left.Segments) == 0 || len(key.Right.Segments) == 0 {
					return p.errAt(key.Pos, "a join key must be a field path")
				}
			}
			continue
		}
		if condition.Expr == nil {
			return p.errAt(condition.Pos, "ON requires a predicate")
		}
		var validateExpr func(*Expr) error
		validateExpr = func(expr *Expr) error {
			if expr == nil {
				return nil
			}
			if expr.Subquery != nil || expr.Kind == ExprExists {
				return p.errAt(expr.Pos, "subqueries are not supported in ON")
			}
			for _, path := range []*PathExpr{expr.Path, expr.RightPath} {
				if path == nil || p.isLateralReference(path) {
					continue
				}
				if path.Source > i {
					return p.errfAt(path.Pos,
						"ON references %q, which this statement joins later",
						p.out.From[path.Source].Alias)
				}
			}
			for _, kid := range expr.Kids {
				if err := validateExpr(kid); err != nil {
					return err
				}
			}
			return nil
		}
		if err := validateExpr(condition.Expr); err != nil {
			return err
		}
		base := len(p.joinKeyScratch)
		terms := []*Expr{condition.Expr}
		if condition.Expr.Kind == ExprAnd {
			terms = condition.Expr.Kids
		}
		keyTerms := 0
		for _, term := range terms {
			if term.Kind != ExprCompare || term.Op != OpEq ||
				term.Path == nil || term.RightPath == nil ||
				p.isLateralReference(term.Path) || p.isLateralReference(term.RightPath) {
				continue
			}
			left, right := term.Path, term.RightPath
			if left.Source == i && right.Source < i {
				left, right = right, left
			}
			if left.Source >= i || right.Source != i {
				continue
			}
			if len(left.Segments) == 0 || len(right.Segments) == 0 {
				return p.errAt(term.Pos, "a join key must be a field path, not a whole document")
			}
			p.joinKeyScratch = append(p.joinKeyScratch, JoinKeyCond{
				Left: left, Right: right, Pos: term.Pos,
			})
			keyTerms++
		}
		keys := p.joinKeyScratch[base:]
		if len(keys) != 0 {
			run := p.keys.allocDirty(len(keys))
			copy(run, keys)
			condition.Keys = run
			condition.Left, condition.Right = run[0].Left, run[0].Right
		}
		p.joinKeyScratch = p.joinKeyScratch[:base]
		condition.Residual = keyTerms != len(terms)
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
			if term.Output != 0 {
				continue
			}
			if !p.isGroupKey(term.Path) {
				return p.errfAt(term.Pos,
					"ORDER BY %q is not a GROUP BY key: grouped rows are ordered by their key",
					term.Path.Spec())
			}
		}
		return nil
	}
	if hasAggregate {
		for i := range p.out.OrderBy {
			if p.out.OrderBy[i].Output == 0 {
				// query's compiler silently drops ORDER BY for a single-row
				// aggregate result. A window output may legitimately supply
				// multiple post-aggregate rows only when it is named by Output;
				// every ordinary path remains unavailable here.
				return p.errAt(p.out.OrderBy[i].Pos,
					"ORDER BY has no effect on an aggregate without GROUP BY, which returns exactly one row")
			}
		}
		return nil
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
		if p.sameResolvedPath(key, path) {
			return true
		}
	}
	return false
}

func (p *Parser) sameResolvedPath(left, right *PathExpr) bool {
	leftBinding, leftOuter := p.lateralReferenceBinding(left)
	rightBinding, rightOuter := p.lateralReferenceBinding(right)
	if leftOuter != rightOuter {
		return false
	}
	if leftOuter && leftBinding != rightBinding {
		return false
	}
	return sameSpec(left, right)
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
		if p.sameResolvedPath(column.Path, path) {
			return i
		}
	}
	return -1
}

// projectionColumn answers the output column projecting path, or -1 when the
// statement groups by a key it does not project.
func (p *Parser) projectionColumn(path *PathExpr) int {
	for i := range p.out.Columns {
		if p.out.Columns[i].Agg == AggNone && p.sameResolvedPath(p.out.Columns[i].Path, path) {
			return i
		}
	}
	return -1
}
