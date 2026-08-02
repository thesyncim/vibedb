package query

import (
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// statementCorrelatedKey is one top-level child equality proved to consume one
// exact correlation occurrence. Keeping the AST pointers in cold statement
// state lets diagnostics and tests retain authored identity; execution uses
// the rendered markKeys below and never revisits the parser tree.
type statementCorrelatedKey struct {
	outer   *sqlast.PathExpr
	inner   *sqlast.PathExpr
	binding int
}

// statementDecorrelatedExists is the cold proof retained by one prepared SQL
// statement. The historical name remains because the single-key EXISTS path is
// deliberately unchanged; mark distinguishes the grouped operators added for
// composite EXISTS and value subqueries. local is a private clone with child
// placeholder ordinals rebased into the owning statement, so lowering never
// mutates the parser tree.
type statementDecorrelatedExists struct {
	expression *sqlast.Expr
	predicate  *sqlast.Expr
	exists     *sqlast.Expr
	subquery   *sqlast.SelectStmt
	keys       []statementCorrelatedKey
	markKeys   []correlatedMarkKey
	outer      *sqlast.PathExpr
	inner      *sqlast.PathExpr
	probe      *sqlast.PathExpr
	project    *sqlast.PathExpr
	local      *sqlast.Expr
	collection string
	kind       correlatedMarkKind
	op         Op
	mark       bool
	anti       bool
}

func (s *Statement) numDecorrelatedExists() int {
	if s == nil || s.nested == nil {
		return 0
	}
	n := 0
	for i := range s.nested.decorrelated {
		if !s.nested.decorrelated[i].mark {
			n++
		}
	}
	return n
}

func (s *Statement) hasDecorrelatedExists() bool {
	return s != nil && s.nested != nil && len(s.nested.decorrelated) != 0
}

// onlyDecorrelatedExists is the narrow cache proof for a parameter-free
// statement whose sole cold feature is one or more hidden semi/anti joins or
// grouped marks. The compiled plan contains no execution-produced literal or
// relation spool: its adaptive bindings still belong to Exec and are filled
// for every snapshot. Every established nested path stays uncached.
func (n *nestedStatements) onlyDecorrelatedExists() bool {
	return n != nil && len(n.decorrelated) != 0 && len(n.subqueries) == 0 &&
		n.derived == nil && n.relationJoin == nil && n.window == nil &&
		n.set == nil && n.scalar == nil && n.ctes == nil && n.cte == nil &&
		!n.ownsCTEs
}

func (s *Statement) decorrelatedExistsFor(
	expression *sqlast.Expr,
) *statementDecorrelatedExists {
	if s == nil || s.nested == nil || expression == nil {
		return nil
	}
	for i := range s.nested.decorrelated {
		if s.nested.decorrelated[i].expression == expression ||
			s.nested.decorrelated[i].predicate == expression ||
			s.nested.decorrelated[i].exists == expression {
			return &s.nested.decorrelated[i]
		}
	}
	return nil
}

// prepareDecorrelatedExists proves every supported correlated predicate
// subquery before ordinary subquery preparation can assume execute-once
// semantics. Only direct top-level WHERE conjuncts, optionally beneath exactly
// one direct NOT, are candidates. Every other correlated sidecar is rejected
// by collectSubqueries at its authored position.
func (s *Statement) prepareDecorrelatedExists() error {
	if s == nil || s.tree == nil || s.tree.Where == nil {
		return nil
	}
	conjuncts := []*sqlast.Expr{s.tree.Where}
	if s.tree.Where.Kind == sqlast.ExprAnd {
		conjuncts = s.tree.Where.Kids
	}
	for _, conjunct := range conjuncts {
		predicate, directNot := directCorrelatedPredicate(conjunct)
		if predicate == nil || predicate.Subquery == nil ||
			predicate.Subquery.Correlation == nil {
			continue
		}
		proved, err := s.proveCorrelatedPredicate(conjunct, predicate, directNot)
		if err != nil {
			return err
		}
		s.ensureNested().decorrelated = append(
			s.ensureNested().decorrelated, proved,
		)
	}
	return nil
}

func directCorrelatedPredicate(expression *sqlast.Expr) (*sqlast.Expr, bool) {
	if expression == nil {
		return nil, false
	}
	if expression.Kind == sqlast.ExprNot && len(expression.Kids) == 1 &&
		expression.Kids[0] != nil {
		return expression.Kids[0], true
	}
	return expression, false
}

func (s *Statement) proveCorrelatedPredicate(
	authored, predicate *sqlast.Expr,
	directNot bool,
) (statementDecorrelatedExists, error) {
	fail := func(pos int, reason string) (statementDecorrelatedExists, error) {
		return statementDecorrelatedExists{}, sqlast.NewFeatureNotSupportedError(
			s.text, pos, reason,
		)
	}
	child := predicate.Subquery
	spec := child.Correlation
	if spec == nil || len(spec.Bindings) == 0 {
		return fail(predicate.Pos,
			"correlated predicate subquery has no validated outer-reference metadata")
	}
	if len(s.tree.From) != 1 || s.tree.From[0].Kind != sqlast.RelationCollection {
		return fail(authored.Pos,
			"correlated predicate decorrelation requires exactly one outer physical relation")
	}
	if child.Set != nil || child.With != nil || len(child.From) != 1 ||
		child.From[0].Kind != sqlast.RelationCollection || child.From[0].Name == "" {
		return fail(correlatedChildRelationPosition(child, spec.Pos),
			"correlated predicate decorrelation requires exactly one inner physical relation")
	}
	if child.Distinct || len(child.GroupBy) != 0 || child.Having != nil ||
		selectHasWindows(child) || len(child.OrderBy) != 0 ||
		child.Limit != nil || child.Offset != nil {
		return fail(correlatedChildTailPosition(child, spec.Pos),
			"correlated predicate grouping, windows, distinctness, ordering, and row-count tails need an APPLY plan")
	}
	if pos, ok := firstNestedPredicateSubqueryPos(child.Where); ok {
		return fail(pos,
			"nested correlated predicate subqueries need a separately proved APPLY plan")
	}
	if pos, ok := firstProjectionAggregatePos(child); ok {
		return fail(pos,
			"a correlated aggregate projection changes empty-input cardinality and needs an APPLY plan")
	}
	if err := validateCorrelationSpec(spec); err != nil {
		return fail(spec.Pos, err.Error())
	}

	kind, op, probe, project, err := correlatedPredicateShape(predicate, directNot)
	if err != nil {
		return fail(predicate.Pos, err.Error())
	}
	if kind == correlatedMarkIn || kind == correlatedMarkNotIn ||
		kind == correlatedMarkScalar {
		project, err = correlatedValueProjection(child)
		if err != nil {
			return fail(correlatedProjectionPosition(child, spec.Pos), err.Error())
		}
	}

	terms := []*sqlast.Expr{child.Where}
	if child.Where != nil && child.Where.Kind == sqlast.ExprAnd {
		terms = child.Where.Kids
	}
	keys := make([]statementCorrelatedKey, 0, len(spec.References))
	locals := make([]*sqlast.Expr, 0, len(terms))
	for _, term := range terms {
		if exprUsesCorrelation(term, spec) {
			if term == nil || term.Kind != sqlast.ExprCompare ||
				term.Op != sqlast.OpEq || term.Path == nil || term.RightPath == nil {
				return fail(termPosition(term, spec.Pos),
					"correlation must be one or more top-level inner = outer path equalities")
			}
			leftBinding := correlationReferenceBinding(spec, term.Path)
			rightBinding := correlationReferenceBinding(spec, term.RightPath)
			if (leftBinding >= 0) == (rightBinding >= 0) {
				return fail(term.Pos,
					"a correlation equality must compare one inner path with one outer path")
			}
			outer, inner, binding := term.RightPath, term.Path, rightBinding
			if leftBinding >= 0 {
				outer, inner, binding = term.Path, term.RightPath, leftBinding
			}
			if inner.Source != 0 || inner.MergedUsing != 0 ||
				len(inner.Segments) == 0 || len(outer.Segments) == 0 {
				return fail(term.Pos,
					"correlation equalities require non-root paths in the single inner and outer relations")
			}
			keys = append(keys, statementCorrelatedKey{
				outer: outer, inner: inner, binding: binding,
			})
			continue
		}
		if err := validateInnerOnlyPredicate(term, spec); err != nil {
			return fail(termPosition(term, spec.Pos), err.Error())
		}
		clone, err := cloneDecorrelatedPredicate(term, child.ParamBase)
		if err != nil {
			return fail(termPosition(term, spec.Pos), err.Error())
		}
		locals = append(locals, clone)
	}
	if len(keys) == 0 {
		return fail(firstUnconsumedCorrelationPosition(spec, keys, spec.Pos),
			"correlated predicate requires at least one top-level inner = outer path equality")
	}
	if len(keys) != len(spec.References) || !allCorrelationBindingsConsumed(spec, keys) {
		return fail(firstUnconsumedCorrelationPosition(spec, keys, spec.Pos),
			"every captured outer reference must be consumed by a correlation equality")
	}
	var local *sqlast.Expr
	switch len(locals) {
	case 0:
	case 1:
		local = locals[0]
	default:
		local = &sqlast.Expr{Kind: sqlast.ExprAnd, Kids: locals, Column: -1, Pos: child.Where.Pos}
	}
	mark := kind == correlatedMarkIn || kind == correlatedMarkNotIn ||
		kind == correlatedMarkScalar || len(keys) > 1
	markKeys := make([]correlatedMarkKey, len(keys))
	for i := range keys {
		markKeys[i] = correlatedMarkKey{
			outer: s.spec(keys[i].outer),
			inner: s.localSpec(keys[i].inner),
		}
	}
	var exists *sqlast.Expr
	if predicate.Kind == sqlast.ExprExists {
		exists = predicate
	}
	return statementDecorrelatedExists{
		expression: authored,
		predicate:  predicate,
		exists:     exists,
		subquery:   child,
		keys:       keys,
		markKeys:   markKeys,
		outer:      keys[0].outer,
		inner:      keys[0].inner,
		probe:      probe,
		project:    project,
		local:      local,
		collection: child.From[0].Name,
		kind:       kind,
		op:         op,
		mark:       mark,
		anti:       kind == correlatedMarkNotExists,
	}, nil
}

func correlatedPredicateShape(
	predicate *sqlast.Expr,
	directNot bool,
) (correlatedMarkKind, Op, *sqlast.PathExpr, *sqlast.PathExpr, error) {
	if predicate == nil || predicate.Subquery == nil {
		return 0, Eq, nil, nil,
			fmt.Errorf("correlated predicate subquery metadata is incomplete")
	}
	switch predicate.Kind {
	case sqlast.ExprExists:
		if directNot {
			return correlatedMarkNotExists, Eq, nil, nil, nil
		}
		return correlatedMarkExists, Eq, nil, nil, nil
	case sqlast.ExprIn:
		if predicate.Agg != sqlast.AggNone || predicate.Path == nil ||
			predicate.Path.Source != 0 || predicate.Path.MergedUsing != 0 ||
			len(predicate.Path.Segments) == 0 {
			return 0, Eq, nil, nil,
				fmt.Errorf("correlated IN requires one outer-relation probe path")
		}
		negated := predicate.Negated != directNot
		if negated {
			return correlatedMarkNotIn, Eq, predicate.Path, nil, nil
		}
		return correlatedMarkIn, Eq, predicate.Path, nil, nil
	case sqlast.ExprCompare:
		if predicate.Agg != sqlast.AggNone || predicate.Path == nil ||
			predicate.Path.Source != 0 || predicate.Path.MergedUsing != 0 ||
			predicate.RightPath != nil || len(predicate.Path.Segments) == 0 {
			return 0, Eq, nil, nil,
				fmt.Errorf("correlated scalar comparison requires one outer-relation probe path")
		}
		op := Op(predicate.Op)
		if directNot {
			op = invertComparisonOp(op)
		}
		return correlatedMarkScalar, op, predicate.Path, nil, nil
	default:
		return 0, Eq, nil, nil,
			fmt.Errorf("only EXISTS, IN, NOT IN, and path-to-scalar comparisons can be decorrelated")
	}
}

func invertComparisonOp(op Op) Op {
	switch op {
	case Eq:
		return Ne
	case Ne:
		return Eq
	case Lt:
		return Ge
	case Le:
		return Gt
	case Gt:
		return Le
	case Ge:
		return Lt
	default:
		return op
	}
}

func correlatedValueProjection(child *sqlast.SelectStmt) (*sqlast.PathExpr, error) {
	if child == nil || len(child.Columns) != 1 {
		return nil, fmt.Errorf(
			"a correlated IN or scalar subquery must project exactly one path")
	}
	column := &child.Columns[0]
	if column.Agg != sqlast.AggNone || column.Path == nil ||
		column.Scalar != nil || column.Window != nil || column.Path.Source != 0 ||
		column.Path.MergedUsing != 0 || len(column.Path.Segments) == 0 {
		return nil, fmt.Errorf(
			"a correlated IN or scalar subquery must project exactly one non-aggregate inner path")
	}
	return column.Path, nil
}

func allCorrelationBindingsConsumed(
	spec *sqlast.CorrelationSpec,
	keys []statementCorrelatedKey,
) bool {
	for binding := range spec.Bindings {
		found := false
		for i := range keys {
			if keys[i].binding == binding {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func firstUnconsumedCorrelationPosition(
	spec *sqlast.CorrelationSpec,
	keys []statementCorrelatedKey,
	fallback int,
) int {
	if spec == nil {
		return fallback
	}
	for i := range spec.References {
		reference := &spec.References[i]
		consumed := false
		for k := range keys {
			if keys[k].outer == reference.Path {
				consumed = true
				break
			}
		}
		if !consumed && reference.Path != nil {
			return reference.Path.Pos
		}
	}
	for binding := range spec.Bindings {
		consumed := false
		for k := range keys {
			if keys[k].binding == binding {
				consumed = true
				break
			}
		}
		if !consumed {
			return spec.Bindings[binding].Pos
		}
	}
	return fallback
}

// firstProjectionAggregatePos finds the first aggregate authored in the first
// aggregate-bearing result column. This proof is deliberately independent of
// whether the projection is otherwise observable: an ungrouped aggregate emits
// one row for empty input, so erasing the projection before EXISTS evaluation
// can invert both EXISTS and NOT EXISTS. The traversal is cold and allocation
// free; ordinary and uncorrelated statements never call it.
func firstProjectionAggregatePos(tree *sqlast.SelectStmt) (int, bool) {
	if tree == nil {
		return 0, false
	}
	for i := range tree.Columns {
		column := &tree.Columns[i]
		if column.Agg != sqlast.AggNone {
			return column.Pos, true
		}
		if pos, ok := firstScalarAggregatePos(column.Scalar); ok {
			return pos, true
		}
	}
	return 0, false
}

func firstScalarAggregatePos(expr *sqlast.ScalarExpr) (int, bool) {
	if expr == nil {
		return 0, false
	}
	best, found := 0, false
	record := func(pos int, ok bool) {
		if ok && (!found || pos < best) {
			best, found = pos, true
		}
	}
	if expr.Kind == sqlast.ScalarAggregate {
		record(expr.Pos, true)
	}
	record(firstScalarAggregatePos(expr.Left))
	record(firstScalarAggregatePos(expr.Right))
	for i := range expr.Whens {
		record(firstPredicateAggregatePos(expr.Whens[i].Predicate))
		record(firstScalarAggregatePos(expr.Whens[i].Match))
		record(firstScalarAggregatePos(expr.Whens[i].Result))
	}
	record(firstScalarAggregatePos(expr.Else))
	return best, found
}

func firstPredicateAggregatePos(expr *sqlast.Expr) (int, bool) {
	if expr == nil {
		return 0, false
	}
	best, found := 0, false
	record := func(pos int, ok bool) {
		if ok && (!found || pos < best) {
			best, found = pos, true
		}
	}
	if expr.Agg != sqlast.AggNone {
		record(expr.Pos, true)
	}
	record(firstScalarAggregatePos(expr.ScalarLeft))
	record(firstScalarAggregatePos(expr.ScalarRight))
	for _, kid := range expr.Kids {
		record(firstPredicateAggregatePos(kid))
	}
	return best, found
}

func firstNestedPredicateSubqueryPos(expr *sqlast.Expr) (int, bool) {
	if expr == nil {
		return 0, false
	}
	if expr.Subquery != nil {
		return expr.Pos, true
	}
	for _, kid := range expr.Kids {
		if pos, ok := firstNestedPredicateSubqueryPos(kid); ok {
			return pos, true
		}
	}
	return 0, false
}

func correlatedChildRelationPosition(child *sqlast.SelectStmt, fallback int) int {
	if child == nil {
		return fallback
	}
	if child.Set != nil {
		return child.Set.Pos
	}
	if child.With != nil {
		return child.With.Pos
	}
	if len(child.From) > 1 {
		return child.From[1].Pos
	}
	if len(child.From) == 1 {
		return child.From[0].Pos
	}
	return fallback
}

func correlatedChildTailPosition(child *sqlast.SelectStmt, fallback int) int {
	best := -1
	record := func(pos int) {
		if pos >= 0 && (best < 0 || pos < best) {
			best = pos
		}
	}
	if child == nil {
		return fallback
	}
	// DISTINCT has no dedicated AST position. The first projection is the
	// narrowest retained location in that clause; otherwise use the subquery.
	if child.Distinct {
		if len(child.Columns) != 0 {
			record(child.Columns[0].Pos)
		} else {
			record(fallback)
		}
	}
	if len(child.GroupBy) != 0 && child.GroupBy[0] != nil {
		record(child.GroupBy[0].Pos)
	}
	if child.Having != nil {
		record(child.Having.Pos)
	}
	for i := range child.Columns {
		if child.Columns[i].Window != nil {
			record(child.Columns[i].Window.Pos)
		}
	}
	if len(child.Windows) != 0 {
		record(child.Windows[0].Pos)
	}
	if len(child.OrderBy) != 0 {
		record(child.OrderBy[0].Pos)
	}
	if child.Limit != nil {
		record(child.Limit.Pos)
	}
	if child.Offset != nil {
		record(child.Offset.Pos)
	}
	if best >= 0 {
		return best
	}
	return fallback
}

func correlatedProjectionPosition(child *sqlast.SelectStmt, fallback int) int {
	if child != nil {
		if len(child.Columns) > 1 {
			return child.Columns[1].Pos
		}
		if len(child.Columns) != 0 {
			return child.Columns[0].Pos
		}
	}
	return fallback
}

func validateCorrelationSpec(spec *sqlast.CorrelationSpec) error {
	if spec == nil {
		return fmt.Errorf("correlation metadata is absent")
	}
	for i := range spec.Bindings {
		binding := &spec.Bindings[i]
		if binding.Depth != 1 || binding.Source != 0 ||
			len(binding.Segments) == 0 {
			return fmt.Errorf(
				"correlation metadata does not identify a non-root path in the single outer relation")
		}
	}
	for i := range spec.References {
		reference := &spec.References[i]
		if reference.Path == nil || reference.Binding < 0 ||
			reference.Binding >= len(spec.Bindings) {
			return fmt.Errorf("correlation reference metadata is invalid")
		}
		for prior := 0; prior < i; prior++ {
			if spec.References[prior].Path == reference.Path {
				return fmt.Errorf("one correlated path occurrence maps to multiple references")
			}
		}
		binding := &spec.Bindings[reference.Binding]
		if binding.Depth != 1 || binding.Source != 0 ||
			binding.Source != reference.Path.Source ||
			!lateralSegmentsEqual(binding.Segments, reference.Path.Segments) {
			return fmt.Errorf("correlated path metadata does not identify the single outer relation")
		}
	}
	return nil
}

func correlationReferenceBinding(spec *sqlast.CorrelationSpec, path *sqlast.PathExpr) int {
	if spec == nil || path == nil {
		return -1
	}
	for i := range spec.References {
		if spec.References[i].Path == path {
			return spec.References[i].Binding
		}
	}
	return -1
}

func correlationReference(spec *sqlast.CorrelationSpec, path *sqlast.PathExpr) bool {
	return correlationReferenceBinding(spec, path) >= 0
}

func exprUsesCorrelation(expr *sqlast.Expr, spec *sqlast.CorrelationSpec) bool {
	if expr == nil {
		return false
	}
	if correlationReference(spec, expr.Path) || correlationReference(spec, expr.RightPath) {
		return true
	}
	if scalarUsesCorrelation(expr.ScalarLeft, spec) ||
		scalarUsesCorrelation(expr.ScalarRight, spec) {
		return true
	}
	for _, kid := range expr.Kids {
		if exprUsesCorrelation(kid, spec) {
			return true
		}
	}
	return false
}

func scalarUsesCorrelation(
	expr *sqlast.ScalarExpr,
	spec *sqlast.CorrelationSpec,
) bool {
	if expr == nil {
		return false
	}
	if correlationReference(spec, expr.Path) ||
		scalarUsesCorrelation(expr.Left, spec) ||
		scalarUsesCorrelation(expr.Right, spec) ||
		scalarUsesCorrelation(expr.Else, spec) {
		return true
	}
	for i := range expr.Whens {
		when := &expr.Whens[i]
		if exprUsesCorrelation(when.Predicate, spec) ||
			scalarUsesCorrelation(when.Match, spec) ||
			scalarUsesCorrelation(when.Result, spec) {
			return true
		}
	}
	return false
}

func validateInnerOnlyPredicate(expr *sqlast.Expr, spec *sqlast.CorrelationSpec) error {
	if expr == nil {
		return fmt.Errorf("correlated predicate contains an empty residual predicate")
	}
	if expr.Subquery != nil || expr.Kind == sqlast.ExprExists {
		return fmt.Errorf("correlated predicate residual subqueries need an APPLY plan")
	}
	if expr.ScalarLeft != nil || expr.ScalarRight != nil || expr.Agg != sqlast.AggNone {
		return fmt.Errorf("correlated predicate residual scalar/aggregate expressions need an APPLY plan")
	}
	for _, path := range []*sqlast.PathExpr{expr.Path, expr.RightPath} {
		if path == nil {
			continue
		}
		if correlationReference(spec, path) || path.Source != 0 || path.MergedUsing != 0 {
			return fmt.Errorf("correlated predicate residual predicates must be inner-only")
		}
	}
	if expr.RightPath != nil {
		return fmt.Errorf("correlated predicate residual path-to-path comparisons need an APPLY plan")
	}
	for _, kid := range expr.Kids {
		if err := validateInnerOnlyPredicate(kid, spec); err != nil {
			return err
		}
	}
	return nil
}

func cloneDecorrelatedPredicate(expr *sqlast.Expr, base int) (*sqlast.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	clone := *expr
	var err error
	clone.Value, err = rebaseDecorrelatedOperand(clone.Value, base)
	if err != nil {
		return nil, err
	}
	if len(expr.List) != 0 {
		clone.List = make([]sqlast.Operand, len(expr.List))
		for i := range expr.List {
			clone.List[i], err = rebaseDecorrelatedOperand(expr.List[i], base)
			if err != nil {
				return nil, err
			}
		}
	}
	if len(expr.Kids) != 0 {
		clone.Kids = make([]*sqlast.Expr, len(expr.Kids))
		for i := range expr.Kids {
			clone.Kids[i], err = cloneDecorrelatedPredicate(expr.Kids[i], base)
			if err != nil {
				return nil, err
			}
		}
	}
	return &clone, nil
}

func rebaseDecorrelatedOperand(operand sqlast.Operand, base int) (sqlast.Operand, error) {
	if operand.Kind != sqlast.OperandParam {
		return operand, nil
	}
	if base < 0 || operand.Ordinal < 0 || base > math.MaxInt-operand.Ordinal {
		return sqlast.Operand{}, fmt.Errorf("correlated predicate placeholder range overflows")
	}
	operand.Ordinal += base
	return operand, nil
}

func termPosition(expr *sqlast.Expr, fallback int) int {
	if expr != nil {
		return expr.Pos
	}
	return fallback
}

func (s *Statement) buildDecorrelatedExists(args []any) error {
	if s == nil || s.nested == nil {
		return nil
	}
	for i := range s.nested.decorrelated {
		proved := &s.nested.decorrelated[i]
		if proved.mark {
			mark := correlatedMark{
				collection: proved.collection,
				keys:       proved.markKeys,
				kind:       proved.kind,
				op:         proved.op,
			}
			if proved.probe != nil {
				mark.probe = s.spec(proved.probe)
			}
			if proved.project != nil {
				mark.project = s.localSpec(proved.project)
			}
			if proved.local != nil {
				s.joinFilter = true
				local, err := s.lowerNode(proved.local, true, args)
				s.joinFilter = false
				if err != nil {
					return err
				}
				mark.where, mark.hasWhere = local, true
			}
			s.q.marks = append(s.q.marks, mark)
			continue
		}
		join := JoinOn(
			proved.collection,
			s.spec(proved.outer),
			s.localSpec(proved.inner),
		)
		join.anti = proved.anti
		join.origin = joinOriginDecorrelatedExists
		if proved.local != nil {
			s.joinFilter = true
			local, err := s.lowerNode(proved.local, true, args)
			s.joinFilter = false
			if err != nil {
				return err
			}
			join.where, join.hasWhere = local, true
		}
		s.q.joins = append(s.q.joins, join)
	}
	return nil
}
