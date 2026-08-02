package query

import (
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// statementDecorrelatedExists is the cold proof retained by one prepared SQL
// statement. local is a private clone with child placeholder ordinals rebased
// into the owning statement; the parser tree remains immutable.
type statementDecorrelatedExists struct {
	expression *sqlast.Expr
	exists     *sqlast.Expr
	subquery   *sqlast.SelectStmt
	outer      *sqlast.PathExpr
	inner      *sqlast.PathExpr
	local      *sqlast.Expr
	collection string
	anti       bool
}

func (s *Statement) numDecorrelatedExists() int {
	if s == nil || s.nested == nil {
		return 0
	}
	return len(s.nested.decorrelated)
}

func (s *Statement) hasDecorrelatedExists() bool {
	return s.numDecorrelatedExists() != 0
}

// onlyDecorrelatedExists is the narrow cache proof for a parameter-free
// statement whose sole cold feature is one or more hidden semi/anti joins. The
// compiled plan contains no execution-produced literal or relation spool: its
// adaptive bindings still belong to Exec and are filled for every snapshot.
// Every established nested path stays uncached.
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
			s.nested.decorrelated[i].exists == expression ||
			expression.Subquery != nil &&
				s.nested.decorrelated[i].subquery == expression.Subquery {
			return &s.nested.decorrelated[i]
		}
	}
	return nil
}

// prepareDecorrelatedExists proves every correlated predicate subquery before
// ordinary subquery preparation can assume execute-once semantics. Only direct
// top-level WHERE conjuncts are candidates; every other correlated sidecar is
// rejected by collectSubqueries with its authored position.
func (s *Statement) prepareDecorrelatedExists() error {
	if s == nil || s.tree == nil || s.tree.Where == nil {
		return nil
	}
	conjuncts := []*sqlast.Expr{s.tree.Where}
	if s.tree.Where.Kind == sqlast.ExprAnd {
		conjuncts = s.tree.Where.Kids
	}
	for _, conjunct := range conjuncts {
		exists, anti := directCorrelatedExists(conjunct)
		if exists == nil || exists.Subquery == nil || exists.Subquery.Correlation == nil {
			continue
		}
		proved, err := s.proveCorrelatedExists(conjunct, exists, anti)
		if err != nil {
			return err
		}
		s.ensureNested().decorrelated = append(
			s.ensureNested().decorrelated, proved,
		)
	}
	return nil
}

func directCorrelatedExists(expression *sqlast.Expr) (*sqlast.Expr, bool) {
	if expression == nil {
		return nil, false
	}
	if expression.Kind == sqlast.ExprExists {
		return expression, false
	}
	if expression.Kind == sqlast.ExprNot && len(expression.Kids) == 1 &&
		expression.Kids[0] != nil && expression.Kids[0].Kind == sqlast.ExprExists {
		return expression.Kids[0], true
	}
	return nil, false
}

func (s *Statement) proveCorrelatedExists(
	authored, exists *sqlast.Expr,
	anti bool,
) (statementDecorrelatedExists, error) {
	fail := func(pos int, reason string) (statementDecorrelatedExists, error) {
		return statementDecorrelatedExists{}, sqlast.NewFeatureNotSupportedError(
			s.text, pos, reason,
		)
	}
	child := exists.Subquery
	spec := child.Correlation
	if spec == nil || len(spec.Bindings) == 0 || len(spec.References) == 0 {
		return fail(exists.Pos, "correlated EXISTS has no validated outer-reference metadata")
	}
	if pos, ok := firstProjectionAggregatePos(child); ok {
		return fail(pos,
			"a correlated EXISTS aggregate projection changes empty-input cardinality and needs an APPLY plan")
	}
	if len(s.tree.From) != 1 || s.tree.From[0].Kind != sqlast.RelationCollection {
		return fail(authored.Pos,
			"correlated EXISTS decorrelation requires exactly one outer physical relation")
	}
	if child.Set != nil || child.With != nil || len(child.From) != 1 ||
		child.From[0].Kind != sqlast.RelationCollection || child.From[0].Name == "" {
		return fail(spec.Pos,
			"correlated EXISTS decorrelation requires exactly one inner physical relation")
	}
	if child.Distinct || len(child.GroupBy) != 0 || child.Having != nil ||
		len(child.Windows) != 0 || len(child.OrderBy) != 0 ||
		child.Limit != nil || child.Offset != nil {
		return fail(spec.Pos,
			"correlated EXISTS grouping, windows, ordering, and row-count tails need an APPLY plan")
	}
	if err := validateCorrelationSpec(spec); err != nil {
		return fail(spec.Pos, err.Error())
	}

	terms := []*sqlast.Expr{child.Where}
	if child.Where != nil && child.Where.Kind == sqlast.ExprAnd {
		terms = child.Where.Kids
	}
	correlated := -1
	var outer, inner *sqlast.PathExpr
	for i, term := range terms {
		if !exprUsesCorrelation(term, spec) {
			continue
		}
		if correlated >= 0 || term == nil || term.Kind != sqlast.ExprCompare ||
			term.Op != sqlast.OpEq || term.Path == nil || term.RightPath == nil {
			return fail(termPosition(term, spec.Pos),
				"correlated EXISTS requires exactly one top-level local = outer path equality")
		}
		leftOuter := correlationReference(spec, term.Path)
		rightOuter := correlationReference(spec, term.RightPath)
		if leftOuter == rightOuter {
			return fail(term.Pos,
				"correlated EXISTS equality must compare one inner path with one outer path")
		}
		if leftOuter {
			outer, inner = term.Path, term.RightPath
		} else {
			outer, inner = term.RightPath, term.Path
		}
		if inner.Source != 0 || inner.MergedUsing != 0 || len(inner.Segments) == 0 ||
			len(outer.Segments) == 0 {
			return fail(term.Pos,
				"correlated EXISTS equality requires non-root value paths in the single inner and outer relations")
		}
		correlated = i
	}
	if correlated < 0 {
		return fail(spec.Pos,
			"correlated EXISTS requires one top-level local = outer path equality")
	}
	// The complete metadata must be consumed by that one equality occurrence.
	// A second projection/predicate capture would otherwise be silently dropped.
	if len(spec.Bindings) != 1 || len(spec.References) != 1 ||
		spec.References[0].Path != outer {
		return fail(spec.Pos,
			"correlated EXISTS contains outer references beyond its single equality key")
	}

	locals := make([]*sqlast.Expr, 0, len(terms)-1)
	for i, term := range terms {
		if i == correlated {
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
	var local *sqlast.Expr
	switch len(locals) {
	case 0:
	case 1:
		local = locals[0]
	default:
		local = &sqlast.Expr{Kind: sqlast.ExprAnd, Kids: locals, Column: -1, Pos: child.Where.Pos}
	}
	return statementDecorrelatedExists{
		expression: authored,
		exists:     exists,
		subquery:   child,
		outer:      outer,
		inner:      inner,
		local:      local,
		collection: child.From[0].Name,
		anti:       anti,
	}, nil
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

func validateCorrelationSpec(spec *sqlast.CorrelationSpec) error {
	if spec == nil {
		return fmt.Errorf("correlation metadata is absent")
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

func correlationReference(spec *sqlast.CorrelationSpec, path *sqlast.PathExpr) bool {
	if spec == nil || path == nil {
		return false
	}
	for i := range spec.References {
		if spec.References[i].Path == path {
			return true
		}
	}
	return false
}

func exprUsesCorrelation(expr *sqlast.Expr, spec *sqlast.CorrelationSpec) bool {
	if expr == nil {
		return false
	}
	if correlationReference(spec, expr.Path) || correlationReference(spec, expr.RightPath) {
		return true
	}
	for _, kid := range expr.Kids {
		if exprUsesCorrelation(kid, spec) {
			return true
		}
	}
	return false
}

func validateInnerOnlyPredicate(expr *sqlast.Expr, spec *sqlast.CorrelationSpec) error {
	if expr == nil {
		return fmt.Errorf("correlated EXISTS contains an empty residual predicate")
	}
	if expr.Subquery != nil || expr.Kind == sqlast.ExprExists {
		return fmt.Errorf("correlated EXISTS residual subqueries need an APPLY plan")
	}
	if expr.ScalarLeft != nil || expr.ScalarRight != nil || expr.Agg != sqlast.AggNone {
		return fmt.Errorf("correlated EXISTS residual scalar/aggregate expressions need an APPLY plan")
	}
	for _, path := range []*sqlast.PathExpr{expr.Path, expr.RightPath} {
		if path == nil {
			continue
		}
		if correlationReference(spec, path) || path.Source != 0 || path.MergedUsing != 0 {
			return fmt.Errorf("correlated EXISTS residual predicates must be inner-only")
		}
	}
	if expr.RightPath != nil {
		return fmt.Errorf("correlated EXISTS residual path-to-path comparisons need an APPLY plan")
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
		return sqlast.Operand{}, fmt.Errorf("correlated EXISTS placeholder range overflows")
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
