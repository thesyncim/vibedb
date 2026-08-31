package query

import (
	"errors"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// sqlPathComparisonError is the source-free runtime diagnostic carried through
// every executor that can evaluate Path <op> Path. Plans retain only the byte
// offset: the owning Statement supplies its one source string at the execution
// boundary, while a negative position preserves definition-owned view errors
// as deliberately unpositioned.
type sqlPathComparisonError struct {
	pos                   int
	left, operator, right string
}

// sqlJoinPathComparisonError is the legacy storage-aware JOIN's source-free
// diagnostic. The execution binding retains only its join ordinal and observed
// domains; Statement recovers authored operand order and the '=' byte from the
// immutable SQL AST at the public execution boundary.
type sqlJoinPathComparisonError struct {
	slot        int
	origin      joinOrigin
	left, right string
}

func (e *sqlJoinPathComparisonError) Error() string {
	if e == nil {
		return "query: undefined SQL join comparison operator"
	}
	return "query: operator does not exist: " + e.left + " = " + e.right
}

func (e *sqlPathComparisonError) Error() string {
	if e == nil {
		return "query: undefined SQL path comparison operator"
	}
	return "query: operator does not exist: " + e.left + " " + e.operator + " " + e.right
}

// compareSQLPathScalars is the one semantic boundary for SQL path-to-path
// comparisons. NULL and an absent path are UNKNOWN; live booleans, exact
// numbers, and decoded strings compare only inside their own domain; JSON
// containers have no implicit PostgreSQL comparison operator in this surface.
func compareSQLPathScalars(
	pos int,
	left scalar,
	op Op,
	right scalar,
) (tri, error) {
	if left.kind == kindNull || right.kind == kindNull {
		return triUnknown, nil
	}
	if left.kind != right.kind || left.kind == kindContainer {
		return triFalse, &sqlPathComparisonError{
			pos: pos, left: sqlComparisonType(left),
			operator: sqlast.CmpOp(op).String(), right: sqlComparisonType(right),
		}
	}
	return boolTri(acceptSign(compareScalar(left, right), op)), nil
}

// hasRuntimeSQLPathComparison is a compile-time walk. Its result is packed
// into plan.runtimeSQLPaths, so ordinary executions never walk a predicate
// tree merely to discover that this SQL-only feature is absent.
func (p *compiledPredicate) hasRuntimeSQLPathComparison() bool {
	if p == nil {
		return false
	}
	if p.kind == predCmpPath || p.kind == predCmpPathBound {
		return true
	}
	for _, child := range p.kids {
		if child.hasRuntimeSQLPathComparison() {
			return true
		}
	}
	return false
}

// requiresSQLDomainScan is the common no-pruning guard. PostgreSQL resolves
// both ON and WHERE operators before row-count tails and before boolean
// evaluation; a runtime-typed document store must therefore inspect every
// reachable live domain before applying candidates, data skipping, or LIMIT.
func (p *plan) requiresSQLDomainScan() bool {
	return p != nil && (p.runtimeSQLPaths || p.hasSQLJoinComparison())
}

// validateSQLPathDomains resolves every authored path comparison for each row
// before the ordinary predicate evaluator may short-circuit it. The traversal
// is row-major, matching the serial runtime's deterministic first-error rule.
// Successful rows allocate nothing; the first incompatible live pair returns
// immediately and constructs the one diagnostic the statement will publish.
func (p *plan) validateSQLPathDomains(
	cols [][]scalar,
	correlations []scalar,
	lo, hi int,
	cancel *CancelFlag,
) error {
	if p == nil || !p.runtimeSQLPaths {
		return nil
	}
	for row := lo; row < hi; row++ {
		if err := cancellationCheckpoint(cancel, row-lo); err != nil {
			return err
		}
		if err := p.where.validateSQLPathDomains(cols, correlations, row); err != nil {
			return err
		}
	}
	return cancellationError(cancel)
}

func (p *compiledPredicate) validateSQLPathDomains(
	cols [][]scalar,
	correlations []scalar,
	row int,
) error {
	if p == nil {
		return nil
	}
	switch p.kind {
	case predCmpPath:
		_, err := compareSQLPathScalars(
			int(p.lit.ival), cols[p.col][row], p.op, cols[p.slot][row],
		)
		return err
	case predCmpPathBound:
		if p.slot < 0 || p.slot >= len(correlations) {
			return nil
		}
		_, err := compareSQLPathScalars(
			int(p.lit.ival), cols[p.col][row], p.op, correlations[p.slot],
		)
		if comparison, ok := err.(*sqlPathComparisonError); ok && p.lit.bval {
			comparison.left, comparison.right = comparison.right, comparison.left
			comparison.operator = p.lit.sval
		}
		return err
	}
	for _, child := range p.kids {
		if err := child.validateSQLPathDomains(cols, correlations, row); err != nil {
			return err
		}
	}
	return nil
}

func sqlComparisonType(value scalar) string {
	switch value.kind {
	case kindBool:
		return "boolean"
	case kindNumber:
		return "numeric"
	case kindString:
		return "text"
	case kindContainer:
		return "json"
	default:
		return "unknown"
	}
}

func (s *Statement) translateSQLPathComparisonError(err error) error {
	if err == nil {
		return nil
	}
	var comparison *sqlPathComparisonError
	if errors.As(err, &comparison) {
		if comparison.pos < 0 {
			return sqlast.NewUnpositionedUndefinedComparisonOperatorError(
				comparison.left, comparison.operator, comparison.right,
			)
		}
		return sqlast.NewUndefinedComparisonOperatorError(
			s.text, comparison.pos,
			comparison.left, comparison.operator, comparison.right,
		)
	}
	var joinComparison *sqlJoinPathComparisonError
	if errors.As(err, &joinComparison) {
		return s.translateSQLJoinPathComparisonError(joinComparison)
	}
	return err
}

func (s *Statement) translateSQLJoinPathComparisonError(
	comparison *sqlJoinPathComparisonError,
) error {
	left, right, pos := comparison.left, comparison.right, -1
	if comparison.origin == joinOriginDecorrelatedExists {
		if proof := s.decorrelatedJoinAtSlot(comparison.slot); proof != nil && len(proof.keys) != 0 {
			pos = proof.keys[0].operatorPos
			if !proof.keys[0].outerFirst {
				left, right = right, left
			}
		}
	} else if comparison.origin == joinOriginSQL && s != nil && s.tree != nil {
		physicalSlot := comparison.slot - s.decorrelatedJoinCount()
		if physicalSlot >= 0 && physicalSlot+1 < len(s.tree.From) {
			cond := s.tree.From[physicalSlot+1].On
			// USING has no authored equality token. PostgreSQL reports its generated
			// comparison as 42883 without a position, even in a direct statement.
			if cond != nil && !cond.Using && len(cond.Keys) != 0 {
				if term, reversed := legacySQLJoinAuthoredTerm(cond, &cond.Keys[0]); term != nil {
					pos = term.Value.Pos
					if reversed {
						left, right = right, left
					}
				}
			}
		}
	}
	if pos < 0 {
		return sqlast.NewUnpositionedUndefinedComparisonOperatorError(left, "=", right)
	}
	return sqlast.NewUndefinedComparisonOperatorError(s.text, pos, left, "=", right)
}

func (s *Statement) decorrelatedJoinCount() int {
	if s == nil || s.nested == nil {
		return 0
	}
	count := 0
	for i := range s.nested.decorrelated {
		if !s.nested.decorrelated[i].mark {
			count++
		}
	}
	return count
}

func (s *Statement) decorrelatedJoinAtSlot(slot int) *statementDecorrelatedExists {
	if s == nil || s.nested == nil || slot < 0 {
		return nil
	}
	at := 0
	for i := range s.nested.decorrelated {
		proof := &s.nested.decorrelated[i]
		if proof.mark {
			continue
		}
		if at == slot {
			return proof
		}
		at++
	}
	return nil
}

// legacySQLJoinAuthoredTerm maps the normalized planner key back to the
// authoritative ON expression. Path identity is normally pointer identity;
// structural equality also covers AST copies made while expanding relations.
func legacySQLJoinAuthoredTerm(
	cond *sqlast.JoinCond,
	key *sqlast.JoinKeyCond,
) (*sqlast.Expr, bool) {
	if cond == nil || cond.Expr == nil || key == nil {
		return nil, false
	}
	terms := []*sqlast.Expr{cond.Expr}
	if cond.Expr.Kind == sqlast.ExprAnd {
		terms = cond.Expr.Kids
	}
	for _, term := range terms {
		if term == nil || term.Kind != sqlast.ExprCompare || term.Op != sqlast.OpEq ||
			term.Path == nil || term.RightPath == nil {
			continue
		}
		if sameLegacySQLJoinPath(term.Path, key.Left) &&
			sameLegacySQLJoinPath(term.RightPath, key.Right) {
			return term, false
		}
		if sameLegacySQLJoinPath(term.Path, key.Right) &&
			sameLegacySQLJoinPath(term.RightPath, key.Left) {
			return term, true
		}
	}
	return nil, false
}

func sameLegacySQLJoinPath(left, right *sqlast.PathExpr) bool {
	if left == right {
		return left != nil
	}
	if left == nil || right == nil || left.Source != right.Source ||
		left.MergedUsing != right.MergedUsing || len(left.Segments) != len(right.Segments) {
		return false
	}
	for i := range left.Segments {
		if left.Segments[i] != right.Segments[i] {
			return false
		}
	}
	return true
}
