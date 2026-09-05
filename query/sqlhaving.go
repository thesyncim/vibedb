package query

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// HAVING: the post-reduction filter the plan has no node for.
//
// query's plan reduces, groups, orders, and limits; there is nowhere between
// the reduction and the ordering for a filter to sit. The parser accepts HAVING
// anyway, because it can resolve one completely — every leaf is bound either to
// an aggregate the SELECT list already computes or to a GROUP BY key — and
// because refusing a clause that needs no new aggregation would be refusing
// wiring rather than capability.
//
// So it is implemented here, over the executed [Result], and it is implemented
// with the engine's own comparator rather than a second one. That is the whole
// design decision worth stating. A filter that re-derived "is this cell equal
// to that literal" would be a second answer to a question this package already
// answers in exactly one place — compareScalar, whose exact-decimal number
// order the exhaustive differential checks against math/big — and two
// comparators that agree today are two comparators that can drift. Converting a
// result Cell back to the scalar it was built from and calling compareScalar
// keeps the count at one.
//
// The clause costs the plan its LIMIT pushdown, because rows dropped after
// execution are rows the plan must not have stopped short of; see buildLimit.
// Ordering is unaffected: filtering a sorted sequence leaves it sorted, so
// ORDER BY still happens inside the executor where it is fast.
//
// Two leaf forms are refused inside HAVING rather than answered approximately.
// IS MISSING cannot be answered at all: a Result cell records null and does not
// record whether the path that produced it was absent, so the existence test
// has no input. Containment is refused because its needle machinery lives on
// the execution path, and reproducing it here would be exactly the second
// implementation this design exists to avoid.

// A tri is a three-valued truth value. The order of the constants is chosen so
// the two-valued cases are 0 and 1 and the comparisons against triUnknown are
// the only ones that need a name.
type tri uint8

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

// A havingLit is one resolved HAVING operand. known is false for a placeholder
// bound to nil, which SQL calls NULL and which makes its comparison UNKNOWN.
type havingLit struct {
	value scalar
	known bool
}

// A havingNode is one node of the compiled HAVING tree.
//
// Children and literals are index runs into the program's flat slices rather
// than per-node slices: the tree is rebuilt on every bind that carries a
// placeholder, and three slices reset to zero length rebuild it without a
// single allocation, where a tree of nodes owning their own children would
// allocate per node per bind.
type havingNode struct {
	kind    sqlast.ExprKind
	op      Op
	negated bool
	// column is the result column this leaf reads, and -1 for a boolean node.
	column   int32
	litStart int32
	litCount int32
	kidStart int32
	kidCount int32
}

// A havingProgram is a compiled HAVING clause. Its zero value filters nothing,
// which is what a statement without HAVING gets.
type havingProgram struct {
	nodes  []havingNode
	lits   []havingLit
	kidIdx []int32
	// stack holds a boolean node's child indices while its siblings are still
	// being compiled. It needs stack discipline because compilation recurses:
	// a nested conjunction would otherwise overwrite the enclosing
	// disjunction's operands, the same hazard the parser's own kid stack has.
	stack []int32
	// scratch formats computed non-integer aggregates so the exact-decimal
	// comparator has digits to read. It is reset per row, so a view taken
	// during one row's evaluation is only ever read during that row's
	// evaluation.
	scratch []byte
	root    int32
	active  bool
}

// reset empties the program while keeping its storage for the next bind.
func (h *havingProgram) reset() {
	h.nodes = h.nodes[:0]
	h.lits = h.lits[:0]
	h.kidIdx = h.kidIdx[:0]
	h.stack = h.stack[:0]
	h.root = -1
	h.active = false
}

// keep reports whether the result row survives the filter. A row survives when
// the clause is TRUE for it — UNKNOWN drops it, exactly as WHERE does.
func (h *havingProgram) keep(res *Result, row int) bool {
	if !h.active {
		return true
	}
	h.scratch = h.scratch[:0]
	return h.eval(h.root, res, row) == triTrue
}

// eval evaluates one node against a result row.
func (h *havingProgram) eval(idx int32, res *Result, row int) tri {
	node := &h.nodes[idx]
	switch node.kind {
	case sqlast.ExprAnd:
		out := triTrue
		for _, kid := range h.kidIdx[node.kidStart : node.kidStart+node.kidCount] {
			switch h.eval(kid, res, row) {
			case triFalse:
				return triFalse
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case sqlast.ExprOr:
		out := triFalse
		for _, kid := range h.kidIdx[node.kidStart : node.kidStart+node.kidCount] {
			switch h.eval(kid, res, row) {
			case triTrue:
				return triTrue
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case sqlast.ExprNot:
		return notTri(h.eval(h.kidIdx[node.kidStart], res, row))
	}

	cell := h.cellScalar(res.Columns[node.column].Cells[row])
	lits := h.lits[node.litStart : node.litStart+node.litCount]
	var out tri
	switch node.kind {
	case sqlast.ExprIsNull:
		out = boolTri(cell.kind == kindNull)
	case sqlast.ExprCompare:
		out = compareTri(cell, lits[0], node.op)
	case sqlast.ExprBetween:
		out = andTri(compareTri(cell, lits[0], Ge), compareTri(cell, lits[1], Le))
	default: // sqlast.ExprIn
		out = memberTri(cell, lits)
	}
	if node.negated {
		return notTri(out)
	}
	return out
}

// compareTri answers one comparison under three-valued logic: UNKNOWN when
// either side is null, and otherwise the sign of the engine's total order read
// through the operator.
func compareTri(cell scalar, lit havingLit, op Op) tri {
	if !lit.known || cell.kind == kindNull {
		return triUnknown
	}
	return boolTri(acceptSign(compareScalar(cell, lit.value), op))
}

// memberTri answers IN under three-valued logic. An unmatched NULL alternative
// leaves the disjunction undecided rather than false, which is what makes
// "NOT IN" over a list holding NULL match nothing at all.
func memberTri(cell scalar, lits []havingLit) tri {
	if cell.kind == kindNull {
		return triUnknown
	}
	unknown := false
	for _, lit := range lits {
		if !lit.known {
			unknown = true
			continue
		}
		if compareScalar(cell, lit.value) == 0 {
			return triTrue
		}
	}
	if unknown {
		return triUnknown
	}
	return triFalse
}

// acceptSign reads a total-order sign through a comparison operator.
func acceptSign(sign int, op Op) bool {
	switch op {
	case Eq:
		return sign == 0
	case Ne:
		return sign != 0
	case Lt:
		return sign < 0
	case Le:
		return sign <= 0
	case Gt:
		return sign > 0
	default: // Ge
		return sign >= 0
	}
}

func boolTri(b bool) tri {
	if b {
		return triTrue
	}
	return triFalse
}

func notTri(t tri) tri {
	switch t {
	case triTrue:
		return triFalse
	case triFalse:
		return triTrue
	default:
		return triUnknown
	}
}

func andTri(a, b tri) tri {
	if a == triFalse || b == triFalse {
		return triFalse
	}
	if a == triUnknown || b == triUnknown {
		return triUnknown
	}
	return triTrue
}

// cellScalar recovers the classified value a result cell was built from, so
// HAVING compares through the same total order the executor does.
func (h *havingProgram) cellScalar(c Cell) scalar {
	switch c.kind {
	case TypeNull:
		return scalar{kind: kindNull}
	case TypeBool:
		return scalar{kind: kindBool, bval: c.flag&cellTrue != 0}
	case TypeNumber:
		s := scalar{kind: kindNumber, num: c.raw, raw: c.raw}
		if c.flag&cellInteger != 0 {
			s.isInt, s.ival = true, int64(c.word)
		}
		if s.num == nil {
			// A computed non-integer aggregate — an AVG, or a SUM that left the
			// integer domain — carries no source bytes, because it was never
			// read from a document. Formatting it once gives the exact-decimal
			// comparator the digits it reads, at the cost of one append into a
			// buffer the row already owns.
			start := len(h.scratch)
			h.scratch = c.AppendJSON(h.scratch)
			s.num = h.scratch[start:len(h.scratch):len(h.scratch)]
			s.raw = s.num
		}
		return s
	case TypeString:
		return scalar{kind: kindString, sval: c.text, raw: c.raw}
	default:
		return scalar{kind: kindContainer, raw: c.raw}
	}
}

// --- compilation -----------------------------------------------------------

// buildHaving compiles the statement's HAVING clause, adding a hidden result
// column for any aggregate or GROUP BY key absent from the SELECT list.
//
// The hidden column is why this runs after buildColumns. A grouped projection
// must be a grouping key, which the parser has already checked, so appending
// one is always legal; and the plan's extra column is invisible to a caller
// because [Statement.Columns] and [Cursor.Cell] both stop at the SELECT list's
// width. The alternative — refusing "GROUP BY team HAVING team <> 'x'" because
// team is not projected — would refuse a statement for a reason that has
// nothing to do with what the engine can compute.
func (s *Statement) buildHaving(args []any) error {
	s.having.reset()
	if s.tree.Having == nil {
		return nil
	}
	if scalar := s.scalarStatement(); scalar != nil &&
		scalar.ordered != nil && scalar.ordered.havingEnd > scalar.ordered.projectionEnd {
		// The scalar stage evaluates this predicate after reduction and before
		// ORDER/OFFSET/LIMIT. Compiling it again as a cell/literal HAVING program
		// would both lose CASE control flow and create a second comparator.
		return nil
	}
	h := &s.having
	if scalar := s.scalarStatement(); scalar != nil && scalar.ordered != nil &&
		scalar.ordered.having != nil {
		h = scalar.ordered.having
		h.reset()
	}
	root, err := s.compileHaving(h, s.tree.Having, args)
	if err != nil {
		return err
	}
	h.root = root
	h.active = true
	return nil
}

// compileHaving lowers one HAVING node, returning its index in the program.
func (s *Statement) compileHaving(
	h *havingProgram,
	e *sqlast.Expr,
	args []any,
) (int32, error) {
	switch e.Kind {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		base := len(h.stack)
		for _, kid := range e.Kids {
			idx, err := s.compileHaving(h, kid, args)
			if err != nil {
				h.stack = h.stack[:base]
				return 0, err
			}
			h.stack = append(h.stack, idx)
		}
		start := int32(len(h.kidIdx))
		h.kidIdx = append(h.kidIdx, h.stack[base:]...)
		count := int32(len(h.stack) - base)
		h.stack = h.stack[:base]
		h.nodes = append(h.nodes, havingNode{
			kind: e.Kind, column: -1, kidStart: start, kidCount: count,
		})
		return int32(len(h.nodes) - 1), nil
	case sqlast.ExprIsMissing:
		return 0, fmt.Errorf(
			"query: IS MISSING is not available in HAVING: a grouped row records that its " +
				"key is null and not whether the path was absent, so the existence test has " +
				"nothing to read")
	case sqlast.ExprContains:
		return 0, fmt.Errorf(
			"query: '@>' is not available in HAVING: containment is answered on the " +
				"execution path, and a post-reduction copy of it would be a second " +
				"implementation of the same operator")
	case sqlast.ExprLike:
		return 0, fmt.Errorf(
			"query: LIKE is not available in HAVING: pattern matching is answered on the " +
				"execution path, and a post-reduction copy would be a second implementation")
	}

	column, err := s.havingColumn(e)
	if err != nil {
		return 0, err
	}
	start := int32(len(h.lits))
	switch e.Kind {
	case sqlast.ExprCompare:
		lit, err := s.havingLiteral(e.Value, args)
		if err != nil {
			return 0, err
		}
		h.lits = append(h.lits, lit)
	case sqlast.ExprIn, sqlast.ExprBetween:
		for _, operand := range e.List {
			lit, err := s.havingLiteral(operand, args)
			if err != nil {
				return 0, err
			}
			h.lits = append(h.lits, lit)
		}
	}
	h.nodes = append(h.nodes, havingNode{
		kind:     e.Kind,
		op:       Op(e.Op),
		negated:  e.Negated,
		column:   int32(column),
		litStart: start,
		litCount: int32(len(h.lits)) - start,
	})
	return int32(len(h.nodes) - 1), nil
}

// havingColumn answers the result column a HAVING leaf reads, materializing a
// hidden reduction column for an unprojected aggregate or GROUP BY key.
func (s *Statement) havingColumn(e *sqlast.Expr) (int, error) {
	if scalar := s.scalarStatement(); scalar != nil {
		// The cold scalar stage projects dependency columns rather than the
		// authored SELECT list. Translate a HAVING leaf's selected-output
		// ordinal back to that dependency namespace: scalar outputs can share a
		// dependency or introduce it before the direct aggregate HAVING names.
		if e.Column >= 0 {
			if e.Column >= len(scalar.outputs) {
				return 0, fmt.Errorf("query: HAVING output is outside the scalar projection")
			}
			root := scalar.outputs[e.Column]
			if root < 0 || int(root) >= len(scalar.nodes) ||
				scalar.nodes[root].kind != statementScalarDependency {
				return 0, fmt.Errorf("query: HAVING output has no scalar dependency")
			}
			dependency := scalar.nodes[root].dependency
			if dependency < 0 || int(dependency) >= len(scalar.deps) {
				return 0, fmt.Errorf("query: HAVING output has an invalid scalar dependency")
			}
			return int(dependency), nil
		}
		spec := s.spec(e.Path)
		agg := aggKind(e.Agg)
		physical := 0
		grouped := len(s.tree.GroupBy) != 0
		for i := range s.q.columns {
			column := &s.q.columns[i]
			if !column.cardinalityOnly && column.agg == agg && column.spec == spec {
				// A statically unreachable CASE arm can already have registered
				// this path for semantic validation only. HAVING makes it live;
				// promote that exact column instead of appending a duplicate.
				column.semanticOnly = false
				return physical, nil
			}
			if column.semanticOnly || column.cardinalityOnly && grouped {
				continue
			}
			physical++
		}
		s.q.columns = append(s.q.columns, Column{agg: agg, spec: spec, header: spec})
		return physical, nil
	}
	if e.Column >= 0 {
		return e.Column, nil
	}
	spec := s.spec(e.Path)
	agg := aggKind(e.Agg)
	for i := s.outputs; i < len(s.q.columns); i++ {
		if s.q.columns[i].agg == agg && s.q.columns[i].spec == spec {
			return i, nil
		}
	}
	s.q.columns = append(s.q.columns, Column{agg: agg, spec: spec, header: spec})
	return len(s.q.columns) - 1, nil
}

// havingLiteral resolves one HAVING operand to the classified scalar the
// comparator takes.
func (s *Statement) havingLiteral(o sqlast.Operand, args []any) (havingLit, error) {
	value, known, err := s.operand(o, args)
	if err != nil {
		return havingLit{}, err
	}
	if !known {
		return havingLit{}, nil
	}
	lit, err := s.c.makeLiteral(value)
	if err != nil {
		return havingLit{}, err
	}
	return havingLit{value: classifyLiteral(lit), known: true}, nil
}
