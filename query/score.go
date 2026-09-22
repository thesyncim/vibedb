package query

import (
	"fmt"
	"math"
	"strconv"

	"github.com/thesyncim/vibedb/internal/tin"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// evalScore evaluates one SCORE() node: the BM25 relevance of the row's
// match-path text against the execution's bound ==> query. Only string cells
// can match (the same rule evalMatch applies), so every other cell scores 0
// without touching the scorer. The formatted decimal borrows the row's eval
// arena exactly like computed arithmetic results, so warm scoring keeps the
// engine's allocation-free row contract.
func (r *statementScalar) evalScore(result *Result, row int, node *statementScalarNode, arena *[]byte) (statementScalarValue, error) {
	if node.dependency < 0 || int(node.dependency) >= len(result.Columns) ||
		r.scoreSlot < 0 || r.scoreStats == nil {
		return statementScalarValue{}, fmt.Errorf("query: SCORE() evaluated before its ==> binding resolved")
	}
	cells := result.Columns[node.dependency].Cells
	if row < 0 || row >= len(cells) {
		return statementScalarValue{}, &ScalarResultShapeError{Dependency: int(node.dependency), Rows: result.RowCount, Cells: len(cells)}
	}
	var score float64
	if text := scalarFromResultCell(cells[row], arena); text.kind == kindString && r.scoreQuery != nil {
		score = tin.ScoreSingle(text.sval, *r.scoreQuery, r.scoreStats, &r.scoreScratch)
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		score = 0
	}
	start := len(*arena)
	*arena = strconv.AppendFloat(*arena, score, 'f', -1, 64)
	return statementScalarValue{value: classifyComputedNumber((*arena)[start:])}, nil
}

// SCORE() preparation and per-execution binding.
//
// A SCORE() scalar node carries only its output position until ==> slots are
// numbered: the SELECT list compiles before the WHERE clause that names the
// full-text query, so the node cannot know its slot or its text column yet.
// bindScorePlan runs after compilePlan (from Statement.lower, where both the
// finished plan and the scalar program are visible) and resolves every
// SCORE() node at once:
//
//   - exactly one ==> node must exist, and it must sit in the driving
//     WHERE: with zero the score has no query, with several it has no
//     single query, and an inner-plan ==> scores a collection whose rows
//     the scalar stage never sees. Every violation is a positioned prepare
//     error, never a silent zero.
//   - the node's text comes from a hidden result column over the ==> path,
//     registered exactly like an ordinary scalar dependency. Score nodes
//     read result.Columns[dependency] directly, so no dependency node is
//     needed and postorder evaluation order is undisturbed.
//   - result columns mirror scalar deps 1:1 (Statement.buildColumns
//     delegates to the scalar program when one exists), with semantic-only
//     extras trailing. The hidden column is inserted at the dep-block end
//     rather than appended, preserving that prefix alignment when extras
//     exist. A re-lowered statement reuses the persisted dep through the
//     normal buildColumns path and performs no surgery.
//
// Per-execution values (the parsed query, BM25 statistics) stay in the
// executing Workspace: bindScore borrows them into the scalar program at
// scalar execute() entry, after the core scan bound its ==> slots. The
// program names the slot; it never holds the values, so the prepared
// statement stays shareable and re-executions rebind against their own
// snapshot generation.
func bindScorePlan(s *Statement, p *plan) error {
	r := s.scalarStatement()
	if r == nil {
		return nil
	}
	// lower() runs per execution, so the steady path below must not
	// allocate: no closures (a self-recursive closure escapes), no slices.
	// Two passes over the node program replace the index list.
	scoreCount := 0
	pos := 0
	for i := range r.nodes {
		if r.nodes[i].kind == statementScalarScore {
			if scoreCount == 0 {
				pos = r.nodes[i].pos
			}
			scoreCount++
		}
	}
	if scoreCount == 0 {
		return nil
	}
	if s.tree.Set != nil {
		return sqlast.NewFeatureNotSupportedError(s.text, pos,
			"SCORE() over a set operation is not supported yet; score each full-text branch before combining it")
	}
	if p.grouped || p.hasAggregate {
		return sqlast.NewFeatureNotSupportedError(s.text, pos,
			"SCORE() with GROUP BY or aggregates is not supported yet; score the rows before reducing them")
	}
	var inv scoreMatchInventory
	inventoryTopMatches(p.where, &inv)
	inventoryInnerMatches(p, &inv)
	switch total := inv.top + inv.inner; {
	case total == 0:
		return sqlast.NewFeatureNotSupportedError(s.text, pos,
			"SCORE() requires a ==> full-text predicate in the statement")
	case total > 1:
		return sqlast.NewFeatureNotSupportedError(s.text, pos,
			"SCORE() needs exactly one ==> predicate in the statement")
	case inv.top == 0:
		return sqlast.NewFeatureNotSupportedError(s.text, pos,
			"SCORE() needs a top-level WHERE ==> predicate; scoring a join or subquery match is not supported yet")
	}
	match := inv.first
	if match.slot < 0 || match.slot >= p.matchCount {
		return fmt.Errorf("query: SCORE() ==> node names slot %d of %d", match.slot, p.matchCount)
	}
	if match.col < 0 || match.col >= len(p.valuePaths) {
		return fmt.Errorf("query: SCORE() ==> node names path %d of %d", match.col, len(p.valuePaths))
	}
	spec := p.valuePaths[match.col].spec
	dep := -1
	for i := range r.deps {
		if r.deps[i].agg == sqlast.AggNone && r.deps[i].spec == spec {
			dep = i
			break
		}
	}
	if dep < 0 {
		// Append to both namespaces at the dep-block end. Appending the
		// plan column (rather than inserting mid-slice) is only sound
		// when no trailing semantic-only extra exists; otherwise the
		// dep index and the column index diverge.
		dep = len(r.deps)
		r.deps = append(r.deps, statementScalarDependencySpec{spec: spec})
		column := planColumn{value: match.col, num: -1, slot: -1}
		if len(p.columns) != dep {
			p.columns = append(p.columns, planColumn{})
			copy(p.columns[dep+1:], p.columns[dep:])
			p.columns[dep] = column
			p.headers = append(p.headers, "")
			copy(p.headers[dep+1:], p.headers[dep:])
			p.headers[dep] = spec
		} else {
			p.columns = append(p.columns, column)
			p.headers = append(p.headers, spec)
		}
		if !containsInt(p.filterCols, match.col) && !containsInt(p.lateCols, match.col) {
			p.lateCols = append(p.lateCols, match.col)
		}
	}
	for i := range r.nodes {
		if r.nodes[i].kind == statementScalarScore {
			r.nodes[i].dependency = int32(dep)
		}
	}
	r.scoreSlot = match.slot
	// A WHERE that is exactly this ==> with a single ORDER BY SCORE() key
	// and a LIMIT lets the index rank the scan's survivors directly (see
	// tin_topk.go). The spec is shape-only, so cached lowerings stay valid;
	// any decline keeps the ordinary scan.
	p.tinTopK = decideTinTopK(s, p, r, match)
	return nil
}

// scoreMatchInventory counts ==> nodes without allocating: top tallies the
// driving WHERE (first keeps the single node the exactly-one rule binds)
// and inner tallies join and mark inner plans.
type scoreMatchInventory struct {
	top   int
	first *compiledPredicate
	inner int
}

func inventoryTopMatches(pd *compiledPredicate, inv *scoreMatchInventory) {
	if pd == nil {
		return
	}
	if pd.kind == predMatch {
		if inv.top == 0 {
			inv.first = pd
		}
		inv.top++
	}
	for _, kid := range pd.kids {
		inventoryTopMatches(kid, inv)
	}
}

func inventoryInnerMatches(pl *plan, inv *scoreMatchInventory) {
	if pl == nil {
		return
	}
	for i := range pl.joins {
		inner := pl.joins[i].inner
		if inner != nil {
			inv.inner += countScoreMatches(inner.where)
			inventoryInnerMatches(inner, inv)
		}
	}
	for i := range pl.marks {
		inner := pl.marks[i].inner
		if inner != nil {
			inv.inner += countScoreMatches(inner.where)
			inventoryInnerMatches(inner, inv)
		}
	}
}

func countScoreMatches(pd *compiledPredicate) int {
	if pd == nil {
		return 0
	}
	n := 0
	if pd.kind == predMatch {
		n++
	}
	for _, kid := range pd.kids {
		n += countScoreMatches(kid)
	}
	return n
}

func containsInt(s []int, v int) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}

// bindScore borrows the SCORE() slot's parsed query and refreshes its BM25
// statistics for this execution. It runs once at scalar execute() entry —
// after the core scan bound its ==> slots against this execution's
// snapshot — and costs nothing when the statement uses no SCORE(). A slot
// whose index build is absent (nil entries decline to the full scan) keeps
// zero statistics, so SCORE() agrees with the match predicate: unbound
// rows score 0 exactly where ==> is false.
func (r *statementScalar) bindScore(w *Workspace) error {
	if r.scoreSlot < 0 {
		return nil
	}
	if r.scoreSlot >= len(w.matchQueries) {
		return fmt.Errorf("query: SCORE() slot %d has no bound ==> query", r.scoreSlot)
	}
	for len(w.matchScoreStats) <= r.scoreSlot {
		w.matchScoreStats = append(w.matchScoreStats, tin.ScoreStats{})
	}
	stats := &w.matchScoreStats[r.scoreSlot]
	// RefreshScoreStats resets the statistics fields while reusing the idf
	// and match buffers, so warm executions allocate nothing. Do not zero
	// the struct here: that would discard the warmed capacity every run.
	// Only an unbound slot (no index build) takes the cold zero, which
	// agrees with the match predicate: unbound rows score 0 exactly where
	// ==> is false.
	if r.scoreSlot < len(w.matchIndexes) && w.matchIndexes[r.scoreSlot] != nil {
		w.matchIndexes[r.scoreSlot].RefreshScoreStats(w.matchQueries[r.scoreSlot], stats)
	} else if r.scoreSlot < len(w.matchTinBuilds) && w.matchTinBuilds[r.scoreSlot] != nil &&
		w.matchTinBuilds[r.scoreSlot].Index() != nil {
		w.matchTinBuilds[r.scoreSlot].Index().RefreshScoreStats(w.matchQueries[r.scoreSlot], stats)
	} else {
		*stats = tin.ScoreStats{}
	}
	r.scoreQuery = &w.matchQueries[r.scoreSlot]
	r.scoreStats = stats
	return nil
}
