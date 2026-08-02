package query

// correlatedMarkKind is the semantic contract of one proof-backed correlated
// predicate subquery.  It deliberately distinguishes existential negation
// from null-aware NOT IN and from cardinality-sensitive scalar comparison:
// those operators share a grouped build, but not a truth table.
type correlatedMarkKind uint8

const (
	correlatedMarkExists correlatedMarkKind = iota
	correlatedMarkNotExists
	correlatedMarkIn
	correlatedMarkNotIn
	correlatedMarkScalar
)

// correlatedMarkKey is one equality consumed by decorrelation.  The outer and
// inner spellings are in their respective collection namespaces.  A group is
// addressable only when every component is non-NULL; SQL equality can never
// correlate a NULL or missing component.
type correlatedMarkKey struct {
	outer string
	inner string
}

// correlatedMark is the cold builder form installed only by the SQL
// decorrelator after it has proved the complete authored correlation graph.
// Programmatic queries cannot construct one.
type correlatedMark struct {
	collection string
	keys       []correlatedMarkKey
	probe      string // outer IN/scalar operand; empty for EXISTS
	project    string // inner IN/scalar value; empty for EXISTS
	where      Predicate
	hasWhere   bool
	kind       correlatedMarkKind
	op         Op // authored scalar comparison; meaningful only for Scalar
}

// planMark is the immutable compiled address map shared by executions.  Every
// execution builds its own grouped state in Workspace; the plan retains no
// snapshot values.  Outer columns address the driving plan and inner columns
// address inner.  probe/value are -1 when their operator does not use them.
type planMark struct {
	collection string
	inner      *plan
	outer      []int
	innerKeys  []int
	probe      int
	value      int
	slot       int
	kind       correlatedMarkKind
	op         Op
}
