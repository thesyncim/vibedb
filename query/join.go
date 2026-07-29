package query

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// Cross-collection equi-joins.
//
// One clause has two deliberate execution shapes. If no selected, filtered,
// grouped, or ordered path reads the joined alias, it is a semi-join: it asks
// only whether a partner exists, so duplicate inner matches keep an outer row
// once. If the query reads the alias, it is an inner join: every matching pair
// becomes an output row and the fan-out build preserves source order. The
// compiler chooses from actual path use, so the cheaper existential plan is an
// optimization of the same clause rather than a second query operator.
//
// # Why the strategy is measured rather than estimated
//
// Choosing a driving side is the hard part of any join planner, and doing it
// well normally needs cardinality statistics. This engine has none, and the
// consequence of guessing wrong is not a rounding error: a nested-equality
// probe costs 126 ns per document against a DocSet or a durable segment and
// 1.77 ns against a heap Snapshot, a 71x cliff that sits at a path-shape
// boundary an estimator cannot see. A planner that guessed would therefore be
// wrong by two orders of magnitude on exactly the shapes it could not
// distinguish.
//
// So nothing is estimated. The inner side runs first, its join-key values are
// collected, and the collection stops the moment it passes a threshold:
//
//   - Under the threshold the values are pushed into the outer predicate as a
//     membership. That is the existing [In] machinery — a sorted, deduplicated
//     set searched per row, lowering to one mask intersection when the outer
//     path carries a declared exact index — and it is the reason the values are
//     collected at all.
//   - Over the threshold the membership is abandoned unbuilt and the join runs
//     as a lookup: scan the outer, probe the inner once per row. A primary-key
//     probe uses the heap collection's key map or the durable collection's
//     ordered primary without a build side; the bounded collection work already
//     spent is the price of learning which side to drive, and it is bounded
//     precisely because the threshold stops it.
//
// The choice changes the cost and never the answer. Both branches evaluate the
// same existential test over the same values, which is what makes forcing the
// threshold to either extreme a real differential rather than a smoke test.
//
// # Why a join must name a database snapshot
//
// Both sides are read from one coherent database snapshot: a
// [store.DatabaseSnapshot] through [FromDatabase], or its durable counterpart
// through [FromFileDatabase]. Resolving the inner collection against a
// separately taken snapshot would let the outer side reference a state it
// never saw — generation 7 of orders joined against generation 12 of
// customers, two states that never coexisted — and no amount of care at the
// call site would make that visible. It is therefore not expressible: the
// inner collection is resolved by name out of the same snapshot the driving
// side came from, and single-collection sources such as [FromSnapshot] and
// [FromFile] reject a plan that has a join.

// JoinKey is the inner-path spelling that names the inner collection's primary
// key rather than a field of its documents. It is the common foreign-key
// shape — an outer document holding the key of an inner one — and the only
// shape with an O(1) probe, which is what lets a join over it fall back to a
// lookup instead of materializing a membership.
const JoinKey = "$key"

// joinPrimaryKey is the inner-column index that stands for [JoinKey]. It is
// negative so it can never be mistaken for a registered value column.
const joinPrimaryKey = -1

// A Join is one equi-join clause. It matches an outer row when the inner
// collection holds a document that satisfies the clause's filter and whose
// join key equals the outer row's value at the outer path.
//
// Without a read of its alias it is an existential filter; with one it expands
// every matching pair as an inner join. The zero Join names no collection and
// is rejected at compile.
type Join struct {
	collection string
	alias      string
	outerPath  string
	innerPath  string
	where      Predicate
	hasWhere   bool
	left       bool
}

// JoinOn builds an equi-join against collection, matching each outer row's
// value at outerPath against innerPath in the inner collection. innerPath is
// either [JoinKey], naming the inner collection's primary key, or a path spec
// in the same syntax as [Path]. Reading an alias declared with [Join.As]
// requests pair-producing inner-join semantics; otherwise the clause is a
// semi-join filter.
//
// A null or absent value on either side matches nothing, the rule [Cmp] and
// [In] already apply: "unknown" is not a join key.
//
//	q := query.Select(query.Path("id")).
//		Join(query.JoinOn("customers", "customer_id", query.JoinKey).
//			Where(query.Cmp("tier", query.Eq, "pro")))
func JoinOn(collection, outerPath, innerPath string) Join {
	return Join{collection: collection, outerPath: outerPath, innerPath: innerPath}
}

// LeftJoinOn builds a left outer equi-join. Every driving row contributes at
// least one result row; when no joined document matches, columns read through
// the joined alias are NULL.
func LeftJoinOn(collection, outerPath, innerPath string) Join {
	return Join{
		collection: collection,
		outerPath:  outerPath,
		innerPath:  innerPath,
		left:       true,
	}
}

// Where narrows the inner collection to the documents satisfying p. Without it
// every inner document is a candidate match. A later Where replaces an earlier
// one, matching [Query.Where].
func (j Join) Where(p Predicate) Join {
	j.where = p
	j.hasWhere = true
	return j
}

// As names the joined collection, so the rest of the query can read its
// columns. A path whose leading dotted segment is a declared alias resolves to
// that collection: with As("o"), Path("o.total") projects the joined
// document's total and Path("total") still projects the driving document's.
//
// Declaring an alias is what turns a semi-join into an inner join. Without one
// no column can name the joined side, the clause can only filter, and the
// planner keeps the semi-join machinery — which is strictly faster, because it
// never has to produce the matching rows, only decide that they exist. With
// one, and with some column actually reading it, the clause fans out: the
// result carries one row per matching (driving, joined) pair, which is SQL's
// inner join. Declaring an alias nothing reads changes nothing.
//
//	q := query.Select(query.Path("name"), query.Path("o.total")).
//		Join(query.JoinOn("orders", "id", "user_id").As("o"))
//
// An alias must not collide with another clause's alias. It may collide with a
// field name in the driving collection, and then it wins — the same rule SQL
// applies, and the only one available to an engine with no schema to consult.
func (j Join) As(alias string) Join {
	j.alias = alias
	return j
}

// Join adds semi-join clauses to q. Clauses conjoin: a row survives only if
// every clause finds a match, and they are evaluated after the query's own
// WHERE so a cheap local filter is never paid for twice.
//
// A query with a join must be executed against [FromDatabase] or
// [FromFileDatabase], whose database snapshot resolves the inner collections
// at the same instant the driving side was captured.
func (q *Query) Join(joins ...Join) *Query {
	q.joins = append(q.joins, joins...)
	return q
}

// A planJoin is one compiled semi-join clause. It carries no values: the set
// the outer predicate tests against is discovered at execution and lives in the
// executing [Workspace] at slot.
type planJoin struct {
	// collection is the inner collection's catalog name, resolved against the
	// DatabaseSnapshot the driving side came from.
	collection string
	// inner is the inner side's compiled predicate plus the value columns it
	// and the join-key projection read. It has no select list, grouping, or
	// ordering: a semi-join asks only which inner rows exist.
	inner *plan
	// outerPath is the value-column index of the outer join key.
	outerPath int
	// innerPath is the inner value-column index of the join key, or
	// joinPrimaryKey when the join matches the inner collection's key.
	innerPath int
	// slot is the membership binding the outer predicate's predInBound node
	// reads, and the index of this clause's storage in Workspace.joins.
	slot int
	// aliased records that the clause declared a name for its collection,
	// which is what says the query wants SQL's inner join rather than this
	// engine's filter. It is kept separately from fanOut because the two are
	// different questions: this one is what the query asked for, and fanOut is
	// how the planner decided to answer it.
	aliased bool
	// fanOut is the shape execution takes. True produces one row per matching
	// pair and needs the build side; false keeps the semi-join, whose binder
	// chooses between its own measured strategies. See planJoinColumns for the
	// one case where an aliased clause can still take the cheaper shape.
	fanOut bool
	// left preserves a driving row whose build-side probe finds no partner and
	// null-extends every column read from the joined collection.
	left bool
	// innerCols are the value-column indexes this clause's collection fills,
	// in the shared column space. They are extracted after the pairs exist,
	// addressed by the joined row of each pair.
	innerCols []int
	// innerNums are the same for the numeric columns an aggregate reduces.
	innerNums []int
}

// --- compilation -----------------------------------------------------------

// compileJoins lowers q's join clauses into p and returns the predicate nodes
// that read their bindings, for the caller to conjoin onto the outer WHERE.
//
// The nodes are returned rather than installed here because their placement is
// a cost decision: a join leaf is the most expensive test in the tree — under a
// lookup binding it is a hash probe and a document decode — so it belongs after
// every local conjunct, where a cheap filter has already rejected the row.
func (c *compiler) compileJoins(q *Query, p *plan, values *pathRegistry) ([]*compiledPredicate, error) {
	if len(q.joins) == 0 {
		return nil, nil
	}
	p.joins = reserve(c.planJoins[:0], len(q.joins))
	nodes := c.kids.alloc(len(q.joins))[:0]
	for i, j := range q.joins {
		compiled, err := c.compileJoin(j, i, values)
		if err != nil {
			return nil, err
		}
		p.joins = append(p.joins, compiled)
		if !compiled.left {
			node := c.nodes.one()
			*node = compiledPredicate{
				kind: predInBound, col: compiled.outerPath, op: Eq, slot: compiled.slot,
			}
			nodes = append(nodes, node)
		}
	}
	c.planJoins = p.joins
	return nodes, nil
}

// conjoin ANDs the join leaves onto an existing WHERE, collapsing the two
// degenerate shapes so a plan is the same whether or not the query also had a
// filter of its own: no filter and one join is that join's leaf, and a filter
// that was already a conjunction absorbs the leaves rather than nesting a
// second And the evaluator would have to descend through per row.
func (c *compiler) conjoin(where *compiledPredicate, nodes []*compiledPredicate) *compiledPredicate {
	if where == nil {
		if len(nodes) == 1 {
			return nodes[0]
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predAnd, kids: nodes}
		return cp
	}
	existing := 1
	if where.kind == predAnd {
		existing = len(where.kids)
	}
	kids := c.kids.alloc(existing + len(nodes))[:0]
	if where.kind == predAnd {
		kids = append(kids, where.kids...)
	} else {
		kids = append(kids, where)
	}
	kids = append(kids, nodes...)
	cp := c.nodes.one()
	*cp = compiledPredicate{kind: predAnd, kids: kids}
	return cp
}

func (c *compiler) compileJoin(j Join, index int, values *pathRegistry) (planJoin, error) {
	if j.collection == "" {
		return planJoin{}, fmt.Errorf(
			"query: join[%d]: a join names the collection to match against; set \"from\"", index)
	}
	if j.outerPath == "" {
		return planJoin{}, fmt.Errorf(
			"query: join[%d]: the outer side of \"on\" must name a path, not the whole document", index)
	}
	if j.innerPath == "" {
		return planJoin{}, fmt.Errorf(
			"query: join[%d]: the inner side of \"on\" must name a path or %q, not the whole document",
			index, JoinKey)
	}
	outer, err := c.addPath(values, j.outerPath)
	if err != nil {
		return planJoin{}, err
	}

	// The inner side gets its own path registry and its own plan. Sharing the
	// outer's would make one collection's column indexes address the other's
	// documents, which is the kind of mistake that produces plausible wrong
	// answers rather than a failure.
	inner := c.joinRegistry(index)
	innerPath := joinPrimaryKey
	if j.innerPath != JoinKey {
		if innerPath, err = c.addPath(inner, j.innerPath); err != nil {
			return planJoin{}, err
		}
	}
	ip := c.joinPlan(index)
	// The column list is taken back before the plan is zeroed, so a warmed
	// recompile refills it rather than allocating a new one.
	cols := ip.filterCols[:0]
	*ip = plan{}
	if j.hasWhere {
		if ip.where, err = c.compilePredicate(j.where, inner); err != nil {
			return planJoin{}, err
		}
	}
	ip.valuePaths = inner.paths
	// Every inner column is a filter column. The inner side is not late
	// materialized: its whole output is one join-key value per surviving row,
	// which the collection loop reads out of a column it had to extract to
	// filter by anyway, so there is no second phase for a gather to defer to.
	for i := range ip.valuePaths {
		cols = append(cols, i)
	}
	ip.filterCols = cols
	// The column lists this clause filled last time are carried into the fresh
	// value rather than left behind with it. p.joins is rebuilt from a
	// zero-length reslice of the compiler's retained array, so the planJoin at
	// this slot still holds the slices the previous compile grew — and
	// discarding them would put one allocation per compile back on a warmed
	// recompile of a clause whose joined columns anything reads, which is every
	// fan-out clause there is.
	var innerCols, innerNums []int
	if index < len(c.planJoins) {
		innerCols = c.planJoins[index].innerCols[:0]
		innerNums = c.planJoins[index].innerNums[:0]
	}
	return planJoin{
		collection: j.collection,
		aliased:    j.alias != "",
		left:       j.left,
		inner:      ip,
		outerPath:  outer,
		innerPath:  innerPath,
		slot:       index,
		innerCols:  innerCols,
		innerNums:  innerNums,
	}, nil
}

// joinRegistry returns the reusable path registry for the index'th join. A
// reusable compiler keeps one per position so a warmed recompile of the same
// shape refills them instead of allocating; a one-shot compiler, which by
// definition never recompiles, hands back a fresh one.
func (c *compiler) joinRegistry(index int) *pathRegistry {
	if c.oneShot {
		return new(pathRegistry)
	}
	for len(c.joinRegs) <= index {
		c.joinRegs = append(c.joinRegs, new(pathRegistry))
	}
	c.joinRegs[index].reset()
	return c.joinRegs[index]
}

// joinPlan returns the reusable inner plan for the index'th join, on the same
// reasoning as joinRegistry. The plans are held behind pointers rather than in
// one slice of values so that growing the list cannot move a plan a previously
// compiled Query still points at within a single compile.
func (c *compiler) joinPlan(index int) *plan {
	if c.oneShot {
		return new(plan)
	}
	for len(c.joinPlans) <= index {
		c.joinPlans = append(c.joinPlans, new(plan))
	}
	return c.joinPlans[index]
}

// --- execution-time binding -------------------------------------------------

// joinMembershipMax is how many inner join-key values the binder collects
// before it abandons the membership strategy and drives the join as a per-row
// keyed lookup instead.
//
// It is a measured crossover, not a guess. BenchmarkJoinThresholdSweep runs one
// query over a 20,000-row driving collection with the threshold forced to each
// side, so every pair of rows below differs only in which strategy answered
// identical work (ns per outer row, Apple M4 Max, Go 1.26):
//
//	inner matches   membership   membership+index   lookup
//	           16         76.2                4.2    175.2
//	          256        121.0               23.6    176.7
//	        1,024        153.4              101.2    183.2
//	        2,048        175.3              134.0    180.8
//	        3,072        188.5              172.6    184.9
//	        4,096        210.9              208.1    183.4
//	       16,384        381.2              664.5    191.7
//	       65,536      1,196.3            8,090.0    235.4
//
// The lookup is nearly flat because its cost is one hash probe and one document
// decode per outer row whatever the inner side looks like. The membership's is
// not: it pays an inner scan proportional to the matches, and then a search per
// outer row over a set that stops being cache-resident. The two cross between
// 2,048 and 3,072 without an index, and between 4,096 and 8,192 with one, where
// the membership's per-row search is replaced by one index probe per value and
// a mask intersection.
//
// 2,048 is the last power of two on the membership's side of the unindexed
// crossing, and comfortably inside the indexed one. Choosing the lower of the
// two is deliberate, because the penalties are not symmetric: below the crossing
// the membership is 2.4x faster without an index and 48x with one, so giving
// that up early is expensive, while just above it the membership is only about
// 1.01x slower before the threshold cuts it off. Making the threshold
// index-aware — 4,096 when the outer path carries a ready exact index — was
// measured and declined twice, before and after the executor grew its late
// materialization phase: at 4,096 it would buy 2% (178.7 against 182.8), for a
// second constant and a branch on a decision that has to be made while the set
// is still being collected.
//
// The threshold counts values rather than bytes because what makes a large
// membership expensive is the comparisons per outer row and the cache lines the
// set spans, and both scale with the count, not with the spelling.
const joinMembershipMax = 2048

// joinBloomScanRatio is how many inner rows the binder will scan, per outer row,
// to build a semi-join reduction filter.
//
// It is the break-even ratio of two per-row costs this package measures
// directly, in BenchmarkJoinCostModel (Apple M4 Max, Go 1.26, ns per row,
// allocation-free steady state):
//
//	inner collection   inner row scanned   outer row probed   ratio
//	           1,000                44.8              199.8     4.5
//	          10,000                44.7              229.5     5.1
//	         100,000                47.4              389.2     8.2
//	         400,000                52.4              546.3    10.4
//
// A probe is the expensive one and gets more so as the inner collection grows:
// it is a random hash lookup into a structure that stops fitting cache, plus a
// document admission, plus a path resolution per inner filter path, plus the
// filter itself. Scanning stays sequential and stays flat. Four is the floor of
// the lowest break-even measured, so it is the one that holds everywhere in the
// range — and it held across the executor's late-materialization rewrite, which
// moved every absolute number here without moving the constant.
//
// Earlier this constant was set to half the break-even, as insurance against
// not knowing how much of the outer side a filter would reject. That insurance
// is no longer needed and was actively harmful — it declined filters measured
// to be 1.55x wins — because keepFiltering now measures that rejection on real
// rows instead of assuming it. The pre-scan gate's job is only to bound how
// much scanning a filter may attempt before the measurement exists.
//
// What this is not is a cardinality estimator. Both quantities it compares —
// the inner candidate popcount and the driving snapshot's length — are exact
// counts already materialized before the scan starts. The engine still refuses
// to estimate how many rows a predicate will select; it only refuses to spend
// more on finding out than the answer can be worth.
const joinBloomScanRatio = 4

// joinBatchRows is how many inner rows are extracted, filtered, and collected
// per pass. Batching is what makes "bounded by a threshold" true of work and
// memory and not only of the answer: an unbatched inner run would materialize a
// column per path over the whole inner collection before the first value was
// collected, so the threshold would bound what was kept after the scan had
// already paid for everything.
const joinBatchRows = 1024

// A joinBindMode is the strategy the binder measured its way into.
type joinBindMode uint8

const (
	// joinBindNone is an unbound clause. It matches no outer row, so a binding
	// that was somehow never filled fails closed rather than admitting
	// everything.
	joinBindNone joinBindMode = iota
	// joinBindSet pushed the collected values down as a membership.
	joinBindSet
	// joinBindProbe drives the join as a per-row keyed lookup.
	joinBindProbe
	// joinBindBuild materialized the joined side so the pairs can be produced.
	// It is not one of the strategies the binder measures between: a clause
	// that fans out has no choice, because the question it answers is which
	// rows match rather than whether any do.
	joinBindBuild
)

// A joinBinding is one join clause's execution-time state. It lives in the
// executing [Workspace] rather than in the compiled plan, because the plan is
// immutable and shared by every concurrent execution while these values belong
// to exactly one.
//
// Its retained capacity follows the Workspace's high-water rule, with the
// threshold as a ceiling the rest of the Workspace does not have: a binding
// never holds more than joinMembershipMax alternatives, so the storage a join
// pins is bounded by configuration rather than by the size of the largest
// collection it ever ran against. [Workspace.Release] gives it back.
type joinBinding struct {
	mode joinBindMode

	// lits is the collected inner join-key set, sorted by compareScalar and
	// deduplicated exactly like a compile-time membership's. The deduplication
	// is also what makes the semi-join contract hold: two inner documents with
	// the same key are one alternative, so they cannot select an outer row
	// twice.
	lits []scalar
	// needles is the exact-index needle per alternative, in lits' order, or
	// empty when the set cannot be probed through an index — because the outer
	// path has no ready single-column exact index, or because an alternative is
	// a container and exact indexes hold scalars. A partial needle list would be
	// an unsound candidate bound, so it is all or nothing.
	needles []vibejson.Index
	// text owns every byte lits views, and needleText every byte needles views.
	// The values are copied out of the inner extraction because that
	// extraction's buffers are reused batch to batch and by nothing else;
	// appending never rewrites what an earlier value already views, and a
	// reallocation leaves the old array alive behind the views holding it, so a
	// collected value stays correct for the whole execution.
	text       []byte
	needleText []byte
	starts     []int
	entries    []vibejson.IndexEntry
	// textReserved is the largest live-plus-growing text arena footprint this
	// binding admitted in the current execution. It is per binding because all
	// joined sides coexist; one global high-water mark would undercount them.
	textReserved int64

	// snapshot and plan are the probe mode's state: the inner collection at the
	// joined instant, and the compiled filter a probe evaluates against one of
	// its documents. Both are read-only once bind has finished, which is what
	// lets the filter phase's workers share this whole binding; everything a
	// probe writes lives in the per-worker joinProbe instead.
	snapshot store.Snapshot
	plan     *plan

	// bloom is the semi-join reduction filter a lookup binding may carry: a
	// fixed-size summary of every inner join key that survived the inner
	// filter, consulted before each probe. It is only ever a prefilter — the
	// probe behind it stays authoritative — so its one-sided error costs
	// probes and never rows. See join_bloom.go.
	bloom joinBloom
	// build is the fan-out side: every surviving joined row, indexed by its
	// join value. It is filled instead of lits and bloom, never alongside
	// them — a clause either produces rows or decides existence.
	build joinBuild
	// candidates is the exact number of inner rows this bind will scan,
	// filtering is whether it is scanning them into a filter, and overflowed
	// is whether it has already crossed the membership threshold. The three
	// exist as fields rather than locals because the collection loop is split
	// across collect and drain, which run once per batch.
	candidates int
	filtering  bool
	overflowed bool
	// scanned counts the inner candidate rows read so far and outerRows the
	// driving collection's length. Together with the filter's insert count they
	// are what the mid-scan abandon check reads: the inner predicate's observed
	// selectivity, measured on the rows already paid for.
	scanned   int
	outerRows int
	ratio     int

	// scan is the inner side's own transient storage. It is a Workspace per
	// join clause rather than one shared with the outer run because the outer
	// run overwrites its own buffers, and because two join clauses in one query
	// would otherwise overwrite each other's.
	scan  Workspace
	masks []store.Mask
	rows  []store.Location
	keys  []string

	// file is the durable inner side, filled instead of snapshot when the join
	// resolves against a durable database. Exactly one of the two is live: a
	// clause binds against one catalog, and file.snapshot being non-nil is what
	// routes matches to the durable probe. See join_file.go.
	file fileJoinSide
}

// reset returns b to an unbound state while keeping its storage. Everything
// that can hold a borrowed view is cleared rather than only truncated: the
// collected alternatives, the probe's one-row columns, and the batch's borrowed
// keys all point into the previous execution's inner snapshot, and leaving them
// reachable through the retained capacity would pin that whole collection's
// byte arena for the Workspace's lifetime. This is the discipline
// runGroupedInto already applies to stale groups and prepareResult to stale
// cells; a join has three more places to apply it because it borrows from a
// second collection.
func (b *joinBinding) reset() {
	clear(b.lits)
	b.lits = b.lits[:0]
	b.needles = b.needles[:0]
	b.text = b.text[:0]
	b.textReserved = 0
	b.needleText = b.needleText[:0]
	clear(b.keys)
	b.keys = b.keys[:0]
	b.mode = joinBindNone
	b.snapshot = store.Snapshot{}
	b.plan = nil
	b.candidates = 0
	b.filtering = false
	b.overflowed = false
	b.scanned = 0
	b.outerRows = 0
	b.scan.clearBorrowedViews()
	b.scan.cancel = nil
	b.scan.heapWorkParent = nil
	b.scan.heapWorkTextReserved = 0
	b.bloom.disable()
	b.build.reset()
	b.file.reset()
}

// resetJoinBindings clears every logical binding before a reused Exec starts
// another query. The slice and all of its buffers remain as warm high-water
// storage, but no prior heap or durable snapshot, borrowed scalar, or built
// row stays reachable when the next plan has fewer joins, no joins, LIMIT 0,
// or fails before binding.
func (w *Workspace) resetJoinBindings() {
	for i := range w.joins {
		w.joins[i].reset()
	}
	w.eval.bindTo(nil)
	if w.pool != nil {
		for i := range w.pool.workers {
			w.pool.workers[i].eval.bindTo(nil)
		}
	}
	w.scanUsed = 0
}

// bindJoins resolves every join clause against catalog and fills w's bindings,
// so that by the time the outer scan starts each predInBound leaf is either a
// membership it can search (and candidate generation can lower to a mask
// intersection) or a probe it can call.
//
// outer is the driving collection's own view, consulted only for its declared
// index catalog: whether the outer join path carries a ready exact index is
// what decides if building needles for the collected set can pay for itself.
func (p *plan) bindJoins(
	w *Workspace,
	outer store.Snapshot,
	catalog store.DatabaseSnapshot,
	limit, ratio int,
	pairBudget *joinPairBudget,
	workBudget *heapWorkBudget,
	stats *ExecStats,
) error {
	if err := w.checkCanceled(); err != nil {
		return err
	}
	if len(p.joins) == 0 {
		// A plan with no joins still clears the evaluator's bindings, so a
		// Workspace reused across a joined and an unjoined query cannot leave
		// the second one aliasing the first one's collected sets.
		w.eval.bindTo(nil)
		w.scanUsed = 0
		return nil
	}
	if limit <= 0 {
		limit = joinMembershipMax
	}
	for len(w.joins) < len(p.joins) {
		w.joins = append(w.joins, joinBinding{})
	}
	// The calling goroutine's own evaluator is pointed at the bindings here;
	// the filter phase's workers are pointed at the same ones when their share
	// is assigned. Everything either of them writes lands in its own probe
	// scratch, which is what makes one set of bindings safe to share.
	defer func() { w.eval.bindTo(w.joins[:len(p.joins)]) }()
	w.storeIndexes = outer.AppendIndexes(w.storeIndexes[:0])
	for i := range p.joins {
		if err := cancellationCheckpoint(w.cancel, i); err != nil {
			return err
		}
		j := &p.joins[i]
		b := &w.joins[j.slot]
		if err := j.bind(
			b, catalog, limit, outer.Len(), ratio,
			pairBudget, workBudget, w.cancel, stats,
		); err != nil {
			return err
		}
		if b.mode == joinBindSet {
			if err := b.buildNeedles(
				p.valuePaths[j.outerPath].indexPath(),
				w.storeIndexes,
				workBudget,
			); err != nil {
				if !errors.Is(err, ErrWorkBudget) {
					return err
				}
				// Needles only push the finished membership into the outer
				// exact index. The membership itself is authoritative, so a
				// tight budget declines this optional acceleration rather than
				// rejecting a query that can scan and recheck it.
				b.needles = b.needles[:0]
			}
		}
	}
	return nil
}

// bind runs one join clause's inner side and installs the strategy it measured.
func (j *planJoin) bind(
	b *joinBinding,
	catalog store.DatabaseSnapshot,
	limit, outerRows, ratio int,
	pairBudget *joinPairBudget,
	workBudget *heapWorkBudget,
	cancel *CancelFlag,
	stats *ExecStats,
) error {
	b.reset()
	b.scan.cancel = cancel
	if err := b.scan.checkCanceled(); err != nil {
		return err
	}
	inner, ok := catalog.Collection(j.collection)
	if !ok {
		return fmt.Errorf(
			"query: join: collection %q is not in the database snapshot", j.collection)
	}

	if j.fanOut {
		// A fan-out clause has nothing to measure: the pairs have to be
		// produced, so the joined side has to be materialized whatever its
		// size. The whole adaptive apparatus above it is for the question this
		// clause is not asking.
		if err := j.buildSide(b, inner, pairBudget, workBudget); err != nil {
			return err
		}
		b.mode = joinBindBuild
		if stats != nil {
			stats.JoinBuilds++
			stats.JoinBuildRows += uint64(len(b.build.rows))
		}
		return nil
	}

	// The inner side may stop early only where the join can fall back to a
	// lookup, which needs the O(1) keyed probe. Joining against an inner field
	// has no such probe — an exact index answers one value per call, and without
	// one the fallback degrades to an inner scan per outer row — so that set is
	// collected in full rather than truncated. A join never returns partial
	// results; the threshold picks between strategies where two exist.
	stoppable := j.innerPath == joinPrimaryKey
	workMark := int64(0)
	if workBudget != nil {
		workMark = workBudget.checkpoint()
	}
	overflowed, err := j.collect(
		b, inner, limit, outerRows, ratio, stoppable, workBudget,
	)
	if err != nil {
		if !stoppable || !errors.Is(err, ErrWorkBudget) {
			return err
		}
		// A primary-key semi-join can always trade the build-side scan for an
		// exact lookup per outer row. A memory admission failure therefore
		// selects that lower-memory strategy rather than rejecting a query
		// whose semantics do not require materializing the inner side.
		b.discardJoinAttempt()
		workBudget.rollback(workMark)
		overflowed = true
		b.filtering = false
		b.bloom.disable()
	}
	b.snapshot = inner
	b.plan = j.inner
	j.installStrategy(b, overflowed, stats)
	return b.scan.checkCanceled()
}

// discardJoinAttempt makes an adaptive primary-key build's storage unreachable
// before its budget reservation is rolled back. This is an exhaustion path,
// so correctness wins over warm reuse: retaining the abandoned capacities
// would let the driving scan allocate the rolled-back bytes alongside them and
// exceed the execution limit in real memory.
func (b *joinBinding) discardJoinAttempt() {
	b.lits = nil
	b.needles = nil
	b.text = nil
	b.textReserved = 0
	b.needleText = nil
	b.starts = nil
	b.entries = nil
	b.rows = nil
	b.keys = nil
	b.masks = nil
	b.scan.Release()
	b.file.index.Release()
	b.file.docs = store.Segment{}
	b.file.data = nil
	b.file.ends = nil
	b.file.keys = nil
	b.file.keyText = nil
	b.file.overflow = nil
	b.file.masks = nil
	b.file.batchRows = 0
	b.file.snapshot = nil
	b.bloom.blocks = nil
	b.bloom.disable()
}

// installStrategy turns a finished inner collection into the binding the outer
// scan will read: a lookup if the set overflowed its threshold, a sorted
// membership if it did not.
//
// It is shared by the heap and durable binders rather than written twice
// because the strategy choice is a property of the measured cardinalities, not
// of the storage backend. Two copies would be two places for the threshold, the
// deduplication, and the filter's arming to drift — and the differential tests
// compare results, which would not notice a backend quietly choosing the other
// sound strategy. The caller has already installed whichever inner snapshot a
// probe would resolve against.
func (j *planJoin) installStrategy(b *joinBinding, overflowed bool, stats *ExecStats) {
	if overflowed {
		// The partial set is dropped rather than kept and topped up: it is a
		// prefix of the inner side in chunk order, and a prefix is exactly the
		// silent truncation this must not produce. What survives it is the
		// filter, which summarizes every surviving key rather than a prefix of
		// them and is therefore not a truncation of anything.
		clear(b.lits)
		b.lits = b.lits[:0]
		b.text = b.text[:0]
		b.mode = joinBindProbe
		if stats != nil {
			stats.JoinLookups++
			if b.bloom.active {
				stats.JoinFilters++
				stats.JoinFilterKeys += b.bloom.inserted
			}
		}
		return
	}

	// A membership never consults the filter, and leaving one armed would only
	// invite a future reader to wire it into a path where a false positive has
	// no exact probe behind it to correct it.
	b.bloom.disable()
	slices.SortFunc(b.lits, compareScalar)
	b.lits = slices.CompactFunc(b.lits, func(a, c scalar) bool { return compareScalar(a, c) == 0 })
	b.mode = joinBindSet
	if stats != nil {
		stats.JoinMemberships++
		stats.JoinKeys += uint64(len(b.lits))
	}
}

// buildSide materializes the joined collection for a fan-out clause: every row
// that passes the clause's own filter, addressed by its join value.
//
// It reuses the same batched mask walk the semi-join collection uses, for the
// same reason it exists there — an inner side of ten million rows is read
// joinBatchRows at a time rather than as one column per path — but it never
// stops early. A partial build is not a cheaper build, it is a wrong answer:
// the rows it did not see are pairs the query will not return.
func (j *planJoin) buildSide(
	b *joinBinding,
	inner store.Snapshot,
	budget *joinPairBudget,
	workBudget *heapWorkBudget,
) error {
	scan := &b.scan
	if err := scan.checkCanceled(); err != nil {
		return err
	}
	scan.heapWorkParent = workBudget
	scan.heapWorkTextReserved = 0
	scan.eval.setWork(workBudget)
	scan.candidateUsed = 0
	scan.storeMaskUsed = 0
	scan.text = scan.text[:0]

	chunks := inner.Chunks()
	if workBudget != nil {
		if err := workBudget.admitJoinCandidates(
			j.inner, inner.Len(), heapWorkSnapshot, false, chunks,
		); err != nil {
			return err
		}
	}
	masks, err := j.inner.storeCandidateMasks(inner, scan)
	if err != nil {
		return err
	}
	if err := scan.checkCanceled(); err != nil {
		return err
	}
	if masks == nil {
		b.masks = inner.AppendLiveMasks(b.masks[:0])
		masks = b.masks
	}
	candidates := 0
	for i, mask := range masks {
		if err := cancellationCheckpoint(scan.cancel, i); err != nil {
			return err
		}
		candidates += bits.OnesCount64(mask.Bits)
	}
	if workBudget != nil {
		if err := workBudget.admitJoinBatch(
			j.inner,
			min(candidates, joinBatchRows),
			heapWorkSnapshot,
			j.innerPath == joinPrimaryKey,
		); err != nil {
			return err
		}
	}
	b.rows = b.rows[:0]
	for i, mask := range masks {
		if err := cancellationCheckpoint(scan.cancel, i); err != nil {
			return err
		}
		for word := mask.Bits; word != 0; word &= word - 1 {
			b.rows = append(b.rows, store.Location{
				Chunk: mask.Chunk,
				Slot:  uint8(bits.TrailingZeros64(word)),
			})
			if len(b.rows) < joinBatchRows {
				continue
			}
			if err := j.drainBuild(b, inner, budget); err != nil {
				return err
			}
		}
	}
	if err := j.drainBuild(b, inner, budget); err != nil {
		return err
	}
	if err := b.build.index(); err != nil {
		return err
	}
	return scan.checkCanceled()
}

// drainBuild filters one batch of joined rows and records each survivor's join
// value and address. The batch is walked in ascending address order and the
// batches themselves arrive in mask order, so the build's entries end up in the
// joined collection's own scan order — which is what the chain construction in
// joinBuild.index then preserves.
func (j *planJoin) drainBuild(
	b *joinBinding,
	inner store.Snapshot,
	budget *joinPairBudget,
) error {
	if len(b.rows) == 0 {
		return nil
	}
	scan := &b.scan
	if err := scan.checkCanceled(); err != nil {
		return err
	}
	ctx := &scan.ctx
	ctx.s, ctx.rows = nil, len(b.rows)
	if err := ctx.extractSnapshotValues(
		j.inner, inner, j.inner.filterCols, b.rows, true, &scan.text, scan,
	); err != nil {
		return err
	}
	selected, err := j.inner.selectRows(ctx, nil, true, scan)
	if err != nil {
		return err
	}
	if err := scan.eval.firstError(); err != nil {
		return err
	}
	if j.innerPath == joinPrimaryKey {
		b.keys = inner.AppendRowKeys(b.keys[:0], b.rows)
	}
	for at, row := range selected {
		if err := cancellationCheckpoint(scan.cancel, at); err != nil {
			return err
		}
		if j.innerPath == joinPrimaryKey {
			cell := scalar{kind: kindString, sval: b.keys[row]}
			if err := budget.admitBuild(joinBuildEntryBytes(cell)); err != nil {
				return err
			}
			b.build.text, _ = copyJoinString(b.build.text, b.keys[row])
			if err := b.build.add(
				lastJoinString(b.build.text, len(b.keys[row])),
				b.rows[row],
			); err != nil {
				return err
			}
			continue
		}
		if err := b.appendBuild(
			ctx.values[j.innerPath][row], b.rows[row], budget,
		); err != nil {
			return err
		}
	}
	b.rows = b.rows[:0]
	// The next batch refills the decoded-string arena in place, so nothing may
	// still be viewing it; every value kept has been copied into the build's.
	scan.text = scan.text[:0]
	return nil
}

// collect runs the inner predicate and appends the surviving rows' join-key
// values to b, reporting whether the set passed limit.
//
// It works in batches so the bound is a bound on work and memory, not only on
// what survives: an inner side of ten million rows extracts joinBatchRows of
// them at a time and abandons the materialized set on the first batch that
// carries it past the threshold.
//
// Past that point the scan either stops or continues into a Bloom filter, which
// is a summary of fixed size rather than a set that grows — so continuing costs
// scan time but no more memory. joinBloomWorthwhile decides which, before the
// first row is read, from cardinalities that are already known exactly.
func (j *planJoin) collect(
	b *joinBinding,
	inner store.Snapshot,
	limit, outerRows, ratio int,
	stoppable bool,
	workBudget *heapWorkBudget,
) (bool, error) {
	scan := &b.scan
	if err := scan.checkCanceled(); err != nil {
		return false, err
	}
	scan.heapWorkParent = workBudget
	scan.heapWorkTextReserved = 0
	scan.eval.setWork(workBudget)
	scan.candidateUsed = 0
	scan.storeMaskUsed = 0
	scan.text = scan.text[:0]

	chunks := inner.Chunks()
	if workBudget != nil {
		if err := workBudget.admitJoinCandidates(
			j.inner, inner.Len(), heapWorkSnapshot, false, chunks,
		); err != nil {
			return false, err
		}
	}
	masks, err := j.inner.storeCandidateMasks(inner, scan)
	if err != nil {
		return false, err
	}
	if err := scan.checkCanceled(); err != nil {
		return false, err
	}
	if masks == nil {
		// An unbounded inner predicate scans the whole inner collection. The
		// live universe is one word per chunk, so naming it this way costs the
		// chunk count rather than the row count even on a large collection.
		b.masks = inner.AppendLiveMasks(b.masks[:0])
		masks = b.masks
	}

	// The candidate count is the exact number of inner rows this scan will
	// read: one bit per row, already materialized as words. Counting them is a
	// popcount per chunk, and it is the input the filter decision needs — an
	// exact cardinality rather than an estimate of one.
	candidates := 0
	for i, mask := range masks {
		if err := cancellationCheckpoint(scan.cancel, i); err != nil {
			return false, err
		}
		candidates += bits.OnesCount64(mask.Bits)
	}
	b.candidates = candidates
	b.outerRows = outerRows
	b.ratio = joinBloomEffectiveRatio(ratio)
	b.filtering = stoppable && joinBloomWorthwhile(candidates, outerRows, limit, b.ratio)
	if workBudget != nil {
		if err := workBudget.admitJoinBatch(
			j.inner,
			min(candidates, joinBatchRows),
			heapWorkSnapshot,
			j.innerPath == joinPrimaryKey,
		); err != nil {
			return false, err
		}
	}

	b.rows = b.rows[:0]
	for i, mask := range masks {
		if err := cancellationCheckpoint(scan.cancel, i); err != nil {
			return false, err
		}
		for word := mask.Bits; word != 0; word &= word - 1 {
			b.rows = append(b.rows, store.Location{
				Chunk: mask.Chunk,
				Slot:  uint8(bits.TrailingZeros64(word)),
			})
			if len(b.rows) < joinBatchRows {
				continue
			}
			if over, err := j.drain(
				b, inner, limit, stoppable, workBudget,
			); over || err != nil {
				return over, err
			}
		}
	}
	over, err := j.drain(b, inner, limit, stoppable, workBudget)
	if err != nil {
		return false, err
	}
	// A filtering scan never returns early, so its overflow is reported by the
	// flag the seeding step set rather than by an early return.
	return over || b.overflowed, nil
}

// joinBloomWorthwhile decides whether a join that has outgrown its membership
// should keep scanning the inner side to build a filter, or stop now and probe
// every outer row.
//
// Both inputs are exact counts, not estimates: candidates is the popcount of
// the inner candidate masks and outerRows is the driving snapshot's length. The
// only judgement here is the ratio, and that is a ratio of two measured per-row
// costs rather than a guess about the data — see joinBloomScanRatio.
//
// The one thing genuinely unknown at this point is how many outer rows the
// filter would reject, and no amount of scanning the inner side reveals it. So
// the rule is written to bound the loss rather than to predict the gain: scan
// at most joinBloomScanRatio inner rows per outer row, which caps what a
// completely ineffective filter can cost at a fraction of what an effective one
// saves. A filter is never built where the lookup path is already cheap enough
// that it could not repay the scan.
// joinBloomEffectiveRatio resolves the caller's setting: zero means the
// measured default and a negative value declines the filter outright.
func joinBloomEffectiveRatio(ratio int) int {
	if ratio == 0 {
		return joinBloomScanRatio
	}
	return ratio
}

func joinBloomWorthwhile(candidates, outerRows, limit, ratio int) bool {
	if ratio < 0 {
		return false // the caller declined the filter outright
	}
	if candidates <= limit {
		// The set fits the membership; this scan will not overflow at all.
		return false
	}
	if outerRows <= 0 {
		return false // nothing to prefilter
	}
	high, low := bits.Mul64(uint64(outerRows), uint64(ratio))
	return high != 0 || uint64(candidates) <= low
}

// drain filters one accumulated batch of inner rows and collects the join-key
// value of each survivor, then empties the batch. It reports whether the
// collected set passed limit, which only a join that can fall back to a lookup
// ever asks about.
func (j *planJoin) drain(
	b *joinBinding,
	inner store.Snapshot,
	limit int,
	stoppable bool,
	workBudget *heapWorkBudget,
) (bool, error) {
	if len(b.rows) == 0 {
		return false, nil
	}
	scan := &b.scan
	if err := scan.checkCanceled(); err != nil {
		return false, err
	}
	ctx := &scan.ctx
	ctx.s, ctx.rows = nil, len(b.rows)
	if err := ctx.extractSnapshotValues(
		j.inner, inner, j.inner.filterCols, b.rows, true, &scan.text, scan,
	); err != nil {
		return false, err
	}
	selected, err := j.inner.selectRows(ctx, nil, true, scan)
	if err != nil {
		return false, err
	}
	if err := scan.eval.firstError(); err != nil {
		return false, err
	}
	if j.innerPath == joinPrimaryKey {
		b.keys = inner.AppendRowKeys(b.keys[:0], b.rows)
	}
	for at, row := range selected {
		if err := cancellationCheckpoint(scan.cancel, at); err != nil {
			return false, err
		}
		if b.overflowed {
			// Past the threshold the set is not grown any further, but the scan
			// is still running because a filter is being built out of it, and a
			// filter that saw only some of the keys would produce false
			// negatives — the one error a Bloom filter must never have, because
			// nothing downstream re-checks a rejection.
			b.bloom.insert(hashJoinKey(b.keys[row]))
			continue
		}
		if j.innerPath == joinPrimaryKey {
			if err := b.appendString(b.keys[row], workBudget); err != nil {
				return false, err
			}
		} else {
			value := ctx.values[j.innerPath][row]
			if value.kind == kindNull {
				continue
			}
			if err := b.appendValue(value, workBudget); err != nil {
				return false, err
			}
		}
		if !stoppable || len(b.lits) <= limit {
			continue
		}
		if !b.filtering {
			return true, nil
		}
		// The threshold is crossed and a filter is wanted. Seed it from the
		// alternatives already collected before they are released, so the
		// filter covers the whole inner side and not just its tail.
		if workBudget != nil {
			if err := workBudget.resetJoinBloom(&b.bloom, b.candidates); err != nil {
				return false, err
			}
		} else {
			b.bloom.reset(b.candidates)
		}
		b.overflowed = true
		for i := range b.lits {
			if err := cancellationCheckpoint(scan.cancel, i); err != nil {
				return false, err
			}
			b.bloom.insert(hashJoinKey(b.lits[i].sval))
		}
	}
	b.scanned += len(b.rows)
	b.rows = b.rows[:0]
	// The next batch's extraction refills the decoded-string arena in place, so
	// nothing may still be viewing it. appendString and appendValue have already
	// copied everything kept into b's own storage, and the filter keeps hashes
	// rather than bytes.
	scan.text = scan.text[:0]
	if b.overflowed && !b.keepFiltering() {
		// The filter cannot repay the rest of the scan. Abandoning it is always
		// sound — the probe behind it is what decides every row — so the only
		// thing lost is the scan already spent, which is why this is checked
		// after every batch rather than at the end.
		b.filtering = false
		b.bloom.disable()
		return true, nil
	}
	return false, nil
}

// keepFiltering re-decides, on the evidence of the rows already scanned,
// whether finishing the inner scan can still pay for itself.
//
// The pre-scan gate had to assume how much of the outer side a filter would
// reject. This does not: the inner predicate's selectivity is now observed on
// real rows, and a filter over an inner predicate that keeps nearly everything
// cannot reject much of anything, because a filter holding nearly every inner
// key admits every outer key that exists. That is the case the pre-scan gate
// cannot see and the case where a filter is pure loss — measured at 1.56x
// slower than never building one.
//
// The comparison weighs the gain against the WHOLE scan, not against the part
// of it still to come. That is not what the sunk-cost argument says it should
// be, and the sunk-cost argument is the one that lost. An earlier version here
// compared against the remaining scan on the reasoning that rows already read
// are paid for whichever way this goes; BenchmarkJoinBloomPrefilter then
// measured that version keeping a filter at 50% inner selectivity and being 7%
// slower for it (297.2 against 278.2 ns per outer row), where the total form
// abandons and lands within noise of not building one (283.9 against 280.6).
// The marginal model is wrong because it accounts for none of what a surviving
// filter goes on to cost: a test on every outer row, and 64 KiB of blocks
// competing for cache with the scan itself. Charging the whole scan is the
// cruder rule that happens to price those in.
//
// A completed scan is still never abandoned, for a reason the measurement does
// not contradict: at that point there is no scan left to save at all, only the
// per-row test, which a filter repays by rejecting almost anything.
// TestJoinBloomKeepsAFilterWhoseScanIsAlreadyPaid pins that branch.
//
// The proxy it rests on is worth stating plainly: it reads the inner
// predicate's selectivity as a stand-in for how many outer keys the filter will
// admit, which is exact only when the outer side's keys are spread across the
// inner collection rather than concentrated on the part of it that matches. A
// layout that concentrates them defeats this — see
// TestJoinBloomClusteredSelectivityDegradesGracefully, where it costs one batch
// of scanning and no rows. The engine does not model data distributions, and
// this does not start.
func (b *joinBinding) keepFiltering() bool {
	if !b.filtering {
		return true
	}
	remaining := b.candidates - b.scanned
	if remaining <= 0 {
		return true // nothing left to save; the filter is already paid for
	}
	// Compared as full-width products: on a 64-bit build a row count times an
	// outer row count times a ratio needs up to 189 bits, long after every
	// individual count remains a valid int.
	rejected := int64(b.scanned) - int64(b.bloom.inserted)
	return joinProduct3GreaterThan2(
		uint64(rejected),
		uint64(b.outerRows),
		uint64(b.ratio),
		uint64(b.candidates),
		uint64(b.scanned),
	)
}

// joinProduct3GreaterThan2 compares a*b*c > x*y without narrowing either
// product. Join cardinalities are signed ints but non-negative at this point;
// on 64-bit systems their three-factor product needs up to 189 bits, so a
// three-limb multiply is both exact and cheaper than allocating a big.Int in a
// check that runs once per inner batch.
func joinProduct3GreaterThan2(a, b, c, x, y uint64) bool {
	abHigh, abLow := bits.Mul64(a, b)
	midFromLow, low := bits.Mul64(abLow, c)
	high, midFromHigh := bits.Mul64(abHigh, c)
	mid, carry := bits.Add64(midFromLow, midFromHigh, 0)
	high, _ = bits.Add64(high, 0, carry)

	rightHigh, rightLow := bits.Mul64(x, y)
	if high != 0 {
		return true
	}
	if mid != rightHigh {
		return mid > rightHigh
	}
	return low > rightLow
}

// appendString collects one alternative spelled as a string: a collection key,
// or a string-valued inner field. A collection key is a string, so an outer
// join value of any other kind matches nothing — the same in-kind rule [Cmp]
// and [In] apply, reached here without a special case.
func (b *joinBinding) appendString(
	text string,
	workBudget *heapWorkBudget,
) error {
	if workBudget != nil {
		if err := workBudget.admitJoinMember(); err != nil {
			return err
		}
	}
	if err := b.reserveText(len(text), workBudget); err != nil {
		return err
	}
	b.text, _ = copyJoinString(b.text, text)
	b.lits = append(b.lits, lastJoinString(b.text, len(text)))
	return nil
}

// copyJoinString appends text to an arena and returns the arena plus the mark
// the copy starts at.
func copyJoinString(arena []byte, text string) ([]byte, int) {
	mark := len(arena)
	return append(arena, text...), mark
}

// lastJoinString views the final n bytes of an arena as a string scalar. The
// view is capped to its own length so a later append cannot extend it.
func lastJoinString(arena []byte, n int) scalar {
	return scalar{
		kind: kindString,
		sval: byteview.String(arena[len(arena)-n : len(arena) : len(arena)]),
	}
}

// copyJoinValue copies the bytes a join value compares by into arena and
// returns the copy alongside the grown arena. It is the shared half of
// collecting a value, used by the semi-join's alternative set and by the
// fan-out build side, which differ in what they do with the result rather than
// in what they have to copy.
func copyJoinValue(arena []byte, cell scalar) ([]byte, scalar) {
	switch cell.kind {
	case kindBool:
		return arena, scalar{kind: kindBool, bval: cell.bval}
	case kindNumber:
		arena, mark := copyJoinString(arena, byteview.String(cell.num))
		num := arena[mark:len(arena):len(arena)]
		return arena, scalar{
			kind: kindNumber, num: num, raw: num, isInt: cell.isInt, ival: cell.ival,
		}
	case kindString:
		arena, _ = copyJoinString(arena, cell.sval)
		return arena, lastJoinString(arena, len(cell.sval))
	default:
		arena, mark := copyJoinString(arena, byteview.String(cell.raw))
		return arena, scalar{
			kind: kindContainer, raw: arena[mark:len(arena):len(arena)],
		}
	}
}

// appendValue collects one selected inner row's join-key cell, copying the
// bytes it compares by into b's own storage. A null or absent inner key is
// dropped: it matches no outer value, so admitting it would only add an
// alternative nothing can equal, and dropping it keeps the collected count an
// honest measure of how selective the join is.
func (b *joinBinding) appendValue(
	cell scalar,
	workBudget *heapWorkBudget,
) error {
	if cell.kind == kindNull {
		return nil
	}
	if workBudget != nil {
		if err := workBudget.admitJoinMember(); err != nil {
			return err
		}
	}
	if err := b.reserveText(joinValuePayloadBytes(cell), workBudget); err != nil {
		return err
	}
	arena, copied := copyJoinValue(b.text, cell)
	b.text = arena
	b.lits = append(b.lits, copied)
	return nil
}

// reserveText makes an append safe for every scalar already viewing b.text.
//
// A plain append is not enough here: when it reallocates, every earlier scalar
// keeps its old backing array alive. Repeating that over a large membership
// retains every geometric generation, so charging only the final capacity is
// unsound. This grows under our control, admits the exact old+new peak before
// allocation, copies once, and rebases every existing view onto the new arena.
// After it returns the old generation is unreachable.
func (b *joinBinding) reserveText(
	additional int,
	workBudget *heapWorkBudget,
) error {
	if additional < 0 || len(b.text) > int(^uint(0)>>1)-additional {
		return fmt.Errorf("query: join membership text exceeds the address space")
	}
	required := len(b.text) + additional
	if required <= cap(b.text) {
		if workBudget == nil {
			return nil
		}
		return workBudget.admitJoinText(int64(required), &b.textReserved)
	}

	nextCapacity := required
	if nextCapacity < 64 {
		nextCapacity = 64
	}
	if current := cap(b.text); current <= int(^uint(0)>>1)/2 {
		if doubled := current * 2; doubled > nextCapacity {
			nextCapacity = doubled
		}
	}
	peak := saturatedBytes(int64(cap(b.text)), int64(nextCapacity))
	if workBudget != nil {
		if err := workBudget.admitJoinText(peak, &b.textReserved); err != nil {
			return err
		}
	}

	next := make([]byte, len(b.text), nextCapacity)
	copy(next, b.text)
	cursor := 0
	for i := range b.lits {
		n := joinValuePayloadBytes(b.lits[i])
		end := cursor + n
		switch b.lits[i].kind {
		case kindNumber:
			b.lits[i].num = next[cursor:end:end]
			b.lits[i].raw = b.lits[i].num
		case kindString:
			b.lits[i].sval = byteview.String(next[cursor:end:end])
		case kindContainer:
			b.lits[i].raw = next[cursor:end:end]
		}
		cursor = end
	}
	b.text = next
	return nil
}

func joinValuePayloadBytes(cell scalar) int {
	switch cell.kind {
	case kindNumber:
		return len(cell.num)
	case kindString:
		return len(cell.sval)
	case kindContainer:
		return len(cell.raw)
	default:
		return 0
	}
}

// appendBuild records one surviving joined row on the fan-out build side: its
// join value copied into the build's own arena, paired with the address the
// expansion will read its columns from. A null or absent join value is dropped
// for the same reason a semi-join drops it — it matches nothing — and dropping
// it here also keeps it out of a chain no probe can ever reach.
func (b *joinBinding) appendBuild(
	cell scalar,
	row store.Location,
	budget *joinPairBudget,
) error {
	if cell.kind == kindNull {
		return nil
	}
	if err := budget.admitBuild(joinBuildEntryBytes(cell)); err != nil {
		return err
	}
	arena, copied := copyJoinValue(b.build.text, cell)
	b.build.text = arena
	return b.build.add(copied, row)
}

// buildNeedles renders the collected set as exact-index needles, which is what
// lets candidate generation lower the whole join to one mask intersection
// instead of a search per row. It is skipped entirely when the outer path has
// no ready single-column exact index, because a needle nothing will probe is
// pure cost — one index build per alternative, paid on the binding path where
// the membership strategy is trying to stay cheap.
func (b *joinBinding) buildNeedles(
	path string,
	indexes []store.IndexInfo,
	workBudget *heapWorkBudget,
) error {
	b.needles = b.needles[:0]
	if len(b.lits) == 0 {
		return nil
	}
	if _, ok := singleColumnIndex(path, indexes); !ok {
		return nil
	}
	var payloadBytes int64
	for i, lit := range b.lits {
		if err := cancellationCheckpoint(b.scan.cancel, i); err != nil {
			return err
		}
		var bytes int
		switch lit.kind {
		case kindBool:
			if lit.bval {
				bytes = len(trueNeedle)
			} else {
				bytes = len(falseNeedle)
			}
		case kindNumber:
			bytes = len(lit.num)
		case kindString:
			bytes = jsonStringEncodedBytes(lit.sval)
		default:
			// A container alternative has no scalar needle, and one missing
			// needle makes the whole set unindexable rather than partially
			// bounded, which would be unsound. Detect it before growing any
			// needle storage.
			b.needleText = b.needleText[:0]
			b.starts = b.starts[:0]
			return nil
		}
		payloadBytes = saturatedBytes(payloadBytes, int64(bytes))
	}
	if workBudget != nil {
		if err := workBudget.admitJoinNeedles(
			len(b.lits), payloadBytes,
		); err != nil {
			return err
		}
	}
	// Rendered in one pass and indexed in a second, so every needle is built
	// over the final buffer. Growing between builds would in fact be safe —
	// append leaves the old array alive and unchanged behind the views holding
	// it — but having to reason about that at every call site is not worth the
	// two lines this costs.
	b.needleText = b.needleText[:0]
	starts := b.starts[:0]
	for i, lit := range b.lits {
		if err := cancellationCheckpoint(b.scan.cancel, i); err != nil {
			return err
		}
		starts = append(starts, len(b.needleText))
		switch lit.kind {
		case kindBool:
			if lit.bval {
				b.needleText = append(b.needleText, trueNeedle...)
			} else {
				b.needleText = append(b.needleText, falseNeedle...)
			}
		case kindNumber:
			b.needleText = append(b.needleText, lit.num...)
		case kindString:
			b.needleText = appendJSONString(b.needleText, lit.sval)
		}
	}
	starts = append(starts, len(b.needleText))
	b.starts = starts
	b.entries = resize(b.entries, len(b.lits))
	for i := range b.lits {
		if err := cancellationCheckpoint(b.scan.cancel, i); err != nil {
			return err
		}
		src := b.needleText[starts[i]:starts[i+1]:starts[i+1]]
		index, err := vibejson.BuildIndex(src, b.entries[i:i+1:i+1])
		if err != nil {
			b.needles = b.needles[:0]
			return nil
		}
		b.needles = append(b.needles, index)
	}
	return nil
}

func jsonStringEncodedBytes(text string) int {
	bytes := 2 // opening and closing quotes
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t':
			bytes += 2
		case c < 0x20:
			bytes += 6
		default:
			bytes++
		}
	}
	return bytes
}

// A joinProbe is one evaluator's mutable state for one join clause: the one-row
// columns a probe resolves the inner filter's paths into, the arena those
// columns decode escaped strings through, and the tallies of what the clause
// did.
//
// It is per evaluator rather than per binding because the filter phase runs on
// several goroutines: the binding they share is read-only after bind, and this
// is everything that is not. It is also exactly the split the durable backend
// will need, whose executor is parallel by construction.
type joinProbe struct {
	// cols is len(inner.valuePaths) columns of exactly one row. The inner
	// predicate indexes columns by row like any other, so giving it a one-row
	// column is what lets a probe reuse the ordinary evaluator rather than grow
	// a second one.
	cols [][]scalar
	// inner is the scratch the inner predicate itself evaluates through. It
	// carries no bindings, because a join's inner side may not itself join.
	inner evalScratch

	// raw is the durable probe's copy-out buffer: a durable snapshot reads
	// through evictable page frames, so the addressed document is copied here
	// rather than borrowed. Retaining it across rows and executions is what
	// makes the copy cost no allocation once the widest document seen fits.
	// The heap probe leaves it nil.
	raw []byte
	// err is the first I/O error a durable probe hit. Predicate evaluation
	// yields a bool with no error channel, so a fault is parked here and the
	// row rejected; the executor checks every worker's probe once the filter
	// phase is done and fails the execution rather than returning a result
	// whose rows were decided by a page that could not be read.
	err error

	probes   uint64
	tested   uint64
	admitted uint64
}

// reset clears a probe scratch for a new execution. The scalars are cleared
// rather than truncated because they view the previous execution's inner
// documents, the same discipline every other retained buffer here follows.
func (pr *joinProbe) reset() {
	for i := range pr.cols {
		clear(pr.cols[i])
	}
	pr.inner.entries = pr.inner.entries[:0]
	pr.inner.setWork(nil)
	pr.raw = pr.raw[:0]
	pr.err = nil
	pr.probes, pr.tested, pr.admitted = 0, 0, 0
}

// sized returns pr's one-row columns, grown to the inner plan's path count.
// Columns past that count are cleared before being cut loose, so a probe
// scratch reused by a narrower plan stops pinning the wider one's documents.
func (pr *joinProbe) sized(paths int) [][]scalar {
	for i := paths; i < len(pr.cols); i++ {
		clear(pr.cols[i])
	}
	pr.cols = resize(pr.cols, paths)
	for i := range pr.cols {
		pr.cols[i] = resize(pr.cols[i], 1)
	}
	return pr.cols
}

// matches reports whether one outer join-key cell has a partner on the inner
// side. It is the single existential test every strategy answers, which is why
// the adaptive choice can never change a result.
func (b *joinBinding) matches(cell scalar, pr *joinProbe) bool {
	// A null or absent outer key joins to nothing. This is one of the two
	// places the rule is enforced — appendValue drops a null inner key for the
	// same reason — and either one alone would keep the answers right, because
	// a set that holds no null cannot match a null and a null cannot name a
	// collection key. Both are kept because the rule is symmetric and a reader
	// who found it stated on only one side would reasonably conclude the other
	// side was an oversight.
	if cell.kind == kindNull {
		return false
	}
	switch b.mode {
	case joinBindSet:
		return memberOf(b.lits, cell)
	case joinBindProbe:
		// One binding names one inner collection, so the backend is fixed for
		// the whole execution and this branch is perfectly predicted rather
		// than being a per-row decision in any real sense.
		if b.file.snapshot != nil {
			return b.probeFile(cell, pr)
		}
		return b.probe(cell, pr)
	case joinBindBuild:
		// The filter phase asks the existential question even for a fan-out
		// clause, because a driving row with no partner contributes no pair and
		// dropping it here spares the expansion a lookup that would find
		// nothing — and because the driving join column has to be a filter
		// column for the expansion to read it at all.
		return b.build.has(cell)
	default:
		return false
	}
}

// probe answers one outer row by looking its key up in the inner collection and
// evaluating the inner filter against that one document. The lookup is a hash
// hit on borrowed bytes and the filter reads a fixed number of paths, so the
// cost is constant in the inner collection's size — the property that makes
// this the sound fallback when a membership would be unbounded.
//
// It reads through GetRaw and resolves each path against the raw bytes, rather
// than through Get and the store's own tape. Get would parse each document at
// most once however many paths the filter names, but on a shape-taped chunk it
// takes a widening mutex and memoizes a classic tape — it mutates the
// collection being read, on a path whose whole justification is that it
// materializes nothing, and it would do so from several filter-phase workers at
// once. The cost of that choice is one validating scan of the document per path
// rather than per probe, which is why a filter naming several paths is the
// shape most worth revisiting if this ever becomes the bottleneck; the common
// single-field filter pays exactly one scan either way.
func (b *joinBinding) probe(cell scalar, pr *joinProbe) bool {
	if err := cancellationError(b.scan.cancel); err != nil {
		if pr.err == nil {
			pr.err = err
		}
		return false
	}
	if cell.kind != kindString {
		return false // collection keys are strings; nothing else can name one
	}
	if b.bloom.active && !b.bloom.admits(hashJoinKey(cell.sval), pr) {
		// The filter holds every key that survived the inner filter, so a
		// rejection here is certain and needs nothing behind it. This is the
		// case worth avoiding: the outer key usually exists in the inner
		// collection and it is the inner predicate that rejects it, which
		// without the filter costs a lookup, a decode, and an evaluation to
		// learn.
		return false
	}
	pr.probes++
	raw, ok := b.snapshot.GetRaw(cell.sval)
	if err := cancellationError(b.scan.cancel); err != nil {
		if pr.err == nil {
			pr.err = err
		}
		return false
	}
	if !ok {
		return false
	}
	if b.plan.where == nil {
		return true
	}
	cols := pr.sized(len(b.plan.valuePaths))
	pr.inner.text = pr.inner.text[:0]
	for i := range b.plan.valuePaths {
		if err := cancellationCheckpoint(b.scan.cancel, i); err != nil {
			if pr.err == nil {
				pr.err = err
			}
			return false
		}
		value, found, err := raw.PointerCompiled(b.plan.valuePaths[i].pointer)
		if err != nil || !found {
			value = vibejson.RawValue{}
		}
		cols[i][0] = classifyRawInto(value, &pr.inner.text)
	}
	return b.plan.where.eval(cols, 0, &pr.inner)
}

// memberOf is [compiledPredicate.member] over a caller-supplied set: the same
// short-set linear scan and long-set binary search, reading a binding's
// alternatives instead of a compiled node's.
func memberOf(lits []scalar, cell scalar) bool {
	if len(lits) <= inLinearMax {
		for i := range lits {
			if compareScalar(lits[i], cell) == 0 {
				return true
			}
		}
		return false
	}
	lo, hi := 0, len(lits)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if compareScalar(lits[mid], cell) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(lits) && compareScalar(lits[lo], cell) == 0
}
