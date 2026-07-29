// Package query is a typed query engine over a
// [store.Segment], heap [store.Snapshot], or a durable snapshot: the product
// layer that turns indexing, projection, containment, and grouping primitives
// into one compiled plan. The exported programmatic builder is the typed plan
// API; SQL through [PrepareStatement] and [PrepareDML] is the sole textual
// query language. Each document is one row and columns are JSON paths. It
// answers SELECT of path projections and aggregates
// (COUNT, SUM, AVG, MIN, MAX); WHERE with comparisons, containment (@>),
// existence, and null tests combined by And/Or/Not; cross-collection
// semi-joins and inner joins; GROUP BY; ORDER BY; and LIMIT. Subqueries and
// mutation are outside this executor; the sql/driver package layers cataloged
// DDL and whole-document DML over it.
//
// The builder and SQL parser are front ends for one immutable compiled plan,
// cached inside the [Query] that produced it. Compiling resolves paths to
// compiled pointers and numeric slots, predicates to typed operators, and
// literals to typed constants. The SQL text and builder tree are then
// discarded; neither is interpreted during execution:
//
//	q := query.Select(query.Path("name"), query.Sum("score")).
//		Where(query.Cmp("active", query.Eq, true)).
//		GroupBy("team").
//		OrderBy("team", query.Asc).
//		Limit(10)
//	result, err := q.Run(query.FromSegment(&docs))
//
// SQL is the textual and wire-facing query language. JSON remains the stored
// row representation, not a second query grammar:
//
//	stmt, err := query.PrepareStatement(`
//		SELECT team, SUM(score) AS total
//		FROM events
//		WHERE active = TRUE AND tier IN ('pro', 'team')
//		GROUP BY team
//		ORDER BY total DESC
//		LIMIT 10`)
//
// A [Statement] owns an internal compiler whose arenas a warmed placeholder binding
// refills rather than reallocates, making steady-state lowering allocation-free
// in the same way a warmed [Query.RunInto] is.
//
// Execution has exactly two entry points. [Query.Run] answers a one-off, and
// [Query.RunInto] runs into a caller-owned [Exec] that retains the destination
// Result, the scratch Workspace, the durable backend's options, and its
// reported stats. Both take a [Source], the discriminated handle naming the
// collection: [FromSegment], [FromSnapshot], [FromFile], [FromFileOverlay],
// [FromDatabase], or [FromFileDatabase]. One backend is therefore never a
// different call shape from another, and a hot loop is one Exec reused:
//
//	var e query.Exec
//	err = q.RunInto(&e, query.FromSnapshot(snapshot))
//
// A query with a [Join] clause reads a second collection and takes
// [FromDatabase] or [FromFileDatabase], so both sides come from one coherent
// database snapshot — the consistent cut that makes snapshot skew across a
// join inexpressible rather than merely discouraged.
//
// The clause is one of two operators, and which one is what it declares. Without
// [Join.As] it is a semi-join: it filters the driving collection by the
// existence of a partner, returns no column of the joined side, and keeps a
// driving row once however many documents match it. Which strategy answers that
// — a membership pushed into the driving predicate, a per-row keyed lookup, or
// that lookup behind a Bloom prefilter — is measured at execution rather than
// estimated, and reported in [ExecStats].
//
// With [Join.As] it is SQL's inner join: the joined collection is named, its
// columns are projectable, orderable, groupable, and reducible through that
// name, and the result carries one row per matching pair in driving-ordinal and
// then joined-ordinal order. See [Join], join.go, join_bloom.go, and
// join_build.go.
//
// [Query.Prepare] forces a programmatically built plan to compile eagerly so a
// malformed query fails where it was written. [PrepareStatement] is the SELECT
// SQL entry point: it returns a [Statement], which adds the three things a plan
// cannot carry — output names from AS aliases, '?' placeholders bound per
// execution, and the HAVING and OFFSET clauses the executor has no node for.
// [PrepareDML] lowers the same parser's non-SELECT statements for the typed SQL
// runtime. A Statement owns its own compiler, so re-binding a placeholder
// rebuilds the plan without allocating.
// The [github.com/thesyncim/vibedb/sql/driver] package exposes the same parsed
// and lowered statements through database/sql and documents in one place every
// way the supported subset differs from general SQL. Output has stable ordinal
// IDs through [Query.AppendSchema], and [Cell] exposes typed values plus
// caller-buffered [Cell.AppendJSON]. A transport encoder can therefore consume
// typed batches without header lookup or intermediate string formatting.
// Field-name bytes remain only in immutable compiled-path metadata because
// schemaless JSON has no external schema ID to replace them.
//
// The executor is column-oriented. Without an applicable posting bound it
// extracts each needed path as a dense column and evaluates WHERE in one full
// scan. With a selective bound it pushes the posting ordinals into extraction:
// [store.ShapeCache.AppendFieldRows] and
// [store.Segment.AppendPointerRows] gather only candidate cells, then the
// same compiled predicate rechecks them exactly before reduction, grouping,
// ordering, and limiting. A compiled query is immutable and safe to run
// concurrently; Run owns its transient scan state, while concurrent RunInto
// calls use one independent Exec per goroutine.
// A [FromFile] execution first late-binds exact persistent indexes. Catalog,
// mask-buffer, certificate, posting, and exact-recheck workspace is
// conservatively admitted before it can grow and debited from the configured
// memory target. A plan whose worst-case index workspace does not fit simply
// declines the optimization and scans every row; it is not a query error. The
// executor then indexes bounded raw batches in parallel, restores source order
// before partial reductions, and externally merges ordered projections or
// groups when their transient frontier reaches the remaining memory target.
// The caller owns the final result; it is outside that
// working-memory target but independently bounded by [ExecOptions.ResultRows]
// and [ExecOptions.ResultBytes].
//
// # Value semantics
//
// The engine defines the following, so results are predictable across every
// document shape:
//
//   - An absent path and an explicit JSON null are one value, "null". IsNull
//     tests for it; Exists distinguishes a present null from an absent path.
//   - Comparisons are within type. Numbers compare by exact decimal value —
//     1, 1.0, and 1e2 versus 100 compare as equals, and integers past
//     float64's mantissa stay distinct — strings by decoded content, bools by
//     value. Across types there is a defined total order (null < bool <
//     number < string < container) for ORDER BY and GROUP BY, and inequality
//     for =/!=; a null or absent value never satisfies a comparison.
//   - SUM, AVG, MIN, and MAX skip null and non-numeric values and are null
//     over an empty input. SUM, MIN, and MAX preserve exact decimal values.
//     AVG emits an exact finite quotient when it fits the 34-significant-digit
//     policy and otherwise rounds once, ties to even. Exact-decimal workspace
//     is bounded by [ExecOptions.AggregateBytes]; exhaustion returns
//     [ErrAggregateBudget] rather than substituting float64. COUNT(path)
//     counts present, non-null values; COUNT(*) counts rows.
//   - Duplicate object keys resolve to the last occurrence, matching the
//     core's Node.Get.
package query

import (
	"fmt"
	"strings"
	"sync"
)

// A Direction is an ORDER BY sort direction.
type Direction uint8

const (
	// Asc sorts ascending, nulls first.
	Asc Direction = iota
	// Desc sorts descending, nulls last.
	Desc
)

// A Query is a compiled, reusable query plan built by Select and the chaining
// methods. It is immutable once built; execution compiles it on first use and
// caches the compiled plan, so later executions reuse it and are safe to call
// concurrently — one Query, one [Exec] per goroutine. There is no separate
// prepared-plan type: a Query that has compiled is the prepared plan, which is
// why [Query.Prepare] only has to report whether compilation succeeded.
type Query struct {
	columns  []Column
	where    Predicate
	hasWhere bool
	joins    []Join
	groupBy  []string
	orderBy  []orderSpec
	limit    int
	hasLimit bool

	once       sync.Once
	plan       *plan
	compileErr error

	// built is the outcome a [compiler] installed directly. A compiler has
	// already compiled by the time it returns, so consulting this first is
	// what lets it reuse one Query value across compiles: sync.Once is not
	// resettable, and a second compile into a Query whose Once had already
	// fired would otherwise keep answering with the first compile's plan.
	built *compileResult
}

// A compileResult is a compiled plan or the failure that prevented one. It is
// one heap object rather than two Query fields so a compiler can publish both
// halves with a single pointer store.
type compileResult struct {
	plan *plan
	err  error
}

type orderSpec struct {
	path string
	dir  Direction
}

// Select begins a query that projects and aggregates the given columns. The
// columns become the result's columns, in order.
func Select(columns ...Column) *Query {
	return &Query{columns: columns}
}

// Where sets the query's filter predicate. A later Where replaces an earlier
// one; combine conditions with And, Or, and Not.
func (q *Query) Where(p Predicate) *Query {
	q.where = p
	q.hasWhere = true
	return q
}

// GroupBy groups rows by the values at the given paths. Every non-aggregate
// projected column must then be one of these paths.
func (q *Query) GroupBy(paths ...string) *Query {
	q.groupBy = append(q.groupBy, paths...)
	return q
}

// OrderBy adds a sort key. Keys apply in the order added. Without GROUP BY the
// key is a per-row path; with GROUP BY it must be a grouped path.
func (q *Query) OrderBy(path string, dir Direction) *Query {
	q.orderBy = append(q.orderBy, orderSpec{path: path, dir: dir})
	return q
}

// Limit caps the number of result rows. A negative n means no limit.
func (q *Query) Limit(n int) *Query {
	if n < 0 {
		q.hasLimit = false
		return q
	}
	q.limit = n
	q.hasLimit = true
	return q
}

// A plan is a compiled query: value columns to extract, a compiled predicate,
// aggregate reductions, grouping, ordering, and the row limit.
type plan struct {
	valuePaths []compiledPath // extracted as scalar columns
	numPaths   []compiledPath // extracted as numeric columns (aggregate args)

	headers []string // result schema; cold metadata, parallel to columns
	columns []planColumn
	where   *compiledPredicate

	// filterCols and lateCols partition valuePaths by when a column has to
	// exist. A filter column is one WHERE reads, so a scan must classify it for
	// every row it looks at; a late column is one only the projection,
	// ordering, or grouping reads, which a filter-first scan can gather for the
	// surviving rows alone. A path named by both clauses is registered once and
	// lands in filterCols, so the dedupe pathRegistry performs is preserved
	// rather than turned into a double extraction.
	//
	// A join leaf is a predicate like any other, so a join's outer path is read
	// by WHERE and lands in filterCols through exactly the same rule.
	filterCols []int
	lateCols   []int
	// lateNums is the same partition for the numeric columns an aggregate
	// reduces: the ones the driving collection fills. A joined clause's are in
	// its own innerNums.
	lateNums []int

	// fanOutJoin is the slot of the clause that fans out, or -1 when every
	// clause only filters. It is a single slot because the pair space this
	// engine builds is one driving row crossed with one clause's matches; a
	// second fanning clause would be a cross product of two match sets, which
	// is a different expansion and is rejected at compile.
	fanOutJoin int

	// joins are the compiled semi-join clauses, in slot order. A plan with any
	// of them can only execute against a database snapshot, because each names
	// an inner collection that must be resolved at the same instant the driving
	// side was captured; see join.go.
	joins []planJoin

	grouped   bool
	groupCols []int // value-column indices of GROUP BY paths

	hasAggregate bool
	singleRow    bool // aggregates without GROUP BY: one result row

	order    []planOrder
	limit    int
	hasLimit bool
}

// A planColumn is one compiled SELECT column.
type planColumn struct {
	agg   aggKind
	value int // scalar-column index for a projection or COUNT(path); -1 for COUNT(*)
	num   int // numeric-column index for SUM/AVG/MIN/MAX; -1 otherwise
	slot  int // for a grouped projection, its position in groupCols; -1 otherwise
}

// A planOrder is one compiled ORDER BY key.
type planOrder struct {
	value int // scalar-column index
	slot  int // grouped: position in groupCols; -1 when ordering per row
	dir   Direction
}

// pathRegistry assigns each distinct value path one column index, so a path
// used by several clauses is extracted once.
//
// The specs are scanned linearly rather than hashed. A query names a handful
// of paths, so the map this replaced cost one allocation to build plus a hash
// per lookup in order to save a few string comparisons that a length mismatch
// already rejects on their first word.
type pathRegistry struct {
	paths []compiledPath
}

// reset empties the registry while keeping its storage for the next compile.
func (r *pathRegistry) reset() {
	r.paths = r.paths[:0]
}

// addPath returns spec's column index, compiling and registering it the first
// time this query names it.
func (c *compiler) addPath(r *pathRegistry, spec string) (int, error) {
	for i := range r.paths {
		if r.paths[i].spec == spec {
			return i, nil
		}
	}
	slot, rest := c.resolveJoinAlias(spec)
	cp, err := c.compilePath(rest)
	if err != nil {
		return 0, err
	}
	// The registry dedupes and reports by the qualified spelling, not the
	// resolved one: "o.total" and a driving-collection path literally named
	// "total" compile to the same pointer and must stay two columns, because
	// they are read from two collections.
	cp.spec, cp.join = spec, slot
	r.paths = append(r.paths, cp)
	return len(r.paths) - 1, nil
}

// resolveJoinAlias splits a path spec into the join clause it names and the
// path within that clause's collection. An unqualified spec, and any spec whose
// leading segment is not a declared alias, is a path in the driving collection.
//
// A declared alias wins over a driving-collection field of the same name. That
// is SQL's own rule and it is the only rule available to a schemaless engine,
// which cannot know whether the driving collection has a field by that name.
// It costs nothing in compatibility because an alias has to be declared to
// exist, and no query written before joins could declare one.
func (c *compiler) resolveJoinAlias(spec string) (int, string) {
	// A pointer spec is never qualified: it is rooted at a document, and which
	// document it is rooted at is the question an alias answers.
	if len(c.joinAliases) == 0 || spec == "" || spec[0] == '/' {
		return joinPathOuter, spec
	}
	dot := strings.IndexByte(spec, '.')
	if dot <= 0 {
		return joinPathOuter, spec
	}
	for _, alias := range c.joinAliases {
		if alias.name == spec[:dot] {
			return alias.slot, spec[dot+1:]
		}
	}
	return joinPathOuter, spec
}

// A joinAlias binds one declared alias to the join clause it names.
type joinAlias struct {
	name string
	slot int
}

// compiled returns the query's compiled plan, compiling once on first call.
func (q *Query) compiled() (*plan, error) {
	if built := q.built; built != nil {
		return built.plan, built.err
	}
	q.once.Do(func() {
		q.plan, q.compileErr = q.compile()
	})
	return q.plan, q.compileErr
}

// compile validates the programmatic builder state and lowers it to a plan,
// with storage the returned plan owns outright. A lazily compiled builder has
// no reusable compiler behind it, so it gets a throwaway one whose arenas
// nothing else can rewind — which is exactly what "the plan owns its storage"
// means here.
func (q *Query) compile() (*plan, error) {
	var c compiler
	c.forOneShot()
	return c.compilePlan(q)
}

// compilePlan validates the builder state and lowers it to a plan in c's
// storage. The reused slices are taken back whatever the outcome, so a failed
// compile does not throw away the capacity the compiler had already grown.
func (c *compiler) compilePlan(q *Query) (*plan, error) {
	p := c.planStorage()
	err := c.buildPlan(q, p)
	c.headers = p.headers
	c.planCols = p.columns
	c.groupCols = p.groupCols
	c.planOrder = p.order
	c.filterCols = p.filterCols
	c.lateCols = p.lateCols
	c.keep(q)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (c *compiler) buildPlan(q *Query, p *plan) error {
	if len(q.columns) == 0 {
		return fmt.Errorf("query: Select requires at least one column")
	}
	values := &c.values
	numReg := &c.numbers

	// The aliases are collected before any path is compiled, because a path's
	// meaning depends on them: "o.total" is a joined column or a nested driving
	// column according to whether some clause declared "o".
	if err := c.collectJoinAliases(q); err != nil {
		return err
	}

	grouped := len(q.groupBy) > 0

	*p = plan{
		grouped:    grouped,
		limit:      q.limit,
		hasLimit:   q.hasLimit,
		fanOutJoin: -1,
	}
	// The column-shaped slices are sized from the select list and the path
	// registries from every clause that can name one, so a one-shot compile
	// fills each in a single allocation rather than growing into it.
	p.headers = reserve(c.headers[:0], len(q.columns))
	p.columns = reserve(c.planCols[:0], len(q.columns))
	p.groupCols = reserve(c.groupCols[:0], len(q.groupBy))
	p.order = reserve(c.planOrder[:0], len(q.orderBy))
	pathBudget := len(q.columns) + len(q.groupBy) + len(q.orderBy)
	p.filterCols = reserve(c.filterCols[:0], pathBudget)
	p.lateCols = reserve(c.lateCols[:0], pathBudget)
	values.paths = reserve(values.paths, pathBudget)

	hasProjection := false
	for _, col := range q.columns {
		pc := planColumn{agg: col.agg, value: -1, num: -1, slot: -1}
		switch col.agg {
		case aggNone:
			hasProjection = true
			if grouped && !namesPath(q.groupBy, col.spec) {
				return fmt.Errorf("query: projected column %q must appear in GROUP BY", col.spec)
			}
			idx, err := c.addPath(values, col.spec)
			if err != nil {
				return err
			}
			pc.value = idx
		case aggCount:
			p.hasAggregate = true
			if col.spec != "" {
				idx, err := c.addPath(values, col.spec)
				if err != nil {
					return err
				}
				pc.value = idx
			}
		default: // SUM, AVG, MIN, MAX
			p.hasAggregate = true
			idx, err := c.addPath(numReg, col.spec)
			if err != nil {
				return err
			}
			pc.num = idx
		}
		p.headers = append(p.headers, col.header)
		p.columns = append(p.columns, pc)
	}

	if p.hasAggregate && hasProjection && !grouped {
		return fmt.Errorf("query: cannot mix a projection with an aggregate without GROUP BY")
	}
	p.singleRow = p.hasAggregate && !grouped

	if q.hasWhere {
		cp, err := c.compilePredicate(q.where, values)
		if err != nil {
			return err
		}
		p.where = cp
	}
	// The join leaves are conjoined after the query's own filter, never before
	// it. A join leaf is the most expensive test in the tree — under a lookup
	// binding it is a hash probe plus a document decode — and And short-circuits
	// left to right, so a cheap local conjunct that already rejected the row
	// must get the chance to.
	joinNodes, err := c.compileJoins(q, p, values)
	if err != nil {
		return err
	}
	if len(joinNodes) != 0 {
		p.where = c.conjoin(p.where, joinNodes)
	}

	for _, g := range q.groupBy {
		idx, err := c.addPath(values, g)
		if err != nil {
			return err
		}
		if !hasColumn(p.groupCols, idx) {
			p.groupCols = append(p.groupCols, idx)
		}
	}
	// Resolve each grouped projection to its group-key slot.
	for i := range p.columns {
		if p.columns[i].agg == aggNone && grouped {
			p.columns[i].slot = groupSlotOf(p.groupCols, p.columns[i].value)
		}
	}

	if err := c.compileOrder(q, p, values); err != nil {
		return err
	}

	p.valuePaths = values.paths
	p.numPaths = numReg.paths
	if err := c.planJoinColumns(p); err != nil {
		return err
	}
	for i := range p.valuePaths {
		// A joined column belongs to neither phase of the driving scan. It does
		// not exist during the filter phase, which runs before the pairs that
		// give it a row to be indexed by, and it is not read from the driving
		// collection at all — the clause that owns it fills it. planJoinColumns
		// has already rejected a WHERE that reads one, so the filter-phase half
		// of this is an invariant rather than a fallback.
		if p.valuePaths[i].join != joinPathOuter {
			continue
		}
		if p.where.readsColumn(i) {
			p.filterCols = append(p.filterCols, i)
		} else {
			p.lateCols = append(p.lateCols, i)
		}
	}
	p.lateNums = reserve(c.lateNums[:0], len(p.numPaths))
	for i := range p.numPaths {
		if p.numPaths[i].join == joinPathOuter {
			p.lateNums = append(p.lateNums, i)
		}
	}
	c.lateNums = p.lateNums
	return nil
}

// collectJoinAliases records each clause's declared alias for path resolution,
// rejecting a duplicate: two clauses answering to one name would make every
// qualified path silently read whichever was declared first.
func (c *compiler) collectJoinAliases(q *Query) error {
	c.joinAliases = c.joinAliases[:0]
	for i, j := range q.joins {
		if j.alias == "" {
			continue
		}
		for _, seen := range c.joinAliases {
			if seen.name == j.alias {
				return fmt.Errorf(
					"query: join[%d]: alias %q is already used by join[%d]", i, j.alias, seen.slot)
			}
		}
		c.joinAliases = append(c.joinAliases, joinAlias{name: j.alias, slot: i})
	}
	return nil
}

// planJoinColumns decides, per clause, between the semi-join and the fan-out
// shapes, and records which columns each fan-out clause has to fill.
//
// The operator is chosen by the clause, not by what the query happens to read.
// A clause with no alias is this engine's filter: it decides whether a driving
// row has a partner, the result has one row per driving row, and the binder is
// free to answer that by whichever measured strategy is cheapest. A clause with
// an alias is SQL's inner join, whose result has one row per matching pair —
// and that cardinality is observable through COUNT(*) alone, so "does any
// column read the joined side" is the wrong question to decide it by. Asking it
// would make SELECT COUNT(*) ... JOIN count driving rows, which is a different
// query.
//
// What column usage does decide is one provable equivalence. When the clause
// joins on the joined collection's primary key, at most one document can match
// a driving row, because a key names at most one; so if nothing reads the
// joined side, the inner join and the semi-join select the same rows and
// produce the same columns, and the semi-join machinery — which never
// materializes a match — is strictly cheaper. That is a plan choice under a
// proof, not a semantics choice.
func (c *compiler) planJoinColumns(p *plan) error {
	if len(p.joins) == 0 {
		return nil
	}
	for i := range p.joins {
		j := &p.joins[i]
		j.innerCols = j.innerCols[:0]
		j.innerNums = j.innerNums[:0]
		for col := range p.valuePaths {
			if p.valuePaths[col].join != j.slot {
				continue
			}
			if p.where.readsColumn(col) {
				// An inner join's WHERE over a joined column is exactly the
				// join's own filter — filtering the joined side before the join
				// and filtering the pairs after it select the same pairs — so
				// this is a redirection to the equivalent spelling rather than a
				// missing feature. Rewriting it here would only be sound for a
				// top-level conjunct, and silently sound-only-sometimes is worse
				// than a message.
				return fmt.Errorf(
					"query: WHERE reads %q, a column of joined collection %q; "+
						"put that condition in the join clause's own filter, "+
						"which selects the same rows for an inner join",
					p.valuePaths[col].spec, j.collection)
			}
			j.innerCols = append(j.innerCols, col)
		}
		for col := range p.numPaths {
			if p.numPaths[col].join == j.slot {
				j.innerNums = append(j.innerNums, col)
			}
		}
		reads := len(j.innerCols) != 0 || len(j.innerNums) != 0
		j.fanOut = j.aliased && (reads || j.innerPath != joinPrimaryKey)
	}
	fanOut := -1
	for i := range p.joins {
		if !p.joins[i].fanOut {
			continue
		}
		if fanOut >= 0 {
			return fmt.Errorf(
				"query: join[%d] and join[%d] both fan out; "+
					"one join clause per query may produce rows",
				fanOut, i)
		}
		fanOut = i
	}
	p.fanOutJoin = fanOut
	return nil
}

// compileOrder resolves the ORDER BY keys, enforcing the grouped-path rule and
// skipping ordering for a single-row aggregate result.
func (c *compiler) compileOrder(q *Query, p *plan, values *pathRegistry) error {
	if p.singleRow {
		return nil // one result row; nothing to order
	}
	for _, o := range q.orderBy {
		if p.grouped && !namesPath(q.groupBy, o.path) {
			return fmt.Errorf("query: ORDER BY %q must appear in GROUP BY", o.path)
		}
		idx, err := c.addPath(values, o.path)
		if err != nil {
			return err
		}
		po := planOrder{value: idx, slot: -1, dir: o.dir}
		if p.grouped {
			po.slot = groupSlotOf(p.groupCols, idx)
		}
		p.order = append(p.order, po)
	}
	return nil
}

// namesPath reports whether paths contains spec. It replaces the set map the
// GROUP BY rules used to build, on the same reasoning as pathRegistry: a
// GROUP BY names a couple of paths, so the map was one allocation spent to
// avoid a handful of comparisons.
func namesPath(paths []string, spec string) bool {
	for _, p := range paths {
		if p == spec {
			return true
		}
	}
	return false
}

// hasColumn reports whether cols already registers the value column idx.
func hasColumn(cols []int, idx int) bool {
	for _, col := range cols {
		if col == idx {
			return true
		}
	}
	return false
}

// groupSlotOf answers a value column's position among the group keys. An
// unregistered column answers slot zero, matching the map lookup this
// replaced; the projection and ordering rules above have already made that
// case unreachable.
func groupSlotOf(groupCols []int, value int) int {
	for i, col := range groupCols {
		if col == value {
			return i
		}
	}
	return 0
}
