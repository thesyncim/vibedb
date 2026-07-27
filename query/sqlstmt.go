package query

import (
	"fmt"

	"github.com/thesyncim/vibejson/x/byteview"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// The prepared-statement front end: a parsed [sqlast.SelectStmt] lowered onto
// the same compiled plan the programmatic builder produces.
//
// The layer exists because a database/sql driver needs three things the plan
// alone does not carry. It needs output names, which belong to the result
// schema rather than to the plan (an AS alias renames a column without changing
// a byte of execution). It needs placeholder binding, which means a statement's
// literals are not known until execution and the plan must be rebuilt per bind.
// And it needs the two clauses the executor has no node for — HAVING and
// OFFSET — applied somewhere, because the parser accepts both and silently
// dropping either returns wrong rows.
//
// Rebuilding the plan per bind is the reason a Statement owns a [Compiler]
// rather than borrowing one. A Compiler's arenas are refilled by a warmed
// compile instead of reallocated, so the steady state of "bind, execute, bind,
// execute" allocates nothing; but a Query it produced is invalidated by its
// next compile, so two statements sharing one Compiler would invalidate each
// other's plan the moment both were prepared. One Compiler per Statement makes
// the lifetime rule "a Statement's plan is valid until that Statement's next
// bind", which is a rule a caller can actually hold to.

// A Statement is a prepared SQL statement: the parsed tree, the compiled plan
// it lowers to, and the output schema the plan does not carry.
//
// It is single-consumer and not safe for concurrent use, for the same reason
// [Compiler] and [Exec] are: it holds the storage that makes a warmed
// bind-and-execute allocation-free, and two goroutines binding one Statement
// would interleave writes into a single plan. A pool of connections gives each
// connection its own Statement.
//
// A Statement with no '?' placeholder compiles exactly once, at
// [PrepareStatement], and every later execution reuses that plan without
// touching the parser or the compiler again. A Statement with placeholders
// re-lowers on each bind; after one warm-up that re-lowering is free of heap
// allocation, but it is not free of work, and the difference is measurable.
// See the package benchmarks.
type Statement struct {
	// text is the statement source, retained only so an error raised during a
	// later bind can quote what the author wrote.
	text string
	// tree is the parsed statement. It is owned outright — sqlast.Parse copies
	// every identifier, key, and literal out of the source — so the specs and
	// literal spellings lowering hands to the compiler may borrow it directly
	// instead of being interned a second time.
	tree *sqlast.SelectStmt

	// names are the output column names, one per SQL result column, in SELECT
	// order. They are computed once here rather than read back from the plan's
	// headers because an AS alias is a property of the statement and the plan
	// may carry hidden columns HAVING needs; see outputs.
	names []string
	// outputs is the number of SQL result columns. The plan may hold more: a
	// HAVING that tests a GROUP BY key the SELECT list does not project needs
	// that key materialized, and appending a hidden column for it is cheaper
	// and simpler than a second grouping pass. Everything past outputs is
	// invisible to the caller.
	outputs int

	// params is the placeholder count, the value a driver validates its
	// argument count against before it does anything else.
	params int

	// c and q are this statement's own compiler and the Query it compiles
	// into. They are values rather than pointers so a Statement is one
	// allocation; see the Compiler's own note on why it hands back pointers it
	// merely holds rather than addresses of its own fields.
	c Compiler
	q Query

	// specBuf and specs memoize each path's rendered engine spec. A path's
	// spelling cannot change between binds, so rendering it once at prepare
	// and handing the same string to every later lowering removes the one
	// allocation a per-bind render would otherwise cost. Views into specBuf
	// stay valid when it grows, because append only ever writes past the end
	// of every view already taken.
	specBuf []byte
	specs   []pathSpec

	// joinFilter marks that lowering is currently building a join clause's own
	// filter, where a path is spelled relative to the joined collection rather
	// than qualified by its alias. It is a mode flag rather than a parameter
	// threaded through every leaf because the alternative is one more argument
	// on eight functions to carry a fact that is constant for a whole subtree.
	joinFilter bool

	// stack is the operand stack the predicate lowering builds n-ary And and
	// Or nodes on. It needs stack discipline because the lowering recurses
	// through both, exactly like the parser's own kid stack: one flat buffer
	// would let an inner conjunction overwrite the outer disjunction's
	// operands.
	stack []Predicate

	// having is the compiled post-reduction filter, empty when the statement
	// has no HAVING.
	having havingProgram

	// offset and limit are resolved per bind, because either may be a
	// placeholder. hasLimit distinguishes "LIMIT 0", which selects no rows,
	// from "no LIMIT".
	offset   int
	limit    int
	hasLimit bool

	// driverLimit records that LIMIT could not be pushed into the plan and the
	// cursor must apply it. That happens whenever rows are dropped after
	// execution — a HAVING filter — because a plan-side limit would cut the
	// result before the filter had a chance to reject anything.
	driverLimit bool

	// cached marks that q holds a plan every execution may reuse, and lowerErr
	// the failure that prevented one. Only a statement with no placeholder can
	// cache: its literals are fixed, so the plan lowered at prepare is the plan
	// every execution wants, and its steady state is the builder's exactly —
	// no parse, no lowering, no compile, the same plan handed to the same
	// executor.
	cached   bool
	lowerErr error

	// args is the argument vector the last bind used, retained only so a
	// prepare-time validation pass has somewhere to put its placeholder
	// stand-ins without allocating.
	args []any
}

// A pathSpec memoizes one parsed path's rendered engine spelling in one of the
// two namespaces a joined path has: qualified by its alias for the query's own
// clauses, and bare for the join clause's own filter, which is compiled against
// the joined collection alone.
type pathSpec struct {
	path  *sqlast.PathExpr
	local bool
	text  string
}

// PrepareStatement parses one SQL statement and lowers it to a compiled plan.
//
// It reports every error it can before any row is read: a syntax error from the
// parser (a *[sqlast.ParseError] carrying a line, column, and byte offset), a
// construct this engine has no operator for, and every plan rule the compiler
// enforces. A statement holding placeholders is lowered here against neutral
// stand-in arguments purely so those failures surface at prepare rather than at
// the first execution; the stand-ins are discarded and the first real bind
// re-lowers.
//
// The returned Statement owns everything it holds and carries no borrowed
// lifetime. It is single-consumer; see [Statement].
func PrepareStatement(src string) (*Statement, error) {
	tree, err := sqlast.Parse(src)
	if err != nil {
		return nil, err
	}
	return prepareTree(src, tree)
}

// prepareTree lowers an already-parsed SELECT.
//
// It exists so a DML statement's row selection can be the SELECT front end
// rather than a copy of it: sqlast hands an UPDATE or a DELETE back with a real
// SelectStmt describing which documents it acts on, and this turns that tree
// into the same Statement PrepareStatement would have produced from the
// equivalent SELECT text. See sqldml.go.
func prepareTree(src string, tree *sqlast.SelectStmt) (*Statement, error) {
	s := &Statement{text: src, tree: tree, params: tree.Params}
	if err := s.describe(); err != nil {
		return nil, err
	}
	// Validate with stand-ins. Every placeholder becomes int64(0), which is a
	// literal type every leaf accepts, so a failure here is a property of the
	// statement's shape rather than of any particular argument.
	s.args = reserve(s.args, s.params)[:s.params]
	for i := range s.args {
		s.args[i] = int64(0)
	}
	if err := s.lower(s.args); err != nil {
		return nil, err
	}
	return s, nil
}

// Columns returns the output column names in SELECT order. The slice is owned
// by s and must not be modified.
//
// A column with an AS alias takes that alias. A projected path takes its
// spelling in the engine's path language, which is the SQL path with quoting
// removed — "u.address.city" becomes "address.city", because the range variable
// names the collection rather than part of the path. An aggregate takes the
// engine's own header spelling, "sum(score)" or "count(*)", so a query written
// in SQL and the same query written with the builder produce one schema.
func (s *Statement) Columns() []string { return s.names[:s.outputs:s.outputs] }

// NumParams returns the number of '?' placeholders. Binding a different number
// of arguments is an error reported before the statement executes, which is the
// check [SelectStmt.Params] exists for.
func (s *Statement) NumParams() int { return s.params }

// SQL returns the statement text as it was prepared.
func (s *Statement) SQL() string { return s.text }

// NumJoins returns the number of JOIN clauses. A statement with any of them
// must execute against a [FromDatabase] source, whose database snapshot
// resolves every collection at one instant; a caller holding a single
// collection can refuse it before it takes a snapshot it would only discard.
func (s *Statement) NumJoins() int { return len(s.tree.From) - 1 }

// AppendSchema appends the statement's output schema to dst, one entry per
// SELECT column. It is [Query.AppendSchema] narrowed to the columns the caller
// can see: a HAVING clause may have added a hidden column, and a result schema
// that advertised it would be advertising a column [Cursor.Cell] refuses to
// return.
func (s *Statement) AppendSchema(dst []OutputColumn) []OutputColumn {
	start := len(dst)
	dst = s.q.AppendSchema(dst)
	if len(dst)-start > s.outputs {
		dst = dst[:start+s.outputs]
	}
	return dst
}

// Release drops every buffer s retains, invalidating its plan. A Statement is
// meant to be held and reused — the retained compiler arenas are exactly what
// makes a warmed bind allocation-free — so Release is for tearing one down, not
// for reclaiming space between executions.
func (s *Statement) Release() {
	if s == nil {
		return
	}
	s.c.Release()
	*s = Statement{}
}

// RunInto binds args and executes s into e's caller-owned storage, returning a
// cursor over the surviving rows.
//
// args must hold exactly [Statement.NumParams] values. Each may be nil, a
// bool, any signed or unsigned integer, a float, a string, a []byte, or a
// [Number]; a nil argument makes its leaf UNKNOWN, exactly as a SQL NULL
// comparison would. Anything else is rejected by name.
//
// The returned [Cursor] walks e.Result, applying the HAVING filter and the
// OFFSET and LIMIT the plan could not carry. It borrows e and stays valid until
// the next execution into e, the same rule e.Result itself carries.
func (s *Statement) RunInto(e *Exec, src Source, args []any) (Cursor, error) {
	if e == nil {
		return Cursor{}, fmt.Errorf("query: RunInto requires a non-nil Exec")
	}
	if err := s.bind(args); err != nil {
		return Cursor{}, err
	}
	if err := s.q.RunInto(e, src); err != nil {
		return Cursor{}, err
	}
	return s.cursor(&e.Result), nil
}

// Run binds args and executes s over src with transient storage this call
// owns. It is [Statement.RunInto] for a one-off; a hot loop holds an [Exec].
func (s *Statement) Run(src Source, args []any) (Result, Cursor, error) {
	var e Exec
	cursor, err := s.RunInto(&e, src, args)
	return e.Result, cursor, err
}

// bind re-lowers the plan for args when it has to. A statement with no
// placeholders lowered once at prepare and is answered from the cached
// outcome, which is what makes its execution byte-identical to the builder's.
func (s *Statement) bind(args []any) error {
	if len(args) != s.params {
		return fmt.Errorf(
			"query: statement has %d placeholder(s) and %d argument(s) were bound",
			s.params, len(args))
	}
	if s.cached {
		return s.lowerErr
	}
	return s.lower(args)
}

// cursor builds the iterator over an executed result.
//
// OFFSET skips rows that survived HAVING, never rows the filter was going to
// reject: SQL applies HAVING first and OFFSET afterwards, and a cursor that
// skipped raw result rows would be skipping groups the clause had already
// removed. When there is no HAVING the two are the same sequence, so the rule
// costs nothing to state uniformly.
func (s *Statement) cursor(res *Result) Cursor {
	c := Cursor{st: s, res: res, skip: s.offset, cur: -1, left: -1}
	if s.hasLimit && s.driverLimit {
		// The plan could not carry the limit, so the cursor does. Without
		// HAVING the plan was asked for offset+limit rows instead, precisely
		// so that skipping offset of them leaves limit behind.
		c.left = s.limit
	}
	return c
}

// A Cursor walks the rows of an executed [Statement] that survive the clauses
// the plan could not carry: the HAVING filter, and the OFFSET and LIMIT the
// plan could not push down. Its zero value is an exhausted cursor.
//
// It is a value, not a handle: it holds no storage of its own and allocates
// nothing, so a driver can keep one inline in its rows object.
type Cursor struct {
	st  *Statement
	res *Result
	// row is the next result row to consider, cur the one Cell reads, skip the
	// surviving rows OFFSET still has to discard, and left the rows still
	// allowed under a cursor-applied LIMIT, or -1 when the plan applied the
	// limit itself.
	row  int
	cur  int
	skip int
	left int
}

// Next advances to the next surviving row, reporting false at the end.
func (c *Cursor) Next() bool {
	if c.res == nil || c.left == 0 {
		return false
	}
	for c.row < c.res.RowCount {
		row := c.row
		c.row++
		if !c.st.having.keep(c.res, row) {
			continue
		}
		if c.skip > 0 {
			c.skip--
			continue
		}
		c.cur = row
		if c.left > 0 {
			c.left--
		}
		return true
	}
	c.cur = -1
	return false
}

// Cell returns the value of output column col in the current row. Calling it
// before the first Next, or after Next reported false, returns a null cell.
func (c *Cursor) Cell(col int) Cell {
	if c.cur < 0 || c.res == nil || col < 0 || col >= c.st.outputs {
		return nullCell()
	}
	return c.res.Columns[col].Cells[c.cur]
}

// Row returns the underlying result-row index of the current row, or -1.
func (c *Cursor) Row() int { return c.cur }

// spec renders p in whichever namespace lowering is currently building for: the
// query's, where a joined path is qualified by its alias, or a join clause's
// own, where it is not.
func (s *Statement) spec(p *sqlast.PathExpr) string { return s.render(p, s.joinFilter) }

// localSpec renders p relative to its own collection, unqualified. It is the
// spelling a join clause's ON key and its own filter take, both of which are
// compiled against the joined collection alone.
func (s *Statement) localSpec(p *sqlast.PathExpr) string { return s.render(p, true) }

// render produces the engine's path spelling for p, memoizing the result.
//
// The memo is a linear scan rather than a map because a statement names a
// handful of paths and each is looked up once per clause that reads it: a map
// would cost one allocation to build and a hash per lookup to save a pointer
// comparison that the first word already decides.
func (s *Statement) render(p *sqlast.PathExpr, local bool) string {
	if p == nil {
		return ""
	}
	qualified := !local && p.Source != 0
	if len(p.Segments) == 0 && !qualified {
		return ""
	}
	for i := range s.specs {
		if s.specs[i].path == p && s.specs[i].local == local {
			return s.specs[i].text
		}
	}
	start := len(s.specBuf)
	if qualified {
		// The engine resolves a qualified path by splitting at the first dot
		// and matching the head against a declared alias, so the alias goes in
		// front of whatever AppendSpec renders — including a JSON Pointer,
		// which the engine never treats as qualified on its own and which
		// therefore has to be told which document it is rooted at.
		s.specBuf = append(s.specBuf, s.tree.From[p.Source].Alias...)
		s.specBuf = append(s.specBuf, '.')
	}
	s.specBuf = p.AppendSpec(s.specBuf)
	if !qualified {
		s.specBuf = s.unshadow(start)
	}
	// The three-index slice is what makes the view immune to specBuf growing:
	// a later append writes at or past the view's end, never inside it, and a
	// reallocation leaves the view pointing at the old array, which is still
	// exactly the bytes it described.
	text := byteview.String(s.specBuf[start:len(s.specBuf):len(s.specBuf)])
	s.specs = append(s.specs, pathSpec{path: p, local: local, text: text})
	return text
}

// unshadow re-renders a driving-collection path as a JSON Pointer when its
// leading segment happens to spell a declared join alias.
//
// SQL and the engine agree that an alias wins over a field of the same name,
// and that agreement is exactly the problem here: "u.o.total" says
// unambiguously that o is a field of the driving collection, but it renders as
// "o.total", which the engine reads as the joined collection's total. Both
// readings are right for what they are looking at, so the fix is to hand the
// engine a spelling that cannot be qualified. A JSON Pointer is that spelling.
//
// The conversion needs no escaping. AppendSpec only produced a dotted form
// because no segment held a '.', a '/', a '~', an index, or an empty key —
// which is precisely the set of things a pointer token would have had to
// escape — so replacing the separators is the whole of it.
func (s *Statement) unshadow(start int) []byte {
	spec := s.specBuf[start:]
	if len(spec) == 0 || spec[0] == '/' {
		return s.specBuf
	}
	dot := -1
	for i := range spec {
		if spec[i] == '.' {
			dot = i
			break
		}
	}
	// A path with no dot cannot be read as qualified, so it cannot collide.
	if dot <= 0 {
		return s.specBuf
	}
	head := byteview.String(spec[:dot])
	shadowed := false
	for i := 1; i < len(s.tree.From); i++ {
		if s.tree.From[i].Alias == head {
			shadowed = true
			break
		}
	}
	if !shadowed {
		return s.specBuf
	}
	// Shift the rendered path one byte right to make room for the leading
	// separator. The slice is re-derived after the growth, because appending
	// may have moved the backing array out from under the view taken above.
	buf := append(s.specBuf, 0)
	copy(buf[start+1:], buf[start:len(buf)-1])
	buf[start] = '/'
	for i := start + 1; i < len(buf); i++ {
		if buf[i] == '.' {
			buf[i] = '/'
		}
	}
	return buf
}

// describe computes the output schema and the parts of the statement that do
// not depend on a binding. It runs once, at prepare.
func (s *Statement) describe() error {
	s.outputs = len(s.tree.Columns)
	s.names = reserve(s.names[:0], s.outputs)
	for i := range s.tree.Columns {
		s.names = append(s.names, s.columnName(&s.tree.Columns[i]))
	}
	return nil
}

// columnName answers the output header for one result column.
func (s *Statement) columnName(col *sqlast.ResultColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	spec := s.spec(col.Path)
	if col.Agg == sqlast.AggNone {
		if spec == "" {
			// The whole document, which '*' and 'alias.*' parse to. The engine
			// spells that path as the empty string, which is not a name a
			// result schema can use, so the SQL spelling stands in.
			return "*"
		}
		return spec
	}
	if col.Agg == sqlast.AggCount && col.Path == nil {
		return "count(*)"
	}
	return s.header(aggName(col.Agg), spec)
}

// header renders "name(spec)" into the statement's own spec buffer, matching
// the engine's header spelling without concatenating two strings onto the heap.
func (s *Statement) header(name, spec string) string {
	start := len(s.specBuf)
	s.specBuf = append(s.specBuf, name...)
	s.specBuf = append(s.specBuf, '(')
	s.specBuf = append(s.specBuf, spec...)
	s.specBuf = append(s.specBuf, ')')
	return byteview.String(s.specBuf[start:len(s.specBuf):len(s.specBuf)])
}

// aggName answers the engine's lowercase header spelling for an aggregate.
func aggName(a sqlast.AggKind) string {
	switch a {
	case sqlast.AggCount:
		return "count"
	case sqlast.AggSum:
		return "sum"
	case sqlast.AggAvg:
		return "avg"
	case sqlast.AggMin:
		return "min"
	default:
		return "max"
	}
}
