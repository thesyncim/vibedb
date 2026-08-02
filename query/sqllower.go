package query

import (
	"errors"
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// ErrParameterType reports a bound SQL parameter whose runtime type is not
// accepted by its syntactic role.
var ErrParameterType = errors.New("query: SQL parameter has incompatible type")

// ErrInvalidPattern reports a malformed literal or bound LIKE/ILIKE pattern.
var ErrInvalidPattern = errors.New("query: invalid LIKE pattern")

// Lowering a parsed SQL statement onto the builder's plan.
//
// # Three-valued logic, and why it is implemented rather than documented away
//
// SQL's comparison against NULL is UNKNOWN: neither true nor false, dropped by
// WHERE, and — the part that bites — not flipped by NOT. This engine is
// two-valued. Its leaves answer false for a null or absent cell, so Not of a
// comparison on a null path answers true, and a row every other SQL database
// drops is kept.
//
// That divergence is not documentable. A driver that silently returns extra
// rows for "WHERE NOT (x = 1)" is worse than one that refuses the query, and it
// is invisible until a null appears in production data. So it is implemented,
// and the implementation needs nothing the engine does not already have.
//
// The insight that makes it cheap is that the engine's leaves are already
// exact for TRUE. "x = 1" answers true on exactly the rows SQL calls TRUE, and
// false on the union of SQL's FALSE and UNKNOWN. What is missing is a way to
// ask for FALSE alone. So lowering carries two functions rather than one:
// lowerTrue(e) builds a predicate true where e is TRUE, and lowerFalse(e) one
// true where e is FALSE. WHERE takes lowerTrue of its root, because WHERE keeps
// the rows a predicate is TRUE on.
//
//	T(x op v)   = x op v                        (already exact)
//	F(x op v)   = NOT (x IS NULL) AND NOT (x op v)
//	T(NOT p)    = F(p)
//	F(NOT p)    = T(p)
//	T(a AND b)  = T(a) AND T(b)     F(a AND b) = F(a) OR  F(b)
//	T(a OR  b)  = T(a) OR  T(b)     F(a OR  b) = F(a) AND F(b)
//
// Those are Kleene's tables written as two boolean functions, and the recursion
// is a proof by structural induction as long as each leaf's pair is right. The
// leaves are enumerated in leafForm below, each with the two facts the pair
// needs: whether the leaf can be UNKNOWN at all, and the predicate that says it
// is not.
//
// The cost lands only where the semantics differ. A positive predicate — the
// overwhelming majority — lowers to exactly what the builder would have built,
// with no added conjunct and no added column: T of a leaf is the leaf. The
// "NOT (x IS NULL)" guard appears only under an odd number of negations and on
// a negated leaf form (NOT IN, NOT BETWEEN), which is precisely where SQL and
// the engine disagreed.

// lower rebuilds and compiles the plan for one binding of args.
//
// It follows the reusable compiler lifecycle — rewind, prepare, build, compile,
// publish — so a warmed compile refills the compiler's arenas instead of
// reallocating them. A statement re-lowered per bind is therefore free of heap
// allocation in the steady state.
func (s *Statement) lower(args []any) error {
	c := &s.c
	c.rewind()
	c.prepare(&s.q)
	if s.correlation != nil {
		s.q.correlationSlots = s.correlation.slots
	}
	s.stack = s.stack[:0]
	err := s.build(args)
	if err == nil {
		var p *plan
		p, err = c.compilePlan(&s.q)
		if err == nil {
			s.q.built = c.outcome(p, nil)
			s.cached = s.canCacheLowering()
			s.lowerErr = nil
			return nil
		}
	}
	// The failure is installed in the Query as well as returned, so a caller
	// that ignores the error and executes anyway sees this error rather than
	// whichever half-built clauses survived into the builder state.
	_ = c.fail(&s.q, err)
	// A placeholder-free statement always has the same failure, so retaining
	// it avoids pointlessly rebuilding the same invalid plan. A placeholder
	// statement is different: prepare already lowered its shape successfully
	// with neutral arguments, which means a later failure can depend on this
	// binding (a bad type, an invalid Number, or a negative LIMIT/OFFSET).
	// Keep that statement reusable so a rejected execution cannot poison the
	// next valid bind.
	s.cached = s.canCacheLowering()
	s.lowerErr = err
	return err
}

// build lowers every clause of the parsed statement into the builder state the
// compiler is about to read.
func (s *Statement) build(args []any) error {
	if window := s.window(); window != nil {
		return s.buildWindow(window, args)
	}
	if err := s.buildColumns(args); err != nil {
		return err
	}
	if err := s.buildJoins(args); err != nil {
		return err
	}
	if err := s.buildGroupBy(); err != nil {
		return err
	}
	if err := s.buildOrderBy(); err != nil {
		return err
	}
	if err := s.buildHaving(args); err != nil {
		return err
	}
	return s.buildLimit(args)
}

func (s *Statement) canCacheLowering() bool {
	if s.params != 0 {
		return false
	}
	return s.nested == nil || s.window() != nil || s.nested.onlyDecorrelatedExists()
}

func (s *Statement) buildWindow(w *statementWindow, args []any) error {
	if err := w.bind(s, args); err != nil {
		return err
	}
	for i := range w.outputs {
		output := &w.outputs[i]
		s.q.columns = append(s.q.columns, Column{
			spec: w.ordinalSpec[output.ordinal], header: output.name,
		})
	}
	if s.tree.Distinct {
		for i := range w.outputs {
			s.q.groupBy = append(s.q.groupBy, w.ordinalSpec[w.outputs[i].ordinal])
		}
	}
	for i := range s.tree.OrderBy {
		term := &s.tree.OrderBy[i]
		ordinal := -1
		if term.Output != 0 {
			column := term.Output - 1
			if column < 0 || column >= len(w.outputStart) {
				return fmt.Errorf("query: window ORDER BY output is outside the SELECT list")
			}
			ordinal = w.outputs[w.outputStart[column]].ordinal
		} else {
			ordinal = w.inputOrdinal(term.Path)
		}
		if ordinal < 0 || ordinal >= len(w.ordinalSpec) {
			return fmt.Errorf("query: window ORDER BY path has no input column")
		}
		direction := Asc
		if term.Desc {
			direction = Desc
		}
		s.q.orderBy = append(s.q.orderBy, orderSpec{
			path: w.ordinalSpec[ordinal], dir: direction,
		})
	}
	s.having.reset()
	return s.buildLimit(args)
}

// Collection returns the name of the driving collection — the FROM entry. A
// statement always has one, because the parser requires FROM.
func (s *Statement) Collection() string {
	if set := s.setSQL(); set != nil {
		return set.Collection()
	}
	if s.nested != nil && s.nested.driving != "" {
		return s.nested.driving
	}
	return s.resolveDrivingCollection()
}

func (s *Statement) resolveDrivingCollection() string {
	if window := s.window(); window != nil {
		return window.input.Collection()
	}
	if join := s.relationJoin(); join != nil {
		return join.drivingCollection()
	}
	if ref := s.cteReference(); ref != nil && ref.def != nil && ref.def.stmt != nil {
		return ref.def.stmt.Collection()
	}
	if derived := s.derived(); derived != nil {
		return derived.stmt.Collection()
	}
	return s.tree.From[0].Name
}

// buildColumns lowers the SELECT list, then appends any hidden column a HAVING
// leaf needs.
func (s *Statement) buildColumns(args []any) error {
	if scalar := s.scalarStatement(); scalar != nil {
		return scalar.buildColumns(s, args)
	}
	name := 0
	for i := range s.tree.Columns {
		col := &s.tree.Columns[i]
		relation := relationBinding{}
		if col.Path != nil {
			relation = s.relationBindingForSource(col.Path.Source)
		}
		if s.hasRelationBinding() && col.Agg == sqlast.AggNone &&
			col.Path != nil && len(col.Path.Segments) == 0 {
			for ordinal := range relation.names {
				s.q.columns = append(s.q.columns, Column{
					spec:   relation.ordinalSpec[ordinal],
					header: s.names[name],
				})
				name++
			}
			continue
		}
		s.q.columns = append(s.q.columns, Column{
			agg:    aggKind(col.Agg),
			spec:   s.spec(col.Path),
			header: s.names[name],
		})
		name++
	}
	return nil
}

// buildGroupBy lowers GROUP BY.
func (s *Statement) buildGroupBy() error {
	for _, key := range s.tree.GroupBy {
		relation := relationBinding{}
		if key != nil {
			relation = s.relationBindingForSource(key.Source)
		}
		if s.hasRelationBinding() && key != nil &&
			len(key.Segments) == 0 {
			s.q.groupBy = append(s.q.groupBy, relation.ordinalSpec...)
			continue
		}
		s.q.groupBy = append(s.q.groupBy, s.spec(key))
	}
	return nil
}

// buildOrderBy lowers ORDER BY.
//
// The engine orders nulls first ascending and last descending, which SQL
// leaves implementation-defined and PostgreSQL answers the other way round for
// ASC. That one is genuinely not repairable here: the plan has no nulls-first
// or nulls-last knob, and the ordering happens inside the executor's sort. It
// is documented as a deviation rather than papered over, and NULLS FIRST and
// NULLS LAST are refused by the parser instead of being silently ignored.
func (s *Statement) buildOrderBy() error {
	for i := range s.tree.OrderBy {
		term := &s.tree.OrderBy[i]
		dir := Asc
		if term.Desc {
			dir = Desc
		}
		s.q.orderBy = append(s.q.orderBy, orderSpec{path: s.spec(term.Path), dir: dir})
	}
	return nil
}

// buildLimit resolves LIMIT and OFFSET, deciding which of them the plan can
// carry and which the cursor has to apply.
//
// The engine's plan has a limit and no offset, so OFFSET is always the
// cursor's. LIMIT is pushed into the plan as offset+limit whenever nothing
// drops rows after execution, because the plan stopping at offset+limit leaves
// exactly limit rows behind once the cursor has skipped offset of them. A
// HAVING clause does drop rows after execution, so it takes the pushdown away
// entirely: a plan that stopped early would be cutting rows the filter had not
// yet had the chance to reject.
func (s *Statement) buildLimit(args []any) error {
	s.offset, s.limit, s.hasLimit = 0, 0, false
	s.driverLimit = (s.tree.Having != nil || s.scalarStatement() != nil) && s.window() == nil
	if s.tree.Offset != nil {
		n, err := s.count(*s.tree.Offset, args, "OFFSET")
		if err != nil {
			return err
		}
		s.offset = n
	}
	if s.tree.Limit != nil {
		n, err := s.count(*s.tree.Limit, args, "LIMIT")
		if err != nil {
			return err
		}
		s.limit, s.hasLimit = n, true
	}
	if s.subqueryLimit > 0 &&
		(!s.hasLimit || s.limit > int(s.subqueryLimit)) {
		s.limit, s.hasLimit = int(s.subqueryLimit), true
	}
	if s.hasLimit && !s.driverLimit {
		bound := s.offset + s.limit
		if bound < s.offset {
			bound = math.MaxInt // offset+limit overflowed; no bound is reachable
		}
		s.q.limit, s.q.hasLimit = bound, true
	}
	return nil
}

// count resolves a LIMIT or OFFSET operand, which the grammar allows to be an
// integer literal or a placeholder.
func (s *Statement) count(o sqlast.Operand, args []any, clause string) (int, error) {
	if o.Kind == sqlast.OperandNumber {
		n, ok := int64Spelling(o.Text)
		if !ok || n < 0 || int64(int(n)) != n {
			return 0, fmt.Errorf("query: %s %s is not a non-negative count", clause, o.Text)
		}
		return int(n), nil
	}
	if o.Kind != sqlast.OperandParam {
		return 0, fmt.Errorf("query: %s must be an integer or a placeholder", clause)
	}
	switch v := args[o.Ordinal].(type) {
	case int64:
		if v < 0 || int64(int(v)) != v {
			return 0, fmt.Errorf("query: %s was bound to %d, which is not a non-negative count", clause, v)
		}
		return int(v), nil
	case *int64:
		if v == nil {
			return 0, fmt.Errorf("query: %s was bound to NULL; a row count cannot be NULL", clause)
		}
		if *v < 0 || int64(int(*v)) != *v {
			return 0, fmt.Errorf("query: %s was bound to %d, which is not a non-negative count", clause, *v)
		}
		return int(*v), nil
	case int:
		if v < 0 {
			return 0, fmt.Errorf("query: %s was bound to %d, which is not a non-negative count", clause, v)
		}
		return v, nil
	case Number:
		n, ok := int64Spelling(string(v))
		if !ok || n < 0 || int64(int(n)) != n {
			return 0, fmt.Errorf(
				"query: %s was bound to %q, which is not a non-negative count",
				clause, v,
			)
		}
		return int(n), nil
	case *Number:
		if v == nil {
			return 0, fmt.Errorf("query: %s was bound to NULL; a row count cannot be NULL", clause)
		}
		n, ok := int64Spelling(string(*v))
		if !ok || n < 0 || int64(int(n)) != n {
			return 0, fmt.Errorf(
				"query: %s was bound to %q, which is not a non-negative count",
				clause, *v,
			)
		}
		return int(n), nil
	default:
		return 0, fmt.Errorf(
			"query: %s was bound to %T; a row count must be a non-negative integer", clause, args[o.Ordinal])
	}
}

// --- joins -----------------------------------------------------------------

// buildJoins lowers the JOIN clauses.
//
// Every clause declares its SQL alias with [Join.As], and that declaration is
// what selects the operator. With an alias the clause is SQL's inner join: the
// joined collection's columns are readable as "alias.path" and the result
// carries one row per matching pair. Without one it would be the engine's
// semi-join, which returns no joined column and emits the driving row once —
// not what JOIN means. So the alias is never omitted, and the one case where
// the cheaper operator is still correct is left to the planner, which takes it
// under a proof: a clause joining on the joined collection's primary key
// matches at most one document, so when nothing reads the joined side the two
// operators select the same rows.
//
// Two restrictions remain, and both are refused rather than approximated.
// A chained join — one whose ON names a collection other than the FROM one —
// has no plan, because the engine crosses the driving collection with one
// clause's matches and not with another clause's output. And only one clause
// per statement may fan out, because a second would be a cross product of two
// match sets that this expansion does not build.
func (s *Statement) buildJoins(args []any) error {
	if err := s.buildDecorrelatedExists(args); err != nil {
		return err
	}
	if len(s.tree.From) == 1 {
		return s.buildWhere(args)
	}
	if s.relationJoin() != nil {
		return s.buildGeneralizedWhere(args)
	}
	for i := 1; i < len(s.tree.From); i++ {
		ref := &s.tree.From[i]
		if err := s.checkAlias(ref.Alias); err != nil {
			return err
		}
		cond := ref.On
		if cond.Left.Source != 0 {
			return fmt.Errorf(
				"query: ON matches %q against %q, but this engine joins every collection "+
					"against the FROM collection %q; a chained join has no plan",
				s.tree.From[cond.Left.Source].Alias, ref.Alias, s.tree.From[0].Alias)
		}
		// JoinOn takes the driving path first and the joined path second, which
		// is the reverse of the order "ON o.user_id = u.id" writes them in; the
		// parser has already normalized the condition so Left is always the
		// side already in scope and Right the side this clause adds.
		join := JoinOn(ref.Name, s.spec(cond.Left), s.localSpec(cond.Right))
		if ref.Join == sqlast.JoinLeft {
			join = LeftJoinOn(ref.Name, s.spec(cond.Left), s.localSpec(cond.Right))
		}
		s.q.joins = append(s.q.joins, join.As(ref.Alias))
	}
	if err := s.checkSingleFanOut(); err != nil {
		return err
	}
	return s.buildWhere(args)
}

// buildGeneralizedWhere evaluates WHERE against the completed flattened join
// relation. ON residuals and outer-row extension have already happened, which
// is the ordering SQL requires and permits predicates spanning any source.
func (s *Statement) buildGeneralizedWhere(args []any) error {
	if s.tree.Where == nil {
		return nil
	}
	pred, err := s.lowerNode(s.tree.Where, true, args)
	if err != nil {
		return err
	}
	s.q.where = pred
	s.q.hasWhere = true
	return nil
}

// checkAlias refuses a range-variable name the engine's path language cannot
// carry.
//
// The engine resolves a qualified path by splitting at its first dot and
// matching the head against a declared alias, and treats any spec beginning
// with a slash as a JSON Pointer rooted at the driving document. A quoted alias
// holding either byte therefore renders into a path the engine reads as
// something else — 'JOIN j AS "o.x"' makes "o.x".b render as "o.x.b", whose
// head is "o", which no clause declared, so the whole thing is read as a nested
// field of the driving collection and every row projects null. That is a wrong
// answer with no error, which is the one outcome worth spending a check on.
//
// Refusing is the right repair rather than escaping the alias, because the
// alias is a name the author chose and can change, while an escaping scheme
// would be a second path language for the sake of a spelling nobody needs.
func (s *Statement) checkAlias(alias string) error {
	for i := 0; i < len(alias); i++ {
		if alias[i] == '.' || alias[i] == '/' {
			return fmt.Errorf(
				"query: the range variable %q contains %q, which this engine's path language "+
					"uses to separate segments and to mark a JSON Pointer; a qualified path "+
					"would resolve to something else, so choose an alias without it",
				alias, string(alias[i]))
		}
	}
	return nil
}

// checkSingleFanOut refuses a statement whose plan would need two pair spaces.
//
// The engine enforces this too, and would report it; the check exists here so
// the message names the aliases the author wrote rather than the slot indices
// the plan uses. The condition is read off the parsed statement, which is the
// same thing the lowering is about to emit, so the two cannot disagree about
// which clauses fan out.
func (s *Statement) checkSingleFanOut() error {
	first := -1
	for i := 1; i < len(s.tree.From); i++ {
		if !s.fansOut(i) {
			continue
		}
		if first >= 0 {
			return fmt.Errorf(
				"query: joining %q and %q both produce rows, and one statement may expand "+
					"only once; the pair space of two fanning joins is a cross product this "+
					"engine does not build",
				s.tree.From[first].Alias, s.tree.From[i].Alias)
		}
		first = i
	}
	return nil
}

// fansOut reports whether the clause at index i produces one row per matching
// pair rather than merely filtering.
//
// A clause fans out unless the planner can prove the cheaper operator answers
// the same question, and it can prove that in exactly one case: joining on the
// joined collection's primary key matches at most one document per driving row,
// so if nothing outside the join clause reads the joined side, one row per pair
// and one row per driving row are the same rows.
func (s *Statement) fansOut(i int) bool {
	if s.localSpec(s.tree.From[i].On.Right) != JoinKey {
		return true
	}
	return s.reads(i)
}

// reads reports whether any clause outside the join itself names the collection
// at index i. A WHERE condition over it does not count: lowering moves that
// into the clause's own filter, where it narrows the joined side before the
// pairing rather than reading a column of the result.
func (s *Statement) reads(i int) bool {
	for j := range s.tree.Columns {
		if path := s.tree.Columns[j].Path; path != nil && path.Source == i {
			return true
		}
	}
	for _, key := range s.tree.GroupBy {
		if key.Source == i {
			return true
		}
	}
	for j := range s.tree.OrderBy {
		if s.tree.OrderBy[j].Path.Source == i {
			return true
		}
	}
	return false
}

// buildWhere lowers WHERE, moving the conditions that read a joined collection
// into that join clause's own filter.
//
// # Why the move is necessary, and exactly when it is sound
//
// SQL writes every condition in one WHERE, including the ones that narrow a
// joined collection. The engine has no operator for that: a joined column does
// not exist during the filter phase, which runs before the pairs that would
// give it a row to be indexed by, so it refuses a WHERE that reads one and says
// to use the clause's own filter instead. It refuses rather than rewriting
// because the rewrite is sound only sometimes, and a rewrite that is sound only
// sometimes returns wrong rows the rest of the time without erroring.
//
// The rewrite is sound for a top-level conjunct of WHERE that names one joined
// collection and nothing else. The argument is short. WHERE keeps the pairs
// every top-level conjunct is TRUE on; a conjunct P reading only the joined
// side is TRUE of a pair (d, j) exactly when P(j) holds, so it partitions the
// pairs by their joined document alone. Filtering the joined collection by P
// before the pairing removes exactly the documents j with not-P(j), and so
// removes exactly the pairs the conjunct would have rejected. The two orders
// select the same pairs. Nothing in that depends on P's shape, so a negation,
// a disjunction, or a null test inside P is fine — what matters is only that P
// sits at the top of the conjunction, where "the pairs WHERE keeps" is the
// intersection of what each conjunct keeps.
//
// It is not sound anywhere else, and the two ways out are refused. Under an OR,
// the condition no longer partitions by the joined document: "u.a = 1 OR
// o.b = 2" keeps a pair whose joined document fails the second test, so
// removing that document before the pairing would drop a row SQL keeps. Under a
// NOT that also covers a driving column, the same thing happens through De
// Morgan. Both of those are conjuncts naming two collections, so the single
// check below — a conjunct must name one collection — is the whole rule, and it
// is a check on the conjunct rather than on the shape of what is inside it.
//
// This lowering can apply the rule where the engine could not, because it still
// has the statement: it knows which nodes are top-level conjuncts of WHERE,
// which the engine's compiled predicate no longer distinguishes from any other
// conjunction it may have folded or hoisted.
func (s *Statement) buildWhere(args []any) error {
	if s.tree.Where == nil {
		return nil
	}
	conjuncts := []*sqlast.Expr{s.tree.Where}
	if s.tree.Where.Kind == sqlast.ExprAnd {
		conjuncts = s.tree.Where.Kids
	}
	base := len(s.stack)
	defer func() { s.stack = s.stack[:base] }()
	for _, conjunct := range conjuncts {
		if s.decorrelatedExistsFor(conjunct) != nil {
			// The hidden semi/anti join or grouped mark installed above is the
			// complete proven replacement for this exact authored conjunct.
			continue
		}
		if scalar := s.scalarStatement(); scalar != nil && scalar.ownsWhere(conjunct) {
			continue
		}
		source, mixed := exprSource(conjunct)
		if mixed {
			return fmt.Errorf(
				"query: a WHERE condition reads two collections at once; this engine tests " +
					"each collection separately, and a condition spanning both is only " +
					"equivalent to doing so when it is one of WHERE's top-level ANDed terms " +
					"over a single collection")
		}
		// A joined conjunct is lowered in the joined collection's own
		// namespace, where its paths are bare rather than alias-qualified,
		// because the clause's filter compiles against that collection alone
		// and Join.Where's contract is a path relative to it.
		//
		// The qualified spelling happens to work today — the inner plan's path
		// registry runs alias resolution too, strips the leading alias, and
		// ignores the slot it resolved — so this is not repairing a bug. It is
		// declining to depend on a coincidence in a layer whose job is to
		// compile paths against one collection.
		s.joinFilter = source > 0
		pred, err := s.lowerNode(conjunct, true, args)
		s.joinFilter = false
		if err != nil {
			return err
		}
		if source <= 0 {
			s.stack = append(s.stack, pred)
			continue
		}
		if s.tree.From[source].Join == sqlast.JoinLeft {
			return fmt.Errorf(
				"query: WHERE reads %q, the nullable side of a LEFT JOIN; "+
					"post-join predicates are not supported yet, so put the condition "+
					"on the preserved collection or omit it",
				s.tree.From[source].Alias)
		}
		if err := s.narrowJoin(source, pred); err != nil {
			return err
		}
	}
	outer := s.stack[base:]
	switch len(outer) {
	case 0:
	case 1:
		s.q.where, s.q.hasWhere = outer[0], true
	default:
		kids := s.c.operands(len(outer))
		kids = append(kids, outer...)
		s.q.where, s.q.hasWhere = Predicate{kind: predAnd, kids: kids}, true
	}
	return nil
}

// narrowJoin conjoins pred onto the inner filter of the join at source index i.
func (s *Statement) narrowJoin(i int, pred Predicate) error {
	j := &s.q.joins[i-1]
	if !j.hasWhere {
		j.where, j.hasWhere = pred, true
		return nil
	}
	j.where = Predicate{kind: predAnd, kids: s.c.pair(j.where, pred)}
	return nil
}

// exprSource answers the single range variable a predicate reads, and whether
// it reads more than one. A predicate that reads no path at all — an empty
// boolean node — answers -1, which callers treat as the driving collection.
func exprSource(e *sqlast.Expr) (source int, mixed bool) {
	source = -1
	var walk func(*sqlast.Expr)
	walk = func(e *sqlast.Expr) {
		if mixed {
			return
		}
		for _, kid := range e.Kids {
			walk(kid)
		}
		if e.Path == nil {
			return
		}
		switch {
		case source < 0:
			source = e.Path.Source
		case source != e.Path.Source:
			mixed = true
		}
	}
	walk(e)
	return source, mixed
}

// --- predicates ------------------------------------------------------------

// lowerNode builds the predicate that is true exactly where e is TRUE, when
// wantTrue, and exactly where e is FALSE otherwise. See the file comment for
// why the two directions are one recursion.
func (s *Statement) lowerNode(e *sqlast.Expr, wantTrue bool, args []any) (Predicate, error) {
	switch e.Kind {
	case sqlast.ExprAnd:
		if wantTrue {
			return s.combine(predAnd, e.Kids, true, args)
		}
		return s.combine(predOr, e.Kids, false, args)
	case sqlast.ExprOr:
		if wantTrue {
			return s.combine(predOr, e.Kids, true, args)
		}
		return s.combine(predAnd, e.Kids, false, args)
	case sqlast.ExprNot:
		return s.lowerNode(e.Kids[0], !wantTrue, args)
	case sqlast.ExprBetween:
		return s.lowerBetween(e, wantTrue, args)
	default:
		form, err := s.leafForm(e, args)
		if err != nil {
			return Predicate{}, err
		}
		return s.resolve(form, wantTrue != e.Negated), nil
	}
}

// combine builds an n-ary boolean node over lowered operands, folding away the
// constants the leaf rules produce.
//
// The folding is not cosmetic. A leaf bound to a NULL argument is UNKNOWN for
// every row, which lowers to a constant, and leaving those constants in the
// tree would put a node the executor evaluates per row where the answer was
// already known — and, worse, would leave an empty disjunction inside a
// conjunction, which is a plan that reads as if it filtered on something.
func (s *Statement) combine(kind predKind, kids []*sqlast.Expr, wantTrue bool, args []any) (Predicate, error) {
	base := len(s.stack)
	defer func() { s.stack = s.stack[:base] }()
	for _, kid := range kids {
		p, err := s.lowerNode(kid, wantTrue, args)
		if err != nil {
			return Predicate{}, err
		}
		if kind == predAnd {
			if alwaysTrue(p) {
				continue
			}
			if alwaysFalse(p) {
				return neverTrue(), nil
			}
		} else {
			if alwaysFalse(p) {
				continue
			}
			if alwaysTrue(p) {
				return alwaysTruePred(), nil
			}
		}
		s.stack = append(s.stack, p)
	}
	operands := s.stack[base:]
	switch len(operands) {
	case 0:
		if kind == predAnd {
			return alwaysTruePred(), nil
		}
		return neverTrue(), nil
	case 1:
		return operands[0], nil
	default:
		nodes := s.c.operands(len(operands))
		nodes = append(nodes, operands...)
		return Predicate{kind: kind, kids: nodes}, nil
	}
}

// lowerBetween lowers BETWEEN as the conjunction it denotes.
//
// Expanding it here rather than in the parser is what keeps the three-valued
// rules exact: "x BETWEEN ? AND 5" with a NULL first argument is UNKNOWN when
// x <= 5 and FALSE when it is not, which only falls out if each bound is a leaf
// with its own UNKNOWN case. Treating the whole range as one leaf would have
// made the pair UNKNOWN in both cases and quietly dropped a row SQL keeps.
func (s *Statement) lowerBetween(e *sqlast.Expr, wantTrue bool, args []any) (Predicate, error) {
	low, err := s.compareForm(e.Path, sqlast.OpGe, e.List[0], args)
	if err != nil {
		return Predicate{}, err
	}
	high, err := s.compareForm(e.Path, sqlast.OpLe, e.List[1], args)
	if err != nil {
		return Predicate{}, err
	}
	inRange := wantTrue != e.Negated
	a, b := s.resolve(low, inRange), s.resolve(high, inRange)
	kind := predAnd
	if !inRange {
		// FALSE of a conjunction is the disjunction of its operands' FALSEs.
		kind = predOr
	}
	switch {
	case kind == predAnd && (alwaysFalse(a) || alwaysFalse(b)):
		return neverTrue(), nil
	case kind == predAnd && alwaysTrue(a):
		return b, nil
	case kind == predAnd && alwaysTrue(b):
		return a, nil
	case kind == predOr && (alwaysTrue(a) || alwaysTrue(b)):
		return alwaysTruePred(), nil
	case kind == predOr && alwaysFalse(a):
		return b, nil
	case kind == predOr && alwaysFalse(b):
		return a, nil
	}
	return Predicate{kind: kind, kids: s.c.pair(a, b)}, nil
}

// A leafForm is one SQL leaf reduced to the three facts the three-valued rules
// need: the engine predicate that is true exactly where the leaf is TRUE, and
// whether the leaf can be FALSE or TRUE at all.
//
// guard is the predicate that holds where the leaf is determinate, and is
// meaningful only when the leaf can be UNKNOWN. total marks the leaves that
// never can be — the null and existence tests, which are defined over every
// value including the absent one, and containment, which this engine answers
// over a JSON null haystack rather than declining to.
type leafForm struct {
	pred  Predicate
	guard Predicate
	total bool
	// noTrue and noFalse mark a leaf that a NULL-bound operand has made
	// constantly UNKNOWN in one or both directions.
	noTrue  bool
	noFalse bool
}

// resolve turns a leaf form into the predicate for the requested direction.
func (s *Statement) resolve(f leafForm, wantTrue bool) Predicate {
	if wantTrue {
		if f.noTrue {
			return neverTrue()
		}
		return f.pred
	}
	if f.noFalse {
		return neverTrue()
	}
	if f.total {
		return s.c.not(f.pred)
	}
	return Predicate{kind: predAnd, kids: s.c.pair(f.guard, s.c.not(f.pred))}
}

// leafForm reduces one non-boolean predicate node to its three-valued form.
func (s *Statement) leafForm(e *sqlast.Expr, args []any) (leafForm, error) {
	spec := s.spec(e.Path)
	switch e.Kind {
	case sqlast.ExprCompare:
		if slot, ok := s.correlation.slot(e); ok {
			return s.correlationCompareForm(e.Path, e.Op, slot)
		}
		if e.Subquery != nil {
			return s.subqueryCompareForm(e)
		}
		return s.compareForm(e.Path, e.Op, e.Value, args)
	case sqlast.ExprIn:
		if e.Subquery != nil {
			return s.subqueryInForm(e, spec)
		}
		return s.inForm(e, spec, args)
	case sqlast.ExprExists:
		sub := s.subquery(e.Subquery)
		if sub == nil {
			return leafForm{}, fmt.Errorf("query: missing prepared EXISTS subquery")
		}
		if sub.preview || sub.exists {
			return leafForm{pred: alwaysTruePred(), total: true}, nil
		}
		return leafForm{pred: neverTrue(), total: true}, nil
	case sqlast.ExprIsNull:
		// IS NULL is two-valued in both dialects, and it is where this engine
		// and SQL disagree about what null means rather than about logic: an
		// absent path is null here and SQL has no absent column at all. The
		// distinction is available as IS MISSING.
		return leafForm{pred: IsNull(spec), total: true}, nil
	case sqlast.ExprIsMissing:
		return leafForm{pred: s.c.not(Exists(spec)), total: true}, nil
	case sqlast.ExprContains:
		// Containment is deliberately two-valued. Its left operand is a JSON
		// document, and JSON null is a value that document can be and that a
		// needle can match, so declining to answer for it would make
		// "x @> 'null'" unanswerable rather than precise. PostgreSQL reaches
		// the same place from the other direction: its jsonb @> is UNKNOWN only
		// for a SQL NULL column, which this model does not have.
		return leafForm{pred: Contains(spec, e.Value.Text), total: true}, nil
	case sqlast.ExprLike:
		return s.likeForm(e, args)
	default:
		return leafForm{}, fmt.Errorf("query: unsupported predicate in SQL lowering")
	}
}

func (s *Statement) likeForm(e *sqlast.Expr, args []any) (leafForm, error) {
	if s.prepareMode && e.Value.Kind == sqlast.OperandParam {
		// Prepare lowers parameterized statements once with int64(0) stand-ins
		// so shape errors surface early. LIKE has a string-only RHS, so use an
		// empty string for this shape pass and validate the real binding below.
		pred := Like(s.spec(e.Path), "")
		if e.Insensitive {
			pred = ILike(s.spec(e.Path), "")
		}
		return leafForm{pred: pred, guard: isString(s.spec(e.Path))}, nil
	}
	value, known, err := s.operand(e.Value, args)
	if err != nil {
		return leafForm{}, err
	}
	if !known {
		return leafForm{noTrue: true, noFalse: true}, nil
	}
	var pattern string
	switch v := value.(type) {
	case *string:
		pattern = *v
	case string:
		pattern = v
	default:
		return leafForm{}, fmt.Errorf(
			"%w: LIKE pattern must be a string, got %T",
			ErrParameterType, value,
		)
	}
	pred := Like(s.spec(e.Path), pattern)
	if e.Insensitive {
		pred = ILike(s.spec(e.Path), pattern)
	}
	return leafForm{pred: pred, guard: isString(s.spec(e.Path))}, nil
}

func (s *Statement) subqueryCompareForm(e *sqlast.Expr) (leafForm, error) {
	sub := s.subquery(e.Subquery)
	if sub == nil {
		return leafForm{}, fmt.Errorf("query: missing prepared scalar subquery")
	}
	if sub.preview {
		return leafForm{
			pred:  Cmp(s.spec(e.Path), Op(e.Op), int64(0)),
			guard: s.c.not(IsNull(s.spec(e.Path))),
		}, nil
	}
	if !sub.exists || sub.hasNull || len(sub.values) == 0 {
		return leafForm{noTrue: true, noFalse: true}, nil
	}
	spec := s.spec(e.Path)
	return leafForm{
		pred:  Cmp(spec, Op(e.Op), sub.values[0]),
		guard: s.c.not(IsNull(spec)),
	}, nil
}

func (s *Statement) subqueryInForm(e *sqlast.Expr, spec string) (leafForm, error) {
	sub := s.subquery(e.Subquery)
	if sub == nil {
		return leafForm{}, fmt.Errorf("query: missing prepared IN subquery")
	}
	if sub.preview {
		return leafForm{
			pred:  In(spec, int64(0)),
			guard: s.c.not(IsNull(spec)),
		}, nil
	}
	return leafForm{
		pred:    In(spec, sub.values...),
		guard:   s.c.not(IsNull(spec)),
		noFalse: sub.hasNull,
	}, nil
}

// compareForm builds the form of one comparison leaf.
func (s *Statement) compareForm(
	path *sqlast.PathExpr, op sqlast.CmpOp, operand sqlast.Operand, args []any,
) (leafForm, error) {
	spec := s.spec(path)
	value, known, err := s.operand(operand, args)
	if err != nil {
		return leafForm{}, err
	}
	if !known {
		// Comparison against NULL is UNKNOWN for every row, in both
		// directions. SQL spells the same thing "x = NULL", which the parser
		// refuses precisely because its reading depends on which semantics the
		// reader has in mind; a placeholder bound to nil is the one way the
		// shape can still arrive, and it means what SQL means.
		return leafForm{noTrue: true, noFalse: true}, nil
	}
	return leafForm{
		pred:  Cmp(spec, Op(op), value),
		guard: s.c.not(IsNull(spec)),
	}, nil
}

// inForm builds the form of a membership leaf.
//
// A NULL among the alternatives is the shape SQL authors get wrong most often,
// so it is worth stating what it lowers to. "x IN (1, NULL)" is TRUE when x is
// 1 and UNKNOWN otherwise — never FALSE — because the unmatched NULL leaves the
// disjunction undecided. So the membership is built over the alternatives that
// are known, which answers TRUE exactly right, and the leaf is marked as never
// FALSE, which makes "x NOT IN (1, NULL)" the empty result SQL says it is.
func (s *Statement) inForm(e *sqlast.Expr, spec string, args []any) (leafForm, error) {
	values := s.c.boxes.alloc(len(e.List))[:0]
	nullAlternative := false
	for _, operand := range e.List {
		value, known, err := s.operand(operand, args)
		if err != nil {
			return leafForm{}, err
		}
		if !known {
			nullAlternative = true
			continue
		}
		values = append(values, value)
	}
	return leafForm{
		pred:    In(spec, values...),
		guard:   s.c.not(IsNull(spec)),
		noFalse: nullAlternative,
	}, nil
}

// operand resolves a literal or a bound placeholder to the typed value
// the internal literal compiler takes, reporting known=false for SQL NULL.
//
// Literal text is handed over by reference rather than copied: a parsed
// statement owns its own text, and the Statement owns the parse tree, so the
// spelling outlives every plan compiled from it. A bound argument is not
// owned by anything the statement can see, so its bytes are interned into the
// compiler's arena — which costs no allocation after the first bind of that
// shape and removes any question about whether database/sql may reuse the
// caller's buffer once the query returns.
func (s *Statement) operand(o sqlast.Operand, args []any) (any, bool, error) {
	switch o.Kind {
	case sqlast.OperandString:
		box := s.c.strs.one()
		*box = o.Text
		return box, true, nil
	case sqlast.OperandNumber:
		box := s.c.nums.one()
		*box = Number(o.Text)
		return box, true, nil
	case sqlast.OperandBool:
		// A bool needs no arena: Go interns the two boolean values, so boxing
		// one costs nothing.
		return o.Bool, true, nil
	case sqlast.OperandParam:
		return s.argument(args[o.Ordinal])
	default:
		return nil, false, fmt.Errorf("query: a JSON document is only an operand of '@>'")
	}
}

// argument converts one bound driver argument to a typed literal value.
//
// The pass-through cases return the caller's interface value rather than the
// type-switched local, which is the difference between reusing the box
// database/sql already made and allocating a second one on every bind.
func (s *Statement) argument(arg any) (any, bool, error) {
	switch v := arg.(type) {
	case nil:
		return nil, false, nil
	case *bool:
		if v == nil {
			return nil, false, nil
		}
		return arg, true, nil
	case *int64:
		if v == nil {
			return nil, false, nil
		}
		return arg, true, nil
	case *float64:
		if v == nil {
			return nil, false, nil
		}
		return arg, true, nil
	case *string:
		if v == nil {
			return nil, false, nil
		}
		return arg, true, nil
	case *Number:
		if v == nil {
			return nil, false, nil
		}
		return arg, true, nil
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		return arg, true, nil
	case string:
		box := s.c.strs.one()
		*box = s.c.internString(v)
		return box, true, nil
	case []byte:
		// A byte slice binds as a string literal, not as a JSON document.
		// database/sql hands text through as []byte from several drivers'
		// conventions, and a document has its own operand ('@>') that takes it
		// verbatim from the statement rather than from an argument.
		box := s.c.strs.one()
		*box = s.c.intern(v)
		return box, true, nil
	case Number:
		box := s.c.nums.one()
		*box = Number(s.c.internString(string(v)))
		return box, true, nil
	default:
		return nil, false, fmt.Errorf(
			"query: cannot bind %T as a SQL literal; bind a bool, an integer, a float, "+
				"a string, a []byte, a query.Number, their pointer-shaped zero-copy "+
				"forms, or nil", arg)
	}
}

// --- boolean constants -----------------------------------------------------
//
// The engine already defines an empty conjunction as true and an empty
// disjunction as false, so the two constants need no new predicate kind and no
// new executor case: they are nodes the evaluator already reaches the right
// answer for, which is what makes folding them optional rather than required
// for correctness.

// neverTrue is the predicate no row satisfies.
func neverTrue() Predicate { return Predicate{kind: predOr} }

// alwaysTruePred is the predicate every row satisfies.
func alwaysTruePred() Predicate { return Predicate{kind: predAnd} }

func alwaysFalse(p Predicate) bool { return p.kind == predOr && len(p.kids) == 0 }
func alwaysTrue(p Predicate) bool  { return p.kind == predAnd && len(p.kids) == 0 }
