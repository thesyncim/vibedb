package query

import (
	"errors"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
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
// Rebuilding the plan per bind is the reason a Statement owns an internal
// compiler rather than borrowing one. Its arenas are refilled by a warmed
// compile instead of reallocated, so the steady state of "bind, execute, bind,
// execute" allocates nothing; but a Query it produced is invalidated by its
// next compile, so two statements sharing one compiler would invalidate each
// other's plan the moment both were prepared. One compiler per Statement makes
// the lifetime rule "a Statement's plan is valid until that Statement's next
// bind", which is a rule a caller can actually hold to.

// A Statement is a prepared SQL statement: the parsed tree, the compiled plan
// it lowers to, and the output schema the plan does not carry.
//
// It is single-consumer and not safe for concurrent use, for the same reason
// its internal compiler and [Exec] are: it holds the storage that makes a warmed
// bind-and-execute allocation-free, and two goroutines binding one Statement
// would interleave writes into a single plan. A pool of connections gives each
// connection its own Statement.
//
// A Statement with neither a '?' placeholder nor a subquery compiles exactly
// once, at [PrepareStatement], and every later execution reuses that plan.
// Placeholders and subqueries re-lower on each execution because their values
// may change; after one warm-up that work is free of heap allocation.
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
	// paramBase is this statement's first placeholder in the owning top-level
	// statement. Nested prepared statements keep their local parameter count
	// while this immutable offset lets metadata be projected back without
	// copying a statement-wide type vector into every child.
	paramBase int
	// paramTypes is a prepare-time flattened view of inferred SQL input domains.
	// It stays nil for the ordinary schemaless path and makes every public
	// metadata lookup one bounded slice access regardless of nesting depth.
	paramTypes []ParameterType
	// paramTypePositions stores the first authored byte position plus one for
	// each inferred typed parameter. It is allocated only beside paramTypes and
	// lets protocol adapters attribute repeated-number conflicts precisely.
	paramTypePositions []int
	// paramTypeTargetDefaults distinguishes PostgreSQL's final target-list
	// UNKNOWN-to-TEXT coercion from a contextual type constraint. It is allocated
	// only when that cold prepare-time rule fires, so the ordinary schemaless path
	// retains no storage or execution cost. Protocol adapters use the provenance
	// when several '?' occurrences map back to one numbered wire parameter.
	paramTypeTargetDefaults []bool
	// parameterTypeHints is borrowed only while the prepared tree is being
	// analyzed. Protocol adapters use it to make client-declared PostgreSQL
	// parameter types participate in common-type selection rather than checking
	// them after unknown outputs have already been finalized. It is nil on the
	// ordinary path and cleared before preparation returns.
	parameterTypeHints []ParameterType
	// preserveDocumentUnknown is a prepare-only INSERT-source context. Whole JSON
	// document lineage is marked after the relation graph exists, so nested
	// derived/CTE/set preparation must not finalize its unknown placeholders to
	// SQL text before that mark can reach them. It is false for ordinary SQL and
	// cleared before preparation returns.
	preserveDocumentUnknown bool
	requiresCatalog         bool
	drivingPredicate        *sqlast.Expr
	// correlation is immutable, cold metadata installed only on a LATERAL child.
	// Live slot values belong to the child Exec.Workspace, never to Statement.
	correlation *statementCorrelationPlan

	// c and q are this statement's own compiler and the Query it compiles
	// into. They are values rather than pointers so a Statement is one
	// allocation; see the compiler's own note on why it hands back pointers it
	// merely holds rather than addresses of its own fields.
	c compiler
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

	// subqueryLimit is an internal semantic cap: EXISTS needs at most one row,
	// and a scalar subquery needs at most two to distinguish its valid
	// cardinalities from an error. It is zero for every top-level statement.
	subqueryLimit uint8

	// cached marks that q holds a plan every execution may reuse, and lowerErr
	// the failure that prevented one. Only a statement with neither
	// placeholders nor subqueries can cache: its literals are fixed, so the
	// plan lowered at prepare is the plan every execution wants.
	cached   bool
	lowerErr error

	// args is the argument vector the last bind used, retained only so a
	// prepare-time validation pass has somewhere to put its placeholder
	// stand-ins without allocating.
	args        []any
	prepareMode bool

	// nested is allocated only for a statement that actually contains a
	// subquery. Keeping one pointer here instead of three slice words makes
	// ordinary prepared statements pay the smallest representable space cost.
	nested *nestedStatements
}

// ParameterType is an analysis-time SQL input domain for one placeholder.
// Its zero value is intentionally unspecified: most VibeDB parameters remain
// schemaless, and only contexts which PostgreSQL itself uses for type
// resolution opt into an exact domain.
type ParameterType uint8

const (
	ParameterTypeUnspecified ParameterType = iota
	ParameterTypeBool
	ParameterTypeText
	ParameterTypeVarchar
	ParameterTypeName
	ParameterTypeBPChar
	// ParameterTypeOther is a concrete client-declared PostgreSQL domain which
	// this bounded analyzer cannot otherwise model. It is not unknown: operator
	// resolution must reject it when a BOOL/TEXT context requires a different
	// category instead of silently inferring through it.
	ParameterTypeOther
	ParameterTypeInvalid
)

func (t ParameterType) String() string {
	switch t {
	case ParameterTypeUnspecified:
		return "unspecified"
	case ParameterTypeBool:
		return "boolean"
	case ParameterTypeText:
		return "text"
	case ParameterTypeVarchar:
		return "character varying"
	case ParameterTypeName:
		return "name"
	case ParameterTypeBPChar:
		return "character"
	case ParameterTypeOther:
		return "other"
	default:
		return "invalid"
	}
}

type nestedStatements struct {
	// frame is inline with the already-cold nested state so recursive relation
	// execution never makes RunInto's stack frame escape. Ordinary statements
	// keep nested nil and use the direct path below.
	frame        statementFrame
	subqueries   []statementSubquery
	derived      *statementDerived
	relationJoin *statementRelationJoin
	window       *statementWindow
	set          *statementSetSQL
	scalar       *statementScalar
	ctes         *statementCTEs
	cte          *statementCTEReference
	// decorrelated holds proof-backed predicate subqueries. The name preserves
	// the established single-key EXISTS cache/plan contract; entries may now
	// also lower to grouped marks.
	decorrelated []statementDecorrelatedExists
	ownsCTEs     bool
	driving      string
}

const statementRootIntermediateResource = "query source result"

type subqueryUse uint8

const (
	subqueryIn subqueryUse = iota
	subqueryExists
	subqueryScalarUse
)

type statementSubquery struct {
	tree    *sqlast.SelectStmt
	stmt    *Statement
	exec    Exec
	use     subqueryUse
	preview bool
	values  []any
	slots   []subqueryScalar
	hasNull bool
	exists  bool
	// resultBytes and activeBytes are the two reservations that can coexist
	// while a child Result is copied into owned predicate values. resultBytes
	// is released as soon as the copy completes; activeBytes stays charged
	// until the outer plan has finished using those values.
	resultBytes int64
	activeBytes int64
}

type subqueryScalar struct {
	b   bool
	i   int64
	s   string
	n   Number
	buf []byte
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
	return PrepareParsedStatement(src, tree)
}

// PrepareParsedStatement lowers an already-parsed SELECT without parsing its
// source a second time.
//
// src is retained only for diagnostics. tree must be a complete, validated
// tree returned by the sql package and must not be mutated while the returned
// Statement is live. The Statement retains tree but does not alter it. If tree
// came from a reusable [sqlast.Parser], that Parser must not parse again or be
// released until this Statement is released: parsing rewinds the arena that
// owns tree. Several Statements may share a genuinely immutable tree, but each
// Statement remains single-consumer because its compiler storage is mutable.
//
// This is the entry point for adapters that must inspect the AST before
// lowering—for example, a database/sql driver enforcing catalog-specific
// surface rules. Parse once, validate once, and pass that same immutable tree
// here.
func PrepareParsedStatement(
	src string,
	tree *sqlast.SelectStmt,
) (*Statement, error) {
	return PrepareParsedStatementWithParameterTypes(src, tree, nil)
}

// PrepareParsedStatementWithParameterTypes lowers an already-parsed SELECT
// while treating each non-unspecified entry as an analysis-time input type for
// the corresponding placeholder. The slice is borrowed only for the duration
// of this call. Supplying nil is exactly equivalent to PrepareParsedStatement
// and preserves the ordinary allocation profile.
func PrepareParsedStatementWithParameterTypes(
	src string,
	tree *sqlast.SelectStmt,
	parameterTypes []ParameterType,
) (*Statement, error) {
	if tree == nil {
		return nil, fmt.Errorf("query: PrepareParsedStatement was given a nil SELECT")
	}
	if len(parameterTypes) > tree.Params {
		return nil, fmt.Errorf(
			"query: %d parameter type hints exceed %d placeholders: %w",
			len(parameterTypes), tree.Params, ErrParameterType,
		)
	}
	for _, parameterType := range parameterTypes {
		if parameterType >= ParameterTypeInvalid {
			return nil, fmt.Errorf(
				"query: invalid parameter type hint %d: %w",
				parameterType, ErrParameterType,
			)
		}
	}
	return prepareTreeWithParameterTypes(src, tree, parameterTypes)
}

// prepareTree lowers an already-parsed SELECT.
//
// It exists so a DML statement's row selection can be the SELECT front end
// rather than a copy of it: sqlast hands an UPDATE or a DELETE back with a real
// SelectStmt describing which documents it acts on, and this turns that tree
// into the same Statement PrepareStatement would have produced from the
// equivalent SELECT text. See sqldml.go.
func prepareTree(src string, tree *sqlast.SelectStmt) (*Statement, error) {
	return prepareTreeWithParameterTypes(src, tree, nil)
}

func prepareTreeWithParameterTypes(
	src string,
	tree *sqlast.SelectStmt,
	parameterTypes []ParameterType,
) (*Statement, error) {
	return prepareTreeWithLimit(src, tree, 0, parameterTypes)
}

func prepareTreeWithParameterTypesPreservingUnknownOutput(
	src string,
	tree *sqlast.SelectStmt,
	parameterTypes []ParameterType,
) (*Statement, error) {
	return prepareTreeInCorrelationContext(
		src, tree, 0, nil, 0, nil, parameterTypes,
		unknownOutputPrepareMode{preserveDocument: true},
	)
}

type unknownOutputPrepareMode struct {
	deferScalar      bool
	preserveDocument bool
}

func prepareTreeWithLimit(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	parameterTypes []ParameterType,
) (*Statement, error) {
	return prepareTreeInContext(src, tree, subqueryLimit, nil, 0, parameterTypes)
}

// prepareTreeInContext prepares one SELECT inside the lexical CTE catalog and
// absolute placeholder range of its parent. A nil catalog stays nil for an
// ordinary SELECT, preserving the absent-feature object and allocation path.
func prepareTreeInContext(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	ctes *statementCTEs,
	argBase int,
	parameterTypes []ParameterType,
	unknownModes ...unknownOutputPrepareMode,
) (*Statement, error) {
	return prepareTreeInCorrelationContext(
		src, tree, subqueryLimit, ctes, argBase, nil, parameterTypes,
		unknownModes...,
	)
}

// prepareTreeInCorrelationContext is the LATERAL-only prepare entry. Keeping
// the lexical frame as an argument, rather than on Statement, leaves every
// non-correlated prepared and execution path byte-for-byte unchanged.
func prepareTreeInCorrelationContext(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	ctes *statementCTEs,
	argBase int,
	correlation *lateralPrepareFrame,
	parameterTypes []ParameterType,
	unknownModes ...unknownOutputPrepareMode,
) (*Statement, error) {
	mode := unknownOutputPrepareMode{}
	if len(unknownModes) != 0 {
		mode = unknownModes[0]
	}
	deferUnknownScalarOutput := mode.deferScalar
	preserveDocumentUnknown := mode.preserveDocument
	if recursive, pos := RecursiveSQLStatementRequired(tree); recursive &&
		!recursiveSQLBridgeReentry(tree) {
		// The owning top-level bridge rewrites each recursive definition to its
		// anchor before re-entering this ordinary path. Every other encounter is
		// still authored recursive SQL and must dispatch explicitly; allowing it
		// to fall into plain CTE preparation produces a misleading lexical lookup
		// failure and can never execute the fixpoint semantics.
		if subqueryLimit == 0 && ctes == nil && argBase == 0 && tree.Set == nil {
			return PrepareParsedRecursiveSQLStatement(
				src, tree, RecursiveSQLStatementOptions{
					preserveDocumentUnknown: preserveDocumentUnknown,
					parameterTypes:          parameterTypes,
				},
			)
		}
		return nil, sqlast.NewFeatureNotSupportedError(
			src, pos,
			"WITH RECURSIVE must enter through the owning recursive statement prepare hook",
		)
	}
	if tree.Set != nil {
		return prepareSetSQLStatement(
			src, tree, subqueryLimit, ctes, argBase, parameterTypes,
			preserveDocumentUnknown,
		)
	}
	staged, err := stageGroupedScalarPredicate(src, tree, correlation)
	if err != nil {
		return nil, err
	}
	tree = staged
	s := &Statement{
		text: src, tree: tree, params: tree.Params, paramBase: argBase,
		subqueryLimit: subqueryLimit, parameterTypeHints: parameterTypes,
		preserveDocumentUnknown: preserveDocumentUnknown,
	}
	if err := s.seedParameterTypes(parameterTypes); err != nil {
		return nil, err
	}
	defer func() {
		s.parameterTypeHints = nil
		s.preserveDocumentUnknown = false
	}()
	if correlation != nil && correlation.apply != nil {
		s.correlation = &correlation.apply.correlation
	}
	if selectHasWindows(tree) {
		if err := s.prepareWindow(ctes, argBase, correlation); err != nil {
			s.Release()
			return nil, err
		}
	} else if tree.With != nil {
		if ctes == nil {
			ctes = new(statementCTEs)
			s.ensureNested().ownsCTEs = true
		}
		s.ensureNested().ctes = ctes
		if err := s.prepareCTEDefinitions(argBase); err != nil {
			s.Release()
			return nil, err
		}
	}
	generalizedJoin := s.window() == nil && len(tree.From) > 1 && requiresGeneralizedRelationJoin(tree)
	if s.window() == nil && !generalizedJoin && len(tree.From) != 0 && tree.From[0].Kind == sqlast.RelationCTE {
		if ctes == nil {
			return nil, fmt.Errorf("query: CTE reference %q has no lexical definition", tree.From[0].Name)
		}
		s.ensureNested().ctes = ctes
		if err := s.prepareCTEReference(); err != nil {
			s.Release()
			return nil, err
		}
	}
	if s.window() == nil {
		if err := s.prepareSubqueries(argBase); err != nil {
			s.Release()
			return nil, err
		}
	}
	if s.window() == nil && generalizedJoin {
		if err := s.prepareRelationJoin(argBase, correlation); err != nil {
			s.Release()
			return nil, err
		}
	} else if s.window() == nil {
		if err := s.prepareDerived(argBase); err != nil {
			s.Release()
			return nil, err
		}
	}
	if err := s.prepareScalar(deferUnknownScalarOutput); err != nil {
		s.Release()
		return nil, err
	}
	if s.window() != nil {
		s.requiresCatalog = s.window().input.RequiresCatalog()
		s.drivingPredicate = s.window().input.DrivingPredicate()
	} else {
		s.requiresCatalog = selectRequiresCatalog(tree)
		if s.hasDecorrelatedExists() {
			// Even a self-correlation needs the coherent database source: the
			// adaptive join resolves its inner snapshot through the catalog rather
			// than manufacturing a second collection cut.
			s.requiresCatalog = true
		}
		s.drivingPredicate = s.resolveDrivingPredicate()
	}
	if s.nested != nil {
		s.nested.driving = s.resolveDrivingCollection()
	}
	if err := s.describe(); err != nil {
		s.Release()
		return nil, err
	}
	if err := s.prepareParameterTypes(); err != nil {
		s.Release()
		return nil, err
	}
	// Validate with stand-ins. Every placeholder becomes int64(0), which is a
	// literal type every leaf accepts, so a failure here is a property of the
	// statement's shape rather than of any particular argument.
	s.args = reserve(s.args, s.params)[:s.params]
	for i := range s.args {
		s.args[i] = int64(0)
	}
	s.prepareMode = true
	if err := s.lower(s.args); err != nil {
		s.prepareMode = false
		s.Release()
		return nil, err
	}
	s.prepareMode = false
	return s, nil
}

func recursiveSQLBridgeReentry(tree *sqlast.SelectStmt) bool {
	if tree == nil || tree.With == nil {
		return false
	}
	found := false
	for i := range tree.With.CTEs {
		definition := &tree.With.CTEs[i]
		if definition.Recursive.Anchor == nil {
			continue
		}
		found = true
		if definition.Query != definition.Recursive.Anchor {
			return false
		}
	}
	return found
}

func (s *Statement) ensureNested() *nestedStatements {
	if s.nested == nil {
		s.nested = new(nestedStatements)
	}
	return s.nested
}

// Columns returns the output column names in SELECT order. The slice is owned
// by s and must not be modified.
//
// A column with an AS alias takes that alias. A driving or single-relation path
// uses its bare engine spelling; a joined-source path retains its range-variable
// prefix ("o.total") so selecting the generalized executor cannot change the
// established RowDescription of the storage-aware join path. A merged USING
// output remains bare. Aggregates use the engine's own header spelling, such as
// "sum(score)" or "count(*)".
func (s *Statement) Columns() []string { return s.names[:s.outputs:s.outputs] }

// NumParams returns the number of '?' placeholders. Binding a different number
// of arguments is an error reported before the statement executes, which is the
// check [SelectStmt.Params] exists for.
func (s *Statement) NumParams() int { return s.params }

// ParameterType reports the SQL input domain inferred for a zero-based
// placeholder. The result is immutable prepared metadata and the lookup
// allocates nothing. An unrelated scalar remains unspecified.
func (s *Statement) ParameterType(index int) ParameterType {
	if s == nil || index < 0 || index >= s.params {
		return ParameterTypeInvalid
	}
	if index >= len(s.paramTypes) {
		return ParameterTypeUnspecified
	}
	return s.paramTypes[index]
}

// ParameterTypePosition reports the zero-based authored byte offset at which
// a placeholder acquired its analyzed type. A declared-only, unspecified, or
// out-of-range parameter returns -1.
func (s *Statement) ParameterTypePosition(index int) int {
	if s == nil || index < 0 || index >= s.params ||
		index >= len(s.paramTypePositions) || s.paramTypePositions[index] == 0 {
		return -1
	}
	return s.paramTypePositions[index] - 1
}

// ParameterTypeTargetDefault reports whether ParameterType was selected only
// by PostgreSQL's final unresolved-target rule. Contextual constraints must be
// consolidated before these defaults when several occurrences represent one
// numbered protocol parameter.
func (s *Statement) ParameterTypeTargetDefault(index int) bool {
	return s != nil && index >= 0 && index < s.params &&
		index < len(s.paramTypeTargetDefaults) &&
		s.paramTypeTargetDefaults[index]
}

func (s *Statement) seedParameterTypes(parameterTypes []ParameterType) error {
	if s == nil || s.params == 0 || len(parameterTypes) == 0 {
		return nil
	}
	end := min(s.paramBase+s.params, len(parameterTypes))
	for absolute := s.paramBase; absolute < end; absolute++ {
		parameterType := parameterTypes[absolute]
		if parameterType == ParameterTypeUnspecified {
			continue
		}
		if err := s.mergeParameterType(absolute, parameterType, -1); err != nil {
			return err
		}
	}
	return nil
}

func (s *Statement) prepareParameterTypes() error {
	if s == nil || s.params == 0 || s.nested == nil {
		return nil
	}
	if err := s.mergeSetParameterTypes(s.nested.set); err != nil {
		return err
	}
	if window := s.nested.window; window != nil {
		if err := s.mergeChildParameterTypes(window.input); err != nil {
			return err
		}
	}
	for i := range s.nested.subqueries {
		if err := s.mergeChildParameterTypes(s.nested.subqueries[i].stmt); err != nil {
			return err
		}
	}
	if derived := s.nested.derived; derived != nil {
		if err := s.mergeChildParameterTypes(derived.stmt); err != nil {
			return err
		}
	}
	if join := s.nested.relationJoin; join != nil {
		for i := range join.operands {
			if err := s.mergeChildParameterTypes(join.operands[i].stmt); err != nil {
				return err
			}
		}
	}
	// Only the statement that created a CTE catalog merges its definitions.
	// Descendants borrow that catalog and already carry their own flattened
	// vectors, so walking it from them would duplicate work and form cycles.
	if s.nested.ownsCTEs && s.nested.ctes != nil {
		for i := range s.nested.ctes.defs {
			if err := s.mergeChildParameterTypes(s.nested.ctes.defs[i].stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Statement) mergeChildParameterTypes(child *Statement) error {
	if child == nil || len(child.paramTypes) == 0 {
		return nil
	}
	for local, paramType := range child.paramTypes {
		if paramType == ParameterTypeUnspecified {
			continue
		}
		if err := s.mergeParameterType(
			child.paramBase+local, paramType, child.ParameterTypePosition(local),
		); err != nil {
			return err
		}
		if child.ParameterTypeTargetDefault(local) {
			s.markParameterTypeTargetDefault(child.paramBase + local)
		}
	}
	return nil
}

func (s *Statement) mergeSetParameterTypes(r *statementSetSQL) error {
	if r == nil {
		return nil
	}
	base := r.rootArgBase + r.rangeBase
	if base < r.rootArgBase {
		return fmt.Errorf("query: set parameter metadata base overflows: %w", ErrParameterType)
	}
	for local, representation := range r.paramTypes {
		paramType := parameterTypeForRepresentation(representation)
		if paramType == ParameterTypeUnspecified {
			continue
		}
		absolute := base + local
		if err := s.mergeParameterType(
			absolute, paramType, r.paramTypePosition(local),
		); err != nil {
			return err
		}
		// A declared string subtype is an input fact, but a set common-type
		// selection is the expression domain. When a concrete TEXT candidate wins,
		// retain that resolved target so bind-time bpchar/name/varchar coercion is
		// applied before cells enter the set runner.
		statementLocal := absolute - s.paramBase
		if statementLocal >= 0 && statementLocal < len(s.paramTypes) &&
			parameterTypesShareStringCategory(s.paramTypes[statementLocal], paramType) {
			s.paramTypes[statementLocal] = paramType
		}
	}
	for i := range r.leaves {
		if err := s.mergeChildParameterTypes(r.leaves[i].stmt); err != nil {
			return err
		}
	}
	for i := range r.groups {
		if err := s.mergeSetParameterTypes(r.groups[i].runner); err != nil {
			return err
		}
	}
	return nil
}

func (s *Statement) mergeParameterType(
	absolute int,
	paramType ParameterType,
	position int,
) error {
	if absolute < s.paramBase || absolute-s.paramBase >= s.params {
		return fmt.Errorf(
			"query: inferred parameter at absolute ordinal %d is outside [%d,%d): %w",
			absolute, s.paramBase, s.paramBase+s.params, ErrParameterType,
		)
	}
	local := absolute - s.paramBase
	if s.paramTypes == nil {
		s.paramTypes = make([]ParameterType, s.params)
	}
	existing := s.paramTypes[local]
	if existing != ParameterTypeUnspecified && existing != paramType &&
		!parameterTypesShareStringCategory(existing, paramType) {
		if position < 0 {
			position = s.ParameterTypePosition(local)
		}
		return &ParameterTypeConflictError{
			Pos: position, Parameter: local + 1,
			Existing: existing, Inferred: paramType,
		}
	}
	if existing == ParameterTypeUnspecified {
		s.paramTypes[local] = paramType
	}
	if local < len(s.paramTypeTargetDefaults) {
		// Every ordinary merge is a contextual constraint. Callers propagating
		// a target-list default mark it explicitly after this merge succeeds.
		s.paramTypeTargetDefaults[local] = false
	}
	if position >= 0 {
		if s.paramTypePositions == nil {
			s.paramTypePositions = make([]int, s.params)
		}
		encoded := position + 1
		if s.paramTypePositions[local] == 0 || encoded < s.paramTypePositions[local] {
			s.paramTypePositions[local] = encoded
		}
	}
	return nil
}

func (s *Statement) markParameterTypeTargetDefault(absolute int) {
	if s == nil || absolute < s.paramBase || absolute-s.paramBase >= s.params {
		return
	}
	local := absolute - s.paramBase
	if s.paramTypeTargetDefaults == nil {
		s.paramTypeTargetDefaults = make([]bool, s.params)
	}
	s.paramTypeTargetDefaults[local] = true
}

func parameterTypesShareStringCategory(left, right ParameterType) bool {
	return parameterTypeIsString(left) && parameterTypeIsString(right)
}

func parameterTypeIsString(paramType ParameterType) bool {
	switch paramType {
	case ParameterTypeText, ParameterTypeVarchar, ParameterTypeName, ParameterTypeBPChar:
		return true
	default:
		return false
	}
}

func parameterTypeForRepresentation(representation OutputRepresentation) ParameterType {
	switch representation {
	case OutputSQLBool:
		return ParameterTypeBool
	case OutputSQLText:
		return ParameterTypeText
	case OutputSQLVarchar:
		return ParameterTypeVarchar
	case OutputSQLName:
		return ParameterTypeName
	case OutputSQLBPChar:
		return ParameterTypeBPChar
	default:
		return ParameterTypeUnspecified
	}
}

// SQL returns the statement text as it was prepared.
func (s *Statement) SQL() string { return s.text }

// NumJoins returns the number of JOIN clauses. A statement with any of them
// must execute against [FromDatabase] or [FromFileDatabase], whose database
// snapshot resolves every collection at one instant; a caller holding a single
// collection can refuse it before it takes a snapshot it would only discard.
func (s *Statement) NumJoins() int {
	return s.catalogCapabilities(0).joins
}

// UsesGeneralizedRelationJoin reports whether this prepared statement executes
// JOINs through the relation-spool pipeline. Adapters use it to retain their
// existing storage-aware path for the legacy physical equi-join subset while
// supplying a coherent catalog directly to generalized relation operands.
// It is a single pointer test and allocates no feature state when false.
func (s *Statement) UsesGeneralizedRelationJoin() bool {
	if s == nil {
		return false
	}
	if window := s.window(); window != nil {
		return window.input.UsesGeneralizedRelationJoin()
	}
	if set := s.setSQL(); set != nil {
		return set.generalizedJoin
	}
	return s.relationJoin() != nil
}

// RequiresCatalog reports whether execution may read a collection other than
// the driving one. JOIN and nested SELECT both require one coherent database
// snapshot; adapters use this to avoid constructing a single-collection Source
// that execution would have to reject.
func (s *Statement) RequiresCatalog() bool {
	return s.catalogCapabilities(0).requires
}

// AppendSchema appends the statement's output schema to dst, one entry per
// SELECT column. It is [Query.AppendSchema] narrowed to the columns the caller
// can see: a HAVING clause may have added a hidden column, and a result schema
// that advertised it would be advertising a column [Cursor.Cell] refuses to
// return.
func (s *Statement) AppendSchema(dst []OutputColumn) []OutputColumn {
	if set := s.setSQL(); set != nil {
		return set.AppendSchema(dst)
	}
	if scalar := s.scalarStatement(); scalar != nil {
		start := len(dst)
		dst = scalar.appendSchema(dst, s.names)
		s.applyDirectRelationSchema(dst[start:])
		return dst
	}
	start := len(dst)
	dst = s.q.AppendSchema(dst)
	if len(dst)-start > s.outputs {
		dst = dst[:start+s.outputs]
	}
	if window := s.window(); window != nil {
		for i := range window.outputs {
			dst[start+i].Reduction = window.outputs[i].reduction
			dst[start+i].Type = window.outputs[i].valueType
			dst[start+i].Representation = window.outputs[i].representation
		}
	}
	s.applyDirectRelationSchema(dst[start:])
	return dst
}

// applyDirectRelationSchema preserves a typed child's SQL boundary through a
// derived-table or CTE identity projection. Execution already preserves the
// native Cell kind in the ordinal relation spool; this cold metadata overlay
// keeps RowDescription and transport encoding aligned with those cells. The
// child schema lives on the existing relation-only sidecars, so ordinary
// Statements gain neither storage nor hot-path branches.
func (s *Statement) applyDirectRelationSchema(outputs []OutputColumn) {
	if s == nil || len(outputs) == 0 || !s.hasRelationBinding() ||
		s.tree == nil {
		return
	}
	output := 0
	for column := range s.tree.Columns {
		if output >= len(outputs) {
			return
		}
		projected := &s.tree.Columns[column]
		if projected.Scalar != nil || projected.Window != nil ||
			projected.Agg != sqlast.AggNone || projected.Path == nil {
			output++
			continue
		}
		binding := s.relationBindingForSource(projected.Path.Source)
		if len(projected.Path.Segments) == 0 {
			for ordinal := range binding.names {
				if output >= len(outputs) {
					return
				}
				applyDirectRelationColumnSchema(&outputs[output], binding.schema, ordinal)
				output++
			}
			continue
		}
		if len(projected.Path.Segments) == 1 {
			ordinal, err := s.resolveRelationColumnAt(
				projected.Path.Source, projected.Path.Segments[0].Key,
			)
			if err == nil {
				applyDirectRelationColumnSchema(
					&outputs[output], binding.schema, ordinal-binding.offset,
				)
			}
		}
		output++
	}
}

func applyDirectRelationColumnSchema(
	output *OutputColumn,
	schema []OutputColumn,
	ordinal int,
) {
	if output == nil || ordinal < 0 || ordinal >= len(schema) ||
		schema[ordinal].Representation == OutputJSON {
		return
	}
	output.Type = schema[ordinal].Type
	output.Representation = schema[ordinal].Representation
}

// Release drops every buffer s retains, invalidating its plan. A Statement is
// meant to be held and reused — the retained compiler arenas are exactly what
// makes a warmed bind allocation-free — so Release is for tearing one down, not
// for reclaiming space between executions.
func (s *Statement) Release() {
	if s == nil {
		return
	}
	s.c.release()
	if s.nested != nil {
		if s.nested.set != nil {
			s.nested.set.Release()
		}
		s.nested.scalar = nil
		for i := range s.nested.subqueries {
			s.nested.subqueries[i].stmt.Release()
			s.nested.subqueries[i].exec.Release()
		}
		clear(s.nested.decorrelated)
		s.nested.decorrelated = nil
		if s.nested.derived != nil {
			s.nested.derived.stmt.Release()
			s.nested.derived.exec.Release()
			s.nested.derived.spool.release()
		}
		if s.nested.relationJoin != nil {
			s.nested.relationJoin.release()
		}
		if s.nested.window != nil {
			s.nested.window.release()
		}
		if s.nested.cte != nil {
			s.nested.cte.spool.release()
		}
		if s.nested.ownsCTEs && s.nested.ctes != nil {
			s.nested.ctes.release()
		}
	}
	*s = Statement{}
}

// RunInto binds args and executes s into e's caller-owned storage, returning a
// cursor over the surviving rows.
//
// args must hold exactly [Statement.NumParams] values. Each may be nil, a
// bool, any signed or unsigned integer, a float, a string, a []byte, or a
// [Number]. The pointer forms *bool, *int64, *float64, *string, and *Number are
// also accepted so protocol adapters can bind stable scalar slots without
// boxing allocations; a nil pointer or nil argument makes its leaf UNKNOWN,
// exactly as a SQL NULL comparison would. Anything else is rejected by name.
//
// The returned [Cursor] walks e.Result, applying the HAVING filter and the
// OFFSET and LIMIT the plan could not carry. It borrows e and stays valid until
// the next execution into e, the same rule e.Result itself carries.
func (s *Statement) RunInto(e *Exec, src Source, args []any) (Cursor, error) {
	if e == nil {
		return Cursor{}, fmt.Errorf("query: RunInto requires a non-nil Exec")
	}
	// An invalid statement-wide option is still an execution attempt. Match
	// Query.RunInto's destination contract by invalidating the prior result and
	// stats before validating anything that can fail.
	clearExecBorrowedViews(e)
	e.Stats = ExecStats{}
	if s.nested == nil {
		// IntermediateBytes is a statement-wide option even when this statement
		// has no relation-valued child. Validate it without constructing a frame;
		// passing a stack frame into the recursive nested executor would force one
		// heap allocation on every ordinary execution.
		if _, err := normalizeIntermediateBytes(e.Options); err != nil {
			return Cursor{}, err
		}
		return s.runDirectInto(e, src, args)
	}
	s.discardRelations()
	frame := &s.nested.frame
	if err := frame.begin(e.Options); err != nil {
		return Cursor{}, err
	}
	frame.args = args
	cursor, err := s.runIntoFrame(e, src, args, frame, "")
	// The persistent frame exists only to keep nested execution allocation-free.
	// It must not extend the lifetime of caller-owned bindings past this
	// synchronous execution; recursive and CTE children have completed before
	// runIntoFrame returns, and the result cursor borrows only Exec.Result.
	frame.args = nil
	return cursor, err
}

// RunIntermediateInto binds and executes s like [Statement.RunInto], but makes
// its final Result part of the statement-wide IntermediateBytes allowance.
// This is the execution boundary for a query result that immediately feeds a
// larger atomic operation, such as INSERT ... SELECT.
//
// The returned retained byte count is exact for the successful execution: it
// includes e.Result plus every relation, CTE, set, window, join, and scalar
// dependency spool that remains live while the cursor is consumed. A caller
// starts its next staging account with that count and keeps it admitted until
// it is finished with the cursor. Historical intermediates already released by
// the query are not included; their peaks were enforced while they were live.
//
// ResultRows and ResultBytes retain their ordinary caller-visible semantics.
// If both byte limits would reject the same growth, the stricter limit selects
// the error type and ResultBytes wins an equal-limit tie. Rejected and canceled
// executions return a zero count and expose no partial Result. The method is
// single-consumer and allocation-free after the same warm-up as RunInto.
func (s *Statement) RunIntermediateInto(
	e *Exec,
	src Source,
	args []any,
) (cursor Cursor, retained int64, err error) {
	if e == nil {
		return Cursor{}, 0, fmt.Errorf(
			"query: RunIntermediateInto requires a non-nil Exec",
		)
	}
	// Match RunInto's failed-attempt contract before validating either budget.
	clearExecBorrowedViews(e)
	e.Stats = ExecStats{}
	limit, err := normalizeIntermediateBytes(e.Options)
	if err != nil {
		return Cursor{}, 0, err
	}

	if s.nested == nil {
		e.Result.beginRootIntermediate(nil, limit)
		defer e.Result.endRootIntermediate()
		cursor, err = s.runDirectInto(e, src, args)
		if err != nil {
			return Cursor{}, 0, err
		}
		return cursor, e.Result.resultBytesUsed, nil
	}

	s.discardRelations()
	frame := &s.nested.frame
	if err = frame.begin(e.Options); err != nil {
		return Cursor{}, 0, err
	}
	frame.args = args
	e.Result.beginRootIntermediate(&frame.intermediate, 0)
	defer func() {
		e.Result.endRootIntermediate()
		frame.args = nil
	}()

	cursor, err = s.runIntoFrame(e, src, args, frame, "")
	if err != nil {
		// Every branch normally releases its own live relation state. The root
		// cleanup is intentionally idempotent and closes that invariant at the
		// API boundary for bind, cancellation, and future execution branches.
		clearExecBorrowedViews(e)
		s.releaseRelations(frame)
		return Cursor{}, 0, err
	}
	retained = saturatedBytes(
		frame.intermediate.used, e.Result.resultBytesUsed,
	)
	return cursor, retained, nil
}

// runDirectInto is the no-nested-state execution path. Keeping it separate is
// an escape-analysis boundary: the recursive relation executor may retain a
// statementFrame during one synchronous nested run, but an ordinary SELECT has
// no frame to retain and therefore remains allocation-free.
func (s *Statement) runDirectInto(e *Exec, src Source, args []any) (Cursor, error) {
	if len(args) != s.params {
		return Cursor{}, fmt.Errorf(
			"query: statement has %d placeholder(s) and %d argument(s) were bound",
			s.params, len(args),
		)
	}
	if err := s.bind(args); err != nil {
		return Cursor{}, err
	}
	if s.sourceIndependent() {
		src = sourceIndependentSQLSource()
	}
	if err := s.q.RunInto(e, src); err != nil {
		return Cursor{}, s.translateSQLPathComparisonError(err)
	}
	return s.cursor(&e.Result), nil
}

// runIntoFrame is RunInto with the statement-wide accounts already installed.
// Nested statements call this entry point so recursion never manufactures a
// fresh intermediate allowance.
func (s *Statement) runIntoFrame(
	e *Exec,
	src Source,
	args []any,
	frame *statementFrame,
	intermediateResource string,
) (Cursor, error) {
	cursor, err := s.runIntoFrameMode(
		e, src, args, frame, intermediateResource, nil, true,
	)
	return cursor, s.translateSQLPathComparisonError(err)
}

func (s *Statement) runIntoCorrelationFrame(
	e *Exec,
	src Source,
	args []any,
	frame *statementFrame,
	intermediateResource string,
	correlations []scalar,
	bindPlan bool,
) (Cursor, error) {
	if s.correlation == nil {
		return Cursor{}, fmt.Errorf("query: correlation values supplied to an ordinary statement")
	}
	cursor, err := s.runIntoFrameMode(
		e, src, args, frame, intermediateResource, correlations, bindPlan,
	)
	return cursor, s.translateSQLPathComparisonError(err)
}

func (s *Statement) runIntoFrameMode(
	e *Exec,
	src Source,
	args []any,
	frame *statementFrame,
	intermediateResource string,
	correlations []scalar,
	bindPlan bool,
) (Cursor, error) {
	// Invalidate the previous root or nested result before any fallible bind or
	// child execution. Query.RunInto performs the same reset later, but a
	// subquery error happens before that call and must not leave a prior success
	// looking like the failed execution's output.
	clearExecBorrowedViews(e)
	// Nested relation materialization runs before the final Query scan. Publish
	// the tuple now so a nested LATERAL can borrow its containing child's slots;
	// Query.runIntoCorrelations will re-publish the same tuple after its own entry
	// cleanup for the final scan. The defer closes every early error/cancel path.
	if err := e.Workspace.bindCorrelations(correlations); err != nil {
		return Cursor{}, err
	}
	defer e.Workspace.resetCorrelationBindings()
	if intermediateResource != "" {
		// ResultRows and ResultBytes bound only the caller-visible result. A
		// relation-valued child is instead bounded by the one statement frame;
		// inheriting the outer limits changes valid LIMIT queries into failures.
		e.Options.ResultRows = -1
		e.Options.ResultBytes = -1
	}
	if len(args) != s.params {
		return Cursor{}, fmt.Errorf(
			"query: statement has %d placeholder(s) and %d argument(s) were bound",
			s.params, len(args))
	}
	if set := s.setSQL(); set != nil {
		if s.nested.ownsCTEs && s.nested.ctes != nil {
			defer s.nested.ctes.releaseExecution(frame)
		}
		limit := e.Options.ResultBytes
		if intermediateResource != "" {
			limit = frame.intermediate.remaining()
			if limit == 0 {
				return Cursor{}, &IntermediateBudgetError{
					Resource: intermediateResource,
					Bytes:    saturatedBytes(frame.intermediate.used, 1),
					Limit:    frame.intermediate.limit,
				}
			}
			e.Options.ResultBytes = limit
		}
		cursor, err := set.runIntoFrame(e, src, args, frame, intermediateResource)
		if err != nil && intermediateResource != "" {
			err = translateSetIntermediateError(
				err, frame, limit, intermediateResource,
			)
		}
		return cursor, err
	}
	runSource := src
	if s.sourceIndependent() {
		runSource = sourceIndependentSQLSource()
	}
	if s.nested != nil {
		if s.nested.window != nil {
			return s.nested.window.run(
				s, e, src, args, frame, intermediateResource,
				correlations, bindPlan,
			)
		}
		if len(correlations) == 0 && s.canFuseCTE() {
			return s.runFusedCTE(e, src, frame, intermediateResource)
		}
		var err error
		runSource, err = s.runRelations(e, runSource, args, frame)
		if err != nil {
			s.releaseRelations(frame)
			return Cursor{}, err
		}
	}
	if err := s.runSubqueries(e, src, args, frame); err != nil {
		s.releaseSubqueries(frame)
		s.releaseRelations(frame)
		return Cursor{}, err
	}
	defer s.releaseSubqueries(frame)
	if bindPlan {
		if err := s.bind(args); err != nil {
			s.releaseRelations(frame)
			return Cursor{}, err
		}
	}
	if intermediateResource != "" {
		remaining := frame.intermediate.remaining()
		if remaining == 0 {
			s.releaseRelations(frame)
			return Cursor{}, &IntermediateBudgetError{
				Resource: intermediateResource,
				Bytes:    saturatedBytes(frame.intermediate.used, 1),
				Limit:    frame.intermediate.limit,
			}
		}
		// Install the remainder immediately before final materialization. A
		// deeper derived relation or predicate subquery may have consumed part
		// of the allowance since this nested statement began.
		e.Options.ResultBytes = remaining
	}
	if scalar := s.scalarStatement(); scalar != nil {
		options := e.Options
		remaining := frame.intermediate.remaining()
		if remaining == 0 {
			s.releaseRelations(frame)
			return Cursor{}, &IntermediateBudgetError{
				Resource: "scalar dependency result",
				Bytes:    saturatedBytes(frame.intermediate.used, 1),
				Limit:    frame.intermediate.limit,
			}
		}
		e.Options.ResultRows = -1
		e.Options.ResultBytes = remaining
		err := s.q.runIntoCorrelations(e, runSource, correlations)
		e.Options = options
		if err != nil {
			clearExecBorrowedViews(e)
			s.releaseRelations(frame)
			var resultErr *ResultBudgetError
			if errors.As(err, &resultErr) && resultErr.ByteLimit == remaining {
				return Cursor{}, &IntermediateBudgetError{
					Resource: "scalar dependency result",
					Bytes:    saturatedBytes(frame.intermediate.used, resultErr.Bytes),
					Limit:    frame.intermediate.limit,
				}
			}
			return Cursor{}, err
		}
		cursor, err := scalar.execute(s, e, frame, options, intermediateResource)
		if err != nil {
			s.releaseRelations(frame)
		}
		return cursor, err
	}
	if err := s.q.runIntoCorrelations(e, runSource, correlations); err != nil {
		// A relation execution may have parked grouped/order state that borrows
		// its spool before a later result-budget or cancellation error. Sever
		// every parent view before resetting the statement-owned spool.
		clearExecBorrowedViews(e)
		s.releaseRelations(frame)
		var resultErr *ResultBudgetError
		if intermediateResource != "" && errors.As(err, &resultErr) &&
			resultErr.ByteLimit == e.Options.ResultBytes {
			return Cursor{}, &IntermediateBudgetError{
				Resource: intermediateResource,
				Bytes: saturatedBytes(
					frame.intermediate.used, resultErr.Bytes,
				),
				Limit: frame.intermediate.limit,
			}
		}
		return Cursor{}, err
	}
	return s.cursor(&e.Result), nil
}

func (s *Statement) sourceIndependent() bool {
	return s != nil && s.tree != nil && s.tree.Set == nil && len(s.tree.From) == 0
}

func (s *Statement) prepareSubqueries(argBase int) error {
	if err := s.prepareDecorrelatedExists(); err != nil {
		return err
	}
	if err := s.collectSubqueries(s.tree.Where, argBase); err != nil {
		return err
	}
	return s.collectSubqueries(s.tree.Having, argBase)
}

func (s *Statement) collectSubqueries(e *sqlast.Expr, argBase int) error {
	if e == nil {
		return nil
	}
	if e.Subquery != nil {
		if e.Subquery.Correlation != nil {
			if s.decorrelatedExistsFor(e) != nil {
				return nil
			}
			return sqlast.NewFeatureNotSupportedError(
				s.text, e.Pos,
				"correlated predicate subquery was not proved as a supported top-level conjunct with complete equality correlation keys",
			)
		}
		use := subqueryScalarUse
		switch e.Kind {
		case sqlast.ExprIn:
			use = subqueryIn
		case sqlast.ExprExists:
			use = subqueryExists
		}
		if use != subqueryExists && len(e.Subquery.Columns) != 1 {
			return fmt.Errorf("query: a scalar or IN subquery must return exactly one column")
		}
		limit := uint8(2)
		if use == subqueryExists {
			limit = 1
		}
		if use == subqueryIn {
			limit = 0
		}
		stmt, err := prepareTreeInContext(
			s.text, e.Subquery, limit, s.cteCatalog(), argBase+e.Subquery.ParamBase,
			s.parameterTypeHints,
		)
		if err != nil {
			return err
		}
		if s.nested == nil {
			s.nested = new(nestedStatements)
		}
		s.nested.subqueries = append(s.nested.subqueries, statementSubquery{
			tree: e.Subquery, stmt: stmt, use: use, preview: true, exists: true,
		})
		return nil
	}
	for _, kid := range e.Kids {
		if err := s.collectSubqueries(kid, argBase); err != nil {
			return err
		}
	}
	return nil
}

func (s *Statement) runSubqueries(
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
) error {
	if s.nested == nil {
		return nil
	}
	for i := range s.nested.subqueries {
		sub := &s.nested.subqueries[i]
		n := sub.stmt.NumParams()
		base := sub.tree.ParamBase
		if base < 0 || base+n > len(args) {
			return fmt.Errorf("query: invalid nested placeholder range")
		}
		nestedSource, err := src.subquerySource(s.Collection(), sub.stmt.Collection())
		if err != nil {
			return err
		}
		sub.exec.Options = parent.Options
		cursor, err := sub.stmt.runIntoFrame(
			&sub.exec, nestedSource, args[base:base+n], frame,
			"predicate subquery result",
		)
		if err != nil {
			return err
		}
		retained := sub.exec.Result.resultBytesUsed
		if err := frame.intermediate.reserve("predicate subquery result", retained); err != nil {
			return err
		}
		sub.resultBytes = retained
		sub.preview = false
		sub.exists = false
		sub.hasNull = false
		clear(sub.values)
		sub.values = sub.values[:0]

		rows, known, payload, hasNull, err := measureSubqueryValues(
			&cursor, sub.use, parent.Options.Cancel,
		)
		if err != nil {
			return err
		}
		sub.exists = rows != 0
		sub.hasNull = hasNull
		if sub.use == subqueryScalarUse && rows > 1 {
			return &CardinalityViolationError{}
		}
		charge := predicateValuesRetainedBytes(known, payload)
		if err := frame.intermediate.reserve(
			"predicate subquery values", charge,
		); err != nil {
			return err
		}
		sub.activeBytes = charge
		if known != 0 {
			sub.slots = reserve(sub.slots[:0], known)[:known]
			for slot := range sub.slots {
				buf := sub.slots[slot].buf[:0]
				sub.slots[slot] = subqueryScalar{buf: buf}
			}
			sub.values = reserve(sub.values, known)
			cursor = sub.stmt.cursor(&sub.exec.Result)
			if err := materializeSubqueryValues(
				sub, &cursor, parent.Options.Cancel,
			); err != nil {
				return err
			}
		} else {
			sub.slots = sub.slots[:0]
		}
		// Predicate values now own every variable-width byte. Drop the child
		// Result, its borrowed workspace views, and any derived spool before
		// evaluating the next sibling subquery.
		s.releaseSubqueryResult(sub, frame)
	}
	return nil
}

func measureSubqueryValues(
	cursor *Cursor,
	use subqueryUse,
	cancel *CancelFlag,
) (rows, known int, payload int64, hasNull bool, err error) {
	for {
		var next bool
		next, err = cursor.nextWithCancel(cancel)
		if err != nil || !next {
			return
		}
		rows++
		if use == subqueryExists {
			continue
		}
		cell := cursor.Cell(0)
		switch cell.Kind() {
		case TypeNull:
			hasNull = true
		case TypeBool:
			known++
		case TypeNumber:
			known++
			if _, integer := cell.Int64(); !integer {
				payload = saturatedBytes(
					payload, int64(encodedCellJSONBytes(cell)),
				)
			}
		case TypeString:
			known++
			text, _ := cell.Text()
			payload = saturatedBytes(payload, int64(len(text)))
		default:
			err = fmt.Errorf(
				"query: a subquery predicate requires scalar values, got %v",
				cell.Kind(),
			)
			return
		}
	}
}

func materializeSubqueryValues(
	sub *statementSubquery,
	cursor *Cursor,
	cancel *CancelFlag,
) error {
	slot := 0
	for {
		next, err := cursor.nextWithCancel(cancel)
		if err != nil {
			return err
		}
		if !next {
			return nil
		}
		if cursor.Cell(0).Kind() == TypeNull {
			continue
		}
		value, known, err := scalarFromCell(&sub.slots[slot], cursor.Cell(0))
		if err != nil {
			return err
		}
		if !known {
			continue
		}
		sub.values = append(sub.values, value)
		slot++
	}
}

func (s *Statement) releaseSubqueryResult(
	sub *statementSubquery,
	frame *statementFrame,
) {
	clearExecBorrowedViews(&sub.exec)
	sub.stmt.releaseRelations(frame)
	frame.intermediate.release(sub.resultBytes)
	sub.resultBytes = 0
}

func (s *Statement) releaseSubqueries(frame *statementFrame) {
	if s.nested == nil {
		return
	}
	for i := range s.nested.subqueries {
		sub := &s.nested.subqueries[i]
		s.releaseSubqueryResult(sub, frame)
		frame.intermediate.release(sub.activeBytes)
		sub.activeBytes = 0
		clear(sub.values)
		sub.values = sub.values[:0]
		for slot := range sub.slots {
			buf := sub.slots[slot].buf[:0]
			sub.slots[slot] = subqueryScalar{buf: buf}
		}
		sub.slots = sub.slots[:0]
		sub.hasNull = false
		sub.exists = false
	}
}

func clearExecBorrowedViews(e *Exec) {
	e.Result.abortResult()
	e.Workspace.clearBorrowedViews()
	e.Workspace.resetJoinBindings()
	for i := range e.file.workers {
		e.file.workers[i].clearBorrowedViews()
	}
}

func scalarFromCell(slot *subqueryScalar, cell Cell) (any, bool, error) {
	switch cell.Kind() {
	case TypeNull:
		return nil, false, nil
	case TypeBool:
		slot.b, _ = cell.Bool()
		return &slot.b, true, nil
	case TypeNumber:
		if v, ok := cell.Int64(); ok {
			slot.i = v
			return &slot.i, true, nil
		}
		slot.buf = cell.AppendJSON(slot.buf[:0])
		slot.n = Number(byteview.String(slot.buf))
		return &slot.n, true, nil
	case TypeString:
		text, _ := cell.Text()
		slot.buf = append(slot.buf[:0], text...)
		slot.s = byteview.String(slot.buf)
		return &slot.s, true, nil
	default:
		return nil, false, fmt.Errorf(
			"query: a subquery predicate requires scalar values, got %v", cell.Kind())
	}
}

func (s *Statement) subquery(tree *sqlast.SelectStmt) *statementSubquery {
	if s.nested == nil {
		return nil
	}
	for i := range s.nested.subqueries {
		if s.nested.subqueries[i].tree == tree {
			return &s.nested.subqueries[i]
		}
	}
	return nil
}

// Run binds args and executes s over src with transient storage this call
// owns. It is [Statement.RunInto] for a one-off; a hot loop holds an [Exec].
func (s *Statement) Run(src Source, args []any) (Result, Cursor, error) {
	var e Exec
	cursor, err := s.RunInto(&e, src, args)
	// Match Query.Run's one-shot lifecycle: a durable execution parks a
	// scanner and workers in Exec for reuse, but this transient Exec has no
	// second call or Release through which to retire them. Stopping the pool
	// does not affect Result or Cursor; durable cells own their bytes and the
	// cursor only walks the completed Result.
	e.file.stopPool()
	return e.Result, cursor, err
}

// bind re-lowers the plan when parameters or nested results can change it. A
// statement with neither lowered once at prepare and uses the cached outcome.
func (s *Statement) bind(args []any) error {
	if s == nil || s.tree == nil {
		return fmt.Errorf("query: cannot execute a nil or released Statement")
	}
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

// NewTextCursor creates a one-row text cursor for protocol metadata such as a
// compile-time EXPLAIN result. The returned cursor owns its small result and
// follows the same value lifetime as every other Cursor; Close is represented
// by dropping the value.
func NewTextCursor(header, text string) Cursor {
	raw, _ := vibejson.Marshal(&text)
	statement := &Statement{outputs: 1}
	result := &Result{
		Columns: []ResultColumn{{
			Header: header,
			Cells:  []Cell{{kind: TypeString, text: text, raw: raw}},
		}},
		RowCount: 1,
	}
	return statement.cursor(result)
}

// Next advances to the next surviving row, reporting false at the end.
func (c *Cursor) Next() bool {
	next, _ := c.nextWithCancel(nil)
	return next
}

// NextWithCancel advances like [Cursor.Next], but observes cancel while it
// walks underlying rows rejected by HAVING or OFFSET. Callers that perform a
// second, atomic operation from a query cursor use this form so cancellation
// latency is bounded even when the query yields no visible rows. A nil flag is
// the same allocation-free path as Next.
func (c *Cursor) NextWithCancel(cancel *CancelFlag) (bool, error) {
	return c.nextWithCancel(cancel)
}

// nextWithCancel is the shared implementation for public cursor consumers and
// internal relation materializers.
func (c *Cursor) nextWithCancel(cancel *CancelFlag) (bool, error) {
	if c.res == nil || c.left == 0 {
		return false, nil
	}
	if err := cancellationError(cancel); err != nil {
		return false, err
	}
	for c.row < c.res.RowCount {
		row := c.row
		c.row++
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return false, err
		}
		if c.st != nil && !c.st.having.keep(c.res, row) {
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
		return true, nil
	}
	c.cur = -1
	return false, nil
}

// Cell returns the value of output column col in the current row. Calling it
// before the first Next, or after Next reported false, returns a null cell.
func (c *Cursor) Cell(col int) Cell {
	if c.cur < 0 || c.res == nil || col < 0 ||
		(c.st != nil && col >= c.st.outputs) ||
		(c.st == nil && col >= len(c.res.Columns)) {
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
	if s.relationJoin() != nil || s.hasRelationBinding() && p.Source == 0 {
		return s.renderDerived(p, local)
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
	if s.window() != nil {
		return nil
	}
	s.outputs = 0
	capacity := len(s.tree.Columns)
	if s.hasRelationBinding() {
		for source := range s.tree.From {
			capacity += len(s.relationBindingForSource(source).names)
		}
	}
	s.names = reserve(s.names[:0], capacity)
	for i := range s.tree.Columns {
		column := &s.tree.Columns[i]
		if column.Scalar != nil {
			s.names = append(s.names, s.columnName(column))
			s.outputs++
			continue
		}
		if s.hasRelationBinding() && column.Agg == sqlast.AggNone &&
			column.Path != nil && len(column.Path.Segments) == 0 {
			relation := s.relationBindingForSource(column.Path.Source)
			if column.Alias != "" {
				return fmt.Errorf(
					"query: relation wildcard expands to %d columns and cannot have the single alias %q",
					len(relation.names), column.Alias,
				)
			}
			s.names = append(s.names, relation.names...)
			s.outputs += len(relation.names)
			continue
		}
		s.names = append(s.names, s.columnName(column))
		s.outputs++
	}
	return nil
}

// columnName answers the output header for one result column.
func (s *Statement) columnName(col *sqlast.ResultColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	if col.Scalar != nil {
		return "?column?"
	}
	spec := s.spec(col.Path)
	if s.relationJoin() != nil && col.Path != nil {
		spec = s.relationJoinDisplaySpec(col.Path)
	} else if s.hasRelationBinding() && col.Path != nil {
		spec = s.derivedDisplaySpec(col.Path)
	}
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

func (s *Statement) relationJoinDisplaySpec(path *sqlast.PathExpr) string {
	if path == nil {
		return ""
	}
	start := len(s.specBuf)
	qualified := path.MergedUsing == 0 && path.Source > 0 && path.Source < len(s.tree.From)
	if qualified {
		s.specBuf = append(s.specBuf, s.tree.From[path.Source].Alias...)
		s.specBuf = append(s.specBuf, '.')
	}
	specStart := len(s.specBuf)
	s.specBuf = path.AppendSpec(s.specBuf)
	if qualified && len(s.specBuf) == specStart {
		s.specBuf = append(s.specBuf, '*')
	}
	return byteview.String(s.specBuf[start:len(s.specBuf):len(s.specBuf)])
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
