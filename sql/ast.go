package sql

// The abstract syntax tree.
//
// Every node is a plain struct with exported fields and no methods that hide
// state, so a lowering pass reads it as data. The tree is deliberately shaped
// after the engine's own vocabulary rather than after SQL's: a predicate is one
// tagged node with an n-ary child list, exactly like query's compiledPredicate,
// because the lowering that consumes this is a rename and a walk rather than a
// translation. Where SQL has a construct the engine has no counterpart for, the
// parser rejects it, so no node here exists to represent one. Clauses such as
// [SelectStmt.Having] and [SelectStmt.Offset] that run around the core scan plan
// remain explicit nodes and are applied by the statement layer.
//
// A tree produced by a [Parser] is owned by that Parser and is valid only until
// its next Parse; see the Parser documentation. It never points into the
// statement text, so the source may be reused as soon as Parse returns.

// A SelectStmt is one parsed SELECT statement.
type SelectStmt struct {
	// With holds the non-recursive common-table-expression definitions visible
	// to this SELECT, or nil when the statement has no WITH clause. Definitions
	// stay in source order because that is both placeholder order and lexical
	// visibility order: a body sees earlier siblings, never itself or a later
	// sibling.
	With *WithClause

	// Columns is the SELECT list in source order.
	Columns []ResultColumn

	// Distinct requests one row per distinct projected value tuple. The parser
	// lowers the non-aggregate form to GROUP BY over the projection paths so the
	// existing spill-aware grouping engine supplies equality and bounded memory.
	Distinct bool

	// From holds the range variables. It is empty for a source-independent
	// one-row SELECT. Otherwise From[0] is the driving relation; each later entry
	// is a JOIN, in source order, and carries its own ON condition. Every
	// [PathExpr] in the statement names one of these by index, so a lowering pass
	// never has to re-resolve a name.
	From []TableRef

	// Where is the filter predicate, or nil when the statement has none.
	Where *Expr

	// GroupBy holds the grouping keys, or nil.
	GroupBy []*PathExpr

	// Having is the post-aggregation filter, or nil.
	//
	// The parser binds every HAVING leaf to a column already in the SELECT list
	// or to a GROUP BY key. The query statement layer evaluates it after
	// reduction and before ordering, offset, and limit. Any other lowerer that
	// cannot supply that step must reject a non-nil Having explicitly; silently
	// dropping a filter returns wrong rows.
	Having *Expr

	// Windows holds the named window definitions declared by this SELECT's
	// WINDOW clause. Definitions are ordered: a definition may inherit only
	// from an earlier entry. Window expressions carry their fully resolved
	// effective specification, so lowerers do not perform a name lookup.
	Windows []NamedWindow

	// OrderBy holds the sort keys, in priority order.
	OrderBy []OrderTerm

	// Limit is the row cap, or nil. It is a numeric literal or a placeholder.
	Limit *Operand

	// Offset is the number of leading rows to skip, or nil.
	//
	// The query statement layer applies it after ordering and before limit.
	// The same rule as Having applies to any other lowerer: reject a non-nil
	// Offset rather than ignore it.
	Offset *Operand

	// Params is the number of '?' placeholders in the statement. Placeholder
	// ordinals run from 0 to Params-1 in source order, which is the order a
	// database/sql driver binds arguments in.
	Params int

	// ParamBase is the first outer-statement placeholder occupied by this
	// statement when it is a subquery. It is zero for a top-level statement.
	ParamBase int

	// Correlation is the cold outer-reference sidecar of a predicate subquery.
	// It is nil for top-level statements, uncorrelated predicate subqueries,
	// CTE definitions, and ordinary derived relations. Keeping the occurrence
	// map here leaves every PathExpr at its established size and makes the
	// overwhelmingly common uncorrelated path pay neither storage nor work.
	//
	// A consumer must not infer executability from a non-nil sidecar: it records
	// all syntactically valid correlated shapes so a semantic layer can either
	// decorrelate them or issue a positioned refusal.
	Correlation *CorrelationSpec

	// Set is the cold query-expression sidecar. It is nil for an ordinary
	// SELECT, preserving the existing direct lowering path. When non-nil, this
	// SelectStmt mirrors Set.First only for stable output metadata; Params is
	// global and Set owns all executable leaves plus the exact binary/group tree
	// and query-expression tail. A consumer MUST branch on Set before lowering
	// any ordinary fields. Ignoring it would execute only the first operand.
	Set *SetExpression
}

// A WithClause owns the common table expressions declared for one SELECT. Its
// definitions and strings are backed by the Parser's arenas and obey the same
// borrowed-lifetime contract as the rest of the AST.
type WithClause struct {
	CTEs []CommonTableExpr
	// Recursive records the authored WITH RECURSIVE scope marker. Individual
	// definitions carry non-zero Recursive metadata only when they actually
	// reference themselves; SQL permits non-recursive definitions in this scope.
	Recursive bool
	// Pos is the byte offset of WITH.
	Pos int
}

// CTEMaterialization records the optimization-boundary spelling attached to a
// common table expression. It is policy, not execution state: the lowerer is
// responsible for proving when a body can be fused safely.
type CTEMaterialization uint8

const (
	CTEMaterializationDefault CTEMaterialization = iota
	CTEMaterialized
	CTENotMaterialized
)

// A CommonTableExpr is one WITH definition.
type CommonTableExpr struct {
	Name string
	// Columns optionally replaces the body's output names positionally. A
	// shorter list is legal; unmentioned outputs keep their derived names.
	Columns   []string
	ColumnPos []int
	// ColumnArityDeferred is true when a wildcard makes the body's expanded
	// output count runtime-dependent. In that case a catalog/runtime binder
	// checks Columns against the materialized schema; the parser must not infer
	// arity from len(Query.Columns).
	ColumnArityDeferred bool
	// Query retains the complete authored body. For a recursive definition it
	// is the lossless UNION set expression; Recursive identifies the two leaves
	// consumed by bounded fixpoint lowering. A consumer must branch on
	// Recursive.Anchor before treating Query as an ordinary CTE body.
	Query           *SelectStmt
	Recursive       RecursiveCTE
	Materialization CTEMaterialization
	// Pos is the byte offset of Name. HintPos is the byte offset of
	// MATERIALIZED or NOT, or -1 when no policy was written.
	Pos     int
	HintPos int
}

// RecursiveCTE is validated, allocation-free metadata for the supported SQL
// recursive shape. Its zero value identifies an ordinary definition, including
// an ordinary definition authored inside WITH RECURSIVE.
//
// Anchor and Term are exact leaves of CommonTableExpr.Query.Set. Their
// ParamBase values are relative to that CTE body; Query.ParamBase is the body's
// absolute base in the containing statement. Lowering adds the two with checked
// arithmetic and never renumbers operands.
type RecursiveCTE struct {
	Anchor    *SelectStmt
	Term      *SelectStmt
	Operation SetOperation
}

// A JoinKind names a join's flavour.
type JoinKind uint8

const (
	// JoinNone marks the leading FROM entry, which is joined to nothing.
	JoinNone JoinKind = iota
	// JoinInner is an inner equi-join.
	JoinInner
	// JoinLeft is a left outer equi-join.
	JoinLeft
	// JoinRight is a right outer join: the newly joined relation is preserved.
	JoinRight
	// JoinFull preserves unmatched rows from both inputs.
	JoinFull
	// JoinCross forms the unrestricted product and carries no ON condition.
	JoinCross
)

// A RelationKind identifies the row source behind a range variable.
type RelationKind uint8

const (
	// RelationCollection is a physical collection. It is the zero value so
	// every existing TableRef literal remains a collection unless it opts into
	// another relation shape explicitly.
	RelationCollection RelationKind = iota
	// RelationDerived is a SELECT materialized as a relation. [TableRef.Query]
	// holds the nested statement and [TableRef.Alias] is mandatory. [TableRef]
	// records LATERAL independently so ordinary and correlated derived inputs
	// retain one relation kind and one output-schema contract.
	RelationDerived
	// RelationCTE is a reference to a lexically visible common table
	// expression. [TableRef.Query] points at the exact definition body, giving
	// lowerers stable identity without a name lookup or per-reference object.
	RelationCTE
)

// CTEReferenceKind classifies a physical-looking relation reference that may
// instead be an illegal self or forward CTE reference if catalog lookup fails.
// The parser cannot reject it outright: in
//
//	WITH users AS (SELECT * FROM users) SELECT * FROM users
//
// the body legally reads a physical collection named users when one exists.
// A catalog-aware binder resolves [TableRef.Name] physically first, then uses
// this metadata for a positioned undefined-relation diagnostic if it does not.
type CTEReferenceKind uint8

const (
	CTEReferenceNone CTEReferenceKind = iota
	CTEReferenceSelf
	CTEReferenceForward
)

// CTEReferenceMetadata describes a deferred self/forward CTE candidate.
type CTEReferenceMetadata struct {
	Kind CTEReferenceKind
	// DefinitionPos is the byte offset of the matching CTE definition.
	DefinitionPos int
}

// A CorrelationBinding is one stable value slot captured from a lexically
// visible outer relation. Bindings are unique by (Depth, Source, Segments) and
// remain in first-reference order, so a prepared lowerer can compile them once
// and fill the same slots for every outer row.
//
// Depth is one for the immediately containing SELECT, two for its parent, and
// so on. Source indexes that SELECT's From slice. This explicit pair avoids
// flattening nested lexical scopes or relying on aliases after parsing. Pos
// identifies the first path occurrence that requested the binding.
type CorrelationBinding struct {
	Depth    int
	Source   int
	Segments []Segment
	Pos      int
}

// A CorrelationReference maps one exact correlated PathExpr occurrence to its
// zero-based CorrelationBinding slot. Keeping this mapping in a cold sidecar
// leaves ordinary PathExpr nodes unchanged in size and cost.
type CorrelationReference struct {
	Path    *PathExpr
	Binding int
}

// CorrelationSpec is cold metadata for exact outer-path capture. Predicate
// subqueries attach it to SelectStmt.Correlation only when at least one outer
// path was captured. Explicit LATERAL relations attach it to TableRef.Lateral
// even when empty so their authored LATERAL semantics remain observable.
type CorrelationSpec struct {
	Bindings   []CorrelationBinding
	References []CorrelationReference
	// Decorrelated is true only for an explicitly LATERAL derived query that
	// captured no outer paths. Predicate-subquery specs are never empty.
	Decorrelated bool
	// Pos is the byte offset of LATERAL for a derived relation, or the nested
	// query's first token for a predicate subquery.
	Pos int
}

// LateralBinding, LateralReference, and LateralSpec preserve the original API
// while sharing the generalized correlation representation. These are aliases,
// not wrapper types, so existing lowerers incur no conversion or runtime cost.
type LateralBinding = CorrelationBinding
type LateralReference = CorrelationReference
type LateralSpec = CorrelationSpec

// A TableRef is one range variable: a physical collection or derived query,
// the name paths use to qualify against it, and, for a joined relation, its
// equi-join condition.
type TableRef struct {
	// Kind discriminates the relation payload. Its zero value is
	// RelationCollection for source compatibility with physical-table ASTs.
	Kind RelationKind
	// Name is the physical collection or visible CTE name written in the
	// statement. It is empty for RelationDerived; lowerers must switch on Kind
	// rather than treating an empty collection name as meaningful.
	Name string
	// Query is the nested SELECT for RelationDerived, the stable definition
	// identity for RelationCTE, and nil for RelationCollection. A derived query
	// has its own local source scope and parser arena. Only an explicitly
	// LATERAL relation may carry paths bound outside that local scope.
	Query *SelectStmt
	// UnresolvedCTE is non-zero only on a RelationCollection that shares a name
	// with its enclosing CTE or a later sibling. Catalog-aware binding uses it
	// only after physical collection lookup fails.
	UnresolvedCTE CTEReferenceMetadata
	// Alias is the name paths qualify with. It is the explicit AS alias when
	// the statement gave one and Name otherwise. Derived relations always have
	// an explicit, non-empty alias.
	Alias string
	// HasAlias records whether Alias was written explicitly, purely so a
	// diagnostic can echo the statement back accurately.
	HasAlias bool
	// Lateral is non-nil only when the statement explicitly wrote LATERAL. It
	// owns both the stable outer-value slots and every correlated path occurrence
	// that consumes one.
	Lateral *LateralSpec
	// Join is JoinNone for From[0] and the requested join kind for the rest.
	Join JoinKind
	// On is the equi-join condition, or nil for From[0]. Left always binds to
	// a range variable declared before this one and Right always binds to this
	// one, whichever order the statement wrote them in, so the build and probe
	// sides are already sorted out.
	On *JoinCond
	// Pos is the byte offset where the relation begins: the collection name or
	// a derived query's opening parenthesis.
	Pos int
}

// A JoinKeyCond is one path equality extracted from a JOIN condition. Left is
// in the accumulated input and Right is in the newly joined relation.
type JoinKeyCond struct {
	Left  *PathExpr
	Right *PathExpr
	Pos   int
}

// A JoinCond is a JOIN condition and its planner-ready equi-key extraction.
type JoinCond struct {
	// Left and Right mirror Keys[0] for compatibility with consumers of the
	// original single-key AST. They are nil when ON has no extractable key.
	Left  *PathExpr
	Right *PathExpr
	// Keys contains every top-level ANDed equality between the newly joined
	// relation and any earlier range variable. Empty Keys selects the bounded
	// nested-loop path.
	Keys []JoinKeyCond
	// Expr is the complete ON predicate. Keys may narrow candidate formation,
	// but Expr remains authoritative and is evaluated before null extension.
	// It is nil for USING and CROSS JOIN.
	Expr *Expr
	// Using records that the equality came from JOIN ... USING rather than ON.
	// The distinction is semantic: USING contributes one unqualified output
	// column whose value is the coalescing of the two keys, while an equivalent
	// ON equality leaves an unqualified name ambiguous.
	Using bool
	// UsingColumns preserves a composite USING list in source order.
	UsingColumns []string
	// Residual reports whether Expr contains a term beyond the extracted
	// top-level equi keys. It is stable planner and EXPLAIN metadata.
	Residual bool
	Pos      int
}

// An AggKind names a result column's reduction. The constants are in the same
// order as query's aggregate kinds so lowering is a conversion.
type AggKind uint8

const (
	// AggNone projects the value at a path.
	AggNone AggKind = iota
	AggCount
	AggSum
	AggAvg
	AggMin
	AggMax
)

// String answers the aggregate's SQL spelling, or "" for AggNone.
func (a AggKind) String() string {
	switch a {
	case AggCount:
		return "COUNT"
	case AggSum:
		return "SUM"
	case AggAvg:
		return "AVG"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	}
	return ""
}

// A ResultColumn is one entry in the SELECT list.
type ResultColumn struct {
	// Agg is the reduction, or AggNone for a projection.
	Agg AggKind
	// Path is the projected or aggregated path. It is nil only for COUNT(*),
	// which reads no path at all. A Path with no segments is the whole
	// document — what '*' and 'alias.*' parse to.
	Path *PathExpr
	// Window is the analytic expression projected by this column, or nil for
	// an ordinary path or grouped aggregate. Window arguments and sort keys
	// retain their own paths; Path is nil for every window expression so a
	// consumer cannot accidentally lower it as a pre-window projection.
	Window *WindowExpr
	// Scalar is a computed scalar expression. It is nil for the established
	// path/aggregate/window forms, preserving their direct lowering path. A
	// scalar leaf retains its own path or aggregate dependencies; Path, Agg,
	// and Window are nil/zero when Scalar is non-nil so a consumer cannot
	// silently execute only one dependency as the result expression.
	Scalar *ScalarExpr
	// Alias is the explicit AS name, or "" when the statement gave none. It is
	// the output header when set; how an unaliased column is headed is a
	// lowering decision, since the header spelling belongs to the result
	// schema rather than to the syntax.
	Alias string
	// Pos is the byte offset the column started at.
	Pos int
}

// WindowFunctionKind names a supported SQL window function.
type WindowFunctionKind uint8

const (
	WindowRowNumber WindowFunctionKind = iota
	WindowRank
	WindowDenseRank
	WindowLag
	WindowLead
	WindowCount
	WindowSum
	WindowAvg
	WindowMin
	WindowMax
	WindowNTile
	WindowPercentRank
	WindowCumeDist
	WindowFirstValue
	WindowLastValue
	WindowNthValue
)

// String answers the canonical SQL spelling of k.
func (k WindowFunctionKind) String() string {
	switch k {
	case WindowRowNumber:
		return "ROW_NUMBER"
	case WindowRank:
		return "RANK"
	case WindowDenseRank:
		return "DENSE_RANK"
	case WindowLag:
		return "LAG"
	case WindowLead:
		return "LEAD"
	case WindowCount:
		return "COUNT"
	case WindowSum:
		return "SUM"
	case WindowAvg:
		return "AVG"
	case WindowMin:
		return "MIN"
	case WindowMax:
		return "MAX"
	case WindowNTile:
		return "NTILE"
	case WindowPercentRank:
		return "PERCENT_RANK"
	case WindowCumeDist:
		return "CUME_DIST"
	case WindowFirstValue:
		return "FIRST_VALUE"
	case WindowLastValue:
		return "LAST_VALUE"
	case WindowNthValue:
		return "NTH_VALUE"
	default:
		return ""
	}
}

// WindowNullOrder records an explicit NULLS modifier. Default preserves the
// fact that the statement omitted one; lowering applies SQL's direction-based
// default without pretending it was written.
type WindowNullOrder uint8

const (
	WindowNullsDefault WindowNullOrder = iota
	WindowNullsFirst
	WindowNullsLast
)

// WindowOrderTerm is one ORDER BY key inside OVER (...).
type WindowOrderTerm struct {
	Path  *PathExpr
	Desc  bool
	Nulls WindowNullOrder
	Pos   int
}

// WindowFrameBoundKind names one endpoint of an explicit ROWS, GROUPS, or
// RANGE frame. The order is semantic and is also used to reject a start that
// follows its end.
type WindowFrameBoundKind uint8

const (
	WindowUnboundedPreceding WindowFrameBoundKind = iota
	WindowPreceding
	WindowCurrentRow
	WindowFollowing
	WindowUnboundedFollowing
)

// WindowFrameBound is one frame endpoint. Offset is meaningful only for
// PRECEDING/FOLLOWING. ROWS and GROUPS require a non-negative integer;
// RANGE retains an exact non-negative numeric spelling or placeholder.
type WindowFrameBound struct {
	Kind   WindowFrameBoundKind
	Offset Operand
	Pos    int
}

// WindowFrame is the explicit frame attached to an OVER clause. Explicit is
// false when no frame was written. ExclusionExplicit distinguishes an authored
// EXCLUDE NO OTHERS from the identical default behavior.
type WindowFrame struct {
	Unit              WindowFrameUnit
	Start             WindowFrameBound
	End               WindowFrameBound
	Exclusion         WindowFrameExclusion
	Pos               int
	ExclusionPos      int
	Explicit          bool
	ExclusionExplicit bool
}

// WindowFrameUnit selects physical-row, peer-group, or exact order-value
// offsets.
type WindowFrameUnit uint8

const (
	WindowFrameRows WindowFrameUnit = iota
	WindowFrameGroups
	WindowFrameRange
)

// WindowFrameExclusion selects rows removed after frame boundary resolution.
type WindowFrameExclusion uint8

const (
	WindowExcludeNoOthers WindowFrameExclusion = iota
	WindowExcludeCurrentRow
	WindowExcludeGroup
	WindowExcludeTies
)

// WindowSpec is one resolved OVER specification. Name is the named definition
// copied by this specification, if any. PartitionInherited, OrderInherited,
// and FrameInherited record which effective values alias a definition's
// parser-owned operands; they prevent nested-statement position rebasing and
// placeholder accounting from visiting shared operands twice.
type WindowSpec struct {
	Name         string
	NamePos      int
	PartitionBy  []*PathExpr
	PartitionPos int
	OrderBy      []WindowOrderTerm
	OrderPos     int
	Frame        WindowFrame
	Pos          int

	PartitionInherited bool
	OrderInherited     bool
	FrameInherited     bool
}

// NamedWindow is one WINDOW-clause definition. Spec is fully resolved against
// an earlier definition while retaining Spec.Name for AST diagnostics/dumps.
type NamedWindow struct {
	Name string
	Spec WindowSpec
	Pos  int
}

// WindowExpr is one SELECT-list window expression.
type WindowExpr struct {
	Kind WindowFunctionKind
	// Argument is nil for ranking functions and COUNT(*).
	Argument *PathExpr
	// Offset is used by LAG/LEAD and defaults to one when HasOffset is false.
	Offset    Operand
	HasOffset bool
	// Buckets is NTILE's required positive bucket count.
	Buckets    Operand
	HasBuckets bool
	// Nth is NTH_VALUE's required positive one-based position.
	Nth    Operand
	HasNth bool
	// Default is used by LAG/LEAD. DefaultNull distinguishes an explicitly
	// written NULL from an absent third argument.
	Default     Operand
	HasDefault  bool
	DefaultNull bool
	Spec        WindowSpec
	// DirectName is true for OVER name. False with Spec.Name set represents
	// OVER (name ...), whose SQL inheritance restrictions are stricter.
	DirectName bool
	Pos        int
}

// An OrderTerm is one ORDER BY key.
type OrderTerm struct {
	// Path is the sort key. An ORDER BY that named a SELECT alias has already
	// been resolved to the path that alias projects.
	Path *PathExpr
	// Output is the one-based SELECT-list ordinal when ORDER BY names a deferred
	// output alias (for example a window, computed scalar, or aggregate
	// expression) or uses an explicit positional reference such as ORDER BY 2.
	// Zero means Path is authoritative. A one-based encoding keeps the zero
	// value compatible with every existing AST literal.
	Output int
	// Scalar is a computed sort key. It is nil for the established path and
	// output-alias forms. Output remains authoritative when non-zero.
	Scalar *ScalarExpr
	// Desc sorts descending.
	Desc bool
	Pos  int
}

// A PathExpr addresses a value inside one range variable's documents.
//
// Segments is empty for the whole document — the form '*' and 'alias.*' take,
// and the form the engine spells as the empty path spec. See [PathExpr.Spec]
// for the rendering the engine's path compiler consumes, and the package
// documentation for how the leading identifier is decided to be a range
// variable rather than a field.
type PathExpr struct {
	// Source is an index into [SelectStmt.From].
	// A path listed by a containing [CorrelationSpec.References] instead indexes
	// the ancestor From named by that reference's binding.
	Source int
	// MergedUsing is the one-based From index of the USING clause whose merged
	// output this unqualified path names. Zero means an ordinary source path.
	// FULL JOIN needs this identity because COALESCE(left,right) cannot be
	// represented by either qualified input path alone.
	MergedUsing int
	// Segments walks into the document from its root.
	Segments []Segment
	// Pos is the byte offset of the path's first token.
	Pos int
}

// A Segment is one step of a path: an object key, or an array index.
type Segment struct {
	// Key is the object key when IsIndex is false. It is the decoded key, so a
	// quoted identifier's doubled quotes are already collapsed.
	Key string
	// Index is the array subscript when IsIndex is true. It is non-negative;
	// the engine's pointer syntax has no negative subscript.
	Index int
	// IsIndex selects between Key and Index.
	IsIndex bool
}

// An ExprKind tags a predicate node.
type ExprKind uint8

const (
	// ExprCompare is Path <op> Value.
	ExprCompare ExprKind = iota
	// ExprIn is Path IN (List), or NOT IN when Negated.
	ExprIn
	// ExprBetween is Path BETWEEN List[0] AND List[1], or NOT BETWEEN when
	// Negated. It is kept as its own kind rather than expanded into two
	// comparisons here because the expansion is a lowering choice — the engine
	// may one day have a range probe, and a parser that had already thrown the
	// range away could not use it.
	ExprBetween
	// ExprIsNull is Path IS NULL, or IS NOT NULL when Negated. It is true for
	// an absent path as well as an explicit null; see the package
	// documentation on why those are one value here and are not in SQL.
	ExprIsNull
	// ExprIsMissing is Path IS MISSING, or IS NOT MISSING when Negated. It is
	// the existence test: true only when the path resolves to nothing at all,
	// so an explicit null is not missing.
	ExprIsMissing
	// ExprContains is Path @> Value, jsonb-style containment, where Value is
	// an OperandJSON.
	ExprContains
	// ExprLike is Path [NOT] LIKE/ILIKE Value. Value must be a string literal
	// or a placeholder; the query lowerer executes SQL's '%' and '_' pattern
	// operators and the optional backslash escape.
	ExprLike
	// ExprAnd is the conjunction of Kids.
	ExprAnd
	// ExprOr is the disjunction of Kids.
	ExprOr
	// ExprNot is the negation of Kids[0].
	ExprNot
	// ExprExists is EXISTS (SELECT ...). Subquery holds the nested statement.
	ExprExists
	// ExprConstant is a boolean literal used as an ON predicate. WHERE keeps
	// requiring a path-led condition, while JOIN accepts ON TRUE/FALSE to
	// express unrestricted or empty matches with outer semantics.
	ExprConstant
	// ExprScalarCompare compares two computed scalar expressions. ScalarLeft
	// and ScalarRight retain the authored arithmetic trees and SQL NULL
	// propagates to UNKNOWN before the comparison operator is applied.
	ExprScalarCompare
	// ExprScalarIsNull is the two-valued IS [NOT] NULL test over ScalarLeft.
	ExprScalarIsNull
	// ExprScalarTruth reads ScalarLeft as a SQL boolean condition. It is emitted
	// only inside searched CASE: NULL is UNKNOWN, boolean values retain their
	// value, and a dynamic non-boolean value is a runtime datatype mismatch.
	ExprScalarTruth
)

// A CmpOp is a comparison operator. The constants are in the same order as
// query's Op so lowering is a conversion.
type CmpOp uint8

const (
	OpEq CmpOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
)

// String answers the operator's canonical spelling.
func (o CmpOp) String() string {
	switch o {
	case OpEq:
		return "="
	case OpNe:
		return "<>"
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	}
	return "?"
}

// An Expr is one node of a predicate tree.
//
// It is a single tagged struct rather than an interface hierarchy for the same
// reason query's compiledPredicate is: the tree is walked by a switch on the
// tag in every consumer, an interface would put one indirection and one
// dynamic dispatch on each node, and a per-kind concrete type would need a
// per-kind arena to keep parsing allocation-free.
type Expr struct {
	Kind ExprKind
	// Op is meaningful for ExprCompare.
	Op CmpOp
	// Negated carries the NOT of "IS NOT NULL", "NOT IN", "NOT BETWEEN", and
	// "IS NOT MISSING". A leading NOT over a whole predicate is an ExprNot
	// node instead, because that NOT is a boolean operator and this one is
	// part of the leaf's own spelling.
	Negated bool
	// Agg is the reduction applied to Path in a HAVING leaf, and AggNone
	// everywhere else. WHERE and ON never carry one: the engine filters rows
	// before it reduces them, so an aggregate in WHERE has nothing to read.
	Agg AggKind
	// Column is the [SelectStmt.Columns] index a HAVING leaf reads, and -1
	// everywhere else. Binding it here is what makes a parsed HAVING
	// executable in principle: every leaf names a value the reduction already
	// produces, so lowering needs no second aggregation pass.
	Column int
	// Path is the left operand of every path-led leaf kind, and nil for boolean
	// nodes and ExprConstant. It is nil for a COUNT(*) HAVING leaf, matching
	// ResultColumn.
	Path *PathExpr
	// RightPath is the right operand of a comparison between two paths. JOIN ON
	// always admits it; a LATERAL derived query also admits it in WHERE so its
	// local path can compare directly with a captured outer path. Ordinary WHERE
	// and HAVING leaves keep it nil and use Value or Subquery.
	RightPath *PathExpr
	// ScalarLeft and ScalarRight are populated only by scalar predicate kinds.
	// Keeping them cold avoids widening every existing path leaf with an
	// interface value or changing its fast lowering representation.
	ScalarLeft  *ScalarExpr
	ScalarRight *ScalarExpr
	// Value is the right operand of ExprCompare and ExprContains.
	Value Operand
	// Insensitive selects ILIKE rather than LIKE for ExprLike.
	Insensitive bool
	// List holds ExprIn's alternatives, and ExprBetween's two bounds.
	List []Operand
	// Kids holds the operands of ExprAnd and ExprOr, and the single operand of
	// ExprNot. Nested same-kind operands are flattened, so "a AND b AND c" is
	// one node with three children — the shape query's And and Or take, and
	// the shape its OR-to-IN coalescing looks for.
	Kids []*Expr
	// Subquery is the nested SELECT used by ExprExists, by ExprIn instead of
	// List, or by ExprCompare instead of Value. It is nil for every ordinary
	// leaf. Nested statements have their own local source scope; exact outer
	// references are recorded in Subquery.Correlation for proof-backed lowering
	// or a positioned feature refusal.
	Subquery *SelectStmt
	Pos      int
}

// An OperandKind tags a right-hand-side value.
type OperandKind uint8

const (
	// OperandString is a '...' string literal, decoded.
	OperandString OperandKind = iota
	// OperandNumber is a numeric literal, kept as its exact source spelling.
	// It is not converted to float64 or int64 here: the engine compares
	// numbers by exact decimal value, and an integer past float64's mantissa
	// would lose that exactness on the way through a Go float.
	OperandNumber
	// OperandBool is TRUE or FALSE.
	OperandBool
	// OperandJSON is the right-hand side of '@>': one JSON document, captured
	// verbatim from the statement text. It is delimited structurally here and
	// validated when it is lowered, by the same indexer that will match it.
	OperandJSON
	// OperandParam is a '?' placeholder. Ordinal is its 0-based position.
	OperandParam
	// OperandNull is the NULL literal accepted by value-producing clauses such
	// as a declared-column UPDATE assignment. Comparisons deliberately continue
	// to reject NULL in favor of IS NULL.
	OperandNull
)

// An Operand is a literal or a placeholder.
type Operand struct {
	Kind OperandKind
	// Text is the decoded string, the exact numeric spelling, or the verbatim
	// JSON document, by Kind.
	Text string
	// Bool is the value of an OperandBool.
	Bool bool
	// Ordinal is the 0-based placeholder index of an OperandParam.
	Ordinal int
	Pos     int
}

// A SavepointStmt names one transaction overlay mark. SAVEPOINT, RELEASE, and
// ROLLBACK TO share this shape: each carries only the mark name and its byte
// offset in the statement text.
type SavepointStmt struct {
	Name string
	Pos  int
}
