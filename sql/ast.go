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
	// Columns is the SELECT list in source order.
	Columns []ResultColumn

	// Distinct requests one row per distinct projected value tuple. The parser
	// lowers the non-aggregate form to GROUP BY over the projection paths so the
	// existing spill-aware grouping engine supplies equality and bounded memory.
	Distinct bool

	// From holds the range variables. From[0] is the FROM collection; each
	// later entry is a JOIN, in source order, and carries its own ON
	// condition. Every [PathExpr] in the statement names one of these by
	// index, so a lowering pass never has to re-resolve a name.
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
	// JoinRight is a right outer equi-join. The parser normalizes it to a
	// JoinLeft before the AST is returned, so lowerers only need to execute one
	// outer-join orientation.
	JoinRight
)

// A TableRef is one range variable: a collection, the name paths use to
// qualify against it, and, for a joined collection, its equi-join condition.
type TableRef struct {
	// Name is the collection named in the statement.
	Name string
	// Alias is the name paths qualify with. It is the explicit AS alias when
	// the statement gave one and Name otherwise, so resolution has a single
	// key to compare against and never has to consider two.
	Alias string
	// HasAlias records whether Alias was written explicitly, purely so a
	// diagnostic can echo the statement back accurately.
	HasAlias bool
	// Join is JoinNone for From[0] and the requested join kind for the rest.
	Join JoinKind
	// On is the equi-join condition, or nil for From[0]. Left always binds to
	// a range variable declared before this one and Right always binds to this
	// one, whichever order the statement wrote them in, so the build and probe
	// sides are already sorted out.
	On *JoinCond
	// Pos is the byte offset of the collection name.
	Pos int
}

// A JoinCond is an equi-join's single key equality.
//
// It is not an [Expr] because the engine joins on one key equality and nothing
// else. Giving the condition its own type is what makes that a compile-time
// fact for the lowering pass instead of a runtime shape check over a general
// predicate tree that could have held a disjunction.
type JoinCond struct {
	// Left names a key of an earlier range variable, Right of the joined one.
	Left  *PathExpr
	Right *PathExpr
	// Using records that the equality came from JOIN ... USING rather than ON.
	// The distinction is semantic: USING contributes one unqualified output
	// column whose value is the coalescing of the two keys, while an equivalent
	// ON equality leaves an unqualified name ambiguous.
	Using bool
	Pos   int
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
	// Alias is the explicit AS name, or "" when the statement gave none. It is
	// the output header when set; how an unaliased column is headed is a
	// lowering decision, since the header spelling belongs to the result
	// schema rather than to the syntax.
	Alias string
	// Pos is the byte offset the column started at.
	Pos int
}

// An OrderTerm is one ORDER BY key.
type OrderTerm struct {
	// Path is the sort key. An ORDER BY that named a SELECT alias has already
	// been resolved to the path that alias projects.
	Path *PathExpr
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
	Source int
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
	// Path is the left operand of every leaf kind, and nil for the boolean
	// kinds. It is nil for a COUNT(*) HAVING leaf, matching ResultColumn.
	Path *PathExpr
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
	// leaf. Nested statements have their own source scope; correlated
	// references are deliberately rejected until the executor has a
	// parameterized nested-loop plan rather than being guessed at here.
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
