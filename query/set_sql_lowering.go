package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// SetSQLArityError is the positioned runtime-schema counterpart of the
// parser's known-width set check. It is reached only when wildcard expansion
// deferred one side's width until its prepared relation schema was available.
type SetSQLArityError struct {
	Left, Right int
	Pos         int
}

// setSQLUnpositionedTypeError models PostgreSQL set-node errors for which the
// parser does not publish an error cursor (notably a VALUES operand). It keeps
// the 42804 classification through ErrScalarType without exposing Position.
type setSQLUnpositionedTypeError struct {
	operation   string
	left, right ValueType
}

func (e *setSQLUnpositionedTypeError) Error() string {
	return fmt.Sprintf(
		"query: scalar %s does not accept %s and %s: %v",
		e.operation, scalarValueTypeName(e.left), scalarValueTypeName(e.right),
		ErrScalarType,
	)
}

func (*setSQLUnpositionedTypeError) Unwrap() error { return ErrScalarType }

func (e *SetSQLArityError) Error() string {
	return fmt.Sprintf(
		"query: set-operation operands have %d and %d output columns; set compatibility is ordinal: %v",
		e.Left, e.Right, ErrSetTreeArity,
	)
}

func (e *SetSQLArityError) Unwrap() error { return ErrSetTreeArity }

func (e *SetSQLArityError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

// statementSetSQL is allocated only for SelectStmt.Set. It is both the owner
// of the prepared physical tree and a lowering-neutral leaf runner, allowing a
// parenthesized expression with a local tail to become one parent leaf without
// flattening its scope or evaluating its child twice.
type statementSetSQL struct {
	expression *sqlast.SetExpr
	tail       *sqlast.SetTail
	rangeBase  int
	params     int

	descriptor  *setStatementDescriptor
	runtime     setStatementRuntime
	leaves      []setSQLLeaf
	values      []setSQLValues
	groups      []setSQLGroup
	sourceOrder []setSQLSource

	// tailCompiler/tailQuery are absent unless this expression has a scoped
	// ORDER BY/LIMIT/OFFSET. ordinalSpec is immutable backing for both columns
	// and order keys across warmed recompiles.
	tailCompiler compiler
	tailQuery    Query
	ordinalSpec  []string
	specData     []byte
	tailBound    bool
	tailCached   bool
	offset       int
	limit        int
	hasLimit     bool

	ctes        *statementCTEs
	rootArgBase int
	subqueryCap uint8
	// preserveUnknown is set only for a parenthesized set operand whose local
	// tail has LIMIT/OFFSET but no ORDER BY. PostgreSQL leaves a simple unknown
	// output available to the enclosing set operation in that shape.
	preserveUnknown bool
	// preserveDocumentUnknown is the INSERT-source exception: whole-document
	// lineage is discovered only after the prepared relation graph exists, so an
	// unknown placeholder must remain JSON-capable until that post-prepare mark.
	preserveDocumentUnknown bool

	requiresCatalog bool
	directCatalog   bool
	directBlocked   bool
	generalizedJoin bool
	joins           int
	driving         string

	// resolved is PostgreSQL's pairwise common-type result for this query
	// boundary. It is deliberately separate from descriptor.schema: a wholly
	// JSON-represented set keeps VibeDB's established heterogeneous public
	// contract, while a parent set still has to remember that an all-unknown
	// child was finalized to text at this boundary.
	resolved           []setSQLResolvedColumn
	paramTypes         []OutputRepresentation
	paramTypePositions []int
	parameterTypeHints []ParameterType
}

type setSQLLeaf struct {
	tree *sqlast.SelectStmt
	stmt *Statement
}

type setSQLGroup struct {
	expr   *sqlast.SetExpr
	runner *statementSetSQL
}

type setSQLValues struct {
	expr   *sqlast.SetExpr
	runner setSQLValuesRunner
}

type setSQLSourceKind uint8

const (
	setSQLSelectSource setSQLSourceKind = iota
	setSQLValuesSource
	setSQLGroupSource
)

type setSQLSource struct {
	kind      setSQLSourceKind
	index     int
	resolved  []setSQLResolvedColumn
	coercions []setStatementColumnCoercion
}

type setSQLLowered struct {
	node        int
	columns     int
	firstSource int
	schema      []OutputColumn
	resolved    []setSQLResolvedColumn
	sourceStart int
	sourceEnd   int
}

type setSQLResolvedKind uint8

const (
	setSQLUnknownType setSQLResolvedKind = iota
	setSQLBoolType
	setSQLTextType
	setSQLNumberType
	setSQLDynamicType
	setSQLConflictingType
)

type setSQLUnknownMask uint8

const (
	setSQLUnknownNull setSQLUnknownMask = 1 << iota
	setSQLUnknownText
	setSQLUnknownParameter
)

// setSQLResolvedColumn carries semantic type information that OutputColumn
// cannot express. In particular, an unadorned string literal and a parameter
// have PostgreSQL's pseudo-type unknown, while an ordinary JSON string cell is
// concrete text. active records whether a SQL representation participated in
// this subtree; only then do we opt the otherwise-legacy JSON set into SQL
// coercion and transport metadata.
type setSQLResolvedColumn struct {
	kind           setSQLResolvedKind
	unknown        setSQLUnknownMask
	representation OutputRepresentation
	active         bool
}

// markInsertDocumentOutput propagates one selected output ordinal through the
// complete prepared set tree. Every leaf contributes that ordinal, including
// SELECT leaves and grouped runners; VALUES leaves alone own the final binding
// mode. rootArgBase is already the absolute owning-statement base captured at
// preparation, so nested set and CTE boundaries cannot renumber a placeholder.
func (r *statementSetSQL) markInsertDocumentOutput(
	output int,
	positions []int,
	depth int,
) {
	if r == nil || output < 0 || output >= len(r.Columns()) ||
		depth > maxInsertDocumentLineageDepth {
		return
	}
	for i := range r.leaves {
		leaf := &r.leaves[i]
		if leaf.stmt != nil && leaf.tree != nil {
			leaf.stmt.markInsertDocumentOutput(
				output, r.rootArgBase+leaf.tree.ParamBase,
				positions, depth+1,
			)
		}
	}
	for i := range r.values {
		value := &r.values[i]
		value.runner.markDocumentOutput(
			value.expr, output, r.rootArgBase, positions,
		)
	}
	for i := range r.groups {
		if group := r.groups[i].runner; group != nil {
			group.markInsertDocumentOutput(output, positions, depth+1)
		}
	}
}

// resolvesOwnSetSources reports that the prepared set runner routes each leaf
// itself. In particular, a VALUES-only set needs no physical Source at all;
// forcing it through Source.subquerySource would reject a valid CTE/derived
// relation merely because both collection names are empty.
func (s *Statement) resolvesOwnSetSources() bool {
	return s != nil && s.setSQL() != nil
}

// prepareSetSQLStatement replaces the old refusal at the only safe dispatch
// point: before any mirrored first-operand field can enter ordinary lowering.
func prepareSetSQLStatement(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	ctes *statementCTEs,
	argBase int,
	parameterTypes []ParameterType,
	preserveDocumentUnknown bool,
) (*Statement, error) {
	if tree == nil || tree.Set == nil || tree.Set.Root == nil {
		return nil, fmt.Errorf("query: invalid empty SQL set expression: %w", ErrSetTreePlan)
	}
	s := &Statement{
		text: src, tree: tree, params: tree.Params, paramBase: argBase,
		subqueryLimit: subqueryLimit, parameterTypeHints: parameterTypes,
	}
	if err := s.seedParameterTypes(parameterTypes); err != nil {
		return nil, err
	}
	defer func() { s.parameterTypeHints = nil }()
	nested := s.ensureNested()
	if ctes == nil && setSQLHasLexicalCTE(tree.Set.Root) {
		ctes = new(statementCTEs)
		nested.ownsCTEs = true
	}
	nested.ctes = ctes
	runner := &statementSetSQL{
		expression:              tree.Set.Root,
		tail:                    tree.Set.Tail,
		rangeBase:               0,
		params:                  tree.Set.Params,
		ctes:                    ctes,
		rootArgBase:             argBase,
		subqueryCap:             subqueryLimit,
		parameterTypeHints:      parameterTypes,
		preserveDocumentUnknown: preserveDocumentUnknown,
	}
	nested.set = runner
	if err := runner.prepare(src); err != nil {
		s.Release()
		return nil, err
	}
	s.names = append(s.names, runner.Columns()...)
	s.outputs = len(s.names)
	s.requiresCatalog = runner.requiresCatalog
	s.drivingPredicate = nil
	nested.driving = runner.Collection()
	if err := s.prepareParameterTypes(); err != nil {
		s.Release()
		return nil, err
	}
	return s, nil
}

func setSQLHasLexicalCTE(expr *sqlast.SetExpr) bool {
	if expr == nil {
		return false
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		return expr.Select != nil && expr.Select.With != nil
	case sqlast.SetValuesExpr, sqlast.SetTableExpr:
		return false
	case sqlast.SetBinaryExpr:
		return setSQLHasLexicalCTE(expr.Left) || setSQLHasLexicalCTE(expr.Right)
	case sqlast.SetGroupExpr:
		return setSQLHasLexicalCTE(expr.Child)
	default:
		return false
	}
}

func (r *statementSetSQL) prepare(src string) error {
	if r == nil || r.expression == nil || r.params < 0 || r.rangeBase < 0 {
		return fmt.Errorf("query: invalid SQL set-expression descriptor: %w", ErrSetTreePlan)
	}
	defer func() { r.parameterTypeHints = nil }()
	r.paramTypes = nil
	r.paramTypePositions = r.paramTypePositions[:0]
	for local := range r.params {
		absolute := r.rootArgBase + r.rangeBase + local
		if absolute < 0 || absolute >= len(r.parameterTypeHints) {
			continue
		}
		representation := parameterTypeOutputRepresentation(
			r.parameterTypeHints[absolute],
		)
		if representation != OutputJSON {
			if r.paramTypes == nil {
				r.paramTypes = make([]OutputRepresentation, r.params)
			}
			r.paramTypes[local] = representation
		}
	}
	plan := SetTreePlan{}
	prepared, err := r.lowerExpr(src, r.expression, &plan)
	if err != nil {
		return err
	}
	r.resolved = append(r.resolved[:0], prepared.resolved...)
	// A complete query boundary resolves a still-unknown output to text. The
	// upstream exception is an internal set operand with only LIMIT/OFFSET:
	// unlike ORDER BY, neither clause needs to resolve a sortable target type.
	if !r.preserveUnknown && !r.preserveDocumentUnknown {
		for column := range r.resolved {
			if r.resolved[column].kind == setSQLUnknownType {
				if err := r.applySetSQLTarget(
					prepared.sourceStart, prepared.sourceEnd, column,
					OutputSQLText, r.expression.Pos,
				); err != nil {
					return err
				}
				r.resolved[column].kind = setSQLTextType
				r.resolved[column].representation = OutputSQLText
				r.resolved[column].active = true
				prepared.schema[column].Type = TypeString
				prepared.schema[column].Representation = OutputSQLText
			}
		}
	}
	plan.Root = prepared.node
	leaves := make([]setStatementLeaf, len(r.sourceOrder))
	// Sources are assigned in encounter order across both slices below. The
	// descriptor receives the exact runner order recorded by sourceRunner.
	for source := range leaves {
		runner, base := r.sourceRunner(source)
		leaves[source] = setStatementLeaf{
			runner: runner, paramBase: base - r.rangeBase,
			coercions: r.sourceOrder[source].coercions,
		}
	}
	descriptor, err := prepareSetStatementDescriptor(
		plan, leaves, prepared.firstSource, r.params,
	)
	if err != nil {
		return err
	}
	// Output metadata comes from the syntactic first operand, but source
	// acquisition must use the first physical dependency. VALUES has none.
	// SQL type metadata is instead the pairwise common type resolved while the
	// set tree is lowered; column names and ordinals still belong to the first
	// operand.
	if len(prepared.schema) != len(descriptor.schema) {
		return fmt.Errorf(
			"query: resolved set schema has %d columns, want %d: %w",
			len(prepared.schema), len(descriptor.schema), ErrSetTreePlan,
		)
	}
	for column := range descriptor.schema {
		if prepared.schema[column].Representation == OutputJSON {
			continue
		}
		descriptor.schema[column].Type = prepared.schema[column].Type
		descriptor.schema[column].Representation = prepared.schema[column].Representation
	}
	descriptor.driving = r.driving
	r.descriptor = descriptor
	if err := r.runtime.prepare(descriptor); err != nil {
		return err
	}
	if len(r.paramTypes) != 0 {
		r.runtime.sqlOwner = r
	}
	if r.tail != nil {
		r.runtime.consumer = r
		if err := r.prepareTail(); err != nil {
			return err
		}
	} else if r.subqueryCap != 0 {
		r.runtime.cursor.limit = int(r.subqueryCap)
		r.runtime.cursor.hasLimit = true
		r.runtime.cursor.driverLimit = true
	}
	return nil
}

// sourceOrder retains heterogeneous runner order without an interface value in
// prepared storage. The concrete ownership slices make release and truthful
// explain traversal explicit while this compact tag keeps lookup exhaustive.
func (r *statementSetSQL) sourceRunner(source int) (setStatementRunner, int) {
	entry := r.sourceOrder[source]
	switch entry.kind {
	case setSQLSelectSource:
		leaf := &r.leaves[entry.index]
		return leaf.stmt, leaf.tree.ParamBase
	case setSQLValuesSource:
		value := &r.values[entry.index]
		return &value.runner, value.expr.ParamBase
	case setSQLGroupSource:
		group := &r.groups[entry.index]
		return group.runner, group.expr.ParamBase
	default:
		return nil, 0
	}
}

func (r *statementSetSQL) lowerExpr(
	src string,
	expr *sqlast.SetExpr,
	plan *SetTreePlan,
) (setSQLLowered, error) {
	if expr == nil {
		return setSQLLowered{}, fmt.Errorf("query: nil SQL set-expression node: %w", ErrSetTreePlan)
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		if expr.Select == nil || expr.Select.Set != nil {
			return setSQLLowered{}, fmt.Errorf(
				"query: invalid SQL set leaf at byte %d: %w", expr.Pos, ErrSetTreePlan,
			)
		}
		stmt, err := prepareTreeInCorrelationContext(
			src, expr.Select, 0, r.ctes,
			r.rootArgBase+expr.Select.ParamBase, nil,
			r.parameterTypeHints, unknownOutputPrepareMode{
				deferScalar:      !r.preserveDocumentUnknown,
				preserveDocument: r.preserveDocumentUnknown,
			},
		)
		if err != nil {
			return setSQLLowered{}, err
		}
		leafIndex := len(r.leaves)
		r.leaves = append(r.leaves, setSQLLeaf{tree: expr.Select, stmt: stmt})
		source := len(r.sourceOrder)
		schema := stmt.AppendSchema(nil)
		resolved := resolveSetSQLSelect(
			expr.Select, schema, r.parameterTypeHints, r.rootArgBase,
		)
		r.sourceOrder = append(r.sourceOrder, setSQLSource{
			kind: setSQLSelectSource, index: leafIndex, resolved: resolved,
		})
		columns := len(stmt.Columns())
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeLeaf(source, columns))
		r.observeRunner(stmt)
		return setSQLLowered{
			node: node, columns: columns, firstSource: source,
			schema: schema, resolved: resolved,
			sourceStart: source, sourceEnd: source + 1,
		}, nil

	case sqlast.SetTableExpr:
		if expr.Select == nil || expr.Table == nil || expr.Select.Set != nil {
			return setSQLLowered{}, fmt.Errorf(
				"query: invalid TABLE set leaf at byte %d: %w", expr.Pos, ErrSetTreePlan,
			)
		}
		stmt, err := prepareTreeInContext(
			src, expr.Select, 0, r.ctes, r.rootArgBase+expr.ParamBase,
			r.parameterTypeHints,
		)
		if err != nil {
			return setSQLLowered{}, err
		}
		leafIndex := len(r.leaves)
		r.leaves = append(r.leaves, setSQLLeaf{tree: expr.Select, stmt: stmt})
		source := len(r.sourceOrder)
		schema := stmt.AppendSchema(nil)
		resolved := resolveSetSQLConcreteSchema(schema)
		r.sourceOrder = append(r.sourceOrder, setSQLSource{
			kind: setSQLSelectSource, index: leafIndex, resolved: resolved,
		})
		columns := len(stmt.Columns())
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeLeaf(source, columns))
		r.observeRunner(stmt)
		return setSQLLowered{
			node: node, columns: columns, firstSource: source,
			schema: schema, resolved: resolved,
			sourceStart: source, sourceEnd: source + 1,
		}, nil

	case sqlast.SetValuesExpr:
		if expr.Values == nil || expr.First == nil {
			return setSQLLowered{}, fmt.Errorf(
				"query: invalid VALUES set leaf at byte %d: %w", expr.Pos, ErrSetTreePlan,
			)
		}
		valueIndex := len(r.values)
		r.values = append(r.values, setSQLValues{expr: expr})
		value := &r.values[valueIndex]
		if err := value.runner.prepare(expr); err != nil {
			return setSQLLowered{}, err
		}
		source := len(r.sourceOrder)
		schema := value.runner.AppendSchema(nil)
		resolved := resolveSetSQLValues(
			expr, r.parameterTypeHints, r.rootArgBase,
			r.preserveDocumentUnknown,
		)
		r.sourceOrder = append(r.sourceOrder, setSQLSource{
			kind: setSQLValuesSource, index: valueIndex, resolved: resolved,
		})
		columns := len(value.runner.Columns())
		for column := range resolved {
			if !resolved[column].active {
				continue
			}
			target, ok := setSQLRepresentationForColumn(resolved[column])
			if !ok {
				position := setSQLValuesConflictPosition(
					expr, column, r.parameterTypeHints, r.rootArgBase,
				)
				return setSQLLowered{}, setSQLCommonTypeError(
					sqlast.SetUnionDistinct, position,
					value.runner.schema[column].Type,
					setSQLValueTypeForKind(resolved[column].kind),
				)
			}
			if err := r.applySetSQLTarget(
				source, source+1, column, target, expr.Pos,
			); err != nil {
				return setSQLLowered{}, err
			}
			schema[column].Type = setSQLValueTypeForRepresentation(target)
			schema[column].Representation = target
		}
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeLeaf(source, columns))
		r.observeRunner(&value.runner)
		return setSQLLowered{
			node: node, columns: columns, firstSource: source,
			schema: schema, resolved: resolved,
			sourceStart: source, sourceEnd: source + 1,
		}, nil

	case sqlast.SetBinaryExpr:
		left, err := r.lowerExpr(src, expr.Left, plan)
		if err != nil {
			return setSQLLowered{}, err
		}
		right, err := r.lowerExpr(src, expr.Right, plan)
		if err != nil {
			return setSQLLowered{}, err
		}
		if left.columns != right.columns {
			return setSQLLowered{}, &SetSQLArityError{
				Left: left.columns, Right: right.columns, Pos: expr.Pos,
			}
		}
		resolved, err := r.resolveSetSQLBinary(left, right, expr.Operation, expr.Pos)
		if err != nil {
			return setSQLLowered{}, err
		}
		operation, err := setSQLOperation(expr.Operation)
		if err != nil {
			return setSQLLowered{}, err
		}
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeBinary(operation, left.node, right.node))
		return setSQLLowered{
			node: node, columns: left.columns, firstSource: left.firstSource,
			schema: left.schema, resolved: resolved,
			sourceStart: left.sourceStart, sourceEnd: right.sourceEnd,
		}, nil

	case sqlast.SetGroupExpr:
		if expr.Child == nil {
			return setSQLLowered{}, fmt.Errorf(
				"query: empty SQL set group at byte %d: %w", expr.Pos, ErrSetTreePlan,
			)
		}
		if expr.Tail == nil {
			// Parentheses already determined this child's binary shape in the AST.
			// With no local consumer they need no physical unary node or copy.
			return r.lowerExpr(src, expr.Child, plan)
		}
		group := &statementSetSQL{
			expression:              expr.Child,
			tail:                    expr.Tail,
			rangeBase:               expr.ParamBase,
			params:                  expr.Params,
			ctes:                    r.ctes,
			rootArgBase:             r.rootArgBase,
			parameterTypeHints:      r.parameterTypeHints,
			preserveUnknown:         len(expr.Tail.OrderBy) == 0,
			preserveDocumentUnknown: r.preserveDocumentUnknown,
		}
		if err := group.prepare(src); err != nil {
			group.Release()
			return setSQLLowered{}, err
		}
		groupIndex := len(r.groups)
		r.groups = append(r.groups, setSQLGroup{expr: expr, runner: group})
		source := len(r.sourceOrder)
		schema := group.AppendSchema(nil)
		resolved := append([]setSQLResolvedColumn(nil), group.resolved...)
		r.sourceOrder = append(r.sourceOrder, setSQLSource{
			kind: setSQLGroupSource, index: groupIndex, resolved: resolved,
		})
		if err := r.copySetSQLGroupParams(groupIndex, expr.Pos); err != nil {
			return setSQLLowered{}, err
		}
		columns := len(group.Columns())
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeLeaf(source, columns))
		r.observeRunner(group)
		return setSQLLowered{
			node: node, columns: columns, firstSource: source,
			schema: schema, resolved: resolved,
			sourceStart: source, sourceEnd: source + 1,
		}, nil

	default:
		return setSQLLowered{}, fmt.Errorf(
			"query: SQL set node at byte %d has kind %d: %w",
			expr.Pos, expr.Kind, ErrSetTreePlan,
		)
	}
}

func setSQLValuesConflictPosition(
	expr *sqlast.SetExpr,
	column int,
	parameterTypes []ParameterType,
	rootArgBase int,
) int {
	if expr == nil || expr.Values == nil {
		return 0
	}
	first := setSQLUnknownType
	for row := range expr.Values.Rows {
		if column < 0 || column >= len(expr.Values.Rows[row].Values) {
			continue
		}
		value := expr.Values.Rows[row].Values[column]
		candidate := resolveSetSQLValue(value, parameterTypes, rootArgBase)
		if candidate.kind == setSQLUnknownType {
			continue
		}
		if first == setSQLUnknownType {
			first = candidate.kind
			continue
		}
		if candidate.kind != first {
			return value.Pos
		}
	}
	return expr.Pos
}

func resolveSetSQLSelect(
	tree *sqlast.SelectStmt,
	schema []OutputColumn,
	parameterTypes []ParameterType,
	rootArgBase int,
) []setSQLResolvedColumn {
	resolved := resolveSetSQLConcreteSchema(schema)
	if tree == nil || len(tree.Columns) != len(resolved) {
		return resolved
	}
	for column := range tree.Columns {
		expr := tree.Columns[column].Scalar
		if expr == nil {
			continue
		}
		switch expr.Kind {
		case sqlast.ScalarNull:
			resolved[column] = setSQLResolvedColumn{
				kind: setSQLUnknownType, unknown: setSQLUnknownNull,
			}
		case sqlast.ScalarLiteral:
			switch expr.Value.Kind {
			case sqlast.OperandString:
				resolved[column] = setSQLResolvedColumn{
					kind: setSQLUnknownType, unknown: setSQLUnknownText,
				}
			case sqlast.OperandParam:
				absolute := rootArgBase + tree.ParamBase + expr.Value.Ordinal
				resolved[column] = resolveSetSQLParameter(
					parameterTypeAt(parameterTypes, absolute),
				)
			}
		}
	}
	return resolved
}

func resolveSetSQLConcreteSchema(schema []OutputColumn) []setSQLResolvedColumn {
	resolved := make([]setSQLResolvedColumn, len(schema))
	for column := range schema {
		resolved[column] = resolveSetSQLConcreteColumn(schema[column])
	}
	return resolved
}

func resolveSetSQLConcreteColumn(column OutputColumn) setSQLResolvedColumn {
	if column.Representation != OutputJSON {
		kind, ok := setSQLKindForRepresentation(column.Representation)
		if !ok {
			return setSQLResolvedColumn{kind: setSQLDynamicType, active: true}
		}
		return setSQLResolvedColumn{
			kind: kind, representation: column.Representation, active: true,
		}
	}
	switch column.Type {
	case TypeNull:
		return setSQLResolvedColumn{
			kind: setSQLUnknownType, unknown: setSQLUnknownNull,
		}
	case TypeBool:
		return setSQLResolvedColumn{
			kind: setSQLBoolType, representation: OutputSQLBool,
		}
	case TypeString:
		return setSQLResolvedColumn{
			kind: setSQLTextType, representation: OutputSQLText,
		}
	case TypeNumber:
		return setSQLResolvedColumn{
			kind: setSQLNumberType, representation: OutputSQLNumber,
		}
	default:
		return setSQLResolvedColumn{kind: setSQLDynamicType}
	}
}

func resolveSetSQLValues(
	expr *sqlast.SetExpr,
	parameterTypes []ParameterType,
	rootArgBase int,
	preserveDocumentUnknown bool,
) []setSQLResolvedColumn {
	if expr == nil || expr.Values == nil || expr.Columns <= 0 {
		return nil
	}
	resolved := make([]setSQLResolvedColumn, expr.Columns)
	for column := range resolved {
		state := setSQLResolvedColumn{kind: setSQLUnknownType}
		for row := range expr.Values.Rows {
			value := expr.Values.Rows[row].Values[column]
			candidate := resolveSetSQLValue(
				value, parameterTypes, rootArgBase,
			)
			state = mergeSetSQLColumns(state, candidate)
		}
		// VALUES resolves its own column before it becomes a set operand. Even a
		// single all-unknown row therefore has type text at the outer UNION.
		if state.kind == setSQLUnknownType && !preserveDocumentUnknown {
			state.kind = setSQLTextType
			state.representation = OutputSQLText
			state.active = true
		}
		resolved[column] = state
	}
	return resolved
}

func resolveSetSQLValue(
	value sqlast.SetValue,
	parameterTypes []ParameterType,
	rootArgBase int,
) setSQLResolvedColumn {
	if value.Null {
		return setSQLResolvedColumn{
			kind: setSQLUnknownType, unknown: setSQLUnknownNull,
		}
	}
	if value.TypedConstant {
		kind := setSQLConflictingType
		switch value.Cast {
		case sqlast.ScalarCastText:
			kind = setSQLTextType
		case sqlast.ScalarCastBoolean:
			kind = setSQLBoolType
		}
		representation, _ := setSQLRepresentationForKind(kind)
		return setSQLResolvedColumn{
			kind: kind, representation: representation, active: true,
		}
	}
	switch value.Operand.Kind {
	case sqlast.OperandString:
		return setSQLResolvedColumn{
			kind: setSQLUnknownType, unknown: setSQLUnknownText,
		}
	case sqlast.OperandParam:
		return resolveSetSQLParameter(parameterTypeAt(
			parameterTypes, rootArgBase+value.Operand.Ordinal,
		))
	case sqlast.OperandBool:
		return setSQLResolvedColumn{
			kind: setSQLBoolType, representation: OutputSQLBool,
		}
	case sqlast.OperandNumber:
		return setSQLResolvedColumn{
			kind: setSQLNumberType, representation: OutputSQLNumber,
		}
	default:
		return setSQLResolvedColumn{kind: setSQLDynamicType}
	}
}

func parameterTypeAt(parameterTypes []ParameterType, absolute int) ParameterType {
	if absolute < 0 || absolute >= len(parameterTypes) {
		return ParameterTypeUnspecified
	}
	return parameterTypes[absolute]
}

func resolveSetSQLParameter(parameterType ParameterType) setSQLResolvedColumn {
	column := setSQLResolvedColumn{
		kind: setSQLUnknownType, unknown: setSQLUnknownParameter,
	}
	switch parameterType {
	case ParameterTypeBool:
		column.kind, column.representation, column.active =
			setSQLBoolType, OutputSQLBool, true
	case ParameterTypeText, ParameterTypeVarchar,
		ParameterTypeName, ParameterTypeBPChar:
		column.kind, column.representation, column.active =
			setSQLTextType, parameterTypeOutputRepresentation(parameterType), true
	case ParameterTypeOther:
		column.kind, column.active = setSQLDynamicType, true
	}
	return column
}

func mergeSetSQLColumns(left, right setSQLResolvedColumn) setSQLResolvedColumn {
	result := setSQLResolvedColumn{
		kind:    mergeSetSQLKinds(left.kind, right.kind),
		unknown: left.unknown | right.unknown,
		active:  left.active || right.active,
	}
	switch {
	case result.kind == setSQLUnknownType:
		return result
	case result.kind == setSQLConflictingType || result.kind == setSQLDynamicType:
		return result
	case left.kind == setSQLUnknownType:
		result.representation = right.representation
	case right.kind == setSQLUnknownType:
		result.representation = left.representation
	case result.kind == setSQLTextType:
		result.representation = mergeSetSQLStringRepresentations(
			left.representation, right.representation,
		)
	default:
		result.representation = left.representation
		if result.representation == OutputJSON {
			result.representation = right.representation
		}
	}
	return result
}

func mergeSetSQLStringRepresentations(
	left, right OutputRepresentation,
) OutputRepresentation {
	if left == OutputJSON {
		return right
	}
	if right == OutputJSON || left == right {
		return left
	}
	// select_common_type keeps its first candidate unless the candidate can be
	// coerced implicitly to the new type and the reverse direction is not
	// implicit. In PostgreSQL 18's string category that sole asymmetry is
	// varchar/bpchar -> name; all text edges and the remaining character edges
	// are bidirectional, so authored order wins.
	if right == OutputSQLName &&
		(left == OutputSQLVarchar || left == OutputSQLBPChar) {
		return right
	}
	return left
}

func mergeSetSQLKinds(left, right setSQLResolvedKind) setSQLResolvedKind {
	if left == setSQLConflictingType || right == setSQLConflictingType {
		return setSQLConflictingType
	}
	if left == setSQLDynamicType || right == setSQLDynamicType {
		return setSQLDynamicType
	}
	if left == setSQLUnknownType {
		return right
	}
	if right == setSQLUnknownType || left == right {
		return left
	}
	return setSQLConflictingType
}

func (r *statementSetSQL) resolveSetSQLBinary(
	left, right setSQLLowered,
	operation sqlast.SetOperation,
	position int,
) ([]setSQLResolvedColumn, error) {
	if len(left.resolved) != len(right.resolved) {
		return nil, &SetSQLArityError{
			Left: len(left.resolved), Right: len(right.resolved), Pos: position,
		}
	}
	resolved := make([]setSQLResolvedColumn, len(left.resolved))
	for column := range resolved {
		leftColumn, rightColumn := left.resolved[column], right.resolved[column]
		result := mergeSetSQLColumns(leftColumn, rightColumn)
		// A binary node whose direct inputs are both unknown finalizes to text.
		if leftColumn.kind == setSQLUnknownType && rightColumn.kind == setSQLUnknownType &&
			!r.preserveDocumentUnknown {
			result.kind = setSQLTextType
			result.representation = OutputSQLText
			result.active = true
		}
		if !result.active {
			resolved[column] = result
			continue
		}
		if result.kind == setSQLConflictingType || result.kind == setSQLDynamicType {
			position = r.setSQLSourceColumnPosition(right, column, position)
			return nil, setSQLCommonTypeError(
				operation, position,
				setSQLValueTypeForKind(leftColumn.kind),
				setSQLValueTypeForKind(rightColumn.kind),
			)
		}
		target, ok := setSQLRepresentationForColumn(result)
		if !ok {
			return nil, setSQLCommonTypeError(
				operation, position,
				setSQLValueTypeForKind(leftColumn.kind),
				setSQLValueTypeForKind(rightColumn.kind),
			)
		}
		if target == OutputSQLNumber &&
			result.unknown&(setSQLUnknownText|setSQLUnknownParameter) != 0 {
			// Numeric typinput is outside this bounded BOOL/TEXT common-type
			// slice. Preserve the previous explicit mismatch instead of silently
			// publishing SQL-number metadata over unconverted cells.
			return nil, setSQLCommonTypeError(
				operation, position, TypeNumber, TypeString,
			)
		}
		if err := r.applySetSQLTarget(
			left.sourceStart, left.sourceEnd, column, target, position,
		); err != nil {
			return nil, err
		}
		if err := r.applySetSQLTarget(
			right.sourceStart, right.sourceEnd, column, target, position,
		); err != nil {
			return nil, err
		}
		valueType := setSQLValueTypeForKind(result.kind)
		left.schema[column].Type = valueType
		left.schema[column].Representation = target
		resolved[column] = result
	}
	return resolved, nil
}

func (r *statementSetSQL) setSQLSourceColumnPosition(
	source setSQLLowered,
	column int,
	fallback int,
) int {
	if r == nil || source.sourceStart < 0 || source.sourceStart >= source.sourceEnd ||
		source.sourceStart >= len(r.sourceOrder) {
		return fallback
	}
	entry := r.sourceOrder[source.sourceStart]
	switch entry.kind {
	case setSQLSelectSource:
		if entry.index >= 0 && entry.index < len(r.leaves) {
			tree := r.leaves[entry.index].tree
			if tree != nil && column >= 0 && column < len(tree.Columns) {
				output := &tree.Columns[column]
				expr := output.Scalar
				if expr == nil {
					return output.Pos
				}
				if expr.Kind == sqlast.ScalarCast && expr.TypedConstant {
					return expr.TargetPos
				}
				if expr.Kind == sqlast.ScalarLiteral &&
					expr.Value.Kind == sqlast.OperandParam {
					return expr.Value.Pos
				}
				return expr.Pos
			}
		}
	case setSQLValuesSource:
		// PostgreSQL's set-operation transform has no expression location for a
		// VALUES RTE, so ErrorResponse deliberately omits P.
		return -1
	case setSQLGroupSource:
		if entry.index >= 0 && entry.index < len(r.groups) {
			group := r.groups[entry.index].runner
			if group != nil {
				return group.setSQLSourceColumnPosition(setSQLLowered{
					sourceStart: 0, sourceEnd: len(group.sourceOrder),
				}, column, fallback)
			}
		}
	}
	return fallback
}

func setSQLKindForRepresentation(
	representation OutputRepresentation,
) (setSQLResolvedKind, bool) {
	switch representation {
	case OutputSQLBool:
		return setSQLBoolType, true
	case OutputSQLText, OutputSQLVarchar, OutputSQLName, OutputSQLBPChar:
		return setSQLTextType, true
	case OutputSQLNumber:
		return setSQLNumberType, true
	default:
		return setSQLDynamicType, false
	}
}

func setSQLRepresentationForColumn(
	column setSQLResolvedColumn,
) (OutputRepresentation, bool) {
	if column.representation != OutputJSON {
		kind, ok := setSQLKindForRepresentation(column.representation)
		if ok && kind == column.kind {
			return column.representation, true
		}
	}
	return setSQLRepresentationForKind(column.kind)
}

func setSQLRepresentationForKind(
	kind setSQLResolvedKind,
) (OutputRepresentation, bool) {
	switch kind {
	case setSQLBoolType:
		return OutputSQLBool, true
	case setSQLTextType:
		return OutputSQLText, true
	case setSQLNumberType:
		return OutputSQLNumber, true
	default:
		return OutputJSON, false
	}
}

func setSQLValueTypeForKind(kind setSQLResolvedKind) ValueType {
	switch kind {
	case setSQLBoolType:
		return TypeBool
	case setSQLTextType:
		return TypeString
	case setSQLNumberType:
		return TypeNumber
	case setSQLUnknownType:
		return TypeNull
	default:
		return TypeAny
	}
}

func (r *statementSetSQL) applySetSQLTarget(
	start, end, column int,
	target OutputRepresentation,
	position int,
) error {
	if start < 0 || end < start || end > len(r.sourceOrder) {
		return fmt.Errorf("query: invalid set common-type source range [%d,%d): %w",
			start, end, ErrSetTreePlan)
	}
	for source := start; source < end; source++ {
		entry := &r.sourceOrder[source]
		if column < 0 || column >= len(entry.resolved) {
			return fmt.Errorf("query: set common-type column %d is outside source %d: %w",
				column, source, ErrSetTreePlan)
		}
		unknown := entry.resolved[column].unknown
		if unknown == 0 || unknown == setSQLUnknownNull {
			continue
		}
		switch entry.kind {
		case setSQLSelectSource:
			if err := r.applySetSQLSelectTarget(source, column, target, position); err != nil {
				return err
			}
		case setSQLValuesSource:
			value := &r.values[entry.index]
			if err := value.runner.applySetCommonColumn(value.expr, column, target); err != nil {
				return err
			}
			if err := r.markSetSQLValuesParams(value.expr, column, target); err != nil {
				return err
			}
		case setSQLGroupSource:
			group := r.groups[entry.index].runner
			if err := group.applySetOutputTarget(column, target, position); err != nil {
				return err
			}
			if err := r.copySetSQLGroupParams(entry.index, position); err != nil {
				return err
			}
		default:
			return fmt.Errorf("query: invalid set source kind %d: %w", entry.kind, ErrSetTreePlan)
		}
	}
	return nil
}

func (r *statementSetSQL) copySetSQLGroupParams(
	groupIndex int,
	position int,
) error {
	if groupIndex < 0 || groupIndex >= len(r.groups) ||
		r.groups[groupIndex].runner == nil {
		return fmt.Errorf("query: invalid grouped set parameter source %d: %w",
			groupIndex, ErrSetTreePlan)
	}
	group := r.groups[groupIndex].runner
	base := r.groups[groupIndex].expr.ParamBase - r.rangeBase
	for parameter := range group.paramTypes {
		if group.paramTypes[parameter] == OutputJSON {
			continue
		}
		parameterPosition := group.paramTypePosition(parameter)
		if parameterPosition < 0 {
			parameterPosition = position
		}
		if err := r.markSetSQLParam(
			base+parameter, group.paramTypes[parameter], parameterPosition,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *statementSetSQL) applySetSQLSelectTarget(
	source, column int,
	target OutputRepresentation,
	position int,
) error {
	entry := &r.sourceOrder[source]
	leaf := &r.leaves[entry.index]
	if leaf.tree == nil || column >= len(leaf.tree.Columns) {
		return fmt.Errorf("query: missing SELECT column for set common type: %w", ErrSetTreePlan)
	}
	expr := leaf.tree.Columns[column].Scalar
	if expr == nil {
		return fmt.Errorf("query: dynamic SELECT output cannot acquire a set common type: %w", ErrSetTreePlan)
	}
	coerce := false
	coercionPos := expr.Pos
	switch expr.Kind {
	case sqlast.ScalarNull:
		return nil
	case sqlast.ScalarLiteral:
		switch expr.Value.Kind {
		case sqlast.OperandString:
			coerce = true
			if target == OutputSQLBool {
				_, err := castScalarBoolean(expr.Value.Pos, statementScalarValue{
					value: scalar{kind: kindString, sval: expr.Value.Text},
				})
				if err != nil {
					return err
				}
			}
		case sqlast.OperandParam:
			coerce = true
			parameter := leaf.tree.ParamBase - r.rangeBase + expr.Value.Ordinal
			if err := r.markSetSQLParam(parameter, target, expr.Value.Pos); err != nil {
				return err
			}
		default:
			return nil
		}
	default:
		return nil
	}
	if !coerce {
		return nil
	}
	if len(entry.coercions) == 0 {
		entry.coercions = make([]setStatementColumnCoercion, len(entry.resolved))
	}
	coercion := &entry.coercions[column]
	if coercion.target != OutputJSON && coercion.target != target &&
		!(setSQLRepresentationIsString(coercion.target) &&
			setSQLRepresentationIsString(target)) {
		return setSQLCommonTypeError(
			sqlast.SetUnionDistinct, position,
			setSQLValueTypeForRepresentation(coercion.target),
			setSQLValueTypeForRepresentation(target),
		)
	}
	*coercion = setStatementColumnCoercion{target: target, pos: coercionPos}
	if r.descriptor != nil && source < len(r.descriptor.leaves) {
		r.descriptor.leaves[source].coercions = entry.coercions
	}
	return nil
}

func (r *statementSetSQL) markSetSQLValuesParams(
	expr *sqlast.SetExpr,
	column int,
	target OutputRepresentation,
) error {
	for row := range expr.Values.Rows {
		value := expr.Values.Rows[row].Values[column]
		if value.Null || value.TypedConstant || value.Operand.Kind != sqlast.OperandParam {
			continue
		}
		if err := r.markSetSQLParam(
			value.Operand.Ordinal-r.rangeBase, target, value.Pos,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *statementSetSQL) markSetSQLParam(
	parameter int,
	target OutputRepresentation,
	position int,
) error {
	if parameter < 0 || parameter >= r.params {
		return fmt.Errorf("query: inferred set parameter %d is outside %d parameters: %w",
			parameter, r.params, ErrSetTreePlan)
	}
	if r.paramTypes == nil {
		r.paramTypes = make([]OutputRepresentation, r.params)
	}
	previous := r.paramTypes[parameter]
	compatibleStrings := setSQLRepresentationIsString(previous) &&
		setSQLRepresentationIsString(target)
	if previous != OutputJSON && previous != target && !compatibleStrings {
		return &ScalarTypeError{
			Pos: position, Operation: "parameter common type",
			Left:  setSQLValueTypeForRepresentation(previous),
			Right: setSQLValueTypeForRepresentation(target),
		}
	}
	if previous == OutputJSON || compatibleStrings {
		r.paramTypes[parameter] = target
	}
	if position >= 0 {
		if len(r.paramTypePositions) == 0 {
			r.paramTypePositions = make([]int, r.params)
		}
		encoded := position + 1
		if r.paramTypePositions[parameter] == 0 ||
			encoded < r.paramTypePositions[parameter] {
			r.paramTypePositions[parameter] = encoded
		}
	}
	return nil
}

func setSQLValueTypeForRepresentation(representation OutputRepresentation) ValueType {
	kind, ok := setSQLKindForRepresentation(representation)
	if !ok {
		return TypeAny
	}
	return setSQLValueTypeForKind(kind)
}

func (r *statementSetSQL) applySetOutputTarget(
	column int,
	target OutputRepresentation,
	position int,
) error {
	if r == nil || column < 0 || column >= len(r.resolved) {
		return fmt.Errorf("query: invalid grouped set output %d: %w", column, ErrSetTreePlan)
	}
	resolvedTarget, ok := setSQLRepresentationForColumn(r.resolved[column])
	compatibleStrings := r.resolved[column].kind == setSQLTextType &&
		setSQLRepresentationIsString(resolvedTarget) &&
		setSQLRepresentationIsString(target)
	if r.resolved[column].kind != setSQLUnknownType &&
		(!ok || resolvedTarget != target) && !compatibleStrings {
		return &ScalarTypeError{
			Pos: position, Operation: "grouped set common type",
			Left:  setSQLValueTypeForKind(r.resolved[column].kind),
			Right: setSQLValueTypeForRepresentation(target),
		}
	}
	if err := r.applySetSQLTarget(0, len(r.sourceOrder), column, target, position); err != nil {
		return err
	}
	if r.resolved[column].kind == setSQLUnknownType {
		kind, targetOK := setSQLKindForRepresentation(target)
		if !targetOK {
			return fmt.Errorf("query: invalid grouped set target %d: %w", target, ErrSetTreePlan)
		}
		r.resolved[column].kind = kind
	}
	r.resolved[column].representation = target
	r.resolved[column].active = true
	if r.descriptor != nil && column < len(r.descriptor.schema) {
		r.descriptor.schema[column].Type = setSQLValueTypeForRepresentation(target)
		r.descriptor.schema[column].Representation = target
	}
	return nil
}

func setSQLRepresentationIsString(representation OutputRepresentation) bool {
	switch representation {
	case OutputSQLText, OutputSQLVarchar, OutputSQLName, OutputSQLBPChar:
		return true
	default:
		return false
	}
}

func (r *statementSetSQL) paramRepresentation(index int) OutputRepresentation {
	if r == nil || index < 0 || index >= len(r.paramTypes) {
		return OutputJSON
	}
	return r.paramTypes[index]
}

func (r *statementSetSQL) paramTypePosition(index int) int {
	if r == nil || index < 0 || index >= len(r.paramTypePositions) ||
		r.paramTypePositions[index] == 0 {
		return -1
	}
	return r.paramTypePositions[index] - 1
}

func setSQLCommonTypeError(
	operation sqlast.SetOperation,
	position int,
	left, right ValueType,
) error {
	if position < 0 {
		return &setSQLUnpositionedTypeError{
			operation: setSQLCommonTypeOperation(operation),
			left:      left, right: right,
		}
	}
	return &ScalarTypeError{
		Pos: position, Operation: setSQLCommonTypeOperation(operation),
		Left: left, Right: right,
	}
}

func setSQLCommonTypeOperation(operation sqlast.SetOperation) string {
	switch operation {
	case sqlast.SetUnionAll, sqlast.SetUnionDistinct:
		return "UNION common type"
	case sqlast.SetIntersectAll, sqlast.SetIntersectDistinct:
		return "INTERSECT common type"
	case sqlast.SetExceptAll, sqlast.SetExceptDistinct:
		return "EXCEPT common type"
	default:
		return "set-operation common type"
	}
}

func setSQLOperation(operation sqlast.SetOperation) (SetTreeOperation, error) {
	switch operation {
	case sqlast.SetUnionAll:
		return SetTreeUnionAll, nil
	case sqlast.SetUnionDistinct:
		return SetTreeUnionDistinct, nil
	case sqlast.SetIntersectAll:
		return SetTreeIntersectAll, nil
	case sqlast.SetIntersectDistinct:
		return SetTreeIntersectDistinct, nil
	case sqlast.SetExceptAll:
		return SetTreeExceptAll, nil
	case sqlast.SetExceptDistinct:
		return SetTreeExceptDistinct, nil
	default:
		return 0, fmt.Errorf(
			"query: SQL set operation %d cannot be lowered: %w",
			operation, ErrSetTreePlan,
		)
	}
}

func (r *statementSetSQL) observeRunner(runner setStatementRunner) {
	collection := runner.Collection()
	if collection != "" && r.driving == "" {
		r.driving = collection
	} else if collection != "" && collection != r.driving {
		r.requiresCatalog = true
	}
	runnerCatalog := runnerRequiresCatalog(runner)
	r.requiresCatalog = r.requiresCatalog || runnerCatalog
	r.joins += runnerNumJoins(runner)
	r.generalizedJoin = r.generalizedJoin || runnerGeneralizedJoin(runner)
	// A set can route multiple ordinary single-collection leaves from one
	// durable catalog even though none of those leaves requires a catalog by
	// itself. A leaf that does require one is different: it must itself be a
	// direct durable consumer. In particular, a legacy projected join requires
	// coherence but still needs the driver's heap adapter; allowing a surrounding
	// set to erase that fact routes fan-out into the semi-join-only file backend.
	r.directBlocked = r.directBlocked ||
		runnerCatalog && !runnerUsesDirectCatalog(runner)
	r.directCatalog = (r.requiresCatalog || r.generalizedJoin) && !r.directBlocked
}

func runnerRequiresCatalog(runner setStatementRunner) bool {
	switch v := runner.(type) {
	case *Statement:
		return v.RequiresCatalog()
	case *statementSetSQL:
		return v.requiresCatalog
	default:
		return false
	}
}

func runnerNumJoins(runner setStatementRunner) int {
	switch v := runner.(type) {
	case *Statement:
		return v.NumJoins()
	case *statementSetSQL:
		return v.joins
	default:
		return 0
	}
}

func runnerUsesDirectCatalog(runner setStatementRunner) bool {
	switch v := runner.(type) {
	case *Statement:
		return v.UsesDirectCatalogExecution()
	case *statementSetSQL:
		return v.directCatalog
	default:
		return false
	}
}

func runnerGeneralizedJoin(runner setStatementRunner) bool {
	switch v := runner.(type) {
	case *Statement:
		return v.UsesGeneralizedRelationJoin()
	case *statementSetSQL:
		return v.generalizedJoin
	default:
		return false
	}
}

func (r *statementSetSQL) prepareTail() error {
	columns := len(r.descriptor.names)
	r.ordinalSpec = make([]string, columns)
	for column := 0; column < columns; column++ {
		start := len(r.specData)
		r.specData = strconv.AppendInt(r.specData, int64(column), 10)
		r.ordinalSpec[column] = byteview.String(
			r.specData[start:len(r.specData):len(r.specData)],
		)
	}
	standins := make([]any, r.params)
	for i := range standins {
		standins[i] = int64(0)
	}
	if err := r.bindTail(standins); err != nil {
		return err
	}
	r.tailCached = r.tail.Params == 0
	return nil
}

func (r *statementSetSQL) bindTail(args []any) error {
	if r.tail == nil {
		return nil
	}
	if len(args) != r.params {
		return fmt.Errorf(
			"query: set tail has %d placeholder(s) and %d argument(s) were bound",
			r.params, len(args),
		)
	}
	if r.tailCached && r.tailBound {
		return nil
	}
	offset, limit, hasLimit := 0, 0, false
	if r.tail.Offset != nil {
		value, err := r.tailCount(*r.tail.Offset, args, "OFFSET")
		if err != nil {
			return err
		}
		offset = value
	}
	if r.tail.Limit != nil {
		value, err := r.tailCount(*r.tail.Limit, args, "LIMIT")
		if err != nil {
			return err
		}
		limit, hasLimit = value, true
	}
	if r.subqueryCap != 0 && (!hasLimit || limit > int(r.subqueryCap)) {
		limit, hasLimit = int(r.subqueryCap), true
	}

	c := &r.tailCompiler
	c.rewind()
	c.prepare(&r.tailQuery)
	for column, spec := range r.ordinalSpec {
		r.tailQuery.columns = append(r.tailQuery.columns, Column{
			spec: spec, header: r.descriptor.names[column],
		})
	}
	for i := range r.tail.OrderBy {
		term := &r.tail.OrderBy[i]
		ordinal := term.Output - 1
		if term.Output == 0 {
			ordinal = -1
			matches := 0
			for candidate := range r.descriptor.names {
				if r.descriptor.names[candidate] == term.Name {
					ordinal = candidate
					matches++
				}
			}
			if matches != 1 {
				return &RelationColumnError{
					Relation: "set expression", Column: term.Name,
					Matches: matches, Pos: term.Pos,
				}
			}
		}
		if ordinal < 0 || ordinal >= len(r.ordinalSpec) {
			return &RelationColumnError{
				Relation: "set expression", Column: term.Name,
				Matches: 0, Pos: term.Pos,
			}
		}
		direction := sqlOrderDirection(term.Desc, term.Nulls)
		r.tailQuery.orderBy = append(r.tailQuery.orderBy, orderSpec{
			path: r.ordinalSpec[ordinal], dir: direction,
		})
	}
	if hasLimit {
		bound := offset + limit
		if bound < offset {
			bound = math.MaxInt
		}
		r.tailQuery.limit, r.tailQuery.hasLimit = bound, true
	}
	p, err := c.compilePlan(&r.tailQuery)
	if err != nil {
		_ = c.fail(&r.tailQuery, err)
		return err
	}
	r.tailQuery.built = c.outcome(p, nil)
	r.offset, r.limit, r.hasLimit = offset, limit, hasLimit
	r.runtime.cursor.offset = offset
	r.runtime.cursor.limit = limit
	r.runtime.cursor.hasLimit = hasLimit
	r.runtime.cursor.driverLimit = false
	r.tailBound = true
	return nil
}

func (r *statementSetSQL) tailCount(
	operand sqlast.Operand,
	args []any,
	clause string,
) (int, error) {
	if operand.Kind == sqlast.OperandParam {
		ordinal := operand.Ordinal - r.rangeBase
		if ordinal < 0 || ordinal >= len(args) {
			return 0, fmt.Errorf("query: invalid %s placeholder range", clause)
		}
		operand.Ordinal = ordinal
	}
	var helper Statement
	return helper.count(operand, args, clause)
}

func (r *statementSetSQL) consumePreparedSetResult(
	runtime *setStatementRuntime,
	result SetTreeResult,
	cancel *CancelFlag,
) error {
	if err := r.bindTail(runtime.args); err != nil {
		return err
	}
	if err := cancellationError(cancel); err != nil {
		return err
	}
	stats := runtime.parent.Stats
	if err := r.tailQuery.RunInto(
		runtime.parent, fromRelationSpool(result.relation),
	); err != nil {
		return err
	}
	mergeSetStatementStats(&runtime.parent.Stats, stats)
	return cancellationError(cancel)
}

func (r *statementSetSQL) Columns() []string {
	if r == nil || r.descriptor == nil {
		return nil
	}
	return r.descriptor.Columns()
}

func (r *statementSetSQL) NumParams() int {
	if r == nil {
		return 0
	}
	return r.params
}

func (r *statementSetSQL) Collection() string {
	if r == nil || r.descriptor == nil {
		return ""
	}
	return r.descriptor.driving
}

func (r *statementSetSQL) AppendSchema(dst []OutputColumn) []OutputColumn {
	if r == nil || r.descriptor == nil {
		return dst
	}
	return r.descriptor.AppendSchema(dst)
}

// A grouped SQL set runner resolves its own heterogeneous leaves from the
// caller's statement-wide source. Treating it as one physical leaf would bind
// the group's empty/independent or distinct driving name twice.
func (*statementSetSQL) setStatementSourceIndependent() {}

func (r *statementSetSQL) runIntoFrame(
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
	intermediateResource string,
) (Cursor, error) {
	if intermediateResource == "" {
		return r.runtime.runIntoFrame(parent, src, args, frame)
	}
	// A grouped set runner is an internal relation-valued child. Public result
	// limits apply only after the outer expression publishes its final root;
	// this child is instead bounded by the exact remainder of the one shared
	// statement frame, matching Statement.runIntoFrame's nested contract.
	parent.Options.ResultRows = -1
	remaining := frame.intermediate.remaining()
	if remaining == 0 {
		return Cursor{}, &IntermediateBudgetError{
			Resource: intermediateResource,
			Bytes:    saturatedBytes(frame.intermediate.used, 1),
			Limit:    frame.intermediate.limit,
		}
	}
	parent.Options.ResultBytes = remaining
	cursor, err := r.runtime.runIntoFrame(parent, src, args, frame)
	if err != nil {
		err = translateSetIntermediateError(
			err, frame, remaining, intermediateResource,
		)
	}
	return cursor, err
}

func (r *statementSetSQL) releaseRelations(*statementFrame) {}

func (r *statementSetSQL) Release() {
	if r == nil {
		return
	}
	r.runtime.Release()
	for i := range r.leaves {
		r.leaves[i].stmt.Release()
	}
	for i := range r.values {
		r.values[i].runner.Release()
	}
	for i := range r.groups {
		r.groups[i].runner.Release()
	}
	r.tailCompiler.release()
	*r = statementSetSQL{}
}

func (r *statementSetSQL) bindForExplain(args []any) error {
	if r == nil || len(args) != r.params {
		return queryExplainError("query: explain argument count does not match set expression")
	}
	for source := range r.sourceOrder {
		runner, base := r.sourceRunner(source)
		local := base - r.rangeBase
		count := runner.NumParams()
		if local < 0 || count < 0 || local > len(args) || count > len(args)-local {
			return fmt.Errorf("query: invalid set leaf explain placeholder range")
		}
		switch leaf := runner.(type) {
		case *Statement:
			if err := leaf.bind(args[local : local+count]); err != nil {
				return err
			}
			if err := leaf.bindFusedExplain(args[local : local+count]); err != nil {
				return err
			}
		case *statementSetSQL:
			if err := leaf.bindForExplain(args[local : local+count]); err != nil {
				return err
			}
		case *setSQLValuesRunner:
			if err := leaf.bindForExplain(args[local : local+count]); err != nil {
				return err
			}
		}
	}
	return r.bindTail(args)
}

func (s *Statement) setSQL() *statementSetSQL {
	if s == nil || s.nested == nil {
		return nil
	}
	return s.nested.set
}

// UsesDirectCatalogExecution reports that the prepared relation pipeline can
// consume a coherent durable catalog without the driver's heap fallback. The
// ordinary point and legacy projected-join paths remain unchanged.
func (s *Statement) UsesDirectCatalogExecution() bool {
	return s.catalogCapabilities(0).direct
}

func translateSetIntermediateError(
	err error,
	frame *statementFrame,
	limit int64,
	resource string,
) error {
	var resultErr *ResultBudgetError
	if errors.As(err, &resultErr) && resultErr.ByteLimit == limit {
		return &IntermediateBudgetError{
			Resource: resource,
			Bytes:    saturatedBytes(frame.intermediate.used, resultErr.Bytes),
			Limit:    frame.intermediate.limit,
		}
	}
	return err
}
