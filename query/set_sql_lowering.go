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
	groups      []setSQLGroup
	sourceOrder []int

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

	requiresCatalog bool
	directCatalog   bool
	generalizedJoin bool
	joins           int
	driving         string
}

type setSQLLeaf struct {
	tree *sqlast.SelectStmt
	stmt *Statement
}

type setSQLGroup struct {
	expr   *sqlast.SetExpr
	runner *statementSetSQL
}

type setSQLLowered struct {
	node        int
	columns     int
	firstSource int
}

// prepareSetSQLStatement replaces the old refusal at the only safe dispatch
// point: before any mirrored first-operand field can enter ordinary lowering.
func prepareSetSQLStatement(
	src string,
	tree *sqlast.SelectStmt,
	subqueryLimit uint8,
	ctes *statementCTEs,
	argBase int,
) (*Statement, error) {
	if tree == nil || tree.Set == nil || tree.Set.Root == nil {
		return nil, fmt.Errorf("query: invalid empty SQL set expression: %w", ErrSetTreePlan)
	}
	s := &Statement{
		text: src, tree: tree, params: tree.Params,
		subqueryLimit: subqueryLimit,
	}
	nested := s.ensureNested()
	if ctes == nil && setSQLHasLexicalCTE(tree.Set.Root) {
		ctes = new(statementCTEs)
		nested.ownsCTEs = true
	}
	nested.ctes = ctes
	runner := &statementSetSQL{
		expression:  tree.Set.Root,
		tail:        tree.Set.Tail,
		rangeBase:   0,
		params:      tree.Set.Params,
		ctes:        ctes,
		rootArgBase: argBase,
		subqueryCap: subqueryLimit,
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
	return s, nil
}

func setSQLHasLexicalCTE(expr *sqlast.SetExpr) bool {
	if expr == nil {
		return false
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		return expr.Select != nil && expr.Select.With != nil
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
	plan := SetTreePlan{}
	prepared, err := r.lowerExpr(src, r.expression, &plan)
	if err != nil {
		return err
	}
	plan.Root = prepared.node
	leaves := make([]setStatementLeaf, len(r.leaves)+len(r.groups))
	// Sources are assigned in encounter order across both slices below. The
	// descriptor receives the exact runner order recorded by sourceRunner.
	for source := range leaves {
		runner, base := r.sourceRunner(source)
		leaves[source] = setStatementLeaf{runner: runner, paramBase: base - r.rangeBase}
	}
	descriptor, err := prepareSetStatementDescriptor(
		plan, leaves, prepared.firstSource, r.params,
	)
	if err != nil {
		return err
	}
	r.descriptor = descriptor
	if err := r.runtime.prepare(descriptor); err != nil {
		return err
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

// sourceOrder retains the mixed leaf/group runner order without an interface
// allocation per entry: non-negative indexes address leaves, negative indexes
// encode groups as -index-1.
//
// It is declared separately from the two ownership slices because those slices
// make recursive release and truthful explain traversal concrete and cheap.
func (r *statementSetSQL) sourceRunner(source int) (setStatementRunner, int) {
	entry := r.sourceOrder[source]
	if entry >= 0 {
		leaf := &r.leaves[entry]
		return leaf.stmt, leaf.tree.ParamBase
	}
	group := &r.groups[-entry-1]
	return group.runner, group.expr.ParamBase
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
		stmt, err := prepareTreeInContext(
			src, expr.Select, 0, r.ctes, r.rootArgBase+expr.Select.ParamBase,
		)
		if err != nil {
			return setSQLLowered{}, err
		}
		leafIndex := len(r.leaves)
		r.leaves = append(r.leaves, setSQLLeaf{tree: expr.Select, stmt: stmt})
		source := len(r.sourceOrder)
		r.sourceOrder = append(r.sourceOrder, leafIndex)
		columns := len(stmt.Columns())
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeLeaf(source, columns))
		r.observeRunner(stmt)
		return setSQLLowered{node: node, columns: columns, firstSource: source}, nil

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
		operation, err := setSQLOperation(expr.Operation)
		if err != nil {
			return setSQLLowered{}, err
		}
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeBinary(operation, left.node, right.node))
		return setSQLLowered{
			node: node, columns: left.columns, firstSource: left.firstSource,
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
			expression:  expr.Child,
			tail:        expr.Tail,
			rangeBase:   expr.ParamBase,
			params:      expr.Params,
			ctes:        r.ctes,
			rootArgBase: r.rootArgBase,
		}
		if err := group.prepare(src); err != nil {
			group.Release()
			return setSQLLowered{}, err
		}
		groupIndex := len(r.groups)
		r.groups = append(r.groups, setSQLGroup{expr: expr, runner: group})
		source := len(r.sourceOrder)
		r.sourceOrder = append(r.sourceOrder, -groupIndex-1)
		columns := len(group.Columns())
		node := len(plan.Nodes)
		plan.Nodes = append(plan.Nodes, NewSetTreeLeaf(source, columns))
		r.observeRunner(group)
		return setSQLLowered{node: node, columns: columns, firstSource: source}, nil

	default:
		return setSQLLowered{}, fmt.Errorf(
			"query: SQL set node at byte %d has kind %d: %w",
			expr.Pos, expr.Kind, ErrSetTreePlan,
		)
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
	if len(r.sourceOrder) == 1 {
		// descriptor.driving is independently derived from the syntactic first
		// operand; this local comparison only classifies catalog acquisition.
		r.driving = collection
		r.requiresCatalog = runnerRequiresCatalog(runner)
	} else if collection != r.driving || runnerRequiresCatalog(runner) {
		r.requiresCatalog = true
	}
	r.joins += runnerNumJoins(runner)
	r.generalizedJoin = r.generalizedJoin || runnerGeneralizedJoin(runner)
	// Every compound relation pipeline can read a coherent durable catalog
	// directly. This flag is meaningful only when catalog acquisition is needed.
	r.directCatalog = r.requiresCatalog || r.generalizedJoin
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
		if ordinal < 0 || ordinal >= len(r.ordinalSpec) {
			return &RelationColumnError{
				Relation: "set expression", Column: term.Name,
				Matches: 0, Pos: term.Pos,
			}
		}
		direction := Asc
		if term.Desc {
			direction = Desc
		}
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
	if s == nil {
		return false
	}
	if set := s.setSQL(); set != nil {
		return set.directCatalog
	}
	return s.UsesGeneralizedRelationJoin()
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
