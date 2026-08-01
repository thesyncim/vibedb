package query

import (
	"fmt"
	"math"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// LateralBindingValueError reports a captured value that the SQL literal
// compiler cannot represent without changing its type. Correlation parameters
// are deliberately scalar: treating a JSON object or array as text would make
// an equality predicate return a plausible but incorrect answer.
type LateralBindingValueError struct {
	Binding int
	Pos     int
	Kind    ValueType
}

func (e *LateralBindingValueError) Error() string {
	return fmt.Sprintf(
		"query: LATERAL binding %d at byte %d has non-scalar type %v",
		e.Binding+1, e.Pos, e.Kind,
	)
}

func (e *LateralBindingValueError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

// statementLateral is the prepared adapter between SQL's correlation sidecar
// and the relation-join pipeline. It owns no state on ordinary statements.
// One child Statement is rebound at most once for every left row (an
// outer-only FALSE/UNKNOWN gate can skip it); every produced right relation
// remains statement-owned until the completed join relation has been consumed,
// so no published scalar can outlive its bytes.
type statementLateral struct {
	spec       *sqlast.LateralSpec
	params     int
	bindings   []relationJoinPath
	bindingAST []sqlast.PathExpr
	args       []any
	slots      []lateralBindingSlot
	argUse     []bool

	outputs      []lateralOutput
	names        []string
	localOutputs int
	gates        []lateralGateExpr

	rights        []relationSpool
	active        []int64
	workspace     int64
	bindingActive int64

	evaluations uint64
}

type lateralBindingSlot struct {
	lookup []byte
	b      bool
	s      string
	n      Number
	value  scalar
}

type lateralClone struct {
	text       string
	spec       *sqlast.LateralSpec
	params     int
	seen       []bool
	bindingUse []bool
	argUse     []bool
	projection []lateralProjection
	gates      []lateralGateExpr
	hidden     int
}

// lateralProjection is one authored SELECT item before relation-wildcard
// expansion. Local items name a column in the reduced child SELECT list;
// correlated items name a stable outer binding synthesized by APPLY.
type lateralProjection struct {
	local   int
	binding int
	name    string
}

// lateralOutput is one physical right-relation column after local wildcard
// expansion. Exactly one of local and binding is non-negative.
type lateralOutput struct {
	local   int
	binding int
}

// lateralGateExpr is a prepared outer-only WHERE conjunct. Such a conjunct is
// constant for one left row and can be evaluated before opening the child.
// Keeping this representation separate from sql.Expr prevents a correlated
// path from ever reaching the ordinary local-path compiler by accident.
type lateralGateExpr struct {
	kind        sqlast.ExprKind
	op          sqlast.CmpOp
	negated     bool
	insensitive bool
	left        int
	right       int
	value       sqlast.Operand
	list        []sqlast.Operand
	kids        []lateralGateExpr
}

func prepareStatementLateral(
	owner *Statement,
	join *statementRelationJoin,
	ref *sqlast.TableRef,
	index int,
	argBase int,
) (*statementLateral, *Statement, error) {
	if ref == nil || ref.Query == nil || ref.Lateral == nil {
		return nil, nil, fmt.Errorf("query: invalid correlated LATERAL relation")
	}
	spec := ref.Lateral
	if spec.Decorrelated || len(spec.Bindings) == 0 {
		return nil, nil, sqlast.NewFeatureNotSupportedError(
			owner.text, spec.Pos,
			"correlated LATERAL metadata has no direct parameter references",
		)
	}
	if len(spec.References) == 0 {
		return nil, nil, sqlast.NewFeatureNotSupportedError(
			owner.text, spec.Bindings[0].Pos,
			"transitive nested LATERAL correlation needs an inherited APPLY frame",
		)
	}
	if ref.Join != sqlast.JoinCross && ref.Join != sqlast.JoinInner &&
		ref.Join != sqlast.JoinLeft {
		return nil, nil, sqlast.NewFeatureNotSupportedError(
			owner.text, spec.Pos,
			"correlated LATERAL execution supports CROSS, INNER, and LEFT joins",
		)
	}

	cloner := lateralClone{
		text: owner.text, spec: spec, params: ref.Query.Params,
		seen:       make([]bool, len(spec.References)),
		bindingUse: make([]bool, len(spec.Bindings)),
		argUse:     make([]bool, len(spec.Bindings)),
	}
	if err := cloner.validateReferences(); err != nil {
		return nil, nil, err
	}
	clone := *ref.Query
	var err error
	clone.Columns, err = cloner.cloneColumns(ref.Query)
	if err != nil {
		return nil, nil, err
	}
	clone.Where, err = cloner.cloneWhere(ref.Query.Where)
	if err != nil {
		return nil, nil, err
	}
	clone.OrderBy = cloner.cloneOrderBy(ref.Query.OrderBy)
	if err := cloner.rejectResidualLocations(ref.Query); err != nil {
		return nil, nil, err
	}
	if err := cloner.finish(); err != nil {
		return nil, nil, err
	}
	if ref.Query.Params > math.MaxInt-len(spec.Bindings) {
		return nil, nil, fmt.Errorf("query: LATERAL parameter count overflows int")
	}
	clone.Params = ref.Query.Params + len(spec.Bindings)

	lateral := &statementLateral{
		spec: spec, params: ref.Query.Params,
		bindings:   make([]relationJoinPath, len(spec.Bindings)),
		bindingAST: make([]sqlast.PathExpr, len(spec.Bindings)),
		args:       make([]any, clone.Params),
		slots:      make([]lateralBindingSlot, len(spec.Bindings)),
		argUse:     cloner.argUse,
		gates:      cloner.gates,
	}
	for i := range spec.Bindings {
		binding := &spec.Bindings[i]
		if binding.Depth != 1 || binding.Source < 0 || binding.Source >= index {
			return nil, nil, sqlast.NewFeatureNotSupportedError(
				owner.text, binding.Pos,
				"nested or forward LATERAL correlation does not yet have an APPLY frame",
			)
		}
		if len(binding.Segments) == 0 && !join.sources[binding.Source].physical {
			return nil, nil, sqlast.NewFeatureNotSupportedError(
				owner.text, binding.Pos,
				"correlated LATERAL wildcard over a derived relation needs schema-aware outer-column expansion",
			)
		}
		lateral.bindingAST[i] = sqlast.PathExpr{
			Source: binding.Source, Segments: binding.Segments, Pos: binding.Pos,
		}
		prepared, err := join.preparePath(&lateral.bindingAST[i])
		if err != nil {
			return nil, nil, owner.positionRelationJoinError(err, &lateral.bindingAST[i])
		}
		lateral.bindings[i] = prepared
	}

	child, err := prepareTreeInContext(
		owner.text, &clone, 0, owner.cteCatalog(), argBase+ref.Query.ParamBase,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := lateral.finishProjection(
		child, cloner.projection, clone.Columns, cloner.hidden,
	); err != nil {
		child.Release()
		return nil, nil, err
	}
	return lateral, child, nil
}

// cloneOrderBy removes correlated sort keys. Every captured outer value is
// constant during one child invocation, so such a key cannot affect row order;
// retaining its Path would instead let the local compiler misread it.
func (c *lateralClone) cloneOrderBy(terms []sqlast.OrderTerm) []sqlast.OrderTerm {
	if len(terms) == 0 {
		return nil
	}
	clone := make([]sqlast.OrderTerm, 0, len(terms))
	for i := range terms {
		reference, binding, outer := c.reference(terms[i].Path)
		if outer {
			c.mark(reference, binding, false)
			continue
		}
		clone = append(clone, terms[i])
	}
	return clone
}

func (c *lateralClone) rejectResidualLocations(query *sqlast.SelectStmt) error {
	for _, path := range query.GroupBy {
		if err := c.rejectPath(path, "GROUP BY"); err != nil {
			return err
		}
	}
	if err := c.rejectExpr(query.Having, "HAVING"); err != nil {
		return err
	}
	for i := range query.Windows {
		if err := c.rejectWindowSpec(&query.Windows[i].Spec); err != nil {
			return err
		}
	}
	for i := range query.Columns {
		window := query.Columns[i].Window
		if window == nil {
			continue
		}
		if err := c.rejectPath(window.Argument, "window argument"); err != nil {
			return err
		}
		if err := c.rejectWindowSpec(&window.Spec); err != nil {
			return err
		}
	}
	for i := 1; i < len(query.From); i++ {
		if query.From[i].On != nil {
			if err := c.rejectExpr(query.From[i].On.Expr, "nested JOIN ON"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *lateralClone) rejectWindowSpec(spec *sqlast.WindowSpec) error {
	if spec == nil {
		return nil
	}
	for _, path := range spec.PartitionBy {
		if err := c.rejectPath(path, "window PARTITION BY"); err != nil {
			return err
		}
	}
	for i := range spec.OrderBy {
		if err := c.rejectPath(spec.OrderBy[i].Path, "window ORDER BY"); err != nil {
			return err
		}
	}
	return nil
}

func (c *lateralClone) rejectExpr(expr *sqlast.Expr, clause string) error {
	if expr == nil {
		return nil
	}
	if err := c.rejectPath(expr.Path, clause); err != nil {
		return err
	}
	if err := c.rejectPath(expr.RightPath, clause); err != nil {
		return err
	}
	for _, kid := range expr.Kids {
		if err := c.rejectExpr(kid, clause); err != nil {
			return err
		}
	}
	return nil
}

func (c *lateralClone) rejectPath(path *sqlast.PathExpr, clause string) error {
	if path == nil {
		return nil
	}
	if _, _, outer := c.reference(path); !outer {
		return nil
	}
	return sqlast.NewFeatureNotSupportedError(
		c.text, path.Pos,
		"correlated LATERAL paths in "+clause+" need an expression-valued child plan",
	)
}

func (c *lateralClone) cloneColumns(
	query *sqlast.SelectStmt,
) ([]sqlast.ResultColumn, error) {
	columns := make([]sqlast.ResultColumn, 0, len(query.Columns))
	c.projection = make([]lateralProjection, 0, len(query.Columns))
	for i := range query.Columns {
		column := &query.Columns[i]
		reference, binding, outer := c.reference(column.Path)
		if !outer {
			c.projection = append(c.projection, lateralProjection{
				local: len(columns), binding: -1,
			})
			columns = append(columns, *column)
			continue
		}
		if column.Agg != sqlast.AggNone || column.Window != nil {
			return nil, sqlast.NewFeatureNotSupportedError(
				c.text, column.Path.Pos,
				"correlated LATERAL aggregate and window projections need an expression-valued projection plan",
			)
		}
		c.mark(reference, binding, false)
		name := column.Alias
		if name == "" {
			name = column.Path.Spec()
			if name == "" {
				name = "*"
			}
		}
		c.projection = append(c.projection, lateralProjection{
			local: -1, binding: binding, name: name,
		})
	}
	if len(columns) == 0 {
		if query.Distinct {
			return nil, sqlast.NewFeatureNotSupportedError(
				c.text, query.Columns[0].Pos,
				"DISTINCT over only correlated LATERAL projections needs a cardinality-only distinct operator",
			)
		}
		path := c.cardinalityPath(query.Where)
		if path == nil {
			path = &sqlast.PathExpr{Source: 0, Pos: query.Columns[0].Pos}
		}
		columns = append(columns, sqlast.ResultColumn{
			Path: path, Pos: query.Columns[0].Pos,
		})
		c.hidden = 1
	}
	return columns, nil
}

// cardinalityPath chooses an already-authored local scalar when possible, so
// an all-correlated projection need not copy a whole child document merely to
// preserve one output row per qualifying child row. The hidden column is never
// exposed through the LATERAL relation schema.
func (c *lateralClone) cardinalityPath(expr *sqlast.Expr) *sqlast.PathExpr {
	if expr == nil {
		return nil
	}
	for _, path := range []*sqlast.PathExpr{expr.Path, expr.RightPath} {
		if path == nil {
			continue
		}
		if _, _, outer := c.reference(path); !outer {
			return path
		}
	}
	for _, kid := range expr.Kids {
		if path := c.cardinalityPath(kid); path != nil {
			return path
		}
	}
	return nil
}

func (l *statementLateral) finishProjection(
	child *Statement,
	projection []lateralProjection,
	columns []sqlast.ResultColumn,
	hidden int,
) error {
	if child == nil {
		return fmt.Errorf("query: correlated LATERAL has no prepared child")
	}
	localNames := child.Columns()
	l.localOutputs = len(localNames)
	l.outputs = make([]lateralOutput, 0, len(localNames)+len(projection))
	l.names = make([]string, 0, len(localNames)+len(projection))
	local := 0
	for i := range projection {
		item := &projection[i]
		if item.binding >= 0 {
			l.outputs = append(l.outputs, lateralOutput{local: -1, binding: item.binding})
			l.names = append(l.names, item.name)
			continue
		}
		if item.local < 0 || item.local >= len(columns) {
			return fmt.Errorf("query: invalid prepared LATERAL projection map")
		}
		width := 1
		column := &columns[item.local]
		if child.hasRelationBinding() && column.Agg == sqlast.AggNone &&
			column.Path != nil && len(column.Path.Segments) == 0 {
			width = len(child.relationBindingForSource(column.Path.Source).names)
		}
		if width < 0 || local > len(localNames)-width {
			return fmt.Errorf("query: LATERAL local wildcard arity is unstable")
		}
		for ordinal := 0; ordinal < width; ordinal++ {
			l.outputs = append(l.outputs, lateralOutput{local: local, binding: -1})
			l.names = append(l.names, localNames[local])
			local++
		}
	}
	if hidden < 0 || local > len(localNames)-hidden || local+hidden != len(localNames) {
		return fmt.Errorf(
			"query: LATERAL local projection produced %d columns, mapped %d",
			len(localNames), local,
		)
	}
	return nil
}

func (c *lateralClone) validateReferences() error {
	for i := range c.spec.References {
		reference := &c.spec.References[i]
		if reference.Path == nil || reference.Binding < 0 ||
			reference.Binding >= len(c.spec.Bindings) {
			return sqlast.NewFeatureNotSupportedError(
				c.text, c.spec.Pos, "invalid correlated LATERAL reference metadata",
			)
		}
		for prior := 0; prior < i; prior++ {
			if c.spec.References[prior].Path == reference.Path {
				return sqlast.NewFeatureNotSupportedError(
					c.text, reference.Path.Pos,
					"one correlated LATERAL path occurrence maps to multiple references",
				)
			}
		}
		binding := &c.spec.Bindings[reference.Binding]
		if binding.Depth != 1 || binding.Source != reference.Path.Source ||
			!lateralSegmentsEqual(binding.Segments, reference.Path.Segments) {
			return sqlast.NewFeatureNotSupportedError(
				c.text, reference.Path.Pos,
				"correlated LATERAL reference does not match its stable binding",
			)
		}
	}
	return nil
}

func lateralSegmentsEqual(a, b []sqlast.Segment) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (c *lateralClone) reference(path *sqlast.PathExpr) (int, int, bool) {
	if path == nil {
		return 0, 0, false
	}
	for i := range c.spec.References {
		if c.spec.References[i].Path == path {
			return i, c.spec.References[i].Binding, true
		}
	}
	return 0, 0, false
}

func (c *lateralClone) mark(reference, binding int, argument bool) {
	c.seen[reference] = true
	c.bindingUse[binding] = true
	if argument {
		c.argUse[binding] = true
	}
}

// cloneWhere extracts only outer-only top-level conjuncts. For one APPLY left
// row they are constants, so SQL's WHERE rule is exactly "open the child iff
// every gate is TRUE"; FALSE and UNKNOWN both produce no right rows. An
// outer-only expression below OR with a local expression is deliberately not
// extracted because doing so would change three-valued boolean semantics.
func (c *lateralClone) cloneWhere(expr *sqlast.Expr) (*sqlast.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.Kind != sqlast.ExprAnd {
		if c.outerOnly(expr) {
			gate, err := c.compileGate(expr)
			if err != nil {
				return nil, err
			}
			c.gates = append(c.gates, gate)
			return nil, nil
		}
		return c.cloneExpr(expr)
	}
	clone := *expr
	clone.Kids = make([]*sqlast.Expr, 0, len(expr.Kids))
	for _, kid := range expr.Kids {
		if c.outerOnly(kid) {
			gate, err := c.compileGate(kid)
			if err != nil {
				return nil, err
			}
			c.gates = append(c.gates, gate)
			continue
		}
		local, err := c.cloneExpr(kid)
		if err != nil {
			return nil, err
		}
		clone.Kids = append(clone.Kids, local)
	}
	switch len(clone.Kids) {
	case 0:
		return nil, nil
	case 1:
		return clone.Kids[0], nil
	default:
		return &clone, nil
	}
}

func (c *lateralClone) outerOnly(expr *sqlast.Expr) bool {
	if expr == nil || expr.Subquery != nil || expr.Kind == sqlast.ExprExists {
		return false
	}
	switch expr.Kind {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		if len(expr.Kids) == 0 {
			return false
		}
		for _, kid := range expr.Kids {
			if !c.outerOnly(kid) {
				return false
			}
		}
		return true
	}
	paths := 0
	for _, path := range []*sqlast.PathExpr{expr.Path, expr.RightPath} {
		if path == nil {
			continue
		}
		paths++
		if _, _, outer := c.reference(path); !outer {
			return false
		}
	}
	return paths != 0
}

func (c *lateralClone) compileGate(expr *sqlast.Expr) (lateralGateExpr, error) {
	gate := lateralGateExpr{
		kind: expr.Kind, op: expr.Op, negated: expr.Negated,
		insensitive: expr.Insensitive, left: -1, right: -1,
		value: expr.Value, list: expr.List,
	}
	switch expr.Kind {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		gate.kids = make([]lateralGateExpr, len(expr.Kids))
		for i := range expr.Kids {
			kid, err := c.compileGate(expr.Kids[i])
			if err != nil {
				return lateralGateExpr{}, err
			}
			gate.kids[i] = kid
		}
		return gate, nil
	case sqlast.ExprCompare, sqlast.ExprIn, sqlast.ExprBetween,
		sqlast.ExprIsNull, sqlast.ExprIsMissing, sqlast.ExprLike:
	case sqlast.ExprContains:
		return lateralGateExpr{}, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Path.Pos,
			"outer-only LATERAL containment needs a prepared captured-value containment program",
		)
	default:
		return lateralGateExpr{}, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Pos, "correlated LATERAL predicate kind is not executable as an outer gate",
		)
	}
	if expr.Subquery != nil {
		return lateralGateExpr{}, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Pos,
			"correlated LATERAL predicate subqueries need an inherited correlation frame",
		)
	}
	if expr.Path != nil {
		reference, binding, outer := c.reference(expr.Path)
		if !outer {
			return lateralGateExpr{}, sqlast.NewFeatureNotSupportedError(
				c.text, expr.Path.Pos, "LATERAL outer gate contains a local path",
			)
		}
		gate.left = binding
		c.mark(reference, binding, false)
	}
	if expr.RightPath != nil {
		reference, binding, outer := c.reference(expr.RightPath)
		if !outer {
			return lateralGateExpr{}, sqlast.NewFeatureNotSupportedError(
				c.text, expr.RightPath.Pos, "LATERAL outer gate contains a local path",
			)
		}
		gate.right = binding
		c.mark(reference, binding, false)
	}
	return gate, nil
}

func (c *lateralClone) cloneExpr(expr *sqlast.Expr) (*sqlast.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	clone := *expr
	if len(expr.Kids) != 0 {
		clone.Kids = make([]*sqlast.Expr, len(expr.Kids))
		for i := range expr.Kids {
			kid, err := c.cloneExpr(expr.Kids[i])
			if err != nil {
				return nil, err
			}
			clone.Kids[i] = kid
		}
	}
	leftRef, leftBinding, leftOuter := c.reference(expr.Path)
	rightRef, rightBinding, rightOuter := c.reference(expr.RightPath)
	if !leftOuter && !rightOuter {
		return &clone, nil
	}
	if expr.Kind != sqlast.ExprCompare || leftOuter == rightOuter {
		path := expr.Path
		if rightOuter {
			path = expr.RightPath
		}
		return nil, sqlast.NewFeatureNotSupportedError(
			c.text, path.Pos,
			"correlated LATERAL paths currently require one local-to-outer comparison in WHERE",
		)
	}
	if rightOuter {
		if expr.Path == nil {
			return nil, sqlast.NewFeatureNotSupportedError(
				c.text, expr.RightPath.Pos, "correlated LATERAL comparison has no local operand",
			)
		}
		clone.RightPath = nil
		clone.Value = sqlast.Operand{
			Kind: sqlast.OperandParam, Ordinal: c.params + rightBinding,
			Pos: expr.RightPath.Pos,
		}
		c.mark(rightRef, rightBinding, true)
		return &clone, nil
	}
	if expr.RightPath == nil {
		return nil, sqlast.NewFeatureNotSupportedError(
			c.text, expr.Path.Pos, "correlated LATERAL comparison has no local operand",
		)
	}
	clone.Path = expr.RightPath
	clone.RightPath = nil
	clone.Op = lateralReverseComparison(expr.Op)
	clone.Value = sqlast.Operand{
		Kind: sqlast.OperandParam, Ordinal: c.params + leftBinding,
		Pos: expr.Path.Pos,
	}
	c.mark(leftRef, leftBinding, true)
	return &clone, nil
}

func lateralReverseComparison(op sqlast.CmpOp) sqlast.CmpOp {
	switch op {
	case sqlast.OpLt:
		return sqlast.OpGt
	case sqlast.OpLe:
		return sqlast.OpGe
	case sqlast.OpGt:
		return sqlast.OpLt
	case sqlast.OpGe:
		return sqlast.OpLe
	default:
		return op
	}
}

func (c *lateralClone) finish() error {
	for i := range c.seen {
		if !c.seen[i] {
			return sqlast.NewFeatureNotSupportedError(
				c.text, c.spec.References[i].Path.Pos,
				"correlated LATERAL paths outside a local-to-outer WHERE comparison are not executable yet",
			)
		}
	}
	for i := range c.bindingUse {
		if !c.bindingUse[i] {
			return sqlast.NewFeatureNotSupportedError(
				c.text, c.spec.Bindings[i].Pos,
				"transitive nested LATERAL binding does not yet have an APPLY frame",
			)
		}
	}
	return nil
}

func (l *statementLateral) gatesPass(
	join *statementRelationJoin,
	cancel *CancelFlag,
) (bool, error) {
	for i := range l.gates {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return false, err
		}
		value, err := l.evalGate(join, &l.gates[i], cancel)
		if err != nil {
			return false, err
		}
		if value != triTrue {
			return false, nil
		}
	}
	return true, cancellationError(cancel)
}

func (l *statementLateral) evalGate(
	join *statementRelationJoin,
	gate *lateralGateExpr,
	cancel *CancelFlag,
) (tri, error) {
	switch gate.kind {
	case sqlast.ExprAnd:
		out := triTrue
		for i := range gate.kids {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			value, err := l.evalGate(join, &gate.kids[i], cancel)
			if err != nil {
				return triFalse, err
			}
			if value == triFalse {
				return triFalse, nil
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprOr:
		out := triFalse
		for i := range gate.kids {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			value, err := l.evalGate(join, &gate.kids[i], cancel)
			if err != nil {
				return triFalse, err
			}
			if value == triTrue {
				return triTrue, nil
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprNot:
		if len(gate.kids) != 1 {
			return triFalse, fmt.Errorf("query: invalid prepared LATERAL NOT gate")
		}
		value, err := l.evalGate(join, &gate.kids[0], cancel)
		return notTri(value), err
	}
	if gate.left < 0 || gate.left >= len(l.slots) {
		return triFalse, fmt.Errorf("query: invalid prepared LATERAL gate binding")
	}
	cell := l.slots[gate.left].value
	var value tri
	switch gate.kind {
	case sqlast.ExprCompare:
		if gate.right >= 0 {
			if gate.right >= len(l.slots) {
				return triFalse, fmt.Errorf("query: invalid prepared LATERAL right gate binding")
			}
			right := l.slots[gate.right].value
			if cell.kind == kindNull || right.kind == kindNull {
				value = triUnknown
			} else {
				value = boolTri(acceptSign(compareScalar(cell, right), Op(gate.op)))
			}
		} else {
			literal, known, err := join.joinOperand(gate.value, l.args)
			if err != nil {
				return triFalse, err
			}
			value = compareTri(cell, havingLit{value: literal, known: known}, Op(gate.op))
		}
	case sqlast.ExprIsNull:
		value = boolTri(cell.kind == kindNull)
	case sqlast.ExprIsMissing:
		value = boolTri(cell.kind == kindNull && len(cell.raw) == 0)
	case sqlast.ExprBetween:
		if len(gate.list) != 2 {
			return triFalse, fmt.Errorf("query: invalid prepared LATERAL BETWEEN gate")
		}
		lower, known, err := join.joinOperand(gate.list[0], l.args)
		if err != nil {
			return triFalse, err
		}
		lo := compareTri(cell, havingLit{value: lower, known: known}, Ge)
		upper, known, err := join.joinOperand(gate.list[1], l.args)
		if err != nil {
			return triFalse, err
		}
		hi := compareTri(cell, havingLit{value: upper, known: known}, Le)
		value = andTri(lo, hi)
	case sqlast.ExprIn:
		if cell.kind == kindNull {
			value = triUnknown
			break
		}
		value = triFalse
		for i := range gate.list {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			literal, known, err := join.joinOperand(gate.list[i], l.args)
			if err != nil {
				return triFalse, err
			}
			if !known {
				if value == triFalse {
					value = triUnknown
				}
				continue
			}
			if compareScalar(cell, literal) == 0 {
				value = triTrue
				break
			}
		}
	case sqlast.ExprLike:
		literal, known, err := join.joinOperand(gate.value, l.args)
		if err != nil {
			return triFalse, err
		}
		if !known || cell.kind == kindNull {
			value = triUnknown
			break
		}
		if literal.kind != kindString {
			return triFalse, fmt.Errorf(
				"%w: LATERAL gate LIKE pattern must be a string",
				ErrParameterType,
			)
		}
		if err := validateLikePattern(literal.sval); err != nil {
			return triFalse, fmt.Errorf("query: invalid LATERAL gate LIKE pattern: %w", err)
		}
		value = boolTri(cell.kind == kindString &&
			likeMatch(literal.sval, cell.sval, gate.insensitive))
	default:
		return triFalse, fmt.Errorf("query: unsupported prepared LATERAL gate kind %d", gate.kind)
	}
	if gate.negated {
		value = notTri(value)
	}
	return value, cancellationError(cancel)
}

func (l *statementLateral) bind(
	left *relationSpool,
	row int,
	rootArgs []any,
	base int,
	frame *statementFrame,
	cancel *CancelFlag,
) ([]any, error) {
	if l == nil || len(l.bindings) != len(l.slots) ||
		len(l.args) != l.params+len(l.bindings) ||
		len(l.argUse) != len(l.bindings) {
		return nil, fmt.Errorf("query: invalid prepared LATERAL binding state")
	}
	if base < 0 || base+l.params > len(rootArgs) {
		return nil, fmt.Errorf("query: invalid LATERAL placeholder range")
	}
	copy(l.args[:l.params], rootArgs[base:base+l.params])
	for i := range l.bindings {
		slot := &l.slots[i]
		value, err := l.bindingScalar(
			left, row, l.bindings[i], slot, frame, cancel,
		)
		if err != nil {
			return nil, err
		}
		slot.value = value
		l.args[l.params+i] = nil
		if l.argUse[i] {
			argument, err := slot.argument(value, i, l.spec.Bindings[i].Pos)
			if err != nil {
				return nil, err
			}
			l.args[l.params+i] = argument
		}
	}
	return l.args, nil
}

func (l *statementLateral) bindingScalar(
	left *relationSpool,
	row int,
	path relationJoinPath,
	slot *lateralBindingSlot,
	frame *statementFrame,
	cancel *CancelFlag,
) (scalar, error) {
	if left == nil || path.column < 0 || path.column >= len(left.columns) ||
		row < 0 || row >= left.rows {
		return scalar{kind: kindNull}, nil
	}
	root := left.columns[path.column][row]
	if path.root {
		return root, cancellationError(cancel)
	}
	raw := root.raw
	if len(raw) == 0 {
		return scalar{kind: kindNull}, nil
	}
	resolved, ok, err := path.suffix.GetRawTrusted(raw)
	if err != nil || !ok {
		return scalar{kind: kindNull}, err
	}
	if resolved.Kind() != document.String {
		slot.lookup = slot.lookup[:0]
		return classifyRawInto(resolved, &slot.lookup), cancellationError(cancel)
	}
	if content, clean := resolved.StringBytes(); clean {
		return scalar{
			kind: kindString, sval: byteview.String(content), raw: resolved.Bytes(),
		}, cancellationError(cancel)
	}
	bytes, err := lateralDecodedJSONStringBytes(resolved.Bytes(), cancel)
	if err != nil {
		return scalar{}, err
	}
	if err := l.reserveBinding(frame, bytes); err != nil {
		return scalar{}, err
	}
	if cap(slot.lookup) < bytes {
		slot.lookup = make([]byte, 0, bytes)
	} else {
		slot.lookup = slot.lookup[:0]
	}
	decoded, err := lateralAppendDecodedJSONString(
		slot.lookup, resolved.Bytes(), cancel,
	)
	if err != nil {
		return scalar{}, err
	}
	if len(decoded) != bytes {
		return scalar{}, fmt.Errorf(
			"query: captured JSON string changed between sizing and binding",
		)
	}
	slot.lookup = decoded
	return scalar{
		kind: kindString, sval: byteview.String(decoded), raw: resolved.Bytes(),
	}, nil
}

func lateralDecodedJSONStringBytes(raw []byte, cancel *CancelFlag) (int, error) {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return 0, fmt.Errorf("query: invalid captured JSON string")
	}
	bytes := 0
	for i := 1; i < len(raw)-1; {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return 0, err
		}
		if raw[i] != '\\' {
			bytes++
			i++
			continue
		}
		if i+1 >= len(raw)-1 {
			return 0, fmt.Errorf("query: truncated captured JSON string escape")
		}
		switch raw[i+1] {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			bytes++
			i += 2
		case 'u':
			first, ok := lateralHexRune(raw, i+2)
			if !ok {
				return 0, fmt.Errorf("query: invalid captured JSON unicode escape")
			}
			r := rune(first)
			i += 6
			if utf16.IsSurrogate(r) {
				if r < 0xD800 || r > 0xDBFF || i+6 > len(raw)-1 ||
					raw[i] != '\\' || raw[i+1] != 'u' {
					return 0, fmt.Errorf("query: invalid captured JSON surrogate pair")
				}
				second, ok := lateralHexRune(raw, i+2)
				if !ok || second < 0xDC00 || second > 0xDFFF {
					return 0, fmt.Errorf("query: invalid captured JSON surrogate pair")
				}
				r = utf16.DecodeRune(r, rune(second))
				i += 6
			}
			width := utf8.RuneLen(r)
			if width < 0 || bytes > math.MaxInt-width {
				return 0, fmt.Errorf("query: captured JSON string size overflows int")
			}
			bytes += width
		default:
			return 0, fmt.Errorf("query: invalid captured JSON string escape")
		}
	}
	return bytes, cancellationError(cancel)
}

func lateralAppendDecodedJSONString(
	dst []byte,
	raw []byte,
	cancel *CancelFlag,
) ([]byte, error) {
	for i := 1; i < len(raw)-1; {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return nil, err
		}
		if raw[i] != '\\' {
			dst = append(dst, raw[i])
			i++
			continue
		}
		switch raw[i+1] {
		case '"', '\\', '/':
			dst = append(dst, raw[i+1])
			i += 2
		case 'b':
			dst = append(dst, '\b')
			i += 2
		case 'f':
			dst = append(dst, '\f')
			i += 2
		case 'n':
			dst = append(dst, '\n')
			i += 2
		case 'r':
			dst = append(dst, '\r')
			i += 2
		case 't':
			dst = append(dst, '\t')
			i += 2
		case 'u':
			first, ok := lateralHexRune(raw, i+2)
			if !ok {
				return nil, fmt.Errorf("query: invalid captured JSON unicode escape")
			}
			r := rune(first)
			i += 6
			if utf16.IsSurrogate(r) {
				second, ok := lateralHexRune(raw, i+2)
				if !ok {
					return nil, fmt.Errorf("query: invalid captured JSON surrogate pair")
				}
				r = utf16.DecodeRune(r, rune(second))
				i += 6
			}
			dst = utf8.AppendRune(dst, r)
		default:
			return nil, fmt.Errorf("query: invalid captured JSON string escape")
		}
	}
	return dst, cancellationError(cancel)
}

func lateralHexRune(raw []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(raw) {
		return 0, false
	}
	var value uint16
	for _, b := range raw[start : start+4] {
		value <<= 4
		switch {
		case b >= '0' && b <= '9':
			value |= uint16(b - '0')
		case b >= 'a' && b <= 'f':
			value |= uint16(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value |= uint16(b-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func (l *statementLateral) reserveBinding(frame *statementFrame, bytes int) error {
	if bytes <= 0 {
		return nil
	}
	if err := frame.intermediate.reserve("LATERAL parameter tuple", int64(bytes)); err != nil {
		return err
	}
	l.bindingActive = saturatedBytes(l.bindingActive, int64(bytes))
	return nil
}

func (l *statementLateral) releaseBinding(frame *statementFrame) {
	if l == nil || frame == nil {
		return
	}
	frame.intermediate.release(l.bindingActive)
	l.bindingActive = 0
}

func (s *lateralBindingSlot) argument(value scalar, binding, pos int) (any, error) {
	switch value.kind {
	case kindNull:
		return nil, nil
	case kindBool:
		s.b = value.bval
		return &s.b, nil
	case kindNumber:
		s.n = Number(byteview.String(value.num))
		return &s.n, nil
	case kindString:
		s.s = value.sval
		return &s.s, nil
	default:
		return nil, &LateralBindingValueError{
			Binding: binding, Pos: pos, Kind: cellFromScalar(value).Kind(),
		}
	}
}

func (l *statementLateral) materializeRights(
	join *statementRelationJoin,
	op *relationJoinOperand,
	owner *Statement,
	parent *Exec,
	src Source,
	rootArgs []any,
	left *relationSpool,
	frame *statementFrame,
) error {
	if l == nil || op == nil || op.stmt == nil || left == nil {
		return fmt.Errorf("query: invalid LATERAL execution state")
	}
	l.evaluations = 0
	workspace := saturatedProduct(
		int64(left.rows), int64(unsafe.Sizeof(relationSpool{})+unsafe.Sizeof(int64(0))),
	)
	if workspace == math.MaxInt64 {
		return &IntermediateBudgetError{
			Resource: "LATERAL per-row workspace", Bytes: math.MaxInt64,
			Limit: frame.intermediate.limit,
		}
	}
	if err := frame.intermediate.reserve("LATERAL per-row workspace", workspace); err != nil {
		return err
	}
	l.workspace = workspace
	l.resize(left.rows)
	for row := 0; row < left.rows; row++ {
		if err := cancellationCheckpoint(parent.Options.Cancel, row); err != nil {
			return err
		}
		bound, err := l.bind(
			left, row, rootArgs, op.ref.Query.ParamBase, frame,
			parent.Options.Cancel,
		)
		if err != nil {
			return err
		}
		passes, err := l.gatesPass(join, parent.Options.Cancel)
		if err != nil {
			l.releaseBinding(frame)
			return err
		}
		if !passes {
			l.releaseBinding(frame)
			continue
		}
		nestedSource, err := src.subquerySource(owner.Collection(), op.stmt.Collection())
		if err != nil {
			l.releaseBinding(frame)
			return err
		}
		op.exec.Options = parent.Options
		cursor, err := op.stmt.runIntoFrame(
			&op.exec, nestedSource, bound, frame, "LATERAL child result",
		)
		if err != nil {
			l.releaseBinding(frame)
			return err
		}
		l.evaluations++
		if op.stmt.outputs != l.localOutputs {
			clearExecBorrowedViews(&op.exec)
			op.stmt.releaseRelations(frame)
			l.releaseBinding(frame)
			return &ApplyRightArityError{Columns: l.localOutputs, Got: op.stmt.outputs}
		}
		resultBytes := op.exec.Result.resultBytesUsed
		if err := frame.intermediate.reserve("LATERAL child result", resultBytes); err != nil {
			clearExecBorrowedViews(&op.exec)
			op.stmt.releaseRelations(frame)
			l.releaseBinding(frame)
			return err
		}
		charge, materializeErr := l.materializeRight(
			cursor, &l.rights[row], frame, parent.Options.Cancel,
		)
		frame.intermediate.release(resultBytes)
		clearExecBorrowedViews(&op.exec)
		op.stmt.releaseRelations(frame)
		l.releaseBinding(frame)
		if materializeErr != nil {
			return materializeErr
		}
		l.active[row] = charge
	}
	return nil
}

func (l *statementLateral) materializeRight(
	cursor Cursor,
	spool *relationSpool,
	frame *statementFrame,
	cancel *CancelFlag,
) (int64, error) {
	spool.reset()
	rows, payload, err := l.measureRight(cursor, cancel)
	if err != nil {
		return 0, err
	}
	charge := relationSpoolRetainedBytes(rows, len(l.outputs), payload)
	if charge == math.MaxInt64 {
		return 0, &IntermediateBudgetError{
			Resource: "LATERAL right relation", Bytes: math.MaxInt64,
			Limit: frame.intermediate.limit,
		}
	}
	if err := frame.intermediate.reserve("LATERAL right relation", charge); err != nil {
		return 0, err
	}
	if err := cancellationError(cancel); err != nil {
		frame.intermediate.release(charge)
		return 0, err
	}
	if err := spool.begin(rows, len(l.outputs), payload); err != nil {
		frame.intermediate.release(charge)
		return 0, err
	}
	if err := l.fillRight(cursor, spool, rows, cancel); err != nil {
		spool.reset()
		frame.intermediate.release(charge)
		return 0, err
	}
	return charge, nil
}

func (l *statementLateral) measureRight(
	cursor Cursor,
	cancel *CancelFlag,
) (rows int, payload int64, err error) {
	for {
		next, nextErr := cursor.nextWithCancel(cancel)
		if nextErr != nil {
			return 0, 0, nextErr
		}
		if !next {
			return rows, payload, nil
		}
		if rows == math.MaxInt {
			return 0, 0, fmt.Errorf("query: LATERAL right row count overflows int")
		}
		rows++
		for column := range l.outputs {
			if err := cancellationCheckpoint(cancel, column); err != nil {
				return 0, 0, err
			}
			cell, err := l.outputCell(&cursor, column)
			if err != nil {
				return 0, 0, err
			}
			bytes, err := relationCellOwnedBytesCancelable(cell, cancel)
			if err != nil {
				return 0, 0, err
			}
			payload = saturatedBytes(payload, int64(bytes))
		}
	}
}

func (l *statementLateral) fillRight(
	cursor Cursor,
	spool *relationSpool,
	rows int,
	cancel *CancelFlag,
) error {
	row := 0
	for {
		next, err := cursor.nextWithCancel(cancel)
		if err != nil {
			return err
		}
		if !next {
			if row != rows || len(spool.data) != spool.plannedData {
				return fmt.Errorf("query: LATERAL right relation changed during publication")
			}
			return nil
		}
		if row >= rows {
			return fmt.Errorf("query: LATERAL right relation grew during publication")
		}
		for column := range l.outputs {
			if err := cancellationCheckpoint(cancel, column); err != nil {
				return err
			}
			cell, err := l.outputCell(&cursor, column)
			if err != nil {
				return err
			}
			owned, err := spool.ownCell(cell, cancel)
			if err != nil {
				return err
			}
			spool.columns[column][row] = owned
		}
		row++
	}
}

func (l *statementLateral) outputCell(cursor *Cursor, column int) (Cell, error) {
	if column < 0 || column >= len(l.outputs) {
		return Cell{}, fmt.Errorf("query: LATERAL output ordinal is out of range")
	}
	output := &l.outputs[column]
	if output.local >= 0 {
		if output.local >= l.localOutputs {
			return Cell{}, fmt.Errorf("query: LATERAL local output ordinal is out of range")
		}
		return cursor.Cell(output.local), nil
	}
	if output.binding < 0 || output.binding >= len(l.slots) {
		return Cell{}, fmt.Errorf("query: LATERAL projection binding is out of range")
	}
	return cellFromScalar(l.slots[output.binding].value), nil
}

func (l *statementLateral) resize(rows int) {
	if cap(l.rights) < rows {
		next := make([]relationSpool, rows)
		copy(next, l.rights)
		l.rights = next
	} else {
		l.rights = l.rights[:rows]
	}
	if cap(l.active) < rows {
		next := make([]int64, rows)
		copy(next, l.active)
		l.active = next
	} else {
		l.active = l.active[:rows]
		clear(l.active)
	}
	for i := range l.rights {
		l.rights[i].reset()
	}
}

func (l *statementLateral) runStage(
	join *statementRelationJoin,
	stage *relationJoinStage,
	op *relationJoinOperand,
	owner *Statement,
	parent *Exec,
	src Source,
	rootArgs []any,
	left, out *relationSpool,
	frame *statementFrame,
) (charge int64, pairs int, err error) {
	// This is the statement-accounted equivalent of ApplyKernel. The child is
	// evaluated exactly once for each gate-TRUE left row (and never for a
	// FALSE/UNKNOWN gate); each right relation reserves IntermediateBytes before
	// its spool grows. Keeping those spools until the final sizing pass avoids
	// both volatile double evaluation and partial publication. No tuple
	// memoization is attempted: the SQL front end has no immutability/volatility
	// proof strong enough to make reuse truthful yet.
	if err := l.materializeRights(
		join, op, owner, parent, src, rootArgs, left, frame,
	); err != nil {
		return 0, 0, err
	}
	pairLimit, err := normalizeJoinPairBytes(parent.Options)
	if err != nil {
		return 0, 0, err
	}
	var payload int64
	pairs, err = l.walkStage(
		join, stage, left, nil, rootArgs, parent.Options.Cancel,
		pairLimit, &payload,
	)
	if err != nil {
		return 0, 0, err
	}
	charge = relationSpoolRetainedBytes(pairs, stage.outputColumns, payload)
	if charge == math.MaxInt64 {
		return 0, 0, &IntermediateBudgetError{
			Resource: "LATERAL APPLY relation", Bytes: math.MaxInt64,
			Limit: frame.intermediate.limit,
		}
	}
	if err := frame.intermediate.reserve("LATERAL APPLY relation", charge); err != nil {
		return 0, 0, err
	}
	if err := cancellationError(parent.Options.Cancel); err != nil {
		frame.intermediate.release(charge)
		return 0, 0, err
	}
	if err := out.begin(pairs, stage.outputColumns, payload); err != nil {
		frame.intermediate.release(charge)
		return 0, 0, err
	}
	filled, err := l.walkStage(
		join, stage, left, out, rootArgs, parent.Options.Cancel,
		pairLimit, nil,
	)
	if err != nil || filled != pairs {
		out.reset()
		frame.intermediate.release(charge)
		if err != nil {
			return 0, 0, err
		}
		return 0, 0, fmt.Errorf("query: LATERAL APPLY changed during publication")
	}
	return charge, pairs, nil
}

func (l *statementLateral) walkStage(
	join *statementRelationJoin,
	stage *relationJoinStage,
	left, out *relationSpool,
	args []any,
	cancel *CancelFlag,
	pairLimit int64,
	payload *int64,
) (int, error) {
	pairs := 0
	for lrow := 0; lrow < left.rows; lrow++ {
		if err := cancellationCheckpoint(cancel, lrow); err != nil {
			return pairs, err
		}
		right := &l.rights[lrow]
		found := false
		for rrow := 0; rrow < right.rows; rrow++ {
			matched := true
			if stage.ref.Join != sqlast.JoinCross {
				cond := stage.ref.On
				if cond == nil || cond.Expr == nil {
					return pairs, fmt.Errorf("query: LATERAL join has no ON expression")
				}
				value, err := join.evalJoinExpr(
					stage, cond.Expr, left, right, lrow, rrow, args,
				)
				if err != nil {
					return pairs, err
				}
				matched = value == triTrue
			}
			if !matched {
				continue
			}
			found = true
			next, err := relationJoinNextPair(0, pairs, pairLimit)
			if err != nil {
				return pairs, err
			}
			if out != nil {
				if err := join.writePair(stage, left, right, out, pairs, lrow, rrow); err != nil {
					return pairs, err
				}
			} else if payload != nil {
				if err := lateralPairPayload(join, stage, left, right, lrow, rrow, payload); err != nil {
					return pairs, err
				}
			}
			pairs = next
		}
		if !found && stage.ref.Join == sqlast.JoinLeft {
			next, err := relationJoinNextPair(0, pairs, pairLimit)
			if err != nil {
				return pairs, err
			}
			if out != nil {
				if err := join.writePair(stage, left, right, out, pairs, lrow, -1); err != nil {
					return pairs, err
				}
			} else if payload != nil {
				if err := lateralPairPayload(join, stage, left, right, lrow, -1, payload); err != nil {
					return pairs, err
				}
			}
			pairs = next
		}
	}
	return pairs, cancellationError(cancel)
}

func lateralPairPayload(
	join *statementRelationJoin,
	stage *relationJoinStage,
	left, right *relationSpool,
	lrow, rrow int,
	payload *int64,
) error {
	for i := range stage.using {
		value, err := join.usingValue(stage, left, right, i, lrow, rrow)
		if err != nil {
			return err
		}
		if relationJoinOwnsDecodedText(value) {
			*payload = saturatedBytes(*payload, int64(len(value.sval)))
		}
	}
	return nil
}

func (l *statementLateral) releaseExecution(frame *statementFrame) {
	if l == nil {
		return
	}
	for i := range l.rights {
		l.rights[i].reset()
		if i < len(l.active) {
			frame.intermediate.release(l.active[i])
			l.active[i] = 0
		}
	}
	l.releaseBinding(frame)
	frame.intermediate.release(l.workspace)
	l.workspace = 0
}

func (l *statementLateral) discardExecution() {
	if l == nil {
		return
	}
	for i := range l.rights {
		l.rights[i].reset()
	}
	clear(l.active)
	l.workspace = 0
	l.bindingActive = 0
}

func (l *statementLateral) release() {
	if l == nil {
		return
	}
	for i := range l.rights {
		l.rights[i].release()
	}
	*l = statementLateral{}
}
