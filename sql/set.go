package sql

import (
	"strconv"
)

// SetOperation is one SQL binary set-operation mode. The values are explicit
// rather than arithmetic aliases of another package's private enum: lowering
// must map them exhaustively, so enum reordering cannot silently change SQL.
type SetOperation uint8

const (
	SetUnionAll SetOperation = iota
	SetUnionDistinct
	SetIntersectAll
	SetIntersectDistinct
	SetExceptAll
	SetExceptDistinct
)

// SetExprKind discriminates leaves, binary operations, and authored groups.
// A group is never flattened even when it does not change precedence: its
// local ORDER BY/LIMIT/OFFSET scope and source positions remain observable.
type SetExprKind uint8

const (
	SetSelectExpr SetExprKind = iota
	SetBinaryExpr
	SetGroupExpr
)

// SetExpr is one node in a lossless set-expression tree.
type SetExpr struct {
	Kind      SetExprKind
	Operation SetOperation
	Select    *SelectStmt
	Left      *SetExpr
	Right     *SetExpr
	Child     *SetExpr
	Tail      *SetTail
	First     *SelectStmt

	// Columns is the statically known ordinal width. ArityDeferred is true
	// when a relation wildcard requires catalog expansion before the width is
	// known. Binary nodes reject unequal known widths while preserving deferred
	// checks for lowering/runtime.
	Columns       int
	ArityDeferred bool

	// ParamBase and Params are the exact contiguous range occupied by this
	// subtree in the containing set expression. A leaf's own Operand ordinals
	// remain local to that leaf; lowering binds its runner to this range.
	ParamBase int
	Params    int

	// Pos is SELECT/WITH, the binary operator, or the opening parenthesis by
	// Kind. End is the closing-parenthesis byte offset for SetGroupExpr and -1
	// otherwise.
	Pos int
	End int
}

// SetOrderTerm orders a completed set result by the syntactic first operand's
// output namespace. Output is one-based; Name retains the authored identifier.
type SetOrderTerm struct {
	Name   string
	Output int
	Desc   bool
	Pos    int
}

// SetTail is ORDER BY/LIMIT/OFFSET scoped to one complete query expression.
// Root Tail applies after the whole set. A group Tail applies only inside that
// authored parenthesis.
type SetTail struct {
	OrderBy []SetOrderTerm
	Limit   *Operand
	Offset  *Operand
	Pos     int

	ParamBase int
	Params    int
}

// SetOutputColumn is first-operand output naming metadata. Deferred marks an
// output whose final name/cardinality depends on wildcard expansion or a
// runtime-described expression; Name is empty when no stable parser-level name
// exists and an explicit alias is required for a final ORDER BY reference.
type SetOutputColumn struct {
	Name     string
	Pos      int
	Deferred bool
}

// SetExpression is the cold sidecar attached to a compound or explicitly
// parenthesized SelectStmt. First is stable syntactic identity, independent of
// optimizer tree shape. Tail belongs to the completed root expression.
type SetExpression struct {
	Root    *SetExpr
	First   *SelectStmt
	Outputs []SetOutputColumn
	Tail    *SetTail
	Pos     int

	ArityDeferred bool
	Params        int
}

type setParserState struct {
	nodes    chunkArena[SetExpr]
	tails    chunkArena[SetTail]
	orders   chunkArena[SetOrderTerm]
	outputs  chunkArena[SetOutputColumn]
	contexts chunkArena[setParseContext]

	orderScratch []SetOrderTerm
	expression   SetExpression
	parser       setExpressionParser
	sized        bool
}

func (s *setParserState) reset() {
	if !s.sized {
		s.nodes.first = 8
		s.tails.first = 2
		s.orders.first = 4
		s.outputs.first = 4
		s.contexts.first = 4
		s.sized = true
	}
	s.nodes.rewind()
	s.tails.rewind()
	s.orders.rewind()
	s.outputs.rewind()
	s.contexts.rewind()
	s.orderScratch = s.orderScratch[:0]
	s.expression = SetExpression{}
	s.parser = setExpressionParser{}
}

func (p *Parser) setState() *setParserState {
	if p.set == nil {
		p.set = new(setParserState)
		p.set.reset()
	}
	return p.set
}

type setParseContext struct {
	scope *cteScope
	local cteScope
	first bool
}

type setExpressionParser struct {
	owner *Parser
	state *setParserState
	lx    lexer
	tok   token

	params int
	depth  int
	nodes  int
}

func isSetOperatorToken(tok token) bool {
	return tok.kind == tokIdent &&
		(tok.kw == kwUnion || tok.kw == kwIntersect || tok.kw == kwExcept)
}

func (p *Parser) parseSetStatement(start int) error {
	dst := p.out
	if dst == nil {
		return p.errAt(start, "set expression has no SELECT destination")
	}
	// The ordinary parser reached a top-level operator without resolving its
	// partial first SELECT. Discard that probe and reuse its child parsers for
	// the lossless expression parse. Captures produced by nested subqueries in
	// the probe are rolled back to the owning LATERAL's exact entry marks.
	p.rollbackSetProbeLateral()
	if p.nested != nil {
		p.nested.used = 0
	}
	*dst = SelectStmt{}
	p.params = 0
	state := p.setState()
	sp := &state.parser
	sp.owner, sp.state = p, state
	sp.lx = lexer{
		src: p.lx.src, pos: start, cancel: p.cancel,
		nextCancelByte: start + parserCancelByteInterval,
	}
	sp.advance()
	context := state.contexts.one()
	*context = setParseContext{scope: p.outerCTEs, first: true}
	root, tail, err := sp.parseQuery(context)
	if err != nil {
		return err
	}
	if err := sp.expectEnd(); err != nil {
		return err
	}
	if sp.lx.cancelErr != nil {
		return sp.lx.cancelErr
	}
	first := root.First
	if first == nil {
		return p.errAt(start, "set expression has no syntactic first operand")
	}
	*dst = *first
	outputs := sp.buildOutputs(first)
	state.expression = SetExpression{
		Root: root, First: first, Outputs: outputs, Tail: tail,
		Pos: start, ArityDeferred: root.ArityDeferred, Params: sp.params,
	}
	dst.Set = &state.expression
	dst.Params = sp.params
	dst.ParamBase = 0
	p.params = sp.params
	return nil
}

func (p *Parser) rollbackSetProbeLateral() {
	if p.lateral == nil || p.lateral.capture == nil {
		return
	}
	capture := p.lateral.capture
	if capture.owner == nil || capture.owner.lateral == nil {
		return
	}
	state := capture.owner.lateral
	state.bindingScratch = state.bindingScratch[:capture.bindingBase]
	state.referenceScratch = state.referenceScratch[:capture.referenceBase]
	if capture.forwardBase <= len(state.forward) {
		state.forward = state.forward[:capture.forwardBase]
	}
}

func (s *setExpressionParser) advance() { s.tok = s.lx.next() }

func (s *setExpressionParser) errHere(message string) error {
	if s.lx.cancelErr != nil {
		return s.lx.cancelErr
	}
	if s.tok.kind == tokError {
		message = s.tok.text
	}
	return s.owner.errAt(s.tok.pos, message)
}

func (s *setExpressionParser) parseQuery(
	context *setParseContext,
) (*SetExpr, *SetTail, error) {
	root, err := s.parseUnionExcept(context)
	if err != nil {
		return nil, nil, err
	}
	tail, err := s.parseTail(root.First)
	if err != nil {
		return nil, nil, err
	}
	if tail != nil && isSetOperatorToken(s.tok) {
		return nil, nil, s.owner.errfAt(
			tail.Pos,
			"ORDER BY/LIMIT/OFFSET before %s is operand-local only inside parentheses; parenthesize that operand or move the clause after the complete set expression",
			s.tok.text,
		)
	}
	return root, tail, nil
}

func (s *setExpressionParser) parseUnionExcept(
	context *setParseContext,
) (*SetExpr, error) {
	left, err := s.parseIntersect(context)
	if err != nil {
		return nil, err
	}
	for s.tok.kind == tokIdent &&
		(s.tok.kw == kwUnion || s.tok.kw == kwExcept) {
		operation, pos, err := s.parseOperation()
		if err != nil {
			return nil, err
		}
		right, err := s.parseIntersect(context)
		if err != nil {
			return nil, err
		}
		left, err = s.binary(operation, pos, left, right)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (s *setExpressionParser) parseIntersect(
	context *setParseContext,
) (*SetExpr, error) {
	left, err := s.parsePrimary(context)
	if err != nil {
		return nil, err
	}
	for s.tok.kind == tokIdent && s.tok.kw == kwIntersect {
		operation, pos, err := s.parseOperation()
		if err != nil {
			return nil, err
		}
		right, err := s.parsePrimary(context)
		if err != nil {
			return nil, err
		}
		left, err = s.binary(operation, pos, left, right)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (s *setExpressionParser) parsePrimary(
	context *setParseContext,
) (*SetExpr, error) {
	if s.depth >= maxSetExpressionDepth {
		return nil, s.owner.errfAt(
			s.tok.pos, "set-expression nesting exceeds %d levels", maxSetExpressionDepth,
		)
	}
	if s.tok.kind == tokLParen {
		base, open := s.params, s.tok.pos
		s.depth++
		s.advance()
		innerContext := s.state.contexts.one()
		*innerContext = setParseContext{scope: context.scope, first: true}
		child, tail, err := s.parseQuery(innerContext)
		s.depth--
		if err != nil {
			return nil, err
		}
		if s.tok.kind != tokRParen {
			return nil, s.errHere("expected ')' after parenthesized set expression")
		}
		close := s.tok.pos
		s.advance()
		context.first = false
		node, err := s.node(open)
		if err != nil {
			return nil, err
		}
		*node = SetExpr{
			Kind: SetGroupExpr, Child: child, Tail: tail, First: child.First,
			Columns: child.Columns, ArityDeferred: child.ArityDeferred,
			ParamBase: base, Params: s.params - base, Pos: open, End: close,
		}
		return node, nil
	}
	if s.tok.kind != tokIdent ||
		(s.tok.kw != kwSelect && s.tok.kw != kwWith) {
		if s.tok.kind == tokIdent && (s.tok.kw == kwValues || s.tok.kw == kwTable) {
			return nil, newFeatureNotSupportedError(
				s.owner.lx.src, s.tok.pos,
				"VALUES and TABLE set operands are not supported yet; write a SELECT operand",
			)
		}
		return nil, s.errHere("expected SELECT or a parenthesized set expression")
	}
	if s.tok.kw == kwWith && !context.first {
		return nil, s.owner.errAt(
			s.tok.pos,
			"a WITH clause after a set operator must be parenthesized so its scope is explicit",
		)
	}
	leaf, err := s.parseLeaf(context.scope)
	if err != nil {
		return nil, err
	}
	if context.first && leaf.Select.With != nil {
		context.local = cteScope{
			defs: leaf.Select.With.CTEs, outer: context.scope,
		}
		context.scope = &context.local
	}
	context.first = false
	return leaf, nil
}

func (s *setExpressionParser) parseLeaf(scope *cteScope) (*SetExpr, error) {
	start := s.tok.pos
	depth := 0
	for {
		if depth == 0 {
			switch {
			case isSetOperatorToken(s.tok),
				s.tok.kind == tokRParen,
				s.tok.kind == tokSemicolon,
				s.tok.kind == tokEOF,
				s.tok.kind == tokIdent &&
					(s.tok.kw == kwOrder || s.tok.kw == kwLimit ||
						s.tok.kw == kwOffset || s.tok.kw == kwFetch):
				goto scanned
			}
		}
		switch s.tok.kind {
		case tokError:
			return nil, s.errHere(s.tok.text)
		case tokLParen:
			depth++
		case tokRParen:
			depth--
		case tokEOF:
			goto scanned
		}
		s.advance()
	}

scanned:
	end := s.tok.pos
	if end <= start {
		return nil, s.errHere("expected a complete SELECT set operand")
	}
	child := s.owner.nextSetLeafParser()
	child.cancel = s.owner.cancel
	var outerRanges *lateralRangeScope
	var capture *lateralCapture
	if s.owner.lateral != nil {
		outerRanges = s.owner.lateral.outerRanges
		capture = s.owner.lateral.capture
	}
	query := &child.sel
	if err := child.parseSelectText(
		query, s.owner.lx.src[start:end], scope, outerRanges, capture,
		s.owner.nesting, false,
	); err != nil {
		return nil, s.owner.rebaseSubqueryError(err, start)
	}
	if query.Set != nil {
		return nil, s.owner.errAt(start,
			"an unparenthesized set operand must be one SELECT")
	}
	if s.params > maxParams-query.Params {
		return nil, s.owner.errfAt(
			start, "a statement may hold at most %d placeholders", maxParams,
		)
	}
	base := s.params
	query.ParamBase = base
	s.params += query.Params
	shiftSelectPositions(query, start)
	columns, deferred := setSelectArity(query)
	node, err := s.node(start)
	if err != nil {
		return nil, err
	}
	*node = SetExpr{
		Kind: SetSelectExpr, Select: query, First: query,
		Columns: columns, ArityDeferred: deferred,
		ParamBase: base, Params: query.Params, Pos: start, End: -1,
	}
	return node, nil
}

func (p *Parser) nextSetLeafParser() *Parser {
	if p.nested == nil {
		p.nested = new(nestedParsers)
	}
	if p.nested.used == len(p.nested.parsers) {
		p.nested.parsers = append(p.nested.parsers, new(Parser))
	}
	child := p.nested.parsers[p.nested.used]
	p.nested.used++
	return child
}

func (s *setExpressionParser) parseOperation() (SetOperation, int, error) {
	keyword, pos := s.tok.kw, s.tok.pos
	s.advance()
	all := false
	switch {
	case s.tok.kind == tokIdent && s.tok.kw == kwAll:
		all = true
		s.advance()
	case s.tok.kind == tokIdent && s.tok.kw == kwDistinct:
		s.advance()
	}
	if s.tok.kind == tokIdent && (s.tok.kw == kwAll || s.tok.kw == kwDistinct) {
		return 0, pos, s.errHere("a set operator accepts exactly one of ALL or DISTINCT")
	}
	switch keyword {
	case kwUnion:
		if all {
			return SetUnionAll, pos, nil
		}
		return SetUnionDistinct, pos, nil
	case kwIntersect:
		if all {
			return SetIntersectAll, pos, nil
		}
		return SetIntersectDistinct, pos, nil
	default:
		if all {
			return SetExceptAll, pos, nil
		}
		return SetExceptDistinct, pos, nil
	}
}

func (s *setExpressionParser) binary(
	operation SetOperation,
	pos int,
	left, right *SetExpr,
) (*SetExpr, error) {
	if !left.ArityDeferred && !right.ArityDeferred && left.Columns != right.Columns {
		return nil, s.owner.errfAt(
			pos,
			"set-operation operands have %d and %d output columns; set compatibility is ordinal",
			left.Columns, right.Columns,
		)
	}
	node, err := s.node(pos)
	if err != nil {
		return nil, err
	}
	columns := left.Columns
	if left.ArityDeferred {
		columns = right.Columns
	}
	*node = SetExpr{
		Kind: SetBinaryExpr, Operation: operation, Left: left, Right: right,
		First: left.First, Columns: columns,
		ArityDeferred: left.ArityDeferred || right.ArityDeferred,
		ParamBase:     left.ParamBase,
		Params:        s.params - left.ParamBase,
		Pos:           pos,
		End:           -1,
	}
	return node, nil
}

func (s *setExpressionParser) node(pos int) (*SetExpr, error) {
	s.nodes++
	if s.nodes > maxClauseItems*2-1 {
		return nil, s.owner.errfAt(
			pos, "a set expression may hold at most %d nodes", maxClauseItems*2-1,
		)
	}
	return s.state.nodes.one(), nil
}

func (s *setExpressionParser) parseTail(first *SelectStmt) (*SetTail, error) {
	if !s.atTail() {
		return nil, nil
	}
	tail := s.state.tails.one()
	tail.Pos = s.tok.pos
	tail.ParamBase = s.params
	if s.tok.kind == tokIdent && s.tok.kw == kwOrder {
		if err := s.parseOrderBy(tail, first); err != nil {
			return nil, err
		}
	}
	for s.tok.kind == tokIdent {
		switch s.tok.kw {
		case kwLimit:
			if tail.Limit != nil {
				return nil, s.errHere("LIMIT is given twice")
			}
			s.advance()
			operand, err := s.parseRowCount("LIMIT")
			if err != nil {
				return nil, err
			}
			tail.Limit = operand
		case kwOffset:
			if tail.Offset != nil {
				return nil, s.errHere("OFFSET is given twice")
			}
			s.advance()
			operand, err := s.parseRowCount("OFFSET")
			if err != nil {
				return nil, err
			}
			tail.Offset = operand
		case kwFetch:
			return nil, s.errHere("FETCH FIRST is not supported; write LIMIT")
		default:
			tail.Params = s.params - tail.ParamBase
			return tail, nil
		}
	}
	tail.Params = s.params - tail.ParamBase
	return tail, nil
}

func (s *setExpressionParser) atTail() bool {
	return s.tok.kind == tokIdent &&
		(s.tok.kw == kwOrder || s.tok.kw == kwLimit ||
			s.tok.kw == kwOffset || s.tok.kw == kwFetch)
}

func (s *setExpressionParser) parseOrderBy(
	tail *SetTail,
	first *SelectStmt,
) error {
	s.advance() // ORDER
	if s.tok.kind != tokIdent || s.tok.kw != kwBy {
		return s.errHere("expected BY after ORDER")
	}
	s.advance()
	base := len(s.state.orderScratch)
	for {
		term, err := s.parseOrderTerm(first)
		if err != nil {
			s.state.orderScratch = s.state.orderScratch[:base]
			return err
		}
		s.state.orderScratch = append(s.state.orderScratch, term)
		if len(s.state.orderScratch)-base > maxClauseItems {
			return s.owner.errfAt(term.Pos,
				"ORDER BY may hold at most %d keys", maxClauseItems)
		}
		if s.tok.kind != tokComma {
			break
		}
		s.advance()
	}
	run := s.state.orders.allocDirty(len(s.state.orderScratch) - base)
	copy(run, s.state.orderScratch[base:])
	s.state.orderScratch = s.state.orderScratch[:base]
	tail.OrderBy = run
	return nil
}

func (s *setExpressionParser) parseOrderTerm(
	first *SelectStmt,
) (SetOrderTerm, error) {
	term := SetOrderTerm{Pos: s.tok.pos}
	if s.tok.kind == tokNumber {
		return term, s.errHere(
			"set ORDER BY does not accept an output position; name a first-operand output",
		)
	}
	if s.tok.kind != tokIdent && s.tok.kind != tokQuotedIdent {
		return term, s.errHere("expected a first-operand output name in set ORDER BY")
	}
	if s.tok.kind == tokIdent && reserved(s.tok.kw) {
		return term, s.errHere("expected an output name; quote a reserved word")
	}
	name := s.owner.internToken(s.tok)
	s.advance()
	if s.tok.kind == tokDot || s.tok.kind == tokLBracket {
		return term, s.errHere(
			"set ORDER BY is scoped to completed output names, not operand input paths; alias the first operand output and order by that alias",
		)
	}
	match := -1
	for column := range first.Columns {
		candidate, deferred := s.outputName(first, &first.Columns[column])
		if deferred || candidate != name {
			continue
		}
		if match >= 0 {
			return term, s.owner.errfAt(
				term.Pos, "set ORDER BY output name %q is ambiguous; give the first operand unique aliases", name,
			)
		}
		match = column
	}
	if match < 0 {
		return term, s.owner.errfAt(
			term.Pos, "set ORDER BY name %q is not an output of the syntactic first operand", name,
		)
	}
	term.Name, term.Output = name, match+1
	if s.tok.kind == tokIdent && s.tok.kw == kwAsc {
		s.advance()
	} else if s.tok.kind == tokIdent && s.tok.kw == kwDesc {
		term.Desc = true
		s.advance()
	}
	if s.tok.kind == tokIdent && s.tok.kw == kwNulls {
		return term, s.errHere(
			"NULLS FIRST/LAST is not supported: the engine sorts nulls first ascending and last descending",
		)
	}
	if s.tok.kind == tokIdent && s.tok.kw == kwCollate {
		return term, s.errHere("COLLATE is not supported: strings compare by decoded content")
	}
	return term, nil
}

func (s *setExpressionParser) parseRowCount(clause string) (*Operand, error) {
	pos := s.tok.pos
	if s.tok.kind == tokParam {
		if s.params >= maxParams {
			return nil, s.owner.errfAt(pos,
				"a statement may hold at most %d placeholders", maxParams)
		}
		operand := s.owner.ops.one()
		*operand = Operand{Kind: OperandParam, Ordinal: s.params, Pos: pos}
		s.params++
		s.advance()
		return operand, nil
	}
	if s.tok.kind != tokNumber {
		return nil, s.owner.errfAt(pos,
			"expected a non-negative integer or '?' after %s", clause)
	}
	text := s.tok.text
	if _, err := strconv.ParseInt(text, 10, 64); err != nil {
		return nil, s.owner.errfAt(pos,
			"%s must be a non-negative whole number that fits in 64 bits", clause)
	}
	if text[0] == '-' {
		return nil, s.owner.errfAt(pos, "%s must not be negative", clause)
	}
	operand := s.owner.ops.one()
	*operand = Operand{
		Kind: OperandNumber, Text: s.owner.internString(text), Pos: pos,
	}
	s.advance()
	return operand, nil
}

func (s *setExpressionParser) buildOutputs(
	first *SelectStmt,
) []SetOutputColumn {
	outputs := s.state.outputs.allocDirty(len(first.Columns))
	for column := range first.Columns {
		name, dynamic := s.outputName(first, &first.Columns[column])
		outputs[column] = SetOutputColumn{
			Name: name, Pos: first.Columns[column].Pos, Deferred: dynamic,
		}
	}
	return outputs
}

func (s *setExpressionParser) outputName(
	statement *SelectStmt,
	column *ResultColumn,
) (string, bool) {
	if column.Alias != "" {
		return column.Alias, false
	}
	if column.Window != nil {
		return "", true
	}
	if column.Path != nil && len(column.Path.Segments) == 0 && column.Agg == AggNone {
		return "*", true
	}
	buffer := s.owner.tmp[:0]
	if column.Agg != AggNone {
		buffer = append(buffer, setAggregateName(column.Agg)...)
		buffer = append(buffer, '(')
	}
	if column.Path == nil {
		buffer = append(buffer, '*')
	} else {
		if len(statement.From) > 1 && column.Path.Source > 0 &&
			column.Path.MergedUsing == 0 && column.Path.Source < len(statement.From) {
			buffer = append(buffer, statement.From[column.Path.Source].Alias...)
			buffer = append(buffer, '.')
		}
		buffer = column.Path.AppendSpec(buffer)
	}
	if column.Agg != AggNone {
		buffer = append(buffer, ')')
	}
	name := s.owner.intern(buffer)
	s.owner.tmp = buffer[:0]
	return name, false
}

func setAggregateName(kind AggKind) string {
	switch kind {
	case AggCount:
		return "count"
	case AggSum:
		return "sum"
	case AggAvg:
		return "avg"
	case AggMin:
		return "min"
	default:
		return "max"
	}
}

func setSelectArity(statement *SelectStmt) (int, bool) {
	for column := range statement.Columns {
		item := &statement.Columns[column]
		if item.Agg == AggNone && item.Path != nil && len(item.Path.Segments) == 0 {
			return len(statement.Columns), true
		}
	}
	return len(statement.Columns), false
}

// shiftSetExpressionPositions rebases every retained source offset while
// avoiding the one shallow-mirrored first SelectStmt. The outward SelectStmt
// has already shifted the slices it shares with that leaf.
func shiftSetExpressionPositions(
	expression *SetExpression,
	delta int,
	mirroredFirst *SelectStmt,
) {
	if expression == nil {
		return
	}
	expression.Pos += delta
	for i := range expression.Outputs {
		expression.Outputs[i].Pos += delta
	}
	shiftSetTailPositions(expression.Tail, delta)
	shiftSetExprPositions(expression.Root, delta, mirroredFirst)
}

func shiftSetExprPositions(expr *SetExpr, delta int, mirroredFirst *SelectStmt) {
	if expr == nil {
		return
	}
	expr.Pos += delta
	if expr.End >= 0 {
		expr.End += delta
	}
	switch expr.Kind {
	case SetSelectExpr:
		if expr.Select != mirroredFirst {
			shiftSelectPositions(expr.Select, delta)
		}
	case SetBinaryExpr:
		shiftSetExprPositions(expr.Left, delta, mirroredFirst)
		shiftSetExprPositions(expr.Right, delta, mirroredFirst)
	case SetGroupExpr:
		shiftSetTailPositions(expr.Tail, delta)
		shiftSetExprPositions(expr.Child, delta, mirroredFirst)
	}
}

func shiftSetTailPositions(tail *SetTail, delta int) {
	if tail == nil {
		return
	}
	tail.Pos += delta
	for i := range tail.OrderBy {
		tail.OrderBy[i].Pos += delta
	}
	if tail.Limit != nil {
		tail.Limit.Pos += delta
	}
	if tail.Offset != nil {
		tail.Offset.Pos += delta
	}
}

func (s *setExpressionParser) expectEnd() error {
	if s.tok.kind == tokSemicolon {
		s.advance()
		if s.tok.kind != tokEOF {
			return s.errHere("only one statement may be parsed at a time")
		}
	}
	if s.tok.kind != tokEOF {
		if s.tok.kind == tokRParen {
			return s.errHere("unexpected ')' without a matching set-expression '('")
		}
		return s.errHere("unexpected trailing input after set expression")
	}
	return nil
}
