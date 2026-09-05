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
	SetValuesExpr
	SetTableExpr
	SetBinaryExpr
	SetGroupExpr
)

// SetValue is one scalar in a VALUES row. Null is separate because Operand's
// zero kind is a string literal and comparison operands deliberately exclude
// NULL. Placeholder ordinals are absolute within the complete set expression.
type SetValue struct {
	Operand       Operand
	Null          bool
	TypedConstant bool
	Cast          ScalarCastTarget
	Pos           int
}

// SetValuesRow retains one authored VALUES tuple in row-major order.
type SetValuesRow struct {
	Values []SetValue
	Pos    int
}

// SetValuesOperand is the lossless scalar VALUES payload of a set leaf.
type SetValuesOperand struct {
	Rows []SetValuesRow
	Pos  int
}

// SetTableOperand retains TABLE's exact relation identity. Select is the
// equivalent parser-owned SELECT * relation used only by ordinary relation
// lowering; keeping both makes TABLE unmissable to AST consumers.
type SetTableOperand struct {
	Ref TableRef
	Pos int
}

// SetExpr is one node in a lossless set-expression tree.
type SetExpr struct {
	Kind      SetExprKind
	Operation SetOperation
	Select    *SelectStmt
	Values    *SetValuesOperand
	Table     *SetTableOperand
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
	Nulls  WindowNullOrder
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
	values   chunkArena[SetValue]
	rows     chunkArena[SetValuesRow]
	operands chunkArena[SetValuesOperand]
	tables   chunkArena[SetTableOperand]
	firsts   chunkArena[SelectStmt]
	columns  chunkArena[ResultColumn]
	paths    chunkArena[PathExpr]
	refs     chunkArena[TableRef]
	tails    chunkArena[SetTail]
	orders   chunkArena[SetOrderTerm]
	outputs  chunkArena[SetOutputColumn]
	contexts chunkArena[setParseContext]

	orderScratch []SetOrderTerm
	valueScratch []SetValue
	rowScratch   []SetValuesRow
	expression   SetExpression
	parser       setExpressionParser
	sized        bool
}

func (s *setParserState) reset() {
	if !s.sized {
		s.nodes.first = 8
		s.values.first = 16
		s.rows.first = 4
		s.operands.first = 2
		s.tables.first = 2
		s.firsts.first = 2
		s.columns.first = 4
		s.paths.first = 2
		s.refs.first = 2
		s.tails.first = 2
		s.orders.first = 4
		s.outputs.first = 4
		s.contexts.first = 4
		s.sized = true
	}
	s.nodes.rewind()
	s.values.rewind()
	s.rows.rewind()
	s.operands.rewind()
	s.tables.rewind()
	s.firsts.rewind()
	s.columns.rewind()
	s.paths.rewind()
	s.refs.rewind()
	s.tails.rewind()
	s.orders.rewind()
	s.outputs.rewind()
	s.contexts.rewind()
	s.orderScratch = s.orderScratch[:0]
	s.valueScratch = s.valueScratch[:0]
	s.rowScratch = s.rowScratch[:0]
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
	// the probe are rolled back to the owning correlation's exact entry marks.
	p.rollbackSetProbeCorrelation()
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

func (p *Parser) rollbackSetProbeCorrelation() {
	if p.correlation == nil || p.correlation.capture == nil {
		return
	}
	capture := p.correlation.capture
	if capture.owner == nil || capture.owner.correlation == nil {
		return
	}
	state := capture.owner.correlation
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
	if s.tok.kind == tokIdent && s.tok.kw == kwValues {
		leaf, err := s.parseValuesLeaf()
		if err != nil {
			return nil, err
		}
		context.first = false
		return leaf, nil
	}
	if s.tok.kind == tokIdent && s.tok.kw == kwTable {
		leaf, err := s.parseTableLeaf(context.scope)
		if err != nil {
			return nil, err
		}
		context.first = false
		return leaf, nil
	}
	if s.tok.kind != tokIdent ||
		(s.tok.kw != kwSelect && s.tok.kw != kwWith) {
		return nil, s.errHere("expected SELECT, VALUES, TABLE, or a parenthesized set expression")
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

func (s *setExpressionParser) parseValuesLeaf() (*SetExpr, error) {
	start, base := s.tok.pos, s.params
	s.advance() // VALUES
	rowsBase := len(s.state.rowScratch)
	valuesBase := len(s.state.valueScratch)
	defer func() {
		s.state.rowScratch = s.state.rowScratch[:rowsBase]
		s.state.valueScratch = s.state.valueScratch[:valuesBase]
	}()
	width := -1
	for {
		rowPos := s.tok.pos
		if s.tok.kind != tokLParen {
			return nil, s.errHere("expected '(' to begin a VALUES row")
		}
		s.advance()
		valueBase := len(s.state.valueScratch)
		for {
			if s.tok.kind == tokRParen {
				return nil, s.errHere("a VALUES row must contain at least one scalar")
			}
			if s.tok.kind == tokError {
				return nil, s.errHere(s.tok.text)
			}
			value, err := s.parseSetValue()
			if err != nil {
				return nil, err
			}
			s.state.valueScratch = append(s.state.valueScratch, value)
			if len(s.state.valueScratch)-valueBase > maxClauseItems {
				return nil, s.owner.errfAt(
					value.Pos, "a VALUES row may hold at most %d columns", maxClauseItems,
				)
			}
			if s.tok.kind != tokComma {
				break
			}
			s.advance()
			if s.tok.kind == tokRParen {
				return nil, s.errHere("expected a scalar after ',' in a VALUES row")
			}
		}
		if s.tok.kind != tokRParen {
			return nil, s.errHere("expected ',' or ')' after a VALUES scalar")
		}
		s.advance()
		values := s.state.values.allocDirty(len(s.state.valueScratch) - valueBase)
		copy(values, s.state.valueScratch[valueBase:])
		s.state.valueScratch = s.state.valueScratch[:valueBase]
		if width < 0 {
			width = len(values)
		} else if len(values) != width {
			return nil, s.owner.errfAt(
				rowPos, "VALUES rows have %d and %d columns; row arity must be uniform",
				width, len(values),
			)
		}
		s.state.rowScratch = append(s.state.rowScratch, SetValuesRow{
			Values: values, Pos: rowPos,
		})
		if len(s.state.rowScratch)-rowsBase > maxClauseItems {
			return nil, s.owner.errfAt(
				rowPos, "a VALUES operand may hold at most %d rows", maxClauseItems,
			)
		}
		if s.tok.kind != tokComma {
			break
		}
		s.advance()
		if s.tok.kind != tokLParen {
			return nil, s.errHere("expected '(' after ',' between VALUES rows")
		}
	}

	rows := s.state.rows.allocDirty(len(s.state.rowScratch) - rowsBase)
	copy(rows, s.state.rowScratch[rowsBase:])
	operand := s.state.operands.one()
	*operand = SetValuesOperand{Rows: rows, Pos: start}
	first := s.valuesFirst(start, base, s.params-base, width)
	node, err := s.node(start)
	if err != nil {
		return nil, err
	}
	*node = SetExpr{
		Kind: SetValuesExpr, Values: operand, First: first,
		Columns: width, ParamBase: base, Params: s.params - base,
		Pos: start, End: -1,
	}
	return node, nil
}

func (s *setExpressionParser) parseSetValue() (SetValue, error) {
	pos := s.tok.pos
	value := SetValue{Pos: pos}
	if target, supported, known := scalarTypedStringHead(s.tok); known {
		head := s.tok
		s.advance()
		if s.tok.kind != tokString {
			return SetValue{}, newFeatureNotSupportedError(
				s.owner.lx.src, pos,
				"VALUES set operands accept scalar literals, NULL, and placeholders; use a SELECT operand for expressions",
			)
		}
		operand, err := s.owner.typedStringOperand(head, s.tok, target, supported)
		if err != nil {
			return SetValue{}, err
		}
		value.Operand = operand
		value.TypedConstant = true
		value.Cast = target
		s.advance()
		return value, nil
	}
	switch s.tok.kind {
	case tokNumber:
		value.Operand = Operand{
			Kind: OperandNumber, Text: s.owner.internString(s.tok.text), Pos: pos,
		}
		s.advance()
		return value, nil
	case tokString:
		value.Operand = Operand{
			Kind: OperandString, Text: s.owner.internToken(s.tok), Pos: pos,
		}
		s.advance()
		return value, nil
	case tokParam:
		if s.params >= maxParams {
			return SetValue{}, s.owner.errfAt(
				pos, "a statement may hold at most %d placeholders", maxParams,
			)
		}
		value.Operand = Operand{
			Kind: OperandParam, Ordinal: s.params, Pos: pos,
		}
		s.params++
		s.advance()
		return value, nil
	case tokIdent:
		switch s.tok.kw {
		case kwTrue:
			value.Operand = Operand{Kind: OperandBool, Bool: true, Pos: pos}
			s.advance()
			return value, nil
		case kwFalse:
			value.Operand = Operand{Kind: OperandBool, Pos: pos}
			s.advance()
			return value, nil
		case kwNull:
			value.Null = true
			s.advance()
			return value, nil
		case kwDefault:
			return SetValue{}, newFeatureNotSupportedError(
				s.owner.lx.src, pos,
				"DEFAULT is not defined for a schemaless VALUES operand; write an explicit scalar or NULL",
			)
		}
	}
	return SetValue{}, newFeatureNotSupportedError(
		s.owner.lx.src, pos,
		"VALUES set operands accept scalar literals, NULL, and placeholders; use a SELECT operand for expressions",
	)
}

func (s *setExpressionParser) valuesFirst(
	pos, base, params, columns int,
) *SelectStmt {
	first := s.state.firsts.one()
	columnRun := s.state.columns.allocDirty(columns)
	for column := 0; column < columns; column++ {
		buffer := s.owner.tmp[:0]
		buffer = append(buffer, "column"...)
		buffer = strconv.AppendInt(buffer, int64(column+1), 10)
		name := s.owner.intern(buffer)
		s.owner.tmp = buffer[:0]
		columnRun[column] = ResultColumn{Alias: name, Pos: pos}
	}
	*first = SelectStmt{
		Columns: columnRun, Params: params, ParamBase: base,
	}
	return first
}

func (s *setExpressionParser) parseTableLeaf(scope *cteScope) (*SetExpr, error) {
	start, base := s.tok.pos, s.params
	s.advance() // TABLE
	if s.tok.kind != tokIdent && s.tok.kind != tokQuotedIdent {
		return nil, s.errHere("expected a collection or common-table-expression name after TABLE")
	}
	if s.tok.kind == tokIdent && reserved(s.tok.kw) {
		return nil, s.owner.errfAt(
			s.tok.pos,
			"expected a collection name, but found the reserved word %s; write %q to use it as a name",
			s.tok.text, s.tok.text,
		)
	}
	namePos := s.tok.pos
	name := s.owner.internToken(s.tok)
	if name == "" {
		return nil, s.owner.errAt(namePos, "a collection name may not be empty")
	}
	s.advance()
	if s.tok.kind == tokDot {
		return nil, newFeatureNotSupportedError(
			s.owner.lx.src, s.tok.pos,
			"qualified TABLE names are not supported; quote a dotted collection name as one identifier",
		)
	}
	if s.tok.kind == tokIdent && s.tok.kw == kwAs {
		return nil, newFeatureNotSupportedError(
			s.owner.lx.src, s.tok.pos,
			"TABLE operands do not carry a range alias; wrap TABLE in a SELECT operand when an alias is needed",
		)
	}
	ref := TableRef{Name: name, Alias: name, Pos: namePos}
	if definition := lookupSetCTE(scope, name); definition != nil {
		ref.Kind = RelationCTE
		ref.Query = definition.Query
	}
	table := s.state.tables.one()
	*table = SetTableOperand{Ref: ref, Pos: start}
	path := s.state.paths.one()
	*path = PathExpr{Source: 0, Pos: start}
	columns := s.state.columns.allocDirty(1)
	columns[0] = ResultColumn{Path: path, Pos: start}
	refs := s.state.refs.allocDirty(1)
	refs[0] = ref
	first := s.state.firsts.one()
	*first = SelectStmt{Columns: columns, From: refs, ParamBase: base}
	node, err := s.node(start)
	if err != nil {
		return nil, err
	}
	*node = SetExpr{
		Kind: SetTableExpr, Select: first, Table: table, First: first,
		Columns: 1, ArityDeferred: true, ParamBase: base, Pos: start, End: -1,
	}
	return node, nil
}

func lookupSetCTE(scope *cteScope, name string) *CommonTableExpr {
	for ; scope != nil; scope = scope.outer {
		for i := len(scope.defs) - 1; i >= 0; i-- {
			if scope.defs[i].Name == name {
				return &scope.defs[i]
			}
		}
	}
	return nil
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
	child.hiddenMutationTable = s.owner.hiddenMutationTable
	child.hiddenMutationAlias = s.owner.hiddenMutationAlias
	var outerRanges *correlationRangeScope
	var capture *correlationCapture
	if s.owner.correlation != nil {
		outerRanges = s.owner.correlation.outerRanges
		capture = s.owner.correlation.capture
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
	if s.tok.kind == tokMinus {
		s.advance()
		if s.tok.kind == tokNumber {
			return term, newInvalidOrderPositionError(
				s.owner.lx.src, term.Pos, "-"+s.tok.text, len(first.Columns),
			)
		}
		return term, newFeatureNotSupportedError(
			s.owner.lx.src, term.Pos,
			"computed scalar expressions in set ORDER BY are not supported",
		)
	}
	if s.tok.kind == tokNumber {
		text := s.tok.text
		position, err := strconv.ParseUint(text, 10, 63)
		if err != nil || position == 0 {
			return term, newInvalidOrderPositionError(
				s.owner.lx.src, term.Pos, text, len(first.Columns),
			)
		}
		for i := range first.Columns {
			if path := first.Columns[i].Path; path != nil && len(path.Segments) == 0 {
				return term, newFeatureNotSupportedError(
					s.owner.lx.src, term.Pos,
					"set ORDER BY output positions cannot bind a wildcard whose expanded result width is known only at prepare time",
				)
			}
		}
		if position > uint64(len(first.Columns)) {
			return term, newInvalidOrderPositionError(
				s.owner.lx.src, term.Pos, text, len(first.Columns),
			)
		}
		term.Output = int(position)
		s.advance()
		if scalarContinues(s.tok) {
			return term, newFeatureNotSupportedError(
				s.owner.lx.src, s.tok.pos,
				"computed scalar expressions in set ORDER BY are not supported; a numeric position must stand alone",
			)
		}
	} else {
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
		deferredOutput := false
		for column := range first.Columns {
			candidate, deferred := s.outputName(first, &first.Columns[column])
			if deferred {
				deferredOutput = true
				continue
			}
			if candidate != name {
				continue
			}
			if match >= 0 {
				return term, s.owner.errfAt(
					term.Pos, "set ORDER BY output name %q is ambiguous; give the first operand unique aliases", name,
				)
			}
			match = column
		}
		if match < 0 && !deferredOutput {
			return term, s.owner.errfAt(
				term.Pos, "set ORDER BY name %q is not an output of the syntactic first operand", name,
			)
		}
		term.Name = name
		if !deferredOutput {
			term.Output = match + 1
		}
	}
	if s.tok.kind == tokIdent && s.tok.kw == kwAsc {
		s.advance()
	} else if s.tok.kind == tokIdent && s.tok.kw == kwDesc {
		term.Desc = true
		s.advance()
	}
	if s.tok.kind == tokIdent && s.tok.kw == kwNulls {
		s.advance()
		if s.tok.kind == tokIdent && s.tok.kw == kwFirst {
			term.Nulls = WindowNullsFirst
		} else if s.tok.kind == tokIdent && s.tok.kw == kwLast {
			term.Nulls = WindowNullsLast
		} else {
			return term, s.errHere("expected FIRST or LAST after NULLS")
		}
		s.advance()
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
	case SetValuesExpr:
		if expr.Values != nil {
			expr.Values.Pos += delta
			for row := range expr.Values.Rows {
				expr.Values.Rows[row].Pos += delta
				for value := range expr.Values.Rows[row].Values {
					item := &expr.Values.Rows[row].Values[value]
					item.Pos += delta
					item.Operand.Pos += delta
				}
			}
		}
		if expr.First != mirroredFirst {
			shiftSelectPositions(expr.First, delta)
		}
	case SetTableExpr:
		if expr.Table != nil {
			expr.Table.Pos += delta
			expr.Table.Ref.Pos += delta
		}
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
