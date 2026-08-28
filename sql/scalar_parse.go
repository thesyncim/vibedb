package sql

// scalarExprContext records where a computed value is authored. Aggregates
// are legal in SELECT, HAVING, and ORDER BY after grouping, but not in WHERE or
// ON, whose rows have not been reduced yet.
type scalarExprContext uint8

const (
	scalarSelect scalarExprContext = iota
	scalarWhere
	scalarHaving
	scalarJoin
	scalarOrder
)

func (c scalarExprContext) clause() string {
	switch c {
	case scalarWhere:
		return "WHERE"
	case scalarHaving:
		return "HAVING"
	case scalarJoin:
		return "ON"
	case scalarOrder:
		return "ORDER BY"
	default:
		return "SELECT"
	}
}

type scalarParserState struct {
	nodes       chunkArena[ScalarExpr]
	whenRuns    chunkArena[ScalarWhen]
	whenScratch []ScalarWhen
	depth       int
	caseItems   int
	caseTruth   int
	sized       bool
}

func (s *scalarParserState) reset() {
	if !s.sized {
		s.nodes.first = 16
		s.whenRuns.first = 8
		s.sized = true
	}
	s.nodes.rewind()
	s.whenRuns.rewind()
	s.whenScratch = s.whenScratch[:0]
	s.depth = 0
	s.caseItems = 0
	s.caseTruth = 0
}

func (p *Parser) scalarState() *scalarParserState {
	if p.scalar == nil {
		p.scalar = new(scalarParserState)
		p.scalar.reset()
	}
	return p.scalar
}

func (p *Parser) newScalar(kind ScalarExprKind, pos int) *ScalarExpr {
	node := p.scalarState().nodes.one()
	*node = ScalarExpr{Kind: kind, Pos: pos}
	return node
}

func scalarStarts(tok token) bool {
	switch tok.kind {
	case tokNumber, tokString, tokParam, tokLParen, tokPlus, tokMinus:
		return true
	case tokIdent:
		return tok.kw == kwTrue || tok.kw == kwFalse || tok.kw == kwNull ||
			tok.kw == kwCase || tok.kw == kwCast
	default:
		return false
	}
}

func scalarContinues(tok token) bool {
	switch tok.kind {
	case tokPlus, tokMinus, tokStar, tokSlash, tokPercent, tokConcat, tokDoubleColon, tokJSONText:
		return true
	case tokNumber:
		// The legacy lexer keeps a directly adjacent negative sign in a JSON
		// number token. After a completed left operand, "a-1" is therefore
		// subtraction; the parser peels the sign without changing literal
		// handling in the many non-expression clauses that accept -1.
		return len(tok.text) > 1 && tok.text[0] == '-'
	default:
		return false
	}
}

func (p *Parser) scalarFromColumn(col ResultColumn) *ScalarExpr {
	kind := ScalarPath
	if col.Agg != AggNone {
		kind = ScalarAggregate
	}
	node := p.newScalar(kind, col.Pos)
	node.Path, node.Agg = col.Path, col.Agg
	return node
}

func (p *Parser) parseScalarExpression(ctx scalarExprContext) (*ScalarExpr, error) {
	return p.parseScalarBinary(nil, 1, ctx)
}

func (p *Parser) continueScalarExpression(
	left *ScalarExpr,
	ctx scalarExprContext,
) (*ScalarExpr, error) {
	return p.parseScalarBinary(left, 1, ctx)
}

// parseScalarBinary is precedence climbing over the fixed SQL scalar ladder:
// concatenation, addition/subtraction, multiplication/division/modulo.
func (p *Parser) parseScalarBinary(
	left *ScalarExpr,
	minimum int,
	ctx scalarExprContext,
) (*ScalarExpr, error) {
	var err error
	if left == nil {
		left, err = p.parseScalarUnary(ctx)
		if err != nil {
			return nil, err
		}
	}
	for {
		if p.tok.kind == tokJSONText {
			if left.Kind != ScalarPath || left.Path == nil {
				return nil, newFeatureNotSupportedError(p.lx.src, p.tok.pos,
					"JSON ->> requires a stored JSON path; text extraction must end the accessor chain")
			}
			pos := p.tok.pos
			p.advance()
			seg, err := p.parseJSONAccessor()
			if err != nil {
				return nil, err
			}
			path := left.Path
			segments := p.segs.allocDirty(len(path.Segments) + 1)
			copy(segments, path.Segments)
			segments[len(path.Segments)] = seg
			path.Segments = segments
			node := p.newScalar(ScalarCast, pos)
			node.Cast, node.Left, node.TargetPos = ScalarCastText, left, pos
			left = node
			continue
		}
		if p.tok.kind == tokDoubleColon {
			return nil, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"the PostgreSQL :: cast shorthand is not supported; use CAST(expression AS type)",
			)
		}
		op, precedence, pos, embedded := scalarBinaryToken(p.tok)
		if precedence < minimum {
			return left, nil
		}
		if embedded {
			// Re-expose the positive numeric body as the right primary. It still
			// aliases the source only until parseOperand interns it below.
			p.tok.text = p.tok.text[1:]
			p.tok.pos++
		} else {
			p.advance()
		}
		right, parseErr := p.parseScalarBinary(nil, precedence+1, ctx)
		if parseErr != nil {
			return nil, parseErr
		}
		node := p.newScalar(ScalarBinary, pos)
		node.Op, node.Left, node.Right = op, left, right
		left = node
	}
}

func scalarBinaryToken(tok token) (ScalarOp, int, int, bool) {
	switch tok.kind {
	case tokConcat:
		return ScalarConcat, 1, tok.pos, false
	case tokPlus:
		return ScalarAdd, 2, tok.pos, false
	case tokMinus:
		return ScalarSubtract, 2, tok.pos, false
	case tokStar:
		return ScalarMultiply, 3, tok.pos, false
	case tokSlash:
		return ScalarDivide, 3, tok.pos, false
	case tokPercent:
		return ScalarModulo, 3, tok.pos, false
	case tokNumber:
		if len(tok.text) > 1 && tok.text[0] == '-' {
			return ScalarSubtract, 2, tok.pos, true
		}
	}
	return 0, 0, tok.pos, false
}

func (p *Parser) parseScalarUnary(ctx scalarExprContext) (*ScalarExpr, error) {
	state := p.scalarState()
	if state.depth >= maxExprDepth {
		return nil, p.errHere("a scalar expression may nest at most 64 levels")
	}
	state.depth++
	defer func() { state.depth-- }()
	if p.tok.kind != tokPlus && p.tok.kind != tokMinus {
		return p.parseScalarPrimary(ctx)
	}
	pos, op := p.tok.pos, ScalarPositive
	if p.tok.kind == tokMinus {
		op = ScalarNegative
	}
	p.advance()
	child, err := p.parseScalarUnary(ctx)
	if err != nil {
		return nil, err
	}
	node := p.newScalar(ScalarUnary, pos)
	node.Op, node.Left = op, child
	return node, nil
}

func (p *Parser) parseScalarPrimary(ctx scalarExprContext) (*ScalarExpr, error) {
	pos := p.tok.pos
	switch {
	case p.tok.kind == tokLParen:
		p.advance()
		expr, err := p.parseScalarExpression(ctx)
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokRParen, "')' after scalar expression"); err != nil {
			return nil, err
		}
		return expr, nil
	case p.atKeyword(kwNull):
		p.advance()
		return p.newScalar(ScalarNull, pos), nil
	case p.atKeyword(kwCase):
		return p.parseScalarCase(ctx)
	case p.atKeyword(kwCast):
		return p.parseScalarCast(ctx)
	case p.tok.kind == tokNumber, p.tok.kind == tokString,
		p.tok.kind == tokParam, p.atKeyword(kwTrue), p.atKeyword(kwFalse):
		value, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarLiteral, pos)
		node.Value = value
		return node, nil
	}

	switch agg, head, state := p.tryAggregate(); state {
	case aggCall:
		if ctx == scalarWhere || ctx == scalarJoin {
			return nil, newFeatureNotSupportedError(
				p.lx.src, head.pos,
				"an aggregate is not allowed in "+ctx.clause()+" because rows are filtered before reduction",
			)
		}
		path, err := p.parseAggregateArgs(agg)
		if err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarAggregate, head.pos)
		node.Agg, node.Path = agg, path
		return node, nil
	case aggHeadOnly:
		path, err := p.continuePath(head, false)
		if err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarPath, path.Pos)
		node.Path = path
		return node, nil
	default:
		path, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarPath, path.Pos)
		node.Path = path
		return node, nil
	}
}

func (p *Parser) parseScalarCast(ctx scalarExprContext) (*ScalarExpr, error) {
	pos := p.tok.pos
	p.advance() // CAST
	if err := p.expect(tokLParen, "'(' after CAST"); err != nil {
		return nil, err
	}
	child, err := p.parseScalarExpression(ctx)
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword(kwAs, "AS in CAST"); err != nil {
		return nil, err
	}
	targetPos := p.tok.pos
	if p.tok.kind != tokIdent {
		return nil, newFeatureNotSupportedError(
			p.lx.src, targetPos,
			"CAST requires one supported unquoted target: TEXT, BOOLEAN, NUMERIC, DECIMAL, or JSON",
		)
	}
	target, ok := scalarCastTargetOf(p.tok.text)
	if !ok {
		return nil, newFeatureNotSupportedError(
			p.lx.src, targetPos,
			"CAST target "+p.tok.text+" is not supported; use TEXT, BOOLEAN, NUMERIC, DECIMAL, or JSON",
		)
	}
	p.advance()
	if p.tok.kind != tokRParen {
		return nil, newFeatureNotSupportedError(
			p.lx.src, p.tok.pos,
			"CAST type modifiers, arrays, collations, and multi-word targets are not supported",
		)
	}
	p.advance()
	node := p.newScalar(ScalarCast, pos)
	node.Cast, node.Left, node.TargetPos = target, child, targetPos
	return node, nil
}

func scalarCastTargetOf(text string) (ScalarCastTarget, bool) {
	switch {
	case equalFoldASCII(text, "text"):
		return ScalarCastText, true
	case equalFoldASCII(text, "boolean"), equalFoldASCII(text, "bool"):
		return ScalarCastBoolean, true
	case equalFoldASCII(text, "numeric"), equalFoldASCII(text, "decimal"):
		return ScalarCastNumeric, true
	case equalFoldASCII(text, "json"):
		return ScalarCastJSON, true
	default:
		return 0, false
	}
}

func equalFoldASCII(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		a, b := left[i], right[i]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}

func shiftScalarPositions(expr *ScalarExpr, delta int) {
	if expr == nil {
		return
	}
	expr.Pos += delta
	shiftPathPosition(expr.Path, delta)
	if expr.Kind == ScalarLiteral {
		expr.Value.Pos += delta
	}
	if expr.Kind == ScalarCast {
		expr.TargetPos += delta
	}
	shiftScalarPositions(expr.Left, delta)
	shiftScalarPositions(expr.Right, delta)
	shiftScalarPositions(expr.Else, delta)
	for i := range expr.Whens {
		expr.Whens[i].Pos += delta
		shiftExprPositions(expr.Whens[i].Predicate, delta)
		shiftScalarPositions(expr.Whens[i].Match, delta)
		shiftScalarPositions(expr.Whens[i].Result, delta)
	}
}
