package sql

func conditionalScalarOp(tok token) (ScalarOp, bool) {
	if tok.kind != tokIdent {
		return 0, false
	}
	switch {
	case equalFoldASCII(tok.text, "coalesce"):
		return ScalarCoalesce, true
	case equalFoldASCII(tok.text, "greatest"):
		return ScalarGreatest, true
	case equalFoldASCII(tok.text, "least"):
		return ScalarLeast, true
	case equalFoldASCII(tok.text, "nullif"):
		return ScalarNullIf, true
	}
	return 0, false
}

func (p *Parser) parseConditionalScalar(op ScalarOp, ctx scalarExprContext) (*ScalarExpr, error) {
	head := p.tok
	p.advance()
	if p.tok.kind != tokLParen {
		path, err := p.continuePath(head, ctx == scalarSelect)
		if err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarPath, head.pos)
		node.Path = path
		return node, nil
	}
	p.advance()
	state := p.scalarState()
	start := len(state.arguments)
	defer func() {
		clear(state.arguments[start:])
		state.arguments = state.arguments[:start]
	}()
	for {
		if len(state.arguments)-start == 1024 {
			return nil, p.errHere("a conditional expression accepts at most 1024 arguments")
		}
		arg, err := p.parseScalarExpression(ctx)
		if err != nil {
			return nil, err
		}
		state.arguments = append(state.arguments, arg)
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(tokRParen, "')' after conditional expression arguments"); err != nil {
		return nil, err
	}
	args := state.arguments[start:]
	if op == ScalarNullIf && len(args) != 2 {
		return nil, p.errfAt(head.pos, "NULLIF requires exactly two arguments")
	}
	// Reuse the BOOL/TEXT typed-string common-type rule already used by CASE.
	// This also validates an invalid unknown Boolean in an unselected argument.
	base := len(state.whenScratch)
	for _, arg := range args {
		state.whenScratch = append(state.whenScratch, ScalarWhen{Result: arg})
	}
	err := p.coerceTypedCaseUnknownResults(state.whenScratch[base:], nil)
	clear(state.whenScratch[base:])
	state.whenScratch = state.whenScratch[:base]
	if err != nil {
		return nil, err
	}
	// Balance associative operators to keep large argument lists within the
	// existing bounded scalar traversal stack. COALESCE still visits left to
	// right; unlike a CASE rewrite, no argument subtree is duplicated.
	if len(args) == 1 {
		node := p.newScalar(ScalarBinary, head.pos)
		node.Op, node.Left, node.Right = op, args[0], p.newScalar(ScalarNull, head.pos)
		return node, nil
	}
	return p.conditionalScalarTree(op, args, head.pos), nil
}

func (p *Parser) conditionalScalarTree(op ScalarOp, args []*ScalarExpr, pos int) *ScalarExpr {
	if len(args) == 1 {
		return args[0]
	}
	mid := len(args) / 2
	node := p.newScalar(ScalarBinary, pos)
	node.Op = op
	node.Left = p.conditionalScalarTree(op, args[:mid], pos)
	node.Right = p.conditionalScalarTree(op, args[mid:], pos)
	return node
}
