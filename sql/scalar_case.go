package sql

// parseScalarCase retains CASE as ordered control flow. In particular, it does
// not normalize searched CASE to arithmetic or eager boolean expressions:
// lowering needs the authored arm order to keep dead WHEN and result branches
// unobserved.
func (p *Parser) parseScalarCase(ctx scalarExprContext) (*ScalarExpr, error) {
	pos := p.tok.pos
	p.advance() // CASE

	state := p.scalarState()
	base := len(state.whenScratch)
	defer func() { state.whenScratch = state.whenScratch[:base] }()

	var selector *ScalarExpr
	var err error
	if !p.atKeyword(kwWhen) {
		selector, err = p.parseScalarExpression(ctx)
		if err != nil {
			return nil, err
		}
	}
	if !p.atKeyword(kwWhen) {
		return nil, p.errHere("CASE requires at least one WHEN arm")
	}

	for p.atKeyword(kwWhen) {
		whenPos := p.tok.pos
		p.advance()
		state.caseItems++
		if state.caseItems > maxClauseItems {
			return nil, p.errfAt(whenPos,
				"a statement may hold at most %d CASE WHEN arms", maxClauseItems)
		}
		arm := ScalarWhen{Pos: whenPos}
		if selector == nil {
			state.caseTruth++
			arm.Predicate, err = p.parseCasePredicate(ctx)
			state.caseTruth--
		} else {
			arm.Match, err = p.parseScalarExpression(ctx)
		}
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword(kwThen, "THEN after CASE WHEN condition"); err != nil {
			return nil, err
		}
		arm.Result, err = p.parseScalarExpression(ctx)
		if err != nil {
			return nil, err
		}
		state.whenScratch = append(state.whenScratch, arm)
	}

	var fallback *ScalarExpr
	if p.acceptKeyword(kwElse) {
		fallback, err = p.parseScalarExpression(ctx)
		if err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword(kwEnd, "END after CASE arms"); err != nil {
		return nil, err
	}

	arms := state.whenRuns.allocDirty(len(state.whenScratch) - base)
	copy(arms, state.whenScratch[base:])
	node := p.newScalar(ScalarCase, pos)
	node.Left, node.Whens, node.Else = selector, arms, fallback
	return node, nil
}

func (p *Parser) parseCasePredicate(ctx scalarExprContext) (*Expr, error) {
	// CASE conditions run per input row in the scalar stage. Aggregates inside
	// the condition would need a combined grouped predicate stage; result arms
	// still retain the caller's scalar context and may contain aggregates.
	if ctx == scalarHaving {
		return p.parseExpr(ctxHaving)
	}
	return p.parseExpr(ctxWhere)
}

func (p *Parser) inCaseTruth() bool {
	return p.scalar != nil && p.scalar.caseTruth != 0
}

func caseTruthTerminator(tok token) bool {
	if tok.kind == tokRParen {
		return true
	}
	return tok.kind == tokIdent && (tok.kw == kwAnd || tok.kw == kwOr ||
		tok.kw == kwThen)
}
