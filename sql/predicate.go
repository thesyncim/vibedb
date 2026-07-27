package sql

// The predicate grammar.
//
// Precedence, loosest to tightest:
//
//	OR
//	AND
//	NOT
//	= <> < <= > >=, IS [NOT] NULL, IS [NOT] MISSING, [NOT] IN, [NOT] BETWEEN, @>
//	( ), path, literal
//
// It is a plain recursive descent rather than a Pratt table because the ladder
// has four fixed rungs and no user-extensible operator set: a table would add a
// binding-power lookup per token to express four functions that never change.
//
// Two placements of NOT are easy to get wrong and are both tested. `NOT a = b`
// binds as `NOT (a = b)`, because NOT is looser than comparison — the reading
// that makes `NOT a = b` mean what an author expects. And the AND inside
// `a BETWEEN 1 AND 2` is part of BETWEEN rather than a conjunction, which falls
// out of parsing the bounds at leaf level, below the rung AND lives on.

// An exprContext says which clause a predicate is being parsed for. It exists
// because the clauses admit genuinely different leaves: WHERE runs before any
// reduction, so an aggregate there has nothing to read, while HAVING runs after
// one and may test only what the reduction produced.
type exprContext uint8

const (
	ctxWhere exprContext = iota
	ctxHaving
)

func (c exprContext) String() string {
	if c == ctxHaving {
		return "HAVING"
	}
	return "WHERE"
}

func (p *Parser) parseExpr(ctx exprContext) (*Expr, error) {
	return p.parseOr(ctx)
}

// parseOr parses OR-separated conjunctions into one n-ary node.
func (p *Parser) parseOr(ctx exprContext) (*Expr, error) {
	if p.depth++; p.depth > maxExprDepth {
		return nil, p.errfHere("predicate nests deeper than %d levels", maxExprDepth)
	}
	defer func() { p.depth-- }()

	first, err := p.parseAnd(ctx)
	if err != nil {
		return nil, err
	}
	if !p.atKeyword(kwOr) {
		return first, nil
	}
	pos := p.tok.pos
	base := len(p.kidStack)
	defer func() { p.kidStack = p.kidStack[:base] }()
	p.pushKid(ExprOr, first)
	for p.acceptKeyword(kwOr) {
		next, err := p.parseAnd(ctx)
		if err != nil {
			return nil, err
		}
		p.pushKid(ExprOr, next)
	}
	return p.newBoolean(ExprOr, base, pos), nil
}

// parseAnd parses AND-separated negations into one n-ary node.
func (p *Parser) parseAnd(ctx exprContext) (*Expr, error) {
	first, err := p.parseNot(ctx)
	if err != nil {
		return nil, err
	}
	if !p.atKeyword(kwAnd) {
		return first, nil
	}
	pos := p.tok.pos
	base := len(p.kidStack)
	defer func() { p.kidStack = p.kidStack[:base] }()
	p.pushKid(ExprAnd, first)
	for p.acceptKeyword(kwAnd) {
		next, err := p.parseNot(ctx)
		if err != nil {
			return nil, err
		}
		p.pushKid(ExprAnd, next)
	}
	return p.newBoolean(ExprAnd, base, pos), nil
}

// pushKid appends one operand of an n-ary boolean node, splicing in the
// operands of a same-kind child instead of nesting it.
//
// Flattening is a normalization, not an optimization: `a OR b OR c` and
// `(a OR b) OR c` denote the same predicate, and a consumer that had to handle
// both shapes would be handling an accident of where the author put brackets.
// It also hands query's OR-to-IN coalescing every disjunct of one disjunction
// in one list, which is exactly what that rewrite scans.
func (p *Parser) pushKid(kind ExprKind, kid *Expr) {
	if kid.Kind == kind {
		p.kidStack = append(p.kidStack, kid.Kids...)
		return
	}
	p.kidStack = append(p.kidStack, kid)
}

// newBoolean builds an n-ary AND or OR from the operands above base.
func (p *Parser) newBoolean(kind ExprKind, base, pos int) *Expr {
	operands := p.kidStack[base:]
	kids := p.kids.allocDirty(len(operands))
	copy(kids, operands)
	e := p.exprs.one()
	*e = Expr{Kind: kind, Column: -1, Kids: kids, Pos: pos}
	return e
}

// parseNot parses an optional NOT prefix over a primary predicate.
func (p *Parser) parseNot(ctx exprContext) (*Expr, error) {
	if !p.atKeyword(kwNot) {
		return p.parsePrimary(ctx)
	}
	if p.depth++; p.depth > maxExprDepth {
		return nil, p.errfHere("predicate nests deeper than %d levels", maxExprDepth)
	}
	defer func() { p.depth-- }()
	pos := p.tok.pos
	p.advance()
	inner, err := p.parseNot(ctx)
	if err != nil {
		return nil, err
	}
	kids := p.kids.allocDirty(1)
	kids[0] = inner
	e := p.exprs.one()
	*e = Expr{Kind: ExprNot, Column: -1, Kids: kids, Pos: pos}
	return e, nil
}

// parsePrimary parses a parenthesized predicate or a path-led leaf.
func (p *Parser) parsePrimary(ctx exprContext) (*Expr, error) {
	switch {
	case p.tok.kind == tokLParen:
		p.advance()
		if p.atKeyword(kwSelect) {
			return nil, p.errHere("subqueries are not supported: the engine executes one plan and has no nested execution")
		}
		inner, err := p.parseOr(ctx)
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokRParen, "')'"); err != nil {
			return nil, err
		}
		return inner, nil
	case p.atKeyword(kwExists):
		return nil, p.errHere("EXISTS is not supported: it takes a subquery, which the engine cannot execute; to test whether a field is present write `path IS NOT MISSING`")
	case p.atKeyword(kwCase):
		return nil, p.errHere("CASE expressions are not supported: the engine evaluates predicates, not computed values")
	case p.atKeyword(kwCast):
		return nil, p.errHere("CAST is not supported: values compare within their JSON type, so there is nothing to cast to")
	case p.tok.kind == tokNumber, p.tok.kind == tokString, p.tok.kind == tokParam:
		return nil, p.errHere("a condition must begin with a path: the engine compares a stored value against a constant, not two constants")
	}

	// The leaf's position is where its left operand starts, captured before
	// anything is consumed. An aggregate leaf would otherwise report at its
	// argument's path, which points inside SUM(...) rather than at the SUM the
	// author has to change.
	leafPos := p.tok.pos
	agg := AggNone
	var path *PathExpr
	switch kind, head, state := p.tryAggregate(); state {
	case aggCall:
		if ctx != ctxHaving {
			return nil, p.errfHere("an aggregate is not allowed in %s: rows are filtered before they are reduced; use HAVING", ctx)
		}
		arg, err := p.parseAggregateArgs(kind)
		if err != nil {
			return nil, err
		}
		agg, path = kind, arg
	case aggHeadOnly:
		p2, err := p.continuePath(head, false)
		if err != nil {
			return nil, err
		}
		path = p2
	default:
		p2, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		path = p2
	}
	return p.parseLeafTail(agg, path, leafPos)
}

// parseLeafTail parses what follows a leaf's left operand.
func (p *Parser) parseLeafTail(agg AggKind, path *PathExpr, pos int) (*Expr, error) {
	if p.acceptKeyword(kwIs) {
		return p.parseIsTail(agg, path, pos)
	}
	negated := p.acceptKeyword(kwNot)
	switch {
	case p.atKeyword(kwIn):
		p.advance()
		return p.parseInTail(agg, path, pos, negated)
	case p.atKeyword(kwBetween):
		p.advance()
		return p.parseBetweenTail(agg, path, pos, negated)
	case p.atKeyword(kwLike), p.atKeyword(kwIlike):
		return nil, p.errHere("pattern matching (LIKE) is not supported: the engine has no pattern operator, only equality, ordering, membership, and jsonb containment")
	case p.atKeyword(kwSimilar):
		return nil, p.errHere("SIMILAR TO is not supported: the engine has no pattern operator")
	case negated:
		return nil, p.errHere("expected IN, BETWEEN, or LIKE after NOT")
	}
	if p.tok.kind == tokContains {
		return p.parseContainsTail(agg, path, pos)
	}
	op, ok := comparisonOp(p.tok.kind)
	if !ok {
		return nil, p.errHere("expected a comparison operator, IS, IN, BETWEEN, or @> after a path; a bare path is not a condition, so a boolean field is tested as `flag = TRUE`")
	}
	p.advance()
	value, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	e := p.exprs.one()
	*e = Expr{Kind: ExprCompare, Op: op, Agg: agg, Column: -1, Path: path, Value: value, Pos: pos}
	return e, nil
}

// parseIsTail parses IS [NOT] NULL and IS [NOT] MISSING.
func (p *Parser) parseIsTail(agg AggKind, path *PathExpr, pos int) (*Expr, error) {
	negated := p.acceptKeyword(kwNot)
	kind := ExprIsNull
	switch {
	case p.acceptKeyword(kwNull):
	case p.acceptKeyword(kwMissing):
		kind = ExprIsMissing
	case p.atKeyword(kwTrue), p.atKeyword(kwFalse):
		return nil, p.errHere("IS TRUE / IS FALSE is not supported; write `flag = TRUE`")
	default:
		return nil, p.errHere("expected NULL or MISSING after IS")
	}
	e := p.exprs.one()
	*e = Expr{Kind: kind, Negated: negated, Agg: agg, Column: -1, Path: path, Pos: pos}
	return e, nil
}

// parseInTail parses the alternatives of IN, which the engine answers with a
// sorted membership rather than a chain of equalities.
func (p *Parser) parseInTail(agg AggKind, path *PathExpr, pos int, negated bool) (*Expr, error) {
	if err := p.expect(tokLParen, "'(' after IN"); err != nil {
		return nil, err
	}
	if p.atKeyword(kwSelect) {
		return nil, p.errHere("IN (SELECT ...) is not supported: the engine has no nested execution")
	}
	if p.tok.kind == tokRParen {
		return nil, p.errHere("IN () has no alternatives; an empty membership matches nothing, so the statement is a mistake rather than a filter")
	}
	base := len(p.opScratch)
	defer func() { p.opScratch = p.opScratch[:base] }()
	for {
		value, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		p.opScratch = append(p.opScratch, value)
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return nil, err
	}
	values := p.opScratch[base:]
	list := p.ops.allocDirty(len(values))
	copy(list, values)
	e := p.exprs.one()
	*e = Expr{Kind: ExprIn, Negated: negated, Agg: agg, Column: -1, Path: path, List: list, Pos: pos}
	return e, nil
}

// parseBetweenTail parses BETWEEN lo AND hi. The AND belongs to BETWEEN, not to
// the boolean ladder; see the file comment.
func (p *Parser) parseBetweenTail(agg AggKind, path *PathExpr, pos int, negated bool) (*Expr, error) {
	lo, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword(kwAnd, "AND between the bounds of BETWEEN"); err != nil {
		return nil, err
	}
	hi, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	list := p.ops.allocDirty(2)
	list[0], list[1] = lo, hi
	e := p.exprs.one()
	*e = Expr{Kind: ExprBetween, Negated: negated, Agg: agg, Column: -1, Path: path, List: list, Pos: pos}
	return e, nil
}

// parseContainsTail parses '@>' and the JSON document that follows it.
func (p *Parser) parseContainsTail(agg AggKind, path *PathExpr, pos int) (*Expr, error) {
	if agg != AggNone {
		return nil, p.errHere("containment does not apply to an aggregate, whose result is a number")
	}
	// The current token is '@>', so the lexer's position sits exactly at the
	// first byte of the needle and the JSON is read straight from the source
	// rather than reassembled from tokens. Reassembling it would mean lexing
	// JSON with SQL's rules, which disagree about quotes, and would lose the
	// verbatim spelling the engine's exact-decimal number comparison needs.
	text, needleStart, end, err := p.scanJSONValue(p.lx.pos)
	if err != nil {
		return nil, err
	}
	p.lx.pos = end
	p.advance()
	e := p.exprs.one()
	*e = Expr{
		Kind: ExprContains, Column: -1, Path: path, Pos: pos,
		Value: Operand{Kind: OperandJSON, Text: p.internString(text), Pos: needleStart},
	}
	return e, nil
}

func comparisonOp(k tokenKind) (CmpOp, bool) {
	switch k {
	case tokEq:
		return OpEq, true
	case tokNe:
		return OpNe, true
	case tokLt:
		return OpLt, true
	case tokLe:
		return OpLe, true
	case tokGt:
		return OpGt, true
	case tokGe:
		return OpGe, true
	}
	return OpEq, false
}

// parseOperand parses a comparison right-hand side: a literal or a placeholder.
//
// NULL is rejected rather than accepted as a literal, and the reason is the
// deepest semantic difference between this dialect and SQL. In SQL `x = NULL`
// is UNKNOWN, never true, so writing it is always a mistake; in the engine a
// null cell satisfies no comparison at all, so the same expression is always
// false. Both readings make the expression useless, and both make IS NULL the
// thing the author meant, so refusing it costs nothing and removes the one
// spelling whose meaning would depend on which of the two semantics a reader
// had in mind.
func (p *Parser) parseOperand() (Operand, error) {
	pos := p.tok.pos
	switch p.tok.kind {
	case tokNumber:
		text := p.internString(p.tok.text)
		p.advance()
		return Operand{Kind: OperandNumber, Text: text, Pos: pos}, nil
	case tokString:
		text := p.internToken(p.tok)
		p.advance()
		return Operand{Kind: OperandString, Text: text, Pos: pos}, nil
	case tokParam:
		if p.params >= maxParams {
			return Operand{}, p.errfHere("a statement may hold at most %d placeholders", maxParams)
		}
		ordinal := p.params
		p.params++
		p.advance()
		return Operand{Kind: OperandParam, Ordinal: ordinal, Pos: pos}, nil
	case tokIdent:
		switch p.tok.kw {
		case kwTrue:
			p.advance()
			return Operand{Kind: OperandBool, Bool: true, Pos: pos}, nil
		case kwFalse:
			p.advance()
			return Operand{Kind: OperandBool, Pos: pos}, nil
		case kwNull:
			return Operand{}, p.errHere("NULL is not a comparison operand: no value compares equal to it; write `IS NULL` or `IS NOT NULL`")
		}
		return Operand{}, p.errHere("expected a literal or '?': the right side of a comparison is a constant, because the engine compares a stored value against one")
	case tokQuotedIdent:
		return Operand{}, p.errHere("a double-quoted name is an identifier, not a string; string literals use single quotes")
	}
	return Operand{}, p.errHere("expected a literal or '?'")
}
