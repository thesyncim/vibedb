package sql

import "errors"

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
	ctxJoin
)

const joinOnPredicateSubqueryUnsupported = "predicate subqueries in JOIN ON are not supported yet; move the predicate to WHERE or materialize and join the nested relation explicitly"

func (c exprContext) String() string {
	if c == ctxHaving {
		return "HAVING"
	}
	if c == ctxJoin {
		return "ON"
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
		if p.atKeyword(kwSelect) || p.atKeyword(kwWith) {
			return nil, p.errHere("a SELECT subquery cannot stand alone as a condition; use EXISTS (SELECT ...)")
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
		pos := p.tok.pos
		p.advance()
		if err := p.expect(tokLParen, "'(' after EXISTS"); err != nil {
			return nil, err
		}
		sub, err := p.parsePredicateSubquery(true)
		if err != nil {
			return nil, err
		}
		if ctx == ctxJoin {
			// Parse the complete nested query first so malformed EXISTS syntax
			// remains a ParseError. A valid shape is then refused at its operator;
			// no executable ON expression is ever returned.
			return nil, newFeatureNotSupportedError(
				p.lx.src, pos, joinOnPredicateSubqueryUnsupported,
			)
		}
		e := p.exprs.one()
		*e = Expr{Kind: ExprExists, Column: -1, Subquery: sub, Pos: pos}
		return e, nil
	case p.atKeyword(kwCase), p.atKeyword(kwCast),
		p.tok.kind == tokNumber, p.tok.kind == tokString,
		p.tok.kind == tokParam, p.tok.kind == tokPlus, p.tok.kind == tokMinus,
		p.inCaseTruth() && p.atKeyword(kwNull):
		return p.parseScalarCondition(ctx, nil, p.tok.pos)
	case (ctx == ctxJoin || p.inCaseTruth()) &&
		(p.atKeyword(kwTrue) || p.atKeyword(kwFalse)):
		pos := p.tok.pos
		value, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		e := p.exprs.one()
		*e = Expr{Kind: ExprConstant, Column: -1, Value: value, Pos: pos}
		return e, nil
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
			if p.inCaseTruth() {
				return nil, newFeatureNotSupportedError(
					p.lx.src, leafPos,
					"aggregate predicates inside searched CASE require a combined grouped CASE stage",
				)
			}
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
	if scalarContinues(p.tok) {
		column := ResultColumn{Agg: agg, Path: path, Pos: leafPos}
		return p.parseScalarCondition(ctx, p.scalarFromColumn(column), leafPos)
	}
	if p.inCaseTruth() && caseTruthTerminator(p.tok) {
		node := p.exprs.one()
		*node = Expr{
			Kind: ExprScalarTruth, Column: -1,
			ScalarLeft: p.scalarFromColumn(ResultColumn{Agg: agg, Path: path, Pos: leafPos}),
			Pos:        leafPos,
		}
		return node, nil
	}
	return p.parseLeafTail(ctx, agg, path, leafPos)
}

func scalarContextForPredicate(ctx exprContext) scalarExprContext {
	switch ctx {
	case ctxHaving:
		return scalarHaving
	case ctxJoin:
		return scalarJoin
	default:
		return scalarWhere
	}
}

// parseScalarCondition parses one computed value followed by a comparison or
// IS [NOT] NULL. Boolean composition remains in the existing predicate ladder,
// so AND/OR/NOT keep their established precedence and flattened AST shape.
func (p *Parser) parseScalarCondition(
	ctx exprContext,
	left *ScalarExpr,
	pos int,
) (*Expr, error) {
	var err error
	scalarCtx := scalarContextForPredicate(ctx)
	if left == nil {
		left, err = p.parseScalarExpression(scalarCtx)
	} else {
		left, err = p.continueScalarExpression(left, scalarCtx)
	}
	if err != nil {
		return nil, err
	}
	if p.inCaseTruth() && caseTruthTerminator(p.tok) {
		node := p.exprs.one()
		*node = Expr{
			Kind: ExprScalarTruth, Column: -1,
			ScalarLeft: left, Pos: pos,
		}
		return node, nil
	}
	if p.acceptKeyword(kwIs) {
		negated := p.acceptKeyword(kwNot)
		if !p.acceptKeyword(kwNull) {
			return nil, p.errHere("a computed scalar supports IS [NOT] NULL; IS MISSING applies only to a stored path")
		}
		node := p.exprs.one()
		*node = Expr{
			Kind: ExprScalarIsNull, Negated: negated, Column: -1,
			ScalarLeft: left, Pos: pos,
		}
		return node, nil
	}
	op, ok := comparisonOp(p.tok.kind)
	if !ok {
		return nil, p.errHere("expected a comparison operator or IS [NOT] NULL after a computed scalar expression")
	}
	p.advance()
	right, err := p.parseScalarExpression(scalarCtx)
	if err != nil {
		return nil, err
	}
	node := p.exprs.one()
	*node = Expr{
		Kind: ExprScalarCompare, Op: op, Column: -1,
		ScalarLeft: left, ScalarRight: right, Pos: pos,
	}
	return node, nil
}

// parseLeafTail parses what follows a leaf's left operand.
func (p *Parser) parseLeafTail(
	ctx exprContext,
	agg AggKind,
	path *PathExpr,
	pos int,
) (*Expr, error) {
	if p.acceptKeyword(kwIs) {
		return p.parseIsTail(agg, path, pos)
	}
	negated := p.acceptKeyword(kwNot)
	switch {
	case p.atKeyword(kwIn):
		operatorPos := p.tok.pos
		p.advance()
		return p.parseInTail(ctx, agg, path, pos, operatorPos, negated)
	case p.atKeyword(kwBetween):
		p.advance()
		return p.parseBetweenTail(agg, path, pos, negated)
	case p.atKeyword(kwLike), p.atKeyword(kwIlike):
		insensitive := p.atKeyword(kwIlike)
		p.advance()
		return p.parseLikeTail(agg, path, pos, negated, insensitive)
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
	if (ctx == ctxJoin || p.correlation != nil && p.correlation.capture != nil) &&
		(p.tok.kind == tokQuotedIdent ||
			p.tok.kind == tokIdent && p.tok.kw == kwNone) {
		right, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		e := p.exprs.one()
		*e = Expr{
			Kind: ExprCompare, Op: op, Agg: agg, Column: -1,
			Path: path, RightPath: right, Pos: pos,
		}
		return e, nil
	}
	if p.tok.kind == tokLParen {
		subqueryPos := p.tok.pos
		p.advance()
		if p.atKeyword(kwSelect) || p.atKeyword(kwWith) {
			sub, err := p.parsePredicateSubquery(false)
			if err != nil {
				return nil, err
			}
			if ctx == ctxJoin {
				return nil, newFeatureNotSupportedError(
					p.lx.src, subqueryPos, joinOnPredicateSubqueryUnsupported,
				)
			}
			e := p.exprs.one()
			*e = Expr{Kind: ExprCompare, Op: op, Agg: agg, Column: -1, Path: path, Subquery: sub, Pos: pos}
			return e, nil
		}
		return nil, p.errHere("a comparison parenthesis must contain a SELECT subquery")
	}
	value, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	e := p.exprs.one()
	*e = Expr{Kind: ExprCompare, Op: op, Agg: agg, Column: -1, Path: path, Value: value, Pos: pos}
	return e, nil
}

// parseLikeTail parses LIKE and ILIKE. Pattern matching is a scalar string
// predicate in the query engine, so its RHS is intentionally narrower than a
// general comparison: accepting numbers or booleans would turn a type error
// into a surprising string conversion.
func (p *Parser) parseLikeTail(
	agg AggKind, path *PathExpr, pos int, negated, insensitive bool,
) (*Expr, error) {
	if agg != AggNone {
		return nil, p.errHere("LIKE does not apply to an aggregate result")
	}
	value, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	if value.Kind != OperandString && value.Kind != OperandParam {
		return nil, p.errAt(value.Pos, "LIKE pattern must be a string literal or a placeholder")
	}
	if p.atKeyword(kwEscape) {
		return nil, p.errHere("LIKE ... ESCAPE is not supported; use the default backslash escape")
	}
	e := p.exprs.one()
	*e = Expr{
		Kind: ExprLike, Negated: negated, Insensitive: insensitive,
		Column: -1, Path: path, Value: value, Pos: pos,
	}
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
func (p *Parser) parseInTail(
	ctx exprContext,
	agg AggKind,
	path *PathExpr,
	pos, operatorPos int,
	negated bool,
) (*Expr, error) {
	if err := p.expect(tokLParen, "'(' after IN"); err != nil {
		return nil, err
	}
	if p.atKeyword(kwSelect) || p.atKeyword(kwWith) {
		sub, err := p.parsePredicateSubquery(false)
		if err != nil {
			return nil, err
		}
		if ctx == ctxJoin {
			return nil, newFeatureNotSupportedError(
				p.lx.src, operatorPos, joinOnPredicateSubqueryUnsupported,
			)
		}
		e := p.exprs.one()
		*e = Expr{Kind: ExprIn, Negated: negated, Agg: agg, Column: -1, Path: path, Subquery: sub, Pos: pos}
		return e, nil
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

// parsePredicateSubquery parses a nested predicate query with the complete
// current FROM scope visible for qualified outer references. Local range
// aliases are still resolved first by the child parser, so aliases and CTE
// references shadow outer names lexically. Only a proven capture is frozen
// into the AST; uncorrelated predicate subqueries retain a nil sidecar.
func (p *Parser) parsePredicateSubquery(exists bool) (*SelectStmt, error) {
	capture := p.beginPredicateCorrelation()
	sub, err := p.parseSubqueryScoped(exists, &capture.scope, capture)
	if err != nil {
		return nil, err
	}
	sub.Correlation = p.finishCorrelationCapture(capture, false, false)
	return sub, nil
}

func (p *Parser) beginPredicateCorrelation() *correlationCapture {
	state := p.correlationState()
	capture := state.captures.one()
	capture.owner = p
	capture.bindingBase = len(state.bindingScratch)
	capture.referenceBase = len(state.referenceScratch)
	capture.forwardBase = len(state.forward)
	capture.source = len(p.from)
	// parseSubqueryScoped rebases the child before this capture is frozen, so
	// use the query's position in the current parser source. An enclosing parse
	// will rebase the complete SelectStmt, including this sidecar, once more.
	capture.pos = p.tok.pos
	capture.scope = correlationRangeScope{
		parser: p,
		limit:  len(p.from),
		outer:  state.outerRanges,
	}
	return capture
}

// parseSubquery parses an independently scoped SELECT beginning at the current
// token and consumes the ')' belonging to its caller.
//
// A child Parser is intentional. A nested statement has its own FROM scope and
// its tree must remain live beside the outer tree; sharing the outer clause
// scratch would either overwrite the outer SELECT list or require copying the
// whole tree. Child parsers retain and refill their arenas, so after a warm-up
// the shape remains allocation-free.
func (p *Parser) parseSubquery(exists bool) (*SelectStmt, error) {
	return p.parseSubqueryScoped(exists, nil, nil)
}

// parseSubqueryScoped is the correlation-aware form shared by LATERAL derived
// relations and predicate subqueries. Ordinary derived tables pass nil scope.
func (p *Parser) parseSubqueryScoped(
	exists bool,
	outerRanges *correlationRangeScope,
	capture *correlationCapture,
) (*SelectStmt, error) {
	start := p.tok.pos
	if !p.atKeyword(kwSelect) && !p.atKeyword(kwWith) && p.tok.kind != tokLParen {
		return nil, p.errHere("expected SELECT, WITH ... SELECT, or a parenthesized query expression in a subquery")
	}
	if p.nesting >= maxSubqueryDepth {
		return nil, p.errfAt(start,
			"subqueries nest deeper than %d levels", maxSubqueryDepth)
	}
	depth := 0
	end := -1
	for {
		switch p.tok.kind {
		case tokEOF:
			return nil, p.errHere("unterminated subquery: expected ')'")
		case tokError:
			return nil, p.errHere(p.tok.text)
		case tokLParen:
			depth++
		case tokRParen:
			if depth == 0 {
				end = p.tok.pos
			} else {
				depth--
			}
		}
		if end >= 0 {
			break
		}
		p.advance()
	}
	if p.nested == nil {
		p.nested = new(nestedParsers)
	}
	if p.nested.used == len(p.nested.parsers) {
		p.nested.parsers = append(p.nested.parsers, new(Parser))
	}
	child := p.nested.parsers[p.nested.used]
	p.nested.used++
	child.cancel = p.cancel
	sub := &child.sel
	if err := child.parseSelectText(
		sub, p.lx.src[start:end], &p.activeCTEs,
		outerRanges, capture, p.nesting+1, exists,
	); err != nil {
		return nil, p.rebaseSubqueryError(err, start)
	}
	if p.params+sub.Params > maxParams {
		return nil, p.errfAt(start, "a statement may hold at most %d placeholders", maxParams)
	}
	sub.ParamBase = p.params
	p.params += sub.Params
	shiftSelectPositions(sub, start)
	p.advance() // consume the caller's ')'
	return sub, nil
}

// rebaseSubqueryError moves a child parser's source-relative position into the
// containing statement while preserving its semantic error class. In
// particular, a valid-but-unsupported construct inside a nested SELECT must
// remain a FeatureNotSupportedError so protocol adapters can still map it to
// SQLSTATE 0A000 without matching prose.
func (p *Parser) rebaseSubqueryError(err error, start int) error {
	var duplicate *DuplicateCTEError
	if errors.As(err, &duplicate) {
		return newDuplicateCTEError(
			p.lx.src, start+duplicate.Pos, duplicate.Name,
			start+duplicate.FirstPos,
		)
	}
	var arity *CTEColumnAliasArityError
	if errors.As(err, &arity) {
		return newCTEColumnAliasArityError(
			p.lx.src, start+arity.Pos, arity.Name,
			arity.Aliases, arity.Outputs,
		)
	}
	var unsupported *FeatureNotSupportedError
	if errors.As(err, &unsupported) {
		return newFeatureNotSupportedError(
			p.lx.src, start+unsupported.Pos, unsupported.Msg,
		)
	}
	var parse *ParseError
	if errors.As(err, &parse) {
		return newParseError(p.lx.src, start+parse.Pos, parse.Msg)
	}
	return err
}

func shiftSelectPositions(s *SelectStmt, delta int) {
	if s.Correlation != nil {
		s.Correlation.Pos += delta
		for i := range s.Correlation.Bindings {
			s.Correlation.Bindings[i].Pos += delta
		}
	}
	if s.With != nil {
		s.With.Pos += delta
		for i := range s.With.CTEs {
			s.With.CTEs[i].Pos += delta
			for j := range s.With.CTEs[i].ColumnPos {
				s.With.CTEs[i].ColumnPos[j] += delta
			}
			if s.With.CTEs[i].HintPos >= 0 {
				s.With.CTEs[i].HintPos += delta
			}
			shiftSelectPositions(s.With.CTEs[i].Query, delta)
		}
	}
	for i := range s.Windows {
		s.Windows[i].Pos += delta
		shiftWindowSpecPositions(&s.Windows[i].Spec, delta)
	}
	for i := range s.Columns {
		s.Columns[i].Pos += delta
		shiftPathPosition(s.Columns[i].Path, delta)
		shiftWindowPositions(s.Columns[i].Window, delta)
		shiftScalarPositions(s.Columns[i].Scalar, delta)
	}
	for i := range s.From {
		s.From[i].Pos += delta
		if s.From[i].Lateral != nil {
			s.From[i].Lateral.Pos += delta
			for binding := range s.From[i].Lateral.Bindings {
				s.From[i].Lateral.Bindings[binding].Pos += delta
			}
		}
		if s.From[i].Kind == RelationDerived && s.From[i].Query != nil {
			shiftSelectPositions(s.From[i].Query, delta)
		}
		if s.From[i].UnresolvedCTE.Kind != CTEReferenceNone {
			s.From[i].UnresolvedCTE.DefinitionPos += delta
		}
		if s.From[i].On != nil {
			s.From[i].On.Pos += delta
			for k := range s.From[i].On.Keys {
				s.From[i].On.Keys[k].Pos += delta
			}
			if s.From[i].On.Expr != nil {
				shiftExprPositions(s.From[i].On.Expr, delta)
			} else {
				for k := range s.From[i].On.Keys {
					shiftPathPosition(s.From[i].On.Keys[k].Left, delta)
					shiftPathPosition(s.From[i].On.Keys[k].Right, delta)
				}
			}
		}
	}
	shiftExprPositions(s.Where, delta)
	for _, path := range s.GroupBy {
		shiftPathPosition(path, delta)
	}
	shiftExprPositions(s.Having, delta)
	for i := range s.OrderBy {
		s.OrderBy[i].Pos += delta
		shiftPathPosition(s.OrderBy[i].Path, delta)
		shiftScalarPositions(s.OrderBy[i].Scalar, delta)
	}
	if s.Limit != nil {
		s.Limit.Pos += delta
	}
	if s.Offset != nil {
		s.Offset.Pos += delta
	}
	var mirroredFirst *SelectStmt
	if s.Set != nil {
		mirroredFirst = s.Set.First
	}
	shiftSetExpressionPositions(s.Set, delta, mirroredFirst)
}

func shiftWindowPositions(w *WindowExpr, delta int) {
	if w == nil {
		return
	}
	w.Pos += delta
	shiftPathPosition(w.Argument, delta)
	if w.HasOffset {
		w.Offset.Pos += delta
	}
	if w.HasBuckets {
		w.Buckets.Pos += delta
	}
	if w.HasNth {
		w.Nth.Pos += delta
	}
	if w.HasDefault {
		w.Default.Pos += delta
	}
	shiftWindowSpecPositions(&w.Spec, delta)
}

func shiftWindowSpecPositions(spec *WindowSpec, delta int) {
	if spec == nil {
		return
	}
	spec.Pos += delta
	if spec.Name != "" {
		spec.NamePos += delta
	}
	if len(spec.PartitionBy) != 0 {
		spec.PartitionPos += delta
	}
	if len(spec.OrderBy) != 0 {
		spec.OrderPos += delta
	}
	if !spec.PartitionInherited {
		for _, path := range spec.PartitionBy {
			shiftPathPosition(path, delta)
		}
	}
	if !spec.OrderInherited {
		for i := range spec.OrderBy {
			spec.OrderBy[i].Pos += delta
			shiftPathPosition(spec.OrderBy[i].Path, delta)
		}
	}
	if spec.Frame.Explicit {
		spec.Frame.Pos += delta
		if spec.Frame.ExclusionExplicit {
			spec.Frame.ExclusionPos += delta
		}
		spec.Frame.Start.Pos += delta
		if spec.Frame.Start.Kind == WindowPreceding ||
			spec.Frame.Start.Kind == WindowFollowing {
			spec.Frame.Start.Offset.Pos += delta
		}
		spec.Frame.End.Pos += delta
		if spec.Frame.End.Kind == WindowPreceding ||
			spec.Frame.End.Kind == WindowFollowing {
			spec.Frame.End.Offset.Pos += delta
		}
	}
}

func shiftPathPosition(path *PathExpr, delta int) {
	if path != nil {
		path.Pos += delta
	}
}

func shiftExprPositions(e *Expr, delta int) {
	if e == nil {
		return
	}
	e.Pos += delta
	shiftPathPosition(e.Path, delta)
	shiftPathPosition(e.RightPath, delta)
	shiftScalarPositions(e.ScalarLeft, delta)
	shiftScalarPositions(e.ScalarRight, delta)
	e.Value.Pos += delta
	for i := range e.List {
		e.List[i].Pos += delta
	}
	for _, kid := range e.Kids {
		shiftExprPositions(kid, delta)
	}
	if e.Subquery != nil {
		shiftSelectPositions(e.Subquery, delta)
	}
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
