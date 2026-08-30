package sql

import "github.com/thesyncim/vibedb/internal/pginput"

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
	if err := p.coerceTypedSimpleCaseUnknownMatches(selector, state.whenScratch[base:]); err != nil {
		return nil, err
	}
	if err := p.coerceTypedCaseUnknownResults(state.whenScratch[base:], fallback); err != nil {
		return nil, err
	}

	arms := state.whenRuns.allocDirty(len(state.whenScratch) - base)
	copy(arms, state.whenScratch[base:])
	node := p.newScalar(ScalarCase, pos)
	node.Left, node.Whens, node.Else = selector, arms, fallback
	return node, nil
}

// coerceTypedSimpleCaseUnknownMatches mirrors transformCaseExpr's asymmetric
// comparison rule. PostgreSQL first fixes the selector's type; an unknown WHEN
// value can then be coerced to that type, but an unknown selector has already
// become TEXT and is not retrospectively changed by a typed WHEN value. Thus
// CASE BOOL 't' WHEN 't' is boolean, while CASE 't' WHEN BOOL 't' is the
// (unsupported) text = boolean operator, exactly as PostgreSQL 18.6 reports.
func (p *Parser) coerceTypedSimpleCaseUnknownMatches(
	selector *ScalarExpr,
	arms []ScalarWhen,
) error {
	selectorTarget, known := scalarCaseConcreteTarget(selector)
	if !known && scalarCaseUnknownSelectorFinalizesText(selector) {
		// transformCaseExpr coerces an unknown CaseTestExpr to TEXT before
		// transforming any WHEN equality. A later typed boolean therefore
		// cannot infer the selector backward to boolean.
		selectorTarget, known = ScalarCastText, true
	}
	if !known || (selectorTarget != ScalarCastBoolean && selectorTarget != ScalarCastText) {
		return nil
	}
	haveTyped := false
	selectorTyped := false
	if typed, ok := typedConstantCastType(selector); ok && typed == selectorTarget {
		haveTyped = true
		selectorTyped = true
	}
	for i := range arms {
		match := arms[i].Match
		matchTyped := false
		if typed, ok := typedConstantCastType(match); ok {
			matchTyped = true
			if typed == selectorTarget {
				haveTyped = true
			}
		}
		if target, concrete := scalarCaseConcreteTarget(match); concrete && target != selectorTarget {
			if selectorTyped || matchTyped {
				return newUndefinedOperatorError(
					p.lx.src, scalarCaseExpressionPosition(match),
					scalarCastTypeName(selectorTarget), scalarCastTypeName(target),
				)
			}
			// Preserve the established dynamic path for an ordinary untyped
			// CASE. This typed-constant seam must not broaden its behavior.
			return nil
		}
	}
	if !haveTyped || selectorTarget == ScalarCastText {
		return nil
	}
	for i := range arms {
		if err := p.coerceCaseUnknownBoolean(arms[i].Match); err != nil {
			return err
		}
	}
	return nil
}

func scalarCaseExpressionPosition(expr *ScalarExpr) int {
	if expr != nil && expr.Kind == ScalarCast && expr.TypedConstant {
		return expr.TargetPos
	}
	if expr == nil {
		return 0
	}
	return expr.Pos
}

func scalarCaseUnknownSelectorFinalizesText(expr *ScalarExpr) bool {
	if expr == nil || expr.Kind == ScalarNull {
		return expr != nil
	}
	return expr.Kind == ScalarLiteral &&
		(expr.Value.Kind == OperandString || expr.Value.Kind == OperandParam)
}

// coerceTypedCaseUnknownResults performs the bounded part of PostgreSQL's
// CASE common-type analysis that is observable for the typed-string domains
// this parser admits. A bare string literal has pseudo-type unknown until its
// surrounding expression selects a real type. When one result arm is a
// PostgreSQL BOOL/BOOLEAN typed constant, every bare unknown result is passed
// through boolin during analysis -- including a statically dead arm. TEXT is
// the corresponding identity coercion.
//
// This pass deliberately activates only when a type 'string' result supplies
// the unambiguous BOOL or TEXT candidate. Ordinary CASE expressions retain
// their established schemaless behavior, and a conflicting concrete result is
// left to the query type checker without evaluating an unrelated unknown.
func (p *Parser) coerceTypedCaseUnknownResults(
	arms []ScalarWhen,
	fallback *ScalarExpr,
) error {
	target, ok := typedCaseCommonTarget(arms, fallback)
	if !ok || target == ScalarCastText {
		return nil
	}
	for i := range arms {
		if err := p.coerceCaseUnknownBoolean(arms[i].Result); err != nil {
			return err
		}
	}
	return p.coerceCaseUnknownBoolean(fallback)
}

// typedCaseCommonTarget returns a candidate only when every statically known
// result belongs to one SQL domain and at least one result is a typed BOOL or
// TEXT constant (possibly followed by explicit casts). Unknown strings, NULL,
// parameters, and schemaless paths are neutral, just as they are during
// PostgreSQL common-type selection.
func typedCaseCommonTarget(
	arms []ScalarWhen,
	fallback *ScalarExpr,
) (ScalarCastTarget, bool) {
	var candidate ScalarCastTarget
	haveCandidate, haveTyped := false, false
	visit := func(expr *ScalarExpr) bool {
		domain, known, typed := scalarCaseExpressionResolution(expr)
		haveTyped = haveTyped || typed
		if !known {
			return true
		}
		if !haveCandidate {
			candidate, haveCandidate = domain, true
			return true
		}
		return candidate == domain
	}
	// PostgreSQL considers ELSE first for CASE, but only category agreement is
	// relevant in this closed BOOL/TEXT coercion seam.
	if fallback != nil && !visit(fallback) {
		return 0, false
	}
	for i := range arms {
		if !visit(arms[i].Result) {
			return 0, false
		}
	}
	if !haveCandidate || !haveTyped ||
		(candidate != ScalarCastBoolean && candidate != ScalarCastText) {
		return 0, false
	}
	return candidate, true
}

func scalarCaseConcreteTarget(expr *ScalarExpr) (ScalarCastTarget, bool) {
	target, known, typed := scalarCaseExpressionResolution(expr)
	if expr != nil && expr.Kind == ScalarCase && !typed {
		// Keep ordinary nested CASE behavior on the runtime descriptor path.
		// Only the typed-string seam needs its resolved domain at parse time.
		return 0, false
	}
	return target, known
}

// scalarCaseExpressionResolution returns the concrete output domain and
// whether it descends from PostgreSQL's type 'string' production. It performs
// one depth-first walk, so a maximally nested CASE remains linear rather than
// recursively asking separate domain and provenance questions at each level.
func scalarCaseExpressionResolution(
	expr *ScalarExpr,
) (target ScalarCastTarget, known, typed bool) {
	if expr == nil {
		return 0, false, false
	}
	switch expr.Kind {
	case ScalarLiteral:
		switch expr.Value.Kind {
		case OperandBool:
			return ScalarCastBoolean, true, false
		case OperandNumber:
			return ScalarCastNumeric, true, false
		case OperandJSON:
			return ScalarCastJSON, true, false
		default:
			// Strings, parameters, and SQL NULL are unknown until context
			// selects a type.
			return 0, false, false
		}
	case ScalarNull, ScalarPath:
		return 0, false, false
	case ScalarUnary, ScalarAggregate:
		return ScalarCastNumeric, true, false
	case ScalarBinary:
		if expr.Op == ScalarConcat {
			return ScalarCastText, true, false
		}
		return ScalarCastNumeric, true, false
	case ScalarCast:
		_, _, childTyped := scalarCaseExpressionResolution(expr.Left)
		return expr.Cast, true, expr.TypedConstant || childTyped
	case ScalarCase:
		var candidate ScalarCastTarget
		haveCandidate, haveTyped := false, false
		visit := func(result *ScalarExpr) bool {
			domain, concrete, resultTyped := scalarCaseExpressionResolution(result)
			haveTyped = haveTyped || resultTyped
			if !concrete {
				return true
			}
			if !haveCandidate {
				candidate, haveCandidate = domain, true
				return true
			}
			return candidate == domain
		}
		if expr.Else != nil && !visit(expr.Else) {
			return 0, false, haveTyped
		}
		for i := range expr.Whens {
			if !visit(expr.Whens[i].Result) {
				return 0, false, haveTyped
			}
		}
		return candidate, haveCandidate, haveTyped
	default:
		return 0, false, false
	}
}

func (p *Parser) coerceCaseUnknownBoolean(expr *ScalarExpr) error {
	if expr == nil || expr.Kind != ScalarLiteral || expr.Value.Kind != OperandString {
		return nil
	}
	value, ok := pginput.Boolean(expr.Value.Text)
	if !ok {
		return newInvalidTextRepresentationError(
			p.lx.src, expr.Value.Pos, "boolean",
			"invalid input syntax for type boolean",
		)
	}
	expr.Value.Kind = OperandBool
	expr.Value.Text = ""
	expr.Value.Bool = value
	return nil
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
