package sql

import (
	"strings"

	"github.com/thesyncim/vibedb/internal/pginput"
)

// scalarExprContext records where a computed value is authored. Aggregates
// are legal in SELECT, HAVING, and ORDER BY after grouping, but not in WHERE,
// ON, or UPDATE SET, whose expressions run over one input row at a time.
type scalarExprContext uint8

const (
	scalarSelect scalarExprContext = iota
	scalarWhere
	scalarHaving
	scalarJoin
	scalarOrder
	scalarUpdate
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
	case scalarUpdate:
		return "UPDATE SET"
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
	arguments   []*ScalarExpr
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
	clear(s.arguments)
	s.arguments = s.arguments[:0]
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
		if _, ok := conditionalScalarOp(tok); ok {
			return true
		}
		return tok.kw == kwTrue || tok.kw == kwFalse || tok.kw == kwNull ||
			tok.kw == kwCase || tok.kw == kwCast
	default:
		return false
	}
}

// scalarTypedStringHead is deliberately a fixed classifier. PostgreSQL also
// allows catalog-resolved type names, but probing every identifier would tax
// ordinary path parsing and could scan attacker-sized tokens more than once.
// Known unsupported built-ins are consumed only to return an explicit 0A000
// when they actually precede a string.
func scalarTypedStringHead(tok token) (ScalarCastTarget, bool, bool) {
	if tok.kind == tokQuotedIdent && !tok.esc {
		switch tok.text {
		case "text":
			return ScalarCastText, true, true
		case "bool":
			return ScalarCastBoolean, true, true
		case "boolean":
			// PostgreSQL's BOOLEAN keyword maps to pg_catalog.bool, but a
			// quoted name is a catalog lookup and no type named boolean exists.
			return 0, false, true
		default:
			return 0, false, false
		}
	}
	if tok.kind != tokIdent {
		return 0, false, false
	}
	switch {
	case equalFoldASCII(tok.text, "text"):
		return ScalarCastText, true, true
	case equalFoldASCII(tok.text, "boolean"), equalFoldASCII(tok.text, "bool"):
		return ScalarCastBoolean, true, true
	case equalFoldASCII(tok.text, "numeric"), equalFoldASCII(tok.text, "decimal"),
		equalFoldASCII(tok.text, "json"), equalFoldASCII(tok.text, "jsonb"),
		equalFoldASCII(tok.text, "int2"), equalFoldASCII(tok.text, "int4"),
		equalFoldASCII(tok.text, "int8"), equalFoldASCII(tok.text, "varchar"),
		equalFoldASCII(tok.text, "char"), equalFoldASCII(tok.text, "character"),
		equalFoldASCII(tok.text, "pg_catalog"):
		return 0, false, true
	default:
		return 0, false, false
	}
}

// rejectUnsupportedTypedStringSuffix catches PostgreSQL typed-constant forms
// that begin with one of the fixed heads above but are outside the bounded
// grammar. The checks inspect a fixed-size suffix and run only after such a
// head, so an ordinary identifier/path never pays for speculative tokenization.
func (p *Parser) rejectUnsupportedTypedStringSuffix(head token) error {
	switch p.tok.kind {
	case tokLParen:
		return newFeatureNotSupportedError(
			p.lx.src, head.pos,
			"PostgreSQL typed-string constants with type modifiers are not supported; use unmodified BOOL, BOOLEAN, or TEXT",
		)
	case tokLBracket:
		// A field named bool may legitimately be subscripted. Refuse only the
		// fixed empty [] type suffix when a string literal follows it; otherwise
		// the ordinary path parser retains ownership.
		end := p.lx.pos
		if end >= len(p.lx.src) || p.lx.src[end] != ']' {
			return nil
		}
		end++
		limit := len(p.lx.src)
		if limit-end > 64 {
			limit = end + 64
		}
		for end < limit {
			switch p.lx.src[end] {
			case ' ', '\t', '\n', '\r', '\v', '\f':
				end++
			default:
				goto arraySuffix
			}
		}
	arraySuffix:
		if end == limit && end < len(p.lx.src) {
			return nil
		}
		if end < len(p.lx.src) && (p.lx.src[end] == '\'' ||
			(end+1 < len(p.lx.src) && (p.lx.src[end] == 'e' || p.lx.src[end] == 'E') &&
				p.lx.src[end+1] == '\'')) {
			return newFeatureNotSupportedError(
				p.lx.src, head.pos,
				"PostgreSQL array typed-string constants are not supported; use scalar BOOL, BOOLEAN, or TEXT",
			)
		}
		return nil
	case tokIdent:
		end := p.lx.pos
		if equalFoldASCII(p.tok.text, "e") && end < len(p.lx.src) && p.lx.src[end] == '\'' {
			return newFeatureNotSupportedError(
				p.lx.src, head.pos,
				"escape-string typed constants are not supported; use a standard quoted BOOL, BOOLEAN, or TEXT constant",
			)
		}
		if equalFoldASCII(p.tok.text, "u") && end+1 < len(p.lx.src) &&
			p.lx.src[end] == '&' && p.lx.src[end+1] == '\'' {
			return newFeatureNotSupportedError(
				p.lx.src, head.pos,
				"Unicode-escape typed constants are not supported; use a standard quoted BOOL, BOOLEAN, or TEXT constant",
			)
		}
	}
	return nil
}

func isPGCatalogTypedStringPath(head token, path *PathExpr) bool {
	if head.kind != tokIdent || !equalFoldASCII(head.text, "pg_catalog") ||
		path == nil || len(path.Segments) != 2 {
		return false
	}
	target := path.Segments[1]
	if target.IsIndex {
		return false
	}
	return equalFoldASCII(target.Key, "bool") ||
		equalFoldASCII(target.Key, "boolean") ||
		equalFoldASCII(target.Key, "text")
}

func (p *Parser) rejectQualifiedTypedStringPath(head token, path *PathExpr) error {
	if !isPGCatalogTypedStringPath(head, path) {
		return nil
	}
	if err := p.rejectUnsupportedTypedStringSuffix(head); err != nil {
		return err
	}
	if p.tok.kind != tokString {
		return nil
	}
	return newFeatureNotSupportedError(
		p.lx.src, head.pos,
		"qualified PostgreSQL typed-string constants are not supported; use unqualified BOOL, BOOLEAN, or TEXT",
	)
}

// parseTypedStringAfterHead consumes the string token after an already
// consumed type head. Each token is lexed once, so large literals retain the
// parser's bounded cancellation behavior.
func (p *Parser) parseTypedStringAfterHead(
	head token, target ScalarCastTarget, supported bool,
) (
	Operand, error,
) {
	if p.tok.kind != tokString {
		if err := p.rejectUnsupportedTypedStringSuffix(head); err != nil {
			return Operand{}, err
		}
		return Operand{}, p.errHere("expected a string after the PostgreSQL type name")
	}
	value, err := p.typedStringOperand(head, p.tok, target, supported)
	if err != nil {
		return Operand{}, err
	}
	p.advance()
	return value, nil
}

func (p *Parser) typedStringOperand(
	head, literal token, target ScalarCastTarget, supported bool,
) (Operand, error) {
	if !supported {
		return Operand{}, newFeatureNotSupportedError(
			p.lx.src, head.pos,
			"this PostgreSQL typed-string constant is not supported; use unqualified BOOL, BOOLEAN, or TEXT without type modifiers",
		)
	}
	literalPos := literal.pos
	text := p.internToken(literal)
	if target == ScalarCastText {
		return Operand{Kind: OperandString, Text: text, Pos: literalPos}, nil
	}
	value, ok := pginput.Boolean(text)
	if !ok {
		return Operand{}, newInvalidTextRepresentationError(
			p.lx.src, literalPos, "boolean",
			"invalid input syntax for type boolean",
		)
	}
	return Operand{Kind: OperandBool, Bool: value, Pos: literalPos}, nil
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
	} else {
		left, err = p.parseScalarTypecasts(left)
	}
	if err != nil {
		return nil, err
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
			if len(path.Segments) >= 2 {
				for i := len(p.pending) - 1; i >= 0; i-- {
					entry := &p.pending[i]
					if entry.path == path {
						if entry.nestedPos == 0 {
							entry.nestedPos = pos + 1
						}
						break
					}
				}
			}
			segments := p.segs.allocDirty(len(path.Segments) + 1)
			copy(segments, path.Segments)
			segments[len(path.Segments)] = seg
			path.Segments = segments
			node := p.newScalar(ScalarCast, pos)
			node.Cast, node.Left, node.TargetPos = ScalarCastText, left, pos
			left = node
			continue
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
		left, err := p.parseScalarPrimary(ctx)
		if err != nil {
			return nil, err
		}
		return p.parseScalarTypecasts(left)
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

// parseScalarTypecasts implements PostgreSQL's highest-precedence TYPECAST
// production. PostgreSQL 18.6 gram.y places TYPECAST above UMINUS and defines
// it as `a_expr TYPECAST Typename`, so casts chain left-to-right and bind before
// unary signs and every arithmetic operator. Keeping the wrappers in the
// parser-owned scalar arena adds no per-parse or execution allocation.
func (p *Parser) parseScalarTypecasts(left *ScalarExpr) (*ScalarExpr, error) {
	for depth := 0; p.tok.kind == tokDoubleColon; depth++ {
		if depth >= maxExprDepth {
			return nil, p.errHere("a scalar expression may nest at most 64 levels")
		}
		pos := p.tok.pos
		p.advance()
		targetPos := p.tok.pos
		if p.tok.kind != tokIdent {
			return nil, newFeatureNotSupportedError(
				p.lx.src, targetPos,
				":: requires one supported unquoted target: TEXT, BOOLEAN, NUMERIC, DECIMAL, or JSON",
			)
		}
		target, ok := scalarCastTargetOf(p.tok.text)
		if !ok {
			return nil, newFeatureNotSupportedError(
				p.lx.src, targetPos,
				"cast target "+p.tok.text+" is not supported; use TEXT, BOOLEAN, NUMERIC, DECIMAL, or JSON",
			)
		}
		p.advance()
		if p.tok.kind == tokLParen || p.tok.kind == tokLBracket || p.tok.kind == tokDot {
			return nil, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"cast type modifiers, arrays, collations, and qualified targets are not supported",
			)
		}
		if err := p.validateTypedConstantCast(left, target, targetPos); err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarCast, pos)
		node.Cast, node.Left, node.TargetPos = target, left, targetPos
		left = node
	}
	return left, nil
}

// validateTypedConstantCast mirrors PostgreSQL's explicit-cast graph for the
// bounded BOOL/TEXT/NUMERIC/JSON domains. CoerceViaIO admits an edge when the
// source or target is TEXT; identity edges are always valid. Every other edge
// lacks a pg_cast entry and is rejected during analysis, including inside a
// dead CASE arm. Crucially, this checks only the type graph: valid conversions
// such as TEXT 'bad'::BOOL remain lazy and are not evaluated here.
func (p *Parser) validateTypedConstantCast(
	left *ScalarExpr, target ScalarCastTarget, targetPos int,
) error {
	source, known := typedConstantCastType(left)
	if !known || source == target || source == ScalarCastText || target == ScalarCastText {
		return nil
	}
	return newCannotCoerceError(
		p.lx.src, targetPos, scalarCastTypeName(source), scalarCastTypeName(target),
	)
}

// typedConstantCastType follows a cast chain or a resolved CASE rooted in
// PostgreSQL's type 'string' production. A nested CASE is its own analysis
// scope, but its selected BOOL/TEXT domain is concrete to the expression that
// contains it. Ordinary expressions retain their existing lazy runtime
// behavior; the parser does not speculate about row values or types.
func typedConstantCastType(expr *ScalarExpr) (ScalarCastTarget, bool) {
	target, known, typed := scalarCaseExpressionResolution(expr)
	return target, known && typed
}

func scalarCastTypeName(target ScalarCastTarget) string {
	switch target {
	case ScalarCastText:
		return "text"
	case ScalarCastBoolean:
		return "boolean"
	case ScalarCastNumeric:
		return "numeric"
	case ScalarCastJSON:
		return "json"
	default:
		return "unknown"
	}
}

func (p *Parser) parseScalarPrimary(ctx scalarExprContext) (*ScalarExpr, error) {
	pos := p.tok.pos
	if op, ok := conditionalScalarOp(p.tok); ok {
		return p.parseConditionalScalar(op, ctx)
	}
	if target, supported, known := scalarTypedStringHead(p.tok); known {
		head := p.tok
		p.advance()
		if p.tok.kind == tokString {
			value, err := p.parseTypedStringAfterHead(head, target, supported)
			if err != nil {
				return nil, err
			}
			literal := p.newScalar(ScalarLiteral, value.Pos)
			literal.Value = value
			node := p.newScalar(ScalarCast, value.Pos)
			node.Cast, node.Left, node.TargetPos = target, literal, head.pos
			node.TypedConstant = true
			return node, nil
		}
		if err := p.rejectUnsupportedTypedStringSuffix(head); err != nil {
			return nil, err
		}
		path, err := p.continuePath(head, ctx == scalarSelect)
		if err != nil {
			return nil, p.normalizeUpdateScalarPrimaryError(ctx, pos, err)
		}
		if err := p.rejectQualifiedTypedStringPath(head, path); err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarPath, path.Pos)
		node.Path = path
		return node, nil
	}
	switch {
	case p.tok.kind == tokLParen:
		openPos := p.tok.pos
		p.advance()
		if ctx == scalarUpdate && scalarQueryExpressionStarts(p.tok) {
			if p.atKeyword(kwSelect) || p.atKeyword(kwWith) {
				if _, err := p.parsePredicateSubquery(false); err != nil {
					return nil, err
				}
			}
			return nil, newFeatureNotSupportedError(
				p.lx.src, openPos,
				"scalar subqueries are not supported in UPDATE SET; compute the value before the mutation",
			)
		}
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
		if ctx == scalarWhere || ctx == scalarJoin || ctx == scalarUpdate {
			return nil, newFeatureNotSupportedError(
				p.lx.src, head.pos,
				"an aggregate is not allowed in "+ctx.clause()+" because the expression is evaluated once per input row",
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
			return nil, p.normalizeUpdateScalarPrimaryError(ctx, pos, err)
		}
		node := p.newScalar(ScalarPath, path.Pos)
		node.Path = path
		return node, nil
	default:
		path, err := p.parsePath(false)
		if err != nil {
			return nil, p.normalizeUpdateScalarPrimaryError(ctx, pos, err)
		}
		node := p.newScalar(ScalarPath, path.Pos)
		node.Path = path
		return node, nil
	}
}

func scalarQueryExpressionStarts(tok token) bool {
	return tok.kind == tokIdent && (tok.kw == kwSelect || tok.kw == kwWith ||
		tok.kw == kwValues || tok.kw == kwTable)
}

func (p *Parser) normalizeUpdateScalarPrimaryError(
	ctx scalarExprContext,
	pos int,
	err error,
) error {
	if ctx != scalarUpdate {
		return err
	}
	parse, ok := err.(*ParseError)
	if !ok || !strings.Contains(parse.Msg, "is not a supported function") {
		return err
	}
	return newFeatureNotSupportedError(p.lx.src, pos, parse.Msg)
}

// normalizeUpdateScalarError preserves ordinary syntax failures while turning
// a syntactically valid scalar-function call into the typed unsupported class
// used by protocol adapters. Function calls are diagnosed by parsePath after
// consuming their complete name, including calls nested inside CASE truth
// predicates where the scalar context is intentionally translated to WHERE.
func (p *Parser) normalizeUpdateScalarError(err error) error {
	parse, ok := err.(*ParseError)
	if !ok || !strings.Contains(parse.Msg, "is not a supported function") {
		return err
	}
	return newFeatureNotSupportedError(p.lx.src, parse.Pos, parse.Msg)
}

// validateUpdateScalarExpression enforces the bounded per-row UPDATE stage
// after parsing. Direct aggregate nodes are rejected while parsing; this walk
// also catches predicate subqueries retained inside searched CASE expressions.
func (p *Parser) validateUpdateScalarExpression(expr *ScalarExpr) error {
	if expr == nil {
		return nil
	}
	if expr.Kind == ScalarAggregate {
		return newFeatureNotSupportedError(
			p.lx.src, expr.Pos,
			"an aggregate is not allowed in UPDATE SET because the expression is evaluated once per input row",
		)
	}
	if err := p.validateUpdateScalarExpression(expr.Left); err != nil {
		return err
	}
	if err := p.validateUpdateScalarExpression(expr.Right); err != nil {
		return err
	}
	for i := range expr.Whens {
		if err := p.validateUpdatePredicateExpression(expr.Whens[i].Predicate); err != nil {
			return err
		}
		if err := p.validateUpdateScalarExpression(expr.Whens[i].Match); err != nil {
			return err
		}
		if err := p.validateUpdateScalarExpression(expr.Whens[i].Result); err != nil {
			return err
		}
	}
	return p.validateUpdateScalarExpression(expr.Else)
}

func (p *Parser) validateUpdatePredicateExpression(expr *Expr) error {
	if expr == nil {
		return nil
	}
	if expr.Subquery != nil {
		return newFeatureNotSupportedError(
			p.lx.src, expr.Pos,
			"subqueries are not supported in UPDATE SET expressions; compute the value before the mutation",
		)
	}
	if expr.Agg != AggNone {
		return newFeatureNotSupportedError(
			p.lx.src, expr.Pos,
			"an aggregate is not allowed in UPDATE SET because the expression is evaluated once per input row",
		)
	}
	if err := p.validateUpdateScalarExpression(expr.ScalarLeft); err != nil {
		return err
	}
	if err := p.validateUpdateScalarExpression(expr.ScalarRight); err != nil {
		return err
	}
	for _, child := range expr.Kids {
		if err := p.validateUpdatePredicateExpression(child); err != nil {
			return err
		}
	}
	return nil
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
	if err := p.validateTypedConstantCast(child, target, targetPos); err != nil {
		return nil, err
	}
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
