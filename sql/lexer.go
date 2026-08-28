package sql

import "fmt"

// The hand-written lexer. It is hand-written rather than generated or
// regexp-driven for two reasons the repository already holds itself to: the
// root module has no third-party dependencies, and regexp would put a compiled
// automaton plus a per-match allocation on a path whose contract is zero
// steady-state allocation.
//
// Every path through next either consumes at least one byte or returns tokEOF
// or tokError. That invariant is what makes the fuzz target's "never hangs"
// property structural rather than empirical: no input can leave the scanner in
// place, so the parser's token loops always terminate.

// A lexer scans src left to right. pos is the offset of the next unlexed byte.
// After next returns a token, pos sits exactly one byte past that token's last
// byte — the invariant the '@>' needle scan depends on, since it reads raw JSON
// straight out of the source starting from pos.
type lexer struct {
	src            string
	pos            int
	cancel         func() error
	cancelErr      error
	nextCancelByte int
	tokens         uint
}

const (
	parserCancelByteInterval  = 4 << 10
	parserCancelTokenInterval = 256
)

func (lx *lexer) captureCancellation() bool {
	if lx.cancelErr != nil {
		return true
	}
	if err := lx.cancel(); err != nil {
		lx.cancelErr = err
		return true
	}
	return false
}

func (lx *lexer) cancelAtToken() bool {
	if lx.cancel == nil {
		return false
	}
	lx.tokens++
	return lx.tokens%parserCancelTokenInterval == 1 && lx.captureCancellation()
}

func (lx *lexer) cancelAtByte() bool {
	if lx.cancel == nil || lx.pos < lx.nextCancelByte {
		return false
	}
	lx.nextCancelByte = lx.pos + parserCancelByteInterval
	return lx.captureCancellation()
}

func (lx *lexer) canceledToken() token {
	return errorToken(lx.pos, "parsing canceled")
}

// next returns the token beginning at or after pos and advances past it.
func (lx *lexer) next() token {
	if lx.cancelAtToken() {
		return lx.canceledToken()
	}
	for {
		if !lx.skipSpace() {
			return lx.canceledToken()
		}
		if lx.pos >= len(lx.src) {
			return token{kind: tokEOF, pos: lx.pos}
		}
		c := lx.src[lx.pos]
		if c == '-' && lx.pos+1 < len(lx.src) && lx.src[lx.pos+1] == '-' {
			if !lx.skipLineComment() {
				return lx.canceledToken()
			}
			continue
		}
		if c == '/' && lx.pos+1 < len(lx.src) && lx.src[lx.pos+1] == '*' {
			if bad, ok := lx.skipBlockComment(); !ok {
				return bad
			}
			continue
		}
		break
	}

	start := lx.pos
	c := lx.src[lx.pos]
	switch {
	case isIdentStart(c):
		return lx.lexIdent()
	case c >= '0' && c <= '9':
		return lx.lexNumber()
	case c == '-' && lx.pos+1 < len(lx.src) && isDigit(lx.src[lx.pos+1]):
		// A leading '-' is part of the numeric literal rather than a unary
		// operator, because the engine's literal space is JSON's and a JSON
		// number carries its own sign. The scalar parser reinterprets such a
		// token as subtraction when it follows a completed left expression.
		return lx.lexNumber()
	case c == '\'':
		return lx.lexQuoted('\'', tokString)
	case c == '"':
		return lx.lexQuoted('"', tokQuotedIdent)
	}

	lx.pos++
	switch c {
	case '*':
		return token{kind: tokStar, pos: start}
	case '+':
		return token{kind: tokPlus, pos: start}
	case '-':
		if lx.accept('>') {
			if lx.accept('>') {
				return token{kind: tokJSONText, pos: start}
			}
			return token{kind: tokJSONArrow, pos: start}
		}
		return token{kind: tokMinus, pos: start}
	case '/':
		return token{kind: tokSlash, pos: start}
	case '%':
		return token{kind: tokPercent, pos: start}
	case ',':
		return token{kind: tokComma, pos: start}
	case ';':
		return token{kind: tokSemicolon, pos: start}
	case '(':
		return token{kind: tokLParen, pos: start}
	case ')':
		return token{kind: tokRParen, pos: start}
	case '[':
		return token{kind: tokLBracket, pos: start}
	case ']':
		return token{kind: tokRBracket, pos: start}
	case '{':
		// A JSON document position consumes the complete object directly from
		// source text. Give its opener a constant token so looking ahead does
		// not format an "unexpected character" error that the JSON scanner
		// would immediately discard.
		return token{kind: tokLBrace, pos: start}
	case '.':
		return token{kind: tokDot, pos: start}
	case '?':
		return token{kind: tokParam, pos: start}
	case '=':
		return token{kind: tokEq, pos: start}
	case '!':
		if lx.accept('=') {
			return token{kind: tokNe, pos: start}
		}
		if lx.accept('~') {
			return errorToken(start, "regular-expression matching is not supported")
		}
		return errorToken(start, "expected '=' after '!'")
	case '<':
		if lx.accept('=') {
			return token{kind: tokLe, pos: start}
		}
		if lx.accept('>') {
			return token{kind: tokNe, pos: start}
		}
		return token{kind: tokLt, pos: start}
	case '>':
		if lx.accept('=') {
			return token{kind: tokGe, pos: start}
		}
		return token{kind: tokGt, pos: start}
	case '@':
		if lx.accept('>') {
			return token{kind: tokContains, pos: start}
		}
		return errorToken(start, "expected '>' after '@'; containment is spelled '@>'")
	case '|':
		if lx.accept('|') {
			return token{kind: tokConcat, pos: start}
		}
		return errorToken(start, "bitwise operators are not supported")
	case ':':
		if lx.accept(':') {
			return token{kind: tokDoubleColon, pos: start}
		}
		return errorToken(start, "named parameters are not supported; use '?' placeholders")
	case '$':
		return errorToken(start, "numbered parameters are not supported; use '?' placeholders")
	case '^', '&', '#':
		return errorToken(start, "bitwise operators are not supported")
	case '~':
		return errorToken(start, "regular-expression matching is not supported")
	}
	return errfToken(start, "unexpected character %q", string(rune(c)))
}

func (lx *lexer) skipSpace() bool {
	for lx.pos < len(lx.src) {
		if lx.cancelAtByte() {
			return false
		}
		switch lx.src[lx.pos] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			lx.pos++
		default:
			return true
		}
	}
	return true
}

// skipLineComment consumes "-- ..." through the end of the line. The newline
// itself is left for skipSpace, so line counting in errors stays correct.
func (lx *lexer) skipLineComment() bool {
	lx.pos += 2
	for lx.pos < len(lx.src) && lx.src[lx.pos] != '\n' {
		if lx.cancelAtByte() {
			return false
		}
		lx.pos++
	}
	return true
}

// skipBlockComment consumes "/* ... */". Block comments do not nest, matching
// the SQL standard: nesting them would make an unterminated inner comment
// swallow the rest of a statement silently instead of being reported.
func (lx *lexer) skipBlockComment() (token, bool) {
	start := lx.pos
	lx.pos += 2
	for lx.pos+1 < len(lx.src) {
		if lx.cancelAtByte() {
			return lx.canceledToken(), false
		}
		if lx.src[lx.pos] == '*' && lx.src[lx.pos+1] == '/' {
			lx.pos += 2
			return token{}, true
		}
		lx.pos++
	}
	lx.pos = len(lx.src)
	return errorToken(start, "unterminated block comment"), false
}

// accept consumes the next byte when it is c.
func (lx *lexer) accept(c byte) bool {
	if lx.pos < len(lx.src) && lx.src[lx.pos] == c {
		lx.pos++
		return true
	}
	return false
}

func (lx *lexer) lexIdent() token {
	start := lx.pos
	for lx.pos < len(lx.src) && isIdentPart(lx.src[lx.pos]) {
		if lx.cancelAtByte() {
			return lx.canceledToken()
		}
		lx.pos++
	}
	text := lx.src[start:lx.pos]
	return token{kind: tokIdent, kw: keywordOf(text), text: text, pos: start}
}

// lexQuoted scans a quoted run closed by quote, where a doubled quote stands
// for one literal quote — SQL's rule for both '...' strings and "..."
// identifiers. The returned text is the undecoded body; esc records whether a
// doubled quote was seen, so the parser decodes only the rare literal that
// needs it.
func (lx *lexer) lexQuoted(quote byte, kind tokenKind) token {
	start := lx.pos
	lx.pos++ // opening quote
	body := lx.pos
	escaped := false
	for lx.pos < len(lx.src) {
		if lx.cancelAtByte() {
			return lx.canceledToken()
		}
		if lx.src[lx.pos] != quote {
			lx.pos++
			continue
		}
		if lx.pos+1 < len(lx.src) && lx.src[lx.pos+1] == quote {
			escaped = true
			lx.pos += 2
			continue
		}
		text := lx.src[body:lx.pos]
		lx.pos++ // closing quote
		return token{kind: kind, text: text, pos: start, esc: escaped}
	}
	if quote == '\'' {
		return errorToken(start, "unterminated string literal")
	}
	return errorToken(start, "unterminated quoted identifier")
}

// lexNumber scans a numeric literal and holds it to JSON's number grammar
// rather than SQL's looser one.
//
// The narrower grammar is the right one because the literal is destined for the
// engine's exact-decimal literal space, which validates its spelling as JSON.
// Accepting "007" or "1." here would produce a statement that parses and then
// fails when it is lowered — exactly the split-brain rejection this package
// exists to avoid.
func (lx *lexer) lexNumber() token {
	start := lx.pos
	lx.accept('-')
	intStart := lx.pos
	for lx.pos < len(lx.src) && isDigit(lx.src[lx.pos]) {
		if lx.cancelAtByte() {
			return lx.canceledToken()
		}
		lx.pos++
	}
	switch {
	case lx.pos == intStart:
		return errorToken(start, "expected a digit in numeric literal")
	case lx.pos-intStart > 1 && lx.src[intStart] == '0':
		return errorToken(start, "numeric literal has a leading zero")
	}
	if lx.pos < len(lx.src) && lx.src[lx.pos] == '.' {
		lx.pos++
		fracStart := lx.pos
		for lx.pos < len(lx.src) && isDigit(lx.src[lx.pos]) {
			if lx.cancelAtByte() {
				return lx.canceledToken()
			}
			lx.pos++
		}
		if lx.pos == fracStart {
			return errorToken(start, "expected a digit after '.' in numeric literal")
		}
	}
	if lx.pos < len(lx.src) && (lx.src[lx.pos] == 'e' || lx.src[lx.pos] == 'E') {
		lx.pos++
		if lx.pos < len(lx.src) && (lx.src[lx.pos] == '+' || lx.src[lx.pos] == '-') {
			lx.pos++
		}
		expStart := lx.pos
		for lx.pos < len(lx.src) && isDigit(lx.src[lx.pos]) {
			if lx.cancelAtByte() {
				return lx.canceledToken()
			}
			lx.pos++
		}
		if lx.pos == expStart {
			return errorToken(start, "expected a digit in the exponent of a numeric literal")
		}
	}
	// A number butted against an identifier ("1abc") is a typo, not two
	// tokens: silently lexing it as 1 followed by abc would turn a mistyped
	// literal into a confusing error somewhere else in the statement.
	if lx.pos < len(lx.src) && isIdentStart(lx.src[lx.pos]) {
		return errorToken(lx.pos, "unexpected character after a numeric literal")
	}
	return token{kind: tokNumber, text: lx.src[start:lx.pos], pos: start}
}

func errorToken(pos int, msg string) token {
	return token{kind: tokError, text: msg, pos: pos}
}

func errfToken(pos int, format string, args ...any) token {
	return token{kind: tokError, text: fmt.Sprintf(format, args...), pos: pos}
}

// isIdentStart accepts ASCII letters, '_', and every byte above ASCII.
//
// Admitting the high bytes is deliberate. An identifier here is a JSON object
// key, and JSON keys are arbitrary UTF-8; requiring `"café"` to be quoted only
// because of its accent would make the common case of non-English data uglier
// than the SQL it replaces. Parser admission has already validated the whole
// statement as UTF-8, so treating every byte of a multibyte rune as an
// identifier byte cannot admit a malformed string into the AST.
func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
