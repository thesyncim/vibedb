package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// A ParseError reports one rejected SQL statement. Every parser error is a
// *ParseError or unwraps to one, and every one names a position, because the
// caller's next action is almost always to point at the offending byte: a
// database/sql driver surfaces the message to an application author who has the
// statement text in front of them but not the parser's state.
//
// Pos is the authoritative position; Line and Col are derived from it once, at
// construction, so a caller that only formats the message never pays for them
// twice and a caller that wants to slice the source has the offset directly.
type ParseError struct {
	// Pos is the byte offset in the statement text where parsing stopped. It
	// is always within [0, len(src)]; len(src) means "at end of input".
	Pos int
	// Line is the 1-based line number containing Pos.
	Line int
	// Col is the 1-based column of Pos within that line, counted in bytes
	// rather than runes. Bytes are what Pos already is, and a mixed unit would
	// make Line/Col disagree with Pos for any statement containing non-ASCII
	// text — which, since JSON keys are arbitrary UTF-8, is not a rare case
	// here.
	Col int
	// Msg says what was wrong and, wherever the construct is one this package
	// deliberately does not support, what to write instead.
	Msg string
}

// Error formats the failure with its position.
func (e *ParseError) Error() string {
	return "sql: " + strconv.Itoa(e.Line) + ":" + strconv.Itoa(e.Col) +
		" (offset " + strconv.Itoa(e.Pos) + "): " + e.Msg
}

// newParseError locates pos within src and builds the error. The line scan is
// deliberately performed here rather than lazily in Error: an error value is
// often logged long after the source text is gone, and a *ParseError that
// borrowed src to compute its own position would either pin the whole statement
// text or report the wrong one.
func newParseError(src string, pos int, msg string) *ParseError {
	error := parseErrorAt(src, pos, msg)
	return &error
}

func parseErrorAt(src string, pos int, msg string) ParseError {
	if pos < 0 {
		pos = 0
	}
	if pos > len(src) {
		pos = len(src)
	}
	line := 1 + strings.Count(src[:pos], "\n")
	col := pos + 1
	if nl := strings.LastIndexByte(src[:pos], '\n'); nl >= 0 {
		col = pos - nl
	}
	return ParseError{Pos: pos, Line: line, Col: col, Msg: msg}
}

// errAt builds a positioned error. It is a method so every rejection in the
// parser reads the same and none of them can forget to carry a position.
func (p *Parser) errAt(pos int, msg string) error {
	if p.lx.cancelErr != nil {
		return p.lx.cancelErr
	}
	return newParseError(p.lx.src, pos, msg)
}

// errfAt is errAt with formatting, for the messages that quote the offending
// text. Formatting allocates, which is why the plain form exists and is used
// for every fixed message: a rejection is cold, but a fuzz campaign runs
// millions of them.
func (p *Parser) errfAt(pos int, format string, args ...any) error {
	if p.lx.cancelErr != nil {
		return p.lx.cancelErr
	}
	return newParseError(p.lx.src, pos, fmt.Sprintf(format, args...))
}

// errHere reports at the current token. When the current token is itself a
// lexical failure, its own message wins: "expected a comparison operator" is
// useless next to "unterminated string literal", and the lexer already knows
// which byte broke.
func (p *Parser) errHere(msg string) error {
	if p.lx.cancelErr != nil {
		return p.lx.cancelErr
	}
	if p.tok.kind == tokError {
		return newParseError(p.lx.src, p.tok.pos, p.tok.text)
	}
	return newParseError(p.lx.src, p.tok.pos, msg)
}

func (p *Parser) errfHere(format string, args ...any) error {
	if p.lx.cancelErr != nil {
		return p.lx.cancelErr
	}
	if p.tok.kind == tokError {
		return newParseError(p.lx.src, p.tok.pos, p.tok.text)
	}
	return newParseError(p.lx.src, p.tok.pos, fmt.Sprintf(format, args...))
}
