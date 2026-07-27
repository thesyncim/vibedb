package pgwire

import (
	"errors"
	"strconv"
	"unicode/utf8"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// Errors, and the SQLSTATE codes clients branch on.
//
// A client does not read our prose. Every real driver exposes the five-character
// SQLSTATE and applications switch on it, so an ErrorResponse carrying a
// plausible-looking message and a wrong code is worse than one carrying no
// message at all: the application takes the wrong branch and never sees the
// text. The codes below are therefore chosen to mean what the PostgreSQL
// appendix says they mean, and each is used in exactly one situation.
//
// The important one is the boundary between 42601 and 0A000. A statement this
// dialect refuses is not the same kind of event as a statement it cannot parse,
// and the difference a client cares about is retryability: 0A000 says "this
// server will never run this", 42601 says "this text is wrong". The rule this
// package applies is decided before parsing, from the statement's leading
// keyword, and not from the parser's message. A statement whose first keyword
// is not one this server implements at all — INSERT, UPDATE, CREATE, ALTER,
// BEGIN, and the rest — is 0A000, named with the feature that is missing. A
// SELECT that the parser then rejects, for any reason including a deliberate
// refusal like a subquery or LIKE, is 42601 with the parser's own message and
// its position. Classifying refusals by matching the parser's message text was
// the alternative and was rejected: those messages are prose maintained in
// another package by other people, and a SQLSTATE that changes when someone
// improves a sentence is a SQLSTATE nobody can rely on.

// SQLSTATE codes this server emits. The names are the PostgreSQL condition
// names so a reader can find each one in the protocol appendix.
const (
	sqlstateSyntaxError            = "42601"
	sqlstateUndefinedTable         = "42P01"
	sqlstateUndefinedObject        = "42704"
	sqlstateFeatureNotSupported    = "0A000"
	sqlstateProtocolViolation      = "08P01"
	sqlstateInvalidPassword        = "28P01"
	sqlstateInvalidAuthorization   = "28000"
	sqlstateInvalidStatementName   = "26000"
	sqlstateInvalidCursorName      = "34000"
	sqlstateDuplicateStatement     = "42P05"
	sqlstateDuplicateCursor        = "42P03"
	sqlstateInvalidParameterValue  = "22023"
	sqlstateProgramLimitExceeded   = "54000"
	sqlstateObjectNotInPrereqState = "55000"
	sqlstateInternalError          = "XX000"
	sqlstateTooManyConnections     = "53300"
	sqlstateQueryCanceled          = "57014"
)

// A pgError is one ErrorResponse: a SQLSTATE, a message, and the optional
// fields that make it actionable.
type pgError struct {
	// severity is "ERROR" for a failure the session survives and "FATAL" for
	// one that ends it. The distinction is not cosmetic: a client that reads
	// FATAL stops waiting for ReadyForQuery, and a client that reads ERROR
	// waits for it. Sending the wrong one hangs the client.
	severity string
	code     string
	message  string
	hint     string
	// position is a 1-based character index into the statement text, or zero
	// for none. Characters, not bytes: the protocol counts characters and a
	// JSON key is arbitrary UTF-8, so a byte offset would point at the wrong
	// place in exactly the statements where pointing accurately matters most.
	position int
}

func (e *pgError) Error() string { return "pgwire: " + e.code + ": " + e.message }

// errorf builds an ERROR-severity failure.
func newError(code, message string) *pgError {
	return &pgError{severity: "ERROR", code: code, message: message}
}

// fatal builds a FATAL-severity failure, which ends the session.
func fatal(code, message string) *pgError {
	return &pgError{severity: "FATAL", code: code, message: message}
}

func (e *pgError) withHint(hint string) *pgError { e.hint = hint; return e }

// errorResponse writes e as an ErrorResponse.
//
// The 'V' field repeats the severity unlocalized. It was added in protocol 3.0
// precisely so a client would not have to parse a translated 'S', and a server
// that omits it forces every client to do the thing the field exists to
// prevent.
func (w *writer) errorResponse(e *pgError) {
	w.begin(msgErrorResponse)
	w.byte('S')
	w.str(e.severity)
	w.byte('V')
	w.str(e.severity)
	w.byte('C')
	w.str(e.code)
	w.byte('M')
	w.str(e.message)
	if e.hint != "" {
		w.byte('H')
		w.str(e.hint)
	}
	if e.position > 0 {
		w.byte('P')
		w.str(strconv.Itoa(e.position))
	}
	w.byte(0)
	w.end()
}

// asPGError maps an error from the SQL front end or the executor onto a
// SQLSTATE.
//
// A *sqlast.ParseError is the interesting case and the only one with a
// position. Its Pos is a byte offset, and the protocol's P field is a 1-based
// character index, so the conversion counts runes rather than adding one — the
// two agree for ASCII and disagree for exactly the statements that name a
// non-ASCII JSON key.
func asPGError(err error) *pgError {
	if err == nil {
		return nil
	}
	var already *pgError
	if errors.As(err, &already) {
		return already
	}
	var parse *sqlast.ParseError
	if errors.As(err, &parse) {
		e := newError(sqlstateSyntaxError, parse.Msg)
		e.position = 0
		return e
	}
	return newError(sqlstateInternalError, err.Error())
}

// asPGErrorIn is asPGError with the statement text available, which is what
// lets a parse error carry a position. The text is needed because the position
// is a character index and the error records a byte offset.
func asPGErrorIn(err error, src string) *pgError {
	e := asPGError(err)
	if e == nil {
		return nil
	}
	var parse *sqlast.ParseError
	if e.position == 0 && errors.As(err, &parse) {
		e.position = charPosition(src, parse.Pos)
	}
	return e
}

// charPosition converts a byte offset into the 1-based character index the
// protocol's P field wants.
func charPosition(src string, byteOffset int) int {
	if byteOffset < 0 {
		byteOffset = 0
	}
	if byteOffset > len(src) {
		byteOffset = len(src)
	}
	return utf8.RuneCountInString(src[:byteOffset]) + 1
}
