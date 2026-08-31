package pgwire

import (
	"errors"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
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
// dialect refuses is not the same kind of event as admitted SQL it cannot
// parse, and the difference a client cares about is retryability: 0A000 says
// "this server will never run this feature", while 42601 says "this admitted
// statement text is wrong". SELECT, stored-row mutations, bounded catalog DDL,
// and unsupported leading SQL kinds all reach the shared front end. Its typed
// FeatureNotSupportedError maps to 0A000 with the same reason database/sql
// returns; ordinary ParseError remains 42601 with a source
// position. SQLSTATEs never depend on matching another package's prose.

// SQLSTATE codes this server emits. The names are the PostgreSQL condition
// names so a reader can find each one in the protocol appendix.
const (
	sqlstateSyntaxError                   = "42601"
	sqlstateUndefinedTable                = "42P01"
	sqlstateDuplicateTable                = "42P07"
	sqlstateDuplicateObject               = "42710"
	sqlstateUndefinedObject               = "42704"
	sqlstateUndefinedColumn               = "42703"
	sqlstateAmbiguousColumn               = "42702"
	sqlstateAmbiguousAlias                = "42P09"
	sqlstateAmbiguousParameter            = "42P08"
	sqlstateDuplicateAlias                = "42712"
	sqlstateDuplicateColumn               = "42701"
	sqlstateNameTooLong                   = "42622"
	sqlstateDependentObjectsStillExist    = "2BP01"
	sqlstateInvalidColumnReference        = "42P10"
	sqlstateInvalidTableDefinition        = "42P16"
	sqlstateWrongObjectType               = "42809"
	sqlstateDatatypeMismatch              = "42804"
	sqlstateCannotCoerce                  = "42846"
	sqlstateUndefinedFunction             = "42883"
	sqlstateInvalidObjectDefinition       = "42P17"
	sqlstateFeatureNotSupported           = "0A000"
	sqlstateProtocolViolation             = "08P01"
	sqlstateInvalidPassword               = "28P01"
	sqlstateInvalidAuthorization          = "28000"
	sqlstateInvalidStatementName          = "26000"
	sqlstateInvalidCursorName             = "34000"
	sqlstateDuplicateStatement            = "42P05"
	sqlstateDuplicateCursor               = "42P03"
	sqlstateCharacterNotInRepertoire      = "22021"
	sqlstateDivisionByZero                = "22012"
	sqlstateNumericValueOutOfRange        = "22003"
	sqlstateInvalidTextRepresentation     = "22P02"
	sqlstateInvalidBinaryRepresentation   = "22P03"
	sqlstateInvalidParameterValue         = "22023"
	sqlstateCardinalityViolation          = "21000"
	sqlstateProgramLimitExceeded          = "54000"
	sqlstateObjectNotInPrereqState        = "55000"
	sqlstateInternalError                 = "XX000"
	sqlstateTooManyConnections            = "53300"
	sqlstateQueryCanceled                 = "57014"
	sqlstateUniqueViolation               = "23505"
	sqlstateCheckViolation                = "23514"
	sqlstateSerializationFailure          = "40001"
	sqlstateStatementCompletionUnknown    = "40003"
	sqlstateNoActiveTransaction           = "25P01"
	sqlstateFailedTransaction             = "25P02"
	sqlstateInvalidSavepointSpecification = "3B001"
	sqlstateActiveSQLTransaction          = "25001"
	sqlstateReadOnlyTransaction           = "25006"
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
	detail   string
	hint     string
	// cause retains the typed runtime error behind a protocol classification.
	// ErrorResponse serialization deliberately exposes only the fields above,
	// while in-process adapters and tests can still use errors.Is/errors.As to
	// distinguish an unknown commit outcome and its underlying device failure.
	// It is populated only on the error path and is never retained by a session.
	cause error
	// position is a 1-based character index into the statement text, or zero
	// for none. Characters, not bytes: the protocol counts characters and a
	// JSON key is arbitrary UTF-8, so a byte offset would point at the wrong
	// place in exactly the statements where pointing accurately matters most.
	position int
}

func (e *pgError) Error() string { return "pgwire: " + e.code + ": " + e.message }

// Unwrap preserves the typed SQL/storage error after SQLSTATE classification.
// In particular, a post-append durability failure continues to match both
// durable.ErrCommitOutcomeUnknown and its root cause.
func (e *pgError) Unwrap() error { return e.cause }

// errorf builds an ERROR-severity failure.
func newError(code, message string) *pgError {
	return &pgError{severity: "ERROR", code: code, message: message}
}

// fatal builds a FATAL-severity failure, which ends the session.
func fatal(code, message string) *pgError {
	return &pgError{severity: "FATAL", code: code, message: message}
}

func (e *pgError) withHint(hint string) *pgError     { e.hint = hint; return e }
func (e *pgError) withDetail(detail string) *pgError { e.detail = detail; return e }

func (e *pgError) withCause(cause error) *pgError {
	if e != nil && e.cause == nil && cause != nil {
		e.cause = cause
		// Keep every pgError safe for the standard errors.Is/errors.As walkers.
		// Those walkers intentionally do not detect cycles; retaining a cause
		// graph which reaches e again would make later classification hang.
		if !errorTreeAcyclic(e) {
			e.cause = nil
		}
	}
	return e
}

const maxProtocolErrorTreeNodes = 1024

// errorTreeAcyclic validates the Unwrap graph before this package calls the
// standard errors.Is/errors.As helpers or retains it behind a pgError. The Go
// helpers deliberately trust error implementations and do not detect cycles.
// Runtime/storage errors are small trees, so the bound also fails closed on a
// hostile or accidentally unbounded custom wrapper without constraining any
// legitimate chain.
func errorTreeAcyclic(root error) bool {
	if root == nil {
		return true
	}
	type frame struct {
		err  error
		exit bool
	}
	stack := []frame{{err: root}}
	// State 1 is on the active DFS path; state 2 is fully visited. Keeping the
	// two states distinguishes a real cycle from a shared errors.Join subtree.
	state := make(map[error]uint8)
	visited := 0
	for len(stack) != 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current.err == nil {
			continue
		}

		comparable := reflect.TypeOf(current.err).Comparable()
		if current.exit {
			if comparable {
				state[current.err] = 2
			}
			continue
		}
		if comparable {
			switch state[current.err] {
			case 1:
				return false
			case 2:
				continue
			}
			state[current.err] = 1
		}
		visited++
		if visited > maxProtocolErrorTreeNodes {
			return false
		}
		stack = append(stack, frame{err: current.err, exit: true})

		switch unwrapped := current.err.(type) {
		case interface{ Unwrap() []error }:
			children := unwrapped.Unwrap()
			if len(children) > maxProtocolErrorTreeNodes-visited ||
				len(stack) > maxProtocolErrorTreeNodes-len(children) {
				return false
			}
			for i := len(children) - 1; i >= 0; i-- {
				stack = append(stack, frame{err: children[i]})
			}
		case interface{ Unwrap() error }:
			if next := unwrapped.Unwrap(); next != nil {
				if len(stack) >= maxProtocolErrorTreeNodes {
					return false
				}
				stack = append(stack, frame{err: next})
			}
		}
	}
	return true
}

func invalidErrorTree() *pgError {
	return newError(sqlstateInternalError,
		"the runtime returned a cyclic or excessively deep error cause chain")
}

// classifiedProtocolError recognizes an error already classified by the
// protocol while still applying cycle safety and authoritative outer
// classifications such as statement_completion_unknown. The boolean is false
// for transport/runtime errors that have not crossed a SQLSTATE mapping
// boundary.
func classifiedProtocolError(err error) (*pgError, bool) {
	if err == nil {
		return nil, false
	}
	if !errorTreeAcyclic(err) {
		return invalidErrorTree(), true
	}
	var classified *pgError
	if !errors.As(err, &classified) {
		return nil, false
	}
	return asPGErrorAcyclic(err), true
}

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
	w.str(truncateErrorField(e.message))
	if e.detail != "" {
		w.byte('D')
		w.str(truncateErrorField(e.detail))
	}
	if e.hint != "" {
		w.byte('H')
		w.str(truncateErrorField(e.hint))
	}
	if e.position > 0 {
		w.byte('P')
		w.str(strconv.Itoa(e.position))
	}
	w.byte(0)
	w.end()
}

// truncateErrorField bounds diagnostic output while preserving valid UTF-8.
// The caller's message may contain a quoted token derived from a maximal SQL
// input; a client needs the reason and position, not megabytes of echoed text.
func truncateErrorField(text string) string {
	if len(text) <= maxErrorFieldBytes {
		return text
	}
	const suffix = "..."
	end := maxErrorFieldBytes - len(suffix)
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + suffix
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
	if !errorTreeAcyclic(err) {
		return invalidErrorTree()
	}
	return asPGErrorAcyclic(err)
}

func asPGErrorAcyclic(err error) (mapped *pgError) {
	// A direct pgError is already the protocol's authoritative classification.
	// Preserve pointer identity: callers may have added position/hint fields and
	// no outer runtime wrapper exists from which to infer a competing outcome.
	if direct, ok := err.(*pgError); ok {
		return direct
	}
	// Unknown completion changes the client's safe retry decision and therefore
	// dominates cancellation and every ordinary runtime or protocol
	// classification in the same error tree. Checking it before an existing
	// nested *pgError is essential: errors.Join(queryCanceled(), unknownOutcome)
	// must never invite the client to assume that no publication occurred.
	if errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		return newError(sqlstateStatementCompletionUnknown, err.Error()).
			withCause(err)
	}
	var already *pgError
	if errors.As(err, &already) {
		// Preserve an existing, more-specific SQLSTATE while retaining an outer
		// wrapper or errors.Join sibling. Pointing already.cause back at err
		// would create an unwrap cycle because err itself contains already, so
		// classify with a shallow copy whose cause is the complete outer chain.
		clone := *already
		clone.cause = nil
		clone.withCause(err)
		return &clone
	}
	var diagnostic interface{ SQLState() string }
	if errors.As(err, &diagnostic) && validSQLState(diagnostic.SQLState()) {
		result := newError(diagnostic.SQLState(), err.Error())
		var hinted interface{ SQLHint() string }
		if errors.As(err, &hinted) && hinted.SQLHint() != "" {
			result.withHint(hinted.SQLHint())
		}
		return result.withCause(err)
	}
	// SQLSTATE is an additional protocol classification, not a replacement for
	// the typed runtime failure. Keeping the complete original chain also means
	// a joined, more-specific error selected below does not discard sibling
	// causes. This defer runs only on the cold error path.
	defer func() { mapped.withCause(err) }()
	if errors.Is(err, query.ErrResultBudget) ||
		errors.Is(err, query.ErrIntermediateBudget) ||
		errors.Is(err, query.ErrAggregateBudget) ||
		errors.Is(err, query.ErrJoinPairBudget) ||
		errors.Is(err, query.ErrWorkBudget) ||
		errors.Is(err, query.ErrSpillBudget) ||
		errors.Is(err, query.ErrSQLViewExpansionLimit) ||
		errors.Is(err, sqldriver.ErrCatalogTooLarge) ||
		errors.Is(err, sqldriver.ErrTooManyTables) ||
		errors.Is(err, sqldriver.ErrTooManyViews) ||
		errors.Is(err, sqldriver.ErrTooManyRetiredTables) ||
		errors.Is(err, sqldriver.ErrTooManyStorageFiles) ||
		errors.Is(err, sqldriver.ErrArgumentsTooLarge) ||
		errors.Is(err, sqldriver.ErrTransactionTooLarge) ||
		errors.Is(err, sqldriver.ErrJoinMaterializationTooLarge) ||
		errors.Is(err, store.ErrTooLarge) ||
		errors.Is(err, store.ErrCheckpointTooLarge) ||
		errors.Is(err, durable.ErrBatchTooLarge) ||
		errors.Is(err, durable.ErrDocumentTooLarge) ||
		errors.Is(err, durable.ErrKeyTooLarge) {
		return newError(sqlstateProgramLimitExceeded, err.Error())
	}
	switch {
	case errors.Is(err, query.ErrCanceled):
		return queryCanceled()
	case errors.Is(err, query.ErrCardinalityViolation):
		return newError(sqlstateCardinalityViolation, err.Error())
	case errors.Is(err, query.ErrParameterType):
		return newError(sqlstateDatatypeMismatch, err.Error())
	case errors.Is(err, query.ErrScalarType):
		return newError(sqlstateDatatypeMismatch, err.Error())
	case errors.Is(err, query.ErrScalarDivisionByZero):
		return newError(sqlstateDivisionByZero, err.Error())
	case errors.Is(err, query.ErrScalarNumericRange):
		return newError(sqlstateNumericValueOutOfRange, err.Error())
	case errors.Is(err, query.ErrScalarInvalidText):
		return newError(sqlstateInvalidTextRepresentation, err.Error())
	case errors.Is(err, query.ErrUndefinedColumn):
		return newError(sqlstateUndefinedColumn, err.Error())
	case errors.Is(err, query.ErrAmbiguousColumn):
		return newError(sqlstateAmbiguousColumn, err.Error())
	case errors.Is(err, query.ErrInvalidPattern):
		return newError(sqlstateInvalidParameterValue, err.Error())
	case errors.Is(err, query.ErrWindowArgument):
		return newError(sqlstateInvalidParameterValue, err.Error())
	case errors.Is(err, query.ErrSetTreeArity):
		return newError(sqlstateSyntaxError, err.Error())
	case errors.Is(err, query.ErrSQLViewCycle):
		return newError(sqlstateInvalidObjectDefinition, err.Error())
	case errors.Is(err, query.ErrSQLViewColumnArity):
		return newError(sqlstateInvalidTableDefinition, err.Error())
	case errors.Is(err, sqldriver.ErrTableNotFound):
		return newError(sqlstateUndefinedTable, err.Error())
	case errors.Is(err, sqldriver.ErrViewNotFound):
		return newError(sqlstateUndefinedTable, err.Error())
	case errors.Is(err, sqldriver.ErrViewChanged):
		return newError(sqlstateFeatureNotSupported, err.Error()).
			withHint("prepare the statement again so it binds the current view definition")
	case errors.Is(err, sqldriver.ErrTableExists):
		return newError(sqlstateDuplicateTable, err.Error())
	case errors.Is(err, sqldriver.ErrViewExists):
		return newError(sqlstateDuplicateTable, err.Error())
	case errors.Is(err, sqldriver.ErrDependentObjects):
		return newError(sqlstateDependentObjectsStillExist, err.Error())
	case errors.Is(err, sqldriver.ErrColumnExists):
		return newError(sqlstateDuplicateColumn, err.Error())
	case errors.Is(err, sqldriver.ErrDuplicateViewColumn):
		return newError(sqlstateDuplicateColumn, err.Error())
	case errors.Is(err, sqldriver.ErrWrongObjectType):
		result := newError(sqlstateWrongObjectType, err.Error())
		var hinted interface{ SQLHint() string }
		if errors.As(err, &hinted) && hinted.SQLHint() != "" {
			result.withHint(hinted.SQLHint())
		}
		return result
	case errors.Is(err, sqldriver.ErrIndexExists):
		return newError(sqlstateDuplicateObject, err.Error())
	case errors.Is(err, sqldriver.ErrIndexNotFound):
		return newError(sqlstateUndefinedObject, err.Error())
	case errors.Is(err, sqldriver.ErrIndexAmbiguous):
		return newError(sqlstateAmbiguousAlias, err.Error())
	case errors.Is(err, sqldriver.ErrDuplicatePrimaryKey),
		errors.Is(err, sqldriver.ErrUniqueConstraint):
		return newError(sqlstateUniqueViolation, err.Error())
	case errors.Is(err, store.ErrSchemaViolation):
		return newError(sqlstateCheckViolation, err.Error())
	case errors.Is(err, store.ErrSchemaDefinition),
		errors.Is(err, store.ErrIndexDefinition):
		return newError(sqlstateInvalidObjectDefinition, err.Error())
	case errors.Is(err, sqldriver.ErrTransactionConflict):
		return newError(sqlstateSerializationFailure, err.Error())
	case errors.Is(err, sqldriver.ErrTransactionFailed):
		return newError(sqlstateFailedTransaction, err.Error())
	case errors.Is(err, sqldriver.ErrSavepointNotFound):
		return newError(sqlstateInvalidSavepointSpecification, err.Error())
	case errors.Is(err, sqldriver.ErrTooManySavepoints):
		return newError(sqlstateProgramLimitExceeded, err.Error())
	case errors.Is(err, sqldriver.ErrNoTransaction):
		return newError(sqlstateNoActiveTransaction, err.Error())
	case errors.Is(err, sqldriver.ErrTransactionActive):
		return newError(sqlstateActiveSQLTransaction, err.Error())
	case errors.Is(err, sqldriver.ErrCursorOpen):
		return newError(sqlstateObjectNotInPrereqState, err.Error())
	case errors.Is(err, sqldriver.ErrIndexBuildInProgress):
		return newError(sqlstateObjectNotInPrereqState, err.Error())
	case errors.Is(err, sqldriver.ErrReadOnlyTransaction):
		return newError(sqlstateReadOnlyTransaction, err.Error())
	case errors.Is(err, sqldriver.ErrDDLInTransaction):
		return newError(sqlstateActiveSQLTransaction, err.Error())
	case errors.Is(err, sqldriver.ErrUnsupportedIsolation),
		errors.Is(err, sqldriver.ErrUpdatePrimaryKey),
		errors.Is(err, sqldriver.ErrTransactionUnsupportedLane):
		return newError(sqlstateFeatureNotSupported, err.Error())
	}
	var unsupported *sqlast.FeatureNotSupportedError
	if errors.As(err, &unsupported) {
		return newError(sqlstateFeatureNotSupported, unsupported.Msg)
	}
	var invalidText *sqlast.InvalidTextRepresentationError
	if errors.As(err, &invalidText) {
		return newError(sqlstateInvalidTextRepresentation, invalidText.Msg)
	}
	var cannotCoerce *sqlast.CannotCoerceError
	if errors.As(err, &cannotCoerce) {
		return newError(sqlstateCannotCoerce, cannotCoerce.Msg)
	}
	var undefinedOperator *sqlast.UndefinedOperatorError
	if errors.As(err, &undefinedOperator) {
		return newError(sqlstateUndefinedFunction, undefinedOperator.Msg)
	}
	var duplicateCTE *sqlast.DuplicateCTEError
	if errors.As(err, &duplicateCTE) {
		return newError(sqlstateDuplicateAlias, duplicateCTE.Msg)
	}
	var ambiguousOutput *sqlast.AmbiguousOutputError
	if errors.As(err, &ambiguousOutput) {
		return newError(sqlstateAmbiguousColumn, ambiguousOutput.Msg)
	}
	var invalidOrderPosition *sqlast.InvalidOrderPositionError
	if errors.As(err, &invalidOrderPosition) {
		return newError(sqlstateInvalidColumnReference, invalidOrderPosition.Msg)
	}
	var cteAliasArity *sqlast.CTEColumnAliasArityError
	if errors.As(err, &cteAliasArity) {
		return newError(sqlstateInvalidColumnReference, cteAliasArity.Msg)
	}
	var runtimeCTEAliasArity *query.CTEColumnAliasArityError
	if errors.As(err, &runtimeCTEAliasArity) {
		return newError(sqlstateInvalidColumnReference, runtimeCTEAliasArity.Error())
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
	if err == nil {
		return nil
	}
	if !errorTreeAcyclic(err) {
		return invalidErrorTree()
	}
	e := asPGErrorAcyclic(err)
	var diagnosticPosition interface{ SQLPosition() (int, bool) }
	if e.position == 0 && errors.As(err, &diagnosticPosition) {
		if position, ok := diagnosticPosition.SQLPosition(); ok {
			e.position = charPosition(src, position)
		}
	}
	var positioned interface{ Position() int }
	if e.position == 0 && errors.As(err, &positioned) {
		e.position = charPosition(src, positioned.Position())
	}
	var parse *sqlast.ParseError
	if e.position == 0 && errors.As(err, &parse) {
		e.position = charPosition(src, parse.Pos)
	}
	return e
}

func validSQLState(code string) bool {
	if len(code) != 5 {
		return false
	}
	for i := range code {
		if code[i] < '0' || code[i] > '9' {
			if code[i] < 'A' || code[i] > 'Z' {
				return false
			}
		}
	}
	return true
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
