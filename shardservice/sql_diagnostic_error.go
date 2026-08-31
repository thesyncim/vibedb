package shardservice

import (
	"errors"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// classifySQLDiagnostic mirrors the SQL semantic classes exposed by pgwire.
// It is deliberately a closed classifier: an unknown execution error remains
// a legacy malformed-request error and reaches pgwire as XX000 rather than
// acquiring a plausible but invented SQLSTATE.
func classifySQLDiagnostic(err error) (SQLDiagnostic, bool) {
	if err == nil {
		return SQLDiagnostic{}, false
	}

	code := ""
	message := err.Error()
	hint := ""
	switch {
	case errors.Is(err, query.ErrCardinalityViolation):
		code = "21000"
	case errors.Is(err, query.ErrParameterType), errors.Is(err, query.ErrScalarType):
		code = "42804"
	case errors.Is(err, query.ErrScalarDivisionByZero):
		code = "22012"
	case errors.Is(err, query.ErrScalarNumericRange):
		code = "22003"
	case errors.Is(err, query.ErrScalarInvalidText):
		code = "22P02"
	case errors.Is(err, query.ErrUndefinedColumn):
		code = "42703"
	case errors.Is(err, query.ErrAmbiguousColumn):
		code = "42702"
	case errors.Is(err, query.ErrInvalidPattern), errors.Is(err, query.ErrWindowArgument):
		code = "22023"
	case errors.Is(err, query.ErrSetTreeArity):
		code = "42601"
	case errors.Is(err, query.ErrSQLViewCycle):
		code = "42P17"
	case errors.Is(err, query.ErrSQLViewColumnArity):
		code = "42P16"
	case errors.Is(err, query.ErrSQLViewExpansionLimit),
		errors.Is(err, sqldriver.ErrCatalogTooLarge),
		errors.Is(err, sqldriver.ErrTooManyTables),
		errors.Is(err, sqldriver.ErrTooManyViews),
		errors.Is(err, sqldriver.ErrTooManyRetiredTables),
		errors.Is(err, sqldriver.ErrTooManyStorageFiles),
		errors.Is(err, sqldriver.ErrArgumentsTooLarge),
		errors.Is(err, sqldriver.ErrTransactionTooLarge),
		errors.Is(err, sqldriver.ErrJoinMaterializationTooLarge),
		errors.Is(err, store.ErrTooLarge),
		errors.Is(err, store.ErrCheckpointTooLarge),
		errors.Is(err, durable.ErrBatchTooLarge),
		errors.Is(err, durable.ErrDocumentTooLarge),
		errors.Is(err, durable.ErrKeyTooLarge):
		code = "54000"
	case errors.Is(err, sqldriver.ErrTableNotFound), errors.Is(err, sqldriver.ErrViewNotFound):
		code = "42P01"
	case errors.Is(err, sqldriver.ErrViewChanged):
		code = "0A000"
		hint = "prepare the statement again so it binds the current view definition"
	case errors.Is(err, sqldriver.ErrTableExists), errors.Is(err, sqldriver.ErrViewExists):
		code = "42P07"
	case errors.Is(err, sqldriver.ErrDependentObjects):
		code = "2BP01"
	case errors.Is(err, sqldriver.ErrColumnExists), errors.Is(err, sqldriver.ErrDuplicateViewColumn):
		code = "42701"
	case errors.Is(err, sqldriver.ErrWrongObjectType):
		code = "42809"
	case errors.Is(err, sqldriver.ErrIndexExists):
		code = "42710"
	case errors.Is(err, sqldriver.ErrIndexNotFound):
		code = "42704"
	case errors.Is(err, sqldriver.ErrIndexAmbiguous):
		code = "42P09"
	case errors.Is(err, sqldriver.ErrDuplicatePrimaryKey), errors.Is(err, sqldriver.ErrUniqueConstraint):
		code = "23505"
	case errors.Is(err, store.ErrSchemaViolation):
		code = "23514"
	case errors.Is(err, store.ErrSchemaDefinition), errors.Is(err, store.ErrIndexDefinition):
		code = "42P17"
	case errors.Is(err, sqldriver.ErrTransactionConflict):
		code = "40001"
	case errors.Is(err, sqldriver.ErrTransactionFailed):
		code = "25P02"
	case errors.Is(err, sqldriver.ErrSavepointNotFound):
		code = "3B001"
	case errors.Is(err, sqldriver.ErrTooManySavepoints):
		code = "54000"
	case errors.Is(err, sqldriver.ErrNoTransaction):
		code = "25P01"
	case errors.Is(err, sqldriver.ErrTransactionActive):
		code = "25001"
	case errors.Is(err, sqldriver.ErrCursorOpen), errors.Is(err, sqldriver.ErrIndexBuildInProgress):
		code = "55000"
	case errors.Is(err, sqldriver.ErrReadOnlyTransaction):
		code = "25006"
	case errors.Is(err, sqldriver.ErrDDLInTransaction):
		code = "25001"
	case errors.Is(err, sqldriver.ErrUnsupportedIsolation),
		errors.Is(err, sqldriver.ErrUpdatePrimaryKey),
		errors.Is(err, sqldriver.ErrTransactionUnsupportedLane):
		code = "0A000"
	}

	if code == "" {
		var unsupported *sqlast.FeatureNotSupportedError
		if errors.As(err, &unsupported) {
			code, message = "0A000", unsupported.Msg
		}
	}
	if code == "" {
		var invalidText *sqlast.InvalidTextRepresentationError
		if errors.As(err, &invalidText) {
			code, message = "22P02", invalidText.Msg
		}
	}
	if code == "" {
		var cannotCoerce *sqlast.CannotCoerceError
		if errors.As(err, &cannotCoerce) {
			code, message = "42846", cannotCoerce.Msg
		}
	}
	if code == "" {
		var undefinedOperator *sqlast.UndefinedOperatorError
		if errors.As(err, &undefinedOperator) {
			code, message = "42883", undefinedOperator.Msg
		}
	}
	if code == "" {
		var duplicateCTE *sqlast.DuplicateCTEError
		if errors.As(err, &duplicateCTE) {
			code, message = "42712", duplicateCTE.Msg
		}
	}
	if code == "" {
		var ambiguousOutput *sqlast.AmbiguousOutputError
		if errors.As(err, &ambiguousOutput) {
			code, message = "42702", ambiguousOutput.Msg
		}
	}
	if code == "" {
		var invalidOrderPosition *sqlast.InvalidOrderPositionError
		if errors.As(err, &invalidOrderPosition) {
			code, message = "42P10", invalidOrderPosition.Msg
		}
	}
	if code == "" {
		var cteAliasArity *sqlast.CTEColumnAliasArityError
		if errors.As(err, &cteAliasArity) {
			code, message = "42P10", cteAliasArity.Msg
		}
	}
	if code == "" {
		var runtimeCTEAliasArity *query.CTEColumnAliasArityError
		if errors.As(err, &runtimeCTEAliasArity) {
			code, message = "42P10", runtimeCTEAliasArity.Error()
		}
	}
	if code == "" {
		var parse *sqlast.ParseError
		if errors.As(err, &parse) {
			code, message = "42601", parse.Msg
		}
	}
	if code == "" {
		var carried interface{ SQLState() string }
		if errors.As(err, &carried) && transportSQLState(carried.SQLState()) {
			code = carried.SQLState()
		}
	}
	if code == "" {
		return SQLDiagnostic{}, false
	}

	diagnostic := SQLDiagnostic{Code: code, Message: message, Hint: hint}
	var carriedPosition interface{ SQLPosition() (int, bool) }
	if errors.As(err, &carriedPosition) {
		if position, ok := carriedPosition.SQLPosition(); ok && position >= 0 {
			diagnostic.Position = position
			diagnostic.HasPosition = true
		}
	}
	if !diagnostic.HasPosition {
		var positioned interface{ Position() int }
		if errors.As(err, &positioned) {
			if position := positioned.Position(); position >= 0 {
				diagnostic.Position = position
				diagnostic.HasPosition = true
			}
		}
	}
	if !diagnostic.HasPosition {
		var parse *sqlast.ParseError
		if errors.As(err, &parse) && parse.Pos >= 0 {
			diagnostic.Position = parse.Pos
			diagnostic.HasPosition = true
		}
	}
	var hinted interface{ SQLHint() string }
	if errors.As(err, &hinted) && hinted.SQLHint() != "" {
		diagnostic.Hint = hinted.SQLHint()
	}
	return diagnostic, true
}

// transportSQLState is the closed set this shard version knows how to create.
// A generic carrier is useful for independently evolving SQL runtimes, but a
// typo or an internal XX-class failure must not become an authoritative wire
// classification merely because it implements a similarly named method.
func transportSQLState(code string) bool {
	switch code {
	case "0A000", "21000", "22003", "22012", "22023", "22P02", "23505", "23514",
		"25001", "25006", "25P01", "25P02", "3B001", "40001", "42601", "42701",
		"42702", "42703", "42704", "42710", "42712", "42804", "42809", "42846",
		"42883", "42P01", "42P07", "42P09", "42P10", "42P16", "42P17", "54000",
		"55000":
		return true
	default:
		return false
	}
}
