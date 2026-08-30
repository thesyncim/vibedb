package serviceauthz

import (
	"sync"

	sqlast "github.com/thesyncim/vibedb/sql"
)

const maxRetainedAuthorizationSQLBytes = 64 << 10

type authorizationSQLParser struct {
	parser    sqlast.Parser
	statement sqlast.Statement
}

var authorizationSQLParsers = sync.Pool{
	New: func() any { return new(authorizationSQLParser) },
}

// SQLCapability parses the complete canonical statement and derives authority
// from its sealed AST kind. A prefix is never sufficient evidence: malformed,
// unsupported, mixed, or trailing syntax requires every SQL capability and
// therefore fails closed before execution. Warm parsers are pooled so the
// successful authorization path remains allocation-free.
func SQLCapability(sql string) Capability {
	workspace := authorizationSQLParsers.Get().(*authorizationSQLParser)
	err := workspace.parser.ParseStatement(&workspace.statement, sql)
	kind := workspace.statement.Kind
	workspace.statement = sqlast.Statement{}
	if len(sql) > maxRetainedAuthorizationSQLBytes {
		workspace.parser.Release()
	}
	authorizationSQLParsers.Put(workspace)
	if err != nil {
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
	switch kind {
	case sqlast.KindCreateTable, sqlast.KindCreateIndex, sqlast.KindAlterTable, sqlast.KindCreateView,
		sqlast.KindDropTable, sqlast.KindDropIndex, sqlast.KindDropView, sqlast.KindTruncate:
		return CapabilitySchema
	case sqlast.KindInsert, sqlast.KindUpdate, sqlast.KindDelete,
		sqlast.KindSavepoint, sqlast.KindReleaseSavepoint, sqlast.KindRollbackToSavepoint:
		return CapabilityDataWrite
	case sqlast.KindSelect:
		return CapabilityDataRead
	default:
		return CapabilityDataRead | CapabilityDataWrite | CapabilitySchema
	}
}
