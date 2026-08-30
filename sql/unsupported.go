package sql

import "strings"

// FeatureNotSupportedError is a positioned refusal of a valid SQL statement
// kind that this engine deliberately does not implement. It unwraps to its
// ParseError so callers keep byte/line/column diagnostics while adapters can
// map the semantic class without matching prose (for example pgwire maps it to
// SQLSTATE 0A000).
type FeatureNotSupportedError struct {
	ParseError
}

func (e *FeatureNotSupportedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ParseError
}

func newFeatureNotSupportedError(
	src string, pos int, reason string,
) *FeatureNotSupportedError {
	return NewFeatureNotSupportedError(src, pos, reason)
}

// NewFeatureNotSupportedError constructs a typed, source-positioned refusal
// for a valid SQL shape that a lowering or execution layer cannot implement
// faithfully yet. It lets downstream SQL consumers preserve the parser's
// FeatureNotSupportedError contract and protocol adapters' SQLSTATE 0A000
// mapping without matching error prose.
func NewFeatureNotSupportedError(
	src string, pos int, reason string,
) *FeatureNotSupportedError {
	return &FeatureNotSupportedError{ParseError: parseErrorAt(src, pos, reason)}
}

// unsupportedStatements is the one shared taxonomy for leading SQL kinds the
// runtime does not implement. Database/sql receives the typed parser error and
// pgwire maps that same type and reason to 0A000; neither adapter owns a second
// opinion about the feature boundary.
var unsupportedStatements = map[string]string{
	"MERGE":   "MERGE is not in the bounded mutation subset; use explicit INSERT, UPDATE, or DELETE",
	"REPLACE": "REPLACE is not supported; INSERT into an existing key is refused, and deliberate overwrites use UPDATE",
	"COPY":    "COPY is not supported: the wire endpoint does not implement the copy subprotocol",

	"GRANT":   "there is no SQL privilege catalog: connection authentication authorizes the configured database as one unit",
	"REVOKE":  "there is no SQL privilege catalog: connection authentication authorizes the configured database as one unit",
	"COMMENT": "catalog comments are not stored by the bounded SQL catalog",
	"VACUUM":  "storage maintenance is the owning application's, not a client's",
	"ANALYZE": "storage maintenance is the owning application's, not a client's",
	"REINDEX": "storage maintenance is the owning application's, not a client's",
	"CLUSTER": "storage maintenance is the owning application's, not a client's",

	"PREPARE":    "SQL-level PREPARE is not supported: use the adapter's prepared-statement API",
	"EXECUTE":    "SQL-level EXECUTE is not supported: use the adapter's prepared-statement API",
	"DEALLOCATE": "SQL-level DEALLOCATE is not supported: close the adapter's prepared statement",

	"DECLARE": "cursors are not supported: use the adapter's streaming rows or pgwire portals",
	"FETCH":   "SQL cursors are not supported: use the adapter's streaming rows or pgwire portals",
	"MOVE":    "SQL cursors are not supported: use the adapter's streaming rows or pgwire portals",

	"LISTEN":   "asynchronous notification is not supported",
	"NOTIFY":   "asynchronous notification is not supported",
	"UNLISTEN": "asynchronous notification is not supported",

	"LOCK": "there is no SQL lock manager: immutable readers never block writers",
	"CALL": "there are no stored procedures",
	"DO":   "there is no procedural language",
}

func unsupportedStatementReason(tok token) (string, bool) {
	if tok.kind != tokIdent {
		return "", false
	}
	// These are admitted leading kinds. Avoid case folding or a map lookup on
	// every successful parse; classification belongs only on the cold refusal
	// path.
	switch tok.kw {
	case kwSelect, kwInsert, kwUpdate, kwDelete, kwCreate, kwDrop, kwTruncate,
		kwSavepoint, kwRelease, kwRollback:
		return "", false
	}
	reason, ok := unsupportedStatements[strings.ToUpper(tok.text)]
	return reason, ok
}

func (p *Parser) featureNotSupportedHere(reason string) error {
	return newFeatureNotSupportedError(p.lx.src, p.tok.pos, reason)
}
