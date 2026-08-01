package conformance

// UnsupportedSQLCase is one leading SQL feature whose refusal must have the
// same typed reason through database/sql and pgwire.
type UnsupportedSQLCase struct {
	ID             string
	Statement      string
	ReasonContains string
}

// UnsupportedSQLCases is intentionally small but crosses the major missing
// statement families. The SQL parser owns the complete taxonomy; this manifest
// makes both public adapters execute representative entries from it.
var UnsupportedSQLCases = []UnsupportedSQLCase{
	{ID: "copy", Statement: "COPY docs TO STDOUT", ReasonContains: "COPY is not supported"},
	{ID: "savepoint", Statement: "SAVEPOINT nested", ReasonContains: "savepoints are not supported"},
	{ID: "alter", Statement: "ALTER TABLE docs ADD COLUMN n STRING", ReasonContains: "ALTER is not"},
	{ID: "cte", Statement: "WITH x AS (SELECT 1) SELECT * FROM x", ReasonContains: "common table expressions"},
	{ID: "listen", Statement: "LISTEN changes", ReasonContains: "asynchronous notification"},
}
