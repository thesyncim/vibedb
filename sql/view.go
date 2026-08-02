package sql

// CreateViewStmt is one durable ordinary-view definition. Query is the parsed
// body used for immediate validation; QuerySQL is the normalized, self-owned
// body persisted by catalog adapters and reparsed on reopen. Views never carry
// bind parameters: a stored definition has no execution that could supply
// their values.
type CreateViewStmt struct {
	Name         string
	Columns      []string
	ColumnPos    []int
	Query        *SelectStmt
	QuerySQL     string
	Pos          int
	QueryPos     int
	Materialized bool
}

// DropViewStmt removes one ordinary view. RESTRICT is the only implemented
// dependency behavior and is also the default; CASCADE is parsed and refused
// rather than silently weakened.
type DropViewStmt struct {
	Name        string
	IfExists    bool
	Restrict    bool
	Pos         int
	BehaviorPos int
}
