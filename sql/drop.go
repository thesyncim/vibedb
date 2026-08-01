package sql

// A DropTableStmt is one parsed DROP TABLE.
//
// DROP is a catalog operation rather than a document mutation. The SQL driver
// owns its durable ordering and collection lifecycle; this AST carries only
// the namespace change requested by the parser.
type DropTableStmt struct {
	// Table is the collection removed from the SQL catalog.
	Table string
	// IfExists makes dropping an absent table a successful no-op.
	IfExists bool
	// Pos is the byte offset of the collection name.
	Pos int
}
