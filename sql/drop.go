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

// A TruncateStmt is one parsed TRUNCATE [TABLE] table statement.
//
// This is syntax and intent only. Whether a storage adapter can publish an
// atomic collection clear is deliberately outside the parser's contract.
type TruncateStmt struct {
	// Table is the collection whose documents are to be removed.
	Table string
	// Pos is the byte offset of the collection name.
	Pos int
}

// A DropIndexStmt is one parsed DROP INDEX statement.
//
// ON table is optional because an index catalog may either have globally
// unique names or require a collection to disambiguate them. The AST preserves
// whether ON was written instead of inferring it from an empty string.
type DropIndexStmt struct {
	// Name is the index removed from the catalog.
	Name string
	// IfExists makes an absent index a successful no-op.
	IfExists bool
	// Table is the optional collection named by ON.
	Table string
	// HasTable records whether ON table was present.
	HasTable bool
	// Pos is the byte offset of the index name.
	Pos int
	// TablePos is the byte offset of Table when HasTable is true.
	TablePos int
}
