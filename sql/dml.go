package sql

// The data-manipulation half of the abstract syntax tree.
//
// The shape here follows one decision, and everything else in this file is a
// consequence of it: a DML statement's row selection is not a second dialect,
// it is the SELECT dialect. An UPDATE's or a DELETE's WHERE is parsed by the
// same predicate grammar, resolved by the same path rule, and carried in a real
// [SelectStmt] — see [UpdateStmt.Filter] — so a lowering pass reuses the SELECT
// lowering verbatim instead of reimplementing it. That is not a convenience.
// The engine's three-valued lowering is subtle enough that a second
// implementation would disagree with the first on exactly the inputs nobody
// tests, and "DELETE removes what SELECT returns" would become a hope rather
// than a structural fact.

// A Kind names a statement's sort. It exists because a driver has to route
// before it can execute: database/sql sends a SELECT to Query and everything
// else to Exec, and the routing decision has to be available without running
// the statement.
type Kind uint8

const (
	// KindSelect is a SELECT statement, carried in [Statement.Select].
	KindSelect Kind = iota
	// KindInsert is an INSERT, carried in [Statement.Insert].
	KindInsert
	// KindUpdate is an UPDATE, carried in [Statement.Update].
	KindUpdate
	// KindDelete is a DELETE, carried in [Statement.Delete].
	KindDelete
	// KindCreateTable is a CREATE TABLE, carried in [Statement.CreateTable].
	KindCreateTable
	// KindCreateIndex is a CREATE INDEX, carried in [Statement.CreateIndex].
	KindCreateIndex
)

// String answers the statement's leading keyword.
func (k Kind) String() string {
	switch k {
	case KindInsert:
		return "INSERT"
	case KindUpdate:
		return "UPDATE"
	case KindDelete:
		return "DELETE"
	case KindCreateTable:
		return "CREATE TABLE"
	case KindCreateIndex:
		return "CREATE INDEX"
	}
	return "SELECT"
}

// IsQuery reports whether this is the SELECT statement kind. A complete
// [Statement] can also return rows when an INSERT carries RETURNING; use
// [Statement.ReturnsRows] when the parsed statement is available.
func (k Kind) IsQuery() bool { return k == KindSelect }

// DocumentColumn is how a statement names the whole stored document.
//
// The store's unit is a document, not a tuple, so the only assignment target an
// UPDATE can have is the document itself. INSERT carries either one complete
// document with VALUES (?) or synthesizes one flat object from a field list.
const DocumentColumn = "$doc"

// A Statement is one parsed statement of any kind.
//
// Exactly one body pointer is non-nil, selected by Kind. It is a tagged struct
// rather than an interface because every consumer switches on the kind anyway,
// and because the six bodies are already concrete types a caller wants by name.
type Statement struct {
	Kind        Kind
	Select      *SelectStmt
	Insert      *InsertStmt
	Update      *UpdateStmt
	Delete      *DeleteStmt
	CreateTable *CreateTableStmt
	CreateIndex *CreateIndexStmt
}

// ReturnsRows reports whether this parsed statement must execute through a
// query path.
func (s *Statement) ReturnsRows() bool {
	return s != nil && (s.Kind == KindSelect ||
		s.Kind == KindInsert && s.Insert != nil && s.Insert.Returning != nil ||
		s.Kind == KindUpdate && s.Update != nil && s.Update.Returning != nil ||
		s.Kind == KindDelete && s.Delete != nil && s.Delete.Returning != nil)
}

// Table answers the collection the statement reads or writes.
func (s *Statement) Table() string {
	switch s.Kind {
	case KindInsert:
		return s.Insert.Table
	case KindUpdate:
		return s.Update.Table
	case KindDelete:
		return s.Delete.Table
	case KindCreateTable:
		return s.CreateTable.Table
	case KindCreateIndex:
		return s.CreateIndex.Table
	}
	if s.Select == nil || len(s.Select.From) == 0 {
		return ""
	}
	return s.Select.From[0].Name
}

// Params answers the number of '?' placeholders in the statement.
func (s *Statement) Params() int {
	switch s.Kind {
	case KindInsert:
		return s.Insert.Params
	case KindUpdate:
		return s.Update.Params
	case KindDelete:
		return s.Delete.Params
	case KindCreateTable, KindCreateIndex:
		// A DDL statement has no placeholders. A schema is not data: a type, a
		// path, and a table name are all compiled into the definition when the
		// statement is prepared, so there is nothing left for a bind to supply.
		return 0
	}
	if s.Select == nil {
		return 0
	}
	return s.Select.Params
}

// An InsertStmt is one parsed INSERT.
//
// # Documents, flat fields, and identity
//
// Every INSERT derives identity from the table's declared scalar JSON PRIMARY
// KEY. One form binds a complete JSON document; the other synthesizes a flat
// object from distinct top-level fields and scalar values:
//
//	INSERT INTO users VALUES (?)
//	INSERT INTO users (id, name) VALUES (?, ?)
//
// Nested construction is deliberately not inferred from a path-shaped column
// list. A caller with a nested value binds the complete document, preserving
// its exact JSON representation and leaving structural editing outside the SQL
// parser.
//
// There is no caller-supplied physical-key form and no generated sequence.
// Keeping identity in the document makes CREATE TABLE's PRIMARY KEY declaration
// the one source of truth for INSERT, point predicates, indexes, and reopen.
type InsertStmt struct {
	// Table is the collection written to.
	Table string
	// Rows are the VALUES tuples in source order. Several rows in one statement
	// are one atomic batch, which is the reason multi-row VALUES exists here at
	// all rather than being sugar for a loop.
	Rows []InsertRow
	// Columns names the flat document fields synthesized by
	// INSERT INTO t (a, b) VALUES (?, ?). It is nil when VALUES carries a
	// whole JSON document.
	Columns []*PathExpr
	// OnConflictDoNothing makes an identity collision a skipped row instead of
	// an error. It is the deliberately narrow, atomic subset of PostgreSQL's
	// ON CONFLICT grammar supported by the storage adapter; conflict targets
	// and DO UPDATE are not implied by this flag.
	OnConflictDoNothing bool
	// Returning is the projection evaluated over the documents this INSERT
	// publishes, in VALUES order. It is nil when the statement returns no rows.
	//
	// This is a real SelectStmt so RETURNING uses the same path resolution,
	// projection semantics, output names, and lowering as SELECT. Its From has
	// exactly one entry: Table.
	Returning *SelectStmt
	// Params is the number of '?' placeholders.
	Params int
	// Pos is the byte offset of the collection name.
	Pos int
}

// An InsertRow is one VALUES tuple.
type InsertRow struct {
	// Values holds either the one whole-document operand of VALUES (?) or the
	// flat field operands corresponding to InsertStmt.Columns.
	Values []Operand
	Pos    int
}

// An UpdateStmt is one parsed UPDATE.
//
// # What SET means here, and why a path is refused
//
// The only assignment this dialect accepts is to the whole document:
//
//	UPDATE users SET "$doc" = ? WHERE tier = 'free'
//
// `SET profile.region = 'eu'` is refused at parse time. The reason is worth
// stating in full, because refusing it is the single largest gap between this
// dialect and SQL, and the alternative was available.
//
// A path assignment is a partial document update, and the engine has no partial
// update: every write primitive it owns — the single-document Put and the
// batch's Put — replaces a document whole. Implementing `SET a.b = v` therefore
// means read-modify-write, and the modify step is a JSON editor: given a
// document, a path, and a value, produce the document with that path set. No
// such primitive exists anywhere in this codebase. Writing one inside the SQL
// front end would put the only implementation of JSON structural editing in the
// layer furthest from the parser and the encoder, where it would have to decide
// on its own — and be the only code deciding — what happens when an
// intermediate object is absent, when a path crosses an array, when the key
// already appears twice in the object, and how a number's exact source spelling
// survives the rewrite. Every one of those has an answer elsewhere in this
// repository; none of them would be shared with this one.
//
// Refusing is not a permanent position. When the core grows a path-set
// primitive, `SET path = value` becomes a lowering that calls it, the read
// happens under the same writer lock the write does, and nothing in this
// grammar has to change except deleting a rejection. Until then a caller reads
// the document with SELECT, edits it where the editing code already lives, and
// writes it back with SET "$doc" = ?; that is three lines instead of one, and
// all three of them are honest.
type UpdateStmt struct {
	// Table is the collection written to.
	Table string
	// Doc is the replacement document, the right-hand side of SET "$doc" = ....
	Doc Operand
	// Filter is the equivalent SELECT whose surviving rows this statement
	// updates: "SELECT count(*) FROM Table WHERE ...", with the same WHERE.
	//
	// Carrying a real SelectStmt rather than a bare *Expr is what makes the
	// promise "UPDATE writes exactly the documents SELECT returns" structural: a
	// lowering pass hands this to the SELECT lowering unchanged.
	Filter *SelectStmt
	// Returning projects the replacement documents after UPDATE, in selected
	// key order. It is nil when the statement reports only RowsAffected.
	Returning *SelectStmt
	// Params is the number of '?' placeholders.
	Params int
	// Pos is the byte offset of the collection name.
	Pos int
	// SetPos is the byte offset of the assignment, for diagnostics.
	SetPos int
}

// A DeleteStmt is one parsed DELETE.
type DeleteStmt struct {
	// Table is the collection written to.
	Table string
	// Filter is the equivalent SELECT whose surviving rows this statement
	// deletes. See [UpdateStmt.Filter].
	Filter *SelectStmt
	// Returning projects the documents removed by DELETE, in selected key
	// order. It is nil when the statement reports only RowsAffected.
	Returning *SelectStmt
	// Params is the number of '?' placeholders.
	Params int
	// Pos is the byte offset of the collection name.
	Pos int
	// All records a DELETE written without a WHERE clause. It is a separate
	// field rather than "Filter with a nil Where" so an executor cannot reach
	// "delete everything" by forgetting to look at one pointer.
	All bool
}
