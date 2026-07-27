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

// IsQuery reports whether the statement returns rows. It is the routing
// predicate a database/sql driver needs and the only thing most callers want
// from a [Kind].
func (k Kind) IsQuery() bool { return k == KindSelect }

// KeyColumn is how a statement names a document's primary key.
//
// It is the same spelling a JOIN uses for the inner collection's key, and it is
// spelled with a leading '$' for the same reason: '$' is not an identifier byte
// in this lexer, so the name can only be written quoted, and a quoted name can
// never collide with a JSON object key an author meant to reach. A document
// with a field genuinely called "$key" is still addressable — as a path with a
// pointer spelling — because this constant is recognized only in the three
// positions the grammar gives it, never as a general path.
const KeyColumn = "$key"

// DocumentColumn is how a statement names the whole stored document.
//
// The store's unit is a document, not a tuple, so the only assignment target an
// UPDATE can have and the only value an INSERT can carry is the document
// itself. Giving it a name — rather than leaving it implicit in a positional
// VALUES list — is what lets `INSERT INTO t ("$key", "$doc") VALUES (?, ?)`
// document itself, and what lets the refusal of a real column list say exactly
// which two names do exist.
const DocumentColumn = "$doc"

// A Statement is one parsed statement of any kind.
//
// Exactly one of the four pointers is non-nil, selected by Kind. It is a tagged
// struct rather than an interface because every consumer switches on the kind
// anyway, and because the four bodies are already concrete types a caller wants
// by name.
type Statement struct {
	Kind        Kind
	Select      *SelectStmt
	Insert      *InsertStmt
	Update      *UpdateStmt
	Delete      *DeleteStmt
	CreateTable *CreateTableStmt
	CreateIndex *CreateIndexStmt
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
// # Why a document and not a column list
//
// SQL's INSERT names columns and supplies a tuple. This store has no columns:
// its unit is a whole JSON document addressed by a primary key, and there is no
// schema anywhere in the engine that could say which fields a tuple's positions
// stand for. A column list would therefore have to invent one, and the
// invention would be per statement rather than per collection — two INSERTs
// into one collection could declare different "schemas", and neither would be
// recorded anywhere the next reader could find it.
//
// So the grammar takes exactly what the store takes: a key and a document.
//
//	INSERT INTO users VALUES ('u-1', ?)
//	INSERT INTO users ("$key", "$doc") VALUES ('u-1', ?)
//
// The two forms are the same statement; the second names the two positions so a
// reader who has not read this documentation can still tell which is which. Any
// other column list is refused by name, because the alternative — accepting
// `INSERT INTO users (name, age) VALUES (?, ?)` and synthesizing
// {"name":...,"age":...} — would make the JSON document's shape a property of
// the SQL text, and would give the same statement a different meaning in a
// collection whose documents nest.
//
// The key is required rather than generated. A store whose keys are opaque
// caller-chosen strings has no sequence to draw from, and inventing one (a
// UUID, a counter) would make INSERT non-deterministic and LastInsertId a lie
// in a different way.
type InsertStmt struct {
	// Table is the collection written to.
	Table string
	// Rows are the VALUES tuples in source order. Several rows in one statement
	// are one atomic batch, which is the reason multi-row VALUES exists here at
	// all rather than being sugar for a loop.
	Rows []InsertRow
	// Explicit records that the statement wrote the ("$key", "$doc") column
	// list, purely so a diagnostic can echo the statement back accurately.
	Explicit bool
	// Params is the number of '?' placeholders.
	Params int
	// Pos is the byte offset of the collection name.
	Pos int
}

// An InsertRow is one VALUES tuple: the primary key and the document.
type InsertRow struct {
	// Key is the primary key. It is a string literal or a placeholder; a number
	// or a boolean is refused, because a key is an opaque byte string and
	// accepting 1 as a key would make it ambiguous with '1'.
	Key Operand
	// Doc is the document. It is a placeholder bound to a []byte or a string, a
	// string literal holding JSON text, or the '@>'-style JSON document literal
	// the lexer already delimits structurally. Its syntax is validated by the
	// same parser that will index it, at execution.
	Doc Operand
	Pos int
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
	// Keys is non-nil when the WHERE was a primary-key test; see
	// [DeleteStmt.Keys], which carries the same thing for the same reason.
	Keys []Operand
	// Filter is the equivalent SELECT whose surviving rows this statement
	// updates: "SELECT count(*) FROM Table WHERE ...", with the same WHERE. It
	// is nil when Keys is non-nil, because a key test needs no scan.
	//
	// Carrying a real SelectStmt rather than a bare *Expr is what makes the
	// promise "UPDATE writes exactly the documents SELECT returns" structural: a
	// lowering pass hands this to the SELECT lowering unchanged.
	Filter *SelectStmt
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
	// Keys holds the primary keys named by a WHERE of the form
	// `"$key" = operand` or `"$key" IN (operand, ...)`, and is nil for every
	// other WHERE.
	//
	// The special case exists because the primary key is not a document field:
	// the predicate compiler reads paths out of stored JSON, and a key is not
	// in the JSON. So a key test has no compiled predicate to become, and the
	// two shapes that need none — equality and membership over the key alone —
	// are recognized here and executed as direct lookups. Every other use of
	// "$key" inside a predicate is refused rather than silently read as a
	// document field that almost certainly does not exist.
	Keys []Operand
	// Filter is the equivalent SELECT whose surviving rows this statement
	// deletes, or nil when Keys is non-nil. See [UpdateStmt.Filter].
	Filter *SelectStmt
	// Params is the number of '?' placeholders.
	Params int
	// Pos is the byte offset of the collection name.
	Pos int
	// All records a DELETE written without a WHERE clause. It is a separate
	// field rather than "Filter with a nil Where" so an executor cannot reach
	// "delete everything" by forgetting to look at one pointer.
	All bool
}
