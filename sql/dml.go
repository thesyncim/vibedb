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
	// KindAlterTable is an ALTER TABLE, carried in [Statement.AlterTable].
	KindAlterTable
	// KindDropTable is a DROP TABLE, carried in [Statement.DropTable].
	KindDropTable
	// KindTruncate is a TRUNCATE, carried in [Statement.Truncate].
	KindTruncate
	// KindDropIndex is a DROP INDEX, carried in [Statement.DropIndex].
	KindDropIndex
	// KindCreateView is a CREATE VIEW, carried in [Statement.CreateView].
	KindCreateView
	// KindDropView is a DROP VIEW, carried in [Statement.DropView].
	KindDropView
	// KindSavepoint is SAVEPOINT name, carried in [Statement.Savepoint].
	KindSavepoint
	// KindReleaseSavepoint is RELEASE [SAVEPOINT] name, carried in
	// [Statement.ReleaseSavepoint].
	KindReleaseSavepoint
	// KindRollbackToSavepoint is ROLLBACK TO [SAVEPOINT] name, carried in
	// [Statement.RollbackToSavepoint].
	KindRollbackToSavepoint
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
	case KindAlterTable:
		return "ALTER TABLE"
	case KindDropTable:
		return "DROP TABLE"
	case KindTruncate:
		return "TRUNCATE"
	case KindDropIndex:
		return "DROP INDEX"
	case KindCreateView:
		return "CREATE VIEW"
	case KindDropView:
		return "DROP VIEW"
	case KindSavepoint:
		return "SAVEPOINT"
	case KindReleaseSavepoint:
		return "RELEASE"
	case KindRollbackToSavepoint:
		return "ROLLBACK TO"
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
// and because the bodies are already concrete types a caller wants by name.
type Statement struct {
	Kind Kind
	// Explain marks a SELECT whose caller requested plan output instead of the
	// target rows. Analyze additionally asks the driver to execute that target
	// and attach measured runtime work to the plan.
	Explain             bool
	Analyze             bool
	Select              *SelectStmt
	Insert              *InsertStmt
	Update              *UpdateStmt
	Delete              *DeleteStmt
	CreateTable         *CreateTableStmt
	CreateIndex         *CreateIndexStmt
	AlterTable          *AlterTableStmt
	DropTable           *DropTableStmt
	Truncate            *TruncateStmt
	DropIndex           *DropIndexStmt
	CreateView          *CreateViewStmt
	DropView            *DropViewStmt
	Savepoint           *SavepointStmt
	ReleaseSavepoint    *SavepointStmt
	RollbackToSavepoint *SavepointStmt
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
	case KindAlterTable:
		return s.AlterTable.Table
	case KindDropTable:
		return s.DropTable.Table
	case KindTruncate:
		return s.Truncate.Table
	case KindDropIndex:
		return s.DropIndex.Table
	case KindCreateView:
		return s.CreateView.Name
	case KindDropView:
		return s.DropView.Name
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
	case KindCreateTable, KindCreateIndex, KindAlterTable, KindDropTable, KindTruncate, KindDropIndex,
		KindCreateView, KindDropView,
		KindSavepoint, KindReleaseSavepoint, KindRollbackToSavepoint:
		// A DDL statement has no placeholders. A schema is not data: a type, a
		// path, and a table name are all compiled into the definition when the
		// statement is prepared, so there is nothing left for a bind to supply.
		// Savepoint control statements likewise bind no parameters: the mark
		// name is an identifier resolved when the statement is prepared.
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
	// Source is the query whose single output column supplies complete JSON
	// documents. It is nil for VALUES. The source tree is deliberately distinct
	// from Returning: the former is evaluated against the pre-statement
	// snapshot, while the latter is evaluated only over rows admitted for
	// publication.
	Source *SelectStmt
	// SourcePos is the byte offset of Source's leading SELECT or query-expression
	// token. Runtime source-shape errors use it when no narrower output position
	// exists.
	SourcePos int
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
	// ON CONFLICT grammar supported by the storage adapter. Conflict targets
	// remain implicit because the document-derived primary key is the only
	// unique key in this SQL surface.
	OnConflictDoNothing bool
	// OnConflictUpdate carries the alternative conflict action. It is nil when
	// there is no DO UPDATE clause. The parser guarantees that this and
	// OnConflictDoNothing are mutually exclusive.
	OnConflictUpdate *InsertConflictUpdate
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

// InsertConflictUpdate is the bounded assignment program of INSERT ... ON
// CONFLICT DO UPDATE. It either replaces the complete conflicting document
// from EXCLUDED."$doc", or replaces distinct declared top-level columns. The
// two forms never mix.
type InsertConflictUpdate struct {
	// Doc is OperandExcluded naming DocumentColumn for the whole-document form.
	// It is otherwise the zero operand and Assignments is non-empty.
	Doc Operand
	// Assignments are scalar literals/placeholders/NULL or OperandExcluded
	// references to declared top-level candidate columns.
	Assignments []UpdateAssignment
	// Pos is the byte offset of UPDATE and SetPos is the first assignment.
	Pos    int
	SetPos int
}

// WholeDocument reports whether the conflict action replaces the complete
// document with EXCLUDED."$doc".
func (u *InsertConflictUpdate) WholeDocument() bool {
	return u != nil && len(u.Assignments) == 0 &&
		u.Doc.Kind == OperandExcluded && u.Doc.Text == DocumentColumn
}

// HasConflictAction reports whether INSERT has either supported ON CONFLICT
// action.
func (s *InsertStmt) HasConflictAction() bool {
	return s != nil && (s.OnConflictDoNothing || s.OnConflictUpdate != nil)
}

// An InsertRow is one VALUES tuple.
type InsertRow struct {
	// Values holds either the one whole-document operand of VALUES (?) or the
	// flat field operands corresponding to InsertStmt.Columns.
	Values []Operand
	Pos    int
}

// An UpdateStmt is one parsed UPDATE. A statement either replaces the whole
// document through Doc, or applies one or more declared top-level column
// assignments. The two forms never mix.
type UpdateStmt struct {
	// Table is the collection written to.
	Table string
	// Doc is the replacement document, the right-hand side of SET "$doc" = ....
	Doc Operand
	// Assignments are declared top-level column replacements. Nested JSON paths
	// remain deliberately outside the SQL table surface: JSON documents retain
	// the explicit whole-document update form.
	Assignments []UpdateAssignment
	// Filter is the equivalent SELECT whose surviving rows this statement
	// updates: "SELECT count(*) FROM Table WHERE ...", with the same WHERE.
	//
	// Carrying a real SelectStmt rather than a bare *Expr is what makes the
	// promise "UPDATE writes exactly the documents SELECT returns" structural: a
	// lowering pass hands this to the SELECT lowering unchanged.
	Filter *SelectStmt
	// OrderBy is the mutation's optional bounded-selection ordering. The
	// driver deliberately keeps it outside Filter: query.Filter evaluates a
	// filtered scan in batches, while a mutation must choose one global set of
	// keys before it publishes a batch.
	OrderBy []OrderTerm
	// Limit caps the number of selected documents. It is nil when UPDATE acts
	// on every matching document.
	Limit *Operand
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

// UpdateAssignment replaces one declared top-level column with one scalar.
type UpdateAssignment struct {
	Column string
	Value  Operand
	Pos    int
}

// A DeleteStmt is one parsed DELETE.
type DeleteStmt struct {
	// Table is the collection written to.
	Table string
	// Filter is the equivalent SELECT whose surviving rows this statement
	// deletes. See [UpdateStmt.Filter].
	Filter *SelectStmt
	// OrderBy and Limit have the same bounded-selection contract as
	// [UpdateStmt.OrderBy] and [UpdateStmt.Limit].
	OrderBy []OrderTerm
	Limit   *Operand
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
