package sql

import "strings"

// The data-definition half of the abstract syntax tree: CREATE TABLE and
// CREATE INDEX.
//
// # The type vocabulary is JSON's
//
// SQL's type system is a tower of widths and precisions — INT, BIGINT,
// SMALLINT, NUMERIC(10,2), VARCHAR(255) — and none of those distinctions exists
// underneath this engine. A stored number is compared by exact decimal value
// (query/scalar.go), deliberately, so that 9007199254740992 and
// 9007199254740993 stay distinct and nothing routes through float64 on its way
// to a comparison. Declaring one column INT and another BIGINT would therefore
// declare a difference the storage, the index, the comparison, and the
// aggregate all refuse to make.
//
// So the vocabulary is the JSON scalar domain the engine has:
//
//	NULL  BOOL  NUMBER  INTEGER  STRING  OBJECT  ARRAY
//
// INTEGER is in the list because the store's own schema has it — it is the
// subset of NUMBER written without a fraction or an exponent — and leaving it
// out would make a real constraint unreachable from SQL.
//
// # Aliases, and the one place an alias would have been a lie
//
// Common SQL spellings whose only relevant promise is a JSON value category
// are accepted as aliases: TEXT, unparameterized VARCHAR, and CLOB are STRING;
// INT, BIGINT, SMALLINT, and TINYINT are INTEGER; FLOAT, REAL, DOUBLE, DECIMAL,
// and NUMERIC are NUMBER; BOOLEAN is BOOL; JSON is ANY. These aliases select a
// JSON kind only; this dialect does not import another database's width,
// precision, or storage representation.
//
// Spellings that promise behavior beyond a JSON kind are refused rather than
// weakened silently. SERIAL would create a sequence-backed default; MONEY has
// fixed-scale currency behavior; JSONB normalizes its input; bare
// CHAR/CHARACTER/NCHAR are fixed-width; RECORD and STRUCT imply a declared
// composite shape. None of those semantics exists here.
//
// A parenthesised precision is refused rather than accepted and ignored.
// VARCHAR(255) means something everywhere it is written, and a dialect that
// took the word and dropped the 255 would be storing a promise it does not
// keep — the first 256-byte string would prove it. Refusing costs the paster
// one edit and tells them exactly what was dropped; accepting costs them a
// constraint they believe they have.
//
// The types with no mapping at all are refused by name: DATE, TIME, TIMESTAMP,
// UUID, BYTEA, BLOB, and the rest. JSON has no date and no byte string, so
// there is nothing for them to become that would not be a second convention
// invented here and enforced nowhere.

// A JSONType is a set of JSON types one column may hold. The values are a bit
// set so a column may accept several — which is how NULL is spelled: a column
// that may be absent or null is its type OR'd with TypeNull.
//
// The bits are declared here rather than reused from the store's SchemaType
// because a parser that depended on another package's constant values would be
// depending on them numerically; the lowering maps them across explicitly, one
// case per bit, so a renumbering on either side is a compile error rather than
// a silently permuted schema.
type JSONType uint16

const (
	// TypeNull is the JSON null value, and — because this engine treats an
	// absent path and an explicit null as one value — also an absent path.
	TypeNull JSONType = 1 << iota
	// TypeBool is true or false.
	TypeBool
	// TypeNumber is any JSON number.
	TypeNumber
	// TypeInteger is a JSON number written without a fraction or an exponent.
	// It is a subset of TypeNumber, not a sibling of it.
	TypeInteger
	// TypeString is a JSON string.
	TypeString
	// TypeArray is a JSON array.
	TypeArray
	// TypeObject is a JSON object.
	TypeObject
)

// TypeAny accepts every JSON value. Integer is omitted because it is already a
// subset of Number, exactly as the store's own SchemaAny omits it.
const TypeAny = TypeNull | TypeBool | TypeNumber | TypeString | TypeArray | TypeObject

// TypeAll is every bit this type defines, including TypeInteger. It exists so a
// consumer can check that a parsed column type holds no bit it does not
// understand — a check worth having because the value crosses a package
// boundary into a mapping that switches on each bit and would otherwise drop an
// unknown one silently.
const TypeAll = TypeAny | TypeInteger

// String answers the canonical spelling of a type set.
func (t JSONType) String() string {
	if t == TypeAny {
		return "ANY"
	}
	var b strings.Builder
	for _, entry := range typeNames {
		if t&entry.bit != 0 {
			if b.Len() != 0 {
				b.WriteString("|")
			}
			b.WriteString(entry.name)
		}
	}
	if b.Len() == 0 {
		return "ANY"
	}
	return b.String()
}

var typeNames = [...]struct {
	bit  JSONType
	name string
}{
	{TypeNull, "NULL"}, {TypeBool, "BOOL"}, {TypeInteger, "INTEGER"},
	{TypeNumber, "NUMBER"}, {TypeString, "STRING"},
	{TypeArray, "ARRAY"}, {TypeObject, "OBJECT"},
}

// A CreateTableStmt is one parsed CREATE TABLE.
//
// A table is a collection. A CREATE TABLE with no column list makes a
// collection with no schema, which is the schemaless default and the shape most
// documents want; a column list makes one whose stored documents are validated
// against it on every write, by the schema the store already has.
//
// # PRIMARY KEY metadata and enforcement
//
// The declaration is parsed and validated in full. Lowering makes every key
// path a required scalar schema field and returns the paths as metadata to the
// storage adapter.
//
// This AST does not prescribe one physical encoding. The database/sql driver
// owns that policy at its catalog boundary: it requires one path, derives a
// canonical typed key from the document's value, and rejects a duplicate
// INSERT. Other consumers may support the compound declaration the parser
// carries, but must derive identity from those declared JSON values and choose
// a stable encoding.
//
// This separation is deliberate: the parser reports the declaration and the
// schema enforces its value domain; the adapter that maps a table onto storage
// owns the physical-key codec.
type CreateTableStmt struct {
	// Table is the collection created.
	Table string
	// Columns are the declared constraints, in source order. Nil for a table
	// declared without a column list, which is a collection with no schema.
	Columns []ColumnDef
	// PrimaryKey holds the paths of the declared primary key, in the declared
	// order, or nil. A column-level PRIMARY KEY and a table-level PRIMARY KEY
	// (a, b) both land here, so a consumer never has to look in two places.
	PrimaryKey []*PathExpr
	// IfNotExists records IF NOT EXISTS, which makes an existing collection a
	// no-op rather than an error.
	IfNotExists bool
	// Pos is the byte offset of the collection name, PrimaryKeyPos of the
	// PRIMARY KEY clause.
	Pos           int
	PrimaryKeyPos int
}

// A ColumnDef is one declared constraint: a path into the document and the JSON
// types it may hold.
//
// It is a path rather than a name because documents nest, and the constraint
// language should be able to say what the query language can say. "profile.tier
// STRING" is a real constraint on a real document shape; refusing it would make
// the schema describe only the top level of documents whose interesting parts
// are never at the top level.
type ColumnDef struct {
	// Path is the constrained path.
	Path *PathExpr
	// Type is the set of JSON types the path may hold.
	Type JSONType
	// Required makes the path's absence a violation. It comes from NOT NULL and
	// from PRIMARY KEY. Note that absence and null are one value in this
	// engine, so Required means "present and not null" and a column declared
	// NOT NULL may not be omitted either — which is the same thing SQL means,
	// arrived at from the other direction.
	Required bool
	// PrimaryKey records that this column carried a column-level PRIMARY KEY.
	PrimaryKey bool
	Pos        int

	// explicitNull distinguishes a nullable column (the SQL default) from one
	// that explicitly wrote NULL. A table-level PRIMARY KEY makes an
	// implicitly nullable column required, but must reject a contradictory
	// explicit NULL constraint.
	explicitNull bool
}

// An AlterTableStmt adds one declared column to an existing table. The bounded
// form is deliberately additive: it can be built and validated against a
// shadow copy, then published atomically without teaching the serving path
// about transitional schemas.
type AlterTableStmt struct {
	Table       string
	Column      ColumnDef
	IfNotExists bool
	Pos         int
}

// A CreateIndexStmt is one parsed CREATE [UNIQUE] INDEX.
//
// The indexed thing is a path, in the same path language everything else uses,
// so an index on "profile.region" indexes what a WHERE on "profile.region"
// reads. Several paths make a compound index, which the engine's exact index
// already supports as an order-sensitive key.
type CreateIndexStmt struct {
	// Unique records UNIQUE. Its enforcement belongs to the catalog/storage
	// adapter that creates the index; carrying it in the AST prevents a unique
	// declaration from being weakened into an ordinary lookup index.
	Unique bool
	// Name is the index's name. It is optional in the grammar; when the
	// statement gives none, a lowering pass derives one from the paths, because
	// the store's index catalog is keyed by name and an unnamed index would
	// have no way to be dropped or reported.
	Name string
	// HasName records whether the name was written, so a diagnostic can echo
	// the statement back accurately.
	HasName bool
	// Table is the collection indexed.
	Table string
	// Paths are the indexed paths in declared order. One path is a column
	// index; several form an order-sensitive compound key.
	Paths []*PathExpr
	// IfNotExists records IF NOT EXISTS.
	IfNotExists bool
	// Pos is the byte offset of the collection name.
	Pos int
}
