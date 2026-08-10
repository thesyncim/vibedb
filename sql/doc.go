// Package sql parses a deliberately small SQL dialect into an abstract syntax
// tree for front ends that lower to the [query] engine's compiled plan.
//
// This root package is the tokenizer, recursive-descent parser, positioned
// errors, and AST. Execution remains a separate layer to avoid an import cycle:
// package query imports this AST, and package sql/driver imports both packages
// to register the "vibedb" database/sql driver.
//
// # The governing rule
//
// The dialect is bounded by what the engine can execute, not by what SQL can
// express. A construct is accepted only where it maps onto something the
// executor already has — comparison, membership, null and existence tests,
// jsonb containment, boolean combination, projection, grouping, the five
// reductions, ordering, and relational joins. Everything else is refused with
// a position and a reason. Set expressions are the one deliberately staged
// boundary: this package now preserves their complete syntax tree, while query
// lowering must either consume [SelectStmt.Set] or return a typed positioned
// refusal. It may never lower the mirrored first operand as the whole query.
//
// That is a stronger rule than it sounds, and it is the reason for most of the
// choices below. A parser that accepted a window function and failed at
// lowering would report the failure from a place that has no statement text
// left: no offset, no line, no quoted token, and an author who has already been
// told their SQL parsed. Refusing at parse time costs a longer keyword table
// and buys every rejection an actionable message.
//
// HAVING, OFFSET, LIMIT, and placeholders need per-execution state and are
// therefore executed by query.Statement rather than a bare compiled plan.
//
// # Grammar
//
// Keywords are case-insensitive; everything else in the grammar below is
// literal.
//
//	statement    = explain | query-statement | insert | update | delete
//	             | create-table | create-index
//	             | savepoint | release-savepoint | rollback-to-savepoint ;

//	explain      = "EXPLAIN" [ "ANALYZE" ] query-statement ;
//
//	query-statement = query-expression [ query-tail ] [ ";" ] EOF ;
//	query-expression = union-except ;
//	union-except = intersect
//	               { ( "UNION" | "EXCEPT" ) [ "ALL" | "DISTINCT" ] intersect } ;
//	intersect    = set-primary
//	               { "INTERSECT" [ "ALL" | "DISTINCT" ] set-primary } ;
//	set-primary  = select | values | table | "(" query-expression [ query-tail ] ")" ;
//	values       = "VALUES" values-row { "," values-row } ;
//	values-row   = "(" values-scalar { "," values-scalar } ")" ;
//	values-scalar= string | number | "TRUE" | "FALSE" | "NULL" | "?" ;
//	table        = "TABLE" name ;
//	query-tail   = "ORDER" "BY" sort-key { "," sort-key }
//	               [ limit-offset ]
//	             | limit-offset ;
//
//	select       = [ with-clause ] "SELECT" [ "ALL" | "DISTINCT" ] select-list
//	               [ "FROM" table-ref { join } ]
//	               [ "WHERE" predicate ]
//	               [ "GROUP" "BY" path { "," path } ]
//	               [ "HAVING" predicate ] ;
//
//	with-clause  = "WITH" cte { "," cte } ;
//	cte          = name [ "(" name { "," name } ")" ] "AS"
//	               [ "MATERIALIZED" | "NOT" "MATERIALIZED" ]
//	               "(" query-statement ")" ;
//
//	select-list  = result-column { "," result-column } ;
//	result-column= ( "*" | ident "." "*" | path | aggregate ) [ "AS" name ] ;
//	aggregate    = "COUNT" "(" ( "*" | path ) ")"
//	             | ( "SUM" | "AVG" | "MIN" | "MAX" ) "(" path ")" ;
//
// A SELECT without FROM is a one-row source-independent relation. Its outputs
// may contain literals, NULL, placeholders, and the supported scalar
// expressions composed from them; document paths, wildcards, aggregates, and
// windows require a FROM relation and are positioned feature refusals.
//
//	table-ref    = collection-ref | derived-ref ;
//	collection-ref = name [ [ "AS" ] name ] ;
//	derived-ref  = "(" query-statement ")" ( "AS" name | name ) ;
//	join         = ( [ "INNER" ] "JOIN" | "LEFT" [ "OUTER" ] "JOIN"
//	               | "RIGHT" [ "OUTER" ] "JOIN"
//	               | "FULL" [ "OUTER" ] "JOIN" )
//	               [ "LATERAL" ] table-ref
//	               ( "ON" predicate | "USING" "(" name { "," name } ")" )
//	             | "CROSS" "JOIN" [ "LATERAL" ] table-ref ;
//
//	predicate    = disjunction ;
//	disjunction  = conjunction { "OR" conjunction } ;
//	conjunction  = negation { "AND" negation } ;
//	negation     = "NOT" negation | primary ;
//	primary      = "(" predicate ")" | "EXISTS" "(" query-statement ")" | leaf ;
//	leaf         = left "IS" [ "NOT" ] ( "NULL" | "MISSING" )
//	             | left [ "NOT" ] "IN" "(" ( query-statement | operand { "," operand } ) ")"
//	             | left [ "NOT" ] "BETWEEN" operand "AND" operand
//	             | left "@>" json-document
//	             | left comparison ( operand | "(" query-statement ")" ) ;
//	left         = path | aggregate ;          (* aggregate only in HAVING *)
//	comparison   = "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" ;
//	operand      = string | number | "TRUE" | "FALSE" | "?" ;
//
//	sort-key     = ( path | output-alias ) [ "ASC" | "DESC" ] ;
//	limit-offset = "LIMIT" count [ "OFFSET" count ]
//	             | "OFFSET" count [ "LIMIT" count ] ;
//	count        = integer | "?" ;
//
//	insert       = "INSERT" "INTO" name [ "(" insert-columns ")" ]
//	               "VALUES" row { "," row } [ ";" ] EOF ;
//	insert-columns = path { "," path } ;
//	row          = "(" document ")"              (* without columns *)
//	             | "(" operand { "," operand } ")" ; (* with columns *)
//	document     = "?" | string | json-object | json-array ;
//
//	update       = "UPDATE" name "SET" '"$doc"' "=" document
//	               [ "WHERE" predicate ] [ ";" ] EOF ;
//	delete       = "DELETE" "FROM" name
//	               [ "WHERE" predicate ] [ ";" ] EOF ;
//
//	create-table = "CREATE" "TABLE" [ "IF" "NOT" "EXISTS" ] name
//	               [ "(" table-item { "," table-item } ")" ] [ ";" ] EOF ;
//	table-item   = column-def | "PRIMARY" "KEY" "(" path { "," path } ")" ;
//	column-def   = path type { "NOT" "NULL" | "NULL" | "PRIMARY" "KEY" } ;
//	type         = "NULL" | "BOOL" | "NUMBER" | "INTEGER" | "STRING"
//	             | "ARRAY" | "OBJECT" | "ANY" | sql-alias ;
//
//	create-index = "CREATE" "INDEX" [ "IF" "NOT" "EXISTS" ] [ name ]
//	               "ON" name "(" path { "," path } ")" [ ";" ] EOF ;
//
//	savepoint            = "SAVEPOINT" name [ ";" ] EOF ;
//	release-savepoint    = "RELEASE" [ "SAVEPOINT" ] name [ ";" ] EOF ;
//	rollback-to-savepoint = "ROLLBACK" "TO" [ "SAVEPOINT" ] name [ ";" ] EOF ;
//
//	path         = name { "." name | "[" integer "]" | "[" string "]" } ;
//	name         = ident | quoted-ident ;
//
// [Parse] accepts the SELECT production alone and refuses the rest by naming
// [ParseStatement], which accepts every statement production implemented by
// this package.
//
// INTERSECT binds tighter than UNION and EXCEPT; UNION and EXCEPT associate
// left. Omitted set quantifiers mean DISTINCT. Parentheses are retained as
// [SetGroupExpr] nodes rather than flattened because they own local tails.
// Final set ORDER BY names the syntactic first operand's outputs by ordinal;
// operand input paths are not visible at that scope. [SelectStmt.Set] is a cold
// sidecar, nil on every ordinary SELECT. When non-nil, the ordinary fields are
// only a shallow mirror of [SetExpression.First] for output metadata, and every
// consumer must branch on Set before attempting ordinary SELECT lowering.
//
// A string literal is single-quoted and a quoted identifier is double-quoted;
// in both, an embedded quote is written by doubling it. Numbers follow
// JSON's grammar rather than SQL's looser one, because the literal is bound for
// the engine's exact-decimal literal space, which validates its spelling as
// JSON: "007" and "1." are refused here rather than at lowering. Comments are
// "-- to end of line" and "/* ... */".
//
// A derived-ref has a mandatory alias and may occupy any relation position.
// LATERAL is accepted on the right of explicit CROSS, INNER, and LEFT joins.
// Its query may qualify paths with only preceding FROM aliases; local aliases
// and CTEs shadow those outer aliases lexically. [LateralSpec.Bindings] gives a
// lowerer a stable first-reference-ordered slot table, while
// [LateralSpec.References] maps each exact correlated [PathExpr] occurrence to
// its slot without widening ordinary path nodes. A LATERAL query with no
// captures has [LateralSpec.Decorrelated] set, allowing it to use the ordinary
// evaluate-once derived plan. Uncorrelated RIGHT/FULL LATERAL is likewise
// accepted as decorrelated; correlation from their nullable left side is
// rejected with the offending path position.
//
// Predicate subqueries use the same exact capture model through
// [SelectStmt.Correlation]. Their local range variables and CTE-backed ranges
// shadow outer aliases; otherwise a qualified path may bind any lexically
// visible outer FROM source and records its exact depth, source, path, and byte
// position. The sidecar remains nil when no outer path is captured. Parsing is
// deliberately broader than execution: correlated EXISTS, IN, scalar, nested,
// CTE, and set shapes remain losslessly annotated so a semantic layer can prove
// a decorrelation or return a positioned feature refusal. Predicate subqueries
// authored directly in JOIN ON are currently refused as a typed unsupported
// feature after their nested grammar is validated: EXISTS and IN point at their
// operator, while scalar comparisons point at the nested query's opening '('.
//
// WITH is non-recursive and lexically scoped. A CTE body sees earlier sibling
// definitions and enclosing WITH scopes; a nested WITH may shadow either.
// Definitions retain source order, stable query identity, materialization
// policy, output aliases, and placeholder ranges in the AST. A self or forward
// spelling remains a physical-collection candidate because SQL permits a CTE
// body to read a same-named catalog table; [TableRef.UnresolvedCTE] lets a
// catalog-aware binder report the CTE-specific failure only after physical
// lookup fails. WITH RECURSIVE and data-modifying bodies are typed feature
// refusals.
//
// # Nested paths, and the one genuinely new decision
//
// Documents are nested and schemaless; SQL assumes flat columns. Three
// spellings were available for reaching into a document: dotted paths, the
// SQL/JSON arrow operators (u.address->>'city'), and SQL:2016's JSON_VALUE.
// This dialect uses dotted paths with bracket subscripts:
//
//	u.address.city        u.tags[0]        u.meta['weird.key']
//
// The reason is not brevity. query already has exactly one path language — a
// dotted name, or an RFC 6901 JSON Pointer when the string starts with '/' —
// and its compilePath turns both into the same compiled pointer. Introducing
// arrow operators would have put a second path language into one codebase.
// [PathExpr.AppendSpec] renders a parsed path into exactly the spelling the
// shared query compiler takes, and renders it deterministically, so every
// clause naming the same path produces byte-identical output — which is what
// lets query's path registry extract that path once no matter how many clauses
// read it.
//
// The rendering keeps the two forms distinct on purpose. A single clean field
// name stays a bare name, because that is the form compilePath marks as a
// single top-level field and routes through the fused columnar fast path;
// several clean names join with dots; and a subscript, an empty key, or a key
// containing '.', '/', or '~' forces the JSON Pointer form, whose tokens escape
// '~' as '~0' and '/' as '~1'. Array subscripts therefore work exactly as they
// already do in query's path syntax rather than being a new idea.
//
// # The ambiguity rule
//
// Dotted paths have one real cost, and joins are what expose it: in
// "u.address.city", "u" may be a range variable, or it may be a top-level field
// of a document in a single-source query. SQL never has to decide this, because
// SQL knows its tables' columns. A schemaless store does not, so the rule has
// to be syntactic:
//
//   - A leading identifier immediately followed by '.' is a range variable if
//     the statement declares one by that name in FROM or JOIN — either an
//     explicit AS alias or, absent one, the collection name itself. The rest of
//     the chain is then the path into that source's documents.
//   - Inside an explicitly LATERAL derived query, a qualified name not declared
//     locally is then searched through the frozen chain of preceding outer FROM
//     sources, nearest lexical scope first. A later source is never visible.
//   - Otherwise the whole chain, leading identifier included, is a path into
//     the statement's only source. A statement with more than one source has no
//     "only source", so an unqualified path there is rejected rather than
//     guessed at.
//
// Range variables therefore shadow top-level fields of the same name. That is
// the only choice that keeps "u.city" meaning what a join author expects, and
// it costs nothing in reach, because the shadowed field is still addressable by
// the same rule — qualify it. In a source aliased "u", the field "u" is "u.u",
// and its member "city" is "u.u.city". A name with no dot after it is never a
// range variable, so "u" alone and "u[0]" are the field, not the source.
//
// Identifiers are case-sensitive, unlike keywords. They are overwhelmingly JSON
// object keys, and JSON keys are case-sensitive, so folding them would make
// "SELECT Name" and "SELECT name" read one field and leave the other silently
// empty. The rule applies to range variables too, so one spelling never means
// two things in two clauses. Quoting an identifier therefore does not change
// its case; it only lets it hold a reserved word, a space, or punctuation.
//
// # Binding and cursor clauses
//
// The compiled plan owns row selection, reduction, and ordering. A prepared
// query.Statement adds the execution state SQL needs around that plan:
// placeholders are rebound for every call, HAVING filters reduced rows, OFFSET
// advances the cursor, and LIMIT is pushed down only where doing so cannot drop
// a row HAVING would have kept. [query.PrepareStatement] is deliberately the
// one SQL preparation entry point, so the restricted plan-only form cannot
// become a second, subtly smaller SQL API.
//
// # Where this dialect and SQL disagree
//
// These are semantic differences, not gaps, and they are what a caller needs to
// know before this is described as SQL compatibility.
//
// SQL predicates use three-valued logic. The underlying predicate primitives
// are boolean, so lowering carries the TRUE and FALSE forms of each expression
// separately. NOT swaps those forms instead of blindly negating a primitive;
// a null comparison therefore remains UNKNOWN and is dropped by WHERE, as SQL
// requires. "x = NULL" is still refused in favor of IS NULL, and a bound NULL
// inside IN participates in the ordinary UNKNOWN rules.
//
// Absent and null are one value. query treats a path that resolves to nothing
// and a path holding an explicit null identically, and IS NULL is true for
// both. SQL has no notion of an absent column at all. The distinction is
// available as "IS [NOT] MISSING", which is this dialect's spelling of the
// engine's field-existence test; it is spelled that way because EXISTS takes a
// SELECT subquery in SQL and this dialect implements that standard meaning.
//
// Comparison is within type, with a cross-type total order. In SQL, comparing a
// number column to a string is a type error or an implicit cast. Here, values
// compare by exact decimal value within numbers, by decoded content within
// strings, and across types by the fixed order null < bool < number < string <
// container. So "age > '5'" is false for every numeric age rather than an error
// or a coercion.
//
// MIN and MAX are numeric. query extracts their argument as a numeric column
// and skips non-numeric values, so MIN over a string field is null rather than
// the least string. SUM and AVG skip non-numeric values instead of failing, so
// a column of mixed types produces a total over its numbers rather than a type
// error.
//
// Ordering puts nulls first ascending and last descending, which SQL leaves
// implementation-defined and PostgreSQL answers the other way for ASC. NULLS
// FIRST and NULLS LAST are refused rather than silently ignored.
//
// Duplicate object keys resolve to the last occurrence, matching the core's
// Node.Get. SQL has no equivalent because a row cannot have two columns of one
// name.
//
// # A row is a document, and INSERT says so
//
// This store's unit is a whole JSON document. The normal INSERT therefore
// carries a complete document, or builds one flat object from a column list:
//
//	INSERT INTO users VALUES (?)
//	INSERT INTO users (id, name) VALUES (?, ?)
//
// A flat column list names distinct top-level JSON fields and its VALUES must
// be scalars. Nested construction is refused; a caller that already has a
// nested value binds the complete document. Several VALUES rows are one
// statement, not a client-side loop.
//
// Identity always comes from the table's declared scalar JSON PRIMARY KEY.
// There is no caller-supplied physical-key row form and no generated sequence,
// so driver.Result's LastInsertId returns an error.
//
// INSERT onto a key that already exists is refused by default. The explicit
// exception is `ON CONFLICT DO NOTHING`, which skips an existing or repeated
// derived key atomically; `DO UPDATE` remains unsupported. Put happens to be an
// upsert, and letting plain INSERT inherit that would make "this row is new"
// silently mean "this row is new or was something else", which loses data
// without saying so.
//
// # SET assigns the whole document, and a path assignment is refused
//
// The only assignment an UPDATE accepts is to the document itself:
//
//	UPDATE users SET "$doc" = ? WHERE tier = 'free'
//
// `SET profile.region = 'eu'` is refused at parse time, and this is the largest
// deliberate gap between this dialect and SQL, so the reason is worth having in
// full.
//
// A path assignment is a partial document update, and the engine has no partial
// update: every write primitive it owns — the single-document Put and the write
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
// So the caller reads the document with SELECT, edits it where their documents
// are already built, and writes it back with SET "$doc" = ?. That is three
// lines instead of one, and all three of them are honest. When the core grows a
// path-set primitive, `SET path = value` becomes a lowering that calls it and
// nothing in this grammar changes except the deletion of a rejection.
//
// # Primary-key predicates use ordinary declared fields
//
// UPDATE, DELETE, and SELECT all write predicates against JSON fields. The
// database/sql driver recognizes equality and positive IN on the table's
// declared primary-key path and answers them with point reads; the AST still
// carries the ordinary predicate, so optimization does not introduce a second
// query vocabulary or a separate execution meaning.
//
// `"$key"` has no special SQL meaning. It addresses a JSON object field
// literally named "$key", and may itself be declared as the primary key. The
// shared programmatic query builder has a private sentinel with that spelling
// for direct store joins, so SQL renders the quoted JSON field through its RFC
// 6901 pointer spelling to keep the two namespaces disjoint.
//
// A mutation condition without a point shape currently scans. Candidate
// pruning for secondary indexes belongs to SELECT's backend entry point today.
// That is a performance difference, not a semantic one: the surviving
// documents are identical because the filter is the same compiled predicate
// reached through the same call.
//
// # Declared types are JSON's, not SQL's
//
// CREATE TABLE's type vocabulary is the JSON scalar domain the engine actually
// has: NULL, BOOL, NUMBER, INTEGER, STRING, ARRAY, OBJECT, and ANY. There is no
// INT versus BIGINT versus NUMERIC, because there is no such distinction
// underneath — a stored number is compared by exact decimal value, so
// 9007199254740992 and 9007199254740993 stay distinct and nothing routes
// through float64. Declaring one column INT and another BIGINT would declare a
// difference the storage, the index, the comparison, and the aggregate all
// refuse to make. INTEGER survives because the store's own schema has it, as
// the subset of NUMBER written without a fraction or an exponent.
//
// Common SQL spellings whose relevant promise is only a JSON category are
// accepted as aliases — TEXT, unparameterized VARCHAR, and CLOB are STRING;
// INT, BIGINT, SMALLINT, and TINYINT are INTEGER; FLOAT, REAL, DOUBLE, DECIMAL,
// and NUMERIC are NUMBER; BOOLEAN is BOOL; JSON is ANY. These aliases select a
// JSON kind; they do not import another database's width, precision, or storage
// representation.
//
// A parenthesised precision is refused rather than accepted and ignored.
// VARCHAR(255) means something everywhere it is written, and a dialect that
// took the word and dropped the 255 would be storing a promise its first
// 256-byte string breaks. Types with no mapping at all — DATE, TIME, TIMESTAMP,
// UUID, BYTEA, BLOB, ENUM, and the rest — are refused by name with the reason,
// because JSON has no date and no byte string, and giving them one would be
// inventing a convention enforced nowhere.
//
// A spelling is also refused when it promises more than a JSON kind: SERIAL
// means sequence-backed generation, MONEY means fixed-scale currency, JSONB
// means normalized storage, bare CHAR/CHARACTER/NCHAR mean fixed-width padding,
// and RECORD/STRUCT imply a declared composite shape. Accepting any of those as
// a coarse type check would silently discard the behavior its author asked for.
//
// # PRIMARY KEY at the parser and driver boundaries
//
// The declaration is parsed and validated in full — a key path must be a
// scalar, must not admit NULL, must not be named twice, and at most four paths
// may compose one. Lowering compiles those paths into required schema fields
// and also returns the primary-key metadata to the storage adapter.
//
// The database/sql driver requires exactly one declared scalar path, extracts
// that value from every inserted or replacement document, and derives the typed
// storage key from it. Equivalent exact-decimal spellings have one numeric
// identity, and INSERT checks uniqueness over that derived identity. Another
// consumer of this AST may choose a different stable codec, but the declared
// JSON key remains the source of identity.
//
// # What is refused, and why
//
// Each refused construct names its missing capability. COUNT(DISTINCT ...) has
// no distinct reduction variant; SIMILAR TO and regular-expression operators
// have no matcher. LIKE and ILIKE are supported with the default backslash
// escape only.
// implicit correlation from a non-LATERAL derived relation, correlated
// RIGHT/FULL LATERAL, JOIN LATERAL ... USING, and subqueries in the SELECT list;
// NATURAL joins and comma-separated FROM items; data-modifying common table
// expressions; searched CASE predicate families beyond boolean truth,
// comparisons, IS NULL, AND, OR, and NOT; scalar functions; ORDER BY
// and GROUP BY over output positions or aggregates.
//
// The mutation and definition grammar refuses, each by name: a nested INSERT
// column list, generated keys, INSERT ... SELECT, DEFAULT VALUES, ON CONFLICT
// DO UPDATE / ON DUPLICATE KEY, a path assignment in SET, two
// assignments in one UPDATE, UPDATE ... FROM, DELETE ... USING, GROUP BY /
// HAVING on a mutation, unsupported mutation OFFSET or ordering forms, a table
// alias on a single-collection statement, ALTER, MERGE, REPLACE, CREATE VIEW,
// CREATE UNIQUE INDEX, CREATE TABLE ... AS SELECT, a
// partial index, an index method or key
// direction, DEFAULT, UNIQUE, CHECK, and FOREIGN KEY. INSERT also refuses the
// old key/document pair: VALUES without a field list contains exactly one
// complete JSON document whose declared primary-key field determines identity.
// INSERT, UPDATE, and DELETE RETURNING accept the ordinary projection list
// (including * and aliases, but not aggregates) and evaluate it over staged
// documents before publication. DELETE projects pre-delete documents, while
// UPDATE projects replacement documents. INSERT ON CONFLICT DO NOTHING
// projects only rows that were actually inserted.
// TRUNCATE [TABLE] and DROP INDEX [IF EXISTS] are represented explicitly;
// storage adapters decide how to publish their collection-level changes.
//
// Which backend accepts which statement is a property of the engine rather than
// of this grammar, and belongs to the layer that executes: see the sql/driver
// package documentation.
//
// A join condition is deliberately not the general predicate grammar. The
// engine joins on one key equality, so ON accepts exactly "left.key =
// right.key" and refuses a conjunction or an inequality, rather than building a
// tree that reads as valid and has no executor.
//
// The plan rules query enforces when it compiles are restated here so they can
// be reported with a position: a projection under GROUP BY must be a grouping
// key, a plain path cannot be selected alongside an aggregate without GROUP BY,
// and a sort key under GROUP BY must be a grouping key.
//
// # Performance
//
// A [Parser] holds chunked arenas that a warmed parse refills rather than
// reallocates, the same shape as query's prepared-statement compiler and for
// the same reason. Reusing a warmed Parser is the allocation-free hot-loop
// form. The package-level [Parse] is the owning convenience form and may
// allocate; a caller preparing statements in a loop holds a Parser. Measured
// results and their reproduction commands live in docs/performance.md rather
// than in this API contract.
//
// Parsing is on the prepare path, so this matters far less than the executor's
// per-row work — but a driver that prepares per request is an ordinary shape,
// and a front end that allocated per statement would be the only part of the
// pipeline that did.
package sql
