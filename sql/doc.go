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
// reductions, ordering, and an inner equi-join. Everything else is refused
// here, with a position and a reason.
//
// That is a stronger rule than it sounds, and it is the reason for most of the
// choices below. A parser that accepted a window function and failed at
// lowering would report the failure from a place that has no statement text
// left: no offset, no line, no quoted token, and an author who has already been
// told their SQL parsed. Refusing at parse time costs a longer keyword table
// and buys every rejection an actionable message.
//
// Two clauses are accepted despite having no executor counterpart today, and
// both are flagged on their fields: [SelectStmt.Having] and
// [SelectStmt.Offset]. See "Accepted ahead of the engine" below.
//
// # Grammar
//
// Keywords are case-insensitive; everything else in the grammar below is
// literal.
//
//	statement    = select | insert | update | delete
//	             | create-table | create-index ;
//
//	select       = "SELECT" [ "ALL" ] select-list
//	               "FROM" table-ref { join }
//	               [ "WHERE" predicate ]
//	               [ "GROUP" "BY" path { "," path } ]
//	               [ "HAVING" predicate ]
//	               [ "ORDER" "BY" sort-key { "," sort-key } ]
//	               [ limit-offset ] [ ";" ] EOF ;
//
//	select-list  = result-column { "," result-column } ;
//	result-column= ( "*" | ident "." "*" | path | aggregate ) [ "AS" name ] ;
//	aggregate    = "COUNT" "(" ( "*" | path ) ")"
//	             | ( "SUM" | "AVG" | "MIN" | "MAX" ) "(" path ")" ;
//
//	table-ref    = name [ [ "AS" ] name ] ;
//	join         = [ "INNER" ] "JOIN" table-ref "ON" join-cond ;
//	join-cond    = [ "(" ] path "=" path [ ")" ] ;
//
//	predicate    = disjunction ;
//	disjunction  = conjunction { "OR" conjunction } ;
//	conjunction  = negation { "AND" negation } ;
//	negation     = "NOT" negation | primary ;
//	primary      = "(" predicate ")" | leaf ;
//	leaf         = left "IS" [ "NOT" ] ( "NULL" | "MISSING" )
//	             | left [ "NOT" ] "IN" "(" operand { "," operand } ")"
//	             | left [ "NOT" ] "BETWEEN" operand "AND" operand
//	             | left "@>" json-document
//	             | left comparison operand ;
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
//	insert-columns = pseudo-columns | path { "," path } ;
//	pseudo-columns = '"$key"' "," '"$doc"' ;
//	row          = "(" document ")"
//	             | "(" value { "," value } ")"
//	             | "(" key "," document ")" ;
//	key          = string | "?" ;
//	document     = "?" | string | json-object | json-array ;
//
//	update       = "UPDATE" name "SET" '"$doc"' "=" document
//	               [ "WHERE" ( predicate | key-condition ) ] [ ";" ] EOF ;
//	delete       = "DELETE" "FROM" name
//	               [ "WHERE" ( predicate | key-condition ) ] [ ";" ] EOF ;
//	key-condition = '"$key"' "=" key | '"$key"' "IN" "(" key { "," key } ")" ;
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
//	path         = name { "." name | "[" integer "]" | "[" string "]" } ;
//	name         = ident | quoted-ident ;
//
// [Parse] accepts the SELECT production alone and refuses the rest by naming
// [ParseStatement], which accepts all six.
//
// A string literal is single-quoted and a quoted identifier is double-quoted;
// in both, an embedded quote is written by doubling it. Numbers follow
// JSON's grammar rather than SQL's looser one, because the literal is bound for
// the engine's exact-decimal literal space, which validates its spelling as
// JSON: "007" and "1." are refused here rather than at lowering. Comments are
// "-- to end of line" and "/* ... */".
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
// arrow operators would have put a second path language into one codebase, so
// that a path written in SQL and the same path written in a query document
// would be different syntax for the same thing, with two implementations to
// keep in agreement. [PathExpr.AppendSpec] renders a parsed path into exactly
// the spelling that existing compiler takes, and renders it deterministically,
// so every clause naming the same path produces byte-identical output — which
// is what lets query's path registry extract that path once no matter how many
// clauses read it.
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
// # Accepted ahead of the engine
//
// HAVING and OFFSET are in the grammar and have no executor counterpart today:
// query's plan has no filter between reduction and ordering, and no row skip.
// They are accepted rather than refused because both are fully resolvable here
// and neither needs anything but a small step added to an existing one. HAVING
// in particular is validated to the point where it cannot fail late: the parser
// binds every HAVING leaf either to an aggregate the SELECT list already
// computes — recording which output column, in [Expr.Column] — or to a GROUP BY
// key, so a HAVING that survives parsing needs a filter-after-reduce and no
// second aggregation pass.
//
// A lowering pass that cannot yet execute them must reject a non-nil Having or
// Offset explicitly. Ignoring either returns wrong rows silently, which is the
// one outcome worse than an error.
//
// # Where this dialect and SQL disagree
//
// These are semantic differences, not gaps, and they are what a caller needs to
// know before this is described as SQL compatibility.
//
// Null is two-valued here, not three. SQL's comparison against NULL yields
// UNKNOWN, which is neither true nor false and which NOT does not flip; the
// engine's evalCmp returns false for a null cell, and NOT of that is true. So
// where x is null, SQL drops a row matching NOT (x = 1) and the engine keeps
// it. The parser removes the one spelling whose reading would depend on which
// semantics the reader had in mind — "x = NULL" and a NULL inside an IN list
// are both refused, pointing at IS NULL — but it cannot remove the difference,
// which lives in NOT over any comparison on a null-or-absent path.
//
// Absent and null are one value. query treats a path that resolves to nothing
// and a path holding an explicit null identically, and IS NULL is true for
// both. SQL has no notion of an absent column at all. The distinction is
// available as "IS [NOT] MISSING", which is this dialect's spelling of the
// engine's existence test; it is spelled that way rather than as EXISTS(path)
// because EXISTS takes a subquery in SQL, and that spelling is reserved for
// refusing subqueries with an accurate message.
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
// SQL's INSERT names columns and supplies a tuple. This store's unit is a whole
// JSON document under a primary key, and there is no schema anywhere in the
// engine that could say which fields a tuple's positions stand for. So an
// INSERT carries exactly what the store takes — a key and a document:
//
//	INSERT INTO users VALUES ('u-1', ?)
//	INSERT INTO users ("$key", "$doc") VALUES ('u-1', {"name": "amy"})
//
// The two forms are one statement; the second names the positions. Any other
// column list is refused, because accepting `INSERT INTO users (name, age)
// VALUES (?, ?)` would mean synthesizing {"name":...,"age":...} and thereby
// making the document's shape a property of the statement text — a different
// shape for each statement, recorded nowhere the next reader could find it.
//
// The key is required and never generated. A store whose keys are opaque
// caller-chosen strings has no sequence to draw from, and inventing a UUID or a
// counter would make INSERT non-deterministic. For the same reason
// driver.Result's LastInsertId returns an error rather than a number.
//
// INSERT onto a key that already exists is refused. Put happens to be an
// upsert, and letting INSERT inherit that would make "this row is new" silently
// mean "this row is new or was something else", which loses data without saying
// so.
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
// # The primary key is not a field, and only two conditions can read it
//
// A predicate compiler resolves a path against stored JSON, and a document's
// key is not in the JSON. `WHERE "$key" = 'u-1'` lowered as an ordinary path
// would compile without complaint and match the documents that happen to
// contain a field literally named "$key" — almost never any of them, and
// silently wrong when it is some of them.
//
// So the two conditions the store can answer without a predicate at all are
// recognized as key lookups and everything else is refused:
//
//	DELETE FROM users WHERE "$key" = ?
//	DELETE FROM users WHERE "$key" IN ('a', 'b')
//
// Those two never scan. Every other appearance of "$key" inside a condition —
// under an OR, under a NOT, conjoined with another test, or with an inequality
// — is refused with a position, because each would need a different execution
// and a dialect that accepted three of the four would be harder to remember
// than one that accepts the two that are free.
//
// A condition that does not mention the key is a full scan. The candidate
// pruning a SELECT gets from persistent indexes belongs to the backend's own
// execution entry point, and a mutation enumerates documents itself, so a
// DELETE over an indexed path reads every document where the SELECT with the
// same WHERE reads only the ones the index admits. That is a performance
// difference, not a semantic one: the surviving documents are identical,
// because the filter is the same compiled predicate reached through the same
// call.
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
// The common SQL spellings are accepted as aliases — TEXT, VARCHAR, CHAR, and
// NVARCHAR are STRING; INT, BIGINT, SMALLINT, TINYINT, and SERIAL are INTEGER;
// FLOAT, REAL, DOUBLE, DECIMAL, and NUMERIC are NUMBER; BOOLEAN is BOOL; JSON
// and JSONB are ANY — because a user pasting a schema from elsewhere should get
// a working table rather than a syntax error on a word with an obvious meaning
// here.
//
// A parenthesised precision is refused rather than accepted and ignored.
// VARCHAR(255) means something everywhere it is written, and a dialect that
// took the word and dropped the 255 would be storing a promise its first
// 256-byte string breaks. Types with no mapping at all — DATE, TIME, TIMESTAMP,
// UUID, BYTEA, BLOB, ENUM, and the rest — are refused by name with the reason,
// because JSON has no date and no byte string, and giving them one would be
// inventing a convention enforced nowhere.
//
// # What PRIMARY KEY does today, and what it will do
//
// The declaration is parsed and validated in full — a key path must be a
// scalar, must not admit NULL, must not be named twice, and at most four paths
// may compose one. It is then lowered to exactly what the engine can enforce
// today, which is less than the declaration promises.
//
// Enforced today: every path PRIMARY KEY names becomes a required,
// scalar-typed field of the collection's declared schema, so a document that
// omits it, or holds a container there, is rejected at write time by the
// store's own validation.
//
// Not enforced today: derivation. The intended model is that a declared primary
// key is one or more paths into the document and the store key is derived from
// them — never passed separately, never stored twice, composites encoded by
// internal/orderedkey. The engine has no such derivation, and adding it changes
// the signature of every write primitive, so it is sequenced after the query
// work whose oracles depend on the current one. Until then INSERT supplies its
// key explicitly, nothing checks that the supplied key agrees with the values at
// the declared paths, and uniqueness is the store's own uniqueness over the
// supplied key rather than over the declared paths.
//
// Nothing in the lowering fakes the missing half.
//
// # What is refused, and why
//
// Each of these is refused with a message naming the missing capability:
// SELECT DISTINCT and COUNT(DISTINCT ...) (no distinct operator); LIKE, ILIKE,
// SIMILAR TO, and regular-expression operators (no pattern operator);
// subqueries in any position, including EXISTS and IN (SELECT ...) (no nested
// execution); outer, cross, and natural joins and comma-separated FROM items
// (the engine matches a key on both sides, so a non-match has no row to emit);
// JOIN ... USING (schemaless documents have no declared columns to match by
// name); set operations, common table expressions, window functions, CASE,
// CAST, arithmetic, string concatenation, and scalar functions (the engine
// evaluates predicates over stored values, not computed expressions); ORDER BY
// and GROUP BY over output positions or aggregates.
//
// The mutation and definition grammar refuses, each by name: a real INSERT
// column list, an INSERT with one value or three, a generated or numeric key,
// INSERT ... SELECT, DEFAULT VALUES, ON CONFLICT / ON DUPLICATE KEY, RETURNING,
// a path assignment in SET, assigning "$key", two assignments in one UPDATE,
// UPDATE ... FROM, DELETE ... USING, LIMIT / ORDER BY / GROUP BY / HAVING on a
// mutation, a table alias on a single-collection statement, "$key" anywhere in
// a condition but the two key shapes above, DROP, ALTER, MERGE, REPLACE,
// TRUNCATE, CREATE VIEW, CREATE UNIQUE INDEX, CREATE TABLE ... AS SELECT, a
// partial index, an index method or key direction, DEFAULT, UNIQUE, CHECK, and
// FOREIGN KEY.
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
// reallocates, the same shape as query's Compiler and for the same reason. The
// steady state is zero allocation: on an Apple M4 Max, a simple SELECT parses
// in about 118 ns, a two-collection join in about 730 ns, and a grouped
// aggregate with HAVING and ORDER BY in about 730 ns, each at 0 allocs/op and
// roughly 180 MB/s of statement text. The package-level [Parse] is the owning
// convenience form and allocates (about 11 allocations for a simple SELECT); a
// caller preparing statements in a loop holds a Parser.
//
// Parsing is on the prepare path, so this matters far less than the executor's
// per-row work — but a driver that prepares per request is an ordinary shape,
// and a front end that allocated per statement would be the only part of the
// pipeline that did.
package sql
