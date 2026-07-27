package query

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

// The mutation and definition front end: a parsed INSERT, UPDATE, DELETE,
// CREATE TABLE, or CREATE INDEX lowered to the values a storage layer can act
// on.
//
// This package does not write. It has no collection handle, no writer lock, and
// no opinion about durability, and adding one would make the query engine the
// owner of a transaction boundary it cannot see the other half of. What it owns
// is the half that is genuinely its: deciding which documents a statement acts
// on, and doing so with the same compiled predicate a SELECT would use.
//
// # Which documents, and why the answer is the SELECT executor's
//
// A DELETE has to remove exactly the documents the equivalent SELECT returns.
// That is not a nice property, it is the only defensible one: a store where
// "SELECT ... WHERE p" and "DELETE ... WHERE p" disagreed about a null, an
// absent path, or a NOT over either would destroy data on the disagreement.
//
// The obvious implementation — walk the parsed predicate and test each document
// — would be a second evaluator, and a second evaluator is a second set of
// answers to every question the first one already answered: three-valued
// negation, cross-type comparison order, duplicate keys resolving to the last
// occurrence, exact-decimal numeric equality. So there is no second evaluator.
// [DMLStatement.Filter] feeds documents through the compiled plan's own filter
// phase, in batches, on a scratch Segment — the identical arrangement the
// durable backend already uses to filter a batch of raw pages — and reports the
// rows it selects. The predicate that runs is the predicate a SELECT would have
// run, because it is the same compiled predicate reached through the same call.
//
// # What this costs
//
// A filtered DELETE or UPDATE is a full scan. The candidate pruning a SELECT
// gets from persistent indexes happens before the scan, against a snapshot's
// index directory, and is bound to the backend's own execution entry point;
// this path is fed documents by a caller that has already chosen how to
// enumerate them. That is a real performance difference and it is stated rather
// than hidden: a DELETE over an indexed path reads every document, where the
// SELECT with the same WHERE reads the ones the index admits.
//
// The exception is a primary-key condition, which the parser splits out for
// exactly this reason: `DELETE FROM t WHERE "$key" = ?` never scans at all.

// A DMLKind names what a [DMLStatement] does.
type DMLKind uint8

const (
	// DMLInsert writes new documents under caller-supplied keys.
	DMLInsert DMLKind = iota
	// DMLUpdate replaces whole documents.
	DMLUpdate
	// DMLDelete removes documents.
	DMLDelete
	// DDLCreateTable declares a collection, with or without a schema.
	DDLCreateTable
	// DDLCreateIndex declares an index over one or more paths.
	DDLCreateIndex
)

// String answers the statement's leading keywords.
func (k DMLKind) String() string {
	switch k {
	case DMLInsert:
		return "INSERT"
	case DMLUpdate:
		return "UPDATE"
	case DMLDelete:
		return "DELETE"
	case DDLCreateTable:
		return "CREATE TABLE"
	case DDLCreateIndex:
		return "CREATE INDEX"
	}
	return "?"
}

// A DMLStatement is a prepared INSERT, UPDATE, DELETE, or CREATE.
//
// It is single-consumer and not safe for concurrent use, for the same reason
// [Statement] is: it holds the filter statement whose compiler is rewound on
// every bind. A connection pool gives each connection its own.
type DMLStatement struct {
	text string
	kind DMLKind
	tree *sqlast.Statement

	// filter is the SELECT this statement's row selection is, or nil when the
	// statement needs no scan — an INSERT, a primary-key condition, a DELETE
	// with no WHERE, or a DDL statement.
	filter *Statement
	// all marks a DELETE written without a WHERE, which acts on every document.
	// It is distinct from "filter is nil" so that no code path can arrive at
	// "every document" by failing to look at one pointer.
	all    bool
	params int

	// scan is the filter pass this statement's Filter hands back, retained
	// rather than minted per execution. Its batch buffers and its scratch
	// Segment are the whole cost of a filtered scan that matches nothing, and a
	// per-execution Filter made that cost proportional to the number of times
	// the statement ran instead of to the collection. Retaining it is why a
	// prepared DELETE re-executed in a loop allocates a constant amount.
	scan Filter
}

// PrepareDML parses one non-SELECT statement and lowers everything about it
// that does not depend on a binding.
//
// It reports every error it can before anything is written: the parser's, with
// a position; and, for a statement whose row selection is a condition, every
// plan rule the compiler enforces, because the condition is compiled here
// exactly as a SELECT's would be.
//
// A SELECT is refused by name rather than accepted: it has a result schema,
// rows, and a cursor, none of which this type carries, and [PrepareStatement]
// is the entry point that does.
func PrepareDML(src string) (*DMLStatement, error) {
	tree, err := sqlast.ParseStatement(src)
	if err != nil {
		return nil, err
	}
	if tree.Kind == sqlast.KindSelect {
		return nil, fmt.Errorf(
			"query: PrepareDML was given a SELECT, which returns rows; use PrepareStatement")
	}
	d := &DMLStatement{text: src, tree: tree, params: tree.Params()}
	switch tree.Kind {
	case sqlast.KindInsert:
		d.kind = DMLInsert
	case sqlast.KindUpdate:
		d.kind = DMLUpdate
		if err := d.prepareFilter(src, tree.Update.Filter, false); err != nil {
			return nil, err
		}
	case sqlast.KindDelete:
		d.kind = DMLDelete
		if err := d.prepareFilter(src, tree.Delete.Filter, tree.Delete.All); err != nil {
			return nil, err
		}
	case sqlast.KindCreateTable:
		d.kind = DDLCreateTable
	case sqlast.KindCreateIndex:
		d.kind = DDLCreateIndex
	}
	return d, nil
}

// prepareFilter compiles the statement's row selection, unless the statement
// acts on every document or on named keys.
func (d *DMLStatement) prepareFilter(src string, tree *sqlast.SelectStmt, all bool) error {
	d.all = all
	if tree == nil || all {
		return nil
	}
	filter, err := prepareTree(src, tree)
	if err != nil {
		return err
	}
	d.filter = filter
	return nil
}

// Kind reports what the statement does.
func (d *DMLStatement) Kind() DMLKind { return d.kind }

// Collection returns the collection the statement acts on.
func (d *DMLStatement) Collection() string { return d.tree.Table() }

// NumParams returns the number of '?' placeholders.
func (d *DMLStatement) NumParams() int { return d.params }

// SQL returns the statement text as it was prepared.
func (d *DMLStatement) SQL() string { return d.text }

// Tree exposes the parsed statement, which a storage layer reads to find out
// what to write.
//
// Handing the tree back rather than re-describing it in this package's own
// vocabulary is deliberate. Every field a writer needs — the documents an
// INSERT carries, the replacement an UPDATE assigns, the paths a CREATE INDEX
// names — is already an exported, documented field of a type in sql, and a
// parallel set of accessors here would be a second description of one thing,
// with a second place for a field to be forgotten.
func (d *DMLStatement) Tree() *sqlast.Statement { return d.tree }

// ScansEveryDocument reports whether executing this statement means visiting
// every document of the collection: true for a filtered UPDATE or DELETE and
// for one written without a WHERE, false for an INSERT, a primary-key
// condition, and DDL.
//
// A caller uses it to decide whether to open a scan at all, and it is exported
// rather than inferred because the inference — "filter is nil and kind is
// DELETE" — is exactly the kind of thing that is right until a fifth statement
// kind arrives.
func (d *DMLStatement) ScansEveryDocument() bool {
	return d.all || d.filter != nil
}

// Release drops the buffers the statement retains, invalidating its plan.
func (d *DMLStatement) Release() {
	if d == nil {
		return
	}
	d.filter.Release()
	d.scan.Release()
	*d = DMLStatement{}
}

// Argument converts one bound driver argument the way this dialect's literals
// are converted, so a caller binding a key or a document reads them by the same
// rules a WHERE does.
//
// It answers the string a key operand denotes. A nil argument is an error
// rather than a null key: a document's identity cannot be absent, and SQL's
// NULL has no key to be.
func (d *DMLStatement) Key(o sqlast.Operand, args []any) (string, error) {
	switch o.Kind {
	case sqlast.OperandString:
		return o.Text, nil
	case sqlast.OperandParam:
		if o.Ordinal >= len(args) {
			return "", fmt.Errorf("query: placeholder %d was not bound", o.Ordinal+1)
		}
		switch v := args[o.Ordinal].(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		case nil:
			return "", fmt.Errorf("query: a primary key was bound to NULL; a document's identity cannot be absent")
		default:
			return "", fmt.Errorf(
				"query: a primary key was bound to %T; bind a string or a []byte, because keys are opaque bytes here", v)
		}
	}
	return "", fmt.Errorf("query: a primary key must be a string literal or a placeholder")
}

// Document resolves a document operand to the JSON bytes to store.
//
// A bound placeholder is handed through by reference: the storage layer copies
// it when it records the write, so returning the caller's own slice is one copy
// rather than two. A document written literally into the statement is converted
// with a copy, which costs one allocation per literal row per execution.
//
// The copy is deliberate, and the alternative was available: this package
// already holds an unsafe read-only view helper, and the parsed statement owns
// its text for as long as any execution of it. What that argument does not
// cover is what happens after Release — an unsafe view retained by a store that
// did not copy would outlive the arena it points into, and "every write
// primitive copies" is a property of four call sites in two packages rather
// than something the type system holds. One allocation on a shape that is not
// the hot one, in exchange for not depending on that, is the right trade.
func (d *DMLStatement) Document(o sqlast.Operand, args []any) ([]byte, error) {
	switch o.Kind {
	case sqlast.OperandJSON, sqlast.OperandString:
		return []byte(o.Text), nil
	case sqlast.OperandParam:
		if o.Ordinal >= len(args) {
			return nil, fmt.Errorf("query: placeholder %d was not bound", o.Ordinal+1)
		}
		switch v := args[o.Ordinal].(type) {
		case []byte:
			return v, nil
		case string:
			return []byte(v), nil
		case nil:
			return nil, fmt.Errorf(
				"query: a document was bound to NULL; to store the JSON value null, bind the two bytes \"null\"")
		default:
			return nil, fmt.Errorf(
				"query: a document was bound to %T; bind a []byte or a string holding JSON text", v)
		}
	}
	return nil, fmt.Errorf("query: a document must be JSON text, a quoted string, or a placeholder")
}

// --- the filter scan ---------------------------------------------------------

// scanBatch is how many documents one filter pass covers.
//
// It is the same order as the durable backend's own batch and for the same
// reason: the scratch Segment holding the batch, the columns extracted from it,
// and the selection written back all want to stay in cache, and a batch large
// enough to spill them turns a filter pass into a memory-bandwidth problem. A
// batch far smaller would pay the per-pass setup — the Segment reset, the
// column classification — once per handful of documents instead of once per
// cache-sized run.
const scanBatch = 256

// scanBatchBytes bounds a batch by its document bytes as well as its count, so
// a collection of megabyte documents does not build a 256-megabyte scratch
// Segment to filter 256 rows.
const scanBatchBytes = 1 << 20

// A Filter feeds documents through a prepared statement's WHERE and reports the
// ones it selects.
//
// A caller enumerates its own collection — which is the only arrangement that
// works for both an in-memory snapshot and a page-backed one, and the only one
// that lets a caller overlay a transaction's own uncommitted writes on the scan
// — and hands each document to [Filter.Add]. Matching documents are reported to
// the visit function in the order they were added.
//
// Both key and doc are copied on the way in, so a caller may hand over the
// borrowed bytes a page-backed scan yields without cloning them first. They are
// []byte rather than string for exactly that reason: a durable scan hands its
// callback borrowed bytes, and a string parameter would make every document in
// the collection pay one heap allocation to be rejected by the predicate. The
// key and doc a visit receives borrow the Filter's own batch buffers and are
// valid until the next Add; a visitor that keeps either copies it.
//
// A Filter is single-consumer and is owned by the [DMLStatement] that made it:
// its batch buffers and scratch Segment are retained across executions, which
// is what keeps a prepared mutation's scan cost constant rather than
// proportional to how many times it has run, and which means one statement has
// exactly one Filter at a time. It borrows the [Exec] it was made with and
// stays valid until that Exec is reused by anything else.
type Filter struct {
	plan  *plan
	e     *Exec
	docs  store.Segment
	visit func(key, doc []byte) error

	// The pending batch: keys packed end to end in keyBuf with one end-offset
	// per row in keyEnds, and documents likewise in buf and ends. All four are
	// retained across batches, so a warmed Filter reuses them and a scan that
	// rejects every document allocates nothing at all.
	keyBuf  []byte
	keyEnds []int
	buf     []byte
	ends    []int
	err     error
}

// Filter begins a filtered pass over the statement's collection, binding args.
//
// The returned Filter reports every document the statement's WHERE selects. A
// statement that acts on every document — a DELETE with no WHERE — returns a
// Filter that selects everything, so a caller writes one loop rather than two.
// A statement that needs no scan at all is a caller error, because it means the
// caller was about to read a collection for nothing; [DMLStatement.ScansEveryDocument]
// is the question to ask first.
func (d *DMLStatement) Filter(e *Exec, args []any, visit func(key, doc []byte) error) (*Filter, error) {
	if e == nil {
		return nil, fmt.Errorf("query: Filter requires a non-nil Exec")
	}
	if visit == nil {
		return nil, fmt.Errorf("query: Filter requires a visit function")
	}
	if !d.ScansEveryDocument() {
		return nil, fmt.Errorf(
			"query: %s acts on named keys and has no scan to filter", d.kind)
	}
	f := &d.scan
	f.reset()
	f.e, f.visit, f.plan, f.err = e, visit, nil, nil
	if d.filter == nil {
		// A DELETE with no WHERE. There is no predicate to compile and no
		// column to extract, so the batch machinery is bypassed entirely and
		// every document is reported as it arrives.
		return f, nil
	}
	if err := d.filter.bind(args); err != nil {
		return nil, err
	}
	p, err := d.filter.q.compiled()
	if err != nil {
		return nil, err
	}
	if len(p.joins) != 0 {
		// Unreachable through the parser, which gives a DML statement exactly
		// one range variable. It is checked anyway because the cost is one
		// comparison and the alternative is a nil inner binding faulting inside
		// the filter phase.
		return nil, fmt.Errorf("query: a mutation cannot join; its condition reads one collection")
	}
	f.plan = p
	return f, nil
}

// Add offers one document to the filter. It reports the first error the pass
// has seen, whether from the filter itself or from a visit.
func (f *Filter) Add(key, doc []byte) error {
	if f.err != nil {
		return f.err
	}
	if f.plan == nil {
		f.err = f.visit(key, doc)
		return f.err
	}
	f.keyBuf = append(f.keyBuf, key...)
	f.keyEnds = append(f.keyEnds, len(f.keyBuf))
	f.buf = append(f.buf, doc...)
	f.ends = append(f.ends, len(f.buf))
	if len(f.ends) >= scanBatch || len(f.buf) >= scanBatchBytes {
		return f.flush()
	}
	return nil
}

// Done filters whatever is still buffered and reports the pass's outcome.
func (f *Filter) Done() error {
	if f.err != nil {
		return f.err
	}
	if f.plan == nil {
		return nil
	}
	return f.flush()
}

// row answers the key and document at batch position i.
func (f *Filter) row(i int) (key, doc []byte) {
	keyStart, docStart := 0, 0
	if i > 0 {
		keyStart, docStart = f.keyEnds[i-1], f.ends[i-1]
	}
	return f.keyBuf[keyStart:f.keyEnds[i]], f.buf[docStart:f.ends[i]]
}

// Release drops the storage the filter retains, including its scratch Segment.
func (f *Filter) Release() {
	if f == nil {
		return
	}
	f.docs.Reset()
	f.keyBuf, f.keyEnds, f.buf, f.ends = nil, nil, nil, nil
}

// flush filters one batch.
//
// The arrangement — reset a scratch Segment, append the batch's documents,
// extract the filter columns, select — is the durable backend's own
// makeFilePartial with the reduction removed. Reaching the filter phase through
// the plan's methods rather than through a predicate walk of our own is the
// whole point: the rows this reports are the rows a SELECT over the same
// documents would have kept, because the selection is literally the same call.
func (f *Filter) flush() error {
	if len(f.ends) == 0 {
		return nil
	}
	defer f.reset()
	f.docs.Reset()
	start := 0
	for row, end := range f.ends {
		if _, err := f.docs.Append(f.buf[start:end]); err != nil {
			// A stored document that no longer parses is a corrupt collection,
			// not a user error, and naming the key is the only thing that makes
			// it actionable — which is why the batch keeps its keys alongside
			// its bytes rather than deriving them afterwards.
			key, _ := f.row(row)
			f.err = fmt.Errorf("query: document %q did not parse during a filtered scan: %w", key, err)
			return f.err
		}
		start = end
	}
	w := &f.e.Workspace
	w.text = w.text[:0]
	w.lateText = w.lateText[:0]
	w.interner.Reset()
	ctx := &w.ctx
	ctx.s, ctx.rows = &f.docs, f.docs.Len()
	if err := ctx.extract(f.plan, nil, w); err != nil {
		f.err = err
		return err
	}
	selected := f.plan.selectRows(ctx, nil, false, w)
	for _, row := range selected {
		if err := f.visit(f.row(row)); err != nil {
			f.err = err
			return err
		}
	}
	return nil
}

func (f *Filter) reset() {
	f.keyBuf = f.keyBuf[:0]
	f.keyEnds = f.keyEnds[:0]
	f.buf = f.buf[:0]
	f.ends = f.ends[:0]
}

// --- DDL lowering ------------------------------------------------------------

// A TableDefinition is a lowered CREATE TABLE: everything a storage layer needs
// to create the collection, and nothing about how to create it.
type TableDefinition struct {
	// Name is the collection to create.
	Name string
	// Schema is the compiled document schema, or nil for a CREATE TABLE with no
	// column list — a collection that validates nothing beyond JSON syntax.
	Schema *store.Schema
	// PrimaryKey holds the declared key paths, rendered in the engine's path
	// language, or nil.
	//
	// It is reported rather than enforced. See [DMLStatement.LowerTable] for
	// exactly what a declared primary key does today.
	PrimaryKey []string
	// IfNotExists makes an existing collection a no-op rather than an error.
	IfNotExists bool
}

// LowerTable compiles a CREATE TABLE into the definition a storage layer
// creates a collection from.
//
// # What a declared primary key does here
//
// Every path the PRIMARY KEY names becomes a required, non-null field of the
// compiled schema, constrained to the types the column list declared for it, or
// to the scalars if it declared none. That much is enforced on every write, by
// the store's own schema validation, and a document missing a key path is
// rejected with the schema's error.
//
// The other half of the agreed model — that the store key is derived from those
// paths, never supplied separately — is not implemented, here or in the engine.
// It changes the signature of every write primitive and is deliberately
// sequenced after the query work whose oracles depend on the current one. So
// today an INSERT still carries its key explicitly, nothing checks that the key
// agrees with the values at the declared paths, and uniqueness remains the
// store's own uniqueness over the supplied key. PrimaryKey is carried out of
// here so a caller can record and report it; it is not fed to anything that
// enforces it.
func (d *DMLStatement) LowerTable() (TableDefinition, error) {
	if d.kind != DDLCreateTable {
		return TableDefinition{}, fmt.Errorf("query: %s is not a CREATE TABLE", d.kind)
	}
	stmt := d.tree.CreateTable
	def := TableDefinition{Name: stmt.Table, IfNotExists: stmt.IfNotExists}
	if len(stmt.Columns) == 0 && len(stmt.PrimaryKey) == 0 {
		return def, nil
	}
	var spec []byte
	fields := make([]store.SchemaField, 0, len(stmt.Columns))
	for i := range stmt.Columns {
		column := &stmt.Columns[i]
		start := len(spec)
		spec = column.Path.AppendPointer(spec)
		fields = append(fields, store.SchemaField{
			Path:     string(spec[start:]),
			Types:    schemaTypeOf(column.Type),
			Required: column.Required,
		})
	}
	// A key path with no column declaration still gets a field, because the one
	// thing a declared key does enforce is that it is there. Constraining it to
	// the scalars rather than to anything is what makes "a key is derived from a
	// scalar" true of the documents even before the derivation exists.
	for _, key := range stmt.PrimaryKey {
		start := len(spec)
		spec = key.AppendPointer(spec)
		pointer := string(spec[start:])
		def.PrimaryKey = append(def.PrimaryKey, key.Spec())
		found := false
		for i := range fields {
			if fields[i].Path == pointer {
				fields[i].Required = true
				found = true
				break
			}
		}
		if !found {
			fields = append(fields, store.SchemaField{
				Path:     pointer,
				Types:    store.SchemaBool | store.SchemaNumber | store.SchemaString,
				Required: true,
			})
		}
	}
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root:   store.SchemaObject,
		Fields: fields,
	})
	if err != nil {
		return TableDefinition{}, fmt.Errorf("query: CREATE TABLE %s: %w", stmt.Table, err)
	}
	def.Schema = schema
	return def, nil
}

// schemaTypeOf maps this dialect's JSON type set onto the store's.
//
// The mapping is one explicit case per bit rather than a numeric conversion,
// even though the two bit layouts happen to agree today. A conversion would
// make sql's constants and store's constants one thing spelled twice, so a
// renumbering on either side would silently permute every declared schema; a
// switch turns the same event into a compile error.
func schemaTypeOf(t sqlast.JSONType) store.SchemaType {
	var out store.SchemaType
	if t&sqlast.TypeNull != 0 {
		out |= store.SchemaNull
	}
	if t&sqlast.TypeBool != 0 {
		out |= store.SchemaBool
	}
	if t&sqlast.TypeNumber != 0 {
		out |= store.SchemaNumber
	}
	if t&sqlast.TypeInteger != 0 {
		out |= store.SchemaInteger
	}
	if t&sqlast.TypeString != 0 {
		out |= store.SchemaString
	}
	if t&sqlast.TypeArray != 0 {
		out |= store.SchemaArray
	}
	if t&sqlast.TypeObject != 0 {
		out |= store.SchemaObject
	}
	return out
}

// An IndexDefinition is a lowered CREATE INDEX.
type IndexDefinition struct {
	// Definition is the store's own index definition, ready to create.
	Definition store.IndexDefinition
	// IfNotExists makes an existing index a no-op rather than an error.
	IfNotExists bool
	// Table is the collection to index.
	Table string
}

// LowerIndex compiles a CREATE INDEX into the store's index definition.
//
// An unnamed index is named after the paths it indexes, joined by '+', because
// the store's catalog is keyed by name and an index with no name could never be
// reported or dropped. The derived name is deterministic, so the same statement
// run twice names the same index and IF NOT EXISTS means what it says.
func (d *DMLStatement) LowerIndex() (IndexDefinition, error) {
	if d.kind != DDLCreateIndex {
		return IndexDefinition{}, fmt.Errorf("query: %s is not a CREATE INDEX", d.kind)
	}
	stmt := d.tree.CreateIndex
	out := IndexDefinition{IfNotExists: stmt.IfNotExists, Table: stmt.Table}
	// The paths go to the store as RFC 6901 pointers, not as the dotted spec.
	// That is not a preference: store.CompileExactIndex hands each path to
	// vibejson.CompilePointer, which refuses anything without a leading '/',
	// and query's own candidate planner compares an index's declared columns
	// against compiledPath.indexPath, which is the pointer spelling too. A
	// dotted path here would fail to compile; one that somehow did would build
	// an index no query could ever match.
	var buf []byte
	paths := make([]string, 0, len(stmt.Paths))
	name := stmt.Name
	for i, path := range stmt.Paths {
		start := len(buf)
		buf = path.AppendPointer(buf)
		paths = append(paths, string(buf[start:]))
		if !stmt.HasName {
			if i > 0 {
				name += "+"
			}
			name += path.Spec()
		}
	}
	out.Definition = store.IndexDefinition{Name: name, Paths: paths}
	return out, nil
}
