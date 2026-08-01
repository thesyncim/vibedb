package pgwire

import (
	"context"
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The catalog shim: psql's introspection meta-commands answered without a
// catalog SQL engine.
//
// # Why this exists, and where it sits
//
// A psql "\d users" expands into SQL this dialect refuses on purpose —
// pg_class joined to pg_namespace, ::regclass casts, OPERATOR(pg_catalog.~)
// regex matching, COLLATE, correlated subqueries, and scalar functions like
// format_type and pg_table_is_visible. Building a subquery-capable SQL engine
// over a synthetic pg_catalog to run them would be a different project. But
// the *questions* those queries ask — which tables exist, which columns, which
// indexes — are questions the SQL catalog can answer directly. So this file
// recognizes the exact query texts psql emits and answers each whole query
// from the catalog, evaluating none of the SQL inside it.
//
// The placement is the load-bearing decision. Every statement goes down the
// existing classify/parse/execute path completely unchanged: there is no
// prefix probe, no flag, no extra branch before or during a successful parse,
// so the hot path's behavior and allocation profile are byte-identical with
// this file present or deleted (catalog_shim_test.go measures exactly that).
// Only after the SQL front end has already refused a SELECT does session.
// prepare consult recognizeCatalogQuery — one recognition attempt on a cold
// path that has already failed. If recognition also fails, the front end's
// original error is returned unchanged: same SQLSTATE, same message, same
// position, same protocol state.
//
// The shim is post-authentication by construction: it is reached only from
// session.prepare, which only the message-loop dispatch calls, and the loop
// starts after startup() — including SCRAM — has completed. An
// unauthenticated peer cannot reach recognition at all.
//
// # What psql sends, and how it is recognized
//
// psql builds its catalog queries from compile-time templates in its
// describe.c, chosen by the *server* version this package reports (16.0).
// The texts below are the verbatim queries PostgreSQL 18.4 psql sends for \l,
// \dn, \dt, \di, \d, \d <name>, \df, \du, and \dv against a version-16
// server, captured with psql -E. This is evidence for that pinned client, not a
// promise about other psql releases. Two spans
// inside them vary at run time — the relation name psql anchors into
// '^(name)$', and the table oid it copies back into the \d detail queries —
// so each recognized shape is data: literal segments with typed captures
// between them, matched exactly, never approximately. A near miss is not
// second-guessed; it gets the parser's original refusal.
//
// # The synthetic catalog
//
// One SQL table is presented as one pg_class row in the single namespace
// "public", owned by the session user; the session's database is the one
// pg_database row. A table's synthetic oid is a session-local monotonic
// continuation token retained for the life of the connection. psql resolves a
// name to an oid in one query and sends that oid back in later queries;
// retaining rather than recomputing the mapping keeps concurrent CREATE from
// retargeting it and makes collisions impossible. Columns
// become pg_attribute rows whose formatted type is this dialect's own
// declared JSON type vocabulary (string, integer, ...), not an invented
// PostgreSQL type. The primary key and each declared exact index become
// pg_index rows whose definition text says "USING exact", because claiming
// btree would claim ordered-scan semantics these indexes do not have.
// Catalogs this engine genuinely lacks — foreign keys, triggers, policies,
// views, roles, functions — answer with honestly empty result sets rather
// than fabricated rows, which psql renders as "no rows" exactly as it would
// for a bare PostgreSQL cluster.

// catalogMarker separates literal segments from captures in the templates
// below. It never appears in any psql query text.
const catalogMarker = "{{*}}"

// A catalogCapture is the validation a shape's variable spans must pass.
// Validation is what keeps exact-match semantics: a capture that is not a
// plain relation name or a plain oid means the text is not one psql built,
// and the original parse error stands.
type catalogCapture uint8

const (
	captureNone catalogCapture = iota
	// captureRelName is the relation name inside psql's anchored regex
	// '^(name)$'. Only a bare identifier is accepted; psql's pattern
	// metacharacters (* and ?) or any regex special makes the query
	// unrecognized rather than approximately matched.
	captureRelName
	// captureOID is a synthetic table oid quoted back by psql. Every
	// occurrence in one query must be the same digits.
	captureOID
)

// A catalogShape is one recognized introspection query: the psql command it
// serves, the literal segments of its text, the capture discipline for the
// spans between them, and the responder that answers it from the catalog.
type catalogShape struct {
	command  string
	segments []string
	capture  catalogCapture
	respond  func(ctx *catalogAnswer, capture string) *fixedResult
}

// catalogShapes is the complete recognition table. It is a data table rather
// than control flow so the inventory of what the shim answers is readable,
// testable, and swappable in one place; catalog_shim_test.go empties it to
// prove the successful-query path does not touch it.
var catalogShapes = buildCatalogShapes()

func buildCatalogShapes() []catalogShape {
	shape := func(command, template string, capture catalogCapture,
		respond func(*catalogAnswer, string) *fixedResult) catalogShape {
		return catalogShape{
			command:  command,
			segments: strings.Split(template, catalogMarker),
			capture:  capture,
			respond:  respond,
		}
	}
	return []catalogShape{
		shape(`\l`, psqlListDatabases, captureNone, respondListDatabases),
		shape(`\dn`, psqlListSchemas, captureNone, respondListSchemas),
		shape(`\dt`, psqlListTables, captureNone, respondListRelations),
		shape(`\di`, psqlListIndexes, captureNone, respondListIndexes),
		shape(`\d`, psqlListRelations, captureNone, respondListRelations),
		shape(`\dv`, psqlListViews, captureNone, respondListViews),
		shape(`\df`, psqlListFunctions, captureNone, respondListFunctions),
		shape(`\du`, psqlListRoles, captureNone, respondListRoles),
		shape(`\d name: resolve oid`, psqlResolveOid, captureRelName, respondResolveOid),
		shape(`\d name: pg_class row`, psqlTableDetail, captureOID, respondTableDetail),
		shape(`\d name: pg_attribute rows`, psqlTableColumns, captureOID, respondTableColumns),
		shape(`\d name: index list`, psqlTableIndexes, captureOID, respondTableIndexes),
		shape(`\d name: foreign keys`, psqlTableForeignKeys, captureOID, respondTableForeignKeys),
		shape(`\d name: referencing foreign keys`, psqlTableReferencedBy, captureOID, respondTableReferencedBy),
		shape(`\d name: row policies`, psqlTablePolicies, captureOID, respondTablePolicies),
		shape(`\d name: extended statistics`, psqlTableStatistics, captureOID, respondTableStatistics),
		shape(`\d name: publications`, psqlTablePublications, captureOID, respondTablePublications),
		shape(`\d name: inheritance parents`, psqlTableInheritanceParents, captureOID, respondTableInheritanceParents),
		shape(`\d name: partition children`, psqlTablePartitionChildren, captureOID, respondTablePartitionChildren),
	}
}

// recognizeCatalogQuery matches one failed statement against every shape.
// It runs only after a parse failure, so its cost is one walk of a small
// table on a path that has already lost; it allocates nothing on a miss.
func recognizeCatalogQuery(text string) (*catalogShape, string, bool) {
	shape, capture, ok, _ := recognizeCatalogQueryCancelable(text, nil)
	return shape, capture, ok
}

func recognizeCatalogQueryCancelable(
	text string,
	check func() error,
) (*catalogShape, string, bool, error) {
	var err error
	text, err = trimSpaceCancelable(text, check)
	if err != nil {
		return nil, "", false, err
	}
	for i := range catalogShapes {
		capture, ok, err := matchCatalogShapeCancelable(
			text, &catalogShapes[i], check,
		)
		if err != nil {
			return nil, "", false, err
		}
		if ok {
			return &catalogShapes[i], capture, true, nil
		}
	}
	return nil, "", false, nil
}

// matchCatalogShape matches text against one shape's literal segments,
// harvesting and validating the capture between each adjacent pair. Every
// capture in one query must be identical — psql always copies the same oid —
// and the final segment must end the text exactly.
func matchCatalogShape(text string, shape *catalogShape) (string, bool) {
	capture, ok, _ := matchCatalogShapeCancelable(text, shape, nil)
	return capture, ok
}

func matchCatalogShapeCancelable(
	text string,
	shape *catalogShape,
	check func() error,
) (string, bool, error) {
	if check != nil {
		if err := check(); err != nil {
			return "", false, err
		}
	}
	segments := shape.segments
	if !strings.HasPrefix(text, segments[0]) {
		return "", false, nil
	}
	rest := text[len(segments[0]):]
	capture := ""
	for _, segment := range segments[1:] {
		next, err := indexStringCancelable(rest, segment, check)
		if err != nil {
			return "", false, err
		}
		if next < 0 {
			return "", false, nil
		}
		span := rest[:next]
		if !validCatalogCapture(shape.capture, span) {
			return "", false, nil
		}
		if capture == "" {
			capture = span
		} else if capture != span {
			return "", false, nil
		}
		rest = rest[next+len(segment):]
	}
	if rest != "" {
		return "", false, nil
	}
	return capture, true, nil
}

func validCatalogCapture(kind catalogCapture, span string) bool {
	if span == "" || len(span) > 128 {
		return false
	}
	switch kind {
	case captureRelName:
		for i := 0; i < len(span); i++ {
			c := span[i]
			if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
				i > 0 && c >= '0' && c <= '9' {
				continue
			}
			return false
		}
		return true
	case captureOID:
		if len(span) > 10 {
			return false
		}
		for i := 0; i < len(span); i++ {
			if span[i] < '0' || span[i] > '9' {
				return false
			}
		}
		_, err := strconv.ParseUint(span, 10, 32)
		return err == nil
	default:
		return false
	}
}

const firstCatalogTableOID = 16384

type catalogOIDEntry struct {
	oid  uint32
	name string
}

// A catalogAnswer is the read-only context a responder builds rows from: the
// session identity psql displays and one sorted catalog snapshot.
type catalogAnswer struct {
	database string
	user     string
	tables   []sqldriver.TableInfo
	oids     []catalogOIDEntry
}

func (a *catalogAnswer) oidByName(name string) (uint32, bool) {
	for i := range a.oids {
		if a.oids[i].name == name {
			return a.oids[i].oid, true
		}
	}
	return 0, false
}

func (a *catalogAnswer) tableByName(name string) *sqldriver.TableInfo {
	for i := range a.tables {
		if a.tables[i].Name == name {
			return &a.tables[i]
		}
	}
	return nil
}

func (a *catalogAnswer) tableByOID(capture string) *sqldriver.TableInfo {
	oid, err := strconv.ParseUint(capture, 10, 32)
	if err != nil {
		return nil
	}
	name := ""
	for i := range a.oids {
		if a.oids[i].oid == uint32(oid) {
			name = a.oids[i].name
			break
		}
	}
	if name == "" {
		return nil
	}
	for i := range a.tables {
		if a.tables[i].Name == name {
			return &a.tables[i]
		}
	}
	return nil
}

// catalogShim answers one recognized introspection query, or reports that the
// caller must return the parse error it already has. It is called only from
// session.prepare's parse-failure branch, and only while the session is idle:
// a failed Prepare inside an explicit transaction has already moved the
// session to the failed state, and answering rows out of a transaction this
// server just marked failed would disagree with the state it reports.
func (s *session) catalogShim(
	text string,
	check func() error,
) (*fixedResult, bool, error) {
	if s.sql == nil || s.sql.State() != sqldriver.SessionIdle {
		return nil, false, nil
	}
	shape, capture, ok, err := recognizeCatalogQueryCancelable(text, check)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	tables, err := s.sql.Tables(context.Background())
	if err != nil {
		return nil, false, nil
	}
	if check != nil {
		if err := check(); err != nil {
			return nil, false, err
		}
	}
	if !s.ensureCatalogOIDs(tables) {
		return nil, false, nil
	}
	answer := catalogAnswer{
		database: s.database, user: s.user, tables: tables, oids: s.catalogOIDs,
	}
	fixed := shape.respond(&answer, capture)
	if fixed == nil {
		return nil, false, nil
	}
	return fixed, true, nil
}

func (s *session) ensureCatalogOIDs(tables []sqldriver.TableInfo) bool {
	if s.nextCatalogOID == 0 {
		s.nextCatalogOID = firstCatalogTableOID
	}
	for i := range tables {
		found := false
		for j := range s.catalogOIDs {
			if s.catalogOIDs[j].name == tables[i].Name {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if s.nextCatalogOID < firstCatalogTableOID {
			return false
		}
		s.catalogOIDs = append(s.catalogOIDs, catalogOIDEntry{
			oid: s.nextCatalogOID, name: strings.Clone(tables[i].Name),
		})
		s.nextCatalogOID++
	}
	return true
}

// --- responders -------------------------------------------------------------

// catalogResult assembles a fixedResult with PostgreSQL's own SELECT tag.
// psql reads these values as text and never by declared type, but the
// declared OIDs are still kept honest — text for names and rendered
// definitions, int8 for counts and synthetic oids, bool for flags — because
// this package's contract is that a RowDescription never lies about a value.
func catalogResult(cols []column, rows [][]*string) *fixedResult {
	return &fixedResult{
		cols: cols,
		rows: rows,
		tag:  "SELECT " + strconv.Itoa(len(rows)),
	}
}

func textCols(names ...string) []column {
	cols := make([]column, len(names))
	for i, name := range names {
		cols[i] = column{name: name, typ: typeText}
	}
	return cols
}

func respondListDatabases(a *catalogAnswer, _ string) *fixedResult {
	// One database row: the catalog this server was opened over, presented
	// under the name the session connected to. Encoding is genuinely UTF8 —
	// the server refuses every other client_encoding — and the locale and ACL
	// columns are NULL because no locale and no privilege catalog exist here.
	return catalogResult(
		textCols("Name", "Owner", "Encoding", "Locale Provider", "Collate",
			"Ctype", "Locale", "ICU Rules", "Access privileges"),
		[][]*string{{
			strPtr(a.database), strPtr(a.user), strPtr("UTF8"),
			nil, nil, nil, nil, nil, nil,
		}},
	)
}

func respondListSchemas(a *catalogAnswer, _ string) *fixedResult {
	return catalogResult(
		textCols("Name", "Owner"),
		[][]*string{{strPtr("public"), strPtr(a.user)}},
	)
}

// respondListRelations serves both \dt and \d with no pattern: this catalog
// holds tables and nothing else, so the two relation lists are the same list.
func respondListRelations(a *catalogAnswer, _ string) *fixedResult {
	rows := make([][]*string, 0, len(a.tables))
	for i := range a.tables {
		rows = append(rows, []*string{
			strPtr("public"), strPtr(a.tables[i].Name),
			strPtr("table"), strPtr(a.user),
		})
	}
	return catalogResult(textCols("Schema", "Name", "Type", "Owner"), rows)
}

func respondListViews(*catalogAnswer, string) *fixedResult {
	// There are no views; psql prints its own "Did not find any views."
	return catalogResult(textCols("Schema", "Name", "Type", "Owner"), nil)
}

func respondListFunctions(*catalogAnswer, string) *fixedResult {
	// There are no user-defined functions, and pretending the built-in shim
	// expressions are cataloged functions would invent oids for them.
	return catalogResult(
		textCols("Schema", "Name", "Result data type", "Argument data types", "Type"),
		nil,
	)
}

func respondListRoles(*catalogAnswer, string) *fixedResult {
	// There is no role system: authentication authorizes the configured
	// database as one unit. An empty pg_roles is the honest answer; a
	// fabricated superuser row would claim ALTER ROLE could exist.
	return catalogResult(
		[]column{
			{name: "rolname", typ: typeText},
			{name: "rolsuper", typ: typeBool},
			{name: "rolinherit", typ: typeBool},
			{name: "rolcreaterole", typ: typeBool},
			{name: "rolcreatedb", typ: typeBool},
			{name: "rolcanlogin", typ: typeBool},
			{name: "rolconnlimit", typ: typeInt8},
			{name: "rolvaliduntil", typ: typeText},
			{name: "rolreplication", typ: typeBool},
			{name: "rolbypassrls", typ: typeBool},
		},
		nil,
	)
}

func respondListIndexes(a *catalogAnswer, _ string) *fixedResult {
	// Each table contributes its synthetic primary-key index plus its declared
	// exact indexes. psql's query orders by (schema, name); the single schema
	// makes that an order by index name.
	type indexRow struct{ name, table string }
	entries := make([]indexRow, 0, 2*len(a.tables))
	for i := range a.tables {
		t := &a.tables[i]
		entries = append(entries, indexRow{name: t.Name + "_pkey", table: t.Name})
		for _, index := range t.Indexes {
			entries = append(entries, indexRow{name: index.Name, table: t.Name})
		}
	}
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].name < entries[j-1].name; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	rows := make([][]*string, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, []*string{
			strPtr("public"), strPtr(entry.name), strPtr("index"),
			strPtr(a.user), strPtr(entry.table),
		})
	}
	return catalogResult(textCols("Schema", "Name", "Type", "Owner", "Table"), rows)
}

func respondResolveOid(a *catalogAnswer, name string) *fixedResult {
	cols := []column{
		{name: "oid", typ: typeInt8},
		{name: "nspname", typ: typeText},
		{name: "relname", typ: typeText},
	}
	t := a.tableByName(name)
	if t == nil {
		// Zero rows is the contract for a missing relation: psql sees the
		// empty resolution and prints its own "Did not find any relation
		// named ..." without a server error.
		return catalogResult(cols, nil)
	}
	tableOID, ok := a.oidByName(t.Name)
	if !ok {
		return catalogResult(cols, nil)
	}
	oid := strconv.FormatUint(uint64(tableOID), 10)
	return catalogResult(cols, [][]*string{{strPtr(oid), strPtr("public"), strPtr(t.Name)}})
}

func respondTableDetail(a *catalogAnswer, capture string) *fixedResult {
	cols := []column{
		{name: "relchecks", typ: typeInt8},
		{name: "relkind", typ: typeText},
		{name: "relhasindex", typ: typeBool},
		{name: "relhasrules", typ: typeBool},
		{name: "relhastriggers", typ: typeBool},
		{name: "relrowsecurity", typ: typeBool},
		{name: "relforcerowsecurity", typ: typeBool},
		{name: "relhasoids", typ: typeBool},
		{name: "relispartition", typ: typeBool},
		{name: "reloptions", typ: typeText},
		{name: "reltablespace", typ: typeInt8},
		{name: "reloftype", typ: typeText},
		{name: "relpersistence", typ: typeText},
		{name: "relreplident", typ: typeText},
		{name: "amname", typ: typeText},
	}
	t := a.tableByOID(capture)
	if t == nil {
		return catalogResult(cols, nil)
	}
	// relhasindex is true because every table has its primary-key index.
	// amname is NULL rather than "heap": psql omits the access-method line,
	// and no PostgreSQL access method actually stores these rows.
	return catalogResult(cols, [][]*string{{
		strPtr("0"), strPtr("r"), strPtr("t"), strPtr("f"), strPtr("f"),
		strPtr("f"), strPtr("f"), strPtr("f"), strPtr("f"), strPtr(""),
		strPtr("0"), strPtr(""), strPtr("p"), strPtr("d"), nil,
	}})
}

func respondTableColumns(a *catalogAnswer, capture string) *fixedResult {
	cols := []column{
		{name: "attname", typ: typeText},
		{name: "format_type", typ: typeText},
		{name: "default", typ: typeText},
		{name: "attnotnull", typ: typeBool},
		{name: "attcollation", typ: typeText},
		{name: "attidentity", typ: typeText},
		{name: "attgenerated", typ: typeText},
	}
	t := a.tableByOID(capture)
	if t == nil {
		return catalogResult(cols, nil)
	}
	rows := make([][]*string, 0, len(t.Columns))
	for _, col := range t.Columns {
		notNull := "f"
		// NOT NULL in this dialect is "must be present and may not be null",
		// which is exactly the pair the catalog stores separately.
		if col.Required && col.Types&sqlast.TypeNull == 0 {
			notNull = "t"
		}
		rows = append(rows, []*string{
			strPtr(pointerDisplayName(col.Path)),
			strPtr(catalogColumnTypeName(col.Types)),
			nil,             // no column defaults exist
			strPtr(notNull), //
			nil,             // no collations exist
			strPtr(""),      // attidentity: never an identity column
			strPtr(""),      // attgenerated: never generated
		})
	}
	return catalogResult(cols, rows)
}

func respondTableIndexes(a *catalogAnswer, capture string) *fixedResult {
	cols := []column{
		{name: "relname", typ: typeText},
		{name: "indisprimary", typ: typeBool},
		{name: "indisunique", typ: typeBool},
		{name: "indisclustered", typ: typeBool},
		{name: "indisvalid", typ: typeBool},
		{name: "pg_get_indexdef", typ: typeText},
		{name: "pg_get_constraintdef", typ: typeText},
		{name: "contype", typ: typeText},
		{name: "condeferrable", typ: typeBool},
		{name: "condeferred", typ: typeBool},
		{name: "indisreplident", typ: typeBool},
		{name: "reltablespace", typ: typeInt8},
		{name: "conperiod", typ: typeBool},
	}
	t := a.tableByOID(capture)
	if t == nil {
		return catalogResult(cols, nil)
	}
	// psql orders primary first, then by index name; it prints the text after
	// " USING " in the definition, so "USING exact (id)" renders as
	// "exact (id)" — the honest spelling of what these indexes are.
	primaryColumn := pointerDisplayName(t.PrimaryKey)
	rows := [][]*string{{
		strPtr(t.Name + "_pkey"), strPtr("t"), strPtr("t"), strPtr("f"), strPtr("t"),
		strPtr("CREATE UNIQUE INDEX " + t.Name + "_pkey ON public." + t.Name +
			" USING exact (" + primaryColumn + ")"),
		strPtr("PRIMARY KEY (" + primaryColumn + ")"),
		strPtr("p"), strPtr("f"), strPtr("f"), strPtr("f"), strPtr("0"), strPtr("f"),
	}}
	sorted := make([]sqldriver.IndexInfo, len(t.Indexes))
	copy(sorted, t.Indexes)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Name < sorted[j-1].Name; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	for _, index := range sorted {
		display := make([]string, len(index.Paths))
		for i, path := range index.Paths {
			display[i] = pointerDisplayName(path)
		}
		rows = append(rows, []*string{
			strPtr(index.Name), strPtr("f"), strPtr("f"), strPtr("f"), strPtr("t"),
			strPtr("CREATE INDEX " + index.Name + " ON public." + t.Name +
				" USING exact (" + strings.Join(display, ", ") + ")"),
			nil, nil, nil, nil, strPtr("f"), strPtr("0"), nil,
		})
	}
	return catalogResult(cols, rows)
}

// The remaining \d follow-up queries ask about catalogs that have no
// counterpart here at all. Each answers with its column shape and zero rows —
// the same result a bare PostgreSQL table produces — so psql prints nothing
// rather than failing the whole \d.

func respondTableForeignKeys(*catalogAnswer, string) *fixedResult {
	return catalogResult([]column{
		{name: "sametable", typ: typeBool},
		{name: "conname", typ: typeText},
		{name: "condef", typ: typeText},
		{name: "ontable", typ: typeText},
	}, nil)
}

func respondTableReferencedBy(*catalogAnswer, string) *fixedResult {
	return catalogResult(textCols("conname", "ontable", "condef"), nil)
}

func respondTablePolicies(*catalogAnswer, string) *fixedResult {
	return catalogResult([]column{
		{name: "polname", typ: typeText},
		{name: "polpermissive", typ: typeBool},
		{name: "roles", typ: typeText},
		{name: "qual", typ: typeText},
		{name: "withcheck", typ: typeText},
		{name: "cmd", typ: typeText},
	}, nil)
}

func respondTableStatistics(*catalogAnswer, string) *fixedResult {
	return catalogResult([]column{
		{name: "oid", typ: typeInt8},
		{name: "stxrelid", typ: typeText},
		{name: "nsp", typ: typeText},
		{name: "stxname", typ: typeText},
		{name: "columns", typ: typeText},
		{name: "ndist_enabled", typ: typeBool},
		{name: "deps_enabled", typ: typeBool},
		{name: "mcv_enabled", typ: typeBool},
		{name: "stxstattarget", typ: typeInt8},
	}, nil)
}

func respondTablePublications(*catalogAnswer, string) *fixedResult {
	return catalogResult(textCols("pubname", "rowfilter", "collist"), nil)
}

func respondTableInheritanceParents(*catalogAnswer, string) *fixedResult {
	return catalogResult(textCols("inhparent"), nil)
}

func respondTablePartitionChildren(*catalogAnswer, string) *fixedResult {
	return catalogResult([]column{
		{name: "oid", typ: typeText},
		{name: "relkind", typ: typeText},
		{name: "inhdetachpending", typ: typeBool},
		{name: "partbound", typ: typeText},
	}, nil)
}

// pointerDisplayName renders a catalog RFC 6901 pointer the way its SQL
// declaration spelled it: "/id" is the column id. A nested pointer joins its
// unescaped segments with dots, which is this dialect's own path spelling.
func pointerDisplayName(pointer string) string {
	trimmed := strings.TrimPrefix(pointer, "/")
	if !strings.ContainsAny(trimmed, "/~") {
		return trimmed
	}
	segments := strings.Split(trimmed, "/")
	for i, segment := range segments {
		segment = strings.ReplaceAll(segment, "~1", "/")
		segments[i] = strings.ReplaceAll(segment, "~0", "~")
	}
	return strings.Join(segments, ".")
}

// catalogColumnTypeName renders a declared column type set for psql's Type
// column in the dialect's own vocabulary, lowercased the way PostgreSQL
// spells type names. Nullability is not part of the spelling — psql has a
// separate Nullable column — so TypeNull is stripped before rendering.
func catalogColumnTypeName(t sqlast.JSONType) string {
	display := t &^ sqlast.TypeNull
	if display == 0 {
		return "null"
	}
	if display == sqlast.TypeAny&^sqlast.TypeNull {
		return "any"
	}
	return strings.ToLower(display.String())
}

// --- the verbatim psql 18.x query texts -------------------------------------
//
// Captured with psql -E from the digest-pinned postgres:18.4-alpine client
// against a server reporting version 16 (what this package reports), over a
// catalog with two tables, one secondary index, and a primary key on each.
// {{*}} marks the spans psql varies: the relation name in \d <name>'s
// anchored pattern, and the table oid it copies into the detail queries.

const psqlListDatabases = `SELECT
  d.datname as "Name",
  pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
  pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
  CASE d.datlocprovider WHEN 'b' THEN 'builtin' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' END AS "Locale Provider",
  d.datcollate as "Collate",
  d.datctype as "Ctype",
  d.daticulocale as "Locale",
  d.daticurules as "ICU Rules",
  CASE WHEN pg_catalog.array_length(d.datacl, 1) = 0 THEN '(none)' ELSE pg_catalog.array_to_string(d.datacl, E'\n') END AS "Access privileges"
FROM pg_catalog.pg_database d
ORDER BY 1`

const psqlListSchemas = `SELECT n.nspname AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1`

const psqlListTables = `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2`

const psqlListIndexes = `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
  c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','I','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2`

const psqlListRelations = `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2`

const psqlListViews = `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('v','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2`

const psqlListFunctions = `SELECT n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
 CASE p.prokind
  WHEN 'a' THEN 'agg'
  WHEN 'w' THEN 'window'
  WHEN 'p' THEN 'proc'
  ELSE 'func'
 END as "Type"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE pg_catalog.pg_function_is_visible(p.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
ORDER BY 1, 2, 4`

const psqlListRoles = `SELECT r.rolname, r.rolsuper, r.rolinherit,
  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
  r.rolconnlimit, r.rolvaliduntil
, r.rolreplication
, r.rolbypassrls
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1`

const psqlResolveOid = `SELECT c.oid,
  n.nspname,
  c.relname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname OPERATOR(pg_catalog.~) '^({{*}})$' COLLATE pg_catalog.default
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 2, 3`

const psqlTableDetail = `SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
FROM pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
WHERE c.oid = '{{*}}'`

const psqlTableColumns = `SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
  a.attnotnull,
  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
  a.attidentity,
  a.attgenerated
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = '{{*}}' AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`

const psqlTableIndexes = `SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace, false AS conperiod
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = '{{*}}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, c2.relname`

const psqlTableForeignKeys = `SELECT true as sametable, conname,
  pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
  conrelid::pg_catalog.regclass AS ontable
FROM pg_catalog.pg_constraint r
WHERE r.conrelid = '{{*}}' AND r.contype = 'f'
     AND conparentid = 0
ORDER BY conname`

const psqlTableReferencedBy = `SELECT conname, conrelid::pg_catalog.regclass AS ontable,
       pg_catalog.pg_get_constraintdef(oid, true) AS condef
  FROM pg_catalog.pg_constraint c
 WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{{*}}')
                     UNION ALL VALUES ('{{*}}'::pg_catalog.regclass))
       AND contype = 'f' AND conparentid = 0
ORDER BY conname`

const psqlTablePolicies = `SELECT pol.polname, pol.polpermissive,
  CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
  CASE pol.polcmd
    WHEN 'r' THEN 'SELECT'
    WHEN 'a' THEN 'INSERT'
    WHEN 'w' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    END AS cmd
FROM pg_catalog.pg_policy pol
WHERE pol.polrelid = '{{*}}' ORDER BY 1`

const psqlTableStatistics = `SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
  'd' = any(stxkind) AS ndist_enabled,
  'f' = any(stxkind) AS deps_enabled,
  'm' = any(stxkind) AS mcv_enabled,
stxstattarget
FROM pg_catalog.pg_statistic_ext
WHERE stxrelid = '{{*}}'
ORDER BY nsp, stxname`

const psqlTablePublications = `SELECT pubname
     , NULL
     , NULL
FROM pg_catalog.pg_publication p
     JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
     JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
WHERE pc.oid ='{{*}}' and pg_catalog.pg_relation_is_publishable('{{*}}')
UNION
SELECT pubname
     , pg_get_expr(pr.prqual, c.oid)
     , (CASE WHEN pr.prattrs IS NOT NULL THEN
         (SELECT string_agg(attname, ', ')
           FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                pg_catalog.pg_attribute
          WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
        ELSE NULL END) FROM pg_catalog.pg_publication p
     JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
     JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
WHERE pr.prrelid = '{{*}}'
UNION
SELECT pubname
     , NULL
     , NULL
FROM pg_catalog.pg_publication p
WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('{{*}}')
ORDER BY 1`

const psqlTableInheritanceParents = `SELECT c.oid::pg_catalog.regclass
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhparent AND i.inhrelid = '{{*}}'
  AND c.relkind != 'p' AND c.relkind != 'I'
ORDER BY inhseqno`

const psqlTablePartitionChildren = `SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhrelid AND i.inhparent = '{{*}}'
ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text`
