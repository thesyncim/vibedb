package sql

// Parsing CREATE TABLE, CREATE INDEX, and bounded ALTER TABLE.
//
// The grammar:
//
//	create-table = "CREATE" "TABLE" [ "IF" "NOT" "EXISTS" ] name
//	               [ "(" table-item { "," table-item } ")" ] ;
//	table-item   = column-def | "PRIMARY" "KEY" "(" path { "," path } ")" ;
//	column-def   = path type-name { column-constraint } ;
//	column-constraint = "NOT" "NULL" | "NULL" | "PRIMARY" "KEY" ;
//
//	create-index = "CREATE" "INDEX" [ "IF" "NOT" "EXISTS" ] [ name ]
//	               "ON" name "(" path { "," path } ")" ;
//
// Type names are matched case-insensitively against a table of spellings rather
// than through the keyword enum. Type names are not clause keywords: they appear
// in exactly one position, they are numerous, and reserving twenty-five more
// words would take twenty-five plausible JSON field names away from every
// SELECT in the dialect to buy nothing. Folding the identifier text at the one
// place a type is expected costs a compare and reserves nothing.

// parseCreate parses a CREATE statement of either kind. The current token is
// CREATE.
func (p *Parser) parseCreate(dst *Statement) error {
	p.advance() // CREATE
	switch {
	case p.acceptKeyword(kwTable):
		dst.Kind, dst.CreateTable = KindCreateTable, &p.tbl
		return p.parseCreateTable()
	case p.atKeyword(kwIndex):
		p.advance()
		dst.Kind, dst.CreateIndex = KindCreateIndex, &p.idx
		return p.parseCreateIndex()
	case p.atKeyword(kwUnique):
		return p.errHere("CREATE UNIQUE INDEX is not supported: this engine's indexes are lookup structures with no uniqueness constraint, and one that silently did not constrain would be worse than none")
	case p.atKeyword(kwView):
		p.advance()
		dst.Kind, dst.CreateView = KindCreateView, &p.view
		return p.parseCreateView(-1)
	case p.atKeyword(kwMaterialized):
		position := p.tok.pos
		p.advance()
		if err := p.expectKeyword(kwView, "VIEW after MATERIALIZED"); err != nil {
			return err
		}
		dst.Kind, dst.CreateView = KindCreateView, &p.view
		return p.parseCreateView(position)
	case p.atKeyword(kwOr):
		return p.featureNotSupportedHere(
			"CREATE OR REPLACE VIEW is not supported because replacing a durable definition requires dependency-generation invalidation; use DROP VIEW followed by CREATE VIEW",
		)
	}
	return p.errHere("expected TABLE, INDEX, VIEW, or MATERIALIZED VIEW after CREATE")
}

// parseAlterTable accepts the additive PostgreSQL spelling
//
//	ALTER TABLE name ADD [COLUMN] [IF NOT EXISTS] column type [NULL|NOT NULL]
//
// Other ALTER actions are refused precisely. Adding one field is enough to use
// the ordinary replicated shadow-copy pipeline: existing documents are checked
// against the target schema before its catalog generation is activated.
func (p *Parser) parseAlterTable() error {
	p.alter = AlterTableStmt{}
	p.advance() // ALTER
	if !p.acceptKeyword(kwTable) {
		return p.featureNotSupportedHere("ALTER supports TABLE ADD COLUMN in the bounded catalog subset")
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.alter.Table, p.alter.Pos = name, pos
	if !tokenTextEqual(p.tok, "ADD") {
		return p.featureNotSupportedHere("ALTER TABLE supports ADD COLUMN; DROP, RENAME, TYPE, and constraint changes require distinct migration semantics")
	}
	p.advance()
	if tokenTextEqual(p.tok, "COLUMN") {
		p.advance()
	}
	exists, err := p.parseIfNotExists()
	if err != nil {
		return err
	}
	p.alter.IfNotExists = exists
	p.out = &p.sel
	*p.out = SelectStmt{}
	p.beginFilter(name, pos)
	column, err := p.parseColumnDef()
	if err != nil {
		return err
	}
	if column.PrimaryKey {
		return p.errAt(column.Pos, "ALTER TABLE cannot add a PRIMARY KEY: changing document identity requires a separately coordinated table migration")
	}
	p.alter.Column = column
	if err := p.expectEnd(); err != nil {
		return err
	}
	return p.resolvePaths()
}

// --- CREATE TABLE ------------------------------------------------------------

func (p *Parser) parseCreateTable() error {
	p.tbl = CreateTableStmt{}
	p.out = &p.sel
	*p.out = SelectStmt{}
	// The synthetic single range variable is what lets a column path be parsed
	// and resolved by the same code every other path in the dialect goes
	// through: "profile.tier" in a column list must mean the same thing it
	// means in a WHERE, and the only way to guarantee that is for it to be the
	// same resolution.
	exists, err := p.parseIfNotExists()
	if err != nil {
		return err
	}
	p.tbl.IfNotExists = exists
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.tbl.Table, p.tbl.Pos = name, pos
	if p.atKeyword(kwAs) {
		return p.errHere("CREATE TABLE ... AS SELECT is not supported: the engine has nowhere to send a plan's rows, so a table is created empty and filled with INSERT")
	}
	if err := p.rejectAlias(); err != nil {
		return err
	}
	p.beginFilter(name, pos)
	if p.tok.kind == tokLParen {
		if err := p.parseTableItems(); err != nil {
			return err
		}
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	if err := p.resolvePaths(); err != nil {
		return err
	}
	return p.validateTable()
}

func (p *Parser) parseTableItems() error {
	p.advance() // '('
	if p.tok.kind == tokRParen {
		return p.errHere("a column list may not be empty; write CREATE TABLE without one to declare a collection with no schema")
	}
	columns := p.cols[:0]
	keys := p.keyPaths[:0]
	for {
		if p.atKeyword(kwPrimary) {
			pos := p.tok.pos
			if len(keys) != 0 {
				return p.errAt(pos, "PRIMARY KEY is declared twice")
			}
			p.tbl.PrimaryKeyPos = pos
			var err error
			keys, err = p.parsePrimaryKeyList(keys)
			if err != nil {
				return err
			}
		} else if err := p.rejectTableConstraint(); err != nil {
			return err
		} else {
			column, err := p.parseColumnDef()
			if err != nil {
				return err
			}
			columns = append(columns, column)
			if len(columns) > maxClauseItems {
				return p.errfAt(column.Pos, "a table may declare at most %d columns", maxClauseItems)
			}
			if column.PrimaryKey {
				if len(keys) != 0 {
					return p.errAt(column.Pos, "PRIMARY KEY is declared twice")
				}
				p.tbl.PrimaryKeyPos = column.Pos
				keys = append(keys, column.Path)
			}
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	p.cols, p.keyPaths = columns, keys
	p.tbl.Columns = columns
	if len(keys) != 0 {
		p.tbl.PrimaryKey = keys
	}
	return p.expect(tokRParen, "')'")
}

// rejectTableConstraint names the table constraints SQL has and this engine has
// no enforcement for, so each is refused where it was written rather than
// parsed as a column named CHECK.
func (p *Parser) rejectTableConstraint() error {
	switch {
	case p.atKeyword(kwUnique):
		return p.errHere("UNIQUE is not supported: the only uniqueness this store enforces is over a document's primary key")
	}
	if p.tok.kind != tokIdent {
		return nil
	}
	var fold [maxTypeNameLen]byte
	switch string(foldASCII(&fold, p.tok.text)) {
	case "CONSTRAINT", "CHECK", "FOREIGN", "REFERENCES", "EXCLUDE":
		return p.errfHere("%s is not supported: the engine validates a document's shape against declared types and enforces nothing else", p.tok.text)
	}
	return nil
}

func (p *Parser) parsePrimaryKeyList(dst []*PathExpr) ([]*PathExpr, error) {
	p.advance() // PRIMARY
	if err := p.expectKeyword(kwKey, "KEY after PRIMARY"); err != nil {
		return dst, err
	}
	if p.tok.kind != tokLParen {
		return dst, p.errHere("expected '(' after PRIMARY KEY: a table-level primary key names the paths it is derived from")
	}
	p.advance()
	for {
		path, err := p.parsePath(false)
		if err != nil {
			return dst, err
		}
		if len(path.Segments) == 0 {
			return dst, p.errAt(path.Pos, "a primary key names a path inside the document, not the whole document")
		}
		dst = append(dst, path)
		if len(dst) > maxIndexColumns {
			return dst, p.errfAt(path.Pos, "a primary key may name at most %d paths", maxIndexColumns)
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	return dst, p.expect(tokRParen, "')'")
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	column := ColumnDef{Pos: p.tok.pos}
	path, err := p.parsePath(false)
	if err != nil {
		return column, err
	}
	if len(path.Segments) == 0 {
		return column, p.errAt(path.Pos, "a column names a path inside the document; \"$doc\" denotes the whole replacement document")
	}
	column.Path = path
	kind, err := p.parseColumnType()
	if err != nil {
		return column, err
	}
	// SQL columns are nullable unless constrained otherwise. Carry null in the
	// type set itself so the AST and the lowered store schema say the same
	// thing: absence is allowed by Required=false, while an explicit JSON null
	// is allowed by TypeNull.
	column.Type = kind | TypeNull
	var nullability uint8
	for {
		switch {
		case p.atKeyword(kwNot):
			if nullability != 0 {
				return column, p.errHere("NULL and NOT NULL are contradictory column constraints")
			}
			p.advance()
			if err := p.expectKeyword(kwNull, "NULL after NOT"); err != nil {
				return column, err
			}
			nullability = 2
			column.Required = true
			column.Type &^= TypeNull
			if column.Type == 0 {
				return column, p.errAt(column.Pos, "a column whose type is NULL cannot also be NOT NULL")
			}
		case p.atKeyword(kwNull):
			if nullability != 0 {
				return column, p.errHere("NULL and NOT NULL are contradictory column constraints")
			}
			if column.PrimaryKey {
				return column, p.errHere("PRIMARY KEY and NULL are contradictory column constraints")
			}
			p.advance()
			nullability = 1
			column.Required = false
			column.Type |= TypeNull
			column.explicitNull = true
		case p.atKeyword(kwPrimary):
			p.advance()
			if err := p.expectKeyword(kwKey, "KEY after PRIMARY"); err != nil {
				return column, err
			}
			if column.PrimaryKey {
				return column, p.errHere("PRIMARY KEY is declared twice")
			}
			if column.explicitNull {
				return column, p.errHere("PRIMARY KEY and NULL are contradictory column constraints")
			}
			column.PrimaryKey = true
			column.Required = true
			column.Type &^= TypeNull
			if column.Type == 0 {
				return column, p.errAt(column.Pos, "a PRIMARY KEY cannot have the type NULL")
			}
		case p.atKeyword(kwDefault):
			return column, p.errHere("DEFAULT is not supported: a default would have to be written into every document that omitted the field, and this store validates documents rather than completing them")
		case p.atKeyword(kwUnique):
			return column, p.errHere("UNIQUE is not supported: the only uniqueness this store enforces is over a document's primary key")
		case p.atKeyword(kwCollate):
			return column, p.errHere("COLLATE is not supported: strings compare by decoded content")
		default:
			return column, nil
		}
	}
}

// parseColumnType reads one type name and answers the JSON types it admits.
func (p *Parser) parseColumnType() (JSONType, error) {
	if p.tok.kind != tokIdent && p.tok.kind != tokQuotedIdent {
		return 0, p.errHere("expected a column type: one of NULL, BOOL, NUMBER, INTEGER, STRING, ARRAY, OBJECT, or ANY")
	}
	var fold [maxTypeNameLen]byte
	name := foldASCII(&fold, p.tok.text)
	spelling, pos := p.tok.text, p.tok.pos
	kind, ok := jsonTypeOf(name)
	if !ok {
		if reason, refused := refusedTypeReason(name); refused {
			return 0, p.errfAt(pos, "the type %s has no JSON equivalent: %s. The types are NULL, BOOL, NUMBER, INTEGER, STRING, ARRAY, OBJECT, and ANY", p.tok.text, reason)
		}
		return 0, p.errfAt(pos, "unknown type %q: the types are NULL, BOOL, NUMBER, INTEGER, STRING, ARRAY, OBJECT, and ANY", p.tok.text)
	}
	p.advance()
	if p.tok.kind == tokLParen {
		// A precision or a length is refused rather than dropped. VARCHAR(255)
		// means something wherever it was written, and a dialect that took the
		// word and ignored the number would be storing a promise its first
		// 256-byte string breaks. See ddl.go.
		return 0, p.errfAt(p.tok.pos,
			"a length or precision is not supported: %s maps to %s, which constrains the JSON type and not the value's size, "+
				"so the number would be accepted and never enforced. Write %s alone",
			spelling, kind, kind)
	}
	return kind, nil
}

// jsonTypeOf maps a folded type spelling to its JSON type set.
func jsonTypeOf(name []byte) (JSONType, bool) {
	switch string(name) {
	case "NULL":
		return TypeNull, true
	case "BOOL", "BOOLEAN":
		return TypeBool, true
	case "NUMBER", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC":
		return TypeNumber, true
	case "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SMALLINT", "TINYINT":
		return TypeInteger, true
	case "STRING", "TEXT", "VARCHAR", "CLOB":
		return TypeString, true
	case "ARRAY":
		return TypeArray, true
	case "OBJECT":
		return TypeObject, true
	case "ANY", "JSON":
		return TypeAny, true
	}
	return 0, false
}

// refusedTypeReason names the types that exist in other dialects, have no JSON
// counterpart, and are therefore refused rather than mapped. Refusing by name
// with a reason is the difference between "unknown type" — which reads as a
// typo — and an answer.
func refusedTypeReason(name []byte) (string, bool) {
	switch string(name) {
	case "DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL":
		return "JSON has no date or time value, so a column of one would be a string or a number by an unwritten convention this engine could not enforce", true
	case "UUID":
		return "JSON has no UUID value; store it as STRING, which is what its textual form already is", true
	case "BYTEA", "BLOB", "BINARY", "VARBINARY":
		return "JSON has no byte string; store it as STRING with an encoding your application chooses", true
	case "ENUM":
		return "the schema constrains types rather than value sets, so an enumeration would be accepted and never checked", true
	case "SERIAL", "SMALLSERIAL", "BIGSERIAL", "SERIAL2", "SERIAL4", "SERIAL8":
		return "SERIAL creates a sequence-backed generated default, but this engine has neither sequences nor generated values; use INTEGER and supply the value explicitly", true
	case "MONEY":
		return "MONEY has fixed fractional and locale-dependent currency semantics this engine cannot enforce; use NUMBER for an exact JSON decimal", true
	case "CHAR", "CHARACTER", "NCHAR":
		return "a bare fixed-width character type implies length and padding semantics this engine cannot enforce; use STRING or VARCHAR", true
	case "NVARCHAR":
		return "its omitted-length semantics vary by SQL dialect and are not part of this PostgreSQL-oriented subset; use STRING or VARCHAR", true
	case "JSONB":
		return "JSONB normalizes object order, duplicate keys, whitespace, and number spellings, while this engine preserves JSON text; use JSON or ANY", true
	case "RECORD", "STRUCT":
		return "a composite type promises a declared field shape, while OBJECT constrains only the JSON kind; use OBJECT", true
	case "GEOMETRY", "GEOGRAPHY", "POINT", "INET", "CIDR", "MACADDR", "XML":
		return "this engine stores JSON and has no such value", true
	}
	return "", false
}

// validateTable enforces the rules a declared schema has to satisfy before it
// can be lowered, with a position.
func (p *Parser) validateTable() error {
	for i := range p.tbl.Columns {
		column := &p.tbl.Columns[i]
		for j := 0; j < i; j++ {
			if sameSpec(p.tbl.Columns[j].Path, column.Path) {
				return p.errfAt(column.Pos, "column %q is declared twice", column.Path.Spec())
			}
		}
	}
	for i, key := range p.tbl.PrimaryKey {
		for j := 0; j < i; j++ {
			if sameSpec(p.tbl.PrimaryKey[j], key) {
				return p.errfAt(p.tbl.PrimaryKeyPos, "PRIMARY KEY names %q twice", key.Spec())
			}
		}
		// A key path declared as a column must be a scalar, because a key is
		// derived from a value and a container has no derivation. Checking it
		// here, against the declaration, is what keeps the failure at the byte
		// the author wrote rather than at the first document that violated it.
		for j := range p.tbl.Columns {
			column := &p.tbl.Columns[j]
			if !sameSpec(column.Path, key) {
				continue
			}
			if column.explicitNull {
				return p.errfAt(column.Pos,
					"PRIMARY KEY names %q, which has an explicit NULL constraint: a key must be present in every document", key.Spec())
			}
			// PRIMARY KEY implies NOT NULL even when it is table-level. The
			// parser initially includes TypeNull because SQL columns are
			// nullable by default; remove that implicit bit before validating
			// the key's scalar domain.
			column.Required = true
			column.Type &^= TypeNull
			if column.Type == 0 {
				return p.errfAt(column.Pos,
					"PRIMARY KEY names %q, whose NULL type has no non-null key value: a key must be present in every document", key.Spec())
			}
			if column.Type&(TypeArray|TypeObject) != 0 {
				return p.errfAt(column.Pos,
					"PRIMARY KEY names %q, which is declared %s: a key is derived from a scalar value, and a container has no ordering to derive one from",
					key.Spec(), column.Type)
			}
		}
	}
	return nil
}

// --- CREATE INDEX ------------------------------------------------------------

func (p *Parser) parseCreateIndex() error {
	p.idx = CreateIndexStmt{}
	p.out = &p.sel
	*p.out = SelectStmt{}
	exists, err := p.parseIfNotExists()
	if err != nil {
		return err
	}
	p.idx.IfNotExists = exists
	if !p.atKeyword(kwOn) {
		name, err := p.parseAliasName("an index name or ON")
		if err != nil {
			return err
		}
		p.idx.Name, p.idx.HasName = name, true
	}
	if err := p.expectKeyword(kwOn, "ON"); err != nil {
		return err
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.idx.Table, p.idx.Pos = name, pos
	p.beginFilter(name, pos)
	if err := p.rejectAlias(); err != nil {
		return err
	}
	if p.tok.kind != tokLParen {
		return p.errHere("expected '(' after the collection name: an index names the paths it indexes")
	}
	p.advance()
	paths := p.idxPaths[:0]
	for {
		path, err := p.parseIndexPath()
		if err != nil {
			return err
		}
		paths = append(paths, path)
		if len(paths) > maxIndexColumns {
			return p.errfAt(path.Pos, "an index may name at most %d paths", maxIndexColumns)
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	p.idxPaths = paths
	p.idx.Paths = paths
	if err := p.expect(tokRParen, "')'"); err != nil {
		return err
	}
	switch {
	case p.atKeyword(kwWhere):
		return p.errHere("a partial index (INDEX ... WHERE) is not supported: this engine's indexes cover every document of the collection")
	case p.atKeyword(kwUsing):
		return p.errHere("INDEX ... USING is not supported: the engine has one index structure, an exact scalar posting index, and no method to choose between")
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	if err := p.resolvePaths(); err != nil {
		return err
	}
	for i, path := range p.idx.Paths {
		for j := 0; j < i; j++ {
			if sameSpec(p.idx.Paths[j], path) {
				return p.errfAt(path.Pos, "index path %q is named twice", path.Spec())
			}
		}
	}
	return nil
}

// parseIndexPath reads one indexed path, refusing the per-key modifiers SQL
// allows and this engine does not implement.
func (p *Parser) parseIndexPath() (*PathExpr, error) {
	path, err := p.parsePath(false)
	if err != nil {
		return nil, err
	}
	if len(path.Segments) == 0 {
		return nil, p.errAt(path.Pos, "an index names a path inside the document; \"$doc\" denotes the whole replacement document")
	}
	switch {
	case p.atKeyword(kwAsc), p.atKeyword(kwDesc):
		return nil, p.errHere("an index key has no direction: this engine's exact index answers equality and membership, not ordered range scans")
	case p.atKeyword(kwCollate):
		return nil, p.errHere("COLLATE is not supported: strings compare by decoded content")
	case p.atKeyword(kwNulls):
		return nil, p.errHere("NULLS FIRST/LAST has no meaning on an index that answers equality")
	}
	return path, nil
}

// --- shared ------------------------------------------------------------------

// maxIndexColumns bounds a compound index's and a primary key's path count. It
// is the engine's own store.MaxIndexColumns, restated here rather than imported
// so this package keeps depending on nothing outside the standard library, and
// so the rejection carries a position; store.CompileExactIndex enforces the
// same bound without one.
const maxIndexColumns = 4

func (p *Parser) parseIfNotExists() (bool, error) {
	if !p.acceptKeyword(kwIf) {
		return false, nil
	}
	if err := p.expectKeyword(kwNot, "NOT after IF"); err != nil {
		return false, err
	}
	if err := p.expectKeyword(kwExists, "EXISTS after IF NOT"); err != nil {
		return false, err
	}
	return true, nil
}

// foldASCII upper-cases an ASCII identifier into buf for type-name matching,
// and answers the folded bytes.
//
// It is deliberately not strings.ToUpper, and the result is deliberately []byte
// rather than string: both of those allocate one string per token, and every
// caller here feeds the result straight into a switch, where `switch
// string(b)` is recognized by the compiler and copies nothing. A spelling
// longer than any type name is returned unfolded, because no fold could make it
// match one, and a fold that overran the buffer would be a bug rather than a
// slow path.
func foldASCII(buf *[maxTypeNameLen]byte, s string) []byte {
	if len(s) > len(buf) {
		return nil
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	return buf[:len(s)]
}

// maxTypeNameLen bounds foldASCII's stack buffer. It is the longest spelling
// jsonTypeOf, refusedTypeReason, or rejectTableConstraint can match.
const maxTypeNameLen = 12 // TIMESTAMPTZ, VARBINARY, CHARACTER, REFERENCES
