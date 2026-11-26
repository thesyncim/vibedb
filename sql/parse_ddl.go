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
//	create-index = "CREATE" [ "UNIQUE" ] "INDEX"
//	               [ "IF" "NOT" "EXISTS" ] [ name ]
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
		return p.parseCreateIndex(false)
	case p.atKeyword(kwUnique):
		p.advance()
		if err := p.expectKeyword(kwIndex, "INDEX after UNIQUE"); err != nil {
			return err
		}
		dst.Kind, dst.CreateIndex = KindCreateIndex, &p.idx
		return p.parseCreateIndex(true)
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
	case p.tok.kind == tokIdent && isUnsupportedCreateObject(p.tok.text):
		return p.featureNotSupportedHere(
			"CREATE " + asciiUpper(p.tok.text) + " is not supported",
		)
	}
	return p.errHere("expected TABLE, INDEX, VIEW, or MATERIALIZED VIEW after CREATE")
}

func isUnsupportedCreateObject(name string) bool {
	switch asciiUpper(name) {
	case "SCHEMA", "SEQUENCE", "TYPE", "DOMAIN", "FUNCTION", "PROCEDURE",
		"TRIGGER", "EXTENSION", "ROLE", "USER", "POLICY", "DATABASE",
		"PUBLICATION", "SUBSCRIPTION", "TABLESPACE", "COLLATION",
		"AGGREGATE", "OPERATOR", "RULE", "SERVER", "FOREIGN", "TEMP", "TEMPORARY", "UNLOGGED":
		return true
	}
	return false
}

func asciiUpper(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
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
		return p.featureNotSupportedHere("ALTER supports TABLE in the bounded catalog subset")
	}
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
		p.alter.IfExists = true
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.alter.Table, p.alter.Pos = name, pos
	p.out = &p.sel
	*p.out = SelectStmt{}
	p.beginFilter(name, pos)

	switch {
	case tokenTextEqual(p.tok, "ADD"):
		p.advance()
		if tokenTextEqual(p.tok, "CONSTRAINT") {
			p.advance()
			if _, err := p.parseAliasName("a constraint name"); err != nil {
				return err
			}
			return p.parseAlterAddConstraint()
		}
		if p.atKeyword(kwUnique) || tokenTextEqual(p.tok, "CHECK") || tokenTextEqual(p.tok, "FOREIGN") {
			return p.parseAlterAddConstraint()
		}
		if tokenTextEqual(p.tok, "COLUMN") {
			p.advance()
		}
		exists, err := p.parseIfNotExists()
		if err != nil {
			return err
		}
		p.alter.Action = AlterAddColumn
		p.alter.IfNotExists = exists
		column, err := p.parseColumnDef()
		if err != nil {
			return err
		}
		if column.PrimaryKey {
			return p.errAt(column.Pos, "ALTER TABLE cannot add a PRIMARY KEY: changing document identity requires a separately coordinated table migration")
		}
		p.alter.Column = column

	case p.atKeyword(kwDrop):
		p.advance()
		if tokenTextEqual(p.tok, "CONSTRAINT") {
			p.advance()
			if p.acceptKeyword(kwIf) {
				if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
					return err
				}
				p.alter.IfExists = true
			}
			cname, err := p.parseAliasName("a constraint name")
			if err != nil {
				return err
			}
			p.alter.Action = AlterDropConstraint
			p.alter.NewName = cname
			_ = p.acceptDropBehavior()
			return p.expectEnd()
		}
		if tokenTextEqual(p.tok, "COLUMN") {
			p.advance()
		}
		if p.acceptKeyword(kwIf) {
			if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
				return err
			}
			p.alter.IfExists = true
		}
		colPos := p.tok.pos
		path, err := p.parsePath(false)
		if err != nil {
			return err
		}
		if p.acceptDropBehavior() == dropCascade {
			return p.featureNotSupportedHere("ALTER TABLE DROP COLUMN CASCADE is not supported")
		}
		p.alter.Action = AlterDropColumn
		p.alter.Column = ColumnDef{Path: path, Pos: colPos}

	case tokenTextEqual(p.tok, "ALTER"):
		p.advance()
		if tokenTextEqual(p.tok, "COLUMN") {
			p.advance()
		}
		colPos := p.tok.pos
		path, err := p.parsePath(false)
		if err != nil {
			return err
		}
		p.alter.Column = ColumnDef{Path: path, Pos: colPos}
		switch {
		case p.atKeyword(kwSet):
			p.advance()
			if p.atKeyword(kwNot) {
				p.advance()
				if err := p.expectKeyword(kwNull, "NULL after SET NOT"); err != nil {
					return err
				}
				p.alter.Action = AlterSetNotNull
			} else if p.atKeyword(kwDefault) {
				p.advance()
				defExpr, err := p.parseScalarExpression(scalarSelect)
				if err != nil {
					return err
				}
				p.alter.Action = AlterSetDefault
				p.alter.Column.HasDefault = true
				p.alter.Column.DefaultExpr = defExpr
			} else if tokenTextEqual(p.tok, "DATA") {
				p.advance()
				if !tokenTextEqual(p.tok, "TYPE") {
					return p.errHere("expected TYPE after SET DATA")
				}
				p.advance()
				kind, err := p.parseColumnType()
				if err != nil {
					return err
				}
				p.alter.Action = AlterSetType
				p.alter.Column.Type = kind
			} else {
				return p.errHere("expected NOT NULL, DEFAULT, or DATA TYPE after SET")
			}
		case p.atKeyword(kwDrop):
			p.advance()
			if p.atKeyword(kwNot) {
				p.advance()
				if err := p.expectKeyword(kwNull, "NULL after DROP NOT"); err != nil {
					return err
				}
				p.alter.Action = AlterDropNotNull
			} else if p.atKeyword(kwDefault) {
				p.advance()
				p.alter.Action = AlterDropDefault
			} else {
				return p.errHere("expected NOT NULL or DEFAULT after DROP")
			}
		case tokenTextEqual(p.tok, "TYPE"):
			p.advance()
			kind, err := p.parseColumnType()
			if err != nil {
				return err
			}
			p.alter.Action = AlterSetType
			p.alter.Column.Type = kind
		default:
			return p.errHere("expected SET, DROP, or TYPE after ALTER COLUMN")
		}

	case tokenTextEqual(p.tok, "RENAME"):
		p.advance()
		if tokenTextEqual(p.tok, "TO") {
			p.advance()
			newTable, _, err := p.parseCollectionName()
			if err != nil {
				return err
			}
			p.alter.Action = AlterRenameTable
			p.alter.NewName = newTable
			return p.expectEnd()
		}
		if tokenTextEqual(p.tok, "COLUMN") {
			p.advance()
		}
		colPos := p.tok.pos
		path, err := p.parsePath(false)
		if err != nil {
			return err
		}
		if !tokenTextEqual(p.tok, "TO") {
			return p.errHere("expected TO in RENAME COLUMN")
		}
		p.advance()
		newCol, err := p.parseAliasName("a new column name")
		if err != nil {
			return err
		}
		p.alter.Action = AlterRenameColumn
		p.alter.Column = ColumnDef{Path: path, Pos: colPos}
		p.alter.NewName = newCol

	default:
		return p.featureNotSupportedHere("ALTER TABLE supports ADD, DROP, ALTER COLUMN, and RENAME")
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	return p.resolvePaths()
}

func (p *Parser) parseAlterAddConstraint() error {
	p.alter.Action = AlterAddConstraint
	switch {
	case p.atKeyword(kwUnique):
		if _, err := p.parseColumnPathList("UNIQUE"); err != nil {
			return err
		}
	case tokenTextEqual(p.tok, "CHECK"):
		if _, err := p.parseCheckConstraint(); err != nil {
			return err
		}
	case tokenTextEqual(p.tok, "FOREIGN"):
		if err := p.parseForeignKeyConstraint(); err != nil {
			return err
		}
	default:
		return p.errHere("expected UNIQUE, CHECK, or FOREIGN KEY in ADD CONSTRAINT")
	}
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
		if tokenTextEqual(p.tok, "CONSTRAINT") {
			p.advance()
			if _, err := p.parseAliasName("a constraint name"); err != nil {
				return err
			}
		}
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
		} else if p.atKeyword(kwUnique) {
			uPaths, err := p.parseColumnPathList("UNIQUE")
			if err != nil {
				return err
			}
			p.tbl.UniqueConstraints = append(p.tbl.UniqueConstraints, uPaths)
		} else if tokenTextEqual(p.tok, "CHECK") {
			chk, err := p.parseCheckConstraint()
			if err != nil {
				return err
			}
			p.tbl.CheckConstraints = append(p.tbl.CheckConstraints, chk)
		} else if tokenTextEqual(p.tok, "FOREIGN") {
			if err := p.parseForeignKeyConstraint(); err != nil {
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

// rejectTableConstraint rejects EXCLUDE constraints which have no counterpart.
func (p *Parser) rejectTableConstraint() error {
	if p.tok.kind != tokIdent {
		return nil
	}
	var fold [maxTypeNameLen]byte
	switch string(foldASCII(&fold, p.tok.text)) {
	case "EXCLUDE":
		return p.errfHere("%s is not supported: the engine validates a document's shape against declared types and enforces nothing else", p.tok.text)
	}
	return nil
}

func (p *Parser) parseColumnPathList(clause string) ([]*PathExpr, error) {
	p.advance() // keyword
	if err := p.expect(tokLParen, "'(' after "+clause); err != nil {
		return nil, err
	}
	var paths []*PathExpr
	for {
		path, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
		if len(paths) > maxIndexColumns {
			return nil, p.errfAt(path.Pos, "%s may name at most %d paths", clause, maxIndexColumns)
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	return paths, p.expect(tokRParen, "')'")
}

func (p *Parser) parseCheckConstraint() (*Expr, error) {
	p.advance() // CHECK
	if err := p.expect(tokLParen, "'(' after CHECK"); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr(ctxWhere)
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokRParen, "')' after CHECK expression"); err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) parseForeignKeyConstraint() error {
	p.advance() // FOREIGN
	if err := p.expectKeyword(kwKey, "KEY after FOREIGN"); err != nil {
		return err
	}
	if err := p.expect(tokLParen, "'(' after FOREIGN KEY"); err != nil {
		return err
	}
	for {
		if _, err := p.parsePath(false); err != nil {
			return err
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return err
	}
	if !tokenTextEqual(p.tok, "REFERENCES") {
		return p.errHere("expected REFERENCES after FOREIGN KEY (...)")
	}
	_, err := p.parseReferencesClause()
	return err
}

func (p *Parser) parseReferencesClause() (string, error) {
	p.advance() // REFERENCES
	refTable, _, err := p.parseCollectionName()
	if err != nil {
		return "", err
	}
	if p.tok.kind == tokLParen {
		p.advance()
		for {
			if _, err := p.parsePath(false); err != nil {
				return "", err
			}
			if p.tok.kind != tokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(tokRParen, "')'"); err != nil {
			return "", err
		}
	}
	for p.atKeyword(kwOn) {
		p.advance() // ON
		if p.atKeyword(kwDelete) || p.atKeyword(kwUpdate) {
			p.advance()
		} else {
			return "", p.errHere("expected DELETE or UPDATE after ON in REFERENCES")
		}
		switch {
		case tokenTextEqual(p.tok, "CASCADE"), tokenTextEqual(p.tok, "RESTRICT"):
			p.advance()
		case tokenTextEqual(p.tok, "NO"):
			p.advance()
			if !tokenTextEqual(p.tok, "ACTION") {
				return "", p.errHere("expected ACTION after NO")
			}
			p.advance()
		case p.atKeyword(kwSet):
			p.advance()
			if !p.atKeyword(kwNull) && !p.atKeyword(kwDefault) {
				return "", p.errHere("expected NULL or DEFAULT after SET")
			}
			p.advance()
		default:
			return "", p.errHere("expected referential action (CASCADE, RESTRICT, NO ACTION, SET NULL, SET DEFAULT)")
		}
	}
	return refTable, nil
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
			p.advance()
			defExpr, err := p.parseScalarExpression(scalarSelect)
			if err != nil {
				return column, err
			}
			column.HasDefault = true
			column.DefaultExpr = defExpr
		case p.atKeyword(kwUnique):
			p.advance()
			column.Unique = true
		case tokenTextEqual(p.tok, "CHECK"):
			chk, err := p.parseCheckConstraint()
			if err != nil {
				return column, err
			}
			column.CheckExpr = chk
		case tokenTextEqual(p.tok, "REFERENCES"):
			ref, err := p.parseReferencesClause()
			if err != nil {
				return column, err
			}
			column.References = ref
		case tokenTextEqual(p.tok, "CONSTRAINT"):
			p.advance()
			if _, err := p.parseAliasName("a constraint name"); err != nil {
				return column, err
			}
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
	pos := p.tok.pos
	kind, ok := jsonTypeOf(name)
	if !ok {
		if reason, refused := refusedTypeReason(name); refused {
			return 0, p.errfAt(pos, "the type %s has no JSON equivalent: %s. The types are NULL, BOOL, NUMBER, INTEGER, STRING, ARRAY, OBJECT, and ANY", p.tok.text, reason)
		}
		return 0, p.errfAt(pos, "unknown type %q: the types are NULL, BOOL, NUMBER, INTEGER, STRING, ARRAY, OBJECT, and ANY", p.tok.text)
	}
	upperName := string(name)
	p.advance()
	// Handle standard PostgreSQL multi-word type spellings:
	// DOUBLE PRECISION, CHARACTER VARYING, BIT VARYING,
	// TIMESTAMP/TIME [WITH|WITHOUT TIME ZONE].
	switch upperName {
	case "DOUBLE":
		if tokenTextEqual(p.tok, "PRECISION") {
			p.advance()
		}
	case "CHARACTER":
		if tokenTextEqual(p.tok, "VARYING") {
			p.advance()
		}
	}
	// Accept standard PostgreSQL type modifiers: VARCHAR(255), NUMERIC(10, 2), TIMESTAMP(6).
	if p.tok.kind == tokLParen {
		p.advance()
		if p.tok.kind != tokNumber {
			return 0, p.errHere("expected positive integer type modifier")
		}
		p.advance()
		if p.tok.kind == tokComma {
			p.advance()
			if p.tok.kind != tokNumber {
				return 0, p.errHere("expected scale integer in type modifier")
			}
			p.advance()
		}
		if err := p.expect(tokRParen, "')' after type modifier"); err != nil {
			return 0, err
		}
	}
	if upperName == "TIMESTAMP" || upperName == "TIME" {
		if tokenTextEqual(p.tok, "WITH") || tokenTextEqual(p.tok, "WITHOUT") {
			p.advance()
			if !tokenTextEqual(p.tok, "TIME") {
				return 0, p.errHere("expected TIME ZONE")
			}
			p.advance()
			if !tokenTextEqual(p.tok, "ZONE") {
				return 0, p.errHere("expected ZONE after TIME")
			}
			p.advance()
		}
	}
	// Accept PostgreSQL array type suffix: TEXT[], INTEGER[], UUID[], etc.
	if p.tok.kind == tokLBracket {
		p.advance()
		if err := p.expect(tokRBracket, "']' for array type"); err != nil {
			return 0, err
		}
		kind = TypeArray
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
	case "NUMBER", "FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE", "DECIMAL", "NUMERIC", "MONEY":
		return TypeNumber, true
	case "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SMALLINT", "TINYINT",
		"SERIAL", "SMALLSERIAL", "BIGSERIAL", "OID":
		return TypeInteger, true
	case "STRING", "TEXT", "VARCHAR", "CLOB", "CHAR", "CHARACTER", "NCHAR", "BPCHAR", "NAME",
		"DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL",
		"UUID", "BYTEA", "INET", "CIDR", "MACADDR", "XML":
		return TypeString, true
	case "ARRAY":
		return TypeArray, true
	case "OBJECT":
		return TypeObject, true
	case "ANY", "JSON", "JSONB":
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

func (p *Parser) parseCreateIndex(unique bool) error {
	p.idx = CreateIndexStmt{Unique: unique}
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
	if p.atKeyword(kwUsing) {
		p.advance() // USING
		method, err := p.parseIndexMethod()
		if err != nil {
			return err
		}
		p.idx.Method = method
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
		if p.idx.Method != "" {
			return p.errHere("duplicate USING clause in CREATE INDEX")
		}
		p.advance() // USING
		method, err := p.parseIndexMethod()
		if err != nil {
			return err
		}
		p.idx.Method = method
	}
	if p.idx.Method == "tin" && len(p.idx.Paths) != 1 {
		return p.errHere("USING tin indexes one text path; compound full-text indexes are not supported")
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

// parseIndexMethod reads the access method after USING. Only tin exists
// beside the default exact index; anything else keeps the precise refusal
// so a new method arrives deliberately rather than by typo.
func (p *Parser) parseIndexMethod() (string, error) {
	name, err := p.parseAliasName("an index method after USING")
	if err != nil {
		return "", err
	}
	if equalFoldASCII(name, "tin") {
		return "tin", nil
	}
	return "", p.errHere("INDEX ... USING names an unknown method: only tin is supported besides the default exact index")
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
