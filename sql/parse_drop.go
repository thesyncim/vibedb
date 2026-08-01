package sql

import "strings"

// Parsing catalog-removal statements. Physical removal is deliberately not
// part of the SQL parser: an adapter owns catalog publication, durable object
// lifecycle, and any deferred cleanup.

func (p *Parser) parseDrop(dst *Statement) error {
	p.advance() // DROP
	switch {
	case p.atKeyword(kwTable):
		dst.Kind, dst.DropTable = KindDropTable, &p.drop
		return p.parseDropTable()
	case p.atKeyword(kwIndex):
		dst.Kind, dst.DropIndex = KindDropIndex, &p.dropIndex
		return p.parseDropIndex()
	default:
		if p.tok.kind == tokIdent && recognizedDropObjectKind(p.tok.text) {
			return p.parseUnsupportedDrop()
		}
		return p.errHere("expected TABLE or INDEX after DROP")
	}
}

func (p *Parser) parseDropTable() error {
	p.drop = DropTableStmt{}
	p.advance() // TABLE
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
		p.drop.IfExists = true
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.drop.Table, p.drop.Pos = name, pos
	multiple := false
	if p.tok.kind == tokComma {
		multiple = true
		for p.tok.kind == tokComma {
			p.advance()
			if _, _, err := p.parseCollectionName(); err != nil {
				return err
			}
		}
	}
	behavior := p.acceptUnsupportedDropBehavior()
	if multiple || behavior {
		if err := p.expectEnd(); err != nil {
			return err
		}
		if multiple {
			return p.featureNotSupportedHere(
				"DROP TABLE of multiple tables is not supported",
			)
		}
		return p.featureNotSupportedHere(
			"DROP TABLE CASCADE/RESTRICT is not supported; remove dependencies explicitly",
		)
	}
	if err := p.rejectAlias(); err != nil {
		return err
	}
	return p.expectEnd()
}

func (p *Parser) parseDropIndex() error {
	p.dropIndex = DropIndexStmt{}
	p.advance() // INDEX
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
		p.dropIndex.IfExists = true
	}
	p.dropIndex.Pos = p.tok.pos
	name, err := p.parseAliasName("an index name")
	if err != nil {
		return err
	}
	p.dropIndex.Name = name
	multiple := false
	if p.tok.kind == tokComma {
		multiple = true
		for p.tok.kind == tokComma {
			p.advance()
			if _, err := p.parseAliasName("an index name"); err != nil {
				return err
			}
		}
	}
	behavior := p.acceptUnsupportedDropBehavior()
	if multiple || behavior {
		if err := p.expectEnd(); err != nil {
			return err
		}
		if multiple {
			return p.featureNotSupportedHere(
				"DROP INDEX of multiple indexes is not supported",
			)
		}
		return p.featureNotSupportedHere(
			"DROP INDEX CASCADE/RESTRICT is not supported",
		)
	}
	if p.acceptKeyword(kwOn) {
		table, pos, err := p.parseCollectionName()
		if err != nil {
			return err
		}
		p.dropIndex.Table = table
		p.dropIndex.HasTable = true
		p.dropIndex.TablePos = pos
		if p.acceptUnsupportedDropBehavior() {
			if err := p.expectEnd(); err != nil {
				return err
			}
			return p.featureNotSupportedHere(
				"DROP INDEX CASCADE/RESTRICT is not supported",
			)
		}
		if err := p.rejectAlias(); err != nil {
			return err
		}
	}
	return p.expectEnd()
}

func (p *Parser) parseTruncate() error {
	p.truncate = TruncateStmt{}
	p.advance() // TRUNCATE
	p.acceptKeyword(kwTable)
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.truncate.Table, p.truncate.Pos = name, pos
	multiple := false
	for p.tok.kind == tokComma {
		multiple = true
		p.advance()
		if _, _, err := p.parseCollectionName(); err != nil {
			return err
		}
	}
	identity := false
	if tokenTextEqual(p.tok, "RESTART") || tokenTextEqual(p.tok, "CONTINUE") {
		identity = true
		option := strings.ToUpper(p.tok.text)
		p.advance()
		if !tokenTextEqual(p.tok, "IDENTITY") {
			return p.errfHere("expected IDENTITY after %s", option)
		}
		p.advance()
	}
	behavior := p.acceptUnsupportedDropBehavior()
	if multiple || identity || behavior {
		if err := p.expectEnd(); err != nil {
			return err
		}
		switch {
		case multiple:
			return p.featureNotSupportedHere(
				"TRUNCATE of multiple tables is not supported",
			)
		case identity:
			return p.featureNotSupportedHere(
				"TRUNCATE identity options are not supported; document keys have no SQL identity generator",
			)
		default:
			return p.featureNotSupportedHere(
				"TRUNCATE CASCADE/RESTRICT is not supported",
			)
		}
	}
	if err := p.rejectAlias(); err != nil {
		return err
	}
	return p.expectEnd()
}

func (p *Parser) acceptUnsupportedDropBehavior() bool {
	if !tokenTextEqual(p.tok, "CASCADE") && !tokenTextEqual(p.tok, "RESTRICT") {
		return false
	}
	p.advance()
	return true
}

// parseUnsupportedDrop validates the complete structural grammar before
// returning FeatureNotSupported. This distinction is observable over pgwire:
// a valid unsupported DROP is 0A000, while a prefix missing its required name,
// ON target, signature delimiter, or closing token remains 42601.
func (p *Parser) parseUnsupportedDrop() error {
	pos := p.tok.pos
	kind := strings.ToUpper(p.tok.text)
	p.advance()
	if kind == "MATERIALIZED" {
		if !tokenTextEqual(p.tok, "VIEW") {
			return p.errHere("expected VIEW after MATERIALIZED")
		}
		p.advance()
		kind = "MATERIALIZED VIEW"
	}
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
	}

	switch kind {
	case "TRIGGER", "POLICY":
		if err := p.consumeUnsupportedQualifiedName("an object name"); err != nil {
			return err
		}
		if !p.acceptKeyword(kwOn) {
			return p.errfHere("expected ON after DROP %s name", kind)
		}
		if err := p.consumeUnsupportedQualifiedName("a table name"); err != nil {
			return err
		}
	case "FUNCTION", "PROCEDURE":
		if err := p.consumeUnsupportedDropNameList(true, true); err != nil {
			return err
		}
	default:
		allowMultiple := kind != "DATABASE" && kind != "EXTENSION" &&
			kind != "COLLATION"
		if err := p.consumeUnsupportedDropNameList(false, allowMultiple); err != nil {
			return err
		}
	}
	if kind == "DATABASE" && tokenTextEqual(p.tok, "WITH") {
		p.advance()
		if err := p.expect(tokLParen, "'(' after WITH"); err != nil {
			return err
		}
		if !tokenTextEqual(p.tok, "FORCE") {
			return p.errHere("expected FORCE in DROP DATABASE WITH option")
		}
		p.advance()
		if err := p.expect(tokRParen, "')' after FORCE"); err != nil {
			return err
		}
	}
	if kind != "DATABASE" && kind != "ROLE" && kind != "USER" {
		p.acceptUnsupportedDropBehavior()
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	return newFeatureNotSupportedError(
		p.lx.src, pos, "DROP "+kind+" is not supported",
	)
}

func (p *Parser) consumeUnsupportedDropNameList(
	signature bool,
	allowMultiple bool,
) error {
	for {
		if err := p.consumeUnsupportedQualifiedName("an object name"); err != nil {
			return err
		}
		if signature && p.tok.kind == tokLParen {
			if err := p.consumeUnsupportedParenthesized(); err != nil {
				return err
			}
		}
		if p.tok.kind != tokComma || !allowMultiple {
			return nil
		}
		p.advance()
	}
}

func (p *Parser) consumeUnsupportedQualifiedName(what string) error {
	for {
		if p.tok.kind != tokIdent && p.tok.kind != tokQuotedIdent {
			return p.errfHere("expected %s", what)
		}
		if p.tok.text == "" {
			return p.errfHere("%s may not be empty", what)
		}
		p.advance()
		if p.tok.kind != tokDot {
			return nil
		}
		p.advance()
	}
}

func (p *Parser) consumeUnsupportedParenthesized() error {
	depth := 0
	argumentHasToken := false
	sawArgumentComma := false
	for {
		switch p.tok.kind {
		case tokEOF:
			return p.errHere("unterminated routine signature")
		case tokError:
			return p.errHere(p.tok.text)
		case tokLParen:
			if depth == 1 {
				argumentHasToken = true
			}
			depth++
			if depth > maxExprDepth {
				return p.errfHere(
					"routine signature nests deeper than %d levels", maxExprDepth,
				)
			}
		case tokRParen:
			if depth == 1 && sawArgumentComma && !argumentHasToken {
				return p.errHere("expected an argument type after ','")
			}
			depth--
			p.advance()
			if depth == 0 {
				return nil
			}
			continue
		case tokComma:
			if depth == 1 {
				if !argumentHasToken {
					return p.errHere("expected an argument type before ','")
				}
				argumentHasToken = false
				sawArgumentComma = true
				p.advance()
				continue
			}
		default:
			if depth == 1 {
				argumentHasToken = true
			}
		}
		p.advance()
	}
}

func tokenTextEqual(tok token, value string) bool {
	return tok.kind == tokIdent && strings.EqualFold(tok.text, value)
}

func recognizedDropObjectKind(value string) bool {
	switch strings.ToUpper(value) {
	case "VIEW", "MATERIALIZED", "SCHEMA", "DATABASE", "SEQUENCE",
		"TYPE", "DOMAIN", "FUNCTION", "PROCEDURE", "TRIGGER", "POLICY",
		"ROLE", "USER", "EXTENSION", "COLLATION":
		return true
	default:
		return false
	}
}
