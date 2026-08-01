package sql

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
	if p.acceptKeyword(kwOn) {
		table, pos, err := p.parseCollectionName()
		if err != nil {
			return err
		}
		p.dropIndex.Table = table
		p.dropIndex.HasTable = true
		p.dropIndex.TablePos = pos
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
	if err := p.rejectAlias(); err != nil {
		return err
	}
	return p.expectEnd()
}
