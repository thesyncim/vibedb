package sql

// Parsing DROP TABLE. Physical removal is deliberately not part of the SQL
// parser: the driver owns catalog publication, durable collection closure,
// and deferred file cleanup.

func (p *Parser) parseDropTable() error {
	p.drop = DropTableStmt{}
	p.advance() // DROP
	if err := p.expectKeyword(kwTable, "TABLE after DROP"); err != nil {
		return err
	}
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
