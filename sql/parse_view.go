package sql

import "strings"

func (p *Parser) parseCreateView(materializedPos int) error {
	p.view = CreateViewStmt{Materialized: materializedPos >= 0}
	name, pos, err := p.parseViewName()
	if err != nil {
		return err
	}
	p.view.Name, p.view.Pos = name, pos
	if p.tok.kind == tokLParen {
		if err := p.parseViewColumnNames(); err != nil {
			return err
		}
	}
	if err := p.expectKeyword(kwAs, "AS before the view query"); err != nil {
		return err
	}
	p.view.QueryPos = p.tok.pos
	p.out = &p.sel
	*p.out = SelectStmt{}
	if err := p.parseStatement(); err != nil {
		return err
	}
	p.view.Query = &p.sel
	if p.sel.Params != 0 {
		position := firstViewParameterPosition(
			p.lx.src, p.view.QueryPos,
		)
		return newFeatureNotSupportedError(
			p.lx.src, position,
			"stored view definitions cannot contain parameters; place the predicate and its placeholder in the statement that reads the view",
		)
	}
	p.view.QuerySQL = p.internString(normalizedViewQuery(
		p.lx.src[p.view.QueryPos:],
	))
	if materializedPos >= 0 {
		return newFeatureNotSupportedError(
			p.lx.src, materializedPos,
			"CREATE MATERIALIZED VIEW requires one atomic publication spanning refreshed rows and durable catalog metadata; that cross-object commit primitive is not available",
		)
	}
	return nil
}

func (p *Parser) parseViewColumnNames() error {
	p.advance() // '('
	if p.tok.kind == tokRParen {
		return p.errHere("a view column list may not be empty")
	}
	names := p.cteNameScratch[:0]
	positions := p.cteAliasPosScratch[:0]
	for {
		position := p.tok.pos
		name, err := p.parseAliasName("a view output column name")
		if err != nil {
			return err
		}
		for i := range names {
			if names[i] == name {
				return p.errfAt(position, "view output column %q is declared twice", name)
			}
		}
		names = append(names, name)
		positions = append(positions, position)
		if len(names) > maxClauseItems {
			return p.errfAt(position, "a view may declare at most %d output columns", maxClauseItems)
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return err
	}
	ownedNames := p.names.allocDirty(len(names))
	copy(ownedNames, names)
	ownedPositions := p.ints.allocDirty(len(positions))
	copy(ownedPositions, positions)
	p.cteNameScratch, p.cteAliasPosScratch = names, positions
	p.view.Columns, p.view.ColumnPos = ownedNames, ownedPositions
	return nil
}

func (p *Parser) parseDropView() error {
	p.dropView = DropViewStmt{}
	p.advance() // VIEW
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
		p.dropView.IfExists = true
	}
	name, pos, err := p.parseViewName()
	if err != nil {
		return err
	}
	p.dropView.Name, p.dropView.Pos = name, pos
	switch {
	case tokenTextEqual(p.tok, "RESTRICT"):
		p.dropView.Restrict = true
		p.dropView.BehaviorPos = p.tok.pos
		p.advance()
	case tokenTextEqual(p.tok, "CASCADE"):
		position := p.tok.pos
		p.advance()
		if err := p.expectEnd(); err != nil {
			return err
		}
		return newFeatureNotSupportedError(
			p.lx.src, position,
			"DROP VIEW CASCADE is not supported; drop dependent views explicitly in dependency order",
		)
	}
	return p.expectEnd()
}

// parseUnsupportedRefreshView validates PostgreSQL's complete materialized
// view refresh shape before returning the typed refusal. Malformed REFRESH text
// therefore remains 42601 while every valid variant is the same positioned
// 0A000 as CREATE/DROP MATERIALIZED VIEW.
func (p *Parser) parseUnsupportedRefreshView() error {
	position := p.tok.pos
	p.advance() // REFRESH
	if err := p.expectKeyword(kwMaterialized, "MATERIALIZED after REFRESH"); err != nil {
		return err
	}
	if err := p.expectKeyword(kwView, "VIEW after REFRESH MATERIALIZED"); err != nil {
		return err
	}
	if tokenTextEqual(p.tok, "CONCURRENTLY") {
		p.advance()
	}
	if err := p.consumeUnsupportedQualifiedName("a materialized view name"); err != nil {
		return err
	}
	if tokenTextEqual(p.tok, "WITH") {
		p.advance()
		if tokenTextEqual(p.tok, "NO") {
			p.advance()
		}
		if !tokenTextEqual(p.tok, "DATA") {
			return p.errHere("expected DATA or NO DATA after WITH")
		}
		p.advance()
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	return newFeatureNotSupportedError(
		p.lx.src, position,
		"REFRESH MATERIALIZED VIEW requires one atomic publication spanning refreshed rows and durable catalog metadata; that cross-object commit primitive is not available",
	)
}

func (p *Parser) parseViewName() (string, int, error) {
	position := p.tok.pos
	switch p.tok.kind {
	case tokIdent:
		if err := p.checkNameable("a view name"); err != nil {
			return "", position, err
		}
	case tokQuotedIdent:
	default:
		return "", position, p.errHere("expected a view name")
	}
	if p.tok.text == "" {
		return "", position, p.errHere("a view name may not be empty")
	}
	name := p.internToken(p.tok)
	p.advance()
	if p.tok.kind == tokDot {
		return "", position, p.errHere("qualified view names (schema.view) are not supported")
	}
	return name, position, nil
}

func normalizedViewQuery(source string) string {
	query := strings.TrimSpace(source)
	if strings.HasSuffix(query, ";") {
		query = strings.TrimSpace(query[:len(query)-1])
	}
	return query
}

func firstViewParameterPosition(source string, start int) int {
	if start < 0 || start > len(source) {
		return max(start, 0)
	}
	lexer := lexer{src: source[start:]}
	for {
		token := lexer.next()
		switch token.kind {
		case tokParam:
			return start + token.pos
		case tokEOF, tokError:
			return start
		}
	}
}
