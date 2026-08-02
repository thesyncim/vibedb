package sql

// Parsing and lexical binding for SELECT CTEs.

func (p *Parser) parseWithClause() error {
	withPos := p.tok.pos
	p.advance() // WITH
	recursive := false
	if p.atKeyword(kwRecursive) {
		recursive = true
		p.advance()
	}

	for {
		name, namePos, err := p.parseCTEName()
		if err != nil {
			return err
		}
		for i := range p.cteScratch {
			if p.cteScratch[i].Name == name {
				return newDuplicateCTEError(
					p.lx.src, namePos, name, p.cteScratch[i].Pos,
				)
			}
		}

		columns, aliasPos, err := p.parseCTEColumnAliases()
		if err != nil {
			return err
		}
		if err := p.expectKeyword(kwAs, "AS after a common table expression name"); err != nil {
			return err
		}

		materialization := CTEMaterializationDefault
		hintPos := -1
		switch {
		case p.atKeyword(kwMaterialized):
			hintPos = p.tok.pos
			materialization = CTEMaterialized
			p.advance()
		case p.atKeyword(kwNot):
			hintPos = p.tok.pos
			p.advance()
			if !p.atKeyword(kwMaterialized) {
				return p.errHere("expected MATERIALIZED after NOT in a common table expression")
			}
			materialization = CTENotMaterialized
			p.advance()
		}

		if err := p.expect(tokLParen, "'(' before a common table expression query"); err != nil {
			return err
		}
		switch {
		case p.atKeyword(kwInsert), p.atKeyword(kwUpdate), p.atKeyword(kwDelete), p.atKeyword(kwMerge):
			return newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"data-modifying common table expression bodies are not supported; use a SELECT body",
			)
		case !p.atKeyword(kwSelect) && !p.atKeyword(kwWith) && p.tok.kind != tokLParen:
			return p.errHere("expected SELECT, WITH ... SELECT, or a parenthesized query expression in a common table expression body")
		}

		// activeCTEs contains only earlier siblings at this point. The current
		// definition is appended after its body parses, which is precisely the
		// non-recursive visibility rule.
		var query *SelectStmt
		if p.correlation != nil && p.correlation.capture != nil {
			// A CTE body is a nested lexical query but remains inside the
			// containing LATERAL/predicate correlation context. Its own FROM
			// aliases resolve first; only actual misses reach the outer scopes.
			query, err = p.parseSubqueryScoped(
				false, p.correlation.outerRanges, p.correlation.capture,
			)
		} else {
			query, err = p.parseSubquery(false)
		}
		if err != nil {
			return err
		}
		if recursive {
			if extension := recursiveCTEClauseExtension(p.tok); extension != "" {
				return newFeatureNotSupportedError(
					p.lx.src, p.tok.pos,
					extension+" on a recursive common table expression is not supported yet",
				)
			}
		}
		arityKnown := cteOutputArityKnown(query)
		if arityKnown && len(columns) > len(query.Columns) {
			return newCTEColumnAliasArityError(
				p.lx.src, aliasPos[len(query.Columns)], name,
				len(columns), len(query.Columns),
			)
		}
		columnPositions := p.ints.allocDirty(len(aliasPos))
		copy(columnPositions, aliasPos)
		p.cteScratch = append(p.cteScratch, CommonTableExpr{
			Name: name, Columns: columns, ColumnPos: columnPositions,
			ColumnArityDeferred: len(columns) != 0 && !arityKnown,
			Query:               query,
			Materialization:     materialization,
			Pos:                 namePos, HintPos: hintPos,
		})
		if len(p.cteScratch) > maxClauseItems {
			return p.errfAt(namePos,
				"a WITH clause may hold at most %d common table expressions", maxClauseItems)
		}
		p.activeCTEs.defs = p.cteScratch

		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}

	definitions := p.ctes.allocDirty(len(p.cteScratch))
	copy(definitions, p.cteScratch)
	p.with = WithClause{CTEs: definitions, Recursive: recursive, Pos: withPos}
	p.out.With = &p.with
	p.activeCTEs.defs = definitions

	// A self/later spelling remains a physical relation candidate. Annotate
	// it only after all sibling names are known; catalog-aware binding can then
	// prefer a real collection and report self/forward use only when absent.
	for i := range definitions {
		markDeferredCTEReferences(definitions[i].Query, definitions[i:])
	}
	if recursive {
		if err := validateRecursiveCTEDefinitions(p.lx.src, definitions); err != nil {
			return err
		}
	}
	return nil
}

// cteOutputArityKnown reports whether the SELECT-list cardinality is also the
// result-schema cardinality. A whole-document wildcard is expanded by the
// relation materializer, so len(query.Columns) is not an arity proof for it.
// COUNT(*) has a nil path but is one scalar output and remains statically known.
func cteOutputArityKnown(query *SelectStmt) bool {
	if query.Set != nil {
		return !query.Set.ArityDeferred
	}
	for i := range query.Columns {
		column := &query.Columns[i]
		if column.Agg == AggNone && column.Path != nil && len(column.Path.Segments) == 0 {
			return false
		}
	}
	return true
}

func (p *Parser) parseCTEName() (string, int, error) {
	pos := p.tok.pos
	switch p.tok.kind {
	case tokIdent:
		if reserved(p.tok.kw) {
			return "", pos, p.errfHere(
				"expected a common table expression name, but found the reserved word %s; quote it to use it as a name",
				p.tok.text,
			)
		}
		fallthrough
	case tokQuotedIdent:
		if p.tok.text == "" {
			return "", pos, p.errHere("a common table expression name may not be empty")
		}
		name := p.internToken(p.tok)
		p.advance()
		return name, pos, nil
	default:
		return "", pos, p.errHere("expected a common table expression name after WITH")
	}
}

// parseCTEColumnAliases parses the optional list immediately after a CTE name.
// Positions are scratch because only the first excess alias needs to survive,
// and only on the error path after the body output count is known.
func (p *Parser) parseCTEColumnAliases() ([]string, []int, error) {
	if p.tok.kind != tokLParen {
		return nil, nil, nil
	}
	listPos := p.tok.pos
	p.advance()
	p.cteNameScratch = p.cteNameScratch[:0]
	p.cteAliasPosScratch = p.cteAliasPosScratch[:0]
	for {
		pos := p.tok.pos
		switch p.tok.kind {
		case tokIdent, tokQuotedIdent:
			if p.tok.text == "" {
				return nil, nil, p.errHere("a common table expression column alias may not be empty")
			}
		default:
			return nil, nil, p.errHere("expected a column name in the common table expression alias list")
		}
		p.cteNameScratch = append(p.cteNameScratch, p.internToken(p.tok))
		p.cteAliasPosScratch = append(p.cteAliasPosScratch, pos)
		if len(p.cteNameScratch) > maxClauseItems {
			return nil, nil, p.errfAt(listPos,
				"a common table expression alias list may hold at most %d columns", maxClauseItems)
		}
		p.advance()
		switch p.tok.kind {
		case tokRParen:
			p.advance()
			names := p.names.allocDirty(len(p.cteNameScratch))
			copy(names, p.cteNameScratch)
			return names, p.cteAliasPosScratch, nil
		case tokComma:
			p.advance()
		default:
			return nil, nil, p.errHere("expected ',' or ')' after a common table expression column alias")
		}
	}
}

func (p *Parser) lookupCTE(name string) *CommonTableExpr {
	for scope := &p.activeCTEs; scope != nil; scope = scope.outer {
		for i := len(scope.defs) - 1; i >= 0; i-- {
			if scope.defs[i].Name == name {
				return &scope.defs[i]
			}
		}
	}
	return nil
}

func markDeferredCTEReferences(query *SelectStmt, candidates []CommonTableExpr) {
	if query == nil {
		return
	}
	if query.With != nil {
		for i := range query.With.CTEs {
			markDeferredCTEReferences(query.With.CTEs[i].Query, candidates)
		}
	}
	for i := range query.From {
		ref := &query.From[i]
		switch ref.Kind {
		case RelationCollection:
			if ref.UnresolvedCTE.Kind != CTEReferenceNone {
				continue
			}
			for candidate := range candidates {
				if ref.Name != candidates[candidate].Name {
					continue
				}
				kind := CTEReferenceForward
				if candidate == 0 {
					kind = CTEReferenceSelf
				}
				ref.UnresolvedCTE = CTEReferenceMetadata{
					Kind: kind, DefinitionPos: candidates[candidate].Pos,
				}
				break
			}
		case RelationDerived:
			markDeferredCTEReferences(ref.Query, candidates)
		}
	}
	markDeferredCTEExpr(query.Where, candidates)
	markDeferredCTEExpr(query.Having, candidates)
	if query.Set != nil {
		markDeferredCTESetExpr(query.Set.Root, query.Set.First, candidates)
	}
}

func markDeferredCTESetExpr(
	expr *SetExpr,
	skip *SelectStmt,
	candidates []CommonTableExpr,
) {
	if expr == nil {
		return
	}
	switch expr.Kind {
	case SetSelectExpr:
		if expr.Select != skip {
			markDeferredCTEReferences(expr.Select, candidates)
		}
	case SetValuesExpr:
		return
	case SetTableExpr:
		if expr.Select != skip {
			markDeferredCTEReferences(expr.Select, candidates)
		}
	case SetBinaryExpr:
		markDeferredCTESetExpr(expr.Left, skip, candidates)
		markDeferredCTESetExpr(expr.Right, skip, candidates)
	case SetGroupExpr:
		markDeferredCTESetExpr(expr.Child, skip, candidates)
	}
}

func markDeferredCTEExpr(expr *Expr, candidates []CommonTableExpr) {
	if expr == nil {
		return
	}
	if expr.Subquery != nil {
		markDeferredCTEReferences(expr.Subquery, candidates)
	}
	for _, child := range expr.Kids {
		markDeferredCTEExpr(child, candidates)
	}
}
