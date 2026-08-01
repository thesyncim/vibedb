package sql

// Parsing INSERT, UPDATE, and DELETE.
//
// The whole file rests on one structural choice, stated once here: an UPDATE's
// or a DELETE's row selection is parsed into a real [SelectStmt], with a
// synthetic COUNT(*) select list, the statement's collection as its single
// range variable, and the statement's WHERE as its filter. Nothing here parses
// a predicate; parseExpr does, exactly as it does for SELECT, and resolvePaths
// and validate then run over the result unchanged.
//
// The payoff is not code reuse — the predicate grammar is a few hundred lines
// and duplicating it would have been cheap. It is that "DELETE removes exactly
// the documents SELECT returns" stops being a property somebody has to test for
// and becomes a property of the representation: there is one predicate parser,
// one path resolver, and downstream one lowering, so the two cannot disagree
// about a null, a missing path, or a NOT over either.

// ParseStatement parses one statement of any supported kind into dst, reusing
// p's storage. See [Parser] for the lifetime dst and everything reachable from
// it inherit.
func (p *Parser) ParseStatement(dst *Statement, src string) error {
	*dst = Statement{}
	if err := validateStatementText(src); err != nil {
		return err
	}
	p.reset(src)
	if err := p.parseAnyStatement(dst); err != nil {
		// As in Parse, the half-parsed statement is thrown away rather than
		// returned beside the error: a caller that ignores the error and lowers
		// dst anyway must get nothing rather than whichever clauses parsed.
		*dst = Statement{}
		return err
	}
	return nil
}

// ParseStatement parses one statement of any supported kind. The returned
// statement owns its storage outright; a caller parsing in a loop holds a
// [Parser] instead.
func ParseStatement(src string) (*Statement, error) {
	var p Parser
	dst := new(Statement)
	if err := p.ParseStatement(dst, src); err != nil {
		return nil, err
	}
	return dst, nil
}

// KindOf reports which sort of statement src begins with, without parsing it.
//
// It lexes exactly one token and allocates nothing, which is what makes it
// usable as a routing decision on a driver's prepare path: database/sql needs
// to know whether a statement returns rows before it decides which of its own
// two entry points to build. An unrecognized leading word answers KindSelect,
// so a malformed statement is reported by the parser with a position rather
// than by a classifier that has no message worth giving.
func KindOf(src string) Kind {
	lx := lexer{src: src}
	switch tok := lx.next(); {
	case tok.kind != tokIdent:
		return KindSelect
	case tok.kw == kwInsert:
		return KindInsert
	case tok.kw == kwUpdate:
		return KindUpdate
	case tok.kw == kwDelete:
		return KindDelete
	case tok.kw == kwCreate:
		// The two CREATE kinds differ in their second word, which is one more
		// token than a router needs: both go to Exec, and both are told apart by
		// the parse that follows. KindCreateTable stands for "a CREATE of some
		// kind" here, and callers that need the distinction have the parsed
		// statement by the time they do.
		return KindCreateTable
	}
	return KindSelect
}

// parseAnyStatement dispatches on the leading keyword. The three statement
// kinds SQL has that this engine has no execution for at all are named here
// rather than left to fall through as "expected SELECT", because a MERGE
// rejected as a syntax error reads as if the syntax were the problem.
func (p *Parser) parseAnyStatement(dst *Statement) error {
	switch {
	case p.atKeyword(kwExplain):
		p.advance()
		analyze := p.atKeyword(kwAnalyze)
		if analyze {
			p.advance()
		}
		if !p.atKeyword(kwSelect) {
			return p.errHere("EXPLAIN accepts SELECT or ANALYZE SELECT only")
		}
		dst.Kind, dst.Explain, dst.Analyze, dst.Select = KindSelect, true, analyze, &p.sel
		p.out = &p.sel
		*p.out = SelectStmt{}
		return p.parseStatement()
	case p.atKeyword(kwInsert):
		dst.Kind, dst.Insert = KindInsert, &p.ins
		return p.parseInsert()
	case p.atKeyword(kwUpdate):
		dst.Kind, dst.Update = KindUpdate, &p.upd
		return p.parseUpdate()
	case p.atKeyword(kwDelete):
		dst.Kind, dst.Delete = KindDelete, &p.del
		return p.parseDelete()
	case p.atKeyword(kwMerge):
		return p.errHere("MERGE is not supported: it is a conditional insert-or-update over a join, and this engine has neither a conditional write nor a join that returns rows to write from")
	case p.atKeyword(kwReplace):
		return p.errHere("REPLACE is not supported; INSERT into a key that already exists is refused, and a deliberate overwrite is written UPDATE ... SET \"$doc\" = ?")
	case p.atKeyword(kwCreate):
		return p.parseCreate(dst)
	case p.atKeyword(kwDrop):
		return p.errHere("DROP is not supported: removing a collection or an index is a catalog operation with consequences a statement cannot undo, so it is left to the application that owns the catalog")
	case p.atKeyword(kwAlter):
		return p.errHere("ALTER is not supported: a declared schema is frozen when the collection is created, and altering one would have to revalidate every stored document")
	case p.atKeyword(kwTruncate):
		return p.errHere("TRUNCATE is not supported: it is a metadata operation on a table, and this store has no such operation; write DELETE FROM, which removes documents one batch at a time")
	}
	dst.Kind, dst.Select = KindSelect, &p.sel
	p.out = &p.sel
	*p.out = SelectStmt{}
	return p.parseStatement()
}

// --- INSERT ------------------------------------------------------------------

func (p *Parser) parseInsert() error {
	p.ins = InsertStmt{}
	p.out = &p.sel
	*p.out = SelectStmt{}
	p.advance() // INSERT
	if err := p.expectKeyword(kwInto, "INTO after INSERT"); err != nil {
		return err
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.ins.Table, p.ins.Pos = name, pos
	if err := p.rejectAlias(); err != nil {
		return err
	}
	if p.tok.kind == tokLParen {
		if err := p.parseInsertColumns(); err != nil {
			return err
		}
	}
	switch {
	case p.atKeyword(kwSelect):
		return p.errHere("INSERT ... SELECT is not supported: the engine executes one plan and has nowhere to send its rows; read with SELECT and write the documents back")
	case p.atKeyword(kwDefault):
		return p.errHere("INSERT ... DEFAULT VALUES is not supported: a schemaless document has no declared columns and therefore no defaults")
	}
	if err := p.expectKeyword(kwValues, "VALUES"); err != nil {
		return err
	}
	rows := p.rows[:0]
	for {
		row, err := p.parseInsertRow()
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if len(rows) > maxClauseItems {
			return p.errfAt(row.Pos, "an INSERT may hold at most %d rows", maxClauseItems)
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	p.rows = rows
	p.ins.Rows = rows
	if p.acceptKeyword(kwReturning) {
		if err := p.parseInsertReturning(name, pos); err != nil {
			return err
		}
		p.ins.Params = p.params
		return nil
	}
	if err := p.rejectTail(); err != nil {
		return err
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	p.ins.Params = p.params
	return nil
}

// parseInsertReturning parses the projection over the documents already
// materialized by INSERT. Keeping it inside a SelectStmt makes RETURNING a
// reuse of the query engine's projection lane rather than a second JSON path
// evaluator in the write adapter.
func (p *Parser) parseInsertReturning(name string, pos int) error {
	p.out = &p.sel
	*p.out = SelectStmt{}
	// INSERT's optional field list is parsed as paths but is not part of a
	// range-variable expression. Do not let RETURNING's resolver bind those
	// retained field names to its synthetic FROM entry.
	p.pending = p.pending[:0]
	p.from = append(p.from[:0], TableRef{
		Name: name, Alias: name, Join: JoinNone, Pos: pos,
	})
	p.out.From = p.from
	if err := p.parseResultColumns(); err != nil {
		return err
	}
	for i := range p.out.Columns {
		if p.out.Columns[i].Agg != AggNone {
			return p.errAt(
				p.out.Columns[i].Pos,
				"RETURNING projects each inserted document; aggregate functions are not allowed",
			)
		}
	}
	if err := p.rejectTail(); err != nil {
		return err
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	if err := p.resolvePaths(); err != nil {
		return err
	}
	// RETURNING's projection grammar contains no value expressions, so the
	// INSERT's VALUES placeholders belong only to the mutation plan.
	p.out.Params = 0
	if err := p.validate(); err != nil {
		return err
	}
	p.ins.Returning = p.out
	return nil
}

// parseInsertColumns parses the flat document fields synthesized by VALUES.
// A complete document uses no column list.
func (p *Parser) parseInsertColumns() error {
	p.advance() // '('
	paths := p.idxPaths[:0]
	for {
		path, err := p.parsePath(false)
		if err != nil {
			return err
		}
		if len(path.Segments) != 1 || path.Segments[0].IsIndex {
			return p.errAt(path.Pos, "an INSERT column list builds a flat JSON object, so each column must be one top-level field")
		}
		for _, prior := range paths {
			if sameSpec(prior, path) {
				return p.errfAt(path.Pos, "INSERT column %q is named twice", path.Spec())
			}
		}
		paths = append(paths, path)
		if len(paths) > maxClauseItems {
			return p.errfAt(path.Pos, "an INSERT may name at most %d columns", maxClauseItems)
		}
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	p.idxPaths = paths
	p.ins.Columns = paths
	return p.expect(tokRParen, "')'")
}

func (p *Parser) parseInsertRow() (InsertRow, error) {
	row := InsertRow{Pos: p.tok.pos}
	if p.tok.kind != tokLParen {
		return row, p.errHere("expected '(' to open a VALUES row")
	}
	p.advance()
	if len(p.ins.Columns) != 0 {
		// Values are retained by the parsed row, so they must come from the
		// Parser's stable arena rather than a per-row heap allocation. The
		// arena also keeps warmed ParseStatement allocation-free.
		values := p.ops.allocDirty(len(p.ins.Columns))
		for i := range p.ins.Columns {
			if i != 0 {
				if p.tok.kind != tokComma {
					return row, p.errfHere("the VALUES row has %d value(s), but the column list has %d", i, len(p.ins.Columns))
				}
				p.advance()
			}
			value, err := p.parseOperand()
			if err != nil {
				return row, err
			}
			values[i] = value
		}
		if p.tok.kind == tokComma {
			return row, p.errfHere("the VALUES row has more values than its %d-column list", len(p.ins.Columns))
		}
		if err := p.expect(tokRParen, "')'"); err != nil {
			return row, err
		}
		row.Values = values
		return row, nil
	}
	// Without a column list, a VALUES row is exactly one complete JSON
	// document. Its storage identity is derived later from the table's declared
	// scalar JSON PRIMARY KEY.
	document, err := p.parseDocumentOperand()
	if err != nil {
		return row, err
	}
	if p.tok.kind == tokComma {
		return row, p.errHere(
			"an INSERT row without a column list holds one complete JSON document; " +
				"put the declared primary-key field inside that document, or use a flat declared-field list")
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return row, err
	}
	values := p.ops.allocDirty(1)
	values[0] = document
	row.Values = values
	return row, nil
}

// parseDocumentOperand reads a whole JSON document.
//
// Three spellings are accepted, and each exists because one of the three ways a
// document reaches a statement needs it: a '?' placeholder for the ordinary
// driver case, where the document is a []byte the application already holds; a
// single-quoted SQL string for a document written inline in SQL text; and a
// bare JSON object or array, delimited structurally out of the source exactly
// as the '@>' needle is, so a hand-written statement does not have to double
// every quote inside its own document.
//
// None of the three is validated here. Validation is the store's, performed by
// the same parser that will index the document, because a second JSON grammar
// in this package would be a second thing to keep in agreement with the first.
func (p *Parser) parseDocumentOperand() (Operand, error) {
	pos := p.tok.pos
	switch {
	case p.tok.kind == tokParam, p.tok.kind == tokString:
		return p.parseOperand()
	case p.tok.kind == tokLBracket, p.tok.kind == tokLBrace:
		text, start, end, err := p.scanJSONDocument(pos)
		if err != nil {
			return Operand{}, err
		}
		p.lx.pos = end
		p.advance()
		return Operand{Kind: OperandJSON, Text: p.internString(text), Pos: start}, nil
	case p.atKeyword(kwDefault):
		return Operand{}, p.errHere("DEFAULT is not a document: a schemaless collection has no declared columns and therefore no defaults")
	case p.atKeyword(kwNull):
		return Operand{}, p.errHere("NULL is not a document: a stored document is JSON text, and the JSON document 'null' is written as the literal null inside a placeholder or a quoted string")
	}
	return Operand{}, p.errHere("expected a document: '?', a single-quoted JSON string, or a bare JSON object or array")
}

// scanJSONDocument delimits one bare JSON document out of the statement text.
// It is scanJSONValue with the diagnostics reworded, since "after '@>'" is
// wrong here and a wrong position hint is worse than none.
func (p *Parser) scanJSONDocument(from int) (text string, start, end int, err error) {
	src := p.lx.src
	i := from
	for i < len(src) && isJSONSpace(src[i]) {
		i++
	}
	if i >= len(src) {
		return "", i, 0, p.errAt(i, "expected a JSON document")
	}
	start = i
	switch src[i] {
	case '{', '[':
		end, err = p.scanJSONContainer(i)
	default:
		return "", start, 0, p.errAt(start, "expected a JSON object or array")
	}
	if err != nil {
		return "", start, 0, err
	}
	return src[start:end], start, end, nil
}

// --- UPDATE ------------------------------------------------------------------

func (p *Parser) parseUpdate() error {
	p.upd = UpdateStmt{}
	p.advance() // UPDATE
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.upd.Table, p.upd.Pos = name, pos
	if err := p.rejectAlias(); err != nil {
		return err
	}
	p.beginFilter(name, pos)
	if err := p.expectKeyword(kwSet, "SET"); err != nil {
		return err
	}
	p.upd.SetPos = p.tok.pos
	doc, err := p.parseAssignment()
	if err != nil {
		return err
	}
	p.upd.Doc = doc
	if p.tok.kind == tokComma {
		return p.errHere("an UPDATE assigns the whole document once: a second assignment would have to be merged into the first, and this engine has no partial document update to merge with")
	}
	if p.atKeyword(kwFrom) {
		return p.errHere("UPDATE ... FROM is not supported: the value written comes from the statement, never from another collection")
	}
	if err := p.parseDMLWhere("UPDATE"); err != nil {
		return err
	}
	p.upd.Filter = p.out
	p.upd.Params = p.params
	return nil
}

// parseAssignment parses the single `"$doc" = value` assignment an UPDATE
// carries, and refuses a path assignment with the reason.
//
// The refusal is the largest deliberate gap in this dialect, and it is written
// out at length on [UpdateStmt] rather than here, because a reader hitting the
// message wants the alternative and a reader reading the type wants the
// argument. What the message has to carry is the alternative: read, edit, write
// back.
func (p *Parser) parseAssignment() (Operand, error) {
	if p.tok.kind == tokQuotedIdent && !p.tok.esc && p.tok.text == DocumentColumn {
		p.advance()
		if p.tok.kind != tokEq {
			return Operand{}, p.errfHere("expected '=' after %q", DocumentColumn)
		}
		p.advance()
		return p.parseDocumentOperand()
	}
	// Anything else that looks like a path is the partial update this engine
	// has no primitive for. Naming the path back is what makes the message
	// actionable rather than a policy statement.
	path, err := p.parsePath(false)
	if err != nil {
		return Operand{}, err
	}
	return Operand{}, p.errfAt(path.Pos,
		"SET %s = ... is a partial document update, which this engine has no primitive for: every write it owns replaces a document whole, "+
			"and there is no JSON path-set operation anywhere in this codebase for a read-modify-write to call. "+
			"Read the document with SELECT, edit it where your documents are already built, and write it back with SET %q = ?",
		path.Spec(), DocumentColumn)
}

// --- DELETE ------------------------------------------------------------------

func (p *Parser) parseDelete() error {
	p.del = DeleteStmt{}
	p.advance() // DELETE
	if err := p.expectKeyword(kwFrom, "FROM after DELETE"); err != nil {
		return err
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.del.Table, p.del.Pos = name, pos
	if err := p.rejectAlias(); err != nil {
		return err
	}
	p.beginFilter(name, pos)
	if p.atKeyword(kwUsing) {
		return p.errHere("DELETE ... USING is not supported: the rows to delete are chosen by a condition on the collection itself, never by a join")
	}
	whereAt := p.tok.pos
	if err := p.parseDMLWhere("DELETE"); err != nil {
		return err
	}
	switch {
	case p.out.Where == nil:
		// A DELETE with no WHERE is legal SQL and means every row. It is
		// recorded as its own flag rather than as "a filter that happens to be
		// nil", so an executor cannot arrive at "delete everything" by failing
		// to look at one pointer.
		p.del.All = true
		p.del.Filter = p.out
		_ = whereAt
	default:
		p.del.Filter = p.out
	}
	p.del.Params = p.params
	return nil
}

// --- shared ------------------------------------------------------------------

// parseCollectionName reads the collection an INSERT, UPDATE, or DELETE names.
func (p *Parser) parseCollectionName() (string, int, error) {
	pos := p.tok.pos
	switch p.tok.kind {
	case tokIdent:
		if err := p.checkNameable("a collection name"); err != nil {
			return "", pos, err
		}
	case tokQuotedIdent:
	case tokLParen:
		return "", pos, p.errHere("subqueries are not supported: a statement writes to one declared collection")
	default:
		return "", pos, p.errHere("expected a collection name")
	}
	if p.tok.text == "" {
		return "", pos, p.errHere("a collection name may not be empty")
	}
	name := p.internToken(p.tok)
	p.advance()
	if p.tok.kind == tokDot {
		return "", pos, p.errHere("qualified collection names (schema.table) are not supported")
	}
	return name, pos, nil
}

// rejectAlias refuses a range variable after a single-collection statement's
// table.
//
// It is refused rather than accepted and ignored. `UPDATE users u SET ...`
// reads as if u were usable to qualify paths, and a single-collection statement
// has nothing for a range variable to disambiguate, so accepting the alias
// would only create a name whose only effect is to be silently unnecessary.
//
// A word that resolved to any keyword is not a candidate alias, reserved or
// not. The clause keywords that can follow a collection name here — RETURNING,
// ON, PRIMARY — are deliberately unreserved so they stay usable as field names,
// and treating one as an alias would answer "a table alias has nothing to
// qualify" to an author who wrote no alias at all. It is also why this is a
// separate step: CREATE TABLE ... AS has its own answer, and reaching this one
// first would give it the wrong message.
func (p *Parser) rejectAlias() error {
	if p.tok.kind == tokQuotedIdent || (p.tok.kind == tokIdent && p.tok.kw == kwNone) || p.atKeyword(kwAs) {
		return p.errHere("a table alias has nothing to qualify in a single-collection statement; write the paths unqualified")
	}
	return nil
}

// beginFilter prepares p.out as the SELECT this statement's row selection is
// defined to be: one COUNT(*) column, the named collection as the only range
// variable, and, once parsed, the statement's own WHERE.
//
// The COUNT(*) is not decoration. The plan compiler requires at least one
// output column, and COUNT(*) is the only column that reads no path at all, so
// the filter plan extracts exactly the columns WHERE names and nothing else —
// which is what makes the DML scan cost the predicate's cost rather than the
// predicate's plus a projection nobody reads.
func (p *Parser) beginFilter(name string, pos int) {
	p.out = &p.sel
	*p.out = SelectStmt{}
	p.columns = append(p.columns[:0], ResultColumn{Agg: AggCount, Pos: pos})
	p.out.Columns = p.columns
	p.from = append(p.from[:0], TableRef{Name: name, Alias: name, Join: JoinNone, Pos: pos})
	p.out.From = p.from
}

// parseDMLWhere parses the optional WHERE of an UPDATE or a DELETE and
// finishes the equivalent SELECT filter.
func (p *Parser) parseDMLWhere(clause string) error {
	if p.acceptKeyword(kwWhere) {
		where, err := p.parseExpr(ctxWhere)
		if err != nil {
			return err
		}
		p.out.Where = where
	}
	switch {
	case p.atKeyword(kwGroup), p.atKeyword(kwHaving):
		return p.errfHere("%s has no GROUP BY or HAVING: it acts on documents, not on groups of them", clause)
	case p.atKeyword(kwOrder):
		return p.errfHere("%s has no ORDER BY: which documents it touches is decided by the condition, and the order it touches them in is not observable", clause)
	case p.atKeyword(kwLimit), p.atKeyword(kwOffset):
		return p.errfHere("%s has no LIMIT: a bounded delete would have to choose which matching documents to spare, and the engine has no ordering to choose by", clause)
	}
	if err := p.rejectTail(); err != nil {
		return err
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	if err := p.resolvePaths(); err != nil {
		return err
	}
	p.out.Params = p.params
	return p.validate()
}

// rejectTail names the clauses a DML statement can be written with in other
// dialects and this one has no execution for.
func (p *Parser) rejectTail() error {
	if p.atKeyword(kwReturning) {
		return p.errHere("RETURNING is supported only on INSERT; UPDATE and DELETE report how many documents they touched and return no rows")
	}
	if p.atKeyword(kwOn) {
		return p.errHere("ON CONFLICT / ON DUPLICATE KEY is not supported: an INSERT onto an existing key is refused, and a deliberate overwrite is written UPDATE ... SET \"$doc\" = ?")
	}
	return nil
}
