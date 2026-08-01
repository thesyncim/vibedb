package sql

import (
	"strconv"
	"unicode/utf8"

	"github.com/thesyncim/vibejson/x/byteview"
)

// maxExprDepth bounds predicate nesting.
//
// It exists for one reason: the predicate grammar is parsed by recursive
// descent, so "((((((..." in the input is recursion in the parser, and an
// unbounded input is an unbounded stack. A stack overflow is not a recoverable
// Go panic — it kills the process — so a driver that parsed attacker-supplied
// SQL without this bound would have a remote crash, and the package's fuzz
// target would find it in seconds. Sixty-four is far past any hand-written
// predicate and far short of the goroutine stack limit.
const maxExprDepth = 64

// maxSubqueryDepth bounds recursive child Parser calls. Predicate parentheses
// stay in one parser and use maxExprDepth; each nested SELECT owns a child
// arena, so it needs a separate call-stack bound.
const maxSubqueryDepth = 32

// maxParams bounds the placeholders in one statement, so a pathological input
// cannot make the parser mint ordinals until the arena exhausts memory. It is
// well past any statement a driver would prepare.
const maxParams = 1 << 16

// maxClauseItems bounds the entries in a comma-separated clause: the select
// list, the range variables, the grouping keys, and the sort keys.
//
// The bound exists because the checks that follow parsing compare those lists
// against each other — every projection against every grouping key, every sort
// key against every projection — and those comparisons are quadratic. The
// quadratic is the right implementation for a list of a handful of entries,
// where a hash would cost more to build than it saves, but it means a
// multi-megabyte select list would spend minutes in validation. One bound turns
// that into a constant ceiling of about a million comparisons, and a thousand
// entries in any one clause is already far past a statement anybody writes.
const maxClauseItems = 1024

// maxStatementBytes is the parser's total input bound. Structural limits cap
// tree shape, but without a byte bound one identifier, quoted string, comment,
// or containment literal could still force an unbounded copy and make context
// cancellation wait on attacker-selected input. Sixteen MiB matches pgwire's
// maximum frontend-message body and is well above ordinary prepared SQL.
const maxStatementBytes = 16 << 20

// A Parser parses SQL statements with reusable storage. Its zero value is ready
// to use. It is single-consumer and not safe for concurrent use.
//
// A statement parsed into dst borrows the Parser's storage and is valid only
// until that Parser's next Parse. This is the same borrowed-lifetime rule
// query's prepared-statement compiler, Result, and Workspace already carry,
// for the same reason:
// the storage that makes the steady state allocation-free is the storage the
// previous answer is still pointing at. A caller that keeps several statements
// alive at once gives each its own Parser, or uses the package-level [Parse],
// which owns everything it returns.
//
// A parsed statement never points into the source text. Every identifier, key,
// literal, and containment needle is copied into the Parser's own storage, so
// the caller may reuse or free src as soon as Parse returns. The copy is also
// what stops one three-byte field name from pinning a ten-kilobyte statement
// for the life of a prepared plan.
//
// A first parse cannot be literally allocation-free — the tree has to come from
// somewhere. The contract is zero steady-state allocation: after one warm-up
// parse of comparable shape, every later parse refills the same chunks.
type Parser struct {
	lx    lexer
	tok   token
	depth int
	out   *SelectStmt
	// cancel is an optional cold-path hook. A nil hook leaves the warmed parser
	// allocation profile unchanged; a non-nil hook is observed at bounded byte
	// and token intervals while attacker-sized input is scanned.
	cancel func() error

	// Arenas whose storage a parsed statement retains.
	text  chunkArena[byte]     // interned identifiers, keys, and literals
	exprs chunkArena[Expr]     // predicate nodes
	kids  chunkArena[*Expr]    // predicate child lists
	ops   chunkArena[Operand]  // IN alternatives and BETWEEN bounds
	paths chunkArena[PathExpr] // path nodes
	segs  chunkArena[Segment]  // path segment runs
	conds chunkArena[JoinCond] // join conditions

	// Reused whole-clause slices. Unlike the arenas these hold no interior
	// pointers into themselves, so a plain reslice-to-zero is enough.
	columns  []ResultColumn
	from     []TableRef
	groupBy  []*PathExpr
	orderBy  []OrderTerm
	rows     []InsertRow
	cols     []ColumnDef
	keyPaths []*PathExpr
	idxPaths []*PathExpr

	// The statement bodies [Parser.ParseStatement] hands back by pointer. They
	// are fields rather than arena allocations because there is exactly one of
	// each per parse, so an arena would buy nothing and would make the lifetime
	// rule harder to state than "valid until the next parse" — which is the
	// rule every other thing a Parser returns already carries. sel doubles as
	// the synthetic SELECT an UPDATE or a DELETE selects its rows with; see
	// parse_dml.go.
	sel       SelectStmt
	returning SelectStmt
	ins       InsertStmt
	upd       UpdateStmt
	del       DeleteStmt
	tbl       CreateTableStmt
	idx       CreateIndexStmt
	drop      DropTableStmt
	truncate  TruncateStmt
	dropIndex DropIndexStmt

	// DML filters and RETURNING projections reuse the clause buffers below.
	// These retained copies keep a filter's slice headers valid while a
	// mutation's RETURNING projection is parsed.
	filterColumns   []ResultColumn
	filterFrom      []TableRef
	filterGroupBy   []*PathExpr
	filterOrderBy   []OrderTerm
	mutationOrderBy []OrderTerm
	mutationLimit   *Operand

	// Parse-time scratch that no parsed statement retains.
	//
	// segScratch collects a path's segments before their count is known, so
	// the arena run can be reserved exactly once. It needs no stack discipline
	// because paths do not nest.
	//
	// kidStack does need it: parseOr and parseAnd recurse through each other,
	// so an inner conjunction would overwrite the outer disjunction's operands
	// if they shared one flat buffer. Operands are pushed above a base marker
	// and cut back to it once the node is built.
	segScratch []Segment
	kidStack   []*Expr
	opScratch  []Operand
	pending    []pendingPath
	tmp        []byte
	// nested is allocated only when the statement contains a subquery, keeping
	// ordinary Parser values at one pointer of added state rather than a slice.
	nested *nestedParsers
	// existsProjection allows SELECT 1 (and equivalent literals) only while
	// parsing an EXISTS body. EXISTS never observes its output value, so
	// lowering the literal to a whole-document projection avoids inventing a
	// constant-expression executor for a value that is discarded.
	existsProjection bool
	nesting          int

	params int
	sized  bool
}

// A pendingPath is a path whose leading identifier has been read but not yet
// decided to be a range variable or a field.
//
// The decision cannot be made where the path is parsed, because the SELECT list
// precedes FROM and a JOIN may declare a range variable later still. Recording
// the two facts resolution needs — whether the head was followed by a '.', and
// whether the path ended in '*' — and settling every path in one pass at the
// end is what keeps the rule uniform: a path in SELECT resolves by exactly the
// same rule as a path in HAVING, rather than by whatever was known at the time.
type pendingPath struct {
	path     *PathExpr
	eligible bool // the head identifier was immediately followed by '.'
	star     bool // the path ended in '*' and names the whole document
}

// Parse parses one SELECT statement into dst, reusing p's storage. See
// [Parser] for the lifetime dst inherits.
func (p *Parser) Parse(dst *SelectStmt, src string) error {
	*dst = SelectStmt{}
	if err := validateStatementText(src, p.cancel); err != nil {
		return err
	}
	p.reset(src)
	p.out = dst
	if err := p.parseStatement(); err != nil {
		// The half-parsed statement is thrown away rather than returned
		// alongside the error, so a caller that ignores the error and lowers
		// dst anyway gets an empty statement it must reject rather than
		// whichever clauses happened to parse before the failure.
		*dst = SelectStmt{}
		if p.lx.cancelErr != nil {
			return p.lx.cancelErr
		}
		return err
	}
	if p.lx.cancelErr != nil {
		*dst = SelectStmt{}
		return p.lx.cancelErr
	}
	return nil
}

// SetCancellationCheck installs an optional cooperative cancellation hook.
// The parser calls check at bounded byte and token intervals and returns its
// error unchanged. Passing nil restores the allocation-free ordinary path.
// The hook remains installed across Parse calls and is cleared by Release.
func (p *Parser) SetCancellationCheck(check func() error) {
	if p != nil {
		p.cancel = check
	}
}

// validateStatementText is the one admission check shared by the SELECT-only
// and all-statement parser entry points. UTF-8 validity belongs here rather
// than in adapters: identifiers and string literals are copied into the AST,
// and a public parser must never manufacture an invalid Go string merely
// because its caller did not come through pgwire or database/sql.
//
// utf8.ValidString is allocation-free and has a fast ASCII path. The second
// walk happens only for rejected input, to put the ParseError on the first
// malformed byte.
func validateStatementText(src string, check func() error) error {
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	if len(src) > maxStatementBytes {
		return newParseError(
			src, maxStatementBytes,
			"statement exceeds the 16 MiB SQL input limit",
		)
	}
	if check == nil {
		if utf8.ValidString(src) {
			return nil
		}
		return newParseError(src, firstInvalidUTF8(src),
			"statement is not valid UTF-8")
	}
	nextCheck := parserCancelByteInterval
	for pos := 0; pos < len(src); {
		if pos >= nextCheck {
			if err := check(); err != nil {
				return err
			}
			nextCheck = pos + parserCancelByteInterval
		}
		if src[pos] < utf8.RuneSelf {
			pos++
			continue
		}
		_, size := utf8.DecodeRuneInString(src[pos:])
		if size == 1 {
			return newParseError(src, pos, "statement is not valid UTF-8")
		}
		pos += size
	}
	return nil
}

func firstInvalidUTF8(src string) int {
	for pos := 0; pos < len(src); {
		_, size := utf8.DecodeRuneInString(src[pos:])
		if size == 1 && src[pos] >= utf8.RuneSelf {
			return pos
		}
		pos += size
	}
	return len(src)
}

// Parse parses one SELECT statement. The returned statement owns its storage
// outright and carries no lifetime caveat; a caller parsing in a loop should
// hold a [Parser] instead, whose arenas a warmed parse refills rather than
// reallocates.
func Parse(src string) (*SelectStmt, error) {
	var p Parser
	dst := new(SelectStmt)
	if err := p.Parse(dst, src); err != nil {
		return nil, err
	}
	return dst, nil
}

// reset returns every arena and scratch buffer to empty while keeping the
// storage behind it, and loads the first token.
func (p *Parser) reset(src string) {
	if !p.sized {
		// Sized once for a statement of ordinary shape, so the common case
		// fills one chunk per arena rather than three.
		p.text.first = 256
		p.exprs.first = 8
		p.kids.first = 8
		p.ops.first = 8
		p.paths.first = 8
		p.segs.first = 16
		p.conds.first = 4
		p.sized = true
	}
	p.text.rewind()
	p.exprs.rewind()
	p.kids.rewind()
	p.ops.rewind()
	p.paths.rewind()
	p.segs.rewind()
	p.conds.rewind()

	p.columns = p.columns[:0]
	p.from = p.from[:0]
	p.groupBy = p.groupBy[:0]
	p.orderBy = p.orderBy[:0]
	p.rows = p.rows[:0]
	p.cols = p.cols[:0]
	p.keyPaths = p.keyPaths[:0]
	p.idxPaths = p.idxPaths[:0]
	p.segScratch = p.segScratch[:0]
	p.kidStack = p.kidStack[:0]
	p.opScratch = p.opScratch[:0]
	p.pending = p.pending[:0]
	p.returning = SelectStmt{}
	p.filterColumns = p.filterColumns[:0]
	p.filterFrom = p.filterFrom[:0]
	p.filterGroupBy = p.filterGroupBy[:0]
	p.filterOrderBy = p.filterOrderBy[:0]
	p.mutationOrderBy = p.mutationOrderBy[:0]
	p.mutationLimit = nil
	if p.nested != nil {
		p.nested.used = 0
	}

	p.lx = lexer{
		src: src, cancel: p.cancel,
		nextCancelByte: parserCancelByteInterval,
	}
	p.depth = 0
	p.params = 0
	p.advance()
}

type nestedParsers struct {
	parsers []*Parser
	used    int
}

// Release drops every chunk p retains, returning it to its zero state and
// invalidating every statement parsed with it. Reusing a warm Parser is the
// point of having one, so Release is for after one unusually large statement
// whose arenas should not stay pinned for the rest of p's lifetime.
func (p *Parser) Release() {
	if p == nil {
		return
	}
	*p = Parser{}
}

func (p *Parser) advance() { p.tok = p.lx.next() }

func (p *Parser) checkCancellation() error {
	if p.lx.cancelErr != nil {
		return p.lx.cancelErr
	}
	if p.cancel == nil {
		return nil
	}
	if err := p.cancel(); err != nil {
		p.lx.cancelErr = err
		return err
	}
	return nil
}

// atKeyword reports whether the current token is the unquoted keyword kw. A
// quoted identifier never matches, which is what makes `"select"` usable as a
// field name.
func (p *Parser) atKeyword(kw keyword) bool {
	return p.tok.kind == tokIdent && p.tok.kw == kw
}

func (p *Parser) acceptKeyword(kw keyword) bool {
	if p.atKeyword(kw) {
		p.advance()
		return true
	}
	return false
}

func (p *Parser) expectKeyword(kw keyword, what string) error {
	if !p.acceptKeyword(kw) {
		return p.errfHere("expected %s", what)
	}
	return nil
}

func (p *Parser) expect(kind tokenKind, what string) error {
	if p.tok.kind != kind {
		return p.errfHere("expected %s", what)
	}
	p.advance()
	return nil
}

// --- interning -------------------------------------------------------------

// intern copies b into the text arena and returns it as a string. The copy is
// what lets a statement retain a name read out of the caller's source text or
// out of the shared decoding scratch: both may be gone or overwritten by the
// time the statement is lowered.
func (p *Parser) intern(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	dst := p.text.allocDirty(len(b))
	all := dst
	if p.cancel == nil {
		copy(dst, b)
	} else {
		for len(b) > 0 {
			if p.checkCancellation() != nil {
				break
			}
			chunk := min(len(b), parserCancelByteInterval)
			copy(dst, b[:chunk])
			dst, b = dst[chunk:], b[chunk:]
		}
	}
	return byteview.String(all)
}

func (p *Parser) internString(s string) string {
	if len(s) == 0 {
		return ""
	}
	dst := p.text.allocDirty(len(s))
	all := dst
	if p.cancel == nil {
		copy(dst, s)
	} else {
		for len(s) > 0 {
			if p.checkCancellation() != nil {
				break
			}
			chunk := min(len(s), parserCancelByteInterval)
			copy(dst, s[:chunk])
			dst, s = dst[chunk:], s[chunk:]
		}
	}
	return byteview.String(all)
}

// internToken interns a token's text, collapsing the doubled quotes of a
// quoted token on the way. Only a token the lexer flagged is decoded, so the
// overwhelmingly common literal without an embedded quote is a straight copy.
func (p *Parser) internToken(t token) string {
	if !t.esc {
		return p.internString(t.text)
	}
	quote := byte('\'')
	if t.kind == tokQuotedIdent {
		quote = '"'
	}
	buf := p.tmp[:0]
	for i := 0; i < len(t.text); i++ {
		if p.cancel != nil && i%parserCancelByteInterval == 0 &&
			p.checkCancellation() != nil {
			break
		}
		buf = append(buf, t.text[i])
		if t.text[i] == quote {
			i++ // skip the second half of the doubled quote
		}
	}
	p.tmp = buf
	return p.intern(buf)
}

// --- statement -------------------------------------------------------------

func (p *Parser) parseStatement() error {
	if err := p.expectSelect(); err != nil {
		return err
	}
	p.out.Distinct = p.acceptKeyword(kwDistinct)
	if p.out.Distinct && p.atKeyword(kwAll) {
		return p.errHere("SELECT DISTINCT and SELECT ALL are mutually exclusive")
	}
	if !p.out.Distinct {
		p.acceptKeyword(kwAll) // SELECT ALL is the default, so it is a no-op
	}
	if err := p.parseResultColumns(); err != nil {
		return err
	}
	if err := p.expectKeyword(kwFrom, "FROM"); err != nil {
		return err
	}
	if err := p.parseFrom(); err != nil {
		return err
	}
	if p.acceptKeyword(kwWhere) {
		where, err := p.parseExpr(ctxWhere)
		if err != nil {
			return err
		}
		p.out.Where = where
	}
	if err := p.parseGroupBy(); err != nil {
		return err
	}
	if p.out.Distinct {
		if len(p.out.GroupBy) != 0 {
			return p.errHere("SELECT DISTINCT with GROUP BY is not supported when grouping changes the projected tuple")
		}
		hasAggregate := false
		for i := range p.out.Columns {
			if p.out.Columns[i].Agg != AggNone {
				hasAggregate = true
				break
			}
		}
		if !hasAggregate {
			p.groupBy = p.groupBy[:0]
			for i := range p.out.Columns {
				if p.out.Columns[i].Path != nil {
					p.groupBy = append(p.groupBy, p.out.Columns[i].Path)
				}
			}
			p.out.GroupBy = p.groupBy
		}
	}
	if p.acceptKeyword(kwHaving) {
		having, err := p.parseExpr(ctxHaving)
		if err != nil {
			return err
		}
		p.out.Having = having
	}
	if err := p.parseOrderBy(); err != nil {
		return err
	}
	if err := p.parseLimitOffset(); err != nil {
		return err
	}
	if err := p.expectEnd(); err != nil {
		return err
	}
	if err := p.resolvePaths(); err != nil {
		return err
	}
	if err := p.normalizeRightJoins(); err != nil {
		return err
	}
	p.out.Params = p.params
	return p.validate()
}

// expectSelect consumes the leading SELECT, naming the specific unsupported
// statement kind when the input begins with one. A driver's user typing an
// UPDATE deserves "only SELECT statements are supported", not "expected
// SELECT" — the second reads as a syntax error in a statement that has none.
func (p *Parser) expectSelect() error {
	switch {
	case p.acceptKeyword(kwSelect):
		return nil
	case p.atKeyword(kwInsert), p.atKeyword(kwUpdate), p.atKeyword(kwDelete),
		p.atKeyword(kwCreate), p.atKeyword(kwDrop), p.atKeyword(kwTruncate):
		// Parse is the SELECT-only entry point, kept because a caller that only
		// wants a query should not have to switch on a kind to find out it got
		// one. The statement is not unsupported, so the message names the entry
		// point that takes it rather than the capability the engine lacks.
		return p.errHere("this entry point parses SELECT; mutations and catalog statements are parsed by ParseStatement")
	}
	if reason, unsupported := unsupportedStatementReason(p.tok); unsupported {
		return p.featureNotSupportedHere(reason)
	}
	return p.errHere("expected SELECT")
}

// expectEnd accepts one optional trailing semicolon and then requires the end
// of input. A second statement is rejected by name because a driver that
// forwards a multi-statement string is a well-known injection shape, and
// silently running only the first half of one would be worse than refusing it.
func (p *Parser) expectEnd() error {
	switch {
	case p.atKeyword(kwUnion), p.atKeyword(kwIntersect), p.atKeyword(kwExcept):
		return p.errHere("set operations (UNION, INTERSECT, EXCEPT) are not supported")
	case p.atKeyword(kwWindow):
		return p.errHere("window functions are not supported")
	}
	if p.tok.kind == tokSemicolon {
		p.advance()
		if p.tok.kind != tokEOF {
			return p.errHere("only one statement may be parsed at a time")
		}
	}
	if p.tok.kind != tokEOF {
		return p.errHere("unexpected trailing input")
	}
	return nil
}

// --- SELECT list -----------------------------------------------------------

func (p *Parser) parseResultColumns() error {
	for {
		col, err := p.parseResultColumn()
		if err != nil {
			return err
		}
		p.columns = append(p.columns, col)
		if len(p.columns) > maxClauseItems {
			return p.errfAt(col.Pos, "a SELECT list may hold at most %d columns", maxClauseItems)
		}
		if p.tok.kind != tokComma {
			p.out.Columns = p.columns
			return nil
		}
		p.advance()
	}
}

// aggState reports what tryAggregate consumed; see there.
type aggState uint8

const (
	aggNothing  aggState = iota // nothing consumed
	aggHeadOnly                 // an aggregate-spelled identifier was consumed and is a path head
	aggCall                     // an aggregate call; the current token is '('
)

// tryAggregate decides whether the current token opens an aggregate call.
//
// The decision needs one token of lookahead the parser does not otherwise
// keep, so the identifier is consumed and handed back when it turns out not to
// be a call. The lookahead is what lets a document field named "count" or "sum"
// still be projected: the keyword is an aggregate only when a '(' follows it,
// and a schemaless store has no schema to forbid such a field.
func (p *Parser) tryAggregate() (AggKind, token, aggState) {
	if p.tok.kind != tokIdent {
		return AggNone, token{}, aggNothing
	}
	agg, ok := aggregateOf(p.tok.kw)
	if !ok {
		return AggNone, token{}, aggNothing
	}
	head := p.tok
	p.advance()
	if p.tok.kind == tokLParen {
		return agg, head, aggCall
	}
	return AggNone, head, aggHeadOnly
}

func (p *Parser) parseResultColumn() (ResultColumn, error) {
	col := ResultColumn{Pos: p.tok.pos}
	if p.existsProjection && (p.tok.kind == tokNumber ||
		p.tok.kind == tokString ||
		p.atKeyword(kwTrue) || p.atKeyword(kwFalse) || p.atKeyword(kwNull)) {
		p.advance()
		col.Path = p.newPath(col.Pos, nil, false, true)
		alias, err := p.parseColumnAlias()
		if err != nil {
			return col, err
		}
		col.Alias = alias
		return col, nil
	}
	switch agg, head, state := p.tryAggregate(); state {
	case aggCall:
		path, err := p.parseAggregateArgs(agg)
		if err != nil {
			return col, err
		}
		col.Agg, col.Path = agg, path
	case aggHeadOnly:
		path, err := p.continuePath(head, true)
		if err != nil {
			return col, err
		}
		col.Path = path
	default:
		path, err := p.parseSelectPath()
		if err != nil {
			return col, err
		}
		col.Path = path
	}
	alias, err := p.parseColumnAlias()
	if err != nil {
		return col, err
	}
	col.Alias = alias
	return col, nil
}

// parseSelectPath parses a SELECT-list path, including the bare '*' that names
// the whole document.
func (p *Parser) parseSelectPath() (*PathExpr, error) {
	if p.tok.kind == tokStar {
		pos := p.tok.pos
		p.advance()
		return p.newPath(pos, nil, false, true), nil
	}
	return p.parsePath(true)
}

// parseAggregateArgs parses an aggregate's argument list. The current token is
// '('.
func (p *Parser) parseAggregateArgs(agg AggKind) (*PathExpr, error) {
	p.advance() // '('
	if p.atKeyword(kwDistinct) {
		return nil, p.errfHere("%s(DISTINCT ...) is not supported: the engine's reductions have no distinct variant", agg)
	}
	if agg == AggCount && p.tok.kind == tokStar {
		p.advance()
		if err := p.expect(tokRParen, "')'"); err != nil {
			return nil, err
		}
		return nil, nil // COUNT(*) reads no path
	}
	path, err := p.parsePath(false)
	if err != nil {
		return nil, err
	}
	if p.tok.kind == tokComma {
		return nil, p.errfHere("%s takes exactly one path", agg)
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return nil, err
	}
	return path, nil
}

// parseColumnAlias parses an optional AS name.
//
// The AS is required. SQL allows a bare identifier after a column to be its
// output name, but here that would silently rescue the far more common typo —
// a missing comma between two projected paths — as a rename, and a schemaless
// engine has no schema check downstream to catch it.
func (p *Parser) parseColumnAlias() (string, error) {
	if p.atKeyword(kwOver) {
		return "", p.errHere("window functions (OVER) are not supported: the engine reduces groups and has no frame")
	}
	if p.acceptKeyword(kwAs) {
		return p.parseAliasName("an output name after AS")
	}
	if p.tok.kind == tokQuotedIdent || (p.tok.kind == tokIdent && p.tok.kw == kwNone) {
		return "", p.errfHere("unexpected identifier %q after a column: an output name requires AS, and two paths need a comma between them", p.tok.text)
	}
	return "", nil
}

// parseAliasName reads an alias identifier, quoted or not. A keyword is
// accepted here because AS leaves no doubt about what follows it, so `AS
// "order"` and `AS order` may both name the output column "order".
func (p *Parser) parseAliasName(what string) (string, error) {
	switch p.tok.kind {
	case tokIdent, tokQuotedIdent:
		if p.tok.text == "" {
			return "", p.errHere("an alias may not be empty")
		}
		name := p.internToken(p.tok)
		p.advance()
		return name, nil
	}
	return "", p.errfHere("expected %s", what)
}

// --- FROM and JOIN ---------------------------------------------------------

func (p *Parser) parseFrom() error {
	ref, err := p.parseTableRef(JoinNone)
	if err != nil {
		return err
	}
	p.from = append(p.from, ref)
	p.out.From = p.from
	for {
		done, err := p.parseJoin()
		if err != nil {
			return err
		}
		if done {
			p.out.From = p.from
			return nil
		}
	}
}

// parseJoin parses one JOIN clause, reporting true when the next token does not
// open one.
func (p *Parser) parseJoin() (bool, error) {
	if len(p.from) > 0 && p.from[0].Kind == RelationDerived {
		switch p.tok.kw {
		case kwJoin, kwInner, kwLeft, kwRight, kwFull, kwOuter, kwCross, kwNatural:
			return false, newFeatureNotSupportedError(
				p.lx.src,
				p.tok.pos,
				"joining a derived table is not supported yet: use the derived table as the sole FROM relation",
			)
		}
	}
	join := JoinInner
	switch {
	case p.acceptKeyword(kwLeft):
		join = JoinLeft
		p.acceptKeyword(kwOuter)
	case p.acceptKeyword(kwRight):
		join = JoinRight
		p.acceptKeyword(kwOuter)
	case p.atKeyword(kwFull), p.atKeyword(kwOuter):
		return false, p.errHere("FULL outer joins are not supported; RIGHT JOIN is supported and normalized to LEFT JOIN")
	case p.atKeyword(kwCross):
		return false, p.errHere("CROSS JOIN is not supported: the engine has no unrestricted product")
	case p.atKeyword(kwNatural):
		return false, p.errHere("NATURAL JOIN is not supported: schemaless documents have no declared columns to match by name; write ON explicitly")
	}
	if p.tok.kind == tokComma {
		return false, p.errHere("comma-separated FROM items are not supported; write an explicit JOIN ... ON")
	}
	if join == JoinInner {
		p.acceptKeyword(kwInner)
	}
	if !p.acceptKeyword(kwJoin) {
		if join == JoinLeft {
			return false, p.errHere("expected JOIN after LEFT")
		}
		return true, nil
	}
	ref, err := p.parseTableRef(join)
	if err != nil {
		return false, err
	}
	var cond *JoinCond
	if p.acceptKeyword(kwUsing) {
		cond, err = p.parseUsingCond(len(p.from))
	} else {
		if err := p.expectKeyword(kwOn, "ON after a JOIN"); err != nil {
			return false, err
		}
		cond, err = p.parseJoinCond()
	}
	if err != nil {
		return false, err
	}
	ref.On = cond
	p.from = append(p.from, ref)
	p.out.From = p.from
	if len(p.from) > maxClauseItems {
		return false, p.errfAt(ref.Pos, "a statement may join at most %d collections", maxClauseItems)
	}
	return false, nil
}

// parseUsingCond lowers the common one-column USING form to the explicit
// equality the executor understands and marks the condition so deferred name
// resolution can expose USING's merged unqualified key. A USING name belongs
// to the relation immediately on the left and to the relation this JOIN adds.
// The current executor only accepts the driving relation as the left side, so
// a chained USING join is rejected during normal lowering with the same
// precise chained-join diagnostic as an ON join.
func (p *Parser) parseUsingCond(joinSource int) (*JoinCond, error) {
	pos := p.tok.pos
	if err := p.expect(tokLParen, "'(' after USING"); err != nil {
		return nil, err
	}
	path, err := p.parsePath(false)
	if err != nil {
		return nil, err
	}
	// parsePath records ordinary paths for the statement-wide resolver. USING
	// has already supplied both range variables, so remove that pending entry
	// and clone the path into two explicitly bound operands.
	if n := len(p.pending); n != 0 && p.pending[n-1].path == path {
		p.pending = p.pending[:n-1]
	}
	if len(path.Segments) != 1 || path.Segments[0].IsIndex {
		return nil, p.errAt(path.Pos, "USING accepts one simple field name; use ON for nested or composite keys")
	}
	if p.tok.kind == tokComma {
		return nil, p.errHere("composite USING joins are not supported yet; write one equality in ON")
	}
	if err := p.expect(tokRParen, "')' after USING"); err != nil {
		return nil, err
	}
	left := p.boundPath(path, joinSource-1)
	right := p.boundPath(path, joinSource)
	cond := p.conds.one()
	*cond = JoinCond{Left: left, Right: right, Using: true, Pos: pos}
	return cond, nil
}

// boundPath clones a parser-owned path without registering it for deferred
// name resolution. This is used only by USING, whose unqualified name is
// intentionally bound to both sides of the join.
func (p *Parser) boundPath(path *PathExpr, source int) *PathExpr {
	clone := p.paths.one()
	segments := p.segs.allocDirty(len(path.Segments))
	copy(segments, path.Segments)
	*clone = PathExpr{Source: source, Segments: segments, Pos: path.Pos}
	return clone
}

func (p *Parser) parseTableRef(join JoinKind) (TableRef, error) {
	ref := TableRef{Join: join, Pos: p.tok.pos}
	if p.atKeyword(kwLateral) {
		return ref, newFeatureNotSupportedError(
			p.lx.src,
			p.tok.pos,
			"LATERAL derived tables are not supported: derived FROM queries are uncorrelated and execute once",
		)
	}
	switch p.tok.kind {
	case tokIdent:
		if err := p.checkNameable("a collection name"); err != nil {
			return ref, err
		}
	case tokQuotedIdent:
	case tokLParen:
		return p.parseDerivedTableRef(ref)
	default:
		return ref, p.errHere("expected a collection name")
	}
	if p.tok.text == "" {
		return ref, p.errHere("a collection name may not be empty")
	}
	ref.Name = p.internToken(p.tok)
	p.advance()
	if p.tok.kind == tokDot {
		return ref, p.errHere("qualified collection names (schema.table) are not supported: a dotted name here would be indistinguishable from a range variable and a field")
	}
	alias, err := p.parseTableAlias()
	if err != nil {
		return ref, err
	}
	// The alias defaults to the collection name, so resolution compares
	// against one field rather than two and can never disagree about which of
	// them a qualified path meant.
	ref.Alias, ref.HasAlias = ref.Name, alias != ""
	if ref.HasAlias {
		ref.Alias = alias
	}
	return ref, p.rejectDuplicateRangeAlias(ref)
}

// parseDerivedTableRef parses the parenthesized SELECT of a derived relation.
// The nested statement uses the same child-parser path as predicate subqueries,
// which preserves independent FROM scope, arena ownership, nesting limits,
// cancellation, position rebasing, and placeholder accounting in one place.
func (p *Parser) parseDerivedTableRef(ref TableRef) (TableRef, error) {
	if ref.Join != JoinNone {
		return ref, newFeatureNotSupportedError(
			p.lx.src,
			ref.Pos,
			"a derived table is not supported in a JOIN position yet: use it as the sole FROM relation",
		)
	}
	p.advance() // consume '('
	if !p.atKeyword(kwSelect) {
		return ref, p.errHere("expected SELECT after '(' in a derived table")
	}
	query, err := p.parseSubquery(false)
	if err != nil {
		return ref, err
	}
	alias, err := p.parseTableAlias()
	if err != nil {
		return ref, err
	}
	if alias == "" {
		return ref, p.errHere("a derived table requires a non-empty alias, written AS alias or directly after ')'")
	}
	ref.Kind = RelationDerived
	ref.Query = query
	ref.Alias = alias
	ref.HasAlias = true
	return ref, p.rejectDuplicateRangeAlias(ref)
}

func (p *Parser) rejectDuplicateRangeAlias(ref TableRef) error {
	for i := range p.from {
		if p.from[i].Alias == ref.Alias {
			return p.errfAt(ref.Pos, "range variable %q is declared twice; give one of them a distinct AS alias", ref.Alias)
		}
	}
	return nil
}

// parseTableAlias parses an optional table alias, with or without AS. Unlike a
// column alias the bare form is accepted, because `FROM users u` is the shape
// every join is written in and there is no adjacent path for it to be confused
// with. A bare alias may not be a keyword, so the clause keyword that follows a
// table reference is never eaten as its name.
func (p *Parser) parseTableAlias() (string, error) {
	if p.acceptKeyword(kwAs) {
		return p.parseAliasName("an alias after AS")
	}
	if p.tok.kind == tokQuotedIdent || (p.tok.kind == tokIdent && !reserved(p.tok.kw)) {
		return p.parseAliasName("an alias")
	}
	return "", nil
}

// parseJoinCond parses an ON condition: exactly one key equality, optionally
// parenthesized.
//
// It is deliberately not the general predicate grammar. The engine joins by
// matching one key on each side; a parser that accepted `ON a.x = b.y AND a.z =
// b.w` or `ON a.x > b.y` would produce a tree that reads as valid and has no
// executor, and the failure would surface at lowering with no position to point
// at.
func (p *Parser) parseJoinCond() (*JoinCond, error) {
	pos := p.tok.pos
	parens := p.tok.kind == tokLParen
	if parens {
		p.advance()
	}
	left, err := p.parsePath(false)
	if err != nil {
		return nil, err
	}
	if p.tok.kind != tokEq {
		return nil, p.errHere("a join condition must be a single key equality, written ON left.key = right.key")
	}
	p.advance()
	right, err := p.parsePath(false)
	if err != nil {
		return nil, err
	}
	if parens {
		if err := p.expect(tokRParen, "')'"); err != nil {
			return nil, err
		}
	}
	if p.atKeyword(kwAnd) || p.atKeyword(kwOr) {
		return nil, p.errHere("a join condition must be a single key equality: the engine matches one key per join")
	}
	cond := p.conds.one()
	*cond = JoinCond{Left: left, Right: right, Pos: pos}
	return cond, nil
}

// --- GROUP BY, ORDER BY, LIMIT ---------------------------------------------

func (p *Parser) parseGroupBy() error {
	if !p.acceptKeyword(kwGroup) {
		return nil
	}
	if err := p.expectKeyword(kwBy, "BY after GROUP"); err != nil {
		return err
	}
	for {
		if p.tok.kind == tokNumber {
			return p.errHere("GROUP BY does not accept an output position; name the path")
		}
		path, err := p.parseKeyPath(
			"GROUP BY cannot group by an aggregate: an aggregate is computed per group, not per row")
		if err != nil {
			return err
		}
		p.groupBy = append(p.groupBy, path)
		if len(p.groupBy) > maxClauseItems {
			return p.errfAt(path.Pos, "GROUP BY may hold at most %d keys", maxClauseItems)
		}
		if p.tok.kind != tokComma {
			p.out.GroupBy = p.groupBy
			return nil
		}
		p.advance()
	}
}

func (p *Parser) parseOrderBy() error {
	if !p.acceptKeyword(kwOrder) {
		return nil
	}
	if err := p.expectKeyword(kwBy, "BY after ORDER"); err != nil {
		return err
	}
	for {
		term, err := p.parseOrderTerm()
		if err != nil {
			return err
		}
		p.orderBy = append(p.orderBy, term)
		if len(p.orderBy) > maxClauseItems {
			return p.errfAt(term.Pos, "ORDER BY may hold at most %d keys", maxClauseItems)
		}
		if p.tok.kind != tokComma {
			p.out.OrderBy = p.orderBy
			return nil
		}
		p.advance()
	}
}

func (p *Parser) parseOrderTerm() (OrderTerm, error) {
	term := OrderTerm{Pos: p.tok.pos}
	if p.tok.kind == tokNumber {
		return term, p.errHere("ORDER BY does not accept an output position; name the path or the output alias")
	}
	path, err := p.parseKeyPath(
		"ORDER BY cannot sort by an aggregate: the engine orders groups by their key, not by their reduction")
	if err != nil {
		return term, err
	}
	term.Path = p.resolveOrderAlias(path)
	switch {
	case p.acceptKeyword(kwAsc):
	case p.acceptKeyword(kwDesc):
		term.Desc = true
	}
	switch {
	case p.atKeyword(kwNulls):
		return term, p.errHere("NULLS FIRST/LAST is not supported: the engine sorts nulls first ascending and last descending")
	case p.atKeyword(kwCollate):
		return term, p.errHere("COLLATE is not supported: strings compare by decoded content")
	}
	return term, nil
}

// resolveOrderAlias replaces a sort key that names an output alias with the
// path that alias projects.
//
// SQL resolves an ORDER BY name against the select list before the table, and
// this follows that rule so `SELECT team AS t ... ORDER BY t` works. The
// replacement shares the projection's own *PathExpr rather than copying it, so
// the two resolve as one and can never disagree about which range variable they
// belong to. The path just parsed is dropped from the pending list — it is the
// last entry, since nothing else has been parsed since — so resolution does not
// later try to bind an identifier that turned out to be an alias.
func (p *Parser) resolveOrderAlias(path *PathExpr) *PathExpr {
	last := len(p.pending) - 1
	if last < 0 || p.pending[last].path != path {
		return path
	}
	if p.pending[last].eligible || len(path.Segments) != 1 {
		return path
	}
	name := path.Segments[0].Key
	for i := range p.columns {
		if p.columns[i].Alias != name || p.columns[i].Path == nil {
			continue
		}
		p.pending = p.pending[:last]
		return p.columns[i].Path
	}
	return path
}

func (p *Parser) parseLimitOffset() error {
	for i := 0; i < 2; i++ {
		switch {
		case p.atKeyword(kwLimit):
			if p.out.Limit != nil {
				return p.errHere("LIMIT is given twice")
			}
			p.advance()
			op, err := p.parseRowCount("LIMIT")
			if err != nil {
				return err
			}
			p.out.Limit = op
		case p.atKeyword(kwOffset):
			if p.out.Offset != nil {
				return p.errHere("OFFSET is given twice")
			}
			p.advance()
			op, err := p.parseRowCount("OFFSET")
			if err != nil {
				return err
			}
			p.out.Offset = op
		case p.atKeyword(kwFetch):
			return p.errHere("FETCH FIRST is not supported; write LIMIT")
		default:
			return nil
		}
	}
	return nil
}

// parseRowCount parses a LIMIT or OFFSET count: a non-negative integer literal
// or a placeholder.
func (p *Parser) parseRowCount(clause string) (*Operand, error) {
	if p.tok.kind == tokParam {
		return p.parseOperandInto()
	}
	if p.tok.kind != tokNumber {
		return nil, p.errfHere("expected a non-negative integer or '?' after %s", clause)
	}
	text := p.tok.text
	if _, err := strconv.ParseInt(text, 10, 64); err != nil {
		return nil, p.errfHere("%s must be a non-negative whole number that fits in 64 bits", clause)
	}
	if text[0] == '-' {
		return nil, p.errfHere("%s must not be negative", clause)
	}
	op := p.ops.one()
	*op = Operand{Kind: OperandNumber, Text: p.internString(text), Pos: p.tok.pos}
	p.advance()
	return op, nil
}

// parseOperandInto reads one operand into arena storage, for the clauses that
// hold a single operand by pointer.
func (p *Parser) parseOperandInto() (*Operand, error) {
	value, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	op := p.ops.one()
	*op = value
	return op, nil
}

// --- paths -----------------------------------------------------------------

// parseKeyPath parses a GROUP BY or ORDER BY key, rejecting an aggregate call
// with reason. The rejection is here rather than at validation because the
// aggregate's '(' is the byte to point at, and by validation time the position
// is gone.
//
// An aggregate keyword that is not a call is an ordinary field name — a
// document may well have a key called "count" — so it continues as a path from
// the identifier tryAggregate already consumed.
func (p *Parser) parseKeyPath(reason string) (*PathExpr, error) {
	switch _, head, state := p.tryAggregate(); state {
	case aggCall:
		return nil, p.errHere(reason)
	case aggHeadOnly:
		return p.continuePath(head, false)
	}
	return p.parsePath(false)
}

// parsePath parses a path beginning at the current token. allowStar admits the
// 'alias.*' form, which only the SELECT list accepts.
func (p *Parser) parsePath(allowStar bool) (*PathExpr, error) {
	switch p.tok.kind {
	case tokIdent:
		if err := p.checkNameable("a field path"); err != nil {
			return nil, err
		}
	case tokQuotedIdent:
	case tokParam:
		return nil, p.errHere("a placeholder cannot name a path: the engine compiles paths into the plan, so they must be known when the statement is prepared")
	case tokStar:
		return nil, p.errHere("'*' must stand alone in the SELECT list or be qualified as alias.*")
	default:
		return nil, p.errHere("expected a field path")
	}
	head := p.tok
	p.advance()
	if !head.esc && head.text == DocumentColumn {
		return nil, p.errfAt(
			head.pos,
			"%q names the whole replacement document only as UPDATE's SET target; "+
				"INSERT one complete document with VALUES (document), include its declared primary-key field, and use ordinary JSON field names everywhere else",
			DocumentColumn,
		)
	}
	return p.continuePath(head, allowStar)
}

// continuePath parses the '.'-segment and '[...]'-subscript tail of a path
// whose head identifier is already consumed.
func (p *Parser) continuePath(head token, allowStar bool) (*PathExpr, error) {
	segs := append(p.segScratch[:0], Segment{Key: p.internToken(head)})
	// A head is a candidate range variable only when a '.' follows it
	// immediately. The rule is purely syntactic on purpose; see the package
	// documentation for why, and for how a field that shares a range
	// variable's name is still addressable.
	eligible := p.tok.kind == tokDot
	star := false

loop:
	for {
		switch p.tok.kind {
		case tokDot:
			p.advance()
			if p.tok.kind == tokStar {
				if !allowStar {
					return nil, p.errHere("'*' is only allowed in the SELECT list")
				}
				if len(segs) != 1 || !eligible {
					return nil, p.errHere("'*' may only follow a range variable, as in u.*")
				}
				p.advance()
				star = true
				break loop
			}
			switch p.tok.kind {
			case tokIdent:
				if err := p.checkNameable("a field name after '.'"); err != nil {
					return nil, err
				}
			case tokQuotedIdent:
			default:
				return nil, p.errHere("expected a field name after '.'")
			}
			segs = append(segs, Segment{Key: p.internToken(p.tok)})
			p.advance()
		case tokLBracket:
			p.advance()
			seg, err := p.parseSubscript()
			if err != nil {
				return nil, err
			}
			segs = append(segs, seg)
			if err := p.expect(tokRBracket, "']'"); err != nil {
				return nil, err
			}
		default:
			break loop
		}
	}
	// A path butted against '(' is a function call. Catching it here rather
	// than letting the '(' fall through as unexpected input is what turns
	// "expected FROM" — pointing at a byte the author did not think was
	// wrong — into a message naming the five reductions that do exist.
	if p.tok.kind == tokLParen {
		return nil, p.errfHere(
			"%q is not a supported function: the engine computes COUNT, SUM, AVG, MIN, and MAX, and no scalar functions",
			head.text)
	}
	p.segScratch = segs
	if star {
		segs = segs[:1] // the head is the range variable; resolution drops it
	}
	return p.newPath(head.pos, segs, eligible, star), nil
}

// parseSubscript parses the content of a '[...]' accessor: a non-negative
// array index, or a quoted key for a name the dotted form cannot spell.
func (p *Parser) parseSubscript() (Segment, error) {
	switch p.tok.kind {
	case tokNumber:
		n, err := strconv.Atoi(p.tok.text)
		if err != nil || n < 0 {
			return Segment{}, p.errHere("an array subscript must be a non-negative integer that fits in an int")
		}
		p.advance()
		return Segment{Index: n, IsIndex: true}, nil
	case tokString, tokQuotedIdent:
		key := p.internToken(p.tok)
		p.advance()
		return Segment{Key: key}, nil
	case tokParam:
		return Segment{}, p.errHere("a placeholder cannot be a subscript: paths are compiled into the plan when the statement is prepared")
	}
	return Segment{}, p.errHere("expected an array index or a quoted key inside '[]'")
}

// checkNameable rejects a reserved word where want was expected, naming the
// quoted form that would make it a name. The two readings — a misplaced clause
// keyword, and a field that happens to be spelled like one — are both covered
// by the one message, because the parser cannot tell which the author meant and
// guessing would make the wrong half of the audience worse off.
func (p *Parser) checkNameable(want string) error {
	if !reserved(p.tok.kw) {
		return nil
	}
	return p.errfAt(p.tok.pos,
		"expected %s, but found the reserved word %s; write %q to use it as a name",
		want, p.tok.text, p.tok.text)
}

// newPath records a path for resolution and returns it.
func (p *Parser) newPath(pos int, segs []Segment, eligible, star bool) *PathExpr {
	path := p.paths.one()
	stored := p.segs.allocDirty(len(segs))
	copy(stored, segs)
	*path = PathExpr{Source: -1, Segments: stored, Pos: pos}
	p.pending = append(p.pending, pendingPath{path: path, eligible: eligible, star: star})
	return path
}
