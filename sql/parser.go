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

// maxSetExpressionDepth bounds authored parenthesized query expressions.
// Unlike predicate parentheses, every level owns semantic scope and must be
// retained as a group node, so this has its own recursion bound.
const maxSetExpressionDepth = 64

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
	keys  chunkArena[JoinKeyCond]
	ctes  chunkArena[CommonTableExpr]
	names chunkArena[string] // CTE output-name runs
	ints  chunkArena[int]    // CTE output-name position runs

	// Reused whole-clause slices. Unlike the arenas these hold no interior
	// pointers into themselves, so a plain reslice-to-zero is enough.
	columns             []ResultColumn
	from                []TableRef
	groupBy             []*PathExpr
	orderBy             []OrderTerm
	rows                []InsertRow
	updateAssignments   []UpdateAssignment
	conflictAssignments []UpdateAssignment
	cols                []ColumnDef
	keyPaths            []*PathExpr
	idxPaths            []*PathExpr
	cteScratch          []CommonTableExpr
	cteNameScratch      []string
	cteAliasPosScratch  []int
	joinKeyScratch      []JoinKeyCond
	joinNameScratch     []string

	// The statement bodies [Parser.ParseStatement] hands back by pointer. They
	// are fields rather than arena allocations because there is exactly one of
	// each per parse, so an arena would buy nothing and would make the lifetime
	// rule harder to state than "valid until the next parse" — which is the
	// rule every other thing a Parser returns already carries. sel doubles as
	// the synthetic SELECT an UPDATE or a DELETE selects its rows with; see
	// parse_dml.go.
	sel              SelectStmt
	with             WithClause
	returning        SelectStmt
	ins              InsertStmt
	conflict         InsertConflictUpdate
	upd              UpdateStmt
	del              DeleteStmt
	tbl              CreateTableStmt
	idx              CreateIndexStmt
	alter            AlterTableStmt
	drop             DropTableStmt
	truncate         TruncateStmt
	dropIndex        DropIndexStmt
	view             CreateViewStmt
	dropView         DropViewStmt
	savepoint        SavepointStmt
	releaseSavepoint SavepointStmt
	rollbackTo       SavepointStmt

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
	// window is allocated only after OVER is encountered. Keeping every window
	// arena and scratch slice behind one pointer preserves the ordinary parser's
	// state size and allocation profile.
	window *windowParserState
	// correlation is allocated only after an accepted LATERAL token or while a
	// predicate subquery is being parsed. It owns both cold arenas and the
	// transient lexical links installed on a child parser. An ordinary query and
	// an uncorrelated predicate AST retain no metadata from this state.
	correlation *correlationParserState
	// set is allocated only after a top-level set operator or parenthesized
	// query expression is encountered. Ordinary SELECT parsing therefore keeps
	// no set-node arenas or scratch slices alive.
	set *setParserState
	// scalar is allocated only when arithmetic, concatenation, CASE, or CAST is
	// encountered. Ordinary path-only statements retain their existing parser
	// arena footprint and allocation profile.
	scalar *scalarParserState
	// existsProjection allows SELECT 1 (and equivalent literals) only while
	// parsing an EXISTS body. EXISTS never observes its output value, so
	// lowering the literal to a whole-document projection avoids inventing a
	// constant-expression executor for a value that is discarded.
	existsProjection bool
	nesting          int
	outerCTEs        *cteScope
	activeCTEs       cteScope

	params int
	sized  bool
}

// A cteScope is parse-time-only lexical state. Relation nodes retain the
// matched definition query pointer, so no AST borrows this chain after parsing
// completes. Values live in Parser fields to keep nested warmed parses free of
// scope-object allocations.
type cteScope struct {
	defs  []CommonTableExpr
	outer *cteScope
}

// correlationRangeScope is parse-time-only lexical visibility. limit freezes
// the visible FROM prefix: at the LATERAL token for a derived relation, or the
// complete outer FROM for a predicate subquery.
type correlationRangeScope struct {
	parser *Parser
	limit  int
	outer  *correlationRangeScope
}

type correlationCapture struct {
	owner         *Parser
	bindingBase   int
	referenceBase int
	forwardBase   int
	source        int
	pos           int
	scope         correlationRangeScope
}

type correlationForwardCandidate struct {
	path   *PathExpr
	alias  string
	source int
}

type correlationScratchBinding struct {
	binding CorrelationBinding
	path    *PathExpr
}

type correlationScratchReference struct {
	path    *PathExpr
	binding int
}

type correlationParserState struct {
	captures   chunkArena[correlationCapture]
	bindings   chunkArena[CorrelationBinding]
	specs      chunkArena[CorrelationSpec]
	references chunkArena[CorrelationReference]

	bindingScratch   []correlationScratchBinding
	referenceScratch []correlationScratchReference
	forward          []correlationForwardCandidate
	outerRanges      *correlationRangeScope
	capture          *correlationCapture
	sized            bool
}

func (s *correlationParserState) reset() {
	if !s.sized {
		s.captures.first = 2
		s.bindings.first = 4
		s.specs.first = 2
		s.references.first = 8
		s.sized = true
	}
	s.captures.rewind()
	s.bindings.rewind()
	s.specs.rewind()
	s.references.rewind()
	s.bindingScratch = s.bindingScratch[:0]
	s.referenceScratch = s.referenceScratch[:0]
	s.forward = s.forward[:0]
	s.outerRanges = nil
	s.capture = nil
}

func (p *Parser) correlationState() *correlationParserState {
	if p.correlation == nil {
		p.correlation = new(correlationParserState)
		// This is first-use initialization, not an accessor rewind. Parser.reset
		// owns the one rewind per parse; resetting here would let a later capture
		// overwrite specs already published by an earlier predicate subquery or
		// LATERAL relation in the same statement.
		p.correlation.reset()
	}
	return p.correlation
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
	document bool // SELECT "$doc" spelling, preserving its default output name
	// documentRoot records that the path began at "$doc" or
	// range."$doc", even when later accessors make it a field path.
	documentRoot bool
	quoted       bool // the leading identifier was quoted and remains case-sensitive
	// qualifiedFieldPos is the first field token after a syntactic range
	// qualifier, plus one. Zero means there is no qualified field token.
	qualifiedFieldPos int
	// nestedPos is the first accessor after a possible range-variable and one
	// top-level field, plus one. Zero means the path has no such accessor.
	nestedPos int
}

// Parse parses one SELECT statement into dst, reusing p's storage. See
// [Parser] for the lifetime dst inherits.
func (p *Parser) Parse(dst *SelectStmt, src string) error {
	return p.parseSelectText(dst, src, nil, nil, nil, 0, false)
}

// parseSelectText is Parse with the lexical context a nested SELECT inherits.
// Keeping the context explicit prevents a child Parser retained for warm reuse
// from leaking an old parent's CTE scope into a later parse.
func (p *Parser) parseSelectText(
	dst *SelectStmt,
	src string,
	outerCTEs *cteScope,
	outerRanges *correlationRangeScope,
	capture *correlationCapture,
	nesting int,
	existsProjection bool,
) error {
	*dst = SelectStmt{}
	if err := validateStatementText(src, p.cancel); err != nil {
		return err
	}
	p.outerCTEs = outerCTEs
	p.nesting = nesting
	p.existsProjection = existsProjection
	p.reset(src)
	if outerRanges != nil || capture != nil {
		state := p.correlationState()
		state.outerRanges = outerRanges
		state.capture = capture
	}
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
		p.keys.first = 8
		p.ctes.first = 4
		p.names.first = 8
		p.ints.first = 8
		p.sized = true
	}
	p.text.rewind()
	p.exprs.rewind()
	p.kids.rewind()
	p.ops.rewind()
	p.paths.rewind()
	p.segs.rewind()
	p.conds.rewind()
	p.keys.rewind()
	p.ctes.rewind()
	p.names.rewind()
	p.ints.rewind()

	p.columns = p.columns[:0]
	p.from = p.from[:0]
	p.groupBy = p.groupBy[:0]
	p.orderBy = p.orderBy[:0]
	p.rows = p.rows[:0]
	p.updateAssignments = p.updateAssignments[:0]
	p.conflictAssignments = p.conflictAssignments[:0]
	p.cols = p.cols[:0]
	p.keyPaths = p.keyPaths[:0]
	p.idxPaths = p.idxPaths[:0]
	p.cteScratch = p.cteScratch[:0]
	p.cteNameScratch = p.cteNameScratch[:0]
	p.cteAliasPosScratch = p.cteAliasPosScratch[:0]
	p.joinKeyScratch = p.joinKeyScratch[:0]
	p.joinNameScratch = p.joinNameScratch[:0]
	p.segScratch = p.segScratch[:0]
	p.kidStack = p.kidStack[:0]
	p.opScratch = p.opScratch[:0]
	p.pending = p.pending[:0]
	p.returning = SelectStmt{}
	p.conflict = InsertConflictUpdate{}
	p.with = WithClause{}
	p.filterColumns = p.filterColumns[:0]
	p.filterFrom = p.filterFrom[:0]
	p.filterGroupBy = p.filterGroupBy[:0]
	p.filterOrderBy = p.filterOrderBy[:0]
	p.mutationOrderBy = p.mutationOrderBy[:0]
	p.mutationLimit = nil
	if p.nested != nil {
		p.nested.used = 0
	}
	if p.window != nil {
		p.window.reset()
	}
	if p.correlation != nil {
		p.correlation.reset()
	}
	if p.set != nil {
		p.set.reset()
	}
	if p.scalar != nil {
		p.scalar.reset()
	}

	p.lx = lexer{
		src: src, cancel: p.cancel,
		nextCancelByte: parserCancelByteInterval,
	}
	p.depth = 0
	p.params = 0
	p.activeCTEs = cteScope{outer: p.outerCTEs}
	p.advance()
}

type nestedParsers struct {
	parsers []*Parser
	used    int
}

type windowParserState struct {
	exprs          chunkArena[WindowExpr]
	pathRuns       chunkArena[*PathExpr]
	orderRuns      chunkArena[WindowOrderTerm]
	definitionRuns chunkArena[NamedWindow]
	partitions     []*PathExpr
	orders         []WindowOrderTerm
	definitions    []NamedWindow
	sized          bool
}

func (w *windowParserState) reset() {
	if !w.sized {
		w.exprs.first = 4
		w.pathRuns.first = 8
		w.orderRuns.first = 8
		w.definitionRuns.first = 4
		w.sized = true
	}
	w.exprs.rewind()
	w.pathRuns.rewind()
	w.orderRuns.rewind()
	w.definitionRuns.rewind()
	w.partitions = w.partitions[:0]
	w.orders = w.orders[:0]
	w.definitions = w.definitions[:0]
}

func (p *Parser) windowState() *windowParserState {
	if p.window == nil {
		p.window = new(windowParserState)
		p.window.reset()
	}
	return p.window
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
	statementPos := p.tok.pos
	if p.tok.kind == tokLParen || p.atKeyword(kwValues) || p.atKeyword(kwTable) {
		return p.parseSetStatement(statementPos)
	}
	hasWith := p.atKeyword(kwWith)
	if hasWith {
		if err := p.parseWithClause(); err != nil {
			return err
		}
		switch {
		case p.atKeyword(kwInsert), p.atKeyword(kwUpdate), p.atKeyword(kwDelete), p.atKeyword(kwMerge):
			return newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"data-modifying WITH statements are not supported; the primary statement must be SELECT",
			)
		case p.atKeyword(kwValues), p.atKeyword(kwTable):
			return newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"WITH directly before VALUES or TABLE is not supported; make a SELECT the first operand so its common table expressions have one lexical execution owner",
			)
		}
	}
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
	if p.acceptKeyword(kwFrom) {
		if err := p.parseFrom(); err != nil {
			return err
		}
		p.discardExistsLiteralProjection()
	} else if err := p.validateSourceIndependentColumns(); err != nil {
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
		if !hasAggregate && !p.hasWindowColumns() {
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
	if err := p.parseWindowClause(); err != nil {
		return err
	}
	if err := p.parseOrderBy(true); err != nil {
		return err
	}
	if err := p.parseLimitOffset(); err != nil {
		return err
	}
	if isSetOperatorToken(p.tok) {
		return p.parseSetStatement(statementPos)
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

// validateSourceIndependentColumns keeps a FROM-less SELECT deliberately
// narrow: it is a one-row scalar relation, not an implicit document source.
// Literals, parameters, and scalar expressions composed solely from them can
// use the ordinary scalar runtime. Paths, wildcards, aggregates, and windows
// need source or reduction semantics and remain positioned refusals.
func (p *Parser) validateSourceIndependentColumns() error {
	for i := range p.out.Columns {
		column := &p.out.Columns[i]
		switch {
		case column.Window != nil:
			return newFeatureNotSupportedError(
				p.lx.src, column.Pos,
				"a window expression requires a FROM relation",
			)
		case column.Agg != AggNone:
			return newFeatureNotSupportedError(
				p.lx.src, column.Pos,
				"an aggregate requires a FROM relation",
			)
		case column.Scalar != nil:
			hasPath, hasAggregate := scalarDependencyKinds(column.Scalar)
			if hasPath {
				return newFeatureNotSupportedError(
					p.lx.src, column.Pos,
					"a FROM-less SELECT cannot read a document path; add FROM to name its relation",
				)
			}
			if hasAggregate {
				return newFeatureNotSupportedError(
					p.lx.src, column.Pos,
					"an aggregate requires a FROM relation",
				)
			}
		case column.Path != nil:
			return newFeatureNotSupportedError(
				p.lx.src, column.Pos,
				"a FROM-less SELECT cannot read a document path or wildcard; add FROM to name its relation",
			)
		default:
			return p.errAt(column.Pos, "a FROM-less SELECT output must be a scalar expression")
		}
	}
	return nil
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
	standaloneStar := false
	if _, _, known := scalarTypedStringHead(p.tok); known {
		return p.parseTypedHeadResultColumn(col)
	}
	if scalarStarts(p.tok) {
		expr, err := p.parseScalarExpression(scalarSelect)
		if err != nil {
			return col, err
		}
		col.Scalar = expr
		alias, err := p.parseColumnAlias(true)
		if err != nil {
			return col, err
		}
		col.Alias = alias
		if col.Alias == "" {
			col.Alias = typedConstantOutputName(expr)
		}
		if expr.Kind == ScalarPath {
			// A BOOL/TEXT type head without a following string is an ordinary
			// path. Restore the native projection shape so fields with those
			// names pay no scalar execution sidecar.
			col.Path, col.Scalar = expr.Path, nil
		}
		return col, nil
	}
	if kind, ok := windowOnlyFunctionOf(p.tok.kw); ok {
		head := p.tok
		p.advance()
		if p.tok.kind == tokLParen {
			window, err := p.parseWindowOnlyCall(kind, head.pos)
			if err != nil {
				return col, err
			}
			col.Window = window
		} else {
			path, err := p.continuePath(head, true)
			if err != nil {
				return col, err
			}
			col.Path = path
		}
	} else {
		standaloneStar = p.tok.kind == tokStar
		switch agg, head, state := p.tryAggregate(); state {
		case aggCall:
			path, err := p.parseAggregateArgs(agg)
			if err != nil {
				return col, err
			}
			if p.atKeyword(kwOver) {
				window, err := p.parseWindowOver(windowAggregateKind(agg), path, col.Pos)
				if err != nil {
					return col, err
				}
				col.Window = window
			} else {
				col.Agg, col.Path = agg, path
			}
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
	}
	if scalarContinues(p.tok) {
		if col.Window != nil {
			return col, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"arithmetic over a window result is not supported by the scalar execution slice yet",
			)
		}
		left := p.scalarFromColumn(col)
		expr, err := p.continueScalarExpression(left, scalarSelect)
		if err != nil {
			return col, err
		}
		col.Agg, col.Path, col.Scalar = AggNone, nil, expr
	}
	alias, err := p.parseColumnAlias(!standaloneStar)
	if err != nil {
		return col, err
	}
	col.Alias = alias
	if alias == "" && col.Path != nil && len(p.pending) > 0 {
		last := p.pending[len(p.pending)-1]
		if last.path == col.Path && last.document {
			col.Alias = DocumentColumn
		}
	}
	return col, nil
}

func (p *Parser) parseTypedHeadResultColumn(col ResultColumn) (ResultColumn, error) {
	target, supported, _ := scalarTypedStringHead(p.tok)
	head := p.tok
	p.advance()
	if p.tok.kind != tokString {
		if err := p.rejectUnsupportedTypedStringSuffix(head); err != nil {
			return col, err
		}
		path, err := p.continuePath(head, true)
		if err != nil {
			return col, err
		}
		if err := p.rejectQualifiedTypedStringPath(head, path); err != nil {
			return col, err
		}
		col.Path = path
		if scalarContinues(p.tok) {
			expr, err := p.continueScalarExpression(p.scalarFromColumn(col), scalarSelect)
			if err != nil {
				return col, err
			}
			col.Path, col.Scalar = nil, expr
		}
		alias, err := p.parseColumnAlias(true)
		if err != nil {
			return col, err
		}
		col.Alias = alias
		if alias == "" && col.Path != nil && len(p.pending) > 0 {
			last := p.pending[len(p.pending)-1]
			if last.path == col.Path && last.document {
				col.Alias = DocumentColumn
			}
		}
		return col, nil
	}

	value, err := p.parseTypedStringAfterHead(head, target, supported)
	if err != nil {
		return col, err
	}
	literal := p.newScalar(ScalarLiteral, value.Pos)
	literal.Value = value
	cast := p.newScalar(ScalarCast, value.Pos)
	cast.Cast, cast.Left, cast.TargetPos = target, literal, head.pos
	cast.TypedConstant = true
	expr, err := p.continueScalarExpression(cast, scalarSelect)
	if err != nil {
		return col, err
	}
	col.Scalar = expr
	alias, err := p.parseColumnAlias(true)
	if err != nil {
		return col, err
	}
	col.Alias = alias
	if col.Alias == "" {
		col.Alias = typedConstantOutputName(expr)
	}
	return col, nil
}

// typedConstantOutputName mirrors PostgreSQL FigureColname for the supported
// typed-string slice. Parentheses disappear from the AST and an outer cast
// replaces the weaker inner type name; any arithmetic or concatenation root
// deliberately falls back to ?column?.
func typedConstantOutputName(expr *ScalarExpr) string {
	if expr == nil || expr.Kind != ScalarCast {
		return ""
	}
	found := false
	for node := expr; node != nil && node.Kind == ScalarCast; node = node.Left {
		found = found || node.TypedConstant
	}
	if !found {
		return ""
	}
	switch expr.Cast {
	case ScalarCastText:
		return "text"
	case ScalarCastBoolean:
		return "bool"
	case ScalarCastNumeric:
		return "numeric"
	case ScalarCastJSON:
		return "json"
	default:
		return ""
	}
}

// discardExistsLiteralProjection retains the existing cheap whole-row plan
// when an EXISTS body has a physical FROM source. A FROM-less body must keep
// its scalar literal so Statement can evaluate it over the synthetic unit row.
func (p *Parser) discardExistsLiteralProjection() {
	if !p.existsProjection {
		return
	}
	for i := range p.out.Columns {
		column := &p.out.Columns[i]
		if column.Scalar == nil ||
			(column.Scalar.Kind != ScalarLiteral && column.Scalar.Kind != ScalarNull) {
			continue
		}
		column.Scalar = nil
		column.Path = p.newPath(column.Pos, nil, false, true)
	}
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

// parseColumnAlias follows PostgreSQL's target_el grammar: AS accepts any
// identifier, while a bare alias accepts IDENT and BareColLabel. The latter is
// an independent keyword category; it is deliberately not this dialect's
// reserved() classification. A standalone '*' is target_el's one separate
// production and cannot carry either spelling of alias, while alias.* remains
// an ordinary expression and can.
func (p *Parser) parseColumnAlias(aliasable bool) (string, error) {
	if !aliasable {
		if p.atKeyword(kwAs) || p.tok.kind == tokQuotedIdent ||
			(p.tok.kind == tokIdent && !postgresAliasRequiresAS(p.tok.text)) {
			return "", p.errHere("a standalone '*' cannot have an output alias; qualify it as range_variable.* to name the projected document")
		}
		return "", nil
	}
	if p.acceptKeyword(kwAs) {
		return p.parseAliasName("an output name after AS")
	}
	if p.tok.kind == tokQuotedIdent ||
		(p.tok.kind == tokIdent && !postgresAliasRequiresAS(p.tok.text)) {
		return p.parseAliasName("an output name")
	}
	return "", nil
}

// postgresAliasRequiresAS mirrors every AS_LABEL entry in PostgreSQL 18.6's
// src/include/parser/kwlist.h. Several spellings are intentionally not lexer
// keywords here because reserving them would break same-named JSON fields, so
// classification uses the original token text. The fixed stack buffer and
// switch retain the parser's zero steady-state allocation contract.
func postgresAliasRequiresAS(name string) bool {
	const maxASLabelLen = 9 // CHARACTER, INTERSECT, PRECISION, RETURNING
	if len(name) == 0 || len(name) > maxASLabelLen {
		return false
	}
	var folded [maxASLabelLen]byte
	for i := range name {
		c := name[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		folded[i] = c
	}
	switch string(folded[:len(name)]) {
	case "ARRAY", "AS", "CHAR", "CHARACTER", "CREATE", "DAY", "EXCEPT",
		"FETCH", "FILTER", "FOR", "FROM", "GRANT", "GROUP", "HAVING",
		"HOUR", "INTERSECT", "INTO", "ISNULL", "LIMIT", "MINUTE", "MONTH",
		"NOTNULL", "OFFSET", "ON", "ORDER", "OVER", "OVERLAPS", "PRECISION",
		"RETURNING", "SECOND", "TO", "UNION", "VARYING", "WHERE", "WINDOW",
		"WITH", "WITHIN", "WITHOUT", "YEAR":
		return true
	default:
		return false
	}
}

func (p *Parser) hasWindowColumns() bool {
	for i := range p.columns {
		if p.columns[i].Window != nil {
			return true
		}
	}
	return false
}

func windowOnlyFunctionOf(kw keyword) (WindowFunctionKind, bool) {
	switch kw {
	case kwRowNumber:
		return WindowRowNumber, true
	case kwRank:
		return WindowRank, true
	case kwDenseRank:
		return WindowDenseRank, true
	case kwNtile:
		return WindowNTile, true
	case kwPercentRank:
		return WindowPercentRank, true
	case kwCumeDist:
		return WindowCumeDist, true
	case kwFirstValue:
		return WindowFirstValue, true
	case kwLastValue:
		return WindowLastValue, true
	case kwNthValue:
		return WindowNthValue, true
	case kwLag:
		return WindowLag, true
	case kwLead:
		return WindowLead, true
	default:
		return 0, false
	}
}

func windowAggregateKind(agg AggKind) WindowFunctionKind {
	switch agg {
	case AggCount:
		return WindowCount
	case AggSum:
		return WindowSum
	case AggAvg:
		return WindowAvg
	case AggMin:
		return WindowMin
	default:
		return WindowMax
	}
}

func (p *Parser) parseWindowOnlyCall(
	kind WindowFunctionKind,
	pos int,
) (*WindowExpr, error) {
	p.advance() // '('
	w := p.windowState().exprs.one()
	*w = WindowExpr{Kind: kind, Pos: pos}
	switch kind {
	case WindowRowNumber, WindowRank, WindowDenseRank,
		WindowPercentRank, WindowCumeDist:
		if p.tok.kind != tokRParen {
			return nil, p.errfHere("%s takes no arguments", kind)
		}
	case WindowNTile:
		buckets, err := p.parsePositiveWindowCount("NTILE bucket count")
		if err != nil {
			return nil, err
		}
		w.Buckets, w.HasBuckets = buckets, true
	case WindowFirstValue, WindowLastValue:
		argument, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		w.Argument = argument
	case WindowNthValue:
		argument, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		w.Argument = argument
		if err := p.expect(tokComma, "',' between NTH_VALUE arguments"); err != nil {
			return nil, err
		}
		nth, err := p.parsePositiveWindowCount("NTH_VALUE position")
		if err != nil {
			return nil, err
		}
		w.Nth, w.HasNth = nth, true
	case WindowLag, WindowLead:
		argument, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		w.Argument = argument
		if p.tok.kind == tokComma {
			p.advance()
			offset, err := p.parseWindowCount(kind.String() + " offset")
			if err != nil {
				return nil, err
			}
			w.Offset, w.HasOffset = offset, true
			if p.tok.kind == tokComma {
				p.advance()
				value, isNull, err := p.parseWindowDefault()
				if err != nil {
					return nil, err
				}
				w.Default, w.DefaultNull, w.HasDefault = value, isNull, true
			}
		}
	}
	if err := p.expect(tokRParen, "')'"); err != nil {
		return nil, err
	}
	if !p.atKeyword(kwOver) {
		return nil, p.errfHere("%s is a window function and requires OVER (...)", kind)
	}
	if err := p.parseWindowSpec(w); err != nil {
		return nil, err
	}
	return w, nil
}

func (p *Parser) parseWindowOver(
	kind WindowFunctionKind,
	argument *PathExpr,
	pos int,
) (*WindowExpr, error) {
	w := p.windowState().exprs.one()
	*w = WindowExpr{Kind: kind, Argument: argument, Pos: pos}
	if err := p.parseWindowSpec(w); err != nil {
		return nil, err
	}
	return w, nil
}

func (p *Parser) parseWindowSpec(w *WindowExpr) error {
	w.Spec.Pos = p.tok.pos
	p.advance() // OVER
	if p.tok.kind != tokLParen {
		name, pos, err := p.parseWindowName("after OVER")
		if err != nil {
			return err
		}
		w.Spec.Name, w.Spec.NamePos = name, pos
		w.DirectName = true
		return nil
	}
	p.advance()
	return p.parseWindowDefinition(&w.Spec)
}

// parseWindowClause reads the statement-local named-window catalog and then
// resolves every SELECT-list reference. Window calls precede this clause in
// SQL source order, so deferring the lookup is required; definitions themselves
// resolve eagerly and may inherit only from an earlier definition.
func (p *Parser) parseWindowClause() error {
	if !p.atKeyword(kwWindow) && p.window == nil {
		return nil
	}
	state := p.windowState()
	if p.acceptKeyword(kwWindow) {
		for {
			name, pos, err := p.parseWindowName("after WINDOW")
			if err != nil {
				return err
			}
			for i := range state.definitions {
				if state.definitions[i].Name == name {
					return p.errfAt(pos,
						"window %q is declared twice; its first declaration is at byte %d",
						name, state.definitions[i].Pos,
					)
				}
			}
			if err := p.expectKeyword(kwAs, "AS after a window name"); err != nil {
				return err
			}
			if p.tok.kind != tokLParen {
				return p.errHere("expected '(' after AS in a WINDOW definition")
			}
			spec := WindowSpec{Pos: p.tok.pos}
			p.advance()
			if err := p.parseWindowDefinition(&spec); err != nil {
				return err
			}
			if err := p.resolveWindowSpec(&spec, false, state.definitions); err != nil {
				return err
			}
			state.definitions = append(state.definitions, NamedWindow{
				Name: name, Spec: spec, Pos: pos,
			})
			if len(state.definitions) > maxClauseItems {
				return p.errfAt(pos, "WINDOW may hold at most %d definitions", maxClauseItems)
			}
			if p.tok.kind != tokComma {
				break
			}
			p.advance()
		}
		definitions := state.definitionRuns.allocDirty(len(state.definitions))
		copy(definitions, state.definitions)
		p.out.Windows = definitions
	}

	for i := range p.out.Columns {
		window := p.out.Columns[i].Window
		if window == nil {
			continue
		}
		if err := p.resolveWindowSpec(
			&window.Spec, window.DirectName, state.definitions,
		); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) parseWindowName(after string) (string, int, error) {
	pos := p.tok.pos
	switch p.tok.kind {
	case tokIdent:
		if reserved(p.tok.kw) {
			return "", pos, p.errfHere(
				"expected a window name %s, but found the reserved word %s; quote it to use it as a name",
				after, p.tok.text,
			)
		}
		fallthrough
	case tokQuotedIdent:
		if p.tok.text == "" {
			return "", pos, p.errHere("a window name may not be empty")
		}
		name := p.internToken(p.tok)
		p.advance()
		return name, pos, nil
	default:
		return "", pos, p.errfHere("expected a window name %s", after)
	}
}

func (p *Parser) parseWindowDefinition(spec *WindowSpec) error {
	if p.tok.kind == tokQuotedIdent ||
		p.tok.kind == tokIdent && !reserved(p.tok.kw) {
		name, pos, err := p.parseWindowName("at the start of the window specification")
		if err != nil {
			return err
		}
		spec.Name, spec.NamePos = name, pos
	}

	state := p.windowState()
	partitionBase, orderBase := len(state.partitions), len(state.orders)
	if p.atKeyword(kwPartition) {
		spec.PartitionPos = p.tok.pos
		p.advance()
		if err := p.expectKeyword(kwBy, "BY after PARTITION"); err != nil {
			return err
		}
		for {
			path, err := p.parseKeyPath(
				"PARTITION BY cannot contain an aggregate or window function",
			)
			if err != nil {
				return err
			}
			state.partitions = append(state.partitions, path)
			if len(state.partitions)-partitionBase > maxClauseItems {
				return p.errfAt(path.Pos, "PARTITION BY may hold at most %d keys", maxClauseItems)
			}
			if p.tok.kind != tokComma {
				break
			}
			p.advance()
		}
	}
	if p.atKeyword(kwOrder) {
		spec.OrderPos = p.tok.pos
		p.advance()
		if err := p.expectKeyword(kwBy, "BY after ORDER"); err != nil {
			return err
		}
		for {
			term, err := p.parseWindowOrderTerm()
			if err != nil {
				return err
			}
			state.orders = append(state.orders, term)
			if len(state.orders)-orderBase > maxClauseItems {
				return p.errfAt(term.Pos, "window ORDER BY may hold at most %d keys", maxClauseItems)
			}
			if p.tok.kind != tokComma {
				break
			}
			p.advance()
		}
	}
	if p.atKeyword(kwRows) || p.atKeyword(kwGroups) || p.atKeyword(kwRange) {
		unit := WindowFrameRows
		switch {
		case p.atKeyword(kwGroups):
			unit = WindowFrameGroups
		case p.atKeyword(kwRange):
			unit = WindowFrameRange
		}
		frame, err := p.parseWindowFrame(unit)
		if err != nil {
			return err
		}
		spec.Frame = frame
	}
	if err := p.expect(tokRParen, "')' after the window specification"); err != nil {
		return err
	}
	partitions := state.partitions[partitionBase:]
	if len(partitions) != 0 {
		run := state.pathRuns.allocDirty(len(partitions))
		copy(run, partitions)
		spec.PartitionBy = run
	}
	orders := state.orders[orderBase:]
	if len(orders) != 0 {
		run := state.orderRuns.allocDirty(len(orders))
		copy(run, orders)
		spec.OrderBy = run
	}
	state.partitions = state.partitions[:partitionBase]
	state.orders = state.orders[:orderBase]
	return nil
}

func (p *Parser) resolveWindowSpec(
	spec *WindowSpec,
	direct bool,
	definitions []NamedWindow,
) error {
	if spec.Name == "" {
		return p.validateWindowSpec(spec)
	}
	var base *WindowSpec
	for i := len(definitions) - 1; i >= 0; i-- {
		if definitions[i].Name == spec.Name {
			base = &definitions[i].Spec
			break
		}
	}
	if base == nil {
		return p.errfAt(spec.NamePos,
			"window %q is not defined in this SELECT", spec.Name)
	}
	if direct {
		spec.PartitionBy = base.PartitionBy
		spec.PartitionPos = base.PartitionPos
		spec.OrderBy = base.OrderBy
		spec.OrderPos = base.OrderPos
		spec.Frame = base.Frame
		spec.PartitionInherited = len(base.PartitionBy) != 0
		spec.OrderInherited = len(base.OrderBy) != 0
		spec.FrameInherited = base.Frame.Explicit
		return p.validateWindowSpec(spec)
	}
	if base.Frame.Explicit {
		return p.errfAt(spec.NamePos,
			"window %q has a frame clause and cannot be copied with OVER (...)", spec.Name)
	}
	if len(spec.PartitionBy) != 0 {
		return p.errAt(spec.PartitionPos,
			"an inherited window cannot override PARTITION BY")
	}
	if len(base.OrderBy) != 0 && len(spec.OrderBy) != 0 {
		return p.errAt(spec.OrderPos,
			"an inherited window cannot override ORDER BY")
	}
	if len(base.PartitionBy) != 0 {
		spec.PartitionBy = base.PartitionBy
		spec.PartitionPos = base.PartitionPos
		spec.PartitionInherited = true
	}
	if len(spec.OrderBy) == 0 && len(base.OrderBy) != 0 {
		spec.OrderBy = base.OrderBy
		spec.OrderPos = base.OrderPos
		spec.OrderInherited = true
	}
	return p.validateWindowSpec(spec)
}

func (p *Parser) validateWindowSpec(spec *WindowSpec) error {
	if !spec.Frame.Explicit {
		return nil
	}
	if spec.Frame.Unit == WindowFrameGroups && len(spec.OrderBy) == 0 {
		return p.errAt(spec.Frame.Pos, "a GROUPS window frame requires ORDER BY")
	}
	if spec.Frame.Unit == WindowFrameRange && windowFrameHasRangeOffset(spec.Frame) &&
		len(spec.OrderBy) != 1 {
		pos := spec.Frame.Start.Pos
		if spec.Frame.Start.Kind != WindowPreceding &&
			spec.Frame.Start.Kind != WindowFollowing {
			pos = spec.Frame.End.Pos
		}
		return p.errAt(pos,
			"a RANGE frame with a PRECEDING or FOLLOWING offset requires exactly one ORDER BY key")
	}
	return nil
}

func windowFrameHasRangeOffset(frame WindowFrame) bool {
	return frame.Start.Kind == WindowPreceding || frame.Start.Kind == WindowFollowing ||
		frame.End.Kind == WindowPreceding || frame.End.Kind == WindowFollowing
}

func (p *Parser) parseWindowOrderTerm() (WindowOrderTerm, error) {
	term := WindowOrderTerm{Pos: p.tok.pos}
	if p.tok.kind == tokNumber {
		return term, p.errHere("window ORDER BY does not accept an output position; name the input path")
	}
	path, err := p.parseKeyPath(
		"window ORDER BY cannot contain an aggregate or window function",
	)
	if err != nil {
		return term, err
	}
	term.Path = path
	if p.acceptKeyword(kwAsc) {
	} else if p.acceptKeyword(kwDesc) {
		term.Desc = true
	}
	if p.acceptKeyword(kwNulls) {
		switch {
		case p.acceptKeyword(kwFirst):
			term.Nulls = WindowNullsFirst
		case p.acceptKeyword(kwLast):
			term.Nulls = WindowNullsLast
		default:
			return term, p.errHere("expected FIRST or LAST after NULLS")
		}
	}
	if p.atKeyword(kwCollate) {
		return term, p.errHere("COLLATE is not supported: strings compare by decoded content")
	}
	return term, nil
}

func (p *Parser) parseWindowFrame(unit WindowFrameUnit) (WindowFrame, error) {
	frame := WindowFrame{Unit: unit, Pos: p.tok.pos, Explicit: true}
	unitName := windowFrameUnitName(unit)
	p.advance() // ROWS, GROUPS, or RANGE
	if p.acceptKeyword(kwBetween) {
		start, err := p.parseWindowFrameBound(unit)
		if err != nil {
			return frame, err
		}
		if start.Kind == WindowUnboundedFollowing {
			return frame, p.errfAt(start.Pos, "a %s frame cannot start at UNBOUNDED FOLLOWING", unitName)
		}
		if err := p.expectKeyword(kwAnd, "AND between window frame bounds"); err != nil {
			return frame, err
		}
		end, err := p.parseWindowFrameBound(unit)
		if err != nil {
			return frame, err
		}
		if end.Kind == WindowUnboundedPreceding {
			return frame, p.errfAt(end.Pos, "a %s frame cannot end at UNBOUNDED PRECEDING", unitName)
		}
		frame.Start, frame.End = start, end
		if err := p.parseWindowFrameExclusion(&frame); err != nil {
			return frame, err
		}
		return frame, p.validateStaticWindowFrame(frame)
	}
	start, err := p.parseWindowFrameBound(unit)
	if err != nil {
		return frame, err
	}
	if start.Kind == WindowUnboundedFollowing {
		return frame, p.errfAt(start.Pos, "a %s frame cannot start at UNBOUNDED FOLLOWING", unitName)
	}
	frame.Start = start
	frame.End = WindowFrameBound{Kind: WindowCurrentRow, Pos: start.Pos}
	if err := p.parseWindowFrameExclusion(&frame); err != nil {
		return frame, err
	}
	return frame, p.validateStaticWindowFrame(frame)
}

func windowFrameUnitName(unit WindowFrameUnit) string {
	switch unit {
	case WindowFrameRows:
		return "ROWS"
	case WindowFrameGroups:
		return "GROUPS"
	case WindowFrameRange:
		return "RANGE"
	default:
		return "window"
	}
}

func (p *Parser) parseWindowFrameBound(unit WindowFrameUnit) (WindowFrameBound, error) {
	bound := WindowFrameBound{Pos: p.tok.pos}
	switch {
	case p.acceptKeyword(kwUnbounded):
		switch {
		case p.acceptKeyword(kwPreceding):
			bound.Kind = WindowUnboundedPreceding
		case p.acceptKeyword(kwFollowing):
			bound.Kind = WindowUnboundedFollowing
		default:
			return bound, p.errHere("expected PRECEDING or FOLLOWING after UNBOUNDED")
		}
		return bound, nil
	case p.acceptKeyword(kwCurrent):
		if err := p.expectKeyword(kwRow, "ROW after CURRENT"); err != nil {
			return bound, err
		}
		bound.Kind = WindowCurrentRow
		return bound, nil
	}
	var offset Operand
	var err error
	if unit == WindowFrameRange {
		offset, err = p.parseWindowRangeOffset()
	} else {
		offset, err = p.parseWindowCount("window frame offset")
	}
	if err != nil {
		return bound, err
	}
	bound.Offset = offset
	switch {
	case p.acceptKeyword(kwPreceding):
		bound.Kind = WindowPreceding
	case p.acceptKeyword(kwFollowing):
		bound.Kind = WindowFollowing
	default:
		return bound, p.errHere("expected PRECEDING or FOLLOWING after a window frame offset")
	}
	return bound, nil
}

func (p *Parser) parseWindowRangeOffset() (Operand, error) {
	if p.tok.kind != tokNumber && p.tok.kind != tokParam {
		return Operand{}, p.errHere(
			"expected an exact non-negative number or '?' for a RANGE frame offset",
		)
	}
	if p.tok.kind == tokNumber && p.tok.text[0] == '-' &&
		!sqlNumberTextIsZero(p.tok.text) {
		return Operand{}, p.errHere("a RANGE frame offset must not be negative")
	}
	return p.parseOperand()
}

func (p *Parser) parseWindowFrameExclusion(frame *WindowFrame) error {
	if !p.atKeyword(kwExclude) {
		return nil
	}
	frame.ExclusionPos = p.tok.pos
	frame.ExclusionExplicit = true
	p.advance()
	switch {
	case p.acceptKeyword(kwCurrent):
		if err := p.expectKeyword(kwRow, "ROW after EXCLUDE CURRENT"); err != nil {
			return err
		}
		frame.Exclusion = WindowExcludeCurrentRow
	case p.acceptKeyword(kwGroup):
		frame.Exclusion = WindowExcludeGroup
	case p.acceptKeyword(kwTies):
		frame.Exclusion = WindowExcludeTies
	case p.acceptKeyword(kwNo):
		if err := p.expectKeyword(kwOthers, "OTHERS after EXCLUDE NO"); err != nil {
			return err
		}
		frame.Exclusion = WindowExcludeNoOthers
	default:
		return p.errHere("expected CURRENT ROW, GROUP, TIES, or NO OTHERS after EXCLUDE")
	}
	return nil
}

func (p *Parser) parseWindowCount(clause string) (Operand, error) {
	if p.tok.kind != tokNumber && p.tok.kind != tokParam {
		return Operand{}, p.errfHere("expected a non-negative integer or '?' for %s", clause)
	}
	if p.tok.kind == tokNumber {
		text := p.tok.text
		if _, err := strconv.ParseInt(text, 10, 64); err != nil || text[0] == '-' {
			return Operand{}, p.errfHere("%s must be a non-negative whole number that fits in 64 bits", clause)
		}
	}
	return p.parseOperand()
}

func (p *Parser) parsePositiveWindowCount(clause string) (Operand, error) {
	value, err := p.parseWindowCount(clause)
	if err != nil {
		return Operand{}, err
	}
	if value.Kind == OperandNumber {
		n, _ := strconv.ParseInt(value.Text, 10, 64)
		if n == 0 {
			return Operand{}, p.errfAt(value.Pos, "%s must be greater than zero", clause)
		}
	}
	return value, nil
}

func (p *Parser) parseWindowDefault() (Operand, bool, error) {
	if p.atKeyword(kwNull) {
		pos := p.tok.pos
		p.advance()
		return Operand{Pos: pos}, true, nil
	}
	value, err := p.parseOperand()
	return value, false, err
}

func (p *Parser) validateStaticWindowFrame(frame WindowFrame) error {
	unitName := windowFrameUnitName(frame.Unit)
	startKind, startMayBeCurrent := staticWindowBoundKind(frame.Start)
	endKind, endMayBeCurrent := staticWindowBoundKind(frame.End)
	if startKind > endKind {
		// A parameterized PRECEDING/FOLLOWING offset can be zero, which SQL
		// treats exactly as CURRENT ROW. Defer shape-dependent ordering to the
		// physical validator rather than rejecting a valid zero bind at prepare.
		minimumStart, maximumEnd := startKind, endKind
		if startMayBeCurrent && WindowCurrentRow < minimumStart {
			minimumStart = WindowCurrentRow
		}
		if endMayBeCurrent && WindowCurrentRow > maximumEnd {
			maximumEnd = WindowCurrentRow
		}
		if minimumStart <= maximumEnd {
			return nil
		}
		return p.errfAt(frame.End.Pos, "a %s frame end cannot precede its start", unitName)
	}
	if startKind != endKind ||
		(startKind != WindowPreceding && startKind != WindowFollowing) ||
		frame.Start.Offset.Kind != OperandNumber || frame.End.Offset.Kind != OperandNumber {
		return nil
	}
	if frame.Unit == WindowFrameRange {
		comparison, known := compareNonNegativeSQLNumbers(
			frame.Start.Offset.Text, frame.End.Offset.Text,
		)
		if !known {
			return nil
		}
		if frame.Start.Kind == WindowPreceding {
			comparison = -comparison
		}
		if comparison > 0 {
			return p.errfAt(frame.End.Pos, "a %s frame end cannot precede its start", unitName)
		}
		return nil
	}
	left, _ := strconv.ParseInt(frame.Start.Offset.Text, 10, 64)
	right, _ := strconv.ParseInt(frame.End.Offset.Text, 10, 64)
	if frame.Start.Kind == WindowPreceding {
		if left < right {
			return p.errfAt(frame.End.Pos, "a %s frame end cannot precede its start", unitName)
		}
	} else if left > right {
		return p.errfAt(frame.End.Pos, "a %s frame end cannot precede its start", unitName)
	}
	return nil
}

func staticWindowBoundKind(bound WindowFrameBound) (WindowFrameBoundKind, bool) {
	if bound.Kind != WindowPreceding && bound.Kind != WindowFollowing {
		return bound.Kind, false
	}
	if bound.Offset.Kind == OperandParam {
		return bound.Kind, true
	}
	if bound.Offset.Kind == OperandNumber && sqlNumberTextIsZero(bound.Offset.Text) {
		return WindowCurrentRow, false
	}
	return bound.Kind, false
}

func sqlNumberTextIsZero(text string) bool {
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case 'e', 'E':
			return true
		case '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return false
		}
	}
	return true
}

type sqlNumberView struct {
	mantissa string
	first    int
	digits   int
	weight   int64
	zero     bool
}

// compareNonNegativeSQLNumbers compares two admitted exact SQL/JSON numeric
// spellings without allocating. known is false only when an exponent is too
// wide for the compact comparison metadata; the physical decimal comparator
// remains authoritative for that cold extreme at bind time.
func compareNonNegativeSQLNumbers(left, right string) (comparison int, known bool) {
	a, ok := parseSQLNumberView(left)
	if !ok {
		return 0, false
	}
	b, ok := parseSQLNumberView(right)
	if !ok {
		return 0, false
	}
	if a.zero || b.zero {
		switch {
		case a.zero && b.zero:
			return 0, true
		case a.zero:
			return -1, true
		default:
			return 1, true
		}
	}
	if a.weight != b.weight {
		if a.weight < b.weight {
			return -1, true
		}
		return 1, true
	}
	leftAt, rightAt := a.first, b.first
	for i, n := 0, max(a.digits, b.digits); i < n; i++ {
		ld := nextSQLNumberDigit(a.mantissa, &leftAt)
		rd := nextSQLNumberDigit(b.mantissa, &rightAt)
		if ld < rd {
			return -1, true
		}
		if ld > rd {
			return 1, true
		}
	}
	return 0, true
}

func parseSQLNumberView(text string) (sqlNumberView, bool) {
	start := 0
	if len(text) != 0 && text[0] == '-' {
		if !sqlNumberTextIsZero(text) {
			return sqlNumberView{}, false
		}
		start = 1
	}
	exponentAt := len(text)
	for i := start; i < len(text); i++ {
		if text[i] == 'e' || text[i] == 'E' {
			exponentAt = i
			break
		}
	}
	exponent := int64(0)
	if exponentAt != len(text) {
		parsed, err := strconv.ParseInt(text[exponentAt+1:], 10, 64)
		if err != nil {
			return sqlNumberView{}, false
		}
		exponent = parsed
	}
	mantissa := text[start:exponentAt]
	decimalDigits, logical, firstLogical, firstByte := 0, 0, -1, -1
	seenDot := false
	for i := 0; i < len(mantissa); i++ {
		if mantissa[i] == '.' {
			seenDot = true
			continue
		}
		if !seenDot {
			decimalDigits++
		}
		if firstLogical < 0 && mantissa[i] != '0' {
			firstLogical, firstByte = logical, i
		}
		logical++
	}
	if firstLogical < 0 {
		return sqlNumberView{zero: true}, true
	}
	delta := int64(decimalDigits - firstLogical - 1)
	const maxInt64 = int64((1 << 63) - 1)
	const minInt64 = -maxInt64 - 1
	if delta > 0 && exponent > maxInt64-delta ||
		delta < 0 && exponent < minInt64-delta {
		return sqlNumberView{}, false
	}
	return sqlNumberView{
		mantissa: mantissa,
		first:    firstByte,
		digits:   logical - firstLogical,
		weight:   exponent + delta,
	}, true
}

func nextSQLNumberDigit(mantissa string, at *int) byte {
	for *at < len(mantissa) {
		digit := mantissa[*at]
		(*at)++
		if digit != '.' {
			return digit
		}
	}
	return '0'
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
	if err := p.appendFromRef(ref); err != nil {
		return err
	}
	// PostgreSQL's grammar gives explicit JOIN tighter binding than the comma
	// in a from_list. The flat public AST can represent a comma after a complete
	// joined table exactly, and a comma-only tail exactly, because both lower to
	// the existing condition-free CROSS product. It cannot yet represent the
	// right-hand join tree in `a, b JOIN c ON ...` without changing ON scope (or
	// RIGHT/FULL unmatched-row multiplicity), so retain that boundary and refuse
	// the mixed tail below rather than silently building `(a CROSS b) JOIN c`.
	afterCommaItem := false
	for {
		if p.tok.kind == tokComma {
			p.advance()
			ref, err = p.parseTableRef(JoinCross)
			if err != nil {
				return err
			}
			if p.correlation != nil {
				if err := p.rejectLateralForwardAlias(ref.Alias, len(p.from)); err != nil {
					return err
				}
			}
			if err := p.appendFromRef(ref); err != nil {
				return err
			}
			afterCommaItem = true
			continue
		}
		if afterCommaItem && p.startsExplicitJoin() {
			return newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"an explicit JOIN after a comma-separated FROM item requires PostgreSQL's right-hand join grouping, which is not supported yet; put the joined tables in a derived SELECT or write an equivalent explicit join tree",
			)
		}
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

func (p *Parser) startsExplicitJoin() bool {
	return p.atKeyword(kwJoin) || p.atKeyword(kwInner) ||
		p.atKeyword(kwLeft) || p.atKeyword(kwRight) ||
		p.atKeyword(kwFull) || p.atKeyword(kwCross)
}

func (p *Parser) appendFromRef(ref TableRef) error {
	p.from = append(p.from, ref)
	p.out.From = p.from
	if len(p.from) > maxClauseItems {
		return p.errfAt(ref.Pos, "a statement may join at most %d collections", maxClauseItems)
	}
	return nil
}

// parseJoin parses one JOIN clause, reporting true when the next token does not
// open one.
func (p *Parser) parseJoin() (bool, error) {
	join := JoinInner
	switch {
	case p.acceptKeyword(kwLeft):
		join = JoinLeft
		p.acceptKeyword(kwOuter)
	case p.acceptKeyword(kwRight):
		join = JoinRight
		p.acceptKeyword(kwOuter)
	case p.acceptKeyword(kwFull):
		join = JoinFull
		p.acceptKeyword(kwOuter)
	case p.acceptKeyword(kwCross):
		join = JoinCross
	case p.atKeyword(kwOuter):
		return false, p.errHere("OUTER must follow LEFT, RIGHT, or FULL")
	case p.atKeyword(kwNatural):
		return false, newFeatureNotSupportedError(
			p.lx.src, p.tok.pos,
			"NATURAL JOIN is not supported: schemaless documents have no declared columns to infer faithfully; write USING explicitly or write ON explicitly",
		)
	}
	if join == JoinInner {
		p.acceptKeyword(kwInner)
	}
	if !p.acceptKeyword(kwJoin) {
		if join != JoinInner {
			return false, p.errHere("expected JOIN after join type")
		}
		return true, nil
	}
	ref, err := p.parseTableRef(join)
	if err != nil {
		return false, err
	}
	if p.correlation != nil {
		if err := p.rejectLateralForwardAlias(ref.Alias, len(p.from)); err != nil {
			return false, err
		}
	}
	var cond *JoinCond
	if join != JoinCross {
		if ref.Lateral != nil && p.atKeyword(kwUsing) {
			return false, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"JOIN LATERAL ... USING is not supported yet; write the equivalent ON predicate explicitly",
			)
		}
		if p.acceptKeyword(kwUsing) {
			cond, err = p.parseUsingCond(len(p.from))
		} else {
			if err := p.expectKeyword(kwOn, "ON or USING after a JOIN"); err != nil {
				return false, err
			}
			cond, err = p.parseJoinCondWithCurrent(ref)
		}
	} else if p.atKeyword(kwOn) || p.atKeyword(kwUsing) {
		return false, p.errHere("CROSS JOIN has no ON or USING condition")
	}
	if err != nil {
		return false, err
	}
	ref.On = cond
	return false, p.appendFromRef(ref)
}

// parseJoinCondWithCurrent exposes the relation currently being joined while
// validating ON syntax. Ordinary ON paths are resolved after the complete FROM
// list is parsed, but a nested predicate subquery resolves immediately in its
// child parser and must see both the accumulated left side and this relation.
// The temporary entry is removed before returning; unsupported subqueries are
// still discarded and no executable ON expression is published for them.
func (p *Parser) parseJoinCondWithCurrent(ref TableRef) (*JoinCond, error) {
	base := len(p.from)
	p.from = append(p.from, ref)
	p.out.From = p.from
	condition, err := p.parseJoinCond()
	p.from = p.from[:base]
	p.out.From = p.from
	return condition, err
}

// parseUsingCond lowers every simple name to one explicitly bound equality.
// The names remain on the condition because SQL exposes one merged,
// unqualified output per item while retaining both qualified inputs.
func (p *Parser) parseUsingCond(joinSource int) (*JoinCond, error) {
	pos := p.tok.pos
	if err := p.expect(tokLParen, "'(' after USING"); err != nil {
		return nil, err
	}
	baseKeys := len(p.joinKeyScratch)
	baseNames := len(p.joinNameScratch)
	defer func() {
		p.joinKeyScratch = p.joinKeyScratch[:baseKeys]
		p.joinNameScratch = p.joinNameScratch[:baseNames]
	}()
	for {
		path, err := p.parsePath(false)
		if err != nil {
			return nil, err
		}
		// USING has supplied both bindings, so remove parsePath's unresolved
		// registration and clone it into the two explicit operands.
		if n := len(p.pending); n != 0 && p.pending[n-1].path == path {
			p.pending = p.pending[:n-1]
		}
		if len(path.Segments) != 1 || path.Segments[0].IsIndex {
			return nil, p.errAt(path.Pos, "USING accepts simple field names")
		}
		name := path.Segments[0].Key
		for _, previous := range p.joinNameScratch[baseNames:] {
			if previous == name {
				return nil, p.errfAt(path.Pos, "USING column %q is listed twice", name)
			}
		}
		left := p.boundPath(path, joinSource-1)
		if merged := p.priorUsingMerge(name, joinSource); merged != 0 {
			left.MergedUsing = merged
		} else if joinSource > 1 {
			return nil, p.errfAt(path.Pos,
				"USING column %q is ambiguous on the accumulated left relation; qualify an ON condition, or merge that name in an earlier USING clause",
				name)
		}
		right := p.boundPath(path, joinSource)
		p.joinKeyScratch = append(p.joinKeyScratch, JoinKeyCond{
			Left: left, Right: right, Pos: path.Pos,
		})
		p.joinNameScratch = append(p.joinNameScratch, name)
		if p.tok.kind != tokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(tokRParen, "')' after USING"); err != nil {
		return nil, err
	}
	keys := p.joinKeyScratch[baseKeys:]
	keyRun := p.keys.allocDirty(len(keys))
	copy(keyRun, keys)
	names := p.joinNameScratch[baseNames:]
	nameRun := p.names.allocDirty(len(names))
	copy(nameRun, names)
	cond := p.conds.one()
	*cond = JoinCond{
		Left: keyRun[0].Left, Right: keyRun[0].Right, Keys: keyRun,
		Using: true, UsingColumns: nameRun, Pos: pos,
	}
	return cond, nil
}

func (p *Parser) priorUsingMerge(name string, before int) int {
	for i := before - 1; i >= 1; i-- {
		cond := p.from[i].On
		if cond == nil || !cond.Using {
			continue
		}
		for _, column := range cond.UsingColumns {
			if column == name {
				return i
			}
		}
	}
	return 0
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
		lateralPos := p.tok.pos
		if join == JoinNone {
			return ref, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"a leading LATERAL item has no preceding explicit FROM source in this dialect; write CROSS JOIN LATERAL after the source it may reference",
			)
		}
		p.advance()
		if p.tok.kind != tokLParen {
			return ref, p.errHere(
				"LATERAL must be followed by a parenthesized SELECT derived table",
			)
		}
		ref.Pos = p.tok.pos
		return p.parseDerivedTableRef(ref, p.beginLateralCapture(lateralPos))
	}
	switch p.tok.kind {
	case tokIdent:
		if err := p.checkNameable("a collection name"); err != nil {
			return ref, err
		}
	case tokQuotedIdent:
	case tokLParen:
		return p.parseDerivedTableRef(ref, nil)
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
	if definition := p.lookupCTE(ref.Name); definition != nil {
		ref.Kind = RelationCTE
		ref.Query = definition.Query
	}
	return ref, p.rejectDuplicateRangeAlias(ref)
}

// parseDerivedTableRef parses the parenthesized SELECT of a derived relation.
// The nested statement uses the same child-parser path as predicate subqueries,
// which preserves independent FROM scope, arena ownership, nesting limits,
// cancellation, position rebasing, and placeholder accounting in one place.
func (p *Parser) parseDerivedTableRef(
	ref TableRef,
	capture *correlationCapture,
) (TableRef, error) {
	p.advance() // consume '('
	if !p.atKeyword(kwSelect) && !p.atKeyword(kwWith) {
		return ref, p.errHere("expected SELECT after '(' in a derived table, optionally preceded by WITH definitions")
	}
	var query *SelectStmt
	var err error
	if capture == nil {
		query, err = p.parseSubquery(false)
	} else {
		query, err = p.parseSubqueryScoped(false, &capture.scope, capture)
	}
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
	if p.tok.kind == tokLParen {
		return ref, p.rejectDerivedColumnAliasList()
	}
	ref.Kind = RelationDerived
	ref.Query = query
	ref.Alias = alias
	ref.HasAlias = true
	if capture != nil {
		ref.Lateral = p.finishLateralCapture(capture)
		if len(ref.Lateral.Bindings) != 0 &&
			(ref.Join == JoinRight || ref.Join == JoinFull) {
			kind := "RIGHT"
			if ref.Join == JoinFull {
				kind = "FULL"
			}
			return ref, p.errfAt(ref.Lateral.Bindings[0].Pos,
				"%s JOIN LATERAL cannot correlate to its left side; use INNER, CROSS, or LEFT JOIN LATERAL, or remove the outer reference",
				kind,
			)
		}
	}
	return ref, p.rejectDuplicateRangeAlias(ref)
}

func (p *Parser) beginLateralCapture(pos int) *correlationCapture {
	state := p.correlationState()
	capture := state.captures.one()
	capture.owner = p
	capture.bindingBase = len(state.bindingScratch)
	capture.referenceBase = len(state.referenceScratch)
	capture.forwardBase = len(state.forward)
	capture.source = len(p.from)
	capture.pos = pos
	capture.scope = correlationRangeScope{
		parser: p,
		limit:  len(p.from),
		outer:  state.outerRanges,
	}
	return capture
}

func (p *Parser) finishLateralCapture(capture *correlationCapture) *LateralSpec {
	return p.finishCorrelationCapture(capture, true, true)
}

// finishCorrelationCapture freezes one capture into exact first-reference
// order. keepEmpty is reserved for authored LATERAL: predicate subqueries use
// a nil sidecar when no outer path was captured. keepForward preserves the
// candidates a LATERAL body may have named before their outer JOIN is parsed.
func (p *Parser) finishCorrelationCapture(
	capture *correlationCapture,
	keepEmpty, keepForward bool,
) *CorrelationSpec {
	if capture == nil || capture.owner != p || p.correlation == nil {
		return nil
	}
	state := p.correlation
	bindings := state.bindingScratch[capture.bindingBase:]
	if len(bindings) == 0 && !keepEmpty {
		state.bindingScratch = state.bindingScratch[:capture.bindingBase]
		state.referenceScratch = state.referenceScratch[:capture.referenceBase]
		if !keepForward {
			state.forward = state.forward[:capture.forwardBase]
		}
		return nil
	}
	var run []CorrelationBinding
	if len(bindings) != 0 {
		run = state.bindings.allocDirty(len(bindings))
		for i := range bindings {
			run[i] = bindings[i].binding
			run[i].Pos = bindings[i].path.Pos
		}
		// Nested LATERAL bodies are parsed before their containing SELECT's
		// deferred paths are resolved. Sort by the first occurrence's byte
		// position so the public slot order remains lexical rather than an
		// artifact of parser traversal. The insertion sort is allocation-free
		// and correlation lists are bounded by the statement item limits.
		for i := 1; i < len(run); i++ {
			binding := run[i]
			j := i
			for j > 0 && binding.Pos < run[j-1].Pos {
				run[j] = run[j-1]
				j--
			}
			run[j] = binding
		}
	}
	references := state.referenceScratch[capture.referenceBase:]
	var referenceRun []CorrelationReference
	if len(references) != 0 {
		referenceRun = state.references.allocDirty(len(references))
		for i := range references {
			old := &bindings[references[i].binding].binding
			binding := 0
			for j := range run {
				if run[j].Depth == old.Depth && run[j].Source == old.Source &&
					sameSegments(run[j].Segments, old.Segments) {
					binding = j
					break
				}
			}
			referenceRun[i] = CorrelationReference{
				Path: references[i].path, Binding: binding,
			}
		}
	}
	spec := state.specs.one()
	spec.Bindings = run
	spec.References = referenceRun
	spec.Decorrelated = keepEmpty && len(run) == 0
	spec.Pos = capture.pos
	state.bindingScratch = state.bindingScratch[:capture.bindingBase]
	state.referenceScratch = state.referenceScratch[:capture.referenceBase]
	if !keepForward {
		state.forward = state.forward[:capture.forwardBase]
	}
	return spec
}

// rejectDerivedColumnAliasList distinguishes a valid-but-unsupported SQL
// column alias list from malformed punctuation. Refusing immediately at the
// opening parenthesis would turn d(), d(id,), and d(id name) into 0A000 even
// though those inputs are not valid instances of the feature. Walking this
// small grammar first keeps protocol classification honest and retains the
// same clause-item bound used by accepted identifier lists.
func (p *Parser) rejectDerivedColumnAliasList() error {
	pos := p.tok.pos
	p.advance() // consume '('
	items := 0
	for {
		switch p.tok.kind {
		case tokError:
			return p.errHere(p.tok.text)
		case tokIdent, tokQuotedIdent:
			if p.tok.text == "" {
				return p.errHere("a derived-table column alias may not be empty")
			}
		default:
			return p.errHere("expected a column name in the derived-table alias list")
		}
		items++
		if items > maxClauseItems {
			return p.errfAt(pos,
				"a derived-table alias list may hold at most %d columns", maxClauseItems)
		}
		p.advance()
		if p.tok.kind == tokRParen {
			return newFeatureNotSupportedError(
				p.lx.src,
				pos,
				"derived-table column alias lists are not supported yet; name each inner SELECT output with AS instead",
			)
		}
		if p.tok.kind != tokComma {
			return p.errHere("expected ',' or ')' after a derived-table column alias")
		}
		p.advance()
	}
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

// parseJoinCond parses the complete ON predicate. Resolution later extracts
// every top-level cross-input equality as a composite hash key; the remaining
// expression is evaluated as a residual during pair formation.
func (p *Parser) parseJoinCond() (*JoinCond, error) {
	pos := p.tok.pos
	expr, err := p.parseExpr(ctxJoin)
	if err != nil {
		return nil, err
	}
	cond := p.conds.one()
	*cond = JoinCond{Expr: expr, Pos: pos}
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

func (p *Parser) parseOrderBy(allowOutputPosition bool) error {
	if !p.acceptKeyword(kwOrder) {
		return nil
	}
	if err := p.expectKeyword(kwBy, "BY after ORDER"); err != nil {
		return err
	}
	deferredKnown := false
	deferredOutputArity := false
	for {
		if allowOutputPosition && p.tok.kind == tokNumber && !deferredKnown {
			var err error
			deferredOutputArity, err = p.selectOutputArityDeferred()
			if err != nil {
				return err
			}
			deferredKnown = true
		}
		term, err := p.parseOrderTerm(allowOutputPosition, deferredOutputArity)
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

func (p *Parser) parseOrderTerm(
	allowOutputPosition bool,
	deferredOutputArity bool,
) (OrderTerm, error) {
	term := OrderTerm{Pos: p.tok.pos}
	if allowOutputPosition && p.tok.kind == tokMinus {
		p.advance()
		if p.tok.kind == tokNumber {
			return term, newInvalidOrderPositionError(
				p.lx.src, term.Pos, "-"+p.tok.text, len(p.columns),
			)
		}
		return term, newFeatureNotSupportedError(
			p.lx.src, term.Pos,
			"computed scalar expressions in ORDER BY require a post-scalar stable sort stage",
		)
	}
	if p.tok.kind == tokNumber {
		if !allowOutputPosition {
			return term, p.errHere("ORDER BY does not accept an output position in a mutation; name the input path")
		}
		text := p.tok.text
		position, err := strconv.ParseUint(text, 10, 63)
		if err != nil || position == 0 {
			return term, newInvalidOrderPositionError(
				p.lx.src, p.tok.pos, text, len(p.columns),
			)
		}
		if deferredOutputArity {
			return term, newFeatureNotSupportedError(
				p.lx.src, term.Pos,
				"ORDER BY output positions cannot bind a wildcard whose expanded SELECT-list width is known only at prepare time",
			)
		}
		if position > uint64(len(p.columns)) {
			return term, newInvalidOrderPositionError(
				p.lx.src, p.tok.pos, text, len(p.columns),
			)
		}
		term.Output = int(position)
		p.advance()
		if scalarContinues(p.tok) {
			return term, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"computed scalar expressions in ORDER BY require a post-scalar stable sort stage; a numeric position must stand alone",
			)
		}
	} else {
		if p.tok.kind == tokPlus || p.tok.kind == tokMinus || p.tok.kind == tokLParen ||
			p.atKeyword(kwCase) || p.atKeyword(kwCast) {
			return term, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"computed scalar expressions in ORDER BY require a post-scalar stable sort stage",
			)
		}
		path, err := p.parseKeyPath(
			"ORDER BY cannot sort by an aggregate: the engine orders groups by their key, not by their reduction")
		if err != nil {
			return term, err
		}
		if scalarContinues(p.tok) {
			return term, newFeatureNotSupportedError(
				p.lx.src, p.tok.pos,
				"computed scalar expressions in ORDER BY require a post-scalar stable sort stage",
			)
		}
		term.Path, term.Output, err = p.resolveOrderAlias(path)
		if err != nil {
			return term, err
		}
	}
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

func (p *Parser) selectOutputArityDeferred() (bool, error) {
	for i := range p.columns {
		if err := p.checkCancellation(); err != nil {
			return false, err
		}
		if p.orderOutputArityDeferred(&p.columns[i]) {
			return true, nil
		}
	}
	return false, nil
}

func (p *Parser) orderOutputArityDeferred(column *ResultColumn) bool {
	if column == nil || column.Path == nil || column.Agg != AggNone {
		return false
	}
	if len(column.Path.Segments) == 0 {
		return true
	}
	for i := range p.pending {
		if p.pending[i].path == column.Path {
			return p.pending[i].star
		}
	}
	return false
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
func (p *Parser) resolveOrderAlias(path *PathExpr) (*PathExpr, int, error) {
	last := len(p.pending) - 1
	if last < 0 || p.pending[last].path != path {
		return path, 0, nil
	}
	if p.pending[last].eligible || len(path.Segments) != 1 {
		return path, 0, nil
	}
	name := path.Segments[0].Key
	match := -1
	for i := range p.columns {
		if p.columns[i].Alias != name {
			continue
		}
		if match >= 0 {
			return nil, 0, newAmbiguousOutputError(p.lx.src, path.Pos, name)
		}
		match = i
	}
	if match < 0 {
		return path, 0, nil
	}
	p.pending = p.pending[:last]
	column := &p.columns[match]
	if column.Window != nil || column.Scalar != nil || column.Agg != AggNone {
		return nil, match + 1, nil
	}
	if column.Path != nil {
		return column.Path, 0, nil
	}
	return path, 0, nil
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
	if head.kind != tokQuotedIdent && !head.esc && head.text == DocumentColumn {
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
	document := false
	qualifiedFieldPos := -1
	nestedPos := -1
	if head.kind == tokQuotedIdent && !head.esc && head.text == DocumentColumn {
		segs = segs[:0]
		eligible, document = false, true
	}

loop:
	for {
		switch p.tok.kind {
		case tokDot:
			if nestedPos < 0 && len(segs) >= 2 {
				nestedPos = p.tok.pos
			}
			p.advance()
			if len(segs) == 1 && eligible && !document && p.tok.kind == tokQuotedIdent && !p.tok.esc && p.tok.text == DocumentColumn {
				qualifiedFieldPos = p.tok.pos
				p.advance()
				star, document = true, true
				continue
			}
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
			if qualifiedFieldPos < 0 && len(segs) == 1 && eligible {
				qualifiedFieldPos = p.tok.pos
			}
			segs = append(segs, Segment{Key: p.internToken(p.tok)})
			p.advance()
		case tokJSONArrow:
			if nestedPos < 0 && len(segs) >= 2 {
				nestedPos = p.tok.pos
			}
			p.advance()
			seg, err := p.parseJSONAccessor()
			if err != nil {
				return nil, err
			}
			segs = append(segs, seg)
		case tokLBracket:
			if nestedPos < 0 && len(segs) >= 2 {
				nestedPos = p.tok.pos
			}
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
	path := p.newPath(head.pos, segs, eligible, star)
	p.pending[len(p.pending)-1].document = document && ((!eligible && len(segs) == 0) || (eligible && len(segs) == 1))
	p.pending[len(p.pending)-1].documentRoot = document
	p.pending[len(p.pending)-1].quoted = head.kind == tokQuotedIdent
	if qualifiedFieldPos >= 0 {
		p.pending[len(p.pending)-1].qualifiedFieldPos = qualifiedFieldPos + 1
	}
	if nestedPos >= 0 {
		p.pending[len(p.pending)-1].nestedPos = nestedPos + 1
	}
	return path, nil
}

// JSON object accessors are compiled exactly like native path segments.
// Numeric keys and array indexes are refused: RFC 6901 paths intentionally
// unify those spellings, whereas PostgreSQL distinguishes ->'0' from ->0.
// Silently accepting them would return an array element for an object key.
func (p *Parser) parseJSONAccessor() (Segment, error) {
	if p.tok.kind == tokString {
		key := p.internToken(p.tok)
		numeric := len(key) != 0
		for i := 0; i < len(key) && numeric; i++ {
			numeric = key[i] >= '0' && key[i] <= '9'
		}
		if !numeric {
			p.advance()
			return Segment{Key: key}, nil
		}
	}
	return Segment{}, newFeatureNotSupportedError(p.lx.src, p.tok.pos,
		"JSON -> and ->> require a constant non-numeric object key; array indexes, numeric keys, and dynamic keys are not supported")
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
