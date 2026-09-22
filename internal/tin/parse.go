package tin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// TINQL surface syntax. ParseTINQL compiles the query language documented by
// PlanetScale's TIN — terms, wildcards, fuzzy match, ranges, MATCHES,
// phrases with gaps and per-position alternatives, boolean operators,
// AT LEAST/ALL OF alternatives, THEN/NEAR/WITHIN proximity, span relations,
// and IN positional filters — down to the evaluator AST.
//
// Grammar notes where TIN leaves room: brackets never admit implicit AND
// (whitespace separates alternatives there); postfix `^`/`~` bind without
// intervening space; `,` always errors (alternatives are whitespace
// separated); keywords are strictly UPPER CASE, so lowercase spellings stay
// searchable terms. Expansion operators resolve against the
// index dictionary at parse time, so evaluation never sees surface syntax.
// Parsing allocates per query shape; evaluation over the result stays
// steady-state.

// ParseTINQL compiles input to a Query. Inputs that analyze to no tokens
// (empty, whitespace, bare punctuation without syntax) yield an empty OpOr
// that matches nothing, per TIN's rule; explicit empty syntax (`""`, `[]`)
// is a parse error.
func (ix *Index) ParseTINQL(input string) (Query, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	p := &tinParser{ix: ix, s: input}
	p.skipWS()
	if p.eof() {
		return Query{Op: OpOr}, nil
	}
	if !hasSyntax(input) {
		var n int
		scanString(input, func(uint64, uint32) { n++ })
		if n == 0 {
			return Query{Op: OpOr}, nil
		}
	}
	q, err := p.parseOr()
	if err != nil {
		return Query{}, err
	}
	p.skipWS()
	if !p.eof() {
		return Query{}, p.errorf("unexpected trailing input")
	}
	return q, nil
}

// hasSyntax reports whether input holds any TINQL syntax character; without
// one, a token-free input matches nothing instead of erroring. `;` and `,`
// count as syntax: neither ever stands alone validly, so a bare `;` (or
// stray `,`) reaches the real parser and errors instead of leniently
// matching nothing.
func hasSyntax(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"', '(', ')', '[', ']', '~', '^', '*', '?', ';', ',':
			return true
		}
	}
	return false
}

type tinParser struct {
	ix  *Index
	s   string
	pos int
	// noImplicit disables juxtaposition-AND inside [...] lists, where
	// whitespace separates alternatives instead.
	noImplicit bool
	// implicitOperand marks the juxtaposition-AND path of parseAnd: the
	// operand arrived with no explicit operator. A term starting with
	// ':' is rejected there (it needs an explicit operator after
	// another expression); explicit positions allow it.
	implicitOperand bool
}

// rejectSpanStar errors when a span, relation, or positional operator
// takes the document-level match-all as a direct operand.
func (p *tinParser) rejectSpanStar(kids ...Query) error {
	for _, k := range kids {
		if k.Op == OpAll {
			return p.errorf("MatchAll (*) is not valid inside a span/positional context")
		}
	}
	return nil
}

func (p *tinParser) errorf(format string, args ...interface{}) error {
	return fmt.Errorf("tinql at byte %d: %s", p.pos, fmt.Sprintf(format, args...))
}

func (p *tinParser) eof() bool { return p.pos >= len(p.s) }

func (p *tinParser) skipWS() {
	for p.pos < len(p.s) {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			p.pos++
		default:
			return
		}
	}
}

// isDelim reports TINQL word boundaries: whitespace, structural characters,
// and the postfix/prefix operators. Slash stays a word character (`and/or`
// is one term), as do `%` and digit-flanked commas (`100%`, `47,000`);
// a bare semicolon is refused outright.
func isDelim(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\v', '\f', '"', '(', ')', '[', ']', '~', '^', ';':
		return true
	}
	return false
}

// isDigit reports ASCII digits for the numeric-comma rule.
func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// scanWord consumes one raw word (escapes intact) or reports an error for
// stray delimiters.
func (p *tinParser) scanWord() (string, error) {
	if p.eof() {
		return "", p.errorf("expected a term")
	}
	c := p.s[p.pos]
	if c == ';' || c == ',' {
		// A word never starts with a bare separator: digit-flanked
		// commas join inside the loop below.
		return "", p.errorf("unexpected %q", string(c))
	}
	if isDelim(c) {
		return "", p.errorf("unexpected %q", string(c))
	}
	start := p.pos
	for p.pos < len(p.s) && !isDelim(p.s[p.pos]) {
		if p.s[p.pos] == ',' &&
			!(p.pos > start && isDigit(p.s[p.pos-1]) && p.pos+1 < len(p.s) && isDigit(p.s[p.pos+1])) {
			// A comma between two digits is a numeric separator
			// (`47,000`); anywhere else it ends the word (an
			// alternatives separator inside [...], an error outside).
			break
		}
		if p.s[p.pos] == '\\' {
			p.pos++
			if p.pos >= len(p.s) {
				break // trailing backslash stays literal
			}
		}
		p.pos++
	}
	return p.s[start:p.pos], nil
}

// peekWord scans the next word without consuming it.
func (p *tinParser) peekWord() (string, bool) {
	save := p.pos
	p.skipWS()
	if p.eof() {
		p.pos = save
		return "", false
	}
	c := p.s[p.pos]
	if isDelim(c) || c == ';' || c == ',' {
		p.pos = save
		return "", false
	}
	start := p.pos
	for p.pos < len(p.s) && !isDelim(p.s[p.pos]) {
		if p.s[p.pos] == ',' &&
			!(p.pos > start && isDigit(p.s[p.pos-1]) && p.pos+1 < len(p.s) && isDigit(p.s[p.pos+1])) {
			break
		}
		if p.s[p.pos] == '\\' {
			p.pos++
			if p.pos >= len(p.s) {
				break
			}
		}
		p.pos++
	}
	w := p.s[start:p.pos]
	p.pos = save
	return w, true
}

// isKeyword reports an exact UPPER CASE keyword (escape-free, since an
// escaped spelling is a term by construction).
func isKeyword(w, kw string) bool {
	if strings.Contains(w, "\\") {
		return false
	}
	return w == kw
}

// unescapeWord resolves `\` escapes and reports bare (active) `*`/`?`
// markers. A backslash escapes `*`, `?`, and itself; before anything
// else it stays literal (`foo\bar`, `C:\path`). A trailing backslash
// stays literal too.
func unescapeWord(raw string) (unescaped string, sawWild bool, err error) {
	var sb []byte
	for i := 0; i < len(raw); {
		c := raw[i]
		if c != '\\' {
			if c == '*' || c == '?' {
				sawWild = true
			}
			sb = append(sb, c)
			i++
			continue
		}
		i++
		if i >= len(raw) {
			sb = append(sb, '\\')
			break
		}
		e := raw[i]
		if e == '*' || e == '?' || e == '\\' {
			sb = append(sb, e)
			i++
			continue
		}
		sb = append(sb, '\\', e)
		i++
	}
	return string(sb), sawWild, nil
}

// foldTokens folds raw term text to its token hashes.
func foldTokens(raw string) []uint64 {
	var out []uint64
	scanString(raw, func(h uint64, _ uint32) { out = append(out, h) })
	return out
}

// parseOr parses A OR B (left associative, flattening unboosted ORs).
func (p *tinParser) parseOr() (Query, error) {
	left, err := p.parseAnd()
	if err != nil {
		return Query{}, err
	}
	for {
		save := p.pos
		p.skipWS()
		w, ok := p.peekWord()
		if !ok || !isKeyword(w, "OR") {
			p.pos = save
			return left, nil
		}
		p.skipWS()
		p.scanWord()
		right, err := p.parseAnd()
		if err != nil {
			return Query{}, err
		}
		left = flattenOr(left, right)
	}
}

// flattenOr combines ORs, merging an unboosted OR kid into its parent.
func flattenOr(left, right Query) Query {
	if left.Op == OpOr && left.Boost == 0 {
		if right.Op == OpOr && right.Boost == 0 {
			left.Kids = append(left.Kids, right.Kids...)
			return left
		}
		left.Kids = append(left.Kids, right)
		return left
	}
	if right.Op == OpOr && right.Boost == 0 {
		right.Kids = append([]Query{left}, right.Kids...)
		return right
	}
	return Query{Op: OpOr, Kids: []Query{left, right}}
}

// parseAnd parses explicit and implicit AND (same level, left associative).
func (p *tinParser) parseAnd() (Query, error) {
	left, err := p.parseAndNot()
	if err != nil {
		return Query{}, err
	}
	for {
		save := p.pos
		p.skipWS()
		if w, ok := p.peekWord(); ok && isKeyword(w, "AND") {
			p.skipWS()
			p.scanWord() // AND
			p.skipWS()
			if w2, ok2 := p.peekWord(); ok2 && isKeyword(w2, "NOT") {
				// Composite left with AND NOT: fold tight, as one level
				// down would (reached here after implicit AND).
				p.skipWS()
				p.scanWord() // NOT
				right, err := p.parseFilter()
				if err != nil {
					return Query{}, err
				}
				left = flattenAndNot(left, right)
				continue
			}
			right, err := p.parseAndNot()
			if err != nil {
				return Query{}, err
			}
			left = flattenAnd(left, right)
			continue
		}
		p.pos = save
		if p.noImplicit || p.noImplicitAhead() {
			return left, nil
		}
		p.implicitOperand = true
		right, err := p.parseAndNot()
		p.implicitOperand = false
		if err != nil {
			return Query{}, err
		}
		left = flattenAnd(left, right)
	}
}

// flattenAnd combines ANDs, merging unboosted AND kids.
func flattenAnd(left, right Query) Query {
	if left.Op == OpAnd && left.Boost == 0 {
		if right.Op == OpAnd && right.Boost == 0 {
			left.Kids = append(left.Kids, right.Kids...)
			return left
		}
		left.Kids = append(left.Kids, right)
		return left
	}
	if right.Op == OpAnd && right.Boost == 0 {
		right.Kids = append([]Query{left}, right.Kids...)
		return right
	}
	return Query{Op: OpAnd, Kids: []Query{left, right}}
}

// noImplicitAhead reports that no implicit-AND operand follows: end of
// input, a closing bracket, or a continuation keyword/operator.
func (p *tinParser) noImplicitAhead() bool {
	save := p.pos
	p.skipWS()
	defer func() { p.pos = save }()
	if p.eof() {
		return true
	}
	switch p.s[p.pos] {
	case ')', ']', '~', '^', ';':
		return true
	}
	w, ok := p.peekWord()
	if !ok {
		return true
	}
	switch {
	case isKeyword(w, "OR") || isKeyword(w, "AND") || isKeyword(w, "NOT") ||
		isKeyword(w, "THEN") || isKeyword(w, "NEAR") || isKeyword(w, "WITHIN") ||
		isKeyword(w, "IN") || isKeyword(w, "TO") || isKeyword(w, "ENCLOSES") ||
		isKeyword(w, "ENCLOSED") || isKeyword(w, "OVERLAPPING") || isKeyword(w, "BEFORE") ||
		isKeyword(w, "AFTER") || isKeyword(w, "OF") ||
		isKeyword(w, "LEAST") || isKeyword(w, "FIRST") || isKeyword(w, "LAST") ||
		isKeyword(w, "MIDDLE") || isKeyword(w, "WORDS"):
		return true
	}
	return false
}

// flattenAndNot collects AND NOT right sides n-ary when unboosted.
func flattenAndNot(left, right Query) Query {
	if left.Op == OpAndNot && left.Boost == 0 {
		left.Kids = append(left.Kids, right)
		return left
	}
	return Query{Op: OpAndNot, Kids: []Query{left, right}}
}

// parseAndNot parses A AND NOT B (left associative, collected n-ary).
func (p *tinParser) parseAndNot() (Query, error) {
	left, err := p.parseFilter()
	if err != nil {
		return Query{}, err
	}
	for {
		save := p.pos
		p.skipWS()
		w, ok := p.peekWord()
		if !ok || !isKeyword(w, "AND") {
			p.pos = save
			return left, nil
		}
		p.skipWS()
		p.scanWord() // AND
		p.skipWS()
		w2, ok2 := p.peekWord()
		if !ok2 || !isKeyword(w2, "NOT") {
			// Plain AND belongs to the looser level: rewind and let the
			// caller combine it.
			p.pos = save
			return left, nil
		}
		p.skipWS()
		p.scanWord() // NOT
		right, err := p.parseFilter()
		if err != nil {
			return Query{}, err
		}
		left = flattenAndNot(left, right)
	}
}

// parseFilter parses a postfix IN filter.
func (p *tinParser) parseFilter() (Query, error) {
	left, err := p.parseRel()
	if err != nil {
		return Query{}, err
	}
	save := p.pos
	p.skipWS()
	w, ok := p.peekWord()
	if !ok || !isKeyword(w, "IN") {
		p.pos = save
		return left, nil
	}
	p.skipWS()
	p.scanWord() // IN
	spec, err := p.parseFilterSpec()
	if err != nil {
		return Query{}, err
	}
	if err := p.rejectSpanStar(left); err != nil {
		return Query{}, err
	}
	return Query{Op: OpFilter, Kids: []Query{left}, Filter: spec}, nil
}

// parseFilterSpec parses FIRST/LAST/MIDDLE/WORDS windows after IN.
func (p *tinParser) parseFilterSpec() (FilterSpec, error) {
	p.skipWS()
	w, err := p.scanWord()
	if err != nil {
		return FilterSpec{}, err
	}
	switch {
	case isKeyword(w, "FIRST") || isKeyword(w, "LAST"):
		first := isKeyword(w, "FIRST")
		n, err := p.scanDigits("count")
		if err != nil {
			return FilterSpec{}, err
		}
		p.skipWS()
		if p.pos < len(p.s) && p.s[p.pos] == '%' {
			p.pos++
			if first {
				return FilterSpec{Kind: FilterFirstPct, N: n}, nil
			}
			return FilterSpec{Kind: FilterLastPct, N: n}, nil
		}
		w2, err := p.scanWord()
		if err != nil || !isKeyword(w2, "WORDS") {
			return FilterSpec{}, p.errorf("expected %% or WORDS after IN FIRST/LAST count")
		}
		if first {
			return FilterSpec{Kind: FilterFirstWords, N: n}, nil
		}
		return FilterSpec{Kind: FilterLastWords, N: n}, nil
	case isKeyword(w, "MIDDLE"):
		n, err := p.scanDigits("percent")
		if err != nil {
			return FilterSpec{}, err
		}
		p.skipWS()
		if p.pos >= len(p.s) || p.s[p.pos] != '%' {
			return FilterSpec{}, p.errorf("expected %% after IN MIDDLE percent")
		}
		p.pos++
		return FilterSpec{Kind: FilterMiddlePct, N: n}, nil
	case isKeyword(w, "WORDS"):
		x, err := p.scanDigits("start")
		if err != nil {
			return FilterSpec{}, err
		}
		p.skipWS()
		w2, err := p.scanWord()
		if err != nil || !isKeyword(w2, "TO") {
			return FilterSpec{}, p.errorf("expected TO in IN WORDS range")
		}
		y, err := p.scanDigits("end")
		if err != nil {
			return FilterSpec{}, err
		}
		// Query positions are 1-based; evaluation windows are 0-based
		// [Lo, Hi+1), so IN WORDS x TO y keeps tokens x-1..y-1. The
		// evaluator clamps both ends into the document.
		return FilterSpec{Kind: FilterWords, Lo: x - 1, Hi: y - 1}, nil
	default:
		return FilterSpec{}, p.errorf("expected FIRST, LAST, MIDDLE, or WORDS after IN")
	}
}

// parseRel parses span relations (left associative).
func (p *tinParser) parseRel() (Query, error) {
	left, err := p.parseProx()
	if err != nil {
		return Query{}, err
	}
	for {
		save := p.pos
		p.skipWS()
		w, ok := p.peekWord()
		if !ok {
			p.pos = save
			return left, nil
		}
		var op Op
		neg := false
		words := 1 // operator words to consume below
		switch {
		case isKeyword(w, "ENCLOSES"):
			op = OpEncloses
		case isKeyword(w, "ENCLOSED"):
			if !p.peekSecond("BY") {
				return Query{}, p.errorf("expected BY after ENCLOSED")
			}
			op = OpEnclosedBy
			words = 2
		case isKeyword(w, "OVERLAPPING"):
			op = OpOverlapping
		case isKeyword(w, "BEFORE"):
			op = OpBefore
		case isKeyword(w, "AFTER"):
			op = OpAfter
		case isKeyword(w, "NOT"):
			p.skipWS()
			p.scanWord() // NOT
			p.skipWS()
			w2, err := p.scanWord()
			if err != nil {
				return Query{}, err
			}
			switch {
			case isKeyword(w2, "ENCLOSES"):
				op, neg = OpEncloses, true
			case isKeyword(w2, "ENCLOSED"):
				p.skipWS()
				w3, err := p.scanWord()
				if err != nil || !isKeyword(w3, "BY") {
					return Query{}, p.errorf("expected BY after NOT ENCLOSED")
				}
				op, neg = OpEnclosedBy, true
			case isKeyword(w2, "OVERLAPPING"):
				op, neg = OpOverlapping, true
			default:
				return Query{}, p.errorf("NOT combines only with AND, ENCLOSES, ENCLOSED BY, or OVERLAPPING")
			}
			words = 0 // NOT form already consumed
		default:
			p.pos = save
			return left, nil
		}
		for ; words > 0; words-- {
			p.skipWS()
			if _, err := p.scanWord(); err != nil {
				return Query{}, err
			}
		}
		right, err := p.parseProx()
		if err != nil {
			return Query{}, err
		}
		if err := p.rejectSpanStar(left, right); err != nil {
			return Query{}, err
		}
		left = Query{Op: op, Kids: []Query{left, right}, Neg: neg}
	}
}

// peekSecond reports whether the word after the next is kw.
func (p *tinParser) peekSecond(kw string) bool {
	save := p.pos
	p.skipWS()
	if _, err := p.scanWord(); err != nil {
		p.pos = save
		return false
	}
	p.skipWS()
	w, ok := p.peekWord()
	p.pos = save
	return ok && isKeyword(w, kw)
}

// splitAttachedProx splits THEN/N and NEAR/N written without spaces (`/`
// stays a word character so `and/or` remains one term). Only an exact
// keyword prefix with an all-digit gap qualifies.
func splitAttachedProx(w string) (ordered bool, n int, ok bool) {
	var rest string
	if len(w) > 5 && w[:5] == "THEN/" {
		ordered, rest = true, w[5:]
	} else if len(w) > 5 && w[:5] == "NEAR/" {
		rest = w[5:]
	} else {
		return false, 0, false
	}
	for i := 0; i < len(rest); i++ {
		if rest[i] < '0' || rest[i] > '9' {
			return false, 0, false
		}
	}
	v, err := strconv.Atoi(rest)
	if err != nil {
		return false, 0, false
	}
	return ordered, v, true
}

// parseProx parses THEN/N and NEAR/N (left associative), attached or spaced.
func (p *tinParser) parseProx() (Query, error) {
	left, err := p.parseWithin()
	if err != nil {
		return Query{}, err
	}
	for {
		save := p.pos
		p.skipWS()
		w, ok := p.peekWord()
		var ordered bool
		var n int
		switch {
		case ok && (isKeyword(w, "THEN") || isKeyword(w, "NEAR")):
			ordered = isKeyword(w, "THEN")
			p.skipWS()
			p.scanWord()
			p.skipWS()
			if p.eof() || p.s[p.pos] != '/' {
				return Query{}, p.errorf("expected /N after THEN/NEAR")
			}
			p.pos++
			n, err = p.scanDigits("gap")
			if err != nil {
				return Query{}, err
			}
		case ok:
			o, v, good := splitAttachedProx(w)
			if !good {
				p.pos = save
				return left, nil
			}
			ordered, n = o, v
			p.skipWS()
			if _, err := p.scanWord(); err != nil {
				return Query{}, err
			}
		default:
			p.pos = save
			return left, nil
		}
		right, err := p.parseWithin()
		if err != nil {
			return Query{}, err
		}
		if err := p.rejectSpanStar(left, right); err != nil {
			return Query{}, err
		}
		if ordered {
			left = Query{Op: OpThen, Kids: []Query{left, right}, Dist: n}
		} else {
			left = Query{Op: OpNear, Kids: []Query{left, right}, Dist: n}
		}
	}
}

// parseWithin parses a postfix WITHIN N.
func (p *tinParser) parseWithin() (Query, error) {
	node, err := p.parseBoost()
	if err != nil {
		return Query{}, err
	}
	save := p.pos
	p.skipWS()
	w, ok := p.peekWord()
	if !ok || !isKeyword(w, "WITHIN") {
		p.pos = save
		return node, nil
	}
	p.skipWS()
	p.scanWord()
	n, err := p.scanDigits("width")
	if err != nil {
		return Query{}, err
	}
	if err := p.rejectSpanStar(node); err != nil {
		return Query{}, err
	}
	return Query{Op: OpWithin, Kids: []Query{node}, Dist: n}, nil
}

// parseBoost parses postfix ^N boosts.
func (p *tinParser) parseBoost() (Query, error) {
	node, err := p.parsePrimary()
	if err != nil {
		return Query{}, err
	}
	for !p.eof() && p.s[p.pos] == '^' {
		p.pos++
		v, err := p.scanBoost()
		if err != nil {
			return Query{}, err
		}
		node.Boost = float32(v)
	}
	return node, nil
}

// scanBoost parses a boost factor in [0.0, 10000.0].
func (p *tinParser) scanBoost() (float64, error) {
	p.skipWS()
	start := p.pos
	for p.pos < len(p.s) && (p.s[p.pos] == '.' || (p.s[p.pos] >= '0' && p.s[p.pos] <= '9')) {
		p.pos++
	}
	v, err := strconv.ParseFloat(p.s[start:p.pos], 64)
	if err != nil || p.pos == start {
		return 0, p.errorf("expected a boost factor after ^")
	}
	if v < 0 || v > 10000 {
		return 0, p.errorf("boost %v out of range [0.0, 10000.0]", v)
	}
	return v, nil
}

// scanDigits parses a non-negative integer.
func (p *tinParser) scanDigits(what string) (int, error) {
	p.skipWS()
	start := p.pos
	for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
		p.pos++
	}
	if p.pos == start {
		return 0, p.errorf("expected %s", what)
	}
	v, err := strconv.Atoi(p.s[start:p.pos])
	if err != nil {
		return 0, p.errorf("bad %s", what)
	}
	return v, nil
}

// parsePrimary parses terms, phrases, brackets, groups, MATCHES, CONTAINS,
// AT LEAST/ALL OF, and `*`.
func (p *tinParser) parsePrimary() (Query, error) {
	p.skipWS()
	if p.eof() {
		return Query{}, p.errorf("expected a query term")
	}
	switch p.s[p.pos] {
	case '(':
		p.pos++
		q, err := p.parseOr()
		if err != nil {
			return Query{}, err
		}
		p.skipWS()
		if p.eof() || p.s[p.pos] != ')' {
			return Query{}, p.errorf("expected )")
		}
		p.pos++
		return q, nil
	case '[':
		return p.parseAlternatives()
	case '"':
		return p.parsePhrase()
	case '~', '^', '%', ',', ';', ')', ']':
		return Query{}, p.errorf("unexpected %q", string(p.s[p.pos]))
	}
	w, err := p.scanWord()
	if err != nil {
		return Query{}, err
	}
	switch {
	case w == "*":
		return p.parseStar()
	case isKeyword(w, "MATCHES"):
		return p.parseMatches()
	case isKeyword(w, "CONTAINS"):
		return p.parseContains()
	case isKeyword(w, "AT"):
		return p.parseAtLeast()
	case isKeyword(w, "ALL"):
		return p.parseAllOf()
	case isKeyword(w, "OR") || isKeyword(w, "AND") || isKeyword(w, "NOT") ||
		isKeyword(w, "THEN") || isKeyword(w, "NEAR") || isKeyword(w, "WITHIN") ||
		isKeyword(w, "IN") || isKeyword(w, "TO") || isKeyword(w, "ENCLOSES") ||
		isKeyword(w, "ENCLOSED") || isKeyword(w, "OVERLAPPING") || isKeyword(w, "BEFORE") ||
		isKeyword(w, "AFTER") || isKeyword(w, "OF") ||
		isKeyword(w, "LEAST") || isKeyword(w, "FIRST") || isKeyword(w, "LAST") ||
		isKeyword(w, "MIDDLE") || isKeyword(w, "WORDS"):
		return Query{}, p.errorf("unexpected keyword %q here", w)
	default:
		return p.parseBareTerm(w)
	}
}

// parseStar handles standalone `*` (match-all) and `* TO B` open ranges.
func (p *tinParser) parseStar() (Query, error) {
	save := p.pos
	p.skipWS()
	if w, ok := p.peekWord(); ok && isKeyword(w, "TO") {
		p.skipWS()
		p.scanWord() // TO
		hi, err := p.parseRangeBound()
		if err != nil {
			return Query{}, err
		}
		return p.rangeQuery(nil, hi)
	}
	p.pos = save
	return Query{Op: OpAll}, nil
}

// parseBareTerm handles plain terms, wildcards, fuzzy match, implicit
// phrases (`wi-fi`), and `A TO B` ranges.
func (p *tinParser) parseBareTerm(w string) (Query, error) {
	if p.implicitOperand && strings.HasPrefix(w, ":") {
		return Query{}, p.errorf("a term starting with ':' needs an explicit operator after another expression")
	}
	if _, _, attached := splitAttachedProx(w); attached {
		return Query{}, p.errorf("proximity operator needs a left operand")
	}
	if strings.HasPrefix(w, "THEN/") || strings.HasPrefix(w, "NEAR/") {
		return Query{}, p.errorf("malformed proximity gap in %q", w)
	}
	unescaped, sawWild, err := unescapeWord(w)
	if err != nil {
		return Query{}, err
	}
	// A following TO makes this word a range bound, addressed as a
	// dictionary spelling without tokenizing (so `1,000` stays one
	// bound); a wildcard word is refused as a bound here.
	save := p.pos
	p.skipWS()
	if w2, ok := p.peekWord(); ok && isKeyword(w2, "TO") {
		if sawWild {
			return Query{}, p.errorf("range bounds must be exact terms")
		}
		if _, n := spellOf(unescaped); n == 0 {
			return Query{}, p.errorf("range bound %q is erased by the analyzer", w)
		}
		p.skipWS()
		p.scanWord() // TO
		hi, err := p.parseRangeBound()
		if err != nil {
			return Query{}, err
		}
		lo := foldPattern(unescaped)
		return p.rangeQuery(&lo, hi)
	}
	p.pos = save
	if sawWild {
		folded := foldPattern(unescaped)
		hashes, err := p.ix.expandWildcard(folded)
		if err != nil {
			return Query{}, err
		}
		if !p.eof() && p.s[p.pos] == '~' {
			return Query{}, p.errorf("fuzzy ~ needs an exact term, not a wildcard")
		}
		return orOf(hashes), nil
	}
	hashes := foldTokens(unescaped)
	switch len(hashes) {
	case 0:
		return Query{}, p.errorf("term %q analyzes to no tokens", w)
	case 1:
		term := hashes[0]
		if !p.eof() && p.s[p.pos] == '~' {
			return p.parseFuzzy(term)
		}
		return Query{Op: OpTerm, Term: term}, nil
	default:
		// A hyphenated (or otherwise multi-token) word searches as an
		// implicit adjacent phrase: `wi-fi` is `"wi fi"`.
		slots := make([]PhrasePos, len(hashes))
		for i, h := range hashes {
			slots[i] = PhrasePos{Alts: []uint64{h}}
		}
		return Query{Op: OpPhrase, Phrase: slots}, nil
	}
}

// parseFuzzy parses ~N and ~P:N after an exact term hash.
func (p *tinParser) parseFuzzy(term uint64) (Query, error) {
	p.pos++ // ~
	p.skipWS()
	n, err := p.scanDigits("edit distance")
	if err != nil {
		return Query{}, err
	}
	prefix := 1
	if !p.eof() && p.s[p.pos] == ':' {
		p.pos++
		prefix = n
		n, err = p.scanDigits("edit distance")
		if err != nil {
			return Query{}, err
		}
	}
	// The dictionary spelling of the exact term anchors the neighborhood.
	spell, ok := p.ix.dict[term]
	if !ok {
		return Query{Op: OpOr}, nil
	}
	return orOf(p.ix.expandFuzzy(spell, prefix, n)), nil
}

// parseRangeBound parses a range bound word or `*` (open).
func (p *tinParser) parseRangeBound() (*string, error) {
	p.skipWS()
	if p.eof() {
		return nil, p.errorf("expected a range bound")
	}
	if p.s[p.pos] == '*' {
		// A lone `*` opens the range; anything longer stays a wildcard and
		// is refused as a bound.
		w, err := p.scanWord()
		if err != nil {
			return nil, err
		}
		if w == "*" {
			return nil, nil
		}
		return nil, p.errorf("range bounds must be exact terms")
	}
	w, err := p.scanWord()
	if err != nil {
		return nil, err
	}
	unescaped, sawWild, err := unescapeWord(w)
	if err != nil {
		return nil, err
	}
	if sawWild {
		return nil, p.errorf("range bounds must be exact terms")
	}
	// Bounds address dictionary spellings without tokenizing (so
	// `1,000` stays one bound); a bound the analyzer erases entirely
	// is rejected rather than treated as open.
	if _, n := spellOf(unescaped); n == 0 {
		return nil, p.errorf("range bound %q is erased by the analyzer", w)
	}
	s := foldPattern(unescaped)
	return &s, nil
}

// rangeQuery builds an inclusive term-dictionary range; a reversed range
// matches nothing.
func (p *tinParser) rangeQuery(lo, hi *string) (Query, error) {
	if lo != nil && hi != nil && *lo > *hi {
		return Query{Op: OpOr}, nil
	}
	return orOf(p.ix.expandRange(lo, hi)), nil
}

// orOf packs hashes as an OR of terms; empty input matches nothing.
func orOf(hashes []uint64) Query {
	if len(hashes) == 0 {
		return Query{Op: OpOr}
	}
	if len(hashes) == 1 {
		return Query{Op: OpTerm, Term: hashes[0]}
	}
	kids := make([]Query, len(hashes))
	for i, h := range hashes {
		kids[i] = Query{Op: OpTerm, Term: h}
	}
	return Query{Op: OpOr, Kids: kids}
}

// parseMatches parses MATCHES <regex>: a full-term match over spellings,
// unfolded (patterns address the normalized dictionary form). The pattern
// runs to unescaped whitespace; unescaped ) and ] close a group or class
// opened inside the pattern and end it otherwise.
func (p *tinParser) parseMatches() (Query, error) {
	p.skipWS()
	start := p.pos
	var sb []byte
	groups, classes := 0, 0
	for p.pos < len(p.s) {
		c := p.s[p.pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			break
		}
		if c == '\\' && p.pos+1 < len(p.s) {
			// `\ ` is a literal space; every other sequence passes
			// through untouched (an escaped bracket never opens or
			// closes a group or class).
			if p.s[p.pos+1] == ' ' {
				sb = append(sb, ' ')
			} else {
				sb = append(sb, '\\', p.s[p.pos+1])
			}
			p.pos += 2
			continue
		}
		switch c {
		case '(':
			if classes == 0 {
				groups++
			}
		case '[':
			if classes == 0 {
				classes++
			}
		case ')':
			if classes == 0 {
				if groups == 0 {
					goto done
				}
				groups--
			}
		case ']':
			if classes == 0 {
				goto done
			}
			classes--
		case '"':
			return Query{}, p.errorf("regex ends at %q; escape whitespace as \\ ", string(c))
		}
		sb = append(sb, c)
		p.pos++
	}
done:
	if p.pos == start {
		return Query{}, p.errorf("expected a pattern after MATCHES")
	}
	re, err := regexp.Compile("^(?:" + string(sb) + ")$")
	if err != nil {
		return Query{}, p.errorf("bad pattern: %v", err)
	}
	return orOf(p.ix.expandRegexp(re)), nil
}

// parseContains parses CONTAINS <term>: a plain single term.
func (p *tinParser) parseContains() (Query, error) {
	p.skipWS()
	w, err := p.scanWord()
	if err != nil {
		return Query{}, err
	}
	if w == "*" || isKeyword(w, "MATCHES") || isKeyword(w, "CONTAINS") || isKeyword(w, "AT") || isKeyword(w, "ALL") {
		return Query{}, p.errorf("CONTAINS needs a plain term")
	}
	unescaped, sawWild, err := unescapeWord(w)
	if err != nil {
		return Query{}, err
	}
	if sawWild {
		return Query{}, p.errorf("CONTAINS needs a plain term")
	}
	hashes := foldTokens(unescaped)
	if len(hashes) != 1 {
		return Query{}, p.errorf("CONTAINS needs exactly one term")
	}
	return Query{Op: OpTerm, Term: hashes[0]}, nil
}

// parseAtLeast parses AT LEAST N[%] OF [...].
func (p *tinParser) parseAtLeast() (Query, error) {
	p.skipWS()
	w, err := p.scanWord()
	if err != nil || !isKeyword(w, "LEAST") {
		return Query{}, p.errorf("expected LEAST after AT")
	}
	n, err := p.scanDigits("count")
	if err != nil {
		return Query{}, err
	}
	p.skipWS()
	pct := false
	if !p.eof() && p.s[p.pos] == '%' {
		pct = true
		p.pos++
	}
	p.skipWS()
	w, err = p.scanWord()
	if err != nil || !isKeyword(w, "OF") {
		return Query{}, p.errorf("expected OF after AT LEAST count")
	}
	alts, err := p.parseBracketed()
	if err != nil {
		return Query{}, err
	}
	threshold := n
	if pct {
		threshold = (n*len(alts) + 99) / 100
	}
	return Query{Op: OpAtLeast, Kids: alts, Threshold: threshold}, nil
}

// parseAllOf parses ALL OF [...] as an AND of alternatives.
func (p *tinParser) parseAllOf() (Query, error) {
	p.skipWS()
	w, err := p.scanWord()
	if err != nil || !isKeyword(w, "OF") {
		return Query{}, p.errorf("expected OF after ALL")
	}
	alts, err := p.parseBracketed()
	if err != nil {
		return Query{}, err
	}
	if len(alts) == 1 {
		return alts[0], nil
	}
	return Query{Op: OpAnd, Kids: alts}, nil
}

// parseAlternatives parses [a b c] as an OR of whitespace-separated
// alternatives (never implicit AND inside).
func (p *tinParser) parseAlternatives() (Query, error) {
	alts, err := p.parseBracketed()
	if err != nil {
		return Query{}, err
	}
	if len(alts) == 1 {
		return alts[0], nil
	}
	return Query{Op: OpOr, Kids: alts}, nil
}

// parseBracketed parses the alternative list after `[`, `AT LEAST n OF`, or
// `ALL OF`. Empty `[]` is a parse error.
func (p *tinParser) parseBracketed() ([]Query, error) {
	if p.eof() || p.s[p.pos] != '[' {
		p.skipWS()
		if p.eof() || p.s[p.pos] != '[' {
			return nil, p.errorf("expected [")
		}
	}
	p.pos++ // [
	saved := p.noImplicit
	p.noImplicit = true
	defer func() { p.noImplicit = saved }()
	var alts []Query
	for {
		p.skipWS()
		if p.eof() {
			return nil, p.errorf("unclosed [")
		}
		if p.s[p.pos] == ']' {
			p.pos++
			break
		}
		alt, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		alts = append(alts, alt)
		// Commas separate alternatives (`[beer, ale]`); a comma between
		// two digits never reaches here (numeric terms join in
		// scanWord). A trailing comma leaves an empty alternative,
		// which is invalid.
		p.skipWS()
		if !p.eof() && p.s[p.pos] == ',' {
			p.pos++
			p.skipWS()
			if !p.eof() && p.s[p.pos] == ']' {
				return nil, p.errorf("empty alternative in [...]")
			}
		}
	}
	if len(alts) == 0 {
		return nil, p.errorf("empty [] is invalid")
	}
	return alts, nil
}

// parsePhrase parses "words", `_` gaps, `[a b]` per-position alternatives,
// and a trailing ~N tolerance.
func (p *tinParser) parsePhrase() (Query, error) {
	p.pos++ // "
	var inner []byte
	closed := false
	for p.pos < len(p.s) {
		c := p.s[p.pos]
		if c == '"' {
			closed = true
			p.pos++
			break
		}
		if c == '\\' {
			if p.pos+1 >= len(p.s) {
				return Query{}, p.errorf("dangling escape in phrase")
			}
			// `\" \\ \_ \[ \]` resolve to the bare character (with `\ `
			// a literal space); any other `\X` passes X through, so
			// `"a\xb"` reads as the word "axb".
			inner = append(inner, p.s[p.pos+1])
			p.pos += 2
			continue
		}
		inner = append(inner, c)
		p.pos++
	}
	if !closed {
		return Query{}, p.errorf("unclosed phrase")
	}
	if len(inner) == 0 {
		return Query{}, p.errorf("empty \"\" is invalid")
	}
	slots, err := p.phraseSlots(string(inner))
	if err != nil {
		return Query{}, err
	}
	slop := 0
	if !p.eof() && p.s[p.pos] == '~' {
		p.pos++
		n, err := p.scanDigits("phrase tolerance")
		if err != nil {
			return Query{}, err
		}
		slop = n
	}
	return Query{Op: OpPhrase, Phrase: slots, Slop: slop}, nil
}

// phraseSlots splits phrase inner text into positions.
func (p *tinParser) phraseSlots(inner string) ([]PhrasePos, error) {
	var slots []PhrasePos
	i := 0
	skip := func() {
		for i < len(inner) && (inner[i] == ' ' || inner[i] == '\t') {
			i++
		}
	}
	for {
		skip()
		if i >= len(inner) {
			break
		}
		switch inner[i] {
		case '_':
			slots = append(slots, PhrasePos{Any: true})
			i++
		case '[':
			i++
			var alts []uint64
			for {
				skip()
				if i >= len(inner) {
					return nil, p.errorf("unclosed [ in phrase")
				}
				if inner[i] == ']' {
					i++
					break
				}
				j := i
				for j < len(inner) && inner[j] != ' ' && inner[j] != '\t' && inner[j] != ']' {
					j++
				}
				word := inner[i:j]
				i = j
				hashes := foldTokens(word)
				if len(hashes) != 1 {
					return nil, p.errorf("phrase alternatives must be single terms")
				}
				alts = append(alts, hashes[0])
			}
			if len(alts) == 0 {
				return nil, p.errorf("empty [] in phrase")
			}
			slots = append(slots, PhrasePos{Alts: alts})
		default:
			j := i
			for j < len(inner) && inner[j] != ' ' && inner[j] != '\t' {
				j++
			}
			word := inner[i:j]
			i = j
			if strings.ContainsAny(word, "*?~^") {
				return nil, p.errorf("wildcards need expression level, not phrases")
			}
			hashes := foldTokens(word)
			for _, h := range hashes {
				slots = append(slots, PhrasePos{Alts: []uint64{h}})
			}
		}
	}
	if len(slots) == 0 {
		return nil, p.errorf("phrase analyzes to no tokens")
	}
	return slots, nil
}
