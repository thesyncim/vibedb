package sql

// tokenKind enumerates the lexemes. Keywords are not distinct kinds: a keyword
// is a tokIdent whose kw field resolved, so an unquoted identifier that happens
// to spell a keyword is still available as a field name wherever the grammar is
// unambiguous — which matters more here than in a schema'd SQL, because a JSON
// document is free to contain a key named "order" or "count".
type tokenKind uint8

const (
	tokEOF tokenKind = iota
	tokError
	tokIdent       // unquoted identifier; kw names its keyword, if any
	tokQuotedIdent // "..."; never a keyword, whatever it spells
	tokNumber      // JSON number grammar, kept as its exact source spelling
	tokString      // '...'
	tokParam       // ?
	tokStar        // *
	tokComma
	tokSemicolon
	tokLParen
	tokRParen
	tokLBracket
	tokRBracket
	tokLBrace // begins a raw JSON object in document/containment positions
	tokDot
	tokEq
	tokNe
	tokLt
	tokLe
	tokGt
	tokGe
	tokContains // @>
)

// A token is one lexeme. text is a slice of the source for identifiers,
// numbers, and the undecoded body of a quoted token; esc says whether that body
// contains a doubled quote and therefore still needs decoding.
//
// Holding the raw body plus a flag, rather than a decoded string, is what keeps
// the lexer allocation-free. The obvious spelling — build the decoded value in
// a strings.Builder as the string is scanned — costs one heap allocation per
// string literal in every statement, on a package whose whole point is to reach
// a zero-allocation steady state. Almost no literal contains a doubled quote,
// so almost no literal needs decoding at all, and the ones that do decode into
// the parser's shared scratch on their way into the arena.
type token struct {
	kind tokenKind
	kw   keyword
	text string
	pos  int
	esc  bool
}

// A keyword identifies a reserved word. Keywords are matched case-insensitively
// (SQL's rule); identifiers are not (see the package documentation — they are
// JSON keys, and JSON keys are case-sensitive).
type keyword uint8

const (
	kwNone keyword = iota
	kwAll
	kwAnalyze
	kwAnd
	kwAs
	kwAsc
	kwAvg
	kwBetween
	kwBy
	kwCase
	kwCast
	kwAlter
	kwCollate
	kwConflict
	kwCount
	kwCreate
	kwCross
	kwDefault
	kwDelete
	kwDesc
	kwDistinct
	kwDo
	kwDrop
	kwEscape
	kwExcept
	kwExplain
	kwExists
	kwFalse
	kwFetch
	kwFirst
	kwFrom
	kwFull
	kwGroup
	kwHaving
	kwIf
	kwIlike
	kwIn
	kwIndex
	kwInner
	kwInsert
	kwIntersect
	kwInto
	kwIs
	kwJoin
	kwKey
	kwLast
	kwLeft
	kwLike
	kwLimit
	kwMax
	kwMerge
	kwMin
	kwMissing
	kwNatural
	kwNot
	kwNull
	kwNulls
	kwNothing
	kwOffset
	kwOn
	kwOr
	kwOrder
	kwOuter
	kwOver
	kwPrimary
	kwReplace
	kwReturning
	kwRight
	kwSelect
	kwSet
	kwSimilar
	kwSum
	kwTable
	kwTruncate
	kwTrue
	kwUnion
	kwUnique
	kwUpdate
	kwUsing
	kwValues
	kwWhen
	kwWhere
	kwView
	kwWindow
	kwWith
)

// maxKeywordLen is the longest keyword above. It bounds the stack buffer
// keywordOf folds into, so keyword recognition needs no map and no heap.
const maxKeywordLen = 9 // INTERSECT

// keywordOf recognizes s case-insensitively, or answers kwNone.
//
// The fold-into-an-array-then-switch shape is chosen over a package-level
// map[string]keyword because the map form requires an uppercased key, and
// strings.ToUpper allocates one string per identifier in every statement — a
// per-token heap allocation to save a comparison the compiler turns into a
// jump table anyway. `switch string(buf[:n])` is recognized by the compiler and
// does not copy the array.
func keywordOf(s string) keyword {
	if len(s) == 0 || len(s) > maxKeywordLen {
		return kwNone
	}
	var buf [maxKeywordLen]byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	switch string(buf[:len(s)]) {
	case "ALL":
		return kwAll
	case "ANALYZE":
		return kwAnalyze
	case "AND":
		return kwAnd
	case "AS":
		return kwAs
	case "ASC":
		return kwAsc
	case "AVG":
		return kwAvg
	case "BETWEEN":
		return kwBetween
	case "BY":
		return kwBy
	case "CASE":
		return kwCase
	case "CAST":
		return kwCast
	case "ALTER":
		return kwAlter
	case "COLLATE":
		return kwCollate
	case "CONFLICT":
		return kwConflict
	case "COUNT":
		return kwCount
	case "CREATE":
		return kwCreate
	case "CROSS":
		return kwCross
	case "DEFAULT":
		return kwDefault
	case "DELETE":
		return kwDelete
	case "DESC":
		return kwDesc
	case "DISTINCT":
		return kwDistinct
	case "DO":
		return kwDo
	case "DROP":
		return kwDrop
	case "ESCAPE":
		return kwEscape
	case "EXCEPT":
		return kwExcept
	case "EXPLAIN":
		return kwExplain
	case "EXISTS":
		return kwExists
	case "FALSE":
		return kwFalse
	case "FETCH":
		return kwFetch
	case "FIRST":
		return kwFirst
	case "FROM":
		return kwFrom
	case "FULL":
		return kwFull
	case "GROUP":
		return kwGroup
	case "HAVING":
		return kwHaving
	case "IF":
		return kwIf
	case "ILIKE":
		return kwIlike
	case "IN":
		return kwIn
	case "INDEX":
		return kwIndex
	case "INNER":
		return kwInner
	case "INSERT":
		return kwInsert
	case "INTERSECT":
		return kwIntersect
	case "INTO":
		return kwInto
	case "IS":
		return kwIs
	case "JOIN":
		return kwJoin
	case "KEY":
		return kwKey
	case "LAST":
		return kwLast
	case "LEFT":
		return kwLeft
	case "LIKE":
		return kwLike
	case "LIMIT":
		return kwLimit
	case "MAX":
		return kwMax
	case "MERGE":
		return kwMerge
	case "MIN":
		return kwMin
	case "MISSING":
		return kwMissing
	case "NATURAL":
		return kwNatural
	case "NOT":
		return kwNot
	case "NULL":
		return kwNull
	case "NULLS":
		return kwNulls
	case "NOTHING":
		return kwNothing
	case "OFFSET":
		return kwOffset
	case "ON":
		return kwOn
	case "OR":
		return kwOr
	case "ORDER":
		return kwOrder
	case "OUTER":
		return kwOuter
	case "OVER":
		return kwOver
	case "PRIMARY":
		return kwPrimary
	case "REPLACE":
		return kwReplace
	case "RETURNING":
		return kwReturning
	case "RIGHT":
		return kwRight
	case "SELECT":
		return kwSelect
	case "SET":
		return kwSet
	case "SIMILAR":
		return kwSimilar
	case "SUM":
		return kwSum
	case "TABLE":
		return kwTable
	case "TRUNCATE":
		return kwTruncate
	case "TRUE":
		return kwTrue
	case "UNION":
		return kwUnion
	case "UNIQUE":
		return kwUnique
	case "UPDATE":
		return kwUpdate
	case "USING":
		return kwUsing
	case "VALUES":
		return kwValues
	case "WHEN":
		return kwWhen
	case "WHERE":
		return kwWhere
	case "VIEW":
		return kwView
	case "WINDOW":
		return kwWindow
	case "WITH":
		return kwWith
	}
	return kwNone
}

// reserved reports whether kw may not name a field or a collection unless it
// is quoted.
//
// The split between reserved and non-reserved words is SQL's, and it is worth
// keeping here even though a schemaless store makes unquoted keywords more
// tempting: JSON documents really do have keys called "order" and "from", and
// admitting all of them unquoted is the obvious accommodation. It is the wrong
// one. A clause keyword that can also start a path makes the commonest typo in
// SQL — a stray comma before FROM — parse as a column list ending in a field
// named FROM, and the error then surfaces two tokens later pointing at the
// table name. Reserving the words the grammar itself uses keeps the error where
// the mistake is, and `"order"` remains available for the field.
//
// The aggregate names are deliberately not reserved. They are only calls when a
// '(' follows, so `SELECT count FROM t` is unambiguous, and a document with a
// field called "count" is not exotic. MISSING is not reserved either: it is
// this dialect's own addition, and reserving a word SQL does not would surprise
// an author who has never seen it.
//
// CONFLICT, DEFAULT, DO, NOTHING, RETURNING, and the three refused statement kinds MERGE,
// REPLACE, and TRUNCATE are not reserved either, and the rule above says why.
// Reserving a word costs every document that has a field of that name, and
// these buy nothing in return: each is matched positionally, where a clause
// keyword is the only thing that could appear, so leaving them nameable cannot
// make any statement ambiguous. INTO and SET are the opposite case and are
// reserved: they are clause keywords of the grammar itself, in the position a
// path could also start, which is exactly where an unreserved word turns a
// missing comma into a misparse two tokens later.
func reserved(kw keyword) bool {
	switch kw {
	case kwNone,
		kwCount, kwSum, kwAvg, kwMin, kwMax,
		kwMissing, kwNulls, kwFirst, kwLast, kwEscape,
		kwConflict, kwDefault, kwDo, kwNothing, kwReturning, kwMerge, kwReplace, kwTruncate,
		kwAlter, kwCreate, kwDrop, kwIf, kwIndex, kwKey, kwPrimary, kwTable,
		kwUnique, kwView:
		return false
	}
	return true
}

// aggregateOf maps the aggregate keywords to their AggKind. A keyword is an
// aggregate call only when a '(' follows it, so this answering non-zero is a
// necessary but not sufficient condition; see parseResultColumn.
func aggregateOf(kw keyword) (AggKind, bool) {
	switch kw {
	case kwCount:
		return AggCount, true
	case kwSum:
		return AggSum, true
	case kwAvg:
		return AggAvg, true
	case kwMin:
		return AggMin, true
	case kwMax:
		return AggMax, true
	}
	return AggNone, false
}
