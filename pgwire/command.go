package pgwire

import (
	"fmt"
	"strconv"
	"strings"
)

// The layer in front of the SQL engine: statement splitting, statement
// classification, run-time parameters, and the small fixed result sets a
// driver's startup handshake asks for.
//
// # Why classification happens before parsing
//
// [sql.Parse] accepts SELECT and refuses every other statement kind, with one
// message and one error type for both "this is not SQL" and "this is SQL this
// engine will never run". A client needs those distinguished, because the
// SQLSTATE it branches on is the difference between a bug in its query and a
// capability this server does not have. Deciding from the leading keyword,
// here, is what makes 0A000 and 42601 mean different things; see pgerror.go for
// why the alternative — classifying by matching the parser's prose — was
// rejected.
//
// # Why an unsupported SET is refused
//
// A run-time parameter that this server accepts and ignores is a parameter the
// client believes is in effect. For most of PostgreSQL's parameters that is
// harmless here only because the feature they govern does not exist: there is
// no float this server formats, so extra_float_digits cannot change a result;
// no timestamp, so TimeZone cannot; no date, so DateStyle cannot. Those are
// accepted, and each entry in the table below records which of those arguments
// makes it safe.
//
// Everything else is refused with 0A000. search_path is the clearest case: this
// dialect's FROM names a collection directly and there are no schemas at all,
// so a client that set a search_path and had it accepted would believe its
// unqualified names were being resolved through it. statement_timeout is the
// second clearest: this server has no cancellation hook, so accepting a timeout
// would be promising an interruption that will never arrive. Both would produce
// a client that misreads its own results, which is exactly the outcome a silent
// accept is worse than an error for.

// A statementKind is what the leading keyword says a statement is.
type statementKind int

const (
	// kindSelect is handed to the SQL front end.
	kindSelect statementKind = iota
	// kindEmpty is a statement with no tokens: whitespace, comments, or a bare
	// semicolon. The protocol has its own reply for it.
	kindEmpty
	kindSet
	kindReset
	kindShow
	kindDiscard
	// kindUnsupported is a statement kind this server does not implement, and
	// carries the message naming the missing feature.
	kindUnsupported
	// kindUnknown is a leading token that is not a statement keyword at all,
	// which is a syntax error rather than a missing feature.
	kindUnknown
)

// unsupportedStatements maps a leading keyword onto the feature it needs and
// this server does not have. Every entry is a deliberate refusal with a reason
// a reader of the message can act on, which is the whole reason the table
// exists instead of a default branch saying "unsupported statement".
var unsupportedStatements = map[string]string{
	"INSERT":   "this server is read-only: the SQL surface is SELECT and this engine has no mutation path through SQL",
	"UPDATE":   "this server is read-only: the SQL surface is SELECT and this engine has no mutation path through SQL",
	"DELETE":   "this server is read-only: the SQL surface is SELECT and this engine has no mutation path through SQL",
	"MERGE":    "this server is read-only: the SQL surface is SELECT and this engine has no mutation path through SQL",
	"TRUNCATE": "this server is read-only: the SQL surface is SELECT and this engine has no mutation path through SQL",
	"COPY":     "COPY is not supported: this server implements the simple and extended query protocols and not the copy subprotocol",

	"CREATE":  "data definition is not supported: collections and their indexes are created by the application that owns the store, not over the wire",
	"ALTER":   "data definition is not supported: collections and their indexes are created by the application that owns the store, not over the wire",
	"DROP":    "data definition is not supported: collections and their indexes are created by the application that owns the store, not over the wire",
	"GRANT":   "there is no privilege system: authentication decides whether a connection may read, and there is nothing finer to grant",
	"REVOKE":  "there is no privilege system: authentication decides whether a connection may read, and there is nothing finer to grant",
	"COMMENT": "there is no catalog to comment on",
	"VACUUM":  "storage maintenance is the owning application's, not a client's",
	"ANALYZE": "storage maintenance is the owning application's, not a client's",
	"REINDEX": "storage maintenance is the owning application's, not a client's",
	"CLUSTER": "storage maintenance is the owning application's, not a client's",
	"REFRESH": "there are no materialized views",

	"BEGIN":     "transactions are not supported: every statement already reads one consistent snapshot, and there is no cross-statement transaction to open",
	"START":     "transactions are not supported: every statement already reads one consistent snapshot, and there is no cross-statement transaction to open",
	"COMMIT":    "transactions are not supported: every statement already reads one consistent snapshot, and there is no cross-statement transaction to open",
	"ROLLBACK":  "transactions are not supported: every statement already reads one consistent snapshot, and there is no cross-statement transaction to open",
	"END":       "transactions are not supported: every statement already reads one consistent snapshot, and there is no cross-statement transaction to open",
	"ABORT":     "transactions are not supported: every statement already reads one consistent snapshot, and there is no cross-statement transaction to open",
	"SAVEPOINT": "transactions are not supported, so there is nothing to set a savepoint in",
	"RELEASE":   "transactions are not supported, so there is nothing to set a savepoint in",

	"PREPARE":    "SQL-level PREPARE is not supported: use the extended query protocol's Parse message, which every client library exposes",
	"EXECUTE":    "SQL-level EXECUTE is not supported: use the extended query protocol's Bind and Execute messages",
	"DEALLOCATE": "SQL-level DEALLOCATE is not supported: use the extended query protocol's Close message",

	"DECLARE": "cursors are not supported: the extended query protocol's portals are the row-at-a-time mechanism here",
	"FETCH":   "cursors are not supported: the extended query protocol's portals are the row-at-a-time mechanism here",
	"MOVE":    "cursors are not supported: the extended query protocol's portals are the row-at-a-time mechanism here",

	"LISTEN":   "asynchronous notification is not supported",
	"NOTIFY":   "asynchronous notification is not supported",
	"UNLISTEN": "asynchronous notification is not supported",

	"LOCK":    "there is no lock manager: readers never block and there is nothing to lock against",
	"EXPLAIN": "EXPLAIN is not supported: the engine's plan is not renderable as PostgreSQL plan text",
	"CALL":    "there are no stored procedures",
	"DO":      "there is no procedural language",

	"WITH":   "common table expressions are not supported: the engine executes one plan and has no nested execution",
	"VALUES": "a bare VALUES list is not supported: the engine reads stored documents and evaluates no constructed rows",
	"TABLE":  "the TABLE shorthand is not supported; write SELECT * FROM name",
}

// classify reports what kind of statement src is, from its leading keyword
// alone. The second result is the refusal message for kindUnsupported.
func classify(src string) (statementKind, string) {
	s := scanner{src: src}
	word := strings.ToUpper(s.word())
	switch word {
	case "":
		return kindEmpty, ""
	case "SELECT":
		return kindSelect, ""
	case "SET":
		return kindSet, ""
	case "RESET":
		return kindReset, ""
	case "SHOW":
		return kindShow, ""
	case "DISCARD":
		return kindDiscard, ""
	}
	if reason, ok := unsupportedStatements[word]; ok {
		return kindUnsupported, reason
	}
	return kindUnknown, ""
}

// A scanner walks SQL text past whitespace and comments. It is the smallest
// thing that can answer "what is the first word" and "where does this statement
// end" without a second SQL parser, and it deliberately understands only the
// four lexical constructs this dialect has: '--' to end of line, '/* */',
// single-quoted strings with ” escaping, and double-quoted identifiers with ""
// escaping.
type scanner struct {
	src string
	pos int
}

// skip advances past whitespace and comments. Block comments do not nest here,
// matching this dialect's lexer.
func (s *scanner) skip() {
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			s.pos++
		case c == '-' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '-':
			if i := strings.IndexByte(s.src[s.pos:], '\n'); i < 0 {
				s.pos = len(s.src)
			} else {
				s.pos += i + 1
			}
		case c == '/' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '*':
			if i := strings.Index(s.src[s.pos+2:], "*/"); i < 0 {
				s.pos = len(s.src)
			} else {
				s.pos += i + 4
			}
		default:
			return
		}
	}
}

// word returns the next bare identifier or keyword, or "" if the next token is
// not one.
func (s *scanner) word() string {
	s.skip()
	start := s.pos
	for s.pos < len(s.src) && isWordByte(s.src[s.pos]) {
		s.pos++
	}
	return s.src[start:s.pos]
}

// peekWord returns the next word without consuming it.
func (s *scanner) peekWord() string {
	save := s.pos
	w := s.word()
	s.pos = save
	return w
}

// symbol consumes c if it is the next non-space byte.
func (s *scanner) symbol(c byte) bool {
	s.skip()
	if s.pos < len(s.src) && s.src[s.pos] == c {
		s.pos++
		return true
	}
	return false
}

// atEnd reports whether only whitespace, comments, and an optional trailing
// semicolon remain.
func (s *scanner) atEnd() bool {
	s.skip()
	if s.pos < len(s.src) && s.src[s.pos] == ';' {
		s.pos++
		s.skip()
	}
	return s.pos >= len(s.src)
}

// rest returns the remaining text with surrounding space and a trailing
// semicolon removed. It is how a SET value is taken: PostgreSQL's SET accepts a
// comma-separated list of bare words, numbers, and strings, and reproducing
// that grammar to then throw the value away would be effort spent on a value
// nothing reads.
func (s *scanner) rest() string {
	s.skip()
	text := strings.TrimSpace(s.src[s.pos:])
	s.pos = len(s.src)
	return strings.TrimSpace(strings.TrimSuffix(text, ";"))
}

// quoted reads a single-quoted string literal, returning its decoded value.
func (s *scanner) quoted() (string, bool) {
	s.skip()
	if s.pos >= len(s.src) || s.src[s.pos] != '\'' {
		return "", false
	}
	var out strings.Builder
	i := s.pos + 1
	for i < len(s.src) {
		if s.src[i] != '\'' {
			out.WriteByte(s.src[i])
			i++
			continue
		}
		if i+1 < len(s.src) && s.src[i+1] == '\'' {
			out.WriteByte('\'')
			i += 2
			continue
		}
		s.pos = i + 1
		return out.String(), true
	}
	return "", false
}

// number reads an unsigned integer literal.
func (s *scanner) number() (string, bool) {
	s.skip()
	start := s.pos
	for s.pos < len(s.src) && s.src[s.pos] >= '0' && s.src[s.pos] <= '9' {
		s.pos++
	}
	if s.pos == start {
		return "", false
	}
	return s.src[start:s.pos], true
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// splitStatements divides a simple-query message into its statements.
//
// The simple query protocol allows several statements in one message, and a
// naive split on ';' would cut a semicolon inside a string literal, a quoted
// identifier, or a comment. That is not a hypothetical: a WHERE clause
// comparing against a document value containing a semicolon is ordinary. The
// splitter therefore tracks exactly the four constructs this dialect's lexer
// tracks, and nothing else.
func splitStatements(src string) []string {
	var out []string
	start := 0
	for i := 0; i < len(src); {
		switch src[i] {
		case '\'':
			i = skipQuoted(src, i, '\'')
		case '"':
			i = skipQuoted(src, i, '"')
		case '-':
			if i+1 < len(src) && src[i+1] == '-' {
				if n := strings.IndexByte(src[i:], '\n'); n < 0 {
					i = len(src)
				} else {
					i += n + 1
				}
			} else {
				i++
			}
		case '/':
			if i+1 < len(src) && src[i+1] == '*' {
				if n := strings.Index(src[i+2:], "*/"); n < 0 {
					i = len(src)
				} else {
					i += n + 4
				}
			} else {
				i++
			}
		case ';':
			out = append(out, src[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	out = append(out, src[start:])
	// An empty statement between two semicolons is dropped rather than answered
	// with an EmptyQueryResponse: PostgreSQL's grammar has no production for it,
	// and a client counting one response per statement it wrote would be handed
	// one more than it expected. A query string that is empty *in its entirety*
	// is a different thing, and does get an EmptyQueryResponse, which is why the
	// filter keeps one statement when everything was empty.
	kept := out[:0]
	for _, text := range out {
		if strings.TrimSpace(text) != "" {
			kept = append(kept, text)
		}
	}
	if len(kept) == 0 {
		return []string{""}
	}
	return kept
}

// skipQuoted advances past a quoted run starting at i, where the quote is
// escaped by doubling. It is used for both string literals and quoted
// identifiers because this dialect escapes both the same way, and because
// standard_conforming_strings is reported as on, so a backslash is an ordinary
// byte.
func skipQuoted(src string, i int, quote byte) int {
	i++
	for i < len(src) {
		if src[i] != quote {
			i++
			continue
		}
		if i+1 < len(src) && src[i+1] == quote {
			i += 2
			continue
		}
		return i + 1
	}
	return len(src)
}

// A parameter is one run-time setting this server recognizes.
type parameter struct {
	// report marks a parameter PostgreSQL flags GUC_REPORT, meaning a change
	// must be announced with a ParameterStatus message. Clients cache these and
	// a change they are not told about is a change they act on wrongly.
	report bool
	// initial is the value a fresh session has.
	initial string
	// check validates a proposed value, or is nil to accept any. It exists for
	// the parameters whose safety argument depends on the value: this server
	// really does emit UTF-8 and really does treat backslashes literally, so a
	// client setting either the other way must be told no rather than allowed
	// to misdecode every string it receives.
	check func(string) error
	// why records the argument that makes ignoring this parameter safe. It is a
	// field rather than a comment because the table is the place someone will
	// look when adding an entry, and "why is it safe to ignore this one" is the
	// question they have to answer.
	why string
}

// parameters is the complete set of run-time settings this server accepts.
// Anything absent is refused with 0A000; see the file comment.
var parameters = map[string]*parameter{
	"application_name": {
		report: true, initial: "",
		why: "a label carried for the client's own logs; nothing here reads it",
	},
	"client_encoding": {
		report: true, initial: "UTF8", check: requireEncoding,
		why: "this server emits UTF-8 and only UTF-8, so the only accepted value is the one already in effect",
	},
	"DateStyle": {
		report: true, initial: "ISO, MDY",
		why: "no column this server produces is a date or a timestamp, so output formatting cannot be affected",
	},
	"IntervalStyle": {
		report: true, initial: "postgres",
		why: "no column this server produces is an interval",
	},
	"TimeZone": {
		report: true, initial: "UTC",
		why: "no column this server produces carries a time zone",
	},
	"extra_float_digits": {
		initial: "1",
		why:     "this server formats no floats: a projected number is sent as the document's own decimal digits and an aggregate as the engine's own rendering",
	},
	"standard_conforming_strings": {
		report: true, initial: "on", check: requireOn,
		why: "this dialect's lexer treats a backslash in a string literal as an ordinary byte, which is what 'on' means; 'off' would change how the client escapes what it sends",
	},
	"bytea_output": {
		initial: "hex",
		why:     "no column this server produces is a bytea",
	},
}

// refusalHints gives a longer answer for the parameters clients most often try
// to set and this server refuses.
var refusalHints = map[string]string{
	"search_path":                         "this dialect has no schemas: a FROM clause names a collection directly, so a search path would silently not apply",
	"statement_timeout":                   "this server has no query cancellation hook, so it cannot honour a timeout and will not pretend to",
	"lock_timeout":                        "there is no lock manager, so nothing ever waits for a lock",
	"idle_in_transaction_session_timeout": "there are no transactions",
	"default_transaction_isolation":       "there are no transactions; every statement reads one consistent snapshot",
	"default_transaction_read_only":       "every session is read-only already",
	"transaction_isolation":               "there are no transactions; every statement reads one consistent snapshot",
	"role":                                "there is no role system",
	"session_authorization":               "there is no role system",
}

func requireEncoding(v string) error {
	switch strings.ToUpper(strings.Trim(v, "'\"")) {
	case "UTF8", "UNICODE", "UTF-8":
		return nil
	}
	return fmt.Errorf("this server emits UTF-8 only, and cannot serve client_encoding %s", v)
}

func requireOn(v string) error {
	if strings.EqualFold(strings.Trim(v, "'\""), "on") {
		return nil
	}
	return fmt.Errorf("this server's lexer conforms to the standard; %s cannot be turned off", v)
}

// lookupParameter finds a parameter by its case-insensitive name, returning the
// canonical spelling. PostgreSQL folds parameter names to lower case, so
// "DateStyle" and "datestyle" are one setting, and a session map keyed by the
// spelling the client happened to use would answer SHOW with whichever it set
// last.
func lookupParameter(name string) (string, *parameter, bool) {
	for canonical, p := range parameters {
		if strings.EqualFold(canonical, name) {
			return canonical, p, true
		}
	}
	return name, nil, false
}

// unsupportedParameter builds the 0A000 for a setting this server will not
// accept.
func unsupportedParameter(name string) *pgError {
	e := newError(sqlstateFeatureNotSupported,
		fmt.Sprintf("unrecognized or unsupported run-time parameter %q", name))
	for known, hint := range refusalHints {
		if strings.EqualFold(known, name) {
			return e.withHint(hint)
		}
	}
	return e.withHint(
		"this server refuses a parameter it would only ignore, because a setting the client " +
			"believes is in effect and is not is how results get misread")
}

// A setCommand is a decoded SET.
type setCommand struct {
	name  string
	value string
	// reset marks SET name = DEFAULT and RESET name, which restore the initial
	// value rather than assigning one.
	reset bool
	// all marks RESET ALL.
	all bool
}

// parseSet decodes "SET [SESSION|LOCAL] name [=|TO] value" and its two special
// forms, SET TIME ZONE and SET NAMES.
//
// SESSION and LOCAL are both accepted and both mean the same thing here,
// because LOCAL scopes a setting to a transaction and there are no
// transactions; a LOCAL setting in a session with no transaction block is a
// session setting in PostgreSQL too.
func parseSet(src string) (setCommand, error) {
	s := scanner{src: src}
	s.word() // SET
	switch strings.ToUpper(s.peekWord()) {
	case "SESSION", "LOCAL":
		s.word()
	}
	name := s.word()
	if name == "" {
		return setCommand{}, newError(sqlstateSyntaxError, "SET requires a parameter name")
	}
	// SET TIME ZONE and SET NAMES are PostgreSQL's two spellings that do not
	// name their parameter directly. Clients do send them.
	if strings.EqualFold(name, "TIME") && strings.EqualFold(s.peekWord(), "ZONE") {
		s.word()
		return setCommand{name: "TimeZone", value: s.rest()}, nil
	}
	if strings.EqualFold(name, "NAMES") {
		return setCommand{name: "client_encoding", value: s.rest()}, nil
	}
	if !s.symbol('=') {
		if !strings.EqualFold(s.peekWord(), "TO") {
			return setCommand{}, newError(sqlstateSyntaxError,
				"SET requires '=' or TO between the parameter name and its value")
		}
		s.word()
	}
	value := s.rest()
	if value == "" {
		return setCommand{}, newError(sqlstateSyntaxError, "SET requires a value")
	}
	if strings.EqualFold(value, "DEFAULT") {
		return setCommand{name: name, reset: true}, nil
	}
	return setCommand{name: name, value: unquote(value)}, nil
}

// parseReset decodes "RESET name" and "RESET ALL".
func parseReset(src string) (setCommand, error) {
	s := scanner{src: src}
	s.word() // RESET
	name := s.word()
	switch {
	case name == "":
		return setCommand{}, newError(sqlstateSyntaxError, "RESET requires a parameter name or ALL")
	case strings.EqualFold(name, "ALL"):
		return setCommand{all: true, reset: true}, nil
	case strings.EqualFold(name, "TIME") && strings.EqualFold(s.peekWord(), "ZONE"):
		return setCommand{name: "TimeZone", reset: true}, nil
	}
	return setCommand{name: name, reset: true}, nil
}

// parseShow decodes "SHOW name", "SHOW ALL", and "SHOW TIME ZONE".
func parseShow(src string) (string, error) {
	s := scanner{src: src}
	s.word() // SHOW
	name := s.word()
	switch {
	case name == "":
		return "", newError(sqlstateSyntaxError, "SHOW requires a parameter name or ALL")
	case strings.EqualFold(name, "TIME") && strings.EqualFold(s.peekWord(), "ZONE"):
		return "TimeZone", nil
	}
	return name, nil
}

// unquote strips one layer of single quotes from a SET value.
func unquote(v string) string {
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return strings.ReplaceAll(v[1:len(v)-1], "''", "'")
	}
	return v
}

// A fixedResult is a result set this package produces itself rather than
// executing: SHOW, and the handful of niladic expressions a driver's handshake
// asks for.
type fixedResult struct {
	cols []column
	rows [][]*string
	// tag is the CommandComplete tag, which for a fixed result is always one of
	// PostgreSQL's own command tags so a client's tag parsing keeps working.
	tag string
}

// shimFunctions is the complete set of expressions the literal-SELECT shim
// evaluates. It is a fixed table and not an expression evaluator, and the
// distinction matters: this exists so a client's connect sequence and health
// check succeed, not so that arbitrary SQL scalar expressions work. Anything
// not in it falls through to the real SQL front end, which refuses it with a
// position.
//
// The values are computed per session because two of them depend on it.
type shimFunctions struct {
	database string
	user     string
	pid      int32
}

// A shimColumn is one recognized select-list item.
type shimColumn struct {
	name  string
	value *string
	typ   columnType
}

// parseFixedSelect recognizes a SELECT with no FROM whose every item is a
// literal or one of the niladic expressions above, and returns its result.
//
// The second result reports whether the statement was one of those. A false
// means "not mine", not "invalid": the caller hands it to the SQL front end,
// which is the thing that knows how to reject it accurately.
func (fn shimFunctions) parseFixedSelect(src string) (fixedResult, bool) {
	s := scanner{src: src}
	if !strings.EqualFold(s.word(), "SELECT") {
		return fixedResult{}, false
	}
	var cols []column
	var row []*string
	for {
		col, ok := fn.parseShimItem(&s)
		if !ok {
			return fixedResult{}, false
		}
		// An explicit output name wins, exactly as AS does in real SQL.
		if alias, ok := parseAlias(&s); ok {
			col.name = alias
		}
		cols = append(cols, column{name: col.name, typ: col.typ})
		row = append(row, col.value)
		if !s.symbol(',') {
			break
		}
	}
	if !s.atEnd() {
		return fixedResult{}, false
	}
	return fixedResult{
		cols: cols,
		rows: [][]*string{row},
		tag:  "SELECT 1",
	}, true
}

// parseShimItem recognizes one select-list item.
func (fn shimFunctions) parseShimItem(s *scanner) (shimColumn, bool) {
	if n, ok := s.number(); ok {
		v := n
		return shimColumn{name: "?column?", value: &v, typ: typeInt8}, true
	}
	if str, ok := s.quoted(); ok {
		v := str
		return shimColumn{name: "?column?", value: &v, typ: typeText}, true
	}
	word := s.word()
	if word == "" {
		return shimColumn{}, false
	}
	// A pg_catalog qualifier is stripped, because pg_catalog is the implicit
	// schema of every built-in and clients write it both ways.
	if strings.EqualFold(word, "pg_catalog") && s.symbol('.') {
		word = s.word()
	}
	lower := strings.ToLower(word)
	switch lower {
	case "null":
		return shimColumn{name: "?column?", value: nil, typ: typeText}, true
	case "true", "false":
		v := "t"
		if lower == "false" {
			v = "f"
		}
		return shimColumn{name: "?column?", value: &v, typ: typeText}, true
	case "current_user", "session_user", "user":
		v := fn.user
		return shimColumn{name: lower, value: &v, typ: typeText}, true
	case "current_catalog":
		v := fn.database
		return shimColumn{name: lower, value: &v, typ: typeText}, true
	}
	// The remaining forms are function calls and must be followed by "()".
	if !s.symbol('(') || !s.symbol(')') {
		return shimColumn{}, false
	}
	switch lower {
	case "version":
		v := versionString
		return shimColumn{name: "version", value: &v, typ: typeText}, true
	case "current_database":
		v := fn.database
		return shimColumn{name: "current_database", value: &v, typ: typeText}, true
	case "current_schema":
		v := "public"
		return shimColumn{name: "current_schema", value: &v, typ: typeText}, true
	case "current_user", "session_user", "user":
		v := fn.user
		return shimColumn{name: lower, value: &v, typ: typeText}, true
	case "pg_backend_pid":
		v := strconv.Itoa(int(fn.pid))
		return shimColumn{name: "pg_backend_pid", value: &v, typ: typeInt8}, true
	}
	return shimColumn{}, false
}

// parseAlias reads an optional output name, with or without AS.
func parseAlias(s *scanner) (string, bool) {
	save := s.pos
	word := s.word()
	if word == "" {
		s.pos = save
		return "", false
	}
	if strings.EqualFold(word, "AS") {
		name := s.word()
		if name == "" {
			s.pos = save
			return "", false
		}
		return name, true
	}
	// A bare word here is an alias only if the item is complete; anything else
	// means this is not a statement the shim understands, and the caller's
	// atEnd check will reject it.
	return word, true
}

// Numbered parameters.
//
// Every PostgreSQL client library writes $1, $2, $3; this dialect's placeholder
// is '?'. That is not a cosmetic difference — it is the difference between a
// server real drivers can run a parameterized query against and one they
// cannot, because a driver builds the placeholder text itself and does not ask
// what spelling the server prefers. So a statement's numbered parameters are
// rewritten to '?' here, in front of the parser, and the mapping is kept so
// Bind can reorder the values.
//
// Rewriting rather than teaching the parser a second placeholder syntax is the
// deliberate choice, and the reason is that '$n' and '?' are not the same
// feature. '?' is positional: the n-th placeholder takes the n-th argument.
// '$n' names an argument, so it may appear out of order and may appear more
// than once, and "$1 = a AND $1 = b" is an ordinary thing for a driver to
// generate. Making the engine's literal binding understand naming would change
// a compiler this package has no business changing. Rewriting handles the whole
// of it: "$2, $1" becomes "?, ?" with the arguments swapped at Bind, and a
// repeated "$1" becomes two '?' bound to one value.
//
// The rewrite preserves byte offsets. A "$12" becomes "?" followed by two
// spaces rather than a shorter string, so a parse error's position still points
// at the same byte of the statement the client wrote — which is the whole point
// of reporting a position at all.

// errMixedPlaceholders reports a statement using both spellings, which has no
// unambiguous reading: the two count parameters differently and there is no
// rule for interleaving them that a client could have intended.
var errMixedPlaceholders = newError(sqlstateSyntaxError,
	"a statement may use '?' placeholders or $n numbered parameters, not both")

// rewriteNumberedParameters replaces $n with '?', preserving byte offsets, and
// returns the mapping from each '?' position to its 1-based parameter number
// along with the highest number seen.
//
// A statement with no $n is returned unchanged with a nil order, which is the
// signal that binding is positional and needs no permutation.
func rewriteNumberedParameters(src string) (string, []int, int, error) {
	if !strings.ContainsRune(src, '$') {
		return src, nil, 0, nil
	}
	var order []int
	highest := 0
	sawQuestion := false
	i := 0
	// The scan is a straight pass with the same four lexical constructs the
	// splitter tracks, because a '$1' inside a string literal or a comment is
	// text and not a parameter.
	out := make([]byte, 0, len(src))
	for i < len(src) {
		switch src[i] {
		case '\'', '"':
			end := skipQuoted(src, i, src[i])
			out = append(out, src[i:end]...)
			i = end
		case '-':
			if i+1 < len(src) && src[i+1] == '-' {
				end := len(src)
				if n := strings.IndexByte(src[i:], '\n'); n >= 0 {
					end = i + n + 1
				}
				out = append(out, src[i:end]...)
				i = end
				continue
			}
			out = append(out, src[i])
			i++
		case '/':
			if i+1 < len(src) && src[i+1] == '*' {
				end := len(src)
				if n := strings.Index(src[i+2:], "*/"); n >= 0 {
					end = i + n + 4
				}
				out = append(out, src[i:end]...)
				i = end
				continue
			}
			out = append(out, src[i])
			i++
		case '?':
			sawQuestion = true
			out = append(out, '?')
			i++
		case '$':
			start := i
			i++
			for i < len(src) && src[i] >= '0' && src[i] <= '9' {
				i++
			}
			digits := src[start+1 : i]
			if digits == "" {
				// Not a parameter: a lone '$', or the start of PostgreSQL's
				// dollar quoting, which this dialect has no use for and the
				// parser refuses on its own.
				out = append(out, '$')
				continue
			}
			n, err := strconv.Atoi(digits)
			if err != nil || n < 1 {
				return "", nil, 0, newError(sqlstateSyntaxError,
					"$"+digits+" is not a usable parameter number")
			}
			if n > maxParameters {
				return "", nil, 0, newError(sqlstateSyntaxError,
					"$"+digits+" exceeds the "+strconv.Itoa(maxParameters)+
						" parameters a Bind message can carry")
			}
			order = append(order, n)
			if n > highest {
				highest = n
			}
			// One '?' plus one space per remaining byte of "$n", so every later
			// byte keeps the offset the client wrote it at.
			out = append(out, '?')
			for range i - start - 1 {
				out = append(out, ' ')
			}
		default:
			out = append(out, src[i])
			i++
		}
	}
	if order == nil {
		return src, nil, 0, nil
	}
	if sawQuestion {
		return "", nil, 0, errMixedPlaceholders
	}
	return string(out), order, highest, nil
}
