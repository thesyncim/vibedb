package pgwire

import (
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/query"
)

// One connection: startup, the message loop, and the simple query protocol.
//
// # Lifecycle
//
// A session is created, negotiates, runs a message loop until the client sends
// Terminate or the connection fails, and then releases everything it owns. The
// release is unconditional and runs on every exit path, because two of the
// things it releases are not memory: a durable snapshot pins a storage
// generation against reuse, and a [query.Statement] holds compiler arenas. A
// session that returned without releasing them would leak a storage generation
// per connection, which is invisible until a file stops shrinking.
//
// A client that vanishes mid-message produces a read error from io.ReadFull —
// io.ErrUnexpectedEOF for a partial message, io.EOF for a clean disconnect —
// and both take the same exit. There is no attempt to write an error to a
// connection that just failed to be read.

// A session is one client connection.
type session struct {
	server *Server
	conn   net.Conn
	r      *reader
	w      *writer

	// pid and secret are the backend key a cancel request names this session
	// by. See session.cancel.
	pid    int32
	secret int32
	cancelFlag

	user     string
	database string
	// params are the run-time settings in effect. It is keyed by the canonical
	// parameter spelling, never by whatever case the client used.
	params map[string]string

	// exec is this session's execution storage, reused by every statement. It
	// is what the engine's single-consumer contract is about: it holds the
	// column, posting, group, and result buffers whose retained capacity makes
	// a warmed execution allocation-free, and two sessions sharing one would
	// interleave writes into a single set of them.
	exec query.Exec

	statements map[string]*prepared
	portals    map[string]*portal

	// generation counts executions into exec. A portal's cursor reads
	// exec.Result directly, which the next execution overwrites, so a portal
	// records the generation it was executed in and is refused if it is asked
	// to continue after another statement has run. See portal.
	generation uint64

	// msg is the decoded frontend message, reused so a session that serves a
	// million messages allocates one set of parameter slices.
	msg frontendMessage
	// rows is the per-row encoding scratch, retained for the same reason.
	rows rowEncoder
	// cells is scratch for collecting one row out of a cursor.
	cells []query.Cell

	// failed marks the extended-protocol error state: after an error, every
	// message up to the next Sync is discarded. Losing this would let a client
	// that sent Parse/Bind/Execute/Sync as one batch have the Execute run
	// against a Bind that had already failed.
	failed bool
	// terminated marks a clean client Terminate, which is not an error.
	terminated bool
}

// A prepared is one prepared statement.
//
// It is a small discriminated union rather than an interface because there are
// exactly four things a statement can be here and all four are known at
// compile time: a query the engine runs, a fixed result set this package
// produces, a run-time parameter assignment, and a statement that is refused.
// The fourth is retained rather than rejected at Parse for statements whose
// refusal the protocol wants reported at Execute — but in practice every
// refusal here is reported at Parse, which is where a client can attribute it.
type prepared struct {
	name string
	sql  string
	kind statementKind

	// stmt is the lowered plan for kindSelect. It is nil for every other kind.
	stmt *query.Statement
	// fixed is the result of a statement this package answers itself.
	fixed *fixedResult
	// set is the decoded SET or RESET.
	set setCommand
	// cols is the RowDescription, empty for a statement that returns no rows.
	cols []column
	// tag is the CommandComplete tag for a statement with no row count.
	tag string
	// paramOIDs are the parameter types the client declared in Parse. They are
	// hints this server did not ask for and does not require, and are used only
	// to disambiguate a bound value; see bindValue.
	paramOIDs []int32
	// paramOrder maps each '?' in the statement the engine compiled to the
	// 1-based numbered parameter it was rewritten from, and is nil when the
	// client wrote '?' directly. wireParams is how many values a Bind must
	// supply, which is the highest number used rather than the number of
	// placeholders: "$1 = a AND $1 = b" has two placeholders and one parameter.
	paramOrder []int
	wireParams int
}

func (p *prepared) release() {
	if p.stmt != nil {
		p.stmt.Release()
		p.stmt = nil
	}
}

// A portal is one bound, executable instance of a prepared statement.
//
// The generation field is the whole of this package's answer to portal
// suspension. A suspended portal holds a [query.Cursor] over the session's one
// [query.Exec], and the next execution into that Exec invalidates it. Rather
// than give every portal its own Exec — which would throw away the retained
// buffers that make execution allocation-free, per portal, forever — a portal
// records the generation it executed in and refuses to continue once another
// statement has run. The refusal is a 55000 naming the reason, which is a
// client-visible limitation and is documented; silently returning rows from
// the wrong result set is the alternative and is not.
type portal struct {
	name string
	stmt *prepared
	// args are the bound parameters, copied out of the Bind message because the
	// message's buffer is reused by the next one.
	args []any
	// argStore backs the string and []byte arguments for the same reason.
	argStore []byte
	formats  []int16

	// started marks a portal that has begun executing; row is the next fixed
	// row to send; cursor and lease belong to an engine execution.
	started    bool
	exhausted  bool
	row        int
	cursor     query.Cursor
	lease      lease
	generation uint64
}

func (p *portal) release() {
	p.lease.release()
	p.cursor = query.Cursor{}
	p.started = false
}

func newSession(s *Server, conn net.Conn) *session {
	return &session{
		server:     s,
		conn:       conn,
		r:          newReader(conn, 16<<10),
		w:          newWriter(conn, 16<<10),
		params:     map[string]string{},
		statements: map[string]*prepared{},
		portals:    map[string]*portal{},
	}
}

// out and push implement authConn, which is how a SASL mechanism reaches the
// connection without being handed the session.
func (s *session) out() *writer { return s.w }
func (s *session) push() error  { return s.flush() }

// cancel records that a cancel request arrived for this session.
//
// What it does is bounded and the bound is the honest part. The executor has no
// cancellation hook: once a scan has started there is no way to stop it short
// of closing the connection, and closing the connection under a running query
// would leave the storage generation the query leased pinned until the goroutine
// noticed. So a cancel is checked at the two points where stopping is possible
// and correct — before a statement starts executing, and between the statements
// of a multi-statement simple query or a pipelined batch — and a query already
// scanning runs to completion.
//
// That is genuinely useful for the shape a cancel usually arrives in (a client
// giving up on a batch) and genuinely useless for the shape people imagine
// (interrupting a slow scan). Both are stated in doc.go rather than left for a
// user to discover, because a client that believes a cancel worked and got a
// full result set has been misled.
func (s *session) cancel() { s.cancelFlag.set() }

// serve runs the whole session.
func (s *session) serve() error {
	defer s.release()
	// Unregistering is deferred before startup rather than after it, because
	// startup can fail after the session has been admitted; a session that
	// returned while still in the registry would be a permanently occupied
	// connection slot and a cancel target that no longer exists. Unregistering
	// a session that was never registered is a no-op, since a backend PID is
	// assigned only on admission and is never zero.
	defer s.server.unregister(s)
	if err := s.startup(); err != nil {
		s.reportStartupFailure(err)
		return err
	}
	return s.loop()
}

// release drops everything the session owns. It runs once, on every exit path.
func (s *session) release() {
	for _, p := range s.portals {
		p.release()
	}
	s.portals = nil
	for _, st := range s.statements {
		st.release()
	}
	s.statements = nil
	s.exec.Release()
}

// reportStartupFailure tells the client why the connection is being closed,
// when there is a client left to tell.
func (s *session) reportStartupFailure(err error) {
	var pg *pgError
	if !errors.As(err, &pg) {
		// A transport failure has no client to report to.
		return
	}
	s.w.errorResponse(pg)
	_ = s.flush()
}

// flush pushes buffered output, applying the write deadline.
func (s *session) flush() error {
	deadline(s.conn, s.conn.SetWriteDeadline, s.server.opts.WriteTimeout)
	return s.w.flush()
}

// startup negotiates the connection: SSL and GSS refusals, the startup packet,
// authentication, the parameter report, and the backend key.
func (s *session) startup() error {
	for {
		deadline(s.conn, s.conn.SetReadDeadline, s.server.opts.ReadTimeout)
		code, body, err := s.r.startup()
		if err != nil {
			return err
		}
		switch code {
		case codeSSLRequest, codeGSSENCRequest:
			// A single byte, not a framed message: this reply is read before
			// the client has agreed on a framing. 'N' means "not available",
			// after which a conforming client continues in the clear or gives
			// up. A server that answered 'S' without a TLS handshake behind it
			// would hang every client that asked.
			s.w.write([]byte{'N'})
			if err := s.flush(); err != nil {
				return err
			}
			continue

		case codeCancelRequest:
			f := fields{b: body}
			pid := f.int32()
			secret := f.int32()
			if err := f.end(); err != nil {
				return err
			}
			s.server.cancelRequest(pid, secret)
			// The protocol specifies no reply to a cancel request at all, and
			// the connection is closed. Answering would let a peer probe for
			// valid backend keys.
			return nil

		case protocolVersion30:
			return s.negotiate(body)

		default:
			major := code >> 16
			return fatal(sqlstateFeatureNotSupported, fmt.Sprintf(
				"unsupported frontend protocol %d.%d; this server speaks 3.0",
				major, code&0xffff))
		}
	}
}

// negotiate consumes the startup packet's parameter list and authenticates.
func (s *session) negotiate(body []byte) error {
	for name, p := range parameters {
		s.params[name] = p.initial
	}
	f := fields{b: body}
	for {
		name := f.cstring()
		if f.bad() {
			return protocolError(errTruncated)
		}
		if name == "" {
			break
		}
		value := f.cstring()
		if f.bad() {
			return protocolError(errTruncated)
		}
		if err := s.startupParameter(name, value); err != nil {
			return err
		}
	}
	if s.user == "" {
		return fatal(sqlstateInvalidAuthorization,
			"no PostgreSQL user name was specified in the startup packet")
	}
	if s.server.opts.Database != "" && s.database != s.server.opts.Database {
		return fatal(sqlstateInvalidAuthorization, fmt.Sprintf(
			"database %q does not exist; this server serves %q",
			s.database, s.server.opts.Database))
	}
	if s.database == "" {
		s.database = s.user
	}

	// The backend key is generated before the session enters the registry.
	// After registration another goroutine may read secret to match a cancel
	// request, and writing it afterwards would be a data race with a reader
	// that is by construction on another connection.
	secret, err := randomKey()
	if err != nil {
		return err
	}
	s.secret = secret
	// Admission happens before authentication, not after. SCRAM costs a PBKDF2
	// round per attempt, so a limit enforced afterwards would bound the sessions
	// that succeeded while leaving the work an unauthenticated peer can demand
	// unbounded. serve unregisters on every exit path, so a connection admitted
	// here and refused below frees its slot.
	if !s.server.register(s) {
		return fatal(sqlstateTooManyConnections,
			"this server is closed or has reached its connection limit")
	}
	if err := s.server.opts.Auth.authenticate(s, s.user); err != nil {
		return err
	}
	s.w.authenticationOK()

	s.reportParameters()
	s.w.backendKeyData(s.pid, s.secret)
	s.w.readyForQuery(statusIdle)
	return s.flush()
}

// startupParameter applies one startup-packet parameter.
//
// The four protocol-level names are handled here; everything else goes through
// the same policy as a SET, and a refusal is fatal because a client that could
// not have its connection configured the way it asked has not connected. That
// is PostgreSQL's own behaviour for an unrecognized parameter at startup, and
// it is the behaviour this package's refuse-rather-than-ignore rule implies:
// accepting a startup parameter and ignoring it is the same mistake as
// accepting a SET and ignoring it, made once per connection instead of once per
// statement.
func (s *session) startupParameter(name, value string) error {
	switch strings.ToLower(name) {
	case "user":
		s.user = value
		return nil
	case "database":
		s.database = value
		return nil
	case "options":
		if strings.TrimSpace(value) == "" {
			return nil
		}
		return fatal(sqlstateFeatureNotSupported,
			"the startup 'options' parameter is not supported: it carries command-line "+
				"settings this server has no equivalent for")
	case "replication":
		if strings.EqualFold(value, "false") || value == "" {
			return nil
		}
		return fatal(sqlstateFeatureNotSupported,
			"this server does not implement the replication protocol")
	case "client_encoding", "datestyle", "intervalstyle", "timezone",
		"application_name", "extra_float_digits", "standard_conforming_strings",
		"bytea_output", "search_path":
		// Fall through to the shared policy below.
	}
	canonical, p, ok := lookupParameter(name)
	if !ok {
		e := unsupportedParameter(name)
		return fatal(e.code, e.message).withHint(e.hint)
	}
	if p.check != nil {
		if err := p.check(value); err != nil {
			return fatal(sqlstateInvalidParameterValue, err.Error())
		}
	}
	s.params[canonical] = value
	return nil
}

// reportParameters sends the ParameterStatus messages a client reads at
// startup. The reported set is PostgreSQL's GUC_REPORT set restricted to the
// parameters that mean anything here, plus the two server_version spellings
// every client library looks for.
func (s *session) reportParameters() {
	s.w.parameterStatus("server_version", serverVersion)
	s.w.parameterStatus("server_version_num", serverVersionNum)
	s.w.parameterStatus("server_encoding", "UTF8")
	s.w.parameterStatus("integer_datetimes", "on")
	s.w.parameterStatus("is_superuser", "off")
	s.w.parameterStatus("session_authorization", s.user)
	for name, p := range parameters {
		if p.report {
			s.w.parameterStatus(name, s.params[name])
		}
	}
}

// nextAuthReply reads the client's next authentication message.
func (s *session) nextAuthReply() ([]byte, error) {
	deadline(s.conn, s.conn.SetReadDeadline, s.server.opts.ReadTimeout)
	tag, body, err := s.r.next(maxStartupPacket)
	if err != nil {
		return nil, err
	}
	if tag != msgPasswordOrSASL {
		return nil, fatal(sqlstateProtocolViolation, fmt.Sprintf(
			"expected an authentication response, got message type %q", string(rune(tag))))
	}
	return body, nil
}

// loop runs the message loop until the client terminates or the connection
// fails.
func (s *session) loop() error {
	for {
		deadline(s.conn, s.conn.SetReadDeadline, s.server.opts.IdleTimeout)
		tag, body, err := s.r.next(maxMessageBody)
		if err != nil {
			if s.terminated || errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := s.dispatch(tag, body); err != nil {
			return err
		}
		if s.terminated {
			return nil
		}
	}
}

// dispatch decodes and handles one frontend message.
//
// A decode failure is fatal. It has to be: the framing is intact (the message
// was read at its declared length) but the sender and this server disagree
// about the message's contents, and there is no way to resynchronize a
// disagreement about semantics. Continuing would mean guessing what the client
// meant.
func (s *session) dispatch(tag byte, body []byte) error {
	if err := decodeFrontend(&s.msg, tag, body); err != nil {
		code := sqlstateProtocolViolation
		if errors.Is(err, errUnimplementedSubprotocol) {
			code = sqlstateFeatureNotSupported
		}
		return s.die(fatal(code, err.Error()))
	}
	switch tag {
	case msgTerminate:
		s.terminated = true
		return nil
	case msgQuery:
		if s.failed {
			// The extended protocol's error state discards every message up to
			// the next Sync, and a simple Query is one of them. Running it would
			// answer with a ReadyForQuery of its own, telling a pipelining
			// client the batch had resynchronized while the messages after it
			// were still being thrown away.
			return nil
		}
		return s.simpleQuery(s.msg.query)
	case msgParse, msgBind, msgDescribe, msgExecute, msgClose, msgSync, msgFlush:
		return s.extended(tag)
	default:
		// A 'p' after authentication is the only tag that reaches here:
		// decodeFrontend accepts it, and there is no exchange left for it to
		// belong to.
		return s.die(fatal(sqlstateProtocolViolation,
			"an authentication response arrived after authentication had completed"))
	}
}

// die reports a session-ending failure to the client and returns it. Telling
// the client before closing is what turns a dropped connection into a
// diagnosable error on the other end.
func (s *session) die(e *pgError) error {
	s.w.errorResponse(e)
	_ = s.flush()
	return e
}

// simpleQuery runs the 'Q' path: split, run each statement, and finish with one
// ReadyForQuery.
//
// The protocol's rule is that an error abandons the rest of the message, and
// that exactly one ReadyForQuery closes it however it went. Sending one per
// statement is the classic bug and desynchronizes every client that counts
// them.
func (s *session) simpleQuery(sql string) error {
	statements := splitStatements(sql)
	for _, text := range statements {
		if s.cancelFlag.take() {
			s.w.errorResponse(newError(sqlstateQueryCanceled,
				"canceling statement due to a cancel request"))
			break
		}
		if err := s.runSimple(text); err != nil {
			var pg *pgError
			if !errors.As(err, &pg) {
				return err
			}
			s.w.errorResponse(pg)
			if pg.severity == "FATAL" {
				_ = s.flush()
				return pg
			}
			break
		}
	}
	s.w.readyForQuery(statusIdle)
	return s.flush()
}

// runSimple executes one statement of a simple query, in text format
// throughout, as the protocol requires: the simple query message has no place
// to request a format, so every column is text.
func (s *session) runSimple(text string) error {
	stmt, err := s.prepare("", text)
	if err != nil {
		return err
	}
	// A simple-query statement is not retained, so its plan is released as soon
	// as the rows are gone.
	defer stmt.release()

	if stmt.kind == kindEmpty {
		s.w.emptyQueryResponse()
		return nil
	}
	p := &portal{stmt: stmt}
	defer p.release()
	if stmt.stmt != nil {
		if stmt.stmt.NumParams() != 0 {
			return newError(sqlstateSyntaxError,
				"a statement with placeholders cannot be run through the simple query "+
					"protocol; use Parse and Bind, which every client library exposes")
		}
	}
	if len(stmt.cols) != 0 {
		s.w.rowDescription(stmt.cols, nil)
	}
	return s.execute(p, 0)
}

// prepare turns statement text into a prepared statement, classifying it first
// so that a refusal carries the right SQLSTATE.
func (s *session) prepare(name, text string) (*prepared, error) {
	kind, reason := classify(text)
	p := &prepared{name: name, sql: text, kind: kind}
	switch kind {
	case kindEmpty:
		return p, nil

	case kindUnsupported:
		return nil, newError(sqlstateFeatureNotSupported, reason)

	case kindUnknown:
		return nil, newError(sqlstateSyntaxError,
			"this statement does not begin with a keyword this server recognizes").
			withHint("the SQL surface is SELECT, plus SET, RESET, SHOW, and DISCARD")

	case kindSet, kindReset:
		var cmd setCommand
		var err error
		if kind == kindSet {
			cmd, err = parseSet(text)
		} else {
			cmd, err = parseReset(text)
		}
		if err != nil {
			return nil, err
		}
		if !cmd.all {
			if _, _, ok := lookupParameter(cmd.name); !ok {
				return nil, unsupportedParameter(cmd.name)
			}
		}
		p.set, p.tag = cmd, "SET"
		return p, nil

	case kindShow:
		name, err := parseShow(text)
		if err != nil {
			return nil, err
		}
		fixed, err := s.showResult(name)
		if err != nil {
			return nil, err
		}
		p.fixed, p.cols, p.tag = fixed, fixed.cols, fixed.tag
		return p, nil

	case kindDiscard:
		p.tag = "DISCARD"
		return p, nil
	}

	// A SELECT: the shim's fixed table first, then the real front end.
	shim := shimFunctions{database: s.database, user: s.user, pid: s.pid}
	if fixed, ok := shim.parseFixedSelect(text); ok {
		p.fixed, p.cols, p.tag = &fixed, fixed.cols, fixed.tag
		return p, nil
	}
	// Numbered parameters are rewritten to this dialect's '?' before the parser
	// ever sees them; see command.go for why that happens here rather than in
	// the parser. The rewrite preserves byte offsets, so the error reported
	// below still points at the byte the client wrote.
	lowered, order, highest, err := rewriteNumberedParameters(text)
	if err != nil {
		return nil, err
	}
	stmt, err := query.PrepareStatement(lowered)
	if err != nil {
		return nil, asPGErrorIn(err, text)
	}
	if order != nil && len(order) != stmt.NumParams() {
		stmt.Release()
		return nil, newError(sqlstateInternalError,
			"the numbered-parameter rewrite and the parser disagree about the placeholder count")
	}
	p.stmt = stmt
	p.paramOrder = order
	p.wireParams = stmt.NumParams()
	if order != nil {
		p.wireParams = highest
	}
	p.cols = columnsFor(nil, stmt.Columns(), stmt.AppendSchema(nil))
	return p, nil
}

// showResult builds the one-row, one-column result SHOW returns.
func (s *session) showResult(name string) (*fixedResult, error) {
	if strings.EqualFold(name, "ALL") {
		result := &fixedResult{
			cols: []column{
				{name: "name", typ: typeText},
				{name: "setting", typ: typeText},
				{name: "description", typ: typeText},
			},
			tag: "SHOW",
		}
		for canonical, p := range parameters {
			value := s.params[canonical]
			result.rows = append(result.rows, []*string{
				strPtr(canonical), strPtr(value), strPtr(p.why),
			})
		}
		sortRowsByFirstColumn(result.rows)
		return result, nil
	}
	canonical, _, ok := lookupParameter(name)
	if !ok {
		// SHOW of a name this server does not have is an undefined object, not
		// a missing feature: the client asked about something, and the answer
		// is that it does not exist here.
		return nil, newError(sqlstateUndefinedObject,
			fmt.Sprintf("unrecognized configuration parameter %q", name))
	}
	value := s.params[canonical]
	return &fixedResult{
		cols: []column{{name: canonical, typ: typeText}},
		rows: [][]*string{{strPtr(value)}},
		tag:  "SHOW",
	}, nil
}

func strPtr(s string) *string { return &s }

// sortRowsByFirstColumn keeps SHOW ALL deterministic. Go's map iteration is
// randomized, and a result set whose row order changed between calls would make
// every test of it flaky and every diff of its output noise.
func sortRowsByFirstColumn(rows [][]*string) {
	slices.SortFunc(rows, func(a, b []*string) int { return strings.Compare(*a[0], *b[0]) })
}

// execute runs a portal for up to limit rows, writing DataRows and finishing
// with either CommandComplete or PortalSuspended.
//
// limit is Execute's row maximum; zero means every remaining row.
func (s *session) execute(p *portal, limit int32) error {
	stmt := p.stmt
	switch {
	case stmt.kind == kindSet || stmt.kind == kindReset:
		if err := s.applySet(stmt.set); err != nil {
			return err
		}
		s.w.commandComplete(stmt.tag)
		return nil

	case stmt.kind == kindDiscard:
		// DISCARD is honoured rather than ignored, because everything it names
		// that exists here really is discardable: the prepared statements and
		// the portals. A client pooling connections uses it to be sure it got a
		// clean one, and a server that answered "DISCARD" without discarding
		// would be answering a question about state with a lie.
		s.discardAll()
		s.w.commandComplete(stmt.tag)
		return nil

	case stmt.fixed != nil:
		return s.executeFixed(p, limit)

	case stmt.stmt != nil:
		return s.executeQuery(p, limit)
	}

	s.w.emptyQueryResponse()
	return nil
}

func (s *session) discardAll() {
	for name, p := range s.portals {
		p.release()
		delete(s.portals, name)
	}
	for name, st := range s.statements {
		st.release()
		delete(s.statements, name)
	}
}

// applySet assigns or resets a run-time parameter, announcing the change when
// the parameter is one clients cache.
func (s *session) applySet(cmd setCommand) error {
	if cmd.all {
		for name, p := range parameters {
			s.setParameter(name, p, p.initial)
		}
		return nil
	}
	canonical, p, ok := lookupParameter(cmd.name)
	if !ok {
		return unsupportedParameter(cmd.name)
	}
	value := cmd.value
	if cmd.reset {
		value = p.initial
	} else if p.check != nil {
		if err := p.check(value); err != nil {
			return newError(sqlstateInvalidParameterValue, err.Error())
		}
	}
	s.setParameter(canonical, p, value)
	return nil
}

func (s *session) setParameter(name string, p *parameter, value string) {
	if s.params[name] == value {
		return
	}
	s.params[name] = value
	if p.report {
		s.w.parameterStatus(name, value)
	}
}

// executeFixed serves rows from a result this package produced itself.
func (s *session) executeFixed(p *portal, limit int32) error {
	fixed := p.stmt.fixed
	sent := 0
	for p.row < len(fixed.rows) {
		if limit > 0 && int32(sent) >= limit {
			s.w.portalSuspended()
			return nil
		}
		s.w.fixedRow(fixed.cols, fixed.rows[p.row], p.formats)
		p.row++
		sent++
	}
	s.w.commandComplete(fixed.tag)
	return nil
}

// executeQuery runs or resumes an engine query.
func (s *session) executeQuery(p *portal, limit int32) error {
	if p.exhausted {
		// Re-executing a drained portal produces no rows, and the tag says so.
		// PostgreSQL reports the rows of the Execute that is completing rather
		// than the portal's running total, and a client that adds the tags of a
		// suspended portal's runs together has to be able to trust that.
		s.w.commandComplete(commandTag(0))
		return nil
	}
	if !p.started {
		if s.cancelFlag.take() {
			return newError(sqlstateQueryCanceled,
				"canceling statement due to a cancel request")
		}
		if err := s.start(p); err != nil {
			return err
		}
	} else if p.generation != s.generation {
		// The portal is provably unusable from here, so its snapshot is dropped
		// now rather than at Close. A durable snapshot pins a storage generation
		// against reuse, and a client that abandons a refused portal without
		// closing it would otherwise block extent reclamation for the rest of
		// the connection.
		p.release()
		p.exhausted = true
		return newError(sqlstateObjectNotInPrereqState,
			"this suspended portal can no longer be resumed because another statement has "+
				"executed on the same connection").
			withHint("this server holds one result set per connection; read a portal to " +
				"completion before executing anything else on the same connection, or run " +
				"the statement without a row limit")
	}

	sent := 0
	for p.cursor.Next() {
		cells := s.rowCells(&p.cursor, len(p.stmt.cols))
		if err := s.rows.row(s.w, cells, p.stmt.cols, p.formats); err != nil {
			return err
		}
		sent++
		if limit > 0 && int32(sent) >= limit {
			s.w.portalSuspended()
			return nil
		}
	}
	p.exhausted = true
	p.lease.release()
	s.w.commandComplete(commandTag(sent))
	return nil
}

// start binds and executes a portal's statement.
func (s *session) start(p *portal) error {
	stmt := p.stmt.stmt
	l, err := s.server.src.resolve(stmt.Collection(), stmt.NumJoins() != 0)
	if err != nil {
		return err
	}
	// Every previously executed portal's cursor is invalidated by this
	// execution; bumping the generation before running is what makes the
	// invalidation observable rather than silent.
	s.generation++
	cursor, err := stmt.RunInto(&s.exec, l.src, p.args)
	if err != nil {
		l.release()
		return asPGErrorIn(err, p.stmt.sql)
	}
	p.cursor = cursor
	p.lease = l
	p.started = true
	p.generation = s.generation
	return nil
}

// rowCells collects the current row's cells into the session's scratch.
func (s *session) rowCells(c *query.Cursor, n int) []query.Cell {
	if cap(s.cells) < n {
		s.cells = make([]query.Cell, n)
	}
	s.cells = s.cells[:n]
	for i := range n {
		s.cells[i] = c.Cell(i)
	}
	return s.cells
}
