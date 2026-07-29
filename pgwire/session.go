package pgwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
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

	user     string
	database string
	// params are the run-time settings in effect. It is keyed by the canonical
	// parameter spelling, never by whatever case the client used.
	params map[string]string

	// exec is the read-only-source execution storage. A writable source uses
	// the typed SQL session below, which owns an equivalent executor.
	exec query.Exec
	// sql is non-nil only for a FromSQLDatabase source. It is the shared typed
	// SQL runtime used by both database/sql and this protocol adapter.
	sql         *sqldriver.Session
	queryCancel query.CancelFlag
	// cancelMu makes the active-command boundary atomic with delivery of an
	// out-of-band CancelRequest. Without it, a request received while this
	// backend was idle could leave queryCancel armed and cancel the next
	// statement instead. The mutex is touched once at command entry/exit and
	// once per CancelRequest, never at an executor checkpoint or row.
	cancelMu       sync.Mutex
	cancelActive   bool
	cancelShutdown bool

	// implicitExtended records the transaction opened for an extended-query
	// batch. An explicit BEGIN leaves it false and therefore survives Sync.
	implicitExtended bool
	// DDL must be the sole catalog execution between Sync points because the
	// storage catalog cannot yet publish DDL inside a transaction.
	extendedDDL bool
	extendedSQL bool
	// Session parameter changes are not transactionally undoable yet. They
	// therefore require their own Sync-delimited batch instead of pretending
	// to roll back with a later stored-row error.
	extendedSessionChange bool

	statements map[string]*prepared
	portals    map[string]*portal
	// statementBytes and portalBytes account input retained by the two object
	// tables. statementBytes includes statement/query names, parameter OIDs,
	// numbered-parameter metadata, and a conservative parser/AST/lowered-plan
	// shape charge, not merely SQL text. Per-message bounds alone are not
	// enough: one session may hold many named statements and portals
	// concurrently.
	statementBytes int
	// statementBindBytes accounts the high-water literal storage prepared
	// statement compilers may retain after binding. It is separate from portal
	// bytes because closing a portal does not release its statement's compiler
	// arenas.
	statementBindBytes int
	portalBytes        int

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
// It is a small discriminated union rather than an interface: a shared-runtime
// SQL statement, a legacy read-only query statement, a fixed compatibility
// result, a session parameter command, or transaction control. All variants
// have explicit ownership in release.
type prepared struct {
	name string
	sql  string
	kind statementKind
	// retainedBytes is this statement's charge against the session-wide
	// prepared-input bound: its statement name, SQL text, parameter OIDs,
	// PostgreSQL placeholder mappings/roles, and conservative retained
	// parser/compiler shape.
	retainedBytes int
	// bindBytes is the largest bound-literal expansion admitted for this
	// statement. The compiler retains high-water arenas between executions, so
	// this charge remains until the statement closes.
	bindBytes int

	// stmt is the lowered plan used by legacy read-only sources.
	stmt *query.Statement
	// runtime is the shared typed SQL runtime statement for a writable catalog.
	runtime *sqldriver.Prepared
	// paramKinds consolidates the scalar/document role of every wire parameter,
	// including repeated $n occurrences.
	paramKinds []sqldriver.ParamKind
	// fixed is the result of a statement this package answers itself.
	fixed *fixedResult
	// set is the decoded SET or RESET.
	set setCommand
	// cols is the RowDescription, empty for a statement that returns no rows.
	cols []column
	// tag is the CommandComplete tag for a statement with no row count.
	tag string
	// txReadOnly is the one BEGIN mode the runtime needs to carry.
	txReadOnly bool
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
	if p.runtime != nil {
		_ = p.runtime.Close()
		p.runtime = nil
	}
	p.paramKinds = nil
}

// A portal is one bound, executable instance of a prepared statement.
//
// A suspended portal borrows the session's one executor. The next execution
// invalidates it rather than allocating one executor per portal. Resumption
// after invalidation returns 55000; silently reading overwritten result storage
// is never an option.
type portal struct {
	name string
	stmt *prepared
	// args are the bound parameters, copied out of the Bind message because the
	// message's buffer is reused by the next one.
	args []any
	// wireArgs holds each wire parameter exactly once. args is then a cheap
	// permutation of these values for $n placeholders, including repeated
	// references. Without this intermediate vector a statement containing $1
	// many times copied the same value many times and defeated the message-size
	// bound.
	wireArgs []any
	// valueSlots own pointer-shaped scalar boxes for wireArgs. Returning a
	// string or Number directly as any makes Go allocate an interface payload
	// on every Bind; pointing at a stable slot keeps the hot unnamed-portal path
	// allocation-free while retaining the same typed value.
	valueSlots []boundValueSlot
	// argStore backs the string and []byte arguments for the same reason.
	argStore []byte
	// decodeStore owns decoded JSON string contents. The wire spelling remains
	// in argStore for exact accounting and portal lifetime; escaped JSON strings
	// need a distinct destination because their comparison value is the decoded
	// string, not the quoted wire representation.
	decodeStore []byte
	formats     []int16
	// literalCharges holds one compiler-retention upper bound per wire
	// parameter. Keeping it beside wireArgs means a repeated $1 scans and
	// classifies the value once, then sums a machine word per occurrence.
	literalCharges []int
	// retainedBytes is this portal's conservative charge against the
	// session-wide portal bound.
	retainedBytes int

	// started marks a portal that has begun executing; row is the next fixed
	// row to send; cursor and lease belong to an engine execution.
	started     bool
	exhausted   bool
	row         int
	cursor      query.Cursor
	runtime     sqldriver.Cursor
	runtimeOpen bool
	lease       lease
	generation  uint64
	invalidated bool
}

// boundValueSlot has one stable address for every scalar representation the
// PostgreSQL decoder can produce. Only one field is live for a given Bind.
// query.Statement accepts the corresponding pointer-shaped literals and copies
// their bytes into its own reusable compiler arena before execution.
type boundValueSlot struct {
	text     string
	number   query.Number
	integer  int64
	floating float64
	boolean  bool
}

func (p *portal) release() {
	p.lease.release()
	if p.runtimeOpen {
		_ = p.runtime.Close()
		p.runtimeOpen = false
	}
	p.cursor = query.Cursor{}
	p.started = false
}

// resetForBind returns an existing portal object to its just-created state
// while preserving the reusable argument buffers. The unnamed portal is
// replaced on nearly every prepared execution; reusing the object as well as
// its buffers is what makes that steady-state path allocation-free.
func (p *portal) resetForBind(name string, stmt *prepared) {
	p.release()
	p.name = name
	p.stmt = stmt
	p.retainedBytes = 0
	p.exhausted = false
	p.row = 0
	p.generation = 0
	p.invalidated = false
}

// sessionQueryWorkers keeps one protocol connection to one executor worker.
// PostgreSQL obtains concurrency from independent sessions; allowing every
// admitted session to inherit query's zero-value GOMAXPROCS policy would
// multiply retained durable worker pools by MaxConnections.
const sessionQueryWorkers = 1

func newSession(s *Server, conn net.Conn) *session {
	session := &session{
		server:     s,
		conn:       conn,
		r:          newReader(conn, 16<<10),
		w:          newWriter(conn, 16<<10),
		params:     map[string]string{},
		statements: map[string]*prepared{},
		portals:    map[string]*portal{},
	}
	session.exec.Options.Workers = sessionQueryWorkers
	session.exec.Options.ResultRows = s.opts.MaxResultRows
	session.exec.Options.ResultBytes = s.opts.MaxResultBytes
	session.exec.Options.Cancel = &session.queryCancel
	// bufio.Writer may flush from inside Write when one DataRow is larger than
	// its buffer. Arming only session.flush is therefore too late: the socket
	// write has already happened. Refresh immediately before every operation
	// that can reach the connection, including an automatic flush.
	session.w.beforeWrite = func() {
		deadline(conn, conn.SetWriteDeadline, s.opts.WriteTimeout)
	}
	return session
}

// out and push implement authConn, which is how a SASL mechanism reaches the
// connection without being handed the session.
func (s *session) out() *writer { return s.w }
func (s *session) push() error  { return s.flush() }

// cancel records that a cancel request arrived for this session.
//
// One shared atomic flag reaches heap/durable scans, workers, joins, filtered
// DML, spills, and DataRow delivery. Execution cleans its reusable pipeline and
// returns ErrCanceled; rows already written cannot be retracted. takeCancel
// uses the flag's atomic Take operation, so a racing request is either consumed
// by this call or remains armed for the next checkpoint and can never disappear
// in a load-then-reset window.
func (s *session) cancel() {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if !s.cancelActive {
		return
	}
	s.queryCancel.Cancel()
}

// shutdownCancel is stronger than a protocol CancelRequest: Close must also
// stop a command whose bytes were already read but whose cancellation window
// has not opened yet. Latching cancelShutdown makes a future beginCancelable
// re-arm the executor instead of clearing the shutdown signal.
func (s *session) shutdownCancel() {
	s.cancelMu.Lock()
	s.cancelShutdown = true
	s.queryCancel.Cancel()
	s.cancelMu.Unlock()
}

// beginCancelable marks one frontend command as the only target of an
// out-of-band CancelRequest. Session execution is single-threaded, so command
// windows never nest.
func (s *session) beginCancelable() {
	s.cancelMu.Lock()
	s.queryCancel.Reset()
	s.cancelActive = true
	if s.cancelShutdown {
		s.queryCancel.Cancel()
	}
	s.cancelMu.Unlock()
}

// endCancelable closes the current cancellation window and consumes any
// request which raced with its final response. Holding cancelMu across both
// operations is the crucial handshake: a cancel either arms this command
// before the reset or observes the backend as idle and is ignored; it cannot
// arm the next command.
func (s *session) endCancelable() {
	s.cancelMu.Lock()
	s.cancelActive = false
	s.queryCancel.Reset()
	s.cancelMu.Unlock()
}

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
	s.portalBytes = 0
	for _, st := range s.statements {
		st.release()
	}
	s.statements = nil
	s.statementBytes = 0
	s.statementBindBytes = 0
	s.exec.Release()
	if s.sql != nil {
		_ = s.sql.Close()
		s.sql = nil
	}
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
	return s.w.flush()
}

// startup negotiates the connection: SSL and GSS refusals, the startup packet,
// authentication, the parameter report, and the backend key.
func (s *session) startup() error {
	var sawSSLRequest, sawGSSENCRequest bool
	for {
		deadline(s.conn, s.conn.SetReadDeadline, s.server.opts.ReadTimeout)
		code, body, err := s.r.startup()
		if err != nil {
			return err
		}
		switch code {
		case codeSSLRequest, codeGSSENCRequest:
			if len(body) != 0 {
				return protocolError(errTrailing)
			}
			requestName := "SSLRequest"
			seen := &sawSSLRequest
			if code == codeGSSENCRequest {
				requestName = "GSSENCRequest"
				seen = &sawGSSENCRequest
			}
			if *seen {
				return fatal(sqlstateProtocolViolation,
					requestName+" may be sent at most once during startup")
			}
			*seen = true
			// A single byte, not a framed message: this reply is read before
			// the client has agreed on a framing. 'N' means "not available",
			// after which a conforming client continues in the clear or gives
			// up. A server that answered 'S' without a TLS handshake behind it
			// would hang every client that asked.
			//
			// PostgreSQL permits one fallback from SSL to GSS after an 'N'
			// response (and the reverse order), so each mechanism gets one
			// attempt rather than imposing one combined attempt. That also makes
			// the number of deadline resets finite for an unauthenticated peer.
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
				return protocolError(err)
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
	if err := f.end(); err != nil {
		return protocolError(err)
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
	if source, ok := s.server.src.(sqlSessionSource); ok {
		runtime, err := source.openSession(context.Background())
		if err != nil {
			return fatal(sqlstateInternalError,
				"could not open this connection's SQL session: "+err.Error())
		}
		if err := runtime.SetCancelFlag(&s.queryCancel); err != nil {
			_ = runtime.Close()
			return fatal(sqlstateInternalError,
				"could not configure this connection's SQL session: "+err.Error())
		}
		s.sql = runtime
	}
	s.w.authenticationOK()

	s.reportParameters()
	s.w.backendKeyData(s.pid, s.secret)
	s.w.readyForQuery(s.transactionStatus())
	return s.flush()
}

func (s *session) transactionStatus() byte {
	if s.sql == nil {
		return statusIdle
	}
	switch s.sql.State() {
	case sqldriver.SessionInTransaction:
		return statusInTx
	case sqldriver.SessionFailedTransaction:
		return statusFailedT
	default:
		return statusIdle
	}
}

func (s *session) markTransactionFailed() {
	if s.sql != nil {
		s.sql.MarkFailed()
	}
}

func (s *session) takeCancel() bool {
	return s.queryCancel.Take()
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
	if !utf8.ValidString(name) || !utf8.ValidString(value) {
		return fatal(sqlstateCharacterNotInRepertoire,
			"startup parameter names and values must be valid UTF-8")
	}
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
	defer s.msg.releaseBodyViews()
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
	s.beginCancelable()
	defer s.endCancelable()

	// PostgreSQL defines a simple Query as destroying both unnamed extended-
	// protocol objects. Closing the unnamed statement also closes every portal
	// built from it, including named portals; the remaining unnamed portal may
	// have been built from a named statement and is closed separately.
	s.destroyUnnamed()
	implicit, preflightErr := s.preflightSimpleQuery(sql)
	if preflightErr != nil {
		s.markTransactionFailed()
		s.w.errorResponse(preflightErr)
		s.w.readyForQuery(s.transactionStatus())
		return s.flush()
	}
	if implicit {
		if err := s.sql.Begin(context.Background(), sqldriver.TxOptions{}); err != nil {
			s.w.errorResponse(asPGError(err))
			s.w.readyForQuery(s.transactionStatus())
			return s.flush()
		}
	}

	iter := statementIterator{src: sql}
	count := 0
	failed := false
	for {
		text, ok := iter.next()
		if !ok {
			break
		}
		count++
		if count > maxSimpleStatements {
			s.w.errorResponse(newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
				"a simple Query message may contain at most %d non-empty statements",
				maxSimpleStatements)))
			failed = true
			break
		}
		if s.takeCancel() {
			s.w.errorResponse(newError(sqlstateQueryCanceled,
				"canceling statement due to a cancel request"))
			failed = true
			break
		}
		if err := s.runSimple(text); err != nil {
			var pg *pgError
			if !errors.As(err, &pg) {
				return err
			}
			s.markTransactionFailed()
			s.w.errorResponse(pg)
			failed = true
			if pg.severity == "FATAL" {
				_ = s.flush()
				return pg
			}
			break
		}
	}
	if implicit {
		if !failed && s.takeCancel() {
			s.markTransactionFailed()
			s.w.errorResponse(queryCanceled())
			failed = true
		}
		if failed {
			if err := s.sql.Rollback(context.Background()); err != nil {
				s.w.errorResponse(asPGError(err))
			}
		} else if err := s.sql.Commit(context.Background()); err != nil {
			s.w.errorResponse(asPGError(err))
		}
		// A cancel racing with the indivisible commit/rollback step cannot
		// replace the storage outcome that operation returned (including an
		// explicitly unknown durability outcome) and must not leak into the
		// next Query message.
		_ = s.takeCancel()
	}
	// Every non-fatal exit completes this Query message. Consume a cancel
	// that raced with final encoding or cleanup so it cannot affect the
	// next message; earlier checkpoints already turned every cancel seen
	// before a reversible outcome into 57014.
	_ = s.takeCancel()
	s.w.readyForQuery(s.transactionStatus())
	return s.flush()
}

func (s *session) preflightSimpleQuery(src string) (bool, *pgError) {
	if s.sql == nil {
		return false, nil
	}
	iter := statementIterator{src: src}
	count := 0
	var first statementKind
	catalogSQL := false
	ddl := false
	dml := false
	sessionChange := false
	txCount := 0
	beginCount := 0
	terminalCount := 0
	terminalAt := 0
	for {
		text, ok := iter.next()
		if !ok {
			break
		}
		count++
		if count > maxSimpleStatements {
			return false, newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
				"a simple Query message may contain at most %d non-empty statements",
				maxSimpleStatements))
		}
		kind, _ := classify(text)
		if count == 1 {
			first = kind
		}
		switch kind {
		case kindSelect:
			catalogSQL = true
		case kindCatalogSQL:
			catalogSQL = true
			scan := scanner{src: text}
			if strings.EqualFold(scan.word(), "CREATE") {
				ddl = true
			} else {
				dml = true
			}
		case kindSet, kindReset, kindDiscard:
			sessionChange = true
		case kindBegin:
			txCount++
			beginCount++
		case kindCommit, kindRollback:
			txCount++
			terminalCount++
			terminalAt = count
		}
	}
	if count <= 1 {
		// A simple DML Query is an implicit transaction even when it carries
		// one statement. Wrapping that mutation matters for
		// cancellation: a CancelRequest observed after DML has staged its
		// result but before CommandComplete can still roll the mutation back.
		// DDL is the deliberate exception because the catalog cannot publish it
		// through the transaction overlay.
		return count == 1 && s.sql.State() == sqldriver.SessionIdle && dml, nil
	}
	if ddl {
		return false, newError(sqlstateFeatureNotSupported,
			"DDL must be the only non-empty statement in a simple Query message").
			withHint("send CREATE TABLE or CREATE INDEX in its own Query message")
	}
	if sessionChange && catalogSQL {
		return false, newError(sqlstateFeatureNotSupported,
			"SET, RESET, and DISCARD cannot be mixed with stored-row SQL in one Query message").
			withHint("send the session command and the SQL statement as separate Query messages")
	}
	if txCount != 0 {
		state := s.sql.State()
		switch {
		case state == sqldriver.SessionIdle && first != kindBegin:
			return false, newError(sqlstateFeatureNotSupported,
				"a multi-statement transaction command must begin with BEGIN")
		case state != sqldriver.SessionIdle && beginCount != 0:
			return false, newError(sqlstateFeatureNotSupported,
				"BEGIN cannot appear inside an active transaction")
		case beginCount > 1:
			return false, newError(sqlstateFeatureNotSupported,
				"a simple Query message may open at most one explicit transaction")
		case terminalCount > 1:
			return false, newError(sqlstateFeatureNotSupported,
				"a simple Query message may contain at most one terminal COMMIT or ROLLBACK")
		case terminalCount == 1 && terminalAt != count:
			return false, newError(sqlstateFeatureNotSupported,
				"COMMIT or ROLLBACK must be the final statement in this simple Query message")
		}
		return false, nil
	}
	return s.sql.State() == sqldriver.SessionIdle && catalogSQL, nil
}

func (s *session) destroyUnnamed() {
	if stmt := s.statements[""]; stmt != nil {
		s.closePortalsOf(stmt)
		stmt.release()
		delete(s.statements, "")
		s.statementBytes -= stmt.retainedBytes
		s.statementBindBytes -= stmt.bindBytes
	}
	if p := s.portals[""]; p != nil {
		p.release()
		delete(s.portals, "")
		s.portalBytes -= p.retainedBytes
	}
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
	if stmt.runtime != nil && stmt.runtime.NumParams() != 0 {
		return newError(sqlstateSyntaxError,
			"a statement with placeholders cannot be run through the simple query "+
				"protocol; use Parse and Bind, which every client library exposes")
	}
	if len(stmt.cols) != 0 {
		if err := s.w.rowDescription(stmt.cols, nil); err != nil {
			return err
		}
	}
	return s.execute(p, 0)
}

// prepare turns statement text into a prepared statement, classifying it first
// so that a refusal carries the right SQLSTATE.
func (s *session) prepare(name, text string) (*prepared, error) {
	if !utf8.ValidString(text) {
		return nil, newError(sqlstateCharacterNotInRepertoire,
			"SQL text is not valid UTF-8")
	}
	kind, reason := classify(text)
	p := &prepared{name: name, sql: text, kind: kind}
	switch kind {
	case kindEmpty:
		return p, nil

	case kindCatalogSQL:
		if s.sql == nil {
			return nil, newError(sqlstateFeatureNotSupported,
				"this server source is read-only; DDL and DML require a SQL catalog").
				withHint("construct the server with FromSQLDatabase")
		}

	case kindBegin, kindCommit, kindRollback:
		if s.sql == nil {
			return nil, newError(sqlstateFeatureNotSupported,
				"transactions require a writable SQL catalog").
				withHint("construct the server with FromSQLDatabase")
		}
		readOnly, err := parseTransactionCommand(text, kind)
		if err != nil {
			return nil, err
		}
		p.txReadOnly = readOnly
		switch kind {
		case kindBegin:
			p.tag = "BEGIN"
		case kindCommit:
			p.tag = "COMMIT"
		case kindRollback:
			p.tag = "ROLLBACK"
		}
		return p, nil

	case kindUnsupported:
		return nil, newError(sqlstateFeatureNotSupported, reason)

	case kindUnknown:
		if reason != "" {
			return nil, newError(sqlstateSyntaxError, reason)
		}
		return nil, newError(sqlstateSyntaxError,
			"this statement does not begin with a keyword this server recognizes").
			withHint("the SQL surface is SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, " +
				"CREATE INDEX, transactions, SET, RESET, SHOW, and DISCARD")

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
		if err := parseDiscard(text); err != nil {
			return nil, err
		}
		p.tag = "DISCARD"
		return p, nil
	}

	// A SELECT tries the compatibility shim before the stored-row front end.
	if kind == kindSelect {
		shim := shimFunctions{database: s.database, user: s.user, pid: s.pid}
		fixed, ok, err := shim.parseFixedSelect(text)
		if err != nil {
			return nil, err
		}
		if ok {
			p.fixed, p.cols, p.tag = &fixed, fixed.cols, fixed.tag
			return p, nil
		}
	}
	// Numbered parameters are rewritten to this dialect's '?' before the parser
	// ever sees them; see command.go for why that happens here rather than in
	// the parser. The rewrite preserves byte offsets, so the error reported
	// below still points at the byte the client wrote.
	lowered, order, highest, err := rewriteNumberedParameters(text)
	if err != nil {
		return nil, err
	}
	if s.sql != nil {
		runtime, err := s.sql.Prepare(context.Background(), lowered)
		if err != nil {
			return nil, asPGErrorIn(err, text)
		}
		p.runtime = runtime
		p.paramOrder = order
		p.wireParams = runtime.NumParams()
		if order != nil {
			p.wireParams = highest
		}
		if order != nil && len(order) != runtime.NumParams() {
			p.release()
			return nil, newError(sqlstateInternalError,
				"the numbered-parameter rewrite and the SQL runtime disagree about the placeholder count")
		}
		if p.wireParams > maxParameters {
			p.release()
			return nil, newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
				"a PostgreSQL prepared statement may hold at most %d parameters",
				maxParameters))
		}
		if err := configureRuntimeParamKinds(p); err != nil {
			p.release()
			return nil, err
		}
		if runtime.Kind().IsQuery() {
			p.cols = columnsFor(nil, runtime.Columns(), runtime.AppendSchema(nil))
		}
		return p, nil
	}
	if kind != kindSelect {
		return nil, newError(sqlstateInternalError,
			"a writable SQL statement reached a read-only query source")
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
	if p.wireParams > maxParameters {
		stmt.Release()
		p.stmt = nil
		return nil, newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
			"a PostgreSQL prepared statement may hold at most %d parameters",
			maxParameters))
	}
	p.cols = columnsFor(nil, stmt.Columns(), stmt.AppendSchema(nil))
	return p, nil
}

func configureRuntimeParamKinds(p *prepared) error {
	if p == nil || p.runtime == nil || p.wireParams == 0 {
		return nil
	}
	p.paramKinds = make([]sqldriver.ParamKind, p.wireParams)
	for occurrence := 0; occurrence < p.runtime.NumParams(); occurrence++ {
		wire := occurrence
		if p.paramOrder != nil {
			wire = p.paramOrder[occurrence] - 1
		}
		role := p.runtime.ParamKind(occurrence)
		if wire < 0 || wire >= len(p.paramKinds) ||
			role == sqldriver.ParamInvalid {
			return newError(sqlstateInternalError,
				"the SQL runtime returned invalid parameter metadata")
		}
		if existing := p.paramKinds[wire]; existing != sqldriver.ParamInvalid &&
			existing != role {
			return newError(sqlstateDatatypeMismatch, fmt.Sprintf(
				"parameter $%d is used as both a JSON document and a scalar", wire+1)).
				withHint("use separate parameters for whole-document writes and scalar predicates")
		}
		p.paramKinds[wire] = role
	}
	for i := range p.paramKinds {
		if p.paramKinds[i] == sqldriver.ParamInvalid {
			// Numbered placeholders may leave gaps (for example, a statement
			// using only $2). PostgreSQL still requires Bind to send $1, but its
			// unused bytes need no document-specific decoding.
			p.paramKinds[i] = sqldriver.ParamScalar
		}
	}
	return nil
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
	if s.sql != nil && s.sql.State() == sqldriver.SessionFailedTransaction &&
		stmt.kind != kindCommit && stmt.kind != kindRollback {
		return asPGError(sqldriver.ErrTransactionFailed)
	}
	if s.takeCancel() {
		return queryCanceled()
	}
	switch {
	case stmt.kind == kindSet || stmt.kind == kindReset:
		if s.sql != nil && s.sql.State() != sqldriver.SessionIdle {
			return newError(sqlstateFeatureNotSupported,
				"SET and RESET inside a transaction are not supported")
		}
		if err := s.applySet(stmt.set); err != nil {
			return err
		}
		// SET has no transactional undo in this adapter. A cancel observed
		// before it is handled above; one racing after the assignment is
		// consumed while the successful outcome is reported.
		_ = s.takeCancel()
		s.w.commandComplete(stmt.tag)
		return nil

	case stmt.kind == kindDiscard:
		if s.sql != nil && s.sql.State() != sqldriver.SessionIdle {
			return newError(sqlstateActiveSQLTransaction,
				"DISCARD ALL cannot run inside a transaction")
		}
		// DISCARD is honoured rather than ignored, because everything it names
		// that exists here really is discardable: the prepared statements and
		// the portals. A client pooling connections uses it to be sure it got a
		// clean one, and a server that answered "DISCARD" without discarding
		// would be answering a question about state with a lie.
		s.discardAll()
		_ = s.takeCancel()
		s.w.commandComplete(stmt.tag)
		return nil

	case stmt.fixed != nil:
		return s.executeFixed(p, limit)

	case stmt.kind == kindBegin || stmt.kind == kindCommit || stmt.kind == kindRollback:
		return s.executeTransaction(stmt)

	case stmt.runtime != nil && stmt.runtime.Kind().IsQuery():
		return s.executeRuntimeQuery(p, limit)

	case stmt.runtime != nil:
		return s.executeRuntimeExec(p)

	case stmt.stmt != nil:
		return s.executeQuery(p, limit)
	}

	_ = s.takeCancel()
	s.w.emptyQueryResponse()
	return nil
}

func (s *session) executeTransaction(stmt *prepared) error {
	if s.takeCancel() {
		return queryCanceled()
	}
	var err error
	tag := stmt.tag
	switch stmt.kind {
	case kindBegin:
		err = s.sql.Begin(context.Background(),
			sqldriver.TxOptions{ReadOnly: stmt.txReadOnly})
	case kindCommit:
		s.closeRuntimePortals()
		err = s.sql.Commit(context.Background())
		if errors.Is(err, sqldriver.ErrTransactionFailed) {
			// PostgreSQL treats COMMIT in an aborted transaction as a rollback
			// and reports ROLLBACK rather than returning another transaction
			// error. The runtime has already discarded every staged write.
			err = nil
			tag = "ROLLBACK"
		}
		s.implicitExtended = false
	case kindRollback:
		s.closeRuntimePortals()
		err = s.sql.Rollback(context.Background())
		s.implicitExtended = false
	}
	pendingCancel := s.takeCancel()
	if err != nil {
		return asPGErrorIn(err, stmt.sql)
	}
	if pendingCancel && stmt.kind == kindBegin {
		return queryCanceled()
	}
	s.w.commandComplete(tag)
	return nil
}

func (s *session) executeRuntimeExec(p *portal) error {
	kind := p.stmt.runtime.Kind()
	if p.exhausted {
		s.w.commandComplete(runtimeCommandTag(kind, 0))
		return nil
	}
	if s.takeCancel() {
		return queryCanceled()
	}
	s.invalidateRuntimePortals(p)
	result, err := p.stmt.runtime.Exec(context.Background(), p.args)
	if err != nil {
		_ = s.takeCancel()
		if errors.Is(err, query.ErrCanceled) {
			return queryCanceled()
		}
		return asPGErrorIn(err, p.stmt.sql)
	}
	if s.takeCancel() {
		if kind != sqlast.KindCreateTable && kind != sqlast.KindCreateIndex {
			return queryCanceled()
		}
		// DDL's atomic catalog publication is the commit point and cannot
		// participate in the transaction overlay. Once it succeeds, a cancel
		// racing with the acknowledgement is consumed but cannot truthfully
		// turn that durable success into a 57014 failure.
	}
	p.started = true
	p.exhausted = true
	s.w.commandComplete(runtimeCommandTag(kind, result.RowsAffected))
	return nil
}

func runtimeCommandTag(kind sqlast.Kind, rows int64) string {
	switch kind {
	case sqlast.KindInsert:
		return fmt.Sprintf("INSERT 0 %d", rows)
	case sqlast.KindUpdate:
		return fmt.Sprintf("UPDATE %d", rows)
	case sqlast.KindDelete:
		return fmt.Sprintf("DELETE %d", rows)
	case sqlast.KindCreateTable:
		return "CREATE TABLE"
	case sqlast.KindCreateIndex:
		return "CREATE INDEX"
	default:
		return kind.String()
	}
}

func (s *session) executeRuntimeQuery(p *portal, limit int32) error {
	if p.exhausted {
		s.w.commandComplete(commandTag(0))
		return nil
	}
	if p.invalidated {
		p.exhausted = true
		return invalidatedPortalError()
	}
	if !p.started {
		if s.takeCancel() {
			return queryCanceled()
		}
		if err := s.startRuntime(p); err != nil {
			_ = s.takeCancel()
			if errors.Is(err, query.ErrCanceled) {
				return queryCanceled()
			}
			return asPGErrorIn(err, p.stmt.sql)
		}
		if s.takeCancel() {
			cancelPortal(p)
			return queryCanceled()
		}
	}

	sent := 0
	for p.runtime.Next() {
		if s.takeCancel() {
			cancelPortal(p)
			return queryCanceled()
		}
		cells := s.rowCellsRuntime(&p.runtime, len(p.stmt.cols))
		if err := s.rows.row(s.w, cells, p.stmt.cols, p.formats); err != nil {
			cancelPortal(p)
			return err
		}
		sent++
		if s.takeCancel() {
			cancelPortal(p)
			return queryCanceled()
		}
		if limit > 0 && int32(sent) >= limit {
			s.w.portalSuspended()
			return nil
		}
	}
	if s.takeCancel() {
		cancelPortal(p)
		return queryCanceled()
	}
	p.exhausted = true
	if err := p.runtime.Close(); err != nil {
		p.runtimeOpen = false
		return asPGErrorIn(err, p.stmt.sql)
	}
	p.runtimeOpen = false
	s.w.commandComplete(commandTag(sent))
	return nil
}

func (s *session) startRuntime(p *portal) error {
	s.invalidateRuntimePortals(p)
	s.generation++
	if err := p.stmt.runtime.QueryInto(context.Background(), p.args, &p.runtime); err != nil {
		return err
	}
	p.runtimeOpen = true
	if err := checkDataRows(p.runtime.Snapshot(), p.stmt.cols, p.formats); err != nil {
		_ = p.runtime.Close()
		p.runtimeOpen = false
		return err
	}
	p.started = true
	p.generation = s.generation
	return nil
}

func (s *session) invalidateRuntimePortals(current *portal) {
	for _, candidate := range s.portals {
		if candidate == current || !candidate.runtimeOpen {
			continue
		}
		_ = candidate.runtime.Close()
		candidate.runtimeOpen = false
		candidate.invalidated = true
	}
}

// closeRuntimePortals applies PostgreSQL's non-holdable portal lifetime at a
// transaction boundary. Prepared statements survive; every bound runtime
// portal and its copied arguments, result formats, snapshot, and cursor do not.
func (s *session) closeRuntimePortals() {
	for name, candidate := range s.portals {
		if candidate.stmt == nil || candidate.stmt.runtime == nil {
			continue
		}
		candidate.release()
		delete(s.portals, name)
		s.portalBytes -= candidate.retainedBytes
	}
}

func invalidatedPortalError() *pgError {
	return newError(sqlstateObjectNotInPrereqState,
		"this suspended portal can no longer be resumed because another statement has "+
			"executed on the same connection").
		withHint("this server holds one result set per connection; read a portal to " +
			"completion before executing anything else on the same connection, or run " +
			"the statement without a row limit")
}

func (s *session) rowCellsRuntime(c *sqldriver.Cursor, n int) []query.Cell {
	if cap(s.cells) < n {
		s.cells = make([]query.Cell, n)
	}
	s.cells = s.cells[:n]
	for i := range n {
		s.cells[i] = c.Cell(i)
	}
	return s.cells
}

func (s *session) discardAll() {
	for name, p := range s.portals {
		p.release()
		delete(s.portals, name)
	}
	s.portalBytes = 0
	for name, st := range s.statements {
		st.release()
		delete(s.statements, name)
	}
	s.statementBytes = 0
	s.statementBindBytes = 0
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
	// SET text may borrow the current simple Query message. The session map
	// outlives that reader buffer, so this is the ownership boundary.
	s.params[name] = strings.Clone(value)
	if p.report {
		s.w.parameterStatus(name, value)
	}
}

// executeFixed serves rows from a result this package produced itself.
func (s *session) executeFixed(p *portal, limit int32) error {
	fixed := p.stmt.fixed
	sent := 0
	for p.row < len(fixed.rows) {
		if s.takeCancel() {
			return queryCanceled()
		}
		if limit > 0 && int32(sent) >= limit {
			s.w.portalSuspended()
			return nil
		}
		if err := s.w.fixedRow(fixed.cols, fixed.rows[p.row], p.formats); err != nil {
			return err
		}
		p.row++
		sent++
	}
	if s.takeCancel() {
		return queryCanceled()
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
		if s.takeCancel() {
			return queryCanceled()
		}
		if err := s.start(p); err != nil {
			_ = s.takeCancel()
			if errors.Is(err, query.ErrCanceled) {
				return queryCanceled()
			}
			return asPGErrorIn(err, p.stmt.sql)
		}
		// RunInto materializes synchronously, so a cancel delivered while it
		// was scanning could not be observed until now. Consume it as belonging
		// to this Execute, discard the materialized result and release its
		// snapshot before any DataRow reaches the client.
		if s.takeCancel() {
			cancelPortal(p)
			return queryCanceled()
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
		// A cancel during delivery cannot retract rows already written, but the
		// protocol explicitly permits DataRows followed by ErrorResponse. Stop
		// at the next row boundary and, crucially, do not let this cancel poison
		// the connection's next statement.
		if s.takeCancel() {
			cancelPortal(p)
			return queryCanceled()
		}
		cells := s.rowCells(&p.cursor, len(p.stmt.cols))
		if err := s.rows.row(s.w, cells, p.stmt.cols, p.formats); err != nil {
			return err
		}
		sent++
		if s.takeCancel() {
			cancelPortal(p)
			return queryCanceled()
		}
		if limit > 0 && int32(sent) >= limit {
			s.w.portalSuspended()
			return nil
		}
	}
	if s.takeCancel() {
		cancelPortal(p)
		return queryCanceled()
	}
	p.exhausted = true
	p.lease.release()
	s.w.commandComplete(commandTag(sent))
	return nil
}

func queryCanceled() *pgError {
	return newError(sqlstateQueryCanceled, "canceling statement due to a cancel request")
}

func cancelPortal(p *portal) {
	p.release()
	p.exhausted = true
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
		return err
	}
	if err := checkDataRows(cursor, p.stmt.cols, p.formats); err != nil {
		l.release()
		return err
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
