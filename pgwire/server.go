package pgwire

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The listener, its configuration, and the registry a cancel request looks a
// session up in.
//
// # Concurrency
//
// One connection is one session, served by one goroutine, and a session owns
// everything that is single-consumer: its SQL runtime session, its prepared
// statements and cursors, its portals, and its read and write buffers.
// None of it is reachable from another goroutine, which is what makes the
// engine's single-consumer contract hold without a lock on the hot path.
//
// Exactly three things are shared, and each is shared deliberately. The SQL
// database opens one independently owned runtime session per connection. The
// [Options] are read-only after construction. And the session registry is a
// map guarded by a mutex, touched
// once when a session starts, once when it ends, and once per cancel request —
// never on a query path.
//
// A cancel request arrives on its own connection, which is the protocol's
// design and the reason the registry exists at all: the connection running the
// query is blocked reading or executing, so the only way to reach it is out of
// band. What this server does with one is described on [session.cancel].

// versionString is what version() and the server_version parameter report.
//
// The number is a compatibility claim and not a description. Client libraries
// parse server_version and several refuse to connect to something they cannot
// place, so a version has to be reported; what it means here is "the protocol
// and the parameter set of that era", not "the SQL surface of that release".
// The parenthetical says what this actually is, and doc.go says what it can and
// cannot do. Nothing about the SQL surface should be inferred from the number.
const (
	serverVersion    = "16.0"
	serverVersionNum = "160000"
	versionString    = "PostgreSQL " + serverVersion + " (vibedb pgwire)"
)

// Safe defaults selected for zero-valued resource fields.
const (
	DefaultMaxConnections       = 128
	DefaultReadTimeout          = 10 * time.Second
	DefaultWriteTimeout         = 30 * time.Second
	DefaultIdleTimeout          = 5 * time.Minute
	DefaultMaxResultRows        = query.DefaultResultRows
	DefaultMaxResultBytes       = query.DefaultResultBytes
	DefaultMaxIntermediateBytes = query.DefaultIntermediateBytes

	// UnlimitedConnections and UnlimitedResults are explicit opt-outs from the
	// corresponding finite defaults. A negative timeout explicitly disables
	// that deadline.
	UnlimitedConnections = -1
	UnlimitedResults     = -1

	// cancelRequestSlots are a bounded pre-startup admission cushion. They give
	// an out-of-band CancelRequest an opportunity to reach a server whose
	// ordinary session slots are full, but are not dedicated: the packet type is
	// unknowable until it is read, so slow ordinary startups can occupy them.
	// The finite default ReadTimeout plus the startup negotiation-attempt bound
	// keeps each such occupation finite.
	cancelRequestSlots = 8
)

// Options configure a [Server]. Authentication is intentionally the one field
// with no default: callers must choose [Trust] or [SCRAM] explicitly.
type Options struct {
	// Auth is the authentication mechanism. It is required. [Trust] accepts
	// every connection and must be written explicitly, which prevents a
	// forgotten production setting from silently disabling authentication.
	Auth Authenticator

	// Database, when non-empty, is the only database name a client may ask
	// for. An empty Database accepts any name, which is what a single-store
	// server usually wants: the name is a label here, because the Database is
	// already the whole SQL catalog this server can reach.
	Database string

	// MaxConnections bounds authenticated and authenticating ordinary
	// sessions. Zero selects DefaultMaxConnections; UnlimitedConnections is the
	// explicit unbounded opt-out. The server additionally holds a fixed,
	// bounded pre-startup cushion so a CancelRequest can contend when the
	// ordinary limit is full; see the ReadTimeout caveat below.
	MaxConnections int

	// ReadTimeout bounds reading the startup packet and each authentication
	// reply — the phase before the peer is known to be anything at all, and the
	// one where a connection that opens, sends half a message, and stops would
	// otherwise hold a goroutine forever. Zero selects DefaultReadTimeout; a
	// negative value explicitly disables the deadline.
	ReadTimeout time.Duration

	// WriteTimeout bounds how long any underlying socket write may block. Zero
	// selects DefaultWriteTimeout; a negative value explicitly disables it.
	// It matters because a client that stops reading while a large result set
	// is being written will otherwise block the session's goroutine
	// indefinitely.
	WriteTimeout time.Duration

	// IdleTimeout bounds reading one message in the main loop, which is the
	// wait between statements plus the transfer of the message itself: it is
	// applied before the header is read and covers the body too. A client
	// sending a multi-megabyte statement over a slow link therefore needs an
	// IdleTimeout longer than that transfer. Zero selects DefaultIdleTimeout; a
	// negative value explicitly disables it.
	IdleTimeout time.Duration

	// MaxResultRows and MaxResultBytes bound one materialized SELECT result
	// before pgwire begins encoding it. Zero selects the finite defaults above;
	// UnlimitedResults is the explicit unbounded opt-out for either field.
	MaxResultRows  int
	MaxResultBytes int64

	// MaxIntermediateBytes bounds statement-wide logical storage retained by
	// derived tables, predicate subqueries, CTE materializations, and other
	// relation-valued subplans. Zero selects DefaultMaxIntermediateBytes and
	// UnlimitedResults (-1) explicitly disables the bound. Caller-visible
	// result rows and bytes remain governed independently above.
	MaxIntermediateBytes int64

	// OnError is called with every session-terminating error, including the
	// ordinary ones (a client that closed its connection). It is the only
	// logging hook, and it is a function rather than an interface so that a
	// caller wiring it to log/slog does not have to implement anything. The
	// session and connection are fully released before the hook runs, so the
	// hook may synchronously call Close.
	OnError func(err error)
}

// A Server is an experimental PostgreSQL wire-protocol endpoint supporting a documented SQL subset over one database.
type Server struct {
	db   *sqldriver.Database
	opts Options

	mu        sync.Mutex
	sessions  map[int32]*session
	conns     map[net.Conn]struct{}
	listeners map[net.Listener]struct{}
	closed    bool
	nextPID   int32
	live      int

	// wg tracks in-flight sessions so Close can wait for them. Waiting is what
	// makes Close a usable shutdown: a caller that closes the server and then
	// closes the store underneath it must know no session is still reading.
	wg sync.WaitGroup
}

// NewServer builds a server over db. The Database is borrowed and remains the
// caller's responsibility. Each authenticated connection owns one independent
// SQL session, which the server closes on every disconnect path. NewServer
// rejects an implicit authentication
// policy and malformed resource settings before any listener is accepted.
func NewServer(db *sqldriver.Database, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("pgwire: a non-nil SQL database is required")
	}
	if opts.Auth == nil {
		return nil, errors.New(
			"pgwire: Options.Auth is required; choose Trust() or SCRAM(...) explicitly")
	}
	if err := opts.Auth.validate(); err != nil {
		return nil, err
	}
	if opts.MaxConnections < UnlimitedConnections {
		return nil, fmt.Errorf("pgwire: MaxConnections must be >= %d", UnlimitedConnections)
	}
	if opts.MaxConnections > int(^uint(0)>>1)-cancelRequestSlots {
		return nil, errors.New("pgwire: MaxConnections is too large")
	}
	if opts.MaxResultRows < UnlimitedResults {
		return nil, fmt.Errorf("pgwire: MaxResultRows must be >= %d", UnlimitedResults)
	}
	if opts.MaxResultBytes < UnlimitedResults {
		return nil, fmt.Errorf("pgwire: MaxResultBytes must be >= %d", UnlimitedResults)
	}
	if opts.MaxIntermediateBytes < UnlimitedResults {
		return nil, fmt.Errorf(
			"pgwire: MaxIntermediateBytes must be >= %d", UnlimitedResults)
	}
	if opts.MaxConnections == 0 {
		opts.MaxConnections = DefaultMaxConnections
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = DefaultReadTimeout
	} else if opts.ReadTimeout < 0 {
		opts.ReadTimeout = 0
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = DefaultWriteTimeout
	} else if opts.WriteTimeout < 0 {
		opts.WriteTimeout = 0
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	} else if opts.IdleTimeout < 0 {
		opts.IdleTimeout = 0
	}
	if opts.MaxResultRows == 0 {
		opts.MaxResultRows = DefaultMaxResultRows
	}
	if opts.MaxResultBytes == 0 {
		opts.MaxResultBytes = DefaultMaxResultBytes
	}
	if opts.MaxIntermediateBytes == 0 {
		opts.MaxIntermediateBytes = DefaultMaxIntermediateBytes
	}
	return &Server{
		db:        db,
		opts:      opts,
		sessions:  map[int32]*session{},
		conns:     map[net.Conn]struct{}{},
		listeners: map[net.Listener]struct{}{},
	}, nil
}

var (
	// ErrServerClosed is returned by [Server.Serve] after [Server.Close].
	ErrServerClosed  = errors.New("pgwire: server closed")
	errNilListener   = errors.New("pgwire: Serve requires a non-nil listener")
	errNilConnection = errors.New(
		"pgwire: ServeConn requires a non-nil connection",
	)
)

// Serve accepts connections on l until l fails or the server is closed. It
// takes ownership of l and closes it on return.
func (s *Server) Serve(l net.Listener) error {
	if l == nil {
		return errNilListener
	}
	if !s.addListener(l) {
		_ = l.Close()
		return ErrServerClosed
	}
	defer s.removeListener(l)
	for {
		conn, err := l.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrServerClosed
			}
			return err
		}
		if !s.admitConnection(conn) {
			_ = conn.Close()
			continue
		}
		go s.serveAdmittedConnection(conn)
	}
}

// ServeConn serves one already-accepted connection and returns when the session
// ends. It takes ownership of conn and closes it.
//
// It is exported because it is what makes an in-process test over net.Pipe
// possible without binding a port, and because a caller that accepts its own
// connections — over a unix socket with peer credentials checked, say — should
// not have to give the listener away to use this package.
func (s *Server) ServeConn(conn net.Conn) {
	if conn == nil {
		if s.opts.OnError != nil {
			s.opts.OnError(errNilConnection)
		}
		return
	}
	if !s.admitConnection(conn) {
		_ = conn.Close()
		return
	}
	s.serveAdmittedConnection(conn)
}

// serveAdmittedConnection runs an admitted connection and reports its terminal
// error only after every resource and shutdown-accounting entry has been
// released. The ordering is what makes OnError reentrant with Close: reporting
// while this connection still contributed to wg would make Close wait for the
// callback that was itself calling Close.
func (s *Server) serveAdmittedConnection(conn net.Conn) {
	err := s.serveConn(conn)
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

func (s *Server) serveConn(conn net.Conn) error {
	// Done was added by admitConnection. Declare it before the resource
	// defers so LIFO ordering removes and closes the connection first.
	defer s.wg.Done()
	defer s.releaseConnection(conn)
	defer conn.Close()
	sess := newSession(s, conn)
	return sess.serve()
}

// Close stops accepting, cancels every registered execution, closes every open
// connection, and waits for every session goroutine to return.
//
// Both signals are necessary. Cooperative cancellation reaches a session
// currently inside heap/durable scan, join, filtered DML, or spill work;
// closing its connection wakes a session blocked in Read or Write. The session
// slice is copied under the registry lock and signaled after unlock, avoiding
// any lock inversion with the unregister defers each exiting session runs.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return nil
	}
	s.closed = true
	listeners := make([]net.Listener, 0, len(s.listeners))
	for l := range s.listeners {
		listeners = append(listeners, l)
	}
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()

	var err error
	for _, l := range listeners {
		if closeErr := l.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	for _, sess := range sessions {
		sess.shutdownCancel()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	return err
}

// admitConnection bounds sockets and goroutines before startup reads or SCRAM
// work begin. The small cushion gives CancelRequest, which arrives on a new
// connection by protocol design, a chance to reach a fully occupied server. It
// cannot be a dedicated reserve because the packet type is unknown until after
// admission and a startup read.
func (s *Server) admitConnection(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if s.opts.MaxConnections != UnlimitedConnections &&
		len(s.conns) >= s.opts.MaxConnections+cancelRequestSlots {
		return false
	}
	// Add while holding the same lock Close uses to cross into Wait. This
	// prevents a ServeConn admitted concurrently with shutdown from calling
	// WaitGroup.Add after Close has already begun waiting at a zero count.
	s.wg.Add(1)
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) releaseConnection(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) addListener(l net.Listener) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.listeners[l] = struct{}{}
	return true
}

func (s *Server) removeListener(l net.Listener) {
	s.mu.Lock()
	delete(s.listeners, l)
	s.mu.Unlock()
	_ = l.Close()
}

// register admits a session, assigning it the backend key a cancel request
// will name it by. It reports false when the server is closed or full.
func (s *Server) register(sess *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if s.opts.MaxConnections > 0 && s.live >= s.opts.MaxConnections {
		return false
	}
	// Backend PIDs are handed out in sequence, skipping zero and any value a
	// live session already holds. Both guards matter only after two billion
	// connections, but the cost of getting it wrong then is a cancel request
	// delivered to the wrong session, and zero is reserved so that unregister
	// can tell an admitted session from one that never was.
	for {
		s.nextPID++
		if s.nextPID <= 0 {
			s.nextPID = 1
		}
		if _, taken := s.sessions[s.nextPID]; !taken {
			break
		}
	}
	sess.pid = s.nextPID
	s.sessions[sess.pid] = sess
	s.live++
	return true
}

// unregister removes sess if it is the session currently holding its PID. The
// identity check rather than a bare delete is what keeps a late unregister from
// evicting a newer session that was given the same number.
func (s *Server) unregister(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[sess.pid] == sess {
		delete(s.sessions, sess.pid)
		s.live--
	}
}

// cancelRequest delivers an out-of-band cancel to the named session, if the
// secret matches. A mismatch is ignored silently, which is what the protocol
// requires: answering would turn the cancel port into an oracle for guessing
// backend keys.
func (s *Server) cancelRequest(pid, secret int32) {
	s.mu.Lock()
	sess := s.sessions[pid]
	s.mu.Unlock()
	if sess != nil && sess.secret == secret {
		sess.cancel()
	}
}

// randomKey returns the secret half of a backend key. It comes from
// crypto/rand because it is the only thing standing between a peer who can open
// a connection and the ability to cancel other people's queries.
func randomKey() (int32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b[:])), nil
}

// deadline applies a timeout to conn, or clears any existing one when d is
// zero.
func deadline(conn net.Conn, set func(time.Time) error, d time.Duration) {
	if d <= 0 {
		_ = set(time.Time{})
		return
	}
	_ = set(time.Now().Add(d))
}
