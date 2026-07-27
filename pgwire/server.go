package pgwire

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// The listener, its configuration, and the registry a cancel request looks a
// session up in.
//
// # Concurrency
//
// One connection is one session, served by one goroutine, and a session owns
// everything that is single-consumer: its [query.Exec], its prepared
// statements' compilers and plans, its portals, its read and write buffers.
// None of it is reachable from another goroutine, which is what makes the
// engine's single-consumer contract hold without a lock on the hot path.
//
// Exactly three things are shared, and each is shared deliberately. The
// [Source] is shared and is safe for concurrent readers by its own contract:
// every execution takes its own snapshot. The [Options] are read-only after
// construction. And the session registry is a map guarded by a mutex, touched
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
	versionString    = "PostgreSQL " + serverVersion + " (vibejson pgwire)"
)

// Options configure a [Server]. The zero value is usable and authenticates
// nobody; see [Options.Auth].
type Options struct {
	// Auth is the authentication mechanism. A nil Auth is [Trust], which
	// accepts every connection: correct for a loopback or unix-socket listener
	// inside a trust boundary and wrong for anything reachable from elsewhere.
	Auth Authenticator

	// Database, when non-empty, is the only database name a client may ask
	// for. An empty Database accepts any name, which is what a single-store
	// server usually wants: the name is a label here, because a Source is
	// already the whole set of collections this server can reach.
	Database string

	// MaxConnections bounds concurrent sessions. Zero means unlimited. A
	// connection past the bound is refused with a FATAL 53300 before
	// authentication, which is the earliest point at which refusing costs
	// nothing.
	MaxConnections int

	// ReadTimeout bounds reading the startup packet and each authentication
	// reply — the phase before the peer is known to be anything at all, and the
	// one where a connection that opens, sends half a message, and stops would
	// otherwise hold a goroutine forever. Zero means no deadline.
	ReadTimeout time.Duration

	// WriteTimeout bounds how long a flush may block. Zero means no deadline.
	// It matters because a client that stops reading while a large result set
	// is being written will otherwise block the session's goroutine
	// indefinitely.
	WriteTimeout time.Duration

	// IdleTimeout bounds reading one message in the main loop, which is the
	// wait between statements plus the transfer of the message itself: it is
	// applied before the header is read and covers the body too. A client
	// sending a multi-megabyte statement over a slow link therefore needs an
	// IdleTimeout longer than that transfer, which is why the default of zero —
	// wait forever, what a pooled client expects — is the default.
	IdleTimeout time.Duration

	// OnError is called with every session-terminating error, including the
	// ordinary ones (a client that closed its connection). It is the only
	// logging hook, and it is a function rather than an interface so that a
	// caller wiring it to log/slog does not have to implement anything.
	OnError func(err error)
}

// A Server serves the PostgreSQL wire protocol over one [Source].
type Server struct {
	src  Source
	opts Options

	mu        sync.Mutex
	sessions  map[int32]*session
	listeners map[net.Listener]struct{}
	closed    bool
	nextPID   int32
	live      int

	// wg tracks in-flight sessions so Close can wait for them. Waiting is what
	// makes Close a usable shutdown: a caller that closes the server and then
	// closes the store underneath it must know no session is still reading.
	wg sync.WaitGroup
}

// NewServer builds a server reading src.
func NewServer(src Source, opts Options) *Server {
	if opts.Auth == nil {
		opts.Auth = Trust()
	}
	return &Server{
		src:       src,
		opts:      opts,
		sessions:  map[int32]*session{},
		listeners: map[net.Listener]struct{}{},
	}
}

// ErrServerClosed is returned by [Server.Serve] after [Server.Close].
var ErrServerClosed = errors.New("pgwire: server closed")

// Serve accepts connections on l until l fails or the server is closed. It
// takes ownership of l and closes it on return.
func (s *Server) Serve(l net.Listener) error {
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
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(conn)
		}()
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
	s.wg.Add(1)
	defer s.wg.Done()
	s.serveConn(conn)
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	sess := newSession(s, conn)
	err := sess.serve()
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

// Close stops accepting, closes every open session's connection, and waits for
// every session goroutine to return.
//
// Closing the connections rather than politely asking sessions to stop is
// deliberate: a session blocked in Read has no other way to be woken, and a
// session blocked in the executor cannot be interrupted at all (see
// [session.cancel]), so closing the socket is what makes its next write fail
// and its loop exit. A client sees the connection drop, which is what a client
// sees when a server shuts down.
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
	conns := make([]net.Conn, 0, len(s.sessions))
	for _, sess := range s.sessions {
		conns = append(conns, sess.conn)
	}
	s.mu.Unlock()

	var err error
	for _, l := range listeners {
		if closeErr := l.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	return err
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

// cancelFlag is a session's cancellation state. It is an atomic because the
// only writer is another connection's goroutine.
type cancelFlag struct{ flag atomic.Bool }

func (c *cancelFlag) set() { c.flag.Store(true) }

// take reports whether a cancel is pending and clears it, so one cancel request
// stops one statement rather than every statement that follows it.
func (c *cancelFlag) take() bool { return c.flag.Swap(false) }
