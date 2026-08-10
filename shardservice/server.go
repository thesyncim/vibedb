package shardservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The shard-service listener, its configuration, and its per-connection
// ownership model.
//
// # Concurrency
//
// One connection is one goroutine and owns one sql/driver Session, exactly like
// pgwire: the Session is single-consumer, so its prepared statements, cursors,
// and pinned snapshot are never reachable from another goroutine and need no
// lock on the request path. Two things are shared and each is shared
// deliberately: the [Options] and [Ownership] are read-only after construction,
// and the connection registry is a mutex-guarded map touched once when a
// connection starts and once when it ends, never on a request.
//
// The wire contract carries SQL text plus typed parameters, never a serialized
// plan; each request is parsed and planned locally through the borrowed Session.

// Safe defaults selected for zero-valued resource fields. They mirror pgwire and
// the query package so a shard and a local server bound the same work the same
// way.
const (
	// DefaultMaxConnections bounds concurrently served connections.
	DefaultMaxConnections = 128
	// DefaultIdleTimeout bounds reading one request frame, so a peer that opens
	// a connection and stalls mid-frame cannot pin a goroutine forever.
	DefaultIdleTimeout = 5 * time.Minute
	// DefaultWriteTimeout bounds writing one response frame.
	DefaultWriteTimeout = 30 * time.Second
	// DefaultRequestDeadline is the execution budget applied when a request
	// carries none.
	DefaultRequestDeadline = 30 * time.Second
	// DefaultMaxResultRows and DefaultMaxResultBytes bound one materialized
	// result. DefaultMaxIntermediateBytes bounds statement-wide subplan storage.
	DefaultMaxResultRows        = query.DefaultResultRows
	DefaultMaxResultBytes       = query.DefaultResultBytes
	DefaultMaxIntermediateBytes = query.DefaultIntermediateBytes

	// UnlimitedConnections and UnlimitedResults are explicit opt-outs from the
	// corresponding finite defaults; a negative timeout disables that deadline.
	UnlimitedConnections = -1
	UnlimitedResults     = -1
)

// Options configure a [Server]. The zero value is usable: every field selects a
// safe finite default.
type Options struct {
	// MaxConnections bounds concurrently served connections. Zero selects
	// DefaultMaxConnections; UnlimitedConnections is the explicit unbounded
	// opt-out.
	MaxConnections int

	// IdleTimeout bounds reading one request frame. Zero selects
	// DefaultIdleTimeout; a negative value disables the deadline.
	IdleTimeout time.Duration
	// WriteTimeout bounds writing one response frame. Zero selects
	// DefaultWriteTimeout; a negative value disables it.
	WriteTimeout time.Duration

	// DefaultRequestDeadline is the execution budget applied to a request whose
	// own Deadline is zero. Zero selects DefaultRequestDeadline; a negative value
	// disables the default execution deadline.
	DefaultRequestDeadline time.Duration

	// MaxResultRows and MaxResultBytes bound one materialized result when a
	// request does not carry a tighter cap. Zero selects the finite defaults;
	// UnlimitedResults is the explicit unbounded opt-out.
	MaxResultRows  int
	MaxResultBytes int64
	// MaxIntermediateBytes bounds statement-wide subplan storage. Zero selects
	// DefaultMaxIntermediateBytes; UnlimitedResults disables it.
	MaxIntermediateBytes int64

	// OnError is called with every connection-terminating error, including the
	// ordinary ones (a peer that closed its connection). It is the only logging
	// hook. The connection is released before the hook runs, so it may
	// synchronously call Close.
	OnError func(err error)
}

// A Server is a leader-only shard endpoint. It admits every request against one
// static ownership identity and executes the admitted statement locally.
type Server struct {
	db        *sqldriver.Database
	ownership Ownership
	opts      Options

	// baseCtx is the parent of every request context; cancel fires on Close so a
	// graceful shutdown cancels in-flight executions rather than waiting them out.
	baseCtx context.Context
	cancel  context.CancelFunc

	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	listeners map[net.Listener]struct{}
	closed    bool

	// wg tracks in-flight connections so Close can wait for them: a caller that
	// closes the server and then the database underneath it must know no
	// connection is still executing.
	wg sync.WaitGroup
}

// NewServer builds a shard server over db that owns the identity in cfg. The
// database is borrowed and remains the caller's responsibility; each connection
// opens one independent Session that the server closes on every disconnect path.
// NewServer defaults and validates opts and rejects an empty ownership identity.
func NewServer(db *sqldriver.Database, cfg Ownership, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("shardservice: a non-nil SQL database is required")
	}
	if cfg.Distribution == "" || cfg.Shard == "" || cfg.AllocationGeneration == 0 {
		return nil, errors.New(
			"shardservice: ownership must name a non-empty distribution and shard with a nonzero allocation generation")
	}
	if _, err := db.RequireShardStore(sqldriver.ShardStoreBinding{
		Distribution:         cfg.Distribution,
		Shard:                cfg.Shard,
		AllocationGeneration: cfg.AllocationGeneration,
	}); err != nil {
		return nil, fmt.Errorf("shardservice: SQL database does not match ownership: %w", err)
	}
	if opts.MaxConnections < UnlimitedConnections {
		return nil, fmt.Errorf("shardservice: MaxConnections must be >= %d", UnlimitedConnections)
	}
	if opts.MaxResultRows < UnlimitedResults {
		return nil, fmt.Errorf("shardservice: MaxResultRows must be >= %d", UnlimitedResults)
	}
	if opts.MaxResultBytes < UnlimitedResults {
		return nil, fmt.Errorf("shardservice: MaxResultBytes must be >= %d", UnlimitedResults)
	}
	if opts.MaxIntermediateBytes < UnlimitedResults {
		return nil, fmt.Errorf("shardservice: MaxIntermediateBytes must be >= %d", UnlimitedResults)
	}
	if opts.MaxConnections == 0 {
		opts.MaxConnections = DefaultMaxConnections
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	} else if opts.IdleTimeout < 0 {
		opts.IdleTimeout = 0
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = DefaultWriteTimeout
	} else if opts.WriteTimeout < 0 {
		opts.WriteTimeout = 0
	}
	if opts.DefaultRequestDeadline == 0 {
		opts.DefaultRequestDeadline = DefaultRequestDeadline
	} else if opts.DefaultRequestDeadline < 0 {
		opts.DefaultRequestDeadline = 0
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
	baseCtx, cancel := context.WithCancel(context.Background())
	return &Server{
		db:        db,
		ownership: cfg,
		opts:      opts,
		baseCtx:   baseCtx,
		cancel:    cancel,
		conns:     map[net.Conn]struct{}{},
		listeners: map[net.Listener]struct{}{},
	}, nil
}

var (
	// ErrServerClosed is returned by [Server.Serve] after [Server.Close].
	ErrServerClosed  = errors.New("shardservice: server closed")
	errNilListener   = errors.New("shardservice: Serve requires a non-nil listener")
	errNilConnection = errors.New("shardservice: ServeConn requires a non-nil connection")
)

// Serve accepts connections on l until l fails or the server is closed. It takes
// ownership of l and closes it on return.
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
		go s.serveAdmitted(conn)
	}
}

// ServeConn serves one already-accepted connection and returns when it ends. It
// takes ownership of conn and closes it.
//
// It is exported because it is what makes an in-process test over net.Pipe
// possible without binding a port, and because a caller accepting its own
// connections should not have to give a listener away to use this package.
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
	s.serveAdmitted(conn)
}

// serveAdmitted runs an admitted connection and reports its terminal error only
// after every resource and shutdown-accounting entry is released, so OnError may
// reentrantly call Close.
func (s *Server) serveAdmitted(conn net.Conn) {
	err := s.serveConn(conn)
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

func (s *Server) serveConn(nc net.Conn) error {
	// Done was added by admitConnection. Declare it before the resource defers
	// so LIFO ordering removes and closes the connection first.
	defer s.wg.Done()
	defer s.releaseConnection(nc)
	defer nc.Close()
	sess, err := s.db.NewSession(s.baseCtx)
	if err != nil {
		return err
	}
	defer sess.Close()
	c := &shardConn{server: s, nc: nc, sess: sess}
	return c.loop()
}

// Close stops accepting, cancels every in-flight execution, closes every open
// connection, and waits for every connection goroutine to return.
//
// Both signals are necessary: canceling baseCtx reaches a connection currently
// inside execution, and closing its socket wakes one blocked reading or writing.
// The connection slice is copied under the registry lock and closed after
// unlock, avoiding a lock inversion with the release defers each connection runs.
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
	s.mu.Unlock()

	s.cancel()
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

// admitConnection bounds connections and goroutines before a Session is opened.
func (s *Server) admitConnection(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if s.opts.MaxConnections != UnlimitedConnections &&
		len(s.conns) >= s.opts.MaxConnections {
		return false
	}
	// Add while holding the same lock Close uses to cross into Wait, so a
	// connection admitted concurrently with shutdown cannot call Add after Close
	// has begun waiting at a zero count.
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

// setDeadline applies a timeout to conn through set, or clears it when d is zero.
func setDeadline(set func(time.Time) error, d time.Duration) {
	if d <= 0 {
		_ = set(time.Time{})
		return
	}
	_ = set(time.Now().Add(d))
}
