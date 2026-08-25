package shardservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The shard-service listener, its configuration, and its per-connection
// ownership model.
//
// # Concurrency
//
// One connection is one goroutine and lazily owns one sql/driver Session when
// it first carries SQL, exactly like pgwire: the Session is single-consumer, so
// its prepared statements, cursors, and pinned snapshot are never reachable
// from another goroutine and need no lock on the request path. Exchange-only
// peer connections never allocate a Session. Two things are shared and each is shared
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
	// DefaultMaxReadFences bounds abandoned leased coherent cuts and their scope
	// copies independently of the connection limit.
	DefaultMaxReadFences = 4096
	// Exchange defaults reserve a finite aggregate promise across ephemeral
	// stage mailboxes. A mailbox allocates only its ring and producer state at
	// Open; payload memory remains bounded by this admitted reservation.
	DefaultMaxExchangeMailboxes   = 1024
	DefaultMaxExchangeBufferBytes = 512 << 20

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
	// MaxReadFences bounds active leased coherent multi-shard read cuts. Zero
	// selects DefaultMaxReadFences; unlike connections it cannot be unbounded.
	MaxReadFences int
	// MaxExchangeMailboxes and MaxExchangeBufferBytes bound worker exchange
	// state independently of SQL results and connections. Zero selects finite
	// defaults; neither resource can be configured as unbounded.
	MaxExchangeMailboxes   int
	MaxExchangeBufferBytes uint64
	// ExchangeDial resolves a repartition target's opaque address bytes. Nil
	// selects TCP and permits loopback targets only. An injected resolver is a
	// trusted control-plane boundary and may interpret addresses as endpoint IDs.
	ExchangeDial ExchangeDialFunc

	// HotRecorder is an optional fixed-space, striped observation window for
	// this exact shard allocation. Its source identity is validated against
	// Ownership before the database serving claim is acquired. Rotation and
	// controller delivery remain the caller's responsibility.
	HotRecorder *autosplit.Recorder

	// OnError is called with every connection-terminating error, including the
	// ordinary ones (a peer that closed its connection). It is the only logging
	// hook. The connection is released before the hook runs, so it may
	// synchronously call Close.
	OnError func(err error)
}

// ExchangeDialFunc opens one direct worker connection for a repartition
// producer. It must honor ctx cancellation and returns an exclusively owned
// connection.
type ExchangeDialFunc func(ctx context.Context, address []byte) (net.Conn, error)

// A Server is a leader-only shard endpoint. It admits every request against one
// static ownership identity and executes the admitted statement locally.
type Server struct {
	db          *sqldriver.Database
	claim       *sqldriver.ShardStoreServingClaim
	journal     *distributedtxn.Journal
	readFences  *readFenceSet
	exchanges   *exchange.Registry
	hotRecorder *autosplit.Recorder
	ownership   Ownership
	opts        Options

	// baseCtx is the parent of every request context; cancel fires on Close so a
	// graceful shutdown cancels in-flight executions rather than waiting them out.
	baseCtx context.Context
	cancel  context.CancelFunc

	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	listeners map[net.Listener]struct{}
	closed    bool
	closeDone chan struct{}
	closeErr  error

	// wg tracks in-flight connections so Close can wait for them: a caller that
	// closes the server and then the database underneath it must know no
	// connection is still executing.
	wg sync.WaitGroup
}

// NewServer builds a shard server over db that owns the identity in cfg. The
// database is borrowed and remains the caller's responsibility; each SQL-bearing
// connection lazily opens one independent Session that the server closes on every disconnect path.
// NewServer defaults and validates opts and rejects an empty ownership identity.
func NewServer(db *sqldriver.Database, cfg Ownership, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("shardservice: a non-nil SQL database is required")
	}
	if cfg.Distribution == "" || cfg.Shard == "" || cfg.AllocationGeneration == 0 ||
		cfg.Epoch == 0 || cfg.RoutingVersion == 0 {
		return nil, errors.New(
			"shardservice: ownership must name a non-empty distribution and shard with nonzero allocation generation, epoch, and routing version")
	}
	if opts.HotRecorder != nil {
		source := opts.HotRecorder.Source()
		if source.Distribution != cfg.Distribution || source.Shard != cfg.Shard ||
			source.AllocationGeneration != cfg.AllocationGeneration ||
			source.RoutingVersion != cfg.RoutingVersion ||
			source.OwnershipEpoch != cfg.Epoch {
			return nil, errors.New(
				"shardservice: hot recorder source does not match the serving ownership identity")
		}
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
	if opts.MaxReadFences < 0 {
		return nil, errors.New("shardservice: MaxReadFences must be nonnegative")
	}
	if opts.MaxExchangeMailboxes < 0 {
		return nil, errors.New("shardservice: MaxExchangeMailboxes must be nonnegative")
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
	if opts.MaxReadFences == 0 {
		opts.MaxReadFences = DefaultMaxReadFences
	}
	if opts.MaxExchangeMailboxes == 0 {
		opts.MaxExchangeMailboxes = DefaultMaxExchangeMailboxes
	}
	if opts.MaxExchangeBufferBytes == 0 {
		opts.MaxExchangeBufferBytes = DefaultMaxExchangeBufferBytes
	}
	// Claim first so an unbound, mismatched, or stale store retains its precise
	// admission error. Opening the journal afterward may create hidden state;
	// failure closes the claim and an equal-fence retry resumes safely.
	claim, err := db.ClaimShardStoreServing(sqldriver.ShardStoreBinding{
		Distribution:         cfg.Distribution,
		Shard:                cfg.Shard,
		AllocationGeneration: cfg.AllocationGeneration,
	}, sqldriver.ShardStoreFence{
		OwnershipEpoch: cfg.Epoch,
		RoutingVersion: cfg.RoutingVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("shardservice: claim SQL shard serving fence: %w", err)
	}
	journal, err := db.OpenDistributedTransactionJournal()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("shardservice: open transaction journal: %w", err),
			claim.Close(),
		)
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	return &Server{
		db:         db,
		claim:      claim,
		journal:    journal,
		readFences: newReadFenceSet(opts.MaxReadFences),
		exchanges: exchange.NewRegistry(exchange.RegistryOptions{
			MaxMailboxes:           opts.MaxExchangeMailboxes,
			MaxReservedBufferBytes: opts.MaxExchangeBufferBytes,
		}),
		hotRecorder: opts.HotRecorder,
		ownership:   cfg,
		opts:        opts,
		baseCtx:     baseCtx,
		cancel:      cancel,
		conns:       map[net.Conn]struct{}{},
		listeners:   map[net.Listener]struct{}{},
		closeDone:   make(chan struct{}),
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

// ServeAuthorizedConn is the production gateway-to-shard boundary. It retains
// the TLS-derived gateway principal and applies the same generation-bound
// policy to both that delegate and every forwarded end-user request.
func (s *Server) ServeAuthorizedConn(connection rafttransport.PeerConnection,
	gate *serviceauthz.Gate, audit serviceauthz.AuditSink) {
	if connection == nil || gate == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardSQL {
		if connection != nil {
			_ = connection.Close()
		}
		return
	}
	if !s.admitConnection(connection) {
		_ = connection.Close()
		return
	}
	s.serveAdmittedAuthorized(connection, connection.PeerIdentity().Node, gate, audit)
}

// serveAdmitted runs an admitted connection and reports its terminal error only
// after every resource and shutdown-accounting entry is released, so OnError may
// reentrantly call Close.
func (s *Server) serveAdmitted(conn net.Conn) {
	err := s.serveConn(conn, rafttransport.NodeID{}, nil, nil)
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

func (s *Server) serveAdmittedAuthorized(conn net.Conn, peer rafttransport.NodeID,
	gate *serviceauthz.Gate, audit serviceauthz.AuditSink) {
	err := s.serveConn(conn, peer, gate, audit)
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

func (s *Server) serveConn(nc net.Conn, peer rafttransport.NodeID,
	gate *serviceauthz.Gate, audit serviceauthz.AuditSink) error {
	// Done was added by admitConnection. Declare it before the resource defers
	// so LIFO ordering removes and closes the connection first.
	defer s.wg.Done()
	defer s.releaseConnection(nc)
	defer nc.Close()
	c := &shardConn{server: s, nc: nc, peer: peer, authorization: gate, audit: audit}
	defer func() {
		if c.sess != nil {
			_ = c.sess.Close()
		}
	}()
	return c.loop()
}

// Close stops accepting, cancels every in-flight execution, closes every open
// connection, waits for every connection goroutine to return, and only then
// releases the store's process-local serving claim.
//
// Both signals are necessary: canceling baseCtx reaches a connection currently
// inside execution, and closing its socket wakes one blocked reading or writing.
// The connection slice is copied under the registry lock and closed after
// unlock, avoiding a lock inversion with the release defers each connection runs.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
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
	s.readFences.close()
	s.exchanges.Close()
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
	err = errors.Join(err, s.journal.Close(), s.claim.Close())
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
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
