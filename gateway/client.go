package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/store/durable"
)

// The thin shard client: one synchronous request/response round-trip per call
// over the shardservice length-prefixed codec, plus the mapping from a shard's
// typed error frame back onto the sentinels a caller matches with errors.Is.

// Gateway error sentinels for the shard refusals that have no distribution
// equivalent. The ownership refusals reuse the distribution sentinels
// (ErrNotShardOwner, ErrOwnershipEpoch, ErrRoutingVersion) instead.
var (
	// ErrClientClosed reports an operation attempted after [Client.Close].
	ErrClientClosed = errors.New("gateway: shard client is closed")
	// ErrShardDeadlineExceeded reports a shard whose execution budget elapsed.
	ErrShardDeadlineExceeded = errors.New("gateway: shard deadline exceeded")
	// ErrResultLimit reports a result that exceeded the request's row or byte cap.
	ErrResultLimit = errors.New("gateway: shard result limit exceeded")
	// ErrMalformedRequest reports a request the shard refused as not well-formed.
	ErrMalformedRequest = errors.New("gateway: shard rejected the request as malformed")
	// ErrReadPolicyUnsupported reports a consistency policy the shard cannot
	// currently prove.
	ErrReadPolicyUnsupported = errors.New("gateway: shard does not support the requested read policy")
	// ErrPositionUnsupported reports that a shard has no replicated apply log
	// against which it can prove a requested session minimum.
	ErrPositionUnsupported = errors.New("gateway: shard does not support logical positions")
	// ErrPositionIdentity reports a minimum naming a different distribution,
	// shard, or log lineage than the shard can serve.
	ErrPositionIdentity = errors.New("gateway: logical position identity mismatch")
	// ErrPositionNotReached reports that the serving replica has not applied the
	// requested minimum. It is not a stale-catalog error and is never retried by
	// refreshing routing metadata.
	ErrPositionNotReached = errors.New("gateway: logical position has not been reached")
	// ErrCommitOutcomeUnknown preserves the storage layer's indeterminate
	// completion identity across the shard wire. It must never be retried without
	// a durable command identity and completion record.
	ErrCommitOutcomeUnknown = durable.ErrCommitOutcomeUnknown
	// ErrUnexpectedError reports an error frame whose kind this client does not
	// recognize, so a future error kind fails closed rather than silently.
	ErrUnexpectedError = errors.New("gateway: shard reported an unrecognized error kind")
	// ErrTransactionConflict reports a non-idempotent transaction retry or an
	// invalid durable state transition.
	ErrTransactionConflict = errors.New("gateway: distributed transaction state conflicts with durable state")
	// ErrTransactionNotFound reports a missing coordinator or participant role.
	ErrTransactionNotFound = errors.New("gateway: distributed transaction record was not found")
	// ErrReadFenceBusy asks a coherent fan-out reader to drop a partial cut and
	// retry after an intersecting write has crossed admission.
	ErrReadFenceBusy = errors.New("gateway: coherent read fence intersects an admitted writer")
)

// ShardError is a typed failure a shard reported in an error frame. Kind is the
// wire kind; Unwrap exposes the matching sentinel so a caller matches the
// failure with errors.Is.
type ShardError struct {
	Kind     shardservice.ErrorKind
	Message  string
	sentinel error
}

func (e *ShardError) Error() string { return e.Message }

func (e *ShardError) Unwrap() error { return e.sentinel }

// sentinelFor maps a wire error kind onto the sentinel a caller matches: the
// distribution ownership sentinels for the admission refusals, gateway
// sentinels for the rest.
func sentinelFor(kind shardservice.ErrorKind) error {
	switch kind {
	case shardservice.ErrorNotOwner:
		return distribution.ErrNotShardOwner
	case shardservice.ErrorOwnershipEpoch:
		return distribution.ErrOwnershipEpoch
	case shardservice.ErrorShardAllocation:
		return distribution.ErrShardAllocation
	case shardservice.ErrorRoutingVersion:
		return distribution.ErrRoutingVersion
	case shardservice.ErrorDeadlineExceeded:
		return ErrShardDeadlineExceeded
	case shardservice.ErrorResourceLimit:
		return ErrResultLimit
	case shardservice.ErrorMalformedRequest:
		return ErrMalformedRequest
	case shardservice.ErrorReadOnly:
		return ErrWriteNotSupported
	case shardservice.ErrorUnsupportedReadPolicy:
		return ErrReadPolicyUnsupported
	case shardservice.ErrorCommitOutcomeUnknown:
		return ErrCommitOutcomeUnknown
	case shardservice.ErrorPositionUnsupported:
		return ErrPositionUnsupported
	case shardservice.ErrorPositionIdentity:
		return ErrPositionIdentity
	case shardservice.ErrorPositionNotReached:
		return ErrPositionNotReached
	case shardservice.ErrorTransactionConflict:
		return ErrTransactionConflict
	case shardservice.ErrorTransactionNotFound:
		return ErrTransactionNotFound
	case shardservice.ErrorReadFenceBusy:
		return ErrReadFenceBusy
	default:
		return ErrUnexpectedError
	}
}

// shardError builds the typed error for an error-frame response.
func shardError(resp *shardservice.ShardResponse) *ShardError {
	return &ShardError{Kind: resp.ErrorKind, Message: resp.ErrorMessage, sentinel: sentinelFor(resp.ErrorKind)}
}

// DialFunc opens a connection to a shard endpoint's network address. It must
// honor the context's deadline and cancellation while dialing.
type DialFunc func(ctx context.Context, address string) (net.Conn, error)

// Defaults for the client's bounded idle connection pool. The limits retain
// enough warm sessions for the built-in fan-out profiles without allowing
// catalog churn or a large endpoint set to retain an unbounded number of file
// descriptors and shard-side Sessions.
const (
	DefaultMaxIdleConnections            = 64
	DefaultMaxIdleConnectionsPerEndpoint = 8
	DefaultIdleConnectionTimeout         = 90 * time.Second
)

// ClientOptions configure a [Client]. The zero value enables bounded connection
// reuse with conservative defaults. DisableConnectionReuse restores one dial
// and close per request, primarily for diagnostics and benchmarks.
type ClientOptions struct {
	// MaxIdleConnections caps retained connections across every endpoint. Zero
	// selects DefaultMaxIdleConnections.
	MaxIdleConnections int
	// MaxIdleConnectionsPerEndpoint caps retained connections for one address.
	// Zero selects DefaultMaxIdleConnectionsPerEndpoint. Values above the total
	// cap are clamped to the total cap.
	MaxIdleConnectionsPerEndpoint int
	// IdleConnectionTimeout bounds how long a returned connection may be reused.
	// Zero selects DefaultIdleConnectionTimeout; a negative value disables age
	// expiry. Expired connections are closed lazily without a reaper goroutine.
	IdleConnectionTimeout time.Duration
	// DisableConnectionReuse closes every connection after one round-trip.
	DisableConnectionReuse bool
}

// idleShardConn is one exclusively owned connection waiting for another call.
// Connections are removed from the pool before use, so the shard protocol
// remains strictly request/response even when Client.Do runs concurrently.
type idleShardConn struct {
	conn       net.Conn
	returnedAt time.Time
}

// Client is a thin shard client: one synchronous request/response round-trip
// per call over a bounded pool of persistent shard connections. It retains no
// authoritative or transactional state and is safe for concurrent use.
type Client struct {
	dial        DialFunc
	idleTimeout time.Duration
	maxIdle     int
	maxPerAddr  int
	now         func() time.Time

	mu     sync.Mutex
	idle   map[string][]idleShardConn
	nidle  int
	closed bool
}

// NewClient returns a client with the default bounded connection pool. A nil
// dial selects TCP, so a production caller passes nil and a test may pass a
// net.Pipe dialer wired to an in-process shardservice.Server.
func NewClient(dial DialFunc) *Client {
	return NewClientWithOptions(dial, ClientOptions{})
}

// NewClientWithOptions returns a client with explicit pooling options.
func NewClientWithOptions(dial DialFunc, opts ClientOptions) *Client {
	if dial == nil {
		dial = tcpDial
	}
	if opts.MaxIdleConnections <= 0 {
		opts.MaxIdleConnections = DefaultMaxIdleConnections
	}
	if opts.MaxIdleConnectionsPerEndpoint <= 0 {
		opts.MaxIdleConnectionsPerEndpoint = DefaultMaxIdleConnectionsPerEndpoint
	}
	if opts.MaxIdleConnectionsPerEndpoint > opts.MaxIdleConnections {
		opts.MaxIdleConnectionsPerEndpoint = opts.MaxIdleConnections
	}
	if opts.IdleConnectionTimeout == 0 {
		opts.IdleConnectionTimeout = DefaultIdleConnectionTimeout
	} else if opts.IdleConnectionTimeout < 0 {
		opts.IdleConnectionTimeout = 0
	}
	if opts.DisableConnectionReuse {
		opts.MaxIdleConnections = 0
		opts.MaxIdleConnectionsPerEndpoint = 0
	}
	return &Client{
		dial:        dial,
		idleTimeout: opts.IdleConnectionTimeout,
		maxIdle:     opts.MaxIdleConnections,
		maxPerAddr:  opts.MaxIdleConnectionsPerEndpoint,
		now:         time.Now,
		idle:        make(map[string][]idleShardConn),
	}
}

// tcpDial is the default connector: one TCP connection honoring the context.
func tcpDial(ctx context.Context, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", address)
}

// Do borrows or dials an exclusive connection to address, sends req, reads one
// reply, and returns a healthy connection to the bounded idle pool. A shard
// error frame is a complete, stream-aligned response and therefore also leaves
// the connection reusable. Transport, framing, validation, and cancellation
// failures close it. Do respects ctx's deadline and cancellation for both the
// dial and the round-trip.
func (c *Client) Do(ctx context.Context, address string, req *shardservice.ShardRequest) (*shardservice.ShardResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequestPosition(req); err != nil {
		return nil, err
	}
	if req != nil && (req.RowBatch.BatchRows != 0 || req.RowBatch.BatchBytes != 0) {
		return nil, fmt.Errorf("%w: a row-batch request requires DoBatches", ErrMalformedRequest)
	}
	conn, err := c.take(address)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		conn, err = c.dial(ctx, address)
		if err != nil {
			return nil, err
		}
	}
	resp, err := RoundTrip(ctx, conn, req)
	if ctx.Err() == nil && roundTripKeepsStream(err) {
		c.put(address, conn)
	} else {
		_ = conn.Close()
	}
	return resp, err
}

// DoBatches performs one opt-in bounded row stream over a pooled connection.
// consume runs synchronously, so its pace applies network backpressure to the
// shard. Batches are provisional until this method returns nil; consumers must
// discard partial work on any error. A consumer refusal closes the connection
// instead of trying to drain an arbitrarily long response.
func (c *Client) DoBatches(
	ctx context.Context,
	address string,
	req *shardservice.ShardRequest,
	consume func(*shardservice.ShardResponse) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequestPosition(req); err != nil {
		return err
	}
	if err := validateBatchCall(req, consume); err != nil {
		return err
	}
	conn, err := c.take(address)
	if err != nil {
		return err
	}
	if conn == nil {
		conn, err = c.dial(ctx, address)
		if err != nil {
			return err
		}
	}
	err = RoundTripBatches(ctx, conn, req, consume)
	if ctx.Err() == nil && roundTripKeepsStream(err) {
		c.put(address, conn)
	} else {
		_ = conn.Close()
	}
	return err
}

// roundTripKeepsStream reports errors produced only after a whole response
// frame was decoded. Typed shard errors and client-side logical-position proof
// refusals do not desynchronize the connection; every other error is treated as
// a transport failure and the connection is discarded.
func roundTripKeepsStream(err error) bool {
	if err == nil {
		return true
	}
	var shardErr *ShardError
	return errors.As(err, &shardErr)
}

// take removes one connection from address's idle stack. Expired connections
// are closed outside the mutex and the search continues. A nil connection with
// a nil error tells Do to dial.
func (c *Client) take(address string) (net.Conn, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClientClosed
		}
		list := c.idle[address]
		if len(list) == 0 {
			c.mu.Unlock()
			return nil, nil
		}
		last := len(list) - 1
		idle := list[last]
		list[last] = idleShardConn{}
		list = list[:last]
		c.nidle--
		if len(list) == 0 {
			delete(c.idle, address)
		} else {
			c.idle[address] = list
		}
		timeout := c.idleTimeout
		now := c.now
		c.mu.Unlock()

		if timeout > 0 && now().Sub(idle.returnedAt) >= timeout {
			_ = idle.conn.Close()
			continue
		}
		return idle.conn, nil
	}
}

// put returns one healthy connection to the pool. At either bound it replaces
// the oldest matching idle connection, keeping the warm set biased toward the
// endpoints serving current traffic rather than letting removed catalog
// endpoints occupy the pool until process exit. Closing happens outside the
// mutex because arbitrary net.Conn implementations may block in Close.
func (c *Client) put(address string, conn net.Conn) {
	c.mu.Lock()
	if c.closed || c.maxIdle == 0 || c.maxPerAddr == 0 {
		c.mu.Unlock()
		_ = conn.Close()
		return
	}
	var evicted net.Conn
	if len(c.idle[address]) >= c.maxPerAddr {
		evicted = c.removeOldestAtLocked(address)
	} else if c.nidle >= c.maxIdle {
		evicted = c.removeOldestLocked()
	}
	c.idle[address] = append(c.idle[address], idleShardConn{
		conn:       conn,
		returnedAt: c.now(),
	})
	c.nidle++
	c.mu.Unlock()
	if evicted != nil {
		_ = evicted.Close()
	}
}

// removeOldestAtLocked removes address's oldest idle connection. Each address
// list is append-ordered by return time and borrowed newest-first.
func (c *Client) removeOldestAtLocked(address string) net.Conn {
	list := c.idle[address]
	if len(list) == 0 {
		return nil
	}
	oldest := list[0].conn
	if len(list) == 1 {
		delete(c.idle, address)
	} else {
		copy(list, list[1:])
		list[len(list)-1] = idleShardConn{}
		c.idle[address] = list[:len(list)-1]
	}
	c.nidle--
	return oldest
}

// removeOldestLocked removes the globally oldest idle connection. The scan is
// bounded by maxIdle and runs only when the total pool is already full.
func (c *Client) removeOldestLocked() net.Conn {
	var (
		oldestAddress string
		oldestTime    time.Time
		found         bool
	)
	for address, list := range c.idle {
		if len(list) == 0 {
			continue
		}
		if !found || list[0].returnedAt.Before(oldestTime) {
			oldestAddress = address
			oldestTime = list[0].returnedAt
			found = true
		}
	}
	if !found {
		return nil
	}
	return c.removeOldestAtLocked(oldestAddress)
}

// Close permanently closes the client and every idle connection. Connections
// already borrowed by Do are closed instead of being pooled when they return.
// Close is idempotent; it does not cancel in-flight round-trips, whose contexts
// remain the caller's cancellation mechanism.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := make([]net.Conn, 0, c.nidle)
	for address, list := range c.idle {
		for _, idle := range list {
			conns = append(conns, idle.conn)
		}
		delete(c.idle, address)
	}
	c.nidle = 0
	c.mu.Unlock()

	var err error
	for _, conn := range conns {
		err = errors.Join(err, conn.Close())
	}
	return err
}

// RoundTrip performs one request/response exchange over a caller-provided
// connection: it encodes req, decodes the reply, and turns a shard error frame
// into a typed *ShardError. It does not close conn.
//
// It bridges ctx onto the connection so a deadline or cancellation unblocks the
// blocking I/O and surfaces as ctx.Err(). On a ctx deadline or cancellation the
// connection's I/O deadline is tripped, so the connection must be discarded
// rather than reused.
func RoundTrip(ctx context.Context, conn net.Conn, req *shardservice.ShardRequest) (*shardservice.ShardResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequestPosition(req); err != nil {
		return nil, err
	}
	if req != nil && (req.RowBatch.BatchRows != 0 || req.RowBatch.BatchBytes != 0) {
		return nil, fmt.Errorf("%w: a row-batch request requires RoundTripBatches", ErrMalformedRequest)
	}
	// AfterFunc trips the socket's I/O deadline once ctx is done, unblocking a
	// blocked Encode or Decode; stop cancels it on the fast path so a healthy
	// round-trip pays nothing after it returns.
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(tripped)
		close(callbackDone)
	})
	defer func() {
		// If the callback won the race with the fast path, wait until it has
		// finished mutating the connection before RoundTrip returns. This is
		// required when callers pool a connection after a canceled exchange.
		if !stop() {
			<-callbackDone
		}
	}()

	if err := shardservice.EncodeRequest(conn, req); err != nil {
		return nil, firstErr(ctx, err)
	}
	resp, err := shardservice.DecodeResponse(conn)
	if err != nil {
		return nil, firstErr(ctx, err)
	}
	if resp.Kind == shardservice.ResponseError {
		return nil, shardError(resp)
	}
	if err := validateReadPosition(req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// RoundTripBatches sends one row-batch request and consumes its ordered frames
// through consume. Sequence, schema arity, terminal framing, error frames, read
// proofs, cancellation, and deadlines are checked before a successful return.
// It does not close conn; a caller using it directly must discard conn after
// any non-shard error because the response may be only partly consumed.
func RoundTripBatches(
	ctx context.Context,
	conn net.Conn,
	req *shardservice.ShardRequest,
	consume func(*shardservice.ShardResponse) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequestPosition(req); err != nil {
		return err
	}
	if err := validateBatchCall(req, consume); err != nil {
		return err
	}

	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(tripped)
		close(callbackDone)
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()

	if err := shardservice.EncodeRequest(conn, req); err != nil {
		return firstErr(ctx, err)
	}
	var (
		expectedSequence uint32
		columnCount      uint32
		totalRows        uint64
		totalPayload     uint64
	)
	for {
		resp, err := shardservice.DecodeResponse(conn)
		if err != nil {
			return firstErr(ctx, err)
		}
		if resp.Kind == shardservice.ResponseError {
			return shardError(resp)
		}
		if resp.Kind != shardservice.ResponseRowBatch {
			return fmt.Errorf("%w: shard returned %s to a row-batch request",
				ErrUnexpectedError, resp.Kind)
		}
		if resp.RowBatch.Sequence != expectedSequence {
			return fmt.Errorf("%w: row-batch sequence is %d, want %d",
				ErrUnexpectedError, resp.RowBatch.Sequence, expectedSequence)
		}
		if expectedSequence == 0 {
			columnCount = resp.RowBatch.ColumnCount
		} else if resp.RowBatch.ColumnCount != columnCount {
			return fmt.Errorf("%w: row-batch column count changed from %d to %d",
				ErrUnexpectedError, columnCount, resp.RowBatch.ColumnCount)
		}
		batchRows, batchBytes, batchPayload := rowBatchSizes(resp)
		if batchRows > uint64(req.RowBatch.BatchRows) ||
			batchBytes > uint64(req.RowBatch.BatchBytes) {
			return fmt.Errorf("%w: shard exceeded negotiated row-batch limits", ErrResultLimit)
		}
		if batchRows > req.MaxRows || batchPayload > req.MaxResultBytes ||
			totalRows > req.MaxRows-batchRows ||
			totalPayload > req.MaxResultBytes-batchPayload {
			return fmt.Errorf("%w: shard exceeded negotiated total result limits", ErrResultLimit)
		}
		totalRows += batchRows
		totalPayload += batchPayload
		if !resp.RowBatch.Final && resp.HasReadPosition {
			return fmt.Errorf("%w: a non-final row batch carried a read proof", ErrUnexpectedError)
		}
		if resp.RowBatch.Final {
			if err := validateReadPosition(req, resp); err != nil {
				return err
			}
		}
		if err := consume(resp); err != nil {
			return err
		}
		if resp.RowBatch.Final {
			return nil
		}
		expectedSequence++
	}
}

func validateBatchCall(
	req *shardservice.ShardRequest,
	consume func(*shardservice.ShardResponse) error,
) error {
	if req == nil || req.RowBatch.BatchRows == 0 || req.RowBatch.BatchBytes == 0 ||
		req.MaxRows == 0 || req.MaxResultBytes == 0 {
		return fmt.Errorf("%w: row batches require nonzero per-batch and total row/byte limits", ErrMalformedRequest)
	}
	if req.RowBatch.BatchRows > shardservice.MaxRowBatchRows ||
		req.RowBatch.BatchBytes > shardservice.MaxRowBatchBytes {
		return fmt.Errorf("%w: row-batch limits exceed the shard protocol bounds", ErrMalformedRequest)
	}
	if consume == nil {
		return fmt.Errorf("%w: row batches require a consumer", ErrMalformedRequest)
	}
	return nil
}

// rowBatchSizes returns negotiated accounting without converting any cell to a
// string. Batch bytes match shardservice.RowBatchRequest (wire row data), while
// payload bytes are a safe lower bound on the SQL engine's stricter logical
// ResultBytes accounting.
func rowBatchSizes(resp *shardservice.ShardResponse) (rows, batchBytes, payload uint64) {
	rows = uint64(len(resp.Rows))
	for i := range resp.Rows {
		for j := range resp.Rows[i] {
			cell := resp.Rows[i][j]
			batchBytes++ // null/non-null marker
			if cell.Null {
				continue
			}
			n := uint64(len(cell.Bytes))
			batchBytes += 4 + n
			payload += n
		}
	}
	return rows, batchBytes, payload
}

// validateRequestPosition binds a session minimum to the logical shard named
// by the request before any connection or wire work. Shard admission repeats
// this check against authoritative ownership; the client-side gate prevents a
// locally inconsistent request from reaching the network at all.
func validateRequestPosition(req *shardservice.ShardRequest) error {
	if req == nil || !req.HasMinPosition {
		return nil
	}
	if err := req.MinPosition.Validate(); err != nil {
		return fmt.Errorf("%w: invalid minimum position: %v", ErrMalformedRequest, err)
	}
	if req.MinPosition.Distribution != req.Distribution ||
		req.MinPosition.Shard != req.Shard {
		return clientPositionError(
			shardservice.ErrorPositionIdentity,
			"gateway: minimum position does not name the request's logical shard",
		)
	}
	return nil
}

// validateReadPosition enforces the proof contract on a successful response.
// The codec already rejects malformed positions and positions on non-row
// response shapes. This gate binds a requested minimum to the exact returned
// log identity and refuses a behind index without leaking the response.
func validateReadPosition(req *shardservice.ShardRequest, resp *shardservice.ShardResponse) error {
	if resp.HasReadPosition {
		if err := resp.ReadPosition.Validate(); err != nil {
			return fmt.Errorf("%w: invalid shard read position: %v", ErrMalformedRequest, err)
		}
	}
	if !req.HasMinPosition {
		return nil
	}
	if !resp.HasReadPosition {
		return clientPositionError(
			shardservice.ErrorPositionUnsupported,
			"gateway: shard returned no applied-position proof for the requested minimum",
		)
	}
	if !req.MinPosition.SameLog(resp.ReadPosition) {
		return clientPositionError(
			shardservice.ErrorPositionIdentity,
			"gateway: shard read-position proof does not match the requested log identity",
		)
	}
	if resp.ReadPosition.Index < req.MinPosition.Index {
		return clientPositionError(
			shardservice.ErrorPositionNotReached,
			fmt.Sprintf("gateway: shard read position %d is below requested minimum %d",
				resp.ReadPosition.Index, req.MinPosition.Index),
		)
	}
	return nil
}

func clientPositionError(kind shardservice.ErrorKind, message string) *ShardError {
	return &ShardError{Kind: kind, Message: message, sentinel: sentinelFor(kind)}
}

// tripped is a deadline safely in the past; setting it unblocks in-flight and
// subsequent connection I/O immediately.
var tripped = time.Unix(1, 0)

// firstErr prefers ctx's cancellation cause over the I/O error it provoked, so a
// deadline or cancellation surfaces as context.DeadlineExceeded or
// context.Canceled rather than an opaque i/o timeout.
func firstErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}
