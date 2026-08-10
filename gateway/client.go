package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
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

// Client is a thin, stateless shard client: one synchronous request/response
// round-trip per call over a connection it dials fresh and closes. It holds no
// per-connection state and is safe for concurrent use.
type Client struct {
	dial DialFunc
}

// NewClient returns a client that opens connections with dial. A nil dial
// selects a default TCP dialer, so a production caller passes nil and a test
// passes a net.Pipe dialer wired to an in-process shardservice.Server.
func NewClient(dial DialFunc) *Client {
	if dial == nil {
		dial = tcpDial
	}
	return &Client{dial: dial}
}

// tcpDial is the default connector: one TCP connection honoring the context.
func tcpDial(ctx context.Context, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", address)
}

// Do dials address, sends req, reads one reply, and closes the connection. A
// shard error frame becomes a typed *ShardError; a rows or completion frame is
// returned as the response. It respects ctx's deadline and cancellation for both
// the dial and the round-trip.
func (c *Client) Do(ctx context.Context, address string, req *shardservice.ShardRequest) (*shardservice.ShardResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := c.dial(ctx, address)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return RoundTrip(ctx, conn, req)
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
