package shardservice

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
)

// ErrUnsupportedReadPolicy reports a consistency policy the leader-only shard
// cannot currently prove. Session and stale replica reads remain reserved until
// requests carry the required session/applied-position metadata.
var ErrUnsupportedReadPolicy = errors.New("shardservice: unsupported read policy")

// Position admission sentinels. These failures are deliberately distinct from
// routing-version and ownership-epoch refusals: a gateway must not refresh or
// retry them as stale topology.
var (
	// ErrPositionUnsupported reports that no replicated apply log exists from
	// which the shard can prove a requested minimum.
	ErrPositionUnsupported = errors.New("shardservice: logical positions are not supported")
	// ErrPositionIdentity reports a minimum naming a different distribution or
	// shard. Its index is not comparable with this shard's log.
	ErrPositionIdentity = errors.New("shardservice: logical position identity mismatch")
	// ErrPositionNotReached reports a matching position above the serving
	// replica's applied index. The current non-replicated service never emits it.
	ErrPositionNotReached = errors.New("shardservice: logical position has not been reached")
)

// Static ownership admission: the pure gate a shard applies to every request
// before it parses or executes anything.
//
// The current service configures ownership statically. A shard owns exactly one
// (distribution, shard) identity at one ownership epoch and one routing version.
// Admit compares an incoming request's coordinates against that configuration
// and returns a typed refusal, or nil to admit. It performs no I/O and executes
// no SQL.

// Ownership is a shard's statically configured startup identity. No online
// movement path advances it.
type Ownership struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	Epoch                distribution.OwnershipEpoch
	RoutingVersion       distribution.RoutingVersion
}

// AdmissionError is the typed refusal Admit returns. Kind selects the wire
// error frame the shard sends back, and Unwrap exposes the distribution
// sentinel so callers can match with errors.Is.
type AdmissionError struct {
	Kind     ErrorKind
	Message  string
	sentinel error
}

func (e *AdmissionError) Error() string { return e.Message }

func (e *AdmissionError) Unwrap() error { return e.sentinel }

// Response builds the typed error ShardResponse for this refusal.
func (e *AdmissionError) Response() *ShardResponse {
	return NewErrorResponse(e.Kind, e.Message)
}

// Admit checks req against the configured ownership and returns a typed
// AdmissionError, or nil if the request may be served.
//
// The checks run outermost identity first: this process must own the named
// (distribution, shard) at all, then both sides must agree on the manifest
// generation, then the caller's fencing epoch must match the current writer
// authority. That ordering yields the most actionable error first — refresh the
// catalog before reconciling the finer fencing epoch.
//
//   - wrong distribution or shard  -> ErrorNotOwner       (ErrNotShardOwner)
//   - stale shard allocation       -> ErrorShardAllocation (ErrShardAllocation)
//   - stale routing version        -> ErrorRoutingVersion (ErrRoutingVersion)
//   - mismatched ownership epoch    -> ErrorOwnershipEpoch (ErrOwnershipEpoch)
//   - malformed minimum position    -> ErrorMalformedRequest
//   - mismatched position identity  -> ErrorPositionIdentity
//   - any matching minimum position -> ErrorPositionUnsupported
//   - session read without minimum  -> ErrorPositionUnsupported
//   - unsupported stale read        -> ErrorUnsupportedReadPolicy
func (o Ownership) Admit(req *ShardRequest) error {
	if req == nil {
		return &AdmissionError{
			Kind:     ErrorMalformedRequest,
			Message:  "shardservice: nil request",
			sentinel: nil,
		}
	}
	if o.Distribution == "" || o.Shard == "" || o.AllocationGeneration == 0 {
		return &AdmissionError{
			Kind:     ErrorNotOwner,
			Message:  "shardservice: ownership is not configured",
			sentinel: distribution.ErrNotShardOwner,
		}
	}
	if req.Distribution != o.Distribution {
		return &AdmissionError{
			Kind: ErrorNotOwner,
			Message: fmt.Sprintf(
				"shardservice: not owner: request distribution %q, this shard serves %q",
				req.Distribution, o.Distribution),
			sentinel: distribution.ErrNotShardOwner,
		}
	}
	if req.Shard != o.Shard {
		return &AdmissionError{
			Kind: ErrorNotOwner,
			Message: fmt.Sprintf(
				"shardservice: not owner: request shard %q, this shard serves %q",
				req.Shard, o.Shard),
			sentinel: distribution.ErrNotShardOwner,
		}
	}
	if req.AllocationGeneration != o.AllocationGeneration {
		return &AdmissionError{
			Kind: ErrorShardAllocation,
			Message: fmt.Sprintf(
				"shardservice: shard allocation generation mismatch: request %d, configured %d",
				req.AllocationGeneration, o.AllocationGeneration),
			sentinel: distribution.ErrShardAllocation,
		}
	}
	if req.RoutingVersion != o.RoutingVersion {
		return &AdmissionError{
			Kind: ErrorRoutingVersion,
			Message: fmt.Sprintf(
				"shardservice: routing version mismatch: request %d, configured %d",
				req.RoutingVersion, o.RoutingVersion),
			sentinel: distribution.ErrRoutingVersion,
		}
	}
	if req.OwnershipEpoch != o.Epoch {
		return &AdmissionError{
			Kind: ErrorOwnershipEpoch,
			Message: fmt.Sprintf(
				"shardservice: ownership epoch mismatch: request %d, configured %d",
				req.OwnershipEpoch, o.Epoch),
			sentinel: distribution.ErrOwnershipEpoch,
		}
	}
	if !req.HasMinPosition && !req.MinPosition.IsZero() {
		return &AdmissionError{
			Kind:     ErrorMalformedRequest,
			Message:  errNonCanonicalPosition.Error(),
			sentinel: errNonCanonicalPosition,
		}
	}
	if req.HasMinPosition {
		if err := req.MinPosition.Validate(); err != nil {
			return &AdmissionError{
				Kind:     ErrorMalformedRequest,
				Message:  err.Error(),
				sentinel: err,
			}
		}
		if req.MinPosition.Distribution != o.Distribution || req.MinPosition.Shard != o.Shard {
			return &AdmissionError{
				Kind: ErrorPositionIdentity,
				Message: fmt.Sprintf(
					"shardservice: logical position identity mismatch: request position names (%q, %q), this shard serves (%q, %q)",
					req.MinPosition.Distribution, req.MinPosition.Shard, o.Distribution, o.Shard),
				sentinel: ErrPositionIdentity,
			}
		}
		return &AdmissionError{
			Kind:     ErrorPositionUnsupported,
			Message:  "shardservice: minimum position cannot be proved without a replicated apply log",
			sentinel: ErrPositionUnsupported,
		}
	}
	if req.ReadPolicy == ReadSession {
		return &AdmissionError{
			Kind:     ErrorPositionUnsupported,
			Message:  "shardservice: session reads cannot be proved without a replicated apply log and applied position",
			sentinel: ErrPositionUnsupported,
		}
	}
	if req.ReadPolicy != ReadStrong {
		return &AdmissionError{
			Kind: ErrorUnsupportedReadPolicy,
			Message: fmt.Sprintf(
				"shardservice: read policy %s is not supported by a leader-only shard",
				req.ReadPolicy),
			sentinel: ErrUnsupportedReadPolicy,
		}
	}
	return nil
}
