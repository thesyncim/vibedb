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

// Static ownership admission: the pure gate a shard applies to every request
// before it parses or executes anything.
//
// PR 4a configures ownership statically. A shard owns exactly one (distribution,
// shard) identity at one ownership epoch and one routing version. Admit compares
// an incoming request's coordinates against that configuration and returns a
// typed refusal, or nil to admit. It performs no I/O and executes no SQL.

// Ownership is a shard's statically configured identity. PR 4a assigns it at
// startup; PR 5 advances the epoch during movement.
type Ownership struct {
	Distribution   distribution.DistributionName
	Shard          distribution.ShardID
	Epoch          distribution.OwnershipEpoch
	RoutingVersion distribution.RoutingVersion
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
//   - stale routing version        -> ErrorRoutingVersion (ErrRoutingVersion)
//   - mismatched ownership epoch    -> ErrorOwnershipEpoch (ErrOwnershipEpoch)
//   - unsupported read policy       -> ErrorUnsupportedReadPolicy
func (o Ownership) Admit(req *ShardRequest) error {
	if req == nil {
		return &AdmissionError{
			Kind:     ErrorMalformedRequest,
			Message:  "shardservice: nil request",
			sentinel: nil,
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
