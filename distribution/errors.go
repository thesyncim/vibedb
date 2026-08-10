package distribution

import (
	"errors"
	"fmt"
)

// InvalidNumberError reports a spelling that is not a well-formed JSON
// number and therefore cannot become a Number scalar.
type InvalidNumberError struct {
	Spelling string
}

func (e *InvalidNumberError) Error() string {
	return fmt.Sprintf("distribution: invalid number spelling %q", e.Spelling)
}

// UnsupportedScalarError reports a Scalar outside the closed placement
// scalar set, which can only occur for an unconstructed (zero-value) Scalar
// since every exported constructor produces KindString or KindNumber.
type UnsupportedScalarError struct {
	Kind ScalarKind
}

func (e *UnsupportedScalarError) Error() string {
	return fmt.Sprintf("distribution: unsupported scalar kind %q", e.Kind)
}

// ErrInvalidManifest is the sentinel every manifest validation failure matches
// under errors.Is.
var ErrInvalidManifest = errors.New("distribution: invalid shard manifest")

// ManifestError reports why NewManifest rejected its input. It wraps
// ErrInvalidManifest.
type ManifestError struct {
	Reason string
}

func (e *ManifestError) Error() string {
	return "distribution: invalid shard manifest: " + e.Reason
}

func (e *ManifestError) Unwrap() error { return ErrInvalidManifest }

// ErrInvalidDestination is the sentinel every DestinationSet malformation
// matches under errors.Is.
var ErrInvalidDestination = errors.New("distribution: invalid destination set")

// DestinationError reports why a DestinationSet was rejected. It wraps
// ErrInvalidDestination.
type DestinationError struct {
	Reason string
}

func (e *DestinationError) Error() string {
	return "distribution: invalid destination set: " + e.Reason
}

func (e *DestinationError) Unwrap() error { return ErrInvalidDestination }

// ErrInvalidShardValue is the sentinel every rejected shard-key value matches
// under errors.Is: a value that cannot be canonically encoded or that a mapper
// refuses.
var ErrInvalidShardValue = errors.New("distribution: invalid shard-key value")

// ShardValueError reports why a shard-key value was rejected. It wraps
// ErrInvalidShardValue.
type ShardValueError struct {
	Reason string
}

func (e *ShardValueError) Error() string {
	return "distribution: invalid shard-key value: " + e.Reason
}

func (e *ShardValueError) Unwrap() error { return ErrInvalidShardValue }

// ErrIncompleteShardKey is the sentinel for a shard key that lacks a component
// the requested prefix length requires.
var ErrIncompleteShardKey = errors.New("distribution: incomplete shard key")

// ErrUnsupportedMapper is the sentinel every unusable-mapper failure matches
// under errors.Is: an unsupported prefix length or a missing mapper/manifest.
var ErrUnsupportedMapper = errors.New("distribution: unsupported mapper")

// MapperError reports why a mapper request was refused. It wraps
// ErrUnsupportedMapper.
type MapperError struct {
	Reason string
}

func (e *MapperError) Error() string {
	return "distribution: unsupported mapper: " + e.Reason
}

func (e *MapperError) Unwrap() error { return ErrUnsupportedMapper }

// ErrInvalidPlacement is the sentinel every ClusterConfig validation failure
// matches under errors.Is.
var ErrInvalidPlacement = errors.New("distribution: invalid placement configuration")

// PlacementError reports why a ClusterConfig was rejected. It wraps
// ErrInvalidPlacement.
type PlacementError struct {
	Reason string
}

func (e *PlacementError) Error() string {
	return "distribution: invalid placement configuration: " + e.Reason
}

func (e *PlacementError) Unwrap() error { return ErrInvalidPlacement }

// ErrNotShardOwner is the sentinel a shard service returns when a request names
// a distribution or shard the process does not own.
var ErrNotShardOwner = errors.New("distribution: shard is not the owner")

// ErrShardAllocation is the sentinel for a request naming a stale physical
// allocation of an otherwise matching logical shard id.
var ErrShardAllocation = errors.New("distribution: shard allocation generation mismatch")

// ErrOwnershipEpoch is the sentinel for a request whose fencing epoch does not
// match the shard's configured ownership epoch.
var ErrOwnershipEpoch = errors.New("distribution: ownership epoch mismatch")

// ErrRoutingVersion is the sentinel for a request routed against a stale
// manifest generation.
var ErrRoutingVersion = errors.New("distribution: routing version mismatch")

// ErrScatterRejected is the sentinel returned when admission forbids an unknown
// or all-shard scatter route.
var ErrScatterRejected = errors.New("distribution: scatter route rejected")

// ErrRouteExpansionLimit is the sentinel every candidate-mapping overflow
// matches under errors.Is.
var ErrRouteExpansionLimit = errors.New("distribution: route expansion limit exceeded")

// ExpansionLimitError reports the candidate-mapping limit a route would exceed.
// It wraps ErrRouteExpansionLimit.
type ExpansionLimitError struct {
	Limit int
}

func (e *ExpansionLimitError) Error() string {
	return fmt.Sprintf("distribution: route expansion limit exceeded: candidate mappings exceed %d", e.Limit)
}

func (e *ExpansionLimitError) Unwrap() error { return ErrRouteExpansionLimit }

// ErrTargetShardLimit is the sentinel every selected-shard overflow matches
// under errors.Is.
var ErrTargetShardLimit = errors.New("distribution: target shard limit exceeded")

// TargetLimitError reports the target-shard limit a route exceeded and how many
// shards it selected. It wraps ErrTargetShardLimit.
type TargetLimitError struct {
	Limit int
	Count int
}

func (e *TargetLimitError) Error() string {
	return fmt.Sprintf("distribution: target shard limit exceeded: %d selected shards exceed %d", e.Count, e.Limit)
}

func (e *TargetLimitError) Unwrap() error { return ErrTargetShardLimit }
