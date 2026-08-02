package gateway

import (
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
)

// The operational profiles that layer a workload class over the core
// distribution.RouteAdmission contract. The router itself carries no workload
// class; a gateway maps each class onto an admission policy and the concurrency,
// deadline, and result caps that bound its fan-out.

// OperationClass names the operational profile a query runs under. It selects an
// admission policy and a set of caps; it never appears on the wire.
type OperationClass uint8

const (
	// ClassInteractive is the default profile: targeted-only routes, low
	// concurrency, and tight caps and deadlines. An all-shard scatter is refused.
	ClassInteractive OperationClass = iota
	// ClassBatch permits explicitly bounded scatter with higher concurrency and
	// larger caps.
	ClassBatch
	// ClassAdmin permits explicitly bounded scatter under separate, wider quotas
	// for maintenance work.
	ClassAdmin
)

// String renders the class name for diagnostics.
func (c OperationClass) String() string {
	switch c {
	case ClassInteractive:
		return "Interactive"
	case ClassBatch:
		return "Batch"
	case ClassAdmin:
		return "Admin"
	default:
		return "Invalid"
	}
}

// Profile is the admission policy and the fan-out caps one operational class
// runs under. Every cap is finite: a zero row/byte cap or deadline means the
// gateway applies its own conservative default rather than an unbounded value,
// so a zero-value profile fails closed.
type Profile struct {
	// Policy is the routing admission and limit contract handed to the router.
	Policy distribution.RoutePolicy
	// ReadPolicy is the consistency contract every shard request carries; this PR
	// serves only ReadStrong.
	ReadPolicy shardservice.ReadPolicy

	// MaxConcurrency caps concurrent in-flight shard requests during a scatter.
	MaxConcurrency int

	// MaxAggregateRows and MaxAggregateBytes cap the rows and result bytes the
	// gateway buffers across every shard of one operation.
	MaxAggregateRows  uint64
	MaxAggregateBytes uint64

	// PerShardRows and PerShardBytes are pushed to each shard as its MaxRows and
	// MaxResultBytes, bounding the work one shard performs.
	PerShardRows  uint64
	PerShardBytes uint64

	// GlobalDeadline bounds the whole operation; PerShardDeadline bounds one shard
	// request and is also carried as the shard's execution budget.
	GlobalDeadline   time.Duration
	PerShardDeadline time.Duration
}

// Conservative fan-out defaults. They are bounded, never unlimited, so an
// unset cap fails closed rather than admitting an unbounded read.
const (
	defaultMaxConcurrency    = 8
	defaultMaxAggregateRows  = 1 << 20
	defaultMaxAggregateBytes = 256 << 20
	defaultPerShardRows      = 1 << 20
	defaultPerShardBytes     = 128 << 20
	defaultGlobalDeadline    = 30 * time.Second
	defaultPerShardDeadline  = 15 * time.Second
)

// withDefaults returns the profile with every non-positive cap replaced by its
// conservative default, so a partially specified profile is still bounded.
func (p Profile) withDefaults() Profile {
	if p.MaxConcurrency <= 0 {
		p.MaxConcurrency = defaultMaxConcurrency
	}
	if p.MaxAggregateRows == 0 {
		p.MaxAggregateRows = defaultMaxAggregateRows
	}
	if p.MaxAggregateBytes == 0 {
		p.MaxAggregateBytes = defaultMaxAggregateBytes
	}
	if p.PerShardRows == 0 {
		p.PerShardRows = defaultPerShardRows
	}
	if p.PerShardBytes == 0 {
		p.PerShardBytes = defaultPerShardBytes
	}
	if p.GlobalDeadline <= 0 {
		p.GlobalDeadline = defaultGlobalDeadline
	}
	if p.PerShardDeadline <= 0 {
		p.PerShardDeadline = defaultPerShardDeadline
	}
	return p
}

// DefaultProfiles returns the built-in profile for every operational class:
// interactive is targeted-only, batch and admin permit explicitly bounded
// scatter under progressively wider concurrency and result quotas.
func DefaultProfiles() map[OperationClass]Profile {
	return map[OperationClass]Profile{
		ClassInteractive: {
			Policy:            distribution.NewRoutePolicy(distribution.AdmissionTargetedOnly, distribution.RouteLimits{}),
			MaxConcurrency:    4,
			MaxAggregateRows:  50_000,
			MaxAggregateBytes: 16 << 20,
			PerShardRows:      50_000,
			PerShardBytes:     16 << 20,
			GlobalDeadline:    5 * time.Second,
			PerShardDeadline:  2 * time.Second,
		},
		ClassBatch: {
			Policy:            distribution.NewRoutePolicy(distribution.AdmissionAllowScatter, distribution.RouteLimits{}),
			MaxConcurrency:    16,
			MaxAggregateRows:  1_000_000,
			MaxAggregateBytes: 256 << 20,
			PerShardRows:      500_000,
			PerShardBytes:     128 << 20,
			GlobalDeadline:    60 * time.Second,
			PerShardDeadline:  30 * time.Second,
		},
		ClassAdmin: {
			Policy:            distribution.NewRoutePolicy(distribution.AdmissionAllowScatter, distribution.RouteLimits{}),
			MaxConcurrency:    32,
			MaxAggregateRows:  10_000_000,
			MaxAggregateBytes: 1 << 30,
			PerShardRows:      5_000_000,
			PerShardBytes:     512 << 20,
			GlobalDeadline:    5 * time.Minute,
			PerShardDeadline:  2 * time.Minute,
		},
	}
}
