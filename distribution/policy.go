package distribution

// RouteLimits bounds the work a single route may generate. A non-positive field
// is never treated as unlimited; the router replaces it with a conservative
// default.
type RouteLimits struct {
	// MaxCandidateMappings caps the Cartesian product of the deduplicated
	// finite domains that feeds the mapper.
	MaxCandidateMappings int
	// MaxTargetShards caps the number of physical shards a targeted route may
	// select before it must be treated as an overflow.
	MaxTargetShards int
}

// Conservative default limits used in place of any non-positive limit. They are
// bounded, never unlimited, so a zero-value policy fails closed.
const (
	DefaultMaxCandidateMappings = 256
	DefaultMaxTargetShards      = 64
)

// withDefaults returns the limits with every non-positive field replaced by its
// conservative default.
func (l RouteLimits) withDefaults() RouteLimits {
	if l.MaxCandidateMappings <= 0 {
		l.MaxCandidateMappings = DefaultMaxCandidateMappings
	}
	if l.MaxTargetShards <= 0 {
		l.MaxTargetShards = DefaultMaxTargetShards
	}
	return l
}

// RouteAdmission selects how unknown/scatter routes and limit overflows are
// admitted. The zero value is the fail-closed AdmissionTargetedOnly.
type RouteAdmission uint8

const (
	// AdmissionTargetedOnly rejects any unknown or all-shard scatter route and
	// any overflow with a typed error.
	AdmissionTargetedOnly RouteAdmission = iota
	// AdmissionAllowScatter permits scatter routes but still returns a typed
	// error on candidate or target overflow.
	AdmissionAllowScatter
	// AdmissionAllowScatterOnOverflow permits scatter routes and degrades an
	// overflow to a scatter instead of erroring.
	AdmissionAllowScatterOnOverflow
)

// RoutePolicy is the caller's admission and limit contract for one route. The
// zero value is fail-closed: targeted routes only, with conservative default
// limits applied by the router.
type RoutePolicy struct {
	Limits    RouteLimits
	Admission RouteAdmission
}

// NewRoutePolicy returns a policy with the given admission and limits, replacing
// any non-positive limit with a conservative default (never unlimited).
func NewRoutePolicy(admission RouteAdmission, limits RouteLimits) RoutePolicy {
	return RoutePolicy{Admission: admission, Limits: limits.withDefaults()}
}

// normalized returns the policy the router enforces: non-positive limits become
// conservative defaults while the zero-value admission stays fail-closed.
func (p RoutePolicy) normalized() RoutePolicy {
	p.Limits = p.Limits.withDefaults()
	return p
}
