package distribution

// RouteKind classifies a route by its physical shard fan-out.
type RouteKind uint8

const (
	// RouteEmpty selects no shard: a contradictory or empty destination.
	RouteEmpty RouteKind = iota
	// RouteSingle selects exactly one shard.
	RouteSingle
	// RouteTargeted selects more than one but not every active shard.
	RouteTargeted
	// RouteScatter selects every active shard or is an unknown route; it is
	// scatter for admission and metrics regardless of how it was derived.
	RouteScatter
)

// String renders the kind name for diagnostics.
func (k RouteKind) String() string {
	switch k {
	case RouteEmpty:
		return "Empty"
	case RouteSingle:
		return "Single"
	case RouteTargeted:
		return "Targeted"
	case RouteScatter:
		return "Scatter"
	default:
		return "Invalid"
	}
}

// Role is the endpoint role a target addresses. The current contract selects
// leaders only.
type Role uint8

const (
	// RoleLeader is the shard's fenced writer authority.
	RoleLeader Role = iota
	// RoleReplica is a follower endpoint; reserved, not selected in this PR.
	RoleReplica
)

// String renders the role name for diagnostics.
func (r Role) String() string {
	switch r {
	case RoleLeader:
		return "Leader"
	case RoleReplica:
		return "Replica"
	default:
		return "Invalid"
	}
}

// Target is one selected shard endpoint. AllocationGeneration identifies the
// topology-created physical shard allocation; OwnershipEpoch fences writer
// authority within that exact allocation independently of the routing version.
type Target struct {
	Shard                ShardID
	AllocationGeneration ShardAllocationGeneration
	// ManifestOrdinal is the allocation's position in the immutable manifest
	// named by Route.RoutingVersion. It recovers exact range geometry in O(1)
	// without retaining another key range per target.
	ManifestOrdinal int
	Endpoint        EndpointID
	OwnershipEpoch  OwnershipEpoch
	Role            Role
}

// Route is an immutable routing result pinned to one routing version. Targets
// is owned by the caller and safe to retain.
type Route struct {
	Kind           RouteKind
	Distribution   DistributionName
	RoutingVersion RoutingVersion
	Targets        []Target
}
