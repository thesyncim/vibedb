package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

// rf3CapacityDirectory is the runtime-owned inventory used by the capacity
// control endpoint. The schema activator is the dynamic group authority, so a
// reload cannot leave a startup-only source list behind. Cold groups are
// included while their bootstrap control service still owns the target.
func newRF3CapacitySourceDirectory(
	schemas *rf3SchemaActivator,
	cold []*preparedColdRF3Group,
	node func(context.Context) (replicacontrol.NodeCapacity, error),
	nodeWithSamples func(context.Context, replicacontrol.CapacityRequest, []replicacontrol.CapacitySourceSample) (replicacontrol.NodeCapacity, error),
) (replicacontrol.CapacitySourceDirectory, error) {
	// A serving node has a live schema activator; a cold bootstrap process has
	// only its prepared groups.  Both are valid complete inventories.  The
	// node report remains mandatory in either mode because placement must never
	// infer physical capacity from one replica's configured limit.
	if len(cold) == 0 && schemas == nil || (node == nil && nodeWithSamples == nil) {
		return replicacontrol.CapacitySourceDirectory{}, replicacontrol.ErrCapacityUnavailable
	}
	for _, group := range cold {
		if group == nil || group.installer == nil || group.repository == nil || group.journal == nil {
			return replicacontrol.CapacitySourceDirectory{}, replicacontrol.ErrCapacityUnavailable
		}
	}
	return replicacontrol.CapacitySourceDirectory{
		Sources: func(ctx context.Context) ([]replicacontrol.CapacitySource, error) {
			if ctx == nil {
				return nil, replicacontrol.ErrCapacityUnavailable
			}
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			sources := make([]replicacontrol.CapacitySource, 0, len(cold))
			if schemas != nil {
				schemas.mu.RLock()
				keys := make([]raftmember.GroupKey, 0, len(schemas.groups))
				for group := range schemas.groups {
					keys = append(keys, group)
				}
				slices.SortFunc(keys, func(left, right raftmember.GroupKey) int {
					return compareRF3CapacityGroups(left, right)
				})
				for _, group := range keys {
					state := schemas.groups[group]
					if state == nil {
						schemas.mu.RUnlock()
						return nil, replicacontrol.ErrCapacityStale
					}
					sources = append(sources, &rf3LiveCapacitySource{state: state})
				}
				schemas.mu.RUnlock()
			}
			for _, group := range cold {
				sources = append(sources, &rf3ColdCapacitySource{group: group})
			}
			if len(sources) == 0 {
				return nil, replicacontrol.ErrCapacityUnavailable
			}
			return sources, nil
		},
		Node: node, NodeWithSamples: nodeWithSamples,
	}, nil
}

// newRF3CapacityControl is the authenticated shard-control service factory.
// The caller supplies the node report because physical capacity and active
// migration reservations belong to the node agent, not to one SQL group.
func newRF3CapacityControl(
	registry *rafttransport.StaticRegistry,
	policy *serviceauthz.Policy,
	provider replicacontrol.CapacityObserver,
	deadline rafttransport.DeadlineFunc,
) (*replicacontrol.CapacityService, error) {
	if registry == nil || policy == nil || provider == nil || deadline == nil {
		return nil, replicacontrol.ErrCapacityUnavailable
	}
	return replicacontrol.NewCapacityService(replicacontrol.CapacityServiceOptions{
		Observer: provider,
		Authorize: func(identity rafttransport.PeerIdentity, request replicacontrol.CapacityRequest) bool {
			if identity.TrustDomain != registry.TrustDomain() ||
				policy.Check(identity.Node, serviceauthz.CapabilityMembership) != serviceauthz.DecisionAllow {
				return false
			}
			member, err := registry.LocalMember(request.Group)
			return err == nil && member != 0
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
}

func compareRF3CapacityGroups(left, right raftmember.GroupKey) int {
	if left.ClusterID != right.ClusterID {
		return compareRF3CapacityBytes(left.ClusterID[:], right.ClusterID[:])
	}
	if left.ClusterIncarnation != right.ClusterIncarnation {
		return compareRF3CapacityBytes(left.ClusterIncarnation[:], right.ClusterIncarnation[:])
	}
	if left.TopologyRecoveryEpoch < right.TopologyRecoveryEpoch {
		return -1
	}
	if left.TopologyRecoveryEpoch > right.TopologyRecoveryEpoch {
		return 1
	}
	if left.ShardIncarnation != right.ShardIncarnation {
		return compareRF3CapacityBytes(left.ShardIncarnation[:], right.ShardIncarnation[:])
	}
	return compareRF3CapacityBytes(left.GroupID[:], right.GroupID[:])
}

func compareRF3CapacityBytes(left, right []byte) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

type rf3LiveCapacitySource struct{ state *rf3SchemaGeneration }

func (source *rf3LiveCapacitySource) Identity() raftmember.RuntimeIdentity {
	if source == nil || source.state == nil {
		return raftmember.RuntimeIdentity{}
	}
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	return source.state.identity
}

func (source *rf3LiveCapacitySource) ObserveCapacity(ctx context.Context) (replicacontrol.CapacitySourceSample, error) {
	return source.observe(ctx)
}

func (source *rf3LiveCapacitySource) ObserveCapacityRequest(ctx context.Context, request replicacontrol.CapacityRequest) (replicacontrol.CapacitySourceSample, error) {
	return source.observe(ctx)
}

func (source *rf3LiveCapacitySource) observe(ctx context.Context) (replicacontrol.CapacitySourceSample, error) {
	if source == nil || source.state == nil || ctx == nil {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	if err := context.Cause(ctx); err != nil {
		return replicacontrol.CapacitySourceSample{}, err
	}
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	if source.state.apply == nil || source.state.wal == nil {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	identity := source.state.identity
	resources, err := source.state.apply.ResourceStats()
	if err != nil {
		return replicacontrol.CapacitySourceSample{}, errors.Join(replicacontrol.ErrCapacityUnavailable, err)
	}
	walBytes, kind, err := rf3CapacityRecoveryLogBytes(source.state.wal)
	if err != nil {
		return replicacontrol.CapacitySourceSample{}, err
	}
	// ResourceStats captures its publication under the same database lock
	// that excludes apply, so ongoing writes cannot invalidate this cut.
	after := resources.Publication
	if after.Applied == 0 || after.ConfState == nil {
		return replicacontrol.CapacitySourceSample{}, fmt.Errorf("group %x apply publication index=%d configuration=%t: %w", identity.Group.GroupID, after.Applied, after.ConfState != nil, replicacontrol.ErrCapacityStale)
	}
	sqlBytes, err := rf3CapacityResourceBytes(resources)
	if err != nil {
		return replicacontrol.CapacitySourceSample{}, err
	}
	liveBytes, overflow := replicacontrol.AddCapacity(sqlBytes, walBytes)
	if overflow {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	var demand autosplit.CapacityVector
	demand[autosplit.ResourceLiveBytes] = liveBytes
	if liveBytes == 0 {
		kind = replicacontrol.CapacityDemandMeasured
	}
	return replicacontrol.CapacitySourceSample{Identity: identity, Applied: after.Applied,
		Demand: demand, MigrationBytes: liveBytes, KnownEmpty: liveBytes == 0, DemandKind: kind}, nil
}

func rf3CapacityResourceBytes(resources sqldriver.ReplicatedApplyResourceStats) (uint64, error) {
	if resources.RelationCount > uint16(len(resources.Relations)) {
		return 0, replicacontrol.ErrCapacityStale
	}
	stats := make([]durable.Stats, 0, int(resources.RelationCount)+2)
	stats = append(stats, resources.System, resources.Capture)
	stats = append(stats, resources.Relations[:resources.RelationCount]...)
	var total uint64
	for _, item := range stats {
		if item.PhysicalCapacityBytes != 0 && item.PhysicalHighWaterBytes > item.PhysicalCapacityBytes {
			return 0, fmt.Errorf("SQL high-water %d exceeds capacity %d: %w", item.PhysicalHighWaterBytes, item.PhysicalCapacityBytes, replicacontrol.ErrCapacityStale)
		}
		var overflow bool
		total, overflow = replicacontrol.AddCapacity(total, item.PhysicalHighWaterBytes)
		if overflow {
			return 0, replicacontrol.ErrCapacityUnavailable
		}
	}
	return total, nil
}

func rf3CapacityRecoveryLogBytes(log rf3RecoveryLog) (uint64, replicacontrol.CapacityDemandKind, error) {
	switch value := log.(type) {
	case *raftstore.Store:
		if value == nil {
			return 0, 0, replicacontrol.ErrCapacityUnavailable
		}
		return value.Metrics().LiveBytes, replicacontrol.CapacityDemandMeasured, nil
	case *raftstore.GroupView:
		if value == nil {
			return 0, 0, replicacontrol.ErrCapacityUnavailable
		}
		bound, err := value.CapacityReservationBytes()
		if err != nil {
			return 0, 0, errors.Join(replicacontrol.ErrCapacityUnavailable, err)
		}
		return bound, replicacontrol.CapacityDemandConservative, nil
	default:
		return 0, 0, replicacontrol.ErrCapacityUnavailable
	}
}

type rf3ColdCapacitySource struct{ group *preparedColdRF3Group }

func (source *rf3ColdCapacitySource) Identity() raftmember.RuntimeIdentity {
	if source == nil || source.group == nil || source.group.installer == nil {
		return raftmember.RuntimeIdentity{}
	}
	binding := source.group.installer.base.Binding
	return raftmember.RuntimeIdentity{Group: source.group.group, Distribution: binding.Distribution,
		Shard: binding.Shard, AllocationGeneration: binding.AllocationGeneration,
		MemberID: binding.MemberID, StoreID: binding.StoreID,
		NodeIncarnation: source.group.authority.target.NodeIncarnation}
}

func (source *rf3ColdCapacitySource) ObserveCapacity(ctx context.Context) (replicacontrol.CapacitySourceSample, error) {
	return source.observe(ctx, replicacontrol.CapacityRequest{})
}

func (source *rf3ColdCapacitySource) ObserveCapacityRequest(ctx context.Context, request replicacontrol.CapacityRequest) (replicacontrol.CapacitySourceSample, error) {
	return source.observe(ctx, request)
}

func (source *rf3ColdCapacitySource) observe(ctx context.Context, request replicacontrol.CapacityRequest) (replicacontrol.CapacitySourceSample, error) {
	if source == nil || source.group == nil || source.group.installer == nil || source.group.repository == nil || source.group.journal == nil || ctx == nil {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	if err := context.Cause(ctx); err != nil {
		return replicacontrol.CapacitySourceSample{}, err
	}
	identity := source.Identity()
	if identity.Group == (raftmember.GroupKey{}) || identity.MemberID == 0 || identity.StoreID == ([16]byte{}) || identity.NodeIncarnation == 0 {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityStale
	}
	if source.group.installer.staticBootstrap == nil {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	applied := source.group.installer.staticBootstrap.GetMetadata().GetIndex()
	if applied == 0 {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	bound := source.group.repository.Stats().DiskCapacity
	if bound == 0 {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	artifactBytes := bound
	if request.Operation != ([32]byte{}) {
		record, err := source.group.journal.ReadBootstrap(ctx, request.Operation)
		if err == nil {
			if request.Step != ([32]byte{}) && record.Request.Step != request.Step {
				return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityStale
			}
			descriptor := record.Request.Descriptor
			if descriptor.Group != identity.Group || descriptor.TargetMember != identity.MemberID ||
				descriptor.TargetStore != identity.StoreID || descriptor.TargetIncarnation != identity.NodeIncarnation {
				return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityStale
			}
			artifact, capacityErr := source.group.repository.Capacity(ctx, descriptor)
			if capacityErr != nil {
				return replicacontrol.CapacitySourceSample{}, errors.Join(replicacontrol.ErrCapacityUnavailable, capacityErr)
			}
			artifactBytes = artifact.Descriptor.ArtifactBytes
			applied = descriptor.SnapshotIndex
		} else if !errors.Is(err, snapshottransfer.ErrBootstrapMissing) {
			return replicacontrol.CapacitySourceSample{}, errors.Join(replicacontrol.ErrCapacityUnavailable, err)
		}
	}
	if artifactBytes == 0 || applied == 0 {
		return replicacontrol.CapacitySourceSample{}, replicacontrol.ErrCapacityUnavailable
	}
	var demand autosplit.CapacityVector
	demand[autosplit.ResourceLiveBytes] = artifactBytes
	return replicacontrol.CapacitySourceSample{Identity: identity, Applied: applied,
		Demand: demand, MigrationBytes: artifactBytes, DemandKind: replicacontrol.CapacityDemandConservative}, nil
}

// RF3CapacityNodeFromSamples builds a node-wide reservation cut from the
// complete source inventory. The template must come from the node's
// generation-fenced physical capacity agent; this helper only fills actual
// used and active reservation counters from owned group evidence.
func RF3CapacityNodeFromSamples(template replicacontrol.NodeCapacity, samples []replicacontrol.CapacitySourceSample) (replicacontrol.NodeCapacity, error) {
	if template.NodeID == (rafttransport.NodeID{}) || template.NodeIncarnation == 0 || template.Revision == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityStale
	}
	if len(samples) == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	var used autosplit.CapacityVector
	var migration uint64
	for _, sample := range samples {
		var overflow bool
		used, overflow = replicacontrol.AddCapacityVectors(used, sample.Demand)
		if overflow {
			return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
		}
		migration, overflow = replicacontrol.AddCapacity(migration, sample.MigrationBytes)
		if overflow {
			return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
		}
	}
	template.Used = used
	template.MigrationUsed = migration
	if template.MaxReceives == 0 || template.ActiveReceives > template.MaxReceives || template.MigrationCapacity == 0 || migration > template.MigrationCapacity {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	for resource := range autosplit.ResourceCount {
		if used[resource] > template.Capacity[resource] {
			return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
		}
	}
	if migration > template.Capacity[autosplit.ResourceLiveBytes]-used[autosplit.ResourceLiveBytes] {
		// A target copy is reserved while the source remains live. The
		// migration budget is therefore bounded by physical free space after
		// every currently hosted source has been accounted for.
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	return template, nil
}

// RF3CapacityNodeFromOwner combines the node-owned authenticated geometry and
// migration scheduler with the complete detached source cut. The source
// samples provide actual/conservative per-group resident demand; the node log
// contributes only its sealed physical reservation. No filesystem walk or
// foreground row scan is performed.
func RF3CapacityNodeFromOwner(
	ctx context.Context,
	owner *rf3NodeOwner,
	nodeIncarnation uint64,
	budget *migrationbudget.Budget,
	revision *atomic.Uint64,
	request replicacontrol.CapacityRequest,
	samples []replicacontrol.CapacitySourceSample,
) (replicacontrol.NodeCapacity, error) {
	if owner == nil || owner.store == nil || budget == nil || revision == nil || ctx == nil {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	if err := context.Cause(ctx); err != nil {
		return replicacontrol.NodeCapacity{}, err
	}
	if len(samples) == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	nodeIdentity := owner.store.NodeIdentity()
	if nodeIdentity.NodeID == ([16]byte{}) {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityStale
	}
	if nodeIncarnation == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityStale
	}
	for _, sample := range samples {
		if sample.Identity.NodeIncarnation == 0 || sample.Identity.Group == (raftmember.GroupKey{}) {
			return replicacontrol.NodeCapacity{}, fmt.Errorf("group %x has invalid runtime identity: %w", sample.Identity.Group.GroupID, replicacontrol.ErrCapacityStale)
		}
	}
	physicalCapacity, err := owner.store.CapacityReservationBytes()
	if err != nil || physicalCapacity == 0 {
		return replicacontrol.NodeCapacity{}, errors.Join(replicacontrol.ErrCapacityUnavailable, err)
	}
	metrics := budget.Metrics()
	if metrics.ActiveCapacity <= 0 || metrics.Active > metrics.ActiveCapacity || uint64(metrics.ActiveCapacity) > math.MaxUint32 || uint64(metrics.Active) > math.MaxUint32 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	reportRevision := revision.Add(1)
	if reportRevision == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	template := replicacontrol.NodeCapacity{NodeID: nodeIdentity.NodeID, NodeIncarnation: nodeIncarnation,
		Revision: reportRevision, MigrationCapacity: physicalCapacity,
		MaxReceives: uint32(metrics.ActiveCapacity), ActiveReceives: uint32(metrics.Active)}
	template.Capacity[autosplit.ResourceLiveBytes] = physicalCapacity
	return RF3CapacityNodeFromSamples(template, samples)
}

// RF3CapacityNodeFromPhysical is the cold-bootstrap counterpart of
// RF3CapacityNodeFromOwner. Before a target WAL exists, the manifest's
// authenticated WAL geometry is the only physical reservation available; it
// is therefore exposed as a conservative capacity ceiling until the node
// owner publishes its live report.
func RF3CapacityNodeFromPhysical(
	ctx context.Context,
	nodeID rafttransport.NodeID,
	nodeIncarnation uint64,
	physicalCapacity uint64,
	budget *migrationbudget.Budget,
	revision *atomic.Uint64,
	samples []replicacontrol.CapacitySourceSample,
) (replicacontrol.NodeCapacity, error) {
	if ctx == nil || nodeID == (rafttransport.NodeID{}) || nodeIncarnation == 0 ||
		physicalCapacity == 0 || budget == nil || revision == nil || len(samples) == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	if err := context.Cause(ctx); err != nil {
		return replicacontrol.NodeCapacity{}, err
	}
	for _, sample := range samples {
		if sample.Identity.NodeIncarnation != nodeIncarnation {
			return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityStale
		}
	}
	metrics := budget.Metrics()
	if metrics.ActiveCapacity <= 0 || metrics.Active > metrics.ActiveCapacity ||
		uint64(metrics.ActiveCapacity) > math.MaxUint32 || uint64(metrics.Active) > math.MaxUint32 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	reportRevision := revision.Add(1)
	if reportRevision == 0 {
		return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
	}
	template := replicacontrol.NodeCapacity{NodeID: nodeID, NodeIncarnation: nodeIncarnation,
		Revision: reportRevision, MigrationCapacity: physicalCapacity,
		MaxReceives: uint32(metrics.ActiveCapacity), ActiveReceives: uint32(metrics.Active)}
	template.Capacity[autosplit.ResourceLiveBytes] = physicalCapacity
	return RF3CapacityNodeFromSamples(template, samples)
}

var _ replicacontrol.CapacitySource = (*rf3LiveCapacitySource)(nil)
var _ replicacontrol.CapacityRequestSource = (*rf3LiveCapacitySource)(nil)
var _ replicacontrol.CapacitySource = (*rf3ColdCapacitySource)(nil)
var _ replicacontrol.CapacityRequestSource = (*rf3ColdCapacitySource)(nil)
