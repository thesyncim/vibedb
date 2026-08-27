package splitcontroller

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

// LocalObservationOwner is the serialized Multi-Raft ownership capability.
// ExecutionOwners implements it; ObserveReplica collects state and leadership
// in one owner turn, ordered with proposals and committed apply.
type LocalObservationOwner interface {
	ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error)
}

// LocalChildRuntimeObserver reconstructs the bounded activated/WAL/runtime
// evidence for one child. A nil result is authoritative absence. Keeping this
// separate from the stage cursor lets a prepared child remain non-serving.
type LocalChildRuntimeObserver interface {
	ObserveLocalSplitChild(context.Context, PlanObservationRequest, uint64) (*ChildObservation, error)
}

// LocalObservationGroup binds one retained group to its durable split runtime
// registry. Command is checked before any operation directory is acquired.
type LocalObservationGroup struct {
	Identity raftmember.RuntimeIdentity
	Command  raftservice.CommandFence
	Registry *RuntimeStoreRegistry
	Children LocalChildRuntimeObserver
}

// PreparedChildObservationGroup projects one authenticated local replica
// target into the exact pre-adoption observation identity. Keeping this
// construction beside Plan validation prevents command composition from
// re-deriving group or command fences from filesystem names.
func PreparedChildObservationGroup(
	target ChildTarget,
	replica ChildReplicaTarget,
	registry *RuntimeStoreRegistry,
	children LocalChildRuntimeObserver,
) (LocalObservationGroup, error) {
	local, err := LocalReplicaChildTarget(target, replica)
	if err != nil || registry == nil || children == nil {
		return LocalObservationGroup{}, errors.Join(ErrPlanObservation, err)
	}
	roster := make([]rafttransport.Member, len(target.Replicas))
	for index, item := range target.Replicas {
		roster[index] = rafttransport.Member{
			Group: groupFromChildTarget(target), MemberID: item.Member, Node: item.Node,
			Role: rafttransport.MemberVoter, ReplicaSetVersion: target.ReplicaSetVersion,
		}
	}
	command, err := validateChildExecutionRoster(local, roster)
	if err != nil {
		return LocalObservationGroup{}, errors.Join(ErrPlanObservation, err)
	}
	return LocalObservationGroup{
		Identity: raftmember.RuntimeIdentity{
			Group: groupFromChildTarget(target), Distribution: string(local.SQL.Binding.Distribution),
			Shard:                string(local.SQL.Binding.Shard),
			AllocationGeneration: uint64(local.SQL.Binding.AllocationGeneration),
			MemberID:             replica.Member, StoreID: replica.WAL.StoreID,
			NodeIncarnation:        replica.NodeIncarnation,
			RelationManifestDigest: target.RelationManifestDigest,
		},
		Command: command, Registry: registry, Children: children,
	}, nil
}

// LocalPlanObservationProvider is the shipped shard-side provider. It reads a
// fixed number of bounded control records and never scans the runtime root.
// Groups are sorted once at construction; the serving lookup allocates no map
// entry proportional to historical split operations.
type LocalPlanObservationProvider struct {
	mu     sync.RWMutex
	owners LocalObservationOwner
	groups []LocalObservationGroup
	limit  int
}

func NewLocalPlanObservationProvider(
	owners LocalObservationOwner,
	groups []LocalObservationGroup,
) (*LocalPlanObservationProvider, error) {
	if owners == nil || len(groups) == 0 || len(groups) > MaxPlanObservationEndpoints {
		return nil, ErrPlanObservation
	}
	owned := slices.Clone(groups)
	slices.SortFunc(owned, func(left, right LocalObservationGroup) int {
		return compareObservationGroup(left.Identity.Group, right.Identity.Group)
	})
	for index := range owned {
		group := owned[index]
		if group.Identity.Group == (raftmember.GroupKey{}) || group.Identity.MemberID == 0 ||
			group.Identity.StoreID == ([16]byte{}) || group.Identity.NodeIncarnation == 0 ||
			!group.Command.Valid() || group.Registry == nil ||
			index != 0 && compareObservationGroup(owned[index-1].Identity.Group, group.Identity.Group) == 0 {
			return nil, ErrPlanObservation
		}
	}
	return &LocalPlanObservationProvider{
		owners: owners, groups: owned, limit: MaxPlanObservationEndpoints,
	}, nil
}

// RegisterGroups publishes exact, already-admitted child identities to the
// observation service. It is bounded by the same endpoint ceiling as the wire
// protocol and is idempotent for byte-identical bindings. A different binding
// for an existing Raft group is rejected; replacement must first pass through
// terminal operation cleanup so an old plan cannot redirect observations.
func (provider *LocalPlanObservationProvider) RegisterGroups(groups []LocalObservationGroup) error {
	if provider == nil || len(groups) == 0 || len(groups) > MaxPlanObservationEndpoints {
		return ErrPlanObservation
	}
	owned := slices.Clone(groups)
	slices.SortFunc(owned, func(left, right LocalObservationGroup) int {
		return compareObservationGroup(left.Identity.Group, right.Identity.Group)
	})
	for index := range owned {
		group := owned[index]
		if !validLocalObservationGroup(group) ||
			index != 0 && compareObservationGroup(owned[index-1].Identity.Group, group.Identity.Group) == 0 {
			return ErrPlanObservation
		}
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	merged := slices.Clone(provider.groups)
	for _, group := range owned {
		index, found := slices.BinarySearchFunc(merged, group.Identity.Group,
			func(item LocalObservationGroup, key raftmember.GroupKey) int {
				return compareObservationGroup(item.Identity.Group, key)
			})
		if found {
			if !sameLocalObservationGroup(merged[index], group) {
				return ErrPlanObservation
			}
			if merged[index].Children == nil && group.Children != nil {
				merged[index].Children = group.Children
			}
			continue
		}
		if len(merged) == provider.limit {
			return ErrPlanObservation
		}
		merged = append(merged, LocalObservationGroup{})
		copy(merged[index+1:], merged[index:])
		merged[index] = group
	}
	provider.groups = merged
	return nil
}

// RefreshRetainedGroup advances only the command fence of the same exclusive
// runtime/registry after a caller has checked the serialized owner state.
func (provider *LocalPlanObservationProvider) RefreshRetainedGroup(group LocalObservationGroup) error {
	if provider == nil || !validLocalObservationGroup(group) {
		return ErrPlanObservation
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	index, found := slices.BinarySearchFunc(provider.groups, group.Identity.Group, func(item LocalObservationGroup, key raftmember.GroupKey) int {
		return compareObservationGroup(item.Identity.Group, key)
	})
	if !found {
		return ErrPlanObservation
	}
	prior := provider.groups[index]
	a, b := prior.Command, group.Command
	if prior.Identity != group.Identity || prior.Registry != group.Registry || a.RelationManifestDigest != b.RelationManifestDigest ||
		b.ReplicaSetVersion < a.ReplicaSetVersion || b.ActivePolicyGeneration < a.ActivePolicyGeneration || b.ProtectionEpoch < a.ProtectionEpoch ||
		b.OwnershipEpoch < a.OwnershipEpoch || b.SchemaGeneration < a.SchemaGeneration || b.RoutingVersion < a.RoutingVersion || b.RouteGeneration < a.RouteGeneration {
		return ErrPlanObservation
	}
	prior.Command = group.Command
	provider.groups[index] = prior
	return nil
}

// UnregisterGroups removes only byte-identical dynamically admitted groups.
// Retained groups are never passed by production admission and a substituted
// observer or registry cannot revoke an existing binding.
func (provider *LocalPlanObservationProvider) UnregisterGroups(groups []LocalObservationGroup) error {
	if provider == nil || len(groups) == 0 || len(groups) > MaxPlanObservationEndpoints {
		return ErrPlanObservation
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for _, group := range groups {
		index, found := slices.BinarySearchFunc(provider.groups, group.Identity.Group,
			func(item LocalObservationGroup, key raftmember.GroupKey) int {
				return compareObservationGroup(item.Identity.Group, key)
			})
		if !found || !sameLocalObservationGroup(provider.groups[index], group) {
			return ErrPlanObservation
		}
		provider.groups = append(provider.groups[:index], provider.groups[index+1:]...)
	}
	return nil
}

func validLocalObservationGroup(group LocalObservationGroup) bool {
	return group.Identity.Group != (raftmember.GroupKey{}) && group.Identity.MemberID != 0 &&
		group.Identity.StoreID != ([16]byte{}) && group.Identity.NodeIncarnation != 0 &&
		group.Command.Valid() && group.Registry != nil
}

func sameLocalObservationGroup(left, right LocalObservationGroup) bool {
	return left.Identity == right.Identity && left.Command == right.Command &&
		left.Registry == right.Registry
}

func (provider *LocalPlanObservationProvider) ObserveSplitSource(
	ctx context.Context,
	request PlanObservationRequest,
	targetMember uint64,
) (SourcePlanObservation, error) {
	group, ok := provider.resolve(request, targetMember)
	if !ok || ctx == nil {
		return SourcePlanObservation{}, ErrPlanObservation
	}
	observation, err := provider.owners.ObserveReplica(ctx, request.Group, targetMember)
	if err != nil || observation.Status.MemberID != targetMember {
		return SourcePlanObservation{}, errors.Join(ErrPlanObservation, err)
	}
	state := observation.State
	serving := raftservice.ServingState{
		Identity: group.Identity,
		Command:  request.Command, Status: observation.Status,
	}
	lease, err := group.Registry.Acquire(request.Operation)
	if err != nil {
		return SourcePlanObservation{}, err
	}
	defer lease.Release()
	result := SourcePlanObservation{
		RequestDigest: request.RequestDigest, State: state, Serving: serving,
		Status: serving.Status,
	}
	if raw, present, loadErr := lease.Load(RuntimeStateCapture, 0); loadErr != nil {
		return SourcePlanObservation{}, loadErr
	} else if present {
		descriptor, openErr := rangesplit.OpenSourceCaptureDescriptor(raw.Payload)
		if openErr != nil {
			return SourcePlanObservation{}, errors.Join(ErrPlanObservation, openErr)
		}
		result.CaptureHead = descriptor.Head.Applied
	}
	if result.Artifacts, err = loadObservedArtifacts(lease); err != nil {
		return SourcePlanObservation{}, err
	}
	if result.Tail, err = loadObservedTail(lease); err != nil {
		return SourcePlanObservation{}, err
	}
	if result.Certificate, err = loadObservedCertificate(lease); err != nil {
		return SourcePlanObservation{}, err
	}
	if result.Prune, err = loadObservedPrune(lease); err != nil {
		return SourcePlanObservation{}, err
	}
	return result, nil
}

func (provider *LocalPlanObservationProvider) ObserveSplitChild(
	ctx context.Context,
	request PlanObservationRequest,
	targetMember uint64,
) (ChildPlanObservation, error) {
	group, ok := provider.resolve(request, targetMember)
	if !ok || ctx == nil {
		return ChildPlanObservation{}, ErrPlanObservation
	}
	lease, err := group.Registry.Acquire(request.Operation)
	if err != nil {
		return ChildPlanObservation{}, err
	}
	defer lease.Release()
	result := ChildPlanObservation{RequestDigest: request.RequestDigest}
	if raw, present, loadErr := lease.Load(RuntimeStateStage, request.Child); loadErr != nil {
		return ChildPlanObservation{}, loadErr
	} else if present {
		cursor, openErr := rangesplit.OpenChildStageCursor(raw.Payload)
		if openErr != nil || cursor == nil || cursor.Child() != request.Child {
			return ChildPlanObservation{}, errors.Join(ErrPlanObservation, openErr)
		}
		result.Stage = cursor
	}
	if group.Children != nil {
		result.Runtime, err = group.Children.ObserveLocalSplitChild(ctx, request, targetMember)
		if err != nil {
			return ChildPlanObservation{}, err
		}
	}
	return result, nil
}

func (provider *LocalPlanObservationProvider) resolve(
	request PlanObservationRequest,
	targetMember uint64,
) (LocalObservationGroup, bool) {
	if provider == nil || !validNetworkPlanObservationRequest(request) || targetMember == 0 {
		return LocalObservationGroup{}, false
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	index, found := slices.BinarySearchFunc(provider.groups, request.Group,
		func(group LocalObservationGroup, key raftmember.GroupKey) int {
			return compareObservationGroup(group.Identity.Group, key)
		})
	if !found {
		return LocalObservationGroup{}, false
	}
	group := provider.groups[index]
	return group, group.Identity.MemberID == targetMember &&
		group.Identity.AllocationGeneration == uint64(request.Allocation) &&
		group.Command == request.Command
}

func compareObservationGroup(left, right raftmember.GroupKey) int {
	for _, pair := range [][2][]byte{
		{left.ClusterID[:], right.ClusterID[:]},
		{left.ClusterIncarnation[:], right.ClusterIncarnation[:]},
		{left.ShardIncarnation[:], right.ShardIncarnation[:]},
		{left.GroupID[:], right.GroupID[:]},
	} {
		if order := bytes.Compare(pair[0], pair[1]); order != 0 {
			return order
		}
	}
	if left.TopologyRecoveryEpoch < right.TopologyRecoveryEpoch {
		return -1
	}
	if left.TopologyRecoveryEpoch > right.TopologyRecoveryEpoch {
		return 1
	}
	return 0
}

func loadObservedArtifacts(lease *RuntimeStoreLease) (*rangesplit.ChildArtifactSet, error) {
	raw, present, err := lease.Load(RuntimeStateArtifacts, 0)
	if err != nil || !present {
		return nil, err
	}
	value, err := rangesplit.OpenChildArtifactSet(raw.Payload)
	if err != nil {
		return nil, errors.Join(ErrPlanObservation, err)
	}
	return &value, nil
}

func loadObservedTail(lease *RuntimeStoreLease) (*rangesplit.TailCursor, error) {
	raw, present, err := lease.Load(RuntimeStateTail, 0)
	if err != nil || !present {
		return nil, err
	}
	value, err := rangesplit.OpenTailCursor(raw.Payload)
	if err != nil {
		return nil, errors.Join(ErrPlanObservation, err)
	}
	return &value, nil
}

func loadObservedCertificate(lease *RuntimeStoreLease) (*rangesplit.CutoverCertificate, error) {
	raw, present, err := lease.Load(RuntimeStateCertificate, 0)
	if err != nil || !present {
		return nil, err
	}
	value, err := rangesplit.OpenCutoverCertificate(raw.Payload)
	if err != nil {
		return nil, errors.Join(ErrPlanObservation, err)
	}
	return value, nil
}

func loadObservedPrune(lease *RuntimeStoreLease) (*rangesplit.RetainedPruneCursor, error) {
	raw, present, err := lease.Load(RuntimeStatePrune, 0)
	if err != nil || !present {
		return nil, err
	}
	value, err := rangesplit.OpenRetainedPruneCursor(raw.Payload)
	if err != nil {
		return nil, errors.Join(ErrPlanObservation, err)
	}
	return value, nil
}
