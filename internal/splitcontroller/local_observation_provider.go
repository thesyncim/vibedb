package splitcontroller

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
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

// LocalPlanObservationProvider is the shipped shard-side provider. It reads a
// fixed number of bounded control records and never scans the runtime root.
// Groups are sorted once at construction; the serving lookup allocates no map
// entry proportional to historical split operations.
type LocalPlanObservationProvider struct {
	owners LocalObservationOwner
	groups []LocalObservationGroup
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
	return &LocalPlanObservationProvider{owners: owners, groups: owned}, nil
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
