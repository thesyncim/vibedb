package main

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// An inventory-restored child already belongs to the serving owner. Replaying
// its original unfinished split must observe that owner, not attempt to reopen
// its exclusively owned stage database or mint another runtime incarnation.
type rf3AdoptedChildReplay struct {
	plan    *splitcontroller.Plan
	child   uint8
	live    rf3RetainedSource
	owners  splitcontroller.LocalObservationOwner
	cutover [32]byte
	target  splitcontroller.ChildReplicaTarget
	apply   rf3AdoptedChildApply
}

type rf3AdoptedChildApply interface {
	Identity() (sqldriver.ReplicatedApplyIdentity, error)
	CapacityQualificationProfile() (sqldriver.ReplicatedApplyCapacityProfile, error)
}

func (executor *rf3AdoptedChildReplay) ExecuteSplitAction(context.Context, *splitcontroller.Plan, splitcontroller.Observation, splitcontroller.Action) error {
	return splitcontroller.ErrRemoteExecution
}

func (executor *rf3AdoptedChildReplay) ExecuteAuthorizedSplitAction(ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation, action splitcontroller.Action) error {
	if executor == nil || ctx == nil || plan != executor.plan || action.Child != executor.child {
		return splitcontroller.ErrRemoteExecution
	}
	if observed.Certificate != nil && observed.Certificate.Digest() != executor.cutover {
		return splitcontroller.ErrTopologyConflict
	}
	switch action.Kind {
	case splitcontroller.ActionStageChild, splitcontroller.ActionActivateChild, splitcontroller.ActionCreateChildWAL, splitcontroller.ActionAdoptChildRuntime:
		_, err := executor.observe(ctx)
		return err
	default:
		return splitcontroller.ErrRemoteExecution
	}
}

func (executor *rf3AdoptedChildReplay) observe(ctx context.Context) (*splitcontroller.ChildObservation, error) {
	identity := executor.live.runtime.identity
	observed, err := executor.owners.ObserveReplica(ctx, identity.Group, identity.MemberID)
	if err != nil || observed.Identity != identity {
		return nil, errors.Join(splitcontroller.ErrTopologyConflict, err)
	}
	applyIdentity, err := executor.apply.Identity()
	if err != nil {
		return nil, err
	}
	profile, err := executor.apply.CapacityQualificationProfile()
	if err != nil {
		return nil, err
	}
	replica := executor.target
	exact := replica.Member == identity.MemberID && replica.StoreID == identity.StoreID && replica.SQL.Binding == profile.Binding && replica.Apply == applyIdentity
	if !exact || !profile.Initialized || identity.RelationManifestDigest != profile.RelationManifestDigest {
		return nil, splitcontroller.ErrTopologyConflict
	}
	return &splitcontroller.ChildObservation{Child: executor.child, Phase: splitcontroller.ChildPhaseRuntimeAdopted,
		ApplyIdentity: applyIdentity, ApplyProfile: profile, WALBinding: profile.Binding, RuntimeIdentity: identity}, nil
}

func (executor *rf3AdoptedChildReplay) ObserveLocalSplitChild(ctx context.Context, request splitcontroller.PlanObservationRequest, member uint64) (*splitcontroller.ChildObservation, error) {
	if executor == nil || request.Operation != executor.plan.OperationID() || request.Child != executor.child || request.Group != executor.live.runtime.identity.Group || member != executor.live.runtime.identity.MemberID {
		return nil, splitcontroller.ErrPlanObservation
	}
	return executor.observe(ctx)
}

func (executor *rf3AdoptedChildReplay) Close() error { return nil }

func (resolver *rf3AdoptedSourceResolver) adoptedChildReplay(plan *splitcontroller.Plan, admission splitcontroller.PlanAdmission, child uint8, replica splitcontroller.ChildReplicaTarget) (*rf3AdoptedChildReplay, splitcontroller.LocalObservationGroup, bool, error) {
	if resolver.inventory == nil {
		return nil, splitcontroller.LocalObservationGroup{}, false, nil
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	group := groupFromBinding(replica.SQL.Binding)
	live, found := resolver.live[group]
	if !found {
		return nil, splitcontroller.LocalObservationGroup{}, false, nil
	}
	inventory := resolver.inventory
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.root == nil || inventory.failed {
		return nil, splitcontroller.LocalObservationGroup{}, false, errRF3Serving
	}
	var entry rf3AdoptedGroupEntry
	for _, candidate := range inventory.entries {
		if candidate.operation == [32]byte(plan.OperationID()) && candidate.child == uint64(child) {
			entry = candidate
			break
		}
	}
	if entry.operation == ([32]byte{}) || entry.plan != admission.PlanDigest || entry.certificate != replica.CertificateDigest {
		return nil, splitcontroller.LocalObservationGroup{}, false, errRF3Serving
	}
	target, found := plan.Target(child)
	if !found {
		return nil, splitcontroller.LocalObservationGroup{}, false, errRF3Serving
	}
	command := raftservice.CommandFence{ReplicaSetVersion: target.ReplicaSetVersion,
		ActivePolicyGeneration: target.Authority.ActivePolicyGeneration, ProtectionEpoch: target.Authority.ProtectionEpoch,
		OwnershipEpoch: target.Authority.OwnershipEpoch, SchemaGeneration: target.Authority.SchemaGeneration,
		RoutingVersion: target.Authority.RoutingVersion, RouteGeneration: target.Authority.RouteGeneration, RelationManifestDigest: target.RelationManifestDigest}
	executor := &rf3AdoptedChildReplay{plan: plan, child: child, live: live, owners: resolver.owners, cutover: entry.cutover, target: replica, apply: live.runtime.apply}
	observation := splitcontroller.LocalObservationGroup{Identity: live.runtime.identity, Command: command, Registry: live.registry, Children: executor, Capture: live.runtime.apply}
	return executor, observation, true, nil
}
