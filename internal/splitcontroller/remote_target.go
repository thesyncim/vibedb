package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/shardcontrol"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const AbsoluteMaxShardActionGrants = 65536

// ShardActionTarget is the fixed-width, string-free destination authority
// carried by every remote split step. PlanDigest authenticates the complete
// string-bearing PlanIntent separately; this value contains only what a mux,
// route adapter, and local runtime need to select one exact group.
type ShardActionTarget struct {
	Group                  raftmember.GroupKey                  `json:"group"`
	Allocation             uint64                               `json:"allocation"`
	Member                 uint64                               `json:"member"`
	Authority              sqldriver.ReplicatedAuthorityProfile `json:"authority"`
	RelationManifestDigest [32]byte                             `json:"relation_manifest_digest"`
}

func (target ShardActionTarget) valid() bool {
	group, authority := target.Group, target.Authority
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{}) && target.Allocation != 0 &&
		authority.ActivePolicyGeneration != 0 && authority.ProtectionEpoch != 0 &&
		authority.OwnershipEpoch != 0 && authority.SchemaGeneration != 0 &&
		authority.RoutingVersion != 0 && authority.RouteGeneration != 0 &&
		target.RelationManifestDigest != ([32]byte{})
}

func remoteActionTargetsChild(kind ActionKind) bool {
	switch kind {
	case ActionStageChild, ActionActivateChild, ActionCreateChildWAL,
		ActionAdoptChildRuntime, ActionAwaitChildReady:
		return true
	default:
		return false
	}
}

func remoteActionTarget(plan *Plan, observed Observation, action Action) (ShardActionTarget, error) {
	if plan == nil || action.Kind < ActionAwaitSourceLeader || action.Kind > ActionComplete {
		return ShardActionTarget{}, ErrRemoteExecution
	}
	if remoteActionTargetsChild(action.Kind) {
		target, ok := plan.Target(action.Child)
		if !ok {
			return ShardActionTarget{}, ErrRemoteExecution
		}
		binding := target.SQL.Binding
		result := ShardActionTarget{
			Group: raftmember.GroupKey{
				ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
				TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
				ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
			},
			Allocation: binding.AllocationGeneration, Member: binding.MemberID,
			Authority: target.Authority, RelationManifestDigest: target.SQL.RelationManifestDigest,
		}
		if !result.valid() || result.Member == 0 {
			return ShardActionTarget{}, ErrRemoteExecution
		}
		return result, nil
	}
	if action.Child != 0 {
		return ShardActionTarget{}, ErrRemoteExecution
	}
	binding := observed.SourceState.Binding
	result := ShardActionTarget{
		Group: raftmember.GroupKey{
			ClusterID: [16]byte(binding.ClusterID), ClusterIncarnation: [16]byte(binding.ClusterIncarnation),
			TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
			ShardIncarnation:      [16]byte(binding.ShardIncarnation), GroupID: [16]byte(binding.GroupID),
		},
		Allocation: binding.AllocationGeneration, Member: observed.SourceStatus.MemberID,
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: binding.ActivePolicyGeneration,
			ProtectionEpoch:        binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
			SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
			RouteGeneration: binding.RouteGeneration,
		},
		RelationManifestDigest: planRelationManifestDigest(plan),
	}
	if !result.valid() {
		return ShardActionTarget{}, ErrRemoteExecution
	}
	return result, nil
}

func planRelationManifestDigest(plan *Plan) [32]byte {
	if plan == nil || plan.relationDigest != ([32]byte{}) {
		if plan == nil {
			return [32]byte{}
		}
		return plan.relationDigest
	}
	for child := uint8(0); child < plan.childCount; child++ {
		if target, ok := plan.Target(child); ok && target.SQL.RelationManifestDigest != ([32]byte{}) {
			return target.SQL.RelationManifestDigest
		}
	}
	return [32]byte{}
}

func targetMatchesRoute(target ShardActionTarget, route gateway.ReplicatedRoute) bool {
	command := route.Command
	if !target.valid() || route.Group != target.Group || route.AllocationGeneration != target.Allocation ||
		command.ActivePolicyGeneration != target.Authority.ActivePolicyGeneration ||
		command.ProtectionEpoch != target.Authority.ProtectionEpoch ||
		command.OwnershipEpoch != target.Authority.OwnershipEpoch ||
		command.SchemaGeneration != target.Authority.SchemaGeneration ||
		command.RoutingVersion != target.Authority.RoutingVersion ||
		command.RouteGeneration != target.Authority.RouteGeneration ||
		command.RelationManifestDigest != target.RelationManifestDigest {
		return false
	}
	if target.Member == 0 {
		return true
	}
	for _, replica := range route.Replicas {
		if replica.Member == target.Member {
			return true
		}
	}
	return false
}

// ShardActionGrant binds one immutable PlanIntent digest and exact destination
// group to local observation/execution capabilities. Grants are installed by
// trusted orchestration, never synthesized from DNS or a local catalog guess.
type ShardActionGrant struct {
	Operation  OperationID
	PlanDigest [32]byte
	Target     ShardActionTarget
	Plan       *Plan
	Observer   PlanObserver
	Executor   ShardActionExecutor
	Actions    uint16
}

type ShardActionExecutor interface {
	ExecuteSplitAction(context.Context, *Plan, Observation, Action) error
}

type shardActionGrantKey struct {
	operation OperationID
	digest    [32]byte
	target    ShardActionTarget
}

type StaticShardActionGrants struct{ grants []ShardActionGrant }

type shardActionGrantResolver interface {
	resolve(OperationID, [32]byte, ShardActionTarget) (ShardActionGrant, bool)
}

func NewStaticShardActionGrants(grants []ShardActionGrant) (*StaticShardActionGrants, error) {
	if len(grants) == 0 || len(grants) > AbsoluteMaxShardActionGrants {
		return nil, ErrRemoteExecution
	}
	owned := slices.Clone(grants)
	for index := range owned {
		grant := &owned[index]
		if grant.Operation == (OperationID{}) || grant.PlanDigest == ([32]byte{}) ||
			!grant.Target.valid() || grant.Plan == nil || grant.Observer == nil || grant.Executor == nil ||
			grant.Actions == 0 || grant.Actions&^uint16((1<<uint(ActionComplete))-1) != 0 ||
			grant.Plan.OperationID() != grant.Operation {
			return nil, ErrRemoteExecution
		}
	}
	slices.SortFunc(owned, func(left, right ShardActionGrant) int {
		return compareShardActionGrantKey(
			shardActionGrantKey{left.Operation, left.PlanDigest, left.Target},
			shardActionGrantKey{right.Operation, right.PlanDigest, right.Target},
		)
	})
	for index := 1; index < len(owned); index++ {
		if compareShardActionGrantKey(
			shardActionGrantKey{owned[index-1].Operation, owned[index-1].PlanDigest, owned[index-1].Target},
			shardActionGrantKey{owned[index].Operation, owned[index].PlanDigest, owned[index].Target},
		) == 0 {
			return nil, ErrRemoteExecution
		}
	}
	return &StaticShardActionGrants{grants: owned}, nil
}

func (grants *StaticShardActionGrants) resolve(
	operation OperationID, digest [32]byte, target ShardActionTarget,
) (ShardActionGrant, bool) {
	if grants == nil {
		return ShardActionGrant{}, false
	}
	key := shardActionGrantKey{operation, digest, target}
	index, found := slices.BinarySearchFunc(grants.grants, key,
		func(grant ShardActionGrant, key shardActionGrantKey) int {
			return compareShardActionGrantKey(
				shardActionGrantKey{grant.Operation, grant.PlanDigest, grant.Target}, key,
			)
		})
	if !found {
		return ShardActionGrant{}, false
	}
	return grants.grants[index], true
}

func compareShardActionGrantKey(left, right shardActionGrantKey) int {
	if order := bytes.Compare(left.operation[:], right.operation[:]); order != 0 {
		return order
	}
	if order := bytes.Compare(left.digest[:], right.digest[:]); order != 0 {
		return order
	}
	return compareShardActionTarget(left.target, right.target)
}

func compareShardActionTarget(left, right ShardActionTarget) int {
	leftRaw, rightRaw := fixedShardActionTarget(left), fixedShardActionTarget(right)
	return bytes.Compare(leftRaw[:], rightRaw[:])
}

func fixedShardActionTarget(target ShardActionTarget) [16*4 + 8*9 + 32]byte {
	var result [16*4 + 8*9 + 32]byte
	_ = appendFixedShardActionTarget(result[:0], target)
	return result
}

func appendFixedShardActionTarget(dst []byte, target ShardActionTarget) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 16*4+8*9+32)...)
	cursor := dst[start:]
	copy(cursor[0:16], target.Group.ClusterID[:])
	copy(cursor[16:32], target.Group.ClusterIncarnation[:])
	copy(cursor[32:48], target.Group.ShardIncarnation[:])
	copy(cursor[48:64], target.Group.GroupID[:])
	values := [...]uint64{
		target.Group.TopologyRecoveryEpoch, target.Allocation, target.Member,
		target.Authority.ActivePolicyGeneration, target.Authority.ProtectionEpoch,
		target.Authority.OwnershipEpoch, target.Authority.SchemaGeneration,
		target.Authority.RoutingVersion, target.Authority.RouteGeneration,
	}
	for index, value := range values {
		binary.LittleEndian.PutUint64(cursor[64+index*8:], value)
	}
	copy(cursor[64+len(values)*8:], target.RelationManifestDigest[:])
	return dst
}

// ShardActionRuntimeDispatcher is the concrete exact-operation runtime. It
// performs an allocation-free grant lookup, obtains a fresh coherent cut, and
// dispatches only a granted action to the pre-bound local capability.
type ShardActionRuntimeDispatcher struct{ grants shardActionGrantResolver }

func NewShardActionRuntimeDispatcher(
	grants shardActionGrantResolver,
) (*ShardActionRuntimeDispatcher, error) {
	if grants == nil {
		return nil, ErrRemoteExecution
	}
	return &ShardActionRuntimeDispatcher{grants: grants}, nil
}

func (runtime *ShardActionRuntimeDispatcher) ObserveSplit(
	ctx context.Context, operation OperationID, digest [32]byte, target ShardActionTarget,
) (*Plan, Observation, error) {
	grant, ok := runtime.grants.resolve(operation, digest, target)
	if !ok {
		return nil, Observation{}, ErrRemoteExecution
	}
	observed, err := grant.Observer.ObservePlan(ctx, grant.Plan)
	if err != nil {
		return nil, Observation{}, err
	}
	return grant.Plan, observed, nil
}

func (runtime *ShardActionRuntimeDispatcher) ExecuteSplitAction(
	ctx context.Context, target ShardActionTarget, plan *Plan, observed Observation, action Action,
) error {
	if plan == nil || action.Kind < ActionAwaitSourceLeader || action.Kind > ActionComplete {
		return ErrRemoteExecution
	}
	intent, err := AppendPlanIntent(nil, observed.Catalog, plan)
	if err != nil {
		return errors.Join(ErrRemoteExecution, err)
	}
	grant, ok := runtime.grants.resolve(plan.OperationID(), sha256Sum(intent), target)
	if !ok || grant.Plan != plan || grant.Actions&(1<<uint(action.Kind-1)) == 0 {
		return ErrRemoteExecution
	}
	return grant.Executor.ExecuteSplitAction(ctx, plan, observed, action)
}

func sha256Sum(value []byte) [32]byte { return sha256.Sum256(value) }

// ShardActionRouteGrant is the controller-side equivalent: one operation and
// plan digest may use only one pre-authorized exact route for this target.
type ShardActionRouteGrant struct {
	Operation  OperationID
	PlanDigest [32]byte
	Target     ShardActionTarget
	Route      gateway.ReplicatedRoute
}

type ExactShardActionRouteResolver struct{ grants []ShardActionRouteGrant }

func NewExactShardActionRouteResolver(
	grants []ShardActionRouteGrant,
) (*ExactShardActionRouteResolver, error) {
	if len(grants) == 0 || len(grants) > AbsoluteMaxShardActionGrants {
		return nil, ErrShardControlRoute
	}
	owned := slices.Clone(grants)
	for index := range owned {
		grant := &owned[index]
		if grant.Operation == (OperationID{}) || grant.PlanDigest == ([32]byte{}) ||
			!targetMatchesRoute(grant.Target, grant.Route) {
			return nil, ErrShardControlRoute
		}
		grant.Route.Replicas = slices.Clone(grant.Route.Replicas)
	}
	slices.SortFunc(owned, func(left, right ShardActionRouteGrant) int {
		return compareShardActionGrantKey(
			shardActionGrantKey{left.Operation, left.PlanDigest, left.Target},
			shardActionGrantKey{right.Operation, right.PlanDigest, right.Target},
		)
	})
	for index := 1; index < len(owned); index++ {
		if compareShardActionGrantKey(
			shardActionGrantKey{owned[index-1].Operation, owned[index-1].PlanDigest, owned[index-1].Target},
			shardActionGrantKey{owned[index].Operation, owned[index].PlanDigest, owned[index].Target},
		) == 0 {
			return nil, ErrShardControlRoute
		}
	}
	return &ExactShardActionRouteResolver{grants: owned}, nil
}

func (resolver *ExactShardActionRouteResolver) ResolveShardControl(
	_ context.Context, target ShardActionTarget, _ Action, request shardcontrol.Request,
) (gateway.ReplicatedRoute, error) {
	if resolver == nil || !target.valid() {
		return gateway.ReplicatedRoute{}, ErrShardControlRoute
	}
	key := shardActionGrantKey{OperationID(request.Operation), request.PlanDigest, target}
	index, found := slices.BinarySearchFunc(resolver.grants, key,
		func(grant ShardActionRouteGrant, key shardActionGrantKey) int {
			return compareShardActionGrantKey(
				shardActionGrantKey{grant.Operation, grant.PlanDigest, grant.Target}, key,
			)
		})
	if !found {
		return gateway.ReplicatedRoute{}, ErrShardControlRoute
	}
	return resolver.grants[index].Route, nil
}

func targetMatchesSourceState(target ShardActionTarget, state replicatedstate.State) bool {
	binding := state.Binding
	return target.Group.ClusterID == [16]byte(binding.ClusterID) &&
		target.Group.ClusterIncarnation == [16]byte(binding.ClusterIncarnation) &&
		target.Group.TopologyRecoveryEpoch == binding.TopologyRecoveryEpoch &&
		target.Group.ShardIncarnation == [16]byte(binding.ShardIncarnation) &&
		target.Group.GroupID == [16]byte(binding.GroupID) &&
		target.Allocation == binding.AllocationGeneration &&
		target.Authority.ActivePolicyGeneration == binding.ActivePolicyGeneration &&
		target.Authority.ProtectionEpoch == binding.ProtectionEpoch &&
		target.Authority.OwnershipEpoch == binding.OwnershipEpoch &&
		target.Authority.SchemaGeneration == binding.SchemaGeneration &&
		target.Authority.RoutingVersion == binding.RoutingVersion &&
		target.Authority.RouteGeneration == binding.RouteGeneration
}

func targetMatchesChild(target ShardActionTarget, child ChildTarget) bool {
	binding := child.SQL.Binding
	return target.Group == (raftmember.GroupKey{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
	}) && target.Allocation == binding.AllocationGeneration && target.Member == binding.MemberID &&
		target.Authority == child.Authority &&
		target.RelationManifestDigest == child.SQL.RelationManifestDigest
}
