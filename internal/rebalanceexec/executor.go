// Package rebalanceexec composes the evidence-driven replica-move controller
// with gateway and shard-control capabilities without introducing an import
// cycle between gateway and internal/rebalance.
package rebalanceexec

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	ErrExecutorConfig   = errors.New("rebalanceexec: invalid replica move executor configuration")
	ErrExecutionFence   = errors.New("rebalanceexec: replica move execution fence conflicts")
	ErrGrantUnavailable = errors.New("rebalanceexec: certified membership grant unavailable")
)

// MoveRoute is one exact controller routing cut. The resolver may retain the
// source and enrolled-target descriptors across catalog publication; ordinary
// data routing must never use those retained control-only endpoints.
type MoveRoute struct {
	Catalog        *gateway.Snapshot
	Membership     gateway.ReplicatedMembershipRoute
	Retiring       gateway.ReplicatedEndpoint
	SnapshotSource gateway.ReplicatedEndpoint
	Target         gateway.ReplicatedEndpoint
	Command        raftservice.CommandFence
}

// MoveRouteResolver supplies the current control-only route for one exact
// operation/action proof. It is the future command wiring boundary for catalog
// history and endpoint enrollment; it grants no membership authority itself.
type MoveRouteResolver interface {
	ResolveReplicaMove(
		context.Context,
		rebalance.OperationID,
		*rebalance.Plan,
		rebalance.ReplicatedMoveExecution,
	) (MoveRoute, error)
}

// MembershipClient is implemented by gateway.ReplicatedExecutor.
type MembershipClient interface {
	ApplyMembership(
		context.Context,
		gateway.ReplicatedMembershipRoute,
		shardservice.ReplicatedMembershipRequest,
	) (gateway.ReplicatedMembershipResult, error)
}

// SnapshotExportRequest binds source preparation/export to the exact journaled
// operation and action proof. The source returns a fixed-width descriptor for
// an already-published artifact; exact retries resume its durable cursor.
type SnapshotExportRequest = snapshottransfer.SourceControlRequest

// SnapshotSource prepares and exports one pinned learner snapshot.
type SnapshotSource interface {
	PrepareReplicaMoveSnapshot(
		context.Context, SnapshotExportRequest,
	) (snapshottransfer.Descriptor, error)
}

// SnapshotBootstrapClient is implemented by snapshottransfer.BootstrapControlClient.
type SnapshotBootstrapClient interface {
	Execute(
		context.Context,
		rafttransport.NodeID,
		snapshottransfer.BootstrapRequest,
	) (snapshottransfer.BootstrapRecord, error)
}

// MoveAwaiter performs a bounded wait/poll for non-mutating reconcile actions.
// Observation is still the authority: nil only means the wait completed, and
// the controller re-observes before deriving another action.
type MoveAwaiter interface {
	AwaitReplicaMove(
		context.Context,
		rebalance.OperationID,
		*rebalance.Plan,
		rebalance.ReplicatedMoveExecution,
	) error
}

// OwnershipProposer persists the exact ownership transition on the shard Raft
// group. Nil means admission is durable; outcome-unknown must be returned so
// the controller resolves it from the next observation.
type OwnershipProposer interface {
	ProposeReplicaMoveOwnership(
		context.Context,
		rebalance.OperationID,
		[32]byte,
		gateway.ReplicatedMembershipRoute,
		[]byte,
	) error
}

// CatalogAuthority is the exact gateway catalog-Raft surface used for final
// roster publication and pending-command settlement.
type CatalogAuthority interface {
	PublishReplicaReplacement(
		context.Context, uint64, *gateway.Snapshot, membershipgrant.Grant,
	) error
	PublishReplicaReplacementPostRemove(
		context.Context, uint64, *gateway.Snapshot, membershipgrant.Grant, uint64,
	) error
	FinalizeReplicaReplacement(context.Context, membershipgrant.Grant) error
	RetryPending(context.Context) error
	Read(context.Context) (*gateway.Snapshot, error)
}

// CatalogDrainCertifier returns authority only after every gateway incarnation
// in the exact cluster roster has drained the requested catalog cut.
type CatalogDrainCertifier interface {
	CertifyClusterCatalogDrain(
		context.Context,
		gateway.ClusterCatalogDrainRequest,
	) (gateway.ClusterCatalogDrainCertificate, error)
}

type SourceRetirementRequest struct {
	Operation            [32]byte
	Step                 [32]byte
	Group                raftmember.GroupKey
	AllocationGeneration uint64
	Command              raftservice.CommandFence
	Source               gateway.ReplicatedEndpoint
	Target               gateway.ReplicatedEndpoint
	Term                 uint64
}

type SourceRetirer interface {
	RetireReplicaSource(context.Context, SourceRetirementRequest) error
}

type Options struct {
	Routes     MoveRouteResolver
	Grants     membershipgrant.Source
	Membership MembershipClient
	Snapshots  SnapshotSource
	Bootstrap  SnapshotBootstrapClient
	Awaiter    MoveAwaiter
	Ownership  OwnershipProposer
	Catalog    CatalogAuthority
	Drainer    CatalogDrainCertifier
	Retirer    SourceRetirer
}

// Executor is the concrete action composition consumed by
// rebalance.ExecuteReplicatedMoveStep. Every dependency is a narrow existing
// authority or a command-wiring seam; no action is authorized by process-local
// progress.
type Executor struct{ options Options }

func New(options Options) (*Executor, error) {
	if options.Routes == nil || options.Grants == nil || options.Membership == nil ||
		options.Snapshots == nil || options.Bootstrap == nil || options.Awaiter == nil ||
		options.Ownership == nil || options.Catalog == nil || options.Drainer == nil ||
		options.Retirer == nil {
		return nil, ErrExecutorConfig
	}
	return &Executor{options: options}, nil
}

func (executor *Executor) ExecuteReplicaMove(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	if executor == nil || ctx == nil || plan == nil || operation == (rebalance.OperationID{}) ||
		plan.OperationID() != operation || execution.Proof == ([32]byte{}) ||
		execution.Action.Kind < rebalance.ActionAwaitLeader ||
		execution.Action.Kind >= rebalance.ActionComplete ||
		execution.PublicationApplied == 0 || execution.PublicationReplicaSet == 0 ||
		!validExecutionAction(plan, execution) {
		return ErrExecutionFence
	}
	switch execution.Action.Kind {
	case rebalance.ActionAwaitLeader, rebalance.ActionAwaitSnapshotInstall,
		rebalance.ActionAwaitCatchUp:
		return executor.options.Awaiter.AwaitReplicaMove(ctx, operation, plan, execution)
	case rebalance.ActionAddLearner, rebalance.ActionPromoteVoter,
		rebalance.ActionTransferLeader, rebalance.ActionRemoveSource:
		return executor.executeMembership(ctx, operation, plan, execution)
	case rebalance.ActionCreateSnapshotBase:
		return executor.executeSnapshot(ctx, operation, plan, execution)
	case rebalance.ActionAdvanceOwnership:
		return executor.executeOwnership(ctx, operation, plan, execution)
	case rebalance.ActionPublishCatalog:
		return executor.executeCatalog(ctx, operation, plan, execution)
	case rebalance.ActionRefreshCatalogFence:
		return executor.executeCatalogRefresh(ctx, operation, plan, execution)
	case rebalance.ActionAwaitCatalogDrain:
		return executor.executeCatalogDrain(ctx, operation, execution)
	case rebalance.ActionRetireSource:
		return executor.executeRetirement(ctx, operation, plan, execution)
	default:
		return ErrExecutionFence
	}
}

func (executor *Executor) executeCatalogDrain(
	ctx context.Context,
	operation rebalance.OperationID,
	execution rebalance.ReplicatedMoveExecution,
) error {
	snapshot, err := executor.options.Catalog.Read(ctx)
	if err != nil || snapshot == nil ||
		snapshot.Generation() != execution.Action.CatalogGeneration {
		return errors.Join(err, ErrExecutionFence)
	}
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil {
		return errors.Join(err, ErrExecutionFence)
	}
	request := gateway.ClusterCatalogDrainRequest{
		Operation: [32]byte(operation), Step: execution.Proof,
		Generation: snapshot.Generation(), CatalogDigest: digest,
	}
	certificate, err := executor.options.Drainer.CertifyClusterCatalogDrain(ctx, request)
	if err != nil || !certificate.ValidFor(request) {
		return errors.Join(err, ErrExecutionFence)
	}
	return nil
}

func (executor *Executor) executeMembership(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	grant, found, err := executor.options.Grants.ReadMembershipGrant(ctx, plan.Group())
	if err != nil || !found {
		return errors.Join(err, ErrGrantUnavailable)
	}
	if err = validateGrant(plan, grant); err != nil {
		return err
	}
	cut, err := executor.resolve(ctx, operation, plan, execution)
	if err != nil || [16]byte(cut.Target.Node) != grant.TargetNode {
		return errors.Join(err, ErrExecutionFence)
	}
	if execution.Action.Kind == rebalance.ActionRemoveSource && execution.LeaderTerm == 0 {
		return ErrExecutionFence
	}
	cut.Membership.Serving.Command.ReplicaSetVersion = execution.PublicationReplicaSet
	kind := raftservice.MembershipAddLearner
	transferTerm := uint64(0)
	switch execution.Action.Kind {
	case rebalance.ActionAddLearner:
		kind = raftservice.MembershipAddLearner
	case rebalance.ActionPromoteVoter:
		kind = raftservice.MembershipPromoteVoter
	case rebalance.ActionTransferLeader:
		kind = raftservice.MembershipTransferLeader
	case rebalance.ActionRemoveSource:
		kind = raftservice.MembershipRemoveVoter
		transferTerm = execution.LeaderTerm
	}
	request := shardservice.ReplicatedMembershipRequest{
		Kind: kind, TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
		CatalogGeneration:         grant.CatalogGeneration,
		ExpectedReplicaSetVersion: execution.PublicationReplicaSet,
		SourceMember:              plan.RetiringMember(), TargetMember: plan.TargetMember(),
		TransferTerm: transferTerm,
	}
	_, err = executor.options.Membership.ApplyMembership(ctx, cut.Membership, request)
	return err
}

func (executor *Executor) executeSnapshot(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	cut, err := executor.resolve(ctx, operation, plan, execution)
	if err != nil || cut.Target.Node == ([16]byte{}) || cut.Target.StoreID == ([16]byte{}) ||
		cut.Target.NodeIncarnation == 0 {
		return errors.Join(err, ErrExecutionFence)
	}
	descriptor, err := executor.options.Snapshots.PrepareReplicaMoveSnapshot(
		ctx, SnapshotExportRequest{
			Operation: [32]byte(operation), Step: execution.Proof, Group: plan.Group(),
			SourceMember: plan.SnapshotSourceMember(), TargetMember: plan.TargetMember(),
			TargetStore: cut.Target.StoreID, TargetIncarnation: cut.Target.NodeIncarnation,
			ReplicaSetVersion: execution.PublicationReplicaSet,
			SourceNode:        cut.SnapshotSource.Node,
		},
	)
	if err != nil {
		return err
	}
	if !descriptor.Valid() || descriptor.Group != plan.Group() ||
		descriptor.SourceMember != plan.SnapshotSourceMember() ||
		descriptor.TargetMember != plan.TargetMember() ||
		descriptor.TargetStore != cut.Target.StoreID ||
		descriptor.TargetIncarnation != cut.Target.NodeIncarnation ||
		descriptor.ReplicaSetVersion != execution.PublicationReplicaSet {
		return ErrExecutionFence
	}
	record, err := executor.options.Bootstrap.Execute(ctx, cut.Target.Node,
		snapshottransfer.BootstrapRequest{
			Operation: [32]byte(operation), Step: execution.Proof, Descriptor: descriptor,
		})
	if err != nil {
		return err
	}
	if record.State != snapshottransfer.BootstrapComplete ||
		record.Request.Operation != [32]byte(operation) ||
		record.Request.Step != execution.Proof || record.Request.Descriptor != descriptor {
		return ErrExecutionFence
	}
	return nil
}

func (executor *Executor) executeOwnership(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	cut, err := executor.resolve(ctx, operation, plan, execution)
	if err != nil {
		return err
	}
	command, err := plan.OwnershipCommand(execution.PublicationReplicaSet)
	if err != nil {
		return err
	}
	cut.Membership.Serving.Command.ReplicaSetVersion = execution.PublicationReplicaSet
	return executor.options.Ownership.ProposeReplicaMoveOwnership(
		ctx, operation, execution.Proof, cut.Membership, command,
	)
}

func (executor *Executor) executeCatalog(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	grant, found, err := executor.options.Grants.ReadMembershipGrant(ctx, plan.Group())
	if err != nil || !found {
		return errors.Join(err, ErrGrantUnavailable)
	}
	if err = validateGrant(plan, grant); err != nil {
		return err
	}
	cut, err := executor.resolve(ctx, operation, plan, execution)
	if err != nil || [16]byte(cut.Target.Node) != grant.TargetNode || cut.Catalog == nil ||
		cut.Command.ReplicaSetVersion != execution.PublicationReplicaSet ||
		cut.Command.OwnershipEpoch == 0 || !cut.Command.Valid() {
		return errors.Join(err, ErrExecutionFence)
	}
	target := gateway.ReplicatedReplicaDescriptor{
		Member: cut.Target.Member, Node: cut.Target.Node, StoreID: cut.Target.StoreID,
		NodeIncarnation: cut.Target.NodeIncarnation,
		Endpoint:        distribution.EndpointID(cut.Target.Endpoint),
		NativeEndpoint:  distribution.EndpointID(cut.Target.NativeEndpoint),
		ControlEndpoint: distribution.EndpointID(cut.Target.ControlEndpoint),
	}
	next, err := gateway.BuildReplicaReplacementTransition(
		cut.Catalog, plan.TargetManifest(), plan.NextCatalogGeneration(),
		grant, target, cut.Command,
	)
	if err != nil {
		return err
	}
	err = executor.options.Catalog.PublishReplicaReplacement(
		ctx, plan.CatalogGeneration(), next, grant,
	)
	if !errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		return err
	}
	if err = executor.options.Catalog.RetryPending(ctx); err != nil {
		return err
	}
	settled, err := executor.options.Catalog.Read(ctx)
	if err != nil || settled.Generation() != next.Generation() {
		return errors.Join(err, ErrExecutionFence)
	}
	return nil
}

func (executor *Executor) executeCatalogRefresh(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	grant, found, err := executor.options.Grants.ReadMembershipGrant(ctx, plan.Group())
	if err != nil || !found {
		return errors.Join(err, ErrGrantUnavailable)
	}
	if err = validateGrant(plan, grant); err != nil {
		return err
	}
	cut, err := executor.resolve(ctx, operation, plan, execution)
	if err != nil || cut.Catalog == nil ||
		cut.Catalog.Generation() != plan.NextCatalogGeneration() ||
		cut.Command.ReplicaSetVersion != execution.Action.ReplicaSetVersion ||
		cut.Command.ReplicaSetVersion != execution.PublicationReplicaSet ||
		!cut.Command.Valid() {
		return errors.Join(err, ErrExecutionFence)
	}
	next, err := gateway.BuildReplicaReplacementPostRemoveTransition(
		cut.Catalog, plan.PostRemoveCatalogGeneration(), grant,
		execution.Action.ReplicaSetVersion,
	)
	if err != nil {
		return err
	}
	err = executor.options.Catalog.PublishReplicaReplacementPostRemove(
		ctx, plan.NextCatalogGeneration(), next, grant,
		execution.Action.ReplicaSetVersion,
	)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		if err = executor.options.Catalog.RetryPending(ctx); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	settled, err := executor.options.Catalog.Read(ctx)
	if err != nil || settled.Generation() != plan.PostRemoveCatalogGeneration() {
		return errors.Join(err, ErrExecutionFence)
	}
	return nil
}

func (executor *Executor) executeRetirement(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	grant, found, err := executor.options.Grants.ReadMembershipGrant(ctx, plan.Group())
	if err != nil {
		return err
	}
	if found {
		if err = validateGrant(plan, grant); err != nil {
			return err
		}
	}
	cut, err := executor.resolve(ctx, operation, plan, execution)
	if err != nil || !exactRetiringReplica(cut.Retiring, plan.RetiringReplica()) ||
		cut.Target.Member != plan.TargetMember() || execution.LeaderTerm == 0 {
		return errors.Join(err, ErrExecutionFence)
	}
	err = executor.options.Retirer.RetireReplicaSource(ctx, SourceRetirementRequest{
		Operation: [32]byte(operation), Step: execution.Proof, Group: plan.Group(),
		AllocationGeneration: cut.Membership.Serving.AllocationGeneration,
		Command:              cut.Command,
		Source:               cut.Retiring, Target: cut.Target, Term: execution.LeaderTerm,
	})
	if err != nil || !found {
		return err
	}
	err = executor.options.Catalog.FinalizeReplicaReplacement(ctx, grant)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		if err = executor.options.Catalog.RetryPending(ctx); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	_, stillPresent, err := executor.options.Grants.ReadMembershipGrant(ctx, plan.Group())
	if err != nil || stillPresent {
		return errors.Join(err, ErrExecutionFence)
	}
	return nil
}

func (executor *Executor) resolve(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) (MoveRoute, error) {
	cut, err := executor.options.Routes.ResolveReplicaMove(ctx, operation, plan, execution)
	if err != nil || cut.Membership.Serving.Group != plan.Group() ||
		!exactRetiringReplica(cut.Retiring, plan.RetiringReplica()) ||
		cut.SnapshotSource.Member != plan.SnapshotSourceMember() ||
		cut.Target.Member != plan.TargetMember() {
		return MoveRoute{}, errors.Join(err, ErrExecutionFence)
	}
	return cut, nil
}

func exactRetiringReplica(endpoint gateway.ReplicatedEndpoint, identity rebalance.ReplicaIdentity) bool {
	return endpoint.Member == identity.Member && endpoint.Node == identity.Node &&
		endpoint.StoreID == identity.StoreID &&
		endpoint.NodeIncarnation == identity.NodeIncarnation &&
		distribution.EndpointID(endpoint.ControlEndpoint) == identity.ControlEndpoint
}

func validateGrant(plan *rebalance.Plan, grant membershipgrant.Grant) error {
	if plan == nil || !grant.Valid() || grant.Group != plan.Group() ||
		grant.CatalogGeneration != plan.CatalogGeneration() ||
		grant.SourceMember != plan.RetiringMember() || grant.TargetMember != plan.TargetMember() {
		return ErrExecutionFence
	}
	return nil
}

func validExecutionAction(plan *rebalance.Plan, execution rebalance.ReplicatedMoveExecution) bool {
	action := execution.Action
	switch action.Kind {
	case rebalance.ActionAddLearner, rebalance.ActionAwaitSnapshotInstall,
		rebalance.ActionAwaitCatchUp,
		rebalance.ActionPromoteVoter, rebalance.ActionTransferLeader,
		rebalance.ActionAdvanceOwnership:
		return action.Member == plan.TargetMember() && action.CatalogGeneration == 0 &&
			action.ReplicaSetVersion == 0
	case rebalance.ActionCreateSnapshotBase:
		return action.Member == plan.SnapshotSourceMember() && action.CatalogGeneration == 0 &&
			action.ReplicaSetVersion == 0
	case rebalance.ActionRemoveSource, rebalance.ActionRetireSource:
		return action.Member == plan.RetiringMember() && action.CatalogGeneration == 0 &&
			action.ReplicaSetVersion == 0
	case rebalance.ActionAwaitLeader:
		return action.Member == 0 && action.CatalogGeneration == 0 &&
			action.ReplicaSetVersion == 0
	case rebalance.ActionPublishCatalog:
		return action.Member == 0 && action.CatalogGeneration == plan.NextCatalogGeneration() &&
			action.ReplicaSetVersion == 0
	case rebalance.ActionAwaitCatalogDrain:
		return action.Member == 0 &&
			(action.CatalogGeneration == plan.NextCatalogGeneration() ||
				action.CatalogGeneration == plan.PostRemoveCatalogGeneration()) &&
			action.ReplicaSetVersion == 0
	case rebalance.ActionRefreshCatalogFence:
		return action.Member == 0 &&
			action.CatalogGeneration == plan.PostRemoveCatalogGeneration() &&
			action.ReplicaSetVersion == execution.PublicationReplicaSet
	default:
		return false
	}
}
