package splitcontroller

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

type LazyReplicatedChildExecutorOptions struct {
	Plan       *Plan
	PlanDigest [32]byte
	Child      uint8
	Replica    ChildReplicaTarget
	Lease      *RuntimeStoreLease

	Registrar       ExecutionGroupRegistrar
	StaticBootstrap *pb.Snapshot
	ArtifactOptions replicatedstate.SnapshotArtifactOptions
	WALKey          raftstore.Key
	WALOptions      raftstore.Options
	CheckpointBytes uint64
	Data            *DynamicSplitData

	Opener        rafttransport.SnapshotStreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	ChunkBytes    uint32
	MaxReconnects int
	Workspace     []byte
}

// LazyReplicatedChildExecutor opens only the exact pre-created child SQL root
// authenticated by PlanIntent. Artifact identity is unavailable at admission,
// so the exclusive SQL stage is claimed on the first witnessed child action
// and reconstructed from its durable typed cursor after restart.
type LazyReplicatedChildExecutor struct {
	mu sync.Mutex

	options   LazyReplicatedChildExecutorOptions
	stage     *ReplicatedChildStageActions
	lifecycle *LocalChildLifecycle
}

func (executor *LazyReplicatedChildExecutor) Close() error {
	if executor == nil {
		return nil
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	defer clear(executor.options.WALKey.Material[:])
	if executor.lifecycle == nil {
		return nil
	}
	err := executor.lifecycle.Close()
	executor.lifecycle, executor.stage = nil, nil
	return err
}

func NewLazyReplicatedChildExecutor(
	options LazyReplicatedChildExecutorOptions,
) (*LazyReplicatedChildExecutor, error) {
	if options.Plan == nil || options.Lease == nil || options.Registrar == nil ||
		options.StaticBootstrap == nil || options.Data == nil || options.Opener == nil ||
		options.ReadDeadline == nil || options.WriteDeadline == nil || options.ChunkBytes == 0 ||
		len(options.Workspace) < int(options.ChunkBytes) || options.MaxReconnects < 0 {
		return nil, ErrRuntimeStore
	}
	target, ok := options.Plan.Target(options.Child)
	if !ok || !targetMatchesPreparedReplica(target, options.Replica) ||
		options.Replica.WALPath == "" || options.Replica.SQLPath == "" ||
		options.PlanDigest == ([32]byte{}) ||
		options.Replica.Apply.Placement.Range != options.Plan.children[options.Child].Range {
		return nil, ErrRuntimeStore
	}
	if err := replicatedstate.ValidateSnapshotArtifactOptions(options.ArtifactOptions); err != nil {
		return nil, err
	}
	options.Workspace = options.Workspace[:options.ChunkBytes]
	return &LazyReplicatedChildExecutor{options: options}, nil
}

func (executor *LazyReplicatedChildExecutor) ExecuteSplitAction(
	context.Context, *Plan, Observation, Action,
) error {
	return ErrRemoteExecution
}

func (executor *LazyReplicatedChildExecutor) ExecuteAuthorizedSplitAction(
	ctx context.Context, plan *Plan, observed Observation, action Action,
) error {
	if executor == nil || ctx == nil || plan != executor.options.Plan ||
		action.Child != executor.options.Child {
		return ErrRemoteExecution
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if err := executor.open(ctx, observed); err != nil {
		return err
	}
	switch action.Kind {
	case ActionStageChild:
		return executor.stage.ExecuteRemoteChildStage(ctx, plan, observed, action.Child)
	case ActionActivateChild:
		if observed.Certificate == nil {
			return ErrTopologyConflict
		}
		return executor.lifecycle.ExecuteActivateChild(plan, *observed.Certificate)
	case ActionCreateChildWAL:
		if observed.Certificate == nil {
			return ErrTopologyConflict
		}
		return executor.lifecycle.ExecuteCreateChildWAL(plan, *observed.Certificate)
	case ActionAdoptChildRuntime:
		return executor.lifecycle.ExecuteAdoptChildRuntime(ctx, plan)
	default:
		return ErrRemoteExecution
	}
}

func (executor *LazyReplicatedChildExecutor) ObserveLocalSplitChild(
	_ context.Context, request PlanObservationRequest, member uint64,
) (*ChildObservation, error) {
	if executor == nil || member != executor.options.Replica.Member || request.Child != executor.options.Child {
		return nil, ErrPlanObservation
	}
	target, ok := executor.options.Plan.Target(request.Child)
	if !ok || request.Group != groupFromChildTarget(target) {
		return nil, ErrPlanObservation
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.lifecycle == nil {
		return nil, nil
	}
	return executor.lifecycle.ObserveChild(request.Child)
}

func (executor *LazyReplicatedChildExecutor) open(
	ctx context.Context, observed Observation,
) error {
	if executor.stage != nil && executor.lifecycle != nil {
		return nil
	}
	if observed.Artifacts == nil || observed.SourceNode == (rafttransport.NodeID{}) ||
		executor.options.Plan.partitioner.ValidateChildArtifactSet(*observed.Artifacts) != nil {
		return ErrTopologyConflict
	}
	artifacts := *observed.Artifacts
	rawArtifacts, err := rangesplit.AppendChildArtifactSet(nil, artifacts)
	if err != nil {
		return err
	}
	storedArtifacts, present, err := executor.options.Lease.Load(RuntimeStateArtifacts, 0)
	if err != nil {
		return err
	}
	if present {
		stored, openErr := rangesplit.OpenChildArtifactSet(storedArtifacts.Payload)
		if openErr != nil || stored != artifacts {
			return errors.Join(ErrTopologyConflict, openErr)
		}
	} else if err = executor.options.Lease.Persist(
		RuntimeStateArtifacts, 0, 1, rawArtifacts,
	); err != nil {
		return err
	}
	stageState, hasStage, err := executor.options.Lease.Load(RuntimeStateStage, executor.options.Child)
	if err != nil {
		return err
	}
	var cursor []byte
	if hasStage {
		cursor = stageState.Payload
	}
	database, err := sqldriver.OpenReplicatedShardStore(
		executor.options.Replica.SQLPath, executor.options.Replica.SQL,
	)
	identity := executor.options.Replica.Apply
	if err == nil {
		reserved, present, reservationErr := database.ReplicatedChildApplyReservation(
			executor.options.Replica.SQL,
		)
		if reservationErr != nil || !present || reserved != identity {
			_ = database.Close()
			return errors.Join(ErrRuntimeStore, reservationErr)
		}
	} else {
		database, identity, err = sqldriver.OpenReplicatedShardStoreForChildStageResume(
			executor.options.Replica.SQLPath, executor.options.Replica.SQL,
			replicatedApplyOptions(executor.options.Replica.Apply),
		)
	}
	if err != nil || identity != executor.options.Replica.Apply {
		if database != nil {
			_ = database.Close()
		}
		return errors.Join(ErrRuntimeStore, err)
	}
	stage, err := database.OpenReplicatedChildStage(
		executor.options.Replica.SQL, executor.options.Plan.partitioner,
		artifacts.Children[executor.options.Child], cursor,
		replicatedApplyOptions(executor.options.Replica.Apply),
		rangesplit.ChildStageOptions{CheckpointBytes: executor.options.CheckpointBytes},
	)
	if err != nil {
		return errors.Join(err, database.Close())
	}
	stageActions, err := NewReplicatedChildStageActions(ReplicatedChildStageActionsOptions{
		Plan: executor.options.Plan, Artifacts: artifacts, Child: executor.options.Child,
		Lease: executor.options.Lease, Stage: stage, Revision: stageState.Revision,
		Opener: executor.options.Opener, SourceNode: observed.SourceNode,
		ReadDeadline: executor.options.ReadDeadline, WriteDeadline: executor.options.WriteDeadline,
		ChunkBytes: executor.options.ChunkBytes, MaxReconnects: executor.options.MaxReconnects,
		Workspace: executor.options.Workspace,
	})
	if err != nil {
		return errors.Join(err, stage.Close(), database.Close())
	}
	target, _ := executor.options.Plan.Target(executor.options.Child)
	localTarget, err := LocalReplicaChildTarget(target, executor.options.Replica)
	if err != nil {
		return errors.Join(err, stage.Close(), database.Close())
	}
	roster := make([]rafttransport.Member, len(target.Replicas))
	for index, replica := range target.Replicas {
		roster[index] = rafttransport.Member{
			Group: groupFromChildTarget(target), MemberID: replica.Member, Node: replica.Node,
			Role: rafttransport.MemberVoter, ReplicaSetVersion: target.ReplicaSetVersion,
		}
	}
	adopter, err := NewPreboundChildRuntimeAdopter(
		executor.options.Registrar, executor.options.Plan.OperationID(), executor.options.Child,
		localTarget, roster,
	)
	if err != nil {
		return errors.Join(err, stage.Close(), database.Close())
	}
	lifecycle, err := NewLocalChildLifecycle(LocalChildLifecycleOptions{
		Child: executor.options.Child, Replica: &executor.options.Replica,
		Stage: stage, Database: database, StaticBootstrap: executor.options.StaticBootstrap,
		ArtifactOptions: executor.options.ArtifactOptions,
		WALPath:         executor.options.Replica.WALPath, WALIdentity: executor.options.Replica.WAL,
		WALKey: executor.options.WALKey, WALOptions: executor.options.WALOptions,
		TopologyRecoveryEpoch: target.TopologyRecoveryEpoch, Authority: target.Authority,
		SQL: executor.options.Replica.SQL, Adopter: adopter,
	})
	if err != nil {
		return errors.Join(err, stage.Close(), database.Close())
	}
	if err = executor.options.Data.InstallChildTargetWithCleanup(
		executor.options.Plan, executor.options.PlanDigest, executor.options.Child,
		executor.options.Lease, stageActions, executor.Close,
	); err != nil {
		return errors.Join(err, stage.Close(), database.Close())
	}
	executor.stage, executor.lifecycle = stageActions, lifecycle
	return nil
}

func replicatedApplyOptions(identity sqldriver.ReplicatedApplyIdentity) sqldriver.ReplicatedApplyOptions {
	return sqldriver.ReplicatedApplyOptions{
		MaxSessions: identity.MaxSessions, RetryWindow: identity.RetryWindow,
		TxnLimits: identity.TxnLimits, Placement: identity.Placement,
		RequestLedgerCapacityBytes:       identity.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: identity.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart:          identity.RequestLedgerRangeStart,
		RequestLedgerRangeEnd:            identity.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity:       identity.RequestLedgerRangeIdentity,
	}
}

func groupFromChildTarget(target ChildTarget) raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
		TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
		ShardIncarnation:      target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID,
	}
}

var _ AuthorizedShardActionExecutor = (*LazyReplicatedChildExecutor)(nil)
var _ ShardActionExecutor = (*LazyReplicatedChildExecutor)(nil)
var _ LocalChildRuntimeObserver = (*LazyReplicatedChildExecutor)(nil)
