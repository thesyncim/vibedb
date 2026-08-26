package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type RemoteChildStageExecutor interface {
	ExecuteRemoteChildStage(context.Context, *Plan, Observation, uint8) error
}

type SplitTailSinkResolver interface {
	ResolveSplitTailSinks(context.Context, *Plan, Observation) ([]rangesplit.TailSink, error)
}

type CompositeShardActionExecutorOptions struct {
	Operation OperationID
	Actions   uint16

	Source      *LocalSourceActions
	Stage       RemoteChildStageExecutor
	TailSinks   SplitTailSinkResolver
	Seal        SourceSealProposer
	PruneSource RetainedPruneAuthoritySource
	Prune       RetainedPruneProposer
	PruneLimits rangesplit.RetainedPruneLimits

	Child     uint8
	Lifecycle *LocalChildLifecycle

	ArtifactChunkBytes int
}

// CompositeShardActionExecutor is one admitted operation's concrete data-plane
// dispatcher. It contains only manifest-owned local handles. Catalog publish,
// drain, waits, and completion are rejected because those remain gateway-local.
type CompositeShardActionExecutor struct {
	options CompositeShardActionExecutorOptions
}

func NewCompositeShardActionExecutor(
	options CompositeShardActionExecutorOptions,
) (*CompositeShardActionExecutor, error) {
	if options.Operation == (OperationID{}) || options.Actions == 0 ||
		options.Actions&^uint16((1<<uint(ActionComplete))-1) != 0 ||
		options.Actions&gatewayOnlySplitActionMask() != 0 {
		return nil, ErrRemoteExecution
	}
	if options.Actions&sourceSplitActionMask() != 0 {
		if options.Source == nil {
			return nil, ErrRemoteExecution
		}
		if options.Actions&actionBit(ActionBuildArtifacts) != 0 &&
			options.ArtifactChunkBytes <= 0 {
			return nil, ErrRemoteExecution
		}
		if options.Actions&actionBit(ActionCatchUpTail) != 0 && options.TailSinks == nil {
			return nil, ErrRemoteExecution
		}
		if options.Actions&actionBit(ActionSealSource) != 0 && options.Seal == nil {
			return nil, ErrRemoteExecution
		}
		if options.Actions&actionBit(ActionPruneRetained) != 0 &&
			(options.PruneSource == nil || options.Prune == nil) {
			return nil, ErrRemoteExecution
		}
	}
	if options.Actions&childSplitActionMask() != 0 {
		if options.Lifecycle == nil &&
			options.Actions&^actionBit(ActionStageChild) != 0 ||
			options.Actions&actionBit(ActionStageChild) != 0 && options.Stage == nil {
			return nil, ErrRemoteExecution
		}
	}
	return &CompositeShardActionExecutor{options: options}, nil
}

func (executor *CompositeShardActionExecutor) ExecuteSplitAction(
	ctx context.Context, plan *Plan, observed Observation, action Action,
) error {
	if executor == nil || ctx == nil || plan == nil ||
		plan.OperationID() != executor.options.Operation ||
		executor.options.Actions&actionBit(action.Kind) == 0 {
		return ErrRemoteExecution
	}
	want, err := Reconcile(plan, observed)
	if err != nil || want != action {
		return errors.Join(ErrRemoteExecution, err)
	}
	options := executor.options
	switch action.Kind {
	case ActionStartCapture:
		_, err = options.Source.ExecuteStartCapture(plan)
		return err
	case ActionBuildArtifacts:
		capture, openErr := options.Source.ExecuteStartCapture(plan)
		if openErr != nil {
			return openErr
		}
		_, err = options.Source.ExecuteBuildArtifacts(plan, capture, options.ArtifactChunkBytes)
		return err
	case ActionStageChild:
		if action.Child != options.Child {
			return ErrRemoteExecution
		}
		return options.Stage.ExecuteRemoteChildStage(ctx, plan, observed, action.Child)
	case ActionCatchUpTail:
		if observed.Artifacts == nil {
			return ErrTopologyConflict
		}
		capture, openErr := options.Source.ExecuteStartCapture(plan)
		if openErr != nil {
			return openErr
		}
		sinks, sinkErr := options.TailSinks.ResolveSplitTailSinks(ctx, plan, observed)
		if sinkErr != nil {
			return sinkErr
		}
		_, _, err = options.Source.ExecuteCatchUpTail(plan, capture, *observed.Artifacts, sinks)
		return err
	case ActionSealSource:
		if observed.Tail == nil {
			return ErrTopologyConflict
		}
		return options.Source.ExecuteSealSource(
			ctx, plan, observed.SourceState, *observed.Tail, observed.SourceServing, options.Seal,
		)
	case ActionCertifyCutover:
		if observed.Tail == nil {
			return ErrTopologyConflict
		}
		capture, openErr := options.Source.ExecuteStartCapture(plan)
		if openErr != nil {
			return openErr
		}
		stages, stageErr := orderedSplitStages(plan, observed)
		if stageErr != nil {
			return stageErr
		}
		_, err = options.Source.ExecuteCertifyCutover(plan, capture, *observed.Tail, stages)
		return err
	case ActionActivateChild:
		if action.Child != options.Child || observed.Certificate == nil {
			return ErrTopologyConflict
		}
		return options.Lifecycle.ExecuteActivateChild(plan, *observed.Certificate)
	case ActionCreateChildWAL:
		if action.Child != options.Child || observed.Certificate == nil {
			return ErrTopologyConflict
		}
		return options.Lifecycle.ExecuteCreateChildWAL(plan, *observed.Certificate)
	case ActionAdoptChildRuntime:
		if action.Child != options.Child {
			return ErrTopologyConflict
		}
		return options.Lifecycle.ExecuteAdoptChildRuntime(ctx, plan)
	case ActionPruneRetained:
		return options.Source.ExecutePruneRetained(
			ctx, plan, observed, observed.SourceServing, options.PruneSource,
			options.Prune, options.PruneLimits,
		)
	default:
		return ErrRemoteExecution
	}
}

func orderedSplitStages(plan *Plan, observed Observation) ([]rangesplit.ChildStageCursor, error) {
	stages := make([]rangesplit.ChildStageCursor, 0, int(plan.childCount)-1)
	for child := uint8(0); child < plan.childCount; child++ {
		if child == plan.retained {
			continue
		}
		if observed.Stages[child] == nil {
			return nil, ErrTopologyConflict
		}
		stages = append(stages, *observed.Stages[child])
	}
	return stages, nil
}

func actionBit(kind ActionKind) uint16 {
	if kind < ActionAwaitSourceLeader || kind > ActionComplete {
		return 0
	}
	return 1 << uint(kind-1)
}

func sourceSplitActionMask() uint16 {
	return actionBit(ActionStartCapture) | actionBit(ActionBuildArtifacts) |
		actionBit(ActionCatchUpTail) | actionBit(ActionSealSource) |
		actionBit(ActionCertifyCutover) | actionBit(ActionPruneRetained)
}

func childSplitActionMask() uint16 {
	return actionBit(ActionStageChild) | actionBit(ActionActivateChild) |
		actionBit(ActionCreateChildWAL) | actionBit(ActionAdoptChildRuntime)
}

func gatewayOnlySplitActionMask() uint16 {
	return actionBit(ActionAwaitSourceLeader) | actionBit(ActionAwaitChildReady) |
		actionBit(ActionPublishCatalog) | actionBit(ActionAwaitCatalogDrain) |
		actionBit(ActionComplete)
}
