package splitcontroller

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/splitartifact"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type ReplicatedChildStageActionsOptions struct {
	Plan      *Plan
	Artifacts rangesplit.ChildArtifactSet
	Child     uint8
	Lease     *RuntimeStoreLease
	Stage     *sqldriver.ReplicatedChildStage
	Revision  uint64

	Opener        rafttransport.SnapshotStreamOpener
	SourceNode    rafttransport.NodeID
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	ChunkBytes    uint32
	MaxReconnects int
	Workspace     []byte
}

// ReplicatedChildStageActions adapts the exclusive SQL child-stage owner
// directly to artifact and tail transports. It avoids opening the same user
// collection through a second durable handle while preserving the existing
// typed runtime cursor journal.
type ReplicatedChildStageActions struct {
	mu sync.Mutex

	options  ReplicatedChildStageActionsOptions
	revision uint64
}

func NewReplicatedChildStageActions(
	options ReplicatedChildStageActionsOptions,
) (*ReplicatedChildStageActions, error) {
	if options.Plan == nil || options.Lease == nil || options.Stage == nil ||
		!options.Plan.validNonRetainedChild(options.Child) ||
		options.Plan.partitioner.ValidateChildArtifactSet(options.Artifacts) != nil ||
		options.Opener == nil || options.SourceNode == (rafttransport.NodeID{}) ||
		options.ReadDeadline == nil || options.WriteDeadline == nil || options.ChunkBytes == 0 ||
		options.MaxReconnects < 0 || len(options.Workspace) < int(options.ChunkBytes) {
		return nil, ErrRuntimeStore
	}
	options.Workspace = options.Workspace[:options.ChunkBytes]
	return &ReplicatedChildStageActions{options: options, revision: options.Revision}, nil
}

func (actions *ReplicatedChildStageActions) ExecuteRemoteChildStage(
	ctx context.Context, plan *Plan, observed Observation, child uint8,
) error {
	if actions == nil || ctx == nil || plan != actions.options.Plan || child != actions.options.Child ||
		observed.Artifacts == nil || *observed.Artifacts != actions.options.Artifacts {
		return ErrRemoteExecution
	}
	manifest := actions.options.Artifacts.Children[child]
	identity, err := splitartifact.NewIdentity([32]byte(plan.OperationID()), manifest)
	if err != nil {
		return err
	}
	stream, err := splitartifact.OpenStream(ctx, splitartifact.StreamOptions{
		Opener: actions.options.Opener, SourceNode: actions.options.SourceNode, Identity: identity,
		ReadDeadline: actions.options.ReadDeadline, WriteDeadline: actions.options.WriteDeadline,
		ChunkBytes: actions.options.ChunkBytes, MaxReconnects: actions.options.MaxReconnects,
		Workspace: actions.options.Workspace,
	})
	if err != nil {
		return err
	}
	defer stream.Close()
	actions.mu.Lock()
	defer actions.mu.Unlock()
	_, err = actions.options.Stage.ReceiveArtifact(stream, actions.persist)
	if err != nil {
		return err
	}
	cursor, ok := actions.options.Stage.Cursor()
	if !ok || cursor.Phase() != rangesplit.ChildStageTail {
		return ErrRuntimeStore
	}
	return nil
}

func (actions *ReplicatedChildStageActions) ObserveTail(
	ctx context.Context,
) (rangesplit.ChildStageCursor, bool, error) {
	if actions == nil || ctx == nil {
		return rangesplit.ChildStageCursor{}, false, ErrTailStreamControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return rangesplit.ChildStageCursor{}, false, cause
	}
	actions.mu.Lock()
	defer actions.mu.Unlock()
	cursor, ok := actions.options.Stage.Cursor()
	return cursor, ok, nil
}

func (actions *ReplicatedChildStageActions) ApplyTail(
	ctx context.Context, batch rangesplit.TailBatch,
) error {
	if actions == nil || ctx == nil || batch.Child != actions.options.Child {
		return ErrTailStreamControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	actions.mu.Lock()
	defer actions.mu.Unlock()
	return actions.options.Stage.ApplyTailBatch(batch, actions.persist)
}

func (actions *ReplicatedChildStageActions) persist(raw []byte) error {
	cursor, err := rangesplit.OpenChildStageCursor(raw)
	if err != nil || cursor == nil || cursor.Child() != actions.options.Child ||
		actions.options.Plan.partitioner.ValidateChildStageCursor(
			actions.options.Artifacts.Children[actions.options.Child], *cursor,
		) != nil || actions.revision == ^uint64(0) {
		return errors.Join(ErrRuntimeStore, err)
	}
	next := actions.revision + 1
	if err = actions.options.Lease.Persist(RuntimeStateStage, actions.options.Child, next, raw); err != nil {
		return err
	}
	actions.revision = next
	return nil
}

var _ RemoteChildStageExecutor = (*ReplicatedChildStageActions)(nil)
var _ TailStreamApplyTarget = (*ReplicatedChildStageActions)(nil)
