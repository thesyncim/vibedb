package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/splitartifact"
)

// SplitArtifactSource exposes only one immutable plan/set pair through the
// splitartifact service. OpenChildArtifact performs the full local semantic
// verification before any remote byte becomes readable.
type SplitArtifactSource struct {
	Actions *LocalSourceActions
	Plan    *Plan
	Set     rangesplit.ChildArtifactSet
}

func (source SplitArtifactSource) OpenSplitArtifact(
	ctx context.Context,
	identity splitartifact.Identity,
) (splitartifact.Artifact, error) {
	if source.Actions == nil || source.Plan == nil || ctx == nil {
		return nil, ErrInvalidPlan
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	child := identity.Child
	if int(child) >= len(source.Set.Children) {
		return nil, ErrInvalidPlan
	}
	want, err := splitartifact.NewIdentity(
		[32]byte(source.Plan.OperationID()), source.Set.Children[child],
	)
	if err != nil || want != identity {
		return nil, errors.Join(ErrTopologyConflict, err)
	}
	return source.Actions.OpenChildArtifact(source.Plan, source.Set, child)
}

// ExecuteStageChildRemote connects the shipped source data plane directly to
// the existing durable child stage. Transport retries resume at a verified
// chunk boundary; stage retries still begin at byte zero so ChildStage can
// re-verify its durable semantic checkpoint prefix.
func (a *LocalChildActions) ExecuteStageChildRemote(
	ctx context.Context,
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
	opener rafttransport.SnapshotStreamOpener,
	sourceNode rafttransport.NodeID,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
	chunkBytes uint32,
	maxReconnects int,
	workspace []byte,
) (rangesplit.ChildStageCursor, error) {
	if a == nil || plan == nil || int(child) >= len(set.Children) {
		return rangesplit.ChildStageCursor{}, ErrInvalidPlan
	}
	identity, err := splitartifact.NewIdentity(
		[32]byte(plan.OperationID()), set.Children[child],
	)
	if err != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrInvalidPlan, err)
	}
	stream, err := splitartifact.OpenStream(ctx, splitartifact.StreamOptions{
		Opener: opener, SourceNode: sourceNode, Identity: identity,
		ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
		ChunkBytes: chunkBytes, MaxReconnects: maxReconnects, Workspace: workspace,
	})
	if err != nil {
		return rangesplit.ChildStageCursor{}, err
	}
	defer stream.Close()
	return a.ExecuteStageChild(plan, set, child, stream)
}

var _ splitartifact.Source = SplitArtifactSource{}
