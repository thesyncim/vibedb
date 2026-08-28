package splitcontroller

import (
	"errors"
	"io"
	"sync"

	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/store/durable"
)

// LocalChildActions owns one non-serving child collection and its typed,
// manifest-bound progress record. It can stage and catch up data, but cannot
// activate the collection, create a WAL, adopt a Raft runtime, or publish a
// route.
type LocalChildActions struct {
	mu sync.Mutex

	store           *DurableRuntimeStore
	collection      *durable.Collection
	checkpointBytes uint64
}

func NewLocalChildActions(
	store *DurableRuntimeStore,
	collection *durable.Collection,
	checkpointBytes uint64,
) (*LocalChildActions, error) {
	if store == nil || collection == nil || !collection.SupportsUpdate() ||
		!collection.HasSynchronousDurability() || collection.HasOpaqueValues() {
		return nil, ErrRuntimeStore
	}
	if checkpointBytes != 0 && checkpointBytes < rangesplit.MaxChildArtifactChunkBytes {
		return nil, ErrRuntimeStore
	}
	return &LocalChildActions{
		store: store, collection: collection, checkpointBytes: checkpointBytes,
	}, nil
}

// ExecuteStageChild verifies and durably installs one immutable artifact. The
// reader must start at byte zero even after a crash; ChildStage re-verifies an
// already checkpointed prefix before skipping its callbacks. At most
// checkpointBytes plus one admitted chunk can require replay.
func (a *LocalChildActions) ExecuteStageChild(
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
	artifact io.Reader,
) (rangesplit.ChildStageCursor, error) {
	if artifact == nil {
		return rangesplit.ChildStageCursor{}, ErrRuntimeStore
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	stage, revision, err := a.openStage(plan, set, child)
	if err != nil {
		return rangesplit.ChildStageCursor{}, err
	}
	persist := a.stagePersistence(plan, set.Children[child], child, &revision)
	if _, err = stage.ReceiveArtifact(artifact, persist); err != nil {
		return rangesplit.ChildStageCursor{}, err
	}
	cursor, ok := stage.Cursor()
	if !ok || cursor.Phase() != rangesplit.ChildStageTail {
		return rangesplit.ChildStageCursor{}, ErrRuntimeStore
	}
	return cursor, nil
}

// ApplyTailBatch applies one already translated source publication. The
// callback is synchronous: TailBatch borrows its transition buffers and must
// not escape this call.
func (a *LocalChildActions) ApplyTailBatch(
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
	batch rangesplit.TailBatch,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	stage, revision, err := a.openStage(plan, set, child)
	if err != nil {
		return err
	}
	return stage.ApplyTailBatch(
		batch, a.stagePersistence(plan, set.Children[child], child, &revision),
	)
}

// Observe returns the exact typed durable cursor without opening or scanning
// the child collection.
func (a *LocalChildActions) Observe(
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
) (rangesplit.ChildStageCursor, bool, error) {
	if a == nil || plan == nil || plan.operation != a.store.operation ||
		!plan.validNonRetainedChild(child) ||
		plan.partitioner.ValidateChildArtifactSet(set) != nil {
		return rangesplit.ChildStageCursor{}, false, ErrInvalidPlan
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	cursor, _, ok, err := a.store.LoadChildStage(
		plan.partitioner, set.Children[child], child,
	)
	return cursor, ok, err
}

func (a *LocalChildActions) openStage(
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
) (*rangesplit.ChildStage, uint64, error) {
	if a == nil || plan == nil || plan.operation != a.store.operation ||
		!plan.validNonRetainedChild(child) ||
		plan.partitioner.ValidateChildArtifactSet(set) != nil {
		return nil, 0, ErrInvalidPlan
	}
	stored, _, ok, err := a.store.LoadChildArtifacts(plan.partitioner)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		if err = a.store.PersistChildArtifacts(1, set); err != nil {
			return nil, 0, err
		}
		stored = set
	}
	if stored != set {
		return nil, 0, ErrTopologyConflict
	}
	var raw []byte
	revision := uint64(0)
	cursor, cursorRevision, hasCursor, err := a.store.LoadChildStage(
		plan.partitioner, set.Children[child], child,
	)
	if err != nil {
		return nil, 0, err
	}
	if hasCursor {
		revision = cursorRevision
		raw, err = rangesplit.AppendChildStageCursor(nil, &cursor)
		if err != nil {
			return nil, 0, errors.Join(ErrRuntimeStore, err)
		}
	}
	stage, err := rangesplit.NewChildStageWithOptions(
		plan.partitioner, set.Children[child], a.collection, raw,
		rangesplit.ChildStageOptions{CheckpointBytes: a.checkpointBytes},
	)
	if err != nil {
		return nil, 0, errors.Join(ErrRuntimeStore, err)
	}
	return stage, revision, nil
}

func (a *LocalChildActions) stagePersistence(
	plan *Plan,
	expected rangesplit.ChildArtifactManifest,
	child uint8,
	revision *uint64,
) rangesplit.ChildStageCursorPersistence {
	return func(raw []byte) error {
		cursor, err := rangesplit.OpenChildStageCursor(raw)
		if err != nil || cursor == nil ||
			plan.partitioner.ValidateChildStageCursor(expected, *cursor) != nil ||
			*revision == ^uint64(0) {
			return errors.Join(ErrRuntimeStore, err)
		}
		next := *revision + 1
		if err = a.store.PersistChildStage(child, next, *cursor); err != nil {
			return err
		}
		*revision = next
		return nil
	}
}

func (p *Plan) validNonRetainedChild(child uint8) bool {
	return p != nil && child < p.childCount && child != p.retained &&
		!p.children[child].Retained
}
