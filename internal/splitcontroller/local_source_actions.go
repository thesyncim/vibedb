package splitcontroller

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
)

// LocalSourceActions executes only the source-local, pre-publication portion
// of a split. The reconciler must separately prove source leadership before
// ExecuteStartCapture. This type never seals ownership or publishes a route.
type LocalSourceActions struct {
	mu sync.Mutex

	store   *DurableRuntimeStore
	machine *replicatedstate.Machine
	capture *durable.Collection
	active  *rangesplit.SourceCapture
}

func NewLocalSourceActions(
	store *DurableRuntimeStore,
	machine *replicatedstate.Machine,
	capture *durable.Collection,
) (*LocalSourceActions, error) {
	if store == nil || machine == nil || capture == nil || !capture.HasOpaqueValues() {
		return nil, ErrRuntimeStore
	}
	return &LocalSourceActions{store: store, machine: machine, capture: capture}, nil
}

// ExecuteStartCapture creates or recovers the exact source capture and binds
// it into replicated apply. A lagging descriptor is advanced only after an
// O(1) chain-membership proof against the durable capture collection.
func (a *LocalSourceActions) ExecuteStartCapture(plan *Plan) (*rangesplit.SourceCapture, error) {
	if a == nil || plan == nil || plan.operation != a.store.operation {
		return nil, ErrInvalidPlan
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	stored, revision, hasStored, err := a.store.LoadSourceCaptureDescriptor(plan.partitioner)
	if err != nil {
		return nil, err
	}
	if a.active == nil {
		if hasStored && a.capture.Len() == 0 {
			// A durable descriptor may recover only its pre-existing capture
			// participant. Never manufacture a fresh header beneath authority
			// copied from another member root.
			return nil, ErrRuntimeStore
		}
		capture, captureErr := rangesplit.NewSourceCapture(
			plan.partitioner, replicatedstate.TransitionCaptureCollectionName, a.capture,
		)
		if captureErr != nil {
			return nil, errors.Join(ErrRuntimeStore, captureErr)
		}
		if captureErr = a.machine.BeginTransitionCapture(capture); captureErr != nil {
			return nil, errors.Join(ErrRuntimeStore, captureErr)
		}
		a.active = capture
	}
	current, err := a.active.Descriptor()
	if err != nil {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	if hasStored {
		var workspace rangesplit.SourceCaptureWorkspace
		if err = a.active.ValidateDescriptorAncestor(stored, &workspace); err != nil {
			return nil, errors.Join(ErrRuntimeStore, err)
		}
		if current == stored {
			return a.active, nil
		}
		if revision == ^uint64(0) {
			return nil, ErrRuntimeStore
		}
		revision++
	} else {
		revision = 1
	}
	if err = a.store.PersistSourceCapture(revision, a.active); err != nil {
		return nil, err
	}
	return a.active, nil
}

// ExecuteBuildArtifacts writes every non-retained child in one source scan.
// Payload memory is bounded by the configured chunk size per child. Files are
// fsynced and atomically renamed before the bounded manifest authority is
// published. Existing authority is always verified instead of overwritten.
func (a *LocalSourceActions) ExecuteBuildArtifacts(
	plan *Plan,
	capture *rangesplit.SourceCapture,
	targetChunkBytes int,
) (rangesplit.ChildArtifactSet, error) {
	if a == nil || plan == nil || capture == nil || plan.operation != a.store.operation {
		return rangesplit.ChildArtifactSet{}, ErrInvalidPlan
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active != capture {
		return rangesplit.ChildArtifactSet{}, ErrTopologyConflict
	}
	if existing, _, ok, err := a.store.LoadChildArtifacts(plan.partitioner); err != nil {
		return rangesplit.ChildArtifactSet{}, err
	} else if ok {
		if err = a.verifyArtifactSet(plan, existing); err != nil {
			return rangesplit.ChildArtifactSet{}, err
		}
		return existing, nil
	}
	target, err := localArtifactChunkBytes(targetChunkBytes)
	if err != nil {
		return rangesplit.ChildArtifactSet{}, err
	}
	cut, err := a.machine.Snapshot()
	if err != nil {
		return rangesplit.ChildArtifactSet{}, err
	}
	if capture.Head() != cut.State().Applied {
		_ = cut.Close()
		return rangesplit.ChildArtifactSet{}, ErrTopologyConflict
	}

	var files [autosplit.MaxSplitChildren]*os.File
	var temporary [autosplit.MaxSplitChildren]string
	options := rangesplit.ChildArtifactOptions{TargetChunkBytes: target}
	cleanup := func(final bool) {
		for child := 0; child < int(plan.childCount); child++ {
			if files[child] != nil {
				_ = files[child].Close()
			}
			if temporary[child] != "" {
				_ = a.store.operationRoot.Remove(temporary[child])
			}
			if final && child != int(plan.retained) {
				_ = a.store.operationRoot.Remove(localArtifactName(uint8(child)))
			}
		}
	}
	for child := 0; child < int(plan.childCount); child++ {
		if child == int(plan.retained) {
			continue
		}
		finalName := localArtifactName(uint8(child))
		temporary[child] = finalName + ".building"
		if removeErr := a.store.operationRoot.Remove(temporary[child]); removeErr != nil &&
			!errors.Is(removeErr, os.ErrNotExist) {
			cleanup(false)
			_ = cut.Close()
			return rangesplit.ChildArtifactSet{}, removeErr
		}
		if removeErr := a.store.operationRoot.Remove(finalName); removeErr != nil &&
			!errors.Is(removeErr, os.ErrNotExist) {
			cleanup(false)
			_ = cut.Close()
			return rangesplit.ChildArtifactSet{}, removeErr
		}
		file, openErr := openRuntimeRegular(
			a.store.operationRoot, temporary[child], os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if openErr != nil {
			cleanup(true)
			_ = cut.Close()
			return rangesplit.ChildArtifactSet{}, openErr
		}
		files[child] = file
		options.Writers[child] = file
		options.PayloadBuffers[child] = make([]byte, 0, target)
	}
	var workspace rangesplit.ChildArtifactWorkspace
	set, buildErr := plan.partitioner.WriteChildArtifacts(cut, options, &workspace)
	closeCutErr := cut.Close()
	if buildErr != nil || closeCutErr != nil {
		cleanup(true)
		return rangesplit.ChildArtifactSet{}, errors.Join(buildErr, closeCutErr)
	}
	for child := 0; child < int(plan.childCount); child++ {
		if child == int(plan.retained) {
			continue
		}
		file := files[child]
		err = errors.Join(file.Sync(), file.Chmod(0o400), file.Close())
		files[child] = nil
		if err != nil {
			cleanup(true)
			return rangesplit.ChildArtifactSet{}, err
		}
		if err = a.store.operationRoot.Rename(
			temporary[child], localArtifactName(uint8(child)),
		); err != nil {
			cleanup(true)
			return rangesplit.ChildArtifactSet{}, err
		}
		temporary[child] = ""
	}
	if err = syncRuntimeRoot(a.store.operationRoot); err != nil {
		return rangesplit.ChildArtifactSet{}, errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	if err = a.store.PersistChildArtifacts(1, set); err != nil {
		return rangesplit.ChildArtifactSet{}, err
	}
	return set, nil
}

// OpenChildArtifact returns the immutable exact child stream after a complete
// verification pass. The returned descriptor is positioned at byte zero.
func (a *LocalSourceActions) OpenChildArtifact(
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
) (*os.File, error) {
	if a == nil || plan == nil || plan.operation != a.store.operation ||
		int(child) >= int(plan.childCount) || child == plan.retained ||
		plan.partitioner.ValidateChildArtifactSet(set) != nil {
		return nil, ErrInvalidPlan
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	stored, _, ok, err := a.store.LoadChildArtifacts(plan.partitioner)
	if err != nil || !ok || stored != set {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	file, err := a.openVerifiedArtifact(plan, stored, child)
	if err != nil {
		return nil, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (a *LocalSourceActions) verifyArtifactSet(plan *Plan, set rangesplit.ChildArtifactSet) error {
	for child := uint8(0); child < plan.childCount; child++ {
		if child == plan.retained {
			continue
		}
		file, err := a.openVerifiedArtifact(plan, set, child)
		if err != nil {
			return err
		}
		if err = file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (a *LocalSourceActions) openVerifiedArtifact(
	plan *Plan,
	set rangesplit.ChildArtifactSet,
	child uint8,
) (*os.File, error) {
	file, err := openRuntimeRegular(a.store.operationRoot, localArtifactName(child), os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	info, err := file.Stat()
	if err != nil || uint64(info.Size()) != set.Children[child].EncodedBytes {
		_ = file.Close()
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	var workspace rangesplit.ChildArtifactVerifyWorkspace
	opened, err := plan.partitioner.VerifyChildArtifact(
		file, child, rangesplit.ChildArtifactCallbacks{}, &workspace,
	)
	if err != nil || opened != set.Children[child] {
		_ = file.Close()
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	return file, nil
}

func localArtifactChunkBytes(target int) (int, error) {
	if target == 0 {
		return rangesplit.DefaultChildArtifactChunkBytes, nil
	}
	if target < rangesplit.MinChildArtifactChunkBytes || target > rangesplit.MaxChildArtifactChunkBytes {
		return 0, ErrRuntimeStore
	}
	return target, nil
}

func localArtifactName(child uint8) string {
	return fmt.Sprintf("child-%02d.artifact", child)
}
