package snapshottransfer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// SourceArtifactCut exposes only the coherent replicated-state cut needed by
// source export. sql/driver.ReplicatedApply implements this interface. Keeping
// the surface narrow prevents the snapshot control path from acquiring SQL or
// Raft mutation authority.
type SourceArtifactCut interface {
	SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error)
}

// RetainedSourceExportOptions is the immutable local provisioning contract for
// one enrolled replacement. RepositoryPath must be an absolute descendant of
// DataRoot. Both paths must be canonical and may not traverse symlinks.
type RetainedSourceExportOptions struct {
	DataRoot       string
	RepositoryPath string
	Limits         Limits
	ChunkBytes     uint32
	MaxConcurrent  int
	// Budget is process-scoped. Every group provider on one physical node must
	// receive the same pointer; nil keeps the package usable by offline tools.
	Budget *migrationbudget.Budget

	RuntimeIdentity   raftmember.RuntimeIdentity
	SourceNode        rafttransport.NodeID
	TargetMember      uint64
	TargetStore       [16]byte
	TargetIncarnation uint64
	Cut               SourceArtifactCut
}

type sourceExportWorkspace struct {
	artifact []byte
	transfer []byte
}

// RetainedSourceExportProvider binds source export to one retained local RF3
// member and one enrolled target. It owns the bounded artifact repository and
// a fixed workspace pool; the hot export path does not allocate chunk buffers.
type RetainedSourceExportProvider struct {
	repository *Repository
	options    RetainedSourceExportOptions
	workspaces chan sourceExportWorkspace

	mu          sync.RWMutex
	closed      bool
	activePlans int
	plansIdle   *sync.Cond
}

// InstallAbandonmentExitFaultForQualification installs one deterministic
// external-process crash cut. It is intentionally unavailable through serving
// manifests; callers must opt in through the qualification-only command path.
func (provider *RetainedSourceExportProvider) InstallAbandonmentExitFaultForQualification(
	phase string, exit func(),
) bool {
	if provider == nil || exit == nil || (phase != "after_rename" && phase != "after_unlink") {
		return false
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed || provider.repository == nil || provider.repository.fault != nil {
		return false
	}
	wanted := faultAfterAbandonRename
	if phase == "after_unlink" {
		wanted = faultAfterAbandonUnlink
	}
	provider.repository.fault = func(got repositoryFault) error {
		if got == wanted {
			exit()
		}
		return nil
	}
	return true
}

// NewDataService binds the snapshot data plane to the exact repository owned
// by this retained source provider. Keeping repository ownership private makes
// it impossible for the shipped source-control and data listeners to drift to
// different artifact directories or accounting limits.
func (provider *RetainedSourceExportProvider) NewDataService(
	options ServiceOptions,
) (*Service, error) {
	if provider == nil || options.Repository != nil {
		return nil, ErrSourceControl
	}
	provider.mu.RLock()
	if provider.closed || provider.repository == nil {
		provider.mu.RUnlock()
		return nil, ErrSourceControl
	}
	if options.Budget != nil && options.Budget != provider.options.Budget {
		provider.mu.RUnlock()
		return nil, ErrSourceControl
	}
	options.Repository = provider.repository
	options.Budget = provider.options.Budget
	provider.mu.RUnlock()
	return NewService(options)
}

func OpenRetainedSourceExportProvider(
	options RetainedSourceExportOptions,
) (*RetainedSourceExportProvider, error) {
	if options.Cut == nil || options.RuntimeIdentity.Group == (raftmember.GroupKey{}) ||
		options.RuntimeIdentity.AllocationGeneration == 0 ||
		options.RuntimeIdentity.MemberID == 0 || options.RuntimeIdentity.StoreID == ([16]byte{}) ||
		options.RuntimeIdentity.NodeIncarnation == 0 ||
		options.RuntimeIdentity.RelationManifestDigest == ([32]byte{}) ||
		options.SourceNode == (rafttransport.NodeID{}) ||
		options.TargetMember == 0 || options.TargetMember == options.RuntimeIdentity.MemberID ||
		options.TargetStore == ([16]byte{}) || options.TargetIncarnation == 0 ||
		options.ChunkBytes < MinChunkBytes || options.ChunkBytes > AbsoluteMaxChunkBytes ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > AbsoluteMaxSourceConcurrency {
		return nil, ErrSourceControl
	}
	path, err := safeSourceRepositoryPath(options.DataRoot, options.RepositoryPath)
	if err != nil {
		return nil, err
	}
	options.Limits.Budget = options.Budget
	repository, err := OpenRepository(path, options.Limits)
	if err != nil {
		return nil, err
	}
	provider := &RetainedSourceExportProvider{
		repository: repository, options: options,
		workspaces: make(chan sourceExportWorkspace, options.MaxConcurrent),
	}
	provider.plansIdle = sync.NewCond(&provider.mu)
	for range options.MaxConcurrent {
		workspace := sourceExportWorkspace{}
		// Production callers provide a node budget. Keep workspace storage lazy
		// in that mode so idle providers across many groups retain no chunk-sized
		// buffers; the active budget bounds how many are materialized together.
		if options.Budget == nil {
			workspace.artifact = make([]byte, 0, options.ChunkBytes)
			workspace.transfer = make([]byte, 0, options.ChunkBytes)
		}
		provider.workspaces <- workspace
	}
	return provider, nil
}

func safeSourceRepositoryPath(rootPath, repositoryPath string) (string, error) {
	if rootPath == "" || repositoryPath == "" || !filepath.IsAbs(rootPath) ||
		!filepath.IsAbs(repositoryPath) || filepath.Clean(rootPath) != rootPath ||
		filepath.Clean(repositoryPath) != repositoryPath || repositoryPath == rootPath {
		return "", ErrSourceControl
	}
	relative, err := filepath.Rel(rootPath, repositoryPath)
	if err != nil || relative == "." || relative == ".." ||
		len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator) {
		return "", ErrSourceControl
	}
	rootInfo, err := os.Lstat(rootPath)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.Join(ErrSourceControl, err)
	}
	walk := rootPath
	parts := splitCleanPath(relative)
	for index, part := range parts {
		walk = filepath.Join(walk, part)
		info, statErr := os.Lstat(walk)
		if errors.Is(statErr, os.ErrNotExist) {
			if index != len(parts)-1 {
				return "", ErrSourceControl
			}
			break
		}
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", errors.Join(ErrSourceControl, statErr)
		}
	}
	return repositoryPath, nil
}

func splitCleanPath(path string) []string {
	var result []string
	for path != "." && path != "" {
		directory, tail := filepath.Split(path)
		if tail != "" {
			result = append(result, tail)
		}
		path = filepath.Clean(directory)
		if path == string(filepath.Separator) {
			break
		}
	}
	slices.Reverse(result)
	return result
}

func (provider *RetainedSourceExportProvider) ObserveSourceExport(
	ctx context.Context,
	request SourceControlRequest,
) (Descriptor, bool, error) {
	if provider == nil || ctx == nil || !provider.matchesRequest(request) {
		return Descriptor{}, false, ErrSourceConflict
	}
	if cause := context.Cause(ctx); cause != nil {
		return Descriptor{}, false, cause
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.closed || provider.repository == nil {
		return Descriptor{}, false, ErrSourceControl
	}
	return provider.repository.findPublishedSource(request)
}

func (provider *RetainedSourceExportProvider) AbandonSourceExport(
	ctx context.Context, request SourceControlRequest, witness ArtifactAbandonmentWitness,
) error {
	if provider == nil || ctx == nil || !provider.matchesRequest(request) || !witness.Valid() ||
		witness.Operation != request.Operation || witness.Step != request.Step ||
		witness.Owner != request.SourceNode || !descriptorMatchesSourceRequest(witness.Descriptor, request) {
		return ErrAbandonment
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.closed || provider.repository == nil {
		return ErrSourceControl
	}
	_, err := provider.repository.AbandonArtifact(witness)
	return err
}

func (provider *RetainedSourceExportProvider) PinSourceExport(
	ctx context.Context,
	request SourceControlRequest,
) (SourceExportPlan, error) {
	if provider == nil || ctx == nil || !provider.matchesRequest(request) {
		return SourceExportPlan{}, ErrSourceConflict
	}
	if cause := context.Cause(ctx); cause != nil {
		return SourceExportPlan{}, cause
	}
	repository, budget, err := provider.retainPlan()
	if err != nil {
		return SourceExportPlan{}, err
	}
	var workspace sourceExportWorkspace
	workspaceOwned := false
	var workspaceBuffer *migrationbudget.BufferLease
	var activeLease *migrationbudget.Lease
	returnWorkspace := func() {
		if budget != nil {
			if activeLease != nil {
				activeLease.Release()
				activeLease = nil
			}
			if workspaceBuffer != nil {
				workspaceBuffer.Release()
				workspaceBuffer = nil
			}
			workspace.artifact = nil
			workspace.transfer = nil
		}
		if workspaceOwned {
			provider.workspaces <- workspace
			workspaceOwned = false
		}
		provider.releasePlan()
	}
	select {
	case workspace = <-provider.workspaces:
		workspaceOwned = true
		if budget != nil {
			// Reserve both workspaces as one atomic node-scoped credit. Taking
			// them independently lets concurrent plans each hold one half and
			// wait forever for the other half when the pool is tight.
			workspaceBytes := uint64(provider.options.ChunkBytes) * 2
			workspaceBuffer, err = budget.AcquireBuffer(ctx, workspaceBytes)
			if err != nil {
				returnWorkspace()
				return SourceExportPlan{}, err
			}
			// Buffer reservations precede the heavyweight permit so a blocked
			// provider never occupies the last active slot while waiting for
			// node-scoped memory.
			activeLease, err = budget.Acquire(ctx)
			if err != nil {
				returnWorkspace()
				return SourceExportPlan{}, err
			}
			bytes := workspaceBuffer.Bytes()
			chunk := int(provider.options.ChunkBytes)
			workspace.artifact = bytes[:0:chunk]
			workspace.transfer = bytes[chunk : chunk : 2*chunk]
		}
		cut, cutErr := provider.options.Cut.SnapshotArtifactCut()
		if cutErr != nil || cut == nil {
			returnWorkspace()
			if cutErr != nil {
				return SourceExportPlan{}, cutErr
			}
			return SourceExportPlan{}, ErrSourceControl
		}
		fence := cut.Fence()
		publication := cut.Publication()
		if fence.ReplicaSetVersion != request.ReplicaSetVersion ||
			publication.ReplicaSetVersion != request.ReplicaSetVersion ||
			publication.ConfState == nil ||
			!slices.Contains(publication.ConfState.GetVoters(), request.SourceMember) ||
			!slices.Contains(publication.ConfState.GetLearners(), request.TargetMember) ||
			slices.Contains(publication.ConfState.GetVoters(), request.TargetMember) {
			_ = cut.Close()
			returnWorkspace()
			return SourceExportPlan{}, ErrStaleFence
		}
		var releaseOnce sync.Once
		return SourceExportPlan{
			Repository: repository, Snapshot: cut, ExpectedFence: fence,
			Context: ctx, Budget: budget,
			lease: activeLease,
			Group: request.Group, SourceMember: request.SourceMember,
			TargetMember: request.TargetMember, TargetStore: request.TargetStore,
			TargetIncarnation: request.TargetIncarnation, ChunkBytes: provider.options.ChunkBytes,
			ArtifactWorkspace: workspace.artifact, TransferWorkspace: workspace.transfer,
			Release: func() {
				releaseOnce.Do(func() {
					returnWorkspace()
				})
			},
		}, nil
	default:
		returnWorkspace()
		return SourceExportPlan{}, ErrBound
	}
}

// retainPlan protects repository and snapshot ownership without holding the
// provider mutex during the potentially paced export. Close waits for the
// reference to drain before closing the repository.
func (provider *RetainedSourceExportProvider) retainPlan() (*Repository, *migrationbudget.Budget, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.closed || provider.repository == nil {
		return nil, nil, ErrSourceControl
	}
	if provider.plansIdle == nil {
		provider.plansIdle = sync.NewCond(&provider.mu)
	}
	provider.activePlans++
	return provider.repository, provider.options.Budget, nil
}

func (provider *RetainedSourceExportProvider) releasePlan() {
	provider.mu.Lock()
	if provider.activePlans > 0 {
		provider.activePlans--
		if provider.activePlans == 0 && provider.plansIdle != nil {
			provider.plansIdle.Broadcast()
		}
	}
	provider.mu.Unlock()
}

func (provider *RetainedSourceExportProvider) matchesRequest(request SourceControlRequest) bool {
	options := provider.options
	identity := options.RuntimeIdentity
	return validSourceControlRequest(request) && request.Group == identity.Group &&
		request.SourceMember == identity.MemberID && request.SourceNode == options.SourceNode &&
		request.TargetMember == options.TargetMember && request.TargetStore == options.TargetStore &&
		request.TargetIncarnation == options.TargetIncarnation
}

func (provider *RetainedSourceExportProvider) ReleaseSourceExport(
	ctx context.Context,
	request SourceControlRequest,
	descriptor Descriptor,
) error {
	if provider == nil || ctx == nil || !provider.matchesRequest(request) ||
		!descriptorMatchesSourceRequest(descriptor, request) {
		return ErrSourceConflict
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.closed || provider.repository == nil {
		return ErrSourceControl
	}
	return provider.repository.ReleasePublished(ArtifactReleaseRequest{
		Operation: request.Operation, Step: request.Step, Descriptor: descriptor,
	})
}

// Close releases the repository writer claim. Callers must first stop the
// source control service and release every plan returned by PinSourceExport.
func (provider *RetainedSourceExportProvider) Close() error {
	if provider == nil {
		return nil
	}
	provider.mu.Lock()
	if provider.closed {
		provider.mu.Unlock()
		return nil
	}
	provider.closed = true
	for provider.activePlans != 0 {
		if provider.plansIdle == nil {
			provider.plansIdle = sync.NewCond(&provider.mu)
		}
		provider.plansIdle.Wait()
	}
	repository := provider.repository
	provider.mu.Unlock()
	return repository.Close()
}

// findPublishedSource is a bounded restart lookup over repository metadata.
// More than one completed artifact for the same immutable request fence is an
// ambiguity, never a "latest wins" choice.
func (repository *Repository) findPublishedSource(
	request SourceControlRequest,
) (Descriptor, bool, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	if repository.closed {
		return Descriptor{}, false, ErrRepository
	}
	var found Descriptor
	for _, record := range repository.records {
		if !record.complete || !descriptorMatchesSourceRequest(record.descriptor, request) {
			continue
		}
		if found != (Descriptor{}) && found != record.descriptor {
			return Descriptor{}, false, ErrSourceConflict
		}
		found = record.descriptor
	}
	return found, found != (Descriptor{}), nil
}

var _ SourceExportPlanProvider = (*RetainedSourceExportProvider)(nil)
