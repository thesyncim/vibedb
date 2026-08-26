package splitcontroller

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/splitartifact"
)

type dynamicSplitDataEntry struct {
	plan        *Plan
	planDigest  [32]byte
	source      *LocalSourceActions
	sourceLease *RuntimeStoreLease
	children    [autosplit.MaxSplitChildren]*LocalChildActions
	childLeases [autosplit.MaxSplitChildren]*RuntimeStoreLease
	sourceNodes []rafttransport.NodeID
}

// DynamicSplitData is the bounded data-plane registry populated by durable
// PlanAdmission binding. It serves immutable artifacts and tail application
// only for exact live operations; terminal cleanup removes the whole entry.
type DynamicSplitData struct {
	mu      sync.RWMutex
	entries map[OperationID]*dynamicSplitDataEntry
	limit   int
}

func NewDynamicSplitData(limit int) (*DynamicSplitData, error) {
	if limit <= 0 || limit > AbsoluteMaxShardActionGrants {
		return nil, ErrRemoteExecution
	}
	return &DynamicSplitData{entries: make(map[OperationID]*dynamicSplitDataEntry, min(limit, 64)), limit: limit}, nil
}

func (registry *DynamicSplitData) InstallSource(
	plan *Plan, digest [32]byte, lease *RuntimeStoreLease, actions *LocalSourceActions,
	sourceNodes []rafttransport.NodeID,
) error {
	if registry == nil || plan == nil || digest == ([32]byte{}) || lease == nil || actions == nil ||
		len(sourceNodes) == 0 || len(sourceNodes) > 8 {
		return ErrRemoteExecution
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, err := registry.entry(plan, digest)
	if err != nil {
		return err
	}
	if entry.source != nil && (entry.source != actions || entry.sourceLease != lease) {
		return ErrRemoteExecution
	}
	entry.source, entry.sourceLease = actions, lease
	entry.sourceNodes = append(entry.sourceNodes[:0], sourceNodes...)
	return nil
}

func (registry *DynamicSplitData) InstallChild(
	plan *Plan, digest [32]byte, child uint8, lease *RuntimeStoreLease, actions *LocalChildActions,
) error {
	if registry == nil || plan == nil || digest == ([32]byte{}) || lease == nil || actions == nil ||
		!plan.validNonRetainedChild(child) {
		return ErrRemoteExecution
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, err := registry.entry(plan, digest)
	if err != nil {
		return err
	}
	if entry.children[child] != nil &&
		(entry.children[child] != actions || entry.childLeases[child] != lease) {
		return ErrRemoteExecution
	}
	entry.children[child], entry.childLeases[child] = actions, lease
	return nil
}

func (registry *DynamicSplitData) entry(plan *Plan, digest [32]byte) (*dynamicSplitDataEntry, error) {
	entry := registry.entries[plan.OperationID()]
	if entry != nil {
		if entry.plan != plan || entry.planDigest != digest {
			return nil, ErrRemoteExecution
		}
		return entry, nil
	}
	if len(registry.entries) == registry.limit {
		return nil, ErrRuntimeRegistryCapacity
	}
	entry = &dynamicSplitDataEntry{plan: plan, planDigest: digest}
	registry.entries[plan.OperationID()] = entry
	return entry, nil
}

func (registry *DynamicSplitData) OpenSplitArtifact(
	ctx context.Context, identity splitartifact.Identity,
) (splitartifact.Artifact, error) {
	if registry == nil || ctx == nil || !identity.Valid() {
		return nil, ErrRemoteExecution
	}
	registry.mu.RLock()
	entry := registry.entries[OperationID(identity.Operation)]
	if entry == nil || entry.source == nil || entry.sourceLease == nil {
		registry.mu.RUnlock()
		return nil, ErrRemoteExecution
	}
	plan, actions, lease := entry.plan, entry.source, entry.sourceLease
	registry.mu.RUnlock()
	artifacts, err := loadObservedArtifacts(lease)
	if err != nil || artifacts == nil || int(identity.Child) >= len(artifacts.Children) {
		return nil, errors.Join(ErrRemoteExecution, err)
	}
	want, err := splitartifact.NewIdentity(identity.Operation, artifacts.Children[identity.Child])
	if err != nil || want != identity {
		return nil, errors.Join(ErrRemoteExecution, err)
	}
	return actions.OpenChildArtifact(plan, *artifacts, identity.Child)
}

func (registry *DynamicSplitData) ResolveSplitTail(
	ctx context.Context, operation OperationID, child uint8,
) (TailStreamResolvedTarget, error) {
	if registry == nil || ctx == nil {
		return TailStreamResolvedTarget{}, ErrTailStreamControl
	}
	registry.mu.RLock()
	entry := registry.entries[operation]
	if entry == nil || child >= uint8(len(entry.children)) || entry.children[child] == nil ||
		entry.childLeases[child] == nil {
		registry.mu.RUnlock()
		return TailStreamResolvedTarget{}, ErrTailStreamControl
	}
	plan, actions, lease := entry.plan, entry.children[child], entry.childLeases[child]
	registry.mu.RUnlock()
	artifacts, err := loadObservedArtifacts(lease)
	if err != nil || artifacts == nil {
		return TailStreamResolvedTarget{}, errors.Join(ErrTailStreamControl, err)
	}
	return ResolveLocalTailStreamTarget(plan, *artifacts, child, actions)
}

func (registry *DynamicSplitData) AuthorizeArtifact(
	peer rafttransport.PeerIdentity, identity splitartifact.Identity,
) bool {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	entry := registry.entries[OperationID(identity.Operation)]
	if entry == nil || identity.PlanDigest != entry.plan.partitioner.Digest() {
		return false
	}
	target, ok := entry.plan.Target(identity.Child)
	if !ok || peer.TrustDomain != (rafttransport.TrustDomain{
		ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
	}) {
		return false
	}
	for _, replica := range target.Replicas {
		if replica.Node == peer.Node {
			return true
		}
	}
	return false
}

func (registry *DynamicSplitData) AuthorizeTail(
	peer rafttransport.PeerIdentity, binding rangesplit.TailStreamBinding,
) bool {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	entry := registry.entries[OperationID(binding.Operation)]
	if entry == nil || binding.PlanDigest != entry.plan.partitioner.Digest() {
		return false
	}
	target, ok := entry.plan.Target(binding.Child)
	if !ok || peer.TrustDomain != (rafttransport.TrustDomain{
		ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
	}) {
		return false
	}
	for _, node := range entry.sourceNodes {
		if node == peer.Node {
			return true
		}
	}
	return false
}

func (registry *DynamicSplitData) retire(operation OperationID, digest [32]byte) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	if entry := registry.entries[operation]; entry != nil && entry.planDigest == digest {
		delete(registry.entries, operation)
	}
	registry.mu.Unlock()
}

var _ splitartifact.Source = (*DynamicSplitData)(nil)
var _ TailStreamTargetResolver = (*DynamicSplitData)(nil)
