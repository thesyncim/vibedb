package splitcontroller

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// RetainedPlanRuntimeRegistry binds one already-open manifest-owned runtime
// registry to the exact retained placement served by the local process.
type RetainedPlanRuntimeRegistry struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	Allocation   distribution.ShardAllocationGeneration
	Registry     *RuntimeStoreRegistry
}

func (plan *Plan) SourceAllocation() (distribution.DistributionName, distribution.ShardID, distribution.ShardAllocationGeneration) {
	if plan == nil {
		return "", "", 0
	}
	return plan.source.Distribution, plan.source.Shard, plan.source.AllocationGeneration
}

// SourceAdmissionIsSealed recognizes only this plan's exact durable narrowed
// ownership. Restart may re-present the pre-publication catalog after sealing;
// that is not permission to admit a different later ownership epoch.
func (plan *Plan) SourceAdmissionIsSealed(state replicatedstate.State) bool {
	return plan != nil && state.Binding.Distribution == string(plan.source.Distribution) &&
		state.Binding.Shard == string(plan.source.Shard) && state.Binding.AllocationGeneration == uint64(plan.source.AllocationGeneration) &&
		plan.sourceBindingSealed(state.Binding)
}

// RegisterRetained publishes a certified adopted child as a future source.
// Dynamically opened child ownership transfers to the live retained set;
// caller-supplied startup registries remain owned by the process composition.
func (registries *LocalPlanAdmissionRegistries) RegisterRetained(item RetainedPlanRuntimeRegistry) error {
	if registries == nil || item.Registry == nil || item.Distribution == "" || item.Shard == "" || item.Allocation == 0 {
		return ErrPlanAdmission
	}
	registries.mu.Lock()
	defer registries.mu.Unlock()
	if registries.closed {
		return ErrPlanAdmission
	}
	for _, prior := range registries.retained {
		if prior.Registry == item.Registry || prior.Distribution == item.Distribution && prior.Shard == item.Shard && prior.Allocation == item.Allocation {
			if prior == item {
				return nil
			}
			return ErrPlanAdmission
		}
	}
	if len(registries.retained) == AbsoluteMaxLocalPlanAdmissionStores {
		return ErrRuntimeRegistryCapacity
	}
	registries.retained = append(registries.retained, item)
	for index, child := range registries.children {
		if child.value == item.Registry {
			registries.ownedRetained = append(registries.ownedRetained, item.Registry)
			registries.children = append(registries.children[:index], registries.children[index+1:]...)
			break
		}
	}
	return nil
}

// OpenPreparedSource uses a receipt-authenticated exact root, sharing any
// existing child registry until promotion transfers its process ownership.
func (registries *LocalPlanAdmissionRegistries) OpenPreparedSource(root string, digest [32]byte) (*RuntimeStoreRegistry, error) {
	if registries == nil {
		return nil, ErrPlanAdmission
	}
	registries.mu.Lock()
	defer registries.mu.Unlock()
	if registries.closed {
		return nil, ErrPlanAdmission
	}
	return registries.openChild(root, digest)
}

type childPlanRuntimeRegistry struct {
	root   string
	digest [32]byte
	value  *RuntimeStoreRegistry
}

// LocalPlanAdmissionRegistries resolves every local source and destination
// store from authenticated plan identities. Destination registries are opened
// only at the exact prepared RuntimeRoot and CertificateDigest carried by the
// canonical PlanIntent; paths are never derived from shard names or DNS.
type LocalPlanAdmissionRegistries struct {
	mu sync.Mutex

	node          rafttransport.NodeID
	retained      []RetainedPlanRuntimeRegistry
	children      []childPlanRuntimeRegistry
	ownedRetained []*RuntimeStoreRegistry
	limit         int
	authority     RuntimeTerminalAuthority
	closed        bool
}

func NewLocalPlanAdmissionRegistries(
	node rafttransport.NodeID,
	retained []RetainedPlanRuntimeRegistry,
	maxPreparedChildren int,
	authority RuntimeTerminalAuthority,
) (*LocalPlanAdmissionRegistries, error) {
	if node == (rafttransport.NodeID{}) || len(retained) == 0 ||
		len(retained) > AbsoluteMaxLocalPlanAdmissionStores || maxPreparedChildren <= 0 ||
		maxPreparedChildren > AbsoluteMaxShardActionGrants {
		return nil, ErrPlanAdmission
	}
	owned := slices.Clone(retained)
	for index, item := range owned {
		if item.Distribution == "" || item.Shard == "" || item.Allocation == 0 || item.Registry == nil {
			return nil, ErrPlanAdmission
		}
		for prior := 0; prior < index; prior++ {
			if owned[prior].Distribution == item.Distribution && owned[prior].Shard == item.Shard &&
				owned[prior].Allocation == item.Allocation {
				return nil, ErrPlanAdmission
			}
		}
	}
	return &LocalPlanAdmissionRegistries{
		node: node, retained: owned, limit: maxPreparedChildren, authority: authority,
		children: make([]childPlanRuntimeRegistry, 0, min(maxPreparedChildren, 16)),
	}, nil
}

func (registries *LocalPlanAdmissionRegistries) ResolveLocalPlanAdmissionStores(
	ctx context.Context, plan *Plan,
) ([]*RuntimeStoreRegistry, error) {
	if registries == nil || ctx == nil || plan == nil {
		return nil, ErrPlanAdmission
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	registries.mu.Lock()
	defer registries.mu.Unlock()
	if registries.closed {
		return nil, ErrPlanAdmission
	}
	result := make([]*RuntimeStoreRegistry, 0, 1+int(plan.childCount))
	for _, retained := range registries.retained {
		if retained.Distribution == plan.source.Distribution && retained.Shard == plan.source.Shard &&
			retained.Allocation == plan.source.AllocationGeneration {
			result = append(result, retained.Registry)
			break
		}
	}
	for child := uint8(0); child < plan.childCount; child++ {
		target, ok := plan.Target(child)
		if !ok {
			continue
		}
		for _, replica := range target.Replicas {
			if replica.Node != registries.node {
				continue
			}
			registry, err := registries.openChild(replica.RuntimeRoot, replica.CertificateDigest)
			if err != nil {
				return nil, err
			}
			if !slices.Contains(result, registry) {
				result = append(result, registry)
			}
		}
	}
	if len(result) == 0 || len(result) > AbsoluteMaxLocalPlanAdmissionStores {
		return nil, ErrPlanAdmission
	}
	return result, nil
}

func (registries *LocalPlanAdmissionRegistries) openChild(
	root string, digest [32]byte,
) (*RuntimeStoreRegistry, error) {
	for _, retained := range registries.retained {
		if retained.Registry.rootPath == root {
			if retained.Registry.manifest != digest {
				return nil, ErrPlanAdmission
			}
			return retained.Registry, nil
		}
	}
	for _, item := range registries.children {
		if item.root == root || item.digest == digest {
			if item.root != root || item.digest != digest {
				return nil, ErrPlanAdmission
			}
			return item.value, nil
		}
	}
	if len(registries.children) == registries.limit {
		return nil, ErrRuntimeRegistryCapacity
	}
	registry, err := OpenRuntimeStoreRegistry(root, digest, 1, registries.authority)
	if err != nil {
		return nil, errors.Join(ErrPlanAdmission, err)
	}
	registries.children = append(registries.children, childPlanRuntimeRegistry{
		root: root, digest: digest, value: registry,
	})
	return registry, nil
}

// Close releases only dynamically opened child registries. Retained registries
// remain owned by the RF3 process composition that supplied them.
func (registries *LocalPlanAdmissionRegistries) Close() error {
	if registries == nil {
		return nil
	}
	registries.mu.Lock()
	defer registries.mu.Unlock()
	if registries.closed {
		return nil
	}
	registries.closed = true
	var result error
	for _, item := range registries.children {
		result = errors.Join(result, item.value.Close())
	}
	for _, item := range registries.ownedRetained {
		result = errors.Join(result, item.Close())
	}
	registries.ownedRetained = nil
	registries.children = nil
	return result
}

var _ PlanAdmissionStoreResolver = (*LocalPlanAdmissionRegistries)(nil)
