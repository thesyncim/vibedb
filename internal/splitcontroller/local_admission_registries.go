package splitcontroller

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// RetainedPlanRuntimeRegistry binds one already-open manifest-owned runtime
// registry to the exact retained placement served by the local process.
type RetainedPlanRuntimeRegistry struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	Allocation   distribution.ShardAllocationGeneration
	Registry     *RuntimeStoreRegistry
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

	node      rafttransport.NodeID
	retained  []RetainedPlanRuntimeRegistry
	children  []childPlanRuntimeRegistry
	limit     int
	authority RuntimeTerminalAuthority
	closed    bool
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
	registries.children = nil
	return result
}

var _ PlanAdmissionStoreResolver = (*LocalPlanAdmissionRegistries)(nil)
