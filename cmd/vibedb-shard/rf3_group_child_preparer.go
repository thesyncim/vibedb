package main

import (
	"context"
	"net"
	"sync"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

// One process-wide admission bound covers all group-local templates. A
// catalog operation may not be relabelled onto another group's disk roots.
type rf3GroupChildPreparer struct {
	mu        sync.Mutex
	manifest  rf3Manifest
	preparers []*rf3ChildPreparer
	slots     [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot
}

type rf3GroupChildPrepareSlot struct {
	operation [32]byte
	group     int
}

func newRF3GroupChildPreparer(
	manifest rf3Manifest, local rafttransport.NodeID,
	peer, native, control, snapshot net.Addr,
) (*rf3GroupChildPreparer, error) {
	limit := manifest.SplitControl.operationLimit()
	groups := manifest.groupBundles()
	if limit <= 0 || limit > maxRF3SplitChildOperations || len(groups) == 0 || len(groups) > maxRF3ManifestGroups {
		return nil, errRF3Serving
	}
	result := &rf3GroupChildPreparer{manifest: manifest, preparers: make([]*rf3ChildPreparer, len(groups))}
	for index, group := range groups {
		if group.ChildRegistry.MaxOperations > limit {
			return nil, errRF3Serving
		}
		registry, err := newRF3SplitChildPathRegistry(group.ChildRegistry)
		if err != nil {
			return nil, err
		}
		result.preparers[index], err = newRF3ChildPreparer(registry, local, peer, native, control, snapshot)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (preparer *rf3GroupChildPreparer) PrepareChild(
	ctx context.Context, preparation splitcontroller.ChildPreparation,
) (splitcontroller.ChildPrepareReceipt, error) {
	if preparer == nil || ctx == nil {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	target := preparation.ReplicaTarget()
	operation := [32]byte(preparation.OperationID())
	index, registry, ok := rf3SplitChildRegistryForTarget(preparer.manifest, operation, preparation.Child(), target)
	if !ok {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	paths, err := registry.childPaths(operation, preparation.Child())
	if err != nil || !preparer.preparers[index].matchesLocalTarget(target, paths) {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	if err := preparer.admit(operation, index); err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	return preparer.preparers[index].PrepareChild(ctx, preparation)
}

func (preparer *rf3GroupChildPreparer) admit(operation [32]byte, group int) error {
	if operation == ([32]byte{}) || group < 0 || group >= len(preparer.preparers) {
		return splitcontroller.ErrChildPreparation
	}
	empty := -1
	for index := 0; index < preparer.manifest.SplitControl.operationLimit(); index++ {
		slot := preparer.slots[index]
		if slot.operation == operation {
			if slot.group != group {
				return splitcontroller.ErrChildPreparation
			}
			return nil
		}
		if empty < 0 && slot.operation == ([32]byte{}) {
			empty = index
		}
	}
	if empty < 0 {
		return errRF3SplitChildRegistryBound
	}
	preparer.slots[empty] = rf3GroupChildPrepareSlot{operation: operation, group: group}
	return nil
}

// The signed catalog target carries its complete retained SQL/apply identity
// and an operation-derived local path. Both must select one and only one
// provisioned group template before any disk or execution authority is used.
func rf3SplitChildRegistryForTarget(
	manifest rf3Manifest, operation [32]byte, child uint8,
	target splitcontroller.ChildReplicaTarget,
) (int, rf3ManifestSplitChildRegistry, bool) {
	found := -1
	var selected rf3ManifestSplitChildRegistry
	for index, group := range manifest.groupBundles() {
		registry := group.ChildRegistry
		paths, err := registry.childPaths(operation, child)
		if err != nil || target.RuntimeRoot != paths.Root || target.SQLPath != paths.Database ||
			target.WALPath != paths.WAL || target.SQL.Binding.Distribution != group.Route.Distribution ||
			!rf3SplitChildTemplateMatchesRetained(registry, target.SQL, target.Apply) {
			continue
		}
		if found >= 0 {
			return 0, rf3ManifestSplitChildRegistry{}, false
		}
		found, selected = index, registry
	}
	return found, selected, found >= 0
}

var _ splitcontroller.ChildPreparer = (*rf3GroupChildPreparer)(nil)
