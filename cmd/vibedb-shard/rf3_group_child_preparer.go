package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

// One process-wide admission bound covers all group-local templates. A
// catalog operation may not be relabelled onto another group's disk roots.
type rf3GroupChildPreparer struct {
	mu                              sync.Mutex
	manifest                        rf3Manifest
	preparers                       []*rf3ChildPreparer
	dynamic                         *rf3DynamicChildResources
	local                           rafttransport.NodeID
	peer, native, control, snapshot net.Addr
	dynamicPreparers                [maxRF3SplitChildOperations][autosplit.MaxSplitChildren]*rf3ChildPreparer
	slots                           [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot
	store                           *rf3ChildAdmissionStore
	inflight                        [maxRF3SplitChildOperations]int
	inventory                       *rf3AdoptedGroupInventory
}

type rf3GroupChildPrepareSlot struct {
	operation    [32]byte
	group        int
	certificates [autosplit.MaxSplitChildren][32]byte
	requests     [autosplit.MaxSplitChildren][32]byte
}

func newRF3GroupChildPreparer(
	manifest rf3Manifest, local rafttransport.NodeID,
	peer, native, control, snapshot net.Addr,
	dynamic ...*rf3DynamicChildResources,
) (*rf3GroupChildPreparer, error) {
	limit := manifest.SplitControl.operationLimit()
	groups := manifest.groupBundles()
	if limit <= 0 || limit > maxRF3SplitChildOperations || len(groups) > maxRF3ManifestGroups ||
		len(dynamic) > 1 || len(groups) == 0 && (len(dynamic) == 0 || dynamic[0] == nil) ||
		local == (rafttransport.NodeID{}) || peer == nil || native == nil || control == nil || snapshot == nil {
		return nil, errRF3Serving
	}
	result := &rf3GroupChildPreparer{manifest: manifest, preparers: make([]*rf3ChildPreparer, len(groups)),
		local: local, peer: peer, native: native, control: control, snapshot: snapshot}
	if len(dynamic) == 1 {
		result.dynamic = dynamic[0]
	}
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
	var err error
	result.store, result.slots, err = openRF3ChildAdmissionStore(manifest.ReplicaControl.SourceDataRoot, manifest.Digest, limit, manifest)
	if err != nil {
		return nil, err
	}
	for slotIndex, slot := range result.slots {
		if slot.operation == ([32]byte{}) {
			continue
		}
		if slot.group == rf3DynamicTemplateSlot {
			for child, certificate := range slot.certificates {
				if certificate == ([32]byte{}) {
					continue
				}
				resources, found, readErr := result.dynamic.ReadResources(slot.operation, uint8(child))
				if readErr != nil || !found || resources.PreparationDigest != slot.requests[child] ||
					resources.Preparation.ReplicaTarget().CertificateDigest != certificate {
					_ = result.Close()
					return nil, errors.Join(errRF3Serving, readErr)
				}
				prepared, prepareErr := result.newDynamicPreparer(resources.Registry)
				if prepareErr != nil {
					_ = result.Close()
					return nil, prepareErr
				}
				if _, prepareErr = prepared.registry.acquire(slot.operation, uint8(child)); prepareErr != nil {
					_ = result.Close()
					return nil, prepareErr
				}
				result.dynamicPreparers[slotIndex][child] = prepared
			}
			continue
		}
		if slot.group < 0 || slot.group >= len(result.preparers) {
			_ = result.Close()
			return nil, errRF3Serving
		}
		if _, err = result.preparers[slot.group].registry.acquire(slot.operation, 0); err != nil {
			_ = result.Close()
			return nil, err
		}
	}
	if err = result.recoverTerminal(); err != nil {
		_ = result.Close()
		return nil, err
	}
	return result, nil
}

func (preparer *rf3GroupChildPreparer) PrepareChild(
	ctx context.Context, preparation splitcontroller.ChildPreparation,
) (splitcontroller.ChildPrepareReceipt, error) {
	if preparer == nil || ctx == nil {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	if err := context.Cause(ctx); err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	preparer.mu.Lock()
	locked := true
	defer func() {
		if locked {
			preparer.mu.Unlock()
		}
	}()
	if err := context.Cause(ctx); err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	target := preparation.ReplicaTarget()
	operation := [32]byte(preparation.OperationID())
	request, err := splitcontroller.ChildPreparationDigest(preparation)
	if err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	index, registry, ok := rf3SplitChildRegistryForTarget(preparer.manifest, operation, preparation.Child(), target)
	var selected *rf3ChildPreparer
	if ok && preparer.dynamic == nil {
		selected = preparer.preparers[index]
	} else {
		if target.Node != preparer.local || target.PeerAddress != preparer.peer.String() ||
			target.NativeAddress != preparer.native.String() || target.ControlAddress != preparer.control.String() ||
			target.SnapshotAddress != preparer.snapshot.String() {
			return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
		}
		// Check the process-wide bound before publishing a durable dynamic
		// template. Rejected excess operations must not leave new disk records.
		if err := preparer.preflightDynamicAdmission(operation, int(preparation.Child()), target.CertificateDigest, request); err != nil {
			return splitcontroller.ChildPrepareReceipt{}, err
		}
		resources, resolveErr := preparer.dynamic.PrepareResources(ctx, preparation)
		if resolveErr != nil {
			return splitcontroller.ChildPrepareReceipt{}, resolveErr
		}
		index, registry = resources.Slot, resources.Registry
		if index != rf3DynamicTemplateSlot {
			return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
		}
		var prepareErr error
		selected, prepareErr = preparer.newDynamicPreparer(registry)
		if prepareErr != nil {
			return splitcontroller.ChildPrepareReceipt{}, prepareErr
		}
	}
	paths, err := registry.childPaths(operation, preparation.Child())
	if err != nil || !selected.matchesLocalTarget(target, paths) {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	previous := preparer.slots
	slot, err := preparer.reserve(operation, index, int(preparation.Child()), target.CertificateDigest, request)
	if err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	if index == rf3DynamicTemplateSlot {
		if prior := preparer.dynamicPreparers[slot][preparation.Child()]; prior != nil {
			selected = prior
		}
	}
	_, registryErr := selected.registry.acquire(operation, preparation.Child())
	if registryErr != nil {
		if previous != preparer.slots {
			if err := preparer.store.save(previous); err != nil {
				return splitcontroller.ChildPrepareReceipt{}, errors.Join(registryErr, err)
			}
			preparer.slots = previous
		}
		return splitcontroller.ChildPrepareReceipt{}, registryErr
	}
	if cause := context.Cause(ctx); cause != nil {
		// No child preparer has run yet: rollback only this known no-I/O
		// admission. Later errors retain their durable outcome-unknown slot.
		if previous != preparer.slots {
			if err := preparer.store.save(previous); err != nil {
				return splitcontroller.ChildPrepareReceipt{}, errors.Join(cause, err)
			}
			preparer.slots = previous
		}
		if previous[slot].operation == ([32]byte{}) {
			selected.registry.release(operation)
		}
		return splitcontroller.ChildPrepareReceipt{}, cause
	}
	if index == rf3DynamicTemplateSlot {
		preparer.dynamicPreparers[slot][preparation.Child()] = selected
	}
	preparer.inflight[slot]++
	preparer.mu.Unlock()
	locked = false
	defer func() { preparer.mu.Lock(); preparer.inflight[slot]--; preparer.mu.Unlock() }()
	return selected.PrepareChild(ctx, preparation)
}

func (preparer *rf3GroupChildPreparer) preflightDynamicAdmission(operation [32]byte, child int, certificate, request [32]byte) error {
	if preparer.store == nil || preparer.store.failed || preparer.store.root == nil {
		return errRF3Serving
	}
	if operation == ([32]byte{}) || child < 0 || child >= autosplit.MaxSplitChildren ||
		certificate == ([32]byte{}) || request == ([32]byte{}) || preparer.dynamic == nil {
		return splitcontroller.ErrChildPreparation
	}
	empty := -1
	for index := 0; index < preparer.manifest.SplitControl.operationLimit(); index++ {
		slot := preparer.slots[index]
		if slot.operation == operation {
			if slot.group != rf3DynamicTemplateSlot || slot.certificates[child] != ([32]byte{}) &&
				(slot.certificates[child] != certificate || slot.requests[child] != request) {
				return splitcontroller.ErrChildPreparation
			}
			empty = index
			break
		}
		if empty < 0 && slot.operation == ([32]byte{}) {
			empty = index
		}
	}
	if empty < 0 {
		return errRF3SplitChildRegistryBound
	}
	if preparer.slots[empty].operation == ([32]byte{}) {
		if err := preparer.checkPriorPreparation(operation, rf3DynamicTemplateSlot); err != nil {
			return err
		}
	}
	next := preparer.slots
	next[empty].operation, next[empty].group = operation, rf3DynamicTemplateSlot
	next[empty].certificates[child], next[empty].requests[child] = certificate, request
	return preparer.inventory.checkCapacity(next)
}

func (preparer *rf3GroupChildPreparer) newDynamicPreparer(template rf3ManifestSplitChildRegistry) (*rf3ChildPreparer, error) {
	registry, err := newRF3SplitChildPathRegistry(template)
	if err != nil {
		return nil, err
	}
	return newRF3ChildPreparer(registry, preparer.local, preparer.peer, preparer.native, preparer.control, preparer.snapshot)
}

func (preparer *rf3GroupChildPreparer) registryForChild(group int, operation [32]byte, child uint8) (rf3ManifestSplitChildRegistry, error) {
	if group == rf3DynamicTemplateSlot {
		resources, found, err := preparer.dynamic.ReadResources(operation, child)
		if err != nil || !found {
			return rf3ManifestSplitChildRegistry{}, errors.Join(splitcontroller.ErrChildPreparation, err)
		}
		return resources.Registry, nil
	}
	groups := preparer.manifest.groupBundles()
	if group < 0 || group >= len(groups) {
		return rf3ManifestSplitChildRegistry{}, splitcontroller.ErrChildPreparation
	}
	return groups[group].ChildRegistry, nil
}

func (preparer *rf3GroupChildPreparer) reserve(operation [32]byte, group, child int, certificate, request [32]byte) (int, error) {
	if preparer.store == nil || preparer.store.failed || preparer.store.root == nil {
		return 0, errRF3Serving
	}
	if operation == ([32]byte{}) || group < 0 || group >= len(preparer.preparers) && (group != rf3DynamicTemplateSlot || preparer.dynamic == nil) ||
		child < 0 || child >= autosplit.MaxSplitChildren || certificate == ([32]byte{}) || request == ([32]byte{}) {
		return 0, splitcontroller.ErrChildPreparation
	}
	registry, err := preparer.registryForChild(group, operation, uint8(child))
	if err != nil {
		return 0, err
	}
	paths, err := registry.childPaths(operation, uint8(child))
	if err != nil {
		return 0, err
	}
	terminal, err := splitcontroller.HasRuntimeTerminalWitness(paths.Root, splitcontroller.OperationID(operation), certificate)
	if err != nil {
		return 0, err
	}
	if terminal {
		return 0, splitcontroller.ErrRuntimeTerminal
	}
	empty := -1
	existing := -1
	for index := 0; index < preparer.manifest.SplitControl.operationLimit(); index++ {
		slot := preparer.slots[index]
		if slot.operation == operation {
			if slot.group != group {
				return 0, splitcontroller.ErrChildPreparation
			}
			existing = index
			break
		}
		if empty < 0 && slot.operation == ([32]byte{}) {
			empty = index
		}
	}
	if existing >= 0 {
		empty = existing
	}
	if empty < 0 {
		return 0, errRF3SplitChildRegistryBound
	}
	if existing < 0 {
		if err := preparer.checkPriorPreparation(operation, group); err != nil {
			return 0, err
		}
	}
	next := preparer.slots
	if existing < 0 {
		next[empty] = rf3GroupChildPrepareSlot{operation: operation, group: group}
	}
	old := next[empty]
	if old.certificates[child] != ([32]byte{}) && (old.certificates[child] != certificate || old.requests[child] != request) {
		return 0, splitcontroller.ErrChildPreparation
	}
	next[empty].certificates[child], next[empty].requests[child] = certificate, request
	if err := preparer.inventory.checkCapacity(next); err != nil {
		return 0, err
	}
	if next != preparer.slots {
		if err := preparer.store.save(next); err != nil {
			return 0, err
		}
		preparer.slots = next
	}
	return empty, nil
}

func (preparer *rf3GroupChildPreparer) Close() error {
	if preparer == nil {
		return nil
	}
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	for _, active := range preparer.inflight {
		if active != 0 {
			return splitcontroller.ErrRuntimeRegistryInUse
		}
	}
	return preparer.store.Close()
}

// A reclaimed admission must not resurrect under another group. Existing
// per-child receipts and terminal witnesses are addressed directly; there is
// no directory walk over historical operations and no second tombstone log.
func (preparer *rf3GroupChildPreparer) checkPriorPreparation(operation [32]byte, group int) error {
	for index, candidate := range preparer.manifest.groupBundles() {
		for child := uint8(0); child < autosplit.MaxSplitChildren; child++ {
			paths, err := candidate.ChildRegistry.childPaths(operation, child)
			if err != nil {
				return err
			}
			terminal, err := splitcontroller.HasBoundRuntimeTerminalWitness(paths.Root, splitcontroller.OperationID(operation))
			if err != nil {
				return err
			}
			if terminal {
				return splitcontroller.ErrRuntimeTerminal
			}
			raw, err := readPrepareRF3File(filepath.Join(paths.Root, rf3ChildPrepareReceiptName), splitcontroller.MaxChildPreparationBytes)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			receipt, err := splitcontroller.OpenChildPrepareReceipt(raw)
			if err != nil || receipt.Operation != splitcontroller.OperationID(operation) || receipt.Child != child || index != group {
				return splitcontroller.ErrChildPreparation
			}
			terminal, err = splitcontroller.HasRuntimeTerminalWitness(paths.Root, receipt.Operation, receipt.Target.CertificateDigest)
			if err != nil {
				return err
			}
			if terminal {
				return splitcontroller.ErrRuntimeTerminal
			}
		}
	}
	for child := uint8(0); child < autosplit.MaxSplitChildren; child++ {
		resources, found, err := preparer.dynamic.ReadResources(operation, child)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if group != rf3DynamicTemplateSlot {
			return splitcontroller.ErrChildPreparation
		}
		paths, err := resources.Registry.childPaths(operation, child)
		if err != nil {
			return err
		}
		terminal, err := splitcontroller.HasBoundRuntimeTerminalWitness(paths.Root, splitcontroller.OperationID(operation))
		if err != nil || terminal {
			return errors.Join(splitcontroller.ErrRuntimeTerminal, err)
		}
	}
	return nil
}

func (preparer *rf3GroupChildPreparer) slotTerminal(slot rf3GroupChildPrepareSlot) (bool, error) {
	found := false
	for child, certificate := range slot.certificates {
		if certificate == ([32]byte{}) {
			continue
		}
		found = true
		registry, err := preparer.registryForChild(slot.group, slot.operation, uint8(child))
		if err != nil {
			return false, err
		}
		paths, err := registry.childPaths(slot.operation, uint8(child))
		if err != nil {
			return false, err
		}
		terminal, err := splitcontroller.HasRuntimeTerminalWitness(paths.Root, splitcontroller.OperationID(slot.operation), certificate)
		if err != nil || !terminal {
			return false, err
		}
	}
	return found, nil
}

func (preparer *rf3GroupChildPreparer) recoverTerminal() error {
	if preparer.store == nil || preparer.store.failed || preparer.store.root == nil {
		return errRF3Serving
	}
	next := preparer.slots
	for index, slot := range next {
		if slot.operation == ([32]byte{}) {
			continue
		}
		if preparer.inflight[index] != 0 {
			continue
		}
		terminal, err := preparer.slotTerminal(slot)
		if err != nil {
			return err
		}
		if terminal {
			next[index] = rf3GroupChildPrepareSlot{}
		}
	}
	if next == preparer.slots {
		return nil
	}
	if err := preparer.store.save(next); err != nil {
		return err
	}
	for index, slot := range preparer.slots {
		if slot.operation != ([32]byte{}) && next[index].operation == ([32]byte{}) {
			if slot.group == rf3DynamicTemplateSlot {
				for child, prepared := range preparer.dynamicPreparers[index] {
					if prepared != nil {
						prepared.registry.release(slot.operation)
						preparer.dynamicPreparers[index][child] = nil
					}
				}
			} else {
				preparer.preparers[slot.group].registry.release(slot.operation)
			}
		}
	}
	preparer.slots = next
	return nil
}

type rf3PreparedChildRetirer struct {
	certified *splitcontroller.LocalTerminalRetirer
	preparer  *rf3GroupChildPreparer
}

func (retirer rf3PreparedChildRetirer) RetireTerminal(retirement splitcontroller.TerminalRetirement) error {
	if retirer.preparer == nil {
		return retirer.certified.RetireTerminal(retirement)
	}
	preparer := retirer.preparer
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	for index, slot := range preparer.slots {
		if slot.operation != [32]byte(retirement.Operation) {
			continue
		}
		if preparer.inflight[index] != 0 {
			return splitcontroller.ErrRuntimeRegistryInUse
		}
	}
	if err := retirer.certified.RetireTerminal(retirement); err != nil {
		return err
	}
	for _, slot := range preparer.slots {
		if slot.operation != [32]byte(retirement.Operation) {
			continue
		}
		terminal, err := preparer.slotTerminal(slot)
		if err != nil || !terminal {
			return errors.Join(splitcontroller.ErrSplitOperationRetirement, err)
		}
	}
	return preparer.recoverTerminal()
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
