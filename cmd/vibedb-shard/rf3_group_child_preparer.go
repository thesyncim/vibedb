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
	mu        sync.Mutex
	manifest  rf3Manifest
	preparers []*rf3ChildPreparer
	slots     [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot
	store     *rf3ChildAdmissionStore
	inflight  [maxRF3SplitChildOperations]int
	inventory *rf3AdoptedGroupInventory
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
	var err error
	result.store, result.slots, err = openRF3ChildAdmissionStore(manifest.ReplicaControl.SourceDataRoot, manifest.Digest, limit)
	if err != nil {
		return nil, err
	}
	for _, slot := range result.slots {
		if slot.operation == ([32]byte{}) {
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
	index, registry, ok := rf3SplitChildRegistryForTarget(preparer.manifest, operation, preparation.Child(), target)
	if !ok {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	paths, err := registry.childPaths(operation, preparation.Child())
	if err != nil || !preparer.preparers[index].matchesLocalTarget(target, paths) {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	request, err := splitcontroller.ChildPreparationDigest(preparation)
	if err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	previous := preparer.slots
	slot, err := preparer.reserve(operation, index, int(preparation.Child()), target.CertificateDigest, request)
	if err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	_, registryErr := preparer.preparers[index].registry.acquire(operation, preparation.Child())
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
			preparer.preparers[index].registry.release(operation)
		}
		return splitcontroller.ChildPrepareReceipt{}, cause
	}
	preparer.inflight[slot]++
	preparer.mu.Unlock()
	locked = false
	defer func() { preparer.mu.Lock(); preparer.inflight[slot]--; preparer.mu.Unlock() }()
	return preparer.preparers[index].PrepareChild(ctx, preparation)
}

func (preparer *rf3GroupChildPreparer) reserve(operation [32]byte, group, child int, certificate, request [32]byte) (int, error) {
	if preparer.store == nil || preparer.store.failed || preparer.store.root == nil {
		return 0, errRF3Serving
	}
	if operation == ([32]byte{}) || group < 0 || group >= len(preparer.preparers) ||
		child < 0 || child >= autosplit.MaxSplitChildren || certificate == ([32]byte{}) || request == ([32]byte{}) {
		return 0, splitcontroller.ErrChildPreparation
	}
	paths, err := preparer.manifest.groupBundles()[group].ChildRegistry.childPaths(operation, uint8(child))
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
	return nil
}

func (preparer *rf3GroupChildPreparer) slotTerminal(slot rf3GroupChildPrepareSlot) (bool, error) {
	found := false
	for child, certificate := range slot.certificates {
		if certificate == ([32]byte{}) {
			continue
		}
		found = true
		paths, err := preparer.manifest.groupBundles()[slot.group].ChildRegistry.childPaths(slot.operation, uint8(child))
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
			preparer.preparers[slot.group].registry.release(slot.operation)
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
